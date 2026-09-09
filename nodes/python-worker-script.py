#!/usr/bin/env python3
"""
Persistent Python Worker for Node-RED Python Executor (hot mode).

Stays alive and processes messages continuously. Talks to Node over a binary,
length-prefixed frame protocol on stdin/stdout:

    u32-le header_length | header JSON (ASCII) | blob_0 | blob_1 | ...

The header carries the request/response body. Binary values (Buffers in Node,
bytes in Python) never go through JSON: small ones ride inline as trailing
blobs (``header["bin"]`` lists their byte lengths and a ``{"__rosepetal_bin__": i}``
descriptor marks their position), large ones go through a shared-memory file
(``{"__rosepetal_shm_path__": path}``). Both directions use the same scheme.

The protocol channel is moved to a private file descriptor at start-up so that
user ``print()`` calls (or C libraries writing to fd 1) end up on stderr and can
never corrupt the framing.
"""

import base64
import json
import os
import struct
import sys
import time
import traceback
import uuid
from typing import Any, Dict, Optional, Tuple

# ---------------------------------------------------------------------------
# Protocol channel setup (must happen before anything prints)
# ---------------------------------------------------------------------------
_PROTO_FD = os.dup(1)
os.dup2(2, 1)  # fd 1 now points at stderr: print() can no longer corrupt frames
_OUT = os.fdopen(_PROTO_FD, "wb", buffering=1 << 16)
_IN = sys.stdin.buffer

# Node (libuv) hands us AF_UNIX socketpairs, not pipes. Ask for bigger socket
# buffers so large frames need fewer wake-ups; the kernel silently clamps the
# request to net.core.{w,r}mem_max, so this is a no-op on default sysctls.
try:
    import socket
    import stat

    for _fd, _opt in ((_IN.fileno(), socket.SO_RCVBUF), (_PROTO_FD, socket.SO_SNDBUF)):
        try:
            if stat.S_ISSOCK(os.fstat(_fd).st_mode):
                _sock = socket.socket(fileno=os.dup(_fd))
                try:
                    _sock.setsockopt(socket.SOL_SOCKET, _opt, 1 << 20)
                finally:
                    _sock.close()
        except OSError:
            pass
except Exception:  # pragma: no cover
    pass
try:
    sys.stdout.reconfigure(line_buffering=True)  # type: ignore[attr-defined]
except Exception:  # pragma: no cover - Python < 3.7
    pass

# Cache compiled user functions keyed by the code id Node sends (hash of pool key + code)
_CODE_CACHE: Dict[str, Tuple[object, Dict[str, object]]] = {}

# Global namespaces per node/pool to share preload imports
_NODE_GLOBALS: Dict[str, Dict[str, object]] = {}

SHARED_SENTINEL_KEY = "__rosepetal_shm_path__"
SHARED_BASE64_KEY = "__rosepetal_base64__"
INLINE_KEY = "__rosepetal_bin__"
MSG_WRAPPER_KEY = "__rosepetal_msg"
CONTEXT_WRAPPER_KEY = "__rosepetal_context"
SHM_DIR = "/dev/shm" if os.path.isdir("/dev/shm") else os.path.abspath(os.getenv("TMPDIR", "/tmp"))

# Blobs at or above this size are handed to Node through a shared-memory file
# instead of the pipe. Node passes its own threshold so both sides agree; the
# default is "never" because the pipe measured faster at every size.
_DEFAULT_INLINE_MAX = 1 << 62
try:
    INLINE_MAX_BYTES = int(os.getenv("ROSEPETAL_PY_INLINE_MAX_BYTES", _DEFAULT_INLINE_MAX))
except ValueError:
    INLINE_MAX_BYTES = _DEFAULT_INLINE_MAX

_JSON_SEPARATORS = (",", ":")
_perf_counter = time.perf_counter


class _ContextProxy:
    """Proxy for flow/global context with get/set semantics."""

    __slots__ = ("_data", "_updates")

    def __init__(self, data: Optional[Dict[str, Any]] = None, updates: Optional[Dict[str, Any]] = None):
        self._data = data or {}
        self._updates = updates if updates is not None else {}

    def get(self, key: str, default: Any = None) -> Any:
        return self._data.get(key, default)

    def set(self, key: str, value: Any) -> None:
        self._data[key] = value
        self._updates[key] = value

    def __getitem__(self, key: str) -> Any:
        return self.get(key)

    def __setitem__(self, key: str, value: Any) -> None:
        self.set(key, value)


class _NodeProxy:
    """Proxy for node.warn calls."""

    __slots__ = ("_logs",)

    def __init__(self, logs: Optional[list] = None):
        self._logs = logs if logs is not None else []

    def warn(self, message: Any) -> None:
        self._logs.append({"level": "warn", "message": str(message)})


# Rosepetal <-> OpenCV image helpers live in a sibling module so cold mode
# (which inlines their source) and hot mode stay in sync. See rp_image_helpers.py.
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from rp_image_helpers import rp_to_cv, rp_from_cv  # noqa: E402


# ---------------------------------------------------------------------------
# Frame I/O
# ---------------------------------------------------------------------------
def _read_exact(n: int) -> bytes:
    if n <= 0:
        return b""
    data = _IN.read(n)  # BufferedReader.read loops until n bytes or EOF
    if data is None or len(data) != n:
        raise EOFError("stdin closed")
    return data


def _send_frame(header: str, blobs=()) -> None:
    encoded = header.encode("utf-8")
    write = _OUT.write
    write(struct.pack("<I", len(encoded)))
    write(encoded)
    for blob in blobs:
        write(blob)
    _OUT.flush()


def _send_json(payload: Dict[str, object]) -> None:
    _send_frame(json.dumps(payload, separators=_JSON_SEPARATORS))


# ---------------------------------------------------------------------------
# Binary value transport
# ---------------------------------------------------------------------------
def _ensure_shared_dir() -> None:
    try:
        os.makedirs(SHM_DIR, exist_ok=True)
    except OSError:
        pass


def _write_shared_file(data) -> str:
    _ensure_shared_dir()
    file_path = os.path.join(SHM_DIR, f"rosepetal-python-{os.getpid()}-{uuid.uuid4().hex}")
    with open(file_path, "wb") as fh:
        fh.write(data)
    return file_path


def _read_shared_file(file_path: str) -> bytes:
    """Read binary contents from a shared-memory file and unlink it."""
    if not file_path or not isinstance(file_path, str):
        return b""

    try:
        with open(file_path, "rb") as fh:
            data = fh.read()
    except OSError:
        return b""

    try:
        os.unlink(file_path)
    except OSError:
        pass

    return data


class _BinCollector:
    """json.dumps ``default`` hook: swaps bytes-like values for transport descriptors.

    Doing it inside the serializer means the result tree is walked exactly once
    (no separate encode pass) and only the binary leaves are touched.
    """

    __slots__ = ("blobs", "lengths")

    def __init__(self):
        self.blobs = []
        self.lengths = []

    def default(self, value):
        if isinstance(value, (bytes, bytearray, memoryview)):
            if isinstance(value, memoryview):
                value = value.tobytes()
            length = len(value)
            if length >= INLINE_MAX_BYTES:
                try:
                    return {SHARED_SENTINEL_KEY: _write_shared_file(value), "length": length}
                except OSError:
                    pass
            self.blobs.append(value)
            self.lengths.append(length)
            return {INLINE_KEY: len(self.blobs) - 1, "length": length}
        raise TypeError(f"Object of type {type(value).__name__} is not JSON serializable")


def _restore_binaries(value: Any, blobs) -> Any:
    """Replace transport descriptors with the actual bytes (only called when some exist)."""
    value_type = type(value)
    if value_type is dict:
        if INLINE_KEY in value:
            try:
                return blobs[value[INLINE_KEY]]
            except (IndexError, TypeError):
                return b""
        if SHARED_SENTINEL_KEY in value:
            return _read_shared_file(value.get(SHARED_SENTINEL_KEY))
        if SHARED_BASE64_KEY in value:
            try:
                return base64.b64decode(value.get(SHARED_BASE64_KEY) or "")
            except Exception:
                return b""
        return {key: _restore_binaries(child, blobs) for key, child in value.items()}

    if value_type is list:
        return [_restore_binaries(item, blobs) for item in value]

    return value


# ---------------------------------------------------------------------------
# User code management
# ---------------------------------------------------------------------------
def _inject_image_helpers(namespace: Dict[str, object]) -> None:
    """Expose the Rosepetal<->OpenCV helpers inside a user namespace."""
    namespace["rp_to_cv"] = rp_to_cv
    namespace["rp_from_cv"] = rp_from_cv


def _get_namespace(node_key: str) -> Dict[str, object]:
    """Return the persistent global namespace for a given node/pool key."""
    key = node_key or "__default__"
    namespace = _NODE_GLOBALS.get(key)
    if namespace is None:
        namespace = {"__builtins__": __builtins__}
        _inject_image_helpers(namespace)
        _NODE_GLOBALS[key] = namespace
    return namespace


def _compile_user_code(node_key: str, code_id: str, user_code: str):
    """Compile user code into user_function(msg) and cache it under code_id."""
    if not user_code.strip():
        indented_body = "    pass"
    else:
        indented_body = "\n".join("    " + line for line in user_code.splitlines())

    function_source = f"def user_function(msg):\n{indented_body}\n"

    global_namespace = _get_namespace(node_key)
    local_namespace: Dict[str, object] = {}

    exec(function_source, global_namespace, local_namespace)
    user_function = local_namespace["user_function"]

    entry = (user_function, global_namespace)
    _CODE_CACHE[code_id] = entry
    return entry


# ---------------------------------------------------------------------------
# Request handling
# ---------------------------------------------------------------------------
def _dumps_id(request_id) -> str:
    return json.dumps(request_id)


def _handle_request(header: Dict[str, Any], blobs, transfer_in_start: float) -> None:
    request_id = header.get("id")
    request_id_json = _dumps_id(request_id)
    code_id = header.get("code_id") or ""
    node_id = header.get("node_id") or "__default__"
    is_preload = bool(header.get("preload"))
    msg = header.get("msg", {})
    context_payload = header.get("ctx") or {}
    if type(msg) is dict and MSG_WRAPPER_KEY in msg:
        # Legacy wrapped payload shape: {__rosepetal_msg, __rosepetal_context}
        context_payload = msg.get(CONTEXT_WRAPPER_KEY) or context_payload
        msg = msg.get(MSG_WRAPPER_KEY, {})

    execution_ms = 0.0
    context_updates = {"flow": {}, "global": {}}
    logs: list = []

    try:
        if blobs or header.get("shm"):
            msg = _restore_binaries(msg, blobs)
            context_payload = _restore_binaries(context_payload, blobs)
        transfer_to_python_ms = (_perf_counter() - transfer_in_start) * 1000.0

        if isinstance(context_payload, dict):
            flow_data = context_payload.get("flow") or {}
            global_data = context_payload.get("global") or {}
        else:
            flow_data = {}
            global_data = {}
        flow_ctx = _ContextProxy(flow_data, context_updates["flow"])
        global_ctx = _ContextProxy(global_data, context_updates["global"])
        node_proxy = _NodeProxy(logs)

        if is_preload:
            namespace = _get_namespace(node_id)
            namespace["flow_ctx"] = flow_ctx
            namespace["global_ctx"] = global_ctx
            namespace["node"] = node_proxy
            exec_start = _perf_counter()
            exec(header.get("code") or "", namespace, namespace)
            execution_ms = (_perf_counter() - exec_start) * 1000.0
            result_object: Any = {}
        else:
            entry = _CODE_CACHE.get(code_id)
            if entry is None:
                user_code = header.get("code")
                if user_code is None:
                    # Node believed we had this code cached (e.g. after a
                    # restart); ask it to resend with the source attached.
                    _send_frame(
                        '{"id":' + request_id_json + ',"status":"error","type":"UnknownCode",'
                        '"error":"code not cached","code_known":false}'
                    )
                    return
                entry = _compile_user_code(node_id, code_id, user_code)
            user_function, global_namespace = entry
            global_namespace["flow_ctx"] = flow_ctx
            global_namespace["global_ctx"] = global_ctx
            global_namespace["node"] = node_proxy

            exec_start = _perf_counter()
            result_value = user_function(msg)
            execution_ms = (_perf_counter() - exec_start) * 1000.0
            result_object = result_value if result_value is not None else {}

        transfer_out_start = _perf_counter()
        collector = _BinCollector()
        result_json = json.dumps(result_object, default=collector.default, separators=_JSON_SEPARATORS)
        context_json = json.dumps(context_updates, default=collector.default, separators=_JSON_SEPARATORS)
        transfer_to_js_ms = (_perf_counter() - transfer_out_start) * 1000.0

        header_json = (
            '{"id":' + request_id_json
            + ',"status":"success","code_known":' + ("true" if code_id in _CODE_CACHE else "false")
            + ',"performance":' + json.dumps(
                {
                    "transfer_to_python_ms": transfer_to_python_ms,
                    "execution_ms": execution_ms,
                    "transfer_to_js_ms": transfer_to_js_ms,
                },
                separators=_JSON_SEPARATORS,
            )
            + ',"logs":' + json.dumps(logs, separators=_JSON_SEPARATORS)
            + ',"bin":' + json.dumps(collector.lengths, separators=_JSON_SEPARATORS)
            + ',"result":' + result_json
            + ',"context_updates":' + context_json
            + "}"
        )
        _send_frame(header_json, collector.blobs)

    except Exception as exc:
        error_performance = {
            "transfer_to_python_ms": (_perf_counter() - transfer_in_start) * 1000.0,
            "execution_ms": execution_ms,
            "transfer_to_js_ms": 0.0,
        }
        collector = _BinCollector()
        try:
            context_json = json.dumps(context_updates, default=collector.default, separators=_JSON_SEPARATORS)
        except Exception:
            collector = _BinCollector()
            context_json = '{"flow":{},"global":{}}'
        response = {
            "id": request_id,
            "status": "error",
            "code_known": code_id in _CODE_CACHE,
            "error": str(exc),
            "type": type(exc).__name__,
            "traceback": traceback.format_exc(),
            "performance": error_performance,
            "logs": logs,
            "bin": collector.lengths,
        }
        header_json = json.dumps(response, separators=_JSON_SEPARATORS)
        header_json = header_json[:-1] + ',"context_updates":' + context_json + "}"
        _send_frame(header_json, collector.blobs)


def main():
    """Main worker loop - stays alive and processes messages continuously"""

    # Signal that worker is ready
    _send_frame('{"status":"ready"}')

    read = _IN.read
    unpack_prefix = struct.Struct("<I").unpack
    loads = json.loads

    while True:
        try:
            prefix = read(4)
            if not prefix or len(prefix) < 4:
                break  # EOF - exit gracefully

            (header_length,) = unpack_prefix(prefix)
            transfer_in_start = _perf_counter()

            header = loads(_read_exact(header_length))
            lengths = header.get("bin")
            blobs = [_read_exact(n) for n in lengths] if lengths else ()

            _handle_request(header, blobs, transfer_in_start)

        except (EOFError, KeyboardInterrupt):
            break
        except Exception as exc:
            # Worker error - try to recover
            try:
                _send_json(
                    {
                        "status": "worker_error",
                        "error": str(exc),
                        "type": type(exc).__name__,
                        "traceback": traceback.format_exc(),
                    }
                )
            except Exception:
                # Cannot recover - exit
                break


if __name__ == "__main__":
    main()
