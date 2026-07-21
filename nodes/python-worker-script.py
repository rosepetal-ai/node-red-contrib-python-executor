#!/usr/bin/env python3
"""
Persistent Python Worker for Node-RED Python Executor
Stays alive and processes multiple messages without restart overhead
"""

import base64
import json
import os
import sys
import time
import traceback
import uuid
from typing import Any, Dict, Optional, Tuple

# Cache compiled user functions so imports and state persist per code string
_CODE_CACHE: Dict[str, Tuple[object, Dict[str, object]]] = {}

# Global namespaces per node/pool to share preload imports
_NODE_GLOBALS: Dict[str, Dict[str, object]] = {}

SHARED_SENTINEL_KEY = "__rosepetal_shm_path__"
SHARED_BASE64_KEY = "__rosepetal_base64__"
SHM_DIR = "/dev/shm" if os.path.isdir("/dev/shm") else os.path.abspath(os.getenv("TMPDIR", "/tmp"))
MSG_WRAPPER_KEY = "__rosepetal_msg"
CONTEXT_WRAPPER_KEY = "__rosepetal_context"


class _ContextProxy:
    """Proxy for flow/global context with get/set semantics."""

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

    def __init__(self, logs: Optional[list] = None):
        self._logs = logs if logs is not None else []

    def warn(self, message: Any) -> None:
        self._logs.append({"level": "warn", "message": str(message)})


def _send_message(payload: Dict[str, object], *, encode_start: Optional[float] = None) -> None:
    """Send a JSON payload to stdout using length-prefixed framing.

    When encode_start is provided alongside a payload.performance dict, the
    helper computes the elapsed time (ms) since encode_start and stores it in
    payload["performance"]["transfer_to_js_ms"] before emitting the frame.
    """
    message_json = json.dumps(payload, separators=(",", ":"))

    if (
        encode_start is not None
        and isinstance(payload.get("performance"), dict)
    ):
        payload["performance"]["transfer_to_js_ms"] = (time.perf_counter() - encode_start) * 1000.0
        message_json = json.dumps(payload, separators=(",", ":"))

    sys.stdout.write(f"{len(message_json)}\n{message_json}\n")
    sys.stdout.flush()


# ---------------------------------------------------------------------------
# Rosepetal <-> OpenCV image helpers
#
# Injected into every user namespace (see _get_namespace) so flows can convert
# between the Rosepetal image dict and an OpenCV/NumPy array with no boilerplate:
#
#     img = rp_to_cv(msg["payload"])                 # -> BGR uint8 ndarray
#     msg["payload"] = rp_from_cv(img, like=msg["payload"])
#
# Rosepetal image dict shape: {data, width, height, channels, colorSpace, dtype}
# `data` may arrive as raw bytes (shared-memory fast path), a Node Buffer-JSON
# object, a plain list of ints, or a base64 string; all are accepted.
# ---------------------------------------------------------------------------

def _rp_coerce_bytes(data: Any) -> bytes:
    """Normalize the many transport encodings of image `data` into raw bytes."""
    if isinstance(data, (bytes, bytearray, memoryview)):
        return bytes(data)
    if isinstance(data, dict) and data.get("type") == "Buffer" and isinstance(data.get("data"), list):
        return bytes(data["data"])
    if isinstance(data, list):
        return bytes(data)
    if isinstance(data, str):
        return base64.b64decode(data)
    raise TypeError(f"Unsupported image data type: {type(data).__name__}")


def _rp_swap_rb(arr):
    """Swap the R and B channels (RGB<->BGR); keeps a 4th alpha channel intact."""
    if arr.ndim == 3 and arr.shape[2] == 3:
        return arr[..., ::-1]
    if arr.ndim == 3 and arr.shape[2] == 4:
        return arr[..., [2, 1, 0, 3]]
    return arr


def rp_to_cv(obj: Dict[str, Any]):
    """Convert a Rosepetal image dict into an OpenCV-ready ndarray (BGR uint8).

    Honors ``colorSpace`` and ``dtype``. RGB/RGBA inputs are reordered to
    BGR/BGRA so the array works directly with cv2. Grayscale stays 2-D.
    """
    import numpy as np

    data = _rp_coerce_bytes(obj["data"])
    H, W, C = int(obj["height"]), int(obj["width"]), int(obj["channels"])
    dtype = np.dtype(obj.get("dtype", "uint8"))

    buf = np.frombuffer(data, dtype=dtype)
    expected = H * W * C
    if buf.size != expected:
        raise ValueError(f"buffer size {buf.size} != {expected} ({W}x{H}x{C} {dtype})")

    img = buf.reshape((H, W, C)) if C > 1 else buf.reshape((H, W))

    if C >= 3 and str(obj.get("colorSpace", "RGB")).upper().startswith("RGB"):
        img = _rp_swap_rb(img)

    return np.ascontiguousarray(img)


def rp_from_cv(arr, like: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """Convert an OpenCV/NumPy array back into a Rosepetal image dict.

    The array is assumed to be BGR (OpenCV convention) and is reordered back to
    RGB for the Rosepetal side. Pass the original dict as ``like`` to preserve
    its ``colorSpace`` label. ``data`` is returned as raw bytes so the worker
    streams it through shared memory automatically.
    """
    import numpy as np

    arr = np.asarray(arr)
    color_space = str((like or {}).get("colorSpace", "RGB")).upper()

    out = arr
    if arr.ndim == 3 and arr.shape[2] >= 3 and color_space.startswith("RGB"):
        out = _rp_swap_rb(arr)  # BGR -> RGB is the same channel swap

    out = np.ascontiguousarray(out)
    H, W = out.shape[:2]
    C = 1 if out.ndim == 2 else out.shape[2]

    return {
        "data": out.tobytes(),
        "width": int(W),
        "height": int(H),
        "channels": int(C),
        "colorSpace": color_space if C >= 3 else "GRAY",
        "dtype": str(out.dtype),
    }


def _inject_image_helpers(namespace: Dict[str, object]) -> None:
    """Expose the Rosepetal<->OpenCV helpers inside a user namespace."""
    namespace["rp_to_cv"] = rp_to_cv
    namespace["rp_from_cv"] = rp_from_cv


def _get_namespace(node_key: str) -> Dict[str, object]:
    """Return the persistent global namespace for a given node/pool key."""
    key = node_key or "__default__"
    if key not in _NODE_GLOBALS:
        namespace: Dict[str, object] = {"__builtins__": __builtins__}
        _inject_image_helpers(namespace)
        _NODE_GLOBALS[key] = namespace
    return _NODE_GLOBALS[key]


def _get_or_compile(node_key: str, cache_key: str, user_code: str):
    """Return (function, globals) tuple for the provided user code."""
    cached = _CODE_CACHE.get(cache_key)
    if cached:
        return cached

    # Ensure the generated function body is valid Python
    if not user_code.strip():
        indented_body = "    pass"
    else:
        lines = user_code.splitlines()
        indented_lines = ["    " + line for line in lines]
        indented_body = "\n".join(indented_lines)

    function_source = f"def user_function(msg):\n{indented_body}\n"

    # Build or reuse namespace so preloaded imports persist
    global_namespace = _get_namespace(node_key)
    local_namespace: Dict[str, object] = {}

    exec(function_source, global_namespace, local_namespace)
    user_function = local_namespace["user_function"]

    _CODE_CACHE[cache_key] = (user_function, global_namespace)
    return user_function, global_namespace


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


def _restore_shared_placeholders(value: Any) -> Any:
    """Replace shared-memory placeholders with actual bytes (or decode base64)."""
    if isinstance(value, dict):
        if SHARED_SENTINEL_KEY in value:
            file_path = value.get(SHARED_SENTINEL_KEY)
            return _read_shared_file(file_path)

        if SHARED_BASE64_KEY in value:
            encoded = value.get(SHARED_BASE64_KEY) or ""
            try:
                return base64.b64decode(encoded)
            except Exception:
                return b""

        return {key: _restore_shared_placeholders(child) for key, child in value.items()}

    if isinstance(value, list):
        return [_restore_shared_placeholders(item) for item in value]

    if isinstance(value, tuple):
        return tuple(_restore_shared_placeholders(item) for item in value)

    return value


def _ensure_shared_dir() -> None:
    """Ensure the shared memory directory exists."""
    try:
        os.makedirs(SHM_DIR, exist_ok=True)
    except OSError:
        pass


def _write_shared_file(data: bytes) -> str:
    """Persist bytes to a shared memory file and return its path."""
    _ensure_shared_dir()
    file_path = os.path.join(SHM_DIR, f"rosepetal-python-{os.getpid()}-{uuid.uuid4().hex}")
    with open(file_path, "wb") as fh:
        fh.write(data)
    return file_path


def _encode_shared_outputs(value: Any) -> Any:
    """Traverse the result object converting bytes into shared-memory descriptors."""
    if isinstance(value, dict):
        return {key: _encode_shared_outputs(child) for key, child in value.items()}

    if isinstance(value, list):
        return [_encode_shared_outputs(item) for item in value]

    if isinstance(value, tuple):
        return [_encode_shared_outputs(item) for item in value]

    if isinstance(value, (bytes, bytearray, memoryview)):
        data = bytes(value)
        try:
            file_path = _write_shared_file(data)
            return {
                SHARED_SENTINEL_KEY: file_path,
                "length": len(data)
            }
        except OSError:
            encoded = base64.b64encode(data).decode("ascii") if data else ""
            return {
                SHARED_BASE64_KEY: encoded,
                "length": len(data)
            }

    return value


def main():
    """Main worker loop - stays alive and processes messages continuously"""

    # Signal that worker is ready
    _send_message({"status": "ready"})

    while True:
        try:
            transfer_in_start: Optional[float] = None

            # Read message length first (protocol: length\n + json data)
            line = sys.stdin.readline()
            if not line:
                # EOF - exit gracefully
                break

            length = int(line.strip())
            transfer_in_start = time.perf_counter()

            # Read the JSON message
            json_data = sys.stdin.read(length)
            if not json_data:
                break

            # Parse input
            request = json.loads(json_data)

            # Extract message and user code
            raw_msg = request.get("msg", {})
            user_code = request.get("code", "")
            request_id = request.get("request_id", "unknown")
            node_id = request.get("node_id") or "__default__"
            cache_key = f"{node_id}:{user_code}"
            is_preload = bool(request.get("preload"))
            context_payload: Any = {}

            if isinstance(raw_msg, dict) and MSG_WRAPPER_KEY in raw_msg:
                context_payload = raw_msg.get(CONTEXT_WRAPPER_KEY) or {}
                msg = raw_msg.get(MSG_WRAPPER_KEY, {})
            else:
                msg = raw_msg

            # Execute user code
            try:
                msg = _restore_shared_placeholders(msg)
                context_payload = _restore_shared_placeholders(context_payload)
                after_restore = time.perf_counter()
                transfer_to_python_ms = 0.0
                if transfer_in_start is not None:
                    transfer_to_python_ms = (after_restore - transfer_in_start) * 1000.0

                execution_ms = 0.0
                context_updates = {"flow": {}, "global": {}}
                logs = []
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
                    exec_start = time.perf_counter()
                    exec(user_code, namespace, namespace)
                    exec_end = time.perf_counter()
                    execution_ms = (exec_end - exec_start) * 1000.0
                    result_object: Any = {}
                else:
                    user_function, global_namespace = _get_or_compile(node_id, cache_key, user_code)
                    global_namespace["flow_ctx"] = flow_ctx
                    global_namespace["global_ctx"] = global_ctx
                    global_namespace["node"] = node_proxy

                    # Call the user function
                    exec_start = time.perf_counter()
                    result_value = user_function(msg)
                    exec_end = time.perf_counter()
                    execution_ms = (exec_end - exec_start) * 1000.0
                    result_object = result_value if result_value is not None else {}

                transfer_out_start = time.perf_counter()
                result_object = _encode_shared_outputs(result_object)
                encoded_context_updates = _encode_shared_outputs(context_updates)

                # Send response
                response = {
                    "status": "success",
                    "request_id": request_id,
                    "result": result_object,
                    "performance": {
                        "transfer_to_python_ms": transfer_to_python_ms,
                        "execution_ms": execution_ms,
                        "transfer_to_js_ms": 0.0,
                    },
                    "context_updates": encoded_context_updates,
                    "logs": logs,
                }

            # Send response (protocol: length\n + json data)
                _send_message(response, encode_start=transfer_out_start)

            except Exception as exc:
                error_performance = {
                    "transfer_to_python_ms": (
                        (time.perf_counter() - transfer_in_start) * 1000.0
                        if transfer_in_start is not None
                        else 0.0
                    ),
                    "execution_ms": locals().get("execution_ms", 0.0),
                    "transfer_to_js_ms": 0.0,
                }
                transfer_out_start = time.perf_counter()
                # Send error response
                response = {
                    "status": "error",
                    "request_id": request_id,
                    "error": str(exc),
                    "type": type(exc).__name__,
                    "traceback": traceback.format_exc(),
                    "performance": error_performance,
                    "context_updates": _encode_shared_outputs(locals().get("context_updates", {"flow": {}, "global": {}})),
                    "logs": locals().get("logs", []),
                }
                _send_message(response, encode_start=transfer_out_start)

        except KeyboardInterrupt:
            # Graceful shutdown
            break
        except Exception as exc:
            # Worker error - try to recover
            try:
                _send_message(
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
