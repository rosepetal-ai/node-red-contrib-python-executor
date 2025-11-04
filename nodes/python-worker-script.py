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


def _get_namespace(node_key: str) -> Dict[str, object]:
    """Return the persistent global namespace for a given node/pool key."""
    key = node_key or "__default__"
    if key not in _NODE_GLOBALS:
        _NODE_GLOBALS[key] = {"__builtins__": __builtins__}
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
            msg = request.get("msg", {})
            user_code = request.get("code", "")
            request_id = request.get("request_id", "unknown")
            node_id = request.get("node_id") or "__default__"
            cache_key = f"{node_id}:{user_code}"
            is_preload = bool(request.get("preload"))

            # Execute user code
            try:
                msg = _restore_shared_placeholders(msg)
                after_restore = time.perf_counter()
                transfer_to_python_ms = 0.0
                if transfer_in_start is not None:
                    transfer_to_python_ms = (after_restore - transfer_in_start) * 1000.0

                execution_ms = 0.0

                if is_preload:
                    namespace = _get_namespace(node_id)
                    exec_start = time.perf_counter()
                    exec(user_code, namespace, namespace)
                    exec_end = time.perf_counter()
                    execution_ms = (exec_end - exec_start) * 1000.0
                    result_object: Any = {}
                else:
                    user_function, _ = _get_or_compile(node_id, cache_key, user_code)

                    # Call the user function
                    exec_start = time.perf_counter()
                    result_value = user_function(msg)
                    exec_end = time.perf_counter()
                    execution_ms = (exec_end - exec_start) * 1000.0
                    result_object = result_value if result_value is not None else {}

                transfer_out_start = time.perf_counter()
                result_object = _encode_shared_outputs(result_object)

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
