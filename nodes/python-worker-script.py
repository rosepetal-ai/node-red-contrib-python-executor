#!/usr/bin/env python3
"""
Persistent Python Worker for Node-RED Python Executor
Stays alive and processes multiple messages without restart overhead
"""

import json
import sys
import traceback
from typing import Dict, Tuple

# Cache compiled user functions so imports and state persist per code string
_CODE_CACHE: Dict[str, Tuple[object, Dict[str, object]]] = {}

# Global namespaces per node/pool to share preload imports
_NODE_GLOBALS: Dict[str, Dict[str, object]] = {}


def _send_message(payload: Dict[str, object]) -> None:
    """Send a JSON payload to stdout using length-prefixed framing."""
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


def main():
    """Main worker loop - stays alive and processes messages continuously"""

    # Signal that worker is ready
    _send_message({"status": "ready"})

    while True:
        try:
            # Read message length first (protocol: length\n + json data)
            line = sys.stdin.readline()
            if not line:
                # EOF - exit gracefully
                break

            length = int(line.strip())

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
                if is_preload:
                    namespace = _get_namespace(node_id)
                    exec(user_code, namespace, namespace)
                    result = {}
                else:
                    user_function, _ = _get_or_compile(node_id, cache_key, user_code)

                    # Call the user function
                    result = user_function(msg)

                # Send response
                response = {
                    "status": "success",
                    "request_id": request_id,
                    "result": result if result is not None else {},
                }

            except Exception as exc:
                # Send error response
                response = {
                    "status": "error",
                    "request_id": request_id,
                    "error": str(exc),
                    "type": type(exc).__name__,
                    "traceback": traceback.format_exc(),
                }

            # Send response (protocol: length\n + json data)
            _send_message(response)

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
