"""Rosepetal <-> OpenCV image helpers shared by both execution modes.

Hot mode imports this module (see python-worker-script.py); cold mode inlines
its source into the generated script (see python-executor.js). Keeping the
single copy here is what stops the two modes drifting apart.

    img = rp_to_cv(msg["payload"])                 # -> BGR uint8 ndarray
    msg["payload"] = rp_from_cv(img, like=msg["payload"])

Rosepetal image dict shape: {data, width, height, channels, colorSpace, dtype}
`data` may arrive as raw bytes (shared-memory fast path), a Node Buffer-JSON
object, a plain list of ints, or a base64 string; all are accepted.
"""

import base64
from typing import Any, Dict, Optional


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
    its ``colorSpace`` label. ``data`` is returned as raw bytes so the caller
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
