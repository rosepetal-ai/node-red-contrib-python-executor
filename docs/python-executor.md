---
title: "Python Executor Node - Run Python Code on Each Message"
description: "Reference of the python-executor node for Node-RED: editor properties, how the Python code receives and returns the message, flow and global context, image helpers, cold and hot execution, status, errors and limits. Part of @rosepetal/node-red-contrib-python-executor."
sidebar_position: 1
head:
  - - meta
    - name: "keywords"
      content: "node-red, python executor, python function node, hot mode, worker pool, preload imports, flow context, opencv, numpy, timeout, rosepetal"
  - - meta
    - property: "og:title"
      content: "Python Executor Node - @rosepetal/node-red-contrib-python-executor"
  - - meta
    - property: "og:description"
      content: "Reference of the python-executor node: properties, input and output messages, context, image helpers, cold and hot execution, errors and limits."
---

# Node `python-executor`

Runs Python code on every message, in the way the function node runs JavaScript. The node has one input and one output, appears under the **function** category of the palette and is labeled **python executor** until it is given a name.

The code runs in a Python process outside Node-RED: a new one for each message (**cold** mode, the default) or one of a pool of processes that stay alive (**hot** mode). See [Execution modes](#execution-modes).

## Properties

| Property | Default | Description |
|----------|---------|-------------|
| **Name** | empty | Label of the node, and the key of its entry in `msg.performance` |
| **Environment** | none | A [python-environment](./python-environment.md) configuration node. The code runs with the interpreter of that virtual environment |
| **Python Path** | `python3` | Interpreter used when no environment is selected: a command looked up in the `PATH` of the Node-RED process, or a full path. The field is hidden while an environment is selected |
| **Timeout (ms)** | `5000` | Time allowed for one message. The spinner of the field goes from 100 to 60000 in steps of 100; an empty value or `0` means 5000 |
| **Hot Mode** | off | Keep Python processes alive between messages instead of starting one for each message |
| **Workers** | `1` | Number of processes of the hot pool, from 1 to 10. Shown in hot mode |
| **Reload Workers** | | Button, shown in hot mode: restarts the workers and runs the preload imports again without a deploy. It does not act on the workers of an environment: see [Limits](#limits) |
| **Preload Imports** | empty | Python statements that every hot worker runs once when it starts. Shown in hot mode; ignored in cold mode, where the editor warns about it if the field has content |
| **Python Code** | an example | The code to run |

A new node comes with example code that doubles `msg.payload` and converts an image in `msg.image`. Replace it: on a message without `msg.image` it fails with `KeyError: 'image'`.

## The code

The text of **Python Code** becomes the body of a function that receives the message:

```python
def user_function(msg):
    # the code of the node, indented
```

So `return` is valid at the first level of the code, and `import` statements can be written in it. These names exist without importing anything:

| Name | Description |
|------|-------------|
| `msg` | The message, as a `dict`: see [Input message](#input-message) |
| `flow_ctx`, `global_ctx` | Flow and global context: see [Context and warnings](#context-and-warnings) |
| `node` | `node.warn(text)` sends a warning to the debug sidebar |
| `rp_to_cv`, `rp_from_cv` | Image helpers: see [Images](#images) |

```python
msg['payload'] = f"Hello, {msg.get('payload', 'world')}!"
return msg
```

With empty code the message passes through unchanged.

## Input message

`msg` is a Python `dict` built from the Node-RED message.

### Which properties are sent

When the node starts it scans the text of the code for message properties written literally, as `msg['name']`, `msg["name"]` or `msg.get('name', ...)`. Only those properties are sent to Python, together with `_msgid`. If the code names no property in that way, the whole message is sent.

- The scan reads text: a property named in a comment or inside a string counts.
- `msg[key]` with a variable, `'name' in msg`, `msg.keys()` and a loop over `msg` see only the properties that were sent.
- The properties that are not sent are not lost: they are in the message that leaves the node. See [Output message](#output-message).
- This lets the node work after an **http in** node: `msg.req` and `msg.res` cannot be converted, and they are not sent unless the code names them.

### How values are converted

The message travels as JSON, in both modes:

| In Node-RED | In Python |
|-------------|-----------|
| object | `dict` |
| array | `list` |
| string | `str` |
| number | `int` or `float` |
| boolean | `bool` |
| `null` | `None` |
| `Date` | `str`, in ISO 8601 |
| `undefined`, function | The property is dropped |
| `Buffer`, typed array | Hot mode: `bytes`. Cold mode: the JSON form of the value, which for a `Buffer` is the `dict` `{"type": "Buffer", "data": [1, 2, ...]}` |

A message with circular references in the properties that are sent cannot be converted, and the node reports an error: see [Errors](#errors).

## Output message

The node sends the incoming message with the returned `dict` merged over it: every key of the returned value replaces the property of the same name, and the other properties stay as they arrived.

- Return `msg` after changing it, or a new `dict` with only the properties to set.
- Code that returns nothing, or `None`, sends the incoming message unchanged. Unlike the function node, the node cannot drop a message: it sends one message for each input, unless the code raises an exception.
- `del msg['name']` does not remove the property from the outgoing message. Set it to `None` to send `null`.
- Return a `dict`. A returned list is not sent as several messages: its items become the properties `0`, `1`, ... of the message, as do the characters of a returned string. A returned number or boolean is ignored.

The returned value must be made of `dict`, `list`, `tuple` (sent as an array), `str`, `int`, `float`, `bool` and `None`. In addition:

- `bytes`, `bytearray` and `memoryview` values, at any depth, leave the node as `Buffer`, in both modes.
- Any other type (a `set`, a NumPy array, a `datetime`) fails with `TypeError: Object of type ... is not JSON serializable`. Convert it first, for example with `array.tolist()` or with `rp_from_cv`.
- `float('nan')` and the infinities cannot be returned: see [Errors](#errors).
- In cold mode, a `Buffer` that the code returns as it received it (for example with `return msg`) leaves the node as the plain object `{type: "Buffer", data: [...]}`, not as a `Buffer`. Return a `dict` with only the properties the code changed, or use hot mode.

### Performance data

Every message that leaves the node, changed or not, carries the times of the execution in `msg.performance`, under the name of the node, or under `python executor` if the node has no name:

```javascript
msg.performance = {
  "python executor": {
    transferToPythonMs: 0.02,  // reading and decoding the message in Python
    executionMs: 0.01,         // running the code
    transferToJsMs: 0.02,      // encoding the result in Python
    totalMs: 0.74              // whole time in the node, measured in Node-RED
  }
}
```

All values are milliseconds. The entries that other nodes wrote to `msg.performance` are kept; two nodes with the same name write the same entry.

## Context and warnings

```python
count = flow_ctx.get("count", 0)
flow_ctx.set("count", count + 1)
node.warn(f"count is now {count + 1}")
return msg
```

`flow_ctx` and `global_ctx` read and write the flow and global context, in the default context store. Both have `get(key, default=None)` and `set(key, value)`, and accept `ctx[key]` and `ctx[key] = value`.

- **Reads need a literal key.** The node scans the code for `flow_ctx.get("key"`, `flow_ctx["key"]` and the same with `global_ctx`, and loads only those keys. A key built while the code runs reads as missing: `get` returns the default.
- **Values are read when the message arrives and written when the code ends.** Writes are applied before the message is sent, and also when the code raises an exception. Messages that overlap do not see each other's writes: the counter above loses counts when a message arrives before the previous one has finished.
- **Values are converted as message values are.** They must be JSON types; `bytes` are stored as `Buffer`.
- `node.warn(text)` shows a warning of the node in the debug sidebar when the code ends, also when it raises an exception. `node` has no other method: there is no `node.error`, `node.send`, `node.status` or `node.log`.

## Images

The helpers convert between the image object that the [`@rosepetal/node-red-contrib-image-tools`](https://docs.rosepetal.ai/flows/node-red-contrib-image-tools) nodes produce and a NumPy array in the channel order OpenCV works in. They exist in both modes, and they need NumPy in the interpreter that runs the code. OpenCV is not required: it is used for the channel swap when it is installed.

```javascript
{
  data: Buffer,        // raw pixels
  width: number,
  height: number,
  channels: number,    // 1, 3 or 4
  colorSpace: string,  // "GRAY", "RGB", "RGBA", "BGR", "BGRA"
  dtype: string        // name of a NumPy dtype, "uint8" when missing
}
```

```python
import cv2

img = rp_to_cv(msg["payload"])          # BGR ndarray, ready for cv2
img = cv2.GaussianBlur(img, (7, 7), 0)
msg["payload"] = rp_from_cv(img, like=msg["payload"])
return msg
```

| Helper | Result |
|--------|--------|
| `rp_to_cv(image)` | A NumPy array of the `dtype` of the image (`uint8` if it has none), with shape `(height, width)` for one channel and `(height, width, channels)` otherwise. When `colorSpace` is `RGB` or `RGBA`, or the image has none, the channels are reordered to BGR or BGRA; an image in another color space is left as it is. `data` can be `bytes`, the `dict` a `Buffer` becomes in cold mode, a list of integers or a base64 string. Raises `ValueError` when `data` does not hold width × height × channels values |
| `rp_from_cv(array, like=None)` | An image object with `data` as `bytes` (a `Buffer` once it leaves the node) and `width`, `height`, `channels` and `dtype` taken from the array. The array is taken as BGR and reordered to RGB, unless `like` has a `colorSpace` that is not `RGB` or `RGBA`. The `colorSpace` of the result is the one of `like` (`RGB` without `like`), or `GRAY` for an array without a channel axis |

In cold mode every message imports NumPy and OpenCV again and the pixels travel as a JSON list of numbers. Use hot mode for images.

## Execution modes

### Cold mode

The default. For every message the node starts the interpreter, writes the message to its standard input as JSON and reads the result from its standard output.

- Nothing remains from one message to the next: imports run every time and variables are lost.
- There is no queue. Every message starts its own process when it arrives, so several can run at the same time.
- The code must not write to standard output: a `print()` corrupts the result and the message fails with `Failed to parse Python output`. What the code writes to standard error is discarded when it succeeds. Use `node.warn`.
- `bytes` that the code returns travel through a temporary file that the node reads and deletes. The file is written to `/dev/shm`, or where that directory does not exist, to the directory in `TMPDIR` (`/tmp` if it is not set).
- **Preload Imports** is ignored.

### Hot mode

The node keeps a pool of Python processes, the **Workers**, that stay alive. Each one runs one message at a time.

- **Start.** The pool starts when the node is deployed: each worker starts the interpreter and then runs the preload imports. The status goes from `hot: starting` to `hot: ready`.
- **Messages that arrive before the pool is ready**, after a deploy or during a reload, are run in cold mode. They do not have the names defined in the preload imports.
- **Queue.** A message goes to a free worker. When all are busy it waits in a queue without a size limit, and the wait counts as part of the timeout.
- **State.** The code is compiled once in each worker, and a variable declared `global` keeps its value between the messages a worker handles. Workers do not share memory: with more than one worker, each has its own copy.
- **Preload Imports** runs in the namespace the code uses for its global names, so the modules it imports and everything it defines are available to the code:

  ```python
  import cv2
  import numpy as np
  ```

- **Shared pools.** Nodes with the same environment, the same number of workers and the same preload imports share one pool, and its global names with it: a `global` variable set by one node is visible to the others. Each node keeps its own code. The pool stops when the last node that uses it is closed.
- **Binary data.** A `Buffer` travels as raw bytes next to the JSON, in both directions, without base64 and without temporary files.
- **`print()`** goes to the standard error of the worker. It is written to the console of the Node-RED process as `Worker 0 error: Error: Worker stderr: ...` followed by a stack trace; it does not reach the debug sidebar.
- **Recovery.** A worker that ends (the code calls `os._exit`, the process crashes, a message times out) is started again and runs the preload imports again. The message it was running fails.
- **Installed packages.** A running worker can import a package installed after it started, and keeps the modules it has already imported: after upgrading a package, the workers have to be restarted to load the new version.

## Status

The node updates its status at most once every 50 ms; when several changes fall in that time, the last one is shown.

| Status | Meaning |
|--------|---------|
| `cold: running` (blue) | The Python process of a message has been started |
| `cold: 25ms` (green) | The message finished in that time. Cleared after 3 seconds without messages |
| `error` (red) | Cold mode: the code raised an exception or the process ended with an error |
| `timeout` (red) | Cold mode: the message exceeded the timeout |
| `spawn error` (red) | Cold mode: the interpreter could not be started |
| `cold: parse error` (red) | Cold mode: the output of the process was not the expected JSON |
| `input error` (red) | Cold mode: the message could not be written to the process |
| `hot: starting` (gray) | The pool is starting, or starting again after **Reload Workers** |
| `hot: ready (workers 2, busy 0, queue 0)` (green) | The pool is ready. The numbers are the workers of the pool, those running a message and the messages waiting |
| `hot: running (...)` (blue) | A message has been given to the pool |
| `hot: 3ms (...)` (green) | The message finished in that time. Returns to `hot: ready` after 3 seconds without messages |
| `hot: error (3ms) (...)` (red) | The code raised an exception, or the worker ended |
| `hot: timeout (...)` (red) | The message exceeded the timeout |
| `hot: disabled` (yellow), `hot: failed` (red) | The pool could not start |
| `hot: dispatch failed` (red) | The message could not be given to the pool |

## Errors

The node reports an error with the message attached, so it appears in the debug sidebar and a **catch** node receives it. No message is sent to the output.

| Situation | Error |
|-----------|-------|
| The code raises an exception | The type and the text of the exception, as in `KeyError: 'image'`. There is no traceback and no line number. The context values written and the warnings issued before the exception are still applied |
| Syntax or indentation error in the code | Reported for every message. Hot mode: `SyntaxError: ... (<string>, line 3)`, where the line is one more than the line in the editor. Cold mode: the text Python prints, where the line number also counts the more than 200 lines the node puts before the code |
| A message exceeds the timeout | `Python execution timed out after 5000ms`. Cold mode: the process is killed. Hot mode: a message that was waiting leaves the queue; for one that was running, the worker is killed and started again |
| The interpreter does not exist, cold mode | `Python interpreter not found: <path>` |
| The hot pool cannot start: the interpreter does not exist, a worker does not answer within 5 seconds, or a preload import fails | `Hot worker pool failed to start: <reason>` when the node is deployed. Every message then fails with the reason; the node does not fall back to cold mode. Correct the cause and deploy again |
| The result has a type that JSON does not have | `TypeError: Object of type set is not JSON serializable` |
| The result has `NaN` or an infinity | Cold mode: `Failed to parse Python output: ...`. Hot mode: `Error: Worker exited unexpectedly`, and the worker is started again |
| `print()` in cold mode | `Failed to parse Python output: ...` |
| The properties to send have circular references | Cold mode: `Failed to send input to Python: Converting circular structure to JSON ...`. Hot mode: `Error: Message nesting too deep (circular structure?)` |
| A hot worker ends while it runs a message | `Error: Worker exited unexpectedly` |
| The cold process ends with an error that is not an exception of the code | The text the process wrote to standard error, or `Python execution failed` |
| The configuration node of the selected environment does not exist | Warning `Python environment not found, using fallback`; the node uses **Python Path** |

## Limits

- One output and one message for each input: the code cannot drop a message, and cannot send several.
- Only JSON types and bytes cross between Node-RED and Python, in both directions.
- Errors carry the type and the text of the exception, without the traceback.
- Context is read when the message arrives and written when the code ends: it is not a lock, and overlapping messages overwrite each other's values.
- Of the Node-RED API, the code has `node.warn` and the flow and global context. There is no node context, and no access to other context stores than the default one.
- In hot mode, without an environment, the **Python Path** is not part of what identifies a pool: two nodes with different paths, the same number of workers and the same preload imports share the pool of the one that started first, and both run on its interpreter. To separate them, make the number of workers or the text of the preload imports differ (a comment is enough), or use an [environment](./python-environment.md).
- **Reload Workers** identifies the pool by the number of workers and the preload imports saved in the node, as for a node without environment. For a node that uses an environment it starts or restarts a separate set of workers on the **Python Path** interpreter and leaves the workers of the environment running. To restart those, restart the flows (**Restart Flows** in the deploy menu, or a full deploy).

## Environment variables

The node reads these variables from the environment of the Node-RED process. The Python processes inherit that environment.

| Variable | Effect |
|----------|--------|
| `ROSEPETAL_PY_INLINE_MAX_BYTES` | Hot mode. A `Buffer` of this size in bytes or larger travels through a file in `/dev/shm` instead of next to the JSON. Not set: every buffer travels next to the JSON |
| `ROSEPETAL_PY_MALLOC_TUNING` | Hot mode, Linux. The node starts the workers with `MALLOC_MMAP_THRESHOLD_` (32 MiB), `MALLOC_TRIM_THRESHOLD_` (128 MiB) and `MALLOC_TOP_PAD_` (16 MiB), so that the memory of large buffers is reused between messages. Each one is set only if the environment does not define it already. `0` disables the three |
