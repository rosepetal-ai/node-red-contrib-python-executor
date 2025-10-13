# node-red-contrib-rosepetal-python-executor

Bring Python into your Node-RED flows with a node that feels familiar, stays friendly, and keeps things fast when you need it.

## What You Get
- Run small Python snippets whenever a message arrives.
- Reuse your favorite Python tools without leaving Node-RED.
- Choose between quick start-up or high-speed processing.
- See clear status updates so you always know what is happening.

## Before You Start
- Install Python 3 on the machine running Node-RED.
- Know where your Python interpreter lives (for most systems it is `python3`).

## Node Options
- **Name** – Optional label that appears on the canvas.
- **Python Path** – The command Node-RED should run (for example `python3` or a full path if you use a virtual environment).
- **Timeout** – How long to wait for Python to finish before giving up. Useful to stop code that gets stuck.
- **Python Code** – Your script. It always receives a message named `msg`, and whatever you return is passed along.
- **Hot Mode** – Keeps Python processes running between messages for faster responses.
- **Workers** – How many hot workers to run in parallel (only appears when Hot Mode is on). Use more workers when you expect bursts of messages.
- **Preload Imports** – Optional lines that run once when each hot worker starts. Handy for heavier libraries you do not want to import on every message.

## Cold vs Hot Mode
- **Cold Mode (default)** – Starts a fresh Python process for every message. Great for occasional runs or quick experiments.
- **Hot Mode** – Keeps a pool of Python workers ready. Messages are handled much faster after the first one. Best for frequent or time-sensitive flows.

## Typical Flow
1. Drop the **python executor** node into your flow.
2. Connect an Inject node (input) and a Debug node (output).
3. Add a short script, for example:
   ```python
   msg['payload'] = f"Hello, {msg.get('payload', 'world')}!"
   return msg
   ```
4. Deploy and trigger the flow. Adjust options as needed.

## Benefits At A Glance
- **Familiar:** Works just like the standard function node, only in Python.
- **Flexible:** Supports both simple scripts and larger libraries.
- **Fast:** Hot mode cuts response time dramatically for repeat work.
- **Clear:** Status messages and notifications help you see what is working and what needs attention.

Enjoy mixing Python logic into your Node-RED projects without extra fuss. When you are ready for more performance, flip on Hot Mode and keep building.***
