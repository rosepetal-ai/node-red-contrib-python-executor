---
title: "Python Environment Node - Managed Virtual Environments"
description: "Reference of the python-environment configuration node for Node-RED: creating a Python virtual environment from the editor, installing and uninstalling its packages with pip, running python-executor nodes in it, and the HTTP endpoints behind the dialog. Part of @rosepetal/node-red-contrib-python-executor."
sidebar_position: 2
head:
  - - meta
    - name: "keywords"
      content: "node-red, python environment, virtual environment, venv, pip, packages, configuration node, python executor, rosepetal"
  - - meta
    - property: "og:title"
      content: "Python Environment Node - @rosepetal/node-red-contrib-python-executor"
  - - meta
    - property: "og:description"
      content: "Create a Python virtual environment from the Node-RED editor, manage its packages with pip and run python-executor nodes in it."
---

# Node `python-environment`

Configuration node of `@rosepetal/node-red-contrib-python-executor`. It creates a Python virtual environment on the machine that runs Node-RED and installs and uninstalls its packages with pip, from the editor. A [python-executor](./python-executor.md) node that selects it in its **Environment** field runs its code with the interpreter of that environment.

The node stores only a name. The environment is a directory on disk, created and changed by the buttons of the edit dialog at the moment they are pressed, not by a deploy.

## Requirements

- Python 3.6 or later in the `PATH` of the Node-RED process. The node tries `python3` and then `python` (on Windows `python`, `python3` and `py`) and uses the first one that runs. If that one is older than 3.6, it reports `Python 3.6+ required` and does not try the next.
- The `venv` module of that Python. On Debian and Ubuntu it is a separate package: `sudo apt install python3-venv`.
- Access to the package index that pip is configured for, to install packages.

## Properties

| Property | Description |
|----------|-------------|
| **Name** | Name of the environment. Required. The directory of the environment is derived from it |

## Creating an environment

1. Open a **python executor** node, and in **Environment** choose to add a new `python-environment`.
2. The dialog looks for Python and shows the version and the path it found, or what is missing.
3. Write a **Name**. If an environment with that name exists already, the dialog shows `Environment already exists` and its packages, and the node uses it as it is.
4. Otherwise press **Create Environment**. The node runs `python -m venv` with the Python it found, upgrades pip inside the new environment, and shows `Environment created` with the Python version.
5. Install the packages the code needs (see [Managing packages](#managing-packages)), add the node, and deploy.

Once the environment exists, the **Name** field is disabled.

A node saved without pressing **Create Environment** has no environment behind it. Node-RED logs `Python environment "<name>" not found or invalid` when it starts the flows, and the `python-executor` nodes that use it fail: see [Errors](#errors).

## Managing packages

The dialog of an existing environment lists its packages with their versions, as `pip list` gives them.

- **Install**: write what you would pass to `pip install` and press **Install** or Enter. Several requirements are separated by spaces: `numpy opencv-python-headless`. A version goes without spaces inside the requirement: `pandas>=1.0`. Options of pip are accepted: `--upgrade numpy`. The output of pip appears in the dialog as it is produced, followed by `Installation complete` or `Installation failed` with the exit code.
- Commas do not separate requirements. The placeholder of the field shows a list with commas, but `numpy, pandas` fails: pip receives `numpy,` and rejects it as an invalid requirement.
- **Uninstall**: the trash button of a package runs `pip uninstall` for it, after a confirmation. `pip`, `setuptools` and `wheel` have no button.
- The button next to **Installed Packages** reads the list again. Right after **Create Environment** in a new configuration node the list stays empty until this button is pressed or a package is installed.

Packages are installed and removed at once, without a deploy. In cold mode the next message sees the change. In hot mode a worker that is running can import a newly installed package, and keeps the modules it has already imported: see [Hot mode](./python-executor.md#hot-mode).

## Deleting an environment

**Delete Environment** removes the directory of the environment, with all its packages, after a confirmation. It happens at once, also if the dialog is then canceled. The nodes that still select the environment no longer have an interpreter to start: see [Errors](#errors).

Deleting the configuration node in the editor does not remove the directory.

## Where environments are stored

```
~/.node-red/python-envs/<directory>/
```

- `~` is the home directory of the user that runs Node-RED. The path does not follow the user directory of Node-RED: with another `userDir`, the environments are still under `~/.node-red/python-envs`.
- `<directory>` is the name in lower case, with every character other than `a`-`z`, `0`-`9`, `_` and `-` replaced by `_`, repeated `_` joined, `_` removed from both ends, and cut to 50 characters. `My Env #1 (Vision)` gives `my_env_1_vision`.
- Two names that give the same directory are the same environment.
- The interpreter the `python-executor` nodes run is `bin/python` inside the directory (`Scripts\python.exe` on Windows).
- The file `.rosepetal-env-info.json` in the directory records the name, the creation date, the Python version and the path of the Python that created the environment.

The environment is not part of the flow. A flow exported to another machine carries the name of the environment: there, open the configuration node, press **Create Environment** and install the packages again.

## Errors

| Message | Where | Cause |
|---------|-------|-------|
| `Python not found. Please install Python 3.6+ and ensure it is in your PATH.` | Dialog | None of the Python commands runs in the `PATH` of the Node-RED process |
| `Python 3.6+ required. Found version ...` | Dialog | The first Python command that runs is older than 3.6 |
| `The venv module is not available. ...` | Dialog | Python is installed without `venv` |
| `Failed to create virtual environment: ...` | Dialog, after **Create Environment** | `python -m venv` failed; the text of its error follows. The partial directory is removed |
| `Python environment "<name>" not found or invalid` | Node-RED log, when the flows start | The directory of the environment does not exist or has no interpreter |
| `Python interpreter not found: <path>` | `python-executor` in cold mode, for every message | Same cause |
| `Hot worker pool failed to start: spawn <path> ENOENT` | `python-executor` in hot mode, when the flows start; then every message fails | Same cause |

## Limits

- The environment is always created from the first of `python3` and `python` found in the `PATH`. To use another interpreter, create a virtual environment yourself and write the path of its `python` in the **Python Path** of the `python-executor` node, without selecting an environment.
- An environment cannot be renamed: the name is fixed once it exists.

## HTTP endpoints

The dialog works through these endpoints of the Node-RED admin API. A script can call them too, for example to prepare the environments of a flow on a new machine without opening the editor.

- Paths are relative to the address of the editor (`httpAdminRoot`), and bodies are JSON. `<name>` is the name of the environment, URL-encoded; it gives the directory as described in [Where environments are stored](#where-environments-are-stored).
- When `adminAuth` is enabled, send the access token of a user who can deploy, as for the Node-RED Admin API: `Authorization: Bearer <token>`.
- A failure answers with a 4xx or 5xx status and its text in `error`. `validate` answers an environment that does not exist with `valid` set to `false`.

| Method and path | Body | Result |
|-----------------|------|--------|
| `POST /python-environment/detect-python` | none | The Python that environments are created from, as the dialog shows it: `success` (`true`), `python` (its path), `version` and `command`. Status 400 with an `errorType` such as `not_found`, `version_too_old` or `venv_missing` when no usable Python is found |
| `POST /python-environment/create-for-name` | `{"name": "vision"}` | Creates the environment as **Create Environment** does and answers `success`, `exists` (`false`), `name`, `sanitizedName`, `envPath`, `pythonPath` and `pythonVersion`. An environment whose interpreter exists is left as it is and answered with `exists` set to `true` |
| `GET /python-environment/by-name/<name>/validate` | none | `valid` (`true` when the interpreter of the environment exists), `envPath`, `pythonPath`, `pythonVersion` and `createdAt` |
| `GET /python-environment/by-name/<name>/packages` | none | `packages`: a list of `{"name": ..., "version": ...}`, as `pip list` gives them |
| `POST /python-environment/by-name/<name>/packages` | `{"package": "numpy opencv-python-headless"}` | Runs `pip install` with the requirements separated by spaces. The answer is a stream of server-sent events: `output` (`text`, and `type` `stdout` or `stderr`) for each piece of output, then `complete` (`code` and `success`). The status is 200 also when pip fails: read `success` |
| `DELETE /python-environment/by-name/<name>/packages/<package>` | none | Runs `pip uninstall -y` for the package and answers `success` (`true`) |
| `DELETE /python-environment/by-name/<name>` | none | Removes the directory of the environment and answers `success` (`true`). Status 404 when it does not exist |
| `GET /python-environment/list` | none | `environments`: every directory under `~/.node-red/python-envs`, with `name`, `sanitizedName`, `envPath`, `pythonPath`, `valid`, `createdAt` and `pythonVersion` |

The same operations exist for a deployed configuration node, with its id in place of `by-name/<name>`: `/python-environment/<id>/validate`, `/python-environment/<id>/packages`, `/python-environment/<id>/packages/<package>` and `/python-environment/<id>`. They answer 404 when no configuration node with that id is deployed. `POST /python-environment/create` takes the same body as `create-for-name`, but answers 400 when the directory of the environment exists already.
