const crypto = require('crypto');
const { spawn } = require('child_process');
const { PythonWorkerPool } = require('./python-worker');

// Global worker pools (one per unique configuration)
const workerPools = new Map();

function getHotStats(pool) {
    if (!pool || typeof pool.getStats !== 'function') {
        return null;
    }
    try {
        return pool.getStats();
    } catch (err) {
        return null;
    }
}

function createPoolKey(pythonPath, poolSize, preloadImports) {
    const hash = crypto.createHash('md5').update(preloadImports || '').digest('hex');
    return `${pythonPath}_${poolSize}_${hash}`;
}

function buildHotStatusSuffix(pool) {
    const stats = getHotStats(pool);
    if (!stats || typeof stats.total === 'undefined') {
        return '';
    }
    const parts = [];
    parts.push(`workers ${stats.total}`);
    if (typeof stats.busy === 'number') {
        parts.push(`busy ${stats.busy}`);
    }
    if (typeof stats.queue === 'number') {
        parts.push(`queue ${stats.queue}`);
    }
    return ` (${parts.join(', ')})`;
}

function setHotStatus(node, fill, shape, text) {
    if (!node) {
        return;
    }
    const suffix = node.hotMode ? buildHotStatusSuffix(node.workerPool) : '';
    node.status({ fill, shape, text: suffix ? `${text}${suffix}` : text });
}

function logHotStats(node, message) {
    if (!node || !node.hotMode || typeof node.debug !== 'function') {
        return;
    }
    const stats = getHotStats(node.workerPool);
    if (stats) {
        node.debug(`${message} (workers=${stats.total}, ready=${stats.ready}, busy=${stats.busy}, queue=${stats.queue})`);
    } else {
        node.debug(`${message} (worker stats unavailable)`);
    }
}

function flushHotQueue(node, error) {
    if (!node || !node.hotPending || node.hotPending.length === 0) {
        return;
    }

    if (!error && (!node.useHot || !node.workerPool || typeof node.workerPool.isReady !== 'function' || !node.workerPool.isReady())) {
        // Still waiting for hot workers; leave queue intact
        return;
    }

    const pending = node.hotPending.splice(0);

    pending.forEach((entry) => {
        if (error) {
            const errObj = error instanceof Error ? error : new Error(String(error));
            if (typeof entry.done === 'function') {
                entry.done(errObj);
            } else if (node) {
                node.error(errObj, entry.msg);
            }
        } else {
            executeHotMode(node, entry.msg, entry.send, entry.done);
        }
    });
}

function detachPoolReadyWatcher(node) {
    if (node && node.workerPool && node.poolReadyHandler && typeof node.workerPool.removeListener === 'function') {
        node.workerPool.removeListener('ready', node.poolReadyHandler);
    }
    if (node && node.workerPool && node.poolErrorHandler && typeof node.workerPool.removeListener === 'function') {
        node.workerPool.removeListener('error', node.poolErrorHandler);
    }
    if (node && node.workerPool && node.poolReloadHandler && typeof node.workerPool.removeListener === 'function') {
        node.workerPool.removeListener('reload-start', node.poolReloadHandler);
    }
    if (node) {
        node.poolReadyHandler = null;
        node.poolErrorHandler = null;
        node.poolReloadHandler = null;
    }
}

function attachPoolReadyWatcher(node) {
    if (!node || !node.workerPool || typeof node.workerPool.isReady !== 'function') {
        return;
    }

    if (node.workerPool.isReady()) {
        node.useHot = true;
        node.hotError = null;
        setHotStatus(node, "green", "dot", "hot: ready");
        logHotStats(node, 'Hot worker pool ready');
        flushHotQueue(node);
        return;
    }

    node.useHot = false;

    if (!node.poolReadyHandler) {
        node.poolReadyHandler = () => {
            node.poolReadyHandler = null;
            node.poolErrorHandler = null;
            node.useHot = true;
            node.hotError = null;
            setHotStatus(node, "green", "dot", "hot: ready");
            logHotStats(node, 'Hot worker pool ready');
            flushHotQueue(node);
        };

        if (typeof node.workerPool.once === 'function') {
            node.workerPool.once('ready', node.poolReadyHandler);
        }
    }

    if (!node.poolErrorHandler) {
        node.poolErrorHandler = (error) => {
            detachPoolReadyWatcher(node);
            node.useHot = false;
            node.hotError = error instanceof Error ? error : new Error(String(error));
            node.status({ fill: "red", shape: "ring", text: "hot: failed" });
            const message = error && error.message ? error.message : String(error);
            node.error(`Hot worker pool failed to start: ${message}`);
            flushHotQueue(node, node.hotError);
        };

        if (typeof node.workerPool.once === 'function') {
            node.workerPool.once('error', node.poolErrorHandler);
        }
    }

    if (!node.poolReloadHandler) {
        node.poolReloadHandler = () => {
            node.useHot = false;
            node.hotError = null;
            node.status({ fill: "grey", shape: "ring", text: "hot: reloading" });
            attachPoolReadyWatcher(node);
        };

        if (typeof node.workerPool.on === 'function') {
            node.workerPool.on('reload-start', node.poolReloadHandler);
        }
    }

    node.status({ fill: "grey", shape: "ring", text: "hot: starting" });
}

function queueHotMessage(node, msg, send, done) {
    if (!node.hotPending) {
        node.hotPending = [];
    }

    node.hotPending.push({ msg, send, done });
    attachPoolReadyWatcher(node);
    if (node.hotMode) {
        node.status({ fill: "grey", shape: "ring", text: "hot: queueing" });
    }
}

module.exports = function(RED) {
    function PythonExecutorNode(config) {
        RED.nodes.createNode(this, config);
        const node = this;

        // Configuration
        node.func = config.func || "";
        node.outputs = 1;
        node.timeout = config.timeout || 5000;
        node.pythonPath = config.pythonPath || "python3";
        node.hotMode = config.hotMode !== undefined ? config.hotMode : false;
        node.workerPoolSize = config.workerPoolSize || 1;
        node.preloadImports = (config.preloadImports || "").trim();
        node.useHot = !!node.hotMode;
        node.hotError = null;

        // Worker pool management (hot mode)
        node.workerPool = null;
        node.workerPoolKey = null;
        node.hotPending = [];
        node.poolReadyHandler = null;
        node.poolErrorHandler = null;
        node.poolReloadHandler = null;

        // Initialize worker pool if hot mode is enabled
        let poolCreated = false;

        if (node.hotMode) {
            node.workerPoolKey = createPoolKey(node.pythonPath, node.workerPoolSize, node.preloadImports);

            if (!workerPools.has(node.workerPoolKey)) {
                // Create new worker pool
                const pool = new PythonWorkerPool(node.pythonPath, node.workerPoolSize, node.preloadImports, node.workerPoolKey);
                poolCreated = true;

                pool.initialize()
                    .then(() => {
                        if (node.preloadImports && node.preloadImports.trim()) {
                            node.log(`Hot mode preloaded imports executed for ${node.workerPoolSize} worker(s)`);
                        }
                        node.log(`Hot mode enabled: ${node.workerPoolSize} worker(s) ready (python: ${node.pythonPath})`);
                    })
                    .catch((error) => {
                        node.error(`Failed to initialize worker pool: ${error.message}`);
                        node.status({ fill: "yellow", shape: "ring", text: "hot: disabled" });
                        detachPoolReadyWatcher(node);
                        workerPools.delete(node.workerPoolKey);
                        node.workerPool = null;
                        node.useHot = false;
                        node.hotError = error instanceof Error ? error : new Error(String(error));
                        flushHotQueue(node, node.hotError);
                    });

                workerPools.set(node.workerPoolKey, pool);
            }

            node.workerPool = workerPools.get(node.workerPoolKey);

            if (!poolCreated && node.workerPool) {
                node.log(`Hot mode using existing worker pool (python: ${node.pythonPath}, workers: ${node.workerPoolSize})`);
            }

            attachPoolReadyWatcher(node);
        }

        // Handle incoming messages
        node.on('input', function(msg, send, done) {
            // For Node-RED 0.x compatibility
            send = send || function() { node.send.apply(node, arguments); };
            done = done || function(err) {
                if (err) {
                    node.error(err, msg);
                }
            };

            if (node.hotMode) {
                const existingPool = node.workerPoolKey ? workerPools.get(node.workerPoolKey) : null;
                if (existingPool && node.workerPool !== existingPool) {
                    detachPoolReadyWatcher(node);
                    node.workerPool = existingPool;
                    node.hotError = null;
                    node.useHot = true;
                }

                if (node.workerPool) {
                    if (typeof node.workerPool.isReady === 'function' && node.workerPool.isReady()) {
                        node.hotError = null;
                        node.useHot = true;
                    }
                    attachPoolReadyWatcher(node);
                }
            }

            if (node.hotMode && node.hotError) {
                const poolReady = node.workerPool && typeof node.workerPool.isReady === 'function' ? node.workerPool.isReady() : false;
                if (poolReady) {
                    node.hotError = null;
                    node.useHot = true;
                } else {
                    const errObj = node.hotError instanceof Error ? node.hotError : new Error(String(node.hotError));
                    node.status({ fill: "red", shape: "ring", text: "hot: failed" });
                    done(errObj);
                    return;
                }
            }

            // Choose execution mode
            if (node.useHot && node.workerPool) {
                const poolReady = typeof node.workerPool.isReady === 'function' ? node.workerPool.isReady() : false;

                if (!poolReady) {
                    queueHotMessage(node, msg, send, done);
                    return;
                }

                executeHotMode(node, msg, send, done);
            } else {
                executeColdMode(node, msg, send, done);
            }
        });

        // Clean up on node close
        node.on('close', function() {
            detachPoolReadyWatcher(node);
            node.hotPending = [];
            node.useHot = false;
            node.status({});
            // Note: Worker pools are shared and cleaned up globally
        });
    }

    /**
     * Execute Python code in HOT mode (persistent worker)
     */
    function executeHotMode(node, msg, send, done) {
        // Show running status
        setHotStatus(node, "blue", "dot", "hot: running");
        logHotStats(node, 'Dispatching message to hot worker');

        const startTime = Date.now();
        let timedOut = false;
        let cancelHandle = null;

        // Set timeout
        const timeoutId = setTimeout(() => {
            timedOut = true;
            setHotStatus(node, "red", "ring", "hot: timeout");
            logHotStats(node, 'Hot execution timed out');

            if (cancelHandle && typeof cancelHandle.cancel === 'function') {
                cancelHandle.cancel('Python execution timed out');
            }

            done(new Error(`Python execution timed out after ${node.timeout}ms`));
        }, node.timeout);

        // Execute on worker pool
        cancelHandle = node.workerPool.execute(msg, node.func, (error, result) => {
            if (timedOut) {
                return;
            }

            clearTimeout(timeoutId);

            const execTime = Date.now() - startTime;

            if (error) {
                setHotStatus(node, "red", "ring", `hot: error (${execTime}ms)`);
                logHotStats(node, `Hot execution failed after ${execTime}ms`);
                const errorMessage = error && (error.message || error.toString());
                done(new Error(`${error.type || 'Error'}: ${errorMessage}`));
                return;
            }

            // Merge result into original message
            const outputMsg = Object.assign({}, msg, result);

            // Send output
            send(outputMsg);
            setHotStatus(node, "green", "dot", `hot: ${execTime}ms`);
            logHotStats(node, `Hot execution completed in ${execTime}ms`);

            // Clear status after 3 seconds
            setTimeout(() => {
                setHotStatus(node, "green", "dot", "hot: ready");
            }, 3000);

            done();
            }, { nodeId: node.workerPoolKey });

        if (!cancelHandle || typeof cancelHandle.cancel !== 'function') {
            cancelHandle = null;
        }
    }

    /**
     * Execute Python code in COLD mode (spawn new process)
     */
    function executeColdMode(node, msg, send, done) {
        // Show running status
        node.status({ fill: "blue", shape: "dot", text: "cold: running" });

        const startTime = Date.now();

        // Prepare Python script
        const pythonScript = `
import sys
import json

try:
    # Read input message from stdin
    input_data = sys.stdin.read()
    msg = json.loads(input_data)

    # Define user function to allow return statements
    def user_function(msg):
${node.func.split('\n').map(line => '        ' + line).join('\n')}

    # Execute user function
    result = user_function(msg)

    # Output the result
    if result is not None:
        print(json.dumps(result))
    else:
        print(json.dumps({}))

except Exception as e:
    error_msg = {
        "error": str(e),
        "type": type(e).__name__
    }
    print(json.dumps(error_msg), file=sys.stderr)
    sys.exit(1)
`;

            // Spawn Python process
            const pythonProcess = spawn(node.pythonPath, ['-c', pythonScript]);

            let stdoutData = '';
            let stderrData = '';
            let timedOut = false;

            // Set timeout
            const timeoutId = setTimeout(() => {
                timedOut = true;
                pythonProcess.kill();
                node.status({ fill: "red", shape: "ring", text: "timeout" });
                done(new Error(`Python execution timed out after ${node.timeout}ms`));
            }, node.timeout);

            // Collect stdout
            pythonProcess.stdout.on('data', (data) => {
                stdoutData += data.toString();
            });

            // Collect stderr
            pythonProcess.stderr.on('data', (data) => {
                stderrData += data.toString();
            });

            // Handle process completion
            pythonProcess.on('close', (code) => {
                clearTimeout(timeoutId);

                if (timedOut) {
                    return; // Already handled by timeout
                }

                if (code !== 0) {
                    // Python script failed
                    let errorMessage = 'Python execution failed';

                    try {
                        const errorObj = JSON.parse(stderrData);
                        errorMessage = `${errorObj.type}: ${errorObj.error}`;
                    } catch (e) {
                        errorMessage = stderrData || errorMessage;
                    }

                    node.status({ fill: "red", shape: "ring", text: "error" });
                    done(new Error(errorMessage));
                    return;
                }

                // Parse output
                try {
                    const result = JSON.parse(stdoutData.trim());
                    const execTime = Date.now() - startTime;

                    // Merge result into original message
                    const outputMsg = Object.assign({}, msg, result);

                    // Send output
                    send(outputMsg);
                    node.status({ fill: "green", shape: "dot", text: `cold: ${execTime}ms` });

                    // Clear status after 3 seconds
                    setTimeout(() => {
                        node.status({});
                    }, 3000);

                    done();
                } catch (e) {
                    node.status({ fill: "red", shape: "ring", text: "cold: parse error" });
                    done(new Error(`Failed to parse Python output: ${e.message}`));
                }
            });

            // Handle process errors
            pythonProcess.on('error', (err) => {
                clearTimeout(timeoutId);
                node.status({ fill: "red", shape: "ring", text: "spawn error" });

                if (err.code === 'ENOENT') {
                    done(new Error(`Python interpreter not found: ${node.pythonPath}`));
                } else {
                    done(new Error(`Failed to spawn Python process: ${err.message}`));
                }
            });

            // Send input message to Python stdin
            try {
                const inputJson = JSON.stringify(msg);
                pythonProcess.stdin.write(inputJson);
                pythonProcess.stdin.end();
            } catch (e) {
                clearTimeout(timeoutId);
                pythonProcess.kill();
                node.status({ fill: "red", shape: "ring", text: "input error" });
                done(new Error(`Failed to send input to Python: ${e.message}`));
            }
    }

    RED.nodes.registerType("python-executor", PythonExecutorNode);

    // Cleanup worker pools on Node-RED shutdown
    RED.events.on("runtime-event", function(event) {
        if (event.id === "runtime-shutdown") {
            workerPools.forEach((pool, key) => {
                pool.stop();
            });
            workerPools.clear();
        }
    });

    const needWritePermission = RED.auth && RED.auth.needsPermission ? RED.auth.needsPermission('python-executor.write') : function(req, res, next) { next(); };

    RED.httpAdmin.post("/python-executor/:id/reload", needWritePermission, function(req, res) {
        const body = req.body || {};
        const hotMode = !!body.hotMode;

        if (!hotMode) {
            res.status(400).json({ error: "Hot mode must be enabled to reload workers" });
            return;
        }

        const pythonPath = body.pythonPath || "python3";
        let workerPoolSize = parseInt(body.workerPoolSize, 10);
        if (isNaN(workerPoolSize) || workerPoolSize < 1) {
            workerPoolSize = 1;
        }
        if (workerPoolSize > 10) {
            workerPoolSize = 10;
        }

        const preloadImports = (body.preloadImports || "").trim();
        const poolKey = createPoolKey(pythonPath, workerPoolSize, preloadImports);

        let pool = workerPools.get(poolKey);

        if (!pool) {
            pool = new PythonWorkerPool(pythonPath, workerPoolSize, preloadImports, poolKey);
            workerPools.set(poolKey, pool);

            pool.initialize()
                .then(() => {
                    res.json({ status: "ok", created: true });
                })
                .catch((error) => {
                    workerPools.delete(poolKey);
                    res.status(500).json({ error: error.message || String(error) });
                });

            return;
        }

        pool.reload(preloadImports)
            .then(() => {
                res.json({ status: "ok", reloaded: true });
            })
            .catch((error) => {
                res.status(500).json({ error: error.message || String(error) });
            });
    });
};
