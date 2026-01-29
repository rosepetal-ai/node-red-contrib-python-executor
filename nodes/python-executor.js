const crypto = require('crypto');
const fs = require('fs');
const { spawn } = require('child_process');
const { PythonWorkerPool } = require('./python-worker');

const MSG_WRAPPER_KEY = "__rosepetal_msg";
const CONTEXT_WRAPPER_KEY = "__rosepetal_context";
const SHARED_SENTINEL_KEY = "__rosepetal_shm_path__";
const SHARED_BASE64_KEY = "__rosepetal_base64__";

// Global worker pools (one per unique configuration)
// Map key -> { pool: PythonWorkerPool, refCount: number }
const workerPools = new Map();

function getWorkerPoolEntry(key) {
    return workerPools.get(key) || null;
}

function getWorkerPool(key) {
    const entry = getWorkerPoolEntry(key);
    return entry ? entry.pool : null;
}

function acquireWorkerPool(key, pythonPath, poolSize, preloadImports) {
    let created = false;
    let entry = workerPools.get(key);

    if (!entry) {
        entry = {
            pool: new PythonWorkerPool(pythonPath, poolSize, preloadImports, key),
            refCount: 0
        };
        workerPools.set(key, entry);
        created = true;
    }

    entry.refCount += 1;

    return { pool: entry.pool, created };
}

function releaseWorkerPool(key) {
    const entry = workerPools.get(key);

    if (!entry) {
        return Promise.resolve();
    }

    entry.refCount = Math.max(0, entry.refCount - 1);

    if (entry.refCount === 0) {
        workerPools.delete(key);
        return entry.pool.stop().catch((err) => {
            console.error(`Failed to stop worker pool for ${key}:`, err);
        });
    }

    return Promise.resolve();
}

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

function createPoolKey(pythonPath, poolSize, preloadImports, environmentId) {
    const hash = crypto.createHash('md5').update(preloadImports || '').digest('hex');
    const envKey = environmentId || 'direct';
    return `${envKey}_${poolSize}_${hash}`;
}

function extractMsgKeysFromCode(code) {
    const keys = new Set();
    if (typeof code !== 'string' || !code.trim()) {
        return keys;
    }

    // msg['payload'] or msg["payload"]
    const bracketRegex = /msg\[['"]([A-Za-z0-9_.$:-]+)['"]\]/g;
    let match = bracketRegex.exec(code);
    while (match) {
        keys.add(match[1]);
        match = bracketRegex.exec(code);
    }

    // msg.get('payload', default)
    const getRegex = /msg\.get\(\s*['"]([^'"]+)['"]/g;
    match = getRegex.exec(code);
    while (match) {
        keys.add(match[1]);
        match = getRegex.exec(code);
    }

    // msg.payload style access (skip common dict methods)
    const skipMethods = new Set(['get', 'items', 'keys', 'values', 'copy', 'pop', 'popitem', 'clear', 'update', 'setdefault']);
    const dotRegex = /msg\.([A-Za-z_][A-Za-z0-9_]*)/g;
    match = dotRegex.exec(code);
    while (match) {
        if (!skipMethods.has(match[1])) {
            keys.add(match[1]);
        }
        match = dotRegex.exec(code);
    }

    return keys;
}

function buildPythonInputMsg(originalMsg, code) {
    if (!originalMsg || typeof originalMsg !== 'object') {
        return originalMsg;
    }

    const keys = extractMsgKeysFromCode(code);

    // If we cannot confidently determine keys, fall back to full message
    if (!keys || keys.size === 0) {
        return originalMsg;
    }

    const subset = {};
    keys.forEach((key) => {
        if (Object.prototype.hasOwnProperty.call(originalMsg, key)) {
            subset[key] = originalMsg[key];
        }
    });

    // Preserve _msgid for traceability
    if (Object.prototype.hasOwnProperty.call(originalMsg, '_msgid') && !Object.prototype.hasOwnProperty.call(subset, '_msgid')) {
        subset._msgid = originalMsg._msgid;
    }

    return subset;
}

function extractContextKeysFromCode(code) {
    const flowKeys = new Set();
    const globalKeys = new Set();

    if (typeof code !== 'string' || !code.trim()) {
        return { flow: flowKeys, global: globalKeys };
    }

    const flowGetRegex = /flow_ctx\.get\(\s*['"]([^'"]+)['"]/g;
    const flowBracketRegex = /flow_ctx\[['"]([^'"]+)['"]\]/g;
    const globalGetRegex = /global_ctx\.get\(\s*['"]([^'"]+)['"]/g;
    const globalBracketRegex = /global_ctx\[['"]([^'"]+)['"]\]/g;

    let match = flowGetRegex.exec(code);
    while (match) {
        flowKeys.add(match[1]);
        match = flowGetRegex.exec(code);
    }

    match = flowBracketRegex.exec(code);
    while (match) {
        flowKeys.add(match[1]);
        match = flowBracketRegex.exec(code);
    }

    match = globalGetRegex.exec(code);
    while (match) {
        globalKeys.add(match[1]);
        match = globalGetRegex.exec(code);
    }

    match = globalBracketRegex.exec(code);
    while (match) {
        globalKeys.add(match[1]);
        match = globalBracketRegex.exec(code);
    }

    return { flow: flowKeys, global: globalKeys };
}

function buildPythonContextSnapshot(node, code) {
    const empty = { flow: {}, global: {} };
    if (!node || typeof node.context !== 'function') {
        return empty;
    }

    const keys = extractContextKeysFromCode(code);
    const flowKeys = keys.flow;
    const globalKeys = keys.global;

    if ((!flowKeys || flowKeys.size === 0) && (!globalKeys || globalKeys.size === 0)) {
        return empty;
    }

    const context = node.context();
    if (!context) {
        return empty;
    }

    const flowContext = context.flow;
    const globalContext = context.global;
    const flowSnapshot = {};
    const globalSnapshot = {};

    const coerceBufferLike = (value) => {
        if (!value || typeof value !== 'object') {
            return value;
        }
        if (Buffer.isBuffer(value)) {
            return value;
        }
        if (value.type === 'Buffer' && Array.isArray(value.data)) {
            return Buffer.from(value.data);
        }
        if (ArrayBuffer.isView(value) && !(value instanceof DataView)) {
            return Buffer.from(value.buffer, value.byteOffset, value.byteLength);
        }
        return value;
    };

    if (flowContext && typeof flowContext.get === 'function') {
        flowKeys.forEach((key) => {
            try {
                flowSnapshot[key] = coerceBufferLike(flowContext.get(key));
            } catch (err) {
                // Ignore context read errors to avoid blocking execution
            }
        });
    }

    if (globalContext && typeof globalContext.get === 'function') {
        globalKeys.forEach((key) => {
            try {
                globalSnapshot[key] = coerceBufferLike(globalContext.get(key));
            } catch (err) {
                // Ignore context read errors to avoid blocking execution
            }
        });
    }

    return { flow: flowSnapshot, global: globalSnapshot };
}

function buildPythonPayload(pythonMsg, contextSnapshot) {
    return {
        [MSG_WRAPPER_KEY]: pythonMsg,
        [CONTEXT_WRAPPER_KEY]: contextSnapshot || { flow: {}, global: {} }
    };
}

function applyContextUpdates(node, updates, msg) {
    if (!node || !updates || typeof updates !== 'object' || typeof node.context !== 'function') {
        return;
    }

    const context = node.context();
    if (!context) {
        return;
    }

    const flowUpdates = updates.flow && typeof updates.flow === 'object' ? updates.flow : {};
    const globalUpdates = updates.global && typeof updates.global === 'object' ? updates.global : {};

    const rehydrateSharedValue = (value) => {
        if (Buffer.isBuffer(value)) {
            return value;
        }

        if (ArrayBuffer.isView(value) && !(value instanceof DataView)) {
            return Buffer.from(value.buffer, value.byteOffset, value.byteLength);
        }

        if (Array.isArray(value)) {
            return value.map((item) => rehydrateSharedValue(item));
        }

        if (value && typeof value === 'object') {
            if (value.type === 'Buffer' && Array.isArray(value.data)) {
                return Buffer.from(value.data);
            }

            if (Object.prototype.hasOwnProperty.call(value, SHARED_SENTINEL_KEY)) {
                const filePath = value[SHARED_SENTINEL_KEY];
                if (!filePath || typeof filePath !== 'string') {
                    return Buffer.alloc(0);
                }
                try {
                    const data = fs.readFileSync(filePath);
                    try {
                        fs.unlinkSync(filePath);
                    } catch (err) {
                        if (err && err.code !== 'ENOENT') {
                            console.error(`Failed to unlink shared memory file ${filePath}:`, err);
                        }
                    }
                    return data;
                } catch (err) {
                    console.error(`Failed to read shared memory file ${filePath}:`, err);
                    return Buffer.alloc(0);
                }
            }

            if (Object.prototype.hasOwnProperty.call(value, SHARED_BASE64_KEY)) {
                try {
                    return Buffer.from(value[SHARED_BASE64_KEY], 'base64');
                } catch (err) {
                    console.error('Failed to decode base64 buffer from context update:', err);
                    return Buffer.alloc(0);
                }
            }

            const obj = Array.isArray(value) ? [] : {};
            Object.keys(value).forEach((key) => {
                obj[key] = rehydrateSharedValue(value[key]);
            });
            return obj;
        }

        return value;
    };

    const hydrateUpdates = (updatesObj) => {
        const hydrated = {};
        Object.keys(updatesObj || {}).forEach((key) => {
            hydrated[key] = rehydrateSharedValue(updatesObj[key]);
        });
        return hydrated;
    };

    const hydratedFlowUpdates = hydrateUpdates(flowUpdates);
    const hydratedGlobalUpdates = hydrateUpdates(globalUpdates);

    if (context.flow && typeof context.flow.set === 'function') {
        Object.keys(hydratedFlowUpdates).forEach((key) => {
            try {
                context.flow.set(key, hydratedFlowUpdates[key]);
            } catch (err) {
                if (typeof node.warn === 'function') {
                    node.warn(`Failed to set flow context "${key}": ${err.message || err}`, msg);
                }
            }
        });
    }

    if (context.global && typeof context.global.set === 'function') {
        Object.keys(hydratedGlobalUpdates).forEach((key) => {
            try {
                context.global.set(key, hydratedGlobalUpdates[key]);
            } catch (err) {
                if (typeof node.warn === 'function') {
                    node.warn(`Failed to set global context "${key}": ${err.message || err}`, msg);
                }
            }
        });
    }
}

function applyPythonLogs(node, logs, msg) {
    if (!node || !Array.isArray(logs)) {
        return;
    }

    logs.forEach((entry) => {
        if (!entry) {
            return;
        }
        const message = typeof entry === 'object' && entry.message !== undefined
            ? String(entry.message)
            : String(entry);
        if (typeof node.warn === 'function') {
            node.warn(message, msg);
        }
    });
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
            executeHotMode(node, entry.originalMsg, entry.pythonPayload, entry.send, entry.done, entry.timing);
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

function queueHotMessage(node, msg, pythonPayload, send, done, timing) {
    if (!node.hotPending) {
        node.hotPending = [];
    }

    node.hotPending.push({ originalMsg: msg, pythonPayload, send, done, timing });
    attachPoolReadyWatcher(node);
    if (node.hotMode) {
        node.status({ fill: "grey", shape: "ring", text: "hot: queueing" });
    }
}

function normalizePerformanceValue(value) {
    const numeric = Number(value);
    return Number.isFinite(numeric) ? numeric : 0;
}

function hrtimeDiffToMs(start) {
    if (typeof start !== 'bigint') {
        return 0;
    }
    const diff = process.hrtime.bigint() - start;
    return Number(diff) / 1e6;
}

function applyPerformanceMetrics(node, originalMsg, targetMsg, performance) {
    if (!performance || typeof performance !== 'object') {
        return;
    }

    const label = (typeof node.name === 'string' && node.name.trim()) ? node.name.trim() : 'python executor';
    if (!label) {
        return;
    }

    const copyPerformance = (source, destination) => {
        if (source && typeof source === 'object' && !Array.isArray(source)) {
            Object.keys(source).forEach((key) => {
                destination[key] = source[key];
            });
        }
    };

    const collected = {};
    if (originalMsg && originalMsg !== targetMsg) {
        copyPerformance(originalMsg.performance, collected);
    }
    copyPerformance(targetMsg.performance, collected);

    collected[label] = {
        transferToPythonMs: normalizePerformanceValue(performance.transfer_to_python_ms ?? performance.transferToPythonMs),
        executionMs: normalizePerformanceValue(performance.execution_ms ?? performance.executionMs),
        transferToJsMs: normalizePerformanceValue(performance.transfer_to_js_ms ?? performance.transferToJsMs),
        totalMs: normalizePerformanceValue(performance.totalMs ?? performance.total_ms ?? performance.total)
    };

    targetMsg.performance = collected;
}

module.exports = function(RED) {
    function PythonExecutorNode(config) {
        RED.nodes.createNode(this, config);
        const node = this;

        // Configuration
        node.func = config.func || "";
        node.outputs = 1;
        node.timeout = config.timeout || 5000;

        // Get pythonPath from environment config node or use direct path
        node.pythonEnvironmentId = null;
        if (config.pythonEnvironment) {
            const envNode = RED.nodes.getNode(config.pythonEnvironment);
            if (envNode && envNode.pythonPath) {
                node.pythonPath = envNode.pythonPath;
                node.pythonEnvironmentId = config.pythonEnvironment;
            } else {
                node.warn("Python environment not found, using fallback");
                node.pythonPath = config.pythonPath || "python3";
            }
        } else {
            node.pythonPath = config.pythonPath || "python3";
        }
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
        node.poolRefAcquired = false;

        // Initialize worker pool if hot mode is enabled
        let poolCreated = false;

        if (node.hotMode) {
            node.workerPoolKey = createPoolKey(node.pythonPath, node.workerPoolSize, node.preloadImports, node.pythonEnvironmentId);

            const poolResult = acquireWorkerPool(node.workerPoolKey, node.pythonPath, node.workerPoolSize, node.preloadImports);
            node.workerPool = poolResult.pool;
            node.poolRefAcquired = true;
            poolCreated = poolResult.created;

            if (poolCreated) {
                node.workerPool.initialize()
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
                        node.workerPool = null;
                        node.useHot = false;
                        node.hotError = error instanceof Error ? error : new Error(String(error));
                        flushHotQueue(node, node.hotError);
                        node.poolRefAcquired = false;
                        releaseWorkerPool(node.workerPoolKey);
                    });
            } else if (node.workerPool) {
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

            const timing = { start: process.hrtime.bigint() };
            const pythonMsg = buildPythonInputMsg(msg, node.func);
            const contextSnapshot = buildPythonContextSnapshot(node, node.func);
            const pythonPayload = buildPythonPayload(pythonMsg, contextSnapshot);

            if (node.hotMode) {
                const existingPool = node.workerPoolKey ? getWorkerPool(node.workerPoolKey) : null;
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
                    queueHotMessage(node, msg, pythonPayload, send, done, timing);
                    return;
                }

                executeHotMode(node, msg, pythonPayload, send, done, timing);
            } else {
                executeColdMode(node, msg, pythonPayload, send, done, timing);
            }
        });

        // Clean up on node close
        node.on('close', function(removed, done) {
            detachPoolReadyWatcher(node);
            node.hotPending = [];
            node.useHot = false;
            node.status({});

            const finish = (typeof done === 'function') ? done : (typeof removed === 'function' ? removed : () => {});

            const releasePromise = (node.poolRefAcquired && node.workerPoolKey)
                ? releaseWorkerPool(node.workerPoolKey)
                : Promise.resolve();

            node.poolRefAcquired = false;

            if (releasePromise && typeof releasePromise.then === 'function') {
                releasePromise
                    .then(() => finish())
                    .catch(() => finish());
            } else {
                finish();
            }
        });
    }

    /**
     * Execute Python code in HOT mode (persistent worker)
     */
    function executeHotMode(node, originalMsg, pythonPayload, send, done, timing) {
        const startTime = Date.now();
        let timedOut = false;
        let cancelHandle = null;
        const updateRunningStatus = () => {
            setHotStatus(node, "blue", "dot", "hot: running");
            logHotStats(node, 'Dispatching message to hot worker');
        };

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
        const executionCallback = (error, payload) => {
            if (timedOut) {
                return;
            }

            clearTimeout(timeoutId);

            const execTime = Date.now() - startTime;

            if (error) {
                if (error.contextUpdates) {
                    applyContextUpdates(node, error.contextUpdates, originalMsg);
                }
                if (error.logs) {
                    applyPythonLogs(node, error.logs, originalMsg);
                }
                setHotStatus(node, "red", "ring", `hot: error (${execTime}ms)`);
                logHotStats(node, `Hot execution failed after ${execTime}ms`);
                const errorMessage = error && (error.message || error.toString());
                done(new Error(`${error.type || 'Error'}: ${errorMessage}`));
                return;
            }

            let resultData;
            let performanceData = null;
            let contextUpdates = null;
            let logs = null;

            if (payload && typeof payload === 'object' && Object.prototype.hasOwnProperty.call(payload, 'result')) {
                resultData = payload.result;
                performanceData = payload.performance || null;
                contextUpdates = payload.contextUpdates || null;
                logs = payload.logs || null;
            } else {
                resultData = payload;
            }

            if (resultData === undefined || resultData === null) {
                resultData = {};
            }

            const totalMs = hrtimeDiffToMs(timing && timing.start);

            // Merge result into original message
            const outputMsg = Object.assign({}, originalMsg, resultData || {});

            const mergedPerformance = Object.assign({}, performanceData || {});
            mergedPerformance.totalMs = totalMs;
            applyPerformanceMetrics(node, originalMsg, outputMsg, mergedPerformance);
            applyContextUpdates(node, contextUpdates, originalMsg);
            applyPythonLogs(node, logs, originalMsg);

            // Send output
            send(outputMsg);
            setHotStatus(node, "green", "dot", `hot: ${execTime}ms`);
            logHotStats(node, `Hot execution completed in ${execTime}ms`);

            // Clear status after 3 seconds
            setTimeout(() => {
                setHotStatus(node, "green", "dot", "hot: ready");
            }, 3000);

            done();
        };

        try {
            cancelHandle = node.workerPool.execute(pythonPayload, node.func, executionCallback, { nodeId: node.workerPoolKey });
        } catch (dispatchError) {
            clearTimeout(timeoutId);
            const errObj = dispatchError instanceof Error ? dispatchError : new Error(String(dispatchError));
            setHotStatus(node, "red", "ring", "hot: dispatch failed");
            done(errObj);
            return;
        }

        updateRunningStatus();

        if (!cancelHandle || typeof cancelHandle.cancel !== 'function') {
            cancelHandle = null;
        }
    }

    /**
     * Execute Python code in COLD mode (spawn new process)
     */
    function executeColdMode(node, msg, pythonPayload, send, done, timing) {
        // Show running status
        node.status({ fill: "blue", shape: "dot", text: "cold: running" });

        const startTime = Date.now();

        // Prepare Python script
        const pythonScript = `
import sys
import json
import time
import os
import base64
import uuid

MSG_WRAPPER_KEY = "${MSG_WRAPPER_KEY}"
CONTEXT_WRAPPER_KEY = "${CONTEXT_WRAPPER_KEY}"
SHARED_SENTINEL_KEY = "${SHARED_SENTINEL_KEY}"
SHARED_BASE64_KEY = "${SHARED_BASE64_KEY}"
SHM_DIR = "/dev/shm" if os.path.isdir("/dev/shm") else os.path.abspath(os.getenv("TMPDIR", "/tmp"))

class _ContextProxy:
    def __init__(self, data=None, updates=None):
        self._data = data or {}
        self._updates = updates if updates is not None else {}

    def get(self, key, default=None):
        return self._data.get(key, default)

    def set(self, key, value):
        self._data[key] = value
        self._updates[key] = value

    def __getitem__(self, key):
        return self.get(key)

    def __setitem__(self, key, value):
        self.set(key, value)

class _NodeProxy:
    def __init__(self, logs=None):
        self._logs = logs if logs is not None else []

    def warn(self, message):
        self._logs.append({"level": "warn", "message": str(message)})

def _ensure_shared_dir():
    try:
        os.makedirs(SHM_DIR, exist_ok=True)
    except OSError:
        pass

def _write_shared_file(data):
    _ensure_shared_dir()
    file_path = os.path.join(SHM_DIR, f"rosepetal-python-{os.getpid()}-{uuid.uuid4().hex}")
    with open(file_path, "wb") as fh:
        fh.write(data)
    return file_path

def _encode_shared_outputs(value):
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

transfer_start = time.perf_counter()
input_data = sys.stdin.read()

flow_updates = {}
global_updates = {}
logs = []

try:
    input_obj = json.loads(input_data)
except Exception as e:
    error_msg = {
        "error": str(e),
        "type": type(e).__name__
    }
    print(json.dumps(error_msg), file=sys.stderr)
    sys.exit(1)

context_payload = {}
if isinstance(input_obj, dict) and MSG_WRAPPER_KEY in input_obj:
    msg = input_obj.get(MSG_WRAPPER_KEY, {})
    context_payload = input_obj.get(CONTEXT_WRAPPER_KEY) or {}
else:
    msg = input_obj

if not isinstance(context_payload, dict):
    context_payload = {}

flow_ctx = _ContextProxy(context_payload.get("flow") or {}, flow_updates)
global_ctx = _ContextProxy(context_payload.get("global") or {}, global_updates)
node = _NodeProxy(logs)

transfer_to_python_ms = (time.perf_counter() - transfer_start) * 1000.0
execution_ms = 0.0

def user_function(msg):
${node.func.split('\n').map(line => '    ' + line).join('\n')}

try:
    exec_start = time.perf_counter()
    result = user_function(msg)
    exec_end = time.perf_counter()
    execution_ms = (exec_end - exec_start) * 1000.0

    result_obj = result if result is not None else {}
    transfer_back_start = time.perf_counter()
    payload = {
        "__rosepetal_result": _encode_shared_outputs(result_obj),
        "__rosepetal_performance": {
            "transfer_to_python_ms": transfer_to_python_ms,
            "execution_ms": execution_ms,
            "transfer_to_js_ms": 0.0
        },
        "__rosepetal_context_updates": _encode_shared_outputs({
            "flow": flow_updates,
            "global": global_updates
        }),
        "__rosepetal_logs": logs
    }
    _ = json.dumps(payload)
    after_dump = time.perf_counter()
    payload["__rosepetal_performance"]["transfer_to_js_ms"] = (after_dump - transfer_back_start) * 1000.0
    print(json.dumps(payload))

except Exception as e:
    error_msg = {
        "error": str(e),
        "type": type(e).__name__,
        "context_updates": _encode_shared_outputs({
            "flow": flow_updates,
            "global": global_updates
        }),
        "logs": logs
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
                    let errorObj = null;

                    try {
                        errorObj = JSON.parse(stderrData);
                        if (errorObj && errorObj.type) {
                            errorMessage = `${errorObj.type}: ${errorObj.error}`;
                        }
                    } catch (e) {
                        errorMessage = stderrData || errorMessage;
                    }

                    if (errorObj) {
                        const contextUpdates = errorObj.context_updates || errorObj.__rosepetal_context_updates || null;
                        const logs = errorObj.logs || errorObj.__rosepetal_logs || null;
                        applyContextUpdates(node, contextUpdates, msg);
                        applyPythonLogs(node, logs, msg);
                    }

                    node.status({ fill: "red", shape: "ring", text: "error" });
                    done(new Error(errorMessage));
                    return;
                }

                // Parse output
                try {
                    const rawOutput = JSON.parse(stdoutData.trim());
                    const execTime = Date.now() - startTime;
                    const totalMs = hrtimeDiffToMs(timing && timing.start);

                    let resultPayload;
                    let performanceData = null;
                    let contextUpdates = null;
                    let logs = null;

                    if (rawOutput && typeof rawOutput === 'object' && Object.prototype.hasOwnProperty.call(rawOutput, '__rosepetal_result')) {
                        resultPayload = rawOutput.__rosepetal_result || {};
                        performanceData = rawOutput.__rosepetal_performance || null;
                        contextUpdates = rawOutput.__rosepetal_context_updates || null;
                        logs = rawOutput.__rosepetal_logs || null;
                    } else {
                        resultPayload = rawOutput || {};
                    }

                    // Merge result into original message
                    const outputMsg = Object.assign({}, msg, resultPayload || {});
                    const mergedPerformance = Object.assign({}, performanceData || {});
                    mergedPerformance.totalMs = totalMs;
                    applyPerformanceMetrics(node, msg, outputMsg, mergedPerformance);
                    applyContextUpdates(node, contextUpdates, msg);
                    applyPythonLogs(node, logs, msg);

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
                const inputJson = JSON.stringify(pythonPayload);
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
            workerPools.forEach((entry, key) => {
                entry.pool.stop();
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

        const existingEntry = getWorkerPoolEntry(poolKey);
        let pool = existingEntry ? existingEntry.pool : null;

        if (!pool) {
            pool = new PythonWorkerPool(pythonPath, workerPoolSize, preloadImports, poolKey);
            workerPools.set(poolKey, { pool, refCount: 0 });

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
