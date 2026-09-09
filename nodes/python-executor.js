const crypto = require('crypto');
const fs = require('fs');
const path = require('path');
const { spawn } = require('child_process');
const { PythonWorkerPool, computeCodeId, readSharedFile } = require('./python-worker');

const MSG_WRAPPER_KEY = "__rosepetal_msg";
const CONTEXT_WRAPPER_KEY = "__rosepetal_context";
const SHARED_SENTINEL_KEY = "__rosepetal_shm_path__";
const SHARED_BASE64_KEY = "__rosepetal_base64__";

// Status updates go to the editor over a websocket and to any Status nodes in
// the flow; at thousands of messages per second that traffic costs more than
// the Python call. Updates inside this window are coalesced to the latest one.
const STATUS_THROTTLE_MS = 50;
const STATUS_RESET_MS = 3000;
const EMPTY_CONTEXT = Object.freeze({ flow: Object.freeze({}), global: Object.freeze({}) });

// Cold mode spawns a bare interpreter, so the image helpers have to travel with
// the generated script. Hot mode imports the same file (python-worker-script.py).
const RP_IMAGE_HELPERS_SOURCE = fs.readFileSync(
    path.join(__dirname, 'rp_image_helpers.py'),
    'utf8'
);

// Global worker pools (one per unique configuration)
// Map key -> { pool: PythonWorkerPool, refCount: number }
const workerPools = new Map();

function getWorkerPoolEntry(key) {
    return workerPools.get(key) || null;
}

function getWorkerPool(key) {
    const entry = workerPools.get(key);
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

function isTypedArray(value) {
    return ArrayBuffer.isView(value) && !(value instanceof DataView);
}

// ---------------------------------------------------------------------------
// Static code analysis (run once per node, not per message)
// ---------------------------------------------------------------------------
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

// ---------------------------------------------------------------------------
// Per-message input preparation
// ---------------------------------------------------------------------------
function buildPythonInputMsg(originalMsg, msgKeys) {
    if (!originalMsg || typeof originalMsg !== 'object') {
        return originalMsg;
    }

    // If we cannot confidently determine keys, fall back to full message
    if (!msgKeys || msgKeys.length === 0) {
        return originalMsg;
    }

    const subset = {};
    for (let i = 0; i < msgKeys.length; i++) {
        const key = msgKeys[i];
        if (Object.prototype.hasOwnProperty.call(originalMsg, key)) {
            subset[key] = originalMsg[key];
        }
    }

    // Preserve _msgid for traceability
    if (Object.prototype.hasOwnProperty.call(originalMsg, '_msgid') && !Object.prototype.hasOwnProperty.call(subset, '_msgid')) {
        subset._msgid = originalMsg._msgid;
    }

    return subset;
}

function coerceBufferLike(value) {
    if (!value || typeof value !== 'object') {
        return value;
    }
    if (Buffer.isBuffer(value)) {
        return value;
    }
    if (value.type === 'Buffer' && Array.isArray(value.data)) {
        return Buffer.from(value.data);
    }
    if (isTypedArray(value)) {
        return Buffer.from(value.buffer, value.byteOffset, value.byteLength);
    }
    return value;
}

function buildPythonContextSnapshot(node, contextKeys) {
    if (!contextKeys || (contextKeys.flow.length === 0 && contextKeys.global.length === 0)) {
        return EMPTY_CONTEXT;
    }
    if (!node || typeof node.context !== 'function') {
        return EMPTY_CONTEXT;
    }

    const context = node.context();
    if (!context) {
        return EMPTY_CONTEXT;
    }

    const flowContext = context.flow;
    const globalContext = context.global;
    const flowSnapshot = {};
    const globalSnapshot = {};
    const flowKeys = contextKeys.flow;
    const globalKeys = contextKeys.global;

    if (flowContext && typeof flowContext.get === 'function') {
        for (let i = 0; i < flowKeys.length; i++) {
            try {
                flowSnapshot[flowKeys[i]] = coerceBufferLike(flowContext.get(flowKeys[i]));
            } catch (err) {
                // Ignore context read errors to avoid blocking execution
            }
        }
    }

    if (globalContext && typeof globalContext.get === 'function') {
        for (let i = 0; i < globalKeys.length; i++) {
            try {
                globalSnapshot[globalKeys[i]] = coerceBufferLike(globalContext.get(globalKeys[i]));
            } catch (err) {
                // Ignore context read errors to avoid blocking execution
            }
        }
    }

    return { flow: flowSnapshot, global: globalSnapshot };
}

// ---------------------------------------------------------------------------
// Result handling
// ---------------------------------------------------------------------------

/**
 * Convert transport artefacts inside a value tree back into Buffers, in place:
 * Buffer-JSON objects, typed arrays, base64 fallbacks and shared-memory files.
 * Shared files are read asynchronously; a promise that patches the parent is
 * pushed to `pending`. Returns the replacement for `value` (a Promise when the
 * value itself is a shared-file descriptor).
 */
function hydrateValue(value, pending) {
    if (value === null || typeof value !== 'object' || Buffer.isBuffer(value)) {
        return value;
    }
    if (isTypedArray(value)) {
        return Buffer.from(value.buffer, value.byteOffset, value.byteLength);
    }
    if (Array.isArray(value)) {
        for (let i = 0; i < value.length; i++) {
            hydrateChild(value, i, pending);
        }
        return value;
    }
    if (value.type === 'Buffer' && Array.isArray(value.data)) {
        return Buffer.from(value.data);
    }
    if (Object.prototype.hasOwnProperty.call(value, SHARED_SENTINEL_KEY)) {
        return readSharedFile(value[SHARED_SENTINEL_KEY]);
    }
    if (Object.prototype.hasOwnProperty.call(value, SHARED_BASE64_KEY)) {
        try {
            return Buffer.from(value[SHARED_BASE64_KEY], 'base64');
        } catch (err) {
            console.error('Failed to decode base64 buffer from context update:', err);
            return Buffer.alloc(0);
        }
    }
    const keys = Object.keys(value);
    for (let i = 0; i < keys.length; i++) {
        hydrateChild(value, keys[i], pending);
    }
    return value;
}

function hydrateChild(parent, key, pending) {
    const child = parent[key];
    if (child === null || typeof child !== 'object') {
        return;
    }
    const replaced = hydrateValue(child, pending);
    if (replaced instanceof Promise) {
        parent[key] = Buffer.alloc(0);
        pending.push(replaced.then((buffer) => {
            parent[key] = buffer;
        }));
    } else if (replaced !== child) {
        parent[key] = replaced;
    }
}

/**
 * Apply flow/global context updates returned by Python.
 * Returns undefined when applied synchronously, or a Promise when values had
 * to be read from shared memory first.
 */
function applyContextUpdates(node, updates, msg) {
    if (!node || !updates || typeof updates !== 'object' || typeof node.context !== 'function') {
        return undefined;
    }

    const flowUpdates = updates.flow && typeof updates.flow === 'object' ? updates.flow : {};
    const globalUpdates = updates.global && typeof updates.global === 'object' ? updates.global : {};
    const flowKeys = Object.keys(flowUpdates);
    const globalKeys = Object.keys(globalUpdates);

    if (flowKeys.length === 0 && globalKeys.length === 0) {
        return undefined;
    }

    const context = node.context();
    if (!context) {
        return undefined;
    }

    const pending = [];
    for (let i = 0; i < flowKeys.length; i++) {
        hydrateChild(flowUpdates, flowKeys[i], pending);
    }
    for (let i = 0; i < globalKeys.length; i++) {
        hydrateChild(globalUpdates, globalKeys[i], pending);
    }

    const apply = () => {
        const hydratedFlowUpdates = flowUpdates;
        const hydratedGlobalUpdates = globalUpdates;

        if (context.flow && typeof context.flow.set === 'function') {
            for (let i = 0; i < flowKeys.length; i++) {
                const key = flowKeys[i];
                try {
                    context.flow.set(key, hydratedFlowUpdates[key]);
                } catch (err) {
                    if (typeof node.warn === 'function') {
                        node.warn(`Failed to set flow context "${key}": ${err.message || err}`, msg);
                    }
                }
            }
        }

        if (context.global && typeof context.global.set === 'function') {
            for (let i = 0; i < globalKeys.length; i++) {
                const key = globalKeys[i];
                try {
                    context.global.set(key, hydratedGlobalUpdates[key]);
                } catch (err) {
                    if (typeof node.warn === 'function') {
                        node.warn(`Failed to set global context "${key}": ${err.message || err}`, msg);
                    }
                }
            }
        }
    };

    if (pending.length === 0) {
        apply();
        return undefined;
    }

    return Promise.all(pending).then(apply);
}

/**
 * Run `fn` after `maybePromise` settles; synchronously when there is nothing to wait for.
 */
function afterContextUpdates(node, maybePromise, msg, fn) {
    if (!maybePromise) {
        fn();
        return;
    }
    maybePromise
        .catch((e) => {
            node.error(`Failed to apply context updates: ${e.message || e}`, msg);
        })
        .then(fn);
}

function tryApplyContextUpdates(node, updates, msg) {
    try {
        return applyContextUpdates(node, updates, msg);
    } catch (e) {
        node.error(`Failed to apply context updates: ${e.message || e}`, msg);
        return undefined;
    }
}

function applyPythonLogs(node, logs, msg) {
    if (!node || !Array.isArray(logs) || logs.length === 0) {
        return;
    }

    for (let i = 0; i < logs.length; i++) {
        const entry = logs[i];
        if (!entry) {
            continue;
        }
        const message = typeof entry === 'object' && entry.message !== undefined
            ? String(entry.message)
            : String(entry);
        if (typeof node.warn === 'function') {
            node.warn(message, msg);
        }
    }
}

// ---------------------------------------------------------------------------
// Status reporting
// ---------------------------------------------------------------------------
function createStatusReporter(node) {
    let lastEmit = 0;
    let pending = null;
    let timer = null;

    const flush = () => {
        timer = null;
        if (pending) {
            const build = pending;
            pending = null;
            lastEmit = Date.now();
            node.status(build());
        }
    };

    return {
        report(build) {
            const now = Date.now();
            if (timer === null && now - lastEmit >= STATUS_THROTTLE_MS) {
                lastEmit = now;
                node.status(build());
                return;
            }
            pending = build;
            if (timer === null) {
                timer = setTimeout(flush, Math.max(1, STATUS_THROTTLE_MS - (now - lastEmit)));
            }
        },
        cancel() {
            if (timer !== null) {
                clearTimeout(timer);
                timer = null;
            }
            pending = null;
        }
    };
}

function buildHotStatusSuffix(pool) {
    const stats = getHotStats(pool);
    if (!stats || typeof stats.total === 'undefined') {
        return '';
    }
    let suffix = ` (workers ${stats.total}`;
    if (typeof stats.busy === 'number') {
        suffix += `, busy ${stats.busy}`;
    }
    if (typeof stats.queue === 'number') {
        suffix += `, queue ${stats.queue}`;
    }
    return suffix + ')';
}

function reportStatus(node, fill, shape, text) {
    if (!node || !node._statusReporter) {
        return;
    }
    node._statusReporter.report(() => ({ fill, shape, text }));
}

function setHotStatus(node, fill, shape, text) {
    if (!node || !node._statusReporter) {
        return;
    }
    node._statusReporter.report(() => {
        const suffix = node.hotMode ? buildHotStatusSuffix(node.workerPool) : '';
        return { fill, shape, text: suffix ? `${text}${suffix}` : text };
    });
}

function scheduleStatusReset(node, reset) {
    if (node._statusResetTimer) {
        clearTimeout(node._statusResetTimer);
    }
    node._statusResetTimer = setTimeout(() => {
        node._statusResetTimer = null;
        reset();
    }, STATUS_RESET_MS);
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

// ---------------------------------------------------------------------------
// Hot pool readiness handling
// ---------------------------------------------------------------------------
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
            executeHotMode(node, entry.originalMsg, entry.pythonMsg, entry.contextSnapshot, entry.send, entry.done, entry.timing);
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
            reportStatus(node, "red", "ring", "hot: failed");
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
            reportStatus(node, "grey", "ring", "hot: reloading");
            attachPoolReadyWatcher(node);
        };

        if (typeof node.workerPool.on === 'function') {
            node.workerPool.on('reload-start', node.poolReloadHandler);
        }
    }

    reportStatus(node, "grey", "ring", "hot: starting");
}

function queueHotMessage(node, msg, pythonMsg, contextSnapshot, send, done, timing) {
    if (!node.hotPending) {
        node.hotPending = [];
    }

    node.hotPending.push({ originalMsg: msg, pythonMsg, contextSnapshot, send, done, timing });
    attachPoolReadyWatcher(node);
    if (node.hotMode) {
        reportStatus(node, "grey", "ring", "hot: queueing");
    }
}

// ---------------------------------------------------------------------------
// Performance metrics
// ---------------------------------------------------------------------------
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

// ---------------------------------------------------------------------------
// Cold mode script (built once per node)
// ---------------------------------------------------------------------------
function buildColdScript(func) {
    return `
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

${RP_IMAGE_HELPERS_SOURCE}

def user_function(msg):
${func.trim() ? func.split('\n').map(line => '    ' + line).join('\n') : '    pass'}

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

        // The code is fixed for the node's lifetime: analyse it once here
        // instead of running the regexes on every message.
        node._msgKeys = Array.from(extractMsgKeysFromCode(node.func));
        const contextKeys = extractContextKeysFromCode(node.func);
        node._contextKeys = { flow: Array.from(contextKeys.flow), global: Array.from(contextKeys.global) };
        node._coldScript = null;
        node._codeId = null;
        node._statusReporter = createStatusReporter(node);
        node._statusResetTimer = null;

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
            node._codeId = computeCodeId(node.workerPoolKey, node.func);

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
                        reportStatus(node, "yellow", "ring", "hot: disabled");
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
            const pythonMsg = buildPythonInputMsg(msg, node._msgKeys);
            const contextSnapshot = buildPythonContextSnapshot(node, node._contextKeys);

            if (node.hotMode) {
                // Fast path: pool attached, ready and current.
                if (node.useHot && node.workerPool && node.workerPool.ready === true
                    && (node.workerPoolKey === null || getWorkerPool(node.workerPoolKey) === node.workerPool)) {
                    node.hotError = null;
                    if (node.hotPending.length > 0) {
                        flushHotQueue(node);
                    }
                    executeHotMode(node, msg, pythonMsg, contextSnapshot, send, done, timing);
                    return;
                }

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
                    reportStatus(node, "red", "ring", "hot: failed");
                    done(errObj);
                    return;
                }
            }

            // Choose execution mode
            if (node.useHot && node.workerPool) {
                const poolReady = typeof node.workerPool.isReady === 'function' ? node.workerPool.isReady() : false;

                if (!poolReady) {
                    queueHotMessage(node, msg, pythonMsg, contextSnapshot, send, done, timing);
                    return;
                }

                executeHotMode(node, msg, pythonMsg, contextSnapshot, send, done, timing);
            } else {
                executeColdMode(node, msg, pythonMsg, contextSnapshot, send, done, timing);
            }
        });

        // Clean up on node close
        node.on('close', function(removed, done) {
            detachPoolReadyWatcher(node);
            node.hotPending = [];
            node.useHot = false;
            if (node._statusResetTimer) {
                clearTimeout(node._statusResetTimer);
                node._statusResetTimer = null;
            }
            node._statusReporter.cancel();
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
    function executeHotMode(node, originalMsg, pythonMsg, contextSnapshot, send, done, timing) {
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
        const executionCallback = (error, payload) => {
            if (timedOut) {
                return;
            }

            clearTimeout(timeoutId);

            const execTime = Date.now() - startTime;

            if (error) {
                const finishError = () => {
                    if (error.logs) {
                        applyPythonLogs(node, error.logs, originalMsg);
                    }
                    setHotStatus(node, "red", "ring", `hot: error (${execTime}ms)`);
                    logHotStats(node, `Hot execution failed after ${execTime}ms`);
                    const errorMessage = error && (error.message || error.toString());
                    done(new Error(`${error.type || 'Error'}: ${errorMessage}`));
                };
                if (error.contextUpdates) {
                    afterContextUpdates(node, tryApplyContextUpdates(node, error.contextUpdates, originalMsg), originalMsg, finishError);
                } else {
                    finishError();
                }
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

            const finish = () => {
                applyPythonLogs(node, logs, originalMsg);

                // Send output
                send(outputMsg);
                setHotStatus(node, "green", "dot", `hot: ${execTime}ms`);
                logHotStats(node, `Hot execution completed in ${execTime}ms`);

                // Clear status after 3 seconds of quiet
                scheduleStatusReset(node, () => setHotStatus(node, "green", "dot", "hot: ready"));

                done();
            };

            afterContextUpdates(node, tryApplyContextUpdates(node, contextUpdates, originalMsg), originalMsg, finish);
        };

        try {
            cancelHandle = node.workerPool.execute(pythonMsg, node.func, executionCallback, {
                nodeId: node.workerPoolKey,
                codeId: node._codeId,
                ctx: contextSnapshot
            });
        } catch (dispatchError) {
            clearTimeout(timeoutId);
            const errObj = dispatchError instanceof Error ? dispatchError : new Error(String(dispatchError));
            setHotStatus(node, "red", "ring", "hot: dispatch failed");
            done(errObj);
            return;
        }

        setHotStatus(node, "blue", "dot", "hot: running");
        logHotStats(node, 'Dispatching message to hot worker');

        if (!cancelHandle || typeof cancelHandle.cancel !== 'function') {
            cancelHandle = null;
        }
    }

    /**
     * Execute Python code in COLD mode (spawn new process)
     */
    function executeColdMode(node, msg, pythonMsg, contextSnapshot, send, done, timing) {
        // Show running status
        reportStatus(node, "blue", "dot", "cold: running");

        const startTime = Date.now();

        if (node._coldScript === null) {
            node._coldScript = buildColdScript(node.func);
        }

        // Spawn Python process
        const pythonProcess = spawn(node.pythonPath, ['-c', node._coldScript]);

        const stdoutChunks = [];
        const stderrChunks = [];
        let timedOut = false;

        // Set timeout
        const timeoutId = setTimeout(() => {
            timedOut = true;
            pythonProcess.kill();
            reportStatus(node, "red", "ring", "timeout");
            done(new Error(`Python execution timed out after ${node.timeout}ms`));
        }, node.timeout);

        // Collect stdout
        pythonProcess.stdout.on('data', (data) => {
            stdoutChunks.push(data);
        });

        // Collect stderr
        pythonProcess.stderr.on('data', (data) => {
            stderrChunks.push(data);
        });

        // Handle process completion
        pythonProcess.on('close', (code) => {
            clearTimeout(timeoutId);

            if (timedOut) {
                return; // Already handled by timeout
            }

            if (code !== 0) {
                // Python script failed
                const stderrData = Buffer.concat(stderrChunks).toString('utf8');
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

                const finishError = () => {
                    reportStatus(node, "red", "ring", "error");
                    done(new Error(errorMessage));
                };

                if (errorObj) {
                    const contextUpdates = errorObj.context_updates || errorObj.__rosepetal_context_updates || null;
                    const logs = errorObj.logs || errorObj.__rosepetal_logs || null;
                    afterContextUpdates(node, tryApplyContextUpdates(node, contextUpdates, msg), msg, () => {
                        applyPythonLogs(node, logs, msg);
                        finishError();
                    });
                    return;
                }

                finishError();
                return;
            }

            // Parse output
            let rawOutput;
            try {
                rawOutput = JSON.parse(Buffer.concat(stdoutChunks).toString('utf8').trim());
            } catch (e) {
                reportStatus(node, "red", "ring", "cold: parse error");
                done(new Error(`Failed to parse Python output: ${e.message}`));
                return;
            }

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

            // Bytes returned by Python arrive as shared-memory descriptors:
            // turn them into Buffers before they reach the flow.
            const pendingReads = [];
            const holder = { result: resultPayload };
            try {
                hydrateColdResult(holder, pendingReads);
            } catch (e) {
                reportStatus(node, "red", "ring", "cold: parse error");
                done(new Error(`Failed to parse Python output: ${e.message}`));
                return;
            }

            const deliver = () => {
                // Merge result into original message
                const outputMsg = Object.assign({}, msg, holder.result || {});
                const mergedPerformance = Object.assign({}, performanceData || {});
                mergedPerformance.totalMs = totalMs;
                applyPerformanceMetrics(node, msg, outputMsg, mergedPerformance);

                afterContextUpdates(node, tryApplyContextUpdates(node, contextUpdates, msg), msg, () => {
                    applyPythonLogs(node, logs, msg);

                    // Send output
                    send(outputMsg);
                    reportStatus(node, "green", "dot", `cold: ${execTime}ms`);

                    // Clear status after 3 seconds of quiet
                    scheduleStatusReset(node, () => node._statusReporter.report(() => ({})));

                    done();
                });
            };

            if (pendingReads.length === 0) {
                deliver();
            } else {
                Promise.all(pendingReads).then(deliver, deliver);
            }
        });

        // Handle process errors
        pythonProcess.on('error', (err) => {
            clearTimeout(timeoutId);
            reportStatus(node, "red", "ring", "spawn error");

            if (err.code === 'ENOENT') {
                done(new Error(`Python interpreter not found: ${node.pythonPath}`));
            } else {
                done(new Error(`Failed to spawn Python process: ${err.message}`));
            }
        });

        // Send input message to Python stdin
        try {
            const inputJson = JSON.stringify({
                [MSG_WRAPPER_KEY]: pythonMsg,
                [CONTEXT_WRAPPER_KEY]: contextSnapshot || EMPTY_CONTEXT
            });
            pythonProcess.stdin.write(inputJson);
            pythonProcess.stdin.end();
        } catch (e) {
            clearTimeout(timeoutId);
            pythonProcess.kill();
            reportStatus(node, "red", "ring", "input error");
            done(new Error(`Failed to send input to Python: ${e.message}`));
        }
    }

    /**
     * Cold results only carry shared-memory / base64 descriptors for bytes;
     * plain Buffer-JSON objects are left untouched (they are ordinary JSON to
     * the flow, exactly as before).
     */
    function hydrateColdResult(holder, pending) {
        const walk = (parent, key) => {
            const value = parent[key];
            if (value === null || typeof value !== 'object') {
                return;
            }
            if (Array.isArray(value)) {
                for (let i = 0; i < value.length; i++) {
                    walk(value, i);
                }
                return;
            }
            if (Object.prototype.hasOwnProperty.call(value, SHARED_SENTINEL_KEY)) {
                parent[key] = Buffer.alloc(0);
                pending.push(readSharedFile(value[SHARED_SENTINEL_KEY]).then((buffer) => {
                    parent[key] = buffer;
                }));
                return;
            }
            if (Object.prototype.hasOwnProperty.call(value, SHARED_BASE64_KEY)) {
                try {
                    parent[key] = Buffer.from(value[SHARED_BASE64_KEY], 'base64');
                } catch (err) {
                    parent[key] = Buffer.alloc(0);
                }
                return;
            }
            const keys = Object.keys(value);
            for (let i = 0; i < keys.length; i++) {
                walk(value, keys[i]);
            }
        };
        walk(holder, 'result');
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
