const { spawn } = require('child_process');
const fs = require('fs');
const fsp = fs.promises;
const os = require('os');
const path = require('path');
const crypto = require('crypto');
const EventEmitter = require('events');

// Default based on platform
let SHM_DIR = process.platform === 'linux' ? '/dev/shm' : os.tmpdir();
if (process.platform === 'linux') {
    fsp.access('/dev/shm').catch(() => {
        SHM_DIR = os.tmpdir();
    });
}
const SHARED_SENTINEL_KEY = '__rosepetal_shm_path__';
const SHARED_BASE64_KEY = '__rosepetal_base64__';
const INLINE_KEY = '__rosepetal_bin__';

// Buffers ride inline in the pipe frame by default: measured end to end, the
// pipe beats a /dev/shm round trip at every size (tmpfs page allocation makes
// the file write the slow part), and it leaves nothing to clean up. Setting
// ROSEPETAL_PY_INLINE_MAX_BYTES routes buffers at or above that size through a
// shared-memory file instead (written off the event loop by the thread pool).
const DEFAULT_INLINE_MAX_BYTES = Number.MAX_SAFE_INTEGER;
const INLINE_MAX_BYTES = (() => {
    const raw = parseInt(process.env.ROSEPETAL_PY_INLINE_MAX_BYTES, 10);
    return Number.isFinite(raw) && raw >= 0 ? raw : DEFAULT_INLINE_MAX_BYTES;
})();

// Hard cap on a single frame header so a corrupted stream cannot make us
// allocate gigabytes. Header = request/response JSON without binary data.
const MAX_HEADER_BYTES = 1024 * 1024 * 1024;
const MAX_OBJECT_DEPTH = 256;
const EMPTY_BUFFER = Buffer.alloc(0);

const STAGE_PREFIX = 0;
const STAGE_HEADER = 1;
const STAGE_BLOBS = 2;

function isTypedArray(value) {
    return ArrayBuffer.isView(value) && !(value instanceof DataView);
}

/**
 * Python Worker - Persistent Python process for fast execution
 */
class PythonWorker extends EventEmitter {
    constructor(pythonPath, workerId) {
        super();
        this.pythonPath = pythonPath || 'python3';
        this.workerId = workerId;
        this.process = null;
        this.ready = false;
        this.busy = false;
        this.activeRequestId = null;
        this.pendingCallbacks = new Map();
        this.requestCounter = 0;
        this.stopPromise = null;
        this.intentionalStop = false;
        this.activeAttachments = [];
        this.sharedIdSeed = crypto.randomBytes(4).toString('hex');
        this.knownCodeIds = new Set();
        this._activeRequest = null;
        this._resetParser();
    }

    _resetParser() {
        this._chunks = [];
        this._chunkOffset = 0;
        this._buffered = 0;
        this._stage = STAGE_PREFIX;
        this._need = 4;
        this._header = null;
        this._blobLengths = null;
        this._blobs = null;
        this._blobIndex = 0;
        this._fill = null;
        this._filled = 0;
    }

    /**
     * Start the persistent Python worker process
     */
    start() {
        return new Promise((resolve, reject) => {
            const scriptPath = path.join(__dirname, 'python-worker-script.py');

            try {
                this.ready = false;
                this.knownCodeIds.clear();
                this._resetParser();
                this.process = spawn(this.pythonPath, [scriptPath], { env: buildWorkerEnv() });

                // Handle stdout (responses from Python)
                this.process.stdout.on('data', (data) => {
                    this.handleStdout(data);
                });

                // Handle stderr (errors, and anything user code prints)
                this.process.stderr.on('data', (data) => {
                    const error = data.toString();
                    const errObj = new Error(`Worker stderr: ${error}`);
                    errObj.isWorkerStderr = true;
                    this.emit('error', errObj);
                });

                // Handle stdin errors (e.g. EPIPE when the child is killed on
                // timeout while a write is still in flight). Without this
                // listener the stream throws an uncaught exception and takes
                // down the whole Node-RED process; report it instead.
                this.process.stdin.on('error', (err) => {
                    this.emit('error', new Error(`Worker stdin error: ${err.message}`));
                });

                // Handle process exit
                this.process.on('close', (code) => {
                    const wasIntentional = this.intentionalStop;
                    this.intentionalStop = false;
                    this.ready = false;
                    this.busy = false;
                    this.activeRequestId = null;
                    this._activeRequest = null;
                    this.process = null;
                    this.knownCodeIds.clear();
                    this._resetParser();

                    if (wasIntentional) {
                        this._failPendingCallbacks(new Error('Worker stopped'), { silent: true });
                    } else {
                        this._failPendingCallbacks(new Error('Worker exited unexpectedly'));
                    }

                    this._cleanupActiveAttachments();
                    this.emit('exit', code);
                    this.emit('available');
                });

                // Handle spawn errors
                this.process.on('error', (err) => {
                    reject(err);
                    this._cleanupActiveAttachments();
                    this.emit('error', err);
                });

                // Wait for ready signal
                const readyTimeout = setTimeout(() => {
                    reject(new Error('Worker startup timeout'));
                }, 5000);

                this.once('ready', () => {
                    clearTimeout(readyTimeout);
                    resolve();
                });

            } catch (error) {
                reject(error);
            }
        });
    }

    /**
     * Handle stdout data from Python worker: binary frame parser.
     * Frame = u32le header length | header JSON | blobs (lengths listed in header.bin)
     */
    handleStdout(data) {
        if (!data || data.length === 0) {
            return;
        }
        this._chunks.push(data);
        this._buffered += data.length;

        for (;;) {
            if (this._stage === STAGE_PREFIX) {
                if (this._buffered < 4) {
                    return;
                }
                const prefix = this._take(4);
                const headerLength = prefix.readUInt32LE(0);
                if (headerLength > MAX_HEADER_BYTES) {
                    this._protocolError(new Error(`Invalid frame header length from worker: ${headerLength}`));
                    return;
                }
                this._need = headerLength;
                this._stage = STAGE_HEADER;
            }

            if (this._stage === STAGE_HEADER) {
                if (this._buffered < this._need) {
                    return;
                }
                const headerBuffer = this._take(this._need);
                let header;
                try {
                    header = JSON.parse(headerBuffer.toString('utf8'));
                } catch (error) {
                    this._protocolError(new Error(`Failed to parse response: ${error.message}`));
                    return;
                }

                const lengths = Array.isArray(header.bin) ? header.bin : null;
                if (!lengths || lengths.length === 0) {
                    this._stage = STAGE_PREFIX;
                    this.handleResponse(header, null);
                    continue;
                }

                this._header = header;
                this._blobLengths = lengths;
                this._blobs = new Array(lengths.length);
                this._blobIndex = 0;
                this._fill = null;
                this._stage = STAGE_BLOBS;
            }

            // STAGE_BLOBS
            const lengths = this._blobLengths;
            while (this._blobIndex < lengths.length) {
                const size = Math.max(0, Math.floor(Number(lengths[this._blobIndex])) || 0);
                if (this._fill === null) {
                    if (size <= 0) {
                        this._blobs[this._blobIndex++] = EMPTY_BUFFER;
                        continue;
                    }
                    if (this._buffered >= size) {
                        this._blobs[this._blobIndex++] = this._take(size);
                        continue;
                    }
                    // Large blob still streaming in: fill its final buffer
                    // directly instead of hoarding pipe chunks.
                    this._fill = Buffer.allocUnsafe(size);
                    this._filled = 0;
                }
                this._filled += this._drainInto(this._fill, this._filled, size - this._filled);
                if (this._filled < size) {
                    return;
                }
                this._blobs[this._blobIndex++] = this._fill;
                this._fill = null;
            }

            const header = this._header;
            const blobs = this._blobs;
            this._header = null;
            this._blobLengths = null;
            this._blobs = null;
            this._stage = STAGE_PREFIX;
            this.handleResponse(header, blobs);
        }
    }

    /**
     * Remove exactly n bytes from the chunk queue and return them.
     * Returns a view when the bytes live in a single chunk (no copy).
     */
    _take(n) {
        if (n === 0) {
            return EMPTY_BUFFER;
        }
        const first = this._chunks[0];
        const available = first.length - this._chunkOffset;
        if (available > n) {
            const out = first.subarray(this._chunkOffset, this._chunkOffset + n);
            this._chunkOffset += n;
            this._buffered -= n;
            return out;
        }
        if (available === n) {
            const out = this._chunkOffset === 0 ? first : first.subarray(this._chunkOffset);
            this._chunks.shift();
            this._chunkOffset = 0;
            this._buffered -= n;
            return out;
        }
        const out = Buffer.allocUnsafe(n);
        this._drainInto(out, 0, n);
        return out;
    }

    /**
     * Copy up to `max` buffered bytes into target at offset. Returns bytes copied.
     */
    _drainInto(target, offset, max) {
        let copied = 0;
        while (copied < max && this._chunks.length > 0) {
            const chunk = this._chunks[0];
            const available = chunk.length - this._chunkOffset;
            const take = Math.min(available, max - copied);
            chunk.copy(target, offset + copied, this._chunkOffset, this._chunkOffset + take);
            copied += take;
            if (take === available) {
                this._chunks.shift();
                this._chunkOffset = 0;
            } else {
                this._chunkOffset += take;
            }
        }
        this._buffered -= copied;
        return copied;
    }

    /**
     * A corrupted stream cannot be resynchronised; report it and recycle the process.
     */
    _protocolError(error) {
        this._resetParser();
        this.emit('error', error);
        if (this.process) {
            this.ready = false;
            this.process.kill();
        }
    }

    /**
     * Handle a complete frame from Python.
     */
    handleResponse(response, blobs) {
        if (response.status === 'ready') {
            this.ready = true;
            this.emit('ready');
            this.emit('available');
            return;
        }

        if (response.status === 'worker_error') {
            const internalError = new Error(response.error || 'Worker error');
            internalError.type = response.type;
            internalError.traceback = response.traceback;

            this.ready = false;
            this.busy = false;
            this.activeRequestId = null;
            this._activeRequest = null;

            this._failPendingCallbacks(internalError, { silent: true });
            this.emit('error', internalError);
            this.emit('available');
            return;
        }

        const requestId = response.id;
        const active = this._activeRequest;
        const callback = this.pendingCallbacks.get(requestId);

        if (response.code_known === true && active && active.requestId === requestId) {
            this.knownCodeIds.add(active.codeId);
        }

        if (response.status === 'error' && response.type === 'UnknownCode'
            && active && active.requestId === requestId && !active.resent) {
            // The worker lost its code cache (fresh process); resend with source.
            active.resent = true;
            this.knownCodeIds.delete(active.codeId);
            try {
                this._sendRequest(active, true);
                return;
            } catch (err) {
                this._finishActive(requestId, err, null);
                return;
            }
        }

        if (!callback) {
            this._finishActive(requestId, null, null);
            return;
        }

        if (response.status === 'success') {
            const holder = { result: undefined, contextUpdates: null };
            const pending = [];
            try {
                holder.result = rehydrateInline(response.result, blobs);
                holder.contextUpdates = response.context_updates
                    ? rehydrateInline(response.context_updates, blobs)
                    : null;
                collectSharedReads(holder, pending);
            } catch (err) {
                this._finishActive(requestId, err, null);
                return;
            }

            const performance = response.performance || null;
            const logs = response.logs || null;
            const buildPayload = () => ({
                result: holder.result,
                performance,
                contextUpdates: holder.contextUpdates,
                logs
            });

            if (pending.length === 0) {
                this._finishActive(requestId, null, buildPayload());
                return;
            }

            // Large blobs came through shared memory: read them off-loop, then finish.
            Promise.all(pending)
                .then(() => this._finishActive(requestId, null, buildPayload()))
                .catch((err) => this._finishActive(requestId, err, null));
            return;
        }

        const error = new Error(response.error || 'Unknown error');
        error.type = response.type;
        error.traceback = response.traceback;
        if (response.performance) {
            error.performance = response.performance;
        }
        if (response.logs) {
            error.logs = response.logs;
        }
        if (response.context_updates) {
            try {
                const pending = [];
                const holder = { ctx: rehydrateInline(response.context_updates, blobs) };
                collectSharedReads(holder, pending);
                if (pending.length > 0) {
                    Promise.all(pending)
                        .catch(() => {})
                        .then(() => {
                            error.contextUpdates = holder.ctx;
                            this._finishActive(requestId, error, null);
                        });
                    return;
                }
                error.contextUpdates = holder.ctx;
            } catch (_err) {
                // ignore context rehydration failures on the error path
            }
        }
        this._finishActive(requestId, error, null);
    }

    /**
     * Mark the worker free (without invoking the callback yet).
     */
    _releaseActive(requestId) {
        const attachments = this.activeAttachments;
        this.activeAttachments = [];
        this.pendingCallbacks.delete(requestId);
        this._activeRequest = null;
        this.busy = false;
        this.activeRequestId = null;
        this._cleanupAttachments(attachments);
    }

    /**
     * Mark the worker free and deliver the result.
     */
    _finishActive(requestId, error, payload) {
        const callback = this.pendingCallbacks.get(requestId);
        this._releaseActive(requestId);
        if (callback) {
            try {
                callback(error, payload);
            } catch (err) {
                this.emit('error', err);
            }
        }
        this.emit('available');
    }

    /**
     * Execute Python code.
     */
    execute(msg, code, options = {}, callback) {
        if (!this.ready) {
            setImmediate(() => callback(new Error('Worker not ready')));
            return null;
        }

        if (this.busy) {
            setImmediate(() => callback(new Error('Worker busy')));
            return null;
        }

        this.busy = true;
        const requestId = `req_${this.workerId}_${this.requestCounter++}`;
        this.activeRequestId = requestId;
        this.pendingCallbacks.set(requestId, callback);

        const codeId = options.codeId || hashCode(options.nodeId, code);
        const request = {
            requestId,
            codeId,
            msg,
            code,
            options,
            resent: false
        };
        this._activeRequest = request;

        try {
            this._sendRequest(request, !this.knownCodeIds.has(codeId));
        } catch (err) {
            this._abortActive(requestId, err);
        }

        return requestId;
    }

    _abortActive(requestId, err) {
        if (this.activeRequestId !== requestId) {
            return;
        }
        const cb = this.pendingCallbacks.get(requestId);
        this._releaseActive(requestId);
        if (cb) {
            setImmediate(() => cb(err));
        }
    }

    /**
     * Encode and write a request frame. Buffers below the inline threshold go
     * straight into the frame; larger ones are written to shared memory first
     * (asynchronously, off the event loop) and referenced by path.
     */
    _sendRequest(request, includeCode) {
        const encoder = new BinaryEncoder();
        const msgPart = request.options.preload ? request.msg : encodeBinaries(request.msg, encoder);
        const ctxPart = request.options.ctx !== undefined
            ? encodeBinaries(request.options.ctx, encoder)
            : undefined;

        const header = {
            id: request.requestId,
            code_id: request.codeId,
            node_id: request.options.nodeId,
            preload: request.options.preload || false,
            msg: msgPart
        };
        if (ctxPart !== undefined) {
            header.ctx = ctxPart;
        }
        if (includeCode || request.options.preload) {
            header.code = request.code;
        }
        if (encoder.lengths.length > 0) {
            header.bin = encoder.lengths;
        }

        if (encoder.large.length === 0) {
            this._writeFrame(header, encoder.blobs);
            return;
        }

        header.shm = true;
        const requestId = request.requestId;
        Promise.all(encoder.large.map(({ buffer, descriptor }) =>
            this._writeBufferToSharedFile(buffer)
                .then((filePath) => {
                    descriptor[SHARED_SENTINEL_KEY] = filePath;
                    return filePath;
                })
                .catch(() => {
                    // Shared memory unavailable: fall back to base64 in the header.
                    descriptor[SHARED_BASE64_KEY] = buffer.toString('base64');
                    return null;
                })
        )).then((paths) => {
            const attachments = paths.filter((p) => typeof p === 'string');
            if (this.activeRequestId !== requestId || !this.process) {
                this._cleanupAttachments(attachments);
                return;
            }
            this.activeAttachments = attachments;
            try {
                this._writeFrame(header, encoder.blobs);
            } catch (err) {
                this._abortActive(requestId, err);
            }
        });
    }

    _writeFrame(header, blobs) {
        const stdin = this.process && this.process.stdin;
        if (!stdin || !stdin.writable) {
            throw new Error('Worker stdin not available');
        }

        const json = JSON.stringify(header);
        const headerLength = Buffer.byteLength(json, 'utf8');
        const frame = Buffer.allocUnsafe(4 + headerLength);
        frame.writeUInt32LE(headerLength, 0);
        frame.write(json, 4, headerLength, 'utf8');

        const onWrite = (err) => {
            if (err) {
                this.emit('error', new Error(`Worker stdin write failed: ${err.message}`));
            }
        };

        if (blobs.length === 0) {
            stdin.write(frame, onWrite);
            return;
        }

        stdin.cork();
        stdin.write(frame);
        for (let i = 0; i < blobs.length - 1; i++) {
            stdin.write(blobs[i]);
        }
        stdin.write(blobs[blobs.length - 1], onWrite);
        stdin.uncork();
    }

    /**
     * Check if worker is available
     */
    isAvailable() {
        return this.ready && !this.busy;
    }

    /**
     * Execute preload code once the worker is ready
     */
    preload(code, nodeId = 'default') {
        const preloadCode = (code || '').trim();

        if (!preloadCode) {
            return Promise.resolve();
        }

        return new Promise((resolve, reject) => {
            const callback = (error) => {
                if (error) {
                    reject(error);
                } else {
                    resolve();
                }
            };

            const requestId = this.execute(
                { __preload: true },
                preloadCode,
                { preload: true, nodeId },
                callback
            );

            if (!requestId) {
                reject(new Error('Failed to schedule preload request'));
            }
        });
    }

    /**
     * Fail and clear all pending callbacks
     */
    _failPendingCallbacks(error, options = {}) {
        const silent = options.silent || false;

        this.pendingCallbacks.forEach((callback) => {
            if (!silent) {
                setImmediate(() => callback(error, null));
            }
        });

        this.pendingCallbacks.clear();
        this.busy = false;
        this.activeRequestId = null;
        this._activeRequest = null;
        this._cleanupActiveAttachments();
    }

    /**
     * Stop the worker
     */
    stop() {
        if (!this.process) {
            this.ready = false;
            this.busy = false;
            this.activeRequestId = null;
            this._activeRequest = null;
            this._cleanupActiveAttachments();
            return Promise.resolve();
        }

        if (this.stopPromise) {
            return this.stopPromise;
        }

        this.ready = false;
        this.busy = false;
        this.activeRequestId = null;
        this._activeRequest = null;

        this.stopPromise = new Promise((resolve) => {
            const currentProcess = this.process;

            const handleClose = () => {
                currentProcess.removeListener('close', handleClose);
                this.process = null;
                this.stopPromise = null;
                this._cleanupActiveAttachments();
                resolve();
            };

            currentProcess.once('close', handleClose);
            this.intentionalStop = true;
            // Close stdin before killing so nothing tries to flush into a
            // dying pipe (belt-and-braces alongside the stdin error handler).
            if (currentProcess.stdin && !currentProcess.stdin.destroyed) {
                currentProcess.stdin.end();
            }
            currentProcess.kill();
        });

        return this.stopPromise;
    }

    /**
     * Restart the worker process
     */
    async restart() {
        await this.stop();
        await this.start();
    }

    /**
     * Terminate the active request and restart the worker
     */
    async terminateActiveRequest(reason = 'Request terminated', { silent = false } = {}) {
        if (!this.activeRequestId) {
            return;
        }

        const callback = this.pendingCallbacks.get(this.activeRequestId);
        if (callback) {
            this.pendingCallbacks.delete(this.activeRequestId);
            if (!silent) {
                setImmediate(() => callback(new Error(reason), null));
            }
        }

        this.busy = false;
        this.activeRequestId = null;
        this._activeRequest = null;

        await this.restart();
    }

    /**
     * Write a Buffer to a unique shared memory file
     */
    async _writeBufferToSharedFile(buffer) {
        const fileName = `rosepetal-python-${process.pid}-${this.workerId}-${Date.now()}-${this.sharedIdSeed}-${crypto.randomBytes(6).toString('hex')}`;
        const filePath = path.join(SHM_DIR, fileName);
        await fsp.writeFile(filePath, buffer);
        return filePath;
    }

    /**
     * Remove temporary attachments created for the active request.
     * Fire-and-forget: unlinks asynchronously, ENOENT is treated as benign.
     */
    _cleanupAttachments(paths = []) {
        if (!paths || paths.length === 0) {
            return;
        }

        paths.forEach((filePath) => {
            if (!filePath || typeof filePath !== 'string') {
                return;
            }
            fsp.unlink(filePath).catch((err) => {
                if (err && err.code !== 'ENOENT') {
                    console.error(`Failed to unlink temporary shared memory file ${filePath}:`, err);
                }
            });
        });
    }

    _cleanupActiveAttachments() {
        if (this.activeAttachments && this.activeAttachments.length) {
            this._cleanupAttachments(this.activeAttachments);
        }
        this.activeAttachments = [];
    }
}

/**
 * Environment for the worker process.
 *
 * glibc serves allocations above its mmap threshold with a fresh mmap and
 * unmaps them on free, so every multi-megabyte image buffer a worker touches
 * is page-faulted in from scratch (measured: ~4 ms for a 2.7 MB copy vs ~1 ms
 * when the heap is reused). Pinning the thresholds keeps those buffers in the
 * heap between requests. Only applied when the user has not set them.
 */
function buildWorkerEnv() {
    const env = Object.assign({}, process.env, {
        ROSEPETAL_PY_INLINE_MAX_BYTES: String(INLINE_MAX_BYTES)
    });
    if (process.platform === 'linux' && env.ROSEPETAL_PY_MALLOC_TUNING !== '0') {
        if (env.MALLOC_MMAP_THRESHOLD_ === undefined) {
            env.MALLOC_MMAP_THRESHOLD_ = String(32 * 1024 * 1024);
        }
        if (env.MALLOC_TRIM_THRESHOLD_ === undefined) {
            env.MALLOC_TRIM_THRESHOLD_ = String(128 * 1024 * 1024);
        }
        if (env.MALLOC_TOP_PAD_ === undefined) {
            env.MALLOC_TOP_PAD_ = String(16 * 1024 * 1024);
        }
    }
    return env;
}

/**
 * Stable id for a (namespace, code) pair so the worker can cache compiled code
 * and Node can stop shipping the source with every request.
 */
const codeIdCache = new Map();
function hashCode(nodeId, code) {
    const key = `${nodeId || ''} ${code || ''}`;
    let id = codeIdCache.get(key);
    if (!id) {
        id = crypto.createHash('sha1').update(key).digest('hex');
        if (codeIdCache.size > 4096) {
            codeIdCache.clear();
        }
        codeIdCache.set(key, id);
    }
    return id;
}

/**
 * Collects binary leaves while a message is encoded for transport.
 */
class BinaryEncoder {
    constructor() {
        this.blobs = [];
        this.lengths = [];
        this.large = [];
    }

    describe(buffer) {
        const length = buffer.length;
        if (length >= INLINE_MAX_BYTES) {
            const descriptor = { length };
            this.large.push({ buffer, descriptor });
            return descriptor;
        }
        this.blobs.push(buffer);
        this.lengths.push(length);
        return { [INLINE_KEY]: this.blobs.length - 1, length };
    }
}

/**
 * Replace Buffers/typed arrays with transport descriptors.
 * Copy-on-write: containers that hold no binary data are returned as-is, so a
 * message without Buffers costs one read-only traversal and zero allocations.
 */
function encodeBinaries(value, encoder, depth = 0) {
    if (value === null || typeof value !== 'object') {
        return value;
    }
    if (Buffer.isBuffer(value)) {
        return encoder.describe(value);
    }
    if (isTypedArray(value)) {
        return encoder.describe(Buffer.from(value.buffer, value.byteOffset, value.byteLength));
    }
    if (depth > MAX_OBJECT_DEPTH) {
        throw new Error('Message nesting too deep (circular structure?)');
    }

    if (Array.isArray(value)) {
        let copy = null;
        for (let i = 0; i < value.length; i++) {
            const item = value[i];
            const encoded = encodeBinaries(item, encoder, depth + 1);
            if (encoded !== item) {
                if (copy === null) {
                    copy = value.slice();
                }
                copy[i] = encoded;
            }
        }
        return copy || value;
    }

    const keys = Object.keys(value);
    let copy = null;
    for (let i = 0; i < keys.length; i++) {
        const key = keys[i];
        const item = value[key];
        const encoded = encodeBinaries(item, encoder, depth + 1);
        if (encoded !== item) {
            if (copy === null) {
                copy = Object.assign({}, value);
            }
            copy[key] = encoded;
        }
    }
    return copy || value;
}

/**
 * Replace inline descriptors in a parsed response with the frame's blobs.
 * Only walks when the frame actually carried blobs.
 */
function rehydrateInline(value, blobs) {
    if (!blobs || blobs.length === 0) {
        return value;
    }
    return rehydrateInlineWalk(value, blobs);
}

function rehydrateInlineWalk(value, blobs) {
    if (value === null || typeof value !== 'object') {
        return value;
    }
    if (Array.isArray(value)) {
        for (let i = 0; i < value.length; i++) {
            const item = value[i];
            if (item !== null && typeof item === 'object') {
                value[i] = rehydrateInlineWalk(item, blobs);
            }
        }
        return value;
    }
    if (Object.prototype.hasOwnProperty.call(value, INLINE_KEY)) {
        const blob = blobs[value[INLINE_KEY]];
        return Buffer.isBuffer(blob) ? blob : Buffer.alloc(0);
    }
    const keys = Object.keys(value);
    for (let i = 0; i < keys.length; i++) {
        const item = value[keys[i]];
        if (item !== null && typeof item === 'object') {
            value[keys[i]] = rehydrateInlineWalk(item, blobs);
        }
    }
    return value;
}

/**
 * Find shared-memory / base64 descriptors in a parsed response. Base64 is
 * decoded in place; shared files are read asynchronously and the resulting
 * promise (which patches the parent in place) is pushed to `pending`.
 * Returns the (possibly replaced) value. Objects are mutated in place, so the
 * caller must not reuse the parsed response elsewhere.
 */
function collectSharedReads(value, pending) {
    if (value === null || typeof value !== 'object' || Buffer.isBuffer(value)) {
        return value;
    }
    if (Array.isArray(value)) {
        for (let i = 0; i < value.length; i++) {
            const item = value[i];
            if (item !== null && typeof item === 'object' && !Buffer.isBuffer(item)) {
                value[i] = collectSharedReads(item, pending);
                if (value[i] instanceof Promise) {
                    const promise = value[i];
                    const index = i;
                    value[i] = Buffer.alloc(0);
                    pending.push(promise.then((buf) => { value[index] = buf; }));
                }
            }
        }
        return value;
    }
    if (Object.prototype.hasOwnProperty.call(value, SHARED_SENTINEL_KEY)) {
        return readSharedFile(value[SHARED_SENTINEL_KEY]);
    }
    if (Object.prototype.hasOwnProperty.call(value, SHARED_BASE64_KEY)) {
        try {
            return Buffer.from(value[SHARED_BASE64_KEY], 'base64');
        } catch (err) {
            console.error('Failed to decode base64 buffer from Python result:', err);
            return Buffer.alloc(0);
        }
    }
    const keys = Object.keys(value);
    for (let i = 0; i < keys.length; i++) {
        const key = keys[i];
        const item = value[key];
        if (item !== null && typeof item === 'object' && !Buffer.isBuffer(item)) {
            const replaced = collectSharedReads(item, pending);
            if (replaced instanceof Promise) {
                value[key] = Buffer.alloc(0);
                pending.push(replaced.then((buf) => { value[key] = buf; }));
            } else {
                value[key] = replaced;
            }
        }
    }
    return value;
}

function readSharedFile(filePath) {
    if (!filePath || typeof filePath !== 'string') {
        return Promise.resolve(Buffer.alloc(0));
    }
    return fsp.readFile(filePath)
        .then((data) => {
            fsp.unlink(filePath).catch((err) => {
                if (err && err.code !== 'ENOENT') {
                    console.error(`Failed to unlink shared memory file ${filePath}:`, err);
                }
            });
            return data;
        })
        .catch((err) => {
            console.error(`Failed to read shared memory file ${filePath}:`, err);
            return Buffer.alloc(0);
        });
}

/**
 * Python Worker Pool - Manages multiple workers
 */
class PythonWorkerPool extends EventEmitter {
    constructor(pythonPath, poolSize = 1, preloadCode = "", namespaceKey = "") {
        super();
        this.pythonPath = pythonPath;
        this.poolSize = Math.max(1, Math.min(poolSize, 10)); // 1-10 workers
        this.workers = [];
        this.roundRobinIndex = 0;
        this.queue = [];
        this.shuttingDown = false;
        this.preloadCode = preloadCode || "";
        this.preloading = false;
        this.namespaceKey = namespaceKey || 'default';
        this.ready = false;
        this.readyPromise = null;
    }

    /**
     * Initialize the worker pool
     */
    async initialize() {
        if (this.ready) {
            return Promise.resolve();
        }

        if (this.readyPromise) {
            return this.readyPromise;
        }

        this.readyPromise = (async () => {
            try {
                const promises = [];

                for (let i = 0; i < this.poolSize; i++) {
                    const worker = new PythonWorker(this.pythonPath, i);

                    worker.on('error', (error) => {
                        console.error(`Worker ${i} error:`, error);
                        this.emit('worker-error', error);
                    });

                    worker.on('exit', (code) => {
                        console.log(`Worker ${i} exited with code ${code}`);
                        if (this.shuttingDown || worker.intentionalStop) {
                            return;
                        }

                        worker.start()
                            .then(() => this._preloadWorker(worker))
                            .then(() => {
                                this.processQueue();
                            })
                            .catch((err) => {
                                console.error(`Failed to restart worker ${i}:`, err);
                            });
                    });

                    worker.on('available', () => {
                        this.processQueue();
                    });

                    promises.push(worker.start());
                    this.workers.push(worker);
                }

                await Promise.all(promises);
                await this.preloadWorkers();
                this.ready = true;
                this.emit('ready');
                this.processQueue();
            } catch (error) {
                this.shuttingDown = true;
                await Promise.all(this.workers.map(worker => worker.stop().catch(() => {})));
                this.workers = [];
                this.readyPromise = null;
                this.ready = false;
                this.preloading = false;
                this.shuttingDown = false;
                this.emit('error', error);
                throw error;
            }

            this.readyPromise = null;
        })();

        return this.readyPromise;
    }

    async _preloadWorker(worker) {
        const code = (this.preloadCode || '').trim();
        if (!code) {
            return;
        }
        await worker.preload(code, this.namespaceKey);
    }

    async preloadWorkers() {
        const code = (this.preloadCode || '').trim();
        if (!code) {
            return;
        }

        this.preloading = true;
        try {
            await Promise.all(this.workers.map((worker, index) =>
                worker.preload(code, this.namespaceKey).catch((err) => {
                    console.error(`Worker ${index} preload failed:`, err);
                    throw err;
                })
            ));
        } finally {
            this.preloading = false;
        }
    }

    isReady() {
        return this.ready === true;
    }

    /**
     * Get an available worker (round-robin)
     */
    getWorker() {
        const count = this.workers.length;
        for (let i = 0; i < count; i++) {
            const index = (this.roundRobinIndex + i) % count;
            const worker = this.workers[index];

            if (worker.ready && !worker.busy) {
                this.roundRobinIndex = (index + 1) % count;
                return worker;
            }
        }

        return null;
    }

    /**
     * Stop all workers
     */
    stop() {
        this.shuttingDown = true;
        const stopPromises = this.workers.map(worker => worker.stop());
        this.workers = [];
        this.ready = false;
        this.readyPromise = null;
        this.queue = [];
        this.preloading = false;
        return Promise.all(stopPromises)
            .catch((err) => {
                console.error('Failed to stop worker pool:', err);
                throw err;
            })
            .finally(() => {
                this.shuttingDown = false;
            });
    }

    async reload(preloadCode = "") {
        if (this.readyPromise) {
            try {
                await this.readyPromise;
            } catch (error) {
                // ignore, we'll attempt restart regardless
            }
        }

        this.preloadCode = preloadCode || "";
        this.emit('reload-start');
        this.shuttingDown = true;
        const stopErrors = [];
        await Promise.all(this.workers.map((worker, index) =>
            worker.stop().catch((err) => {
                stopErrors.push({ index, err });
            })
        ));

        this.workers = [];
        this.queue = [];
        this.ready = false;
        this.shuttingDown = false;
        this.preloading = false;

        if (stopErrors.length) {
            stopErrors.forEach(({ index, err }) => {
                console.error(`Worker ${index} failed to stop during reload:`, err);
            });
        }

        return this.initialize();
    }

    /**
     * Get pool statistics
     */
    getStats() {
        let ready = 0;
        let busy = 0;
        let available = 0;
        const workers = this.workers;
        for (let i = 0; i < workers.length; i++) {
            const worker = workers[i];
            if (worker.ready) {
                ready++;
                if (!worker.busy) {
                    available++;
                }
            }
            if (worker.busy) {
                busy++;
            }
        }
        return {
            total: workers.length,
            ready,
            busy,
            available,
            queue: this.queue.length
        };
    }

    /**
     * Queue execution request and handle dispatching
     */
    execute(msg, code, callback, options = {}) {
        const request = {
            msg,
            code,
            callback,
            options,
            assigned: false,
            completed: false,
            cancelled: false,
            worker: null,
            requestId: null
        };

        request.wrappedCallback = (error, result) => {
            request.completed = true;
            if (!request.cancelled) {
                callback(error, result);
            }
        };

        this.queue.push(request);
        this.processQueue();

        return {
            cancel: (reason = 'Request cancelled') => {
                if (request.completed || request.cancelled) {
                    return;
                }

                request.cancelled = true;

                if (!request.assigned) {
                    this.queue = this.queue.filter(item => item !== request);
                    return;
                }

                if (request.worker) {
                    request.worker.terminateActiveRequest(reason, { silent: true })
                        .then(() => {
                            this.processQueue();
                        })
                        .catch((err) => {
                            console.error('Failed to terminate worker after cancellation:', err);
                        });
                }
            }
        };
    }

    /**
     * Process queued execution requests
     */
    processQueue() {
        if (this.shuttingDown || this.preloading || !this.ready) {
            return;
        }

        let worker = this.getWorker();

        while (worker && this.queue.length > 0) {
            const request = this.queue.shift();

            if (!request || request.cancelled) {
                worker = this.getWorker();
                continue;
            }

            request.assigned = true;
            request.worker = worker;

            const requestId = worker.execute(request.msg, request.code, request.options, (error, result) => {
                request.wrappedCallback(error, result);
                this.processQueue();
            });

            if (!requestId) {
                // Worker rejected the request, requeue and try later
                request.assigned = false;
                request.worker = null;
                if (!request.cancelled) {
                    this.queue.unshift(request);
                }
                break;
            }

            request.requestId = requestId;

            worker = this.getWorker();
        }
    }
}

module.exports = {
    PythonWorker,
    PythonWorkerPool,
    computeCodeId: hashCode,
    readSharedFile,
    INLINE_MAX_BYTES,
    INLINE_KEY,
    SHARED_SENTINEL_KEY,
    SHARED_BASE64_KEY
};
