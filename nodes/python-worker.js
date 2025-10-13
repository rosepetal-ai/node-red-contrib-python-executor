const { spawn } = require('child_process');
const path = require('path');
const EventEmitter = require('events');

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
        this.buffer = '';
        this.responseBuffer = '';
        this.awaitingLength = true;
        this.expectedLength = 0;
        this.pendingCallbacks = new Map();
        this.requestCounter = 0;
        this.stopPromise = null;
        this.intentionalStop = false;
    }

    /**
     * Start the persistent Python worker process
     */
    start() {
        return new Promise((resolve, reject) => {
            const scriptPath = path.join(__dirname, 'python-worker-script.py');

            try {
                this.ready = false;
                this.process = spawn(this.pythonPath, [scriptPath]);

                // Handle stdout (responses from Python)
                this.process.stdout.on('data', (data) => {
                    this.handleStdout(data);
                });

                // Handle stderr (errors)
                this.process.stderr.on('data', (data) => {
                    const error = data.toString();
                    const errObj = new Error(`Worker stderr: ${error}`);
                    errObj.isWorkerStderr = true;
                    this.emit('error', errObj);
                });

                // Handle process exit
                this.process.on('close', (code) => {
                    const wasIntentional = this.intentionalStop;
                    this.intentionalStop = false;
                    this.ready = false;
                    this.busy = false;
                    this.activeRequestId = null;
                    this.process = null;

                    if (wasIntentional) {
                        this._failPendingCallbacks(new Error('Worker stopped'), { silent: true });
                    } else {
                        this._failPendingCallbacks(new Error('Worker exited unexpectedly'));
                    }

                    this.emit('exit', code);
                    this.emit('available');
                });

                // Handle spawn errors
                this.process.on('error', (err) => {
                    reject(err);
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
     * Handle stdout data from Python worker
     */
    handleStdout(data) {
        this.buffer += data.toString();

        while (true) {
            if (this.awaitingLength) {
                // Look for newline indicating length
                const newlineIndex = this.buffer.indexOf('\n');
                if (newlineIndex === -1) {
                    break; // Wait for more data
                }

                const lengthStr = this.buffer.substring(0, newlineIndex).trim();
                this.buffer = this.buffer.substring(newlineIndex + 1);

                if (lengthStr.length === 0) {
                    continue;
                }

                this.expectedLength = parseInt(lengthStr, 10);

                if (Number.isNaN(this.expectedLength)) {
                    this.emit('error', new Error(`Invalid length prefix from worker: "${lengthStr}"`));
                    this._resetBufferState();
                    continue;
                }

                this.awaitingLength = false;
                this.responseBuffer = '';

            } else {
                // Read JSON data
                if (this.buffer.length < this.expectedLength) {
                    break; // Wait for more data
                }

                this.responseBuffer = this.buffer.substring(0, this.expectedLength);
                this.buffer = this.buffer.substring(this.expectedLength);
                this.awaitingLength = true;

                // Process the complete response
                try {
                    const response = JSON.parse(this.responseBuffer);
                    this.handleResponse(response);
                } catch (error) {
                    this.emit('error', new Error(`Failed to parse response: ${error.message}`));
                    this._resetBufferState();
                }
            }
        }
    }

    /**
     * Reset buffering state after protocol errors
     */
    _resetBufferState() {
        this.buffer = '';
        this.responseBuffer = '';
        this.awaitingLength = true;
        this.expectedLength = 0;
    }

    /**
     * Handle a complete response from Python
     */
    handleResponse(response) {
        if (response.status === 'ready') {
            // Worker is ready
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

            this._failPendingCallbacks(internalError, { silent: true });
            this.emit('error', internalError);
            this.emit('available');
            return;
        }

        // Response to a request
        const requestId = response.request_id;
        const callback = this.pendingCallbacks.get(requestId);

        if (callback) {
            this.pendingCallbacks.delete(requestId);
            this.busy = false;
            this.activeRequestId = null;

            if (response.status === 'success') {
                callback(null, response.result);
            } else {
                const error = new Error(response.error || 'Unknown error');
                error.type = response.type;
                error.traceback = response.traceback;
                callback(error, null);
            }

            this.emit('available');
        }
    }

    /**
     * Execute Python code
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

        // Store callback
        this.pendingCallbacks.set(requestId, callback);

        // Prepare request
        const request = {
            request_id: requestId,
            msg: msg,
            code: code,
            node_id: options.nodeId,
            preload: options.preload || false
        };

        // Send request (protocol: length\n + json data)
        const requestJson = JSON.stringify(request);
        const message = `${requestJson.length}\n${requestJson}`;

        try {
            this.process.stdin.write(message);
        } catch (error) {
            this.busy = false;
            this.activeRequestId = null;
            this.pendingCallbacks.delete(requestId);
            setImmediate(() => callback(error));
            return null;
        }

        return requestId;
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
    }

    /**
     * Stop the worker
     */
    stop() {
        if (!this.process) {
            this.ready = false;
            this.busy = false;
            this.activeRequestId = null;
            return Promise.resolve();
        }

        if (this.stopPromise) {
            return this.stopPromise;
        }

        this.ready = false;
        this.busy = false;
        this.activeRequestId = null;

        this.stopPromise = new Promise((resolve) => {
            const currentProcess = this.process;

            const handleClose = () => {
                currentProcess.removeListener('close', handleClose);
                this.process = null;
                this.stopPromise = null;
                resolve();
            };

            currentProcess.once('close', handleClose);
            this.intentionalStop = true;
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

        await this.restart();
    }
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
        for (let i = 0; i < this.workers.length; i++) {
            const index = (this.roundRobinIndex + i) % this.workers.length;
            const worker = this.workers[index];

            if (worker.isAvailable()) {
                this.roundRobinIndex = (index + 1) % this.workers.length;
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
        return {
            total: this.workers.length,
            ready: this.workers.filter(w => w.ready).length,
            busy: this.workers.filter(w => w.busy).length,
            available: this.workers.filter(w => w.isAvailable()).length,
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
    PythonWorkerPool
};
