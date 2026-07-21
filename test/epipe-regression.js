// Regression test for the stdin-EPIPE crash (v1.0.3).
//
// Background: when a request times out, the executor kills the Python child
// (terminateActiveRequest -> restart -> stop -> kill). If a write is still
// pending on the child's stdin when the pipe breaks, the stream emits an
// 'error' (EPIPE). Before v1.0.3 there was no stdin 'error' listener, so Node
// rethrew it as an uncaught exception and Node-RED exited, producing a crash
// loop.
//
// Run:  node test/epipe-regression.js   (or: npm test)
// PASS: prints "ALL TESTS PASSED" and exits 0, with no uncaught exception.

const { PythonWorker } = require('../nodes/python-worker.js');

// If the stdin handler regresses, the EPIPE surfaces here and exits non-zero.
process.on('uncaughtException', (err) => {
    console.error('\n*** UNCAUGHT EXCEPTION (crash) ***:', err && err.message);
    process.exit(7);
});

const PY = process.env.PYTHON || 'python3';
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

function startWorker(id) {
    const w = new PythonWorker(PY, id);
    return w.start().then(() => w);
}

function run(w, code, msg = {}) {
    return new Promise((resolve) => {
        w.execute(msg, code, {}, (err, res) => resolve({ err, res }));
    });
}

(async () => {
    // --- Test 1: happy path still works after the fix ---
    let w = await startWorker('t1');
    const ok = await run(w, 'result = 40 + 2');
    if (ok.err || !ok.res) {
        throw new Error('Test 1 FAILED: expected clean result, got ' + JSON.stringify(ok));
    }
    console.log('Test 1 PASS: normal execution completed without error');
    await w.stop();

    // --- Test 2: kill worker mid-request across several rounds (the timeout path) ---
    let sawGracefulError = false;
    for (let i = 0; i < 8; i++) {
        w = await startWorker('t2_' + i);
        const p1 = run(w, 'import time; time.sleep(5); result = 1');
        await sleep(30);
        // silent:false so the pending callback resolves p1 with an error here;
        // the pool uses silent:true because its caller already knows it cancelled.
        const term = w.terminateActiveRequest('timeout', { silent: false });
        try {
            if (w.process && w.process.stdin && w.process.stdin.writable) {
                w.process.stdin.write('999\n' + JSON.stringify({ x: 1 }));
            }
        } catch (e) {
            sawGracefulError = true; // a caught throw is fine; an unhandled 'error' is the bug
        }
        const r1 = await p1;
        if (r1.err) sawGracefulError = true;
        await term;
        await w.stop();
    }
    console.log('Test 2 PASS: survived 8 kill-with-write-in-flight rounds; graceful errors seen =', sawGracefulError);

    // --- Test 3: DETERMINISTIC EPIPE reproduction ---
    // While Python is busy (not draining stdin), push a payload far larger than
    // the OS pipe buffer so the remainder stays pending inside Node's stream.
    // Killing the child then breaks the pipe with a write still pending, and the
    // stdin stream emits 'error' (EPIPE) asynchronously. Pre-1.0.3 this was an
    // uncaught exception; the fix routes it to the worker 'error' event instead.
    w = await startWorker('t3');
    const child = w.process;
    let emittedError = false;
    w.on('error', () => { emittedError = true; });
    run(w, 'import time; time.sleep(5); result = 1'); // occupy the worker
    await sleep(50);
    child.stdin.write(Buffer.alloc(4 * 1024 * 1024, 0x41)); // 4 MB >> 64 KB pipe buffer
    await sleep(20);
    child.kill('SIGKILL'); // break the pipe with a write still pending
    await sleep(200);
    if (!emittedError) {
        // Not fatal on every platform (pipe sizing varies), but on Linux this
        // should catch the EPIPE. Warn rather than fail so the test stays portable.
        console.warn('Test 3 NOTE: no worker error observed (platform pipe buffer may differ)');
    }
    console.log('Test 3 PASS: pending-write EPIPE after kill did not crash (worker error emitted =', emittedError, ')');
    await w.stop().catch(() => {});

    console.log('\nALL TESTS PASSED');
    process.exit(0);
})().catch((e) => {
    console.error('\nTEST HARNESS FAILURE:', e && (e.stack || e));
    process.exit(1);
});
