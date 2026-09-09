// Tests for the hot-mode worker protocol (v1.1.0): binary frames, inline
// binary transport, per-worker code cache, and the node-level behaviour that
// depends on them (context updates, warnings, errors, timeouts, recovery).
//
// The node is driven through a minimal mock of the Node-RED runtime API, so
// this covers python-executor.js end to end without a Node-RED install.
//
// Run:  node test/hot-protocol.js   (or: npm test)
// PASS: prints "ALL TESTS PASSED" and exits 0.

'use strict';
const assert = require('assert');
const EventEmitter = require('events');
const path = require('path');
const util = require('util');
const { spawn } = require('child_process');
const { PythonWorker, PythonWorkerPool } = require('../nodes/python-worker.js');

const PY = process.env.PYTHON || 'python3';
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

process.on('uncaughtException', (err) => {
    console.error('\n*** UNCAUGHT EXCEPTION ***:', err && (err.stack || err.message));
    process.exit(7);
});

// ---------------------------------------------------------------------------
// Mock Node-RED runtime
// ---------------------------------------------------------------------------
class MockNode extends EventEmitter {}
const registry = {};
const flowStore = new Map();
const globalStore = new Map();
const RED = {
    nodes: {
        createNode(node, config) {
            node.id = config.id || ('n' + Math.random().toString(16).slice(2));
            node.name = config.name || '';
            node.type = 'python-executor';
            node.log_ = { status: [], warn: [], error: [], log: [] };
            node.status = (s) => { node.log_.status.push(s); };
            node.warn = (m) => { node.log_.warn.push(String(m)); };
            node.error = (e) => { node.log_.error.push(String(e && e.message ? e.message : e)); };
            node.log = (m) => { node.log_.log.push(String(m)); };
            node.debug = () => {};
            node.send = () => {};
            node.context = () => ({
                flow: { get: (k) => flowStore.get(k), set: (k, v) => flowStore.set(k, v) },
                global: { get: (k) => globalStore.get(k), set: (k, v) => globalStore.set(k, v) }
            });
        },
        registerType(type, ctor) { util.inherits(ctor, MockNode); registry[type] = ctor; },
        getNode() { return null; }
    },
    events: { on() {} },
    httpAdmin: { post() {} },
    auth: null
};
require('../nodes/python-executor.js')(RED);
const PythonExecutor = registry['python-executor'];

function makeNode(config) {
    return new PythonExecutor(Object.assign({
        pythonPath: PY, timeout: 5000, hotMode: true, workerPoolSize: 1, preloadImports: ''
    }, config));
}

async function ready(node) {
    for (let i = 0; i < 500; i++) {
        if (node.workerPool && node.workerPool.isReady()) return;
        if (node.hotError) throw node.hotError;
        await sleep(20);
    }
    throw new Error('pool never became ready');
}

function sendMsg(node, msg) {
    return new Promise((resolve) => {
        const sent = [];
        node.emit('input', msg, (m) => sent.push(m), (err) => resolve({ sent, err: err || null }));
    });
}

function closeNode(node) {
    return new Promise((resolve) => node.emit('close', false, resolve));
}

function runPython(script, env) {
    return new Promise((resolve) => {
        const proc = spawn(PY, ['-c', script], { env: Object.assign({}, process.env, env || {}) });
        let out = '';
        proc.stdout.on('data', (d) => { out += d; });
        proc.on('close', (code) => resolve({ code, out }));
    });
}

let passed = 0;
async function test(name, fn) {
    await fn();
    passed++;
    console.log(`PASS ${name}`);
}

(async () => {
    const hasNumpy = (await runPython('import numpy')).code === 0;

    // ---- worker level ----------------------------------------------------
    await test('worker: non-BMP unicode survives the frame protocol (JS length != Python length)', async () => {
        const w = new PythonWorker(PY, 'u');
        await w.start();
        const res = await new Promise((resolve) => w.execute({ payload: 'héllo ✓ 😀' }, "return {'payload': msg['payload'] + ' 😀', 'n': len(msg['payload'])}", {}, (err, r) => resolve({ err, r })));
        assert.ifError(res.err);
        assert.strictEqual(res.r.result.payload, 'héllo ✓ 😀 😀');
        assert.strictEqual(res.r.result.n, 9);
        await w.stop();
    });

    await test('worker: Buffers travel inline both ways, byte-identical, no shared files', async () => {
        const w = new PythonWorker(PY, 'b');
        await w.start();
        const big = Buffer.alloc(3 * 1024 * 1024);
        for (let i = 0; i < big.length; i++) big[i] = (i * 31) & 0xff;
        const msg = { payload: big, list: [Buffer.from('a'), { deep: Buffer.from('bc') }], n: 1 };
        const res = await new Promise((resolve) => w.execute(msg, [
            "assert isinstance(msg['payload'], bytes), type(msg['payload'])",
            "assert msg['list'][0] == b'a' and msg['list'][1]['deep'] == b'bc'",
            "return {'echo': msg['payload'], 'rev': msg['payload'][:8][::-1], 'ba': bytearray(b'xy'), 'mv': memoryview(b'mv'), 'tup': (1, b'z')}"
        ].join('\n'), {}, (err, r) => resolve({ err, r })));
        assert.ifError(res.err);
        assert.ok(Buffer.isBuffer(res.r.result.echo) && res.r.result.echo.equals(big));
        assert.deepStrictEqual(Array.from(res.r.result.rev), Array.from(big.subarray(0, 8)).reverse());
        assert.strictEqual(res.r.result.ba.toString(), 'xy');
        assert.strictEqual(res.r.result.mv.toString(), 'mv');
        assert.deepStrictEqual(res.r.result.tup, [1, Buffer.from('z')]);
        // The original message must not have been mutated by the transport
        assert.ok(Buffer.isBuffer(msg.payload) && Buffer.isBuffer(msg.list[0]));
        await w.stop();
    });

    await test('worker: typed arrays are delivered as bytes', async () => {
        const w = new PythonWorker(PY, 't');
        await w.start();
        const res = await new Promise((resolve) => w.execute({ payload: new Uint16Array([1, 256]) }, "return {'v': list(msg['payload'])}", {}, (err, r) => resolve({ err, r })));
        assert.ifError(res.err);
        assert.deepStrictEqual(res.r.result.v, [1, 0, 0, 1]);
        await w.stop();
    });

    await test('worker: user print() goes to stderr and cannot corrupt the protocol', async () => {
        const w = new PythonWorker(PY, 'p');
        const stderr = [];
        w.on('error', (e) => { if (e.isWorkerStderr) stderr.push(e.message); });
        await w.start();
        const run = () => new Promise((resolve) => w.execute({ payload: 1 }, "print('hello from user code')\nimport sys\nsys.stdout.write('raw\\n')\nreturn {'ok': True}", {}, (err, r) => resolve({ err, r })));
        for (let i = 0; i < 3; i++) {
            const res = await run();
            assert.ifError(res.err);
            assert.strictEqual(res.r.result.ok, true);
        }
        await sleep(100);
        assert.ok(stderr.some((m) => m.includes('hello from user code')), 'print output should surface on stderr');
        await w.stop();
    });

    await test('worker: code is shipped once per worker and cached by id; cache survives errors', async () => {
        const w = new PythonWorker(PY, 'c');
        await w.start();
        const code = "return {'v': msg['payload'] * 2}";
        const run = (payload) => new Promise((resolve) => w.execute({ payload }, code, { nodeId: 'ns' }, (err, r) => resolve({ err, r })));
        let res = await run(2);
        assert.strictEqual(res.r.result.v, 4);
        assert.strictEqual(w.knownCodeIds.size, 1);
        const frames = [];
        const origWrite = w._writeFrame.bind(w);
        w._writeFrame = (h, b) => { frames.push(h); origWrite(h, b); };
        res = await run(3);
        assert.strictEqual(res.r.result.v, 6);
        assert.strictEqual(frames.length, 1);
        assert.strictEqual(frames[0].code, undefined, 'second request must not carry the source');
        // Simulate a lost cache (fresh interpreter): worker must ask for the code and Node must resend it
        w.knownCodeIds.clear();
        await w.restart();
        w.knownCodeIds.add(frames[0].code_id); // lie: pretend the new process knows it
        frames.length = 0;
        res = await run(5);
        assert.ifError(res.err);
        assert.strictEqual(res.r.result.v, 10);
        assert.strictEqual(frames.length, 2, 'expected a resend with source after UnknownCode');
        assert.strictEqual(typeof frames[1].code, 'string');
        // A runtime error must not evict the compiled code
        const bad = await new Promise((resolve) => w.execute({ payload: null }, code, { nodeId: 'ns' }, (err, r) => resolve({ err, r })));
        assert.ok(bad.err && bad.err.type === 'TypeError');
        assert.ok(w.knownCodeIds.size >= 1);
        await w.stop();
    });

    await test('worker: shared-memory transport still works when forced via ROSEPETAL_PY_INLINE_MAX_BYTES', async () => {
        const script = `
const { PythonWorker } = require(${JSON.stringify(path.join(__dirname, '..', 'nodes', 'python-worker.js'))});
const w = new PythonWorker(${JSON.stringify(PY)}, 's');
w.start().then(() => {
    const big = Buffer.alloc(200000, 7);
    w.execute({ payload: big, small: Buffer.from('s') }, "return {'echo': msg['payload'], 'small': msg['small'], 'n': len(msg['payload'])}", {}, (err, r) => {
        if (err) { console.log(JSON.stringify({ err: err.message })); process.exit(1); }
        console.log(JSON.stringify({ ok: r.result.echo.equals(big), n: r.result.n, small: r.result.small.toString() }));
        w.stop().then(() => process.exit(0));
    });
});`;
        const r = await runPython('', {});
        void r;
        const res = await new Promise((resolve) => {
            const proc = spawn(process.execPath, ['-e', script], { env: Object.assign({}, process.env, { ROSEPETAL_PY_INLINE_MAX_BYTES: '65536' }) });
            let out = '';
            proc.stdout.on('data', (d) => { out += d; });
            proc.on('close', (code) => resolve({ code, out }));
        });
        assert.strictEqual(res.code, 0, res.out);
        const parsed = JSON.parse(res.out.trim().split('\n').pop());
        assert.deepStrictEqual(parsed, { ok: true, n: 200000, small: 's' });
    });

    await test('pool: concurrent requests spread over workers and all complete', async () => {
        const pool = new PythonWorkerPool(PY, 3, '', 'pool3');
        await pool.initialize();
        const run = (i) => new Promise((resolve, reject) => pool.execute({ payload: i }, "import time\ntime.sleep(0.05)\nreturn {'v': msg['payload']}", (err, r) => err ? reject(err) : resolve(r.result.v), { nodeId: 'pool3' }));
        const t0 = Date.now();
        const out = await Promise.all([0, 1, 2, 3, 4, 5].map(run));
        assert.deepStrictEqual(out.sort(), [0, 1, 2, 3, 4, 5]);
        assert.ok(Date.now() - t0 < 250, 'six 50ms jobs on three workers should take ~100ms');
        await pool.stop();
    });

    // ---- node level ------------------------------------------------------
    await test('node: result merges into the original message and performance metrics are attached', async () => {
        const node = makeNode({ name: 'QC step', func: "msg['payload'] = msg['payload'] + 1\nreturn msg" });
        await ready(node);
        const r = await sendMsg(node, { payload: 1, topic: 'keep', _msgid: 'm1' });
        assert.ifError(r.err);
        assert.strictEqual(r.sent.length, 1);
        assert.strictEqual(r.sent[0].payload, 2);
        assert.strictEqual(r.sent[0].topic, 'keep');
        assert.strictEqual(r.sent[0]._msgid, 'm1');
        const perf = r.sent[0].performance['QC step'];
        for (const k of ['transferToPythonMs', 'executionMs', 'transferToJsMs', 'totalMs']) {
            assert.strictEqual(typeof perf[k], 'number');
        }
        await closeNode(node);
    });

    await test('node: flow/global context get/set, Buffers in context, and node.warn', async () => {
        flowStore.clear(); globalStore.clear();
        flowStore.set('count', 41);
        globalStore.set('img', Buffer.from([1, 2, 3]));
        const node = makeNode({ func: [
            "count = flow_ctx.get('count', 0)",
            "flow_ctx.set('count', count + 1)",
            "assert global_ctx['img'] == b'\\x01\\x02\\x03', global_ctx['img']",
            "global_ctx.set('out', {'blob': b'\\xff\\x00', 'n': count})",
            "node.warn(f'count is now {count + 1}')",
            "return {'seen': count}"
        ].join('\n') });
        await ready(node);
        const r = await sendMsg(node, { payload: 1 });
        assert.ifError(r.err);
        assert.strictEqual(r.sent[0].seen, 41);
        assert.strictEqual(flowStore.get('count'), 42);
        assert.ok(Buffer.isBuffer(globalStore.get('out').blob));
        assert.deepStrictEqual(Array.from(globalStore.get('out').blob), [255, 0]);
        assert.deepStrictEqual(node.log_.warn, ['count is now 42']);
        await closeNode(node);
    });

    await test('node: Python exceptions become done(err) with type prefix; context updates before the raise are kept', async () => {
        flowStore.clear();
        const node = makeNode({ func: "flow_ctx.set('partial', 7)\nnode.warn('before')\nraise ValueError('boom')" });
        await ready(node);
        const r = await sendMsg(node, { payload: 1 });
        assert.strictEqual(r.sent.length, 0);
        assert.strictEqual(r.err.message, 'ValueError: boom');
        assert.strictEqual(flowStore.get('partial'), 7);
        assert.deepStrictEqual(node.log_.warn, ['before']);
        await closeNode(node);
    });

    await test('node: timeout kills the request, the worker restarts and the next message succeeds', async () => {
        const node = makeNode({ timeout: 300, func: "import time\nif msg['payload'] == 'slow':\n    time.sleep(3)\nreturn {'payload': 'ok'}" });
        await ready(node);
        const r1 = await sendMsg(node, { payload: 'slow' });
        assert.strictEqual(r1.err.message, 'Python execution timed out after 300ms');
        await ready(node);
        const r2 = await sendMsg(node, { payload: 'fast' });
        assert.ifError(r2.err);
        assert.strictEqual(r2.sent[0].payload, 'ok');
        await closeNode(node);
    });

    await test('node: worker crash is reported and the pool self-heals', async () => {
        const node = makeNode({ func: "import os\nif msg['payload'] == 'die':\n    os._exit(3)\nreturn {'payload': 'alive'}" });
        await ready(node);
        const r1 = await sendMsg(node, { payload: 'die' });
        assert.ok(r1.err && /Worker exited unexpectedly/.test(r1.err.message), r1.err && r1.err.message);
        await sleep(1500);
        const r2 = await sendMsg(node, { payload: 'x' });
        assert.ifError(r2.err);
        assert.strictEqual(r2.sent[0].payload, 'alive');
        await closeNode(node);
    });

    await test('node: status updates are coalesced under load but settle on the final state', async () => {
        const node = makeNode({ func: "return {'payload': 1}" });
        await ready(node);
        node.log_.status.length = 0;
        for (let i = 0; i < 200; i++) await sendMsg(node, { payload: i });
        const during = node.log_.status.length;
        assert.ok(during < 60, `expected throttled status traffic, got ${during} updates for 200 messages`);
        await sleep(3300);
        const last = node.log_.status[node.log_.status.length - 1];
        assert.ok(/^hot: ready/.test(last.text), last.text);
        await closeNode(node);
    });

    await test('node: two nodes on one pool keep their own code', async () => {
        const a = makeNode({ func: "return {'payload': 'A' + str(msg['payload'])}" });
        const b = makeNode({ func: "return {'payload': 'B' + str(msg['payload'])}" });
        await ready(a); await ready(b);
        assert.strictEqual(a.workerPool, b.workerPool);
        for (let i = 0; i < 3; i++) {
            assert.strictEqual((await sendMsg(a, { payload: i })).sent[0].payload, 'A' + i);
            assert.strictEqual((await sendMsg(b, { payload: i })).sent[0].payload, 'B' + i);
        }
        await closeNode(a); await closeNode(b);
    });

    await test('node: cold mode returns bytes as Buffers (no shared-memory descriptors leak into the flow)', async () => {
        const node = makeNode({ hotMode: false, func: "return {'payload': b'\\x01\\x02', 'nested': [b'x']}" });
        const r = await sendMsg(node, { payload: 0 });
        assert.ifError(r.err);
        assert.ok(Buffer.isBuffer(r.sent[0].payload));
        assert.deepStrictEqual(Array.from(r.sent[0].payload), [1, 2]);
        assert.ok(Buffer.isBuffer(r.sent[0].nested[0]));
        await closeNode(node);
    });

    await test('node: cold mode with empty code passes the message through', async () => {
        const node = makeNode({ hotMode: false, func: "" });
        const r = await sendMsg(node, { payload: 'p' });
        assert.ifError(r.err);
        assert.strictEqual(r.sent[0].payload, 'p');
        await closeNode(node);
    });

    if (hasNumpy) {
        await test('node: rp_to_cv / rp_from_cv round trip in hot mode is byte-identical and BGR-ordered', async () => {
            const w = 64, h = 48;
            const data = Buffer.alloc(w * h * 3);
            for (let i = 0; i < data.length; i++) data[i] = (i * 7) & 0xff;
            const node = makeNode({ func: "img = rp_to_cv(msg['payload'])\nout = rp_from_cv(img, like=msg['payload'])\nreturn {'payload': out, 'bgr0': [int(v) for v in img[0][0]], 'shape': list(img.shape)}" });
            await ready(node);
            const r = await sendMsg(node, { payload: { data, width: w, height: h, channels: 3, colorSpace: 'RGB', dtype: 'uint8' } });
            assert.ifError(r.err);
            assert.ok(r.sent[0].payload.data.equals(data));
            assert.deepStrictEqual(r.sent[0].bgr0, [data[2], data[1], data[0]]);
            assert.deepStrictEqual(r.sent[0].shape, [h, w, 3]);
            await closeNode(node);
        });

        await test('node: unserialisable return values report the same TypeError as before', async () => {
            const node = makeNode({ func: "import numpy as np\nreturn {'x': np.float32(1.5)}" });
            await ready(node);
            const r = await sendMsg(node, { payload: 0 });
            assert.strictEqual(r.err.message, 'TypeError: Object of type float32 is not JSON serializable');
            await closeNode(node);
        });
    } else {
        console.log('SKIP numpy-dependent tests (numpy not available to ' + PY + ')');
    }

    console.log(`\nALL TESTS PASSED (${passed})`);
    process.exit(0);
})().catch((e) => {
    console.error('\nTEST FAILURE:', e && (e.stack || e));
    process.exit(1);
});
