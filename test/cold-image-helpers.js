// Regression test for "NameError: name 'rp_to_cv' is not defined" in cold mode.
//
// Background: rp_to_cv / rp_from_cv were only defined in python-worker-script.py,
// which cold mode never runs -- it builds its own script and spawns it with
// `python -c`. Since cold mode is the default and the node's stock example code
// calls rp_to_cv, every freshly-dragged node failed on its first message.
//
// Both modes now share nodes/rp_image_helpers.py: hot imports it, cold inlines it.
//
// Run:  node test/cold-image-helpers.js   (or: npm test)
// PASS: prints "ALL TESTS PASSED" and exits 0.
// SKIP: exits 0 early if the interpreter has no numpy (helpers need it).

const { spawn } = require('child_process');
const fs = require('fs');
const path = require('path');

const PY = process.env.PYTHON || 'python3';
const HELPERS = fs.readFileSync(path.join(__dirname, '..', 'nodes', 'rp_image_helpers.py'), 'utf8');

function runPython(script, stdin = '') {
    return new Promise((resolve) => {
        const proc = spawn(PY, ['-c', script]);
        let out = '';
        let err = '';
        proc.stdout.on('data', (d) => { out += d; });
        proc.stderr.on('data', (d) => { err += d; });
        proc.on('close', (code) => resolve({ code, out, err }));
        proc.stdin.end(stdin);
    });
}

// Mirrors how executeColdMode() assembles its script: helper source at module
// level, user code indented into user_function.
function buildColdScript(userCode) {
    const body = userCode.split('\n').map((line) => '    ' + line).join('\n');
    return `${HELPERS}\n\ndef user_function(msg):\n${body}\n`;
}

// A Node Buffer serialized by JSON.stringify -- exactly what cold mode's stdin
// carries, since only hot mode swaps buffers for shared-memory descriptors.
function bufferJson(bytes) {
    return { type: 'Buffer', data: Array.from(bytes) };
}

(async () => {
    const probe = await runPython('import numpy');
    if (probe.code !== 0) {
        console.log('SKIP: numpy not available to ' + PY);
        process.exit(0);
    }

    // --- Test 0: the real cold template still inlines the helpers ---
    // Tests 1-3 rebuild the script the way executeColdMode does, so on their own
    // they would not notice the interpolation being dropped again. This reads the
    // actual source to guard that.
    const executorSrc = fs.readFileSync(path.join(__dirname, '..', 'nodes', 'python-executor.js'), 'utf8');
    const helperMark = executorSrc.indexOf('${RP_IMAGE_HELPERS_SOURCE}');
    const userFnMark = executorSrc.indexOf('def user_function(msg):');
    if (helperMark === -1 || userFnMark === -1 || helperMark > userFnMark) {
        throw new Error('Test 0 FAILED: cold template does not inline the helpers before user_function');
    }
    console.log('Test 0 PASSED: executeColdMode inlines rp_image_helpers.py');

    // --- Test 1: the helpers are defined in a cold-mode script ---
    let script = buildColdScript('return [rp_to_cv.__name__, rp_from_cv.__name__]')
        + 'print(user_function({}))\n';
    let r = await runPython(script);
    if (r.code !== 0 || !r.out.includes('rp_to_cv')) {
        throw new Error('Test 1 FAILED: helpers missing from cold script: ' + (r.err || r.out));
    }
    console.log('Test 1 PASSED: rp_to_cv / rp_from_cv defined in cold mode');

    // --- Test 2: round-trip a grayscale image through the cold path ---
    const gray = Buffer.alloc(4 * 4, 200);
    const msg = { payload: { data: bufferJson(gray), width: 4, height: 4, channels: 1, colorSpace: 'GRAY', dtype: 'uint8' } };
    script = buildColdScript([
        "img = rp_to_cv(msg['payload'])",
        "out = rp_from_cv(img, like=msg['payload'])",
        "return {'shape': list(img.shape), 'roundtrip': out['data'] == bytes(msg['payload']['data']['data'])}"
    ].join('\n')) + `import json, sys\nprint(json.dumps(user_function(json.loads(sys.stdin.read()))))\n`;
    r = await runPython(script, JSON.stringify(msg));
    if (r.code !== 0) {
        throw new Error('Test 2 FAILED: cold round-trip errored: ' + r.err);
    }
    let parsed = JSON.parse(r.out);
    if (parsed.shape.join('x') !== '4x4' || parsed.roundtrip !== true) {
        throw new Error('Test 2 FAILED: unexpected round-trip result: ' + r.out);
    }
    console.log('Test 2 PASSED: grayscale round-trip through cold mode is byte-identical');

    // --- Test 3: RGB input is handed to cv2 as BGR, and swapped back on return ---
    const rgb = Buffer.from([10, 20, 30]);
    const rgbMsg = { payload: { data: bufferJson(rgb), width: 1, height: 1, channels: 3, colorSpace: 'RGB', dtype: 'uint8' } };
    script = buildColdScript([
        "img = rp_to_cv(msg['payload'])",
        "out = rp_from_cv(img, like=msg['payload'])",
        "return {'bgr': [int(v) for v in img[0][0]], 'back': list(out['data'])}"
    ].join('\n')) + `import json, sys\nprint(json.dumps(user_function(json.loads(sys.stdin.read()))))\n`;
    r = await runPython(script, JSON.stringify(rgbMsg));
    if (r.code !== 0) {
        throw new Error('Test 3 FAILED: RGB case errored: ' + r.err);
    }
    parsed = JSON.parse(r.out);
    if (parsed.bgr.join(',') !== '30,20,10' || parsed.back.join(',') !== '10,20,30') {
        throw new Error('Test 3 FAILED: channel order wrong: ' + r.out);
    }
    console.log('Test 3 PASSED: RGB -> BGR -> RGB channel order preserved');

    // --- Test 4: hot mode resolves the helpers via the shared module ---
    const workerDir = path.join(__dirname, '..', 'nodes');
    r = await runPython(`import sys; sys.path.insert(0, ${JSON.stringify(workerDir)});\n`
        + 'from rp_image_helpers import rp_to_cv, rp_from_cv; print("ok")');
    if (r.code !== 0 || !r.out.includes('ok')) {
        throw new Error('Test 4 FAILED: worker script cannot import shared helpers: ' + r.err);
    }
    console.log('Test 4 PASSED: hot mode imports the same shared module');

    console.log('\nALL TESTS PASSED');
    process.exit(0);
})().catch((err) => {
    console.error('\n' + (err && err.message));
    process.exit(1);
});
