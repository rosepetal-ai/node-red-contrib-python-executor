const path = require('path');
const fs = require('fs');
const os = require('os');
const { spawn, execFile } = require('child_process');

// Environment storage base path
const ENVS_BASE_PATH = path.join(os.homedir(), '.node-red', 'python-envs');

// Ensure base path exists
function ensureEnvsBasePath() {
    if (!fs.existsSync(ENVS_BASE_PATH)) {
        fs.mkdirSync(ENVS_BASE_PATH, { recursive: true });
    }
}

// Sanitize environment name for filesystem
function sanitizeName(name) {
    if (!name || typeof name !== 'string') {
        return '';
    }
    return name
        .trim()
        .toLowerCase()
        .replace(/[^a-z0-9_-]/g, '_')
        .replace(/_+/g, '_')
        .replace(/^_|_$/g, '')
        .substring(0, 50);
}

// Get platform-specific Python path within environment
function getPythonPath(envPath) {
    const isWindows = process.platform === 'win32';
    return isWindows
        ? path.join(envPath, 'Scripts', 'python.exe')
        : path.join(envPath, 'bin', 'python');
}

// Get platform-specific pip path within environment
function getPipPath(envPath) {
    const isWindows = process.platform === 'win32';
    return isWindows
        ? path.join(envPath, 'Scripts', 'pip.exe')
        : path.join(envPath, 'bin', 'pip');
}

// Try to detect system Python
function detectSystemPython() {
    return new Promise((resolve, reject) => {
        const isWindows = process.platform === 'win32';
        const candidates = isWindows
            ? ['python', 'python3', 'py']
            : ['python3', 'python'];

        tryPythonCandidate(candidates, 0, resolve, reject);
    });
}

function tryPythonCandidate(candidates, index, resolve, reject) {
    if (index >= candidates.length) {
        const error = new Error('Python not found. Please install Python 3.6+ and ensure it is in your PATH.');
        error.errorType = 'not_found';
        reject(error);
        return;
    }

    const candidate = candidates[index];
    const proc = spawn(candidate, ['--version'], {
        stdio: ['ignore', 'pipe', 'pipe'],
        shell: process.platform === 'win32'
    });

    let stdout = '';
    let stderr = '';

    proc.stdout.on('data', (data) => {
        stdout += data.toString();
    });

    proc.stderr.on('data', (data) => {
        stderr += data.toString();
    });

    proc.on('error', () => {
        tryPythonCandidate(candidates, index + 1, resolve, reject);
    });

    proc.on('close', (code) => {
        if (code !== 0) {
            tryPythonCandidate(candidates, index + 1, resolve, reject);
            return;
        }

        const output = stdout || stderr;
        const match = output.match(/Python\s+(\d+)\.(\d+)\.(\d+)/i);

        if (!match) {
            tryPythonCandidate(candidates, index + 1, resolve, reject);
            return;
        }

        const major = parseInt(match[1], 10);
        const minor = parseInt(match[2], 10);
        const patch = parseInt(match[3], 10);

        if (major < 3 || (major === 3 && minor < 6)) {
            const error = new Error(`Python 3.6+ required. Found version ${major}.${minor}.${patch}.`);
            error.errorType = 'version_too_old';
            reject(error);
            return;
        }

        // Get the full path to Python
        const whichCmd = process.platform === 'win32' ? 'where' : 'which';
        const whichProc = spawn(whichCmd, [candidate], {
            stdio: ['ignore', 'pipe', 'pipe'],
            shell: process.platform === 'win32'
        });

        let whichOutput = '';
        whichProc.stdout.on('data', (data) => {
            whichOutput += data.toString();
        });

        whichProc.on('close', () => {
            const pythonPath = whichOutput.trim().split('\n')[0].trim() || candidate;
            resolve({
                path: pythonPath,
                version: `${major}.${minor}.${patch}`,
                command: candidate
            });
        });

        whichProc.on('error', () => {
            resolve({
                path: candidate,
                version: `${major}.${minor}.${patch}`,
                command: candidate
            });
        });
    });
}

// Check if venv module is available
function checkVenvModule(pythonPath) {
    return new Promise((resolve, reject) => {
        const proc = spawn(pythonPath, ['-c', 'import venv'], {
            stdio: ['ignore', 'pipe', 'pipe'],
            shell: process.platform === 'win32'
        });

        proc.on('error', (err) => {
            const error = new Error(`Failed to check venv module: ${err.message}`);
            error.errorType = 'venv_check_failed';
            reject(error);
        });

        proc.on('close', (code) => {
            if (code !== 0) {
                const error = new Error(
                    'The venv module is not available. ' +
                    'On Debian/Ubuntu, install it with: sudo apt install python3-venv'
                );
                error.errorType = 'venv_missing';
                reject(error);
            } else {
                resolve(true);
            }
        });
    });
}

// Create a virtual environment
function createVenv(pythonPath, envPath) {
    return new Promise((resolve, reject) => {
        ensureEnvsBasePath();

        const proc = spawn(pythonPath, ['-m', 'venv', envPath], {
            stdio: ['ignore', 'pipe', 'pipe'],
            shell: process.platform === 'win32'
        });

        let stderr = '';
        proc.stderr.on('data', (data) => {
            stderr += data.toString();
        });

        proc.on('error', (err) => {
            reject(new Error(`Failed to create virtual environment: ${err.message}`));
        });

        proc.on('close', (code) => {
            if (code !== 0) {
                reject(new Error(`Failed to create virtual environment: ${stderr || 'Unknown error'}`));
            } else {
                resolve(true);
            }
        });
    });
}

// Upgrade pip in the environment
function upgradePip(envPath) {
    return new Promise((resolve, reject) => {
        const pipPath = getPipPath(envPath);
        const proc = spawn(pipPath, ['install', '--upgrade', 'pip'], {
            stdio: ['ignore', 'pipe', 'pipe'],
            shell: process.platform === 'win32',
            env: { ...process.env, PYTHONUNBUFFERED: '1' }
        });

        proc.on('error', (err) => {
            // Non-critical error, resolve anyway
            resolve(false);
        });

        proc.on('close', (code) => {
            resolve(code === 0);
        });
    });
}

// List installed packages
function listPackages(envPath) {
    return new Promise((resolve, reject) => {
        const pipPath = getPipPath(envPath);

        if (!fs.existsSync(pipPath)) {
            reject(new Error('Environment pip not found. Environment may be corrupted.'));
            return;
        }

        const proc = spawn(pipPath, ['list', '--format=json'], {
            stdio: ['ignore', 'pipe', 'pipe'],
            shell: process.platform === 'win32'
        });

        let stdout = '';
        let stderr = '';

        proc.stdout.on('data', (data) => {
            stdout += data.toString();
        });

        proc.stderr.on('data', (data) => {
            stderr += data.toString();
        });

        proc.on('error', (err) => {
            reject(new Error(`Failed to list packages: ${err.message}`));
        });

        proc.on('close', (code) => {
            if (code !== 0) {
                reject(new Error(`Failed to list packages: ${stderr || 'Unknown error'}`));
                return;
            }

            try {
                const packages = JSON.parse(stdout.trim());
                resolve(packages);
            } catch (e) {
                reject(new Error(`Failed to parse package list: ${e.message}`));
            }
        });
    });
}

// Install package with streaming output
function installPackageStreaming(envPath, packageSpec, res) {
    const pipPath = getPipPath(envPath);

    if (!fs.existsSync(pipPath)) {
        res.status(500).json({ error: 'Environment pip not found' });
        return;
    }

    // Parse package spec - can be multiple packages separated by spaces
    const packages = packageSpec.trim().split(/\s+/).filter(p => p.length > 0);

    if (packages.length === 0) {
        res.status(400).json({ error: 'No packages specified' });
        return;
    }

    // Set SSE headers
    res.writeHead(200, {
        'Content-Type': 'text/event-stream',
        'Cache-Control': 'no-cache',
        'Connection': 'keep-alive',
        'X-Accel-Buffering': 'no'
    });

    const sendEvent = (event, data) => {
        res.write(`event: ${event}\ndata: ${JSON.stringify(data)}\n\n`);
    };

    const proc = spawn(pipPath, ['install', ...packages], {
        stdio: ['ignore', 'pipe', 'pipe'],
        shell: process.platform === 'win32',
        env: { ...process.env, PYTHONUNBUFFERED: '1' }
    });

    proc.stdout.on('data', (data) => {
        sendEvent('output', { text: data.toString(), type: 'stdout' });
    });

    proc.stderr.on('data', (data) => {
        sendEvent('output', { text: data.toString(), type: 'stderr' });
    });

    proc.on('error', (err) => {
        sendEvent('error', { message: err.message });
        res.end();
    });

    proc.on('close', (code) => {
        sendEvent('complete', { code, success: code === 0 });
        res.end();
    });

    // Handle client disconnect
    res.on('close', () => {
        if (proc.exitCode === null) {
            proc.kill('SIGTERM');
        }
    });
}

// Uninstall a package
function uninstallPackage(envPath, packageName) {
    return new Promise((resolve, reject) => {
        const pipPath = getPipPath(envPath);

        if (!fs.existsSync(pipPath)) {
            reject(new Error('Environment pip not found'));
            return;
        }

        // Prevent uninstalling pip itself
        if (packageName.toLowerCase() === 'pip') {
            reject(new Error('Cannot uninstall pip'));
            return;
        }

        const proc = spawn(pipPath, ['uninstall', '-y', packageName], {
            stdio: ['ignore', 'pipe', 'pipe'],
            shell: process.platform === 'win32'
        });

        let stderr = '';
        proc.stderr.on('data', (data) => {
            stderr += data.toString();
        });

        proc.on('error', (err) => {
            reject(new Error(`Failed to uninstall package: ${err.message}`));
        });

        proc.on('close', (code) => {
            if (code !== 0) {
                reject(new Error(`Failed to uninstall package: ${stderr || 'Unknown error'}`));
            } else {
                resolve(true);
            }
        });
    });
}

// Delete an environment
function deleteEnvironment(envPath) {
    return new Promise((resolve, reject) => {
        // Safety check - only delete paths under our managed directory
        if (!envPath.startsWith(ENVS_BASE_PATH)) {
            reject(new Error('Cannot delete environment outside managed directory'));
            return;
        }

        fs.rm(envPath, { recursive: true, force: true }, (err) => {
            if (err) {
                reject(new Error(`Failed to delete environment: ${err.message}`));
            } else {
                resolve(true);
            }
        });
    });
}

// Validate an environment
function validateEnvironment(envPath) {
    const pythonPath = getPythonPath(envPath);
    return fs.existsSync(pythonPath);
}

// Get environment info from stored data
function getStoredEnvInfo(envPath) {
    const infoPath = path.join(envPath, '.rosepetal-env-info.json');
    if (fs.existsSync(infoPath)) {
        try {
            return JSON.parse(fs.readFileSync(infoPath, 'utf8'));
        } catch (e) {
            return null;
        }
    }
    return null;
}

// Store environment info
function storeEnvInfo(envPath, info) {
    const infoPath = path.join(envPath, '.rosepetal-env-info.json');
    try {
        fs.writeFileSync(infoPath, JSON.stringify(info, null, 2));
        return true;
    } catch (e) {
        return false;
    }
}

module.exports = function(RED) {

    // ============================================
    // Config Node Definition
    // ============================================
    function PythonEnvironmentNode(config) {
        RED.nodes.createNode(this, config);
        const node = this;

        node.name = config.name || '';
        const sanitized = sanitizeName(node.name);
        node.envPath = sanitized ? path.join(ENVS_BASE_PATH, sanitized) : '';
        node.pythonPath = node.envPath ? getPythonPath(node.envPath) : '';
        node.valid = node.envPath ? validateEnvironment(node.envPath) : false;

        // Log status
        if (node.name && node.valid) {
            node.log(`Python environment "${node.name}" ready at ${node.envPath}`);
        } else if (node.name && !node.valid) {
            node.warn(`Python environment "${node.name}" not found or invalid`);
        }
    }

    RED.nodes.registerType("python-environment", PythonEnvironmentNode);

    // ============================================
    // HTTP Admin API Endpoints
    // ============================================

    const needWritePermission = RED.auth && RED.auth.needsPermission
        ? RED.auth.needsPermission('python-environment.write')
        : function(req, res, next) { next(); };

    // POST /python-environment/detect-python
    // Detect system Python installation
    RED.httpAdmin.post("/python-environment/detect-python",
        needWritePermission,
        async function(req, res) {
            try {
                const pythonInfo = await detectSystemPython();
                await checkVenvModule(pythonInfo.path);
                res.json({
                    success: true,
                    python: pythonInfo.path,
                    version: pythonInfo.version,
                    command: pythonInfo.command
                });
            } catch (err) {
                res.status(400).json({
                    success: false,
                    error: err.message,
                    errorType: err.errorType || 'unknown'
                });
            }
        }
    );

    // POST /python-environment/create
    // Create new virtual environment
    RED.httpAdmin.post("/python-environment/create",
        needWritePermission,
        async function(req, res) {
            const { name } = req.body || {};

            if (!name || !name.trim()) {
                return res.status(400).json({ error: 'Environment name is required' });
            }

            const sanitized = sanitizeName(name);
            if (!sanitized) {
                return res.status(400).json({ error: 'Invalid environment name' });
            }

            const envPath = path.join(ENVS_BASE_PATH, sanitized);

            if (fs.existsSync(envPath)) {
                return res.status(400).json({
                    error: 'An environment with this name already exists',
                    errorType: 'env_exists'
                });
            }

            try {
                const pythonInfo = await detectSystemPython();
                await checkVenvModule(pythonInfo.path);
                await createVenv(pythonInfo.path, envPath);
                await upgradePip(envPath);

                // Store environment info
                storeEnvInfo(envPath, {
                    name: name.trim(),
                    sanitizedName: sanitized,
                    createdAt: new Date().toISOString(),
                    pythonVersion: pythonInfo.version,
                    systemPython: pythonInfo.path
                });

                res.json({
                    success: true,
                    name: name.trim(),
                    sanitizedName: sanitized,
                    envPath: envPath,
                    pythonPath: getPythonPath(envPath),
                    pythonVersion: pythonInfo.version
                });
            } catch (err) {
                // Cleanup on failure
                if (fs.existsSync(envPath)) {
                    try {
                        await deleteEnvironment(envPath);
                    } catch (cleanupErr) {
                        // Ignore cleanup errors
                    }
                }
                res.status(500).json({
                    error: err.message,
                    errorType: err.errorType || 'creation_failed'
                });
            }
        }
    );

    // GET /python-environment/list
    // List all managed environments
    RED.httpAdmin.get("/python-environment/list",
        function(req, res) {
            ensureEnvsBasePath();

            try {
                const entries = fs.readdirSync(ENVS_BASE_PATH, { withFileTypes: true });
                const envs = [];

                for (const entry of entries) {
                    if (entry.isDirectory()) {
                        const envPath = path.join(ENVS_BASE_PATH, entry.name);
                        const valid = validateEnvironment(envPath);
                        const info = getStoredEnvInfo(envPath) || {};

                        envs.push({
                            name: info.name || entry.name,
                            sanitizedName: entry.name,
                            envPath: envPath,
                            pythonPath: getPythonPath(envPath),
                            valid: valid,
                            createdAt: info.createdAt,
                            pythonVersion: info.pythonVersion
                        });
                    }
                }

                res.json({ environments: envs });
            } catch (err) {
                res.status(500).json({ error: err.message });
            }
        }
    );

    // GET /python-environment/:id/packages
    // List installed packages
    RED.httpAdmin.get("/python-environment/:id/packages",
        function(req, res) {
            const configNode = RED.nodes.getNode(req.params.id);

            if (!configNode) {
                return res.status(404).json({ error: 'Environment configuration not found' });
            }

            if (!configNode.envPath || !validateEnvironment(configNode.envPath)) {
                return res.status(400).json({
                    error: 'Environment not found or invalid',
                    errorType: 'env_invalid'
                });
            }

            listPackages(configNode.envPath)
                .then(packages => res.json({ packages }))
                .catch(err => res.status(500).json({ error: err.message }));
        }
    );

    // POST /python-environment/:id/packages (SSE streaming)
    // Install package with streaming output
    RED.httpAdmin.post("/python-environment/:id/packages",
        needWritePermission,
        function(req, res) {
            const configNode = RED.nodes.getNode(req.params.id);
            const packageSpec = req.body && req.body.package;

            if (!configNode) {
                return res.status(404).json({ error: 'Environment configuration not found' });
            }

            if (!configNode.envPath || !validateEnvironment(configNode.envPath)) {
                return res.status(400).json({ error: 'Environment not found or invalid' });
            }

            if (!packageSpec || !packageSpec.trim()) {
                return res.status(400).json({ error: 'Package name is required' });
            }

            installPackageStreaming(configNode.envPath, packageSpec, res);
        }
    );

    // DELETE /python-environment/:id/packages/:name
    // Uninstall package
    RED.httpAdmin.delete("/python-environment/:id/packages/:name",
        needWritePermission,
        async function(req, res) {
            const configNode = RED.nodes.getNode(req.params.id);
            const packageName = decodeURIComponent(req.params.name);

            if (!configNode) {
                return res.status(404).json({ error: 'Environment configuration not found' });
            }

            if (!configNode.envPath || !validateEnvironment(configNode.envPath)) {
                return res.status(400).json({ error: 'Environment not found or invalid' });
            }

            if (!packageName || !packageName.trim()) {
                return res.status(400).json({ error: 'Package name is required' });
            }

            try {
                await uninstallPackage(configNode.envPath, packageName);
                res.json({ success: true });
            } catch (err) {
                res.status(500).json({ error: err.message });
            }
        }
    );

    // DELETE /python-environment/:id
    // Delete entire environment
    RED.httpAdmin.delete("/python-environment/:id",
        needWritePermission,
        async function(req, res) {
            const configNode = RED.nodes.getNode(req.params.id);

            if (!configNode) {
                return res.status(404).json({ error: 'Environment configuration not found' });
            }

            if (!configNode.envPath) {
                return res.status(400).json({ error: 'Environment path not set' });
            }

            try {
                await deleteEnvironment(configNode.envPath);
                res.json({ success: true });
            } catch (err) {
                res.status(500).json({ error: err.message });
            }
        }
    );

    // GET /python-environment/:id/validate
    // Check if environment is valid
    RED.httpAdmin.get("/python-environment/:id/validate",
        function(req, res) {
            const configNode = RED.nodes.getNode(req.params.id);

            if (!configNode) {
                return res.status(404).json({
                    valid: false,
                    error: 'Configuration not found'
                });
            }

            if (!configNode.envPath) {
                return res.json({
                    valid: false,
                    error: 'Environment not configured'
                });
            }

            const valid = validateEnvironment(configNode.envPath);
            const info = getStoredEnvInfo(configNode.envPath);

            res.json({
                valid: valid,
                envPath: configNode.envPath,
                pythonPath: configNode.pythonPath,
                pythonVersion: info ? info.pythonVersion : null,
                createdAt: info ? info.createdAt : null
            });
        }
    );

    // POST /python-environment/create-for-name
    // Create environment by name (used before config node is saved)
    RED.httpAdmin.post("/python-environment/create-for-name",
        needWritePermission,
        async function(req, res) {
            const { name } = req.body || {};

            if (!name || !name.trim()) {
                return res.status(400).json({ error: 'Environment name is required' });
            }

            const sanitized = sanitizeName(name);
            if (!sanitized) {
                return res.status(400).json({ error: 'Invalid environment name' });
            }

            const envPath = path.join(ENVS_BASE_PATH, sanitized);

            // Check if already exists and is valid
            if (fs.existsSync(envPath) && validateEnvironment(envPath)) {
                const info = getStoredEnvInfo(envPath);
                return res.json({
                    success: true,
                    exists: true,
                    name: name.trim(),
                    sanitizedName: sanitized,
                    envPath: envPath,
                    pythonPath: getPythonPath(envPath),
                    pythonVersion: info ? info.pythonVersion : null
                });
            }

            // Create new environment
            try {
                const pythonInfo = await detectSystemPython();
                await checkVenvModule(pythonInfo.path);

                // Clean up any partial environment
                if (fs.existsSync(envPath)) {
                    await deleteEnvironment(envPath);
                }

                await createVenv(pythonInfo.path, envPath);
                await upgradePip(envPath);

                storeEnvInfo(envPath, {
                    name: name.trim(),
                    sanitizedName: sanitized,
                    createdAt: new Date().toISOString(),
                    pythonVersion: pythonInfo.version,
                    systemPython: pythonInfo.path
                });

                res.json({
                    success: true,
                    exists: false,
                    name: name.trim(),
                    sanitizedName: sanitized,
                    envPath: envPath,
                    pythonPath: getPythonPath(envPath),
                    pythonVersion: pythonInfo.version
                });
            } catch (err) {
                if (fs.existsSync(envPath)) {
                    try {
                        await deleteEnvironment(envPath);
                    } catch (cleanupErr) {
                        // Ignore
                    }
                }
                res.status(500).json({
                    error: err.message,
                    errorType: err.errorType || 'creation_failed'
                });
            }
        }
    );

    // ============================================
    // Name-based API Endpoints (work before deploy)
    // ============================================

    // Helper to get envPath from name
    function getEnvPathFromName(name) {
        const sanitized = sanitizeName(name);
        if (!sanitized) return null;
        return path.join(ENVS_BASE_PATH, sanitized);
    }

    // GET /python-environment/by-name/:name/packages
    // List installed packages by environment name
    RED.httpAdmin.get("/python-environment/by-name/:name/packages",
        function(req, res) {
            const envName = decodeURIComponent(req.params.name);
            const envPath = getEnvPathFromName(envName);

            if (!envPath) {
                return res.status(400).json({ error: 'Invalid environment name' });
            }

            if (!validateEnvironment(envPath)) {
                return res.status(400).json({
                    error: 'Environment not found or invalid',
                    errorType: 'env_invalid'
                });
            }

            listPackages(envPath)
                .then(packages => res.json({ packages }))
                .catch(err => res.status(500).json({ error: err.message }));
        }
    );

    // POST /python-environment/by-name/:name/packages (SSE streaming)
    // Install package by environment name with streaming output
    RED.httpAdmin.post("/python-environment/by-name/:name/packages",
        needWritePermission,
        function(req, res) {
            const envName = decodeURIComponent(req.params.name);
            const packageSpec = req.body && req.body.package;
            const envPath = getEnvPathFromName(envName);

            if (!envPath) {
                return res.status(400).json({ error: 'Invalid environment name' });
            }

            if (!validateEnvironment(envPath)) {
                return res.status(400).json({ error: 'Environment not found or invalid' });
            }

            if (!packageSpec || !packageSpec.trim()) {
                return res.status(400).json({ error: 'Package name is required' });
            }

            installPackageStreaming(envPath, packageSpec, res);
        }
    );

    // DELETE /python-environment/by-name/:name/packages/:pkgname
    // Uninstall package by environment name
    RED.httpAdmin.delete("/python-environment/by-name/:name/packages/:pkgname",
        needWritePermission,
        async function(req, res) {
            const envName = decodeURIComponent(req.params.name);
            const packageName = decodeURIComponent(req.params.pkgname);
            const envPath = getEnvPathFromName(envName);

            if (!envPath) {
                return res.status(400).json({ error: 'Invalid environment name' });
            }

            if (!validateEnvironment(envPath)) {
                return res.status(400).json({ error: 'Environment not found or invalid' });
            }

            if (!packageName || !packageName.trim()) {
                return res.status(400).json({ error: 'Package name is required' });
            }

            try {
                await uninstallPackage(envPath, packageName);
                res.json({ success: true });
            } catch (err) {
                res.status(500).json({ error: err.message });
            }
        }
    );

    // DELETE /python-environment/by-name/:name
    // Delete entire environment by name
    RED.httpAdmin.delete("/python-environment/by-name/:name",
        needWritePermission,
        async function(req, res) {
            const envName = decodeURIComponent(req.params.name);
            const envPath = getEnvPathFromName(envName);

            if (!envPath) {
                return res.status(400).json({ error: 'Invalid environment name' });
            }

            if (!fs.existsSync(envPath)) {
                return res.status(404).json({ error: 'Environment not found' });
            }

            try {
                await deleteEnvironment(envPath);
                res.json({ success: true });
            } catch (err) {
                res.status(500).json({ error: err.message });
            }
        }
    );

    // GET /python-environment/by-name/:name/validate
    // Validate environment by name
    RED.httpAdmin.get("/python-environment/by-name/:name/validate",
        function(req, res) {
            const envName = decodeURIComponent(req.params.name);
            const envPath = getEnvPathFromName(envName);

            if (!envPath) {
                return res.json({ valid: false, error: 'Invalid environment name' });
            }

            const valid = validateEnvironment(envPath);
            const info = getStoredEnvInfo(envPath);

            res.json({
                valid: valid,
                envPath: envPath,
                pythonPath: getPythonPath(envPath),
                pythonVersion: info ? info.pythonVersion : null,
                createdAt: info ? info.createdAt : null
            });
        }
    );
};
