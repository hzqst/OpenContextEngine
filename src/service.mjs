import { spawn } from 'node:child_process';
import { existsSync, realpathSync, openSync, closeSync } from 'node:fs';
import { createHash, randomBytes } from 'node:crypto';
import { homedir } from 'node:os';
import { resolve, dirname, delimiter } from 'node:path';
import { createInterface } from 'node:readline';
import { EventEmitter } from 'node:events';
import { mkdir, readFile, unlink } from 'node:fs/promises';
import { projectRoot, loadEnvironment } from './config.mjs';
import { defaultPython, venvPython } from './runtime.mjs';
import { embeddingTransportConfig, remoteRerankerConfig, rerankerExecutionTransport } from './eval/remote-models.mjs';

const STARTUP_TIMEOUT_MS = 15000;
// A startup failure only reports an exit code, so keep the tail of stderr for the caller.
const DIAGNOSTIC_LINES = 12, DIAGNOSTIC_CHARS = 2000;
const SHARED_PROTOCOL = 1, HEARTBEAT_MS = 5000, DISCOVERY_POLL_MS = 100, CONTENDER_RETRY_MS = 1000;

function stable(value) {
  if (Array.isArray(value)) return value.map(stable);
  if (value && typeof value === 'object') return Object.fromEntries(Object.keys(value).sort().map(key => [key,stable(value[key])]));
  return value;
}

// A handle owns a lease, never the shared process. The writer lock elects the server.
export function startSharedService(settings) {
  const child = new EventEmitter(), lease = randomBytes(24).toString('hex');
  const {serviceKey, port, state, ...identity} = settings.config;
  const fingerprint = createHash('sha256').update(JSON.stringify(stable({protocol:SHARED_PROTOCOL,
    identity,workerEnv:settings.workerEnv}))).digest('hex');
  const descriptorPath = resolve(state,'worker.json');
  let descriptor, timer, closed = false, exited = false, heartbeat = Promise.resolve();
  function lost() {if (!exited) {exited = true; child.emit('exit');}}
  async function request(record, release = false) {
    const url = new URL(record.baseUrl);
    if (url.protocol !== 'http:' || url.hostname !== '127.0.0.1' || url.username || url.password ||
        url.pathname !== '/' || url.search || url.hash) throw new Error('Invalid shared worker address');
    const response = await fetch(record.baseUrl+'/lease',{method:'POST',redirect:'error',signal:AbortSignal.timeout(2000),
      headers:{authorization:'Bearer '+record.apiKey,'content-type':'application/json'},
      body:JSON.stringify({instance:record.instance,lease,release,protocol:SHARED_PROTOCOL,fingerprint})});
    if (response.status === 409) throw new Error('Existing shared worker has incompatible configuration or protocol; close its clients before changing settings');
    if (!response.ok) throw new Error('Shared worker is unavailable');
    const body = await response.json();
    if (body.instance !== record.instance) throw new Error('Shared worker identity mismatch');
  }
  async function discover() {
    try {
      const record = JSON.parse(await readFile(descriptorPath,'utf8'));
      await request(record);
      return record;
    } catch (error) {
      if (/incompatible/.test(error.message)) throw error;
      return undefined;
    }
  }
  const ready = (async () => {
    await mkdir(state,{recursive:true,mode:0o700});
    descriptor = await discover();
    if (!descriptor && !closed) {
      const errorPath = resolve(state,`startup-${lease}.json`);
      const config = {...settings.config,shared:{protocol:SHARED_PROTOCOL,fingerprint,errorPath}};
      const logPath = resolve(state,'worker.log');
      let failure, nextStart;
      let stopped = false, finished = false;
      function launch() {
        stopped = false;
        const logFile = openSync(logPath,'a',0o600);
        let processHandle;
        try {
          processHandle = spawn(settings.python,[resolve(projectRoot,'scripts/retrieval-server.py')],{
            cwd:projectRoot,detached:true,windowsHide:true,
            env:{...process.env,...settings.workerEnv,PATH:dirname(process.execPath)+delimiter+(process.env.PATH || ''),
              PYTHONUTF8:'1',PYTHONIOENCODING:'utf-8',OPENBLAS_NUM_THREADS:'2',OMP_NUM_THREADS:'2'},
            stdio:['pipe',logFile,logFile]});
        } finally {closeSync(logFile);}
        processHandle.once('exit',()=>{stopped = true; if (finished) void unlink(errorPath).catch(()=>{});});
        processHandle.once('error',error => {failure = error;});
        processHandle.stdin.on('error',()=>{});
        processHandle.stdin.end(JSON.stringify(config)+'\n');
        processHandle.unref();
        nextStart = Date.now()+CONTENDER_RETRY_MS;
      }
      launch();
      const deadline = Date.now()+STARTUP_TIMEOUT_MS;
      try {
        while (!closed && Date.now()<deadline) {
          descriptor = await discover();
          if (descriptor) break;
          if (failure) throw failure;
          try {
            const report = JSON.parse(await readFile(errorPath,'utf8'));
            if (!report.error.includes('already has a running writer')) throw new Error(report.error);
            // Another contender may still be publishing its endpoint.
            // If the holder is exiting, retry election after it releases the lock.
            if (stopped && Date.now() >= nextStart) {
              await unlink(errorPath);
              launch();
            }
          } catch (error) {
            if (error.code !== 'ENOENT' && !(error instanceof SyntaxError)) throw error;
            if (stopped && error.code === 'ENOENT') {
              const diagnostic = (await readFile(logPath,'utf8')).slice(-DIAGNOSTIC_CHARS);
              throw new Error('Shared worker exited before startup: '+diagnostic);
            }
          }
          await new Promise(resolve => setTimeout(resolve,DISCOVERY_POLL_MS));
        }
        if (!descriptor && !closed) throw new Error('Timed out discovering shared worker; the index may be held by an older, non-shared worker');
      } finally {finished = true; await unlink(errorPath).catch(()=>{});}
    }
    if (closed) {if (descriptor) await request(descriptor,true).catch(()=>{}); throw new Error('Shared worker handle closed');}
    timer = setInterval(()=>{heartbeat = request(descriptor).catch(()=>{clearInterval(timer); lost();});},HEARTBEAT_MS);
    timer.unref();
    return {baseUrl:descriptor.baseUrl,apiKey:descriptor.apiKey};
  })();
  return {child,ready,
    async check() {await ready; try {await request(descriptor);} catch (error) {lost(); throw error;}},
    async close() {
      closed = true; clearInterval(timer);
      await ready.catch(()=>{});
      await heartbeat;
      if (descriptor) await request(descriptor,true).catch(()=>{});
    },
  };
}

export function serviceConfig({root, state, port = 0} = {}, environment = process.env) {
  const env = loadEnvironment(environment);
  const embedding = embeddingTransportConfig(env);
  const reranker = remoteRerankerConfig(env), runtime = rerankerExecutionTransport(env);
  const repository = root ? realpathSync.native(resolve(root)) : undefined;
  const workspaceId = repository && createHash('sha256').update(repository).digest('hex').slice(0, 24);
  const candidates = ['.venv', '.pilot-state/baselines/cocoindex-venv',
    '.pilot-state/language-adapters-venv'].map(path => venvPython(resolve(projectRoot, path)));
  const currentState = repository && resolve(homedir(), '.cache/opencontextengine', workspaceId);
  const previousState = repository && resolve(homedir(), '.cache/reponerve', workspaceId);
  const defaultState = repository && (existsSync(currentState) ? currentState : existsSync(previousState) ? previousState : currentState);
  return {
    python: env.OCE_PYTHON || candidates.find(existsSync) || defaultPython(),
    workerEnv: {...(env.OCE_GO_BINARY ? {OCE_GO_BINARY:env.OCE_GO_BINARY} : {}),
      ...(env.TIKTOKEN_CACHE_DIR ? {TIKTOKEN_CACHE_DIR:env.TIKTOKEN_CACHE_DIR} : {})},
    config: {root: repository, state: state ? resolve(state) : repository
      ? defaultState : resolve(projectRoot, '.pilot-state/reponerve/index'),
    serviceKey: env.OCE_API_KEY || randomBytes(32).toString('hex'), port,
    embeddingUrl: embedding.requestBaseUrl, embeddingIdentity: embedding.baseUrl,
    embeddingKey: env.EMBEDDING_API_KEY,
    embeddingModel: env.EMBEDDING_MODEL || 'Qwen3-Embedding-4B',
    embeddingDimensions: Number(env.OCE_EMBEDDING_DIMENSIONS || 1024),
    embeddingRevision: env.OCE_EMBEDDING_REVISION || '1',
    reranker: {...reranker, baseUrl: runtime.requestBaseUrl},
    languageOptions: env.OCE_LANGUAGE_OPTIONS ? JSON.parse(env.OCE_LANGUAGE_OPTIONS) : {},
    // The worker validates each suffix; splitting keeps the environment form simple.
    excludeSuffixes: (env.OCE_EXCLUDE_SUFFIXES || '').split(',').map(entry => entry.trim()).filter(Boolean),
    pollSeconds: Number(env.OCE_POLL_SECONDS || 1),
    debounceSeconds: Number(env.OCE_DEBOUNCE_SECONDS || .3)},
  };
}

export function startService(settings, {log = line => process.stderr.write(line + '\n')} = {}) {
  const {python, config} = settings;
  const child = spawn(python, [resolve(projectRoot, 'scripts/retrieval-server.py')], {
    cwd: projectRoot, env: {...process.env, ...settings.workerEnv,
      PATH:dirname(process.execPath)+delimiter+(process.env.PATH || ''),
      PYTHONUTF8:'1', PYTHONIOENCODING:'utf-8', OPENBLAS_NUM_THREADS:'2', OMP_NUM_THREADS:'2'},
    stdio:['pipe', 'pipe', 'pipe'],
  });
  const lines = createInterface({input: child.stdout});
  const errors = createInterface({input: child.stderr});
  const diagnostics = [];
  errors.on('line', line => {
    diagnostics.push(line);
    if (diagnostics.length > DIAGNOSTIC_LINES) diagnostics.shift();
    log(line);
  });
  function startupFailure(summary) {
    const tail = diagnostics.join('\n').slice(-DIAGNOSTIC_CHARS);
    return new Error(tail ? `${summary}\nWorker stderr:\n${tail}` : summary);
  }
  child.stdin.on('error', () => {}); // Spawn/exit handlers report early failures.
  child.stdin.end(JSON.stringify(config) + '\n');
  const ready = new Promise((resolveReady, reject) => {
    const timer = setTimeout(() => {child.kill(); reject(startupFailure('Retrieval worker startup timed out'));}, STARTUP_TIMEOUT_MS);
    child.once('error', error => {clearTimeout(timer); reject(error);});
    child.once('exit', code => {clearTimeout(timer); reject(startupFailure(`Retrieval worker exited (${code})`));});
    lines.on('line', line => {
      try {
        const value = JSON.parse(line);
        if (value.listening) {
          clearTimeout(timer);
          resolveReady({baseUrl:value.listening, apiKey:config.serviceKey});
          return;
        }
        // The worker names the reason before it exits with its traceback.
        if (value.error) {
          clearTimeout(timer);
          reject(startupFailure(`Retrieval worker failed to start: ${value.error}`));
          return;
        }
      } catch { /* Non-protocol worker logs belong on stderr. */ }
      log(line);
    });
  });
  async function close() {
    if (!child.pid || child.exitCode !== null || child.signalCode !== null) return;
    await new Promise(resolveClosed => {
      const timer = setTimeout(() => child.kill('SIGKILL'), 5000);
      child.once('exit', () => {clearTimeout(timer); resolveClosed();});
      child.kill('SIGTERM');
    });
  }
  return {child, ready, close};
}
