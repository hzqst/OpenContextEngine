import { realpathSync, statSync } from 'node:fs';
import { isAbsolute, resolve, join } from 'node:path';
import { createHash } from 'node:crypto';
import { serviceConfig, startSharedService } from './service.mjs';
import { loadEnvironment } from './config.mjs';

// A failed worker start repeats identically, so retrying every call only spawns doomed processes.
const STARTUP_RETRY_MS = 10000;
const DEFAULT_IDLE_SECONDS = 300;

function idleWindow(environment) {
  const configured = loadEnvironment(environment).OCE_WORKER_IDLE_SECONDS;
  if (configured === undefined || configured === '') return DEFAULT_IDLE_SECONDS*1000;
  const seconds = Number(configured);
  if (!Number.isFinite(seconds) || seconds < 0) {
    throw new Error('OCE_WORKER_IDLE_SECONDS must be zero or a positive number of seconds');
  }
  return seconds*1000;
}

function directory(path) {
  const canonical = realpathSync.native(path);
  if (!statSync(canonical).isDirectory()) throw new Error('directory_path must point to a directory');
  return canonical;
}

function repeat(sweep, interval) {
  const timer = setInterval(sweep, interval);
  timer.unref(); // Idle reclamation must not keep the MCP process alive.
  return timer;
}

// Each canonical repository owns one worker, including while it is starting.
export function createWorkspaceManager({root, state} = {}, {configure = serviceConfig, start = startSharedService,
  environment = process.env, now = () => Date.now(), retryMs = STARTUP_RETRY_MS, idleMs = idleWindow(environment),
  schedule = repeat, cancel = clearInterval} = {}) {
  const fixedRoot = root ? directory(resolve(root)) : undefined;
  const workers = new Map(), failures = new Map(), retiring = new Map();
  let closing = false, closed;

  function lease(entry) {
    let released = false;
    return () => {
      if (released) return;
      released = true;
      entry.leases--;
      entry.idleSince = now();
    };
  }

  async function get(directoryPath) {
    if (closing) throw new Error('MCP workspace manager is shutting down');
    if (directoryPath !== undefined && (typeof directoryPath !== 'string' || !isAbsolute(directoryPath))) {
      throw new Error('directory_path must be an absolute project directory');
    }
    if (!directoryPath && !fixedRoot) {
      throw new Error('Pass directory_path with the absolute path of the project to search, or start MCP with --root');
    }
    const repository = directoryPath ? directory(directoryPath) : fixedRoot;
    if (fixedRoot && repository !== fixedRoot) {
      throw new Error('This MCP server is fixed to --root; omit --root at startup to search multiple projects');
    }
    // A reclaimed worker must release its writer lock before a replacement locks it.
    const pending = retiring.get(repository);
    if (pending) await pending.catch(() => {}); // A failed close is reported where the caller retries.
    const failure = failures.get(repository);
    if (failure) {
      const waited = now() - failure.at;
      if (waited < retryMs) {
        throw new Error(`${failure.message}\nRetry delayed ${Math.ceil((retryMs - waited)/1000)}s after the previous startup failure.`);
      }
      failures.delete(repository);
    }
    let entry = workers.get(repository);
    if (entry?.started && entry.worker.check) {
      const checking = entry;
      checking.leases++;
      try {await entry.worker.check();} catch {
        if (workers.get(repository) === entry) workers.delete(repository);
        await entry.worker.close();
        entry = workers.get(repository);
      } finally {
        checking.leases--;
        checking.idleSince = now();
      }
      if (closing) throw new Error('MCP workspace manager is shutting down');
    }
    if (!entry) {
      const workspaceState = state && (fixedRoot ? state : join(resolve(state),
        createHash('sha256').update(repository).digest('hex').slice(0, 24)));
      const worker = start(configure({root:repository, state:workspaceState}));
      entry = {worker, leases:0, idleSince:now()};
      workers.set(repository, entry);
      const remove = () => {if (workers.get(repository) === entry) workers.delete(repository);};
      worker.child.once('exit', remove);
      entry.ready = worker.ready.then(config => {entry.started = true; return config;}).catch(async error => {
        failures.set(repository, {at:now(), message:error.message});
        try {await worker.close();} finally {remove();}
        throw error;
      });
    }
    // The lease spans the caller's request, so reclamation cannot stop a worker mid-search.
    entry.leases++;
    try {
      return {config:await entry.ready, release:lease(entry)};
    } catch (error) {
      entry.leases--;
      entry.idleSince = now();
      throw error;
    }
  }

  function sweep() {
    if (closing || !idleMs) return;
    for (const [repository, entry] of workers) {
      if (entry.leases || now() - entry.idleSince < idleMs) continue;
      workers.delete(repository);
      // Start the shutdown now and keep it visible, so a replacement waits for the writer lock.
      const closingWorker = (async () => entry.worker.close())().finally(() => {
        if (retiring.get(repository) === closingWorker) retiring.delete(repository);
      });
      retiring.set(repository, closingWorker);
    }
  }

  const timer = idleMs ? schedule(sweep, Math.min(idleMs, 60000)) : undefined;

  function close() {
    if (!closed) {
      closing = true;
      if (timer) cancel(timer);
      const pending = [...workers.values()].map(entry => entry.worker.close());
      closed = Promise.allSettled([...pending, ...retiring.values()]).then(results => {
        workers.clear();
        const errors = results.filter(result => result.status === 'rejected').map(result => result.reason);
        if (errors.length) throw new AggregateError(errors, 'Failed to close repository workers');
      });
    }
    return closed;
  }
  return {get, close};
}
