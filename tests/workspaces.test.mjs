import test from 'node:test';
import assert from 'node:assert/strict';
import { EventEmitter } from 'node:events';
import { mkdtemp, mkdir, writeFile, symlink, rm, realpath } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { createWorkspaceManager } from '../src/workspaces.mjs';

async function setup(t, options = {}) {
  const dir = await mkdtemp(join(tmpdir(), 'oce-workspaces-'));
  const first = join(dir, 'first'), second = join(dir, 'second');
  await mkdir(first); await mkdir(second);
  const started = [], sweeps = [];
  const clock = {ms:0};
  const manager = createWorkspaceManager(options.fixed ? {root:first,state:join(dir,'fixed-state')}
    : {state:join(dir,'states')}, {
    configure: value => value,
    environment: {OCE_CONFIG_HOME:join(dir,'config')}, // Keep the shared user settings out of the test.
    now: () => clock.ms,
    idleMs: options.idleMs, retryMs: options.retryMs,
    schedule: (sweep, interval) => {sweeps.push({sweep,interval}); return sweeps.length;},
    cancel: () => {},
    start: settings => {
      const child = new EventEmitter();
      const worker = {child, settings, closes:0, ready:options.ready?.(started.length) ?? Promise.resolve({root:settings.root}),
        async close() {
          this.closes++;
          if (options.closeGate) await options.closeGate(this);
          child.emit('exit', 0);
        }};
      started.push(worker);
      return worker;
    },
  });
  t.after(async () => {await manager.close(); await rm(dir, {recursive:true,force:true});});
  return {manager, started, sweeps, clock, advance: ms => {clock.ms += ms;}, dir, first, second};
}

test('Workspace routing canonicalizes aliases, deduplicates concurrent starts and separates state', async t => {
  let ready;
  const gate = new Promise(resolve => {ready = resolve;});
  const {manager, started, dir, first, second} = await setup(t, {ready: () => gate});
  const alias = join(dir,'alias');
  await symlink(first,alias,process.platform === 'win32' ? 'junction' : 'dir');
  const a = manager.get(first), b = manager.get(alias), c = manager.get(second);
  assert.equal(started.length,2);
  assert.notEqual(started[0].settings.state,started[1].settings.state);
  assert.equal(started[0].settings.root,await realpath(first));
  ready({baseUrl:'fixture'});
  assert.equal((await a).config,(await b).config);
  await c;
  await manager.get(first);
  assert.equal(started.length,2);
  await manager.close();
  assert.deepEqual(started.map(worker => worker.closes),[1,1]);
  await assert.rejects(manager.get(first),/shutting down/);
});

test('Workspace routing rejects missing, relative and non-directory paths before starting a worker', async t => {
  const {manager,started,dir} = await setup(t);
  await writeFile(join(dir,'file'),'source');
  for (const path of [undefined, '', 'relative', join(dir,'missing'), join(dir,'file')]) {
    await assert.rejects(manager.get(path));
  }
  assert.equal(started.length,0);
});

test('Fixed workspace mode accepts its own path and refuses switching projects', async t => {
  const {manager,started,first,second,dir} = await setup(t,{fixed:true});
  await manager.get();
  await manager.get(first);
  await assert.rejects(manager.get(second),/fixed to --root/);
  assert.equal(started.length,1);
  assert.equal(started[0].settings.state,join(dir,'fixed-state'));
});

test('Failed startup is cleaned up, reports the reason and delays the next attempt', async t => {
  const {manager,started,first,advance} = await setup(t,{ready: n => n === 0 ? Promise.reject(new Error('startup failed')) : undefined});
  await assert.rejects(manager.get(first),/startup failed/);
  assert.equal(started[0].closes,1);
  // The same failure would repeat, so a repeated call reports it without spawning again.
  await assert.rejects(manager.get(first),/startup failed[\s\S]*Retry delayed 10s/);
  assert.equal(started.length,1);
  advance(9999);
  await assert.rejects(manager.get(first),/Retry delayed/);
  assert.equal(started.length,1);
  advance(1);
  await manager.get(first);
  assert.equal(started.length,2);
});

test('Exited workers are restarted and closing also stops a pending startup', async t => {
  let ready;
  const {manager,started,first} = await setup(t,{ready: n => n === 1 ? new Promise(resolve => {ready=resolve;}) : undefined});
  await manager.get(first);
  started[0].child.emit('exit',1);
  const pending = manager.get(first);
  assert.equal(started.length,2);
  await manager.close();
  assert.equal(started[1].closes,1);
  ready({baseUrl:'fixture'});
  await pending;
});

test('Idle workers are reclaimed only after their last request finished', async t => {
  const {manager,started,first,sweeps,advance} = await setup(t,{idleMs:1000});
  assert.deepEqual(sweeps.map(entry => entry.interval),[1000]);
  const sweep = sweeps[0].sweep;
  const held = await manager.get(first);
  advance(5000);
  sweep();
  assert.equal(started[0].closes,0); // A request in flight keeps its worker.
  held.release();
  held.release();                    // Releasing twice must not shorten the window.
  advance(999);
  sweep();
  assert.equal(started[0].closes,0);
  advance(1);
  sweep();
  assert.equal(started[0].closes,1); // Idle for the whole window releases the writer lock.
  const restarted = await manager.get(first);
  assert.equal(started.length,2);
  restarted.release();
});

test('A replacement worker waits for the reclaimed worker to release its index lock', async t => {
  // Closing resolves on the next macrotask, so the replacement below starts while it is pending.
  const {manager,started,first,sweeps,advance} = await setup(t,{idleMs:1000, closeGate:() => new Promise(setImmediate)});
  const held = await manager.get(first);
  held.release();
  advance(1000);
  sweeps[0].sweep();
  assert.equal(started[0].closes,1);
  const replacing = manager.get(first);
  assert.equal(started.length,1); // Still closing: the new writer must not lock the same index.
  assert.equal((await replacing).config.root,await realpath(first));
  assert.equal(started.length,2);
});

test('The idle window comes from the shared settings and rejects unusable values', async t => {
  const dir = await mkdtemp(join(tmpdir(), 'oce-idle-'));
  t.after(() => rm(dir,{recursive:true,force:true}));
  const intervals = [];
  const options = {configure: value => value, environment: {OCE_CONFIG_HOME:dir},
    schedule: (sweep, interval) => {intervals.push(interval); return intervals.length;}, cancel: () => {}};
  const disabled = createWorkspaceManager({state:join(dir,'states')}, {...options, environment:{OCE_CONFIG_HOME:dir,OCE_WORKER_IDLE_SECONDS:'0'}});
  await disabled.close();
  assert.deepEqual(intervals,[]); // Zero disables reclamation.
  const configured = createWorkspaceManager({state:join(dir,'states')}, {...options, environment:{OCE_CONFIG_HOME:dir,OCE_WORKER_IDLE_SECONDS:'30'}});
  await configured.close();
  assert.deepEqual(intervals,[30000]);
  assert.throws(() => createWorkspaceManager({state:join(dir,'states')},
    {...options, environment:{OCE_CONFIG_HOME:dir,OCE_WORKER_IDLE_SECONDS:'soon'}}),
    /OCE_WORKER_IDLE_SECONDS must be zero or a positive number of seconds/);
});
