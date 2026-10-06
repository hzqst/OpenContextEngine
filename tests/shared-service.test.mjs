import test from 'node:test';
import assert from 'node:assert/strict';
import {mkdtemp, mkdir, rm, readFile, access} from 'node:fs/promises';
import {fork} from 'node:child_process';
import {tmpdir} from 'node:os';
import {join} from 'node:path';
import {startSharedService, startService} from '../src/service.mjs';
import {python} from './helpers/live-service.mjs';

test('Independent clients share a writer, survive a client closing, and recover after a crash', {skip:!python,timeout:30000}, async t => {
  const dir = await mkdtemp(join(tmpdir(), 'oce-shared-'));
  const root = join(dir, 'repo');
  await mkdir(root);
  const settings = {python, config:{root,state:join(dir,'state'),port:0,
    serviceKey:'test-service-key',embeddingUrl:'http://127.0.0.1:1',embeddingIdentity:'test',
    embeddingKey:'test',embeddingDimensions:2,reranker:{baseUrl:'http://127.0.0.1:1'},pollSeconds:1,debounceSeconds:0}};
  const clients = [];
  const start = (value = settings) => {
    const processHandle = fork(new URL('./helpers/shared-client.mjs',import.meta.url),[],{silent:true});
    let sequence = 0;
    const pending = new Map();
    processHandle.on('message',message => {
      const request = pending.get(message.id); pending.delete(message.id);
      if (message.error) request.reject(new Error(message.error)); else request.resolve(message.result);
    });
    processHandle.on('exit',()=>{for (const request of pending.values()) request.reject(new Error('Client exited')); pending.clear();});
    const call = (method) => new Promise((resolve,reject) => {
      const id = sequence++; pending.set(id,{resolve,reject}); processHandle.send({id,method,settings:value});
    });
    let closing;
    const client = {ready:call('start'),check:()=>call('check'),close:()=>closing ||= call('close')};
    clients.push(client); return client;
  };
  t.after(async () => {
    await Promise.allSettled(clients.map(client => client.close()));
    // The worker exits after its last lease is released.
    for (const state of ['state','other-state','transition-state']) {
      for (let i=0;i<60;i++) {
        try {await access(join(dir,state,'worker.json'));} catch {break;}
        await new Promise(r=>setTimeout(r,100));
      }
      await assert.rejects(access(join(dir,state,'worker.json')));
    }
    await rm(dir,{recursive:true,force:true});
  });
  const a = start(), b = start();
  const [first, second] = await Promise.all([a.ready,b.ready]);
  assert.equal(first.baseUrl,second.baseUrl);
  await a.close();
  await b.check();
  const status = await fetch(second.baseUrl+'/status',{headers:{authorization:'Bearer '+second.apiKey}});
  assert.equal(200,status.status);
  const search = await fetch(second.baseUrl+'/search',{method:'POST',
    headers:{authorization:'Bearer '+second.apiKey,'content-type':'application/json'},
    body:JSON.stringify({query:'find code',budget:256,freshnessWaitMs:5000})});
  assert.equal(200,search.status);
  assert.equal('',(await search.json()).context);
  const incompatible = start({...settings,config:{...settings.config,embeddingDimensions:3}});
  await assert.rejects(incompatible.ready,/incompatible/i);
  const descriptor = JSON.parse(await readFile(join(settings.config.state,'worker.json'),'utf8'));
  process.kill(descriptor.pid);
  await assert.rejects(b.check());
  const replacement = start();
  const next = await replacement.ready;
  assert.notEqual(first.baseUrl,next.baseUrl);
  const otherRoot = join(dir,'other'); await mkdir(otherRoot);
  const other = await start({...settings,config:{...settings.config,root:otherRoot,state:join(dir,'other-state')}}).ready;
  assert.notEqual(next.baseUrl,other.baseUrl);
  const transition = {...settings,config:{...settings.config,state:join(dir,'transition-state')}};
  const retiring = startService(transition,{log:()=>{}});
  try {
    await retiring.ready;
    const waiting = start(transition);
    const release = setTimeout(()=>{void retiring.close();},500);
    try {await waiting.ready;} finally {clearTimeout(release);}
  } finally {await retiring.close();}
});

test('Missing shared worker executable reports startup failure', async t => {
  const dir = await mkdtemp(join(tmpdir(),'oce-shared-missing-'));
  t.after(()=>rm(dir,{recursive:true,force:true}));
  const worker = startSharedService({python:'/nonexistent/python',config:{state:dir,root:dir}});
  await assert.rejects(worker.ready,/ENOENT/);
  await worker.close();
});
