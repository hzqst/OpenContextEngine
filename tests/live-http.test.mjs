import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp, mkdir, writeFile, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join, delimiter, dirname, resolve } from 'node:path';
import { search, indexStatus } from '../src/client.mjs';
import { fixture, python } from './helpers/live-service.mjs';
import { startService } from '../src/service.mjs';

test('A missing worker executable fails startup and closes without hanging', {timeout:2000}, async () => {
  const worker = startService({python:'/nonexistent/opencontextengine/python',config:{}},{log:()=>{}});
  await assert.rejects(worker.ready,/ENOENT/);
  await worker.close();
});

test('A worker that dies before listening reports its stderr tail instead of an exit code alone', {timeout:5000}, async () => {
  const worker = startService({python:process.execPath,config:{}},{log:()=>{}});
  await assert.rejects(worker.ready,error => {
    assert.match(error.message,/Retrieval worker exited \(1\)/);
    assert.match(error.message,/Worker stderr:\n\s*\S/); // The tail names the real cause.
    return true;
  });
  await worker.close();
});

test('A worker that rejects its configuration reports the reason, not just the exit code',
  {skip:!python,timeout:15000}, async t => {
    const dir = await mkdtemp(join(tmpdir(),'oce-startup-failure-'));
    t.after(() => rm(dir,{recursive:true,force:true}));
    const root = join(dir,'repo');
    await mkdir(root);
    const worker = startService({python,config:{root,state:join(dir,'state'),port:0,
      serviceKey:'test-only-at-least-24-characters',embeddingUrl:'http://127.0.0.1:1/v1',
      embeddingIdentity:'https://test-model.invalid/v1',embeddingKey:'test-only',
      reranker:{baseUrl:'http://127.0.0.1:1/v1'},pollSeconds:.05,debounceSeconds:0,
      excludeSuffixes:['md']}},{log:()=>{}});
    await assert.rejects(worker.ready,/Invalid OCE_EXCLUDE_SUFFIXES entry: md/);
    await worker.close();
  });

test('Live HTTP search synchronizes saved files and rejects unauthorized/invalid requests',
  {skip:!python,timeout:15000}, async t => {
    const {root,config} = await fixture(t);
    await writeFile(join(root,'main.py'),'def save():\n    return "first"\n');
    const first = await search('find save',{config,freshnessWaitMs:5000});
    assert.match(first.context, /first/);
    assert.equal(first.index.freshness,'verified-after-search');
    await writeFile(join(root,'main.py'),'def save():\n    return "second 中文"\n');
    const next = await search('find save',{config,freshnessWaitMs:5000});
    assert.match(next.context,/second/);
    assert.match(next.context,/中文/);
    assert.notEqual(first.index.identity,next.index.identity);
    assert.equal((await indexStatus({config})).status,'ready');
    await assert.rejects(search('find save',{config:{...config,apiKey:'incorrect'}}),/401/);
    const invalid = await fetch(config.baseUrl+'/search',{method:'POST',headers:{authorization:`Bearer ${config.apiKey}`},
      body:JSON.stringify({query:'q',freshnessWaitMs:-1})});
    assert.equal(invalid.status,422);
    await writeFile(join(root,'main.py'),'def broken(\n');
    // The failure names its reason, not only the fact that the index is unavailable.
    await assert.rejects(search('find save',{config,freshnessWaitMs:5000}),/Index unavailable: Index update failed: SyntaxError: /);
    await rm(join(root,'main.py'));
    const empty = await search('find save',{config,freshnessWaitMs:5000});
    assert.equal(empty.context,'');
  });

test('Live HTTP refuses results if source changes during model retrieval',
  {skip:!python,timeout:15000}, async t => {
    const {root,config,hooks} = await fixture(t);
    await writeFile(join(root,'main.py'),'def save():\n    return "before"\n');
    hooks.rerank = async () => {
      hooks.rerank = null;
      await writeFile(join(root,'main.py'),'def save():\n    return "after"\n');
    };
    await assert.rejects(search('find save',{config,freshnessWaitMs:5000}),/Source changed during search/);
    assert.match((await search('find save',{config,freshnessWaitMs:5000})).context,/after/);
  });

test('Managed workers can parse JavaScript when the desktop client PATH does not contain Node',
  {skip:!python,timeout:15000}, async t => {
    const original = process.env.PATH;
    let service;
    try {
      process.env.PATH = original.split(delimiter)
        .filter(directory => resolve(directory).toLowerCase() !== dirname(process.execPath).toLowerCase()).join(delimiter);
      service = await fixture(t);
    } finally {process.env.PATH = original;}
    await writeFile(join(service.root,'store.js'),'export function persist() { return "desktop-path-ready"; }\n');
    const result = await search('find persist',{config:service.config,freshnessWaitMs:5000});
    assert.match(result.context,/desktop-path-ready/);
  });
