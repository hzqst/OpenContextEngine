import {startSharedService} from '../../src/service.mjs';

let worker;
process.on('message', async ({id,method,settings}) => {
  try {
    let result;
    if (method === 'start') {worker = startSharedService(settings); result = await worker.ready;}
    if (method === 'check') await worker.check();
    if (method === 'close') await worker?.close();
    process.send({id,result});
    if (method === 'close') process.disconnect();
  } catch (error) {process.send({id,error:error.message});}
});
