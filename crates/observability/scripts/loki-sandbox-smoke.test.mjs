import test from 'node:test';
import assert from 'node:assert/strict';
import http from 'node:http';
import { run } from './loki-sandbox-smoke.mjs';

async function fixture(t, { noData=false, deliveryStatus='delivered' } = {}) {
  const requests=[];
  const server=http.createServer(async(req,res)=>{
    let raw='';for await(const chunk of req)raw+=chunk;
    requests.push({path:req.url,body:raw?JSON.parse(raw):null});
    assert.equal(req.headers['x-internal-api-key'],'local-secret');
    res.setHeader('Content-Type','application/json');
    res.end(JSON.stringify(req.url.endsWith('/evaluate')?{
      evaluated_at:'2026-10-25T10:00:00Z',delivery_enabled:false,
      rules:[{id:'test-rule',status:'evaluated',no_data:noData,instances:noData?[]:[{value:3,level:'critical',labels:{app:'sandbox-app',flow:'Publish',request_id:'must-not-forward',extra_error:'must-not-forward',merchant_id:'must-not-forward'}}]}]
    }:{status:deliveryStatus,message_id:'message-1'}));
  });
  await new Promise(resolve=>server.listen(0,'127.0.0.1',resolve));
  t.after(()=>new Promise(resolve=>server.close(resolve)));
  return {requests,env:{LOCAL_OBSERVABILITY_URL:'http://127.0.0.1:'+server.address().port,LOCAL_OBSERVABILITY_API_KEY:'local-secret'}};
}

test('dry run never notifies; explicit send delivers one sanitized measured snapshot',async t=>{
  const {requests,env}=await fixture(t);
  const dry=await run(['--rule','test-rule'],env);
  assert.equal(dry.delivery_attempted,false);
  assert.equal(requests.length,1);
  const sent=await run(['--rule','test-rule','--send','--destination','xyne_test'],env);
  assert.equal(sent.delivery_status,'delivered');
  assert.equal(sent.message_id,'message-1');
  const posts=requests.filter(r=>r.path.includes('/notify/'));
  assert.equal(posts.length,1);
  assert.equal(posts[0].path,'/alerts/chat/notify/xyne_test');
  assert.match(posts[0].body.text,/Measured value: 3/);
  assert.match(posts[0].body.text,/NOT A PRODUCTION ALARM/);
  assert.doesNotMatch(posts[0].body.text,/must-not-forward|local-secret|request_id|merchant_id|extra_error/);
});

test('no-data does not fabricate an alert or contact delivery',async t=>{
  const {requests,env}=await fixture(t,{noData:true});
  await assert.rejects(run(['--rule','test-rule','--send','--destination','xyne_test'],env),/no successful live observations/);
  assert.equal(requests.length,1);
});

test('HTTP 200 provider refusal is not a successful delivery and is not retried',async t=>{
  const {requests,env}=await fixture(t,{deliveryStatus:'refused'});
  await assert.rejects(run(['--rule','test-rule','--send','--destination','xyne_test'],env),/refused/);
  assert.equal(requests.filter(r=>r.path.includes('/notify/')).length,1);
});

test('remote application origins are rejected before any network request',async()=>{
  await assert.rejects(run(['--rule','test-rule'],{LOCAL_OBSERVABILITY_URL:'https://sandbox.example',LOCAL_OBSERVABILITY_API_KEY:'local-secret'}),/loopback/);
});
