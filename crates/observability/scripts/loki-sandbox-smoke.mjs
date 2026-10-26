#!/usr/bin/env node
import fs from 'node:fs';

// One-shot local application check. Network data sources/destinations are configured in the app.
export async function run(args = process.argv.slice(2), env = process.env) {
  const value = key => { const i = args.indexOf(key); return i < 0 ? undefined : args[i + 1]; };
  const origin = new URL(env.LOCAL_OBSERVABILITY_URL ?? 'http://127.0.0.1:8085');
  if (origin.protocol !== 'http:' || !['127.0.0.1', 'localhost', '[::1]'].includes(origin.hostname) || origin.username || origin.password || origin.pathname !== '/') {
    throw Error('Smoke test only accepts a loopback HTTP application origin');
  }
  const key = env.LOCAL_OBSERVABILITY_API_KEY;
  if (!key) throw Error('Set LOCAL_OBSERVABILITY_API_KEY without putting credentials in argv');
  const ruleId = value('--rule');
  if (!ruleId) throw Error('Select one configured application rule with --rule ID');
  const send = args.includes('--send');
  const destination = value('--destination');
  if (send && (!destination || !/^[a-z0-9_]+$/.test(destination))) throw Error('--send requires one explicit configured --destination ID');
  async function request(path, payload) {
    const r = await fetch(new URL(path, origin), { method:payload ? 'POST' : 'GET', headers:{'X-Internal-Api-Key':key,...(payload ? {'Content-Type':'application/json'} : {})},body:payload ? JSON.stringify(payload) : undefined,redirect:'error',signal:AbortSignal.timeout(60000)});
    if (!r.ok) throw Error('Local application request failed: HTTP '+r.status);
    return r.json();
  }
  const preview = await request('/alerts/grafana/loki/evaluate');
  const rule = preview.rules?.find(r => r.id === ruleId);
  if (!rule || rule.status !== 'evaluated' || rule.no_data || !rule.instances?.length) throw Error('Selected rule has no successful live observations; nothing sent');
  const instance = rule.instances.find(i => Number.isFinite(i.value));
  if (!instance) throw Error('Selected rule has no measured value; nothing sent');
  const labelKeys = ['app','source_namespace','namespace','flow','method','request_method','status_code','extra_status_code'];
  const safeLabels = Object.fromEntries(Object.entries(instance.labels ?? {}).filter(([k]) => labelKeys.includes(k)).map(([k,v]) => [k,String(v).replace(/[\r\n\t]/g,' ').slice(0,160)]));
  const result = {rule_id:ruleId,evaluated_at:preview.evaluated_at,instance_count:rule.instances.length,value:instance.value,level:instance.level,delivery_attempted:false};
  if (send) {
    const text = ['SANDBOX E2E SNAPSHOT — NOT A PRODUCTION ALARM','Rule: '+ruleId,'Evaluated: '+preview.evaluated_at,'Measured value: '+instance.value,'Observed level: '+instance.level,...Object.entries(safeLabels).map(([k,v])=>k+': '+v)].join('\n');
    const reply = await request('/alerts/chat/notify/'+encodeURIComponent(destination), {text});
    // A provider refusal can be an HTTP 200; never report it as delivery success.
    if (reply.status !== 'delivered') throw Error('Destination refused the snapshot; delivery is not verified');
    result.delivery_attempted = true;
    result.delivery_status = reply.status;
    result.message_id = reply.message_id ?? null;
  }
  return result;
}

if (process.argv[1] && fs.realpathSync(process.argv[1]) === fs.realpathSync(new URL(import.meta.url))) {
  run().then(result => console.log(JSON.stringify(result,null,2))).catch(() => { console.error('Smoke check failed; no success is claimed. Check access, rule data and configured destination.');process.exitCode=1; });
}
