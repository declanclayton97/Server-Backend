// Live checks for bpScreen.js against a real BP order screen.
//
// Default target is the SANDBOX account (tuffbsitc, a copy of live), so nothing
// here can touch a real customer's order. On the live account it refuses to
// write unless the order is in status 110 "New Order - Test".
//
//   node bpScreen.live.mjs <orderId>                   read-only checks, sandbox
//   node bpScreen.live.mjs <orderId> --write           + change, verify, restore each action
//   node bpScreen.live.mjs <orderId> --client=tuffworkwear [--write]
//
// Login: bpWebSession's credentials for the client (sandbox: DEC_USER/DEC_PASS
// or BP_WEB_TEST_EMAIL/BP_WEB_TEST_PASSWORD; live: BP_WEB_EMAIL/BP_WEB_PASSWORD).
// Env is read from ./.env, then C:/Users/DeclanClayton/Downloads/Server-Backend.env.
import fs from 'node:fs';
import { readOrder, updateOrder, webTransport } from './bpScreen.js';

for (const f of ['./.env', 'C:/Users/DeclanClayton/Downloads/Server-Backend.env']) {
  if (!fs.existsSync(f)) continue;
  for (const l of fs.readFileSync(f, 'utf8').split(/\r?\n/)) {
    const m = l.match(/^([A-Z0-9_]+)=(.*)$/);
    if (m && !process.env[m[1]]) process.env[m[1]] = m[2].replace(/^"|"$/g, '');
  }
}

const args = process.argv.slice(2);
const orderId = args.find((a) => /^\d+$/.test(a));
const write = args.includes('--write');
const client = (args.find((a) => a.startsWith('--client=')) || '--client=tuffbsitc').split('=')[1];
const TEST_STATUS = '110';
if (!orderId) { console.error('usage: node bpScreen.live.mjs <orderId> [--write] [--client=tuffbsitc]'); process.exit(2); }

const transport = await webTransport(client);
// The order's rows' VAT comes from the API (the screen's raw HTML reads 0.00 - see bpScreen.js). Only the live
// account's API token is held here; on the sandbox the writes refuse for want of it.
const getApiOrder = client === process.env.BRIGHTPEARL_ACCOUNT_ID && process.env.BRIGHTPEARL_API_TOKEN
  ? async (id) => {
    const host = `https://${process.env.BRIGHTPEARL_DATACENTER || 'euw1'}.brightpearlconnect.com`;
    const r = await fetch(`${host}/public-api/${client}/order-service/order/${id}`, {
      headers: { 'brightpearl-app-ref': process.env.BRIGHTPEARL_APP_REF, 'brightpearl-account-token': process.env.BRIGHTPEARL_API_TOKEN },
    });
    if (!r.ok) throw new Error(`API order ${id} answered ${r.status}`);
    return (await r.json()).response[0];
  }
  : null;
const results = [];
const check = (name, ok, detail = '') => { results.push({ name, ok }); console.log(`${ok ? 'ok  ' : 'FAIL'} - ${name}${detail ? `  (${detail})` : ''}`); };

// --- read-only -------------------------------------------------------------
const t0 = Date.now();
const page = await readOrder(orderId, { transport, getApiOrder });
check('order screen reads (signed in, form found)', page.orderId === orderId, `order ${page.orderId}`);
check('status picker found', Array.isArray(page.statuses) && page.statuses.length > 10, `${page.statuses?.length ?? 0} statuses`);
check('shipping methods found', (page.selects.ship_method || []).length > 10, `${(page.selects.ship_method || []).length} methods, current ${page.shipMethodId}`);
for (const f of ['PCF_SUPPLIER', 'PCF_BOBSORTI', 'PCF_ASDAAREA', 'PCF_PRINTSNE', 'PCF_ACCOUNTS', 'PCF_BADGE']) {
  check(`custom field ${f} on the screen`, f in page.fieldKinds, page.fieldKinds[f] || 'missing');
}
check('CSRF token found', !!page.csrf);
check('notify-customer box is not ticked by default', !page.notifyCustomerTicked);
const again = await readOrder(orderId, { transport, getApiOrder });
check('two reads give the same form (nothing random to resubmit)',
  JSON.stringify(again.pairs.filter(([n]) => n !== '__fc_csrf_token')) === JSON.stringify(page.pairs.filter(([n]) => n !== '__fc_csrf_token')),
  `${page.pairs.length} fields`);

const n = 10, tr = Date.now();
for (let i = 0; i < n; i++) await transport.get(`${transport.host}/patt-op.php?scode=invoice&oID=${orderId}`);
const per = (Date.now() - tr) / n;
check('screen speed', per < 3000, `${Math.round(per)} ms a load, about ${Math.round(60000 / per)} loads a minute`);

// --- writes: change, prove, put back --------------------------------------
if (write) {
  if (client !== 'tuffbsitc' && page.statusId !== TEST_STATUS) {
    console.error(`\nRefusing to write: ${client} is not the sandbox and order ${orderId} is in status ${page.statusId}, not ${TEST_STATUS} (New Order - Test).`);
    process.exit(1);
  }
  const otherOption = (list, current) => (list || []).find((o) => o.value !== '' && String(o.value) !== String(current))?.value;
  const original = { statusId: page.statusId, shipMethodId: page.shipMethodId, customFields: { ...page.customFields } };

  const step = async (name, changes, restore) => {
    const r = await updateOrder(orderId, changes, { transport, getApiOrder });
    check(`${name}: saved and read back`, r.ok, r.ok ? r.diff.map((d) => `${d.field} ${d.from} -> ${d.to}`).join(', ') : JSON.stringify(r.misses));
    const again2 = await updateOrder(orderId, changes, { transport, getApiOrder });
    check(`${name}: repeating it posts nothing`, again2.skipped === 'already set');
    const back = await updateOrder(orderId, restore, { transport, getApiOrder });
    check(`${name}: put back`, back.ok, back.skipped || '');
  };

  await step('text field PCF_SUPPLIER', { customFields: { PCF_SUPPLIER: 'SCREEN TEST' } }, { customFields: { PCF_SUPPLIER: original.customFields.PCF_SUPPLIER } });
  const bob = original.customFields.PCF_BOBSORTI;
  await step('yes/no field PCF_BOBSORTI', { customFields: { PCF_BOBSORTI: bob === true ? false : true } }, { customFields: { PCF_BOBSORTI: bob } });
  const area = otherOption(page.selects.PCF_ASDAAREA, original.customFields.PCF_ASDAAREA);
  if (area) await step('list field PCF_ASDAAREA', { customFields: { PCF_ASDAAREA: area } }, { customFields: { PCF_ASDAAREA: original.customFields.PCF_ASDAAREA } });
  const ship = otherOption(page.selects.ship_method, original.shipMethodId);
  await step('shipping method', { shipMethodId: ship }, { shipMethodId: original.shipMethodId });
  const status = client === 'tuffbsitc' ? otherOption(page.statuses, original.statusId) : '119';
  await step('status', { statusId: status }, { statusId: original.statusId });

  const stamp = `Screen test note ${new Date().toISOString().slice(0, 16)}`;
  await updateOrder(orderId, { note: stamp }, { transport, getApiOrder });
  const html = await transport.get(`${transport.host}/patt-op.php?scode=invoice&oID=${orderId}`);
  check('order note added', html.includes(stamp), stamp);
}

const failed = results.filter((r) => !r.ok).length;
console.log(`\n${results.length - failed} passed, ${failed} failed  (${client}, order ${orderId}, ${Math.round((Date.now() - t0) / 1000)}s)`);
process.exit(failed ? 1 : 0);
