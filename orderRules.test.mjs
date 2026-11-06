// Offline tests for orderRules.js: no network, no BP, no database.
// Orders are shaped like the API's order GET (5 Nov 2026), with made-up customers. Rules are the real copies
// in orderRules.rules.json.
//
// Run: node orderRules.test.mjs
import assert from 'node:assert';
import fs from 'node:fs';
import { evaluate, runRules, unsupported, pollOnce, watchReport, noteRuleName, WATCHED } from './orderRules.js';

const RULES = JSON.parse(fs.readFileSync(new URL('./orderRules.rules.json', import.meta.url), 'utf8')).rules;
const rule = (name) => RULES.find((r) => r.name === name);

const order = ({ id = 900001, status = 1, chan = 17, pay = 'PAID', alloc = 'AAA', ship = 104, country = 222, pc = 'LE3 8BR', skus = ['X1'], cf = {} } = {}) => ({
  id,
  orderStatus: { orderStatusId: status },
  orderPaymentStatus: pay,
  allocationStatusCode: alloc,
  stockStatusCode: 'SON',
  assignment: { current: { channelId: chan } },
  delivery: { shippingMethodId: ship },
  parties: { delivery: { countryId: country, postalCode: pc, addressLine1: '1 High St' } },
  orderRows: Object.fromEntries(skus.map((s, i) => [String(1000 + i), { productSku: s }])),
  customFields: cf,
  createdById: 4,
  invoices: [{ invoiceReference: '' }],
});

let passed = 0;
const test = async (name, fn) => { await fn(); passed++; console.log(`ok - ${name}`); };

await test('every watched rule is in our copy and fully supported', () => {
  for (const n of WATCHED) {
    assert.ok(rule(n), `${n} missing`);
    assert.deepEqual(unsupported(rule(n)), [], n);
  }
});

await test('Bob Custom Fields: Item Out of Stock without the flag -> would set it; with it -> done', () => {
  assert.deepEqual(evaluate(rule('Bob Custom Fields'), order({ status: 120 })),
    { verdict: 'would', changes: [{ field: 'cf:PCF_BOBSORTI', from: '', to: '1', label: 'PCF_BOBSORTI = 1' }] });
  assert.equal(evaluate(rule('Bob Custom Fields'), order({ status: 120, cf: { PCF_BOBSORTI: true } })).verdict, 'done');
  assert.equal(evaluate(rule('Bob Custom Fields'), order({ status: 2 })), null);
});

await test('eBay DPD: paid, allocated, New order, SHIPDPD -> Interlink + pick/pack/ship; done once moved on', () => {
  const ebay = (o) => order({ chan: Number(rule('Ebay Automation - DPD').conditions.find((c) => c['condition-type'] === 'channel').options.ids[0]), skus: ['SHIPDPD', 'ABC'], ...o });
  const newOrder = Number(rule('Ebay Automation - DPD').conditions.find((c) => c['condition-type'] === 'order-status').options.ids[0]);
  const r = evaluate(rule('Ebay Automation - DPD'), ebay({ status: newOrder }));
  assert.equal(r.verdict, 'would');
  assert.deepEqual(r.changes.map((c) => c.label), ['Interlink Next Day', 'In stock, pick/pack/ship']);
  assert.equal(evaluate(rule('Ebay Automation - DPD'), ebay({ status: newOrder, alloc: 'ANA' })), null, 'not allocated');
  assert.equal(evaluate(rule('Ebay Automation - DPD'), ebay({ status: 48, ship: 14 })).verdict, 'done', 'status condition skipped once the rule moved it');
});

await test('BT Postcode Catcher: BT postcodes on Interlink -> DPD Local; not BS or a BT-less postcode', () => {
  const bt = rule('BT Postcode Catcher');
  assert.equal(evaluate(bt, order({ ship: 14, pc: 'BT7 1NN' })).verdict, 'would');
  assert.equal(evaluate(bt, order({ ship: 14, pc: 'bt471aa' })).verdict, 'would', 'lower case, no space');
  assert.equal(evaluate(bt, order({ ship: 14, pc: 'BS7 1NN' })), null);
  assert.equal(evaluate(bt, order({ ship: 104, pc: 'BT7 1NN' })), null, 'not on Interlink');
});

await test('runRules: a later rule sees an earlier rule\'s change (BP priority order)', () => {
  const rules = [rule('Ebay Automation - DPD'), rule('BT Postcode Catcher')];
  const chan = Number(rules[0].conditions.find((c) => c['condition-type'] === 'channel').options.ids[0]);
  const newOrder = Number(rules[0].conditions.find((c) => c['condition-type'] === 'order-status').options.ids[0]);
  const out = runRules(order({ chan, status: newOrder, skus: ['SHIPDPD'], pc: 'BT1 1AA' }), rules);
  assert.deepEqual(out.map((r) => r.rule.name), ['Ebay Automation - DPD', 'BT Postcode Catcher']);
});

// A pretend Postgres that understands the few statements pollOnce/watchReport send.
const fakePool = () => {
  const rows = []; const state = {};
  return {
    rows,
    async query(sql, p = []) {
      if (/CREATE TABLE/.test(sql)) return { rows: [] };
      if (/SELECT v FROM order_rules_state/.test(sql)) return { rows: state.cursor ? [{ v: state.cursor }] : [] };
      if (/INSERT INTO order_rules_state/.test(sql)) { state.cursor = p[0]; return { rows: [] }; }
      if (/INSERT INTO order_rules_log/.test(sql)) {
        const hit = rows.find((r) => r.order_id === p[0] && r.rule_id === p[1] && r.verdict === p[3]);
        if (hit) hit.seen++; else rows.push({ order_id: p[0], rule_id: p[1], rule_name: p[2], verdict: p[3], changes: JSON.parse(p[4]), first_seen: new Date(Date.now() - 3600e3), seen: 1 });
        return { rows: [] };
      }
      if (/SELECT 1 FROM order_rules_log/.test(sql)) return { rows: rows.filter((r) => r.order_id === p[0] && r.rule_id === p[1] && r.verdict !== 'would') };
      if (/FROM order_rules_log w/.test(sql)) {
        return { rows: rows.map((w) => ({ ...w, other: rows.filter((o) => o.order_id === w.order_id && o.rule_id === w.rule_id && o.verdict !== w.verdict).map((o) => o.verdict) })) };
      }
      throw new Error('fake pool: ' + sql.slice(0, 60));
    },
  };
};
const search = (ids) => ({ metaData: { columns: [{ name: 'orderId' }], morePagesAvailable: false, resultsReturned: ids.length }, results: ids.map((i) => [i]) });
const notes = (list) => ({ metaData: { columns: [{ name: 'orderId' }, { name: 'noteText' }], morePagesAvailable: false, resultsReturned: list.length }, results: list });

await test('pollOnce: logs would, reads BP\'s rule notes, and never re-fires a rule on the same order', async () => {
  const pool = fakePool();
  let orders = [order({ id: 1, status: 120 }), order({ id: 2, status: 120, cf: { PCF_BOBSORTI: false } })];
  let newNotes = [];
  const oldNotes = { 2: [[2, "Automation rule 'Bob Custom Fields': Updating sales custom field PCF_BOBSORTI"]] };
  const calls = [];
  const get = async (p) => {
    calls.push(p.split('?')[0]);
    if (p.includes('/order-search')) return search(orders.map((o) => o.id));
    if (p.includes('order-note-search?dateAdded')) return notes(newNotes);
    if (p.includes('order-note-search?orderId=')) return notes(oldNotes[p.match(/orderId=(\d+)/)[1]] || []);
    return orders;
  };
  await pollOnce({ pool, get });
  // order 1: first time -> would. order 2: BP's note shows the rule already ran before someone set it back -> no would.
  assert.deepEqual(pool.rows.map((r) => `${r.order_id}:${r.verdict}`).sort(), ['1:would', '2:already']);

  // BP then does it on order 1 and leaves its note: done + bp, and the report counts it as agreed.
  orders = [order({ id: 1, status: 120, cf: { PCF_BOBSORTI: true } })];
  newNotes = [[1, "Automation rule 'Bob Custom Fields': Updating sales custom field PCF_BOBSORTI"]];
  await pollOnce({ pool, get });
  const rep = await watchReport(pool);
  assert.equal(rep.rules['Bob Custom Fields'].agreed, 1);
  assert.deepEqual(rep.rules['Bob Custom Fields'].oursOnly, []);

  // Someone sets it back to No: the rule has fired on this order, so no new would.
  orders = [order({ id: 1, status: 120, cf: { PCF_BOBSORTI: false } })];
  newNotes = [];
  await pollOnce({ pool, get });
  assert.equal(pool.rows.filter((r) => r.order_id === 1 && r.verdict === 'would').length, 1);
  assert.ok(!calls.some((c) => /\/order\/\d.*\/(row|note)$/.test(c)), 'read-only: nothing but GETs of search/order');
});

await test('noteRuleName reads both of the note formats BP writes, and nothing else', () => {
  assert.equal(noteRuleName("Automation rule 'Name Badges': Updating sales custom field PCF_BADGE"), 'Name Badges');
  assert.equal(noteRuleName('Updating shipping method to "DPD Local Two Day" by Automation with rule "BT Postcode Catcher"'), 'BT Postcode Catcher');
  assert.equal(noteRuleName('Updated status to "Stock needs ordering"<br />'), null);
  assert.equal(noteRuleName(null), null);
});

await test('a shipping rule BP ran before we watched and someone undid is not fired again (SO 501902)', async () => {
  const pool = fakePool();
  const o = order({ id: 7, ship: 14, pc: 'BT31 9ED' });
  const get = async (p) => (p.includes('/order-search') ? search([7])
    : p.includes('order-note-search?dateAdded') ? notes([])
      : p.includes('order-note-search?orderId=') ? notes([[7, 'Updating shipping method to "DPD Local Two Day" by Automation with rule "BT Postcode Catcher"']])
        : [o]);
  await pollOnce({ pool, get });
  assert.deepEqual(pool.rows.map((r) => r.verdict), ['already']);
  const rep = await watchReport(pool);
  assert.deepEqual([rep.rules['BT Postcode Catcher'].oursOnly, rep.rules['BT Postcode Catcher'].bpOnly], [[], []]);
});

await test('BP beats our poll on a shipping rule: its note plus our done counts as agreed; its note alone is bpOnly', async () => {
  const pool = fakePool();
  const bt = rule('BT Postcode Catcher');
  await pool.query('INSERT INTO order_rules_log', [8, bt.id, bt.name, 'bp', '[]']);
  await pool.query('INSERT INTO order_rules_log', [8, bt.id, bt.name, 'done', '[]']);
  await pool.query('INSERT INTO order_rules_log', [9, bt.id, bt.name, 'bp', '[]']);
  const s = (await watchReport(pool)).rules['BT Postcode Catcher'];
  assert.equal(s.agreed, 1);
  assert.deepEqual(s.bpOnly.map((o) => o.orderId), [9]);
});

await test('watchReport: a would that BP never followed is listed as oursOnly', async () => {
  const pool = fakePool();
  const get = async (p) => (p.includes('/order-search') ? search([5]) : p.includes('order-note-search') ? notes([]) : [order({ id: 5, status: 120 })]);
  await pollOnce({ pool, get });
  const rep = await watchReport(pool, { graceMin: 15 });
  assert.deepEqual(rep.rules['Bob Custom Fields'].oursOnly.map((o) => o.orderId), [5]);
});

console.log(`\n${passed} passed`);
