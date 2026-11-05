// Offline tests for bpScreen.js: no network, no BP.
// The fixture copies the shape of a real order screen (SO 497759, 26 Oct 2026):
// field names, types and quoting as BP serves them, with made-up customer details.
//
// Run: node bpScreen.test.mjs
import assert from 'node:assert';
import { parseOrderPage, planSave, verifySaved, updateOrder, attrs, withRowTax, rowChanges } from './bpScreen.js';

const screen = ({ status = 47, ship = 104, supplier = '', bob = null, asda = '', notify = false, carhartt = '498257' } = {}) => {
  const radio = (name, v) => ['1', '-1', '0'].map((x) => `<input type="radio" name="${name}" id="${x === '1' ? 'yes' : x === '-1' ? 'no' : 'any'}_${name}" value="${x}"${(v === true && x === '1') || (v === false && x === '-1') || (v === null && x === '0') ? ' checked="checked"' : ''}>`).join('');
  return `<!doctype html><html><head>
<meta name="__fc_csrf_token" content="tok-abc123">
<title>Sales Order#900001</title></head><body>
<form id="global-search" method="get" action="//euw1.brightpearlapp.com/report.php"><input type="hidden" name="report_type" value="sales"></form>
<select id="titleOrderStatus" class="status-picker">
  <option value="2">Pending Magento Orders</option>
  <option value="23">Stock needs ordering</option>
  <option value="47"${status === 47 ? ' selected' : ''}>Invoiced and Completed</option>
  <option value="110"${status === 110 ? ' selected' : ''}>New Order - Test</option>
</select>
<form name="form" id="form" method="post">
<input type="hidden" name="oID" id="order-id-input" value="900001">
<input type="hidden" name="orders_status" id="orders_status" value="${status}">
<input type="text" name="orders_customer_ref" id="orders_customer_ref" value="REF &amp; CO &quot;1&quot;">
<select name="department_id" id="department_id"><option value="17" selected>Magento TuffShop.co.uk</option></select>
<select name="owner_id" id="owner_id"><option value="0">Not set</option><option value="12">Someone</option></select>
<input type="hidden" name="orders_products_id[]" value="2384744"><input type="hidden" name="qty[]" value="1"><input type="hidden" name="listPrice[]" value="18.2900"><input type="text" name="itemnet[]" value="15.2400"><select name="tax_code[]"><option value="1" selected>T20</option><option value="3">T0</option></select><input type="text" name="rwTotalTaxUser[]" value="0.00">
<input type="hidden" name="orders_products_id[]" value="2384747"><input type="hidden" name="qty[]" value="1.0000"><input type="hidden" name="listPrice[]" value="0.0000"><input type="text" name="itemnet[]" value="0.0000"><select name="tax_code[]"><option value="1" selected>T20</option><option value="3">T0</option></select><input type="text" name="rwTotalTaxUser[]" value="0.00">
<select name="ship_method" id="ship_method">
  <option value="98">DPD Local Two Day</option>
  <option value="104"${ship === 104 ? ' selected="selected"' : ''}>Royal Mail Tracked 24</option>
  <option value="120"${ship === 120 ? ' selected="selected"' : ''}>Fedex Priority - Pak</option>
</select>
<input type="text" name="customer_name" id="customer_name" value="Test Person">
<input type="text" name="customer_postcode" id="customer_postcode" value="LS1 1AA">
${radio('PCF_RTRNAPRV', null)}
<input type="text" name="PCF_SUPPLIER" id="PCF_SUPPLIER" value="${supplier}">
<input type="text" name="PCF_CARHARTT" id="PCF_CARHARTT" value='${carhartt}'>
<textarea name="PCF_XPICKINF" id="PCF_XPICKINF">line one
line two &lt;b&gt;</textarea>
<select name="PCF_ASDAAREA" id="PCF_ASDAAREA"><option value="">--</option><option value="1"${asda === '1' ? ' selected' : ''}>Area 1</option><option value="6"${asda === '6' ? ' selected' : ''}>Area 6</option></select>
${radio('PCF_BOBSORTI', bob)}
<input type="text" name="PCF_URGENT" id="PCF_URGENT" value="00/00/0000">
<input type="text" name="locked_field" value="x" disabled>
<input type="submit" name="save" value="Save changes">
<textarea name="note_text" id="note_text"></textarea>
<input type="checkbox" name="notify_customer" id="notify_customer"${notify ? ' checked' : ''}>
<input type="hidden" name="__fc_csrf_token" value="">
</form>
<form id="submit-to-url" method="post"><input type="hidden" name="__fc_csrf_token" value=""></form>
</body></html>`;
};

// The same order as the API returns it: the rows' real VAT (the screen's raw HTML says 0.00).
const apiOrder = (tax1 = '3.05', net1 = '15.24') => ({ orderRows: {
  2384744: { rowValue: { taxClassId: 1, rowNet: { value: net1 }, rowTax: { value: tax1 } } },
  2384747: { rowValue: { taxClassId: 1, rowNet: { value: '0.00' }, rowTax: { value: '0.00' } } },
} });
const API = apiOrder();

let passed = 0;
const test = async (name, fn) => { await fn(); passed++; console.log(`ok - ${name}`); };

await test('attribute parser takes double, single, unquoted and bare attributes', () => {
  const a = attrs(`<input type=text name='x' value="a b" checked disabled>`);
  assert.deepEqual(a, { type: 'text', name: 'x', value: 'a b', checked: '', disabled: '' });
});

await test('reads status, shipping method and the custom fields', () => {
  const p = parseOrderPage(screen({ supplier: 'SNICKERS', bob: true, asda: '6' }));
  assert.equal(p.orderId, '900001');
  assert.equal(p.statusId, '47');
  assert.equal(p.shipMethodId, '104');
  assert.equal(p.customFields.PCF_SUPPLIER, 'SNICKERS');
  assert.equal(p.customFields.PCF_CARHARTT, '498257', 'single-quoted value');
  assert.equal(p.customFields.PCF_BOBSORTI, true);
  assert.equal(p.customFields.PCF_RTRNAPRV, null, 'the "not set" radio reads as null');
  assert.equal(p.customFields.PCF_ASDAAREA, '6');
  assert.equal(p.customFields.PCF_XPICKINF, 'line one\nline two <b>');
  assert.equal(p.csrf, 'tok-abc123');
  assert.deepEqual(p.statuses.map((s) => s.value), ['2', '23', '47', '110']);
});

await test('ignores every form but the order form', () => {
  const p = parseOrderPage(screen());
  assert.ok(!p.pairs.some(([n]) => n === 'report_type'));
});

await test('posts what a browser posts: rows kept in order, no disabled/submit/unticked controls', () => {
  const p = parseOrderPage(screen());
  const names = p.pairs.map(([n]) => n);
  assert.deepEqual(p.pairs.filter(([n]) => n === 'orders_products_id[]').map(([, v]) => v), ['2384744', '2384747']);
  assert.deepEqual(p.pairs.filter(([n]) => n === 'listPrice[]').map(([, v]) => v), ['18.2900', '0.0000']);
  assert.ok(!names.includes('locked_field'));
  assert.ok(!names.includes('save'));
  assert.equal(p.pairs.find(([n]) => n === 'owner_id')[1], '0', 'a select with nothing selected sends its first option');
  assert.equal(p.pairs.find(([n]) => n === 'orders_customer_ref')[1], 'REF & CO "1"', 'entities decoded once');
  assert.equal(names.filter((n) => n === 'PCF_RTRNAPRV').length, 1, 'one radio per group');
});

await test('never sends notify_customer, even when the box is ticked on the screen', () => {
  const p = parseOrderPage(screen({ notify: true }));
  assert.equal(p.notifyCustomerTicked, true);
  const { pairs } = planSave(p, { customFields: { PCF_SUPPLIER: 'STERLING' } }, { apiOrder: API });
  assert.ok(!pairs.some(([n]) => n === 'notify_customer'));
});

await test('changes only the named fields and fills the CSRF token from the meta tag', () => {
  const p = parseOrderPage(screen());
  const { pairs, diff } = planSave(p, { statusId: 23, shipMethodId: 120, customFields: { PCF_SUPPLIER: 'STERLING', PCF_BOBSORTI: true, PCF_ASDAAREA: '1' } }, { apiOrder: API });
  const get = (n) => pairs.find(([k]) => k === n)?.[1];
  assert.equal(get('orders_status'), '23');
  assert.equal(get('ship_method'), '120');
  assert.equal(get('PCF_SUPPLIER'), 'STERLING');
  assert.equal(get('PCF_BOBSORTI'), '1');
  assert.equal(get('PCF_ASDAAREA'), '1');
  assert.equal(get('__fc_csrf_token'), 'tok-abc123');
  assert.equal(get('customer_postcode'), 'LS1 1AA', 'untouched fields go back as they were');
  assert.deepEqual(diff.map((d) => d.field).sort(), ['PCF_ASDAAREA', 'PCF_BOBSORTI', 'PCF_SUPPLIER', 'orders_status', 'ship_method']);
  const before = parseOrderPage(screen()).pairs.length;
  assert.equal(pairs.length, before - 0, 'no fields added or lost');
});

await test('a yes/no field can be cleared back to "not set"', () => {
  const p = parseOrderPage(screen({ bob: true }));
  const { pairs } = planSave(p, { customFields: { PCF_BOBSORTI: null } }, { apiOrder: API });
  assert.equal(pairs.find(([k]) => k === 'PCF_BOBSORTI')[1], '0');
  assert.throws(() => planSave(p, { customFields: { PCF_BOBSORTI: 'yes' } }), /yes\/no/);
});

await test('refuses a status, shipping method or select value the screen does not offer', () => {
  const p = parseOrderPage(screen());
  assert.throws(() => planSave(p, { statusId: 999 }), /status 999 is not offered/);
  assert.throws(() => planSave(p, { shipMethodId: 7 }), /shipping method 7 is not offered/);
  assert.throws(() => planSave(p, { customFields: { PCF_ASDAAREA: '9' } }), /not offered/);
});

await test('refuses a custom field that is not on the screen, instead of adding it', () => {
  const p = parseOrderPage(screen());
  assert.throws(() => planSave(p, { customFields: { PCF_NOPE: 'x' } }), /not on this order screen/);
});

await test('a note goes in note_text and must be plain ASCII', () => {
  const p = parseOrderPage(screen());
  const { pairs } = planSave(p, { note: 'Automation: single-item rule set Stock needs ordering' }, { apiOrder: API });
  assert.equal(pairs.find(([k]) => k === 'note_text')[1], 'Automation: single-item rule set Stock needs ordering');
  assert.throws(() => planSave(p, { note: 'cost £12' }), /plain ASCII/);
});

await test('verifySaved names every change that did not stick', () => {
  const after = parseOrderPage(screen({ status: 23, supplier: 'STERLING, ' }));
  const misses = verifySaved(after, { statusId: 23, shipMethodId: 120, customFields: { PCF_SUPPLIER: 'STERLING' } });
  assert.deepEqual(misses.map((m) => m.field), ['ship_method', 'PCF_SUPPLIER']);
  assert.deepEqual(verifySaved(null, {}), [{ field: '(page)', want: 'order screen', got: 'no order form' }]);
});

// A fake BP: serves the screen, applies a post the way BP would, counts calls.
const fakeBp = (initial, { dropField, rerate } = {}) => {
  let state = { ...initial };
  let api = apiOrder();
  const calls = { get: 0, post: 0, api: 0, lastPairs: null };
  return {
    calls,
    async getApiOrder() { calls.api++; return JSON.parse(JSON.stringify(api)); },
    transport: {
      host: 'https://bp.test',
      async get() { calls.get++; return screen(state); },
      async post(url, pairs) {
        calls.post++; calls.lastPairs = pairs;
        const v = (n) => pairs.find(([k]) => k === n)?.[1];
        const next = { ...state, status: Number(v('orders_status')), ship: Number(v('ship_method')), supplier: v('PCF_SUPPLIER'), asda: v('PCF_ASDAAREA'), bob: { '1': true, '-1': false, '0': null }[v('PCF_BOBSORTI')] };
        if (dropField) next[dropField] = state[dropField];
        state = next;
        // BP saves the posted VAT as the row's VAT (that is how 0.00 zeroed 18 orders)
        const vat = pairs.filter(([k]) => k === 'rwTotalTaxUser[]').map(([, x]) => x);
        api = apiOrder(rerate ? '0.00' : vat[0]);
        return 302;
      },
    },
  };
};

await test('updateOrder posts once, then re-reads and confirms', async () => {
  const bp = fakeBp({ status: 2, ship: 104 });
  const r = await updateOrder('900001', { statusId: 23, customFields: { PCF_SUPPLIER: 'SNICKERS' } }, { transport: bp.transport, getApiOrder: bp.getApiOrder });
  assert.equal(r.ok, true);
  assert.deepEqual(r.misses, []);
  assert.equal(bp.calls.post, 1);
  assert.equal(bp.calls.get, 2, 'read before, read after');
  assert.equal(bp.calls.api, 2, 'API read before (the VAT) and after (rows unchanged)');
  assert.equal(bp.calls.lastPairs.find(([k]) => k === 'rwTotalTaxUser[]')[1], '3.05', 'the row VAT was put back, not posted as 0.00');
});

await test('updateOrder does nothing when the order is already in that state (repeat webhook)', async () => {
  const bp = fakeBp({ status: 23, supplier: 'SNICKERS' });
  const r = await updateOrder('900001', { statusId: 23, customFields: { PCF_SUPPLIER: 'SNICKERS' } }, { transport: bp.transport, getApiOrder: bp.getApiOrder });
  assert.equal(r.skipped, 'already set');
  assert.equal(bp.calls.post, 0);
});

await test('updateOrder reports a save that BP silently ignored', async () => {
  const bp = fakeBp({ status: 2 }, { dropField: 'supplier' });
  const r = await updateOrder('900001', { statusId: 23, customFields: { PCF_SUPPLIER: 'SNICKERS' } }, { transport: bp.transport, getApiOrder: bp.getApiOrder });
  assert.equal(r.ok, false);
  assert.deepEqual(r.misses.map((m) => m.field), ['PCF_SUPPLIER']);
});

await test('dry run plans the change without posting', async () => {
  const bp = fakeBp({ status: 2 });
  const r = await updateOrder('900001', { statusId: 23 }, { transport: bp.transport, dryRun: true });
  assert.equal(r.dryRun, true);
  assert.deepEqual(r.diff, [{ field: 'orders_status', from: '2', to: '23' }]);
  assert.equal(bp.calls.post, 0);
});

await test('a signed-out screen is an error, not an empty order', async () => {
  const transport = { host: 'https://bp.test', get: async () => '<html><title>Brightpearl - Login</title></html>', post: async () => { throw new Error('should not post'); } };
  await assert.rejects(updateOrder('900001', { statusId: 23 }, { transport }), /no order form/);
});

await test("the order form is the one holding oID - not BP's global search form before it (3 Nov)", () => {
  const html = `<html><form id="global-search" data-x="id=form" method="get"><input name="search" value=""><input name="report_type" value="x"></form>
    <form id="form" class="invoice-form" method="post" name="form"><input type="hidden" id="order-id-input" name="oID" value="498798">
    <input name="orders_customer_ref" value="REF"></form><form id="submit-to-url" method="post"></form></html>`;
  const page = parseOrderPage(html);
  assert.equal(page.orderId, '498798');
  assert.equal(page.pairs.some((p) => p[0] === 'search'), false);
  assert.equal(parseOrderPage("<form id='global-search'><input name='search'></form>"), null);
});

await test('a form with rows is never planned for posting without the API order (raw VAT reads 0.00)', () => {
  const p = parseOrderPage(screen());
  assert.equal(p.pairs.find(([k]) => k === 'rwTotalTaxUser[]')[1], '0.00');
  assert.throws(() => planSave(p, { statusId: 23 }), /zeroes it, refusing/);
  assert.ok(planSave(p, { statusId: 23 }, { planOnly: true }).diff.length, 'planOnly still gives the diff');
});

await test("withRowTax puts each row's VAT back and refuses rows that do not match the API", () => {
  const pairs = parseOrderPage(screen()).pairs;
  assert.deepEqual(withRowTax(pairs, API).filter(([k]) => k === 'rwTotalTaxUser[]').map(([, v]) => v), ['3.05', '0.00']);
  assert.throws(() => withRowTax(pairs, apiOrder('3.05', '20.00')), /net 15.24 isn't the API's 20.00/);
  const extra = apiOrder(); extra.orderRows[1] = { rowValue: { taxClassId: 1, rowNet: { value: '1' }, rowTax: { value: '0.2' } } };
  assert.throws(() => withRowTax(pairs, extra), /in the API but not on the order screen/);
  const t0 = apiOrder(); t0.orderRows[2384744].rowValue.taxClassId = 3;
  assert.throws(() => withRowTax(pairs, t0), /VAT code 1 isn't the API's 3/);
});

await test('updateOrder refuses an order with rows when it has no API reader, before posting', async () => {
  const bp = fakeBp({ status: 2 });
  await assert.rejects(updateOrder('900001', { statusId: 23 }, { transport: bp.transport }), /needs getApiOrder/);
  assert.equal(bp.calls.post, 0);
});

await test('updateOrder reports rows whose VAT changed in the save', async () => {
  const bp = fakeBp({ status: 2 }, { rerate: true });
  const r = await updateOrder('900001', { statusId: 23 }, { transport: bp.transport, getApiOrder: bp.getApiOrder });
  assert.equal(r.ok, false);
  assert.match(r.misses.find((m) => m.field === '(order rows)').got, /row 2384744: net 15.24 -> 15.24, VAT 3.05 -> 0.00/);
});

await test('rowChanges is empty for identical reads', () => assert.deepEqual(rowChanges(API, apiOrder()), []));

console.log(`\n${passed} passed`);
