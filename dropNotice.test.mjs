// notifyDroppedLines — who gets told a line was left off the order, for EVERY supplier.
// Runs the real function against a fake pool, so the routing, the productId-vs-SKU matching,
// the demand_log fallback and the dedupe are all exercised for real.
import { notifyDroppedLines } from './purchasingSchedule.js';
import { extractBlockedLines } from './blockedLines.js';

let pass = true;
const check = (name, ok) => { console.log((ok ? 'PASS  ' : 'FAIL  ') + name); if (!ok) pass = false; };

// ── fakes ────────────────────────────────────────────────────────────────────
// demand: [{ po_id, so_id, product_id, sku }] — what a run recorded it was buying, for whom.
const makePool = (notified = [], demand = []) => ({
  rows: notified,
  async query(sql, args) {
    if (/CREATE TABLE/i.test(sql)) return { rows: [] };
    if (/FROM demand_log/i.test(sql)) return { rows: demand.filter((d) => String(d.po_id) === String(args[0])) };
    if (/^\s*SELECT/i.test(sql)) {
      const [soId, key] = args;
      return { rows: this.rows.filter((r) => String(r.so_id) === String(soId) && r.product_id === key) };
    }
    if (/INSERT/i.test(sql)) { this.rows.push({ so_id: args[0], product_id: args[1] }); return { rows: [] }; }
    return { rows: [] };
  },
});

// Fristads: our SO row code differs from the supplier's resolved code.
const FRISTADS_LBO = {
  489299: [{ productId: 9001, sku: 'CB170321004', qty: 1 }],
  489300: [{ productId: 9002, sku: 'CB170321005', qty: 2 }],
};
const FRISTADS_DROPPED = [{ productId: 9001, sku: '119627-271-407', qty: 1, size: 'Large', name: 'Airtech Coverall', avail: 0, deldate: '2026-10-01' }];

// Portwest: no productId on a dropped line, SKU matches directly.
const PORTWEST_LBO = { 487469: [{ sku: 'CD883DKR40', qty: 1 }] };
const PORTWEST_DROPPED = [{ sku: 'CD883DKR40', want: 1 }];

const run = (pool, opts) => notifyDroppedLines(pool, { execute: false, ...opts });
const hit = (r, soId) => JSON.stringify(r.detail || []).includes(String(soId));

// ── matching, when the run hands us the mapping ──────────────────────────────
let r = await run(makePool(), { supplier: 'FRISTADS', poId: 489329, dropped: FRISTADS_DROPPED, linesByOrder: FRISTADS_LBO });
check('a Fristads drop finds its SO by productId', hit(r, 489299));
check('…and does not implicate the other SO',      !hit(r, 489300));

r = await run(makePool(), { supplier: 'PORTWEST', poId: 489300, dropped: PORTWEST_DROPPED, linesByOrder: PORTWEST_LBO });
check('a Portwest drop finds its SO by SKU',       hit(r, 487469));

r = await run(makePool(), {
  supplier: 'FRISTADS', poId: 1, linesByOrder: FRISTADS_LBO,
  dropped: [{ productId: 7777, sku: 'CB170321004', qty: 1 }],
});
check('an unmatched productId never falls back to SKU', r.sent === 0 && /no customer lines/.test(r.reason || ''));

// ── the demand_log fallback: every OTHER supplier, which builds no mapping ───
const CHADWICK_DEMAND = [
  { po_id: 488281, so_id: 488100, product_id: 4001, sku: 'NEX-1234' },
  { po_id: 488281, so_id: 488101, product_id: 4002, sku: 'HER-9999' },
];
r = await run(makePool([], CHADWICK_DEMAND), {
  supplier: 'CHADWICK', poId: 488281, linesByOrder: {},
  dropped: [{ sku: 'NEX-1234', qty: 2, reason: 'the supplier rejected this item code outright' }],
});
check('a supplier with no mapping resolves via demand_log', hit(r, 488100));
check('…and only the SO that wanted that code',            !hit(r, 488101));

r = await run(makePool([], CHADWICK_DEMAND), {
  supplier: 'CHADWICK', poId: 488281, linesByOrder: {},
  dropped: [{ productId: 4002, sku: 'SOMETHING-ELSE', qty: 1 }],
});
check('demand_log matches on product_id when given one',   hit(r, 488101));

r = await run(makePool([], CHADWICK_DEMAND), {
  supplier: 'CHADWICK', poId: 999999, linesByOrder: {},
  dropped: [{ sku: 'NEX-1234', qty: 2 }],
});
check('a different PO matches nobody',                     r.sent === 0);

// ── placed vs not placed: two different messages ─────────────────────────────
r = await run(makePool([], CHADWICK_DEMAND), {
  supplier: 'CHADWICK', poId: 488281, linesByOrder: {}, placed: false,
  dropped: [{ sku: 'NEX-1234', qty: 2 }],
});
check('an abort is still routed to the right person',      hit(r, 488100));

// ── what must NEVER reach a salesperson ──────────────────────────────────────
// extractBlockedLines is the gate: a failure naming no item yields no lines, so no email.
const noise = [
  { supplier: 'SCRUFFS', step: 'unknown', message: 'auth failed: 401 from the portal', context: { poId: 1 } },
  { supplier: 'CHADWICK', step: 'checkout', message: 'Invalid Request Date', context: { poId: 1 } },
  { supplier: 'BLAKLADER', step: 'checkout', message: 'the storefront cart was empty', context: { poId: 1 } },
];
for (const row of noise) {
  check('no email for: ' + row.message.slice(0, 28), extractBlockedLines(row).length === 0);
}

// ── reorder lines (nobody waiting) ───────────────────────────────────────────
r = await run(makePool(), { supplier: 'FRISTADS', poId: 1, dropped: [{ productId: 5555, sku: 'X', qty: 9 }], linesByOrder: FRISTADS_LBO });
check('a pure stock reorder emails nobody',        r.sent === 0 && /no customer lines/.test(r.reason || ''));
r = await run(makePool(), { supplier: 'FRISTADS', poId: 1, dropped: [], linesByOrder: FRISTADS_LBO });
check('nothing dropped, nothing sent',             r.sent === 0 && /nothing dropped/.test(r.reason || ''));

// ── the dedupe ───────────────────────────────────────────────────────────────
r = await run(makePool([{ so_id: 489299, product_id: '9001' }]), { supplier: 'FRISTADS', poId: 489329, dropped: FRISTADS_DROPPED, linesByOrder: FRISTADS_LBO });
check('a line already notified is not re-sent',    r.sent === 0 && /already notified/.test(r.reason || ''));

r = await run(makePool([{ so_id: 487469, product_id: 'sku:CD883DKR40' }]), { supplier: 'PORTWEST', poId: 1, dropped: PORTWEST_DROPPED, linesByOrder: PORTWEST_LBO });
check('…and the SKU-keyed dedupe holds too',       r.sent === 0 && /already notified/.test(r.reason || ''));

const broken = { async query() { throw new Error('no such table'); } };
r = await run(broken, { supplier: 'PORTWEST', poId: 1, dropped: PORTWEST_DROPPED, linesByOrder: PORTWEST_LBO });
check('a broken dedupe tells them anyway',         hit(r, 487469));

// ── never throws ─────────────────────────────────────────────────────────────
for (const bad of [{}, { dropped: null }, { dropped: [{}], linesByOrder: null }, { dropped: [{ sku: null }], linesByOrder: PORTWEST_LBO }]) {
  try { await run(makePool(), bad); } catch (e) { check('survives ' + JSON.stringify(bad) + ': ' + e.message, false); }
}
check('survives malformed input', true);

console.log(pass ? '\nALL PASS' : '\nFAILURES');
process.exit(pass ? 0 : 1);
