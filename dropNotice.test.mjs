// notifyDroppedLines — who gets told a line was left off the order.
// Runs against the real function with a fake pool and a stubbed transport, so the routing,
// the productId-vs-SKU matching and the dedupe are all exercised for real.
import { notifyDroppedLines } from './purchasingSchedule.js';

let pass = true;
const check = (name, ok) => { console.log((ok ? 'PASS  ' : 'FAIL  ') + name); if (!ok) pass = false; };

// ── fakes ────────────────────────────────────────────────────────────────────
const makePool = (notified = []) => ({
  rows: notified,
  async query(sql, args) {
    if (/CREATE TABLE/i.test(sql)) return { rows: [] };
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

// ── the matching ─────────────────────────────────────────────────────────────
let r = await run(makePool(), { supplier: 'FRISTADS', poId: 489329, dropped: FRISTADS_DROPPED, linesByOrder: FRISTADS_LBO });
check('a Fristads drop finds its SO by productId', JSON.stringify(r.detail || []).includes('489299'));
check('…and does not implicate the other SO',      !JSON.stringify(r.detail || []).includes('489300'));

r = await run(makePool(), { supplier: 'PORTWEST', poId: 489300, dropped: PORTWEST_DROPPED, linesByOrder: PORTWEST_LBO });
check('a Portwest drop finds its SO by SKU',       JSON.stringify(r.detail || []).includes('487469'));

// A productId that matches nothing must NOT quietly fall back to the SKU and guess.
r = await run(makePool(), {
  supplier: 'FRISTADS', poId: 1, linesByOrder: FRISTADS_LBO,
  dropped: [{ productId: 7777, sku: 'CB170321004', qty: 1 }],
});
check('an unmatched productId never falls back to SKU', r.sent === 0 && /no customer lines/.test(r.reason || ''));

// ── reorder lines (nobody waiting) ───────────────────────────────────────────
r = await run(makePool(), { supplier: 'FRISTADS', poId: 1, dropped: [{ productId: 5555, sku: 'X', qty: 9 }], linesByOrder: FRISTADS_LBO });
check('a pure stock reorder emails nobody',        r.sent === 0 && /no customer lines/.test(r.reason || ''));
r = await run(makePool(), { supplier: 'FRISTADS', poId: 1, dropped: [], linesByOrder: FRISTADS_LBO });
check('nothing dropped, nothing sent',             r.sent === 0 && /nothing dropped/.test(r.reason || ''));

// ── the dedupe ───────────────────────────────────────────────────────────────
const seen = makePool([{ so_id: 489299, product_id: '9001' }]);
r = await run(seen, { supplier: 'FRISTADS', poId: 489329, dropped: FRISTADS_DROPPED, linesByOrder: FRISTADS_LBO });
check('a line already notified is not re-sent',    r.sent === 0 && /already notified/.test(r.reason || ''));

const seenPw = makePool([{ so_id: 487469, product_id: 'sku:CD883DKR40' }]);
r = await run(seenPw, { supplier: 'PORTWEST', poId: 1, dropped: PORTWEST_DROPPED, linesByOrder: PORTWEST_LBO });
check('…and the SKU-keyed dedupe holds too',       r.sent === 0 && /already notified/.test(r.reason || ''));

// A dedupe table that throws must not silence the notice.
const broken = { async query() { throw new Error('no such table'); } };
r = await run(broken, { supplier: 'PORTWEST', poId: 1, dropped: PORTWEST_DROPPED, linesByOrder: PORTWEST_LBO });
check('a broken dedupe tells them anyway',         JSON.stringify(r.detail || []).includes('487469'));

// ── never throws ─────────────────────────────────────────────────────────────
for (const bad of [{}, { dropped: null }, { dropped: [{}], linesByOrder: null }, { dropped: [{ sku: null }], linesByOrder: PORTWEST_LBO }]) {
  try { await run(makePool(), bad); } catch (e) { check('survives ' + JSON.stringify(bad) + ': ' + e.message, false); }
}
check('survives malformed input', true);

console.log(pass ? '\nALL PASS' : '\nFAILURES');
process.exit(pass ? 0 : 1);
