// Offline test for the Fristads out-of-stock pre-flight.
//
// Why this exists: fristadsAddToBasket groups the cart lines by article+colour and sends ONE
// order-form POST per group, so a single out-of-stock size makes Fristads reject the whole POST
// and takes every other size of that garment with it. PO 488528 (2026-09-11) lost a Large and an
// XL with 123 and 67 on the shelf because the Medium was on zero — 3 of 6 units reached the basket
// and nothing was placed at all.
//
// A local stub stands in for Alt-Items so this asserts the REQUEST as well as the parse: the whole
// guard is worthless if the query params are wrong, and a response-only stub would pass against a
// request that the portal could never answer.
import http from 'node:http';
import { fristadsAvailable, partitionFristadsLines } from './purchasingSchedule.js';

const seen = [];
const server = http.createServer((req, res) => {
  seen.push(req.url);
  const u = new URL(req.url, 'http://x');
  const sku = u.searchParams.get('sku');
  res.setHeader('content-type', 'application/json');
  if (sku === 'BOOM') { res.statusCode = 500; return res.end('nope'); }
  if (sku === 'JUNK') return res.end('not json at all');
  const by = {
    '125949-171-406': { found: true, avail: 0, status: null, deldate: '2026-10-01' },   // the Medium that sank the group
    '125949-171-407': { found: true, avail: 123, status: null, deldate: null },
    '125949-171-408': { found: true, avail: 67, status: null, deldate: '2026-10-01' },
    'NO-AVAIL': { found: true, avail: null, status: null, deldate: null },
    'GONE': { found: false },
  };
  res.end(JSON.stringify(by[sku] || { found: false }));
});
await new Promise((r) => server.listen(0, r));
const base = `http://127.0.0.1:${server.address().port}`;
let pass = true;
const check = (name, cond) => { console.log((cond ? 'PASS  ' : 'FAIL  ') + name); if (!cond) pass = false; };

// ── the probe ──────────────────────────────────────────────────────────────
const m = await fristadsAvailable(base, { sku: '125949-171-406', size: 'Medium', name: 'Fristads FLAME HIGH VIS COVERALL' });
check('out-of-stock size reports avail 0', !!m && m.avail === 0);
check('and carries the delivery date',     !!m && m.deldate === '2026-10-01');

const l = await fristadsAvailable(base, { sku: '125949-171-407', size: 'Large', name: 'Fristads FLAME HIGH VIS COVERALL' });
check('in-stock size reports its count',   !!l && l.avail === 123);

// The request must carry all three — the portal matches on the size TEXT, and sku alone answers
// for the wrong variant (or nothing at all).
const u0 = seen[0] || '';
check('request carries sku',  u0.includes('sku=125949-171-406'));
check('request carries size', u0.includes('size=Medium'));
check('request carries name', /name=Fristads/.test(u0));

// ── fail-open: anything we cannot get a trustworthy answer for must NOT be dropped ──
check('HTTP error  → null', (await fristadsAvailable(base, { sku: 'BOOM', size: 'M' })) === null);
check('bad JSON    → null', (await fristadsAvailable(base, { sku: 'JUNK', size: 'M' })) === null);
check('not found   → null', (await fristadsAvailable(base, { sku: 'GONE', size: 'M' })) === null);
check('null avail  → null', (await fristadsAvailable(base, { sku: 'NO-AVAIL', size: 'M' })) === null);

// ── the partition rule ─────────────────────────────────────────────────────
const lines = [
  { sku: '100032-940-999', size: 'One Size', qty: 1 },
  { sku: '125949-171-406', size: 'Medium', qty: 1 },
  { sku: '125949-171-407', size: 'Large', qty: 1 },
  { sku: '125949-171-408', size: 'XL', qty: 1 },
];
const stock = new Map([
  ['125949-171-406', { avail: 0, deldate: '2026-10-01' }],
  ['125949-171-407', { avail: 123, deldate: null }],
  ['125949-171-408', { avail: 67, deldate: '2026-10-01' }],
  // 100032 deliberately absent — the probe failed for it
]);
const p = partitionFristadsLines(lines, stock);
check('only the zero-stock size is held back', p.short.length === 1 && p.short[0].sku === '125949-171-406');
check('the held-back line keeps its delivery date', p.short[0].deldate === '2026-10-01');
check('the other two sizes of the SAME garment still go', p.orderable.some((x) => x.sku === '125949-171-407') && p.orderable.some((x) => x.sku === '125949-171-408'));
check('an unprobed line is kept, never dropped', p.orderable.some((x) => x.sku === '100032-940-999'));

// Partial stock counts as short: asking for 5 when they hold 2 refuses the group just the same.
const p2 = partitionFristadsLines([{ sku: 'A', qty: 5 }, { sku: 'B', qty: 2 }], new Map([['A', { avail: 2 }], ['B', { avail: 2 }]]));
check('qty above availability is short', p2.short.length === 1 && p2.short[0].sku === 'A');
check('qty exactly equal to availability is orderable', p2.orderable.length === 1 && p2.orderable[0].sku === 'B');

// Nothing probed at all (Alt-Items down) must leave the order exactly as it was.
const p3 = partitionFristadsLines(lines, new Map());
check('no stock data at all → nothing dropped', p3.short.length === 0 && p3.orderable.length === 4);

server.close();
console.log(pass ? '\nALL PASS' : '\nFAILURES');
process.exit(pass ? 0 : 1);
