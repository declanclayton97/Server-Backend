// Supplier lead times, measured: purchase order raised -> first goods booked in, in UK
// working days, per supplier, against the lead time set on the supplier's Brightpearl
// contact (which is what fills in a PO's due date). Read-only via the Alt-Items BP
// passthrough. Writes supplier-leadtimes.json next to itself for salesHub.js.
//
//   node scripts/supplier-leadtimes.mjs 2026-03-28
import { writeFileSync } from 'node:fs';
import { isBankHoliday } from '../quoteChase.js';

const SINCE = process.argv[2] || '2026-03-28';
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
async function bp(path) {
  for (let a = 0; a < 14; a++) {
    const j = await (await fetch(`https://alternate-items.onrender.com/api/debug/bp?path=${encodeURIComponent(path)}`)).json().catch(() => ({}));
    if (j.status === 200 && j.json && typeof j.json.response === 'object') return j.json.response;
    await sleep(Math.min(3000 * (a + 1), 30000));
  }
  throw new Error('gave up ' + path.slice(0, 80));
}
async function search(path) {
  const out = [];
  for (let first = 1; ; first += 500) {
    const r = await bp(`${path}&pageSize=500&firstResult=${first}`);
    const cols = r.metaData.columns.map((c) => c.name);
    for (const row of r.results) out.push(Object.fromEntries(cols.map((c, i) => [c, row[i]])));
    if (!r.metaData.morePagesAvailable) break;
  }
  return out;
}
const ukDay = (iso) => { const [y, m, d] = new Date(iso).toLocaleDateString('en-CA', { timeZone: 'Europe/London' }).split('-').map(Number); return new Date(y, m - 1, d); };
function workingDays(a, b) {
  let d = ukDay(a); const end = ukDay(b); let n = 0;
  while (d < end) { d = new Date(d.getFullYear(), d.getMonth(), d.getDate() + 1); if (d.getDay() % 6 !== 0 && !isBankHoliday(d)) n++; }
  return n;
}

const pos = await search(`/order-service/order-search?orderTypeId=2&createdOn=${SINCE}/`);
console.error('purchase orders', pos.length);
const gin = await search(`/warehouse-service/goods-in-search?receivedDate=${SINCE}/`);
console.error('goods-in lines', gin.length);

const firstIn = new Map();
for (const g of gin) {
  if (!g.purchaseOrderId || !g.receivedDate || !(g.receivedQuantity > 0)) continue;
  const cur = firstIn.get(g.purchaseOrderId);
  if (!cur || g.receivedDate < cur) firstIn.set(g.purchaseOrderId, g.receivedDate);
}

// supplier names + their Brightpearl lead-time setting
const supIds = [...new Set(pos.map((p) => p.contactId).filter(Boolean))].sort((a, b) => a - b);
const sup = new Map();
for (let i = 0; i < supIds.length; i += 100) {
  for (const c of await bp(`/contact-service/contact/${supIds.slice(i, i + 100).join(',')}`) || []) {
    const name = (c.organisation && c.organisation.name) || [c.firstName, c.lastName].filter(Boolean).join(' ') || String(c.contactId);
    sup.set(c.contactId, { name, bpLeadTime: c.leadTime ?? null });
  }
}

const by = new Map();
let skipped = 0;
for (const p of pos) {
  const got = firstIn.get(p.orderId);
  if (!got) continue;
  if (got < p.createdOn) { skipped++; continue; }                 // booked in before it was raised: a data slip
  const wd = workingDays(p.createdOn, got);
  if (wd > 60) { skipped++; continue; }                           // a PO left open for months is not a lead time
  (by.get(p.contactId) || by.set(p.contactId, []).get(p.contactId)).push(wd);
}
const pct = (a, q) => a[Math.min(a.length - 1, Math.floor(q * (a.length - 1)))];
const rows = [];
for (const [cid, v] of by) {
  if (v.length < 3) continue;
  v.sort((a, b) => a - b);
  const s = sup.get(cid) || { name: String(cid), bpLeadTime: null };
  rows.push({ supplier: s.name, contactId: cid, pos: v.length, median: pct(v, 0.5), p75: pct(v, 0.75), p90: pct(v, 0.9), bpLeadTimeDays: s.bpLeadTime });
}
rows.sort((a, b) => b.pos - a.pos);
console.table(rows.map(({ contactId, ...r }) => r));
console.error('skipped (booked in before raised, or >60 working days)', skipped);
writeFileSync(new URL('./supplier-leadtimes.json', import.meta.url), JSON.stringify({ measuredOn: new Date().toISOString().slice(0, 10), since: SINCE, suppliers: rows }, null, 1));
