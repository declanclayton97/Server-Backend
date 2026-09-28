// Lead times: order placed -> last parcel shipped, in UK working days, split logo / non-logo
// and by channel. Read-only via the Alt-Items BP passthrough. Prints aggregates only.
// Feeds LEAD_TIMES in salesHub.js — re-run every few months:  node scripts/sales-hub-leadtimes.mjs 2026-06-28
import { writeFileSync } from 'node:fs';
import { classifyOrderRow } from '../salesHub.js';
import { isBankHoliday } from '../quoteChase.js';

const SINCE = process.argv[2] || '2026-06-28';
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
  const out = []; let cols;
  for (let first = 1; ; first += 500) {
    const r = await bp(`${path}&pageSize=500&firstResult=${first}`);
    cols = r.metaData.columns.map((c) => c.name);
    for (const row of r.results) out.push(Object.fromEntries(cols.map((c, i) => [c, row[i]])));
    if (!r.metaData.morePagesAvailable) break;
  }
  return out;
}

const orders = await search(`/order-service/order-search?orderTypeId=1&createdOn=${SINCE}/`);
console.error('orders', orders.length);
const gons = await search(`/warehouse-service/goods-note/goods-out-search?createdOn=${SINCE}/`);
console.error('goods-out notes', gons.length);

// per order: shipped notes and whether any note is still unshipped
const ship = new Map();
for (const g of gons) {
  if (g.transfer) continue;
  const s = ship.get(g.orderId) || { last: null, unshipped: 0, n: 0 };
  s.n++;
  if (g.shipped && g.shippedOn) { if (!s.last || g.shippedOn > s.last) s.last = g.shippedOn; } else s.unshipped++;
  ship.set(g.orderId, s);
}
const done = orders.filter((o) => !o.parentOrderId && ship.has(o.orderId) && ship.get(o.orderId).unshipped === 0 && ship.get(o.orderId).last);
console.error('fully shipped, not exchanges', done.length);

// rows -> logo or not
const ids = done.map((o) => o.orderId).sort((a, b) => a - b);
const rowsBy = new Map(); const pids = new Set();
for (let i = 0; i < ids.length; i += 200) {
  for (const o of await bp(`/order-service/order/${ids.slice(i, i + 200).join(',')}`)) {
    const rows = Object.values(o.orderRows || {});
    rowsBy.set(o.id, rows);
    rows.forEach((r) => r.productId && pids.add(r.productId));
  }
  if (i % 2000 === 0) console.error('  orders read', i);
}
const meta = new Map(); const plist = [...pids].sort((a, b) => a - b);
for (let i = 0; i < plist.length; i += 200) {
  for (const p of await bp(`/product-service/product/${plist.slice(i, i + 200).join(',')}`) || [])
    meta.set(p.id, { stockTracked: !!(p.stock && p.stock.stockTracked), brandId: p.brandId });
}

const ukDay = (iso) => { const [y, m, d] = new Date(iso).toLocaleDateString('en-CA', { timeZone: 'Europe/London' }).split('-').map(Number); return new Date(y, m - 1, d); };
function workingDays(a, b) {
  let d = ukDay(a), end = ukDay(b), n = 0;
  while (d < end) { d = new Date(d.getFullYear(), d.getMonth(), d.getDate() + 1); if (d.getDay() % 6 !== 0 && !isBankHoliday(d)) n++; }
  return n;
}

const groups = {};
const add = (k, v) => (groups[k] ||= []).push(v);
for (const o of done) {
  const rows = rowsBy.get(o.orderId) || [];
  const kinds = rows.map((r) => classifyOrderRow(r, meta.get(r.productId)));
  if (!kinds.includes('goods')) continue;                       // nothing physical to wait for
  // Decoration is found STRUCTURALLY: a non-stocked brand-74 service row.
  const logo = rows.some((r, i) => kinds[i] === 'service' && (meta.get(r.productId) || {}).brandId === 74);
  const channel = o.installedIntegrationInstanceId ? 'web/marketplace' : 'direct (sales team)';
  const wd = workingDays(o.createdOn, ship.get(o.orderId).last);
  add(`${logo ? 'LOGO' : 'NON-LOGO'} | all channels`, wd);
  add(`${logo ? 'LOGO' : 'NON-LOGO'} | ${channel}`, wd);
}
const pct = (a, p) => a[Math.min(a.length - 1, Math.floor(p * (a.length - 1)))];
const out = {};
for (const [k, v] of Object.entries(groups).sort()) {
  v.sort((a, b) => a - b);
  const b = (lo, hi) => Math.round(100 * v.filter((x) => x >= lo && x <= hi).length / v.length);
  out[k] = { orders: v.length, median: pct(v, 0.5), mean: +(v.reduce((a, x) => a + x, 0) / v.length).toFixed(1),
    p75: pct(v, 0.75), p90: pct(v, 0.9),
    '% 0-2d': b(0, 2), '% 3-5d': b(3, 5), '% 6-10d': b(6, 10), '% 11-15d': b(11, 15), '% 16d+': b(16, 9999) };
}
console.table(out);
writeFileSync('leadtimes.json', JSON.stringify({ since: SINCE, groups: out }, null, 1));
