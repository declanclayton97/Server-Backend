// BP "Low Inventory" (reorder replenishment) — the public API has no low-stock
// LIST endpoint, so we scrape the legacy web report (Products ▸ Inventory ▸ Low
// inventory report) via the authenticated web session (bpWebSession, live account).
// The report already computes On hand / Open SO / On PO / Minimum Stock per product;
// the Open SO count is scoped by the salesOrderStatusId[] filter (we pass every SO
// status EXCEPT Draft/Quote(1), Quote sent(18), Order Confirmation Sent(60)).
//
// Order qty (user formula) = Minimum Stock + Open SO − On PO − On hand  (floor 0).

import { fetchAuthed } from './bpWebSession.js';

const BP_HOST = process.env.BP_WEB_HOST || 'https://euw1.brightpearlapp.com';

const cellText = (h) => String(h || '')
  .replace(/<[^>]*>/g, ' ')
  .replace(/&nbsp;/gi, ' ').replace(/&amp;/gi, '&').replace(/&#39;|&apos;/gi, "'")
  .replace(/\s+/g, ' ').trim();
const toNum = (s) => { const n = parseInt(cellText(s).replace(/[^\-0-9]/g, ''), 10); return isNaN(n) ? 0 : n; };

// Parse the report's results table → one object per product variant.
export function parseLowInventoryTable(html) {
  const trs = html.match(/<tr[\s\S]*?<\/tr>/gi) || [];
  let ci = null;
  const out = [];
  for (const tr of trs) {
    const cells = (tr.match(/<t[dh][\s\S]*?<\/t[dh]>/gi) || []).map(cellText);
    if (!cells.length) continue;
    if (!ci && cells.some((c) => /Minimum Stock/i.test(c)) && cells.some((c) => /Open SO/i.test(c))) {
      const idx = (re) => cells.findIndex((h) => re.test(h));
      ci = {
        sku: idx(/^SKU/i), name: idx(/Product Name/i), opt: idx(/Options/i), sup: idx(/Primary supplier/i),
        inStock: idx(/In stock/i), alloc: idx(/Allocated/i), onHand: idx(/On hand/i),
        openSO: idx(/Open SO/i), openSC: idx(/Open SC/i), onPO: idx(/On PO/i),
        minStock: idx(/Minimum Stock/i), reorder: idx(/Reorder Qty/i),
      };
      continue;
    }
    if (!ci || ci.minStock < 0) continue;
    if (cells.length < ci.minStock + 1 || !cells[ci.sku]) continue;
    const onHand = toNum(cells[ci.onHand]), openSO = toNum(cells[ci.openSO]);
    const onPO = toNum(cells[ci.onPO]), minStock = toNum(cells[ci.minStock]);
    out.push({
      sku: cells[ci.sku], name: cells[ci.name], options: ci.opt >= 0 ? cells[ci.opt] : '',
      primarySupplier: ci.sup >= 0 ? cells[ci.sup] : '',
      inStock: toNum(cells[ci.inStock]), allocated: toNum(cells[ci.alloc]),
      onHand, openSO, onPO, minStock, bpReorderQty: ci.reorder >= 0 ? toNum(cells[ci.reorder]) : null,
      orderQty: Math.max(0, minStock + openSO - onPO - onHand),
    });
  }
  return out;
}

// Fetch the low-inventory report for a supplier/brand, scoped by SO statuses.
// Brightpearl accepts numResults up to 500 and SILENTLY FALLS BACK TO ITS DEFAULT OF 50 for
// anything larger. We asked for 10000, so every replenishment read since this was written has seen
// FIFTY rows — Snickers alone has 233. Measured 2026-08-19: n=250 -> 233 rows (all of them),
// n=500 -> 233, n=1000 -> 50, n=10000 -> 50. Asking for more returned less, and nothing said so.
// The user found roughly GBP13k of reordering the report had never surfaced.
const MAX_NUM_RESULTS = 500;

export async function fetchLowInventory({ client = 'tuffworkwear', supplierId, manufacturerId, statusIds = [], numResults = MAX_NUM_RESULTS } = {}) {
  // Clamp rather than trust the caller: passing 10000 is what caused the silent truncation, and a
  // number over the limit is always a mistake we would rather correct than honour.
  const n = Math.min(Math.max(1, Number(numResults) || MAX_NUM_RESULTS), MAX_NUM_RESULTS);
  const p = new URLSearchParams();
  p.set('p', 'report'); p.set('report', 'product-lowstock');
  if (supplierId) p.set('supplierId', String(supplierId));
  if (manufacturerId) p.set('manufacturerId', String(manufacturerId));
  p.set('numResults', String(n));
  for (const s of statusIds) p.append('salesOrderStatusId[]', String(s));
  const url = `${BP_HOST}/p.php?${p.toString()}`;
  const { status, html } = await fetchAuthed(url, { client });
  const isLogin = /name=["']email_address["']|Brightpearl - Login|data-theme=["']sage["']/i.test(html);
  const rows = isLogin ? [] : parseLowInventoryTable(html);
  // At exactly the ceiling we cannot tell "500 rows" from "the first 500 of more". Say so loudly
  // rather than let a truncated read look like a complete one — that is the whole failure above.
  const truncated = rows.length >= n;
  return { status, isLogin, htmlLen: (html || '').length, rows, url, html, numResults: n, truncated };
}
