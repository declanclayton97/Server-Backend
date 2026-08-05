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
export async function fetchLowInventory({ client = 'tuffworkwear', supplierId, manufacturerId, statusIds = [], numResults = 10000 } = {}) {
  const p = new URLSearchParams();
  p.set('p', 'report'); p.set('report', 'product-lowstock');
  if (supplierId) p.set('supplierId', String(supplierId));
  if (manufacturerId) p.set('manufacturerId', String(manufacturerId));
  p.set('numResults', String(numResults));
  for (const s of statusIds) p.append('salesOrderStatusId[]', String(s));
  const url = `${BP_HOST}/p.php?${p.toString()}`;
  const { status, html } = await fetchAuthed(url, { client });
  const isLogin = /name=["']email_address["']|Brightpearl - Login|data-theme=["']sage["']/i.test(html);
  return { status, isLogin, htmlLen: (html || '').length, rows: isLogin ? [] : parseLowInventoryTable(html), url, html };
}
