// Resolve a Sterling PO line to the portal-order payload the worker needs.
// Our BP SKU is (or resolves to) an EAN barcode — the same key the stock feed uses —
// which sterlingProducts.json maps to { search (style name), colour, size }.
import fs from 'fs';
import { bpLiveGet } from './purchasingAuto.js';

const PRODUCTS = JSON.parse(fs.readFileSync(new URL('./sterlingProducts.json', import.meta.url), 'utf8'));

// Leg×waist trousers (e.g. Bancroft) render as a grid: one ROW per leg length, one COLUMN
// per waist. The shop labels the waist columns but NOT the leg rows — rows are just in
// ascending leg order. So to target the right cell we pass the leg's ordinal position among
// the style's distinct legs. Build, per style+colour, the sorted list of distinct leg values.
const legWaist = (size) => {
  const s = String(size || '');
  const l = s.match(/L\s*(\d{2})/i); const w = s.match(/W\s*(\d{2})/i);
  return { leg: l ? Number(l[1]) : null, waist: w ? Number(w[1]) : null };
};
const styleKey = (search, colour) => `${String(search || '').trim().toLowerCase()}|${String(colour || '').trim().toLowerCase()}`;
const LEGS_BY_STYLE = (() => {
  const m = new Map();
  for (const e of Object.values(PRODUCTS)) {
    const { leg } = legWaist(e.size);
    if (leg == null) continue;
    const k = styleKey(e.search, e.colour);
    if (!m.has(k)) m.set(k, new Set());
    m.get(k).add(leg);
  }
  const out = new Map();
  for (const [k, set] of m) out.set(k, [...set].sort((a, b) => a - b));
  return out;
})();

const _barcode = {};
async function barcodeFor(productId) {
  if (productId in _barcode) return _barcode[productId];
  let bc = null;
  try { const p = (await bpLiveGet(`/product-service/product/${productId}`))[0]; bc = (p && p.identity && (p.identity.barcode || p.identity.ean)) || null; } catch { /* leave null */ }
  _barcode[productId] = bc; return bc;
}

// Service/non-product lines that legitimately appear on an SO but are NOT Sterling
// orderables (personalisation, misc/shipping) — skip, don't treat as unresolved.
export const isNonSterlingOrderable = (sku) => /^(OPPR|MISC|SHIP|CARR|DELIV)/i.test(String(sku || ''));

// Sterling's workbook can carry a `search` value their own SITE does not recognise. The workbook
// lists every Apache Industry cargo trouser as APINDBLACK, but the shop only finds APINDBLK —
// verified side by side with a worker dry-run on 2026-09-02:
//     FOUND     search="APINDBLK"   size L33W38 -> matchedSize 38
//     NOT FOUND search="APINDBLACK" size L33W38 -> qty short: basket +0, wanted 1
// 33 black variants carry the bad term. The cost of one of them (141383, EAN 5055338400317) was a
// line that quietly never reached an order while SO 484193 was flipped to "Ordered Stock Awaiting
// Delivery" — the customer waiting on a trouser nothing had bought.
//
// Aliased here rather than as 33 per-EAN overrides: one wrong search term, one correction, and a
// re-ingest cannot undo it. NAVY (APINDNAV) is deliberately NOT aliased — it has not been verified
// against the site, and guessing a search term is how the wrong garment gets ordered.
const SEARCH_ALIAS = { APINDBLACK: 'APINDBLK', APINDNAV: 'APINDNAVY' };
const aliasSearch = (s) => SEARCH_ALIAS[String(s || '').trim().toUpperCase()] || s;

export async function resolveSterlingLine({ sku, productId }) {
  let ean = /^\d{8,}$/.test(String(sku || '')) ? String(sku) : null;
  if (!ean && productId) ean = await barcodeFor(productId);
  ean = ean ? String(ean).replace(/\D/g, '') : null;
  const e = ean ? PRODUCTS[ean] : null;
  if (!e) return { resolved: false, sku, ean, reason: 'not in Sterling product data' };
  const out = { resolved: true, ean, search: aliasSearch(e.search), colour: e.colour, size: e.size, brand: e.brand };
  // For leg×waist trousers, attach the leg's ordinal (row) among the style's legs + total
  // legs, so the worker can target the exact (leg-row, waist-column) cell and verify the
  // grid's row count matches — refusing to guess if the catalogue/grid drift.
  const { leg, waist } = legWaist(e.size);
  if (leg != null) {
    const legs = LEGS_BY_STYLE.get(styleKey(e.search, e.colour)) || [leg];
    out.leg = leg; out.waist = waist; out.legIndex = legs.indexOf(leg); out.legCount = legs.length; out.legs = legs;
  }
  return out;
}
