// Resolve a Sterling PO line to the portal-order payload the worker needs.
// Our BP SKU is (or resolves to) an EAN barcode — the same key the stock feed uses —
// which sterlingProducts.json maps to { search (style name), colour, size }.
import fs from 'fs';
import { bpLiveGet } from './purchasingAuto.js';

const PRODUCTS = JSON.parse(fs.readFileSync(new URL('./sterlingProducts.json', import.meta.url), 'utf8'));

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

export async function resolveSterlingLine({ sku, productId }) {
  let ean = /^\d{8,}$/.test(String(sku || '')) ? String(sku) : null;
  if (!ean && productId) ean = await barcodeFor(productId);
  ean = ean ? String(ean).replace(/\D/g, '') : null;
  const e = ean ? PRODUCTS[ean] : null;
  if (!e) return { resolved: false, sku, ean, reason: 'not in Sterling product data' };
  return { resolved: true, ean, search: e.search, colour: e.colour, size: e.size, brand: e.brand };
}
