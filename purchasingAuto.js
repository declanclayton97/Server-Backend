// Purchasing automation (Phase A) — build supplier POs from Brightpearl demand.
//
// Flow per supplier (e.g. SNICKERS):
//   1. Find demand = sales orders in "Stock needs ordering" (23) whose
//      PCF_SUPPLIER tag contains the supplier (a clean tag, not a "leave"
//      note), that don't already carry this supplier's PO number.
//   2. Gather ONLY that supplier's product rows (skip note/free-text rows and
//      other suppliers' lines).
//   3. createPO: create ONE "Pending PO"(6) for the supplier with those lines
//      (cost from the supplier's cost price list), attach a source note listing
//      the contributing orders, and stamp the PO number into each order's
//      per-supplier PO field (linkage + dedupe). No status/tag change yet.
//   4. finalizePO (called AFTER the order is actually placed on the supplier
//      portal): strip the supplier from PCF_SUPPLIER, flip status to
//      "Ordered Stock Awaiting Delivery"(22) when it was the last supplier, and
//      drop an "ordered via PO N" note on each order.
//
// Runs against the TEST account by default (env BP_TEST_*). Custom-field writes
// use JSON-Patch op:"add" to set/upsert and op:"remove" to clear (op:"replace"
// fails on an empty field).

import { getOrderAllocations } from './bpWebSession.js';

const DC = process.env.BP_TEST_DATACENTER || 'euw1';
const ACCOUNT = process.env.BP_TEST_ACCOUNT || 'tuffbsitc';
const BASE = () => `https://${DC}.brightpearlconnect.com/public-api/${ACCOUNT}`;
const HEADERS = () => ({
  'brightpearl-app-ref': process.env.BP_TEST_APP_REF,
  'brightpearl-account-token': process.env.BP_TEST_TOKEN,
  'Content-Type': 'application/json',
});
export const isConfigured = () => !!(process.env.BP_TEST_APP_REF && process.env.BP_TEST_TOKEN);

const DEMAND_STATUS = 23;   // Stock needs ordering

// SO statuses that are NOT real, committed demand — excluded from the Low Inventory
// "Open SO" count (pre-order / non-committed states). User-defined (2026-08-05):
//   1 Draft / Quote · 2 New order · 18 Quote sent · 36 Ignore – awaiting deletion ·
//   53 Sample orders · 60 Order Confirmation Sent · 87 Pending Magento Orders ·
//   89 Belgrade Pending Orders · 114 Cancellation Pending
export const NON_DEMAND_SO_STATUS_IDS = [1, 2, 18, 36, 53, 60, 87, 89, 114];
const ORDERED_STATUS = 22;  // Ordered Stock Awaiting Delivery
const PENDING_PO_STATUS = 6; // (informational — POs default to this on create)
const WAREHOUSE_ID = 2;

// Supplier registry. Each entry: BP supplier contactId, the supplier's cost
// price list id, the per-supplier PO custom-field code, and a line detector
// (matches a product name/sku to this supplier — used to pick the supplier's
// rows out of a mixed order). Cost falls back to the SO row's itemCost if the
// cost list has no price for a product.
export const SUPPLIERS = {
  SNICKERS:     { contactId: 331,   costList: 10, poField: 'PCF_SNICKPO', detect: (n) => /snickers|solid\s*gear/i.test(n || '') },
  BLAKLADER:    { contactId: 323,   costList: 12, poField: 'PCF_BLAKLPO', detect: (n) => /bl[åa]kl[äa]der/i.test(n || '') },
  PORTWEST:     { contactId: 298,   costList: 7,  poField: 'PCF_PORTWPO', detect: (n) => /portwest/i.test(n || '') },
  UNEEK:        { contactId: 322,   costList: 11, poField: 'PCF_UNEEKPO', detect: (n) => /uneek/i.test(n || '') },
  'HELLY HANSEN': { contactId: 214, costList: 6,  poField: 'PCF_HELLYPO', detect: (n) => /helly\s*hansen|hh\s*workwear/i.test(n || '') },
  MASCOT:       { contactId: 334,   costList: null, poField: 'PCF_MASCOTPO', detect: (n) => /mascot/i.test(n || '') },
  // Live-automated suppliers below (contactId + Launch cost list 20 + low-inv supplierId).
  FRISTADS:     { contactId: 37419, costList: 20, poField: 'PCF_FRISTPO', lowInvSupplierId: 37419, detect: (n) => /fristads/i.test(n || '') },
  // Castle Clothing distributes TuffStuff / Makita / Fort (+ Fort Footwear) and the
  // licensed DeWalt workwear range. Their products are NOT named "castle" — detect by
  // brand. No dedicated PO custom field yet, so re-pickup is prevented by clearing the
  // CASTLE tag on finalize.
  CASTLE:       { contactId: 332,   costList: 20, poField: 'PCF_CASTLEPO', lowInvSupplierId: 332, detect: (n) => /tuffstuff|makita|\bfort\b/i.test(n || '') }, // DeWalt moved to Sterling (Castle no longer sells it)
  // Sterling Safetywear — brands Apache / City Knights / DeWalt. ⚠ DeWalt ALSO comes
  // via Castle, so on a multi-supplier order a DeWalt row is ambiguous — the
  // PCF_SUPPLIER tag decides which supplier the order is for; single-supplier orders
  // use the all-rows fallback. Portal ordering is client-side (localStorage/JS) → needs
  // a headless browser (built separately); the BP side is fully generic.
  STERLING:     { contactId: 341,   costList: 20, poField: 'PCF_STERLPO', lowInvSupplierId: 341, detect: (n) => /apache|city\s*knights|dewalt/i.test(n || '') },
  // Ralawise = distributor (Stanley Stella exclusive + Gildan/AWDis/etc). Detect by
  // Stanley Stella name OR a Ralawise-format SKU (2 letters + 3 digits + …).
  RALAWISE:     { contactId: 205,   costList: null, poField: 'PCF_RALAPO', detect: (n, sku) => /stanley\s*stella/i.test(n || '') || /^[A-Z]{2}\d{3}[A-Z0-9]/.test(String(sku || '').replace(/[\s_-]/g, '')) },
};

// ---- low-level API with throttle back-off ----
async function api(method, path, body, attempt = 0) {
  const opts = { method, headers: HEADERS() };
  if (body !== undefined) opts.body = JSON.stringify(body);
  const res = await fetch(`${BASE()}${path}`, opts);
  if ((res.status === 429 || res.status === 503) && attempt < 5) {
    const h = res.headers;
    const wait = parseInt(h.get('brightpearl-next-throttle-period') || '2000', 10);
    await new Promise((r) => setTimeout(r, Math.min(isNaN(wait) ? 2000 : wait, 60000) + 300));
    return api(method, path, body, attempt + 1);
  }
  const text = await res.text();
  let json = null;
  try { json = text ? JSON.parse(text) : null; } catch { /* non-JSON */ }
  if (!res.ok) {
    const msg = json && json.errors ? JSON.stringify(json.errors) : text.slice(0, 200);
    const err = new Error(`BP ${method} ${path} -> ${res.status}: ${msg}`);
    err.status = res.status;
    throw err;
  }
  return json ? json.response : null;
}

// Thin passthrough to the internal BP api (TEST account) — used by gated debug/seed
// endpoints for validation. Same throttle/error handling.
export async function bpApi(method, path, body) { return api(method, path, body); }

// Read-only LIVE GET passthrough (for debugging allocation/warehouse endpoints).
export async function bpLiveGet(path) { return liveGet(path); }

// ---- Product identity (SKU/EAN/UPC/MPN/ISBN) read + SAFE merge-write ----
// PUT /product-service/product/{id}/identity REPLACES the whole identity block:
// any identifier NOT supplied is CLEARED. So NEVER PUT a partial body directly —
// always read-merge-write. `barcode` is returned by GET but is NOT one of the
// PUT-settable keys (BP stores/derives it) — the sandbox identity-test tells us
// whether writing `ean` also populates `barcode` (which is what our stock lookups
// read from Alt-Items product_cache).
const IDENTITY_KEYS = ['sku', 'ean', 'upc', 'mpn', 'isbn'];

export async function getProductIdentity(productId) {
  const resp = await api('GET', `/product-service/product/${productId}`);
  const p = Array.isArray(resp) ? resp[0] : resp;
  return (p && p.identity) || {};
}

// changes: any of { sku, ean, upc, mpn, isbn }. Pass "" / null to intentionally
// clear one. Every identifier NOT in `changes` is preserved from the current
// identity. Returns { productId, before, put } for logging/verification.
export async function setProductIdentity(productId, changes = {}) {
  const cur = await getProductIdentity(productId);
  const put = {};
  for (const k of IDENTITY_KEYS) {
    const v = (k in changes) ? changes[k] : cur[k];
    if (v != null && String(v).trim() !== '') put[k] = String(v).trim();
  }
  await api('PUT', `/product-service/product/${productId}/identity`, put);
  return { productId, before: cur, put };
}

// ---- helpers ----
const tagsOf = (v) => String(v || '').split('/').map((x) => x.trim()).filter(Boolean);
// Pick a product-option value by matching the option KEY (names vary: "Size",
// "Mascot Trouser Size", "Colour", "Color"…).
const optValue = (opts, re) => { for (const k of Object.keys(opts || {})) if (re.test(k)) return opts[k]; return null; };
const isNoteRow = (r) => String(r.productId) === '1000' || !r.productSku;
// Not orderable from a supplier: BP note/message rows (pid 1000), the misc/free-text
// product (pid 1001), or a service/personalisation/shipping sku (MISC1, OPPR = "Print
// …"/embroidery, SHIP/CARR/DELIV = carriage). These are decoration/charges on the SO,
// never something the supplier ships us — keep them off the PO.
// Rows that are never supplier-orderable: notes, the 1001 marker, service/shipping SKUs,
// in-house DECORATION lines (OPEM = embroider, OPPR = personalisation), the decorated bundle
// variants (SKU carries a "#…" decoration suffix, e.g. 26706#EW — BP rejects bundles on a PO
// anyway with ORDC-023), and the standalone "Embroidery (Our Garments)" service product.
const isNonOrderableRow = (r) => isNoteRow(r) || String(r.productId) === '1001'
  || /^(MISC|OPPR|OPEM|SHIP|CARR|DELIV)/i.test(r.productSku || '')
  || /#/.test(r.productSku || '')
  || /^embroider/i.test(r.productName || '');

// VAT: a BP tax code encodes its rate (T20=20%, T5=5%, T0=0%). Parse the rate so a
// PO row carries the product's ACTUAL VAT — never a blanket T20 (which wrongly charges
// 20% on zero-rated items like kids' clothing). BP does NOT silently fix a wrong code.
const taxRate = (code) => { const m = String(code || '').match(/T(\d+(?:\.\d+)?)/i); return m ? Number(m[1]) / 100 : 0.20; };
const _productTaxCache = {};
async function productTaxCodeLive(productId) {
  if (productId in _productTaxCache) return _productTaxCache[productId];
  let code = 'T20';
  try { const p = (await liveGet(`/product-service/product/${productId}`))[0]; code = (p && p.financialDetails && p.financialDetails.taxCode && p.financialDetails.taxCode.code) || 'T20'; } catch { /* default */ }
  _productTaxCache[productId] = code; return code;
}
const isLeaveNote = (v) => /unable to order|awaiting|leave|do not order|chased|response|on hold/i.test(v || '');

async function costOf(productId, costList, fallback) {
  if (costList == null) return fallback;
  try {
    const resp = await api('GET', `/product-service/product-price/${productId}`);
    const pl = resp[0].priceLists.find((x) => x.priceListId === costList);
    const v = pl && pl.quantityPrice && pl.quantityPrice['1'];
    return v ? parseFloat(v) : fallback;
  } catch {
    return fallback;
  }
}

// Detect overrides for suppliers whose product NAME isn't the supplier name
// (e.g. Panther supplies Aboutblu). Otherwise the detector is built from the
// supplier name. Used for dynamic (email) suppliers not in the hardcoded registry.
const SUPPLIER_ALIASES = {
  PANTHER: /aboutblu|panther/i,
  BUCKBOOTZ: /buckler|buckbootz/i,
  'BLUE MAX BANNER': /\bbanner\b/i,
  'SHOES FOR CREWS': /shoes\s*for\s*crews|\bsfc\b/i,
  DISLEY: /disley/i,
  OCTOGRIP: /octogrip/i,
};
function dynamicDetect(key) {
  const k = String(key || '').toUpperCase();
  if (SUPPLIER_ALIASES[k]) { const re = SUPPLIER_ALIASES[k]; return (n) => re.test(n || ''); }
  const words = String(key).replace(/\bltd\b|\blimited\b|\buk\b|\(.*?\)/gi, ' ').replace(/[^a-z0-9\s]/gi, ' ').trim().split(/\s+/).filter(Boolean);
  const re = words.length ? new RegExp(words.map((w) => w.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')).join('\\s*'), 'i') : null;
  return (n) => (re ? re.test(n || '') : false);
}
// Resolve a supplier's BP contactId by company name (cached).
const _supContact = {};
async function lookupSupplierContactId(name) {
  const key = String(name).toUpperCase();
  if (key in _supContact) return _supContact[key];
  let id = null;
  try {
    const s = await api('GET', `/contact-service/contact-search?companyName=${encodeURIComponent(name)}&pageSize=20`);
    const idx = {}; s.metaData.columns.forEach((c, i) => { idx[c.name] = i; });
    const rows = s.results || [];
    // Skip dead/duplicate records ("... ++ OLD ACC ++", "OLD ACCOUNT", "CLOSED",
    // "DO NOT USE") — some suppliers have a stale contact alongside the live one
    // (e.g. Fristads "Fristads Kansas ++ OLD ACC ++" vs "Fristads Workwear Ltd").
    const isDead = (r) => idx.companyName != null && /\bOLD ACC(OUNT)?\b|\bCLOSED\b|DO ?NOT ?USE|DEAD ACC/i.test(String(r[idx.companyName] || ''));
    // Prefer a contact actually flagged as a supplier (a name match alone can hit a
    // customer of the same name, e.g. "Prestige Building Supplies" vs the supplier
    // "Prestige Leisure UK Ltd"), skip dead records, and — because contact-search is
    // a prefix/substring match — prefer an EXACT company-name match, then a
    // starts-with, over a mere substring hit (e.g. "Engel" must resolve to "Engel"
    // 83093, NOT "engelbert strauss Ltd" 23986). Fall back progressively so a match
    // is still returned if every candidate is dead / none is flagged.
    const wanted = String(name).trim().toLowerCase();
    const cname = (r) => (idx.companyName != null ? String(r[idx.companyName] || '') : '').trim().toLowerCase();
    const best = (list) => list.find((r) => cname(r) === wanted) || list.find((r) => cname(r).startsWith(wanted)) || list[0];
    const supplierRows = idx.isSupplier != null ? rows.filter((r) => r[idx.isSupplier] === true) : [];
    const pick = best(supplierRows.filter((r) => !isDead(r))) || best(rows.filter((r) => !isDead(r))) || supplierRows[0] || rows[0];
    if (pick) id = pick[idx.contactId];
  } catch { /* leave null */ }
  _supContact[key] = id;
  return id;
}
// Registry entry first (portal suppliers with specific detect/costList); otherwise
// a DYNAMIC entry for email/unknown suppliers — contactId looked up from BP by
// name, costList null (falls back to SO itemCost), name/alias detector.
async function resolveSupplier(key) {
  const k = String(key || '').toUpperCase();
  if (SUPPLIERS[k]) return { key: k, ...SUPPLIERS[k] };
  const contactId = await lookupSupplierContactId(key);
  if (!contactId) throw new Error(`Unknown supplier "${key}" — not in registry and no BP contact found by that name`);
  return { key: k, name: String(key), contactId, costList: null, poField: null, detect: dynamicDetect(key), dynamic: true };
}

// Find the orders that contribute lines to this supplier's PO.
// Returns [{ id, ref, tag, remaining, complete, lines:[{productId,sku,name,qty,cost}] }].
async function findContributors(sup, orderIds) {
  let ids = orderIds;
  if (!ids || !ids.length) {
    const search = await api('GET', `/order-service/sales-order-search?orderStatusId=${DEMAND_STATUS}&pageSize=500`);
    const idx = {};
    search.metaData.columns.forEach((c, i) => { idx[c.name] = i; });
    ids = search.results.map((r) => r[idx.salesOrderId]);
  }
  const out = [];
  for (const id of ids) {
    const cf = (await api('GET', `/order-service/order/${id}/custom-field`)) || {};
    const tag = cf.PCF_SUPPLIER;
    if (!tag || isLeaveNote(tag)) continue;                                   // empty / leave-note
    if (!tagsOf(tag).some((t) => t.toUpperCase() === sup.key)) continue;      // not this supplier
    if (sup.poField && cf[sup.poField]) continue;                             // already has a PO for this supplier
    const order = (await api('GET', `/order-service/order/${id}`))[0];
    let rows = Object.values(order.orderRows).filter((r) => !isNoteRow(r) && sup.detect(r.productName, r.productSku));
    // Single-supplier order where the name-detector matched nothing (common for
    // email suppliers whose products aren't named after the supplier): take all
    // product rows — the whole order was tagged for this one supplier.
    const allTags = tagsOf(tag);
    if (!rows.length && allTags.length === 1 && allTags[0].toUpperCase() === sup.key) {
      rows = Object.values(order.orderRows).filter((r) => !isNoteRow(r));
    }
    if (!rows.length) continue;
    const lines = [];
    for (const r of rows) {
      const qty = parseFloat(r.quantity.magnitude);
      const cost = await costOf(r.productId, sup.costList, r.itemCost ? parseFloat(r.itemCost.value) : 0);
      lines.push({ productId: r.productId, sku: r.productSku, name: r.productName, qty, cost, size: optValue(r.productOptions, /size/i), colour: optValue(r.productOptions, /colou?r/i) });
    }
    const remaining = tagsOf(tag).filter((t) => t.toUpperCase() !== sup.key);
    out.push({
      id, ref: order.reference || '', tag, remaining, complete: remaining.length === 0, lines,
      createdById: order.createdById || null,
      channelId: (order.assignment && order.assignment.current && order.assignment.current.channelId) || null,
    });
  }
  return out;
}

function summarise(sup, contributors) {
  const lines = contributors.flatMap((c) => c.lines);
  const total = lines.reduce((a, l) => a + l.cost * l.qty, 0);
  return {
    supplier: sup.key,
    supplierContactId: sup.contactId,
    costListId: sup.costList,
    orderCount: contributors.length,
    lineCount: lines.length,
    totalQty: lines.reduce((a, l) => a + l.qty, 0),
    totalNet: Number(total.toFixed(2)),
    orders: contributors.map((c) => ({ orderId: c.id, ref: c.ref, createdById: c.createdById, channelId: c.channelId, tag: c.tag, willComplete: c.complete, tagAfter: c.remaining.join(' / '), lines: c.lines })),
  };
}

// Read-only preview.
export async function preview(supplierKey, orderIds) {
  const sup = await resolveSupplier(supplierKey);
  const contributors = await findContributors(sup, orderIds);
  return { dryRun: true, ...summarise(sup, contributors) };
}

// ─────────────────────────────────────────────────────────────────────────────
// LIVE read-only demand preview — verification tool for go-live. Runs the SAME
// demand-detection filter as findContributors, but against the LIVE Brightpearl
// account using the main app's creds, and it is **GET-ONLY** (liveGet takes no
// method → it physically cannot create/patch/put anything). Nothing is written.
// It also paginates the status search + reports a full include/exclude accounting
// so we can confirm every tagged order is picked up before enabling any writes.
// ─────────────────────────────────────────────────────────────────────────────
const LIVE_BASE = () => {
  const dc = process.env.BRIGHTPEARL_DATACENTER === 'euw1' ? 'euw1' : (process.env.BRIGHTPEARL_DATACENTER || 'euw1');
  return `https://${dc}.brightpearlconnect.com/public-api/${process.env.BRIGHTPEARL_ACCOUNT_ID}`;
};
const LIVE_HEADERS = () => ({
  'brightpearl-app-ref': process.env.BRIGHTPEARL_APP_REF,
  'brightpearl-account-token': process.env.BRIGHTPEARL_API_TOKEN,
  'Content-Type': 'application/json',
});
export const isLiveConfigured = () => !!(process.env.BRIGHTPEARL_APP_REF && process.env.BRIGHTPEARL_API_TOKEN && process.env.BRIGHTPEARL_ACCOUNT_ID);

// GET-only, throttle-aware. No `method` parameter by design — this client cannot write.
async function liveGet(path, attempt = 0) {
  const res = await fetch(`${LIVE_BASE()}${path}`, { headers: LIVE_HEADERS() });
  if ((res.status === 429 || res.status === 503) && attempt < 6) {
    const wait = parseInt(res.headers.get('brightpearl-next-throttle-period') || '2000', 10);
    await new Promise((r) => setTimeout(r, Math.min(isNaN(wait) ? 2000 : wait, 60000) + 500));
    return liveGet(path, attempt + 1);
  }
  const text = await res.text();
  let json = null; try { json = text ? JSON.parse(text) : null; } catch { /* non-JSON */ }
  if (!res.ok) { const e = new Error(`BP GET ${path} -> ${res.status}: ${text.slice(0, 200)}`); e.status = res.status; throw e; }
  return json ? json.response : null;
}
const chunk = (a, n) => { const o = []; for (let i = 0; i < a.length; i += n) o.push(a.slice(i, i + n)); return o; };
const pause = (ms) => new Promise((r) => setTimeout(r, ms));

// SO status ids on the live account, minus `exclude`. Used to scope the Low
// Inventory report's "Open SO" to real demand (exclude Draft/Quote, Quote sent,
// Order Confirmation Sent). Read-only.
export async function liveSalesOrderStatusIds(exclude = []) {
  const ex = new Set(exclude.map(String));
  const statuses = await liveGet('/order-service/order-status');
  const list = Array.isArray(statuses) ? statuses : Object.values(statuses || {});
  return list
    .filter((s) => String(s.orderTypeCode || '') === 'SO')
    .map((s) => s.statusId ?? s.orderStatusId ?? s.id)
    .filter((id) => id != null && !ex.has(String(id)));
}

// Diagnostic (read-only): dump warehouse availability + the product record for a
// set of product ids, so we can locate the reorder/min-stock level + on-hand /
// on-order / allocated fields that drive the "Low Inventory" replenishment calc.
export async function debugLiveStock(productIds) {
  const idSet = productIds.join(',');
  const out = { productIds };
  try { out.availability = await liveGet(`/warehouse-service/product-availability/${idSet}`); } catch (e) { out.availabilityError = e.message; }
  try {
    const prods = await liveGet(`/product-service/product/${idSet}`);
    // Trim to the stock-relevant bits so the payload is readable.
    out.products = (Array.isArray(prods) ? prods : [prods]).map((p) => ({
      id: p.id, name: p.productName || (p.identity && p.identity.sku),
      stock: p.stock, reorder: p.reorderLevel, inventory: p.inventory,
      keys: Object.keys(p),
    }));
  } catch (e) { out.productsError = e.message; }
  return out;
}

// Diagnostic: scan every order in a status and report which custom-field CODES
// carry values (+ which contain `find`). Read-only. Finds the real code behind a
// human field label like "SUPPLIERS NEEDED" when it isn't PCF_SUPPLIER.
export async function debugLiveCustomFields(statusId, find) {
  const sid = statusId || DEMAND_STATUS;
  let ids = [], firstResult = 1;
  for (let guard = 0; guard < 40; guard++) {
    const s = await liveGet(`/order-service/sales-order-search?orderStatusId=${sid}&pageSize=500&firstResult=${firstResult}`);
    const idx = {}; s.metaData.columns.forEach((c, i) => { idx[c.name] = i; });
    ids.push(...s.results.map((r) => r[idx.salesOrderId]));
    if (!s.metaData.morePagesAvailable || !s.results.length) break;
    firstResult += 500; await pause(250);
  }
  const re = find ? new RegExp(find, 'i') : null;
  const fieldCodesSeen = {};   // code -> { count, sampleValues[] }
  const hits = [];
  for (const id of ids) {
    const cf = (await liveGet(`/order-service/order/${id}/custom-field`)) || {};
    await pause(120);
    for (const [k, v] of Object.entries(cf)) {
      if (v == null || String(v).trim() === '') continue;
      if (!fieldCodesSeen[k]) fieldCodesSeen[k] = { count: 0, sampleValues: [] };
      fieldCodesSeen[k].count++;
      if (fieldCodesSeen[k].sampleValues.length < 4 && !fieldCodesSeen[k].sampleValues.includes(String(v))) fieldCodesSeen[k].sampleValues.push(String(v));
    }
    if (re) {
      const matched = Object.entries(cf).filter(([, v]) => re.test(String(v || '')));
      if (matched.length) hits.push({ orderId: id, fields: matched.map(([code, value]) => ({ code, value })) });
    }
  }
  return { statusId: sid, ordersScanned: ids.length, find: find || null, matchCount: hits.length, fieldCodesSeen, hits };
}

// Add a private note to any order (PO or SO) via the API — the reliable way to record the
// supplier's order number against a PO (the legacy web-form "reference" write only renders
// its editable form when the PO is open in a real browser, so it fails from a headless run).
export async function addOrderNoteLive(orderId, text, contactId) {
  const addedOn = new Date().toISOString().replace('Z', '+00:00');
  return liveWrite('POST', `/order-service/order/${orderId}/note`, { text: String(text), addedOn, contactId: contactId || 1, isPublic: false });
}

// Set an order's Reference field via the API (JSON-Patch replace). SURGICAL — it touches ONLY
// /reference, so unlike the legacy web-form re-submit (which zeroes row tax → needed a fragile
// reprice) it can't disturb rows/tax. PUT is unsupported (405); PATCH replace works. Verified
// tax-unchanged on sandbox POs (empty + tax-bearing) 2026-08-07.
export async function setOrderReferenceLive(orderId, reference) {
  return liveWrite('PATCH', `/order-service/order/${orderId}`, [{ op: 'replace', path: '/reference', value: String(reference) }]);
}

export async function previewLive(supplierKey, orderIds) {
  const k = String(supplierKey || '').toUpperCase();
  // Use the hardcoded registry detector/poField (same as production). contactId/
  // costList are irrelevant to a read-only pickup check, so no dynamic lookup.
  const sup = SUPPLIERS[k]
    ? { key: k, ...SUPPLIERS[k] }
    : { key: k, detect: dynamicDetect(k), poField: null, costList: null };

  // 1. Confirm the status id actually IS "Stock needs ordering" on the live account
  //    (status ids are account-config; verify 23 rather than trust it blindly).
  let statusName = null, nameMatchedStatusId = null, statusDebug = null;
  try {
    const statuses = await liveGet('/order-service/order-status');
    const list = Array.isArray(statuses) ? statuses : Object.values(statuses || {});
    for (const s of list) {
      const id = s.orderStatusId ?? s.id ?? s.statusId, nm = s.name || s.orderStatusName || s.statusName || '';
      if (String(id) === String(DEMAND_STATUS)) statusName = nm;
      if (/stock\s*needs\s*ordering/i.test(nm)) nameMatchedStatusId = id;
    }
    statusDebug = { count: list.length, sample: list.slice(0, 3), matches: list.filter((s) => /stock|order/i.test(s.name || s.statusName || '')).map((s) => ({ id: s.orderStatusId ?? s.id, name: s.name || s.statusName })) };
  } catch (e) { statusDebug = { error: e.message }; }

  // 2. All SO ids in the demand status — PAGINATED (production findContributors
  //    caps at pageSize 500 with no paging, so surface if live exceeds that).
  let ids = orderIds, resultsAvailable = null;
  if (!ids || !ids.length) {
    ids = [];
    let firstResult = 1;
    for (let guard = 0; guard < 40; guard++) {
      const s = await liveGet(`/order-service/sales-order-search?orderStatusId=${DEMAND_STATUS}&pageSize=500&firstResult=${firstResult}`);
      const idx = {}; s.metaData.columns.forEach((c, i) => { idx[c.name] = i; });
      const page = s.results.map((r) => r[idx.salesOrderId]);
      ids.push(...page);
      resultsAvailable = s.metaData.resultsAvailable;
      if (!s.metaData.morePagesAvailable || !page.length) break;
      firstResult += 500;
      await pause(250);
    }
  }

  // 3. Custom fields per order (the /custom-field sub-resource is single-id only —
  //    an id-set 404s), paced to stay gentle on the shared live rate limit → apply
  //    the EXACT findContributors tag filter.
  const skipped = { noTag: 0, leaveNote: 0, otherSupplier: 0, alreadyHasPo: 0, noMatchingRows: 0 };
  const tagCounts = {};   // every distinct PCF_SUPPLIER tag value seen in the pool → count
  const candidates = [];
  for (const id of ids) {
    const cf = (await liveGet(`/order-service/order/${id}/custom-field`)) || {};
    await pause(120);
    const tag = cf.PCF_SUPPLIER;
    if (tag) tagCounts[tag] = (tagCounts[tag] || 0) + 1;
    if (!tag) { skipped.noTag++; continue; }
    if (isLeaveNote(tag)) { skipped.leaveNote++; continue; }
    if (!tagsOf(tag).some((t) => t.toUpperCase() === sup.key)) { skipped.otherSupplier++; continue; }
    if (sup.poField && cf[sup.poField]) { skipped.alreadyHasPo++; continue; }
    candidates.push({ id, tag });
  }

  // 4. Full order for each candidate (id-set batches, per-order fallback) → rows.
  const orderById = {};
  for (const c of chunk(candidates.map((x) => x.id), 60)) {
    try {
      const arr = await liveGet(`/order-service/order/${c.join(',')}`) || [];
      for (const o of (Array.isArray(arr) ? arr : [arr])) orderById[String(o.id)] = o;
    } catch {
      for (const id of c) { try { const a = await liveGet(`/order-service/order/${id}`); const o = Array.isArray(a) ? a[0] : a; if (o) orderById[String(o.id)] = o; await pause(120); } catch { /* skip */ } }
    }
    await pause(200);
  }
  const orders = [];
  for (const cand of candidates) {
    const order = orderById[String(cand.id)];
    if (!order) { skipped.noMatchingRows++; continue; }
    let rows = Object.values(order.orderRows).filter((r) => !isNoteRow(r) && sup.detect(r.productName, r.productSku));
    const allTags = tagsOf(cand.tag);
    if (!rows.length && allTags.length === 1 && allTags[0].toUpperCase() === sup.key) {
      rows = Object.values(order.orderRows).filter((r) => !isNoteRow(r));
    }
    if (!rows.length) { skipped.noMatchingRows++; continue; }
    orders.push({
      orderId: cand.id,
      ref: order.reference || '',
      tag: cand.tag,
      otherSuppliersOnOrder: tagsOf(cand.tag).filter((t) => t.toUpperCase() !== sup.key),
      lineCount: rows.length,
      totalQty: rows.reduce((a, r) => a + parseFloat(r.quantity.magnitude), 0),
      lines: rows.map((r) => ({
        productId: r.productId, sku: r.productSku, name: r.productName,
        qty: parseFloat(r.quantity.magnitude),
        size: optValue(r.productOptions, /size/i), colour: optValue(r.productOptions, /colou?r/i),
      })),
    });
  }

  return {
    live: true, readOnly: true, wrote: false,
    account: process.env.BRIGHTPEARL_ACCOUNT_ID,
    supplier: sup.key,
    demandStatusId: DEMAND_STATUS,
    demandStatusNameOnLive: statusName,                       // must read "Stock needs ordering"
    statusIdNamed_StockNeedsOrdering: nameMatchedStatusId,    // sanity: should equal demandStatusId
    statusDebug,
    tagDistribution: Object.entries(tagCounts).sort((a, b) => b[1] - a[1]).map(([tag, n]) => ({ tag, n })),
    ordersInDemandStatus: ids.length,
    resultsAvailable,
    productionCapWouldMiss: resultsAvailable != null ? Math.max(0, resultsAvailable - 500) : null,
    matchedOrderCount: orders.length,
    totalQty: orders.reduce((a, o) => a + o.totalQty, 0),
    totalLines: orders.reduce((a, o) => a + o.lineCount, 0),
    excludedFromStatusPool: skipped,
    orders,
  };
}

// ─────────────────────────────────────────────────────────────────────────────
// LIVE combined PO: SO-driven demand + a "=====LOW INV====" separator + reorder
// replenishment (BP Low Inventory report), on the LIVE account. Writes only when
// { execute: true } — otherwise returns the full plan (dry run). One PO, no split.
// ─────────────────────────────────────────────────────────────────────────────

// Live WRITE client (POST/PATCH/PUT). Only reachable through createComboPOLive,
// which is itself gated behind an explicit execute flag on the route.
async function liveWrite(method, path, body, attempt = 0) {
  const opts = { method, headers: LIVE_HEADERS() };
  if (body !== undefined) opts.body = JSON.stringify(body);
  const res = await fetch(`${LIVE_BASE()}${path}`, opts);
  if ((res.status === 429 || res.status === 503) && attempt < 5) {
    const wait = parseInt(res.headers.get('brightpearl-next-throttle-period') || '2000', 10);
    await new Promise((r) => setTimeout(r, Math.min(isNaN(wait) ? 2000 : wait, 60000) + 500));
    return liveWrite(method, path, body, attempt + 1);
  }
  const text = await res.text();
  let json = null; try { json = text ? JSON.parse(text) : null; } catch { /* non-JSON */ }
  if (!res.ok) { const e = new Error(`BP ${method} ${path} -> ${res.status}: ${text.slice(0, 200)}`); e.status = res.status; throw e; }
  return json ? json.response : null;
}

async function costOfLive(productId, priceListId, fallback = 0) {
  try {
    const resp = await liveGet(`/product-service/product-price/${productId}`);
    const pl = (resp[0].priceLists || []).find((x) => x.priceListId === priceListId);
    const v = pl && pl.quantityPrice && pl.quantityPrice['1'];
    return v ? parseFloat(v) : fallback;
  } catch { return fallback; }
}

async function skuToProductId(sku) {
  try {
    const r = await liveGet(`/product-service/product-search?SKU=${encodeURIComponent(sku)}&pageSize=10`);
    const idx = {}; r.metaData.columns.forEach((c, i) => { idx[c.name] = i; });
    const rows = r.results || [];
    const exact = rows.find((row) => String(row[idx.SKU]).toUpperCase() === String(sku).toUpperCase());
    const pick = exact || rows[0];
    return pick ? pick[idx.productId] : null;
  } catch { return null; }
}

// SO demand on live (status 23, tag contains supplierKey, not a leave-note, not
// already carrying this supplier's PO). Mirrors findContributors, GET-only reads.
async function gatherLiveDemand({ supplierKey, detect, poField }) {
  let ids = [], firstResult = 1;
  for (let g = 0; g < 40; g++) {
    const s = await liveGet(`/order-service/sales-order-search?orderStatusId=${DEMAND_STATUS}&pageSize=500&firstResult=${firstResult}`);
    const idx = {}; s.metaData.columns.forEach((c, i) => { idx[c.name] = i; });
    ids.push(...s.results.map((r) => r[idx.salesOrderId]));
    if (!s.metaData.morePagesAvailable || !s.results.length) break;
    firstResult += 500; await pause(250);
  }
  const contributors = [];
  for (const id of ids) {
    const cf = (await liveGet(`/order-service/order/${id}/custom-field`)) || {};
    await pause(120);
    const tag = cf.PCF_SUPPLIER;
    if (!tag || isLeaveNote(tag)) continue;
    if (!tagsOf(tag).some((t) => t.toUpperCase() === supplierKey)) continue;
    if (poField && cf[poField]) continue;
    const order = (await liveGet(`/order-service/order/${id}`))[0];
    await pause(120);
    // Rows for this supplier (keep the rowId — needed to look up allocation).
    // Single-supplier order (tag is just us) → the WHOLE order is ours, take every
    // orderable row (brand regexes miss un-branded/licensed lines). Multi-supplier
    // order → only the rows whose brand we detect, so we don't order another
    // supplier's items. Shipping/misc lines are excluded either way.
    const allTags = tagsOf(tag);
    const singleSupplier = allTags.length === 1 && allTags[0].toUpperCase() === supplierKey;
    const entries = Object.entries(order.orderRows).filter(([, r]) => !isNonOrderableRow(r) && (singleSupplier || detect(r.productName, r.productSku)));
    if (!entries.length) continue;
    // Only order the UNALLOCATED qty: ordered − allocated − fulfilled − onOrder.
    // Allocation isn't in the order API — read it from the legacy order page.
    const alloc = await getOrderAllocations(id, { client: process.env.BP_WEB_CLIENT_ID || 'tuffworkwear' });
    const rows = [];
    for (const [rowId, r] of entries) {
      const ordered = parseFloat(r.quantity.magnitude);
      const a = alloc[rowId] || { allocated: 0, fulfilled: 0, onOrder: 0 };
      const toOrder = ordered - a.allocated - a.fulfilled - a.onOrder;
      if (toOrder <= 0) continue; // fully allocated / already ordered — skip
      rows.push({ productId: r.productId, sku: r.productSku, name: r.productName, qty: toOrder, orderedQty: ordered, allocation: a, itemCost: r.itemCost ? parseFloat(r.itemCost.value) : 0, taxCode: (r.rowValue && r.rowValue.taxCode) || null });
    }
    if (!rows.length) continue;
    contributors.push({
      id, ref: order.reference || '', tag,
      lines: rows.map((r) => ({ productId: r.productId, sku: r.sku, name: r.name, qty: r.qty, orderedQty: r.orderedQty, allocation: r.allocation, itemCost: r.itemCost, taxCode: r.taxCode })),
    });
  }
  return contributors;
}

export async function createComboPOLive(opts = {}) {
  const supplierKey = String(opts.supplierKey || 'FRISTADS').toUpperCase();
  const reg = SUPPLIERS[supplierKey] || {};                        // registry = single source of truth
  const detect = opts.detect || reg.detect || dynamicDetect(supplierKey);
  const contactId = opts.contactId || reg.contactId || 37419;      // supplier BP contact id
  const priceListId = opts.priceListId || reg.costList || 20;      // Launch (cost) price list — where pricing updates write
  const poField = opts.poField !== undefined ? opts.poField : (reg.poField || null);
  const lowInvSupplierId = opts.lowInvSupplierId || reg.lowInvSupplierId || contactId;
  const excludeStatusIds = opts.excludeStatusIds || NON_DEMAND_SO_STATUS_IDS;
  const reference = opts.reference || `Auto-PO ${supplierKey}`;
  const execute = opts.execute === true;
  // Day-3 top-up: if the normal order is under this free-delivery threshold, pad the
  // low-inv lines by up to padMaxPct ABOVE each item's minimum stock level (spread
  // evenly) to reach it — but ONLY if the pad can actually cross the threshold; if not,
  // leave the normal quantities (carriage applies, no pointless over-order).
  const padToThreshold = Number(opts.padToThreshold) || 0;
  const padMaxPct = opts.padMaxPct != null ? Number(opts.padMaxPct) : 0.40;

  // 1. SO-driven demand + per-SKU qty (for dedupe)
  const contributors = await gatherLiveDemand({ supplierKey, detect, poField });
  const soLines = []; const soQtyBySku = {};
  for (const c of contributors) for (const l of c.lines) {
    const cost = await costOfLive(l.productId, priceListId, l.itemCost);
    soLines.push({ productId: l.productId, sku: l.sku, name: l.name, qty: l.qty, cost, order: c.id, taxCode: l.taxCode });
    const k = String(l.sku).toUpperCase(); soQtyBySku[k] = (soQtyBySku[k] || 0) + l.qty;
  }

  // 2. low-inventory replenishment (deduped against SO qty for the same SKU)
  const { fetchLowInventory } = await import('./lowInventory.js');
  let statusIds = []; try { statusIds = await liveSalesOrderStatusIds(excludeStatusIds); } catch { /* report default */ }
  const li = await fetchLowInventory({ supplierId: lowInvSupplierId, statusIds, numResults: 10000 });
  const lowLines = [];
  for (const d of li.rows) {
    if (d.orderQty <= 0) continue;
    const soQ = soQtyBySku[String(d.sku).toUpperCase()] || 0;
    const qty = Math.max(0, d.orderQty - soQ);          // dedupe: SO units already ordered above the line
    if (qty <= 0) continue;
    const productId = await skuToProductId(d.sku);
    const cost = productId ? await costOfLive(productId, priceListId, 0) : 0;
    lowLines.push({ productId, sku: d.sku, name: d.name, qty, cost, unresolved: !productId });
  }

  // 2b. DAY-3 TOP-UP — pad low-inv up to padMaxPct above each item's min stock to reach
  // the free-delivery threshold (spread evenly), but only if it can actually reach it.
  let padInfo = null;
  const lineNet = (arr) => arr.reduce((a, l) => a + (l.cost || 0) * l.qty, 0);
  if (padToThreshold > 0) {
    const netNormal = lineNet(soLines) + lineNet(lowLines);
    if (netNormal < padToThreshold) {
      // Build pad candidates from the low-inv report rows (incl. at-min items ordering 0).
      const bySku = {}; for (const l of lowLines) bySku[String(l.sku).toUpperCase()] = l;
      const candidates = [];
      for (const d of li.rows) {
        const min = Number(d.minStock) || 0;
        if (min <= 0) continue;
        const padCeiling = Math.ceil(min * (1 + padMaxPct));                 // e.g. ceil(7*1.4)=10
        const existing = bySku[String(d.sku).toUpperCase()];
        const alreadyOrdering = existing ? existing.qty : 0;
        const projected = (Number(d.onHand) || 0) + (Number(d.onPO) || 0) - (Number(d.openSO) || 0) + alreadyOrdering;
        const cap = Math.max(0, padCeiling - Math.max(min, projected));      // extra units allowed above the normal plan
        if (cap <= 0) continue;
        let productId = existing ? existing.productId : await skuToProductId(d.sku);
        if (!productId) continue;                                             // can't pad an unresolvable SKU
        const cost = existing ? existing.cost : await costOfLive(productId, priceListId, 0);
        if (!(cost > 0)) continue;
        candidates.push({ sku: d.sku, name: d.name, productId, cost, cap, existing });
      }
      const maxExtraValue = candidates.reduce((a, c) => a + c.cap * c.cost, 0);
      if (netNormal + maxExtraValue >= padToThreshold && candidates.length) {
        // Reachable — distribute extra units round-robin (even spread) until we cross it.
        const added = new Map(); let running = netNormal, guard = 0;
        while (running < padToThreshold && guard++ < 100000) {
          let any = false;
          for (const c of candidates) {
            const used = added.get(c) || 0;
            if (used >= c.cap) continue;
            added.set(c, used + 1); running += c.cost; any = true;
            if (running >= padToThreshold) break;
          }
          if (!any) break;
        }
        let extraUnits = 0;
        for (const [c, n] of added) {
          if (n <= 0) continue; extraUnits += n;
          if (c.existing) c.existing.qty += n;
          else lowLines.push({ productId: c.productId, sku: c.sku, name: c.name, qty: n, cost: c.cost, padded: true });
        }
        padInfo = { padded: true, threshold: padToThreshold, netBefore: +netNormal.toFixed(2), netAfter: +running.toFixed(2), extraUnits, maxPct: padMaxPct };
      } else {
        // Even the full +40% can't reach the threshold — order normal qty + carriage.
        padInfo = { padded: false, reason: 'max +' + Math.round(padMaxPct * 100) + '% still under threshold — carriage applies', threshold: padToThreshold, netNormal: +netNormal.toFixed(2), maxReachable: +(netNormal + maxExtraValue).toFixed(2) };
      }
    }
  }

  const plan = {
    supplierKey, contactId, priceListId, reference, warehouseId: WAREHOUSE_ID,
    soLines, separator: '=====LOW INV====', lowLines, padInfo,
    soUnits: soLines.reduce((a, l) => a + l.qty, 0),
    lowUnits: lowLines.reduce((a, l) => a + l.qty, 0),
    unresolvedSkus: lowLines.filter((l) => l.unresolved).map((l) => l.sku),
  };

  if (!execute) return { dryRun: true, ...plan };
  if (plan.unresolvedSkus.length) return { created: false, reason: `unresolved low-inv SKUs (aborted, nothing written): ${plan.unresolvedSkus.join(', ')}`, ...plan };
  if (!soLines.length && !lowLines.length) return { created: false, reason: 'nothing to order', ...plan };

  // 3. WRITE — PO header (Pending PO), rows (SO, then separator, then low-inv), note, SO stamps.
  const poId = await liveWrite('POST', '/order-service/order', {
    orderTypeCode: 'PO', reference, priceListId, priceModeCode: 'EXC',
    warehouseId: WAREHOUSE_ID, currency: { orderCurrencyCode: 'GBP' },
    parties: { supplier: { contactId } },
  });
  // Per-row VAT: use the row's tax code (SO line carries it; else the product's default);
  // rowTax = net × the code's rate. Never blanket T20 (would over-tax zero-rated items).
  const addRow = async (productId, qty, cost, nameOverride, taxCode) => {
    const net = cost * qty;
    const code = taxCode || (String(productId) === '1000' ? 'T20' : await productTaxCodeLive(productId));
    const rate = taxRate(code);
    const body = { productId, quantity: { magnitude: String(qty) }, rowValue: { taxCode: code, rowNet: { currency: 'GBP', value: net.toFixed(2) }, rowTax: { currency: 'GBP', value: (net * rate).toFixed(2) } } };
    if (nameOverride) body.productName = nameOverride;
    await liveWrite('POST', `/order-service/order/${poId}/row`, body);
    await pause(150);
  };
  // Bundle products can't be added to a PO (BP ORDC-023) — you order their components, not
  // the kit. Skip any such row so ONE bundle doesn't abort the whole PO; report them so the
  // components can be handled manually. Genuine (non-bundle) row errors still throw.
  const skippedBundles = [];
  const tryRow = async (l, from) => {
    try { await addRow(l.productId, l.qty, l.cost, undefined, l.taxCode); }
    catch (e) { if (/ORDC-023|bundle/i.test(e.message || '')) skippedBundles.push({ sku: l.sku, name: l.name, productId: l.productId, qty: l.qty, from }); else throw e; }
  };
  for (const l of soLines) await tryRow(l, 'SO');
  await addRow(1000, 1, 0, '=====LOW INV====');           // separator note row (net 0 → no tax)
  for (const l of lowLines) await tryRow(l, 'low-inv');

  // note (SO#nnn render as clickable links)
  const nl = [`Auto-PO for ${supplierKey}.`];
  if (soLines.length) { nl.push('Order demand from:'); for (const c of contributors) nl.push(`  SO#${c.id} (${c.ref}): ` + c.lines.map((l) => `${l.sku} x${l.qty}`).join(', ')); }
  if (lowLines.length) { nl.push('Low-inventory replenishment:'); for (const l of lowLines) nl.push(`  ${l.sku} x${l.qty}`); }
  if (skippedBundles.length) { nl.push('⚠ SKIPPED — bundles (cannot add to a PO; order the components manually):'); for (const b of skippedBundles) nl.push(`  ${b.sku || b.productId} x${b.qty} (${b.name || ''})`); }
  const addedOn = new Date().toISOString().replace('Z', '+00:00');
  await liveWrite('POST', `/order-service/order/${poId}/note`, { text: nl.join('\n'), addedOn, contactId, isPublic: false });

  // stamp the PO number onto each contributing SO (linkage + dedupe) — only if this
  // supplier has a dedicated PO custom field. Otherwise the tag-clear on finalize is
  // the sole re-pickup guard.
  if (poField) for (const c of contributors) await liveWrite('PATCH', `/order-service/order/${c.id}/custom-field`, [{ op: 'add', path: `/${poField}`, value: String(poId) }]);

  return { created: true, poId, skippedBundles, ...plan };
}

// LIVE: strip a supplier from the SUPPLIERS-NEEDED tag (PCF_SUPPLIER) on ordered
// SOs so they aren't picked up again. If the supplier was the only tag, the field
// is cleared; optionally flip status to "Ordered Stock Awaiting Delivery" (22).
// Dry-run unless { execute: true }.
export async function finalizeSupplierTagsLive({ orderIds = [], supplierKey = 'FRISTADS', poId = null, noteContactId = null, setOrderedStatus = false, linesByOrder = null, execute = false } = {}) {
  // linesByOrder: { <soId>: ["<item name>", ...] } — when given, the SO note lists the
  // ordered item names (one per line) then "Ordered on PO:<poId>".
  const key = String(supplierKey).toUpperCase();
  const plan = [];
  for (const id of orderIds) {
    const cf = (await liveGet(`/order-service/order/${id}/custom-field`)) || {};
    const before = cf.PCF_SUPPLIER || '';
    const remaining = tagsOf(before).filter((t) => t.toUpperCase() !== key);
    plan.push({ id, before, after: remaining.join(' / '), willClear: remaining.length === 0 });
    await pause(120);
  }
  if (!execute) return { dryRun: true, supplierKey: key, poId, setOrderedStatus, plan };
  const results = [];
  for (const p of plan) {
    // tag: only PATCH if there was something to change (avoid op:remove on an empty field)
    if (p.before && p.after) await liveWrite('PATCH', `/order-service/order/${p.id}/custom-field`, [{ op: 'add', path: '/PCF_SUPPLIER', value: p.after }]);
    else if (p.before && !p.after) await liveWrite('PATCH', `/order-service/order/${p.id}/custom-field`, [{ op: 'remove', path: '/PCF_SUPPLIER' }]);
    // status: → Ordered Stock Awaiting Delivery ONLY when no supplier remains (else stays on SNO)
    let statusChanged = false;
    if (setOrderedStatus && p.willClear) { await liveWrite('PUT', `/order-service/order/${p.id}/status`, { orderStatusId: ORDERED_STATUS }); statusChanged = true; }
    // note on the SO: the item names ordered for this SO, then "Ordered on PO#<poId>".
    // The '#' before the id is REQUIRED — BP only renders a clickable order link for
    // the "#<orderId>" pattern (was "PO:<id>", which stayed plain text).
    let noted = false;
    if (poId) {
      const names = (linesByOrder && (linesByOrder[p.id] || linesByOrder[String(p.id)])) || null;
      const text = (names && names.length)
        ? `${names.join('\n')}\nOrdered on PO#${poId}`
        : `${key} items ordered via PO#${poId}`;
      const addedOn = new Date().toISOString().replace('Z', '+00:00');
      await liveWrite('POST', `/order-service/order/${p.id}/note`, { text, addedOn, contactId: noteContactId || 1, isPublic: false });
      noted = true;
    }
    results.push({ id: p.id, tag: p.after || '(cleared)', keptOnSNO: !p.willClear, statusChanged, noted });
    await pause(150);
  }
  return { done: true, supplierKey: key, poId, setOrderedStatus, results };
}

// Stamp a supplier's PO custom field (e.g. PCF_CASTLEPO) with the PO id on each SO.
// Used to back-fill orders whose PO was created before the field was known, and as a
// belt-and-braces re-stamp. poField resolves from the SUPPLIERS registry if not given.
// Dry-run unless { execute: true }.
export async function stampPoFieldLive({ orderIds = [], supplierKey, poField, poId, execute = false } = {}) {
  const field = poField || (SUPPLIERS[String(supplierKey || '').toUpperCase()] || {}).poField;
  if (!field) return { error: `no PO custom field known for supplier ${supplierKey}` };
  if (!poId) return { error: 'poId required' };
  if (!execute) return { dryRun: true, field, poId, orderIds };
  const results = [];
  for (const id of orderIds) {
    await liveWrite('PATCH', `/order-service/order/${id}/custom-field`, [{ op: 'add', path: `/${field}`, value: String(poId) }]);
    results.push({ id, stamped: field, value: String(poId) });
    await pause(150);
  }
  return { done: true, field, poId, results };
}

// Re-price an existing PO's rows from a price list (BP has no row-value update, so
// each row is deleted + re-added at the correct cost, preserving order + separator).
// Dry-run unless { execute: true }. Used to correct a PO built on the wrong list.
export async function repriceComboPOLive({ poId, priceListId = 20, keepNet = false, execute = false } = {}) {
  // keepNet=true: keep each row's CURRENT net (don't recompute from the price list)
  // and just re-add — used to RESTORE row tax after the legacy reference-write zeroes
  // it (the API stores explicit rowTax; only the legacy form drops it).
  const order = (await liveGet(`/order-service/order/${poId}`))[0];
  const entries = Object.entries(order.orderRows || {}).sort((a, b) => Number(a[0]) - Number(b[0])); // ascending rowId = creation order
  const plan = [];
  for (const [rowId, r] of entries) {
    const isSep = String(r.productId) === '1000';
    const qty = parseFloat(r.quantity.magnitude);
    const oldNet = parseFloat((r.rowValue && r.rowValue.rowNet && r.rowValue.rowNet.value) || 0);
    const cost = isSep ? 0 : (keepNet ? (qty ? oldNet / qty : 0) : await costOfLive(r.productId, priceListId, qty ? oldNet / qty : 0));
    plan.push({ rowId, productId: r.productId, qty, name: r.productName, isSep, oldNet: oldNet.toFixed(2), newNet: (cost * qty).toFixed(2), cost });
  }
  if (!execute) return { dryRun: true, poId, priceListId, plan };
  for (const p of plan) { await liveWrite('DELETE', `/order-service/order/${poId}/row/${p.rowId}`); await pause(150); }
  for (const p of plan) {
    const net = p.cost * p.qty;
    // Restore each row's REAL VAT from the product's tax code (the reference-write can
    // reset it) — not a blanket T20, which was over-taxing zero-rated items.
    const code = p.isSep ? 'T20' : await productTaxCodeLive(p.productId);
    const rate = taxRate(code);
    const body = { productId: p.productId, quantity: { magnitude: String(p.qty) }, rowValue: { taxCode: code, rowNet: { currency: 'GBP', value: net.toFixed(2) }, rowTax: { currency: 'GBP', value: (net * rate).toFixed(2) } } };
    if (p.isSep) body.productName = '=====LOW INV====';
    await liveWrite('POST', `/order-service/order/${poId}/row`, body);
    await pause(150);
  }
  return { done: true, poId, priceListId, rows: plan.length };
}

// Create the Pending PO (+ source note) and stamp the PO number onto each
// contributing order (linkage + dedupe). Does NOT strip tags or change status.
export async function createPO(supplierKey, { orderIds, dryRun } = {}) {
  const sup = await resolveSupplier(supplierKey);
  const contributors = await findContributors(sup, orderIds);
  const plan = summarise(sup, contributors);
  if (dryRun) return { dryRun: true, ...plan };
  if (!contributors.length) return { created: false, reason: 'no demand', ...plan };

  // 1. PO header (defaults to Pending PO). Omit delivery block (shippingMethodId 0 rejected).
  const poId = await api('POST', '/order-service/order', {
    orderTypeCode: 'PO',
    reference: `Auto-PO ${sup.key}`,
    priceListId: sup.costList != null ? sup.costList : 3,
    priceModeCode: 'EXC',
    warehouseId: WAREHOUSE_ID,
    currency: { orderCurrencyCode: 'GBP' },
    parties: { supplier: { contactId: sup.contactId } },
  });

  // 2. rows
  for (const c of contributors) {
    for (const l of c.lines) {
      const net = l.cost * l.qty;
      await api('POST', `/order-service/order/${poId}/row`, {
        productId: l.productId,
        quantity: { magnitude: String(l.qty) },
        rowValue: {
          taxCode: 'T20',
          rowNet: { currency: 'GBP', value: net.toFixed(2) },
          rowTax: { currency: 'GBP', value: (net * 0.2).toFixed(2) },
        },
      });
    }
  }

  // 3. source note on the PO (SO#nnn renders as a clickable order link in BP)
  const noteText = `Auto-PO for ${sup.key}. Lines sourced from:\n` +
    contributors.map((c) => `  SO#${c.id} (${c.ref}): ` + c.lines.map((l) => `${l.sku} x${l.qty}`).join(', ')).join('\n');
  await addOrderNote(poId, noteText, sup.contactId);

  // 4. stamp the PO number onto each contributing SO (linkage + dedupe)
  for (const c of contributors) {
    if (sup.poField) await api('PATCH', `/order-service/order/${c.id}/custom-field`, [{ op: 'add', path: `/${sup.poField}`, value: String(poId) }]);
  }

  return { created: true, poId, ...plan };
}

export const PO_BACKORDER_STATUS = 45; // "On Back Order"
export const PLACED_WITH_SUPPLIER_STATUS = 7;

// Cart lines for a PO's product rows (skip the =====LOW INV==== separator) — {sku,
// size, qty}. Size comes from the row's productOptions (needed by the portal cart).
export async function getOrderCartLines(orderId) {
  const order = (await liveGet(`/order-service/order/${orderId}`))[0];
  const lines = [];
  for (const r of Object.values(order.orderRows || {})) {
    if (String(r.productId) === '1000') continue;
    lines.push({ sku: r.productSku, size: optValue(r.productOptions, /size/i), qty: parseFloat(r.quantity.magnitude) });
  }
  return lines;
}

// LIVE: set a PO's status (e.g. → 7 "Placed with supplier" after the supplier order
// is placed). Uses the live write client. Single call.
export async function setOrderStatusLive(orderId, statusId) {
  await liveWrite('PUT', `/order-service/order/${orderId}/status`, { orderStatusId: statusId });
  return { orderId, statusId };
}

// Create a PO for a supplier from explicit line items — used to split demand
// into a main PO (in-stock qty) and a separate back-order PO (shortfall qty).
// lineItems = [{ productId, qty, cost }]. opts: { reference, status, note }.
export async function createSupplierPO(supplierKey, lineItems, opts = {}) {
  const sup = await resolveSupplier(supplierKey);
  if (!lineItems.length) return { created: false, reason: 'no lines' };
  const poId = await api('POST', '/order-service/order', {
    orderTypeCode: 'PO',
    reference: opts.reference || `Auto-PO ${sup.key}`,
    ...(opts.parentOrderId ? { parentOrderId: opts.parentOrderId } : {}),
    priceListId: sup.costList != null ? sup.costList : 3,
    priceModeCode: 'EXC',
    warehouseId: WAREHOUSE_ID,
    currency: { orderCurrencyCode: 'GBP' },
    parties: { supplier: { contactId: sup.contactId } },
  });
  for (const l of lineItems) {
    const net = (l.cost || 0) * l.qty;
    await api('POST', `/order-service/order/${poId}/row`, {
      productId: l.productId,
      quantity: { magnitude: String(l.qty) },
      rowValue: { taxCode: 'T20', rowNet: { currency: 'GBP', value: net.toFixed(2) }, rowTax: { currency: 'GBP', value: (net * 0.2).toFixed(2) } },
    });
  }
  if (opts.note) await addOrderNote(poId, opts.note, sup.contactId);
  if (opts.status) await api('PUT', `/order-service/order/${poId}/status`, { orderStatusId: opts.status });
  return { created: true, poId };
}

// Stamp a supplier PO number into an order's per-supplier PO custom field.
export async function stampPoField(supplierKey, orderId, poId) {
  const sup = await resolveSupplier(supplierKey);
  if (!sup.poField) return { skipped: true, reason: 'no poField for supplier' };
  return api('PATCH', `/order-service/order/${orderId}/custom-field`, [{ op: 'add', path: `/${sup.poField}`, value: String(poId) }]);
}

// Post-placement: strip the supplier from the tag, flip status when it was the
// last supplier, and add an "ordered via PO N" note. Call after the order has
// actually been placed with the supplier.
export async function finalizePO(supplierKey, { poId, orderIds, notes } = {}) {
  const sup = await resolveSupplier(supplierKey);
  if (!poId) throw new Error('poId required');
  if (!orderIds || !orderIds.length) throw new Error('orderIds required');
  const results = [];
  for (const id of orderIds) {
    const cf = (await api('GET', `/order-service/order/${id}/custom-field`)) || {};
    const remaining = tagsOf(cf.PCF_SUPPLIER).filter((t) => t.toUpperCase() !== sup.key);
    if (remaining.length) {
      await api('PATCH', `/order-service/order/${id}/custom-field`, [{ op: 'add', path: '/PCF_SUPPLIER', value: remaining.join(' / ') }]);
    } else {
      await api('PATCH', `/order-service/order/${id}/custom-field`, [{ op: 'remove', path: '/PCF_SUPPLIER' }]);
      await api('PUT', `/order-service/order/${id}/status`, { orderStatusId: ORDERED_STATUS });
    }
    await addOrderNote(id, (notes && notes[id]) || `${sup.key} items ordered via PO#${poId}`, sup.contactId);
    results.push({ orderId: id, tagAfter: remaining.join(' / '), completed: remaining.length === 0 });
  }
  return { finalized: true, poId, results };
}

// Order note. addedOn is required by BP; caller has no clock dependency here so
// we send the current time (this runs server-side, not in a workflow script).
export async function addOrderNote(orderId, text, contactId) {
  const addedOn = new Date().toISOString().replace('Z', '+00:00');
  return api('POST', `/order-service/order/${orderId}/note`, { text, addedOn, contactId: contactId || 1, isPublic: false });
}

// channelId -> { name, provider } (cached). Used to spot Magento (website) orders.
let _channelMap = null;
export async function getChannelMap() {
  if (_channelMap) return _channelMap;
  const chs = await api('GET', '/product-service/channel');
  _channelMap = {};
  for (const c of chs || []) _channelMap[c.id] = { name: c.name, provider: c.integrationDetail && c.integrationDetail.providerCode };
  return _channelMap;
}

// staff contactId -> email (cached).
const _staffEmail = {};
export async function staffEmailOf(contactId) {
  if (!contactId) return null;
  if (contactId in _staffEmail) return _staffEmail[contactId];
  let email = null;
  try {
    const c = await api('GET', `/contact-service/contact/${contactId}`);
    const emails = c && c[0] && c[0].communication && c[0].communication.emails;
    if (emails) { const first = Object.values(emails)[0]; email = (first && first.email) || null; }
  } catch { /* leave null */ }
  _staffEmail[contactId] = email;
  return email;
}

// A supplier's order email, taken from their Brightpearl contact record (the
// email on the supplier details of the PO). Used to email POs to email-method
// suppliers. Resolves the registry contactId → contact → communication.emails.
// The order email as it sits ON the created PO (the supplier party's snapshot email).
// This is the authoritative address for the email-PO — it's exactly who the PO is for.
export async function poSupplierEmail(poId) {
  try {
    const o = await api('GET', `/order-service/order/${poId}`);
    const sup = o && o[0] && o[0].parties && o[0].parties.supplier;
    return (sup && sup.email) || null;
  } catch { return null; }
}

export async function supplierEmailOf(supplierKey) {
  try {
    const sup = await resolveSupplier(supplierKey);
    const c = await api('GET', `/contact-service/contact/${sup.contactId}`);
    const emails = c && c[0] && c[0].communication && c[0].communication.emails;
    if (!emails) return null;
    // Prefer the PRIMARY email (the supplier's order address), else any.
    return (emails.PRI && emails.PRI.email) || (Object.values(emails)[0] || {}).email || null;
  } catch { return null; }
}

// A specific contact's email by id (e.g. the supplier party on a PO).
export const contactEmailOf = (contactId) => staffEmailOf(contactId);

// Who should get the stock email for an order: Magento (website) orders go to
// the sales email; everything else goes to the staff member who created it.
export async function orderRecipient(order, salesEmail) {
  const cm = await getChannelMap();
  const ch = cm[order.channelId];
  if (ch && (/magento/i.test(ch.name || '') || /magento/i.test(ch.provider || ''))) return salesEmail;
  return (await staffEmailOf(order.createdById)) || salesEmail;
}
