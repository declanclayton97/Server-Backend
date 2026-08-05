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
  // Ralawise = distributor (Stanley Stella exclusive + Gildan/AWDis/etc). Detect by
  // Stanley Stella name OR a Ralawise-format SKU (2 letters + 3 digits + …).
  RALAWISE:     { contactId: 205,   costList: null, poField: 'PCF_RALAWPO', detect: (n, sku) => /stanley\s*stella/i.test(n || '') || /^[A-Z]{2}\d{3}[A-Z0-9]/.test(String(sku || '').replace(/[\s_-]/g, '')) },
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
    let rows = Object.values(order.orderRows).filter((r) => !isNoteRow(r) && detect(r.productName, r.productSku));
    const allTags = tagsOf(tag);
    if (!rows.length && allTags.length === 1 && allTags[0].toUpperCase() === supplierKey) rows = Object.values(order.orderRows).filter((r) => !isNoteRow(r));
    if (!rows.length) continue;
    contributors.push({
      id, ref: order.reference || '', tag,
      lines: rows.map((r) => ({ productId: r.productId, sku: r.productSku, name: r.productName, qty: parseFloat(r.quantity.magnitude), itemCost: r.itemCost ? parseFloat(r.itemCost.value) : 0 })),
    });
  }
  return contributors;
}

export async function createComboPOLive(opts = {}) {
  const supplierKey = String(opts.supplierKey || 'FRISTADS').toUpperCase();
  const detect = opts.detect || ((n) => /fristads/i.test(n || ''));
  const contactId = opts.contactId || 37419;         // Fristads Workwear Ltd
  const priceListId = opts.priceListId || 20;         // Launch (cost) price list — the one the pricing updates write to
  const poField = opts.poField || 'PCF_FRISTPO';
  const lowInvSupplierId = opts.lowInvSupplierId || 37419;
  const excludeStatusIds = opts.excludeStatusIds || NON_DEMAND_SO_STATUS_IDS;
  const reference = opts.reference || `Auto-PO ${supplierKey}`;
  const execute = opts.execute === true;

  // 1. SO-driven demand + per-SKU qty (for dedupe)
  const contributors = await gatherLiveDemand({ supplierKey, detect, poField });
  const soLines = []; const soQtyBySku = {};
  for (const c of contributors) for (const l of c.lines) {
    const cost = await costOfLive(l.productId, priceListId, l.itemCost);
    soLines.push({ productId: l.productId, sku: l.sku, name: l.name, qty: l.qty, cost, order: c.id });
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

  const plan = {
    supplierKey, contactId, priceListId, reference, warehouseId: WAREHOUSE_ID,
    soLines, separator: '=====LOW INV====', lowLines,
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
  const addRow = async (productId, qty, cost, nameOverride) => {
    const net = cost * qty;
    const body = { productId, quantity: { magnitude: String(qty) }, rowValue: { taxCode: 'T20', rowNet: { currency: 'GBP', value: net.toFixed(2) }, rowTax: { currency: 'GBP', value: (net * 0.2).toFixed(2) } } };
    if (nameOverride) body.productName = nameOverride;
    await liveWrite('POST', `/order-service/order/${poId}/row`, body);
    await pause(150);
  };
  for (const l of soLines) await addRow(l.productId, l.qty, l.cost);
  await addRow(1000, 1, 0, '=====LOW INV====');           // separator note row
  for (const l of lowLines) await addRow(l.productId, l.qty, l.cost);

  // note (SO#nnn render as clickable links)
  const nl = [`Auto-PO for ${supplierKey}.`];
  if (soLines.length) { nl.push('Order demand from:'); for (const c of contributors) nl.push(`  SO#${c.id} (${c.ref}): ` + c.lines.map((l) => `${l.sku} x${l.qty}`).join(', ')); }
  if (lowLines.length) { nl.push('Low-inventory replenishment:'); for (const l of lowLines) nl.push(`  ${l.sku} x${l.qty}`); }
  const addedOn = new Date().toISOString().replace('Z', '+00:00');
  await liveWrite('POST', `/order-service/order/${poId}/note`, { text: nl.join('\n'), addedOn, contactId, isPublic: false });

  // stamp the PO number onto each contributing SO (linkage + dedupe)
  for (const c of contributors) await liveWrite('PATCH', `/order-service/order/${c.id}/custom-field`, [{ op: 'add', path: `/${poField}`, value: String(poId) }]);

  return { created: true, poId, ...plan };
}

// LIVE: strip a supplier from the SUPPLIERS-NEEDED tag (PCF_SUPPLIER) on ordered
// SOs so they aren't picked up again. If the supplier was the only tag, the field
// is cleared; optionally flip status to "Ordered Stock Awaiting Delivery" (22).
// Dry-run unless { execute: true }.
export async function finalizeSupplierTagsLive({ orderIds = [], supplierKey = 'FRISTADS', poId = null, noteContactId = null, setOrderedStatus = false, execute = false } = {}) {
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
    // note on the SO (the "ordered via PO#" note previously discussed)
    let noted = false;
    if (poId) {
      const addedOn = new Date().toISOString().replace('Z', '+00:00');
      await liveWrite('POST', `/order-service/order/${p.id}/note`, { text: `${key} items ordered via PO#${poId}`, addedOn, contactId: noteContactId || 1, isPublic: false });
      noted = true;
    }
    results.push({ id: p.id, tag: p.after || '(cleared)', keptOnSNO: !p.willClear, statusChanged, noted });
    await pause(150);
  }
  return { done: true, supplierKey: key, poId, setOrderedStatus, results };
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
    const body = { productId: p.productId, quantity: { magnitude: String(p.qty) }, rowValue: { taxCode: 'T20', rowNet: { currency: 'GBP', value: net.toFixed(2) }, rowTax: { currency: 'GBP', value: (net * 0.2).toFixed(2) } } };
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
