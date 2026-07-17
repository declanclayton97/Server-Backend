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
    const s = await api('GET', `/contact-service/contact-search?companyName=${encodeURIComponent(name)}&pageSize=5`);
    const idx = {}; s.metaData.columns.forEach((c, i) => { idx[c.name] = i; });
    const rows = s.results || [];
    if (rows.length) id = rows[0][idx.contactId];
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
