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
// Added 2026-08-10 (not committed demand): 34 Proof Required · 35 Proof Sent ·
//   44 Awaiting payment before despatch · 49 Pending Room–Awaiting Instruction ·
//   51 Await payment before Ordering in · 55 ON STOP DO NOT PROCESS ·
//   78 Awaiting Payment before Workshop · 117 Await Payment – Link Sent ·
//   91 Sportswear – Proof Required · 92 Sportswear – Proof Sent
export const NON_DEMAND_SO_STATUS_IDS = [1, 2, 18, 34, 35, 36, 44, 49, 51, 53, 55, 60, 78, 87, 89, 91, 92, 114, 117];
const ORDERED_STATUS = 22;  // Ordered Stock Awaiting Delivery
// Some sales orders carry a decoration instruction as a free-text row (productId 1000, the
// "Misc item" product, the same carrier as shipping rows): "+++ PLEASE PUT TO PROOF REQUIRED
// ONCE ORDERED +++". Those must land on Proof Required, NOT Ordered Stock, once their stock is
// on order (user, 2026-08-17 — SO 482510 went to 22 and had to be corrected by hand).
// 34 = "Proof Required-Send to Customer". NOTE Brightpearl also has 91 "Sportswear - Proof
// Required"; if sportswear orders need that instead, set PROOF_REQUIRED_STATUS_ID.
const PROOF_REQUIRED_STATUS = Number(process.env.PROOF_REQUIRED_STATUS_ID || 34);
// Staff write routing instructions as free-text rows on productId 1000, in one family:
//   "+++ PLEASE PUT TO <target status> ONCE <trigger> +++"
// A scan of 2,500 sales orders (2026-08-19) found FOURTEEN spellings of it, and BOTH halves matter:
//   target  — PROOF REQUIRED (34) · PROOF SENT (35) · ORDERED STOCK (22) · STOCK NEEDS ORDERING (23)
//   trigger — ORDERED · PICKED · PROOFED
// We finalise at the ORDERED trigger, so ONLY "ONCE ORDERED" rows may change the status here.
// "ONCE PICKED" (7 orders) and "ONCE PROOFED" (13) fire LATER in the process — acting on them at
// order time would advance the order early, so they are deliberately ignored.
// Tolerates the real spellings seen: with or without spaces inside the +++, "SET TO" for "PUT TO",
// a missing "PUT", the "PROOF REQUIED" typo, and trailing prose after the trigger word.
const STATUS_TARGETS = [
  [/PROOF\s*REQUI?R?E?D/, 34],          // matches both "PROOF REQUIRED" and the real-world "PROOF REQUIED" typo
  [/PROOF\s*SENT/, 35],
  [/ORDERED\s*STOCK/, 22],
  [/STOCK\s*NEEDS\s*ORDERING/, 23],
];
const normInstruction = (t) => String(t || '').toUpperCase().replace(/[+*#_]+/g, ' ').replace(/\s+/g, ' ').trim();
// → { statusId, marker } when a row says to change status AT ORDER TIME; otherwise null.
export function orderStatusInstruction(order) {
  for (const r of Object.values((order && order.orderRows) || {})) {
    const raw = String(r.productName || '');
    const m = /PLEASE\s+(?:PUT\s+|SET\s+)?TO\s+(.+?)\s+ONCE\s+([A-Z]+)/.exec(normInstruction(raw));
    if (!m) continue;
    if (!/^ORDERED$/.test(m[2])) continue;               // fires at picking/proofing, not at ordering
    for (const [re, id] of STATUS_TARGETS) if (re.test(m[1])) return { statusId: id, marker: raw.trim().slice(0, 120) };
  }
  return null;
}
// kept for compatibility: true when the instruction routes to any proof status
export const orderNeedsProof = (order) => { const i = orderStatusInstruction(order); return !!i && [34, 35, 91, 92].includes(i.statusId); };
// Per-SO "don't auto-order these items" blocklist custom field (user-created in BP). Value =
// SKU(s)/style code(s) to skip on that order (e.g. a pair already ordered elsewhere). Code is
// env-overridable so it matches whatever PCF code BP assigns.
const SKIP_SKU_FIELD = process.env.SKIP_SKU_FIELD || 'PCF_SKIPSKU';
const PENDING_PO_STATUS = 6; // (informational — POs default to this on create)
const WAREHOUSE_ID = 2;

// Supplier registry. Each entry: BP supplier contactId, the supplier's cost
// price list id, the per-supplier PO custom-field code, and a line detector
// (matches a product name/sku to this supplier — used to pick the supplier's
// rows out of a mixed order). Cost falls back to the SO row's itemCost if the
// cost list has no price for a product.
export const SUPPLIERS = {
  // Snickers = the HULTAFORS GROUP account, which supplies SIX brands, only two of which are
  // named "Snickers"/"Solid Gear": also Hellberg (ear defenders, SKUs like 48012-001), EMMA
  // (safety footwear, MM…), CLC (tool storage, CL…) and Toe Guard. Per the supplier sheet
  // ([[reference_supplier_carriage_terms]]) none of those four comes from any OTHER supplier, so
  // matching them here can't drag a rival supplier's line onto a Snickers PO. They were ordering
  // fine until d950468 (2026-08-07) made brand-detect suppliers filter by regex even on a sole
  // tag — that silently stopped Hellberg/EMMA/CLC demand being seen at all (8 SOs by 2026-08-17).
  // COST LIST = 20 (Launch) FOR EVERY SUPPLIER. The business prices on Launch and nothing else is
  // maintained; the per-supplier lists (Snickers 10, Portwest 7, Uneek 11, Blaklader 12, Helly
  // Hansen 6) were partial stale copies. HH list 6 held prices for only 2 of the 7 products on PO
  // 483239 — which is why that PO looked like it had no costs at all — and where it did exist it
  // matched Launch exactly. Sampled before the switch: Snickers and Blaklader identical on both,
  // Portwest DX411BKRS 6.95→7.50, Uneek 25599/25672/25670 2.90→2.70 and 27777 12.00→11.50.
  SNICKERS:     { contactId: 331,   costList: 20, poField: 'PCF_SNICKPO', detect: (n) => /snickers|solid\s*gear|hellberg|toe\s*guard|hultafors|\bemma\b|\bclc\b/i.test(n || '') },
  BLAKLADER:    { contactId: 323,   costList: 20, poField: 'PCF_BLAKLPO', detect: (n) => /bl[åa]kl[äa]der/i.test(n || '') },
  PORTWEST:     { contactId: 298,   costList: 20, poField: 'PCF_PORTWPO', detect: (n) => /portwest/i.test(n || '') }, // low-inv ON (min-stock data sorted 2026-08-14): SO demand + reorder
  UNEEK:        { contactId: 322,   costList: 20, poField: 'PCF_UNEEKPO', detect: (n) => /uneek/i.test(n || '') },
  'HELLY HANSEN': { contactId: 214, costList: 20, portalPriceIsPreDiscount: true, supplierDiscountPct: 0.42,  poField: 'PCF_HELLYPO', detect: (n) => /helly\s*hansen|hh\s*workwear/i.test(n || '') },
  // Launch(20) IS populated for Mascot — the null here was a config gap, not missing data (sampled
  // from PO 476715: EAN 5711074495160 → list20 9.41, EAN 5711074486861 → 11.77). Without it, costs
  // fell back to the sales-order row's itemCost and the price healer skipped Mascot entirely.
  // ⚠️ Do NOT heal Mascot costs from its B2B basket: that cart shows LIST price (the same polo reads
  // £15.95 there = list3/list13 RRP, vs the £9.41 real cost). Mascot applies the discount when the
  // order is created — which is what the button labelled "Check Discount" actually does — so the true
  // net price only exists after createOrder. See [[project_mascot_ordering]].
  MASCOT:       { contactId: 334,   costList: 20,   poField: 'PCF_MASCOTPO', detect: (n) => /mascot/i.test(n || '') },
  CARHARTT:     { contactId: 65173, costList: 20, poField: 'PCF_CARHARTT', detect: (n) => /carhartt/i.test(n || '') }, // Carhartt UK LTD; no dedicated cost list → Launch(20) fallback, portal wholesale price is the real cost source
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
  // PenCarrie = leisurewear DISTRIBUTOR (Gildan/AWDis/FOTL/B&C/Kustom Kit/Result/…). Products
  // are NOT named "pencarrie", so TAG-ONLY (no brand detect): a single-supplier PENCARRIE order
  // takes all orderable rows; re-pickup prevented by clearing the tag on finalize. Ordering is
  // the official pcautoorder XML API (not a web basket). Cost = Launch(20) where live repricing
  // writes; PenCarrie's API single-price is the truer source (TODO override, like the Elastic ones).
  // BRAND DETECT built from BRANDS_supplier_list_NEW_2025.xlsx — the 51 brands whose supplier
  // column names PenCarrie ([[reference_supplier_carriage_terms]]). Before this, PenCarrie had NO
  // detect, so `dynamicDetect` fell back to a regex on its own NAME — which never matches a
  // distributor's products. On any MULTI-supplier order that meant zero candidate rows, no audit
  // row, and a silent "no demand": SO 482060 / 482218 / 482673 all tagged "PENCARRIE (CHECKED) /
  // <other>" were skipped entirely while holding real Result / Regatta / Bagbase demand (2026-08-17).
  // "(CHECKED)" is the buyer confirming PenCarrie has the stock, i.e. order from them rather than
  // Ralawise/BTC/Prestige — an annotation, NOT a scope, and it's stripped on finalise by tagSupplier.
  // ⚠️ "SF" is deliberately LEFT OUT — a two-letter token collides with too much. An SF product on a
  // multi-supplier order will not be detected; scope the tag if that ever comes up.
  // Verified against 48 real product names: matches all 18 lines of the first live order (PO 482741)
  // and none of the Portwest/Uneek/Snickers/Apache/TuffStuff/Fort/Blaklader/CT/AS0xx lines sharing
  // those orders. Shared brands (AWDis, Gildan, Regatta… also sold by Ralawise/BTC/Prestige) resolve
  // to PenCarrie here, which is what "(CHECKED)" is asserting.
  'PENCARRIE':  { contactId: 204,   costList: 20, poField: null,
    detect: (n) => /\b(afd|anthem|awdis|babybugz|bagbase|beechfield|bella|brand\s*lab|canterbury|comfort\s*grip|craghoppers|denny'?s|ecologie|finden\s*hales|flexfit|front\s*row|fruit\s*of\s*the\s*loom|gildan|henbury|kariban|kimood|kustom\s*kit|larkwood|le\s*chef|mantis|mumbles|native\s*spirit|neoblue|premier|pro\s*rtx|proact|quadra|regatta|result|russell|so\s*denim|sol'?s|spiro|stormtech|tactical\s*threads|tee\s*jays|tombo|towel\s*city|warrior|westford\s*mill|yoko|yupoong)\b/i.test(n || '') },
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
// A PCF_SUPPLIER token may carry a trailing note in parens — e.g. "HELLY HANSEN (BACK ORDER)"
// — a human annotation that the order is on back order. It STILL needs ordering (the OOS line
// isn't placed yet), so match the supplier ignoring that trailing note. Genuine "hold" notes
// ("on hold", "do not order", "awaiting"…) are filtered separately by isLeaveNote, and the
// toOrder maths prevents any double-order. Used for both queue-matching and finalise tag-clear.
const tagSupplier = (t) => String(t || '').replace(/\s*\([^)]*\)\s*$/, '').trim().toUpperCase();
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

// ── PCF_SUPPLIER parenthetical SCOPE ────────────────────────────────────────
// The tag's trailing note is USUALLY a human annotation ("HELLY HANSEN (BACK ORDER)"), but it is
// sometimes a genuine instruction narrowing what to order from that supplier (user, 2026-08-17):
//   "PENCARRIE (RG165 NAVY, M X1 ONLY)"          → only 1 × RG165 Navy, size M
//   "RALAWISE (TR010 ONLY) / PENCARRIE (JC020 ONLY)" → per-supplier scopes in one field
// Without this, a sole PENCARRIE tag hands the supplier EVERY row (it has no brand detect), so
// SO 481798 would have ordered Uneek polos, hoodies and hi-vis waistcoats off a PenCarrie PO.
//
// Telling a scope from an annotation: parse the note into terms, then require that EVERY term is
// recognised somewhere on the order's rows. "BACK ORDER" fails that ("back" may hit a "BACK PRINT"
// row but "order" hits nothing) so it stays an annotation and nothing is filtered — i.e. an
// unrecognised note keeps today's behaviour rather than silently dropping demand.
const SCOPE_NOISE = new Set(['ONLY', 'IN', 'AND', 'ALL', 'THE', 'FOR', 'PLEASE', 'NOTE', 'OF', 'ITEM', 'ITEMS', 'PCS', 'QTY']);
const SIZE_WORDS = [['XXS', 'XXSMALL'], ['XS', 'XSMALL'], ['S', 'SMALL'], ['M', 'MEDIUM'], ['L', 'LARGE'], ['XL', 'XLARGE'],
  ['XXL', '2XL', 'XXLARGE'], ['3XL', 'XXXL'], ['4XL', 'XXXXL'], ['5XL', 'XXXXXL']];
const sizeEq = (a, b) => {
  const n = (s) => String(s || '').toUpperCase().replace(/[^A-Z0-9]/g, '');
  const x = n(a), y = n(b);
  if (!x || !y) return false;
  if (x === y) return true;
  const g = SIZE_WORDS.find((grp) => grp.includes(x));
  return !!g && g.includes(y);
};
// Parse "(RG165 NAVY, M X1 ONLY)" → { terms:['RG165','NAVY','M'], qty:1 }; null when there's no note.
export function parseTagScope(tag) {
  const m = /\(([^)]*)\)\s*$/.exec(String(tag || ''));
  if (!m) return null;
  const toks = m[1].toUpperCase().split(/[\s,;]+/).filter(Boolean);
  const terms = []; let qty = null;
  for (let i = 0; i < toks.length; i++) {
    const q = /^[X×](\d+)$/.exec(toks[i]);
    if (q) { qty = Number(q[1]); continue; }
    if (/^[X×]$/.test(toks[i]) && /^\d+$/.test(toks[i + 1] || '')) { qty = Number(toks[++i]); continue; }
    if (SCOPE_NOISE.has(toks[i])) continue;
    terms.push(toks[i]);
  }
  return terms.length || qty != null ? { terms, qty } : null;
}
// Does this row satisfy a single scope term? SKU (exact / dash-part / substring), a whole word in
// the product name, the colour, or the size (letter⇄word aware, since BP stores "Medium" and the
// note says "M").
const rowMatchesTerm = (r, term) => {
  const t = String(term).toUpperCase();
  const sku = String(r.productSku || '').toUpperCase();
  if (sku && (sku === t || sku.split('-').includes(t) || sku.includes(t))) return true;
  const name = String(r.productName || '').toUpperCase();
  try { if (new RegExp('\\b' + t.replace(/[.*+?^${}()|[\]\\]/g, '\\$&') + '\\b').test(name)) return true; } catch { /* ignore */ }
  const colour = String(optValue(r.productOptions, /colou?r/i) || '').toUpperCase();
  if (colour && (colour === t || colour.split(/[\s/]+/).includes(t))) return true;
  return sizeEq(optValue(r.productOptions, /size/i), t);
};

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
// A registry entry may have no `detect` (a tag-only distributor). The demand read falls back to
// dynamicDetect; the preview/diagnostic paths called sup.detect() straight and threw
// "sup.detect is not a function" the moment such a supplier had a surviving candidate order.
const detectOf = (sup) => (sup && sup.detect) || dynamicDetect(sup && sup.key);
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
    let rows = Object.values(order.orderRows).filter((r) => !isNoteRow(r) && detectOf(sup)(r.productName, r.productSku));
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
    if (!tagsOf(tag).some((t) => tagSupplier(t) === sup.key)) { skipped.otherSupplier++; continue; }
    // A stamped poField is NOT a reason to skip — the real demand read (gatherLiveDemand) counts
    // these, because a stamp only records a PRIOR PO while the tag surviving means there is still
    // something to order (residual demand from a partial or re-allocated order). This preview used
    // to drop them, so it under-reported demand and disagreed with what a run would actually do —
    // e.g. SO 481102 showed as "alreadyHasPo, excluded" while its 3 × Fort 167 were genuinely
    // outstanding (2026-08-17). Counted now for visibility, not skipped.
    if (sup.poField && cf[sup.poField]) skipped.alreadyHasPo++;
    candidates.push({ id, tag, stampedPo: (sup.poField && cf[sup.poField]) || null });
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
    let rows = Object.values(order.orderRows).filter((r) => !isNoteRow(r) && detectOf(sup)(r.productName, r.productSku));
    const allTags = tagsOf(cand.tag);
    if (!rows.length && allTags.length === 1 && tagSupplier(allTags[0]) === sup.key) {
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
async function gatherLiveDemand({ supplierKey, detect, poField, hasBrandDetect = false }) {
  let ids = [], firstResult = 1;
  for (let g = 0; g < 40; g++) {
    const s = await liveGet(`/order-service/sales-order-search?orderStatusId=${DEMAND_STATUS}&pageSize=500&firstResult=${firstResult}`);
    const idx = {}; s.metaData.columns.forEach((c, i) => { idx[c.name] = i; });
    ids.push(...s.results.map((r) => r[idx.salesOrderId]));
    if (!s.metaData.morePagesAvailable || !s.results.length) break;
    firstResult += 500; await pause(250);
  }
  const contributors = [];
  const demandAudit = [];                                          // per-line decision breakdown (for the audit log)
  for (const id of ids) {
    const cf = (await liveGet(`/order-service/order/${id}/custom-field`)) || {};
    await pause(120);
    const tag = cf.PCF_SUPPLIER;
    if (!tag || isLeaveNote(tag)) continue;
    if (!tagsOf(tag).some((t) => tagSupplier(t) === supplierKey)) continue;
    // Do NOT skip SOs already stamped with this supplier's poField. Still being TAGGED for the
    // supplier (checked just above; the finalize removes the tag once fully ordered) means it
    // still needs ordering — a stamp only records a prior PO. Skipping stamped SOs permanently
    // buried RESIDUAL demand from partial/re-allocated orders (e.g. 480109 stamped
    // PCF_CASTLEPO=480396 but still needed 1× 710-GRY-32L). What prevents a double-order is the
    // finalise step (it clears the tag + moves the SO out of the demand status), NOT the stamp and
    // NOT `onOrder` — see the toOrder calc below for why product-level onOrder can't be trusted as
    // "already covered". Runs are sequential (lock), so there's no stamp-then-regather race.
    const order = (await liveGet(`/order-service/order/${id}`))[0];
    await pause(120);
    // Which rows to order (keep the rowId — needed to look up allocation).
    // The PCF_SUPPLIER tag lists the suppliers STILL TO BE ORDERED on this order (each tag
    // is removed as that supplier is placed), so a lone remaining tag means "only THIS
    // supplier's items are left" — NOT "the whole order is this supplier". So for a
    // brand-detect supplier we ALWAYS filter by its brand regex (even when it's the sole
    // tag), or we'd drag other suppliers' lines onto the PO (e.g. a Uneek tee + Apache boot
    // pulled onto a Castle PO). Email/dynamic suppliers have only a weak name-derived
    // detector and their orders ARE single-supplier, so for them a sole tag = take every
    // orderable row. Shipping/misc/decoration lines are excluded either way.
    const allTags = tagsOf(tag);
    const singleSupplier = allTags.length === 1 && tagSupplier(allTags[0]) === supplierKey;
    // Per-SO SKIP list (PCF_SKIPSKU): the user marks items NOT to auto-order on this SO (e.g. a
    // pair already ordered manually/elsewhere) — everything else still orders. A skip token
    // matches by exact SKU, a dash-part of the SKU, a SKU substring, or a whole word in the name
    // (so "126515-940-302", "126515", or "2700" all match the Fristads 2700 line).
    // QUANTITIES (user, 2026-08-17): a token may carry a count — "332410509990 x1" skips ONE unit
    // and still orders the rest, so a row of 2 orders 1. No count = skip the whole line (the
    // original behaviour). Entries split on comma/semicolon/pipe/newline OR plain whitespace, so
    // "126515 2700" is still two tokens; a bare "x1" (or "x 1") attaches to the token BEFORE it
    // instead of becoming a token in its own right — which is what it used to do, and it could
    // then match a SKU or a product name by accident.
    const skipEntries = [];
    for (const chunk of String(cf[SKIP_SKU_FIELD] || '').split(/[,;|\n]+/)) {
      const toks = chunk.trim().toLowerCase().split(/\s+/).filter(Boolean);
      for (let i = 0; i < toks.length; i++) {
        let qty = null;
        const m = /^[x×](\d+)$/.exec(toks[i]);
        if (m) qty = Number(m[1]);
        else if (/^[x×]$/.test(toks[i]) && /^\d+$/.test(toks[i + 1] || '')) qty = Number(toks[++i]);
        if (qty != null) { if (skipEntries.length) skipEntries[skipEntries.length - 1].qty = qty; continue; }
        skipEntries.push({ token: toks[i], qty: null, used: 0 });
      }
    }
    const matchSkip = (r) => skipEntries.find((e) => {
      const sku = String(r.productSku || '').toLowerCase();
      if (sku === e.token || sku.split('-').includes(e.token) || sku.includes(e.token)) return true;
      try { return new RegExp('\\b' + e.token.replace(/[.*+?^${}()|[\]\\]/g, '\\$&') + '\\b').test(String(r.productName || '').toLowerCase()); } catch { return false; }
    }) || null;
    const orderableRows = Object.entries(order.orderRows).filter(([, r]) => !isNonOrderableRow(r));
    let candidateRows = (singleSupplier && !hasBrandDetect)
      ? orderableRows
      : orderableRows.filter(([, r]) => detect(r.productName, r.productSku));
    // Apply THIS supplier's parenthetical scope, if its note is a real instruction rather than an
    // annotation. Only bites when every term is recognised on the order AND at least one row
    // satisfies them all — otherwise the note is left alone and nothing is filtered.
    const ourTag = allTags.find((t) => tagSupplier(t) === supplierKey);
    const scope = parseTagScope(ourTag);
    let scopeCap = null;
    if (scope && scope.terms.length) {
      // A TAG-ONLY supplier (no brand regex) sharing an order with other suppliers would otherwise
      // be filtered by `dynamicDetect`, a regex on the supplier's own NAME — which can never match
      // a distributor's products ("pencarrie" appears in no product name), so it would order
      // nothing at all. Where such a supplier carries an explicit scope, that scope IS the row
      // selector, so evaluate it against every orderable row instead. This is what makes
      // "PENCARRIE (RS121M ONLY) / PORTWEST" work the way a human would expect.
      const rowsForScope = (!hasBrandDetect && !singleSupplier) ? orderableRows : candidateRows;
      const recognised = scope.terms.every((t) => rowsForScope.some(([, r]) => rowMatchesTerm(r, t)));
      const qualifying = rowsForScope.filter(([, r]) => scope.terms.every((t) => rowMatchesTerm(r, t)));
      if (recognised && qualifying.length) candidateRows = rowsForScope;   // so the drop-audit below reports against the right set
      // A term shaped like a product code (RG165, JC020, TR010, or a long numeric SKU) that we
      // CANNOT satisfy means a real instruction we don't understand — order nothing from this SO and
      // flag it, rather than fall back to taking every row. Word-only notes (BACK ORDER, LOW STOCK)
      // are annotations and change nothing.
      const codeLike = scope.terms.some((t) => /^[A-Z]{1,4}\d{2,}[A-Z0-9-]*$/.test(t) || /^\d{5,}$/.test(t));
      if (codeLike && !(recognised && qualifying.length)) {
        for (const [rowId, r] of candidateRows) demandAudit.push({ soId: id, rowId, productId: r.productId, sku: r.productSku, name: r.productName, ordered: parseFloat(r.quantity.magnitude), allocated: 0, fulfilled: 0, onOrder: 0, inStock: 0, toOrder: 0, note: `PCF_SUPPLIER scope "${ourTag}" names an item no row satisfies — nothing ordered, needs review` });
        candidateRows = [];
      } else if (recognised && qualifying.length) {
        for (const [rowId, r] of candidateRows) {
          if (qualifying.some(([q]) => q === rowId)) continue;
          demandAudit.push({ soId: id, rowId, productId: r.productId, sku: r.productSku, name: r.productName, ordered: parseFloat(r.quantity.magnitude), allocated: 0, fulfilled: 0, onOrder: 0, inStock: 0, toOrder: 0, note: `outside the PCF_SUPPLIER scope "${ourTag}"` });
        }
        candidateRows = qualifying;
        // "X1" caps the TOTAL units taken from this SO, spent lowest rowId first.
        if (scope.qty != null) {
          scopeCap = new Map(); let left = scope.qty;
          for (const [rowId, r] of [...qualifying].sort((a, b) => Number(a[0]) - Number(b[0]))) {
            const take = Math.min(left, parseFloat(r.quantity.magnitude) || 0);
            scopeCap.set(rowId, Math.max(0, take)); left -= take;
          }
        }
      }
    }
    // How many units each row loses to the skip list. A COUNTED token is a budget spent across
    // every row it matches (lowest rowId first, so the outcome is deterministic when the same SKU
    // sits on several rows); an UNCOUNTED token takes the whole row.
    const skipByRow = new Map();
    for (const [rowId, r] of [...candidateRows].sort((a, b) => Number(a[0]) - Number(b[0]))) {
      const e = matchSkip(r);
      if (!e) continue;
      const rowQty = parseFloat(r.quantity.magnitude) || 0;
      if (e.qty == null) { skipByRow.set(rowId, rowQty); continue; }
      const take = Math.min(Math.max(0, e.qty - e.used), rowQty);
      if (take > 0) { e.used += take; skipByRow.set(rowId, take); }
    }
    // audit-log the FULLY skipped rows so it's visible WHY they weren't ordered (a partially
    // skipped row still orders, and carries its `skipped` count on the normal audit line below)
    for (const [rowId, r] of candidateRows) {
      const s = skipByRow.get(rowId) || 0;
      const rowQty = parseFloat(r.quantity.magnitude) || 0;
      if (s > 0 && s >= rowQty) demandAudit.push({ soId: id, rowId, productId: r.productId, sku: r.productSku, name: r.productName, ordered: rowQty, allocated: 0, fulfilled: 0, onOrder: 0, inStock: 0, toOrder: 0, skipped: s, note: `skipped via ${SKIP_SKU_FIELD}` });
    }
    const entries = candidateRows.filter(([rowId, r]) => (skipByRow.get(rowId) || 0) < (parseFloat(r.quantity.magnitude) || 0));
    // An order TAGGED for this supplier that yields NOTHING is the dangerous case — it's how a
    // brand-detect gap hides real demand with no error and no trace: Hellberg/EMMA/CLC on Snickers
    // orders (d950468, 7–17 Aug) and every multi-supplier PenCarrie order until its detect existed.
    // Record why, per row, so a miss is queryable in demand_log instead of vanishing. Only when the
    // order contributes nothing — a partially-matched order isn't suspicious and would just be noise.
    if (!entries.length) {
      for (const [rowId, r] of orderableRows) {
        demandAudit.push({ soId: id, rowId, productId: r.productId, sku: r.productSku, name: r.productName,
          ordered: parseFloat(r.quantity.magnitude), allocated: 0, fulfilled: 0, onOrder: 0, inStock: 0, toOrder: 0,
          note: `tagged "${tag}" for ${supplierKey} but NO row was selected — ${hasBrandDetect ? "none matched its brand detect" : "no rows orderable"}; demand may be hidden` });
      }
      continue;
    }
    // Only order the UNALLOCATED qty: ordered − allocated − fulfilled.
    // Allocation isn't in the order API — read it from the legacy order page.
    const alloc = await getOrderAllocations(id, { client: process.env.BP_WEB_CLIENT_ID || 'tuffworkwear' });
    const rows = [];
    for (const [rowId, r] of entries) {
      const ordered = parseFloat(r.quantity.magnitude);
      // CRITICAL: the legacy order page only renders a reserved[] allocation input for rows
      // still needing allocation. A FULFILLED (shipped) row has NO reserved[] input, so it's
      // absent from `alloc`. Treat "absent from alloc" as fully fulfilled → order nothing
      // (else we re-order already-shipped rows — e.g. SO 481017 ordered all 7 rows when only
      // the 2 with reserved[] inputs still needed it). Backorder rows still needing stock DO
      // render the input (with zeros), so they stay in `alloc` and order correctly.
      const a = alloc[rowId];
      if (!a) {
        demandAudit.push({ soId: id, rowId, productId: r.productId, sku: r.productSku, name: r.productName, ordered, allocated: 0, fulfilled: ordered, onOrder: 0, inStock: 0, toOrder: 0, note: 'no reserved[] input on order page — treated as fulfilled' });
        continue;
      }
      // Order this SO's UNALLOCATED qty: ordered − allocated − fulfilled. `onOrder` is deliberately
      // NOT subtracted (user, 2026-08-17): in Brightpearl it is a PRODUCT-level figure — stock on
      // some purchase order somewhere — NOT stock earmarked for THIS sales order. That inbound stock
      // may be replenishment or already spoken for by other SOs, so treating it as covering this
      // line under-orders and strands the customer. Subtracting it had silently suppressed real
      // demand on 10 Snickers SOs (~13 units) that sat tagged in the pool for days.
      // ⚠️ This means the ONLY guard against re-ordering an SO is the finalise step (tag cleared +
      // status → Ordered Stock Awaiting Delivery, which drops it out of the demand pool). If a
      // finalise ever half-fails, that SO WILL be re-ordered next run — onOrder used to mask that.
      // `onOrder`/`inStock` are still audited on every line below so a double-order is diagnosable.
      // …minus any units the SO's skip list withholds from THIS row (PCF_SKIPSKU "sku x1"), and
      // capped by a PCF_SUPPLIER scope quantity ("PENCARRIE (RG165 NAVY, M X1 ONLY)" → at most 1).
      const skipQty = skipByRow.get(rowId) || 0;
      const cap = scopeCap ? (scopeCap.get(rowId) || 0) : Infinity;
      const toOrder = Math.min(ordered - a.allocated - a.fulfilled - skipQty, cap);
      // Audit EVERY considered line (incl. ones we decide NOT to order) so a later
      // "why did it order N?" is answerable from the exact decision-time figures.
      demandAudit.push({ soId: id, rowId, productId: r.productId, sku: r.productSku, name: r.productName, ordered, allocated: a.allocated || 0, fulfilled: a.fulfilled || 0, onOrder: a.onOrder || 0, inStock: a.inStock || 0, toOrder, ...(skipQty ? { skipped: skipQty, note: `${skipQty} withheld via ${SKIP_SKU_FIELD}` } : {}) });
      if (toOrder <= 0) continue; // fully allocated / already ordered — skip
      rows.push({ productId: r.productId, sku: r.productSku, name: r.productName, qty: toOrder, orderedQty: ordered, allocation: a, itemCost: r.itemCost ? parseFloat(r.itemCost.value) : 0, taxCode: (r.rowValue && r.rowValue.taxCode) || null });
    }
    if (!rows.length) continue;
    contributors.push({
      id, ref: order.reference || '', tag,
      lines: rows.map((r) => ({ productId: r.productId, sku: r.sku, name: r.name, qty: r.qty, orderedQty: r.orderedQty, allocation: r.allocation, itemCost: r.itemCost, taxCode: r.taxCode })),
    });
  }
  return { contributors, demandAudit };
}

// Persist the per-line demand decision to the demand_log table (best-effort). Lets a later
// "why did PO#N order only M of X?" be answered from the exact ordered/allocated/fulfilled/
// on-order figures at the moment the PO ran, rather than inferring from current state.
let _demandLogReady = false;
async function writeDemandLog(pool, poId, supplierKey, demandAudit) {
  if (!pool || !demandAudit || !demandAudit.length) return;
  if (!_demandLogReady) {
    await pool.query(`CREATE TABLE IF NOT EXISTS demand_log (
      id BIGSERIAL PRIMARY KEY, po_id INTEGER, supplier TEXT, so_id INTEGER,
      product_id INTEGER, sku TEXT, name TEXT,
      ordered NUMERIC, allocated NUMERIC, fulfilled NUMERIC, on_order NUMERIC,
      in_stock NUMERIC, to_order NUMERIC, logged_at TIMESTAMPTZ DEFAULT now())`);
    await pool.query(`CREATE INDEX IF NOT EXISTS demand_log_po_idx ON demand_log(po_id)`);
    await pool.query(`CREATE INDEX IF NOT EXISTS demand_log_sku_idx ON demand_log(sku)`);
    // `skipped` + `note` carry WHY a line was withheld or dropped — units held back by PCF_SKIPSKU,
    // rows outside a PCF_SUPPLIER scope, a row treated as fulfilled because the order page rendered
    // no reserved[] input. Those reasons existed only in the API response before, so a past run's
    // decision couldn't be queried later, which is the whole point of this table. ADD COLUMN IF NOT
    // EXISTS so existing deployments migrate on the next run.
    await pool.query(`ALTER TABLE demand_log ADD COLUMN IF NOT EXISTS skipped NUMERIC`);
    await pool.query(`ALTER TABLE demand_log ADD COLUMN IF NOT EXISTS note TEXT`);
    await pool.query(`CREATE INDEX IF NOT EXISTS demand_log_so_idx ON demand_log(so_id)`);
    _demandLogReady = true;
  }
  for (const d of demandAudit) {
    await pool.query(
      `INSERT INTO demand_log (po_id, supplier, so_id, product_id, sku, name, ordered, allocated, fulfilled, on_order, in_stock, to_order, skipped, note)
       VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14)`,
      [poId, supplierKey, d.soId, d.productId, d.sku, d.name, d.ordered, d.allocated, d.fulfilled, d.onOrder, d.inStock, d.toOrder, d.skipped ?? null, d.note ?? null]);
  }
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

  // 1. SO-driven demand + per-SKU qty (for dedupe). hasBrandDetect = this is a hardcoded
  // brand-regex supplier (vs a dynamic/email supplier with a weak name-derived detector).
  const hasBrandDetect = !!(opts.detect || reg.detect);
  const { contributors, demandAudit } = await gatherLiveDemand({ supplierKey, detect, poField, hasBrandDetect });
  const soLines = []; const soQtyBySku = {};
  for (const c of contributors) for (const l of c.lines) {
    const cost = await costOfLive(l.productId, priceListId, l.itemCost);
    soLines.push({ productId: l.productId, sku: l.sku, name: l.name, qty: l.qty, cost, order: c.id, taxCode: l.taxCode });
    const k = String(l.sku).toUpperCase(); soQtyBySku[k] = (soQtyBySku[k] || 0) + l.qty;
  }

  // 2. low-inventory replenishment (deduped against SO qty for the same SKU).
  // includeLowInv=false (per-supplier via registry, or per-call via opts) orders ONLY
  // sales-order demand and skips the reorder entirely — used when a supplier's min-stock
  // data still needs sorting (e.g. PORTWEST). Default ON for every other supplier.
  const includeLowInv = opts.includeLowInv != null ? (opts.includeLowInv === true) : (reg.includeLowInv !== false);
  const { fetchLowInventory } = await import('./lowInventory.js');
  let statusIds = []; try { statusIds = await liveSalesOrderStatusIds(excludeStatusIds); } catch { /* report default */ }
  const li = includeLowInv ? await fetchLowInventory({ supplierId: lowInvSupplierId, statusIds, numResults: 10000 }) : { rows: [] };
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

  // 2a. PORTAL PRICE OVERRIDES (Elastic suppliers: Carhartt / Helly Hansen). The live
  // wholesale price per SKU (captured from the portal draft in the pre-flight) WINS over
  // Brightpearl's stored cost, so the PO net reconciles to what the supplier actually
  // invoices — and self-heals any £0/stale BP cost. Keyed by SKU (uppercase); only a real
  // positive price overrides. Applied before the pad so the threshold maths use it too.
  const priceOverrides = opts.priceOverrides && typeof opts.priceOverrides === 'object' ? opts.priceOverrides : null;
  const priceOverridesApplied = [];
  if (priceOverrides) {
    const applyOverride = (l) => {
      const p = Number(priceOverrides[String(l.sku || '').toUpperCase()]);
      if (Number.isFinite(p) && p > 0 && p !== l.cost) { priceOverridesApplied.push({ sku: l.sku, was: l.cost, now: p }); l.cost = p; }
    };
    for (const l of soLines) applyOverride(l);
    for (const l of lowLines) applyOverride(l);
  }

  // 2b. DAY-3 TOP-UP — pad low-inv up to padMaxPct above each item's min stock to reach
  // the free-delivery threshold (spread evenly), but only if it can actually reach it.
  let padInfo = null;
  const lineNet = (arr) => arr.reduce((a, l) => a + (l.cost || 0) * l.qty, 0);
  if (includeLowInv && padToThreshold > 0) {
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
    fillExistingPoId: opts.fillExistingPoId || null,               // echoed so callers can confirm the option is wired
    soLines, separator: '=====LOW INV====', lowLines, padInfo, includeLowInv,
    priceOverridesApplied,                                          // portal-price overrides applied to line costs (Elastic suppliers)
    soUnits: soLines.reduce((a, l) => a + l.qty, 0),
    lowUnits: lowLines.reduce((a, l) => a + l.qty, 0),
    unresolvedSkus: lowLines.filter((l) => l.unresolved).map((l) => l.sku),
  };

  if (!execute) return { dryRun: true, ...plan };
  if (plan.unresolvedSkus.length) return { created: false, reason: `unresolved low-inv SKUs (aborted, nothing written): ${plan.unresolvedSkus.join(', ')}`, ...plan };
  if (!soLines.length && !lowLines.length) return { created: false, reason: 'nothing to order', ...plan };

  // 3. WRITE — PO header (Pending PO), rows (SO, then separator, then low-inv), note, SO stamps.
  // fillExistingPoId: reuse an existing EMPTY PO shell (e.g. one left behind by a prior failed
  // run) so its number/links stay stable, instead of minting a new PO. Guarded to empty POs so
  // it can never double-fill.
  let poId;
  if (opts.fillExistingPoId) {
    poId = Number(opts.fillExistingPoId);
    const ex = (await liveGet(`/order-service/order/${poId}`) || [])[0];
    if (!ex) return { created: false, reason: `fillExistingPoId ${poId} not found`, ...plan };
    const existingRows = Object.keys(ex.rows || {}).length;
    if (existingRows) return { created: false, reason: `PO ${poId} already has ${existingRows} row(s) — refusing to double-fill`, ...plan };
  } else {
    poId = await liveWrite('POST', '/order-service/order', {
      orderTypeCode: 'PO', reference, priceListId, priceModeCode: 'EXC',
      warehouseId: WAREHOUSE_ID, currency: { orderCurrencyCode: 'GBP' },
      parties: { supplier: { contactId } },
    });
  }
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
  if (lowLines.length) {
    await addRow(1000, 1, 0, '=====LOW INV====');         // separator note row (net 0 → no tax) — only when there IS low-inv
    for (const l of lowLines) await tryRow(l, 'low-inv');
  }

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

  // ONE ROW PER VARIANT. The same product/colour/size demanded by several sales orders lands as
  // several rows, which makes booking the delivery in a matching exercise — the goods arrive as one
  // pile of N and the PO shows it split across rows. Collapse them at birth (per-unit cost and real
  // tax preserved), so EVERY supplier's PO is tidy, not just Portwest, whose flow already did this.
  // Per-SO traceability is unaffected: it lives in the PO note above and in demand_log, not in the
  // row split. Non-fatal — a tidy-up must never lose a created PO. Opt out with dedupeRows:false.
  if (opts.dedupeRows !== false) {
    try {
      const c = await consolidatePoRows({ poId, execute: true });
      if (c.merged && c.merged.length) { plan.consolidated = c.merged; }
    } catch (e) { plan.consolidateWarn = e.message; }
  }

  // audit trail: persist the per-line demand decision for this PO (best-effort, non-fatal)
  if (opts.logPool) { try { await writeDemandLog(opts.logPool, poId, supplierKey, demandAudit); } catch (e) { /* logging must never break a PO */ } }

  return { created: true, poId, skippedBundles, demandAudit, ...plan };
}

// Clean a product name for an order NOTE: keep the product name only, dropping the
// customisation/decoration blob a customer line carries (e.g. "UC105 Active Polo\n\nAdd
// Customisation - 3 x Add Embroidery\n\nUpload Logo - https://…"). Takes the first non-empty
// line (the actual product) and collapses whitespace.
const cleanItemName = (n) => (String(n || '').split(/\r?\n/).map((s) => s.trim()).find(Boolean) || '').replace(/\s+/g, ' ').trim();

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
    const remaining = tagsOf(before).filter((t) => tagSupplier(t) !== key);
    // Read the order too, so a "PLEASE PUT TO PROOF REQUIRED ONCE ORDERED" row can route the
    // status to Proof Required instead of Ordered Stock. Non-fatal: if the read fails we fall
    // back to the normal ordered status rather than block the finalise.
    let instruction = null;
    try { instruction = orderStatusInstruction((await liveGet(`/order-service/order/${id}`))[0]); } catch { /* keep default */ }
    plan.push({ id, before, after: remaining.join(' / '), willClear: remaining.length === 0, instruction, needsProof: !!(instruction && instruction.statusId !== ORDERED_STATUS) });
    await pause(120);
  }
  if (!execute) return { dryRun: true, supplierKey: key, poId, setOrderedStatus, plan };
  const results = [];
  for (const p of plan) {
    // tag: only PATCH if there was something to change (avoid op:remove on an empty field)
    if (p.before && p.after) await liveWrite('PATCH', `/order-service/order/${p.id}/custom-field`, [{ op: 'add', path: '/PCF_SUPPLIER', value: p.after }]);
    else if (p.before && !p.after) await liveWrite('PATCH', `/order-service/order/${p.id}/custom-field`, [{ op: 'remove', path: '/PCF_SUPPLIER' }]);
    // status: → Ordered Stock Awaiting Delivery ONLY when no supplier remains (else stays on SNO)
    let statusChanged = false, statusSet = null;
    if (setOrderedStatus && p.willClear) {
      // an "ONCE ORDERED" instruction row decides the status; PROOF_REQUIRED_STATUS_ID still overrides the 34 case
      statusSet = p.instruction ? (p.instruction.statusId === 34 ? PROOF_REQUIRED_STATUS : p.instruction.statusId) : ORDERED_STATUS;
      await liveWrite('PUT', `/order-service/order/${p.id}/status`, { orderStatusId: statusSet });
      statusChanged = true;
    }
    // note on the SO: the item names ordered for this SO, then "Ordered on PO#<poId>".
    // The '#' before the id is REQUIRED — BP only renders a clickable order link for
    // the "#<orderId>" pattern (was "PO:<id>", which stayed plain text).
    let noted = false;
    if (poId) {
      // items[] is either legacy name STRINGS, or {sku,qty,name} objects. Render per VARIANT
      // as "<clean name> — <SKU> x<qty>" (de-dupe by SKU, summing qty) so the note reflects
      // EVERY ordered variant — product names omit colour/size (Carhartt/HH), so name-only
      // de-dupe used to collapse e.g. 4 colour/size lines into 2 and under-report the order.
      const items = (linesByOrder && (linesByOrder[p.id] || linesByOrder[String(p.id)])) || null;
      let lines = null;
      if (Array.isArray(items) && items.length) {
        const bySku = new Map(); const nameOnly = [];
        for (const it of items) {
          if (it && typeof it === 'object' && it.sku) {
            const k = String(it.sku).toUpperCase();
            const cur = bySku.get(k) || { sku: it.sku, qty: 0, name: cleanItemName(it.name) };
            cur.qty += Number(it.qty) || 0; bySku.set(k, cur);
          } else {
            const nm = cleanItemName(typeof it === 'object' ? it.name : it);
            if (nm && !nameOnly.includes(nm)) nameOnly.push(nm);
          }
        }
        lines = [
          ...[...bySku.values()].map((v) => `${v.name ? v.name + ' — ' : ''}${v.sku}${v.qty ? ` x${v.qty}` : ''}`),
          ...nameOnly,
        ];
      }
      const text = (lines && lines.length)
        ? `${lines.join('\n')}\nOrdered on PO#${poId}`
        : `${key} items ordered via PO#${poId}`;
      const addedOn = new Date().toISOString().replace('Z', '+00:00');
      await liveWrite('POST', `/order-service/order/${p.id}/note`, { text, addedOn, contactId: noteContactId || 1, isPublic: false });
      noted = true;
    }
    results.push({ id: p.id, tag: p.after || '(cleared)', keptOnSNO: !p.willClear, statusChanged, statusSet, proofRequired: !!p.needsProof, instruction: p.instruction ? p.instruction.marker : null, noted });
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
export async function repriceComboPOLive({ poId, priceListId = 20, keepNet = false, execute = false, allowMiscRows = false } = {}) {
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
  // ⚠️ SAFETY: the re-add below forces every productId-1000 row to the name "=====LOW INV====" with
  // net 0. That is correct for the separator this module creates, but it would SILENTLY rename and
  // zero any OTHER misc row — a "Shipping:" line, or a
  // "+++ PLEASE PUT TO PROOF REQUIRED ONCE ORDERED +++" instruction (both live on productId 1000).
  // Refuse rather than damage them; pass allowMiscRows:true only if you have checked the PO by hand.
  const miscRows = plan.filter((p) => p.isSep && String(p.name || "").trim() !== "=====LOW INV====");
  if (miscRows.length && allowMiscRows !== true) {
    return { refused: true, poId, priceListId, reason: miscRows.length + ' productId-1000 row(s) are NOT the =====LOW INV==== separator — repricing would rename and zero them', miscRows: miscRows.map((m) => m.name), plan };
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

// ─────────────────────────────────────────────────────────────────────────────
// Fold a lump-sum supplier discount row INTO the line costs.
//
// Helly Hansen (and Carhartt) quote LIST on the availability sheet and take the trade discount
// once, at invoice, as a single negative row. The PO total is then right and every single line is
// wrong — PO 483239 booked £661.00 of list-priced rows against a "42% Discount" row of -£277.62 to
// reach the real £383.38, which means goods-in would have valued that stock ~42% high per line.
//
// This rewrites each product row to unit x (1 - discountPct) and deletes the lump row, so the PO
// nets to exactly the same figure while each line finally carries the real cost.
//
// THE SAFETY IS THE RECONCILIATION: the folded rows must net to the PO's CURRENT total (discount
// row included) within a penny. If they don't, the discount is not uniform across the lines and a
// flat percentage is the wrong tool — so it refuses and changes nothing. Nothing here is guesswork.
// ─────────────────────────────────────────────────────────────────────────────
const round2 = (n) => Math.round((Number(n) + Number.EPSILON) * 100) / 100;

export async function absorbSupplierDiscountPOLive({ poId, discountPct, expectedNet = null, execute = false } = {}) {
  const pct = Number(discountPct);
  if (!(pct > 0 && pct < 1)) throw new Error('discountPct must be a fraction between 0 and 1 (0.42 for 42%)');
  const order = (await liveGet(`/order-service/order/${poId}`))[0];
  if (!order) throw new Error(`PO ${poId} not found`);

  const entries = Object.entries(order.orderRows || {}).sort((a, b) => Number(a[0]) - Number(b[0]));
  const netOf = (r) => parseFloat((r.rowValue && r.rowValue.rowNet && r.rowValue.rowNet.value) || 0);
  const keep = [], lump = [];
  for (const [rowId, r] of entries) {
    const qty = parseFloat(r.quantity.magnitude);
    const net = netOf(r);
    const rec = { rowId, productId: r.productId, sku: r.productSku || '', name: r.productName || '', qty, oldNet: round2(net), oldUnit: qty ? round2(net / qty) : 0 };
    if (String(r.productId) === '1000' && net < 0) lump.push(rec); else keep.push(rec);
  }
  if (!lump.length && expectedNet == null) return { refused: true, poId, reason: 'no negative productId-1000 row and no expectedNet — there is nothing independent to check a flat rate against, so this will not guess' };
  // A non-discount misc row (a "Shipping:" line, a proof instruction) must not be silently dropped
  // or re-priced, so bail rather than guess what it meant.
  const misc = keep.filter((k) => String(k.productId) === '1000');
  if (misc.length) return { refused: true, poId, reason: `${misc.length} other productId-1000 row(s) present — check them by hand first`, miscRows: misc.map((m) => m.name) };

  const plan = keep.map((k) => { const newUnit = round2(k.oldUnit * (1 - pct)); return { ...k, newUnit, newNet: round2(newUnit * k.qty) }; });
  const currentTotal = round2(entries.reduce((s, [, r]) => s + netOf(r), 0));   // list rows + any negative lump
  const foldedTotal = round2(plan.reduce((s, p) => s + p.newNet, 0));
  // WHAT THE FOLD IS CHECKED AGAINST — never the same arithmetic that produced it:
  //   • lump row present  → the PO already nets to the invoice figure, so use its own total
  //   • expectedNet given → the invoice figure, supplied by the caller (e.g. read off the PO before
  //                         someone deleted the lump row, or off the supplier's confirmation)
  // Without one of those it refuses above, because "list x 0.58 equals list x 0.58" proves nothing.
  const anchor = lump.length ? currentTotal : round2(expectedNet);
  const anchorSource = lump.length ? `the PO's own total, which already nets ${lump.length} lump discount row(s)` : 'the expectedNet passed in';
  const drift = round2(foldedTotal - anchor);
  const summary = { poId, discountPct: pct, lumpRow: lump.map((l) => `${l.name} ${l.oldNet.toFixed(2)}`), listTotal: round2(keep.reduce((s, k) => s + k.oldNet, 0)), currentTotal, foldedTotal, anchor, anchorSource, drift, rows: plan.length };
  if (Math.abs(drift) > 0.02) {
    return { refused: true, ...summary, reason: `folding ${(pct * 100).toFixed(0)}% into the lines nets £${foldedTotal.toFixed(2)} against £${anchor.toFixed(2)} from ${anchorSource} (out by £${drift.toFixed(2)}) — the discount is not a flat ${(pct * 100).toFixed(0)}% on every line, so do NOT use a single rate here`, plan };
  }
  if (!execute) return { dryRun: true, ...summary, plan };

  for (const r of [...keep, ...lump]) { await liveWrite('DELETE', `/order-service/order/${poId}/row/${r.rowId}`); await pause(150); }
  const readded = [];
  for (const p of plan) {
    const code = await productTaxCodeLive(p.productId);
    const rate = taxRate(code);
    await liveWrite('POST', `/order-service/order/${poId}/row`, {
      productId: p.productId,
      quantity: { magnitude: String(p.qty) },
      rowValue: { taxCode: code, rowNet: { currency: 'GBP', value: p.newNet.toFixed(2) }, rowTax: { currency: 'GBP', value: round2(p.newNet * rate).toFixed(2) } },
    });
    readded.push(`${p.sku || p.productId} ${p.qty} x £${p.newUnit.toFixed(2)}`);
    await pause(150);
  }
  return { done: true, ...summary, readded };
}

// Point a PO's header at a different price list — the list only defaults what a human sees when
// adding a row by hand in BP, but leaving Helly Hansen's POs pointing at the dead list 6 is exactly
// how the "this PO has no prices" confusion started.
export async function setOrderPriceListLive(orderId, priceListId) {
  return liveWrite('PATCH', `/order-service/order/${orderId}`, [{ op: 'replace', path: '/priceListId', value: Number(priceListId) }]);
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
  // Consolidate by SKU (+size): the same variant can sit on SEVERAL PO rows when it
  // was demanded by multiple sales orders. Supplier baskets add ONE qty per SKU field,
  // so un-merged duplicates make later rows overwrite earlier ones → the basket
  // under-orders (e.g. Castle PO 481278: 134-BLK-XL on 3 rows added 1 not 5).
  const bySku = new Map();
  for (const r of Object.values(order.orderRows || {})) {
    if (String(r.productId) === '1000') continue;
    const sku = r.productSku;
    const size = optValue(r.productOptions, /size/i);
    const colour = optValue(r.productOptions, /colou?r/i);
    const qty = parseFloat(r.quantity.magnitude);
    const key = `${sku}|${size || ''}`;
    if (bySku.has(key)) bySku.get(key).qty += qty;
    else bySku.set(key, { sku, size, colour, name: r.productName, productId: r.productId, qty });
  }
  return [...bySku.values()];
}

// Reconcile a Portwest PO's rows to what the portal cart will ACTUALLY ship: bump any
// quantity Portwest rounded up to its carton/min-order, and drop any line the portal wouldn't
// take (0 in the cart). `cart` = SKU(upper)→qty (Map or object). BP has no row-value update,
// so a changed row is deleted + re-added preserving unit cost + real tax code (like reprice).
// Dry-run unless { execute:true }. Returns { bumped:[{sku,from,to}], dropped:[{sku,was}] }.
export async function reconcilePortwestPO({ poId, cart = {}, execute = false } = {}) {
  const get = (sku) => { const k = String(sku).toUpperCase(); const v = cart instanceof Map ? cart.get(k) : cart[k]; return v == null ? 0 : Number(v); };
  const order = (await liveGet(`/order-service/order/${poId}`))[0];
  const entries = Object.entries(order.orderRows || {}).sort((a, b) => Number(a[0]) - Number(b[0]));
  // Group PO rows by SKU — the same variant can sit on several rows (multiple SOs). The cart
  // qty is the TOTAL for the SKU, so a changed SKU collapses to ONE row at the cart qty.
  const bySku = new Map();
  for (const [rowId, r] of entries) {
    if (String(r.productId) === '1000') continue;                 // separator/note row — leave it
    const sku = String(r.productSku);
    const q = parseFloat(r.quantity.magnitude);
    const net = parseFloat((r.rowValue && r.rowValue.rowNet && r.rowValue.rowNet.value) || 0);
    const cur = bySku.get(sku) || { rowIds: [], qty: 0, net: 0, productId: r.productId };
    cur.rowIds.push(rowId); cur.qty += q; cur.net += net; cur.productId = r.productId;
    bySku.set(sku, cur);
  }
  const bumped = [], dropped = [], toDelete = [], toReadd = [];
  for (const [sku, info] of bySku) {
    const cq = get(sku);
    if (cq === info.qty) continue;                                // matches (summed) — nothing to do
    if (cq === 0) { dropped.push({ sku, was: info.qty }); toDelete.push(...info.rowIds); continue; }
    bumped.push({ sku, from: info.qty, to: cq });
    toDelete.push(...info.rowIds);
    toReadd.push({ productId: info.productId, qty: cq, unit: info.qty ? info.net / info.qty : 0 });
  }
  if (!execute) return { dryRun: true, poId, bumped, dropped };
  for (const rowId of toDelete) { await liveWrite('DELETE', `/order-service/order/${poId}/row/${rowId}`); await pause(150); }
  for (const a of toReadd) {
    const net = a.unit * a.qty;
    const code = await productTaxCodeLive(a.productId);
    const rate = taxRate(code);
    await liveWrite('POST', `/order-service/order/${poId}/row`, { productId: a.productId, quantity: { magnitude: String(a.qty) }, rowValue: { taxCode: code, rowNet: { currency: 'GBP', value: net.toFixed(2) }, rowTax: { currency: 'GBP', value: (net * rate).toFixed(2) } } });
    await pause(150);
  }
  return { done: true, poId, bumped, dropped };
}

// Consolidate duplicate PO rows: any SKU sitting on >1 row (the same variant demanded by
// several SOs) is collapsed to a SINGLE row at the summed qty (delete its rows + re-add one,
// preserving the per-unit cost + real tax). Single-row SKUs and the separator are untouched.
// Dry-run unless { execute:true }. Returns { merged:[{sku,rows,qty}] }.
export async function consolidatePoRows({ poId, execute = false } = {}) {
  const order = (await liveGet(`/order-service/order/${poId}`))[0];
  const entries = Object.entries(order.orderRows || {}).sort((a, b) => Number(a[0]) - Number(b[0]));
  // Key on productId, NOT productSku. In Brightpearl the productId IS the variant (a specific
  // colour+size), whereas several variants can SHARE a base SKU with the size held in
  // productOptions — on PO 482661, SKU 25020400 sat on 6 rows across 4 productIds (Small, Medium,
  // Large, XL). Grouping by SKU would have merged four different sizes into one row of 21: worse
  // than the duplication it set out to fix, and it loses the size. Grouping by productId merges
  // only genuine duplicates of the same variant (that same PO had 25562900006 and 90014104000 twice
  // each, and 25020400 Medium and XL twice each — those are the ones that should collapse).
  const byVariant = new Map();
  for (const [rowId, r] of entries) {
    if (String(r.productId) === '1000') continue;                 // separator / note row — leave alone
    const key = String(r.productId);
    const q = parseFloat(r.quantity.magnitude);
    const net = parseFloat((r.rowValue && r.rowValue.rowNet && r.rowValue.rowNet.value) || 0);
    const cur = byVariant.get(key) || { rowIds: [], qty: 0, net: 0, productId: r.productId, sku: r.productSku };
    cur.rowIds.push(rowId); cur.qty += q; cur.net += net;
    byVariant.set(key, cur);
  }
  const merged = [], toDelete = [], toReadd = [];
  for (const [, info] of byVariant) {
    if (info.rowIds.length <= 1) continue;
    merged.push({ sku: info.sku, productId: info.productId, rows: info.rowIds.length, qty: info.qty });
    toDelete.push(...info.rowIds);
    toReadd.push({ productId: info.productId, qty: info.qty, unit: info.qty ? info.net / info.qty : 0 });
  }
  if (!execute) return { dryRun: true, poId, merged };
  for (const rowId of toDelete) { await liveWrite('DELETE', `/order-service/order/${poId}/row/${rowId}`); await pause(150); }
  for (const a of toReadd) {
    const net = a.unit * a.qty;
    const code = await productTaxCodeLive(a.productId);
    const rate = taxRate(code);
    await liveWrite('POST', `/order-service/order/${poId}/row`, { productId: a.productId, quantity: { magnitude: String(a.qty) }, rowValue: { taxCode: code, rowNet: { currency: 'GBP', value: net.toFixed(2) }, rowTax: { currency: 'GBP', value: (net * rate).toFixed(2) } } });
    await pause(150);
  }
  return { done: true, poId, merged };
}

// ── PRICE AUTO-HEAL ──────────────────────────────────────────────────────────
// After a placement, write the supplier's ACTUAL price back onto the product's cost so the next PO
// is right without anyone noticing the drift by hand. The rules that keep it safe:
//  • Writes ONLY the list that supplier reads (reg.costList). CL1001526 was correct on list 20 and
//    stale on list 10 while Snickers prices from 10 — "just write 20" would have healed nothing.
//  • Suppliers with costList null (Ralawise, Mascot, dynamic) are SKIPPED: no list of their own to
//    own, and they price off the sales-order row instead.
//  • A move must be MODEST to apply automatically: within PRICE_HEAL_MAX_PCT (default 20%) AND
//    PRICE_HEAL_MAX_ABS (default £5). Anything larger is ESCALATED to the error log for a human —
//    an 84% drop (CL1001526 £30.70 → £4.80) is as likely a parse or pack/unit error as a real price.
//  • A SKU that maps to more than one productId on the PO is skipped: several variants can share a
//    base SKU (25020400 spans four sizes), so we cannot tell which one the price belongs to.
//  • Never touches retail. Off unless PRICE_HEAL_ENABLED=true. Non-fatal. Every decision — applied,
//    escalated or skipped — is written to price_heal_log.
const HEAL_MAX_PCT = Number(process.env.PRICE_HEAL_MAX_PCT || 20) / 100;
const HEAL_MAX_ABS = Number(process.env.PRICE_HEAL_MAX_ABS || 5);
let _healLogReady = false;
async function writeHealLog(pool, rows) {
  if (!pool || !rows.length) return;
  if (!_healLogReady) {
    await pool.query(`CREATE TABLE IF NOT EXISTS price_heal_log (
      id BIGSERIAL PRIMARY KEY, at TIMESTAMPTZ DEFAULT now(), supplier TEXT, po_id INTEGER,
      product_id INTEGER, sku TEXT, list_id INTEGER, was NUMERIC, now_price NUMERIC,
      action TEXT, reason TEXT)`);
    await pool.query(`CREATE INDEX IF NOT EXISTS price_heal_sku_idx ON price_heal_log(sku)`);
    _healLogReady = true;
  }
  for (const r of rows) {
    await pool.query(`INSERT INTO price_heal_log (supplier, po_id, product_id, sku, list_id, was, now_price, action, reason)
      VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)`,
      [r.supplier, r.poId, r.productId, r.sku, r.listId, r.was, r.now, r.action, r.reason || null]);
  }
}
// Write one price list on a product, preserving every other populated list (same shape as the
// /product-price-live route). Cost only — retail is never in `overrides`.
async function setCostLive(productId, priceListId, value) {
  const cur = (await liveGet(`/product-service/product-price/${productId}`))[0];
  const lists = (cur && cur.priceLists) || [];
  const body = lists.filter((pl) => pl.quantityPrice && pl.quantityPrice['1'] != null)
    .map((pl) => ({ priceListId: pl.priceListId, quantityPrice: { '1': String(pl.priceListId === priceListId ? value : pl.quantityPrice['1']) } }));
  if (!body.some((pl) => pl.priceListId === priceListId)) body.push({ priceListId, quantityPrice: { '1': String(value) } });
  await liveWrite('PUT', `/product-service/product-price/${productId}/price-list`, { priceLists: body });
}
export async function healSupplierCosts({ supplierKey, poId, changes = [], pool = null, execute = false } = {}) {
  const key = String(supplierKey || '').toUpperCase();
  const reg = SUPPLIERS[key];
  if (!reg) return { skipped: `unknown supplier ${key}` };
  if (reg.costList == null) return { skipped: `${key} has no cost list of its own — nothing to heal` };
  // NEVER heal from a price that has not had the trade discount taken off it. Nearly every supplier
  // quotes a price that is already net — Helly Hansen is the exception: they show LIST and work the
  // discount out at the very end. Carhartt was flagged here too, on nothing better than sharing the
  // Elastic code path, and its sheet turned out to be net all along (103296 £11.50 = Launch £11.50,
  // vs RRP £16.65; 105072 £63.00 = £63.00 vs RRP £104.99). Do not assume from the integration style.
  // HH apply the discount
  // once, as a lump, at invoice — HH PO 483239 carried £661 of list-priced rows and a single
  // "42% Discount" row of -£277.62 to reach the real £383.38. Comparing that £80/£110/£37 against
  // BP's already-net cost makes every line look 35-45% stale: all nine "escalations" on
  // 2026-08-19 were this, and the empty-cost heal would have WRITTEN the inflated list price in.
  // BP's Launch(20) costs were correct the whole time. Until the portal hands us a net price per
  // line, these suppliers report for review and write nothing.
  // Once the rate is known the harvested price is converted to net before it ever reaches the PO
  // or the healer, so there is nothing left to guard against — only an UNKNOWN rate is dangerous.
  if (reg.portalPriceIsPreDiscount && !(reg.supplierDiscountPct > 0)) {
    return { supplier: key, listId: reg.costList, skipped: `${key}: the portal quotes LIST prices (discount applied at invoice) — cost healing is disabled for this supplier; check against the discounted invoice by hand` };
  }
  if (!changes.length) return { supplier: key, listId: reg.costList, applied: [], escalated: [], skipped: [] };
  // map SKU → productId from the PO's own rows, and spot SKUs shared by several variants
  const rows = await getOrderCartLines(poId).catch(() => []);
  const pidBySku = new Map();
  for (const r of rows) {
    if (!r.sku) continue;
    const k = String(r.sku).toUpperCase();
    const set = pidBySku.get(k) || new Set();
    set.add(String(r.productId)); pidBySku.set(k, set);
  }
  const applied = [], escalated = [], skipped = [], logRows = [];
  for (const c of changes) {
    const sku = String(c.sku || '').toUpperCase();
    const now = Number(c.now);
    const pids = pidBySku.get(sku);
    const rec = { supplier: key, poId, sku, listId: reg.costList, was: Number(c.was), now, productId: null };
    if (!Number.isFinite(now) || now <= 0) { skipped.push({ ...rec, reason: 'no usable supplier price' }); logRows.push({ ...rec, action: 'skipped', reason: 'no usable supplier price' }); continue; }
    if (!pids || pids.size === 0) { skipped.push({ ...rec, reason: 'SKU not on the PO' }); logRows.push({ ...rec, action: 'skipped', reason: 'SKU not on the PO' }); continue; }
    if (pids.size > 1) { skipped.push({ ...rec, reason: `SKU spans ${pids.size} variants — ambiguous` }); logRows.push({ ...rec, action: 'skipped', reason: 'SKU spans several variants' }); continue; }
    rec.productId = Number([...pids][0]);
    const was = await costOfLive(rec.productId, reg.costList, 0);
    rec.was = was;
    const diff = +(now - was).toFixed(4);
    if (Math.abs(diff) < 0.005) { skipped.push({ ...rec, reason: 'already correct' }); continue; }
    // A cost of ZERO is not a price that moved — it is a MISSING price, and the band was never
    // meant to protect it. On 2026-08-19 Helly Hansen raised nine review alerts in one run and five
    // were £0.00 costs (72183_590-L, 70295_269-L/-4XL, 79249_951-L, 79241_991-L/-S), each reported
    // as a "100% move" needing a human. There is no wrong direction to guard against: 0 is never a
    // real cost, and the supplier's own invoice price is authoritative. So heal it outright, and
    // record that it came from an empty cost rather than a normal move.
    const fromNothing = !(was > 0);
    if (fromNothing) {
      if (execute) { try { await setCostLive(rec.productId, reg.costList, now.toFixed(2)); await pause(150); } catch (e) { skipped.push({ ...rec, reason: 'write failed: ' + e.message }); logRows.push({ ...rec, action: 'failed', reason: e.message }); continue; } }
      applied.push({ ...rec, diff, fromNothing: true });
      logRows.push({ ...rec, action: execute ? 'applied' : 'would-apply', reason: 'BP cost was empty — filled from the supplier price' });
      continue;
    }
    const pct = Math.abs(diff) / was;
    if (pct > HEAL_MAX_PCT || Math.abs(diff) > HEAL_MAX_ABS) {
      const reason = `move of £${diff.toFixed(2)} (${(pct * 100).toFixed(0)}%) exceeds the auto-heal band (±${(HEAL_MAX_PCT * 100).toFixed(0)}% / ±£${HEAL_MAX_ABS}) — needs a human`;
      escalated.push({ ...rec, diff, reason });
      logRows.push({ ...rec, action: 'escalated', reason });
      continue;
    }
    if (execute) { try { await setCostLive(rec.productId, reg.costList, now.toFixed(2)); await pause(150); } catch (e) { skipped.push({ ...rec, reason: 'write failed: ' + e.message }); logRows.push({ ...rec, action: 'failed', reason: e.message }); continue; } }
    applied.push({ ...rec, diff });
    logRows.push({ ...rec, action: execute ? 'applied' : 'would-apply', reason: null });
  }
  if (execute) await writeHealLog(pool, logRows).catch(() => {});
  return { supplier: key, listId: reg.costList, dryRun: !execute, applied, escalated, skipped };
}

// Read a PO's contributing SOs from its OWN note (createComboPOLive writes "SO#<id> (<ref>):
// <sku> x<qty>, …" lines). Returns { soIds:[], linesByOrder:{ id:[{sku,qty}] } }. Used to
// finalise a REUSED PO whose live demand now nets to zero (it's already on order via that PO).
export async function getPoContributors(poId) {
  let notes = [];
  try { notes = (await liveGet(`/order-service/order/${poId}/note`)) || []; } catch { return { soIds: [], linesByOrder: {} }; }
  const text = (Array.isArray(notes) ? notes : []).map((x) => x.text || '').join('\n');
  const soIds = [], linesByOrder = {};
  for (const line of text.split('\n')) {
    const m = line.match(/SO#(\d+)\s*\([^)]*\):\s*(.+)$/);
    if (!m) continue;
    const id = Number(m[1]); soIds.push(id);
    const items = [];
    for (const tok of m[2].split(',')) { const t = tok.trim().match(/^([A-Za-z0-9]+)\s*x(\d+)/); if (t) items.push({ sku: t[1], qty: Number(t[2]) }); }
    linesByOrder[id] = items;
  }
  return { soIds: [...new Set(soIds)], linesByOrder };
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

// LIVE: clear a supplier PO custom field (e.g. PCF_CASTLEPO) off SOs — used when a PO
// was scrapped/voided but its number is still stamped on the contributing orders. Safe:
// reads the field first and only removes it when it matches ifValue (so it can't wipe a
// still-valid PO number). Dry-run unless execute:true.
export async function clearOrderCustomFieldLive({ orderIds = [], field, ifValue = null, execute = false } = {}) {
  if (!field) return { error: 'field required' };
  if (!execute) return { dryRun: true, field, ifValue, orderIds };
  const results = [];
  for (const id of orderIds) {
    try {
      const cf = (await liveGet(`/order-service/order/${id}/custom-field`)) || {};
      const cur = cf[field];
      if (cur === undefined || cur === null || String(cur) === '') { results.push({ id, skipped: 'already empty' }); continue; }
      if (ifValue != null && String(cur) !== String(ifValue)) { results.push({ id, skipped: `value ${cur} != ${ifValue}` }); continue; }
      await liveWrite('PATCH', `/order-service/order/${id}/custom-field`, [{ op: 'remove', path: `/${field}` }]);
      results.push({ id, cleared: field, was: cur });
    } catch (e) { results.push({ id, error: e.message }); }
    await pause(150);
  }
  return { done: true, field, results };
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
