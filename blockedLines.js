// ── WHICH LINE STOPPED THE ORDER ─────────────────────────────────────────────
// A supplier run is usually abandoned over ONE line. The basket guard is right to refuse a short
// order, but the cost is the whole PO sits unplaced while the offending item is buried — in a
// context blob, in a JSON array inside the message, or only in the supplier's own wording. Finding
// it has meant reading raw error rows by hand, every time.
//
// This turns any purchasing error row into the lines that blocked it, so the hub can show the item,
// its size and quantity, and the supplier's REASON — enough to go and find the right code without
// opening the log.
//
// Deliberately best-effort and additive: an error shape it does not recognise yields [] and the row
// is still shown with its message. It must never INVENT a line, because a wrong SKU here sends
// someone hunting a product that was never the problem.

// Every shape below was taken from real rows in the live log (2026-09-14), not imagined.
const str = (v) => (v == null ? null : String(v).trim() || null);
const clean = (s) => String(s || '').replace(/\s+/g, ' ').trim();

// A token that looks like a supplier item code. Deliberately broad — these span house SKUs
// (125949-171-406), barcodes (5063777009268), HH underscore codes (79242_900-L) and Chadwick's
// slashed codes (925-40/37-A-S) — but it must still reject prose, or every message would "contain"
// a line. Requires a digit and no lowercase, which prose always has.
// No dot: a decimal is money, not a code, and "3,084.60 GBP" in a scraped cart page otherwise
// yields "084.60" as an item to go and look for.
const CODE = /^(?=.*\d)[A-Z0-9][A-Z0-9_/-]{4,}$/;
const looksLikeCode = (t) => CODE.test(String(t || '').trim());

// The free-text scrape is the last resort and prose is full of code-shaped things — our own order
// ids most of all ("Stopped: Order ID (TUWO_TW486420) already exists"), which read exactly like a
// product and send someone hunting one that never existed. So only scrape a message that SAYS it is
// naming items it could not handle.
const NAMES_ITEMS = /unresolved|not in the|not found|NOT placed|codes? don'?t match|sku\(s\)|item codes?/i;

// Pull a trailing comma/space separated list of codes off a message, e.g.
//   "...not in the product-data file (order NOT placed): 5063777009268."
//   "refusing — 3 unresolved SKU(s): 79242_900-L, 75117_991-L, 75106_595-L"
function codesAfterColon(message) {
  const m = /:\s*([^:]*)$/.exec(String(message || ''));
  if (!m) return [];
  // Tokenise rather than splitting on commas: the codes are usually followed by prose in the same
  // breath — "…(order NOT placed): 5063777009268. Update the Sterling product-data file / ingest."
  // — so the comma-chunk is code AND sentence, and matched nothing.
  // A number introduced as a PO/SO/order is OURS, not an item. Chadwick's refusal message explains
  // itself with "(a Brightpearl SKU carrying a stray "CT" prefix did this on PO 488574)", and 488574
  // was listed on the Stuck items tab as a third thing to go and find. Same class of false positive
  // as the order id TUWO_TW486420; the difference is that a bare number passes CODE on its own, so
  // it has to be caught by what INTRODUCES it.
  const ours = new Set();
  for (const mm of String(message).matchAll(/\b(?:PO|SO|order)\s*#?\s*(\d{4,})\b/gi)) ours.add(mm[1]);
  return m[1].split(/[\s,;]+/)
    .map((x) => x.replace(/^[("']+|[.,)"']+$/g, '').trim())
    .filter((x) => looksLikeCode(x) && !ours.has(x));
}

// Some rows carry their detail as JSON INSIDE the message rather than in context — the Fristads
// cart failure is the live example. Find the first balanced [...] and parse it.
function jsonArrayInMessage(message) {
  const s = String(message || '');
  const start = s.indexOf('[');
  if (start < 0) return null;
  let depth = 0;
  for (let i = start; i < s.length; i++) {
    if (s[i] === '[') depth++;
    else if (s[i] === ']' && --depth === 0) {
      try { const v = JSON.parse(s.slice(start, i + 1)); return Array.isArray(v) ? v : null; } catch { return null; }
    }
  }
  return null;
}

const line = (o) => {
  // `o.line` is Sterling's identifier: its worker resolves by STYLE NAME + size, never a SKU, so
  // without this every Sterling failure yields nothing — and Sterling fails on resolution more than
  // any other supplier. "Mercury size 9" is just as findable by hand as a code.
  const sku = str(o.sku || o.stockCode || o.Item || o.item || o.code || o.line);
  if (!sku) return null;
  return {
    sku,
    name: str(o.name || o.productName || o.Description) || null,
    size: str(o.size || o.Size) || null,
    qty: o.qty != null ? Number(o.qty) : (o.wanted != null ? Number(o.wanted) : (o.want != null ? Number(o.want) : (o.Quantity != null ? Number(o.Quantity) : null))),
    reason: clean(o.reason || o.Message || o.note || o.status || '') || null,
    // what the supplier says it can supply, when it said anything — Fristads gives both
    avail: o.avail != null ? Number(o.avail) : null,
    deldate: str(o.deldate) || null,
  };
};

export function extractBlockedLines(row) {
  const ctx = (row && row.context) || {};
  const msg = String((row && row.message) || '');
  const out = [];
  const push = (o) => { const l = o && line(o); if (l) out.push(l); };

  // 1. lines we deliberately took off the PO (Fristads out-of-stock, Chadwick rejected code)
  for (const d of ctx.dropped || []) push(d);

  // 2. codes the supplier could not resolve. Strings (Helly Hansen) or objects (PenCarrie).
  for (const u of ctx.unresolved || []) push(typeof u === 'string' ? { sku: u, reason: 'not found on the portal' } : u);

  // 3. lines that never reached the basket (Snickers)
  for (const m of ctx.missingLines || []) push({ ...m, reason: m.reason || (m.inCart ? `only ${m.inCart} of ${m.wanted} in basket` : 'not in basket') });

  // 4. per-line results where the line failed (Performance Brands)
  for (const r of ctx.results || []) if (r && r.ok === false) push(r);

  // 5. a nested supplier response (Chadwick, Mascot, Helly Hansen via Alt-Items)
  const resp = ctx.response || {};
  const basket = resp.basket || {};
  for (const m of resp.missing || []) push(typeof m === 'string' ? { sku: m, reason: 'not accepted by the supplier' } : m);
  for (const e of basket.errors || []) push({ sku: e.Id || e.Number, size: e.Size, reason: e.Message });
  // Chadwick names the offending code in the 400 body itself
  const invalid = /invalid item[:\s]+([^\s,;"'}\]]+)/i.exec(String(basket.resp || resp.error || ''));
  if (invalid) push({ sku: invalid[1], reason: 'the supplier rejected this item code outright' });
  for (const r of ctx.rejected || []) push(typeof r === 'string' ? { sku: r, reason: 'the supplier rejected this item code outright' } : r);

  // 6. detail carried as JSON inside the message (Fristads cart, Sterling's worker payload)
  if (!out.length) {
    const arr = jsonArrayInMessage(msg) || [];
    // When the entries carry an ok flag, only the FAILED ones are blocking — a Sterling payload
    // lists every line it added alongside the one it could not.
    const failedOnly = arr.filter((o) => o && o.ok === false);
    for (const o of (failedOnly.length ? failedOnly : arr.filter((o) => !o || o.ok !== true))) push(o);
  }
  // 7. last resort: a trailing list of codes in the message (Sterling, Helly Hansen) — only when
  // the message says it is naming items, or prose supplies false ones.
  if (!out.length && NAMES_ITEMS.test(msg)) {
    for (const c of codesAfterColon(msg)) push({ sku: c, reason: 'named in the failure message' });
  }

  // de-dupe on sku+size, keeping the entry that says the most
  const best = new Map();
  for (const l of out) {
    const k = `${l.sku}|${l.size || ''}`.toUpperCase();
    const cur = best.get(k);
    if (!cur) { best.set(k, l); continue; }
    best.set(k, {
      ...cur,
      name: cur.name || l.name, size: cur.size || l.size, qty: cur.qty ?? l.qty,
      reason: cur.reason || l.reason, avail: cur.avail ?? l.avail, deldate: cur.deldate || l.deldate,
    });
  }
  return [...best.values()];
}

// Did this row stop an order, or merely annotate one? Only the first kind needs a person.
export const isBlockingRow = (row) => row && row.severity === 'error' && !row.handled_at;
