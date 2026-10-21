// ── PenCarrie → Ralawise handover (Dec, 2026-10-14) ──────────────────────────
// Ralawise (16:00) picks up the lines PenCarrie (15:40) cannot SUPPLY: discontinued, or not enough
// free stock for the WHOLE line. Whole lines only — 3 of 5 at PenCarrie means all 5 from Ralawise —
// and only when Ralawise can take the whole line on one exact SKU. Anything else (a code PenCarrie's
// resolver cannot map, an API error, Ralawise short too) stays PenCarrie's exactly as before:
// PenCarrie back-orders a shortfall (assumebo=1), or the line fails there and is reported.
//
// WHY BEFORE PENCARRIE PLACES: once PenCarrie has taken a line it is a PenCarrie back order and is
// ON ORDER — Ralawise touching it then buys it twice. So the decision is made while PenCarrie's PO
// is being built, the line is left off it, and it becomes a Ralawise carry-forward line
// (pendingLines.js), which createPo appends to Ralawise's next order and consumes only when that
// order PLACES.
//
// NEVER TWICE: every handed-over (SO, SKU) is excluded from BOTH lanes' demand from then on, for
// good (120 days). The demand maths does not subtract stock on order, so without this PenCarrie
// would re-order the row the day after Ralawise bought it.
//
// added_by says what the SO had left at PenCarrie:
//   handover:PENCARRIE:only  — every PenCarrie line on the SO went to Ralawise. PenCarrie will never
//                              finalise it, so Ralawise's finalise clears the PENCARRIE tag.
//   handover:PENCARRIE:mixed — PenCarrie still orders the rest and its own finalise clears the tag.
//
// ON since Ralawise went live (owner, 2026-10-21; no env gate). The exclusion of PAST handovers
// applies regardless.
import { addPendingLine, listPendingLines } from './pendingLines.js';

export const HANDOVER_TAG = 'handover:PENCARRIE';
export const ralawiseFallbackLive = () => true;
export const handoverKey = (soId, sku) => `${Number(soId)}:${String(sku || '').trim().toUpperCase()}`;
const altItems = () => process.env.ALT_ITEMS_URL || 'https://alternate-items.onrender.com';

// Every (SO, SKU) ever handed over. FAILS CLOSED: a read error throws, so no PO is built on a
// demand read that might re-order a handed-over row.
export async function handedOverKeys(pool) {
  if (!pool) return new Set();
  await listPendingLines(pool, 'RALAWISE');   // creates the table if this is its first use
  const r = await pool.query(
    `SELECT so_id, sku FROM purchasing_pending_lines
      WHERE added_by LIKE $1 AND so_id IS NOT NULL AND created_at > now() - interval '120 days'`,
    [`${HANDOVER_TAG}%`],
  );
  return new Set(r.rows.map((x) => handoverKey(x.so_id, x.sku)));
}

// lines: PenCarrie SO lines [{ order, productId, sku, name, colour, size, qty }].
// identityOf(productIds) → { [productId]: { sku, mpn, ean, barcode } } (best-effort).
// Returns { decisions, handed: [line + decision] }. Writes ONLY when execute is true.
export async function checkHandover({ lines, identityOf, execute = false, pool = null }) {
  const soLines = (lines || []).filter((l) => l.order && l.sku && l.qty > 0);
  if (!soLines.length) return { decisions: [], handed: [] };
  let ids = {};
  try { ids = (identityOf && await identityOf([...new Set(soLines.map((l) => l.productId).filter(Boolean))])) || {}; } catch { ids = {}; }
  const body = soLines.map((l, i) => {
    const id = ids[String(l.productId)] || {};
    return { key: i, sku: String(id.sku || l.sku), rowSku: String(l.sku), mpn: id.mpn || null, ean: id.ean || null, barcode: id.barcode || null, name: l.name, colour: l.colour || null, size: l.size || null, qty: Math.round(l.qty) };
  });
  // A failed check hands nothing over: every line stays PenCarrie's, which is today's behaviour.
  let decisions = [];
  try {
    const r = await fetch(`${altItems()}/api/pencarrie-ralawise-handover`, {
      method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ lines: body }),
      signal: AbortSignal.timeout(Number(process.env.HANDOVER_CHECK_MS || 600000)),
    });
    const j = await r.json();
    decisions = Array.isArray(j.decisions) ? j.decisions : [];
  } catch (e) { return { decisions: [], handed: [], error: `handover check failed (${e.message}) — every line stays with PenCarrie` }; }
  const handed = [];
  for (const d of decisions) {
    const l = soLines[Number(d.key)];
    if (!l || d.handover !== true || !d.ralSku) continue;
    handed.push({ ...l, decision: d });
  }
  if (execute && handed.length && pool) {
    const handedBySo = new Map();
    for (const h of handed) handedBySo.set(h.order, (handedBySo.get(h.order) || 0) + 1);
    const allBySo = new Map();
    for (const l of soLines) allBySo.set(l.order, (allBySo.get(l.order) || 0) + 1);
    for (const h of handed) {
      const only = handedBySo.get(h.order) === allBySo.get(h.order);
      await addPendingLine(pool, {
        supplier: 'RALAWISE', sku: h.sku, qty: Math.round(h.qty), soId: h.order,
        note: `From PenCarrie: ${h.decision.reason} - Ralawise ${h.decision.ralSku} (${h.decision.ralAvail} free)`,
        addedBy: `${HANDOVER_TAG}:${only ? 'only' : 'mixed'}`,
      });
    }
  }
  return { decisions, handed };
}

// The SOs Ralawise's finalise must also settle for PENCARRIE: those whose every PenCarrie line was
// handed over (PenCarrie never orders them, so nothing else will clear that tag).
export function handoverOnlySoIds(carryLines) {
  return [...new Set((carryLines || []).filter((l) => l.soId && /^handover:PENCARRIE:only$/.test(String(l.addedBy || ''))).map((l) => Number(l.soId)))];
}
