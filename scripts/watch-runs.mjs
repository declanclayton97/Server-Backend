#!/usr/bin/env node
// Independent watcher for a day's purchasing runs. Answers one question: did anything happen
// today that SHOULDN'T have? It places nothing, changes nothing, and needs no credentials — it
// reads the backend's own read-only routes.
//
// Written 2026-08-25 after a day where two suppliers failed and the damage was only visible by
// hand-diffing POs against carts. Every check below is a thing that actually went wrong, not a
// thing that might.
//
//   node scripts/watch-runs.mjs              # today, human-readable
//   node scripts/watch-runs.mjs --json       # machine-readable
//   node scripts/watch-runs.mjs --date=2026-08-25
//
// Exit code: 0 = nothing to flag, 1 = at least one FLAG. So it can drive a scheduled task.

const BASE = process.env.WATCH_BASE || 'https://server-backend-1i47.onrender.com';
const args = process.argv.slice(2);
const asJson = args.includes('--json');
const dateArg = (args.find((a) => a.startsWith('--date=')) || '').split('=')[1] || null;

// Supplier contactIds, so a PO can be attributed without guessing. Kept here rather than imported
// so this script stays runnable on its own with no backend module loading.
const SUPPLIERS = {
  37419: 'FRISTADS', 204: 'PENCARRIE', 205: 'RALAWISE', 214: 'HELLY HANSEN', 298: 'PORTWEST',
  322: 'UNEEK', 323: 'BLAKLADER', 331: 'SNICKERS', 42485: 'CHADWICK', 130243: 'SCRUFFS',
};
// PO statuses, read from BP 2026-08-25. The names matter: the first version of this script flagged
// "more than one PO today" on any two POs and produced three false positives on its very first run
// — a CANCELLED Blaklader PO, a cancelled Portwest one, and a legitimate back-order. A watcher that
// cries wolf gets ignored, which is worse than no watcher.
const PO_STATUS = {
  6: 'Pending PO', 7: 'Placed with supplier', 45: 'On Back Order',
  62: 'PO Cancelled', 68: 'Invoice Received Check stock', 86: 'Sportswear Placed With Supplier',
};
const PENDING = 6;
const PLACED_STATES = new Set([7, 86]);          // actually sent to the supplier
const DEAD_STATES = new Set([62]);               // cancelled — settled, never a duplicate
const stName = (s) => PO_STATUS[s] || `status ${s}`;

const ukDate = () => {
  const p = {};
  for (const x of new Intl.DateTimeFormat('en-GB', { timeZone: 'Europe/London', year: 'numeric', month: '2-digit', day: '2-digit' }).formatToParts(new Date())) p[x.type] = x.value;
  return `${p.year}-${p.month}-${p.day}`;
};
const DATE = dateArg || ukDate();

async function get(path, ms = 120000) {
  const r = await fetch(`${BASE}${path}`, { signal: AbortSignal.timeout(ms) });
  const t = await r.text();
  try { return JSON.parse(t); } catch { throw new Error(`non-JSON from ${path}: ${t.slice(0, 120)}`); }
}
const bp = (p) => get(`/api/purchasing/bp-live-get?path=${encodeURIComponent(p)}`);

const flags = [];   // things that should not be true
const notes = [];   // things worth seeing but not wrong
const flag = (supplier, what, detail) => flags.push({ supplier, what, detail });
const note = (supplier, what, detail) => notes.push({ supplier, what, detail });

// ── 1. Every PO raised today, by supplier ────────────────────────────────────
async function posToday() {
  const s = await bp(`/order-service/order-search?orderTypeId=2&createdOn=${DATE}/&pageSize=500`);
  const cols = ((s.metaData && s.metaData.columns) || []).map((c) => c.name);
  const i = (n) => cols.indexOf(n);
  if (i('contactId') < 0) throw new Error('PO search returned no contactId column — cannot attribute POs');
  return (s.results || []).map((r) => ({
    poId: r[i('orderId')], status: Number(r[i('orderStatusId')]),
    contactId: r[i('contactId')], supplier: SUPPLIERS[r[i('contactId')]] || null,
    createdOn: String(r[i('createdOn')] || ''),
  }));
}

// ── 2. A PO at Placed whose sales orders were never finalised ────────────────
// Finalise is the ONLY double-order guard: an SO left tagged and on "Stock needs ordering" will be
// picked up again tomorrow and ordered twice. This is the single most expensive thing to miss.
async function checkFinalised(po) {
  let soIds = [];
  try {
    const notesArr = await bp(`/order-service/order/${po.poId}/note`);
    const txt = (Array.isArray(notesArr) ? notesArr : []).map((n) => n.text || '').join('\n');
    soIds = [...new Set([...txt.matchAll(/SO#(\d+)/g)].map((m) => Number(m[1])))];
  } catch { return note(po.supplier, 'could not read PO note', `PO ${po.poId} — contributors unknown, finalise NOT verified`); }
  if (!soIds.length) return note(po.supplier, 'no SO contributors in the PO note', `PO ${po.poId} (low-inv only, or the note did not record them)`);
  const stillTagged = [];
  for (const so of soIds.slice(0, 60)) {
    try {
      const cf = await bp(`/order-service/order/${so}/custom-field`);
      const tag = String((cf && cf.PCF_SUPPLIER) || '');
      // The supplier still naming itself on the tag means finalise did not clear it.
      if (tag && new RegExp(po.supplier.replace(/[.*+?^${}()|[\]\\]/g, '\\$&'), 'i').test(tag)) stillTagged.push({ so, tag });
    } catch { /* a single unreadable SO is not evidence of anything */ }
  }
  if (stillTagged.length) {
    flag(po.supplier, 'PLACED but sales orders still tagged for it', `PO ${po.poId}: ${stillTagged.length} of ${soIds.length} SOs still carry the tag — finalise did not run or did not clear. These WILL be re-ordered. e.g. ${stillTagged.slice(0, 4).map((x) => `SO ${x.so} "${x.tag}"`).join('; ')}`);
  }
}

async function main() {
  const out = { date: DATE, base: BASE, checkedAt: new Date().toISOString() };

  const pos = await posToday();
  // Name any supplier not in the table above, rather than printing a bare contact id — an
  // unnamed row is one a reader skips.
  for (const p of pos) {
    if (p.supplier) continue;
    try {
      const c = await bp(`/contact-service/contact/${p.contactId}`);
      const o = Array.isArray(c) ? c[0] : c;
      p.supplier = (o && o.organisation && o.organisation.name) || (o && o.companyName) || `contact ${p.contactId}`;
    } catch { p.supplier = `contact ${p.contactId}`; }
  }
  out.pos = pos;

  // ── duplicate POs for one supplier: the failure-then-retry loop ────────────
  const bySupplier = {};
  for (const p of pos) (bySupplier[p.supplier] = bySupplier[p.supplier] || []).push(p);
  for (const [sup, list] of Object.entries(bySupplier)) {
    const live = list.filter((p) => !DEAD_STATES.has(p.status));
    const placed = live.filter((p) => PLACED_STATES.has(p.status));
    const drafts = live.filter((p) => p.status === PENDING);
    // TWO ORDERS ACTUALLY SENT is the expensive one — that is a real double order at the supplier,
    // and it cannot be undone by a revert; someone has to ring them.
    if (placed.length > 1) {
      flag(sup, 'TWO POs PLACED WITH THE SUPPLIER', `${placed.map((p) => `${p.poId}(${stName(p.status)})`).join(', ')} — this is a DOUBLE ORDER unless one was deliberate. Check both at the supplier before anything else.`);
    }
    // A draft sitting next to a live PO is the failure-then-retry loop: the retry minted a new PO
    // instead of adopting the orphan, and the orphan's rows suppressed the low-inv read.
    if (drafts.length && live.length > drafts.length) {
      flag(sup, 'a draft PO alongside a live one', `${live.map((p) => `${p.poId}(${stName(p.status)})`).join(', ')} — a retry should ADOPT the failed run's PO, not mint another. Check adoptedPo in the run output.`);
    }
    if (list.length > live.length) {
      note(sup, 'a cancelled PO today', `${list.filter((p) => DEAD_STATES.has(p.status)).map((p) => p.poId).join(', ')} — settled, not counted as a duplicate.`);
    }
  }

  // ── orphan drafts left behind ─────────────────────────────────────────────
  for (const p of pos.filter((x) => x.status === PENDING)) {
    flag(p.supplier, 'draft PO left open', `PO ${p.poId} is still status 6 (created ${p.createdOn.slice(11, 16)}). Either the run failed after creating it, or it was never placed. Its rows count as ON ORDER and will suppress tomorrow's low-inventory read.`);
  }

  // ── placed POs: was the demand actually closed out? ────────────────────────
  for (const p of pos.filter((x) => PLACED_STATES.has(x.status))) await checkFinalised(p);

  // ── the error log ─────────────────────────────────────────────────────────
  const errs = await get('/api/purchasing/error-log?sinceHours=24&limit=60&includeInfo=1');
  out.errors = (errs.errors || []).map((e) => ({ id: e.id, supplier: e.supplier, step: e.step, severity: e.severity, handled: !!e.handled_at, at: e.created_at, poId: e.context && e.context.poId, message: String(e.message || '').slice(0, 160) }));
  for (const e of out.errors) {
    if (e.severity === 'error' && !e.handled) flag(e.supplier, `unhandled ERROR (#${e.id})`, `${e.step}: ${e.message}`);
    // A review row that says placed:false is NOT evidence an order went out — see
    // reference-severity-review-is-not-proof-of-an-order. Only note it.
    if (e.severity === 'review' && !e.handled) note(e.supplier, `review row (#${e.id})`, `${e.step}: ${e.message}`);
  }

  // ── suppliers that placed nothing at all ──────────────────────────────────
  const placedSuppliers = new Set(pos.filter((x) => PLACED_STATES.has(x.status)).map((x) => x.supplier));
  out.placed = [...placedSuppliers];

  out.flags = flags; out.notes = notes;
  if (asJson) { console.log(JSON.stringify(out, null, 2)); return flags.length ? 1 : 0; }

  console.log(`\nPurchasing watch — ${DATE}\n${'='.repeat(52)}`);
  console.log(`POs raised today: ${pos.length}`);
  for (const [sup, list] of Object.entries(bySupplier).sort()) {
    console.log(`  ${sup.padEnd(16)} ${list.map((p) => `${p.poId}(${stName(p.status)})`).join(', ')}`);
  }
  console.log(`\nErrors in the last 24h: ${out.errors.length} (${out.errors.filter((e) => !e.handled).length} unhandled)`);
  if (!flags.length) console.log(`\n✅ Nothing to flag.`);
  else {
    console.log(`\n⚠  ${flags.length} FLAG(S) — things that should not be true:\n`);
    for (const f of flags) console.log(`  [${f.supplier}] ${f.what}\n      ${f.detail}\n`);
  }
  if (notes.length) {
    console.log(`ℹ  ${notes.length} note(s):\n`);
    for (const n of notes) console.log(`  [${n.supplier}] ${n.what} — ${n.detail}`);
  }
  console.log('');
  return flags.length ? 1 : 0;
}

main().then((code) => process.exit(code)).catch((e) => { console.error('watch-runs failed:', e.message); process.exit(2); });
