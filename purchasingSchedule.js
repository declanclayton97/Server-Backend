// Scheduled Fristads purchasing (weekday 10:30 UK). Each run dry-runs the combined
// PO (SO demand + low-inventory), and:
//   • net ≥ £300 (ex-VAT, free-shipping threshold) → place the whole order;
//   • net < £300 → hold and re-check next working day; on the 3rd working day of
//     waiting, place it anyway (Fristads adds carriage).
// State (working-days-waited, last-run-date) is persisted in Postgres so it survives
// restarts. When placing, it runs the full chain we validated by hand:
//   createComboPOLive → cart (Alt-Items) → checkout/placeorder (Alt-Items) →
//   pull order# (Alt-Items) → reference-write + tax-restore + status 7 → finalize SOs.

import nodemailer from 'nodemailer';
import * as bp from './purchasingAuto.js';
import { updateOrderReference, emailOrderDocument } from './bpWebSession.js';

const THRESHOLD_NET = Number(process.env.FRISTADS_FREESHIP_THRESHOLD || 300); // £ ex-VAT
// Where an order goes when part of it turned out to be discontinued. NOT 22 ("Ordered Stock
// Awaiting Delivery") — that would claim the whole order is on its way. 60 is "Order Confirmation
// Sent", which the low inventory report excludes from Open SO (with 1 and 18), so parking here also
// stops anything re-ordering behind it while a person decides on a substitute or a refund.
const DISCONTINUED_PARK_STATUS = Number(process.env.DISCONTINUED_PARK_STATUS_ID || 60);
const MAX_WAIT_WORKING_DAYS = 3;
const NOTIFY_TO = process.env.PURCHASING_SCHEDULE_EMAIL || 'dec@tuffshop.co.uk';
const FRISTADS_SUPPLIER_CONTACT = 37419;
const CASTLE_SUPPLIER_CONTACT = 332;
const STERLING_SUPPLIER_CONTACT = 341;
const STERLING_WORKER_URL = process.env.STERLING_WORKER_URL || 'https://portal-order-worker.onrender.com';
const STERLING_WORKER_SECRET = process.env.STERLING_WORKER_SECRET || '';

let running = false; // in-process guard against overlapping runs

// The PO id of the run IN FLIGHT, so a failure that happens AFTER the PO exists can be adopted by
// the next attempt instead of minting a second PO for the same demand. Until now only Blaklader's
// checkout error carried a poId — every other supplier throws through jfetch(), which attaches no
// context at all, so findRecordedFailedPo() had nothing to match on and the fixer built a NEW PO
// every time. MASCOT 2026-08-27 is the case: PO 485126 failed at the basket, the retry raised
// 485130, and the three sales-order lines then sat on both.
//
// Reset at the START of every run, so a stale id from an earlier run can never be attached to an
// unrelated failure — adopting the WRONG PO is far worse than not adopting at all. Safe as module
// state because `running` above permits only one run in this process at a time.
let activePoId = null;

// Every PO creation goes through here so the id is captured in ONE place rather than 13. Preview
// calls (execute:false) come back created:false and are deliberately NOT recorded — there is no PO.
async function createPo(opts) {
  const po = await bp.createComboPOLive(opts);
  if (po && po.created && po.poId) activePoId = po.poId;
  return po;
}

// ── UK local time ────────────────────────────────────────────────────────────
export function ukNow(d = new Date()) {
  const p = {};
  for (const x of new Intl.DateTimeFormat('en-GB', { timeZone: 'Europe/London', weekday: 'short', year: 'numeric', month: '2-digit', day: '2-digit', hour: '2-digit', minute: '2-digit', hour12: false }).formatToParts(d)) p[x.type] = x.value;
  return { weekday: p.weekday, date: `${p.year}-${p.month}-${p.day}`, hour: +p.hour, minute: +p.minute };
}
export const isUkWeekday = (wd) => !['Sat', 'Sun'].includes(wd);

// ── state ────────────────────────────────────────────────────────────────────
// ── Carry-forward lines ───────────────────────────────────────────────────────
// Things that must go on a supplier's NEXT order but which BP demand will never produce again,
// because the sales order they belong to was already finalised. First case: PO 483480 bought ONE
// 3625 shirt where the BP unit is a 5-pack, so four are owed to SO 483415 — and that SO is closed,
// so no future demand scan will ever ask for them.
//
// A note in a PO or an email does not order anything. This does: the next run for that supplier
// appends these lines to the cart, and only marks them consumed once the order is actually placed.
// If the run aborts they stay pending and go on the run after.
//
// qty is in the SUPPLIER'S OWN UNITS and is sent RAW — no multipack multiplication. The 3625 case is
// four PIECES, which is not a whole BP pack, and that is exactly the shape these will usually take.
async function ensurePendingTable(pool) {
  await pool.query(`CREATE TABLE IF NOT EXISTS purchasing_pending_lines (
    id serial PRIMARY KEY,
    supplier text NOT NULL,
    sku text NOT NULL,
    qty int NOT NULL,
    note text,
    created_at timestamptz DEFAULT now(),
    consumed_at timestamptz,
    consumed_po int
  )`);
}

export async function addPendingLine(pool, { supplier, sku, qty, note }) {
  if (!pool) return { error: 'no database' };
  await ensurePendingTable(pool);
  const r = await pool.query(
    `INSERT INTO purchasing_pending_lines (supplier, sku, qty, note) VALUES ($1,$2,$3,$4) RETURNING *`,
    [String(supplier).toUpperCase(), String(sku), Math.round(Number(qty) || 0), note || null],
  );
  return r.rows[0];
}

export async function listPendingLines(pool, supplier, { includeConsumed = false } = {}) {
  if (!pool) return [];
  await ensurePendingTable(pool);
  const r = await pool.query(
    `SELECT * FROM purchasing_pending_lines WHERE supplier=$1 ${includeConsumed ? '' : 'AND consumed_at IS NULL'} ORDER BY id`,
    [String(supplier).toUpperCase()],
  );
  return r.rows;
}

export async function updatePendingLine(pool, id, { qty, note, remove, consumedPoId } = {}) {
  if (!pool) return { error: "no database" };
  await ensurePendingTable(pool);
  // Mark a line FULFILLED rather than deleting it. When PO 483751 was placed by hand the only
  // option was `remove`, which threw away the record of what was owed and why; consumed_at +
  // consumed_po keep it. `remove` stays for lines added in error.
  if (consumedPoId) {
    const c = await pool.query(
      "UPDATE purchasing_pending_lines SET consumed_at=now(), consumed_po=$2 WHERE id=$1 AND consumed_at IS NULL RETURNING *",
      [id, consumedPoId],
    );
    return c.rows[0] || { error: "not found or already consumed" };
  }
  if (remove) { await pool.query("DELETE FROM purchasing_pending_lines WHERE id=$1 AND consumed_at IS NULL", [id]); return { removed: id }; }
  const r = await pool.query(
    "UPDATE purchasing_pending_lines SET qty=COALESCE($2,qty), note=COALESCE($3,note) WHERE id=$1 AND consumed_at IS NULL RETURNING *",
    [id, qty != null ? Math.round(Number(qty)) : null, note != null ? String(note) : null],
  );
  return r.rows[0] || { error: "not found or already consumed" };
}

async function consumePendingLines(pool, ids, poId) {
  if (!pool || !ids.length) return;
  await pool.query(`UPDATE purchasing_pending_lines SET consumed_at=now(), consumed_po=$2 WHERE id = ANY($1::int[])`, [ids, poId]);
}

async function ensureTable(pool) {
  await pool.query(`CREATE TABLE IF NOT EXISTS fristads_purchase_schedule (
    id int PRIMARY KEY DEFAULT 1,
    working_days_waited int NOT NULL DEFAULT 0,
    last_run_date date,
    last_result jsonb,
    updated_at timestamptz DEFAULT now()
  )`);
  await pool.query(`INSERT INTO fristads_purchase_schedule (id) VALUES (1) ON CONFLICT (id) DO NOTHING`);
}
// State rows are keyed by id so each supplier keeps its own working-days-waited /
// last-run-date (Fristads=1, Castle=2, …). ensureTable seeds the row for `id`.
async function getState(pool, id = 1) {
  await pool.query(`INSERT INTO fristads_purchase_schedule (id) VALUES ($1) ON CONFLICT (id) DO NOTHING`, [id]);
  const r = await pool.query(`SELECT * FROM fristads_purchase_schedule WHERE id=$1`, [id]);
  return r.rows[0];
}
async function saveState(pool, { id = 1, workingDaysWaited, lastRunDate, result }) {
  await pool.query(
    `UPDATE fristads_purchase_schedule SET working_days_waited=$1, last_run_date=COALESCE($2,last_run_date), last_result=$3, updated_at=now() WHERE id=$4`,
    [workingDaysWaited, lastRunDate, result ? JSON.stringify(result) : null, id],
  );
}

// ── error log + alerts ───────────────────────────────────────────────────────
// Tag an error with the step that raised it so the log/alert is specific.
const stepErr = (step, message, context = null) => Object.assign(new Error(message), { step, ...(context ? { context } : {}) });
// create-po specifically: if the PO shell was already created before the row/note/stamp write
// that failed, e.poId names it — attach it to context so findRecordedFailedPo (the orphan-adoption
// check in purchasingAuto.js) can find and empty it on retry instead of a force-run minting a
// second PO alongside the first (which then sits uncounted as "on order" forever).
const createPoErr = (e) => stepErr('create-po', `Brightpearl error building the PO: ${e.message}`, e.poId ? { poId: e.poId } : null);

// Exported because server.js used to CREATE this table itself with its own column list, so a
// column added here was missing over there depending on which ran first. One definition now.
export async function ensureErrorTable(pool) {
  await pool.query(`CREATE TABLE IF NOT EXISTS purchasing_error_log (
    id serial PRIMARY KEY,
    created_at timestamptz DEFAULT now(),
    supplier text,
    step text,
    message text,
    context jsonb
  )`);
  // severity was only ever passed to the email, never stored — so nothing reading the log could
  // tell "the run stopped" from "the order went through, fix the data afterwards".
  // handled_* lets an automated triage pass claim a row, so the same error isn't worked twice.
  // triage_sig/triage_fired_at exist to stop the routine being fired at the same unfixable failure
  // over and over — see fireTriageRoutine.
  await pool.query(`ALTER TABLE purchasing_error_log
    ADD COLUMN IF NOT EXISTS severity text,
    ADD COLUMN IF NOT EXISTS handled_at timestamptz,
    ADD COLUMN IF NOT EXISTS handled_by text,
    ADD COLUMN IF NOT EXISTS handled_note text,
    ADD COLUMN IF NOT EXISTS triage_sig text,
    ADD COLUMN IF NOT EXISTS triage_fired_at timestamptz,
    ADD COLUMN IF NOT EXISTS triage_claimed_at timestamptz,
    ADD COLUMN IF NOT EXISTS triage_claimed_by text`);
  await pool.query(`CREATE INDEX IF NOT EXISTS purchasing_error_log_triage_idx
    ON purchasing_error_log(triage_sig, triage_fired_at)`);
}

// ── ONE SESSION PER FAILURE ──────────────────────────────────────────────────────────────────
// handled_at says a failure is FINISHED. Nothing said a failure was BEING WORKED, so between a
// session starting and it marking the row handled — twenty minutes, on a good day — every other
// session that woke saw the same unhandled row and started on it too. On 2026-09-18 three sessions
// worked the same Carhartt preflight failure (rows 411/413/416: the 12:39 run and two retry-sweep
// re-fires) at once. The cooldown in triageAlreadyChasing only stops US firing the routine again;
// it cannot stop a session the routine started on its own schedule, or one already running.
//
// A claim is per SIGNATURE, not per row: the three rows above are one failure, and a session that
// claims any of them takes all of them. It expires after TRIAGE_CLAIM_MINUTES so a session that
// died mid-way does not lock the failure forever, and it never blocks a human — the hub does not
// claim, it reads. Atomic: the UPDATE's WHERE is the check, so two sessions claiming in the same
// second cannot both win.
const TRIAGE_CLAIM_MINUTES = 90;
export async function claimTriageRows(pool, { id, by = 'triage-routine' } = {}) {
  await ensureErrorTable(pool);
  const cur = await pool.query('SELECT id, supplier, step, message, context, severity, handled_at, triage_sig, triage_claimed_at, triage_claimed_by FROM purchasing_error_log WHERE id = $1', [id]);
  const row = cur.rows[0];
  if (!row) return { ok: false, status: 404, error: `no error row ${id}` };
  if (row.handled_at) return { ok: false, status: 409, error: 'already handled', handledAt: row.handled_at };
  const { extractBlockedLines } = await import('./blockedLines.js');
  const sigOf = (r) => r.triage_sig || triageSignature(r, extractBlockedLines(r));
  const sig = sigOf(row);
  // Every unhandled row of the SAME failure in the last 36h (same supplier + step, then the full
  // signature computed per row — a stored sig only exists on rows the routine was fired for).
  const cands = await pool.query(
    `SELECT id, supplier, step, message, context, triage_sig, triage_claimed_at, triage_claimed_by FROM purchasing_error_log
      WHERE handled_at IS NULL AND created_at > now() - interval '36 hours' AND upper(supplier) = upper($1) AND step = $2`, [row.supplier, row.step]);
  const same = cands.rows.filter((r) => r.id === id || sigOf(r) === sig);
  // Is someone else on it right now? (Re-claiming under the same `by` is fine — a session re-reading its own claim.)
  const live = same.filter((r) => r.triage_claimed_at && r.triage_claimed_by !== by
    && (Date.now() - new Date(r.triage_claimed_at).getTime()) < TRIAGE_CLAIM_MINUTES * 60000)
    .sort((a, b) => new Date(b.triage_claimed_at) - new Date(a.triage_claimed_at));
  if (live.length) {
    const h = live[0];
    return { ok: false, status: 409, error: 'another session is working this failure', claimedBy: h.triage_claimed_by, claimedAt: h.triage_claimed_at,
      minutesAgo: Math.round((Date.now() - new Date(h.triage_claimed_at).getTime()) / 60000), expiresAfterMinutes: TRIAGE_CLAIM_MINUTES, sig, rows: same.map((r) => r.id) };
  }
  const ids = same.map((r) => r.id);
  // The WHERE repeats the check so two sessions claiming in the same second cannot both win.
  const upd = await pool.query(
    `UPDATE purchasing_error_log SET triage_claimed_at = now(), triage_claimed_by = $2, triage_sig = COALESCE(triage_sig, $3)
      WHERE id = ANY($1::int[]) AND handled_at IS NULL
        AND (triage_claimed_at IS NULL OR triage_claimed_at < now() - ($4 || ' minutes')::interval OR triage_claimed_by = $2)
      RETURNING id`, [ids, by, sig, String(TRIAGE_CLAIM_MINUTES)]);
  const won = upd.rows.map((r) => r.id);
  if (!won.includes(id)) return { ok: false, status: 409, error: 'lost the race — another session claimed it first', sig, rows: ids };
  return { ok: true, claimed: won, by, sig, expiresAfterMinutes: TRIAGE_CLAIM_MINUTES };
}

// What makes two failures "the same failure" for the purpose of not re-triaging one. The supplier
// and step, plus the ITEMS involved — a Chadwick checkout that refuses TB150922148 today is the
// same problem as the one that refused it an hour ago, and a different problem from one refusing a
// different code. Falls back to the message with every number masked, so a PO id or a quantity
// changing does not read as a new failure.
export function triageSignature({ supplier, step, message, context }, lines) {
  const skus = (lines || []).map((l) => String(l.sku || '').toUpperCase()).filter(Boolean).sort();
  const tail = skus.length ? skus.join(',') : String(message || '').replace(/\d+/g, '#').slice(0, 300);
  return `${String(supplier || '').toUpperCase()}|${step || ''}|${tail}`;
}

// Persist an error AND email an alert. Used for every failure in the flow.
// `placed` = did stock actually go to the supplier on this run? Until 2026-08-24 that was INFERRED
// from severity ('review' meant "the order went through"), which held for every review site except
// tagged-but-nothing-to-order — that one fires straight after the demand is valued, BEFORE the run
// has even decided whether to place. So a PenCarrie run that placed nothing still emailed "The
// order was placed", and force-run-safety refused a legitimate re-run on the same false reading.
// Left as null where the caller doesn't say, so every existing site keeps its current wording.
// notify:false RECORDS without emailing. For an outcome that is expected, already handled, and
// happens most days — Blaklader accepting an order and never answering — an alert every morning
// trains everyone to ignore Blaklader alerts, which is worse than not sending one. The row is still
// written, so the hub shows it, force-run-safety reads it and the history is intact; only the email
// is withheld. Never use it for something nobody has looked at.
export async function logPurchasingError(pool, { supplier = 'FRISTADS', step = 'unknown', message = '', context = null, severity = 'error', placed = null, notify = true } = {}) {
  let errorId = null;
  // Recorded IN the context so the stored row carries the fact too — an email is read once, but
  // force-run-safety and the triage routine read the row for the rest of the day.
  const ctx = placed === null ? context : { ...(context || {}), placed };
  try { if (pool) { await ensureErrorTable(pool); const r = await pool.query(`INSERT INTO purchasing_error_log (supplier, step, message, context, severity) VALUES ($1,$2,$3,$4,$5) RETURNING id`, [supplier, step, message, ctx ? JSON.stringify(ctx) : null, severity]); errorId = r.rows[0] && r.rows[0].id; } } catch (e) { console.error('[purchasing-error-log] insert failed:', e.message); }
  if (notify) { try { await sendAlertEmail({ supplier, step, message, context: ctx, severity, placed }); } catch (e) { console.error('[purchasing-error-log] email failed:', e.message); } }
  // ── and the person who raised the order, for EVERY supplier ────────────────────────────────
  // Wired here rather than at each drop site so no supplier can be forgotten — including ones
  // added later. extractBlockedLines already knows how to read every supplier's failure shape
  // (that is what the Stuck items tab runs on) and returns NOTHING for a failure that names no
  // line, so a login timeout or a bad request date reaches nobody's inbox.
  //
  // A line the supplier could not supply is a sales problem wherever it happened; a line WE could
  // not resolve is purchasing's problem to fix and is deliberately not routed to a salesperson.
  if (notify && pool && severity === 'error') {
    try {
      const c = ctx || {};
      const lines = (Array.isArray(c.dropped) && c.dropped.length)
        ? c.dropped                                            // carries productId — the exact match
        : (await import('./blockedLines.js')).extractBlockedLines({ supplier, step, message, context: c });
      if (lines && lines.length) {
        await notifyDroppedLines(pool, {
          supplier, poId: c.poId || null, dropped: lines, linesByOrder: c.linesByOrder || {},
          placed: /-dropped$/.test(String(step)) || placed === true,
          backorderPoId: c.backorderPoId || null,
          execute: !c.dryRun,
        });
      }
    } catch (e) { console.error('[dropped-line-notice] failed:', e.message); }
  }
  try { await fireTriageRoutine({ pool, supplier, step, message, context, severity, errorId }); } catch (e) { console.error('[purchasing-error-log] triage fire failed:', e.message); }
}

// Push a failure at the triage routine the moment it happens, instead of a routine waking on a
// timer and asking whether anything broke. Two reasons that matters: a scheduled run can only fire
// hourly (the platform minimum), so a 09:30 failure could sit for most of an hour before anything
// looks at it; and scheduled runs burn the daily routine allowance even when nothing is wrong.
//
// ONLY severity 'error' fires. That is the case where the run STOPPED and nothing was ordered —
// the one worth interrupting for. 'review' means the order already went through (a data problem to
// correct later) and 'info' is a notification, so waking a session for either would spend a run to
// do nothing, and risk it acting where it should not.
//
// Fire-and-forget by design: purchasing must never fail because a notification failed. Unset env
// vars make it a silent no-op, so nothing breaks before the routine exists.
// ── DO NOT KEEP SENDING A BOT AT SOMETHING IT CANNOT FIX ────────────────────────────────────
// Every severity 'error' row fired a routine, with no memory of what it had already been sent at.
// A supplier retries three times a day and the same demand is re-gathered tomorrow, so ONE bad
// item code spawned a triage session on every attempt, indefinitely. Chadwick's TB150922148 /
// ML110722012 refusal on 2026-09-15 is the case that surfaced it: the codes are wrong in
// Brightpearl, no amount of re-reading the portal changes that, and each pass burned a session to
// reach the same conclusion.
//
// Fire once per distinct failure, then leave it alone:
//   - nothing within COOLDOWN hours for the same signature, which covers the same-day retries;
//   - and at most MAX_FIRES attempts at a signature that has NEVER been resolved, after which it
//     is a human's problem and the Stuck items tab is where it waits.
// A signature that WAS handled and then comes back is new information, so its count starts again.
const TRIAGE_COOLDOWN_HOURS = 20;
const TRIAGE_MAX_FIRES = 3;

async function triageAlreadyChasing(pool, sig) {
  if (!pool || !sig) return null;
  const r = await pool.query(
    `SELECT count(*)::int AS fires,
            max(triage_fired_at) AS last_fired,
            bool_or(handled_at IS NOT NULL) AS ever_handled
       FROM purchasing_error_log
      WHERE triage_sig = $1 AND triage_fired_at IS NOT NULL
        AND triage_fired_at > now() - interval '7 days'`, [sig]);
  const row = r.rows[0] || {};
  if (!row.fires) return null;
  if (row.ever_handled) return null;                         // it was fixed before; this is new
  const hrs = (Date.now() - new Date(row.last_fired).getTime()) / 3600000;
  if (hrs < TRIAGE_COOLDOWN_HOURS) return `already fired ${hrs.toFixed(1)}h ago (cooldown ${TRIAGE_COOLDOWN_HOURS}h)`;
  if (row.fires >= TRIAGE_MAX_FIRES) return `fired ${row.fires}× already and never resolved — leaving it for a human`;
  return null;
}

async function fireTriageRoutine({ pool, supplier, step, message, context, severity, errorId }) {
  const url = process.env.TRIAGE_ROUTINE_URL, token = process.env.TRIAGE_ROUTINE_TOKEN;
  if (!url || !token) return;                 // not configured yet
  if (severity !== 'error') return;
  // A line already moved to a back-order PO is an expected outcome with a notice on its way to
  // sales, not a failure for a bot to chase — the row is severity error only so the notice fires
  // and the hub shows it. Everything else on 'error' still escalates.
  if (context && context.backorderPoId && /-dropped$/.test(String(step))) return;

  let sig = null;
  try {
    const { extractBlockedLines } = await import('./blockedLines.js');
    sig = triageSignature({ supplier, step, message, context },
      extractBlockedLines({ supplier, step, message, context }));
    const skip = await triageAlreadyChasing(pool, sig);
    if (skip) { console.log(`[purchasing-error-log] triage NOT fired for ${supplier}/${step}: ${skip}`); return; }
  } catch (e) {
    // Never let the guard itself stop a genuine escalation — fire, and accept a duplicate.
    console.error('[purchasing-error-log] triage dedupe failed:', e.message);
  }
  // The routine receives this wrapped as UNTRUSTED data, so its own prompt has to opt into acting
  // on it. Give it the error id first: everything else it needs is already in the log behind that
  // id, and the id is the thing it marks handled.
  const text = [
    `Purchasing failure logged as error id ${errorId == null ? '(unknown)' : errorId}.`,
    `Supplier: ${supplier}`,
    `Step: ${step}`,
    `Severity: ${severity} (the run stopped — nothing was ordered)`,
    `Message: ${message}`,
    context ? `Context: ${JSON.stringify(context).slice(0, 4000)}` : null,
  ].filter(Boolean).join('\n');
  const res = await fetch(url, {
    method: 'POST',
    headers: {
      Authorization: `Bearer ${token}`,
      'anthropic-beta': 'experimental-cc-routine-2026-04-01',
      'anthropic-version': '2023-06-01',
      'Content-Type': 'application/json',
    },
    body: JSON.stringify({ text }),
    signal: AbortSignal.timeout(15000),
  });
  const body = await res.text();
  if (!res.ok) throw new Error(`routine fire HTTP ${res.status}: ${body.slice(0, 200)}`);
  // Stamp the row AFTER a successful fire — a firing that failed should be retried, not counted.
  if (pool && errorId != null) {
    await pool.query(`UPDATE purchasing_error_log SET triage_sig = $2, triage_fired_at = now() WHERE id = $1`,
      [errorId, sig]).catch((e) => console.error('[purchasing-error-log] triage stamp failed:', e.message));
  }
  console.log(`[purchasing-error-log] triage routine fired for ${supplier}/${step}:`, body.slice(0, 200));
}

function transporter() {
  return nodemailer.createTransport({ host: process.env.SMTP_SERVER || 'mail-eu.smtp2go.com', port: parseInt(process.env.SMTP_PORT || '2525'), secure: false, auth: { user: process.env.SMTP_USERNAME || 'tuffshop.co.uk', pass: process.env.SMTP_PASS } });
}

// severity 'error'  = the run stopped, nothing further was placed.
// severity 'review'  = the ORDER WENT THROUGH; this is a data issue to fix afterwards (a price
//                      check, a heal outside the auto-apply band, a PO that could not be re-priced).
// They used to render identically — "⚠ … auto-purchase FAILED … Nothing further was placed on this
// run" — so nine Helly Hansen price-heal escalations on 2026-08-19 read as nine failed runs when the
// order had in fact been placed (HH 569124, £661). Never let a non-fatal alert claim a failure.
async function sendAlertEmail({ supplier, step, message, context, severity = 'error', placed = null }) {
  if (!process.env.SMTP_PASS) return;
  const when = new Date().toLocaleString('en-GB', { timeZone: 'Europe/London' });
  const review = severity === 'review';
  // 'info' is a NOTIFICATION, not a problem — currently "we changed a cost price in Brightpearl".
  // Without its own rendering it inherited the error styling and went out as "auto-purchase
  // failed", which is how nine price-heal escalations once read as nine failed runs.
  const info = severity === 'info';
  const colour = info ? '#2e7d32' : review ? '#e65100' : '#c62828';
  const mark = info || review ? 'ⓘ' : '⚠';
  const heading = info ? `${supplier} — Brightpearl cost prices updated`
    : review ? `${supplier} auto-purchase — NEEDS REVIEW`
    : `${supplier} auto-purchase failed`;
  const footer = info
    ? `<p><strong>Nothing is wrong and nothing needs doing.</strong> The supplier charged a different price to the one held in Brightpearl, so the cost (Launch/list 20) was corrected automatically and the PO re-priced to match. Told to you because every automatic cost change is worth seeing.</p>`
    : review && placed === false
    ? `<p><strong>Nothing was placed for these orders.</strong> This is flagged for a human to check in Brightpearl. Whether the run placed anything ELSE is a separate question — read the run's own alerts.</p>`
    : review
    ? `<p><strong>The order was placed.</strong> This is flagged for a human to correct in Brightpearl — nothing is blocked and the next scheduled run is unaffected.</p>`
    : `<p>Nothing further was placed on this run. Check Brightpearl + the ${supplier} portal, then it will retry on the next scheduled run.</p>`;
  const html = `<p style="color:${colour}"><strong>${mark} ${heading}</strong> — ${when}</p>
    <ul>
      <li><strong>Step:</strong> ${step}</li>
      <li><strong>${info ? 'Change' : review ? 'Detail' : 'Problem'}:</strong> ${escapeHtml(message)}</li>
    </ul>
    ${context ? `<pre style="background:#f5f5f5;padding:8px;border-radius:4px;white-space:pre-wrap">${escapeHtml(JSON.stringify(context, null, 2))}</pre>` : ''}
    ${footer}`;
  await transporter().sendMail({
    from: '"Tuff Purchasing" <noreply@tuffshop.co.uk>', to: NOTIFY_TO,
    subject: info ? `ⓘ ${supplier} — cost price updated in Brightpearl` : `${mark} ${supplier} auto-purchase ${review ? 'review' : 'error'} — ${step}`,
    html,
    text: `${supplier} ${info ? 'COST PRICE UPDATED (no action needed)' : review ? `auto-purchase REVIEW (${placed === false ? 'nothing placed for these orders' : 'order was placed'})` : 'auto-purchase ERROR (run stopped)'} at step "${step}": ${message}\n\n${context ? JSON.stringify(context, null, 2) : ''}`,
  });
}
const escapeHtml = (s) => String(s).replace(/[&<>]/g, (c) => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;' }[c]));

// The CLOSING half of the pair. sendAlertEmail says something broke; this says what was done about
// it, and until now nothing did — a failure arrived by email and the resolution only existed in the
// error log, where nobody looks unless they already know to. Sent whether the triage routine or a
// person marked the row handled, so a quiet inbox genuinely means nothing is outstanding.
//
// The note is the whole point: the runbook already requires it to be written FOR A PERSON ("what
// broke, what you changed, whether it is live, whether it got ordered"), so it is reproduced verbatim
// rather than summarised — anything that rewrote it here would only lose detail.
//
// DELIBERATELY MAKES NO CLAIM ABOUT THE ORDER. Only the note knows whether stock was actually
// placed, and a heading that guessed would repeat the 2026-08-19 mistake in the other direction:
// nine escalations read as nine failed runs because the rendering asserted something the data did
// not say. "Resolved" here means the row was closed, nothing more.
export async function sendResolutionEmail({ id, supplier, step, severity = 'error', by = 'unknown', note = '', message = '', loggedAt = null, handledAt = null } = {}) {
  if (!process.env.SMTP_PASS) return;
  const fmt = (d) => (d ? new Date(d).toLocaleString('en-GB', { timeZone: 'Europe/London' }) : null);
  const when = fmt(handledAt) || new Date().toLocaleString('en-GB', { timeZone: 'Europe/London' });
  // How long it sat. The reason the push trigger exists is that a scheduled routine could leave a
  // 09:30 failure most of an hour, so the number that proves the push is working belongs in the mail.
  let took = null;
  if (loggedAt) {
    const mins = Math.round((new Date(handledAt || Date.now()) - new Date(loggedAt)) / 60000);
    if (Number.isFinite(mins) && mins >= 0) took = mins < 60 ? `${mins} min` : `${Math.floor(mins / 60)}h ${mins % 60}m`;
  }
  const review = severity === 'review';
  const html = `<p style="color:#2e7d32"><strong>✓ ${supplier} auto-purchase — resolved</strong> — ${when}</p>
    <ul>
      <li><strong>Error:</strong> #${id} at step "${step}"${review ? ' (review — the order had gone through)' : ''}</li>
      <li><strong>Handled by:</strong> ${escapeHtml(by)}</li>
      ${took ? `<li><strong>Open for:</strong> ${took}</li>` : ''}
    </ul>
    <p><strong>What was done</strong></p>
    <pre style="background:#f5f5f5;padding:8px;border-radius:4px;white-space:pre-wrap">${escapeHtml(note)}</pre>
    <p style="color:#666"><strong>Originally reported</strong></p>
    <pre style="background:#fafafa;padding:8px;border-radius:4px;white-space:pre-wrap;color:#666">${escapeHtml(String(message).slice(0, 1500))}</pre>
    <p style="color:#666">Read the note before assuming the stock was ordered — closing the row and placing the order are not the same thing, and some fixes deliberately leave the order for the next scheduled run.</p>`;
  await transporter().sendMail({
    from: '"Tuff Purchasing" <noreply@tuffshop.co.uk>', to: NOTIFY_TO,
    subject: `✓ ${supplier} auto-purchase resolved — ${step} (error #${id})`,
    html,
    text: `${supplier} auto-purchase RESOLVED — error #${id} at step "${step}", handled by ${by}${took ? ` after ${took}` : ''}.\n\nWHAT WAS DONE:\n${note}\n\nORIGINALLY REPORTED:\n${message}\n\nRead the note before assuming the stock was ordered.`,
  });
}

// ── the full placement chain ─────────────────────────────────────────────────
// fetch that turns network failures ("website down") + non-2xx into step-tagged errors.
// Supplier calls are SLOW. PenCarrie's order build alone measured 170s for PO 486420's 45 lines
// before the gateway submit, because resolving a line can pull the 217MB product feed.
//
// Node's built-in fetch enforces undici's own 300s headersTimeout, and an AbortSignal does NOT
// raise it — so a long order died at five minutes no matter what timeout we asked for. Worse, the
// catch below reported that as "can't reach (website/network down?)", which is a LIE about a
// supplier that answered perfectly well: on 2026-09-02 PenCarrie was called four times, every call
// reported the site as down, Alt-Items was healthy throughout (44ms on resolve, gateway 200), and
// the order HAD been placed on one of them — found only because PenCarrie's own duplicate guard
// refused the fifth with "Order ID (TUWO_TW486420) already exists".
//
// A custom dispatcher is the only way to lift the ceiling. Guarded: if undici cannot be imported
// the call still works on the default stack — a missing module must never stop the service booting.
const SUPPLIER_HTTP_TIMEOUT_MS = Number(process.env.SUPPLIER_HTTP_TIMEOUT_MS || 900000); // 15 min
let _dispatcher; let _dispatcherTried = false;
async function longDispatcher() {
  if (_dispatcherTried) return _dispatcher;
  _dispatcherTried = true;
  try {
    const { Agent } = await import('undici');
    _dispatcher = new Agent({ headersTimeout: SUPPLIER_HTTP_TIMEOUT_MS, bodyTimeout: SUPPLIER_HTTP_TIMEOUT_MS });
  } catch { _dispatcher = null; }                    // not installed → default 300s, still honest below
  return _dispatcher;
}

async function jfetch(step, url, opts) {
  let res;
  const t0 = Date.now();
  const dispatcher = await longDispatcher();
  try { res = await fetch(url, dispatcher ? { ...opts, dispatcher } : opts); }
  catch (e) {
    const secs = Math.round((Date.now() - t0) / 1000);
    const msg = String((e && e.message) || e);
    const cause = String((e && e.cause && e.cause.code) || '');
    const timedOut = /timeout|aborted|UND_ERR_(HEADERS|BODY)_TIMEOUT/i.test(msg + ' ' + cause) || secs >= 280;
    // A timeout is NOT evidence the order failed. The request may have completed at the supplier
    // after we stopped listening. Saying "website down" here sends whoever reads it to check the
    // wrong thing and invites a retry that double-orders.
    if (timedOut) {
      throw stepErr(step, `NO ANSWER from ${url} after ${secs}s — this is NOT proof the order failed. `
        + `The request may have COMPLETED at the supplier after we stopped waiting. `
        + `CHECK THE SUPPLIER'S OWN ORDER LIST before retrying: a retry can order it twice. `
        + `(${msg}${cause ? ` / ${cause}` : ''})`);
    }
    throw stepErr(step, `can't reach ${url} after ${secs}s (website/network down?): ${msg}`);
  }
  const text = await res.text();
  let j = null; try { j = text ? JSON.parse(text) : null; } catch { /* non-json */ }
  if (!res.ok) throw stepErr(step, `HTTP ${res.status} from ${url}: ${(j ? JSON.stringify(j) : text).slice(0, 250)}`);
  // A supplier helper that answers {error} almost always answers a great deal MORE than that, and
  // all of it used to be dropped right here: this throw took the sentence and discarded the parsed
  // body, so the call sites' own carefully-built context — placeMascotOrder's `{ poId, basket:
  // r.basket }` — was unreachable, because jfetch threw before the caller ever saw `r`.
  // 2026-09-09: Mascot failed with "basket did not accept every line" and nothing else reached the
  // alert, the error log or the triage routine, which stopped for want of a reason. The reason was
  // in the reply all along — an SAP exception on MASCOT's side (CX_SY_MESSAGE_IN_PLUGIN_MODE,
  // "Message E WVA 032 cannot be processed in plugin mode") with every one of our lines accepted
  // and nothing unresolved. A supplier-side fault we spent an afternoon unable to see.
  if (j && j.error) {
    const { error, ...rest } = j;
    throw stepErr(step, `${url} returned error: ${error}`, Object.keys(rest).length ? { response: rest } : null);
  }
  return j;
}

// Feed the supplier's ACTUAL prices back onto the BP cost (see healSupplierCosts for the rules).
// Non-fatal. ON by default since 2026-08-19 (user); set PRICE_HEAL_ENABLED=false to put it back to
// it's switched on, and the run report still shows what it would have done. Anything outside the
// auto-heal band is raised as a price-heal error for a human rather than written.
// silent: write the rows (the hub, the cost-decision list and force-run-safety all read them) but
// send no email — the caller is folding the outcome into ONE price notice for the order instead of
// the three-email pattern (price-check review, then "cost updated", then one review per escalation)
// that made every order look like a problem when most had already been corrected. Returns the heal
// result so the caller can say what was fixed and what still needs a person.
async function healPrices(steps, { supplierKey, poId, changes, pool, silent = false }) {
  if (!changes || !changes.length) return null;
  try {
    const r = await bp.healSupplierCosts({ supplierKey, poId, changes, pool, execute: process.env.PRICE_HEAL_ENABLED !== 'false' });
    if (r.skipped && !Array.isArray(r.skipped)) { steps.priceHeal = r; return r; }        // e.g. "no cost list of its own"
    steps.priceHeal = { listId: r.listId, dryRun: !!r.dryRun, applied: (r.applied || []).length, escalated: (r.escalated || []).length, skipped: (r.skipped || []).length, changes: (r.applied || []).map((a) => `${a.sku} £${Number(a.was).toFixed(2)}->£${Number(a.now).toFixed(2)}`) };
    // Healing fixes the PRODUCT cost; the PO still carries the cost it snapshotted when it was
    // created, so it stays wrong until re-priced — that needed doing by hand three times before this
    // (user, 2026-08-18). Re-price from the SUPPLIER'S OWN list, only when a heal actually applied.
    // repriceComboPOLive refuses if the PO holds a productId-1000 row that is not the =====LOW INV====
    // separator, so a Shipping or proof-instruction row can never be renamed and zeroed by this.
    // Every cost written to Brightpearl gets an alert. Applied heals used to appear ONLY in the run
    // report — the escalated ones emailed, the ones actually CHANGED did not — so the automatic
    // edits to live cost data were the invisible ones. severity 'info' renders as a notification,
    // not a failure, and is kept out of the triage work queue.
    if (r.applied && r.applied.length && !r.dryRun) {
      const list = r.applied.map((a) => `${a.sku} £${Number(a.was).toFixed(2)} → £${Number(a.now).toFixed(2)}`).join('; ');
      await logPurchasingError(pool, {
        supplier: supplierKey, step: 'price-heal-applied', severity: 'info', notify: !silent,
        message: `${r.applied.length} cost price(s) corrected on list ${r.listId} from what ${supplierKey} actually charged: ${list}`,
        context: { poId, listId: r.listId, applied: r.applied },
      }).catch(() => {});
    }
    if (r.applied && r.applied.length && process.env.PRICE_HEAL_ENABLED !== 'false') {
      try {
        const rp = await bp.repriceComboPOLive({ poId, priceListId: r.listId, execute: true });
        steps.priceHealReprice = rp.refused
          ? { refused: true, reason: rp.reason, miscRows: rp.miscRows }
          : { done: !!rp.done, priceListId: r.listId, rows: (rp.plan || []).length };
        if (rp.refused) {
          await logPurchasingError(pool, {
            supplier: supplierKey, step: 'price-heal-reprice', severity: 'review',
            message: `Costs were healed but PO#${poId} was NOT re-priced: ${rp.reason}. The PO still shows the old cost — re-price it by hand after checking those rows.`,
            context: { poId, listId: r.listId, miscRows: rp.miscRows },
          }).catch(() => {});
        }
      } catch (e) { steps.priceHealRepriceWarn = e.message; }
    }
    for (const e of (r.escalated || [])) {
      await logPurchasingError(pool, {
        supplier: supplierKey, step: 'price-heal', severity: 'review', notify: !silent,
        message: `${e.sku}: supplier charges £${Number(e.now).toFixed(2)} but BP cost (list ${r.listId}) is £${Number(e.was).toFixed(2)} — ${e.reason}`,
        context: { poId, ...e },
      }).catch(() => {});
    }
    return r;
  } catch (e) { steps.priceHealWarn = e.message; return null; }
}

// ONE price notice per order. Heals first (silently), then logs a single 'price-check' row whose
// severity says whether anyone needs to act: 'info' when every named difference was corrected on
// the product's cost in Brightpearl and the PO re-priced — nothing to do — and 'review' only when
// something was NOT corrected (outside the ±20%/£10 band, a SKU that could not be pinned to one
// product, or a difference the supplier's data could not name). Until 2026-09-18 the review row
// went out BEFORE the heal ran, so every order emailed "prices don't match" about lines that were
// fixed thirty seconds later, and the fixes went out as a second email — Dec was re-keying costs
// by hand that were already right. The message keeps the "SKU ours £X vs theirs £Y" form: the
// cost-decision list in server.js parses it.
const fx = (n) => Number(n).toFixed(2);
async function logPriceCheck(pool, steps, { supplierKey, poId, message, context = {}, changes = [], ambiguous = [] }) {
  const r = await healPrices(steps, { supplierKey, poId, pool, changes, silent: true });
  const applied = (r && Array.isArray(r.applied)) ? r.applied : [];
  const escalated = (r && Array.isArray(r.escalated)) ? r.escalated : [];
  const notApplied = (r && Array.isArray(r.skipped)) ? r.skipped.filter((x) => x.reason !== 'already correct') : [];
  const healerOff = !r || r.dryRun || (r.skipped && !Array.isArray(r.skipped));
  const outstanding = [
    ...escalated.map((e) => `${e.sku} ours £${fx(e.was)} vs theirs £${fx(e.now)} — ${e.reason}`),
    ...notApplied.map((x) => `${x.sku} — ${x.reason}`),
    ...ambiguous.map((x) => `article ${x.article} ours £${fx(x.ourUnit)} vs theirs £${fx(x.theirUnit)} (spans ${(x.skus || []).join(', ')} — size not pinned)`),
  ];
  // Nothing named at all (a total-only gap) or the healer could not run → a person has to look.
  const allFixed = changes.length > 0 && !healerOff && !outstanding.length;
  const severity = allFixed ? 'info' : 'review';
  const tail = (applied.length ? `\nCorrected automatically on list ${r.listId} (product cost + PO re-priced): ${applied.map((a) => `${a.sku} £${fx(a.was)} → £${fx(a.now)}`).join('; ')}.` : '')
    + (outstanding.length ? `\nSTILL NEEDS A HUMAN: ${outstanding.join('; ')}.` : '')
    + (changes.length && healerOff ? '\nNot corrected automatically (price heal off or unavailable) — check these by hand.' : '');
  await logPurchasingError(pool, {
    supplier: supplierKey, step: 'price-check', severity, placed: true,
    message: message + tail,
    context: { ...context, healed: applied.length, outstanding: outstanding.length },
  }).catch(() => {});
  return r;
}

// Ask the Fristads portal what it can actually supply for ONE line. Returns null when the answer
// can't be trusted (probe failed, article not found, no numeric availability) — the caller then
// leaves the line IN, because failing loudly at the basket is better than dropping a customer's
// line on a network blip.
// Split the cart lines into what Fristads can supply and what they cannot. FAILS OPEN by design:
// anything the probe could not answer for (null) stays in `orderable`, because a dropped customer
// line is far worse than the loud basket failure we already had. Pure, so the rule is pinned by a
// test rather than only exercised on a live run.
export function partitionFristadsLines(cartLines, stockBySku) {
  const short = [], orderable = [];
  for (const l of cartLines || []) {
    const st = stockBySku && stockBySku.get(String(l.sku));
    const avail = st && Number.isFinite(Number(st.avail)) ? Number(st.avail) : null;
    if (avail !== null && avail < l.qty) short.push({ ...l, avail, deldate: (st && st.deldate) || null });
    else orderable.push(l);
  }
  return { short, orderable };
}

export async function fristadsAvailable(altItemsUrl, line) {
  const url = `${altItemsUrl}/api/fristads-stock?name=${encodeURIComponent(line.name || '')}`
    + `&sku=${encodeURIComponent(line.sku || '')}&size=${encodeURIComponent(line.size || '')}`;
  try {
    const j = await (await fetch(url, { signal: AbortSignal.timeout(30000) })).json();
    if (!j || j.found !== true || j.avail == null || !Number.isFinite(Number(j.avail))) return null;
    return { avail: Number(j.avail), deldate: j.deldate || null };
  } catch (e) {
    // Same trap as snickersLineStatus: a silent null here would disable the guard with no word
    // said, and nobody would know the check had stopped working.
    console.log(`[fristads-stock] ${line.sku}: ${e.message}`);
    return null;
  }
}

async function placeFristadsOrder(pool, altItemsUrl, { padToThreshold = 0 } = {}) {
  const steps = {};
  // 1. create the combined PO (SO + low-inv + separator + notes; stamps the SOs)
  let po;
  try { po = await createPo({ supplierKey: 'FRISTADS', execute: true, padToThreshold, logPool: pool }); }
  catch (e) { throw createPoErr(e); }
  if (!po.created) throw stepErr('create-po', `no PO created: ${po.reason || 'unknown'}` + (po.unresolvedSkus && po.unresolvedSkus.length ? ` — item codes not found in Brightpearl: ${po.unresolvedSkus.join(', ')}` : ''));
  const poId = po.poId;
  let soIds = [...new Set((po.soLines || []).map((l) => l.order).filter(Boolean))];
  // item names ordered per SO — for the SO note. productId is carried because it is the ONLY key
  // that survives to the cart: soLines hold the Brightpearl SO row SKU (CB170321004) while the PO
  // row, and so the cart line, holds the resolved Fristads code (125949-171-406). Matching these
  // two by SKU silently matches nothing.
  const linesByOrder = {};
  for (const l of (po.soLines || [])) { if (l.order) (linesByOrder[l.order] = linesByOrder[l.order] || []).push({ sku: l.sku, qty: l.qty, name: l.name, productId: l.productId }); }
  steps.po = { poId, soUnits: po.soUnits, lowUnits: po.lowUnits, soIds, skippedBundles: po.skippedBundles || [] };

  // 2. push the PO lines to the Fristads cart (unresolved = size/item not on the portal)
  const cartLines = await bp.getOrderCartLines(poId).catch((e) => { throw stepErr('cart', `couldn't read PO ${poId} rows: ${e.message}`); });

  // OUT-OF-STOCK PRE-FLIGHT. fristadsAddToBasket groups the lines by article+colour and sends ONE
  // order-form POST per group, every size sharing the body. So Fristads refusing a single size
  // rejects the whole POST and takes every OTHER size of that garment down with it — the basket is
  // left short and nothing is placed at all.
  //
  // PO 488528 was refused exactly that way, twice, on 2026-09-11: the Medium of a FLAME hi-vis
  // coverall was on zero, and it lost the Large and XL — which had 123 and 67 on the shelf — along
  // with it. 3 of 6 units landed, the run threw, and four lines nobody had any trouble supplying
  // went unordered. So ask first, and hold back only the sizes they cannot supply.
  // Probed a few at a time, not one after another. Each probe scrapes a portal page, so a 50-line
  // order would sit through 50 round trips holding the shared run lock — and a run that overruns
  // its window swallows the NEXT supplier's slot (Snickers lost its 10:00 on 2 Sept exactly that
  // way). Six at a time is well inside what the portal tolerates and turns minutes into seconds.
  const stockBySku = new Map();
  const FRISTADS_STOCK_CONCURRENCY = 6;
  for (let i = 0; i < cartLines.length; i += FRISTADS_STOCK_CONCURRENCY) {
    const batch = cartLines.slice(i, i + FRISTADS_STOCK_CONCURRENCY);
    const got = await Promise.all(batch.map((l) => fristadsAvailable(altItemsUrl, l)));
    batch.forEach((l, n) => { if (got[n]) stockBySku.set(String(l.sku), got[n]); });
  }
  const { short: shortLines, orderable } = partitionFristadsLines(cartLines, stockBySku);
  steps.stockCheck = { checked: cartLines.length, probed: stockBySku.size, short: shortLines };
  // Keyed on productId, not SKU: po.soLines carry the Brightpearl SO row code (CB170321004) while
  // the cart line carries the resolved Fristads one (125949-171-406), so a SKU comparison between
  // them matches nothing. Used by BOTH the note/finalise trim below and the price check at the end,
  // which otherwise values demand we deliberately did not buy.
  const droppedPids = new Set(shortLines.map((s) => String(s.productId)));
  if (shortLines.length) {
    if (!orderable.length) {
      throw stepErr('cart', `every line on PO#${poId} is out of stock at Fristads — nothing to order: `
        + shortLines.map((s) => `${s.sku} (${s.size || '?'}) want ${s.qty}, they have ${s.avail}`).join('; '), { poId, shortLines });
    }
    // Take them OFF the PO. A row left on a Placed PO reads as ordered and counts as on-order, which
    // is how the 4004 on PO 486597 and the HH row on 485410 both came to be "on the PO" and never
    // arriving. Best-effort per row: a removal that fails is reported, not fatal — the log row below
    // is what actually gets the line bought.
    const removedRows = [];
    for (const s of shortLines) {
      try {
        const r = await bp.removePoRowLive({ poId, sku: s.sku, execute: true });
        // `done`, not `removed`/`ok` — removePoRowLive returns { done: !still } after reading the
        // PO back. All three call sites read fields it has never returned, so a row that HAD come
        // off reported ok:false. Seen live on PO 488528 (2026-09-11): the row was gone and the log
        // said it was not. A false alarm about a dead line still sitting on a placed PO is the
        // exact thing this reporting exists to rule out.
        removedRows.push({ sku: s.sku, ok: !!(r && r.done) });
      } catch (e) { removedRows.push({ sku: s.sku, ok: false, error: e.message }); }
    }
    steps.stockCheck.removedRows = removedRows;
    // Neither the SO note nor the finalise may claim a line we did not order. Drop the dead lines
    // from the note, and drop from soIds any order left with NOTHING ordered — finalising that one
    // would clear its supplier tag and set it to "Ordered Stock Awaiting Delivery" for goods that
    // were never bought. Leaving the tag on also means the 17:30 tag audit keeps nagging about it.
    for (const id of Object.keys(linesByOrder)) {
      linesByOrder[id] = linesByOrder[id].filter((x) => !droppedPids.has(String(x.productId)));
      if (!linesByOrder[id].length) delete linesByOrder[id];
    }
    // The lines are NOT lost. Fristads DO take a back order: the portal's status 3 on a zero-stock
    // size is a phase-out question ("alternative, or the original?"), not a refusal, and
    // fristadsAddToBasket answers it with the original — see fristadsAnswerPhaseOut. They still
    // cannot share the POST with the in-stock sizes (the whole group comes back as a question), so
    // they go as a SEPARATE Fristads order once this one is through. Brightpearl-side they move to
    // a child PO at status 45 NOW, noted both ends, so the parent matches the goods that will
    // arrive first and the back order is visible even if the main placement below then fails.
    // This replaces the 2026-09-15 behaviour of dropping the line and telling someone to buy it
    // by hand — which is what PO 490183 did on 2026-09-18 with the Airtech Large for SO 490142.
    const bo = await bp.createBackorderPoLive({
      supplierKey: 'FRISTADS', parentPoId: poId, execute: true,
      lines: shortLines.map((s) => ({ productId: s.productId, sku: s.sku, name: s.name, qty: s.qty, deldate: s.deldate })),
      note: `Out of stock at Fristads on ${new Date().toISOString().slice(0, 10)}, split from PO#${poId} so the rest could go. Placed with Fristads as a separate back order — see the note below.`,
    }).catch((e) => ({ created: false, error: e.message }));
    steps.backorder = { ...bo, fristads: null };
    // An SO whose only line is on back order is still ORDERED — finalise it like the rest, with
    // the line named against the back-order PO in its note rather than dropped from it.
    if (bo.created) {
      for (const id of Object.keys(linesByOrder)) {
        for (const x of linesByOrder[id]) if (droppedPids.has(String(x.productId))) x.name = `${x.name || x.sku} (ON BACK ORDER — PO#${bo.poId})`;
      }
      // droppedPids is still used for the PRICE CHECK below, which must value the main order only.
    } else {
      for (const id of Object.keys(linesByOrder)) {
        linesByOrder[id] = linesByOrder[id].filter((x) => !droppedPids.has(String(x.productId)));
        if (!linesByOrder[id].length) delete linesByOrder[id];
      }
      const stranded = soIds.filter((id) => !linesByOrder[id]);
      soIds = soIds.filter((id) => !!linesByOrder[id]);
      steps.stockCheck.stranded = stranded;
    }
    // severity ERROR still: a customer is waiting and the line has left the order they will look
    // at. With backorderPoId set the notice goes out as "on back order, PO#…" rather than "not
    // ordered", and triage is not summoned for it — see fireTriageRoutine.
    await logPurchasingError(pool, {
      supplier: 'FRISTADS', step: 'out-of-stock-dropped', severity: 'error',
      message: `${shortLines.length} line(s) are out of stock at Fristads and were taken OFF PO#${poId} so the rest of the order could go through. `
        + (bo.created
          ? `They are on back-order PO#${bo.poId} and will be placed with Fristads as a separate back order once this order is through:\n`
          : `The back-order PO could NOT be created (${bo.error || bo.reason}) — these are NOT ordered and nothing will chase them automatically:\n`)
        + shortLines.map((s) => `      ${s.qty} × ${s.sku} (${s.size || '?'}) ${s.name || ''} — Fristads have ${s.avail}`
          + (s.deldate ? `, next delivery ${s.deldate}` : '')).join('\n'),
      context: { poId, dropped: shortLines, removedRows, backorderPoId: bo.created ? bo.poId : null },
    }).catch(() => {});
  }

  const expectUnits = orderable.reduce((a, l) => a + l.qty, 0);
  const cart = await jfetch('cart', `${altItemsUrl}/api/fristads-basket`, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ clearFirst: true, lines: orderable }) });
  // `results` carries the portal's OWN message for each article+colour group it refused. Dropping it
  // is why "portal shows 3, expected 6" was all anyone got from PO 488528 — the reason was in the
  // response the whole time, and which lines dropped had to be reconstructed by hand afterwards.
  const refused = (cart.results || []).filter((r) => !r.ok);
  steps.cart = { cartCount: cart.cartCount, expectUnits, unresolved: cart.unresolved, refused };
  if ((cart.unresolved || []).length) throw stepErr('cart', `size/item not found on the Fristads portal (codes don't match): ${JSON.stringify(cart.unresolved)}`, { poId, unresolved: cart.unresolved, refused });
  if (cart.cartCount !== expectUnits) throw stepErr('cart', `cart quantity mismatch: portal shows ${cart.cartCount}, expected ${expectUnits} — some lines didn't add`
    + (refused.length ? `. Fristads refused ${refused.length} group(s): ${refused.map((r) => `${r.key} — ${JSON.stringify(r.resp && r.resp.messages || r.reason || r.status)}`).join('; ').slice(0, 300)}` : ''),
    { poId, cartCount: cart.cartCount, expectUnits, refused, sent: orderable.map((l) => ({ sku: l.sku, size: l.size, qty: l.qty })) });

  // 3. checkout / placeorder (Mark of goods=WORKWEAR, order ref = our PO#)
  const co = await jfetch('checkout', `${altItemsUrl}/api/fristads-checkout`, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ goodsMark: 'WORKWEAR', orderRef: String(poId), execute: true }) });
  const reservationNo = co.reservationNo; // Fristads "Reservation No" (= basket id)
  steps.checkout = { allOk: co.allOk, status: co.status, reservationNo };
  if (!co.allOk) throw stepErr('checkout', `checkout fields didn't set/verify on the portal: ${JSON.stringify(co.verify || co)}`);

  // 3b. Placement confirmation, two-stage:
  // Full checkout diagnostic — recorded on ANY checkout failure (via the error log context) so we
  // can see WHY it didn't confirm (messageType, confirmation status, respHead) instead of guessing.
  const coDiag = { placed: co.placed, confirmed: co.confirmed, allOk: co.allOk, status: co.status, messageType: co.messageType, confStatus: co.confStatus, reservationNo, verify: co.verify, respHead: co.respHead };
  steps.checkout.diag = coDiag;
  // co.placed = the placeorder → payment → confirmation chain reached the ORDER CONFIRMATION
  // ("Thank you for your order"). placeorder alone does NOT commit (Aug-2026 portal change); the
  // Alt-Items checkout now follows the full chain, so this is the authoritative placement signal.
  // The basket clears + the order NUMBER indexes in the background over the next several minutes,
  // so we DON'T gate on those (they'd time out on a real placement).
  if (!co.placed) throw stepErr('checkout', `order not confirmed — the placeorder→payment→confirmation chain failed (status ${co.status}, messageType ${co.messageType}, confirmed ${co.confirmed})`, { checkout: coDiag });
  steps.checkout.placed = true;

  // 4. pull the Fristads order number by our PO ref (= the ExternalVerificationNo on the order).
  // Try for ~7 min but DO NOT fail the run if it hasn't indexed — placement is already confirmed
  // above, so throwing here would leave the order placed-but-unlinked and the SOs un-finalised
  // (re-order risk). If it's not visible yet, fall back to the reservation no + a note; the real
  // order number can be backfilled later (our PO# is on the Fristads order as the search key).
  let order = null;
  for (let i = 0; i < 21; i++) {
    order = await jfetch('order-pull', `${altItemsUrl}/api/fristads-order?ref=${encodeURIComponent(poId)}`).catch(() => null);
    if (order && order.found && order.orderNo) break;
    await new Promise((r) => setTimeout(r, 20000));
  }
  const orderNo = (order && order.found && order.orderNo) ? order.orderNo : null;
  steps.order = orderNo
    ? { orderNo, orderStatus: order.orderStatus, sum: order.sum }
    : { orderNo: null, pending: true, note: `order# not indexed within ~7min — reference set to reservation ${reservationNo}; backfill from our PO ref later` };

  // price sanity check (NON-FATAL): the Fristads order total (what they'll invoice,
  // ex-VAT) vs our PO net. A gap means a BP cost price is stale → alert so it can be
  // adjusted; the order still stands (Fristads charges their price regardless).
  // Value only what we actually BOUGHT. An out-of-stock line held back above is still in po.soLines
  // — it was demand, we just didn't order it — and counting it makes the comparison nonsense: on PO
  // 488528 this reported "Fristads £320.75 vs our PO net £444.00, diff £-123.25" when £124.95 of
  // that was simply the coverall we deliberately left off. A check that cries wolf on our own drop
  // is worse than none, because the real drift (£1.70 of stale costs) is invisible underneath it.
  const poNet = [...(po.soLines || []), ...(po.lowLines || [])]
    .filter((l) => !droppedPids.has(String(l.productId)))
    .reduce((a, l) => a + (l.cost || 0) * l.qty, 0);
  const fristadsTotal = parseFloat(String((order && order.sum) || '').replace(/[^\d.]/g, '')) || 0; // 0 if order# not indexed yet (skips the check)
  const priceGap = fristadsTotal ? +(fristadsTotal - poNet).toFixed(2) : 0;
  steps.priceCheck = { fristadsTotal, poNet: +poNet.toFixed(2), gap: priceGap };
  if (fristadsTotal && Math.abs(priceGap) > 0.50) {
    // A total-only gap can't say WHICH line is wrong — the 302119-940-47 insole (their Rec. Price
    // £8.50 sitting in our cost column against a true trade cost of £4.25) had to be found by hand.
    // So pull the placed order's own line prices and name the offender.
    //
    // Matching is by 6-digit ARTICLE, not by SKU, because the two systems don't agree on a SKU:
    // their line reads "100222-900" + a display size of "L" where ours is 100222-910-407 — a
    // different colour code AND a numeric size code. The article is the only key both sides share.
    // Non-fatal throughout: the order is placed and Fristads charge their price regardless.
    const ourLines = [...(po.soLines || []), ...(po.lowLines || [])].filter((l) => !droppedPids.has(String(l.productId)));
    const breakdown = ourLines.map((l) => `${l.qty} × ${l.sku} — our £${(l.cost || 0).toFixed(2)}/ea (${l.name})`);
    const artOf = (sku) => (String(sku).match(/^(\d{6})/) || [])[1] || null;
    let changes = [];
    const ambiguous = [];
    let lineNote = null;
    try {
      const det = await jfetch('price-check', `${altItemsUrl}/api/fristads-order-lines?ref=${encodeURIComponent(orderNo || "")}`, { method: 'GET' });
      // Refuse to act on a partial parse: if their line totals don't add up to the order total then
      // a line was missed, and a missing line reads as a price difference that isn't there.
      if (det && det.reconciles === false) {
        lineNote = `line prices ignored — parsed £${det.linesTotal} across ${det.lineCount} lines but the order totals £${det.sum}`;
      } else if (det && det.lineCount) {
        const theirs = new Map(); // article → { qty, sum }
        for (const l of det.lines || []) {
          const art = artOf(l.code);
          if (!art || !(l.lineSum > 0) || !(l.qty > 0)) continue;
          const t = theirs.get(art) || { qty: 0, sum: 0 };
          t.qty += l.qty; t.sum += l.lineSum;
          theirs.set(art, t);
        }
        const ours = new Map(); // article → { qty, net, rows[] }
        for (const l of ourLines) {
          const art = artOf(l.sku);
          if (!art || !(l.qty > 0)) continue;                     // a £0 cost is a MISSING cost, not a reason to skip — see the heal note below
          const o = ours.get(art) || { qty: 0, net: 0, rows: [] };
          o.qty += l.qty; o.net += l.cost * l.qty; o.rows.push(l);
          ours.set(art, o);
        }
        for (const [art, o] of ours) {
          const t = theirs.get(art);
          if (!t) continue;
          // A qty mismatch is the multipack signature (one of our units = N of theirs), where any
          // unit-price comparison is meaningless — see the Blaklader 3625104299004XL case.
          if (t.qty !== o.qty) continue;
          const theirUnit = +(t.sum / t.qty).toFixed(4);
          const ourUnit = +(o.net / o.qty).toFixed(4);
          if (Math.abs(theirUnit - ourUnit) <= 0.005) continue;
          // Only one row for this article → the wrong cost is pinned to a SKU and can be healed.
          // Several rows → the article is out but not which size, so name it and leave it alone.
          if (o.rows.length === 1) changes.push({ sku: o.rows[0].sku, was: o.rows[0].cost, now: theirUnit });
          else ambiguous.push({ article: art, ourUnit, theirUnit, skus: o.rows.map((r) => r.sku) });
        }
        steps.priceCheck.lines = {
          articlesCompared: [...ours.keys()].filter((k) => theirs.has(k)).length,
          differences: changes.length + ambiguous.length,
          named: changes.map((c) => c.sku),
          ambiguous: ambiguous.map((x) => x.article),
        };
      }
    } catch (e) { lineNote = `couldn't read line prices from order ${orderNo}: ${e.message}`; }
    if (lineNote) steps.priceCheck.lineWarn = lineNote;

    const parts = [];
    for (const c of changes) parts.push(`${c.sku} ours £${c.was.toFixed(2)} vs theirs £${c.now.toFixed(2)}`);
    for (const x of ambiguous) parts.push(`article ${x.article} ours £${x.ourUnit.toFixed(2)} vs theirs £${x.theirUnit.toFixed(2)} (spans ${x.skus.join(", ")} — size not pinned)`);
    const named = parts.length
      ? ` Offending line(s): ${parts.join("; ")}.`
      : `${lineNote ? " " + lineNote + "." : " Couldn't pin it to a line."}`;
    // Fristads quote TRADE prices — the £4.25 insole proves it — not list like the Elastic
    // portals, so a difference pinned to a single SKU is safe to heal. One notice, after healing.
    await logPriceCheck(pool, steps, {
      supplierKey: 'FRISTADS', poId, changes, ambiguous,
      message: `Prices don't match: Fristads order total £${fristadsTotal} vs our PO net £${poNet.toFixed(2)} (diff £${priceGap}).${named} Order ${orderNo} still placed.`,
      context: { poId, orderNo, fristadsTotal, poNet: +poNet.toFixed(2), gap: priceGap, changes, ambiguous, poLines: breakdown },
    });
  }

  // 5. mark the PO placed + link the Fristads order. Status → Placed FIRST (guaranteed via
  // the API). Record the order number as a PO NOTE (reliable). The legacy web-form "reference"
  // write only renders its editable form when the PO is open in a real browser, so headlessly
  // it fails for POs — make it best-effort/non-fatal so it never blocks the finalize.
  // Reference = the Fristads order number if it's indexed; otherwise the reservation no as a
  // stand-in (the real order# can be backfilled — our PO# is on the Fristads order to find it).
  const poRef = orderNo || `Reservation ${reservationNo}`;
  await bp.setOrderStatusLive(poId, bp.PLACED_WITH_SUPPLIER_STATUS);
  let refWritten = false;
  try { await bp.setOrderReferenceLive(poId, poRef); refWritten = true; }   // API PATCH — tax-safe, no reprice
  catch (e) { steps.linkWarn = `reference-set failed (non-fatal): ${e.message}`; await bp.addOrderNoteLive(poId, `Placed with Fristads — order ${orderNo || '(order# pending indexing)'} (reservation ${reservationNo}). Reference-set failed: ${e.message}`, FRISTADS_SUPPLIER_CONTACT).catch(() => {}); }
  if (!orderNo) await bp.addOrderNoteLive(poId, `Placed with Fristads (reservation ${reservationNo}). Order# had not indexed yet, so the reference is the reservation no — our PO#${poId} is on the Fristads order (ExternalVerificationNo); backfill the real order# when it appears in history.`, FRISTADS_SUPPLIER_CONTACT).catch(() => {});
  steps.link = { reference: poRef, refWritten, reservationNo, orderNo: orderNo || null, orderNoPending: !orderNo, status: 7 };

  // 5b. The out-of-stock lines, as their own Fristads BACK ORDER. Only now — after the main order
  // is confirmed — so a failure here can never cost the in-stock lines their order. Same basket +
  // checkout as the main order with the back-order PO# as the reference; the basket answers the
  // phase-out question for each zero-stock size. Proven by hand on POs 489968/489969 (2026-09-17).
  // Non-fatal: the main order is placed and linked; a back order that did not go through is logged
  // as its own error so someone places it, and the BO PO stays at 45 with no "PLACED" note.
  if (steps.backorder && steps.backorder.created) {
    const boPoId = steps.backorder.poId;
    const boLines = shortLines.map((s) => ({ sku: s.sku, size: s.size, qty: s.qty }));
    const boUnits = boLines.reduce((a, l) => a + l.qty, 0);
    try {
      const bcart = await jfetch('backorder-cart', `${altItemsUrl}/api/fristads-basket`, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ clearFirst: true, lines: boLines }) });
      const brefused = (bcart.results || []).filter((r) => !r.ok);
      if ((bcart.unresolved || []).length || bcart.cartCount !== boUnits) {
        throw new Error(`basket holds ${bcart.cartCount}, expected ${boUnits}` + (brefused.length ? `; refused: ${brefused.map((r) => `${r.key} — ${JSON.stringify(r.resp && r.resp.messages || r.reason || r.status)}`).join('; ').slice(0, 300)}` : ''));
      }
      const bco = await jfetch('backorder-checkout', `${altItemsUrl}/api/fristads-checkout`, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ goodsMark: 'WORKWEAR', orderRef: String(boPoId), execute: true }) });
      if (!bco.placed) throw new Error(`checkout not confirmed (status ${bco.status}, messageType ${bco.messageType}, confirmed ${bco.confirmed})`);
      const boRes = bco.reservationNo;
      steps.backorder.fristads = { placed: true, reservationNo: boRes, cartCount: bcart.cartCount, backOrdered: (bcart.results || []).filter((r) => r.backOrdered).length };
      // Reference = the reservation for now (the order# indexes later, same as the main order);
      // status stays 45 On Back Order — that is the point of the PO.
      await bp.setOrderReferenceLive(boPoId, `Fristads reservation ${boRes}`).catch((e) => { steps.backorder.fristads.refWarn = e.message; });
      await bp.addOrderNoteLive(boPoId, `PLACED at Fristads on back order ${new Date().toISOString().slice(0, 10)} — reservation ${boRes}, our PO#${boPoId} on the order as ExternalVerificationNo. `
        + shortLines.map((s) => `${s.qty} × ${s.sku} (${s.size || '?'})${s.deldate ? ` expected ${s.deldate}` : ''}`).join('; ')
        + `. Order number will index at Fristads shortly. Status left at On Back Order deliberately.`, FRISTADS_SUPPLIER_CONTACT).catch(() => {});
      await logPurchasingError(pool, {
        supplier: 'FRISTADS', step: 'back-order-placed', severity: 'info', placed: true,
        message: `Back order placed with Fristads for PO#${boPoId} (reservation ${boRes}): ` + shortLines.map((s) => `${s.qty} × ${s.sku}${s.deldate ? ` due ${s.deldate}` : ''}`).join(', '),
        context: { poId: boPoId, parentPoId: poId, reservationNo: boRes, lines: boLines },
      }).catch(() => {});
    } catch (e) {
      steps.backorder.fristads = { placed: false, error: e.message };
      await bp.addOrderNoteLive(boPoId, `NOT YET PLACED at Fristads — the automatic back order failed: ${e.message}. Place this PO with Fristads by hand (basket answers the phase-out prompt with "add the original").`, FRISTADS_SUPPLIER_CONTACT).catch(() => {});
      await logPurchasingError(pool, {
        supplier: 'FRISTADS', step: 'back-order-not-placed', severity: 'error', placed: false,
        message: `Main order PO#${poId} placed, but the back order PO#${boPoId} did NOT place with Fristads: ${e.message}. Place it by hand — `
          + shortLines.map((s) => `${s.qty} × ${s.sku} (${s.size || '?'})`).join(', '),
        context: { poId: boPoId, parentPoId: poId, lines: boLines, backorderPoId: boPoId },
      }).catch(() => {});
    }
  }

  // 6. finalize the contributing SOs (clear tag, status 22, "ordered via PO#" note)
  if (soIds.length) { try { steps.finalize = await bp.finalizeSupplierTagsLive({ orderIds: soIds, supplierKey: 'FRISTADS', poId, noteContactId: FRISTADS_SUPPLIER_CONTACT, setOrderedStatus: true, linesByOrder, execute: true }); } catch (e) { throw stepErr('finalize', `order placed + PO linked, but finalising SOs failed: ${e.message}`); } }

  return { poId, reservationNo: poRef, orderNo, orderStatus: order && order.orderStatus, sum: order && order.sum, orderNoPending: !orderNo, steps };
}

// ── Castle placement chain ───────────────────────────────────────────────────
// Same skeleton as Fristads, but Castle's checkout POST *places directly* (no
// separate placeorder step) and the reference we write is Castle's order number.
async function placeCastleOrder(pool, altItemsUrl, { padToThreshold = 0 } = {}) {
  const steps = {};
  // 1. combined PO (allocation-aware demand + low-inv + separator)
  let po;
  try { po = await createPo({ supplierKey: 'CASTLE', execute: true, padToThreshold, logPool: pool }); }
  catch (e) { throw createPoErr(e); }
  if (!po.created) throw stepErr('create-po', `no PO created: ${po.reason || 'unknown'}` + (po.unresolvedSkus && po.unresolvedSkus.length ? ` — item codes not found in Brightpearl: ${po.unresolvedSkus.join(', ')}` : ''));
  const poId = po.poId;
  const soIds = [...new Set((po.soLines || []).map((l) => l.order).filter(Boolean))];
  const linesByOrder = {};
  for (const l of (po.soLines || [])) { if (l.order) (linesByOrder[l.order] = linesByOrder[l.order] || []).push({ sku: l.sku, qty: l.qty, name: l.name, productId: l.productId }); }
  steps.po = { poId, soUnits: po.soUnits, lowUnits: po.lowUnits, soIds, skippedBundles: po.skippedBundles || [] };

  // 2. push PO lines to the Castle basket (SKU-direct; size ignored by Castle).
  // Build from the PO-creation result (soLines/lowLines) — NOT getOrderCartLines:
  // the created BP PO row can degrade a variant SKU to the product's base SKU (e.g.
  // "177-GRY-L" → "177"), but the creation result keeps the SO row's real SKU.
  // Same merge as Performance Brands and Sterling, for the same reason: the PO can carry the same
  // SKU on two rows, and a basket that does not accumulate repeat adds keeps the LAST qty rather
  // than the sum, leaving the cart short. Castle's cartCount check below would then stall the run
  // — a safe failure, but an avoidable one, and this shape has already cost three orders.
  const cartLines = mergePoLinesBySku(po).map((l) => ({ sku: l.sku, qty: l.qty }));
  const expectUnits = cartLines.reduce((a, l) => a + l.qty, 0);
  const cart = await jfetch('cart', `${altItemsUrl}/api/castle-basket`, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ clearFirst: true, lines: cartLines }) });
  // `results` is one entry per STYLE — Castle adds a whole style's variants in a single form POST,
  // so when the basket ends short the failing group is named there and nowhere else. Dropping it is
  // why PO 489088 could only say "48, expected 49" on 2026-09-14: every SKU resolved, every one had
  // hundreds in stock, and which of 30 lines never went in was simply not recorded. Same omission
  // the Fristads cart step had, and the same fix.
  const castleRefused = (cart.results || []).filter((r) => !r.ok);
  steps.cart = { cartCount: cart.cartCount, expectUnits, unresolved: cart.unresolved, refused: castleRefused };
  if ((cart.unresolved || []).length) throw stepErr('cart', `item not found on the Castle portal (codes don't match): ${JSON.stringify(cart.unresolved)}`, { poId, unresolved: cart.unresolved, refused: castleRefused });
  // "A line was refused" and "every add was accepted and the basket is still short" are different
  // faults needing different work, and the old message could not tell them apart. Castle reports
  // what it SET per style (results[].added), so compare that with what the basket ended up holding.
  const castleAttempted = (cart.results || []).reduce((a, r) => a + (r.added || []).reduce((b, x) => b + (Number(x.qty) || 0), 0), 0);
  // Alt-Items now reads the basket back BY ITEM CODE, so name the lines outright rather than
  // leaving a count to be diffed by hand.
  const castleMissing = cart.missing || [];
  // A PACK rule is a different answer from a missing line and the only one with an obvious action:
  // Castle sell it in sixes, we asked for one, so ask for six. Put it first — it explains the
  // shortfall, and without it 606-BLK-ONE just reads as "never reached the basket" with no reason.
  const packBlocked = cart.packBlocked || [];
  if (cart.cartCount !== expectUnits) throw stepErr('cart', `cart quantity mismatch: portal shows ${cart.cartCount}, expected ${expectUnits} — some lines didn't add`
    + (packBlocked.length ? `. PACK SIZE: ${packBlocked.map((p) => `${p.sku} is sold in multiples of ${p.pack} — we asked for ${p.qty}, order ${p.suggest}`).join('; ').slice(0, 300)}` : '')
    + (castleMissing.length ? `. MISSING: ${castleMissing.map((m) => `${m.sku} (wanted ${m.wanted}${m.inBasket ? `, only ${m.inBasket} in basket` : ', never reached the basket'})`).join('; ').slice(0, 300)}` : '')
    + ((cart.unexpected || []).length ? `. Also in the basket but NOT asked for: ${cart.unexpected.map((u) => `${u.sku} x${u.qty}`).join(', ').slice(0, 150)}` : '')
    + (castleRefused.length
      ? `. Castle refused ${castleRefused.length} style group(s): ${castleRefused.map((r) => `${r.id} — ${r.error || r.reason || r.status}`).join('; ').slice(0, 200)}`
      : castleAttempted === expectUnits
        ? `. Castle ACCEPTED every add (all ${(cart.results || []).length} style group(s) redirected) and the basket is still ${expectUnits - cart.cartCount} short — the portal dropped a line it had taken, so this is theirs, not a bad code.`
        : `. Only ${castleAttempted} of ${expectUnits} units were even attempted — a variant did not map to an order field.`),
    // The whole request goes in the context: with per-style results AND what we asked for, the
    // missing line is a diff rather than a hunt through thirty SKUs by hand.
    { poId, cartCount: cart.cartCount, expectUnits, attempted: castleAttempted, refused: castleRefused,
      missing: castleMissing, packBlocked, unexpected: cart.unexpected || [], basketItems: cart.basketItems || null,
      results: cart.results || null, sent: cartLines.map((l) => ({ sku: l.sku, qty: l.qty })) });

  // 3. checkout — Castle's POST places the order in one step. CustomerPO = our PO#.
  const co = await jfetch('checkout', `${altItemsUrl}/api/castle-checkout`, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ customerPO: String(poId), execute: true }) });
  steps.checkout = { placed: co.placed, status: co.status, goodsTotal: co.goodsTotal, totalValue: co.totalValue, orderNo: co.orderNo || null };
  if (!co.placed) throw stepErr('checkout', `Castle checkout did not confirm placement (status ${co.status}): ${JSON.stringify(co.problems || co.bodyPeek || co)}`);

  // 4. pull the Castle order number by our PO# (CustomerPO); retry briefly
  let order = co.orderNo ? { found: true, orderNo: co.orderNo } : null;
  for (let i = 0; !(order && order.found && order.orderNo) && i < 6; i++) {
    order = await jfetch('order-pull', `${altItemsUrl}/api/castle-order?po=${encodeURIComponent(poId)}`);
    if (order && order.found && order.orderNo) break;
    await new Promise((r) => setTimeout(r, 5000));
  }
  if (!order || !order.found || !order.orderNo) throw stepErr('order-pull', `order placed but not found in Castle history for PO ${poId} — link it manually (checkout status ${co.status})`);
  steps.order = { orderNo: order.orderNo };

  // price sanity check (NON-FATAL): Castle goods total (ex-VAT) vs our PO net
  const poNet = [...(po.soLines || []), ...(po.lowLines || [])].reduce((a, l) => a + (l.cost || 0) * l.qty, 0);
  const castleGoods = parseFloat(String(co.goodsTotal || '').replace(/[^\d.]/g, '')) || 0;
  const priceGap = castleGoods ? +(castleGoods - poNet).toFixed(2) : 0;
  steps.priceCheck = { castleGoods, poNet: +poNet.toFixed(2), gap: priceGap };
  if (castleGoods && Math.abs(priceGap) > 0.50) {
    const breakdown = [...(po.soLines || []), ...(po.lowLines || [])].map((l) => `${l.qty} × ${l.sku} — our £${(l.cost || 0).toFixed(2)}/ea (${l.name})`);
    await logPurchasingError(pool, {
      supplier: 'CASTLE', step: 'price-check', severity: 'review',
      message: `Prices don't match: Castle goods total £${castleGoods} vs our PO net £${poNet.toFixed(2)} (diff £${priceGap}). A Brightpearl cost price (Launch/list 20) may need adjusting. Order ${order.orderNo} still placed.`,
      context: { poId, orderNo: order.orderNo, castleGoods, poNet: +poNet.toFixed(2), gap: priceGap, poLines: breakdown },
    }).catch(() => {});
  }

  // 5. write the Castle order number onto our PO reference + restore tax + status 7
  await bp.setOrderStatusLive(poId, bp.PLACED_WITH_SUPPLIER_STATUS);
  let castleRefWritten = false;
  try { await bp.setOrderReferenceLive(poId, order.orderNo); castleRefWritten = true; }   // API PATCH — tax-safe
  catch (e) { steps.linkWarn = `reference-set failed (non-fatal): ${e.message}`; await bp.addOrderNoteLive(poId, `Placed with Castle — order ${order.orderNo}. Reference-set failed: ${e.message}`, CASTLE_SUPPLIER_CONTACT).catch(() => {}); }
  steps.link = { reference: order.orderNo, refWritten: castleRefWritten, orderNo: order.orderNo, status: 7 };

  // 6. finalize the contributing SOs (clear CASTLE tag, status 22 when fully ordered, note)
  if (soIds.length) { try { steps.finalize = await bp.finalizeSupplierTagsLive({ orderIds: soIds, supplierKey: 'CASTLE', poId, noteContactId: CASTLE_SUPPLIER_CONTACT, setOrderedStatus: true, linesByOrder, execute: true }); } catch (e) { throw stepErr('finalize', `order placed + PO linked, but finalising SOs failed: ${e.message}`); } }

  return { poId, orderNo: order.orderNo, steps };
}

// Supplier registry for the scheduled runner (per-supplier state row id + place fn).
// Drive the portal worker asynchronously: start a job, then poll until done/error.
// A full order takes many minutes (WebForms postbacks per line), so we can't hold one
// HTTP request open — the worker returns a jobId and we poll GET /job/:id.
// opts is passed straight through to the supplier module. Blaklader needs it: its module does NOT
// drive the checkout UI, it posts the order body Alt-Items already built from inside the logged-in
// page, so it must be handed { body, cartId }.
// confirmPlaced — an OPTIONAL async probe that asks the SUPPLIER whether this PO already exists on
// their order list. Supply it and the poll stops the moment the order is confirmed, instead of
// waiting out a worker that is never going to answer.
//
// Blaklader routinely accept an order and never reply: 27 and 31 Aug, 1, 3, 4 and 7 Sept. Every one
// of those was recovered by reading their order list — but only AFTER the worker gave up, so each
// cost 20-25 minutes of a lock that every other supplier queues behind. On 2026-09-07 the run held
// it from 10:35 while the order had been sitting at Blaklader almost the whole time.
//
// The waiting was never necessary. Their list is the authority the recovery already trusts, so ask
// it DURING the poll: order confirmed, stop waiting, hand back the same not-ok shape a submit
// timeout produces so the caller's existing recovery does the rest. Nothing new decides anything.
//
// Not before confirmAfterMs — the order cannot exist until the submit has been made, and an early
// miss proves nothing. It matches on OUR PO number, so it can never mistake a previous order for
// this one; a probe that throws is ignored, because failing to read the list is not evidence.
async function workerPlaceOrder({ supplier = 'STERLING', ref, lines, execute, opts = null,
  confirmPlaced = null, confirmAfterMs = 3 * 60 * 1000, confirmEveryMs = 36000 }) {
  const headers = { 'Content-Type': 'application/json', 'x-worker-secret': STERLING_WORKER_SECRET };
  const start = await jfetch('checkout', `${STERLING_WORKER_URL}/place-order`, {
    method: 'POST', headers,
    body: JSON.stringify({ supplier, ref: String(ref), lines, execute, async: true, ...(opts ? { opts } : {}) }),
  });
  const jobId = start && start.jobId;
  if (!jobId) throw stepErr('checkout', `worker didn't start a job: ${JSON.stringify(start)}`);
  const startedAt = Date.now();
  let nextConfirm = startedAt + confirmAfterMs;
  const deadline = Date.now() + 25 * 60 * 1000;      // orders can be long; generous ceiling
  while (Date.now() < deadline) {
    await new Promise((r) => setTimeout(r, 12000));
    // Poll the job WITHOUT jfetch's generic `.error` short-circuit. A job that FINISHED but did not
    // place carries its error string ALONGSIDE the evidence — expectedUnits, cart, importSteps and
    // missingLines — and jfetch throws on the string, discarding all of it.
    //
    // That is exactly what blinded us on SNICKERS 2026-08-31. placeSnickersOrder already names the
    // dropped lines and puts missingLines/expectedUnits/cartUnits in the error context, deliberately
    // untruncated, so a person and the triage pass can both act on it — but that code is UNREACHABLE
    // while this poll throws first. All anyone ever saw was "not ready to place" and {poId, dryRun,
    // ukTime}, and by the time it was read the worker had expired the job, so the evidence was gone
    // twice over. Every caller already handles a returned not-ok job, so hand it back and let them.
    // Only a transport or HTTP failure is an error HERE.
    let j = null;
    try {
      const res = await fetch(`${STERLING_WORKER_URL}/job/${jobId}`, { headers });
      const text = await res.text();
      try { j = text ? JSON.parse(text) : null; } catch { j = null; }
      if (!res.ok) throw stepErr('checkout', `HTTP ${res.status} polling worker job ${jobId}: ${String(text).slice(0, 200)}`, { jobId });
    } catch (e) {
      if (e.step) throw e;                       // already a step error - keep it
      throw stepErr('checkout', `can't reach worker job ${jobId}: ${e.message}`, { jobId });
    }
    if (!j) throw stepErr('checkout', `worker job ${jobId} returned no JSON`, { jobId });
    // Hand the jobId back with the result. The worker holds a finished job — screenshot, trail and
    // all — for 30 minutes, but NEITHER service logs the id, so when Snickers PO 488518 failed at
    // confirm on 2026-09-11 the one image that would have said whether £3k had been spent was
    // sitting in memory at an address nobody could name, and it expired untouched. Callers put this
    // in the error context; GET {worker}/job/{jobId} then retrieves it while it lasts.
    if (j.status === 'done') return { ...j, jobId };  // ok OR not-ok - the caller inspects it
    if (j.status === 'error') throw stepErr('checkout', `worker job errored: ${j.error}`, { jobId, job: j });

    // Still running. Ask the supplier whether the order already landed.
    if (confirmPlaced && Date.now() >= nextConfirm) {
      nextConfirm = Date.now() + confirmEveryMs;
      const landed = await confirmPlaced().catch(() => null);
      if (landed) {
        const waited = Math.round((Date.now() - startedAt) / 60000);
        // submitTimedOut marks this as the KNOWN unanswered-submit case, which is what makes the
        // caller log it quietly. It is the honest label: the worker is still waiting on a reply
        // that already produced an order.
        return {
          status: 'done', ok: false, earlyConfirmed: true, submitTimedOut: true, jobId, landed,
          error: `${supplier} never answered the submit, but the order IS on their order list `
            + `(${landed.internalId || landed.orderNumber}) — stopped waiting after ${waited} min `
            + `instead of holding the run lock for the full 25.`,
        };
      }
    }
  }
  throw stepErr('checkout', `worker job ${jobId} timed out (still running after 25 min)`);
}

// Ask the worker what its browser sees at Blaklader's checkout. READ-ONLY — cartProbe never touches
// the basket and never submits; it exists because the empty-cart refusal records counts, not the
// page, and the two causes it could have need opposite fixes.
export async function blakladerCartProbe({ tries = 2 } = {}) {
  const headers = { 'Content-Type': 'application/json', 'x-worker-secret': STERLING_WORKER_SECRET };
  const start = await jfetch('cart-probe', `${STERLING_WORKER_URL}/place-order`, {
    method: 'POST', headers,
    body: JSON.stringify({ supplier: 'BLAKLADER', ref: 'probe', lines: [{ stockCode: 'x', qty: 1 }], execute: false, async: true, opts: { cartProbe: true, tries } }),
  });
  const jobId = start && start.jobId;
  if (!jobId) throw new Error(`worker didn't start a probe job: ${JSON.stringify(start).slice(0, 200)}`);
  const deadline = Date.now() + 6 * 60 * 1000;
  while (Date.now() < deadline) {
    await new Promise((r) => setTimeout(r, 8000));
    const res = await fetch(`${STERLING_WORKER_URL}/job/${jobId}`, { headers });
    const text = await res.text();
    let j = null; try { j = text ? JSON.parse(text) : null; } catch { /* keep polling */ }
    if (j && j.status === 'done') return j;
    if (j && j.status === 'error') throw new Error(`probe job errored: ${j.error}`);
  }
  throw new Error(`probe job ${jobId} did not finish within 6 minutes`);
}

// Fallback order-number pull: if place() didn't return the Sterling OrderID, read the
// account's Order Status (worker ordersList mode) and find the row carrying OUR ref (PO#).
// Retries a few times — a just-placed order can take a moment to list.
async function pullSterlingOrderNo(poId) {
  const headers = { 'Content-Type': 'application/json', 'x-worker-secret': STERLING_WORKER_SECRET };
  for (let attempt = 0; attempt < 4; attempt++) {
    try {
      const start = await jfetch('order-pull', `${STERLING_WORKER_URL}/place-order`, {
        method: 'POST', headers,
        body: JSON.stringify({ supplier: 'STERLING', ref: String(poId), lines: [{ search: 'x', colour: 'x', size: 'x', qty: 1 }], execute: false, async: true, opts: { ordersList: true } }),
      });
      const jobId = start && start.jobId;
      if (jobId) {
        const deadline = Date.now() + 3 * 60 * 1000;
        while (Date.now() < deadline) {
          await new Promise((r) => setTimeout(r, 8000));
          const j = await jfetch('order-pull', `${STERLING_WORKER_URL}/job/${jobId}`, { headers });
          if (j.status === 'done') {
            const hit = (j.rows || []).find((r) => new RegExp(`\\b${poId}\\b`).test(String(r)));
            const m = hit && String(hit).match(/Select\s+(\d{5,})/);
            if (m) return m[1];
            break;
          }
          if (j.status === 'error') break;
        }
      }
    } catch { /* try again */ }
    await new Promise((r) => setTimeout(r, 20000));   // let the order settle into the list
  }
  return null;
}

// ── Sterling placement chain ─────────────────────────────────────────────────
// Sterling's shop (sterling.famlive.net) is a WebForms site with no HTTP order API, so
// the order is placed by the headless portal-order WORKER. Each PO line's EAN resolves
// (sterlingProducts.json) to { search, colour, size } the worker uses to drive the shop.
async function placeSterlingOrder(pool, altItemsUrl, { padToThreshold = 0 } = {}) {
  const steps = {};
  let po;
  try { po = await createPo({ supplierKey: 'STERLING', execute: true, padToThreshold, logPool: pool }); }
  catch (e) { throw createPoErr(e); }
  if (!po.created) throw stepErr('create-po', `no PO created: ${po.reason || 'unknown'}` + (po.unresolvedSkus && po.unresolvedSkus.length ? ` — item codes not found in Brightpearl: ${po.unresolvedSkus.join(', ')}` : ''));
  const poId = po.poId;
  const soIds = [...new Set((po.soLines || []).map((l) => l.order).filter(Boolean))];
  const linesByOrder = {};
  for (const l of (po.soLines || [])) { if (l.order) (linesByOrder[l.order] = linesByOrder[l.order] || []).push({ sku: l.sku, qty: l.qty, name: l.name, productId: l.productId }); }
  steps.po = { poId, soUnits: po.soUnits, lowUnits: po.lowUnits, soIds, skippedBundles: po.skippedBundles || [] };

  // resolve each PO line (EAN -> search/colour/size); skip service lines; abort on genuinely-unresolved
  const { resolveSterlingLine, isNonSterlingOrderable } = await import('./sterlingResolve.js');
  let poLines = [...(po.soLines || []), ...(po.lowLines || [])].filter((l) => String(l.productId) !== '1000');
  const unresolved = [], skipped = [];

  // ── STOCK PRE-FLIGHT: MARK WHAT STERLING CANNOT SHIP YET AS BACK ORDER ─────────────────────
  // Sterling's shop accepts a zero-stock line without a word and simply holds it — so an order
  // with one out-of-stock item ships late in full, and nothing on our side says why. The feed knew
  // all along: 5055160050803 (Dewalt Easton tee, Black L, SO 489794) went onto PO 489978 on
  // 2026-09-17 while supplier_stock said avail 0, due 20/9, refreshed at 08:37 that morning. The
  // run never asked.
  //
  // EVERY LINE IS STILL ORDERED. The short ones stay in Sterling's basket so Sterling hold them
  // on back order their end — which is what lets someone later cancel with Sterling, or leave it,
  // as they choose. What changes is the Brightpearl side: the short lines move from the PO that
  // ships onto their own PO at "On Back Order", child of the original, noted both ends with the
  // date, so the receipt for the shipped goods reconciles and the back order is visibly on order.
  // (Taking them OUT of the basket — the first version of this — would have meant nobody had
  // ordered them from Sterling at all, and a human sending the back-order PO by hand on the day.)
  //
  // FAIL OPEN. An unreadable feed leaves the line where it is — that is today's behaviour — and a
  // feed row with no figure says nothing and must act like it. Only a confident "0 available" acts.
  const sterlingShort = [];
  {
    const probe = async (l) => {
      try {
        const j = await (await fetch(`${altItemsUrl}/api/feed-stock?supplier=Sterling&code=${encodeURIComponent(l.sku)}`, { signal: AbortSignal.timeout(20000) })).json();
        // j.avail == null must be null here, not 0: Number(null) is 0, and 0 is the one value that
        // moves a line onto back order. A feed row with no figure says nothing and must act like it.
        if (!j || j.found !== true || j.avail == null || !Number.isFinite(Number(j.avail))) return null;
        return { avail: Number(j.avail), deldate: j.deldate || null };
      } catch { return null; }
    };
    const STERLING_STOCK_CONCURRENCY = 6;
    for (let i = 0; i < poLines.length; i += STERLING_STOCK_CONCURRENCY) {
      const batch = poLines.slice(i, i + STERLING_STOCK_CONCURRENCY);
      const got = await Promise.all(batch.map(probe));
      batch.forEach((l, n) => { if (got[n] && got[n].avail === 0) sterlingShort.push({ ...l, avail: 0, deldate: got[n].deldate }); });
    }
    steps.stockCheck = { checked: poLines.length, short: sterlingShort.map((s) => ({ sku: s.sku, qty: s.qty, deldate: s.deldate })) };
    if (sterlingShort.length) {
      // Move the short rows to a back-order PO, child of this one, noted both ends. The rows come
      // off THIS PO only so that it matches the goods that will actually arrive on the first
      // delivery; the units themselves are still in the basket and still ordered. Best-effort per
      // row, and if the back-order PO cannot be created the rows are LEFT on this PO — a line that
      // is ordered but sits on the wrong PO beats one that has vanished from both.
      const bo = await bp.createBackorderPoLive({
        supplierKey: 'STERLING', parentPoId: poId, execute: true,
        lines: sterlingShort.map((s) => ({ productId: s.productId, sku: s.sku, name: s.name, qty: s.qty, deldate: s.deldate })),
        note: `Ordered from Sterling on PO#${poId} (${new Date().toISOString().slice(0, 10)}) but out of stock their end — held on back order by Sterling. Cancel with them or leave it, as the customer needs.`,
      }).catch((e) => ({ created: false, error: e.message }));
      steps.backorder = bo;
      if (bo.created) {
        for (const s of sterlingShort) { await bp.removePoRowLive({ poId, sku: s.sku, execute: true }).catch(() => {}); }
        // The SO note and finalise name the PO the goods are ON — the back-order one for these.
        for (const id of Object.keys(linesByOrder)) {
          linesByOrder[id] = linesByOrder[id].filter((x) => !sterlingShort.some((s) => String(s.sku).toUpperCase() === String(x.sku).toUpperCase()));
          if (!linesByOrder[id].length) delete linesByOrder[id];
        }
      }
      await logPurchasingError(pool, {
        supplier: 'STERLING', step: 'out-of-stock-dropped', severity: 'error', placed: true,
        message: `${sterlingShort.length} line(s) on PO#${poId} are out of stock at Sterling — STILL ORDERED and held on back order by Sterling`
          + (bo.created ? `, moved to back-order PO#${bo.poId} in Brightpearl` : ' (back-order PO could NOT be created, so they remain on this PO: ' + (bo.error || bo.reason) + ')')
          + `. The rest of the order ships now:
`
          + sterlingShort.map((s) => `      ${s.qty} × ${s.sku} ${s.name || ''} — Sterling have 0${s.deldate ? `, due ${s.deldate}` : ''}`).join('\n'),
        context: { poId, dropped: sterlingShort, backorderPoId: bo.created ? bo.poId : null, linesByOrder },
      }).catch(() => {});
    }
  }
  // Merge lines that resolve to the SAME shop variant (search|colour|size) into ONE add,
  // summing qty. The PO can carry two rows for the same variant (e.g. two SOs both needing
  // Mercury Black 11); the shop's basket merges duplicate adds and keeps the LAST qty, not
  // the sum — so adding them separately silently drops units. One deduped add avoids that.
  const byVariant = new Map();
  for (const l of poLines) {
    if (isNonSterlingOrderable(l.sku)) { skipped.push(l.sku); continue; }
    const r = await resolveSterlingLine({ sku: l.sku, productId: l.productId });
    if (!r.resolved) { unresolved.push(l.sku); continue; }
    const key = [r.search, r.colour || '', r.size].map((s) => String(s).trim().toLowerCase()).join('|');
    if (byVariant.has(key)) byVariant.get(key).qty += Math.round(l.qty);
    else byVariant.set(key, { search: r.search, colour: r.colour, size: r.size, qty: Math.round(l.qty), leg: r.leg, waist: r.waist, legIndex: r.legIndex, legCount: r.legCount });
  }
  const lines = [...byVariant.values()];
  if (unresolved.length) throw stepErr('resolve', `Sterling lines not in the product-data file (order NOT placed): ${unresolved.join(', ')}. Update the Sterling product-data file / ingest.`);
  if (!lines.length) throw stepErr('resolve', 'no resolvable Sterling lines to order');
  steps.resolve = { lines: lines.length, skipped };

  // drive the headless worker (async job + poll) to place the order on the shop
  const wr = await workerPlaceOrder({ ref: poId, lines, execute: true });
  // WHY it failed, before WHAT was in the basket. `wr.results` is the (large) per-line add log, so
  // `wr.results || wr.error` always picked it and every field that explains a failure — was the
  // confirm button still there, what did the page say, did the delivery address and customer ref
  // land — was discarded at the one moment it mattered. Two Sterling runs on 1 and 2 Sept failed
  // identically at confirm and taught us nothing: error #118 and #120 are 4kB of "ok":true adds.
  // Diagnostics FIRST because the log truncates, and the lines array is long enough to push them
  // out of every view that reads it.
  if (!wr || !wr.placed) {
    const why = wr ? {
      stillOnConfirm: wr.stillOnConfirm, confirmText: wr.confirmText, url: wr.url,
      delAddr: wr.delAddr, refSet: wr.refSet, cartCount: wr.cartCount,
      added: wr.added, expected: wr.expected, units: wr.units, ready: wr.ready,
      orderNo: wr.orderNo, cleared: wr.cleared, error: wr.error,
      hasScreenshot: !!wr.screenshot, results: wr.results,
    } : wr;
    throw stepErr('checkout', `Sterling worker did not confirm placement: ${JSON.stringify(why)}`);
  }
  let orderNo = wr.orderNo || null;
  if (!orderNo) { try { orderNo = await pullSterlingOrderNo(poId); } catch { /* leave null → Placed-<poId> marker */ } }
  steps.checkout = { placed: true, orderNo, orderNoSource: wr.orderNo ? 'place' : (orderNo ? 'order-status' : 'none'), cartCount: wr.cartCount, added: wr.added };

  // link + finalise (order# onto PO ref if known, else a marker) + restore tax + status 7
  const ref = orderNo || `Placed-${poId}`;
  await bp.setOrderStatusLive(poId, bp.PLACED_WITH_SUPPLIER_STATUS);
  let sterlRefWritten = false;
  try { await bp.setOrderReferenceLive(poId, ref); sterlRefWritten = true; }   // API PATCH — tax-safe
  catch (e) { steps.linkWarn = `reference-set failed (non-fatal): ${e.message}`; await bp.addOrderNoteLive(poId, `Placed with Sterling — order ${ref}. Reference-set failed: ${e.message}`, STERLING_SUPPLIER_CONTACT).catch(() => {}); }
  steps.link = { reference: ref, refWritten: sterlRefWritten, orderNo, status: 7 };

  if (soIds.length) { try { steps.finalize = await bp.finalizeSupplierTagsLive({ orderIds: soIds, supplierKey: 'STERLING', poId, noteContactId: STERLING_SUPPLIER_CONTACT, setOrderedStatus: true, linesByOrder, execute: true }); } catch (e) { throw stepErr('finalize', `order placed + PO linked, but finalising SOs failed: ${e.message}`); } }
  return { poId, orderNo, steps };
}

// ── Uneek placement chain (EMAIL supplier) ───────────────────────────────────
// No portal: create the combined PO, EMAIL Brightpearl's OWN PO PDF to Uneek's order desk
// (template_print.php via the File Uploader — send_type=pdf, recipient = ONLY email_to_0),
// mark the PO Placed, and finalise the SOs. Email suppliers have no supplier order number,
// so the PO# is the reference.
const UNEEK_SUPPLIER_CONTACT = 322;
const UNEEK_ORDER_EMAIL = process.env.UNEEK_ORDER_EMAIL || 'orders@uneekclothing.com';
async function placeUneekOrder(pool, altItemsUrl, { padToThreshold = 0 } = {}) {
  const steps = {};
  // 1. combined PO (SO demand + low-inv; stamps the SOs)
  let po;
  try { po = await createPo({ supplierKey: 'UNEEK', execute: true, padToThreshold, logPool: pool }); }
  catch (e) { throw createPoErr(e); }
  if (!po.created) throw stepErr('create-po', `no PO created: ${po.reason || 'unknown'}` + (po.unresolvedSkus && po.unresolvedSkus.length ? ` — item codes not found in Brightpearl: ${po.unresolvedSkus.join(', ')}` : ''));
  const poId = po.poId;
  const soIds = [...new Set((po.soLines || []).map((l) => l.order).filter(Boolean))];
  const linesByOrder = {};
  for (const l of (po.soLines || [])) { if (l.order) (linesByOrder[l.order] = linesByOrder[l.order] || []).push({ sku: l.sku, qty: l.qty, name: l.name, productId: l.productId }); }
  steps.po = { poId, soUnits: po.soUnits, lowUnits: po.lowUnits, soIds, skippedBundles: po.skippedBundles || [] };

  // 2. EMAIL Brightpearl's real PO PDF to Uneek's order desk (only email_to_0 = the order
  // address; BP's pre-filled supplier/account rows are cleared inside emailOrderDocument).
  const mail = await emailOrderDocument(poId, { contactId: UNEEK_SUPPLIER_CONTACT, to: UNEEK_ORDER_EMAIL, send: true });
  if (!mail.sent) throw stepErr('email', `Brightpearl did not confirm emailing PO#${poId} to ${UNEEK_ORDER_EMAIL}: ${JSON.stringify(mail).slice(0, 200)}`);
  steps.email = { to: UNEEK_ORDER_EMAIL, sent: true, status: mail.status };

  // 3. mark the PO Placed (status 7) + a note recording the email.
  await bp.setOrderStatusLive(poId, bp.PLACED_WITH_SUPPLIER_STATUS);
  await bp.addOrderNoteLive(poId, `PO emailed to Uneek (${UNEEK_ORDER_EMAIL}).`, UNEEK_SUPPLIER_CONTACT).catch(() => {});
  steps.link = { status: 7, emailedTo: UNEEK_ORDER_EMAIL };

  // 4. finalise the contributing SOs (clear the Uneek tag, status → 22, "ordered via PO#" note)
  if (soIds.length) { try { steps.finalize = await bp.finalizeSupplierTagsLive({ orderIds: soIds, supplierKey: 'UNEEK', poId, noteContactId: UNEEK_SUPPLIER_CONTACT, setOrderedStatus: true, linesByOrder, execute: true }); } catch (e) { throw stepErr('finalize', `PO emailed + placed, but finalising SOs failed: ${e.message}`); } }
  return { poId, emailedTo: UNEEK_ORDER_EMAIL, steps };
}

// ── Email suppliers: one placement chain, configured per supplier ────────────
// Brightpearl builds its own PO PDF and emails it to the supplier's order desk. There is no portal,
// so there is no checkout that can half-succeed — the whole class is the simplest we have. Uneek
// and Scruffs predate this and keep their own copies; V12 and Buckler share this one, and the next
// email supplier is a config block rather than another near-identical function.
//
// CARRIAGE GOES ON THE PO. Every other supplier only ever WAITS for free carriage; these are
// ordered every few days and mostly pay it, so the charge has to be on the document. Without it the
// PO says X, the invoice says X + carriage, and every reconciliation afterwards is out by that
// amount with nothing explaining it.
//
// It is added BEFORE the PDF is emailed — a PO that reaches the supplier without the carriage on it
// does not match what they will invoice — and only when the order is genuinely under the threshold.
// addPoMiscRowLive refuses a duplicate of the same text, so a re-run cannot stack a second charge.
const EMAIL_SUPPLIER_CONFIG = {
  V12: {
    label: 'V12 Footwear', contactId: 92811,
    email: () => process.env.PO_EMAIL_V12 || 'sales@v12footwear.com',
    carriageNet: () => Number(process.env.V12_CARRIAGE_CHARGE || 6.95),
    freeOver: () => Number(process.env.V12_FREESHIP_THRESHOLD || 200),
  },
  // ── HELLBERG ────────────────────────────────────────────────────────────────────────────────
  // Split out of Snickers on 2026-09-16. Hellberg clears customs separately and takes about two
  // weeks, so one Hellberg line on a Snickers portal order held up everything else on it. Its own
  // PO now, EMAILED rather than placed through the Hultafors portal, at 15:20.
  //
  // It is the same supplier commercially — the PO goes to the Snickers order desk on contact 331 —
  // so the address is read from Brightpearl at run time rather than written here. If it changes on
  // the contact record, this follows it; there is no second copy to forget. PO_EMAIL_HELLBERG
  // overrides when the desk ever differs.
  //
  // NO MINIMUM ORDER, hence no carriage and no free-delivery threshold: the run places whatever
  // demand exists on the day rather than accumulating, which is the whole point of separating it.
  HELLBERG: {
    label: 'Hellberg', contactId: 331,
    email: () => process.env.PO_EMAIL_HELLBERG || null,   // null → resolved from the BP contact below
    emailFromSupplier: 'SNICKERS',
    carriageNet: () => 0,
    freeOver: () => 0,
  },
  BUCKLER: {
    label: 'Buckler Boots', contactId: 8981,
    email: () => process.env.PO_EMAIL_BUCKLER || 'orders@bucklerboots.com',
    // Left unset deliberately until the terms are confirmed: with no charge configured the run
    // places as normal and adds nothing, which is the safe direction — a missing carriage line is
    // a reconciliation gap, an invented one is a wrong PO sent to a supplier.
    carriageNet: () => Number(process.env.BUCKLER_CARRIAGE_CHARGE || 0),
    freeOver: () => Number(process.env.BUCKLER_FREESHIP_THRESHOLD || 0),
  },
};

async function placeEmailSupplierOrder(supplierKey, pool, altItemsUrl, { padToThreshold = 0, live = true } = {}) {
  const cfg = EMAIL_SUPPLIER_CONFIG[supplierKey];
  if (!cfg) throw stepErr('create-po', `no email-supplier config for ${supplierKey}`);
  // A config may name ANOTHER supplier's order desk instead of carrying its own address — Hellberg
  // is ordered from Snickers commercially and only separated for customs. Reading it from
  // Brightpearl keeps one copy of the truth: change the contact, and this follows.
  let to = cfg.email();
  if (!to && cfg.emailFromSupplier) {
    to = await bp.supplierEmailOf(cfg.emailFromSupplier).catch(() => null);
    if (!to) throw stepErr('email', `no order email for ${supplierKey}: Brightpearl holds none for `
      + `${cfg.emailFromSupplier} either. Set PO_EMAIL_${supplierKey} or put the address on the contact.`);
  }
  if (!to) throw stepErr('email', `no order email configured for ${supplierKey} — set PO_EMAIL_${supplierKey}`);
  const carriageNet = cfg.carriageNet();
  const freeOver = cfg.freeOver();
  const steps = {};

  let po;
  try { po = await createPo({ supplierKey, execute: live, padToThreshold, logPool: pool }); }
  catch (e) { throw createPoErr(e); }
  if (!po.created) throw stepErr('create-po', `no PO created: ${po.reason || 'unknown'}` + (po.unresolvedSkus && po.unresolvedSkus.length ? ` — item codes not found in Brightpearl: ${po.unresolvedSkus.join(', ')}` : ''));
  const poId = po.poId;
  const soIds = [...new Set((po.soLines || []).map((l) => l.order).filter(Boolean))];
  const linesByOrder = {};
  for (const l of (po.soLines || [])) { if (l.order) (linesByOrder[l.order] = linesByOrder[l.order] || []).push({ sku: l.sku, qty: l.qty, name: l.name, productId: l.productId }); }
  steps.po = { poId, soUnits: po.soUnits, lowUnits: po.lowUnits, soIds, skippedBundles: po.skippedBundles || [] };

  // createPo returns NONE of netValue, soNet or lowNet — it never has — so this read 0 on every
  // run since it was written, and 0 satisfies no branch below: not "under the threshold, add
  // carriage", not "over it, don't", not "this supplier has no carriage". Nothing ran, steps.carriage
  // stayed undefined, and V12 has therefore never had a carriage line added to anything. PO 489092
  // today is the example: £104.37 of goods against a £200 free-carriage threshold, £6.95 that should
  // have gone on and did not. The scheduler had the figure the whole time (its own report says
  // netValue 104.37); it just never reached here.
  //
  // Value the lines we actually put on the PO, the same way the Fristads price check does.
  const goodsNet = Number(po.netValue != null ? po.netValue
    : [...(po.soLines || []), ...(po.lowLines || [])].reduce((a, l) => a + (Number(l.cost) || 0) * (Number(l.qty) || 0), 0)) || 0;
  if (live && carriageNet > 0 && freeOver > 0 && goodsNet > 0 && goodsNet < freeOver) {
    try {
      const c = await bp.addPoMiscRowLive({ poId, name: `Carriage (order under £${freeOver} ex-VAT)`, net: carriageNet, qty: 1, execute: true });
      steps.carriage = (c && c.refused)
        ? { added: false, reason: c.reason, net: carriageNet }
        : { added: true, net: carriageNet, goodsNet, freeOver };
    } catch (e) {
      steps.carriage = { added: false, error: e.message, net: carriageNet };
      await logPurchasingError(pool, {
        supplier: supplierKey, step: 'carriage', severity: 'review',
        message: `Could not add the £${carriageNet} carriage line to PO#${poId} (goods £${goodsNet.toFixed(2)}, free over £${freeOver}): ${e.message}. The order was still placed — the PO will read £${carriageNet} light against ${cfg.label}'s invoice.`,
        context: { poId, goodsNet, carriage: carriageNet },
      }).catch(() => {});
    }
  } else if (carriageNet > 0 && freeOver > 0 && goodsNet >= freeOver) {
    steps.carriage = { added: false, reason: `£${goodsNet.toFixed(2)} is over the £${freeOver} free-carriage threshold`, goodsNet };
  } else if (!(carriageNet > 0)) {
    steps.carriage = { added: false, reason: 'no carriage charge configured for this supplier' };
  } else {
    // The case that hid the bug for as long as it existed: a supplier that HAS a carriage charge,
    // under a threshold, and we could not value the goods — so none of the branches above fit and
    // the step silently did not happen. steps.carriage stayed undefined and the run report looked
    // complete. Never let this be silent again: it is money the PO will be light by.
    steps.carriage = { added: false, reason: `could not value the goods (goodsNet ${goodsNet}) — carriage NOT added`, goodsNet };
    await logPurchasingError(pool, {
      supplier: supplierKey, step: 'carriage', severity: 'error',
      message: `Could not value PO#${poId}'s goods, so the £${carriageNet} carriage line was NOT added (free over £${freeOver}). `
        + `The order was placed and the PO will read £${carriageNet} light against ${cfg.label}'s invoice.`,
      context: { poId, goodsNet, carriage: carriageNet, freeOver },
    }).catch(() => {});
  }

  const mail = await emailOrderDocument(poId, { contactId: cfg.contactId, to, send: live });
  if (!mail.sent) throw stepErr('email', `Brightpearl did not confirm emailing PO#${poId} to ${to}: ${JSON.stringify(mail).slice(0, 200)}`);
  steps.email = { to, sent: true, status: mail.status };

  await bp.setOrderStatusLive(poId, bp.PLACED_WITH_SUPPLIER_STATUS);
  await bp.addOrderNoteLive(poId, `PO emailed to ${cfg.label} (${to}).`
    + (steps.carriage && steps.carriage.added ? ` Carriage £${carriageNet} added — order under £${freeOver} ex-VAT.` : ''), cfg.contactId).catch(() => {});
  steps.link = { status: 7, emailedTo: to };

  if (soIds.length) { try { steps.finalize = await bp.finalizeSupplierTagsLive({ orderIds: soIds, supplierKey, poId, noteContactId: cfg.contactId, setOrderedStatus: true, linesByOrder, execute: live }); } catch (e) { throw stepErr('finalize', `PO emailed + placed, but finalising SOs failed: ${e.message}`); } }
  return { poId, emailedTo: to, steps };
}

const placeV12Order = (pool, altItemsUrl, opts) => placeEmailSupplierOrder('V12', pool, altItemsUrl, opts);
const placeBucklerOrder = (pool, altItemsUrl, opts) => placeEmailSupplierOrder('BUCKLER', pool, altItemsUrl, opts);
const placeHellbergOrder = (pool, altItemsUrl, opts) => placeEmailSupplierOrder('HELLBERG', pool, altItemsUrl, opts);


// ── Scruffs placement chain (email supplier) ─────────────────────────────────
// Same shape as Uneek: Brightpearl builds and emails its own PO PDF, so there is no portal or API
// to go wrong. First order placed by hand 2026-08-20 (PO 483634, £108.70) to prove the address.
// Carriage minimum is £100 ex-VAT — a £90 order was seen carrying carriage, so treat it as real.
// poField is the SHARED PCF_STOCKPO ("Any Other Suppliers"), which Engel also writes to; that field
// is the dedupe guard, so until PCF_SCRUFFSPO exists an Engel-stamped SO looks already-ordered here.
const SCRUFFS_SUPPLIER_CONTACT = 130243;
const SCRUFFS_ORDER_EMAIL = process.env.SCRUFFS_ORDER_EMAIL || 'salesorders@scruffs.com';

async function placeScruffsOrder(pool, altItemsUrl, { padToThreshold = 0 } = {}) {
  const steps = {};
  // 1. combined PO (SO demand + low-inv; stamps the SOs)
  let po;
  try { po = await createPo({ supplierKey: 'SCRUFFS', execute: true, padToThreshold, logPool: pool }); }
  catch (e) { throw createPoErr(e); }
  if (!po.created) throw stepErr('create-po', `no PO created: ${po.reason || 'unknown'}` + (po.unresolvedSkus && po.unresolvedSkus.length ? ` — item codes not found in Brightpearl: ${po.unresolvedSkus.join(', ')}` : ''));
  const poId = po.poId;
  const soIds = [...new Set((po.soLines || []).map((l) => l.order).filter(Boolean))];
  const linesByOrder = {};
  for (const l of (po.soLines || [])) { if (l.order) (linesByOrder[l.order] = linesByOrder[l.order] || []).push({ sku: l.sku, qty: l.qty, name: l.name, productId: l.productId }); }
  steps.po = { poId, soUnits: po.soUnits, lowUnits: po.lowUnits, soIds, skippedBundles: po.skippedBundles || [] };

  // 2. EMAIL Brightpearl's real PO PDF to the Scruffs order desk. Only email_to_0 is set —
  // emailOrderDocument clears BP's pre-filled rows, which for this contact include
  // "SalesOrders@Scruffs.com / CS@scruffs.com" stored as ONE address and two of our own
  // sales@tuffshop.co.uk rows. Sending those unedited would bounce and CC ourselves.
  const mail = await emailOrderDocument(poId, { contactId: SCRUFFS_SUPPLIER_CONTACT, to: SCRUFFS_ORDER_EMAIL, subject: `Purchase Order: #${poId}`, send: true });
  if (!mail.sent) throw stepErr('email', `Brightpearl did not confirm emailing PO#${poId} to ${SCRUFFS_ORDER_EMAIL}: ${JSON.stringify(mail).slice(0, 200)}`);
  steps.email = { to: SCRUFFS_ORDER_EMAIL, sent: true, status: mail.status };

  // 3. mark the PO Placed (status 7) + a note recording the email.
  await bp.setOrderStatusLive(poId, bp.PLACED_WITH_SUPPLIER_STATUS);
  await bp.addOrderNoteLive(poId, `PO emailed to Scruffs (${SCRUFFS_ORDER_EMAIL}).`, SCRUFFS_SUPPLIER_CONTACT).catch(() => {});
  steps.link = { status: 7, emailedTo: SCRUFFS_ORDER_EMAIL };

  // 4. finalise the contributing SOs (clear the Scruffs tag, status → 22, "ordered via PO#" note)
  if (soIds.length) { try { steps.finalize = await bp.finalizeSupplierTagsLive({ orderIds: soIds, supplierKey: 'SCRUFFS', poId, noteContactId: SCRUFFS_SUPPLIER_CONTACT, setOrderedStatus: true, linesByOrder, execute: true }); } catch (e) { throw stepErr('finalize', `PO emailed + placed, but finalising SOs failed: ${e.message}`); } }
  return { poId, emailedTo: SCRUFFS_ORDER_EMAIL, steps };
}

// ── Performance Brands placement chain (WooCommerce trade shop) ──────────────
// Alt-Items resolves each of our SKUs against the LIVE variation grid, loads the basket and checks
// out on the b2b_credit_limit trade CREDIT account (no card is ever involved). Our PO number goes in
// their required `po_field` — the site refuses the order outright without it.
// First order placed by hand 2026-08-21 (#24454, £416.73) to prove the flow before automating.
// Free delivery at £200 + VAT; under that it is £7.00 flat.
const PERFORMANCE_BRANDS_SUPPLIER_CONTACT = 11611;

// One line per SKU, quantities summed. SHARED — used by Performance Brands and Castle. Put it
// here rather than inline so there is one implementation to test and no fourth copy to miss.
//
// A PO can carry the same SKU on two rows (two sales orders wanting it, or a customer line and a
// reorder line). Supplier baskets do not reliably accumulate repeat adds of one variation: some
// keep the LAST quantity rather than the sum, so sending it twice silently drops units.
//
// On 2026-09-09 the Performance Brands demand carried PB271-BRN-06 twice and went out as two
// qty-1 requests. Both came back ok, and the second one's own notice still read "1 × Brown, 06".
// Six adds produced a five-unit cart, the cart check refused to place a short order, and PO 488064
// was orphaned — which then suppressed the reorder demand below the £200 threshold and talked the
// retry into waiting. All from sending one variation twice instead of once with qty 2.
//
// This is the fourth place the same shape has appeared. Sterling merges byVariant with the note
// that "the shop's basket merges duplicate adds and keeps the LAST qty, not the sum"; Snickers
// sums into bySku; Performance Brands and Castle now use this.
//
// lowInv is AND-ed, so a SKU wanted by both a customer order and the reorder merges as lowInv
// false. That direction matters: an unorderable lowInv line is allowed to drop out quietly, while
// a customer line stops the run. Merging the other way would silently not buy something a customer
// is waiting for.
export function mergePoLinesBySku(po) {
  const bySku = new Map();
  for (const l of [
    ...((po && po.soLines) || []).map((x) => ({ ...x, lowInv: false })),
    ...((po && po.lowLines) || []).map((x) => ({ ...x, lowInv: true })),
  ]) {
    if (String(l.productId) === '1000' || !l.sku) continue;   // the =====LOW INV==== separator
    const k = String(l.sku);
    const cur = bySku.get(k);
    if (!cur) { bySku.set(k, { sku: k, qty: Math.round(l.qty), cost: l.cost, name: l.name, lowInv: !!l.lowInv, productId: l.productId }); continue; }
    cur.qty += Math.round(l.qty);
    cur.lowInv = cur.lowInv && !!l.lowInv;
    if (!cur.name) cur.name = l.name;
    if (cur.cost == null) cur.cost = l.cost;
  }
  return [...bySku.values()];
}

async function placePerformanceBrandsOrder(pool, altItemsUrl, { padToThreshold = 0, live = true } = {}) {
  const steps = {};
  let po;
  try { po = await createPo({ supplierKey: 'PERFORMANCE BRANDS', execute: live, padToThreshold, logPool: pool }); }
  catch (e) { throw createPoErr(e); }
  if (!po.created) throw stepErr('create-po', `no PO created: ${po.reason || 'unknown'}` + (po.unresolvedSkus && po.unresolvedSkus.length ? ` — item codes not found in Brightpearl: ${po.unresolvedSkus.join(', ')}` : ''));
  const poId = po.poId;
  const soIds = [...new Set((po.soLines || []).map((l) => l.order).filter(Boolean))];
  const linesByOrder = {};
  for (const l of (po.soLines || [])) { if (l.order) (linesByOrder[l.order] = linesByOrder[l.order] || []).push({ sku: l.sku, qty: l.qty, name: l.name, productId: l.productId }); }
  steps.po = { poId, soUnits: po.soUnits, lowUnits: po.lowUnits, soIds, skippedBundles: po.skippedBundles || [] };

  // name is sent as well as sku because a few of our products carry a Brightpearl-internal SKU with
  // no supplier code in it (191339 is the Y-Shield H3) and can only be found by the style code in
  // the NAME. cost is sent so Alt-Items can report where the supplier's live price disagrees with
  // our Launch cost — PB56C was £43.05 against £39.50 when this was built.
  const orderLines = mergePoLinesBySku(po);
  if (!orderLines.length) throw stepErr('cart', 'no orderable Performance Brands lines');
  steps.lines = { count: orderLines.length, units: orderLines.reduce((a, l) => a + l.qty, 0) };

  // RESOLVE HERE, ADD IN THE BROWSER. The old path posted everything to
  // /api/performance-brands-order, which resolved AND added over plain HTTP — and the adding half
  // stopped working: on 2026-09-02 it reported "basket has 0 line(s), expected 3" and nothing was
  // ordered. Their grid is a WooCommerce plugin that builds the add-to-cart request in JavaScript,
  // so a server-side POST can look accepted and put nothing in the basket. Order 24821 (£307.50)
  // went through the browser worker by hand on 2026-09-03; this wires the scheduled run onto that
  // same proven route.
  //
  // The RESOLVER is untouched and still Alt-Items' — it is healthy, it knows the style/colour/size
  // rules and the Brightpearl-internal SKUs like 191339, and the worker deliberately does not
  // resolve anything. Only the add-to-basket step moves.
  const resolved = [], failedLines = [];
  for (const l of orderLines) {
    let rr = null;
    try {
      rr = await jfetch('resolve', `${altItemsUrl}/api/performance-brands-resolve?sku=${encodeURIComponent(l.sku)}&name=${encodeURIComponent(l.name || '')}`, {});
    } catch (e) { rr = { ok: false, sku: l.sku, reason: `resolve call failed: ${e.message}` }; }
    if (rr && rr.ok && rr.url && rr.pid) resolved.push({ ...l, url: rr.url, pid: rr.pid, sitePrice: rr.price, maxQty: rr.maxQty });
    else failedLines.push({ sku: l.sku, reason: (rr && rr.reason) || 'could not resolve', outOfStock: !!(rr && rr.outOfStock), lowInv: !!l.lowInv });
  }
  // Two SO lines can resolve to the SAME variation (two orders both wanting PB271-BRN-06). The
  // worker types one quantity into one box per pid per page visit — sending the same pid twice
  // overwrites the box instead of summing it, so the second line is reported as added but never
  // actually reaches the basket (error 212, 2026-09-09: added 6/expected 6, but only 5 units in
  // cart). Same fix as Sterling's byVariant merge: one resolved line per pid, qty summed, before
  // the worker ever sees it.
  const byPid = new Map();
  for (const l of resolved) {
    const cur = byPid.get(l.pid);
    if (cur) cur.qty += l.qty; else byPid.set(l.pid, { ...l });
  }
  resolved.length = 0;
  resolved.push(...byPid.values());
  steps.resolve = { asked: orderLines.length, resolved: resolved.length, failed: failedLines.length };

  // A LOW-INVENTORY line that cannot be ordered is dropped rather than aborting the run; a line a
  // CUSTOMER is waiting for stops it. Same rule the old path applied, kept deliberately.
  const droppedLowInv = failedLines.filter((f) => f.lowInv).map((f) => ({ sku: f.sku, reason: f.reason, qty: (orderLines.find((l) => l.sku === f.sku) || {}).qty }));
  const blocking = failedLines.filter((f) => !f.lowInv);
  const r = { failed: blocking, droppedLowInv, ok: false, priceWarns: [] };

  if (!blocking.length) {
    if (!resolved.length) throw stepErr('resolve', `no Performance Brands lines could be resolved — PO#${poId} left for review`, { poId, failed: failedLines });
    // ── PACK MINIMUMS ───────────────────────────────────────────────────────────────────────────
    // Performance Brands sell some lines in fixed multiples only. Unlike Hultafors they REFUSE the
    // line and say why ("supplier minimum is 20 (in steps of 20), this line wants 5") rather than
    // dropping it in silence — but a refused line still fails the basket check and strands the
    // whole order, which is what PO 489405 did on 2026-09-16 over two helmet lines.
    //
    // Round UP to the multiple, exactly as the Snickers path does. Ordering 20 of something we
    // wanted 5 of is a stock decision; leaving seven other lines unordered is not one at all.
    const pbPacks = performanceBrandsPackMultiples();
    const packApplied = [];
    for (const l of resolved) {
      const p = Number(pbPacks[String(l.sku).toUpperCase()]);
      if (p > 1 && l.qty % p !== 0) {
        const q = Math.ceil(l.qty / p) * p;
        packApplied.push({ sku: l.sku, demand: l.qty, ordered: q, packOf: p });
        l.qty = q;
      }
    }
    if (packApplied.length) steps.packRounding = packApplied;
    const wr = await workerPlaceOrder({
      supplier: 'PERFORMANCE BRANDS', ref: poId, execute: live,
      lines: resolved.map((l) => ({ sku: l.sku, url: l.url, pid: l.pid, qty: l.qty })),
    });
    // The worker verifies the basket against what it was asked for and refuses a short one, so a
    // not-ok result here means nothing was submitted — never a maybe.
    if (!wr || !wr.placed) {
      const miss = (wr && wr.results ? wr.results.filter((x) => !x.ok) : []);
      throw stepErr('checkout', `Performance Brands worker did not confirm placement`
        + (miss.length ? ` — ${miss.length} line(s) refused: ${miss.map((x) => `${x.sku}: ${x.reason || '?'}`).join('; ').slice(0, 300)}` : '')
        + `: ${JSON.stringify({ added: wr && wr.added, expected: wr && wr.expected, cartCount: wr && wr.cartCount, units: wr && wr.units, wantUnits: wr && wr.wantUnits, ready: wr && wr.ready, error: wr && wr.error }).slice(0, 250)}`,
        { poId, results: (wr && wr.results) || null, added: wr && wr.added, expected: wr && wr.expected });
    }
    r.ok = true; r.orderNo = wr.orderNo || null; r.total = wr.total ?? null;
    // the site price came back from the RESOLVE step, so a stale cost is still visible without the
    // old endpoint doing the comparison
    r.priceWarns = resolved
      .filter((l) => Number(l.sitePrice) > 0 && Number(l.cost) > 0 && Math.abs(Number(l.sitePrice) - Number(l.cost)) > 0.02)
      .map((l) => ({ sku: l.sku, bpCost: Number(l.cost), sitePrice: Number(l.sitePrice) }));
  }

  // A line we could not resolve stops the run naming the SKU and the reason, rather than quietly
  // ordering a short basket. Out-of-stock is called out separately because this site offers NO
  // back-order route at all (its out-of-stock cells have no quantity input), so those lines need a
  // human to chase the supplier rather than a retry.
  if (r.failed && r.failed.length) {
    const oos = r.failed.filter((f) => f.outOfStock).map((f) => f.sku);
    throw stepErr('resolve', `${r.failed.length} line(s) could not be ordered — NOT ordered, PO#${poId} left for review: `
      + r.failed.map((f) => `${f.sku}: ${f.reason}`).join(' | ').slice(0, 400)
      + (oos.length ? ` — ${oos.length} of these are OUT OF STOCK and cannot be back-ordered on this site; email sales@performance-brands.com` : ''),
      { poId, failed: r.failed });
  }
  if (!r.ok) throw stepErr(r.step || 'checkout', `Performance Brands did not confirm the order: ${String(r.error || JSON.stringify(r)).slice(0, 300)}`, { poId });

  const orderNo = r.orderNo || null;
  // priceWarns makes a stale cost list visible in the run report instead of only on the invoice.
  steps.checkout = { ok: true, orderNo, total: r.total, expectNet: r.expectNet, priceWarns: r.priceWarns || [] };

  // Feed those differences into the SAME healer every other supplier uses, rather than leaving them
  // as a line in a report nobody reads. Performance Brands quote NET on the product grid — verified
  // line-by-line against order #24454, where PB94C/PB278/PB271 matched the PO to the penny — so a
  // difference here is a genuinely wrong cost, not a discount, and is safe to heal.
  // (Contrast Mascot and the Elastic suppliers, whose portals quote LIST: a "mismatch" there is the
  // discount and must never be healed.)
  await healPrices(steps, {
    supplierKey: 'PERFORMANCE BRANDS', poId, pool,
    changes: (r.priceWarns || []).filter((w) => w.sku && Number(w.sitePrice) > 0).map((w) => ({ sku: w.sku, was: w.bpCost, now: w.sitePrice })),
  });

  // A LOW-INVENTORY line that could not be ordered is dropped rather than allowed to abort the run
  // — see the reasoning in performanceBrands.js — but it must never disappear quietly. Log it for
  // review so a top-up that keeps failing gets noticed instead of silently never arriving.
  const dropped = r.droppedLowInv || [];
  if (dropped.length) {
    steps.droppedLowInv = dropped;
    // Take them OFF the PO. Logging alone left the PO claiming stock that was never bought: PO
    // 489129 (2026-09-14) went out at £156.76 while Performance Brands order 25099 covered only
    // £107.98, because 14 units of H1C hard hats stayed on it after being dropped from the basket.
    // Those rows also count as ON ORDER, so the very items that failed to order are then suppressed
    // from the next reorder — the same trap as the 4004 on PO 486597 and the HH row on 485410.
    const removedRows = [];
    for (const d of dropped) {
      try { const rm = await bp.removePoRowLive({ poId, sku: d.sku, execute: live }); removedRows.push({ sku: d.sku, ok: !!(rm && rm.done) }); }
      catch (e) { removedRows.push({ sku: d.sku, ok: false, error: e.message }); }
    }
    steps.droppedRowsRemoved = removedRows;
    await logPurchasingError(pool, {
      supplier: 'PERFORMANCE BRANDS', step: 'low-inv-dropped', severity: 'review',
      message: `${dropped.length} low-inventory line(s) could NOT be ordered and were taken OFF PO#${poId} `
        + `(${removedRows.filter((x) => x.ok).length} of ${removedRows.length} rows removed). `
        + `Customer demand was unaffected and the order was placed. This supplier has no back-order route, `
        + `so these need stock or an email to sales@performance-brands.com:\n`
        + dropped.map((d) => `      ${d.qty} × ${d.sku} — ${d.reason}`).join('\n'),
      context: { poId, dropped, removedRows },
    }).catch(() => {});
  }

  const ref = orderNo || `Placed-${poId}`;
  await bp.setOrderStatusLive(poId, bp.PLACED_WITH_SUPPLIER_STATUS);
  let refWritten = false;
  try { await bp.setOrderReferenceLive(poId, ref); refWritten = true; }
  catch (e) { steps.linkWarn = `reference-set failed (non-fatal): ${e.message}`; await bp.addOrderNoteLive(poId, `Placed with Performance Brands — order ${ref}. Reference-set failed: ${e.message}`, PERFORMANCE_BRANDS_SUPPLIER_CONTACT).catch(() => {}); }
  steps.link = { reference: ref, refWritten, orderNo, status: 7 };

  if (soIds.length) { try { steps.finalize = await bp.finalizeSupplierTagsLive({ orderIds: soIds, supplierKey: 'PERFORMANCE BRANDS', poId, noteContactId: PERFORMANCE_BRANDS_SUPPLIER_CONTACT, setOrderedStatus: true, linesByOrder, execute: live }); } catch (e) { throw stepErr('finalize', `order placed + PO linked, but finalising SOs failed: ${e.message}`); } }
  return { poId, orderNo, steps };
}

// ── Mascot placement chain (b2b.mascot.dk, ASP.NET portal) ──────────────────
// Alt-Items fills the basket, then runs Mascot's TWO-STAGE commit: /Sap/CreateOrder creates the SAP
// order (the portal's "Check Discount" button) and /Sap/ReleaseOrder commits it (the "Release"
// button). Our PO number goes in DealerRequisitionNumber — not RequisitionNumber, which is what the
// original scope wrongly said.
// First order placed by hand 2026-08-21: SAP 0006520340 for PO 483781, 11 units.
// Free-carriage threshold £250.
//
// Mascot's basket shows LIST prices — they ran exactly 1.695x our Launch cost on every line of that
// first order (£986.45 list = £581.98 to us). So the run report deliberately records the list value
// WITHOUT comparing it to the PO: a "mismatch" there is the discount, not an error.
const MASCOT_SUPPLIER_CONTACT = 334;

async function placeMascotOrder(pool, altItemsUrl, { padToThreshold = 0, live = true } = {}) {
  const steps = {};
  let po;
  try { po = await createPo({ supplierKey: 'MASCOT', execute: live, padToThreshold, logPool: pool }); }
  catch (e) { throw createPoErr(e); }
  if (!po.created) throw stepErr('create-po', `no PO created: ${po.reason || 'unknown'}` + (po.unresolvedSkus && po.unresolvedSkus.length ? ` — item codes not found in Brightpearl: ${po.unresolvedSkus.join(', ')}` : ''));
  const poId = po.poId;
  const soIds = [...new Set((po.soLines || []).map((l) => l.order).filter(Boolean))];
  const linesByOrder = {};
  for (const l of (po.soLines || [])) { if (l.order) (linesByOrder[l.order] = linesByOrder[l.order] || []).push({ sku: l.sku, qty: l.qty, name: l.name, productId: l.productId }); }
  steps.po = { poId, soUnits: po.soUnits, lowUnits: po.lowUnits, soIds, skippedBundles: po.skippedBundles || [] };

  // name is sent because the EAN resolver narrows the catalogue by the style code parsed out of the
  // product NAME before matching our EAN against the size rows.
  const orderLines = [...(po.soLines || []), ...(po.lowLines || [])]
    .filter((l) => String(l.productId) !== '1000' && l.sku)
    .map((l) => ({ sku: String(l.sku), qty: Math.round(l.qty), cost: l.cost, name: l.name }));
  if (!orderLines.length) throw stepErr('cart', 'no orderable Mascot lines');
  steps.lines = { count: orderLines.length, units: orderLines.reduce((a, l) => a + l.qty, 0) };

  const r = await jfetch('checkout', `${altItemsUrl}/api/mascot-order`, {
    method: 'POST', headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ lines: orderLines, purchaseOrder: String(poId), place: live }),
  });

  // A release failure is the one outcome that must never be retried: CreateOrder has already left a
  // draft at Mascot that Brightpearl cannot see, so a re-run would order everything twice. Surface
  // the SAP number in the message so a human can release or cancel that exact draft.
  if (r.needsHuman || r.step === 'release') {
    throw stepErr('release', `${r.error || 'ReleaseOrder failed'} — PO#${poId} left for review. DO NOT re-run Mascot until SAP ${r.sapNumber || '(unknown)'} is released or cancelled in the portal.`, { poId, sapNumber: r.sapNumber, needsHuman: true });
  }
  if (!r.ok) throw stepErr(r.step || 'checkout', `Mascot did not confirm the order: ${String(r.error || JSON.stringify(r)).slice(0, 300)}`, { poId, basket: r.basket });

  const sapNumber = r.sapNumber || null;
  steps.checkout = { ok: true, sapNumber, units: r.units, listValue: r.listValue, lineCount: r.lineCount };

  const ref = sapNumber || `Placed-${poId}`;
  await bp.setOrderStatusLive(poId, bp.PLACED_WITH_SUPPLIER_STATUS);
  let refWritten = false;
  try { await bp.setOrderReferenceLive(poId, ref); refWritten = true; }
  catch (e) { steps.linkWarn = `reference-set failed (non-fatal): ${e.message}`; await bp.addOrderNoteLive(poId, `Placed with Mascot — SAP order ${ref}. Reference-set failed: ${e.message}`, MASCOT_SUPPLIER_CONTACT).catch(() => {}); }
  steps.link = { reference: ref, refWritten, sapNumber, status: 7 };

  if (soIds.length) { try { steps.finalize = await bp.finalizeSupplierTagsLive({ orderIds: soIds, supplierKey: 'MASCOT', poId, noteContactId: MASCOT_SUPPLIER_CONTACT, setOrderedStatus: true, linesByOrder, execute: live }); } catch (e) { throw stepErr('finalize', `order placed + PO linked, but finalising SOs failed: ${e.message}`); } }
  return { poId, orderNo: sapNumber, steps };
}

// ── Chadwick placement chain (portal.chadwicktextiles.co.uk) ────────────────
// Alt-Items uploads the lines (POST /qx/wcp-ordupload with json=[{Item,Quantity}] — their
// "spreadsheet upload" is parsed in the browser, so no file is ever built) and then places the
// order with POST /qx/wcp-cartorder, whose response body IS the Chadwick order number.
// Our PO number goes in `pono`.
// NOT yet proven on a real order — the supplied HAR loaded the cart but never checked out, so the
// checkout shape was read out of my-basket.php's own jQuery. Treat the first live run as the test.
const CHADWICK_SUPPLIER_CONTACT = 42485;

async function placeChadwickOrder(pool, altItemsUrl, { padToThreshold = 0, live = true } = {}) {
  const steps = {};
  let po;
  try { po = await createPo({ supplierKey: 'CHADWICK', execute: live, padToThreshold, logPool: pool }); }
  catch (e) { throw createPoErr(e); }
  if (!po.created) throw stepErr('create-po', `no PO created: ${po.reason || 'unknown'}` + (po.unresolvedSkus && po.unresolvedSkus.length ? ` — item codes not found in Brightpearl: ${po.unresolvedSkus.join(', ')}` : ''));
  const poId = po.poId;
  const soIds = [...new Set((po.soLines || []).map((l) => l.order).filter(Boolean))];
  const linesByOrder = {};
  for (const l of (po.soLines || [])) { if (l.order) (linesByOrder[l.order] = linesByOrder[l.order] || []).push({ sku: l.sku, qty: l.qty, name: l.name, productId: l.productId }); }
  steps.po = { poId, soUnits: po.soUnits, lowUnits: po.lowUnits, soIds, skippedBundles: po.skippedBundles || [] };

  // Chadwick key on their ITEM CODE, which most of our SKUs already are (882-01-A-L). Some products
  // carry a Brightpearl-internal code instead (ML070622072) which their upload will silently drop —
  // the basket line-count check in Alt-Items catches that and refuses rather than ordering short.
  // One line per SKU, same as Performance Brands and Castle. Chadwick's cart MERGES duplicate
  // adds, and the basket check compares LINE COUNTS — so two demand rows for one SKU are sent as
  // two lines, come back as one cart row, and the run refuses a basket that is actually complete.
  // 2026-09-10, PO 488281: 17 lines sent, 15 rows in the cart, and Chadwick's own reply said
  // TotalItems 17 — the order was right and the count was not. Brightpearl had already
  // consolidated the same duplicates into 15 PO rows, which is why the PO and the cart agreed with
  // each other and only our line count disagreed with both.
  // lowInv rides along so Alt-Items can tell the two apart when Chadwick refuses a code by name:
  // dropping a REORDER line to save the rest of the batch is a fair trade, dropping a line someone
  // is waiting for is not, and only the caller knows which is which.
  const orderLines = mergePoLinesBySku(po).map((l) => ({ sku: l.sku, qty: l.qty, cost: l.cost, name: l.name, lowInv: !!l.lowInv, productId: l.productId }));
  if (!orderLines.length) throw stepErr('cart', 'no orderable Chadwick lines');
  steps.lines = { count: orderLines.length, units: orderLines.reduce((a, l) => a + l.qty, 0) };

  // A handful of products carry a Brightpearl-internal SKU (ML110722012) instead of Chadwick's own
  // item code, and their upload silently drops those. Two places can hold the real code:
  //   - the PO's OWN row can already carry it (873-39/39-A-L for TB150922148 on PO 489373 —
  //     confirmed live against Chadwick's stock feed) even though the product's identity.sku is
  //     still the internal code. It is unclear how it gets there — a per-row correction, not a
  //     product-record edit — but it is the freshest, most specific thing we can read, so it wins
  //     when it differs from the plain SKU.
  //   - otherwise the product's `mpn` field, when set (837-39-A-3XL for ML110722012 — also
  //     confirmed live).
  // Fall back to the SKU when neither is set; a product with none of the three is a genuine data
  // gap and will still be refused and reported as before.
  let poRowSkuByProductId = {};
  try {
    const liveOrder = (await bp.bpLiveGet(`/order-service/order/${poId}`))[0];
    for (const r of Object.values((liveOrder && liveOrder.orderRows) || {})) {
      if (r.productId != null) poRowSkuByProductId[String(r.productId)] = r.productSku;
    }
  } catch { /* fall back to mpn/sku only */ }
  for (const l of orderLines) {
    l.itemCode = l.sku;
    if (!l.productId) continue;
    const rowSku = poRowSkuByProductId[String(l.productId)];
    if (rowSku && String(rowSku).trim() && String(rowSku).trim().toUpperCase() !== String(l.sku).toUpperCase()) {
      l.itemCode = String(rowSku).trim();
      continue;
    }
    try {
      const identity = await bp.getProductIdentityLive(l.productId);
      if (identity.mpn && String(identity.mpn).trim()) l.itemCode = String(identity.mpn).trim();
    } catch { /* fall back to sku */ }
  }

  // ── TWO PRODUCTS MUST NEVER RESOLVE TO ONE ITEM CODE ────────────────────────────────────────
  // Everything above rewrites a line's code from data that can be wrong. Brightpearl product
  // 253317 (CT 835 Impact Rugby Shorts YOUTH XL, sku 835-39-Y-XL) carried mpn 835-39-A-XL — the
  // ADULT code — so the youth line and the adult line both went out as 835-39-A-XL. Chadwick did
  // the only sensible thing and summed them: one row of 7 against demand for 4 adult and 3 youth.
  // Nothing was short, so a line COUNT saw nothing wrong; only reading the basket back caught it.
  //
  // Catch it here instead. Two different products sharing one code is always a data fault on our
  // side, it is knowable before a single request reaches the supplier, and letting it through
  // spends a whole basket load to learn what this comparison already knows.
  const byCode = new Map();
  for (const l of orderLines) {
    const k = String(l.itemCode || '').toUpperCase();
    if (!k) continue;
    if (!byCode.has(k)) byCode.set(k, []);
    byCode.get(k).push(l);
  }
  const collisions = [...byCode.entries()]
    .filter(([, ls]) => new Set(ls.map((l) => String(l.productId))).size > 1)
    .map(([code, ls]) => ({ code, lines: ls.map((l) => ({ productId: l.productId, sku: l.sku, qty: l.qty, name: l.name })) }));
  if (collisions.length) {
    throw stepErr('resolve', `${collisions.length} Chadwick item code(s) are shared by more than one product — NOT ordering, `
      + `the supplier would merge them and deliver the wrong goods: `
      + collisions.map((c) => `${c.code} ← ` + c.lines.map((l) => `${l.qty} × ${l.sku} (product ${l.productId})`).join(' + ')).join('; ')
      + `. Fix the mpn on the product whose own SKU is not that code.`,
      { poId, collisions });
  }

  const r = await jfetch('checkout', `${altItemsUrl}/api/chadwick-order`, {
    method: 'POST', headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ lines: orderLines.map((l) => ({ sku: l.itemCode, qty: l.qty, lowInv: l.lowInv })), purchaseOrder: String(poId), place: live }),
  });
  if (!r.ok) {
    const miss = (r.missing && r.missing.length) ? ` — item codes Chadwick did not accept: ${r.missing.join(', ').slice(0, 200)}` : '';
    const rej = (r.rejected && r.rejected.length) ? ` — Chadwick rejected by name: ${r.rejected.join(', ')}` : '';
    throw stepErr(r.step || 'checkout', `Chadwick did not confirm the order: ${String(r.error || JSON.stringify(r)).slice(0, 250)}${miss}${rej}`, { poId, missing: r.missing, rejected: r.rejected || [] });
  }

  // Reorder lines Chadwick refused by name were dropped so the rest of the batch could load. The
  // order stands and no customer is waiting on them — but they must not disappear quietly, because
  // a code that is wrong stays wrong and will fail again every day until someone fixes it in
  // Brightpearl. That is the whole lesson of PO 488574: one stray "CT" cost four good lines, and
  // was only found because a person went looking.
  const rejected = r.rejected || [];
  if (rejected.length) {
    steps.rejected = rejected;
    const dropped = orderLines.filter((l) => rejected.some((x) => String(x).toUpperCase() === String(l.itemCode).toUpperCase()));
    for (const d of dropped) {
      try { const rm = await bp.removePoRowLive({ poId, sku: d.sku, execute: live }); steps.rejectedRowsRemoved = [...(steps.rejectedRowsRemoved || []), { sku: d.sku, ok: !!(rm && rm.done) }]; }
      catch (e) { steps.rejectedRowsRemoved = [...(steps.rejectedRowsRemoved || []), { sku: d.sku, ok: false, error: e.message }]; }
    }
    await logPurchasingError(pool, {
      supplier: 'CHADWICK', step: 'item-code-rejected', severity: 'error',
      message: `Chadwick rejected ${rejected.length} item code(s) outright and they were taken OFF PO#${poId} so the rest of the order could be placed. `
        + `Their upload aborts at the first bad code and discards every line after it, so leaving them in loses good lines too. `
        + `Each of these is almost certainly a WRONG SKU in Brightpearl rather than a dead product — check it against their catalogue:\n`
        + dropped.map((d) => `      ${d.qty} × ${d.sku} ${d.name || ''}`).join('\n'),
      context: { poId, rejected, dropped: dropped.map((d) => ({ sku: d.sku, qty: d.qty, name: d.name })) },
    }).catch(() => {});
  }

  // NEVER let a non-string reach the reference. Brightpearl stores whatever it is given, and an
  // object arrives as the literal "[object Object]" — which is what PO 487454 carried on
  // 2026-09-07 instead of SG215761. A reference that names nothing is worse than an obviously
  // absent one: it looks filled in, so nobody checks it.
  const rawOrderNo = r.orderNo;
  const orderNo = (typeof rawOrderNo === 'string' || typeof rawOrderNo === 'number')
    ? String(rawOrderNo).trim() || null
    : null;
  if (rawOrderNo != null && orderNo == null) {
    await logPurchasingError(pool, {
      supplier: 'CHADWICK', step: 'checkout', severity: 'review',
      message: `Chadwick placed the order but did not return a usable order number (got ${typeof rawOrderNo}). `
        + `PO#${poId} is marked placed with a fallback reference — find the real number on their order list `
        + `by matching CustomerPO ${poId} (wcp-orders) and set it by hand.`,
      context: { poId, rawOrderNo: JSON.stringify(rawOrderNo).slice(0, 300), rid: r.rid },
    }).catch(() => {});
  }
  steps.checkout = { ok: true, orderNo, cartCount: r.cartCount, rid: r.rid };

  // PRICE CHECK against what Chadwick will ACTUALLY invoice. Chadwick was the only automated
  // supplier with no price check at all, so a cost drift went into the accounts silently every
  // time: on 2026-09-07 PO 487454 read £412.40 against their £405.31 and nothing flagged it — the
  // gap was spotted by eye. Every other supplier logs this, which is how Fristads' £12 and
  // Snickers' £2.81 surfaced the same morning.
  //
  // Their order list is the authority (it is what they will bill), and their price CSV names the
  // line that moved. Purely diagnostic — the order is already placed and nothing here can unplace
  // it, so a failure to read prices must never look like a failed order.
  let landed = null;
  try {
    // Same basis every other supplier's check uses: the lines we asked for at our costs.
    const poNet = +[...(po.soLines || []), ...(po.lowLines || [])].reduce((a, l) => a + (l.cost || 0) * l.qty, 0).toFixed(2);
    const skus = [...new Set(orderLines.map((l) => l.itemCode).filter(Boolean))];
    landed = await jfetch('price-check', `${altItemsUrl}/api/chadwick-order-lookup?pono=${encodeURIComponent(poId)}&skus=${encodeURIComponent(skus.join(','))}`, {});
    if (landed && landed.found && landed.value != null && poNet != null) {
      const gap = +(landed.value - poNet).toFixed(2);
      steps.priceCheck = { theirs: landed.value, poNet: +poNet.toFixed(2), gap, theirLines: landed.lines, orderNo: landed.orderNo };
      if (Math.abs(gap) >= 0.01) {
        // Name the lines whose unit cost has moved, so this is actionable rather than a bare total.
        const prices = landed.prices || {};
        const changes = [];
        for (const l of orderLines) {
          const theirs = prices[String(l.itemCode).toUpperCase()];
          const ours = Number(l.unitCost != null ? l.unitCost : l.cost);
          if (theirs == null || !Number.isFinite(ours)) continue;
          if (Math.abs(theirs - ours) >= 0.01) changes.push({ sku: l.sku, was: ours, now: theirs });
        }
        const named = changes.length
          ? ` Offending line(s): ${changes.map((c) => `${c.sku} ours £${c.was.toFixed(2)} vs theirs £${c.now.toFixed(2)}`).join('; ')}.`
          : ' Could not pin it to a line from their price file.';
        steps.priceCheck.changes = changes;
        // Chadwick's price file is what they invoice (order total = sum of it), so a pinned line
        // is safe to heal — and until 2026-09-18 Chadwick never was, which is why 865-39/39 at
        // £9.35 vs £9.15 came back on the 11th, 15th and 17th.
        await logPriceCheck(pool, steps, {
          supplierKey: 'CHADWICK', poId, changes,
          message: `Prices don't match: Chadwick order total £${landed.value.toFixed(2)} vs our PO net £${poNet.toFixed(2)} (diff £${gap}).${named} A Brightpearl cost price (Launch/list 20) may need adjusting. Order ${landed.orderNo || orderNo || '(number unknown)'} still placed.`,
          context: { poId, orderNo: landed.orderNo || orderNo, theirs: landed.value, poNet: +poNet.toFixed(2), gap, changes, theirLines: landed.lines },
        });
      }
    } else if (landed && !landed.found) {
      steps.priceCheck = { skipped: `their order list has no order carrying PO ${poId} (scanned ${landed.scanned})` };
    }
  } catch (e) { steps.priceCheck = { skipped: `couldn't read Chadwick's order list: ${e.message}` }; }

  // Recover the order number from their list when the checkout response was unreadable.
  const ref = orderNo || (landed && landed.found && landed.orderNo) || `Placed-${poId}`;
  await bp.setOrderStatusLive(poId, bp.PLACED_WITH_SUPPLIER_STATUS);
  let refWritten = false;
  try { await bp.setOrderReferenceLive(poId, ref); refWritten = true; }
  catch (e) { steps.linkWarn = `reference-set failed (non-fatal): ${e.message}`; await bp.addOrderNoteLive(poId, `Placed with Chadwick — order ${ref}. Reference-set failed: ${e.message}`, CHADWICK_SUPPLIER_CONTACT).catch(() => {}); }
  steps.link = { reference: ref, refWritten, orderNo, status: 7 };

  if (soIds.length) { try { steps.finalize = await bp.finalizeSupplierTagsLive({ orderIds: soIds, supplierKey: 'CHADWICK', poId, noteContactId: CHADWICK_SUPPLIER_CONTACT, setOrderedStatus: true, linesByOrder, execute: live }); } catch (e) { throw stepErr('finalize', `order placed + PO linked, but finalising SOs failed: ${e.message}`); } }
  return { poId, orderNo, steps };
}

// ── Snickers placement chain (Hultafors partner portal) ──────────────────────
// Placed by the headless worker (portal-order-worker suppliers/hultafors.js): CSV basket
// import → the checkout wizard (#btnCheckout → #btnDelivery → #btnPayment → #btnSummary →
// #btnConfirm). Lines are BP SKUs (StockCode) DIRECTLY — no per-line resolution. Full cycle:
// create PO → worker place → mark placed + ref → finalise SOs (note + status + tag clear),
// the two-step finalise from the supplier PO checklist. `live` gates the writes (default on).
const SNICKERS_SUPPLIER_CONTACT = 331;
// PACK MINIMUMS — SKUs the Hultafors portal only sells in multiples of N.
// ⚠️ This is a DIFFERENT rule from Portwest's `packSizes`, which counts BOXES (our unit demand ÷
// pack = how many boxes to order). Here the portal takes UNITS and rounds nothing itself: a qty
// that isn't a multiple of the pack is **silently dropped** from the basket at import (the upload
// step flags HasInvalidLines, validate clears it, and the line just isn't there), after which the
// order fails stage()'s unit-count gate with NO indication of which line vanished. So round UP to
// the nearest multiple: a demand of 1 badge holder is ordered as 10.
// Extend without a deploy via SNICKERS_PACK_MULTIPLES='{"97600400000":10}'.
const SNICKERS_PACK_MULTIPLES = {
  '97600400000': 10,   // Snickers 9760 ID badge holder — 10-pack only (user, 2026-08-17; found when order 0004116942 refused over this one £2.66 line)
  '20031-091': 20,     // Hellberg Helium AF/AS safety glasses — 20-pack only (user, 2026-08-25)
  // Same failure as the badge holder, eight days later, and it cost a whole run: PO 484528 asked
  // for ONE pair, Hultafors silently dropped the line on CSV import, and the worker's unit gate
  // then refused to place 128 units against an expected 130 — GBP4,350 of stock stuck on their
  // checkout page over a GBP4.71 line. Nothing names the dropped SKU from our side; it was found by
  // diffing the PO against the worker's cart.
  // The DROP IS SILENT, so every pack-only code has to be listed here before it bites. Extend
  // without a deploy via SNICKERS_PACK_MULTIPLES='{"<code>":<n>}' on Render.
};
// PACK MINIMUMS — Performance Brands lines sold only in fixed multiples.
// They REFUSE the line and name the rule, so unlike Hultafors these surface immediately rather than
// vanishing — but a refusal still fails the basket check and strands every other line on the order.
// Extend without a deploy via PERFORMANCE_BRANDS_PACK_MULTIPLES='{"<sku>":<n>}' on Render.
const PERFORMANCE_BRANDS_PACK_MULTIPLES = {
  'H1C-BLK': 20,   // Y Shield H1C DS3 slip-ratchet helmet, black — "supplier minimum is 20 (in steps of 20)"
  'H1C-WHT': 20,   // …and white. PO 489405 (2026-09-16) asked for 5 and 9, both refused, and the
  // seven other lines — including the PB1C boot that had already cost a day — went unordered with
  // them. £330 of stock held over two lines nobody could have known were pack-only until it failed.
};
function performanceBrandsPackMultiples() {
  let env = {};
  try { env = JSON.parse(process.env.PERFORMANCE_BRANDS_PACK_MULTIPLES || '{}'); } catch { /* bad JSON → built-ins only, never blocks a run */ }
  const out = {};
  for (const [k, v] of Object.entries(PERFORMANCE_BRANDS_PACK_MULTIPLES)) out[String(k).toUpperCase()] = v;
  for (const [k, v] of Object.entries(env)) { const n = Number(v); if (n > 1) out[String(k).toUpperCase()] = n; }
  return out;
}
function snickersPackMultiples() {
  let env = {};
  try { env = JSON.parse(process.env.SNICKERS_PACK_MULTIPLES || '{}'); } catch { /* bad JSON → built-ins only, never blocks a run */ }
  const out = { ...SNICKERS_PACK_MULTIPLES };
  for (const [k, v] of Object.entries(env)) { const n = Number(v); if (n > 1) out[String(k).toUpperCase()] = n; }
  return out;
}
/* ── Discontinued lines ───────────────────────────────────────────────────────
   A discontinued code cannot be bought at any price, on any run, ever. Left alone it does the
   maximum damage for the minimum cause: Hultafors' CSV import drops it in silence, the unit gate
   then refuses the WHOLE order, and PO 486870 sat holding 98 lines and £6,399 net over one £74.70
   trouser. Retrying achieves nothing, so the run must deal with it rather than stall behind it.

   What happens when the supplier's own portal says a line is discontinued:
     · the PO row is removed          — it is never arriving, and a row left on a placed PO counts
                                        as ON ORDER, which both overstates stock and suppresses the
                                        re-order that would otherwise replace it
     · sales@ is emailed              — a customer is usually waiting on it, and only a person can
                                        decide between a substitute size, a different product or a
                                        refund. The email carries the sales orders that want it and
                                        any sibling sizes still in stock, so that decision is quick
     · the supplier tag is settled    — via finalizeSupplierTagsLive with supplied:false, which
                                        removes only THIS supplier from the tag. An alternatives
                                        group ("PENCARRIE, RALAWISE") correctly falls back to the
                                        other one; a sole supplier clears, so the order stops being
                                        re-tried daily for something that will never come
     · the SKU is remembered          — so the next run excludes it BEFORE staging instead of
                                        rediscovering it by failing again

   The customer's SALES-ORDER line is deliberately NOT touched. Only the purchase-order row is
   removed. Deleting what a customer asked for, automatically, on the strength of a supplier feed,
   is not a decision this code gets to make — that is what the email is for. */

async function ensureDiscontinuedTable(pool) {
  if (!pool) return false;
  await pool.query(`CREATE TABLE IF NOT EXISTS purchasing_discontinued (
    id serial PRIMARY KEY,
    supplier text NOT NULL,
    sku text NOT NULL,
    product_name text,
    detected_at timestamptz NOT NULL DEFAULT now(),
    po_id integer,
    status text,
    note text
  )`);
  await pool.query(`CREATE UNIQUE INDEX IF NOT EXISTS purchasing_discontinued_key
                    ON purchasing_discontinued (upper(supplier), upper(sku))`);
  return true;
}

// SKUs already proven dead for this supplier — excluded before the basket is built.
async function knownDiscontinued(pool, supplierKey) {
  try {
    if (!(await ensureDiscontinuedTable(pool))) return new Set();
    const r = await pool.query('SELECT upper(sku) AS sku FROM purchasing_discontinued WHERE upper(supplier) = $1', [String(supplierKey).toUpperCase()]);
    return new Set(r.rows.map((x) => x.sku));
  } catch { return new Set(); }
}

async function recordDiscontinued(pool, supplierKey, { sku, productName, poId, status, note }) {
  try {
    if (!(await ensureDiscontinuedTable(pool))) return;
    await pool.query(
      `INSERT INTO purchasing_discontinued (supplier, sku, product_name, po_id, status, note)
       VALUES ($1,$2,$3,$4,$5,$6)
       ON CONFLICT (upper(supplier), upper(sku)) DO UPDATE SET detected_at = now(), status = EXCLUDED.status`,
      [String(supplierKey).toUpperCase(), String(sku), productName || null, poId || null, status || 'Discontinued', note || null]);
  } catch (e) { console.error('[discontinued] record failed:', e.message); }
}

// Sibling sizes of the same style+colour that ARE still available — the single most useful thing
// to put in front of whoever has to ring the customer. Numeric sizes only: our SKU carries the
// real size for those, while a lettered garment carries a size CODE the portal does not use, so
// there is nothing reliable to step through. Best-effort throughout — it decorates the email, it
// never gates anything.
async function snickersAlternativeSizes(altItemsUrl, sku) {
  const d = String(sku || '').replace(/[^0-9]/g, '');
  if (d.length < 9) return [];
  const size = Number(d.slice(8));
  if (!Number.isFinite(size) || size <= 0) return [];
  const out = [];
  for (const delta of [-6, -4, -2, 2, 4, 6]) {
    const s = size + delta;
    if (s <= 0) continue;
    try {
      const u = `${altItemsUrl}/api/supplier-stock?supplier=SNICKERS&code=${d.slice(0, 4)}&colour=${d.slice(4, 8)}&size=${s}&live=1`;
      const j = await (await fetch(u, { signal: AbortSignal.timeout(60000) })).json();
      if (j && j.found === true && !/discontinued/i.test(j.status || '') && Number(j.avail) > 0) {
        out.push({ size: String(s), avail: Number(j.avail) });
      }
    } catch { /* one probe failing is not worth failing the email over */ }
  }
  return out;
}

async function emailDiscontinued({ supplierKey, poId, items }) {
  const to = process.env.DISCONTINUED_EMAIL_TO || 'sales@tuffshop.co.uk';
  const esc = (s) => String(s == null ? '' : s).replace(/[&<>]/g, (c) => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;' }[c]));
  const block = items.map((it) => {
    const orders = (it.orders || []).length
      ? `<p style="margin:6px 0"><b>Wanted by:</b> ${it.orders.map((o) => `order <b>#${esc(o.id)}</b>${o.ref ? ` (${esc(o.ref)})` : ''} &times;${o.qty}`).join(', ')}</p>`
      : '<p style="margin:6px 0;color:#666">Not on any sales order — this was a low-inventory top-up, so no customer is waiting.</p>';
    const alts = (it.alternatives || []).length
      ? `<p style="margin:6px 0"><b>Still available in this colour:</b> ${it.alternatives.map((a) => `size ${esc(a.size)} (${a.avail} in stock)`).join(', ')}</p>`
      : '<p style="margin:6px 0;color:#666">No alternative size found automatically — worth checking the portal.</p>';
    return `<div style="border-left:3px solid #b91c1c;padding:8px 12px;margin:12px 0;background:#fef2f2">
      <p style="margin:0 0 4px"><b>${esc(it.name || it.sku)}</b></p>
      <p style="margin:0 0 6px;font-family:monospace">${esc(it.sku)}</p>
      ${orders}${alts}
      <p style="margin:6px 0;color:#666;font-size:12px">Removed from PO ${esc(poId)} so it no longer reads as on order. The sales-order line has NOT been touched.</p>
    </div>`;
  }).join('');
  const html = `<div style="font-family:system-ui,Segoe UI,Arial,sans-serif;max-width:680px">
    <h2 style="margin:0 0 4px">${esc(supplierKey)} — discontinued item${items.length > 1 ? 's' : ''}</h2>
    <p style="color:#555;margin:0 0 12px">${supplierKey} confirm the following can no longer be ordered in this size/colour. The rest of PO ${esc(poId)} went through as normal.</p>
    ${block}
    <p style="font-size:12px;color:#888;margin-top:16px">Someone needs to decide on a substitute, a different product, or a refund, and let the customer know. Detected automatically from the supplier's own stock data.</p>
  </div>`;
  const subject = `${supplierKey} discontinued: ${items.map((i) => i.sku).join(', ').slice(0, 80)}`;
  // FROM must be noreply@, not the recipient. The first live run sent from sales@ TO sales@ —
  // SMTP2GO accepted it without complaint and nothing ever arrived. Every alert that demonstrably
  // lands uses this sender, so this one does too.
  //
  // And report what the SMTP server actually said. Returning `to` on the strength of "sendMail did
  // not throw" is not evidence of anything: it produced a log line reading "sales@ notified" for a
  // mail that was never delivered, which is worse than no claim at all.
  const info = await transporter().sendMail({
    from: '"Tuff Purchasing" <noreply@tuffshop.co.uk>', to, subject, html,
    text: `${supplierKey} — discontinued: ${items.map((i) => `${i.sku}${i.orders.length ? ` (wanted by ${i.orders.map((o) => '#' + o.id).join(', ')})` : ''}`).join('; ')}. `
      + `Removed from PO ${poId}; the rest of the order was placed. The sales-order line has NOT been touched — a substitute or refund needs agreeing with the customer.`,
  });
  return {
    to,
    accepted: (info && info.accepted) || [],
    rejected: (info && info.rejected) || [],
    messageId: (info && info.messageId) || null,
    response: (info && info.response) || null,
  };
}

// Remove the dead rows, settle the tags, tell sales, remember the SKUs. Returns what it did.
// Every step is independently best-effort: the point is that the ORDER proceeds, so a failure to
// e.g. send the email must not turn a recoverable run back into a stalled one. Whatever fails is
// reported in the return value and in the review log.
async function handleDiscontinuedLines({ pool, altItemsUrl, supplierKey, poId, dead, po }) {
  const done = { removedRows: [], tagsSettled: [], emailed: null, recorded: [], problems: [] };
  const soLines = [...((po && po.soLines) || [])];
  const items = [];

  for (const d of dead) {
    const sku = String(d.sku).toUpperCase();
    const wanting = soLines.filter((l) => String(l.sku || '').toUpperCase() === sku && l.order);
    const name = (soLines.find((l) => String(l.sku || '').toUpperCase() === sku) || {}).name || d.name || null;
    items.push({
      sku: d.sku, name, status: d.status || 'Discontinued',
      orders: wanting.map((l) => ({ id: l.order, ref: l.orderRef || null, qty: Math.round(Number(l.qty) || 0) })),
      alternatives: String(supplierKey).toUpperCase() === 'SNICKERS' ? await snickersAlternativeSizes(altItemsUrl, d.sku) : [],
    });
    // 1. the PO row — it is never arriving
    try {
      const r = await bp.removePoRowLive({ poId, sku: d.sku, execute: true });
      done.removedRows.push({ sku: d.sku, ok: !!(r && r.done) });
    } catch (e) { done.problems.push(`remove PO row ${d.sku}: ${e.message}`); }
    await recordDiscontinued(pool, supplierKey, { sku: d.sku, productName: name, poId, status: d.status || 'Discontinued' });
    done.recorded.push(d.sku);
  }

  // 2. tags — ONLY for orders left with nothing else to get from this supplier. An order that
  // still has a live line from them must keep its tag, or that line stops being ordered; the
  // normal finalise clears those when the PO places.
  const deadSet = new Set(dead.map((d) => String(d.sku).toUpperCase()));
  const strandedOrders = [];
  const affectedOrders = [];          // every order carrying a dead line, whether or not it has others
  for (const id of new Set(soLines.filter((l) => l.order).map((l) => l.order))) {
    const mine = soLines.filter((l) => l.order === id);
    if (!mine.some((l) => deadSet.has(String(l.sku || '').toUpperCase()))) continue;
    affectedOrders.push(id);
    const alive = mine.filter((l) => !deadSet.has(String(l.sku || '').toUpperCase()));
    if (!alive.length) strandedOrders.push(id);
  }
  done.affectedOrders = affectedOrders;
  done.deadByOrder = {};
  for (const id of affectedOrders) {
    done.deadByOrder[id] = soLines.filter((l) => l.order === id && deadSet.has(String(l.sku || '').toUpperCase()))
      .map((l) => ({ sku: l.sku, qty: Math.round(Number(l.qty) || 0), name: l.name || null }));
  }
  if (strandedOrders.length) {
    try {
      // supplied:false — this supplier did NOT supply it. settleGroup then drops only this
      // supplier from the tag, so "PENCARRIE, RALAWISE" falls back to RALAWISE and a sole
      // supplier clears outright. Status is deliberately left alone: nothing was ordered for
      // these, so moving them to "Ordered Stock Awaiting Delivery" would be a lie.
      const r = await bp.finalizeSupplierTagsLive({
        orderIds: strandedOrders, supplierKey, poId, supplied: false, setOrderedStatus: false, execute: true,
      });
      done.tagsSettled = (r && r.results) ? r.results.map((x) => x.id || x) : strandedOrders;
    } catch (e) { done.problems.push(`settle tags ${strandedOrders.join(',')}: ${e.message}`); }
  }

  // 3. tell sales — a customer is usually waiting and only a person can decide what to do
  try { done.emailed = await emailDiscontinued({ supplierKey, poId, items }); }
  catch (e) { done.problems.push(`email sales: ${e.message}`); }

  done.items = items;
  return done;
}

// Re-send a discontinued notice that did not reach anyone.
//
// The mail can fail for reasons that have nothing to do with the detection — the first live one was
// sent from sales@ to sales@ and vanished — and when it does, the only record is a note on the
// sales order that nobody is watching. A customer is waiting at the other end of this, so there has
// to be a way to send it again without re-running the purchase.
//
// It rebuilds the message from the ORIGINAL log row rather than from anything passed in, so what
// goes out is exactly what the run computed: same SKUs, same sales orders, same alternative sizes.
// Nothing is re-detected and nothing is re-ordered.
export async function resendDiscontinuedNotice({ pool, errorId }) {
  if (!pool) throw new Error('DB not available');
  const r = await pool.query(
    `SELECT id, supplier, step, message, context, created_at
       FROM purchasing_error_log WHERE id = $1`, [Number(errorId)]);
  const row = r.rows[0];
  if (!row) throw new Error(`no error-log row ${errorId}`);
  if (String(row.step) !== 'discontinued') throw new Error(`row ${errorId} is step "${row.step}", not "discontinued"`);
  const ctx = row.context || {};
  const handled = ctx.handled || {};
  const items = handled.items || [];
  if (!items.length) throw new Error(`row ${errorId} recorded no items to notify about`);
  const sent = await emailDiscontinued({ supplierKey: row.supplier, poId: ctx.poId, items });
  return {
    errorId: Number(errorId), supplier: row.supplier, poId: ctx.poId,
    skus: items.map((i) => i.sku),
    originallyLoggedAt: row.created_at,
    sent,
    delivered: (sent.accepted || []).length > 0,
  };
}

// WHY did Hultafors refuse a line? The CSV import drops what it will not accept and says nothing,
// so "1 line never reached the basket" has always been where the trail went cold and a person had
// to log into the portal. The portal WILL answer per variant, so ask it.
//
// Our Snickers SKU is style(4) + colour(4) + size, and the portal wants those three separately.
// snickersParams() on the Alternate-Items side needs a size LABEL to do this; from a bare SKU the
// trailing digits are the size once leading zeros are dropped ("046" → "46"), which is the same
// split that route already uses for style and colour.
//
// Best-effort by design: if the derivation is wrong the lookup simply finds nothing and the error
// reads exactly as it does today. It can add an answer, never remove one.
// Ask the portal what it says about ONE line. This is the only thing that can tell the
// discontinued check that a code is genuinely dead, so when it cannot answer, the whole guard
// silently does nothing.
//
// It used to derive the size from the SKU — String(Number('12180400006'.slice(8))) → "6" — and pass
// that as the size. The portal matches on the size TEXT: 12180400006 is "L", and canonSize("6")
// never equals canonSize("L"), so every Snickers line came back {found:false,"no colour/size
// match"} and returned null. dead stayed empty, handleDiscontinuedLines was unreachable, and the
// guard had in fact never worked for any line at all. On 2026-09-09 that turned one discontinued
// jacket (12180400006, "WP Soft Shell Jacket Hood Black Size: L", whose portal row says
// "Discontinued" in plain text) into a stalled Snickers run and a manual exclude.
//
// The size LABEL now comes from Brightpearl and goes to the endpoint's own sku+sizeLabel path,
// which is what the other portal suppliers already use. Without a label there is nothing to match
// on, so say so rather than asking a question that always answers "no".
export async function snickersLineStatus(altItemsUrl, sku, sizeLabel) {
  const digits = String(sku || '').replace(/[^0-9]/g, '');
  if (digits.length < 9) return null;
  if (!String(sizeLabel || '').trim()) return null;
  const url = `${altItemsUrl}/api/supplier-stock?supplier=SNICKERS`
    + `&sku=${encodeURIComponent(digits)}&sizeLabel=${encodeURIComponent(String(sizeLabel).trim())}&live=1`;
  try {
    const r = await fetch(url, { signal: AbortSignal.timeout(120000) });
    const j = await r.json();
    if (!j || j.found !== true) return null;
    // The portal's own size, not the label we asked with — it is the authoritative text and it is
    // what the row actually matched on.
    return { status: j.status || null, avail: j.avail == null ? null : Number(j.avail), barcode: j.barcode || null, size: j.size || sizeLabel || null };
  } catch (e) {
    // Returning null here means "not discontinued" to the caller, so a fault in this lookup
    // disables the whole guard without a word — which is how it went unnoticed that it had never
    // worked. Say something, even though the caller still gets null.
    console.error(`[snickers-line-status] ${sku}: ${e.message}`);
    return null;
  }
}

async function placeSnickersOrder(pool, altItemsUrl, { padToThreshold = 0, live = true, excludeSkus = [], includeSalesOrders = true, includeLowInv = true } = {}) {
  const steps = {};
  // excludeSkus: lines Hultafors genuinely cannot sell us — a discontinued code is the usual one.
  // Kept OFF the basket so one dead line cannot strand the whole order (486870 held £6,399 of 98
  // lines over a single £74.70 trouser). It is an explicit instruction, never automatic: a
  // discontinued line a CUSTOMER ordered still needs a human, and dropping it quietly would leave
  // that sales order unfulfillable while looking like a clean run.
  const excl = new Set((excludeSkus || []).map((x) => String(x).trim().toUpperCase()).filter(Boolean));
  let po;
  try { po = await createPo({ supplierKey: 'SNICKERS', execute: live, padToThreshold, logPool: pool, includeSalesOrders, includeLowInv }); }
  catch (e) { throw createPoErr(e); }
  if (!po.created) throw stepErr('create-po', `no PO created: ${po.reason || 'unknown'}` + (po.unresolvedSkus && po.unresolvedSkus.length ? ` — item codes not found in Brightpearl: ${po.unresolvedSkus.join(', ')}` : ''));
  const poId = po.poId;
  const soIds = [...new Set((po.soLines || []).map((l) => l.order).filter(Boolean))];
  const linesByOrder = {};
  for (const l of (po.soLines || [])) { if (l.order) (linesByOrder[l.order] = linesByOrder[l.order] || []).push({ sku: l.sku, qty: l.qty, name: l.name, productId: l.productId }); }
  steps.po = { poId, soUnits: po.soUnits, lowUnits: po.lowUnits, soIds, skippedBundles: po.skippedBundles || [] };

  // Worker lines = the PO's SKUs (skip the =====LOW INV==== separator productId 1000), summed
  // per SKU. Build from soLines/lowLines (FULL SKUs); the /po-cart-lines route truncates them.
  const bySku = new Map();
  // productId is kept per SKU purely so a line that later fails to reach the basket can be looked
  // up on the portal — that lookup needs the size TEXT ("L"), which only Brightpearl knows, and
  // bySku itself is sku -> qty. See the discontinued check below.
  const detailBySku = new Map();
  for (const l of [...(po.soLines || []), ...(po.lowLines || [])]) {
    if (String(l.productId) === '1000' || !l.sku) continue;
    const k = String(l.sku).toUpperCase();
    bySku.set(k, (bySku.get(k) || 0) + Math.round(l.qty));
    if (!detailBySku.has(k)) detailBySku.set(k, { sku: l.sku, productId: l.productId, name: l.name, colour: l.colour, size: l.size });
  }
  // Codes already proven discontinued are dropped BEFORE the basket is built, so a dead line is
  // rediscovered by failing exactly once and never again.
  for (const sku of await knownDiscontinued(pool, 'SNICKERS')) excl.add(sku);
  // Excluded from the BASKET only. The PO row is left alone HERE, because at this point a manual
  // exclusion is just "don't buy it today" and the row is still the record of what a sales order
  // wants. A row is only removed when the supplier CONFIRMS the code is dead — see
  // handleDiscontinuedLines, where the email that tells someone about it goes out too.
  const excluded = [...excl].filter((sku) => bySku.delete(sku));
  if (excluded.length) steps.excluded = excluded;
  const packs = snickersPackMultiples();
  let packApplied = [];
  const buildLines = () => {
    packApplied = [];
    return [...bySku.entries()].map(([stockCode, qty]) => {
      const p = Number(packs[stockCode]);
      if (p > 1 && qty % p !== 0) {
        const q = Math.ceil(qty / p) * p;
        packApplied.push({ sku: stockCode, demand: qty, ordered: q, packOf: p });
        return { stockCode, qty: q };
      }
      return { stockCode, qty };
    });
  };
  let lines = buildLines();
  if (!lines.length) throw stepErr('resolve', 'no orderable Snickers lines');
  if (packApplied.length) steps.packRounding = packApplied;
  steps.resolve = { lines: lines.length, units: lines.reduce((a, l) => a + l.qty, 0) };

  // Drive the Hultafors worker (async job + poll). ref = PO id → the portal PO-number field.
  let wr = await workerPlaceOrder({ supplier: 'SNICKERS', ref: poId, lines, execute: live });

  // ONE retry, and only when the supplier itself says every refused line is discontinued. Safe to
  // repeat because the worker did NOT place: it stages the basket, clears it on re-import, and the
  // failure we are recovering from is by definition "nothing was submitted". Anything else — a
  // pack quantity, an unreadable cart, a genuine outage — falls through to the error below rather
  // than being retried on a guess.
  if ((!wr || !wr.placed) && (wr && (wr.missingLines || []).length)) {
    // ONE retry, and only for a line whose BP `sku` is not what Hultafors actually calls the
    // product. Chadwick already resolves this (see placeChadwickOrder: mpn wins when it differs
    // from the plain SKU) because several Hultafors-group brands sold through Snickers — Hellberg,
    // EMMA, CLC, Toe Guard (see SUPPLIERS.SNICKERS.detect) — are catalogued under their own
    // manufacturer code, not our internal SKU numbering. Snickers itself never hit this so the
    // checkout here never got the same lookup — until a Hellberg line did. ML1207210001 ("Hellberg
    // Secure 2 Foldable Ear Defenders", PO 489448, 2026-09-15) carries identity.mpn "41502-001",
    // which IS the live Hultafors stockcode (1335 in stock); the plain SKU is not a code the portal
    // has ever heard of, so the CSV import silently dropped it — same shape as Chadwick's "CT-"
    // prefix bug. Safe to retry for the same reason as the discontinued check below: the worker did
    // NOT place, and re-import replaces the basket rather than adding to it.
    const wrongCode = [];
    for (const m of (wr.missingLines || []).slice(0, 8)) {
      const d = detailBySku.get(String(m.stockCode).toUpperCase());
      if (!d || !d.productId) continue;
      let itemCode = null;
      try {
        const identity = await bp.getProductIdentityLive(d.productId);
        if (identity.mpn && String(identity.mpn).trim()) itemCode = String(identity.mpn).trim();
      } catch { /* fall back to sku */ }
      if (itemCode && itemCode.toUpperCase() !== String(m.stockCode).toUpperCase()) wrongCode.push({ sku: m.stockCode, itemCode, detail: d });
    }
    if (wrongCode.length) {
      // TWO PRODUCTS MUST NEVER RESOLVE TO ONE ITEM CODE. Chadwick was bitten by exactly this the
      // same day (product 253317's mpn pointed at a DIFFERENT product's code, and the supplier
      // merged both lines into one — nothing short, so a unit-count gate saw nothing wrong). Same
      // rewrite here, same risk: if the corrected code already names a line already in the basket,
      // that is a data fault on our side, not something to silently sum. Leave that one line alone
      // — it falls through to the missing-line report below, same as any other unresolved code —
      // and only retry the lines whose corrected code is not already spoken for.
      const collisions = [];
      const fixed = [];
      for (const { sku, itemCode, detail } of wrongCode) {
        const k = itemCode.toUpperCase();
        if (bySku.has(k)) { collisions.push({ sku, itemCode }); continue; }
        const qty = bySku.get(sku);
        if (qty == null) continue;
        bySku.delete(sku);
        bySku.set(k, qty);
        if (!detailBySku.has(k)) detailBySku.set(k, { ...detail, sku: itemCode });
        fixed.push({ sku, itemCode });
      }
      if (collisions.length) steps.itemCodeCollision = collisions;
      if (fixed.length) {
        steps.itemCodeFixed = fixed;
        lines = buildLines();
        wr = await workerPlaceOrder({ supplier: 'SNICKERS', ref: poId, lines, execute: live });
      }
    }
  }
  if ((!wr || !wr.placed) && (wr && (wr.missingLines || []).length)) {
    const dead = [];
    // The portal needs the size TEXT, which lives in Brightpearl, not in the SKU. Enrich the
    // refused lines from the PO rows first — one bulk product read for the lot — or the lookup
    // below has nothing to match on and the guard cannot fire.
    const missing = (wr.missingLines || []).slice(0, 8)
      .map((m) => ({ ...(detailBySku.get(String(m.stockCode).toUpperCase()) || {}), sku: m.stockCode }));
    await withVariantDetail(missing);
    for (const m of missing) {
      const st = await snickersLineStatus(altItemsUrl, m.sku, m.size);
      if (st && /discontinued/i.test(st.status || '')) dead.push({ sku: m.sku, status: st.status });
    }
    if (dead.length && dead.length === (wr.missingLines || []).length) {
      // If the cleanup itself falls over, do NOT retry the placement: the PO may be half-tidied and
      // guessing past that is how a wrong order gets sent. Record it and fall through to the normal
      // missing-lines error, which still names the code and says it is discontinued.
      let handled;
      try {
        handled = await handleDiscontinuedLines({ pool, altItemsUrl, supplierKey: 'SNICKERS', poId, dead, po });
      } catch (e) {
        steps.discontinuedError = e.message;
        await logPurchasingError(pool, {
          supplier: 'SNICKERS', step: 'discontinued', severity: 'error',
          message: `Found ${dead.length} discontinued line(s) on PO ${poId} (${dead.map((d) => d.sku).join(', ')}) but could not clean them up: ${e.message}. NOTHING was placed and nothing was emailed — this needs doing by hand.`,
          context: { poId, dead },
        }).catch(() => {});
        handled = null;
      }
      if (!handled) throw stepErr('checkout', `Snickers lines are discontinued (${dead.map((d) => d.sku).join(', ')}) but the automatic cleanup failed: ${steps.discontinuedError}`, { poId, dead });
      steps.discontinued = handled;
      await logPurchasingError(pool, {
        supplier: 'SNICKERS', step: 'discontinued', severity: 'review',
        message: `${dead.length} line(s) discontinued at Snickers and removed from PO ${poId}: `
          + handled.items.map((it) => `${it.sku}${it.orders.length ? ` (wanted by ${it.orders.map((o) => '#' + o.id).join(', ')})` : ' (low-inventory only)'}`
            + (it.alternatives.length ? ` — still available: ${it.alternatives.map((a) => 'size ' + a.size).join(', ')}` : '')).join('; ')
          + `. ${(handled.emailed && (handled.emailed.accepted || []).length)
              ? `sales@ notified — accepted for delivery to ${handled.emailed.accepted.join(', ')} (${handled.emailed.response || 'no SMTP detail'}).`
              : handled.emailed
                ? `EMAIL NOT DELIVERED — the server accepted the connection but no recipient: rejected ${JSON.stringify((handled.emailed.rejected) || [])}. NOBODY HAS BEEN TOLD.`
                : 'EMAIL FAILED — nobody has been told.'}`
          + (handled.tagsSettled.length ? ` Tag cleared on ${handled.tagsSettled.length} order(s) with nothing else to come from Snickers.` : '')
          + ' The rest of the order was placed. The sales-order lines were NOT touched — someone still has to agree a substitute or a refund with the customer.'
          + (handled.problems.length ? ` PROBLEMS: ${handled.problems.join('; ')}` : ''),
        context: { poId, dead, handled },
      }).catch(() => {});
      for (const d of dead) bySku.delete(String(d.sku).toUpperCase());
      lines = buildLines();
      if (!lines.length) throw stepErr('resolve', 'every Snickers line was discontinued — nothing left to order');
      steps.resolve = { lines: lines.length, units: lines.reduce((a, l) => a + l.qty, 0), afterDiscontinued: true };
      if (packApplied.length) steps.packRounding = packApplied;
      wr = await workerPlaceOrder({ supplier: 'SNICKERS', ref: poId, lines, execute: live });
    }
  }

  if (!wr || !wr.placed) {
    // NAME THE LINES. Hultafors' CSV import drops what it will not accept and reports nothing, so
    // the unit gate refuses with no clue which line caused it. On 2026-08-25 that meant £4,350 sat
    // on their checkout page and the two culprits (a discontinued code and one sold in 20s) were
    // found only by diffing the PO against the worker's cart by hand. The worker now returns
    // missingLines; put it where a person and the triage pass will both see it.
    const miss = (wr && wr.missingLines) || [];
    // ASK THE PORTAL WHY. Naming the line was the previous fix and it stopped the hand-diffing, but
    // it still left "32235804046 (wanted 1, not in basket)" — true, and no use without a reason.
    // The portal knows: that code is DISCONTINUED, which no amount of retrying will change.
    const why = new Map();
    // Enriched exactly like the discontinued check above, and for the same reason: the portal
    // matches on the size TEXT, which only Brightpearl holds. Asking without it can only ever
    // answer "no", which is why this map was silently empty and every refused line was reported
    // with no reason at all — the one thing this block exists to provide.
    const missDetail = miss.slice(0, 8)
      .map((m) => ({ ...(detailBySku.get(String(m.stockCode).toUpperCase()) || {}), sku: m.stockCode }));
    await withVariantDetail(missDetail);
    for (const m of missDetail) {
      const st = await snickersLineStatus(altItemsUrl, m.sku, m.size);
      if (st) why.set(String(m.sku).toUpperCase(), st);
    }
    const reason = (m) => {
      const st = why.get(String(m.stockCode).toUpperCase());
      if (!st) return '';
      if (/discontinued/i.test(st.status || '')) return ' — DISCONTINUED at Snickers, it will never be orderable';
      if (st.avail === 0) return ' — out of stock at Snickers';
      if (st.avail > 0) return ` — Snickers show ${st.avail} in stock, so the import refused it for another reason (pack quantity?)`;
      return st.status ? ` — Snickers status "${st.status}"` : '';
    };
    const named = miss.map((m) => `${m.stockCode} (wanted ${m.wanted}${m.inCart ? `, only ${m.inCart} in cart` : ', not in basket'})${reason(m)}`).join('; ');
    const dead = miss.filter((m) => /discontinued/i.test((why.get(String(m.stockCode).toUpperCase()) || {}).status || ''));
    // Did we actually submit? Hultafors is an `unreadable` supplier, so nothing downstream can ask
    // the basket, and a blind re-run risks buying the whole order twice. The worker's confirmGone
    // is the one thing that separates the two cases, and the answer has to be IN the error — on
    // 2026-09-11 (PO 488518, 84/84 units staged, no missing lines) the run said only "did not
    // confirm placement" and the evidence expired 30 minutes later with the worker's job.
    const stillOnConfirm = wr && wr.confirmGone === false;
    const verdict = miss.length ? ''
      : stillOnConfirm
        ? `. The Confirm button was STILL on screen after the click, so nothing was submitted — this order can be re-run.`
        : `. The cart was complete (${(wr && wr.cart && wr.cart.qtySum) ?? '?'} of ${(wr && wr.expectedUnits) ?? '?'} units, no missing lines) and the Confirm button `
          + `${wr && wr.confirmGone === true ? 'had gone' : 'could not be read'} without a confirmation appearing. It is NOT known whether Hultafors took this order. `
          + `DO NOT re-run it: check the CLOSED order list on the partner portal first`
          + (wr && wr.jobId ? `, and pull the worker's screenshot from {worker}/job/${wr.jobId} within 30 minutes` : '') + '.';
    throw stepErr('checkout',
      `Snickers worker did not confirm placement${miss.length ? ` — ${miss.length} line(s) never reached the basket: ${named.slice(0, 420)}` : ''}`
      + (dead.length ? `. Nothing else is wrong with this order: re-run excluding ${dead.map((d) => d.stockCode).join(', ')} to place the rest, and sort those line(s) separately — a discontinued code cannot be bought at any size.` : '')
      + verdict
      + `: ${JSON.stringify((wr && (wr.error || wr.statusText)) || wr).slice(0, 200)}`,
      // context is NOT truncated — the full list belongs here, with the counts that prove the gap.
      { poId, missingLines: miss, lineStatus: Object.fromEntries(why), discontinued: dead.map((d) => d.stockCode),
        expectedUnits: (wr && wr.expectedUnits) || null, cartUnits: (wr && wr.cart && wr.cart.qtySum) || null,
        // the evidence trail: jobId while the worker still holds it, then what it saw
        jobId: (wr && wr.jobId) || null, confirmGone: (wr && wr.confirmGone) ?? null,
        submitted: stillOnConfirm ? false : null, url: (wr && wr.url) || null, trail: (wr && wr.trail) || null });
  }
  const orderNo = wr.orderNo || null;
  steps.checkout = { placed: true, orderNo, poSet: wr.poSet || null };

  // price sanity check (NON-FATAL) — what Hultafors will invoice vs our PO costs. Fristads and
  // Castle have had this for a while; Snickers didn't, which is how two wrong CLC costs reached a
  // live order unnoticed on 2026-08-17 (CL1001526 our £30.70 vs their £4.80 — found only by eye,
  // reading the basket screenshot). Per-LINE when the worker could parse the basket grid, so the
  // alert names the offending SKU; otherwise the cart total alone, like Fristads.
  const costBySku = new Map();
  for (const l of [...(po.soLines || []), ...(po.lowLines || [])]) { if (l.sku) costBySku.set(String(l.sku).toUpperCase(), Number(l.cost) || 0); }
  const orderedNet = +lines.reduce((a, l) => a + (costBySku.get(l.stockCode) || 0) * l.qty, 0).toFixed(2);
  const cartTotal = Number(wr.cart && wr.cart.totalCost) || 0;
  const gap = cartTotal ? +(cartTotal - orderedNet).toFixed(2) : 0;
  const lineGaps = [];
  for (const cl of ((wr.cart && wr.cart.lines) || [])) {
    if (!cl.code || cl.unit == null) continue;
    const ours = costBySku.get(String(cl.code).toUpperCase());
    if (ours == null) continue;                                   // not a line we sent (e.g. the add-article row)
    const d = +(cl.unit - ours).toFixed(2);
    if (Math.abs(d) > 0.02) lineGaps.push({ sku: cl.code, ours: +ours.toFixed(2), theirs: cl.unit, diffEach: d, qty: cl.qty });
  }
  steps.priceCheck = { cartTotal: cartTotal || null, orderedNet, gap, lineGaps };
  if (lineGaps.length || (cartTotal && Math.abs(gap) > 0.50)) {
    // Heals from what Hultafors actually charges (their unit price vs our cost), then ONE notice.
    await logPriceCheck(pool, steps, {
      supplierKey: 'SNICKERS', poId, changes: lineGaps.map((g) => ({ sku: g.sku, was: g.ours, now: g.theirs })),
      message: `Prices don't match: Hultafors basket £${cartTotal || '?'} vs our PO net £${orderedNet} (diff £${gap}).`
        + (lineGaps.length ? ` Offending line(s): ${lineGaps.map((g) => `${g.sku} ours £${g.ours} vs theirs £${g.theirs}`).join('; ')}.` : '')
        + ` Order ${orderNo} still placed.`,
      context: { poId, orderNo, cartTotal, orderedNet, gap, lineGaps, packRounding: packApplied },
    });
  }

  // If a pack multiple bumped a line, the PO must show what we will actually RECEIVE (10 badge
  // holders, not 1). Match the PO row by **productId**, not SKU: PO rows carry the BASE product
  // SKU with size in the options while the portal needs the sales-order row's full variant code
  // (PO `25020900` vs portal `25020900008`), so SKU-keying mismatches — and a SKU missing from the
  // cart map is DROPPED, not left alone. Skip with a warning if a SKU spans several products,
  // rather than risk collapsing distinct size variants into one row.
  if (packApplied.length) {
    try {
      const poRows = (await bp.getOrderCartLines(poId)).filter((r) => r.sku);
      const map = new Map();
      for (const r of poRows) { const k = String(r.sku).toUpperCase(); map.set(k, (map.get(k) || 0) + Math.round(r.qty)); }
      const pidBySku = new Map();
      for (const l of [...(po.soLines || []), ...(po.lowLines || [])]) { if (l.sku) pidBySku.set(String(l.sku).toUpperCase(), l.productId); }
      const skipped = [];
      for (const p of packApplied) {
        const pid = pidBySku.get(p.sku);
        const hits = poRows.filter((r) => String(r.productId) === String(pid));
        const skus = new Set(hits.map((r) => String(r.sku).toUpperCase()));
        if (!hits.length) { skipped.push({ ...p, reason: 'no PO row for that productId' }); continue; }
        if (skus.size !== 1) { skipped.push({ ...p, reason: 'productId spans several PO SKUs' }); continue; }
        const k = [...skus][0];
        if (poRows.some((r) => String(r.sku).toUpperCase() === k && String(r.productId) !== String(pid))) { skipped.push({ ...p, reason: 'PO SKU shared by other products' }); continue; }
        map.set(k, p.ordered);
      }
      steps.reconcile = await bp.reconcilePortwestPO({ poId, cart: Object.fromEntries(map), execute: live });
      if (skipped.length) steps.reconcileSkipped = skipped;
    } catch (e) { steps.reconcileWarn = `couldn't bump the PO to the pack quantities: ${e.message}`; }
  }

  // Finalise — BOTH sides (supplier PO checklist item 7). PO: status 7 + reference.
  const ref = orderNo || `Placed-${poId}`;
  await bp.setOrderStatusLive(poId, bp.PLACED_WITH_SUPPLIER_STATUS);
  let refWritten = false;
  try { await bp.setOrderReferenceLive(poId, ref); refWritten = true; }               // API PATCH — tax-safe
  catch (e) { steps.linkWarn = `reference-set failed (non-fatal): ${e.message}`; await bp.addOrderNoteLive(poId, `Placed with Snickers — order ${ref}. Reference-set failed: ${e.message}`, SNICKERS_SUPPLIER_CONTACT).catch(() => {}); }
  steps.link = { reference: ref, refWritten, orderNo, status: 7 };

  // SO: note ("… Ordered on PO#<id>") + status → Ordered Stock Awaiting Delivery + clear tag.
  // An order that lost a line to a discontinued code must NOT be finalised as if everything was
  // bought. Its in-stock items ARE ordered — that half is normal — but sending it to "Ordered Stock
  // Awaiting Delivery" would say the whole order is on its way, and that is how SO 484193 sat
  // looking complete for a trouser that reached no PO until a customer chased it. Those orders go
  // to "Order Confirmation Sent" (60) instead: it parks them off the ordering flow (the low
  // inventory report excludes 1/18/60 from Open SO, so nothing re-orders behind it) and leaves them
  // visibly unfinished for whoever picks up the email to sales.
  const hitByDiscontinued = new Set(((steps.discontinued && steps.discontinued.affectedOrders) || []).map(Number));
  const cleanIds = soIds.filter((id) => !hitByDiscontinued.has(Number(id)));
  const partIds = soIds.filter((id) => hitByDiscontinued.has(Number(id)));
  if (cleanIds.length) { try { steps.finalize = await bp.finalizeSupplierTagsLive({ orderIds: cleanIds, supplierKey: 'SNICKERS', poId, noteContactId: SNICKERS_SUPPLIER_CONTACT, setOrderedStatus: true, linesByOrder, execute: live }); } catch (e) { throw stepErr('finalize', `order placed + PO linked, but finalising SOs failed: ${e.message}`); } }
  if (partIds.length) {
    try {
      // Tag still clears — there genuinely is nothing further to get from Snickers for these, and
      // leaving it set would re-order the good lines tomorrow. setOrderedStatus:false so the status
      // is ours to choose, not 22.
      // The finalise note lists what was ordered for each SO, and linesByOrder was built BEFORE the
      // discontinued line was pulled — so it cheerfully wrote "32235804046 x1 Ordered on PO#486870"
      // directly above the note saying that exact line is discontinued and was NOT ordered. Strip
      // the dead SKUs first: a note that contradicts itself is worse than no note, because someone
      // reads the first one and stops.
      const deadSkus = new Set(((steps.discontinued && steps.discontinued.affectedOrders) || []).length
        ? Object.values((steps.discontinued && steps.discontinued.deadByOrder) || {}).flat().map((d) => String(d.sku).toUpperCase())
        : []);
      const linesByOrderClean = {};
      for (const [oid, items] of Object.entries(linesByOrder || {})) {
        linesByOrderClean[oid] = (items || []).filter((it) => !deadSkus.has(String(it && it.sku || '').toUpperCase()));
      }
      steps.finalizePartial = await bp.finalizeSupplierTagsLive({ orderIds: partIds, supplierKey: 'SNICKERS', poId, noteContactId: SNICKERS_SUPPLIER_CONTACT, setOrderedStatus: false, linesByOrder: linesByOrderClean, execute: live });
      const parked = [];
      for (const id of partIds) {
        const deadHere = ((steps.discontinued && steps.discontinued.deadByOrder) || {})[id] || [];
        const what = deadHere.map((d) => `${d.sku}${d.qty > 1 ? ` x${d.qty}` : ''}${d.name ? ` (${d.name})` : ''}`).join(', ');
        try {
          // Say what actually happened to the email. The first live run wrote "sales@ have been
          // emailed" onto a customer's order for a mail that never arrived — a false reassurance
          // sitting exactly where someone would rely on it.
          const em = (steps.discontinued && steps.discontinued.emailed) || null;
          const emailLine = em && (em.accepted || []).length
            ? `sales@ have been emailed (${em.accepted.join(', ')}).`
            : 'THE EMAIL TO sales@ DID NOT SEND — this note is the only record, so tell them.';
          if (live) await bp.addOrderNoteLive(id, `DISCONTINUED at Snickers — NOT ordered and cannot be: ${what}. Everything else on this order was ordered on PO#${poId}. ${emailLine} This order is on "Order Confirmation Sent" rather than "Ordered Stock Awaiting Delivery" because it is NOT complete — agree a substitute or a refund with the customer, then move it on.`, SNICKERS_SUPPLIER_CONTACT);
          if (live) await bp.setOrderStatusLive(id, DISCONTINUED_PARK_STATUS);
          parked.push({ id, status: DISCONTINUED_PARK_STATUS, dead: deadHere.map((d) => d.sku) });
        } catch (e) { parked.push({ id, error: e.message }); }
      }
      steps.parkedForDiscontinued = parked;
    } catch (e) { throw stepErr('finalize', `order placed + PO linked, but finalising the discontinued-affected SOs failed: ${e.message}`); }
  }
  return { poId, orderNo, steps };
}

// ── Carhartt / Helly Hansen (Elastic Suite "Skillet" portals) ────────────────
// An order is a "document" the Alt-Items basket route builds + submits (guarded on that
// side by <X>_PLACE_ENABLED, and it refuses on any unresolved line). Full cycle: create BP
// PO → POST /api/<x>-basket {place} → mark placed + ref → finalise SOs (note+status+tag).
// `live` gates the writes (default on). Contacts: Carhartt 65173, Helly Hansen 214.
// Fill in colour/size from Brightpearl's variant options for any line missing them, and RETURN the
// same array so it can be used either way. ONE function used by BOTH the preflight and the
// checkout, deliberately: enriching only the preflight is worse than not enriching at all, because
// the preflight then passes, the PO gets created, and the checkout — sending bare SKUs — refuses on
// the very line the preflight just resolved. PO 483861 was orphaned exactly that way on 2026-08-21.
// Costs one batched product read at most, and only for lines that actually need it.
// This enrichment is NOT merely additive, despite what the catch below used to claim. For a legacy
// TB-coded product with no EAN, style + colour + size is the ONLY way the portal can resolve the
// line, so a failure here does not degrade gracefully — it makes a stocked item look DISCONTINUED
// and aborts the entire run before any PO exists. HELLY HANSEN 2026-08-27: one transient Brightpearl
// failure on an EIGHT-id call (an 80-character URL) blocked all 8 lines and £492 of demand, and the
// resulting error told the reader to go fix aliases that were never broken.
//
// So: retry, and RECORD the failure, so the caller can tell "the supplier does not stock this"
// apart from "we could not look it up". Those are opposite conclusions and they looked identical.
async function withVariantDetail(lines) {
  const need = lines.filter((l) => !l.colour || !l.size).map((l) => l.productId).filter(Boolean);
  if (!need.length) return lines;
  const ids = [...new Set(need)].sort((a, b) => a - b);    // Brightpearl 400s on an unsorted id set
  let arr = null, lastErr = null;
  for (let attempt = 0; attempt < 3 && arr == null; attempt++) {
    try { arr = await bp.bpLiveGet(`/product-service/product/${ids.join(',')}`) || []; }
    catch (e) { lastErr = e; if (attempt < 2) await new Promise((r) => setTimeout(r, 1200 * (attempt + 1))); }
  }
  if (arr == null) { lines.enrichFailed = (lastErr && lastErr.message) || 'unknown'; return lines; }
  try {
    const byId = {};
    for (const p of (Array.isArray(arr) ? arr : [arr])) {
      if (!p || p.id == null) continue;
      const v = {};
      for (const o of (p.variations || [])) v[String(o.optionName || '').toLowerCase()] = o.optionValue;
      byId[String(p.id)] = v;
    }
    for (const l of lines) {
      const v = byId[String(l.productId)];
      if (!v) continue;
      if (!l.colour) l.colour = v.colour || v.color || null;
      if (!l.size) l.size = v.size || null;
    }
  } catch { /* additive only — without it we simply fall back to the old behaviour */ }
  return lines;
}
async function placeElasticOrder(pool, altItemsUrl, { supplierKey, contactId, basketPath, padToThreshold = 0, live = true, excludeSkus = [] } = {}) {
  // excludeSkus: lines the supplier genuinely cannot sell us (discontinued colour, dead code).
  // They come off BOTH the portal basket AND the PO — unlike the Portwest equivalent, which
  // leaves them on the PO on purpose for manual handling. Leaving them on here would tell
  // Brightpearl three discontinued garments are ON ORDER when nothing was bought, which is
  // exactly the state SO 484193 is stuck in: "Ordered Stock Awaiting Delivery" for an Apache
  // trouser with no stock, no PO and nothing left in demand asking for it.
  const excl = new Set((excludeSkus || []).map((x) => String(x).trim().toUpperCase()).filter(Boolean));
  const steps = {};
  // Pre-flight (only on a real run): value the demand + confirm the portal resolves EVERY line
  // BEFORE creating the BP PO, so a resolution miss can't leave an orphan PO. (The PO is created
  // before the portal submit, so without this an unresolved line would strand a Pending PO.)
  // It ALSO harvests the portal's live wholesale price per SKU (pricedLines) → priceOverrides,
  // so the PO net reconciles to the supplier invoice instead of trusting BP's stored cost.
  let priceOverrides = null;
  if (live) {
    let preview;
    try { preview = await createPo({ supplierKey, execute: false }); }
    catch (e) { throw stepErr('preflight', `couldn't value the demand: ${e.message}`); }
    // name/colour/size go with the line so the portal can fall back to style + colour NAME + size
    // when neither our SKU nor our EAN is in its sheet. 802211-001L aborted the whole Carhartt run
    // on 2026-08-21 for exactly that: it is style SC4223M in Black/L, in the sheet with 5,718
    // available, but our SKU carries a legacy code and our EAN appears nowhere in the sheet.
    const preLines = [...(preview.soLines || []), ...(preview.lowLines || [])]
      .filter((l) => String(l.productId) !== '1000' && l.sku && !excl.has(String(l.sku).toUpperCase()))
      .map((l) => ({ sku: l.sku, qty: l.qty, name: l.name, colour: l.colour, size: l.size, productId: l.productId }));
    await withVariantDetail(preLines);
    if (preLines.length) {
      const dry = await jfetch('preflight', `${altItemsUrl}${basketPath}`, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ lines: preLines, dryRun: true }) });
      // An unresolved line means one of two OPPOSITE things, and saying the wrong one sends whoever
      // reads this — person or triage bot — off to fix data that is not broken. If the variant
      // lookup failed we never had colour/size to match on, so the miss is very likely a false
      // negative and the right move is to RETRY, not to touch an alias.
      if (dry.unresolved && dry.unresolved.length) {
        const why = preLines.enrichFailed
          ? ` — but the Brightpearl variant lookup FAILED (${preLines.enrichFailed}), so colour/size were missing and these are probably FALSE NEGATIVES. Re-run before changing any alias or SKU.`
          : ' — fix the resolver/aliases first';
        throw stepErr('preflight', `${supplierKey} lines not on the portal (PO NOT created): ${dry.unresolved.join(', ')}${why}`, { enrichFailed: preLines.enrichFailed || null, unresolved: dry.unresolved });
      }
      // A line rescued by the style+colour-name+size fallback is REPORTED, not silently accepted:
      // it means our SKU or EAN disagrees with the supplier sheet and should be corrected at
      // source, or the alias quietly carries a wrong mapping forever.
      if (Array.isArray(dry.aliased) && dry.aliased.length) {
        steps.aliased = dry.aliased;
        await logPurchasingError(pool, {
          supplier: supplierKey, step: 'sku-alias', severity: 'review',
          message: `${dry.aliased.length} ${supplierKey} line(s) only resolved by matching style + colour + size, because our SKU/EAN is not in the supplier sheet. `
            + 'They WERE ordered. Worth correcting the SKU or EAN in Brightpearl:\n'
            + dry.aliased.map((a) => `      ${a.sku} -> style ${a.matchedStyle} ${a.colour}/${a.size} (UPC ${a.upc})`).join('\n'),
          context: { supplier: supplierKey, aliased: dry.aliased },
        }).catch(() => {});
      }
      if (Array.isArray(dry.pricedLines) && dry.pricedLines.length) {
        // The Elastic sheet quotes LIST. Convert to net HERE, at the single point the price enters
        // the system, so the PO rows, the SO costings and the healer all see the same real number and
        // no lump discount row is ever needed. PO 483239 had to be rebuilt by hand for want of this.
        const disc = Number((bp.SUPPLIERS[supplierKey] || {}).supplierDiscountPct) || 0;
        priceOverrides = {};
        for (const p of dry.pricedLines) {
          const price = Number(p.price);
          if (!p.sku || !Number.isFinite(price) || price <= 0) continue;
          priceOverrides[String(p.sku).toUpperCase()] = disc > 0 ? Math.round(price * (1 - disc) * 100) / 100 : price;
        }
        steps.priceOverrides = { count: Object.keys(priceOverrides).length, discountPct: disc || null };
      }
    }
  }
  let po;
  try { po = await createPo({ supplierKey, execute: live, padToThreshold, priceOverrides, logPool: pool }); }
  catch (e) { throw createPoErr(e); }
  if (!po.created) throw stepErr('create-po', `no PO created: ${po.reason || 'unknown'}` + (po.unresolvedSkus && po.unresolvedSkus.length ? ` — item codes not found in Brightpearl: ${po.unresolvedSkus.join(', ')}` : ''));
  const poId = po.poId;
  const soIds = [...new Set((po.soLines || []).map((l) => l.order).filter(Boolean))];
  const linesByOrder = {};
  for (const l of (po.soLines || [])) { if (l.order) (linesByOrder[l.order] = linesByOrder[l.order] || []).push({ sku: l.sku, qty: l.qty, name: l.name, productId: l.productId }); }
  steps.po = { poId, soUnits: po.soUnits, lowUnits: po.lowUnits, soIds, skippedBundles: po.skippedBundles || [], priceOverridesApplied: po.priceOverridesApplied || [] };

  // Take the excluded SKUs off the PO before anything is built from it. The basket below is
  // derived from these same rows, so removing them here keeps BP and the supplier order
  // identical — no line on the PO that was never bought.
  if (excl.size) {
    steps.excluded = [];
    for (const sku of excl) {
      try {
        const r = await bp.removePoRowLive({ poId, sku, execute: live });
        steps.excluded.push({ sku, removed: !!(r && r.done) });
      } catch (e) { steps.excluded.push({ sku, error: e.message }); }
    }
  }
  // Order lines = the PO's SKUs (skip the =====LOW INV==== separator), summed per SKU.
  const bySku = new Map();
  for (const l of [...(po.soLines || []), ...(po.lowLines || [])]) {
    if (String(l.productId) === '1000' || !l.sku) continue;
    if (excl.has(String(l.sku).toUpperCase())) continue;   // dropped: never reaches the basket
    const k = String(l.sku).toUpperCase();
    const cur = bySku.get(k) || { qty: 0, name: l.name, productId: l.productId, colour: l.colour, size: l.size };
    cur.qty += Math.round(l.qty);
    if (!cur.name) cur.name = l.name;
    bySku.set(k, cur);
  }
  let lines = [...bySku.entries()].map(([sku, v]) => ({ sku, qty: v.qty, name: v.name, colour: v.colour, size: v.size, productId: v.productId }));
  if (!lines.length) throw stepErr('resolve', `no orderable ${supplierKey} lines`);
  // Carry name/colour/size through to the CHECKOUT too, not just the preflight. Enriching only the
  // preflight is worse than not enriching at all: the preflight then PASSES, the PO gets created,
  // and the checkout - sending bare SKUs - refuses on the very line the preflight just resolved.
  // That is exactly how PO 483861 was orphaned on 2026-08-21 over 802211-001L.
  lines = await withVariantDetail(lines);
  steps.resolve = { lines: lines.length, units: lines.reduce((a, l) => a + l.qty, 0) };

  // Build + submit the document via the Alt-Items basket route (ref = PO# → the document PO field).
  // The route refuses if any line is unresolved, so a null/short order can never be submitted.
  const r = await jfetch('checkout', `${altItemsUrl}${basketPath}`, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ lines, purchaseOrder: String(poId), place: live }) });
  if (r.unresolved && r.unresolved.length) throw stepErr('resolve', `${supplierKey} lines not in the availability sheet (order NOT placed): ${r.unresolved.join(', ')}`);
  if (!r.placed) throw stepErr('checkout', `${supplierKey} did not confirm placement: ${JSON.stringify(r.error || r).slice(0, 250)}`);
  const orderNo = r.orderNo || null;
  steps.checkout = { placed: true, orderNo, wantedUnits: r.wantedUnits };

  // Finalise both sides. PO: status 7 + reference.
  const ref = orderNo || `Placed-${poId}`;
  await bp.setOrderStatusLive(poId, bp.PLACED_WITH_SUPPLIER_STATUS);
  let refWritten = false;
  try { await bp.setOrderReferenceLive(poId, ref); refWritten = true; }
  catch (e) { steps.linkWarn = `reference-set failed (non-fatal): ${e.message}`; await bp.addOrderNoteLive(poId, `Placed with ${supplierKey} — order ${ref}. Reference-set failed: ${e.message}`, contactId).catch(() => {}); }
  steps.link = { reference: ref, refWritten, orderNo, status: 7 };
  // SO: note + status → Ordered Stock Awaiting Delivery + clear tag.
  if (soIds.length) { try { steps.finalize = await bp.finalizeSupplierTagsLive({ orderIds: soIds, supplierKey, poId, noteContactId: contactId, setOrderedStatus: true, linesByOrder, execute: live }); } catch (e) { throw stepErr('finalize', `order placed + PO linked, but finalising SOs failed: ${e.message}`); } }
  // heal BP costs from the portal's live wholesale price — already harvested for priceOverrides
  await healPrices(steps, { supplierKey, poId, changes: (po.priceOverridesApplied || []).map((p) => ({ sku: p.sku, was: p.was, now: p.now })), pool });
  return { poId, orderNo, steps };
}
async function placeCarharttOrder(pool, altItemsUrl, opts = {}) { return placeElasticOrder(pool, altItemsUrl, { supplierKey: 'CARHARTT', contactId: 65173, basketPath: '/api/carhartt-basket', ...opts }); }
async function placeHellyHansenOrder(pool, altItemsUrl, opts = {}) { return placeElasticOrder(pool, altItemsUrl, { supplierKey: 'HELLY HANSEN', contactId: 214, basketPath: '/api/hellyhansen-basket', ...opts }); }

// ── Portwest placement chain (portwest.com — CodeIgniter) ────────────────────
// Whole order goes up as a CSV (item,qty) → the cart lands on /cart/checkout → ONE
// checkout_summary POST places it with our PO# in `custref` (account payment, default
// delivery). The Alt-Items /api/portwest-order route does upload→place in one call.
// Same skeleton as Castle. £150 free-carriage threshold. Contact 298.
const PORTWEST_SUPPLIER_CONTACT = 298;
// TEMP safety: never auto-order these Portwest codes (unit/pack mismatch being fixed in BP).
// P351WHR is a BOX OF 20 but BP tracks it in singles → user is re-adding it as a NEW box
// product. REMOVE from this list once that's done. Applied to every Portwest run (incl. scheduled).
//
// 196109 is the SAME product as P351WHR — it is the Brightpearl SKU our PO rows actually carry,
// while P351WHR is Portwest's own code. On PO 483845 (2026-08-21) this exclusion therefore did NOT
// match: the line went up as 196109, Portwest did not recognise it, and it was dropped. The right
// outcome, but by luck rather than by the guard — had Portwest accepted that code we would have
// bought 48 loose masks against an explicit instruction not to. An exclusion has to be expressed in
// the code the PO ROW carries, so BOTH spellings are listed until the box product replaces it.
// PACK LINES — a Brightpearl SKU that is a MULTIPLE of what Portwest sell as one item.
//   ours: the BP sku on the PO row.   theirs: the Portwest item code.   pack: our units per their item.
// P351WHR-20 is a 20-pack of the P351 respirator on our side; Portwest sell P351WHR singly in boxes
// of 20. So 48 of ours = ceil(48/20) = 3 boxes of theirs, sent under THEIR code. The quantity half
// of this rule has existed (packSizes) since the day this file mentioned "P351WHR IS a box of 20
// masks" as its worked example — and no scheduled run ever passed a packSizes, so it never fired
// once. P351WHR sat in PORTWEST_TEMP_EXCLUDE as "handled manually" instead, which also never
// matched the PO row, because the row is P351WHR-20. 48 masks dropped on 2026-09-17 with 5,485 in
// stock at Portwest. Extend without a deploy via PORTWEST_PACK_LINES='{"OURSKU":{"theirs":"X","pack":20}}'.
const PORTWEST_PACK_LINES = {
  'P351WHR-20': { theirs: 'P351WHR', pack: 20 },   // P351 FFP3 Dolomite fold-flat respirator, white
};
function portwestPackLines() {
  let env = {};
  try { env = JSON.parse(process.env.PORTWEST_PACK_LINES || '{}'); } catch { /* bad JSON → built-ins only, never blocks a run */ }
  const out = {};
  for (const [k, v] of Object.entries(PORTWEST_PACK_LINES)) out[String(k).toUpperCase()] = v;
  for (const [k, v] of Object.entries(env)) if (v && v.theirs && Number(v.pack) > 1) out[String(k).toUpperCase()] = { theirs: String(v.theirs), pack: Number(v.pack) };
  return out;
}
// Codes to leave OFF the Portwest order entirely. 196109 is an internal code with no Portwest
// equivalent yet. P351WHR used to sit here as "handled manually" — it is a pack line now, above.
const PORTWEST_TEMP_EXCLUDE = ['196109'];
async function placePortwestOrder(pool, altItemsUrl, { padToThreshold = 0, verifyOnly = false, poId: existingPoId = null, packSizes = {}, excludeSkus = [] } = {}) {
  const steps = {};
  let poId, soIds, linesByOrder;
  if (existingPoId) {
    // Reuse a PO created by a prior verifyOnly run — re-derive the SO mapping (for the finalise
    // notes) from current demand (unchanged over the few minutes between prepare and place).
    poId = existingPoId;
    // The contributing SOs come from the PO's OWN note — a fresh demand read would net to zero
    // (those lines are already on order via this PO), which would skip the SO finalise.
    let contrib;
    try { contrib = await bp.getPoContributors(poId); }
    catch (e) { throw stepErr('create-po', `couldn't read PO ${poId} contributors: ${e.message}`); }
    soIds = contrib.soIds; linesByOrder = contrib.linesByOrder;
    steps.po = { poId, reused: true, soIds };
  } else {
    let po;
    try { po = await createPo({ supplierKey: 'PORTWEST', execute: true, padToThreshold, logPool: pool }); }
    catch (e) { throw createPoErr(e); }
    if (!po.created) throw stepErr('create-po', `no PO created: ${po.reason || 'unknown'}` + (po.unresolvedSkus && po.unresolvedSkus.length ? ` — item codes not found in Brightpearl: ${po.unresolvedSkus.join(', ')}` : ''));
    poId = po.poId;
    soIds = [...new Set((po.soLines || []).map((l) => l.order).filter(Boolean))];
    linesByOrder = {};
    for (const l of (po.soLines || [])) { if (l.order) (linesByOrder[l.order] = linesByOrder[l.order] || []).push({ sku: l.sku, qty: l.qty, name: l.name, productId: l.productId }); }
    steps.po = { poId, soUnits: po.soUnits, lowUnits: po.lowUnits, soIds, skippedBundles: po.skippedBundles || [] };
  }
  // Upload lines = the ACTUAL PO ROWS (canonical Portwest codes, e.g. P351WHR — the SO demand
  // may carry a different internal SKU like 196109 that Portwest's portal doesn't recognise).
  let poRowLines;
  // productId and name ride along so a dropped line can be matched to its customer EXACTLY. A
  // dropped line was recorded by sku alone, and on 2026-09-17 that sku was "biz2" — which matched
  // nothing in demand_log, so BIZ2NVRXXL for SO 487261 was reported as "no customer waiting" on
  // its FOURTH silent drop. The productId is the one identifier nothing along the way can mangle.
  try { poRowLines = (await bp.getOrderCartLines(poId)).filter((l) => l.sku).map((l) => ({ sku: String(l.sku), qty: Math.round(l.qty), productId: l.productId, name: l.name })); }
  catch (e) { throw stepErr('cart', `couldn't read PO ${poId} rows: ${e.message}`); }
  // Upload starts from the PO rows. packSizes[sku] = how many of OUR units make ONE Portwest
  // order item (e.g. P351WHR IS a box of 20 masks). The Portwest order qty is therefore
  // ceil(demand / pack) — 48 masks in boxes of 20 → 3 boxes. The reconcile below then sets the
  // PO row to that pack count so PO == the actual order.
  // excludeSkus: leave these OFF the Portwest order entirely (handled manually — e.g. a box/single
  // unit mismatch). They stay in poRowLines so the reconcile then drops them from the PO too.
  const excl = new Set([...(excludeSkus || []), ...PORTWEST_TEMP_EXCLUDE].map((s) => String(s).toUpperCase()));
  let cartLines = poRowLines.filter((l) => !excl.has(String(l.sku).toUpperCase())).map((l) => ({ ...l }));
  if (excl.size) steps.excluded = [...excl];
  const packApplied = [];
  // Built-in pack lines first (code AND quantity), then any caller-supplied packSizes (quantity only,
  // same code) on top — the caller form is kept so portwestPrepare / portwestPlaceExisting still work.
  const packLines = portwestPackLines();
  const theirsToOurs = new Map();   // Portwest code → our PO sku, so the cart read-back matches the PO row
  // A PO row does not always carry the product's SKU. When a product has a SUPPLIER SKU set in
  // Brightpearl, that is what BP writes onto the row of a PO for that supplier — product 106648
  // (BIZ2NVRXXL) rowed as "biz2", B303RERL as "B303", FR26NARL as "FR26". Portwest's portal only
  // knows the real code, so the upload sent "biz2", Portwest rejected it, and the reconcile
  // dropped the customer line — SO 487261 lost its jacket four runs in a row (2026-09-08 → 09-17)
  // and the row read as a reorder line nobody was waiting for. Send the product's real SKU and
  // translate the cart back to the row's code, exactly as a pack line is.
  const skuRepaired = [];
  try {
    const real = await bp.liveSkusOf(cartLines.map((l) => l.productId));
    cartLines = cartLines.map((l) => {
      const s = real.get(String(l.productId));
      if (!s || s.toUpperCase() === String(l.sku).toUpperCase()) return l;
      skuRepaired.push({ productId: l.productId, rowSku: l.sku, sentAs: s, name: l.name });
      theirsToOurs.set(s.toUpperCase(), String(l.sku).toUpperCase());
      return { ...l, sku: s };
    });
  } catch (e) { steps.skuRepairWarn = e.message; }
  if (skuRepaired.length) steps.skuRepaired = skuRepaired;
  cartLines = cartLines.map((l) => {
    const rule = packLines[String(l.sku).toUpperCase()];
    if (!rule) return l;
    const q = Math.max(1, Math.ceil(l.qty / rule.pack));
    packApplied.push({ sku: l.sku, sentAs: rule.theirs, demandUnits: l.qty, packs: q, packOf: rule.pack });
    // Chain through any SKU repair above so the cart still lands on the PO ROW's code.
    theirsToOurs.set(String(rule.theirs).toUpperCase(), theirsToOurs.get(String(l.sku).toUpperCase()) || String(l.sku).toUpperCase());
    return { ...l, sku: rule.theirs, qty: q };
  });
  if (packSizes && Object.keys(packSizes).length) {
    cartLines = cartLines.map((l) => { const p = Number(packSizes[String(l.sku).toUpperCase()]); if (p > 1) { const q = Math.max(1, Math.ceil(l.qty / p)); if (q !== l.qty) { packApplied.push({ sku: l.sku, demandUnits: l.qty, packs: q, packOf: p }); return { ...l, qty: q }; } } return l; });
  }
  if (packApplied.length) steps.packConversion = packApplied;
  const expectUnits = cartLines.reduce((a, l) => a + l.qty, 0);
  if (!cartLines.length) throw stepErr('cart', 'no orderable Portwest lines');

  // 1) CSV-upload the order to the cart WITHOUT placing, and read the cart contents back.
  const up = await jfetch('cart', `${altItemsUrl}/api/portwest-order`, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ lines: cartLines, purchaseOrder: String(poId) }) });
  const cart = (up.checkout && up.checkout.lines) || [];
  steps.cart = { uploaded: up.upload && up.upload.units, cartUnits: up.checkout && up.checkout.cartUnits, cartLineCount: cart.length, expectUnits, poLineCount: cartLines.length };

  // 2) RECONCILE the PO to what Portwest will ACTUALLY ship. Portwest rounds quantities up to
  //    its carton/min-order, so the cart can hold MORE than we asked — we accept that (order the
  //    min) and bump the PO to match. A line the portal wouldn't take (0 in the cart) is dropped
  //    from the PO. Guard against a systemic failure (empty cart / unexpected extras / mass drop).
  // Compare on a SLASH-NORMALISED code. Portwest sizes can contain a slash ("L/XL") and the cart's
  // delete-line URL does not reliably carry it, so our PO row C472YERL/XL and the cart's C472YERL
  // are the SAME line. Matching them literally produced two contradictory findings at once on
  // 2026-08-21 — the cart code counted as an unexpected extra AND the PO row counted as dropped —
  // and aborted PO 483845 over a cart that was entirely correct.
  const nk = (s) => String(s).toUpperCase().replace(/\//g, '');
  // A pack line comes back from the cart under PORTWEST's code, in PACKS. Translate it to our PO
  // row's sku and back into our units before the diff, or the PO row reads as dropped and the cart
  // line reads as an unexpected extra — the same double miscount the slash fix above describes.
  const got = new Map();
  for (const l of cart) {
    const theirs = String(l.sku).toUpperCase();
    const ours = theirsToOurs.get(theirs);
    const rule = ours ? packLines[ours] : null;
    const key = nk(ours || l.sku);
    const units = rule ? (Number(l.qty) || 0) * rule.pack : (Number(l.qty) || 0);
    got.set(key, (got.get(key) || 0) + units);
  }
  // Diff the CART against the ORIGINAL PO ROWS (not the pack-adjusted upload) so BOTH Portwest's
  // carton rounding and our own pack rounding surface as bumps to apply to the PO.
  const bumped = [], droppedLines = [], matched = [];
  for (const l of poRowLines) { const g = got.get(nk(l.sku)) || 0; if (g === 0) droppedLines.push({ sku: l.sku, want: l.qty, productId: l.productId, name: l.name }); else if (g !== l.qty) bumped.push({ sku: l.sku, from: l.qty, to: g }); else matched.push(l.sku); }
  const extra = [...got.keys()].filter((s) => !poRowLines.some((l) => nk(l.sku) === s));
  const cartUnits = [...got.values()].reduce((a, b) => a + b, 0);
  steps.verify = { poLineCount: poRowLines.length, cartLineCount: cart.length, matched: matched.length, bumped, dropped: droppedLines, extra };
  if (!cart.length || cartUnits === 0) throw stepErr('verify', `Portwest cart is empty after upload — aborting. PO#${poId} left for review.`, { poId });
  if (extra.length) throw stepErr('verify', `Portwest cart has ${extra.length} line(s) NOT on the PO (${extra.slice(0, 8).join(', ')}) — aborting for review. PO#${poId}.`, { poId, extra });
  if (droppedLines.length > Math.max(3, Math.ceil(poRowLines.length * 0.25))) throw stepErr('verify', `${droppedLines.length} of ${poRowLines.length} lines dropped from the Portwest cart — too many, aborting for review. PO#${poId}.`, { poId, dropped: droppedLines });

  // Apply the reconcile to the PO (bump round-ups, drop un-orderable lines) so PO == cart.
  if (bumped.length || droppedLines.length) {
    try { steps.reconcile = await bp.reconcilePortwestPO({ poId, cart: got, execute: true }); }
    catch (e) { throw stepErr('reconcile', `couldn't update PO ${poId} to match the cart: ${e.message}`, { poId, bumped, dropped: droppedLines }); }
  }
  // Tidy: collapse any duplicate PO rows (same SKU from several SOs) into one row per SKU.
  try { const c = await bp.consolidatePoRows({ poId, execute: true }); if (c.merged && c.merged.length) steps.consolidated = c.merged; }
  catch (e) { steps.consolidateWarn = e.message; }

  // verifyOnly: stop here — PO created + reconciled to match the loaded basket; nothing placed.
  if (verifyOnly) return { poId, verifyOnly: true, cartUnits, verify: steps.verify, reconcile: steps.reconcile || null, soIds, steps };

  // 3) Place it (custref = our PO#). No re-upload; place the current (reconciled) cart.
  const r = await jfetch('checkout', `${altItemsUrl}/api/portwest-order`, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ purchaseOrder: String(poId), place: true }) });
  if (!r.placed) throw stepErr('checkout', `Portwest did not confirm placement (status ${r.status}): ${JSON.stringify(r.bodyPeek || r.error || r).slice(0, 250)}`, { poId, verify: steps.verify });
  const orderNo = r.orderNo || null;
  steps.checkout = { placed: true, orderNo, custref: r.sentCustref };

  // link + finalise (order# onto PO ref, status 7, SO notes + status 22 + tag clear)
  const ref = orderNo || `Placed-${poId}`;
  await bp.setOrderStatusLive(poId, bp.PLACED_WITH_SUPPLIER_STATUS);
  let refWritten = false;
  try { await bp.setOrderReferenceLive(poId, ref); refWritten = true; }
  catch (e) { steps.linkWarn = `reference-set failed (non-fatal): ${e.message}`; await bp.addOrderNoteLive(poId, `Placed with Portwest — order ${ref}. Reference-set failed: ${e.message}`, PORTWEST_SUPPLIER_CONTACT).catch(() => {}); }
  steps.link = { reference: ref, refWritten, orderNo, status: 7 };

  // ── A LINE PORTWEST WOULD NOT TAKE MUST NOT VANISH ──────────────────────────────────────────
  // Dropping it from the PO (above) is right: the PO must not claim stock nobody bought. Carrying
  // on instead of hard-aborting is right too — one dead code should not kill a 41-line order, which
  // is why d913882 changed it on 2026-08-14. What that change never added is the other half: SAYING
  // SO. The drop went into steps.reconcile and nowhere else, so the PO became honest while the
  // SALES ORDER became the lie — finalised to "Ordered Stock Awaiting Delivery" for a line that had
  // just been deleted.
  //
  // SO 487469 is what that costs. CD883DKR40 (a damage-claim replacement, "DAMAGE CLAIM CREDITED
  // VIA SC#487468") was gathered with toOrder 1 on 7 Sept and again on 11 Sept, dropped both times
  // because Portwest do not stock that colour, and finalised both times as ordered. Eight days, two
  // runs, no error row anywhere, and a customer waiting on it. Found only because someone asked.
  const soOf = (sku) => Object.entries(linesByOrder || {})
    .filter(([, items]) => (items || []).some((x) => String(x.sku || '').toUpperCase() === String(sku).toUpperCase()))
    .map(([id]) => id);
  const droppedForCustomers = [];
  for (const d of (steps.verify && steps.verify.dropped) || []) {
    const sos = soOf(d.sku);
    if (sos.length) droppedForCustomers.push({ ...d, soIds: sos });
  }
  if ((steps.verify && steps.verify.dropped || []).length) {
    await logPurchasingError(pool, {
      supplier: 'PORTWEST',
      step: droppedForCustomers.length ? 'customer-line-dropped' : 'low-inv-dropped',
      severity: droppedForCustomers.length ? 'error' : 'review',
      message: `Portwest would not take ${steps.verify.dropped.length} line(s); they were removed from PO#${poId} so it matches the order actually placed (${ref}). `
        + (droppedForCustomers.length
          ? `⚠ ${droppedForCustomers.length} of them are CUSTOMER lines — those sales orders are NOT fully ordered and nothing will chase them: `
            + droppedForCustomers.map((d) => `${d.want} × ${d.sku} (SO ${d.soIds.join(', ')})`).join('; ')
            + `. Source them elsewhere or credit the customer.`
          : `All were reorder lines, so no customer is waiting: `
            + steps.verify.dropped.map((d) => `${d.want} × ${d.sku}`).join('; ')),
      context: { poId, orderNo, dropped: steps.verify.dropped, droppedForCustomers },
    }).catch(() => {});
  }
  // The line STAYS dropped — Portwest will not take it, and a PO that claims otherwise is the
  // orphan-rows problem all over again. The SO still finalises too: holding it back would re-tag it
  // every night and re-drop it every night, which is noise, not a fix. What changes is that it is
  // no longer silent — the error row above is the notification, and a customer line raises it to
  // severity error so it lands in triage and on the hub instead of dying in steps.reconcile.
  try { steps.finalize = await bp.finalizeSupplierTagsLive({ orderIds: soIds, supplierKey: 'PORTWEST', poId, noteContactId: PORTWEST_SUPPLIER_CONTACT, setOrderedStatus: true, linesByOrder, execute: true }); } catch (e) { throw stepErr('finalize', `order placed + PO linked, but finalising SOs failed: ${e.message}`); }
  return { poId, orderNo, steps };
}

// ── PenCarrie placement chain (official pcautoorder XML API — no web basket) ──
// A DIRECT-order API (like Ralawise): build lines from the PO rows (BP SKU = PenCarrie prodcode
// "STYLE COLOUR SIZE") and submit pcautoorder with parkorder=2 (process for picking) + assumebo=1
// (PenCarrie auto-creates back orders for shortfalls their end). No basket/checkout/reconcile.
// `sandbox` forces the test gateway. Simplest placeFn of the lot. Contact 204.
const PENCARRIE_SUPPLIER_CONTACT = 204;
// ── did it actually land? ──────────────────────────────────────────────────
// Same failure class as Blaklader (see below): pcautoorder has no separate ack-then-confirm step,
// so a lost/timed-out response on OUR side says nothing about whether PenCarrie received and
// processed it — reproduced live 2026-09-02 on PO 486420 (error #139/140): checkout logged "fetch
// failed" three times, then a same-day retry got "Order ID (TUWO_TW486420) already exists" straight
// from PenCarrie, proving an earlier attempt HAD landed and only the response was lost. Ask
// PenCarrie's own order list before failing — never re-submit on an ambiguous result. ref = TW<poId>
// is what we send as custref, so it's also what identifies our order in pclist. Read-only.
async function pencarrieOrderForPo(altItemsUrl, poId) {
  const want = `TW${poId}`;
  try {
    const r = await fetch(`${altItemsUrl}/api/debug/pencarrie?fn=pclist&full=1`);
    const j = await r.json().catch(() => null);
    const raw = String((j && j.raw) || '');
    for (const m of raw.matchAll(/<order\b([^>]*)\/>/gi)) {
      const a = m[1];
      const at = (n) => (a.match(new RegExp(`${n}=['"]([^'"]*)['"]`, 'i')) || [])[1];
      if (at('custref') === want) {
        return { gateway: (j && j.gateway) || null, ordno: at('ordno') || null, ordcode: at('ordcode') || null,
          net: Number(at('net') || 0), vat: Number(at('vat') || 0), lineCount: Number(at('line_count') || 0),
          ordstat: at('ordstat') || null, created: at('created') || null };
      }
    }
  } catch { return null; }
  return null;
}
async function placePencarrieOrder(pool, altItemsUrl, { padToThreshold = 0, live = true, sandbox = false } = {}) {
  const steps = {};
  let po;
  try { po = await createPo({ supplierKey: 'PENCARRIE', execute: live, padToThreshold, logPool: pool }); }
  catch (e) { throw createPoErr(e); }
  if (!po.created) throw stepErr('create-po', `no PO created: ${po.reason || 'unknown'}` + (po.unresolvedSkus && po.unresolvedSkus.length ? ` — item codes not found in Brightpearl: ${po.unresolvedSkus.join(', ')}` : ''));
  const poId = po.poId;
  const soIds = [...new Set((po.soLines || []).map((l) => l.order).filter(Boolean))];
  const linesByOrder = {};
  for (const l of (po.soLines || [])) { if (l.order) (linesByOrder[l.order] = linesByOrder[l.order] || []).push({ sku: l.sku, qty: l.qty, name: l.name, productId: l.productId }); }
  steps.po = { poId, soUnits: po.soUnits, lowUnits: po.lowUnits, soIds, skippedBundles: po.skippedBundles || [] };

  // Order lines from the PO rows, carrying name + colour + size so /api/pencarrie-order can
  // RESOLVE each BP product → PenCarrie prodcode ("STYLE COLOUR SIZE") — many BP SKUs are numeric
  // internal codes (e.g. 14185 = Regatta RG045), not prodcodes.
  let orderLines;
  try { orderLines = (await bp.getOrderCartLines(poId)).filter((l) => l.sku).map((l) => ({ sku: String(l.sku), qty: Math.round(l.qty), name: l.name, colour: l.colour, size: l.size, ref: `PO${poId}` })); }
  catch (e) { throw stepErr('cart', `couldn't read PO ${poId} rows: ${e.message}`); }
  if (!orderLines.length) throw stepErr('cart', 'no orderable PenCarrie lines');
  steps.lines = { count: orderLines.length, units: orderLines.reduce((a, l) => a + l.qty, 0) };

  // Submit pcautoorder (LIVE gateway unless sandbox). The route resolves each line → prodcode and
  // REFUSES (ok:false + unresolved[]) if any line can't be mapped — so we never place a short order.
  // ref = TW<poId>; parkorder=2 processes for picking; assumebo=1 auto-back-orders shortfalls.
  let r, checkoutFailure = null;
  try {
    r = await jfetch('checkout', `${altItemsUrl}/api/pencarrie-order`, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ lines: orderLines, reference: `TW${poId}`, parkorder: 2, assumebo: 1, resolve: true, sandbox: !!sandbox }) });
  } catch (e) { checkoutFailure = e; }
  if (r && r.unresolved && r.unresolved.length) throw stepErr('resolve', `${r.unresolved.length} PenCarrie line(s) couldn't be mapped to a prodcode — NOT placing. PO#${poId} left for review. e.g. ${r.unresolved.slice(0, 6).map((u) => u.sku).join(', ')}`, { poId, unresolved: r.unresolved });
  if (checkoutFailure || !r || !r.ok) {
    // ASK PENCARRIE, not ourselves — see pencarrieOrderForPo above. Never retry on this: that is
    // exactly how one live order becomes two.
    const landed = !sandbox ? await pencarrieOrderForPo(altItemsUrl, poId).catch(() => null) : null;
    if (!landed) {
      const msg = checkoutFailure ? checkoutFailure.message : `PenCarrie did not confirm the order: ${JSON.stringify((r && (r.result || r.error || r.rawSnippet)) || r).slice(0, 250)}`;
      throw stepErr('checkout', msg, { poId, checkedSupplierOrderList: true });
    }
    steps.recovered = { via: 'pencarrie order list', foundOrder: landed.ordcode, ordno: landed.ordno };
    await logPurchasingError(pool, {
      supplier: 'PENCARRIE', step: 'checkout-recovered', severity: 'review',
      message: `The checkout call didn't confirm ("${String(checkoutFailure ? checkoutFailure.message : 'not ok').slice(0, 120)}") but the order IS at PenCarrie as ${landed.ordcode} (${landed.lineCount} lines, £${landed.net}). Marked placed from THEIR order list — re-running would have bought it twice.`,
      context: { poId, landed, checkoutFailure: checkoutFailure ? String(checkoutFailure.message).slice(0, 400) : null },
    }).catch(() => {});
    r = { ok: true, gateway: landed.gateway, ordercode: landed.ordcode, ordno: landed.ordno, custorderno: null, lines: [], recovered: true };
  }
  // 🔴 GATEWAY GUARD — Alternate-Items DEFAULTS to the sandbox gateway when neither PENCARRIE_ENV
  // nor PENCARRIE_GATEWAY is set, and a sandbox order answers exactly like a real one: ok:true,
  // sent:true, an ordercode, every line "confirmed". Without this check a scheduled run would place
  // into the sandbox and then mark the PO placed + finalise the SOs, so the demand would disappear
  // and NOTHING would have been bought — silently, every single day. Caught for real on
  // 2026-08-17: the first live attempt went to sandbox.pencarrie.com. Refuse before finalising.
  const PENCARRIE_LIVE_GATEWAY = 'https://pencarrie.com/gateway';
  if (!sandbox && String(r.gateway || '') !== PENCARRIE_LIVE_GATEWAY) {
    throw stepErr('gateway', `PenCarrie order went to ${r.gateway || 'an unknown gateway'}, not the LIVE gateway — NOT finalising, nothing has been bought. Set PENCARRIE_ENV=live on Alternate-Items. PO#${poId} left for review; order ${r.ordercode || '(none)'} exists on that gateway only.`, { poId, gateway: r.gateway || null, ordercode: r.ordercode || null });
  }
  steps.gateway = r.gateway;

  // price check (NON-FATAL) — pclist's `net` is PenCarrie's OWN order total, i.e. the "My Price"
  // rate we're actually invoiced. It is the only authoritative price source we have: the local
  // catalogue index holds LIST prices (~20% higher) and must never be used for this. Same alert
  // shape as Fristads/Castle, which Snickers also gained today.
  try {
    const poNet = +[...(po.soLines || []), ...(po.lowLines || [])].reduce((a, l) => a + (l.cost || 0) * l.qty, 0).toFixed(2);
    const code = String(r.ordercode || '');
    const lst = await jfetch('price-check', `${altItemsUrl}/api/debug/pencarrie?fn=pclist&full=1&ordcode=${encodeURIComponent(code)}`, { method: 'GET' });
    // `net` precedes `ordcode` in the order element, so anchor on our ordcode to be sure it's ours.
    const esc = code.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
    const m = new RegExp(`net="([\\d.]+)"[^>]*ordcode="${esc}"`).exec(String(lst.raw || ''));
    const theirNet = m ? Number(m[1]) : 0;
    const gap = theirNet ? +(theirNet - poNet).toFixed(2) : 0;
    steps.priceCheck = { theirNet: theirNet || null, poNet, gap };
    if (theirNet && Math.abs(gap) > 0.50) {
      await logPurchasingError(pool, {
        supplier: 'PENCARRIE', step: 'price-check', severity: 'review',
        message: `Prices don't match: PenCarrie order total £${theirNet} vs our PO net £${poNet} (diff £${gap}). A Brightpearl cost price (list 20) may need adjusting. Order ${code} still placed.`,
        context: { poId, ordercode: code, theirNet, poNet, gap },
      }).catch(() => {});
    }
  } catch (e) { steps.priceCheckWarn = e.message; }

  if (r.resolved) steps.resolved = { count: r.resolved };
  const orderNo = r.custorderno || r.ordercode || r.ordno || null;
  const backorderUnits = (r.lines || []).reduce((a, l) => a + (Number(l.backord) || 0), 0);
  steps.checkout = { ok: true, orderNo, ordercode: r.ordercode, custorderno: r.custorderno, backorderUnits, sandbox: !!sandbox };

  // Finalise: PO status 7 + ref, SO notes + status 22 + tag clear.
  const ref = orderNo || `Placed-${poId}`;
  await bp.setOrderStatusLive(poId, bp.PLACED_WITH_SUPPLIER_STATUS);
  let refWritten = false;
  try { await bp.setOrderReferenceLive(poId, ref); refWritten = true; }
  catch (e) { steps.linkWarn = `reference-set failed (non-fatal): ${e.message}`; await bp.addOrderNoteLive(poId, `Placed with PenCarrie — order ${ref}. Reference-set failed: ${e.message}`, PENCARRIE_SUPPLIER_CONTACT).catch(() => {}); }
  steps.link = { reference: ref, refWritten, orderNo, status: 7 };
  if (soIds.length) { try { steps.finalize = await bp.finalizeSupplierTagsLive({ orderIds: soIds, supplierKey: 'PENCARRIE', poId, noteContactId: PENCARRIE_SUPPLIER_CONTACT, setOrderedStatus: true, linesByOrder, execute: live }); } catch (e) { throw stepErr('finalize', `order placed + PO linked, but finalising SOs failed: ${e.message}`); } }
  return { poId, orderNo, steps };
}

// ── Blaklader placement chain (api.blaklader.com order API — no scraping) ─────
// Create PO → load the lines into the Blaklader basket → POST /order/orders (built from a
// template order for the account's buyer/address/payment+delivery values) → record the BLK
// internalId + finalise. SKUs are exact Blåkläder part numbers (no resolver/reconcile). Contact
// 323. ⚠️ The submit body is NOT yet validated against a real order — only run once confirmed.
const BLAKLADER_SUPPLIER_CONTACT = 323;
// ── did it actually land? ─────────────────────────────────────────────────────
// An AMBIGUOUS worker failure — a 25-minute timeout, a dropped connection — does NOT mean the order
// was not placed. BLK-1932804 on 2026-08-31 is the case that proves it: the order went in at 12:55,
// the worker then hung, the backend gave up at 13:18, and PO 485844 sat Pending with all five sales
// orders still tagged, primed to buy the same £1,465.28 again at 09:30 the next morning. It had
// already happened once (BLK-1929470, 08-27). Both times a human had to notice and reconcile by hand.
//
// Blaklader stamp OUR PO number onto the order (metadata.OrderNumber), so their own order list is
// the authority — not our guess about what the worker meant. Read it back before failing.
// NEVER retry on an ambiguous result: that is how one live order becomes two.
async function blakladerOrderForPo(altItemsUrl, poId, { scan = 8 } = {}) {
  const want = String(poId);
  let seen = [];
  try {
    // The endpoint answers a miss with 404 + the recent internalIds on the body, which is the only
    // way to enumerate; a raw fetch keeps that body where jfetch would throw it away.
    const r = await fetch(`${altItemsUrl}/api/blaklader-order-lines?internalId=__LOOKUP__&scan=${scan}`);
    const j = await r.json().catch(() => null);
    seen = (j && Array.isArray(j.seen) && j.seen) || [];
  } catch { return null; }
  for (const id of seen.slice(0, scan)) {
    try {
      const r2 = await fetch(`${altItemsUrl}/api/blaklader-order-lines?internalId=${encodeURIComponent(id)}&scan=${scan}`);
      const o = await r2.json().catch(() => null);
      if (o && String(o.orderNumber || '') === want) {
        return { internalId: o.internalId, orderNumber: o.orderNumber, created: o.created,
          lineCount: o.lineCount, units: o.units, total: o.totalAmount, linesNet: o.linesNet };
      }
    } catch { /* try the next one — a single unreadable order proves nothing */ }
  }
  return null;
}
async function placeBlakladerOrder(pool, altItemsUrl, { padToThreshold = 0, live = true, includeSalesOrders = true, includeLowInv = true } = {}) {
  const steps = {};
  let po;
  try { po = await createPo({ supplierKey: 'BLAKLADER', execute: live, padToThreshold, logPool: pool, includeSalesOrders, includeLowInv }); }
  catch (e) { throw createPoErr(e); }
  if (!po.created) throw stepErr('create-po', `no PO created: ${po.reason || 'unknown'}` + (po.unresolvedSkus && po.unresolvedSkus.length ? ` — item codes not found in Brightpearl: ${po.unresolvedSkus.join(', ')}` : ''));
  const poId = po.poId;
  const soIds = [...new Set((po.soLines || []).map((l) => l.order).filter(Boolean))];
  const linesByOrder = {};
  for (const l of (po.soLines || [])) { if (l.order) (linesByOrder[l.order] = linesByOrder[l.order] || []).push({ sku: l.sku, qty: l.qty, name: l.name, productId: l.productId }); }
  steps.po = { poId, soUnits: po.soUnits, lowUnits: po.lowUnits, soIds, skippedBundles: po.skippedBundles || [] };

  // Order lines = the PO rows (BP SKU = Blåkläder part number, exact — no resolver).
  // cost and name are sent for MULTIPACK detection, not for pricing. One BP unit can be a pack that
  // Blaklader sell per piece: PO 483480 ordered 1 x 362510428600L expecting five shirts and bought
  // ONE, because their 3625 is a "5 pcs multipack" at 4.55 an item against BP's 22.75 unit cost.
  // Alt-Items compares BP cost against their per-piece price and refuses unless that ratio and the
  // pack size in the name agree. Without the cost it has nothing to compare and cannot detect it.
  const orderLines = [...(po.soLines || []), ...(po.lowLines || [])].filter((l) => String(l.productId) !== '1000' && l.sku).map((l) => ({ sku: String(l.sku), qty: Math.round(l.qty), cost: l.cost, name: l.name }));
  // Carry-forward lines: owed to a sales order that is already finalised, so no demand scan will
  // ever ask for them again. Sent RAW — qty is already in Blaklader's own units (pieces), so the
  // multipack multiplier must not touch them.
  const pending = await listPendingLines(pool, 'BLAKLADER').catch(() => []);
  for (const p of pending) orderLines.push({ sku: String(p.sku), qty: Math.round(p.qty), rawQty: true, pendingId: p.id, note: p.note });
  if (pending.length) steps.pending = { count: pending.length, lines: pending.map((p) => `${p.sku} x${p.qty}${p.note ? ` (${p.note})` : ''}`) };
  if (!orderLines.length) throw stepErr('cart', 'no orderable Blaklader lines');
  steps.lines = { count: orderLines.length, units: orderLines.reduce((a, l) => a + l.qty, 0) };

  // STEP 1 — BASKET, via Alt-Items. This half always worked, and it is where multipack detection
  // and the "verify what actually landed" check live: their cart accepts codes it cannot resolve
  // and prunes them asynchronously, so an unverified basket is how you place a short order.
  let basket = { ok: true, packWarns: [], packNotes: [] };
  if (live) {
    basket = await jfetch('cart', `${altItemsUrl}/api/blaklader-basket`, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ lines: orderLines, clearFirst: true }) });
  }
  // A pack-size we could not verify stops the run with a message that names the SKU, rather than
  // ordering an unchecked multiple. That is the failure mode being fixed here, so it must not be
  // possible to sail past it.
  const pw = basket.packWarns || [];
  if (pw.length) throw stepErr('cart', `Blaklader pack size could not be verified on ${pw.length} line(s) — NOT ordered, PO#${poId} left for review: ${pw.map((w) => `${w.sku}: ${w.warn}`).join(' | ').slice(0, 300)}`, { poId, packWarns: pw });
  // The DETAIL goes in the context, not the message. stepErr truncates its message at 250 chars, so
  // both 2026-08-25 basket failures logged "...the basket does not match what was" and nothing that
  // said WHICH line — the one fact needed to fix it. context is not truncated.
  if (live && !basket.ok) {
    // createStatus/addStatus/cartId were already computed by blakladerAddToBasket (it returns them)
    // but never made it into this context, so every basket failure today read "0/N missing" with no
    // way to tell "their API rejected the create/add call outright" from "it was accepted and is
    // still populating" — the two have very different fixes. Both are cheap to include; neither was
    // being dropped on purpose.
    throw stepErr('cart', `the Blaklader basket does not hold what was asked for — NOT ordering. PO#${poId} left for review.`,
      { poId, missing: basket.missing || [], badSkus: basket.badSkus || [], cart: basket.cart ? { lines: basket.cart.lines, totalQty: basket.cart.totalQty } : null,
        cartId: basket.cartId || null, createStatus: basket.createStatus ?? null, addStatus: basket.addStatus ?? null,
        createError: basket.createError || null, addError: basket.addError || null });
  }

  // STEP 2 — build the order body from a template order. Reads the basket for its cartId; writes nothing.
  const prev = await jfetch('checkout', `${altItemsUrl}/api/blaklader-order`, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ lines: orderLines, purchaseOrder: String(poId), place: false }) });
  if (!prev.body) throw stepErr('checkout', `couldn't build the Blaklader order body: ${JSON.stringify(prev).slice(0, 200)}`, { poId });

  // STEP 3 — SUBMIT THROUGH THE PORTAL WORKER, never api.blaklader.com directly.
  // The direct call cannot check out: POST /api/orders/send answers 403 "Cart with id <guid> is not
  // allowed to be fetched" because the request carries no Blk._Auth storefront cookie
  // (hasStorefrontAuth:false). The worker drives a real browser session that HAS that cookie and
  // posts this exact body from inside the page, which is accepted — inspect on 2026-08-25 showed
  // hasStorefrontAuth:true against the API's false, side by side.
  // This wiring was the missing half of the 2026-08-24 worker build. Without it BOTH scheduled runs
  // on 2026-08-25 failed at the 403 with nothing ordered, the triage bot re-ran into the same 403,
  // and BLK-1926556 (GBP979) had to be placed by hand through this same worker.
  //
  // 2026-08-26, PO 484833: a SEPARATE failure, at the cart step (api.blaklader.com POST /cart/carts
  // 500 "Maximum length exceeded", every attempt 08:33-10:24 UK, 0/N lines every time). It cleared on
  // its own — replaying the exact same 35-line create by hand at 10:35 UK succeeded twice — so no
  // code fix went in for it; the theory (products array size / a stuck account-side draft) was never
  // confirmed and does not need to be for this to be worth recording as "not ours, and not current
  // any more" if it recurs.
  // Getting past that step surfaced a THIRD failure, here at checkout: orders/send 500 "Internal
  // Server Error" from inside the worker's browser session, twice in a row (10:39 and 10:44 UK),
  // basket populated correctly both times, no PO placed either time. No further detail is available
  // from this side — the worker's job record carries only that one line, and portal-order-worker's
  // own logs are not reachable from here. Do not re-run a third time on the same guess; if it is
  // still doing this, someone needs to read the worker's logs for that job id, or watch a checkout by
  // hand.
  // RECURRED 2026-08-31, error id 98, PO 485844, job job_mtgzdarfws9f62 — same "orders/send 500"
  // signature, basket held 29 lines / 67 units per force-run-safety, so checkout got as far as
  // populating the basket again. NEW THIS TIME: the worker's job record carried a screenshot (it
  // hadn't before), taken on the checkout page mid-failure — step "1. CART", "TOTAL: £0", no order
  // rows visible. That reads as the browser session's OWN cart being empty, distinct from the
  // API-owned cart (cartId in opts) force-run-safety confirms is populated — consistent with the
  // cart-ownership split already established above for the direct 403 (server-to-server auth
  // creates one cart, the storefront session another). If the worker checks out in its own session
  // instead of loading the passed cartId, it would be submitting an empty order and get exactly this
  // 500. Not confirmed, and not fixable from this repo: the browser automation is portal-order-worker,
  // a separate service whose logs and code are not reachable from here. Left unhandled again — do
  // not re-run on this guess. The basket this attempt left occupied is the real order, not a stray
  // rehearsal; let it ride along rather than clearing it, until a human has looked at the worker.
  let wr;
  if (!live) wr = { ok: true, orderNo: null, dryRun: true };
  else {
    let failure = null;
    try {
      wr = await workerPlaceOrder({ supplier: 'BLAKLADER', ref: poId, lines: orderLines, opts: { body: prev.body, cartId: prev.cartId }, execute: true,
        // Blaklader are the supplier that accepts and goes quiet, so they are the one that needs
        // this. Same lookup the recovery below uses, just asked while there is still time to save.
        confirmPlaced: () => blakladerOrderForPo(altItemsUrl, poId) });
      if (!wr || !wr.ok) failure = new Error(`Blaklader did not confirm the order: ${JSON.stringify((wr && (wr.error || wr)) || wr).slice(0, 250)}`);
    } catch (e) { wr = null; failure = e; }
    if (failure) {
      // ASK BLAKLADER, not ourselves. A timeout or a dropped connection says nothing about whether
      // the order landed, and guessing wrong in the optimistic direction loses an order while
      // guessing wrong in the pessimistic direction BUYS IT TWICE. Their list is the only authority.
      const landed = await blakladerOrderForPo(altItemsUrl, poId).catch(() => null);
      if (!landed) {
        throw stepErr('checkout', failure.message, { poId,
          worker: { status: (wr && wr.status) || null, note: (wr && wr.note) || null },
          repriced: !!(wr && wr.repriced), cartPricesRead: ((wr && wr.supplierPrices) || []).length || 0,
          cartView: (wr && wr.cartView) || (failure && failure.cartView) || null,
          checkedSupplierOrderList: true });
      }
      // IS THIS THE KNOWN ONE? Blaklader routinely accept an order and never answer the request:
      // 27 and 31 Aug, 1, 3 and 4 Sept, every time with the order sitting on their list. That is
      // not a fault to be told about each morning — the submit deadline catches it in five minutes,
      // their list confirms it, and the run finalises exactly as a clean one would. Alerting daily
      // on an outcome that is expected and already handled just teaches everyone to ignore
      // Blaklader alerts, and then the day it IS something else nobody looks.
      //
      // Narrow on purpose. Only a submit that went unanswered, or a run the worker abandoned on its
      // own ceiling, is treated as routine. A 500, an empty cart, a refused basket — anything that
      // says something actually went wrong — still alerts even though the order was recovered,
      // because those are not understood and one of them may not be recoverable next time.
      const quiet = !!(wr && (wr.submitTimedOut || wr.jobCeilingHit))
        || /did not answer within|abandoned this run after/i.test(String(failure.message || ''));
      steps.recovered = { workerError: String(failure.message).slice(0, 200), foundOrder: landed.internalId, via: 'blaklader order list', routine: quiet };
      await logPurchasingError(pool, {
        supplier: 'BLAKLADER', step: 'checkout-recovered', severity: quiet ? 'info' : 'review',
        notify: !quiet,
        message: quiet
          ? `Blaklader accepted the order and did not answer — their usual behaviour. Confirmed on THEIR order list as ${landed.internalId} (${landed.units} units, £${landed.total}) and finalised as normal. No action: the order is placed, and a re-run would buy it twice. Recorded without an alert because this is expected and already handled.`
          : `The worker failed ("${String(failure.message).slice(0, 120)}") but the order IS at Blaklader as ${landed.internalId} (${landed.units} units, £${landed.total}). Marked placed from THEIR order list — re-running would have bought it twice.`,
        context: { poId, landed, workerError: String(failure.message).slice(0, 400), routine: quiet },
      }).catch(() => {});
      wr = { ok: true, orderNo: landed.internalId, internalId: landed.internalId, recovered: true };
    }
  }
  const orderNo = wr.orderNo || wr.internalId || null;
  // packNotes records every line whose quantity was multiplied, so a 1-becomes-5 is visible in the
  // run report instead of only showing up on the invoice.
  // repriced/supplierPrices come from the cart view the worker now does before submitting. The
  // reprice is the only moment Blaklader's real prices are visible, and a run that did NOT reprice
  // is the state that returned 500 all day on 2026-08-26 — so record both.
  steps.checkout = { ok: true, orderNo, via: 'portal-worker', packMultiplied: basket.packNotes || [],
    repriced: wr.repriced === true, cartPricesRead: ((wr.supplierPrices || []).length) || 0 };
  // Only now, with the order actually placed. An aborted run leaves them pending for the next one —
  // marking them consumed any earlier would lose them silently, which is the failure this exists to
  // undo in the first place.
  if (pending.length) await consumePendingLines(pool, pending.map((p) => p.id), poId).catch(() => {});

  // PRICE CHECK against what Blaklader will ACTUALLY invoice. Blaklader had no price check at all,
  // and diffing PO 483751 against BLK-1923644 by hand on 2026-08-21 found two real cost errors that
  // nothing would otherwise have caught: 712018009900C40 at £13.65 against £22.45 charged (retail
  // £32.21, so it showed 58% margin and really made 30%), and 150113109900C60 at £43.15 vs £41.40.
  //
  // MULTIPACK LINES ARE EXCLUDED. One BP unit can be a pack Blaklader sell per piece — 3625104299004XL
  // is one unit of ours at £22.75 and five of theirs at £4.55. Comparing unit prices there would
  // "find" a 5x error on a line that reconciles perfectly, and healing it would destroy a correct
  // cost. packNotes already records exactly which lines were multiplied, so they are skipped.
  try {
    const packed = new Set((basket.packNotes || []).map((p) => String(p.sku || p).toUpperCase()));
    const inv = await jfetch('price-check', `${altItemsUrl}/api/blaklader-order-lines?internalId=${encodeURIComponent(orderNo || '')}`, { method: 'GET' });
    const theirs = new Map();
    for (const l of (inv.lines || [])) if (l.sku && Number(l.price) > 0) theirs.set(String(l.sku).toUpperCase(), Number(l.price));
    const changes = [];
    for (const l of orderLines) {
      const k = String(l.sku).toUpperCase();
      if (packed.has(k) || l.rawQty) continue;                 // multipack or carry-forward — not comparable per unit
      const now = theirs.get(k);
      if (now == null) continue;                               // NOT `l.cost > 0` — a £0 cost is the worst error there is, not an absent one
      if (Math.abs(now - l.cost) > 0.005) changes.push({ sku: l.sku, was: l.cost, now });
    }
    steps.priceCheck = { compared: theirs.size, skippedPacks: packed.size, differences: changes.length };
    await healPrices(steps, { supplierKey: 'BLAKLADER', poId, pool, changes });
  } catch (e) { steps.priceCheckWarn = `couldn't price-check against ${orderNo}: ${e.message}`; }

  // Finalise: PO status 7 + ref (BLK internalId), SO notes + status 22 + tag clear.
  const ref = orderNo || `Placed-${poId}`;
  await bp.setOrderStatusLive(poId, bp.PLACED_WITH_SUPPLIER_STATUS);
  let refWritten = false;
  try { await bp.setOrderReferenceLive(poId, ref); refWritten = true; }
  catch (e) { steps.linkWarn = `reference-set failed (non-fatal): ${e.message}`; await bp.addOrderNoteLive(poId, `Placed with Blaklader — order ${ref}. Reference-set failed: ${e.message}`, BLAKLADER_SUPPLIER_CONTACT).catch(() => {}); }
  steps.link = { reference: ref, refWritten, orderNo, status: 7 };
  if (soIds.length) { try { steps.finalize = await bp.finalizeSupplierTagsLive({ orderIds: soIds, supplierKey: 'BLAKLADER', poId, noteContactId: BLAKLADER_SUPPLIER_CONTACT, setOrderedStatus: true, linesByOrder, execute: live }); } catch (e) { throw stepErr('finalize', `order placed + PO linked, but finalising SOs failed: ${e.message}`); } }
  return { poId, orderNo, steps };
}

const SCHEDULED_SUPPLIERS = {
  FRISTADS: { supplierKey: 'FRISTADS', stateId: 1, placeFn: placeFristadsOrder, threshold: Number(process.env.FRISTADS_FREESHIP_THRESHOLD || 300) },
  CARHARTT: { supplierKey: 'CARHARTT', stateId: 6, placeFn: placeCarharttOrder, threshold: Number(process.env.CARHARTT_FREESHIP_THRESHOLD || 300) }, // Elastic Suite; also gated on Alt-Items by CARHARTT_PLACE_ENABLED
  'HELLY HANSEN': { supplierKey: 'HELLY HANSEN', stateId: 7, placeFn: placeHellyHansenOrder, threshold: Number(process.env.HELLYHANSEN_FREESHIP_THRESHOLD || 300) }, // Elastic Suite; gated on Alt-Items by HELLYHANSEN_PLACE_ENABLED
  // SNICKERS and BLAKLADER are SPLIT into two runs a day (owner, 2026-09-07).
  //
  // lineMode 'so' orders customer demand only, early; 'low' orders the reorder only, late. The
  // point is that the two have different urgency and different economics, and one combined PO gave
  // them the same treatment: a small reorder total could hold up customer stock under the
  // free-carriage threshold, while customer demand carrying the order over that line meant tiny
  // reorders shipped immediately whether they were worth shipping or not. Split, each half is
  // judged on its own — customers first thing, replenishment when it earns its carriage.
  //
  // Each half needs its OWN stateId: the day-claim is per state row, so sharing one would mean the
  // morning run claimed the day and the afternoon never fired.
  SNICKERS: { supplierKey: 'SNICKERS', stateId: 5, placeFn: placeSnickersOrder, threshold: Number(process.env.SNICKERS_FREESHIP_THRESHOLD || 300), lineMode: 'so' }, // Hultafors portal worker; £300 ex-VAT failsafe (rarely hit — high volume) so tiny orders accumulate instead of placing daily. Real Snickers carriage terms are "?" on the supplier sheet — confirm
  SNICKERS_LOW: { supplierKey: 'SNICKERS', stateId: 18, placeFn: placeSnickersOrder, threshold: Number(process.env.SNICKERS_FREESHIP_THRESHOLD || 300), lineMode: 'low' }, // reorder half, 16:40
  UNEEK: { supplierKey: 'UNEEK', stateId: 3, placeFn: placeUneekOrder, threshold: Number(process.env.UNEEK_FREESHIP_THRESHOLD || 100) }, // email supplier, free carriage @ £100 ex-VAT, no min order
  CASTLE: { supplierKey: 'CASTLE', stateId: 2, placeFn: placeCastleOrder, threshold: Number(process.env.CASTLE_FREESHIP_THRESHOLD || 150) }, // Castle free carriage @ £150 ex-VAT
  STERLING: { supplierKey: 'STERLING', stateId: 4, placeFn: placeSterlingOrder, threshold: Number(process.env.STERLING_FREESHIP_THRESHOLD || 150) },
  PORTWEST: { supplierKey: 'PORTWEST', stateId: 8, placeFn: placePortwestOrder, threshold: Number(process.env.PORTWEST_FREESHIP_THRESHOLD || 150) }, // portwest.com CSV upload + checkout_summary; free carriage @ £150 ex-VAT (else £7.50)
  PENCARRIE: { supplierKey: 'PENCARRIE', stateId: 9, placeFn: placePencarrieOrder, threshold: Number(process.env.PENCARRIE_FREESHIP_THRESHOLD || 175) }, // official pcautoorder XML API (parkorder=2); carriage paid @ £175 ex-VAT, else £8.70 (BRANDS_supplier_list_NEW_2025.xlsx "Supplier Info", 2026-08-17 — was a £150 guess)
  BLAKLADER: { supplierKey: 'BLAKLADER', stateId: 10, placeFn: placeBlakladerOrder, threshold: Number(process.env.BLAKLADER_FREESHIP_THRESHOLD || 300), lineMode: 'so' }, // api.blaklader.com order API (POST /order/orders); carriage paid @ £300 ex-VAT, else £13.00 (same sheet — was a £150 guess); submit body still needs first-order validation
  BLAKLADER_LOW: { supplierKey: 'BLAKLADER', stateId: 17, placeFn: placeBlakladerOrder, threshold: Number(process.env.BLAKLADER_FREESHIP_THRESHOLD || 300), lineMode: 'low' }, // reorder half, 16:20
  SCRUFFS: { supplierKey: 'SCRUFFS', stateId: 11, placeFn: placeScruffsOrder, threshold: Number(process.env.SCRUFFS_FREESHIP_THRESHOLD || 100) }, // email supplier (salesorders@scruffs.com), BP emails its own PO PDF; carriage minimum £100 ex-VAT — a £90 order was seen carrying carriage
  'PERFORMANCE BRANDS': { supplierKey: 'PERFORMANCE BRANDS', stateId: 12, placeFn: placePerformanceBrandsOrder, threshold: Number(process.env.PERFORMANCE_BRANDS_FREESHIP_THRESHOLD || 200) }, // WooCommerce trade shop; free delivery @ £200 ex-VAT (user), else £7.00 flat. Needs PERFORMANCE_BRANDS_USER/PASS on Alt-Items
  MASCOT: { supplierKey: 'MASCOT', stateId: 13, placeFn: placeMascotOrder, threshold: Number(process.env.MASCOT_FREESHIP_THRESHOLD || 250) }, // b2b.mascot.dk two-stage SAP commit (CreateOrder then ReleaseOrder); free carriage @ £250 ex-VAT. Basket shows LIST price (~1.695x our cost) — never threshold-test on it
  V12: { supplierKey: 'V12', stateId: 15, placeFn: placeV12Order, threshold: Number(process.env.V12_FREESHIP_THRESHOLD || 200) }, // V12 Footwear — email supplier; free carriage @ £200 ex-VAT, £6.95 below it and the charge goes ON the PO (owner, 2026-09-07)
  // threshold 0 — Hellberg has NO minimum order, so every day's demand goes rather than accumulating.
  // That is the point of the split: these take ~2 weeks through customs, so holding them back to reach
  // a carriage threshold would add a fortnight to a line that is already the slowest on the order.
  HELLBERG: { supplierKey: 'HELLBERG', stateId: 19, placeFn: placeHellbergOrder, threshold: 0 },
  BUCKLER: { supplierKey: 'BUCKLER', stateId: 16, placeFn: placeBucklerOrder, threshold: Number(process.env.BUCKLER_FREESHIP_THRESHOLD || 0) }, // Buckler Boots — email supplier; carriage terms not yet confirmed, so no threshold and no charge added until they are
  CHADWICK: { supplierKey: 'CHADWICK', stateId: 14, placeFn: placeChadwickOrder, threshold: Number(process.env.CHADWICK_FREESHIP_THRESHOLD || 300) }, // portal.chadwicktextiles.co.uk (wcp-ordupload then wcp-cartorder); free carriage @ £300 ex-VAT (user, 2026-08-21). weekdays 12:40 UK — the slot between Castle (12:00) and Sterling (13:00), after V12 at 12:20
};

// ── what the scheduler currently knows ───────────────────────────────────────
// Deploy safety used to be a pure guess from the clock: refuse from 10 minutes before a window
// until 45 minutes after it opens. Chain that across every supplier and the whole trading day is
// blocked bar a five-minute gap at 12:45 — so "fix it and get it back in before the next window"
// was impossible to do safely.
//
// The actual hazard is narrower. On 2026-08-19 the damage was done by restarting the service
// WHILE A RUN WAS IN FLIGHT: the day-claim never saved, so the poller fired again and placed a
// duplicate live order. A restart is harmless to a supplier that has ALREADY claimed today, because
// the claim stops it firing again regardless.
//
// So expose the two facts that decide it — is a run in flight, and who has claimed today — and let
// the caller reason from those instead of from the clock.
export const isRunInFlight = () => running;

// Every supplier the schedule knows about. Exported so a host process can prove it has armed a
// window for each one: a service that silently drops a supplier does not fail, it just never orders
// that supplier again, and nobody finds out until the stock runs out.
export const scheduledSupplierKeys = () => Object.keys(SCHEDULED_SUPPLIERS);

// Display-only window times, so a dashboard can say "runs at 10:30" without scraping server.js.
// THE AUTHORITY IS THE POLLERS in server.js (each one tests uk.hour/uk.minute itself) — this map
// only describes them. If a poller time changes, change it here too or the page will lie.
// Carhartt runs Mon/Wed/Fri only; everything else is every weekday.
// Supplier contactIds, so a PO can be attributed to a supplier without guessing at its reference
// (a PLACED PO no longer says "Auto-PO X" — it carries the supplier's own order number).
// Verified against real POs on 2026-09-02.
const SUPPLIER_CONTACT = {
  FRISTADS: 37419, CARHARTT: 65173, 'HELLY HANSEN': 214, SNICKERS: 331, UNEEK: 322,
  CASTLE: 332, STERLING: 341, PORTWEST: 298, PENCARRIE: 204, BLAKLADER: 323,
  SCRUFFS: 130243, 'PERFORMANCE BRANDS': 11611, MASCOT: 334, CHADWICK: 42485, V12: 92811, BUCKLER: 8981,
  HELLBERG: 331,   // shares the Snickers contact — the hub tells the two apart by which RUN placed the PO
};
const WINDOW_DISPLAY = {
  BLAKLADER: { at: '09:30' },
  SNICKERS: { at: '10:00' },
  FRISTADS: { at: '10:30' },
  'HELLY HANSEN': { at: '11:00' },
  MASCOT: { at: '11:30' },
  CASTLE: { at: '12:00' },
  STERLING: { at: '13:00' },
  CARHARTT: { at: '13:30', days: ['Mon', 'Wed', 'Fri'] },
  SCRUFFS: { at: '14:00' },
  'PERFORMANCE BRANDS': { at: '14:30' },
  PORTWEST: { at: '15:00' },
  PENCARRIE: { at: '15:40' },
  UNEEK: { at: '16:00' },
  V12: { at: '12:20' },
  BUCKLER: { at: '12:30' },
  HELLBERG: { at: '15:20' },
  CHADWICK: { at: '12:40' },
  // The reorder halves of the split suppliers. Deliberately last in the day: replenishment is not
  // customer-urgent, and by running after every other window they see the day's orders already on
  // the PO, so the reorder is calculated against what has actually been bought.
  BLAKLADER_LOW: { at: '16:20' },
  SNICKERS_LOW: { at: '16:40' },
};

export async function schedulerState(pool) {
  const uk = ukNow();
  const suppliers = [];
  for (const [key, cfg] of Object.entries(SCHEDULED_SUPPLIERS)) {
    let st = null;
    try { st = await getState(pool, cfg.stateId); } catch { /* a missing row is not fatal here */ }
    const lastRunDate = st && st.last_run_date ? ukDateStr(st.last_run_date) : null;
    const waited = st && st.working_days_waited != null ? Number(st.working_days_waited) : null;
    suppliers.push({
      supplier: key,
      stateId: cfg.stateId,
      lastRunDate,
      claimedToday: lastRunDate === uk.date,
      thresholdNet: cfg.threshold,
      // Why a supplier bought nothing is the question people actually ask, and it was only ever
      // answerable by re-running a dry run (which takes the run lock). These three make it readable.
      workingDaysWaited: waited,
      maxWaitDays: MAX_WAIT_WORKING_DAYS,
      willForceNextRun: waited != null ? waited + 1 >= MAX_WAIT_WORKING_DAYS : null,
      lastResult: (st && st.last_result) || null,
      window: WINDOW_DISPLAY[key] || null,
      contactId: SUPPLIER_CONTACT[key] || null,
    });
  }
  return {
    uk: { ...uk, isWeekday: isUkWeekday(uk.weekday) },
    running,
    suppliers,
    note: 'claimedToday=true means that supplier will NOT fire again today, so a restart cannot cause a duplicate run for it.',
  };
}

// ── one scheduled run (supplier-generic) ─────────────────────────────────────
// notify:false silences the report email and the error-log row for a run whose result is for
// COMPARISON only. The shadow runs on Purchasing-Automation use it: 18 suppliers dry-running every
// weekday would otherwise send 18 emails a day about orders nobody placed, and — worse — write
// duplicate rows into the shared purchasing_error_log, where the hub would count them as real
// failures and the triage routine would wake up to fix a service that is not live yet.
// Defaults to TRUE, so the live schedule's behaviour is unchanged.
export async function runSupplierScheduled({ pool, altItemsUrl, supplier = 'FRISTADS', dryRun = false, force = false, forcePlace = false, excludeSkus = [], notify = true } = {}) {
  const cfg = SCHEDULED_SUPPLIERS[String(supplier).toUpperCase()];
  if (!cfg) return { error: `unknown scheduled supplier ${supplier}` };
  const threshold = cfg.threshold ?? THRESHOLD_NET; // free-carriage threshold (ex-VAT), per supplier — `??` so a deliberate 0 (no minimum, e.g. Snickers) is honoured, not treated as "unset"
  if (running) return { skipped: 'a run is already in progress' };
  running = true;
  activePoId = null;                    // never carry a PO id across runs
  const uk = ukNow();
  // Declared OUT here because the CATCH reports it. A `const` inside the try is block-scoped and
  // does not exist in the catch, so referencing it there threw ReferenceError while building the
  // log context — over the top of the real error, and before .catch() was ever attached. That
  // aborted the rest of the catch, so the failure logged nothing, emailed nothing, fired no triage
  // and never wrote its state: the run just vanished, leaving the day claimed and the supplier
  // unordered. Cost 8 Sep 2026: Helly Hansen, Performance Brands and Portwest, all silent.
  const lineMode = cfg.lineMode || 'both';
  try {
    await ensureTable(pool);
    const state = await getState(pool, cfg.stateId);
    if (!force && !dryRun && state.last_run_date && ukDateStr(state.last_run_date) === uk.date) {
      return { skipped: `already ran today (${uk.date})`, ukTime: `${uk.weekday} ${uk.hour}:${String(uk.minute).padStart(2, '0')}` };
    }

    // dry-run the combined PO to value the demand (net, ex-VAT)
    // The value-check MUST see the same half the placement will order. Valuing the combined demand
    // for a reorder-only run would threshold-test it against money that run is never going to
    // spend, so it would place — or wait — on a figure that does not exist.
    const splitOpts = { includeSalesOrders: lineMode !== 'low', includeLowInv: lineMode !== 'so' };
    let plan;
    try { plan = await createPo({ supplierKey: cfg.supplierKey, execute: false, ...splitOpts }); }
    catch (e) { throw stepErr('value-check', `couldn't value the demand (Brightpearl down or demand read failed): ${e.message}`); }
    if (plan.unresolvedSkus && plan.unresolvedSkus.length) throw stepErr('value-check', `low-inventory item codes don't match any Brightpearl product: ${plan.unresolvedSkus.join(', ')}`);
    const lines = [...(plan.soLines || []), ...(plan.lowLines || [])];
    const netValue = Number(lines.reduce((a, l) => a + (l.cost || 0) * l.qty, 0).toFixed(2));
    const units = (plan.soUnits || 0) + (plan.lowUnits || 0);

    // A tag is a human saying "this supplier is needed on this order". Contributing no rows means
    // we disagree, and the usual cause is a product this supplier's brand detect doesn't know about
    // — Hellberg/EMMA/CLC were invisible to Snickers that way for ten days, and SO 482630 sat
    // tagged UNEEK since 18 Aug with five unordered lines. Report it EVERY day the demand is valued,
    // not only when an order is placed: a supplier that never reaches its threshold (Uneek) would
    // otherwise never say a word. severity 'review' — nothing is broken and nothing is blocked.
    if (plan.tagFlags && plan.tagFlags.length && !dryRun) {
      const detail = plan.tagFlags.map((f) => `SO ${f.soId} (tagged "${f.tag}") — ${f.reason}\n` +
        f.rows.slice(0, 12).map((r) => `      ${r.qty} × ${r.sku || '(no SKU)'}  ${r.name || ''}`).join('\n') +
        (f.rows.length > 12 ? `\n      …and ${f.rows.length - 12} more rows` : '')).join('\n\n');
      await logPurchasingError(pool, {
        // placed:false — this fires straight after the demand is valued, BEFORE the run has decided
        // whether to place anything, so it can never mean "the order went through". By definition
        // these orders contributed nothing, so nothing was placed FOR THEM either way.
        supplier: cfg.supplierKey, step: 'tagged-but-nothing-to-order', severity: 'review', placed: false,
        message: `${plan.tagFlags.length} order(s) are tagged for ${cfg.supplierKey} but contributed NOTHING to today's demand. `
          + `That usually means an item on them isn't recognised as a ${cfg.supplierKey} product — please check whether it should have been ordered.\n\n${detail}`,
        context: { supplier: cfg.supplierKey, orders: plan.tagFlags.map((f) => ({ soId: f.soId, tag: f.tag, reason: f.reason, rows: f.rows.length })) },
      }).catch(() => {});
    }

    let decision, willPlace = false, reason = null, newWaitDays = state.working_days_waited, padOnPlace = false;
    if (netValue <= 0) {
      decision = 'no demand'; newWaitDays = 0;
    } else {
      const over = netValue >= threshold;
      const wouldBeDay = state.working_days_waited + 1;
      // forcePlace: a deliberate MANUAL override to place NOW regardless of the free-carriage
      // threshold (e.g. an URGENT back-order of a single OOS line). Never set by the pollers.
      if (over) { willPlace = true; reason = 'over-threshold'; }
      else if (forcePlace) { willPlace = true; reason = `forced place (manual — under £${threshold}, threshold ignored)`; }
      else if (wouldBeDay >= MAX_WAIT_WORKING_DAYS) { willPlace = true; padOnPlace = true; reason = `held ${MAX_WAIT_WORKING_DAYS} working days (under £${threshold} — top up low-inv to reach free delivery, else carriage)`; }
      else { decision = `waiting — day ${wouldBeDay} of ${MAX_WAIT_WORKING_DAYS} (£${netValue} < £${threshold})`; newWaitDays = wouldBeDay; }
    }

    let placement = null;
    if (willPlace) {
      // On the final wait day (under threshold) pass the threshold so createComboPOLive
      // pads low-inv up to +40% above min to reach free delivery — else normal + carriage.
      const padTo = padOnPlace ? threshold : 0;
      if (dryRun) { decision = `WOULD place (${reason})`; }
      else {
        // CLAIM THE DAY BEFORE TOUCHING THE SUPPLIER. last_run_date used to be written only at the
        // very END of a run, so anything that killed the process between placing and finishing —
        // a deploy, a crash, an OOM — left NO record that we had run, and the next 5-minute tick
        // placed the SAME order again. That is not theoretical: on 2026-08-19 two deploys inside
        // the Fristads window produced orphan PO 483226, real order 2597307 (PO 483228) and then a
        // DUPLICATE live order 2597326 (PO 483231) — £539.85 ordered twice, caught by the user, and
        // returnable only with a restocking fee. `running` is a module-level lock so it dies with
        // the process too and protects nothing here.
        // Writing the date first flips the failure mode: a crash now means the order is SKIPPED and
        // visible (no run report, demand still queued) instead of DUPLICATED and invisible.
        // A deliberate re-run is still possible with force:true.
        await saveState(pool, { id: cfg.stateId, workingDaysWaited: 0, lastRunDate: uk.date, result: { supplier: cfg.supplierKey, ran: uk.date, claimedAt: `${uk.hour}:${String(uk.minute).padStart(2, '0')}`, state: 'placing — day claimed before contacting the supplier' } }).catch(() => {});
        placement = await cfg.placeFn(pool, altItemsUrl, { padToThreshold: padTo, excludeSkus, ...splitOpts });
        decision = `placed — ${reason}` + (lineMode === 'so' ? ' (customer orders)' : lineMode === 'low' ? ' (reorder)' : '');
        newWaitDays = 0;
      }
    }

    // ── REPORT WHAT WAS ORDERED, NOT WHAT WAS GATHERED ──────────────────────────────────────────
    // netValue/units describe the demand this run GATHERED. When the run fills a PO that already
    // held rows, that is not what goes to the supplier — the placement sends the whole PO.
    //
    // Snickers PO 489448 on 2026-09-15: the reorder lane subtracts on-order from need, so the 27
    // units already sitting on the pending PO suppressed themselves and the gather saw 4 units
    // (£179.13). The order placed was 31 units, £1,619.88 net, and Hultafors charged £1,671.64 —
    // while the report emailed "£179.13 ex-VAT (4 units)" directly above the order number. Anyone
    // reconciling that against the invoice is out by a factor of nine.
    //
    // Read the PO back and say what it actually contains. Best-effort: a failure here must never
    // turn a placed order into a failed run, so it degrades to the gathered figures as before.
    // getOrderCartLines DISCARDS the row cost — it exists to build supplier baskets, which only
    // need sku/size/qty. Summing a field it never returns gives £0.00, and a confident "Actually
    // ordered: £0.00" is worse than the wrong-but-plausible number this set out to replace. Read
    // the order itself and take itemCost, the same field getPoContributors uses.
    //
    // net stays NULL unless every orderable row carries a cost. A partial sum understates the
    // order, and understating what was committed is the failure being fixed here, not a lesser one.
    let ordered = null;
    if (placement && placement.poId && !dryRun) {
      try {
        const o = (await bp.bpLiveGet(`/order-service/order/${placement.poId}`))[0];
        const rows = Object.values((o && o.orderRows) || {}).filter((r) => String(r.productId) !== '1000');
        let net = 0, costed = 0;
        for (const r of rows) {
          const qty = parseFloat(r.quantity && r.quantity.magnitude) || 0;
          const cost = r.itemCost ? parseFloat(r.itemCost.value) : NaN;
          if (Number.isFinite(cost)) { net += qty * cost; costed++; }
        }
        ordered = {
          poId: placement.poId,
          lines: rows.length,
          units: rows.reduce((a, r) => a + (parseFloat(r.quantity && r.quantity.magnitude) || 0), 0),
          net: costed === rows.length && rows.length ? Number(net.toFixed(2)) : null,
        };
      } catch (e) { ordered = { poId: placement.poId, unreadable: e.message }; }
    }
    const report = { supplier: cfg.supplierKey, ran: uk.date, ukTime: `${uk.weekday} ${uk.hour}:${String(uk.minute).padStart(2, '0')}`, dryRun, netValue, units, threshold, decision, reason, workingDaysWaited: newWaitDays, placement, ordered };
    if (!dryRun) await saveState(pool, { id: cfg.stateId, workingDaysWaited: newWaitDays, lastRunDate: uk.date, result: report });
    if (notify) await sendReportEmail(report).catch(() => {});
    return report;
  } catch (e) {
    const step = e.step || 'unknown';
    const report = { supplier: cfg.supplierKey, ran: uk.date, dryRun, step, error: e.message };
    // persist to the error log + email a specific alert (what step, what went wrong)
    // lineMode is recorded so PO adoption can tell the two halves of a split supplier apart. Both
    // log under the same supplier name, so without it the 16:20 reorder run would happily adopt the
    // 09:30 customer run's orphaned draft, empty it, and refill it with reorder lines only —
    // dropping the customer lines from that PO with nothing to show it had happened.
    if (notify) await logPurchasingError(pool, { supplier: cfg.supplierKey, step, message: e.message, context: { dryRun, ukTime: `${uk.weekday} ${uk.hour}:${String(uk.minute).padStart(2, '0')}`, lineMode, ...(activePoId ? { poId: activePoId } : {}), ...(e.context || {}) } }).catch(() => {});
    if (!dryRun) { try { await saveState(pool, { id: cfg.stateId, workingDaysWaited: (await getState(pool, cfg.stateId)).working_days_waited, lastRunDate: uk.date, result: report }); } catch {} }
    return report;
  } finally { running = false; }
}


// ── the run that never came back ─────────────────────────────────────────────
// The day-claim is written BEFORE the supplier is contacted, so a run that dies after it leaves
// the claim behind and nothing else: no report, no error row, no alert email, no triage fire. The
// supplier is silently skipped for the day and its demand just sits there. Skipped-and-visible is
// the RIGHT trade against duplicated-and-invisible — but nothing ever actually LOOKED, so it was
// only visible in principle.
//
// 2026-09-08 is what that costs: Helly Hansen, Performance Brands and Portwest each ended the day
// wearing this marker, and all three were found by hand the next morning. The cause that day was a
// ReferenceError in the catch (fixed in 6c4abaf), but this hole is more general than that bug — the
// catch cannot run at all if the process is gone. A crash, an OOM, or a deploy landing mid-window
// produces the same silence, and 6c4abaf does nothing for any of them.
//
// The CLAIM TIMESTAMP is the ground truth here, not the window table. WINDOW_DISPLAY says of itself
// that it is display-only and that the pollers are the authority, so a drifted entry would make this
// either miss a failure or cry wolf. A run still wearing the marker an hour after it claimed is gone,
// whatever its window was.
const STUCK_CLAIM_MINUTES = 60;   // the longest legitimate run seen is a ~25 min Blaklader worker job

export async function sweepStuckClaims({ pool, execute = true } = {}) {
  const uk = ukNow();
  // A run in flight legitimately wears the marker, and `running` is per-process — so if the process
  // died, this is false and the check below is exactly the question we want to ask.
  if (running) return { skipped: 'a run is in flight', found: [] };
  const found = [];
  for (const key of Object.keys(SCHEDULED_SUPPLIERS)) {
    const cfg = SCHEDULED_SUPPLIERS[key];
    let state;
    try { state = await getState(pool, cfg.stateId); } catch { continue; }
    if (!state.last_run_date || ukDateStr(state.last_run_date) !== uk.date) continue;
    const res = state.last_result || {};
    // Anything that reported — placed, waiting, no demand, or a caught error — overwrote the marker.
    if (!/^placing/.test(String(res.state || ''))) continue;
    const m = String(res.claimedAt || '').match(/^(\d{1,2}):(\d{2})$/);
    if (!m) continue;
    const minutesAgo = (uk.hour * 60 + uk.minute) - (Number(m[1]) * 60 + Number(m[2]));
    if (minutesAgo < STUCK_CLAIM_MINUTES) continue;
    const lineMode = cfg.lineMode || 'both';
    // One row per supplier-half per day. Without this the 5-minute poller re-reports the same dead
    // run every tick and wakes triage each time.
    try {
      const dup = await pool.query(
        `SELECT 1 FROM purchasing_error_log
          WHERE upper(supplier) = $1 AND step = 'stuck-claim'
            AND created_at > now() - interval '20 hours'
            AND (context->>'lineMode') IS NOT DISTINCT FROM $2 LIMIT 1`,
        [String(cfg.supplierKey).toUpperCase(), lineMode]);
      if (dup.rows.length) continue;
    } catch { /* if the check fails, fall through and report — a duplicate row beats silence */ }
    found.push({ supplier: cfg.supplierKey, scheduleKey: key, claimedAt: res.claimedAt, minutesAgo, lineMode });
    if (!execute) continue;
    await logPurchasingError(pool, {
      supplier: cfg.supplierKey, step: 'stuck-claim', severity: 'error',
      // Deliberately NOT worded as "nothing was ordered". The claim is written before the supplier
      // is contacted, but the run can die at ANY point after that — including after checkout, with
      // the confirmation never read. Same rule as a worker timeout: absence of a result is not
      // evidence of absence of an order, and a re-run on that assumption is how stock gets bought
      // twice.
      message: `${cfg.supplierKey} claimed today at ${res.claimedAt} and never reported back — the run is gone `
        + `(${minutesAgo} min, no result, no error). It died between claiming the day and finishing: a crash, `
        + `an OOM, or a deploy landing inside its window. The day is claimed, so its own poller will NOT run `
        + `again today and the demand is still sitting there.\n\n`
        + `THIS IS NOT PROOF THE ORDER FAILED. The run may have reached the supplier before it died. `
        + `CHECK THE SUPPLIER'S OWN ORDER LIST AND BASKET before any re-run — force-run-safety first, `
        + `and a re-run on the assumption it placed nothing is how the same stock gets bought twice.`,
      context: { supplier: cfg.supplierKey, lineMode, claimedAt: res.claimedAt, minutesAgo, ran: res.ran, stuckClaim: true },
    }).catch(() => {});
  }
  return { uk: `${uk.weekday} ${uk.hour}:${String(uk.minute).padStart(2, '0')}`, execute, found };
}

// ── end-of-day retry of failures that never reached the supplier ─────────────
// PLACING THE ORDER IS THE GOAL. A run that failed BEFORE it ever contacted the supplier bought
// nothing, and the fix for it often lands within the hour — but TWO separate guards stop it ever
// trying again that day: the day-claim (last_run_date), and the poller's own 30-minute window.
//
// HELLY HANSEN on 2026-08-27 is the case this exists for. It failed at 11:03 on a transient
// Brightpearl blip, both fixes were deployed by 12:13, and it STILL bought nothing — its window
// (11:00–11:29) had closed and the day was already claimed. £454.77 of orderable demand, over the
// threshold and fully resolvable, simply waited for the next day.
//
// ONLY these steps are retried. Each provably happens BEFORE the PO exists and BEFORE any cart or
// portal contact, so a retry cannot duplicate an order:
const RETRY_SAFE_STEPS = new Set(['preflight', 'create-po', 'resolve']);
// Everything from 'cart' onwards is deliberately EXCLUDED, even though some of those never place
// either. A cart step leaves state on the SUPPLIER'S own system, and a 'checkout' failure may mean
// an order that DID go through and whose confirmation we failed to read — Blaklader timed out after
// 25 minutes on 2026-08-27 and the order had in fact been placed. Fail closed: an unrecognised or
// later step is never retried, because a duplicate order is far worse than a late one.
// supplierKey → { date, count }. A BUDGET of attempts per supplier per day, not a single shot.
//
// One attempt was right when the sweep only ran at 17:00, by which time any fix was long deployed.
// It is wrong as soon as the sweep runs during the day: a triage fix landing at 13:15 wants a retry
// soon after, but an attempt spent at 11:00 — before the fix existed — used up the only one, and
// the order then waited for tomorrow. On 2026-09-07 Sterling failed at "resolve" at 13:03, was
// fixed by 13:15, and would still have sat unplaced until the 17:00 sweep.
//
// A budget is safe by the same argument that makes the sweep safe at all: only pre-supplier steps
// are ever retried, so no attempt can reach the supplier twice. And it is self-limiting — if a
// retry gets further and then fails at 'cart' or later, the step is no longer retry-safe and the
// sweep stops considering it at all.
const RETRY_ATTEMPTS_PER_DAY = 3;
// The in-process map is only the same-process race guard (two sweeps cannot both claim an attempt).
// The COUNT that enforces the budget comes from the error log: on 2026-09-17 three deploys between
// 14:40 and 16:04 each restarted the process, each restart zeroed this map, and PenCarrie was
// retried four times on the same unresolvable code. Every attempt writes a 'retry-sweep' row, so
// counting today's rows survives any number of restarts.
const _retriedToday = new Map();
async function retrySweepAttemptsToday(pool, key, ukDate) {
  const q = await pool.query(
    "SELECT count(*)::int AS n FROM purchasing_error_log WHERE upper(supplier) = $1 AND step = 'retry-sweep' AND (created_at AT TIME ZONE 'Europe/London')::date = $2::date",
    [String(key).toUpperCase(), ukDate]);
  return (q.rows[0] && q.rows[0].n) || 0;
}

export async function retrySafeFailuresToday({ pool, altItemsUrl, execute = true } = {}) {
  const uk = ukNow();
  const suppliers = [];
  for (const key of Object.keys(SCHEDULED_SUPPLIERS)) {
    const cfg = SCHEDULED_SUPPLIERS[key];
    let state;
    try { state = await getState(pool, cfg.stateId); }
    catch (e) { suppliers.push({ supplier: key, skipped: 'state unreadable: ' + e.message }); continue; }
    if (!state.last_run_date || ukDateStr(state.last_run_date) !== uk.date) continue;   // never ran today
    const res = state.last_result || {};
    const spentMem = (() => { const r = _retriedToday.get(key); return r && r.date === uk.date ? r.count : 0; })();
    let spentDb = 0;
    try { spentDb = await retrySweepAttemptsToday(pool, key, uk.date); }
    catch (e) { suppliers.push({ supplier: key, skipped: 'retry count unreadable: ' + e.message }); continue; }  // cannot prove the budget -> do not spend it
    const spent = Math.max(spentMem, spentDb);
    const already = spent >= RETRY_ATTEMPTS_PER_DAY;

    // NO threshold re-evaluation here. A run that WAITED under the free-carriage threshold is
    // working as designed, and the sweep used to re-run it in the evening if the value had since
    // crossed the line. That was never what this was for, and on 2026-09-02 it did real damage:
    //   CASTLE  held at day 1 of 3 (GBP 93.35 < 150) and the sweep placed GBP 388.86 (PO 486453),
    //           an order Castle had deliberately held for free carriage.
    //   MASCOT  held at day 1 of 3 (GBP 242.15 < 250); the sweep re-ran it and SAP answered
    //           IsDuplicateRequest:true, leaving PO 486454 Pending. Mascot is the two-stage SAP
    //           commit where a failed Release leaves an INVISIBLE draft — the one supplier that
    //           must never be retried on a guess.
    // The wait is a deliberate business decision, not a failure to recover from. This sweep exists
    // ONLY to retry runs that FAILED before reaching the supplier and had no window left to fix in
    // (user, 2026-09-03).

    // CASE 2 - today's run FAILED before it ever reached the supplier.
    if (!RETRY_SAFE_STEPS.has(String(res.step))) {
      suppliers.push({ supplier: key, skipped: 'step "' + res.step + '" reached, or may have reached, the supplier', step: res.step });
      continue;
    }
    // Someone may have dealt with it BY HAND between the failure and the sweep. That is exactly what
    // happened to STERLING on 2026-08-28: the run failed at "resolve" on one line missing from the
    // product-data file, the order was then placed manually against PO 485458, and a blind retry
    // would have minted a fresh draft for the single line that still cannot resolve. If the PO the
    // failure recorded is no longer a DRAFT, the failure has been settled — skip it.
    let settledPo = null;
    try {
      const q = await pool.query(
        "SELECT context FROM purchasing_error_log WHERE upper(supplier) = $1 AND severity = 'error' AND context ? 'poId' ORDER BY id DESC LIMIT 1",
        [String(key).toUpperCase()]);
      const failedPo = q.rows[0] && q.rows[0].context && Number(q.rows[0].context.poId);
      if (failedPo) {
        const po = (await bp.bpLiveGet('/order-service/order/' + failedPo) || [])[0];
        const st = po && po.orderStatus && Number(po.orderStatus.orderStatusId);
        if (st && st !== 6) settledPo = { poId: failedPo, statusId: st };
      }
    } catch (e) { /* cannot tell -> fall through and retry, the guards below still apply */ }
    if (settledPo) {
      suppliers.push({ supplier: key, skipped: 'the PO its failure recorded is no longer a draft - already dealt with', poId: settledPo.poId, statusId: settledPo.statusId, step: res.step });
      continue;
    }
    if (already) { suppliers.push({ supplier: key, skipped: `already retried ${spent}/${RETRY_ATTEMPTS_PER_DAY} times today` }); continue; }
    if (!execute) { suppliers.push({ supplier: key, wouldRetry: true, step: res.step, attempt: spent + 1, error: String(res.error).slice(0, 140) }); continue; }
    // Claim BEFORE running, same discipline as the day-claim, so two sweeps cannot both retry.
    _retriedToday.set(key, { date: uk.date, count: spent + 1 });
    try {
      const r = await runSupplierScheduled({ pool, altItemsUrl, supplier: key, force: true });
      // A run that never happened must not cost an attempt. runSupplierScheduled returns
      // { skipped } when another run holds the lock — spending the budget on that would mean a
      // sweep unlucky enough to land mid-run silently used up the supplier's chances without ever
      // retrying anything. Hand it back and let the next sweep try.
      if (r && r.skipped) {
        _retriedToday.set(key, { date: uk.date, count: spent });
        suppliers.push({ supplier: key, skipped: `a run was in flight — attempt not spent (${spent}/${RETRY_ATTEMPTS_PER_DAY} used)` });
        continue;
      }
      const poId = r && r.placement && r.placement.poId;
      suppliers.push({ supplier: key, retried: true, afterStep: res.step, placed: !!poId, poId: poId || null, decision: (r && r.decision) || null, error: (r && r.error) || null });
      await logPurchasingError(pool, {
        supplier: key, step: 'retry-sweep', severity: poId ? 'info' : 'review', placed: !!poId,
        message: poId
          ? 'Retried after the "' + res.step + '" failure earlier today and PLACED (PO#' + poId + ').'
          : 'Retried after the "' + res.step + '" failure earlier today and it did NOT place: ' + ((r && (r.error || r.decision)) || 'no result'),
        context: { afterStep: res.step, retryOf: String(res.error).slice(0, 300), result: r || null },
      }).catch(() => {});
    } catch (e) {
      suppliers.push({ supplier: key, retried: true, afterStep: res.step, placed: false, error: e.message });
      // An attempt that threw still spent budget — record it, or the DB count would hand it back.
      await logPurchasingError(pool, {
        supplier: key, step: 'retry-sweep', severity: 'review', placed: false,
        message: 'Retried after the "' + res.step + '" failure earlier today and the retry itself threw: ' + e.message,
        context: { afterStep: res.step, retryOf: String(res.error).slice(0, 300), thrown: e.message },
      }).catch(() => {});
    }
  }
  return { ran: uk.date, ukTime: uk.weekday + ' ' + uk.hour + ':' + String(uk.minute).padStart(2, '0'), execute, suppliers };
}

// Back-compat wrapper — the 10:30 poller + existing /fristads-scheduled-run route call this.
export async function runFristadsScheduled(opts = {}) { return runSupplierScheduled({ ...opts, supplier: 'FRISTADS' }); }

// Portwest two-step first-order helpers (review-before-place). prepare = create PO + load
// the Portwest basket + verify it matches the PO, WITHOUT placing. place = place a PO that
// prepare already created + verified (custref = PO#) + finalise. Non-mutating vs mutating.
export async function portwestPrepare({ pool, altItemsUrl, poId = null, packSizes = {}, excludeSkus = [] }) { return placePortwestOrder(pool, altItemsUrl, { verifyOnly: true, poId: poId ? Number(poId) : null, packSizes, excludeSkus }); }
export async function portwestPlaceExisting({ pool, altItemsUrl, poId, packSizes = {}, excludeSkus = [] }) { return placePortwestOrder(pool, altItemsUrl, { poId, packSizes, excludeSkus }); }


// ── TELL THE PERSON WITH THE CUSTOMER, NOT JUST PURCHASING ───────────────────────────────────
// A dropped line is only half-reported by the error row: that tells purchasing. It does not tell
// whoever raised the sales order, and they are the one with a customer expecting the garment.
//
// This was built once already, on /api/purchasing/prepare-supplier-order — orderRecipient sends a
// website order's shortfall to sales and everything else to the staff member who created it. The
// scheduled runs replaced that route and never picked the notification up, so from the day the
// scheduler took over, every dropped line went quiet. SO 487469 sat eight days on the back of it.
//
// MATCH ON productId, NEVER ON SKU. A PO's SO rows carry Brightpearl's code (CB170321004) while
// the cart line carries the supplier's resolved one (125949-171-406); comparing the two matches
// nothing, which is the same trap that makes demandMatched unreliable on the Stuck items tab.
const DROP_NOTICE_REPEAT_DAYS = 7;

async function ensureDropNoticeTable(pool) {
  await pool.query(`CREATE TABLE IF NOT EXISTS dropped_line_notice (
    so_id integer NOT NULL, product_id text NOT NULL, supplier text, sku text, po_id integer,
    notified_to text, first_seen timestamptz DEFAULT now(), last_notified timestamptz DEFAULT now(),
    PRIMARY KEY (so_id, product_id))`);
}

// dropped: [{ productId, sku, qty, size, name, avail, deldate }]
// linesByOrder: the snapshot taken BEFORE the dropped lines were filtered out of it.
// `placed` says which of two very different things happened, and the email must not blur them:
//   true  — the order went through WITHOUT this line. Nothing will ever chase it. Act now.
//   false — the run stopped, so nothing was ordered at all. The SO keeps its supplier tag and is
//           picked up again on the next run, so this is a heads-up, not a task.
// Telling someone "nothing will chase this" about a line that retries in the morning is how a
// warning stops being read.
// ── EVERY OUTCOME IS RECORDED ─────────────────────────────────────────────────────────────────
// This function was silent on 2026-09-15 and again on the 17th: two Fristads drops, both with a
// customer waiting on SO 488357, and no email reached them. Every step in it is defensive — an
// unreadable dedupe table tells them twice, an unreadable order falls back to sales — so it should
// not be ABLE to go quiet, and yet nobody could say whether it ran, sent, skipped or threw, because
// the only trace was console.error on a Render box. A notification whose own delivery is
// unobservable is not a notification.
//
// So the wrapper below writes an 'info' row to the error log on EVERY exit — sent, skipped
// with the reason, or threw — carrying the recipients and the lines. It reads on the hub like any
// other row, and "did the salesperson get told?" becomes a query rather than an inference.
export async function notifyDroppedLines(pool, opts = {}) {
  let out;
  try { out = await notifyDroppedLinesInner(pool, opts); }
  catch (e) { out = { sent: 0, error: e.message, threw: true }; }
  try {
    if (pool && Array.isArray(opts.dropped) && opts.dropped.length) {
      const who = (out.detail || []).map((x) => x.to + (x.error ? ' ✗ ' + x.error : x.skipped ? ' (skipped: ' + x.skipped + ')' : ' ✓')).join('; ');
      const what = opts.dropped.map((d) => `${d.qty != null ? d.qty : d.want} × ${d.sku}`).join(', ');
      await pool.query(
        `INSERT INTO purchasing_error_log (supplier, step, message, context, severity) VALUES ($1,$2,$3,$4,$5)`,
        [opts.supplier || '?', 'dropped-line-notice',
          out.sent ? `Told ${out.sent} recipient(s) about dropped line(s) ${what}: ${who}`
            : `Dropped-line notice NOT sent for ${what} — ${out.error || out.reason || 'unknown'}${who ? ': ' + who : ''}`,
          JSON.stringify({ poId: opts.poId || null, sent: out.sent, reason: out.reason || null, error: out.error || null, detail: out.detail || null, dropped: opts.dropped }),
          'info']);
    }
  } catch { /* recording the outcome must never mask it */ }
  return out;
}
async function notifyDroppedLinesInner(pool, { supplier = 'FRISTADS', poId = null, dropped = [], linesByOrder = {}, execute = true, placed = true, force = false, backorderPoId = null } = {}) {
  if (!Array.isArray(dropped) || !dropped.length) return { sent: 0, reason: 'nothing dropped' };
  const label = supplier.charAt(0) + supplier.slice(1).toLowerCase();

  // productId where we have one, SKU only where we do not. Fristads resolves our code to theirs
  // (CB170321004 -> 125949-171-406) so only the productId can bridge the two; Portwest orders by
  // our SKU directly and carries no productId on a dropped line, where matching on SKU is exact.
  // Never fall back to SKU when a productId was supplied — that is how a near-miss becomes a
  // confident wrong answer.
  // Match on productId when BOTH sides have one — the one identifier nothing along the way can
  // mangle. Portwest's cart reads BIZ2NVRXXL back as "biz2"; matched on sku that is nothing, and
  // SO 487261 was reported as "no customer waiting" on its fourth silent drop (2026-09-17). Only
  // when a side lacks a productId does sku decide, and never as a fallback from a productId that
  // simply did not match — that near-miss is what makes demandMatched unreliable on the tab.
  const sosFor = (d) => Object.keys(linesByOrder || {}).filter((id) => (linesByOrder[id] || []).some((x) => (
    (d.productId != null && x.productId != null)
      ? String(x.productId) === String(d.productId)
      : String(x.sku || '').toUpperCase() === String(d.sku || '').toUpperCase())));

  // A line with no productId keys on its SKU instead, so the dedupe still holds for Portwest.
  const keyOf = (d) => String(d.productId != null ? d.productId : `sku:${String(d.sku || '').toUpperCase()}`);
  const perSo = new Map();  // soId -> [line]
  for (const d of dropped) {
    for (const soId of sosFor(d)) {
      if (!perSo.has(soId)) perSo.set(soId, []);
      perSo.get(soId).push(d);
    }
  }

  // No linesByOrder means this came from a supplier whose run does not build one — every supplier
  // except Fristads and Portwest. demand_log answers the same question for all of them: it records
  // po_id, so_id, product_id AND sku for every line a run gathered, so it bridges the two codes the
  // same way, and it is already what the Stuck items tab reads. Only consulted as a fallback,
  // because a run that HAS the mapping in hand knows it more precisely than the log does.
  if (!perSo.size && poId) {
    try {
      const d = await pool.query(
        `SELECT DISTINCT so_id, product_id, sku FROM demand_log WHERE po_id = $1 AND so_id IS NOT NULL`, [poId]);
      for (const line of dropped) {
        for (const row of d.rows) {
          const hit = line.productId != null && row.product_id != null
            ? String(row.product_id) === String(line.productId)
            : String(row.sku || '').toUpperCase() === String(line.sku || '').toUpperCase();
          if (!hit) continue;
          const soId = String(row.so_id);
          if (!perSo.has(soId)) perSo.set(soId, []);
          if (!perSo.get(soId).some((x) => x === line)) perSo.get(soId).push(line);
        }
      }
    } catch (e) { console.error('[dropped-line-notice] demand_log lookup failed:', e.message); }
  }
  if (!perSo.size) return { sent: 0, reason: 'no customer lines among the dropped ones' };

  // Don't re-tell someone the same thing every run. The same line is re-gathered and re-dropped
  // daily until it is resolved, with three retry attempts a day on top of that.
  const fresh = new Map();
  try {
    await ensureDropNoticeTable(pool);
    // force: a re-fire for a notice known to have gone astray. The dedupe exists to stop the same
    // line being re-told daily, not to stop a human deliberately telling it again.
    if (force) throw Object.assign(new Error('forced re-notify'), { forced: true });
    for (const [soId, lines] of perSo) {
      const keep = [];
      for (const d of lines) {
        const r = await pool.query(
          `SELECT last_notified FROM dropped_line_notice WHERE so_id = $1 AND product_id = $2
             AND last_notified > now() - ($3 || ' days')::interval`,
          [Number(soId), keyOf(d), DROP_NOTICE_REPEAT_DAYS]);
        if (!r.rows.length) keep.push(d);
      }
      if (keep.length) fresh.set(soId, keep);
    }
  } catch (e) {
    // A dedupe table we cannot read must not silence the notification — tell them twice rather
    // than not at all.
    console.error('[dropped-line-notice] dedupe unavailable:', e.message);
    for (const [k, v] of perSo) fresh.set(k, v);
  }
  if (!fresh.size) return { sent: 0, reason: 'already notified within ' + DROP_NOTICE_REPEAT_DAYS + ' days' };

  // Who to tell. Website orders go to sales, everything else to whoever raised it.
  const salesEmail = process.env.PURCHASING_SALES_EMAIL || 'sales@tuffshop.co.uk';
  const ids = [...fresh.keys()].map(Number).sort((a, b) => a - b);   // ascending, or BP 400s
  const orderById = new Map();
  try {
    for (let i = 0; i < ids.length; i += 50) {
      const got = await bp.bpLiveGet(`/order-service/order/${ids.slice(i, i + 50).join(',')}`);
      for (const o of got || []) orderById.set(String(o.id), o);
    }
  } catch (e) { console.error('[dropped-line-notice] order read failed:', e.message); }

  const byRecipient = new Map();
  for (const [soId, lines] of fresh) {
    const o = orderById.get(String(soId)) || {};
    let to = salesEmail;
    try {
      to = (await bp.orderRecipient({
        createdById: o.createdById || null,
        channelId: (o.assignment && o.assignment.current && o.assignment.current.channelId) || null,
      }, salesEmail)) || salesEmail;
    } catch { /* fall back to sales */ }
    if (!byRecipient.has(to)) byRecipient.set(to, []);
    byRecipient.get(to).push({ soId, ref: o.reference || '', customer: (o.parties && o.parties.customer && o.parties.customer.companyName) || '', lines });
  }

  const esc = (v) => String(v == null ? '' : v).replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;');
  const sent = [];
  for (const [to, orders] of byRecipient) {
    const count = orders.reduce((a, x) => a + x.lines.length, 0);
    const plural = count === 1 ? '' : 's';
    const subject = label + ' could not supply ' + count + ' line' + plural
      + (backorderPoId ? ' — on back order, PO#' + backorderPoId : placed ? '' : ' (order not placed)') + ' — ' + orders.map((x) => 'SO ' + x.soId).join(', ');
    const blocks = orders.map((x) => {
      const head = '<p><strong>SO ' + esc(x.soId) + '</strong>'
        + (x.ref ? ' — ' + esc(x.ref) : '') + (x.customer ? ' — ' + esc(x.customer) : '') + '</p>';
      const items = x.lines.map((d) => '<li><strong>' + esc(d.qty != null ? d.qty : d.want) + ' × ' + esc(d.sku) + '</strong>'
        + (d.size ? ' (' + esc(d.size) + ')' : '')
        + (d.name ? ' — ' + esc(d.name) : '')
        + (typeof d.avail === 'number' ? ' — ' + label + ' have ' + esc(d.avail) : '')
        + (d.deldate ? ', next delivery <strong>' + esc(d.deldate) + '</strong>' : '')
        + '</li>').join('');
      return head + '<ul>' + items + '</ul>';
    }).join('');
    // Three cases, three messages, because each asks something different of the reader:
    //   backorderPoId — the line is ON ORDER, on its own PO, with a date. Tell the customer; no action.
    //   placed        — the order went through WITHOUT it and nothing will chase it. Act.
    //   not placed    — the run stopped; it retries. A heads-up.
    const html = (backorderPoId
      ? '<p><strong>' + label + ' cannot supply the line' + plural + ' below yet, so '
        + (count === 1 ? 'it has' : 'they have') + ' been moved onto back-order PO#' + esc(backorderPoId)
        + ' and the rest of PO#' + esc(poId) + ' is shipping now.</strong></p>'
        + '<p>' + (count === 1 ? 'It is' : 'They are') + ' <strong>still ordered</strong> — ' + label + ' are holding '
        + (count === 1 ? 'it' : 'them') + ' on back order and will ship when stock lands; the expected date is against each line below. '
        + 'Nothing to do unless the customer cannot wait that long — then cancel the back order with ' + label + ', and source it elsewhere or credit.</p>'
      : placed
      ? '<p><strong>' + label + ' would not supply the line' + plural + ' below, so '
        + (count === 1 ? 'it was' : 'they were') + ' left off PO#' + esc(poId) + '.</strong> '
        + 'The rest of the order went through as normal.</p>'
        + '<p>Nothing will chase ' + (count === 1 ? 'this' : 'these') + ' automatically — '
        + 'it needs sourcing elsewhere, putting on back order with the customer, or crediting.</p>'
      : '<p><strong>' + label + ' could not supply the line' + plural + ' below, and the order was '
        + 'not placed because of ' + (count === 1 ? 'it' : 'them') + '.</strong></p>'
        + '<p>The order will be tried again on the next run, so there is nothing to do yet — but if '
        + (count === 1 ? 'this line is' : 'these lines are') + ' wrong or discontinued '
        + (count === 1 ? 'it' : 'they') + ' will keep holding the whole order up until someone '
        + 'changes ' + (count === 1 ? 'it' : 'them') + '.</p>')
      + blocks
      + '<p style="color:#666;font-size:12px">Sent once per line per ' + DROP_NOTICE_REPEAT_DAYS
      + ' days. You are getting this because you raised the order.</p>';
    if (!execute || !process.env.SMTP_PASS) {
      sent.push({ to, orders: orders.map((x) => x.soId), skipped: !execute ? 'dry run' : 'no SMTP_PASS' });
      continue;
    }
    try {
      // Keep what the SMTP relay actually SAID. "sendMail resolved" only means the relay accepted
      // the message; it says nothing about delivery — and noreply@tuffshop.co.uk goes out via
      // smtp2go, which sits in neither SPF nor DKIM under a p=quarantine DMARC, so an accepted
      // message can still be quarantined at the far end. The message-id and the relay's response
      // are the only evidence there is that anything left the building.
      const info = await transporter().sendMail({ from: '"Tuff Purchasing" <noreply@tuffshop.co.uk>', to, subject, html, text: subject });
      sent.push({ to, orders: orders.map((x) => x.soId), lines: count,
        messageId: info && info.messageId, response: info && String(info.response || '').slice(0, 120),
        accepted: info && info.accepted, rejected: info && info.rejected });
      for (const x of orders) {
        for (const d of x.lines) {
          await pool.query(
            `INSERT INTO dropped_line_notice (so_id, product_id, supplier, sku, po_id, notified_to)
             VALUES ($1,$2,$3,$4,$5,$6)
             ON CONFLICT (so_id, product_id) DO UPDATE SET last_notified = now(), notified_to = EXCLUDED.notified_to`,
            [Number(x.soId), keyOf(d), supplier, d.sku || null, poId, to]).catch(() => {});
        }
      }
    } catch (e) { sent.push({ to, error: e.message }); }
  }
  return { sent: sent.filter((x) => !x.error && !x.skipped).length, detail: sent };
}

function ukDateStr(d) { // normalise a pg date (Date or 'YYYY-MM-DD') to YYYY-MM-DD
  if (typeof d === 'string') return d.slice(0, 10);
  return new Intl.DateTimeFormat('en-CA', { timeZone: 'Europe/London', year: 'numeric', month: '2-digit', day: '2-digit' }).format(d);
}

async function sendReportEmail(report) {
  if (!process.env.SMTP_PASS) return;
  const t = nodemailer.createTransport({ host: process.env.SMTP_SERVER || 'mail-eu.smtp2go.com', port: parseInt(process.env.SMTP_PORT || '2525'), secure: false, auth: { user: process.env.SMTP_USERNAME || 'tuffshop.co.uk', pass: process.env.SMTP_PASS } });
  const p = report.placement;
  const S = report.supplier || 'FRISTADS';
  const label = S.charAt(0) + S.slice(1).toLowerCase(); // "Fristads" / "Castle"
  const dr = report.dryRun ? '[DRY RUN] ' : '';
  const subject = report.error ? `${dr}${label} auto-purchase: ERROR` : `${dr}${label} auto-purchase: ${report.decision}`;
  // Placement line — supplier-agnostic; extra fields (reservation/status/sum) only if present.
  const placedLine = p
    ? `<li>PO <strong>${p.poId}</strong> → ${label} order <strong>${p.orderNo}</strong>${p.reservationNo && p.reservationNo !== p.orderNo ? ` (reservation ${p.reservationNo})` : ''}${p.orderStatus ? `, ${p.orderStatus}` : ''}${p.sum ? `, ${p.sum} GBP` : ''}</li>`
    : '';
  const html = report.error
    ? `<p><strong>Error during the scheduled ${label} run (${report.ran}).</strong></p><pre>${report.error}</pre><p>Nothing may have been placed — check BP + the ${label} portal before the next run.</p>`
    : `<p>${label} auto-purchase — ${report.ukTime}${report.dryRun ? ' (DRY RUN)' : ''}</p>
       <ul>
         <li>Demand value: <strong>£${report.netValue}</strong> ex-VAT (${report.units} units), threshold £${report.threshold}</li>
         <li>Decision: <strong>${report.decision}</strong></li>
         ${report.ordered && report.ordered.lines
           ? `<li><strong>Actually ordered: ${report.ordered.net != null ? `£${report.ordered.net} ex-VAT ` : ''}(${report.ordered.units} units over ${report.ordered.lines} lines)</strong>`
             + (report.ordered.net == null
               ? ` on PO ${report.ordered.poId} — value not shown because at least one row carries no cost, so any total would understate the order.`
               : Number(report.ordered.net) !== Number(report.netValue)
                 // Say THAT they differ and which to trust. Do NOT assert why: the first version of
                 // this named a pre-existing PO as the cause, and the very first live run it
                 // described (Snickers 489574, 2026-09-16) was a PO created fresh by that same run —
                 // the gap was the price heal correcting 23 costs upward between the demand being
                 // valued and the PO being read back. A confident wrong reason is worse than none.
                 ? ` on PO ${report.ordered.poId} — <em>reconcile against this figure, not the demand value.</em>`
                   + ` The two differ when the PO already held rows, when a pack minimum rounds a line up,`
                   + ` or when a cost is healed after the demand was valued.`
                 : '')
             + `</li>`
           : ''}
         ${report.ordered && report.ordered.unreadable ? `<li>Could not read PO ${report.ordered.poId} back to confirm what was ordered: ${report.ordered.unreadable}</li>` : ''}
         ${placedLine}
         ${!p && report.workingDaysWaited ? `<li>Working days waited: ${report.workingDaysWaited} of ${MAX_WAIT_WORKING_DAYS}</li>` : ''}
       </ul>`;
  await t.sendMail({ from: '"Tuff Purchasing" <noreply@tuffshop.co.uk>', to: NOTIFY_TO, subject, html, text: subject + '\n\n' + JSON.stringify(report, null, 2) });
}
