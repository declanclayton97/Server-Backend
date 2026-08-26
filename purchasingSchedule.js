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
const MAX_WAIT_WORKING_DAYS = 3;
const NOTIFY_TO = process.env.PURCHASING_SCHEDULE_EMAIL || 'dec@tuffshop.co.uk';
const FRISTADS_SUPPLIER_CONTACT = 37419;
const CASTLE_SUPPLIER_CONTACT = 332;
const STERLING_SUPPLIER_CONTACT = 341;
const STERLING_WORKER_URL = process.env.STERLING_WORKER_URL || 'https://portal-order-worker.onrender.com';
const STERLING_WORKER_SECRET = process.env.STERLING_WORKER_SECRET || '';

let running = false; // in-process guard against overlapping runs

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
  await pool.query(`ALTER TABLE purchasing_error_log
    ADD COLUMN IF NOT EXISTS severity text,
    ADD COLUMN IF NOT EXISTS handled_at timestamptz,
    ADD COLUMN IF NOT EXISTS handled_by text,
    ADD COLUMN IF NOT EXISTS handled_note text`);
}

// Persist an error AND email an alert. Used for every failure in the flow.
// `placed` = did stock actually go to the supplier on this run? Until 2026-08-24 that was INFERRED
// from severity ('review' meant "the order went through"), which held for every review site except
// tagged-but-nothing-to-order — that one fires straight after the demand is valued, BEFORE the run
// has even decided whether to place. So a PenCarrie run that placed nothing still emailed "The
// order was placed", and force-run-safety refused a legitimate re-run on the same false reading.
// Left as null where the caller doesn't say, so every existing site keeps its current wording.
export async function logPurchasingError(pool, { supplier = 'FRISTADS', step = 'unknown', message = '', context = null, severity = 'error', placed = null } = {}) {
  let errorId = null;
  // Recorded IN the context so the stored row carries the fact too — an email is read once, but
  // force-run-safety and the triage routine read the row for the rest of the day.
  const ctx = placed === null ? context : { ...(context || {}), placed };
  try { if (pool) { await ensureErrorTable(pool); const r = await pool.query(`INSERT INTO purchasing_error_log (supplier, step, message, context, severity) VALUES ($1,$2,$3,$4,$5) RETURNING id`, [supplier, step, message, ctx ? JSON.stringify(ctx) : null, severity]); errorId = r.rows[0] && r.rows[0].id; } } catch (e) { console.error('[purchasing-error-log] insert failed:', e.message); }
  try { await sendAlertEmail({ supplier, step, message, context: ctx, severity, placed }); } catch (e) { console.error('[purchasing-error-log] email failed:', e.message); }
  try { await fireTriageRoutine({ supplier, step, message, context, severity, errorId }); } catch (e) { console.error('[purchasing-error-log] triage fire failed:', e.message); }
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
async function fireTriageRoutine({ supplier, step, message, context, severity, errorId }) {
  const url = process.env.TRIAGE_ROUTINE_URL, token = process.env.TRIAGE_ROUTINE_TOKEN;
  if (!url || !token) return;                 // not configured yet
  if (severity !== 'error') return;
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
async function jfetch(step, url, opts) {
  let res;
  try { res = await fetch(url, opts); }
  catch (e) { throw stepErr(step, `can't reach ${url} (website/network down?): ${e.message}`); }
  const text = await res.text();
  let j = null; try { j = text ? JSON.parse(text) : null; } catch { /* non-json */ }
  if (!res.ok) throw stepErr(step, `HTTP ${res.status} from ${url}: ${(j ? JSON.stringify(j) : text).slice(0, 250)}`);
  if (j && j.error) throw stepErr(step, `${url} returned error: ${j.error}`);
  return j;
}

// Feed the supplier's ACTUAL prices back onto the BP cost (see healSupplierCosts for the rules).
// Non-fatal. ON by default since 2026-08-19 (user); set PRICE_HEAL_ENABLED=false to put it back to
// it's switched on, and the run report still shows what it would have done. Anything outside the
// auto-heal band is raised as a price-heal error for a human rather than written.
async function healPrices(steps, { supplierKey, poId, changes, pool }) {
  if (!changes || !changes.length) return;
  try {
    const r = await bp.healSupplierCosts({ supplierKey, poId, changes, pool, execute: process.env.PRICE_HEAL_ENABLED !== 'false' });
    if (r.skipped && !Array.isArray(r.skipped)) { steps.priceHeal = r; return; }        // e.g. "no cost list of its own"
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
        supplier: supplierKey, step: 'price-heal-applied', severity: 'info',
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
        supplier: supplierKey, step: 'price-heal', severity: 'review',
        message: `${e.sku}: supplier charges £${Number(e.now).toFixed(2)} but BP cost (list ${r.listId}) is £${Number(e.was).toFixed(2)} — ${e.reason}`,
        context: { poId, ...e },
      }).catch(() => {});
    }
  } catch (e) { steps.priceHealWarn = e.message; }
}

async function placeFristadsOrder(pool, altItemsUrl, { padToThreshold = 0 } = {}) {
  const steps = {};
  // 1. create the combined PO (SO + low-inv + separator + notes; stamps the SOs)
  let po;
  try { po = await bp.createComboPOLive({ supplierKey: 'FRISTADS', execute: true, padToThreshold, logPool: pool }); }
  catch (e) { throw createPoErr(e); }
  if (!po.created) throw stepErr('create-po', `no PO created: ${po.reason || 'unknown'}` + (po.unresolvedSkus && po.unresolvedSkus.length ? ` — item codes not found in Brightpearl: ${po.unresolvedSkus.join(', ')}` : ''));
  const poId = po.poId;
  const soIds = [...new Set((po.soLines || []).map((l) => l.order).filter(Boolean))];
  // item names ordered per SO — for the SO note
  const linesByOrder = {};
  for (const l of (po.soLines || [])) { if (l.order) (linesByOrder[l.order] = linesByOrder[l.order] || []).push({ sku: l.sku, qty: l.qty, name: l.name }); }
  steps.po = { poId, soUnits: po.soUnits, lowUnits: po.lowUnits, soIds, skippedBundles: po.skippedBundles || [] };

  // 2. push the PO lines to the Fristads cart (unresolved = size/item not on the portal)
  const cartLines = await bp.getOrderCartLines(poId).catch((e) => { throw stepErr('cart', `couldn't read PO ${poId} rows: ${e.message}`); });
  const expectUnits = cartLines.reduce((a, l) => a + l.qty, 0);
  const cart = await jfetch('cart', `${altItemsUrl}/api/fristads-basket`, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ clearFirst: true, lines: cartLines }) });
  steps.cart = { cartCount: cart.cartCount, expectUnits, unresolved: cart.unresolved };
  if ((cart.unresolved || []).length) throw stepErr('cart', `size/item not found on the Fristads portal (codes don't match): ${JSON.stringify(cart.unresolved)}`);
  if (cart.cartCount !== expectUnits) throw stepErr('cart', `cart quantity mismatch: portal shows ${cart.cartCount}, expected ${expectUnits} — some lines didn't add`);

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
  const poNet = [...(po.soLines || []), ...(po.lowLines || [])].reduce((a, l) => a + (l.cost || 0) * l.qty, 0);
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
    const ourLines = [...(po.soLines || []), ...(po.lowLines || [])];
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
          if (!art || !(l.cost > 0) || !(l.qty > 0)) continue;
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
    await logPurchasingError(pool, {
      supplier: 'FRISTADS', step: 'price-check', severity: 'review',
      message: `Prices don't match: Fristads order total £${fristadsTotal} vs our PO net £${poNet.toFixed(2)} (diff £${priceGap}).${named} A Brightpearl cost price (Launch/list 20) may need adjusting. Order ${orderNo} still placed.`,
      context: { poId, orderNo, fristadsTotal, poNet: +poNet.toFixed(2), gap: priceGap, changes, ambiguous, poLines: breakdown },
    }).catch(() => {});

    // Fristads quote TRADE prices — the £4.25 insole proves it — not list like the Elastic
    // portals, so a difference pinned to a single SKU is safe to heal.
    if (changes.length) await healPrices(steps, { supplierKey: 'FRISTADS', poId, pool, changes });
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
  try { po = await bp.createComboPOLive({ supplierKey: 'CASTLE', execute: true, padToThreshold, logPool: pool }); }
  catch (e) { throw createPoErr(e); }
  if (!po.created) throw stepErr('create-po', `no PO created: ${po.reason || 'unknown'}` + (po.unresolvedSkus && po.unresolvedSkus.length ? ` — item codes not found in Brightpearl: ${po.unresolvedSkus.join(', ')}` : ''));
  const poId = po.poId;
  const soIds = [...new Set((po.soLines || []).map((l) => l.order).filter(Boolean))];
  const linesByOrder = {};
  for (const l of (po.soLines || [])) { if (l.order) (linesByOrder[l.order] = linesByOrder[l.order] || []).push({ sku: l.sku, qty: l.qty, name: l.name }); }
  steps.po = { poId, soUnits: po.soUnits, lowUnits: po.lowUnits, soIds, skippedBundles: po.skippedBundles || [] };

  // 2. push PO lines to the Castle basket (SKU-direct; size ignored by Castle).
  // Build from the PO-creation result (soLines/lowLines) — NOT getOrderCartLines:
  // the created BP PO row can degrade a variant SKU to the product's base SKU (e.g.
  // "177-GRY-L" → "177"), but the creation result keeps the SO row's real SKU.
  const cartLines = [...(po.soLines || []), ...(po.lowLines || [])]
    .filter((l) => String(l.productId) !== '1000' && l.sku)
    .map((l) => ({ sku: String(l.sku), qty: l.qty }));
  const expectUnits = cartLines.reduce((a, l) => a + l.qty, 0);
  const cart = await jfetch('cart', `${altItemsUrl}/api/castle-basket`, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ clearFirst: true, lines: cartLines }) });
  steps.cart = { cartCount: cart.cartCount, expectUnits, unresolved: cart.unresolved };
  if ((cart.unresolved || []).length) throw stepErr('cart', `item not found on the Castle portal (codes don't match): ${JSON.stringify(cart.unresolved)}`);
  if (cart.cartCount !== expectUnits) throw stepErr('cart', `cart quantity mismatch: portal shows ${cart.cartCount}, expected ${expectUnits} — some lines didn't add`);

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
async function workerPlaceOrder({ supplier = 'STERLING', ref, lines, execute, opts = null }) {
  const headers = { 'Content-Type': 'application/json', 'x-worker-secret': STERLING_WORKER_SECRET };
  const start = await jfetch('checkout', `${STERLING_WORKER_URL}/place-order`, {
    method: 'POST', headers,
    body: JSON.stringify({ supplier, ref: String(ref), lines, execute, async: true, ...(opts ? { opts } : {}) }),
  });
  const jobId = start && start.jobId;
  if (!jobId) throw stepErr('checkout', `worker didn't start a job: ${JSON.stringify(start)}`);
  const deadline = Date.now() + 25 * 60 * 1000;      // orders can be long; generous ceiling
  while (Date.now() < deadline) {
    await new Promise((r) => setTimeout(r, 12000));
    const j = await jfetch('checkout', `${STERLING_WORKER_URL}/job/${jobId}`, { headers });
    if (j.status === 'done') return j;               // carries ok/placed/orderNo/results/…
    if (j.status === 'error') throw stepErr('checkout', `worker job errored: ${j.error}`);
  }
  throw stepErr('checkout', `worker job ${jobId} timed out (still running after 25 min)`);
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
  try { po = await bp.createComboPOLive({ supplierKey: 'STERLING', execute: true, padToThreshold, logPool: pool }); }
  catch (e) { throw createPoErr(e); }
  if (!po.created) throw stepErr('create-po', `no PO created: ${po.reason || 'unknown'}` + (po.unresolvedSkus && po.unresolvedSkus.length ? ` — item codes not found in Brightpearl: ${po.unresolvedSkus.join(', ')}` : ''));
  const poId = po.poId;
  const soIds = [...new Set((po.soLines || []).map((l) => l.order).filter(Boolean))];
  const linesByOrder = {};
  for (const l of (po.soLines || [])) { if (l.order) (linesByOrder[l.order] = linesByOrder[l.order] || []).push({ sku: l.sku, qty: l.qty, name: l.name }); }
  steps.po = { poId, soUnits: po.soUnits, lowUnits: po.lowUnits, soIds, skippedBundles: po.skippedBundles || [] };

  // resolve each PO line (EAN -> search/colour/size); skip service lines; abort on genuinely-unresolved
  const { resolveSterlingLine, isNonSterlingOrderable } = await import('./sterlingResolve.js');
  const poLines = [...(po.soLines || []), ...(po.lowLines || [])].filter((l) => String(l.productId) !== '1000');
  const unresolved = [], skipped = [];
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
  if (!wr || !wr.placed) throw stepErr('checkout', `Sterling worker did not confirm placement: ${JSON.stringify((wr && (wr.results || wr.error)) || wr)}`);
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
  try { po = await bp.createComboPOLive({ supplierKey: 'UNEEK', execute: true, padToThreshold, logPool: pool }); }
  catch (e) { throw createPoErr(e); }
  if (!po.created) throw stepErr('create-po', `no PO created: ${po.reason || 'unknown'}` + (po.unresolvedSkus && po.unresolvedSkus.length ? ` — item codes not found in Brightpearl: ${po.unresolvedSkus.join(', ')}` : ''));
  const poId = po.poId;
  const soIds = [...new Set((po.soLines || []).map((l) => l.order).filter(Boolean))];
  const linesByOrder = {};
  for (const l of (po.soLines || [])) { if (l.order) (linesByOrder[l.order] = linesByOrder[l.order] || []).push({ sku: l.sku, qty: l.qty, name: l.name }); }
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
  try { po = await bp.createComboPOLive({ supplierKey: 'SCRUFFS', execute: true, padToThreshold, logPool: pool }); }
  catch (e) { throw createPoErr(e); }
  if (!po.created) throw stepErr('create-po', `no PO created: ${po.reason || 'unknown'}` + (po.unresolvedSkus && po.unresolvedSkus.length ? ` — item codes not found in Brightpearl: ${po.unresolvedSkus.join(', ')}` : ''));
  const poId = po.poId;
  const soIds = [...new Set((po.soLines || []).map((l) => l.order).filter(Boolean))];
  const linesByOrder = {};
  for (const l of (po.soLines || [])) { if (l.order) (linesByOrder[l.order] = linesByOrder[l.order] || []).push({ sku: l.sku, qty: l.qty, name: l.name }); }
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

async function placePerformanceBrandsOrder(pool, altItemsUrl, { padToThreshold = 0, live = true } = {}) {
  const steps = {};
  let po;
  try { po = await bp.createComboPOLive({ supplierKey: 'PERFORMANCE BRANDS', execute: live, padToThreshold, logPool: pool }); }
  catch (e) { throw createPoErr(e); }
  if (!po.created) throw stepErr('create-po', `no PO created: ${po.reason || 'unknown'}` + (po.unresolvedSkus && po.unresolvedSkus.length ? ` — item codes not found in Brightpearl: ${po.unresolvedSkus.join(', ')}` : ''));
  const poId = po.poId;
  const soIds = [...new Set((po.soLines || []).map((l) => l.order).filter(Boolean))];
  const linesByOrder = {};
  for (const l of (po.soLines || [])) { if (l.order) (linesByOrder[l.order] = linesByOrder[l.order] || []).push({ sku: l.sku, qty: l.qty, name: l.name }); }
  steps.po = { poId, soUnits: po.soUnits, lowUnits: po.lowUnits, soIds, skippedBundles: po.skippedBundles || [] };

  // name is sent as well as sku because a few of our products carry a Brightpearl-internal SKU with
  // no supplier code in it (191339 is the Y-Shield H3) and can only be found by the style code in
  // the NAME. cost is sent so Alt-Items can report where the supplier's live price disagrees with
  // our Launch cost — PB56C was £43.05 against £39.50 when this was built.
  const orderLines = [
    ...(po.soLines || []).map((l) => ({ ...l, lowInv: false })),
    ...(po.lowLines || []).map((l) => ({ ...l, lowInv: true })),
  ]
    .filter((l) => String(l.productId) !== '1000' && l.sku)
    // lowInv must survive this map: Alt-Items uses it to decide whether an unorderable line drops
    // out or kills the run.
    .map((l) => ({ sku: String(l.sku), qty: Math.round(l.qty), cost: l.cost, name: l.name, lowInv: !!l.lowInv }));
  if (!orderLines.length) throw stepErr('cart', 'no orderable Performance Brands lines');
  steps.lines = { count: orderLines.length, units: orderLines.reduce((a, l) => a + l.qty, 0) };

  const r = await jfetch('checkout', `${altItemsUrl}/api/performance-brands-order`, {
    method: 'POST', headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ lines: orderLines, purchaseOrder: String(poId), place: live }),
  });

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
    await logPurchasingError(pool, {
      supplier: 'PERFORMANCE BRANDS', step: 'low-inv-dropped', severity: 'review',
      message: `${dropped.length} low-inventory line(s) could NOT be ordered and were left off PO#${poId}. `
        + `Customer demand was unaffected and the order was placed. This supplier has no back-order route, `
        + `so these need stock or an email to sales@performance-brands.com:\n`
        + dropped.map((d) => `      ${d.qty} × ${d.sku} — ${d.reason}`).join('\n'),
      context: { poId, dropped },
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
  try { po = await bp.createComboPOLive({ supplierKey: 'MASCOT', execute: live, padToThreshold, logPool: pool }); }
  catch (e) { throw createPoErr(e); }
  if (!po.created) throw stepErr('create-po', `no PO created: ${po.reason || 'unknown'}` + (po.unresolvedSkus && po.unresolvedSkus.length ? ` — item codes not found in Brightpearl: ${po.unresolvedSkus.join(', ')}` : ''));
  const poId = po.poId;
  const soIds = [...new Set((po.soLines || []).map((l) => l.order).filter(Boolean))];
  const linesByOrder = {};
  for (const l of (po.soLines || [])) { if (l.order) (linesByOrder[l.order] = linesByOrder[l.order] || []).push({ sku: l.sku, qty: l.qty, name: l.name }); }
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
  try { po = await bp.createComboPOLive({ supplierKey: 'CHADWICK', execute: live, padToThreshold, logPool: pool }); }
  catch (e) { throw createPoErr(e); }
  if (!po.created) throw stepErr('create-po', `no PO created: ${po.reason || 'unknown'}` + (po.unresolvedSkus && po.unresolvedSkus.length ? ` — item codes not found in Brightpearl: ${po.unresolvedSkus.join(', ')}` : ''));
  const poId = po.poId;
  const soIds = [...new Set((po.soLines || []).map((l) => l.order).filter(Boolean))];
  const linesByOrder = {};
  for (const l of (po.soLines || [])) { if (l.order) (linesByOrder[l.order] = linesByOrder[l.order] || []).push({ sku: l.sku, qty: l.qty, name: l.name }); }
  steps.po = { poId, soUnits: po.soUnits, lowUnits: po.lowUnits, soIds, skippedBundles: po.skippedBundles || [] };

  // Chadwick key on their ITEM CODE, which most of our SKUs already are (882-01-A-L). Some products
  // carry a Brightpearl-internal code instead (ML070622072) which their upload will silently drop —
  // the basket line-count check in Alt-Items catches that and refuses rather than ordering short.
  const orderLines = [...(po.soLines || []), ...(po.lowLines || [])]
    .filter((l) => String(l.productId) !== '1000' && l.sku)
    .map((l) => ({ sku: String(l.sku), qty: Math.round(l.qty), cost: l.cost, name: l.name }));
  if (!orderLines.length) throw stepErr('cart', 'no orderable Chadwick lines');
  steps.lines = { count: orderLines.length, units: orderLines.reduce((a, l) => a + l.qty, 0) };

  const r = await jfetch('checkout', `${altItemsUrl}/api/chadwick-order`, {
    method: 'POST', headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ lines: orderLines, purchaseOrder: String(poId), place: live }),
  });
  if (!r.ok) {
    const miss = (r.missing && r.missing.length) ? ` — item codes Chadwick did not accept: ${r.missing.join(', ').slice(0, 200)}` : '';
    throw stepErr(r.step || 'checkout', `Chadwick did not confirm the order: ${String(r.error || JSON.stringify(r)).slice(0, 250)}${miss}`, { poId, missing: r.missing });
  }

  const orderNo = r.orderNo || null;
  steps.checkout = { ok: true, orderNo, cartCount: r.cartCount, rid: r.rid };

  const ref = orderNo || `Placed-${poId}`;
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
function snickersPackMultiples() {
  let env = {};
  try { env = JSON.parse(process.env.SNICKERS_PACK_MULTIPLES || '{}'); } catch { /* bad JSON → built-ins only, never blocks a run */ }
  const out = { ...SNICKERS_PACK_MULTIPLES };
  for (const [k, v] of Object.entries(env)) { const n = Number(v); if (n > 1) out[String(k).toUpperCase()] = n; }
  return out;
}
async function placeSnickersOrder(pool, altItemsUrl, { padToThreshold = 0, live = true } = {}) {
  const steps = {};
  let po;
  try { po = await bp.createComboPOLive({ supplierKey: 'SNICKERS', execute: live, padToThreshold, logPool: pool }); }
  catch (e) { throw createPoErr(e); }
  if (!po.created) throw stepErr('create-po', `no PO created: ${po.reason || 'unknown'}` + (po.unresolvedSkus && po.unresolvedSkus.length ? ` — item codes not found in Brightpearl: ${po.unresolvedSkus.join(', ')}` : ''));
  const poId = po.poId;
  const soIds = [...new Set((po.soLines || []).map((l) => l.order).filter(Boolean))];
  const linesByOrder = {};
  for (const l of (po.soLines || [])) { if (l.order) (linesByOrder[l.order] = linesByOrder[l.order] || []).push({ sku: l.sku, qty: l.qty, name: l.name }); }
  steps.po = { poId, soUnits: po.soUnits, lowUnits: po.lowUnits, soIds, skippedBundles: po.skippedBundles || [] };

  // Worker lines = the PO's SKUs (skip the =====LOW INV==== separator productId 1000), summed
  // per SKU. Build from soLines/lowLines (FULL SKUs); the /po-cart-lines route truncates them.
  const bySku = new Map();
  for (const l of [...(po.soLines || []), ...(po.lowLines || [])]) {
    if (String(l.productId) === '1000' || !l.sku) continue;
    const k = String(l.sku).toUpperCase();
    bySku.set(k, (bySku.get(k) || 0) + Math.round(l.qty));
  }
  const packs = snickersPackMultiples();
  const packApplied = [];
  const lines = [...bySku.entries()].map(([stockCode, qty]) => {
    const p = Number(packs[stockCode]);
    if (p > 1 && qty % p !== 0) {
      const q = Math.ceil(qty / p) * p;
      packApplied.push({ sku: stockCode, demand: qty, ordered: q, packOf: p });
      return { stockCode, qty: q };
    }
    return { stockCode, qty };
  });
  if (!lines.length) throw stepErr('resolve', 'no orderable Snickers lines');
  if (packApplied.length) steps.packRounding = packApplied;
  steps.resolve = { lines: lines.length, units: lines.reduce((a, l) => a + l.qty, 0) };

  // Drive the Hultafors worker (async job + poll). ref = PO id → the portal PO-number field.
  const wr = await workerPlaceOrder({ supplier: 'SNICKERS', ref: poId, lines, execute: live });
  if (!wr || !wr.placed) {
    // NAME THE LINES. Hultafors' CSV import drops what it will not accept and reports nothing, so
    // the unit gate refuses with no clue which line caused it. On 2026-08-25 that meant £4,350 sat
    // on their checkout page and the two culprits (a discontinued code and one sold in 20s) were
    // found only by diffing the PO against the worker's cart by hand. The worker now returns
    // missingLines; put it where a person and the triage pass will both see it.
    const miss = (wr && wr.missingLines) || [];
    const named = miss.map((m) => `${m.stockCode} (wanted ${m.wanted}${m.inCart ? `, only ${m.inCart} in cart` : ', not in basket'})`).join('; ');
    throw stepErr('checkout',
      `Snickers worker did not confirm placement${miss.length ? ` — ${miss.length} line(s) never reached the basket: ${named.slice(0, 160)}` : ''}: ${JSON.stringify((wr && (wr.error || wr.statusText)) || wr).slice(0, 200)}`,
      // context is NOT truncated — the full list belongs here, with the counts that prove the gap.
      { poId, missingLines: miss, expectedUnits: (wr && wr.expectedUnits) || null, cartUnits: (wr && wr.cart && wr.cart.qtySum) || null });
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
    await logPurchasingError(pool, {
      supplier: 'SNICKERS', step: 'price-check', severity: 'review',
      message: `Prices don't match: Hultafors basket £${cartTotal || '?'} vs our PO net £${orderedNet} (diff £${gap}).`
        + (lineGaps.length ? ` Brightpearl cost (Snickers/list 10) looks wrong on: ${lineGaps.map((g) => `${g.sku} ours £${g.ours} vs theirs £${g.theirs}`).join('; ')}.` : '')
        + ` Order ${orderNo} still placed.`,
      context: { poId, orderNo, cartTotal, orderedNet, gap, lineGaps, packRounding: packApplied },
    }).catch(() => {});
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
  if (soIds.length) { try { steps.finalize = await bp.finalizeSupplierTagsLive({ orderIds: soIds, supplierKey: 'SNICKERS', poId, noteContactId: SNICKERS_SUPPLIER_CONTACT, setOrderedStatus: true, linesByOrder, execute: live }); } catch (e) { throw stepErr('finalize', `order placed + PO linked, but finalising SOs failed: ${e.message}`); } }
  // heal BP costs from what Hultafors actually charges (lineGaps = their unit price vs our cost)
  await healPrices(steps, { supplierKey: 'SNICKERS', poId, changes: lineGaps.map((g) => ({ sku: g.sku, was: g.ours, now: g.theirs })), pool });
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
async function withVariantDetail(lines) {
  const need = lines.filter((l) => !l.colour || !l.size).map((l) => l.productId).filter(Boolean);
  if (!need.length) return lines;
  try {
    const ids = [...new Set(need)].sort((a, b) => a - b);   // Brightpearl 400s on an unsorted id set
    const arr = await bp.bpLiveGet(`/product-service/product/${ids.join(',')}`) || [];
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
async function placeElasticOrder(pool, altItemsUrl, { supplierKey, contactId, basketPath, padToThreshold = 0, live = true } = {}) {
  const steps = {};
  // Pre-flight (only on a real run): value the demand + confirm the portal resolves EVERY line
  // BEFORE creating the BP PO, so a resolution miss can't leave an orphan PO. (The PO is created
  // before the portal submit, so without this an unresolved line would strand a Pending PO.)
  // It ALSO harvests the portal's live wholesale price per SKU (pricedLines) → priceOverrides,
  // so the PO net reconciles to the supplier invoice instead of trusting BP's stored cost.
  let priceOverrides = null;
  if (live) {
    let preview;
    try { preview = await bp.createComboPOLive({ supplierKey, execute: false }); }
    catch (e) { throw stepErr('preflight', `couldn't value the demand: ${e.message}`); }
    // name/colour/size go with the line so the portal can fall back to style + colour NAME + size
    // when neither our SKU nor our EAN is in its sheet. 802211-001L aborted the whole Carhartt run
    // on 2026-08-21 for exactly that: it is style SC4223M in Black/L, in the sheet with 5,718
    // available, but our SKU carries a legacy code and our EAN appears nowhere in the sheet.
    const preLines = [...(preview.soLines || []), ...(preview.lowLines || [])]
      .filter((l) => String(l.productId) !== '1000' && l.sku)
      .map((l) => ({ sku: l.sku, qty: l.qty, name: l.name, colour: l.colour, size: l.size, productId: l.productId }));
    await withVariantDetail(preLines);
    if (preLines.length) {
      const dry = await jfetch('preflight', `${altItemsUrl}${basketPath}`, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ lines: preLines, dryRun: true }) });
      if (dry.unresolved && dry.unresolved.length) throw stepErr('preflight', `${supplierKey} lines not on the portal (PO NOT created): ${dry.unresolved.join(', ')} — fix the resolver/aliases first`);
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
  try { po = await bp.createComboPOLive({ supplierKey, execute: live, padToThreshold, priceOverrides, logPool: pool }); }
  catch (e) { throw createPoErr(e); }
  if (!po.created) throw stepErr('create-po', `no PO created: ${po.reason || 'unknown'}` + (po.unresolvedSkus && po.unresolvedSkus.length ? ` — item codes not found in Brightpearl: ${po.unresolvedSkus.join(', ')}` : ''));
  const poId = po.poId;
  const soIds = [...new Set((po.soLines || []).map((l) => l.order).filter(Boolean))];
  const linesByOrder = {};
  for (const l of (po.soLines || [])) { if (l.order) (linesByOrder[l.order] = linesByOrder[l.order] || []).push({ sku: l.sku, qty: l.qty, name: l.name }); }
  steps.po = { poId, soUnits: po.soUnits, lowUnits: po.lowUnits, soIds, skippedBundles: po.skippedBundles || [], priceOverridesApplied: po.priceOverridesApplied || [] };

  // Order lines = the PO's SKUs (skip the =====LOW INV==== separator), summed per SKU.
  const bySku = new Map();
  for (const l of [...(po.soLines || []), ...(po.lowLines || [])]) {
    if (String(l.productId) === '1000' || !l.sku) continue;
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
const PORTWEST_TEMP_EXCLUDE = ['P351WHR', '196109'];
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
    try { po = await bp.createComboPOLive({ supplierKey: 'PORTWEST', execute: true, padToThreshold, logPool: pool }); }
    catch (e) { throw createPoErr(e); }
    if (!po.created) throw stepErr('create-po', `no PO created: ${po.reason || 'unknown'}` + (po.unresolvedSkus && po.unresolvedSkus.length ? ` — item codes not found in Brightpearl: ${po.unresolvedSkus.join(', ')}` : ''));
    poId = po.poId;
    soIds = [...new Set((po.soLines || []).map((l) => l.order).filter(Boolean))];
    linesByOrder = {};
    for (const l of (po.soLines || [])) { if (l.order) (linesByOrder[l.order] = linesByOrder[l.order] || []).push({ sku: l.sku, qty: l.qty, name: l.name }); }
    steps.po = { poId, soUnits: po.soUnits, lowUnits: po.lowUnits, soIds, skippedBundles: po.skippedBundles || [] };
  }
  // Upload lines = the ACTUAL PO ROWS (canonical Portwest codes, e.g. P351WHR — the SO demand
  // may carry a different internal SKU like 196109 that Portwest's portal doesn't recognise).
  let poRowLines;
  try { poRowLines = (await bp.getOrderCartLines(poId)).filter((l) => l.sku).map((l) => ({ sku: String(l.sku), qty: Math.round(l.qty) })); }
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
  if (packSizes && Object.keys(packSizes).length) {
    cartLines = cartLines.map((l) => { const p = Number(packSizes[String(l.sku).toUpperCase()]); if (p > 1) { const q = Math.max(1, Math.ceil(l.qty / p)); if (q !== l.qty) { packApplied.push({ sku: l.sku, demandUnits: l.qty, packs: q, packOf: p }); return { ...l, qty: q }; } } return l; });
    if (packApplied.length) steps.packConversion = packApplied;
  }
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
  const got = new Map(); for (const l of cart) got.set(nk(l.sku), (got.get(nk(l.sku)) || 0) + (Number(l.qty) || 0));
  // Diff the CART against the ORIGINAL PO ROWS (not the pack-adjusted upload) so BOTH Portwest's
  // carton rounding and our own pack rounding surface as bumps to apply to the PO.
  const bumped = [], droppedLines = [], matched = [];
  for (const l of poRowLines) { const g = got.get(nk(l.sku)) || 0; if (g === 0) droppedLines.push({ sku: l.sku, want: l.qty }); else if (g !== l.qty) bumped.push({ sku: l.sku, from: l.qty, to: g }); else matched.push(l.sku); }
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
  if (soIds.length) { try { steps.finalize = await bp.finalizeSupplierTagsLive({ orderIds: soIds, supplierKey: 'PORTWEST', poId, noteContactId: PORTWEST_SUPPLIER_CONTACT, setOrderedStatus: true, linesByOrder, execute: true }); } catch (e) { throw stepErr('finalize', `order placed + PO linked, but finalising SOs failed: ${e.message}`); } }
  return { poId, orderNo, steps };
}

// ── PenCarrie placement chain (official pcautoorder XML API — no web basket) ──
// A DIRECT-order API (like Ralawise): build lines from the PO rows (BP SKU = PenCarrie prodcode
// "STYLE COLOUR SIZE") and submit pcautoorder with parkorder=2 (process for picking) + assumebo=1
// (PenCarrie auto-creates back orders for shortfalls their end). No basket/checkout/reconcile.
// `sandbox` forces the test gateway. Simplest placeFn of the lot. Contact 204.
const PENCARRIE_SUPPLIER_CONTACT = 204;
async function placePencarrieOrder(pool, altItemsUrl, { padToThreshold = 0, live = true, sandbox = false } = {}) {
  const steps = {};
  let po;
  try { po = await bp.createComboPOLive({ supplierKey: 'PENCARRIE', execute: live, padToThreshold, logPool: pool }); }
  catch (e) { throw createPoErr(e); }
  if (!po.created) throw stepErr('create-po', `no PO created: ${po.reason || 'unknown'}` + (po.unresolvedSkus && po.unresolvedSkus.length ? ` — item codes not found in Brightpearl: ${po.unresolvedSkus.join(', ')}` : ''));
  const poId = po.poId;
  const soIds = [...new Set((po.soLines || []).map((l) => l.order).filter(Boolean))];
  const linesByOrder = {};
  for (const l of (po.soLines || [])) { if (l.order) (linesByOrder[l.order] = linesByOrder[l.order] || []).push({ sku: l.sku, qty: l.qty, name: l.name }); }
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
  const r = await jfetch('checkout', `${altItemsUrl}/api/pencarrie-order`, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ lines: orderLines, reference: `TW${poId}`, parkorder: 2, assumebo: 1, resolve: true, sandbox: !!sandbox }) });
  if (r.unresolved && r.unresolved.length) throw stepErr('resolve', `${r.unresolved.length} PenCarrie line(s) couldn't be mapped to a prodcode — NOT placing. PO#${poId} left for review. e.g. ${r.unresolved.slice(0, 6).map((u) => u.sku).join(', ')}`, { poId, unresolved: r.unresolved });
  if (!r.ok) throw stepErr('checkout', `PenCarrie did not confirm the order: ${JSON.stringify(r.result || r.error || r.rawSnippet || r).slice(0, 250)}`, { poId });
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
async function placeBlakladerOrder(pool, altItemsUrl, { padToThreshold = 0, live = true } = {}) {
  const steps = {};
  let po;
  try { po = await bp.createComboPOLive({ supplierKey: 'BLAKLADER', execute: live, padToThreshold, logPool: pool }); }
  catch (e) { throw createPoErr(e); }
  if (!po.created) throw stepErr('create-po', `no PO created: ${po.reason || 'unknown'}` + (po.unresolvedSkus && po.unresolvedSkus.length ? ` — item codes not found in Brightpearl: ${po.unresolvedSkus.join(', ')}` : ''));
  const poId = po.poId;
  const soIds = [...new Set((po.soLines || []).map((l) => l.order).filter(Boolean))];
  const linesByOrder = {};
  for (const l of (po.soLines || [])) { if (l.order) (linesByOrder[l.order] = linesByOrder[l.order] || []).push({ sku: l.sku, qty: l.qty, name: l.name }); }
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
        cartId: basket.cartId || null, createStatus: basket.createStatus ?? null, addStatus: basket.addStatus ?? null });
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
  const wr = live
    ? await workerPlaceOrder({ supplier: 'BLAKLADER', ref: poId, lines: orderLines, opts: { body: prev.body, cartId: prev.cartId }, execute: true })
    : { ok: true, orderNo: null, dryRun: true };
  if (!wr.ok) throw stepErr('checkout', `Blaklader did not confirm the order: ${JSON.stringify(wr.error || wr).slice(0, 250)}`, { poId, worker: { status: wr.status || null, note: wr.note || null } });
  const orderNo = wr.orderNo || wr.internalId || null;
  // packNotes records every line whose quantity was multiplied, so a 1-becomes-5 is visible in the
  // run report instead of only showing up on the invoice.
  steps.checkout = { ok: true, orderNo, via: 'portal-worker', packMultiplied: basket.packNotes || [] };
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
      if (now == null || !(l.cost > 0)) continue;
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
  SNICKERS: { supplierKey: 'SNICKERS', stateId: 5, placeFn: placeSnickersOrder, threshold: Number(process.env.SNICKERS_FREESHIP_THRESHOLD || 300) }, // Hultafors portal worker; £300 ex-VAT failsafe (rarely hit — high volume) so tiny orders accumulate instead of placing daily. Real Snickers carriage terms are "?" on the supplier sheet — confirm
  UNEEK: { supplierKey: 'UNEEK', stateId: 3, placeFn: placeUneekOrder, threshold: Number(process.env.UNEEK_FREESHIP_THRESHOLD || 100) }, // email supplier, free carriage @ £100 ex-VAT, no min order
  CASTLE: { supplierKey: 'CASTLE', stateId: 2, placeFn: placeCastleOrder, threshold: Number(process.env.CASTLE_FREESHIP_THRESHOLD || 150) }, // Castle free carriage @ £150 ex-VAT
  STERLING: { supplierKey: 'STERLING', stateId: 4, placeFn: placeSterlingOrder, threshold: Number(process.env.STERLING_FREESHIP_THRESHOLD || 150) },
  PORTWEST: { supplierKey: 'PORTWEST', stateId: 8, placeFn: placePortwestOrder, threshold: Number(process.env.PORTWEST_FREESHIP_THRESHOLD || 150) }, // portwest.com CSV upload + checkout_summary; free carriage @ £150 ex-VAT (else £7.50)
  PENCARRIE: { supplierKey: 'PENCARRIE', stateId: 9, placeFn: placePencarrieOrder, threshold: Number(process.env.PENCARRIE_FREESHIP_THRESHOLD || 175) }, // official pcautoorder XML API (parkorder=2); carriage paid @ £175 ex-VAT, else £8.70 (BRANDS_supplier_list_NEW_2025.xlsx "Supplier Info", 2026-08-17 — was a £150 guess)
  BLAKLADER: { supplierKey: 'BLAKLADER', stateId: 10, placeFn: placeBlakladerOrder, threshold: Number(process.env.BLAKLADER_FREESHIP_THRESHOLD || 300) }, // api.blaklader.com order API (POST /order/orders); carriage paid @ £300 ex-VAT, else £13.00 (same sheet — was a £150 guess); submit body still needs first-order validation
  SCRUFFS: { supplierKey: 'SCRUFFS', stateId: 11, placeFn: placeScruffsOrder, threshold: Number(process.env.SCRUFFS_FREESHIP_THRESHOLD || 100) }, // email supplier (salesorders@scruffs.com), BP emails its own PO PDF; carriage minimum £100 ex-VAT — a £90 order was seen carrying carriage
  'PERFORMANCE BRANDS': { supplierKey: 'PERFORMANCE BRANDS', stateId: 12, placeFn: placePerformanceBrandsOrder, threshold: Number(process.env.PERFORMANCE_BRANDS_FREESHIP_THRESHOLD || 200) }, // WooCommerce trade shop; free delivery @ £200 ex-VAT (user), else £7.00 flat. Needs PERFORMANCE_BRANDS_USER/PASS on Alt-Items
  MASCOT: { supplierKey: 'MASCOT', stateId: 13, placeFn: placeMascotOrder, threshold: Number(process.env.MASCOT_FREESHIP_THRESHOLD || 250) }, // b2b.mascot.dk two-stage SAP commit (CreateOrder then ReleaseOrder); free carriage @ £250 ex-VAT. Basket shows LIST price (~1.695x our cost) — never threshold-test on it
  CHADWICK: { supplierKey: 'CHADWICK', stateId: 14, placeFn: placeChadwickOrder, threshold: Number(process.env.CHADWICK_FREESHIP_THRESHOLD || 300) }, // portal.chadwicktextiles.co.uk (wcp-ordupload then wcp-cartorder); free carriage @ £300 ex-VAT (user, 2026-08-21). NO POLLER YET — runnable only via /api/purchasing/supplier-scheduled-run until a slot is agreed
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

export async function schedulerState(pool) {
  const uk = ukNow();
  const suppliers = [];
  for (const [key, cfg] of Object.entries(SCHEDULED_SUPPLIERS)) {
    let st = null;
    try { st = await getState(pool, cfg.stateId); } catch { /* a missing row is not fatal here */ }
    const lastRunDate = st && st.last_run_date ? ukDateStr(st.last_run_date) : null;
    suppliers.push({
      supplier: key,
      stateId: cfg.stateId,
      lastRunDate,
      claimedToday: lastRunDate === uk.date,
      thresholdNet: cfg.threshold,
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
export async function runSupplierScheduled({ pool, altItemsUrl, supplier = 'FRISTADS', dryRun = false, force = false, forcePlace = false } = {}) {
  const cfg = SCHEDULED_SUPPLIERS[String(supplier).toUpperCase()];
  if (!cfg) return { error: `unknown scheduled supplier ${supplier}` };
  const threshold = cfg.threshold ?? THRESHOLD_NET; // free-carriage threshold (ex-VAT), per supplier — `??` so a deliberate 0 (no minimum, e.g. Snickers) is honoured, not treated as "unset"
  if (running) return { skipped: 'a run is already in progress' };
  running = true;
  const uk = ukNow();
  try {
    await ensureTable(pool);
    const state = await getState(pool, cfg.stateId);
    if (!force && !dryRun && state.last_run_date && ukDateStr(state.last_run_date) === uk.date) {
      return { skipped: `already ran today (${uk.date})`, ukTime: `${uk.weekday} ${uk.hour}:${String(uk.minute).padStart(2, '0')}` };
    }

    // dry-run the combined PO to value the demand (net, ex-VAT)
    let plan;
    try { plan = await bp.createComboPOLive({ supplierKey: cfg.supplierKey, execute: false }); }
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
        placement = await cfg.placeFn(pool, altItemsUrl, { padToThreshold: padTo });
        decision = `placed — ${reason}`; newWaitDays = 0;
      }
    }

    const report = { supplier: cfg.supplierKey, ran: uk.date, ukTime: `${uk.weekday} ${uk.hour}:${String(uk.minute).padStart(2, '0')}`, dryRun, netValue, units, threshold, decision, reason, workingDaysWaited: newWaitDays, placement };
    if (!dryRun) await saveState(pool, { id: cfg.stateId, workingDaysWaited: newWaitDays, lastRunDate: uk.date, result: report });
    await sendReportEmail(report).catch(() => {});
    return report;
  } catch (e) {
    const step = e.step || 'unknown';
    const report = { supplier: cfg.supplierKey, ran: uk.date, dryRun, step, error: e.message };
    // persist to the error log + email a specific alert (what step, what went wrong)
    await logPurchasingError(pool, { supplier: cfg.supplierKey, step, message: e.message, context: { dryRun, ukTime: `${uk.weekday} ${uk.hour}:${String(uk.minute).padStart(2, '0')}`, ...(e.context || {}) } }).catch(() => {});
    if (!dryRun) { try { await saveState(pool, { id: cfg.stateId, workingDaysWaited: (await getState(pool, cfg.stateId)).working_days_waited, lastRunDate: uk.date, result: report }); } catch {} }
    return report;
  } finally { running = false; }
}

// Back-compat wrapper — the 10:30 poller + existing /fristads-scheduled-run route call this.
export async function runFristadsScheduled(opts = {}) { return runSupplierScheduled({ ...opts, supplier: 'FRISTADS' }); }

// Portwest two-step first-order helpers (review-before-place). prepare = create PO + load
// the Portwest basket + verify it matches the PO, WITHOUT placing. place = place a PO that
// prepare already created + verified (custref = PO#) + finalise. Non-mutating vs mutating.
export async function portwestPrepare({ pool, altItemsUrl, poId = null, packSizes = {}, excludeSkus = [] }) { return placePortwestOrder(pool, altItemsUrl, { verifyOnly: true, poId: poId ? Number(poId) : null, packSizes, excludeSkus }); }
export async function portwestPlaceExisting({ pool, altItemsUrl, poId, packSizes = {}, excludeSkus = [] }) { return placePortwestOrder(pool, altItemsUrl, { poId, packSizes, excludeSkus }); }

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
         ${placedLine}
         ${!p && report.workingDaysWaited ? `<li>Working days waited: ${report.workingDaysWaited} of ${MAX_WAIT_WORKING_DAYS}</li>` : ''}
       </ul>`;
  await t.sendMail({ from: '"Tuff Purchasing" <noreply@tuffshop.co.uk>', to: NOTIFY_TO, subject, html, text: subject + '\n\n' + JSON.stringify(report, null, 2) });
}
