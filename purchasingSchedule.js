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
import { updateOrderReference } from './bpWebSession.js';

const THRESHOLD_NET = Number(process.env.FRISTADS_FREESHIP_THRESHOLD || 300); // £ ex-VAT
const MAX_WAIT_WORKING_DAYS = 3;
const NOTIFY_TO = process.env.PURCHASING_SCHEDULE_EMAIL || 'dec@tuffshop.co.uk';
const FRISTADS_SUPPLIER_CONTACT = 37419;

let running = false; // in-process guard against overlapping runs

// ── UK local time ────────────────────────────────────────────────────────────
export function ukNow(d = new Date()) {
  const p = {};
  for (const x of new Intl.DateTimeFormat('en-GB', { timeZone: 'Europe/London', weekday: 'short', year: 'numeric', month: '2-digit', day: '2-digit', hour: '2-digit', minute: '2-digit', hour12: false }).formatToParts(d)) p[x.type] = x.value;
  return { weekday: p.weekday, date: `${p.year}-${p.month}-${p.day}`, hour: +p.hour, minute: +p.minute };
}
export const isUkWeekday = (wd) => !['Sat', 'Sun'].includes(wd);

// ── state ────────────────────────────────────────────────────────────────────
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
async function getState(pool) { const r = await pool.query(`SELECT * FROM fristads_purchase_schedule WHERE id=1`); return r.rows[0]; }
async function saveState(pool, { workingDaysWaited, lastRunDate, result }) {
  await pool.query(
    `UPDATE fristads_purchase_schedule SET working_days_waited=$1, last_run_date=COALESCE($2,last_run_date), last_result=$3, updated_at=now() WHERE id=1`,
    [workingDaysWaited, lastRunDate, result ? JSON.stringify(result) : null],
  );
}

// ── the full placement chain ─────────────────────────────────────────────────
async function placeFristadsOrder(altItemsUrl) {
  const steps = {};
  // 1. create the combined PO (SO + low-inv + separator + notes; stamps the SOs)
  const po = await bp.createComboPOLive({ supplierKey: 'FRISTADS', execute: true });
  if (!po.created) throw new Error(`createComboPOLive did not create a PO: ${po.reason || 'unknown'}`);
  const poId = po.poId;
  const soIds = [...new Set((po.soLines || []).map((l) => l.order).filter(Boolean))];
  steps.po = { poId, soUnits: po.soUnits, lowUnits: po.lowUnits, soIds };

  // 2. push the PO lines to the Fristads cart
  const cartLines = await bp.getOrderCartLines(poId);
  const expectUnits = cartLines.reduce((a, l) => a + l.qty, 0);
  const cart = await (await fetch(`${altItemsUrl}/api/fristads-basket`, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ clearFirst: true, lines: cartLines }) })).json();
  steps.cart = { cartCount: cart.cartCount, expectUnits, unresolved: cart.unresolved };
  if ((cart.unresolved || []).length || cart.cartCount !== expectUnits) throw new Error(`cart mismatch: got ${cart.cartCount}, expected ${expectUnits}, unresolved ${JSON.stringify(cart.unresolved || [])}`);

  // 3. checkout / placeorder (Mark of goods=WORKWEAR, order ref = our PO#)
  const co = await (await fetch(`${altItemsUrl}/api/fristads-checkout`, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ goodsMark: 'WORKWEAR', orderRef: String(poId), execute: true }) })).json();
  steps.checkout = { allOk: co.allOk, status: co.status };
  if (!co.allOk) throw new Error(`checkout fields did not verify: ${JSON.stringify(co.verify || co)}`);
  // placeorder redirects to the credit-account payment step (order gets placed there);
  // we confirm via the order-history pull, so a null orderNo from checkout is expected.

  // 4. pull the Fristads order number (by our PO ref); retry — it may take a moment
  let order = null;
  for (let i = 0; i < 6; i++) {
    order = await (await fetch(`${altItemsUrl}/api/fristads-order?ref=${encodeURIComponent(poId)}`)).json();
    if (order && order.found && order.orderNo) break;
    await new Promise((r) => setTimeout(r, 5000));
  }
  if (!order || !order.found || !order.orderNo) throw new Error(`order not found in history for PO ${poId} after placeorder (checkout status ${co.status})`);
  steps.order = { orderNo: order.orderNo, orderStatus: order.orderStatus, sum: order.sum };

  // 5. write their order# onto our PO reference + restore tax + status 7
  await updateOrderReference(poId, String(order.orderNo), { client: process.env.BP_WEB_CLIENT_ID || 'tuffworkwear' });
  await bp.repriceComboPOLive({ poId, keepNet: true, execute: true }); // legacy reference-write zeroes row tax → restore
  await bp.setOrderStatusLive(poId, bp.PLACED_WITH_SUPPLIER_STATUS);
  steps.link = { reference: order.orderNo, status: 7 };

  // 6. finalize the contributing SOs (clear tag, status 22, "ordered via PO#" note)
  if (soIds.length) steps.finalize = await bp.finalizeSupplierTagsLive({ orderIds: soIds, supplierKey: 'FRISTADS', poId, noteContactId: FRISTADS_SUPPLIER_CONTACT, setOrderedStatus: true, execute: true });

  return { poId, orderNo: order.orderNo, orderStatus: order.orderStatus, sum: order.sum, steps };
}

// ── one scheduled run ────────────────────────────────────────────────────────
export async function runFristadsScheduled({ pool, altItemsUrl, dryRun = false, force = false } = {}) {
  if (running) return { skipped: 'a run is already in progress' };
  running = true;
  const uk = ukNow();
  try {
    await ensureTable(pool);
    const state = await getState(pool);
    if (!force && !dryRun && state.last_run_date && ukDateStr(state.last_run_date) === uk.date) {
      return { skipped: `already ran today (${uk.date})`, ukTime: `${uk.weekday} ${uk.hour}:${String(uk.minute).padStart(2, '0')}` };
    }

    // dry-run the combined PO to value the demand (net, ex-VAT)
    const plan = await bp.createComboPOLive({ supplierKey: 'FRISTADS', execute: false });
    const lines = [...(plan.soLines || []), ...(plan.lowLines || [])];
    const netValue = Number(lines.reduce((a, l) => a + (l.cost || 0) * l.qty, 0).toFixed(2));
    const units = (plan.soUnits || 0) + (plan.lowUnits || 0);

    let decision, willPlace = false, reason = null, newWaitDays = state.working_days_waited;
    if (netValue <= 0) {
      decision = 'no demand'; newWaitDays = 0;
    } else {
      const over = netValue >= THRESHOLD_NET;
      const wouldBeDay = state.working_days_waited + 1;
      if (over) { willPlace = true; reason = 'over-threshold'; }
      else if (wouldBeDay >= MAX_WAIT_WORKING_DAYS) { willPlace = true; reason = `held ${MAX_WAIT_WORKING_DAYS} working days (under £${THRESHOLD_NET} — carriage applies)`; }
      else { decision = `waiting — day ${wouldBeDay} of ${MAX_WAIT_WORKING_DAYS} (£${netValue} < £${THRESHOLD_NET})`; newWaitDays = wouldBeDay; }
    }

    let placement = null;
    if (willPlace) {
      if (dryRun) { decision = `WOULD place (${reason})`; }
      else { placement = await placeFristadsOrder(altItemsUrl); decision = `placed — ${reason}`; newWaitDays = 0; }
    }

    const report = { ran: uk.date, ukTime: `${uk.weekday} ${uk.hour}:${String(uk.minute).padStart(2, '0')}`, dryRun, netValue, units, threshold: THRESHOLD_NET, decision, reason, workingDaysWaited: newWaitDays, placement };
    if (!dryRun) await saveState(pool, { workingDaysWaited: newWaitDays, lastRunDate: uk.date, result: report });
    await sendReportEmail(report).catch(() => {});
    return report;
  } catch (e) {
    const report = { ran: uk.date, dryRun, error: e.message };
    if (!dryRun) { try { await saveState(pool, { workingDaysWaited: (await getState(pool)).working_days_waited, lastRunDate: uk.date, result: report }); } catch {} }
    await sendReportEmail(report).catch(() => {});
    return report;
  } finally { running = false; }
}

function ukDateStr(d) { // normalise a pg date (Date or 'YYYY-MM-DD') to YYYY-MM-DD
  if (typeof d === 'string') return d.slice(0, 10);
  return new Intl.DateTimeFormat('en-CA', { timeZone: 'Europe/London', year: 'numeric', month: '2-digit', day: '2-digit' }).format(d);
}

async function sendReportEmail(report) {
  if (!process.env.SMTP_PASS) return;
  const t = nodemailer.createTransport({ host: process.env.SMTP_SERVER || 'mail-eu.smtp2go.com', port: parseInt(process.env.SMTP_PORT || '2525'), secure: false, auth: { user: process.env.SMTP_USERNAME || 'tuffshop.co.uk', pass: process.env.SMTP_PASS } });
  const p = report.placement;
  const subject = report.error ? `Fristads auto-purchase: ERROR` : `Fristads auto-purchase: ${report.decision}`;
  const html = report.error
    ? `<p><strong>Error during the scheduled Fristads run (${report.ran}).</strong></p><pre>${report.error}</pre><p>Nothing may have been placed — check BP + the Fristads portal before the next run.</p>`
    : `<p>Fristads auto-purchase — ${report.ukTime}${report.dryRun ? ' (DRY RUN)' : ''}</p>
       <ul>
         <li>Demand value: <strong>£${report.netValue}</strong> ex-VAT (${report.units} units), threshold £${report.threshold}</li>
         <li>Decision: <strong>${report.decision}</strong></li>
         ${p ? `<li>PO <strong>${p.poId}</strong> → Fristads order <strong>${p.orderNo}</strong> (${p.orderStatus}, ${p.sum} GBP)</li>` : ''}
         ${!p && report.workingDaysWaited ? `<li>Working days waited: ${report.workingDaysWaited} of ${MAX_WAIT_WORKING_DAYS}</li>` : ''}
       </ul>`;
  await t.sendMail({ from: '"Tuff Purchasing" <noreply@tuffshop.co.uk>', to: NOTIFY_TO, subject, html, text: subject + '\n\n' + JSON.stringify(report, null, 2) });
}
