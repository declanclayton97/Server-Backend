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
const CASTLE_SUPPLIER_CONTACT = 332;

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
const stepErr = (step, message) => Object.assign(new Error(message), { step });

async function ensureErrorTable(pool) {
  await pool.query(`CREATE TABLE IF NOT EXISTS purchasing_error_log (
    id serial PRIMARY KEY,
    created_at timestamptz DEFAULT now(),
    supplier text,
    step text,
    message text,
    context jsonb
  )`);
}

// Persist an error AND email an alert. Used for every failure in the flow.
export async function logPurchasingError(pool, { supplier = 'FRISTADS', step = 'unknown', message = '', context = null } = {}) {
  try { if (pool) { await ensureErrorTable(pool); await pool.query(`INSERT INTO purchasing_error_log (supplier, step, message, context) VALUES ($1,$2,$3,$4)`, [supplier, step, message, context ? JSON.stringify(context) : null]); } } catch (e) { console.error('[purchasing-error-log] insert failed:', e.message); }
  try { await sendAlertEmail({ supplier, step, message, context }); } catch (e) { console.error('[purchasing-error-log] email failed:', e.message); }
}

function transporter() {
  return nodemailer.createTransport({ host: process.env.SMTP_SERVER || 'mail-eu.smtp2go.com', port: parseInt(process.env.SMTP_PORT || '2525'), secure: false, auth: { user: process.env.SMTP_USERNAME || 'tuffshop.co.uk', pass: process.env.SMTP_PASS } });
}

async function sendAlertEmail({ supplier, step, message, context }) {
  if (!process.env.SMTP_PASS) return;
  const when = new Date().toLocaleString('en-GB', { timeZone: 'Europe/London' });
  const html = `<p style="color:#c62828"><strong>⚠ ${supplier} auto-purchase failed</strong> — ${when}</p>
    <ul>
      <li><strong>Step:</strong> ${step}</li>
      <li><strong>Problem:</strong> ${escapeHtml(message)}</li>
    </ul>
    ${context ? `<pre style="background:#f5f5f5;padding:8px;border-radius:4px;white-space:pre-wrap">${escapeHtml(JSON.stringify(context, null, 2))}</pre>` : ''}
    <p>Nothing further was placed on this run. Check Brightpearl + the Fristads portal, then it will retry on the next scheduled run.</p>`;
  await transporter().sendMail({ from: '"Tuff Purchasing" <noreply@tuffshop.co.uk>', to: NOTIFY_TO, subject: `⚠ ${supplier} auto-purchase error — ${step}`, html, text: `${supplier} auto-purchase error at step "${step}": ${message}\n\n${context ? JSON.stringify(context, null, 2) : ''}` });
}
const escapeHtml = (s) => String(s).replace(/[&<>]/g, (c) => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;' }[c]));

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

async function placeFristadsOrder(pool, altItemsUrl) {
  const steps = {};
  // 1. create the combined PO (SO + low-inv + separator + notes; stamps the SOs)
  let po;
  try { po = await bp.createComboPOLive({ supplierKey: 'FRISTADS', execute: true }); }
  catch (e) { throw stepErr('create-po', `Brightpearl error building the PO: ${e.message}`); }
  if (!po.created) throw stepErr('create-po', `no PO created: ${po.reason || 'unknown'}` + (po.unresolvedSkus && po.unresolvedSkus.length ? ` — item codes not found in Brightpearl: ${po.unresolvedSkus.join(', ')}` : ''));
  const poId = po.poId;
  const soIds = [...new Set((po.soLines || []).map((l) => l.order).filter(Boolean))];
  // item names ordered per SO — for the SO note
  const linesByOrder = {};
  for (const l of (po.soLines || [])) { if (l.order) (linesByOrder[l.order] = linesByOrder[l.order] || []).push(l.name); }
  steps.po = { poId, soUnits: po.soUnits, lowUnits: po.lowUnits, soIds };

  // 2. push the PO lines to the Fristads cart (unresolved = size/item not on the portal)
  const cartLines = await bp.getOrderCartLines(poId).catch((e) => { throw stepErr('cart', `couldn't read PO ${poId} rows: ${e.message}`); });
  const expectUnits = cartLines.reduce((a, l) => a + l.qty, 0);
  const cart = await jfetch('cart', `${altItemsUrl}/api/fristads-basket`, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ clearFirst: true, lines: cartLines }) });
  steps.cart = { cartCount: cart.cartCount, expectUnits, unresolved: cart.unresolved };
  if ((cart.unresolved || []).length) throw stepErr('cart', `size/item not found on the Fristads portal (codes don't match): ${JSON.stringify(cart.unresolved)}`);
  if (cart.cartCount !== expectUnits) throw stepErr('cart', `cart quantity mismatch: portal shows ${cart.cartCount}, expected ${expectUnits} — some lines didn't add`);

  // 3. checkout / placeorder (Mark of goods=WORKWEAR, order ref = our PO#)
  const co = await jfetch('checkout', `${altItemsUrl}/api/fristads-checkout`, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ goodsMark: 'WORKWEAR', orderRef: String(poId), execute: true }) });
  const reservationNo = co.reservationNo; // Fristads "Reservation No" (= basket id) → goes on our PO ref
  steps.checkout = { allOk: co.allOk, status: co.status, reservationNo };
  if (!co.allOk) throw stepErr('checkout', `checkout fields didn't set/verify on the portal: ${JSON.stringify(co.verify || co)}`);
  // placeorder redirects to the credit-account payment step (order gets placed there);
  // we confirm via the order-history pull, so a null orderNo from checkout is expected.

  // 4. pull the Fristads order number (by our PO ref); retry — it may take a moment
  let order = null;
  for (let i = 0; i < 6; i++) {
    order = await jfetch('order-pull', `${altItemsUrl}/api/fristads-order?ref=${encodeURIComponent(poId)}`);
    if (order && order.found && order.orderNo) break;
    await new Promise((r) => setTimeout(r, 5000));
  }
  if (!order || !order.found || !order.orderNo) throw stepErr('order-pull', `order not found in Fristads history for PO ${poId} after placeorder — it may not have placed (checkout status ${co.status})`);
  steps.order = { orderNo: order.orderNo, orderStatus: order.orderStatus, sum: order.sum };

  // price sanity check (NON-FATAL): the Fristads order total (what they'll invoice,
  // ex-VAT) vs our PO net. A gap means a BP cost price is stale → alert so it can be
  // adjusted; the order still stands (Fristads charges their price regardless).
  const poNet = [...(po.soLines || []), ...(po.lowLines || [])].reduce((a, l) => a + (l.cost || 0) * l.qty, 0);
  const fristadsTotal = parseFloat(String(order.sum || '').replace(/[^\d.]/g, '')) || 0;
  const priceGap = fristadsTotal ? +(fristadsTotal - poNet).toFixed(2) : 0;
  steps.priceCheck = { fristadsTotal, poNet: +poNet.toFixed(2), gap: priceGap };
  if (fristadsTotal && Math.abs(priceGap) > 0.50) {
    const breakdown = [...(po.soLines || []), ...(po.lowLines || [])].map((l) => `${l.qty} × ${l.sku} — our £${(l.cost || 0).toFixed(2)}/ea (${l.name})`);
    await logPurchasingError(pool, {
      supplier: 'FRISTADS', step: 'price-check',
      message: `Prices don't match: Fristads order total £${fristadsTotal} vs our PO net £${poNet.toFixed(2)} (diff £${priceGap}). A Brightpearl cost price (Launch/list 20) may need adjusting. Order ${order.orderNo} still placed.`,
      context: { poId, orderNo: order.orderNo, fristadsTotal, poNet: +poNet.toFixed(2), gap: priceGap, poLines: breakdown },
    }).catch(() => {});
  }

  // 5. write the Fristads RESERVATION No onto our PO reference + restore tax + status 7
  const poRef = reservationNo || order.orderNo; // reservation no is the reference; order no is a fallback
  try {
    await updateOrderReference(poId, String(poRef), { client: process.env.BP_WEB_CLIENT_ID || 'tuffworkwear' });
    await bp.repriceComboPOLive({ poId, keepNet: true, execute: true }); // legacy reference-write zeroes row tax → restore
    await bp.setOrderStatusLive(poId, bp.PLACED_WITH_SUPPLIER_STATUS);
  } catch (e) { throw stepErr('link', `Fristads order ${order.orderNo} (reservation ${poRef}) WAS placed, but linking to PO ${poId} failed: ${e.message}`); }
  steps.link = { reference: poRef, reservationNo, orderNo: order.orderNo, status: 7 };

  // 6. finalize the contributing SOs (clear tag, status 22, "ordered via PO#" note)
  if (soIds.length) { try { steps.finalize = await bp.finalizeSupplierTagsLive({ orderIds: soIds, supplierKey: 'FRISTADS', poId, noteContactId: FRISTADS_SUPPLIER_CONTACT, setOrderedStatus: true, linesByOrder, execute: true }); } catch (e) { throw stepErr('finalize', `order placed + PO linked, but finalising SOs failed: ${e.message}`); } }

  return { poId, reservationNo: poRef, orderNo: order.orderNo, orderStatus: order.orderStatus, sum: order.sum, steps };
}

// ── Castle placement chain ───────────────────────────────────────────────────
// Same skeleton as Fristads, but Castle's checkout POST *places directly* (no
// separate placeorder step) and the reference we write is Castle's order number.
async function placeCastleOrder(pool, altItemsUrl) {
  const steps = {};
  // 1. combined PO (allocation-aware demand + low-inv + separator)
  let po;
  try { po = await bp.createComboPOLive({ supplierKey: 'CASTLE', execute: true }); }
  catch (e) { throw stepErr('create-po', `Brightpearl error building the PO: ${e.message}`); }
  if (!po.created) throw stepErr('create-po', `no PO created: ${po.reason || 'unknown'}` + (po.unresolvedSkus && po.unresolvedSkus.length ? ` — item codes not found in Brightpearl: ${po.unresolvedSkus.join(', ')}` : ''));
  const poId = po.poId;
  const soIds = [...new Set((po.soLines || []).map((l) => l.order).filter(Boolean))];
  const linesByOrder = {};
  for (const l of (po.soLines || [])) { if (l.order) (linesByOrder[l.order] = linesByOrder[l.order] || []).push(l.name); }
  steps.po = { poId, soUnits: po.soUnits, lowUnits: po.lowUnits, soIds };

  // 2. push PO lines to the Castle basket (SKU-direct; size ignored by Castle)
  const cartLines = await bp.getOrderCartLines(poId).catch((e) => { throw stepErr('cart', `couldn't read PO ${poId} rows: ${e.message}`); });
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
      supplier: 'CASTLE', step: 'price-check',
      message: `Prices don't match: Castle goods total £${castleGoods} vs our PO net £${poNet.toFixed(2)} (diff £${priceGap}). A Brightpearl cost price (Launch/list 20) may need adjusting. Order ${order.orderNo} still placed.`,
      context: { poId, orderNo: order.orderNo, castleGoods, poNet: +poNet.toFixed(2), gap: priceGap, poLines: breakdown },
    }).catch(() => {});
  }

  // 5. write the Castle order number onto our PO reference + restore tax + status 7
  try {
    await updateOrderReference(poId, String(order.orderNo), { client: process.env.BP_WEB_CLIENT_ID || 'tuffworkwear' });
    await bp.repriceComboPOLive({ poId, keepNet: true, execute: true }); // legacy reference-write zeroes row tax → restore
    await bp.setOrderStatusLive(poId, bp.PLACED_WITH_SUPPLIER_STATUS);
  } catch (e) { throw stepErr('link', `Castle order ${order.orderNo} WAS placed, but linking to PO ${poId} failed: ${e.message}`); }
  steps.link = { reference: order.orderNo, orderNo: order.orderNo, status: 7 };

  // 6. finalize the contributing SOs (clear CASTLE tag, status 22 when fully ordered, note)
  if (soIds.length) { try { steps.finalize = await bp.finalizeSupplierTagsLive({ orderIds: soIds, supplierKey: 'CASTLE', poId, noteContactId: CASTLE_SUPPLIER_CONTACT, setOrderedStatus: true, linesByOrder, execute: true }); } catch (e) { throw stepErr('finalize', `order placed + PO linked, but finalising SOs failed: ${e.message}`); } }

  return { poId, orderNo: order.orderNo, steps };
}

// Supplier registry for the scheduled runner (per-supplier state row id + place fn).
const SCHEDULED_SUPPLIERS = {
  FRISTADS: { supplierKey: 'FRISTADS', stateId: 1, placeFn: placeFristadsOrder, threshold: Number(process.env.FRISTADS_FREESHIP_THRESHOLD || 300) },
  CASTLE: { supplierKey: 'CASTLE', stateId: 2, placeFn: placeCastleOrder, threshold: Number(process.env.CASTLE_FREESHIP_THRESHOLD || 150) }, // Castle free carriage @ £150 ex-VAT
};

// ── one scheduled run (supplier-generic) ─────────────────────────────────────
export async function runSupplierScheduled({ pool, altItemsUrl, supplier = 'FRISTADS', dryRun = false, force = false } = {}) {
  const cfg = SCHEDULED_SUPPLIERS[String(supplier).toUpperCase()];
  if (!cfg) return { error: `unknown scheduled supplier ${supplier}` };
  const threshold = cfg.threshold || THRESHOLD_NET; // free-carriage threshold (ex-VAT), per supplier
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

    let decision, willPlace = false, reason = null, newWaitDays = state.working_days_waited;
    if (netValue <= 0) {
      decision = 'no demand'; newWaitDays = 0;
    } else {
      const over = netValue >= threshold;
      const wouldBeDay = state.working_days_waited + 1;
      if (over) { willPlace = true; reason = 'over-threshold'; }
      else if (wouldBeDay >= MAX_WAIT_WORKING_DAYS) { willPlace = true; reason = `held ${MAX_WAIT_WORKING_DAYS} working days (under £${threshold} — carriage applies)`; }
      else { decision = `waiting — day ${wouldBeDay} of ${MAX_WAIT_WORKING_DAYS} (£${netValue} < £${threshold})`; newWaitDays = wouldBeDay; }
    }

    let placement = null;
    if (willPlace) {
      if (dryRun) { decision = `WOULD place (${reason})`; }
      else { placement = await cfg.placeFn(pool, altItemsUrl); decision = `placed — ${reason}`; newWaitDays = 0; }
    }

    const report = { supplier: cfg.supplierKey, ran: uk.date, ukTime: `${uk.weekday} ${uk.hour}:${String(uk.minute).padStart(2, '0')}`, dryRun, netValue, units, threshold, decision, reason, workingDaysWaited: newWaitDays, placement };
    if (!dryRun) await saveState(pool, { id: cfg.stateId, workingDaysWaited: newWaitDays, lastRunDate: uk.date, result: report });
    await sendReportEmail(report).catch(() => {});
    return report;
  } catch (e) {
    const step = e.step || 'unknown';
    const report = { supplier: cfg.supplierKey, ran: uk.date, dryRun, step, error: e.message };
    // persist to the error log + email a specific alert (what step, what went wrong)
    await logPurchasingError(pool, { supplier: cfg.supplierKey, step, message: e.message, context: { dryRun, ukTime: `${uk.weekday} ${uk.hour}:${String(uk.minute).padStart(2, '0')}` } }).catch(() => {});
    if (!dryRun) { try { await saveState(pool, { id: cfg.stateId, workingDaysWaited: (await getState(pool, cfg.stateId)).working_days_waited, lastRunDate: uk.date, result: report }); } catch {} }
    return report;
  } finally { running = false; }
}

// Back-compat wrapper — the 10:30 poller + existing /fristads-scheduled-run route call this.
export async function runFristadsScheduled(opts = {}) { return runSupplierScheduled({ ...opts, supplier: 'FRISTADS' }); }

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
         ${p ? `<li>PO <strong>${p.poId}</strong> → Fristads reservation <strong>${p.reservationNo}</strong> (order ${p.orderNo}, ${p.orderStatus}, ${p.sum} GBP)</li>` : ''}
         ${!p && report.workingDaysWaited ? `<li>Working days waited: ${report.workingDaysWaited} of ${MAX_WAIT_WORKING_DAYS}</li>` : ''}
       </ul>`;
  await t.sendMail({ from: '"Tuff Purchasing" <noreply@tuffshop.co.uk>', to: NOTIFY_TO, subject, html, text: subject + '\n\n' + JSON.stringify(report, null, 2) });
}
