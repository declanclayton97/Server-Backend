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
export async function logPurchasingError(pool, { supplier = 'FRISTADS', step = 'unknown', message = '', context = null, severity = 'error' } = {}) {
  try { if (pool) { await ensureErrorTable(pool); await pool.query(`INSERT INTO purchasing_error_log (supplier, step, message, context) VALUES ($1,$2,$3,$4)`, [supplier, step, message, context ? JSON.stringify(context) : null]); } } catch (e) { console.error('[purchasing-error-log] insert failed:', e.message); }
  try { await sendAlertEmail({ supplier, step, message, context, severity }); } catch (e) { console.error('[purchasing-error-log] email failed:', e.message); }
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
async function sendAlertEmail({ supplier, step, message, context, severity = 'error' }) {
  if (!process.env.SMTP_PASS) return;
  const when = new Date().toLocaleString('en-GB', { timeZone: 'Europe/London' });
  const review = severity === 'review';
  const colour = review ? '#e65100' : '#c62828';
  const heading = review ? `${supplier} auto-purchase — NEEDS REVIEW` : `${supplier} auto-purchase failed`;
  const footer = review
    ? `<p><strong>The order was placed.</strong> This is flagged for a human to correct in Brightpearl — nothing is blocked and the next scheduled run is unaffected.</p>`
    : `<p>Nothing further was placed on this run. Check Brightpearl + the ${supplier} portal, then it will retry on the next scheduled run.</p>`;
  const html = `<p style="color:${colour}"><strong>${review ? 'ⓘ' : '⚠'} ${heading}</strong> — ${when}</p>
    <ul>
      <li><strong>Step:</strong> ${step}</li>
      <li><strong>${review ? 'Detail' : 'Problem'}:</strong> ${escapeHtml(message)}</li>
    </ul>
    ${context ? `<pre style="background:#f5f5f5;padding:8px;border-radius:4px;white-space:pre-wrap">${escapeHtml(JSON.stringify(context, null, 2))}</pre>` : ''}
    ${footer}`;
  await transporter().sendMail({
    from: '"Tuff Purchasing" <noreply@tuffshop.co.uk>', to: NOTIFY_TO,
    subject: `${review ? 'ⓘ' : '⚠'} ${supplier} auto-purchase ${review ? 'review' : 'error'} — ${step}`,
    html,
    text: `${supplier} auto-purchase ${review ? 'REVIEW (order was placed)' : 'ERROR (run stopped)'} at step "${step}": ${message}\n\n${context ? JSON.stringify(context, null, 2) : ''}`,
  });
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
  catch (e) { throw stepErr('create-po', `Brightpearl error building the PO: ${e.message}`); }
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
    const breakdown = [...(po.soLines || []), ...(po.lowLines || [])].map((l) => `${l.qty} × ${l.sku} — our £${(l.cost || 0).toFixed(2)}/ea (${l.name})`);
    await logPurchasingError(pool, {
      supplier: 'FRISTADS', step: 'price-check', severity: 'review',
      message: `Prices don't match: Fristads order total £${fristadsTotal} vs our PO net £${poNet.toFixed(2)} (diff £${priceGap}). A Brightpearl cost price (Launch/list 20) may need adjusting. Order ${orderNo} still placed.`,
      context: { poId, orderNo, fristadsTotal, poNet: +poNet.toFixed(2), gap: priceGap, poLines: breakdown },
    }).catch(() => {});
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
  catch (e) { throw stepErr('create-po', `Brightpearl error building the PO: ${e.message}`); }
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
async function workerPlaceOrder({ supplier = 'STERLING', ref, lines, execute }) {
  const headers = { 'Content-Type': 'application/json', 'x-worker-secret': STERLING_WORKER_SECRET };
  const start = await jfetch('checkout', `${STERLING_WORKER_URL}/place-order`, {
    method: 'POST', headers,
    body: JSON.stringify({ supplier, ref: String(ref), lines, execute, async: true }),
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
  catch (e) { throw stepErr('create-po', `Brightpearl error building the PO: ${e.message}`); }
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
  catch (e) { throw stepErr('create-po', `Brightpearl error building the PO: ${e.message}`); }
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
  catch (e) { throw stepErr('create-po', `Brightpearl error building the PO: ${e.message}`); }
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
  if (!wr || !wr.placed) throw stepErr('checkout', `Snickers worker did not confirm placement: ${JSON.stringify((wr && (wr.error || wr.statusText)) || wr).slice(0, 250)}`);
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
    const preLines = [...(preview.soLines || []), ...(preview.lowLines || [])].filter((l) => String(l.productId) !== '1000' && l.sku).map((l) => ({ sku: l.sku, qty: l.qty }));
    if (preLines.length) {
      const dry = await jfetch('preflight', `${altItemsUrl}${basketPath}`, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ lines: preLines, dryRun: true }) });
      if (dry.unresolved && dry.unresolved.length) throw stepErr('preflight', `${supplierKey} lines not on the portal (PO NOT created): ${dry.unresolved.join(', ')} — fix the resolver/aliases first`);
      if (Array.isArray(dry.pricedLines) && dry.pricedLines.length) {
        priceOverrides = {};
        for (const p of dry.pricedLines) { const price = Number(p.price); if (p.sku && Number.isFinite(price) && price > 0) priceOverrides[String(p.sku).toUpperCase()] = price; }
        steps.priceOverrides = { count: Object.keys(priceOverrides).length };
      }
    }
  }
  let po;
  try { po = await bp.createComboPOLive({ supplierKey, execute: live, padToThreshold, priceOverrides, logPool: pool }); }
  catch (e) { throw stepErr('create-po', `Brightpearl error building the PO: ${e.message}`); }
  if (!po.created) throw stepErr('create-po', `no PO created: ${po.reason || 'unknown'}` + (po.unresolvedSkus && po.unresolvedSkus.length ? ` — item codes not found in Brightpearl: ${po.unresolvedSkus.join(', ')}` : ''));
  const poId = po.poId;
  const soIds = [...new Set((po.soLines || []).map((l) => l.order).filter(Boolean))];
  const linesByOrder = {};
  for (const l of (po.soLines || [])) { if (l.order) (linesByOrder[l.order] = linesByOrder[l.order] || []).push({ sku: l.sku, qty: l.qty, name: l.name }); }
  steps.po = { poId, soUnits: po.soUnits, lowUnits: po.lowUnits, soIds, skippedBundles: po.skippedBundles || [], priceOverridesApplied: po.priceOverridesApplied || [] };

  // Order lines = the PO's SKUs (skip the =====LOW INV==== separator), summed per SKU.
  const bySku = new Map();
  for (const l of [...(po.soLines || []), ...(po.lowLines || [])]) { if (String(l.productId) === '1000' || !l.sku) continue; const k = String(l.sku).toUpperCase(); bySku.set(k, (bySku.get(k) || 0) + Math.round(l.qty)); }
  const lines = [...bySku.entries()].map(([sku, qty]) => ({ sku, qty }));
  if (!lines.length) throw stepErr('resolve', `no orderable ${supplierKey} lines`);
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
const PORTWEST_TEMP_EXCLUDE = ['P351WHR'];
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
    catch (e) { throw stepErr('create-po', `Brightpearl error building the PO: ${e.message}`); }
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
  const got = new Map(); for (const l of cart) got.set(String(l.sku).toUpperCase(), (got.get(String(l.sku).toUpperCase()) || 0) + (Number(l.qty) || 0));
  // Diff the CART against the ORIGINAL PO ROWS (not the pack-adjusted upload) so BOTH Portwest's
  // carton rounding and our own pack rounding surface as bumps to apply to the PO.
  const bumped = [], droppedLines = [], matched = [];
  for (const l of poRowLines) { const g = got.get(String(l.sku).toUpperCase()) || 0; if (g === 0) droppedLines.push({ sku: l.sku, want: l.qty }); else if (g !== l.qty) bumped.push({ sku: l.sku, from: l.qty, to: g }); else matched.push(l.sku); }
  const extra = [...got.keys()].filter((s) => !poRowLines.some((l) => String(l.sku).toUpperCase() === s));
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
  catch (e) { throw stepErr('create-po', `Brightpearl error building the PO: ${e.message}`); }
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
  catch (e) { throw stepErr('create-po', `Brightpearl error building the PO: ${e.message}`); }
  if (!po.created) throw stepErr('create-po', `no PO created: ${po.reason || 'unknown'}` + (po.unresolvedSkus && po.unresolvedSkus.length ? ` — item codes not found in Brightpearl: ${po.unresolvedSkus.join(', ')}` : ''));
  const poId = po.poId;
  const soIds = [...new Set((po.soLines || []).map((l) => l.order).filter(Boolean))];
  const linesByOrder = {};
  for (const l of (po.soLines || [])) { if (l.order) (linesByOrder[l.order] = linesByOrder[l.order] || []).push({ sku: l.sku, qty: l.qty, name: l.name }); }
  steps.po = { poId, soUnits: po.soUnits, lowUnits: po.lowUnits, soIds, skippedBundles: po.skippedBundles || [] };

  // Order lines = the PO rows (BP SKU = Blåkläder part number, exact — no resolver).
  const orderLines = [...(po.soLines || []), ...(po.lowLines || [])].filter((l) => String(l.productId) !== '1000' && l.sku).map((l) => ({ sku: String(l.sku), qty: Math.round(l.qty) }));
  if (!orderLines.length) throw stepErr('cart', 'no orderable Blaklader lines');
  steps.lines = { count: orderLines.length, units: orderLines.reduce((a, l) => a + l.qty, 0) };

  // Submit via the Blaklader order API (loads basket then POST /order/orders). place=live.
  const r = await jfetch('checkout', `${altItemsUrl}/api/blaklader-order`, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ lines: orderLines, purchaseOrder: String(poId), place: live }) });
  if (!r.ok) throw stepErr('checkout', `Blaklader did not confirm the order: ${JSON.stringify(r.error || r.add || r).slice(0, 250)}`, { poId });
  const orderNo = r.internalId || null;
  steps.checkout = { ok: true, orderNo, sent: r.sent };

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
};

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
