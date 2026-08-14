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
    <p>Nothing further was placed on this run. Check Brightpearl + the ${supplier} portal, then it will retry on the next scheduled run.</p>`;
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
      supplier: 'FRISTADS', step: 'price-check',
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
      supplier: 'CASTLE', step: 'price-check',
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
  const lines = [...bySku.entries()].map(([stockCode, qty]) => ({ stockCode, qty }));
  if (!lines.length) throw stepErr('resolve', 'no orderable Snickers lines');
  steps.resolve = { lines: lines.length, units: lines.reduce((a, l) => a + l.qty, 0) };

  // Drive the Hultafors worker (async job + poll). ref = PO id → the portal PO-number field.
  const wr = await workerPlaceOrder({ supplier: 'SNICKERS', ref: poId, lines, execute: live });
  if (!wr || !wr.placed) throw stepErr('checkout', `Snickers worker did not confirm placement: ${JSON.stringify((wr && (wr.error || wr.statusText)) || wr).slice(0, 250)}`);
  const orderNo = wr.orderNo || null;
  steps.checkout = { placed: true, orderNo, poSet: wr.poSet || null };

  // Finalise — BOTH sides (supplier PO checklist item 7). PO: status 7 + reference.
  const ref = orderNo || `Placed-${poId}`;
  await bp.setOrderStatusLive(poId, bp.PLACED_WITH_SUPPLIER_STATUS);
  let refWritten = false;
  try { await bp.setOrderReferenceLive(poId, ref); refWritten = true; }               // API PATCH — tax-safe
  catch (e) { steps.linkWarn = `reference-set failed (non-fatal): ${e.message}`; await bp.addOrderNoteLive(poId, `Placed with Snickers — order ${ref}. Reference-set failed: ${e.message}`, SNICKERS_SUPPLIER_CONTACT).catch(() => {}); }
  steps.link = { reference: ref, refWritten, orderNo, status: 7 };

  // SO: note ("… Ordered on PO#<id>") + status → Ordered Stock Awaiting Delivery + clear tag.
  if (soIds.length) { try { steps.finalize = await bp.finalizeSupplierTagsLive({ orderIds: soIds, supplierKey: 'SNICKERS', poId, noteContactId: SNICKERS_SUPPLIER_CONTACT, setOrderedStatus: true, linesByOrder, execute: live }); } catch (e) { throw stepErr('finalize', `order placed + PO linked, but finalising SOs failed: ${e.message}`); } }
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
async function placePortwestOrder(pool, altItemsUrl, { padToThreshold = 0, verifyOnly = false, poId: existingPoId = null } = {}) {
  const steps = {};
  let poId, soIds, linesByOrder, cartLines;
  if (existingPoId) {
    // Reuse a PO created by a prior verifyOnly run — re-derive the SO mapping + order lines
    // from current demand (unchanged over the few minutes between prepare and place).
    poId = existingPoId;
    let dry;
    try { dry = await bp.createComboPOLive({ supplierKey: 'PORTWEST', execute: false }); }
    catch (e) { throw stepErr('create-po', `couldn't re-read demand for PO ${poId}: ${e.message}`); }
    soIds = [...new Set((dry.soLines || []).map((l) => l.order).filter(Boolean))];
    linesByOrder = {};
    for (const l of (dry.soLines || [])) { if (l.order) (linesByOrder[l.order] = linesByOrder[l.order] || []).push({ sku: l.sku, qty: l.qty, name: l.name }); }
    cartLines = [...(dry.soLines || []), ...(dry.lowLines || [])].filter((l) => String(l.productId) !== '1000' && l.sku).map((l) => ({ sku: String(l.sku), qty: Math.round(l.qty) }));
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
    // Order lines from the PO-creation result (full SKUs; skip the =====LOW INV==== separator).
    cartLines = [...(po.soLines || []), ...(po.lowLines || [])].filter((l) => String(l.productId) !== '1000' && l.sku).map((l) => ({ sku: String(l.sku), qty: Math.round(l.qty) }));
    steps.po = { poId, soUnits: po.soUnits, lowUnits: po.lowUnits, soIds, skippedBundles: po.skippedBundles || [] };
  }
  const expectUnits = cartLines.reduce((a, l) => a + l.qty, 0);
  if (!cartLines.length) throw stepErr('cart', 'no orderable Portwest lines');

  // 1) CSV-upload the order to the cart WITHOUT placing, and read the cart contents back.
  const up = await jfetch('cart', `${altItemsUrl}/api/portwest-order`, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ lines: cartLines, purchaseOrder: String(poId) }) });
  const cart = (up.checkout && up.checkout.lines) || [];
  steps.cart = { uploaded: up.upload && up.upload.units, cartUnits: up.checkout && up.checkout.cartUnits, cartLineCount: cart.length, expectUnits, poLineCount: cartLines.length };

  // 2) HARD VERIFY: the Portwest cart must match the PO line-for-line before we place anything.
  //    (Portwest can silently drop an unknown code or cap a qty to its max-order allowance.)
  const want = new Map(); for (const l of cartLines) want.set(String(l.sku).toUpperCase(), (want.get(String(l.sku).toUpperCase()) || 0) + l.qty);
  const got = new Map(); for (const l of cart) got.set(String(l.sku).toUpperCase(), (got.get(String(l.sku).toUpperCase()) || 0) + (Number(l.qty) || 0));
  const missing = [], qtyMismatch = [], extra = [];
  for (const [sku, q] of want) { const g = got.get(sku) || 0; if (g === 0) missing.push({ sku, want: q }); else if (g !== q) qtyMismatch.push({ sku, want: q, got: g }); }
  for (const [sku, g] of got) { if (!want.has(sku)) extra.push({ sku, got: g }); }
  const allMatch = !missing.length && !qtyMismatch.length && !extra.length;
  steps.verify = { allMatch, missing, qtyMismatch, extra };
  if (!allMatch) throw stepErr('verify', `basket does NOT match the PO — NOT placing. missing:${JSON.stringify(missing).slice(0, 200)} qtyMismatch:${JSON.stringify(qtyMismatch).slice(0, 200)} extra:${JSON.stringify(extra).slice(0, 120)}. PO#${poId} left for review.`, { poId, missing, qtyMismatch, extra });

  // verifyOnly: stop here with the basket loaded + verified == PO. Nothing is placed; the PO
  // stands for review. A follow-up run with { poId } places this same PO.
  if (verifyOnly) return { poId, verifyOnly: true, allMatch: true, verify: steps.verify, expectUnits, cartUnits: steps.cart.cartUnits, soIds, steps };

  // 3) Basket verified == PO → place it (custref = our PO#). No re-upload; place the current cart.
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

const SCHEDULED_SUPPLIERS = {
  FRISTADS: { supplierKey: 'FRISTADS', stateId: 1, placeFn: placeFristadsOrder, threshold: Number(process.env.FRISTADS_FREESHIP_THRESHOLD || 300) },
  CARHARTT: { supplierKey: 'CARHARTT', stateId: 6, placeFn: placeCarharttOrder, threshold: Number(process.env.CARHARTT_FREESHIP_THRESHOLD || 300) }, // Elastic Suite; also gated on Alt-Items by CARHARTT_PLACE_ENABLED
  'HELLY HANSEN': { supplierKey: 'HELLY HANSEN', stateId: 7, placeFn: placeHellyHansenOrder, threshold: Number(process.env.HELLYHANSEN_FREESHIP_THRESHOLD || 300) }, // Elastic Suite; gated on Alt-Items by HELLYHANSEN_PLACE_ENABLED
  SNICKERS: { supplierKey: 'SNICKERS', stateId: 5, placeFn: placeSnickersOrder, threshold: Number(process.env.SNICKERS_FREESHIP_THRESHOLD || 300) }, // Hultafors portal worker; £300 ex-VAT failsafe (rarely hit — high volume) so tiny orders accumulate instead of placing daily
  SNICKERS: { supplierKey: 'SNICKERS', stateId: 5, placeFn: placeSnickersOrder, threshold: Number(process.env.SNICKERS_FREESHIP_THRESHOLD || 300) }, // Hultafors portal worker; £300 ex-VAT failsafe (rarely hit — high volume) so tiny orders accumulate instead of placing daily
  UNEEK: { supplierKey: 'UNEEK', stateId: 3, placeFn: placeUneekOrder, threshold: Number(process.env.UNEEK_FREESHIP_THRESHOLD || 100) }, // email supplier, free carriage @ £100 ex-VAT, no min order
  CASTLE: { supplierKey: 'CASTLE', stateId: 2, placeFn: placeCastleOrder, threshold: Number(process.env.CASTLE_FREESHIP_THRESHOLD || 150) }, // Castle free carriage @ £150 ex-VAT
  STERLING: { supplierKey: 'STERLING', stateId: 4, placeFn: placeSterlingOrder, threshold: Number(process.env.STERLING_FREESHIP_THRESHOLD || 150) },
  PORTWEST: { supplierKey: 'PORTWEST', stateId: 8, placeFn: placePortwestOrder, threshold: Number(process.env.PORTWEST_FREESHIP_THRESHOLD || 150) }, // portwest.com CSV upload + checkout_summary; free carriage @ £150 ex-VAT (else £7.50)
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
      else { placement = await cfg.placeFn(pool, altItemsUrl, { padToThreshold: padTo }); decision = `placed — ${reason}`; newWaitDays = 0; }
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
export async function portwestPrepare({ pool, altItemsUrl }) { return placePortwestOrder(pool, altItemsUrl, { verifyOnly: true }); }
export async function portwestPlaceExisting({ pool, altItemsUrl, poId }) { return placePortwestOrder(pool, altItemsUrl, { poId }); }

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
