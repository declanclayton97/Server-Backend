// saleAlerts.js - "the item you bought is on sale" emails for stock in the STW sale warehouse.
//
// Everything in a BP location whose first grouping is STW is sale stock (Dec, 7 Oct 2026).
// Each weekday it features a few STW items it has not featured before, finds everyone who bought
// that exact product (BP goods-out movements), and emails them from info@ through Graph:
// "you bought X in size/colour - we have one that has just gone on sale, first come first served".
// No price in the email (Dec). Never emailed:
//   - anyone with a sales credit for that product (returned it)
//   - Amazon / eBay buyers (relay addresses), company webstores, decorated (embroidered/printed) purchases
//   - trade/rep channels unless SALE_ALERTS_TRADE=on
//   - unsubscribed addresses, staff/supplier contacts, anyone alerted in the last 7 days
// Every send, skip and failure is a row in sale_alert_sends.
//
// Env:
//   SALE_ALERTS_ENABLED  off (default) | review | live
//       review: the daily run emails SALE_ALERTS_REVIEW_TO the batch it WOULD send; no customer is emailed.
//       live:   customers are emailed.
//   SALE_ALERTS_SECRET   signs unsubscribe links (required for review and live)
//   SALE_ALERTS_FROM (info@tuffshop.co.uk), SALE_ALERTS_REVIEW_TO (dec@tuffshop.co.uk), SALE_ALERTS_PER_DAY (5),
//   SALE_ALERTS_AT (10:00, weekdays, until +60 min), SALE_ALERTS_LINK (https://tuffshop.co.uk/sale.html),
//   SALE_ALERTS_TRADE (off), SALE_ALERTS_LOOKBACK_DAYS (1095), SALE_ALERTS_PUBLIC_URL (this service's base URL)

import crypto from "crypto";
import { bpLiveGet } from "./purchasingAuto.js";
import { sendNew, mailboxContext, graphConfigured } from "./graphMail.js";

const WAREHOUSE = 2;
const WEBSITE_CHANNELS = new Set([2, 3, 14, 17]);                                   // tuffshop.co.uk (old/online/Magento), tuffkids
const TRADE_CHANNELS = new Set([1, 4, 5, 6, 8, 10, 11, 12, 15, 16, 21, 33, 34]);    // phone, shop, email, reps, trades hub
// Everything else (Amazon 9, eBay 7, the company webstores) is never emailed.
const DECORATION = /^OP(EM|PR)/i, DECORATION_NAME = /embroid|\bprint(ed|ing)?\b|set ?up fee|upload logo|logo design/i;

const cfg = () => ({
  mode: String(process.env.SALE_ALERTS_ENABLED || "off").toLowerCase(),
  secret: process.env.SALE_ALERTS_SECRET || "",
  from: process.env.SALE_ALERTS_FROM || "info@tuffshop.co.uk",
  reviewTo: process.env.SALE_ALERTS_REVIEW_TO || "dec@tuffshop.co.uk",
  perDay: Math.max(1, Math.min(20, Number(process.env.SALE_ALERTS_PER_DAY) || 5)),
  at: process.env.SALE_ALERTS_AT || "10:00",
  link: process.env.SALE_ALERTS_LINK || "https://tuffshop.co.uk/sale.html",
  trade: String(process.env.SALE_ALERTS_TRADE || "off").toLowerCase() === "on",
  lookbackDays: Number(process.env.SALE_ALERTS_LOOKBACK_DAYS) || 1095,
  publicUrl: (process.env.SALE_ALERTS_PUBLIC_URL || "https://server-backend-1i47.onrender.com").replace(/\/$/, ""),
});

const esc = (s) => String(s ?? "").replace(/[&<>"']/g, (c) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[c]));
const chunk = (a, n) => { const o = []; for (let i = 0; i < a.length; i += n) o.push(a.slice(i, i + n)); return o; };
const idSet = (ids) => [...new Set(ids.map(Number))].sort((a, b) => a - b).join(",");   // BP 400s on unordered id-sets
const rowsOf = (resp) => { const c = resp.metaData.columns.map((x) => x.name); return resp.results.map((r) => Object.fromEntries(c.map((k, i) => [k, r[i]]))); };
const normEmail = (e) => String(e || "").trim().toLowerCase();
const ukDay = (d = new Date()) => d.toLocaleDateString("en-CA", { timeZone: "Europe/London" });

// ---- unsubscribe tokens ---------------------------------------------------------------------
export function unsubToken(email, secret = cfg().secret) {
  return crypto.createHmac("sha256", secret).update(normEmail(email)).digest("base64url").slice(0, 32);
}
const unsubUrl = (email) => { const c = cfg(); return `${c.publicUrl}/sale-alerts/unsubscribe?e=${encodeURIComponent(normEmail(email))}&t=${unsubToken(email)}`; };

// ---- BP reads ---------------------------------------------------------------------------------
async function stwLocations() {
  const locs = (await bpLiveGet(`/warehouse-service/warehouse/${WAREHOUSE}/location/`)) || [];
  return new Set(locs.filter((l) => String(l.groupingA || "").toUpperCase() === "STW").map((l) => Number(l.id)));
}

/** Every product with free stock in an STW location: Map(pid -> units). Reads the whole catalogue in SMALL ranges:
 *  a 60k-id range is tens of MB of JSON and pushed this service over its memory limit on 12 Oct (crash + restart). */
async function stwStock(stw) {
  const lo = rowsOf(await bpLiveGet(`/product-service/product-search?sort=productId%7CASC&pageSize=1`))[0].productId;
  const hi = rowsOf(await bpLiveGet(`/product-service/product-search?sort=productId%7CDESC&pageSize=1`))[0].productId;
  const out = new Map();
  const STEP = 4000;
  for (let a = lo; a <= hi; a += STEP) {
    const resp = (await bpLiveGet(`/warehouse-service/product-availability/${a}-${Math.min(hi, a + STEP - 1)}?includeOptional=breakDownByLocation`)) || {};
    for (const [pid, v] of Object.entries(resp)) {
      const byLoc = v.warehouses?.[WAREHOUSE]?.byLocation || {};
      let q = 0; for (const [lid, x] of Object.entries(byLoc)) if (stw.has(Number(lid))) q += Math.max(0, Number(x.onHand ?? x.inStock) || 0);
      if (q > 0) out.set(Number(pid), q);
    }
    await new Promise((r) => setTimeout(r, 300));   // gentle on BP and on memory
  }
  return out;
}

async function stwUnitsNow(pid, stw) {
  const resp = (await bpLiveGet(`/warehouse-service/product-availability/${pid}?includeOptional=breakDownByLocation`)) || {};
  let q = 0; for (const [lid, x] of Object.entries(resp[pid]?.warehouses?.[WAREHOUSE]?.byLocation || {})) if (stw.has(Number(lid))) q += Math.max(0, Number(x.onHand ?? x.inStock) || 0);
  return q;
}

async function movements(pid, type) {
  const out = [];
  for (let first = 1; ; first += 500) {
    const r = await bpLiveGet(`/warehouse-service/goods-movement-search?productId=${pid}&goodsNoteTypeCode=${type}&pageSize=500&firstResult=${first}`);
    out.push(...rowsOf(r));
    if (!r.metaData.morePagesAvailable) return out;
  }
}

async function orders(ids) {
  const out = [];
  for (const c of chunk([...new Set(ids)].sort((a, b) => a - b), 50)) out.push(...((await bpLiveGet(`/order-service/order/${c.join(",")}`)) || []));
  return out;
}

async function productInfo(pid) {
  const p = ((await bpLiveGet(`/product-service/product/${pid}`)) || [])[0] || {};
  const v = Object.fromEntries((p.variations || []).map((x) => [String(x.optionName).toLowerCase(), x.optionValue]));
  const name = (p.salesChannels?.[0]?.productName || "").replace(/\s*\[[^\]]*\]\s*/g, " ").replace(/\s*\(.*?\)\s*$/, "").trim();
  return { productId: pid, sku: p.identity?.sku || "", name, size: v.size || "", colour: v.colour || v.color || "" };
}

/**
 * Who should hear about this product? Every past buyer minus everyone the rules exclude.
 * Returns { product, eligible: [{contactId, email, firstName, orderId, date}], skipped: [{contactId, email, reason}] }.
 */
export async function recipientsFor(pid, { pool, cutoffDays = 7 } = {}) {
  const c = cfg();
  const since = new Date(Date.now() - c.lookbackDays * 864e5).toISOString().slice(0, 10);
  const shipped = await movements(pid, "GO");
  const sales = (await orders(shipped.map((m) => m.orderId).filter(Boolean))).filter((o) => o.orderTypeCode === "SO");
  // returns: credits that took this product back (goods-in against a credit) or credit the order it was sold on
  const returned = new Set();
  const gi = await movements(pid, "GI");
  for (const o of await orders(gi.map((m) => m.orderId).filter(Boolean))) if (o.orderTypeCode === "SC") returned.add(o.parties?.customer?.contactId);
  for (const o of sales) {
    const cr = rowsOf(await bpLiveGet(`/order-service/order-search?orderTypeId=3&parentOrderId=${o.id}&columns=orderId`));
    if (!cr.length) continue;
    for (const credit of await orders(cr.map((x) => x.orderId)))
      if (Object.values(credit.orderRows || {}).some((r) => Number(r.productId) === Number(pid))) returned.add(credit.parties?.customer?.contactId);
  }
  // one candidate per customer: their latest qualifying purchase
  const best = new Map(), skipped = [];
  for (const o of sales) {
    const cid = o.parties?.customer?.contactId, date = String(o.placedOn || o.createdOn || "").slice(0, 10), ch = Number(o.assignment?.current?.channelId);
    const why = !cid ? "no customer" : returned.has(cid) ? "returned it" : date < since ? "bought too long ago"
      : !(WEBSITE_CHANNELS.has(ch) || (c.trade && TRADE_CHANNELS.has(ch))) ? (TRADE_CHANNELS.has(ch) ? "trade channel (off)" : "marketplace / company webstore")
      : Object.values(o.orderRows || {}).some((r) => DECORATION.test(String(r.productSku || "")) || DECORATION_NAME.test(String(r.productName || ""))) ? "decorated order" : "";
    if (why) { skipped.push({ contactId: cid, orderId: o.id, reason: why }); continue; }
    if (!best.has(cid) || best.get(cid).date < date) best.set(cid, { contactId: cid, orderId: o.id, date });
  }
  const eligible = [];
  const contacts = new Map();
  for (const cc of chunk([...best.keys()].sort((a, b) => a - b), 60)) for (const x of (await bpLiveGet(`/contact-service/contact/${idSet(cc)}`)) || []) contacts.set(x.contactId, x);
  const unsub = new Set(), recent = new Set(), already = new Set();
  if (pool) {
    (await pool.query(`SELECT email FROM sale_alert_unsubs`)).rows.forEach((r) => unsub.add(r.email));
    (await pool.query(`SELECT email FROM sale_alert_sends WHERE status = 'sent' AND at > now() - ($1 || ' days')::interval`, [String(cutoffDays)])).rows.forEach((r) => recent.add(r.email));
    (await pool.query(`SELECT email FROM sale_alert_sends WHERE status = 'sent' AND product_id = $1`, [pid])).rows.forEach((r) => already.add(r.email));
  }
  const seenEmail = new Set();
  for (const b of best.values()) {
    const x = contacts.get(b.contactId) || {}, email = normEmail(x.communication?.emails?.PRI?.email);
    const why = !email ? "no email" : x.relationshipToAccount?.isStaff || x.relationshipToAccount?.isSupplier ? "staff/supplier"
      : unsub.has(email) ? "unsubscribed" : already.has(email) ? "already told about this item" : recent.has(email) ? "had a sale email in the last 7 days"
      : seenEmail.has(email) ? "duplicate email" : "";
    if (why) { skipped.push({ contactId: b.contactId, email, orderId: b.orderId, reason: why }); continue; }
    seenEmail.add(email);
    eligible.push({ ...b, email, firstName: String(x.firstName || "").trim() });
  }
  return { eligible, skipped };
}

// ---- the email --------------------------------------------------------------------------------
export function renderEmail({ product, firstName, email }) {
  const c = cfg();
  const what = [product.colour, product.size ? `size ${product.size}` : ""].filter(Boolean).join(", ");
  const subject = `The ${product.name} you bought is now on sale`;
  const html = `<div style="font-family:Arial,sans-serif;font-size:15px;line-height:1.5;color:#222;max-width:560px">
<p>Hi ${esc(firstName || "there")},</p>
<p>We noticed you've bought the <b>${esc(product.name)}</b>${what ? ` in <b>${esc(what)}</b>` : ""} from us before.</p>
<p>We have one available that has just gone <b>on sale</b>. It's first come, first served, so be quick if you'd like it.</p>
<p><a href="${esc(c.link)}" style="display:inline-block;background:#1f5fbf;color:#fff;padding:10px 18px;border-radius:6px;text-decoration:none;font-weight:bold">Shop the sale</a></p>
<p>Or just reply to this email and we'll sort it for you.</p>
<p>Thanks,<br>The Tuffshop team</p>
<p style="font-size:12px;color:#888;margin-top:28px">You're getting this because you bought this item from Tuffshop. <a href="${esc(unsubUrl(email))}" style="color:#888">Unsubscribe from sale emails</a>.</p>
</div>`;
  return { subject, html };
}

async function send(to, subject, html) {
  const c = cfg();
  return mailboxContext.run({ box: c.from }, () => sendNew({ to, subject, html }));
}

// ---- one day's run ------------------------------------------------------------------------------
/** dryRun: pick + check everything, send nothing, record nothing. */
export async function runSaleAlerts({ pool, dryRun = false, mode = cfg().mode, maxEvaluate = 40 } = {}) {
  const c = cfg();
  if (!dryRun && !c.secret) throw new Error("SALE_ALERTS_SECRET is not set (needed for unsubscribe links)");
  if (!dryRun && !graphConfigured()) throw new Error("Graph mail is not configured");
  const day = ukDay();
  const stw = await stwLocations();
  const stock = await stwStock(stw);
  // Featured already? A product only REVIEWED is still owed its real send once live; in review mode it is not re-reviewed.
  const done = new Set((await pool.query(`SELECT product_id, status FROM sale_alert_items`)).rows.filter((r) => r.status !== 'review' || mode === 'review').map((r) => Number(r.product_id)));
  // most units in STW first, then oldest product id (longest-standing lines)
  const queue = [...stock.entries()].filter(([pid]) => !done.has(pid)).sort((a, b) => b[1] - a[1] || a[0] - b[0]);
  const batch = [], evaluated = [];
  for (const [pid, units] of queue) {
    if (batch.length >= c.perDay || evaluated.length >= maxEvaluate) break;
    const product = await productInfo(pid);
    const r = await recipientsFor(pid, { pool });
    evaluated.push({ productId: pid, sku: product.sku, units, eligible: r.eligible.length });
    if (!r.eligible.length) {
      if (!dryRun) await pool.query(`INSERT INTO sale_alert_items (product_id, sku, featured_on, status, recipients) VALUES ($1,$2,$3,'no buyers',0) ON CONFLICT (product_id) DO NOTHING`, [pid, product.sku, day]);
      continue;
    }
    batch.push({ product, units, ...r });
  }
  const result = { day, mode: dryRun ? "preview" : mode, stwProducts: stock.size, notYetFeatured: queue.length, evaluated, products: [] };
  for (const b of batch) {
    const p = { productId: b.product.productId, sku: b.product.sku, name: b.product.name, size: b.product.size, colour: b.product.colour, units: b.units,
      recipients: b.eligible.map((e) => ({ email: e.email, firstName: e.firstName, orderId: e.orderId, date: e.date })),
      skipped: b.skipped.map((s) => ({ email: s.email || null, orderId: s.orderId, reason: s.reason })), sent: 0, failed: 0 };
    result.products.push(p);
    if (dryRun) continue;
    await pool.query(`INSERT INTO sale_alert_items (product_id, sku, featured_on, status, recipients) VALUES ($1,$2,$3,$4,$5) ON CONFLICT (product_id) DO UPDATE SET status = EXCLUDED.status, featured_on = EXCLUDED.featured_on, recipients = EXCLUDED.recipients`,
      [b.product.productId, b.product.sku, day, mode, b.eligible.length]);
    for (const s of b.skipped) await logSend(pool, { day, pid: b.product.productId, contactId: s.contactId, email: s.email, status: "skipped", reason: s.reason });
    if (mode !== "live") { for (const e of b.eligible) await logSend(pool, { day, pid: b.product.productId, contactId: e.contactId, email: e.email, status: "review", reason: "review mode - not sent" }); continue; }
    for (const e of b.eligible) {
      // first come, first served: stop the moment the STW stock has gone
      if ((await stwUnitsNow(b.product.productId, stw)) <= 0) { await logSend(pool, { day, pid: b.product.productId, contactId: e.contactId, email: e.email, status: "skipped", reason: "sold out before sending" }); continue; }
      const { subject, html } = renderEmail({ product: b.product, firstName: e.firstName, email: e.email });
      try { const r = await send(e.email, subject, html); p.sent++; await logSend(pool, { day, pid: b.product.productId, contactId: e.contactId, email: e.email, status: "sent", reason: r?.via || "graph" }); }
      catch (err) { p.failed++; await logSend(pool, { day, pid: b.product.productId, contactId: e.contactId, email: e.email, status: "failed", reason: err.message.slice(0, 300) }); }
    }
  }
  if (!dryRun && mode === "review") await sendReview(result).catch((e) => { result.reviewError = e.message; });
  return result;
}

async function logSend(pool, { day, pid, contactId, email, status, reason }) {
  await pool.query(`INSERT INTO sale_alert_sends (day, product_id, contact_id, email, status, reason) VALUES ($1,$2,$3,$4,$5,$6)`,
    [day, pid, contactId || null, email || null, status, reason || null]);
}

async function sendReview(result) {
  const c = cfg();
  const rows = result.products.map((p) => `<h3 style="margin:18px 0 4px">${esc(p.name)} - ${esc([p.colour, p.size].filter(Boolean).join(", "))} <span style="color:#888">(${esc(p.sku)}, ${p.units} in STW)</span></h3>
<p style="margin:0">Would email <b>${p.recipients.length}</b>: ${p.recipients.map((r) => esc(r.email)).join(", ")}</p>
<p style="margin:0;color:#888;font-size:13px">Skipped ${p.skipped.length}: ${Object.entries(p.skipped.reduce((a, s) => ((a[s.reason] = (a[s.reason] || 0) + 1), a), {})).map(([k, v]) => `${esc(k)} ${v}`).join(", ") || "none"}</p>`).join("");
  const sample = result.products[0] ? renderEmail({ product: { name: result.products[0].name, size: result.products[0].size, colour: result.products[0].colour }, firstName: "Sam", email: c.reviewTo }) : null;
  await send(c.reviewTo, `Sale alerts ${result.day}: ${result.products.length} items, ${result.products.reduce((a, p) => a + p.recipients.length, 0)} customers (review - nothing sent)`,
    `<div style="font-family:Arial,sans-serif;font-size:14px">${rows || "<p>Nothing to send today.</p>"}${sample ? `<hr><p><b>What customers would receive:</b></p>${sample.html}` : ""}</div>`);
}

// ---- routes + schedule ---------------------------------------------------------------------------
export function registerSaleAlerts(app, { getPool }) {
  const requireUser = (req, res, next) => app.locals.requireHubUser(req, res, next);
  const ensure = async () => getPool().query(`
    CREATE TABLE IF NOT EXISTS sale_alert_items (product_id bigint PRIMARY KEY, sku text, featured_on date NOT NULL, status text, recipients int);
    CREATE TABLE IF NOT EXISTS sale_alert_sends (id bigserial PRIMARY KEY, at timestamptz NOT NULL DEFAULT now(), day date, product_id bigint, contact_id bigint, email text, status text NOT NULL, reason text);
    CREATE INDEX IF NOT EXISTS idx_sale_alert_sends_email ON sale_alert_sends(email, at);
    CREATE TABLE IF NOT EXISTS sale_alert_unsubs (email text PRIMARY KEY, at timestamptz NOT NULL DEFAULT now());
    CREATE TABLE IF NOT EXISTS sale_alert_runs (day date PRIMARY KEY, ran_at timestamptz NOT NULL DEFAULT now(), result jsonb);`);

  // What today's run would do. Nothing is sent or recorded.
  app.get("/api/sale-alerts/preview", requireUser, async (req, res) => {
    try { await ensure(); res.json(await runSaleAlerts({ pool: getPool(), dryRun: true })); }
    catch (e) { res.status(500).json({ error: e.message }); }
  });
  // One sample email to an @tuffshop.co.uk address, to see exactly what customers get.
  app.post("/api/sale-alerts/send-sample", requireUser, async (req, res) => {
    try {
      const to = normEmail(req.query.to || req.body?.to);
      if (!/@tuffshop\.co\.uk$/.test(to)) return res.status(400).json({ error: "Samples only go to @tuffshop.co.uk addresses" });
      const { subject, html } = renderEmail({ product: { name: "Snickers 6241 AllroundWork Stretch Trousers", colour: "Black", size: "34 Waist 32 Leg" }, firstName: "Sam", email: to });
      res.json(await send(to, "[SAMPLE] " + subject, html));
    } catch (e) { res.status(500).json({ error: e.message }); }
  });
  app.get("/api/sale-alerts/log", requireUser, async (req, res) => {
    try {
      await ensure();
      const sends = await getPool().query(`SELECT * FROM sale_alert_sends ORDER BY id DESC LIMIT $1`, [Math.min(2000, Number(req.query.limit) || 300)]);
      const items = await getPool().query(`SELECT * FROM sale_alert_items ORDER BY featured_on DESC LIMIT 200`);
      const runs = await getPool().query(`SELECT day, ran_at, result FROM sale_alert_runs ORDER BY day DESC LIMIT 30`);
      res.json({ sends: sends.rows, items: items.rows, runs: runs.rows });
    } catch (e) { res.status(500).json({ error: e.message }); }
  });

  // Unsubscribe (public, from the email footer).
  app.get("/sale-alerts/unsubscribe", async (req, res) => {
    const email = normEmail(req.query.e), t = String(req.query.t || "");
    const page = (msg) => res.type("html").send(`<!doctype html><meta name="viewport" content="width=device-width,initial-scale=1"><title>Tuffshop</title><div style="font-family:Arial,sans-serif;max-width:480px;margin:60px auto;padding:0 16px;font-size:16px">${msg}</div>`);
    const secret = cfg().secret;
    if (!secret || !email || t.length !== 32 || !crypto.timingSafeEqual(Buffer.from(t), Buffer.from(unsubToken(email, secret)))) return page("<p>That unsubscribe link isn't valid. Reply to any of our emails and we'll take you off the list.</p>");
    try { await ensure(); await getPool().query(`INSERT INTO sale_alert_unsubs (email) VALUES ($1) ON CONFLICT DO NOTHING`, [email]); page(`<p>Done. <b>${esc(email)}</b> won't get any more sale emails from Tuffshop.</p>`); }
    catch (e) { console.error("[sale-alerts] unsubscribe:", e.message); page("<p>Something went wrong - please reply to the email and we'll remove you.</p>"); }
  });

  // Weekdays at SALE_ALERTS_AT (UK), once a day even across restarts.
  setInterval(async () => {
    const c = cfg();
    if (!["review", "live"].includes(c.mode) || !getPool()) return;
    const now = new Date();
    const hm = now.toLocaleTimeString("en-GB", { hour: "2-digit", minute: "2-digit", hour12: false, timeZone: "Europe/London" });
    const dow = now.toLocaleDateString("en-GB", { weekday: "short", timeZone: "Europe/London" });
    const [h, m] = c.at.split(":").map(Number), end = `${String(h + 1).padStart(2, "0")}:${String(m).padStart(2, "0")}`;
    if (/Sat|Sun/.test(dow) || hm < c.at || hm > end) return;
    const day = ukDay(now);
    try {
      await ensure();
      const claim = await getPool().query(`INSERT INTO sale_alert_runs (day) VALUES ($1) ON CONFLICT (day) DO NOTHING RETURNING day`, [day]);
      if (!claim.rowCount) return;
      let result;
      try { result = await runSaleAlerts({ pool: getPool() }); }
      catch (e) {
        console.error("[sale-alerts] run failed:", e.message);
        await getPool().query(`UPDATE sale_alert_runs SET result = $2 WHERE day = $1`, [day, JSON.stringify({ error: e.message })]);
        return;   // not retried: a half-finished send must not repeat; the log shows what went out
      }
      await getPool().query(`UPDATE sale_alert_runs SET result = $2 WHERE day = $1`, [day, JSON.stringify(result)]);
      console.log("[sale-alerts]", day, c.mode, JSON.stringify(result.products.map((p) => ({ sku: p.sku, recipients: p.recipients.length, sent: p.sent, failed: p.failed }))));
    } catch (e) { console.error("[sale-alerts] scheduler:", e.message); }
  }, 60 * 1000);
}
