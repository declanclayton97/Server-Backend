// returnsRoutes.js — self-service returns (customer page + staff queue).
//
// Mounted with one call from server.js, like salesHubRoutes, with its dependencies
// injected. The rules live in returns.js; this module only gathers facts, stores
// the request and sends the emails.
//
// PUBLIC routes (the customer page, no sign-in):
//   GET  /returns                     the page
//   POST /api/returns/lookup          { orderNumber, postcode } -> what can be returned
//   POST /api/returns/submit          { orderNumber, postcode, email, lines, comments }
// The order number AND its postcode are required for anything to come back, the
// answer carries no address or contact details, and both routes are rate-limited.
//
// STAFF routes (Sales Hub sign-in):
//   GET  /api/returns/requests?status=
//   POST /api/returns/requests/:ref/status  { status, note }

import path from "path";
import nodemailer from "nodemailer";
import { graphConfigured, salesMailbox, sendNew } from "./graphMail.js";
import { returnRef } from "./salesHub.js";
import { getSiteChrome, wrapInChrome, wrapForEmbed, siteChromeStatus } from "./siteChrome.js";
import fs from "fs";
import {
  assessReturn, validateSelection, postcodeMatches, returnEmailHtml, returnNoteText,
  RETURN_WINDOW_DAYS, prettyDate, EXCHANGE_CHOICES, REFUND_REASONS, WEB_RETURNS_ADDRESS,
  orderEmail, maskEmail, statusEmail, validatePhotos, styleName,
} from "./returns.js";
import { buildBrightpearlReport, summariseOnline } from "./returnsReport.js";
import { planBrightpearl, executeBrightpearl } from "./returnsBp.js";


const STATUSES = ["requested", "received", "refunded", "exchanged", "rejected", "cancelled"];
const OPEN_STATUSES = ["requested", "received"];

export function registerReturnsRoutes(app, deps) {
  const { bpLive, postBpOrderNote, useDatabase, rootDir } = deps;
  const getPool = () => (typeof deps.pool === "function" ? deps.pool() : deps.pool);
  const requireUser = (req, res, next) => app.locals.requireHubUser(req, res, next);

  // ---- storage ----------------------------------------------------------------
  let ready = null;
  const ensureTables = () => (ready ||= (async () => {
    const pool = getPool();
    // The same reference table the Sales Hub numbers its returns from, so a web
    // return and a salesperson's return can never be given the same reference.
    await pool.query(`CREATE TABLE IF NOT EXISTS sales_hub_return_refs (
      ref        text PRIMARY KEY,
      initials   text NOT NULL,
      day        date NOT NULL,
      seq        int  NOT NULL,
      order_id   bigint NOT NULL,
      created_by text NOT NULL,
      created_at timestamptz NOT NULL DEFAULT now()
    )`);
    await pool.query(`CREATE TABLE IF NOT EXISTS returns_requests (
      ref           text PRIMARY KEY,
      order_id      bigint NOT NULL,
      order_ref     text,
      customer_name text,
      email         text NOT NULL,
      lines         jsonb NOT NULL,
      comments      text,
      status        text NOT NULL DEFAULT 'requested',
      history       jsonb NOT NULL DEFAULT '[]'::jsonb,
      emailed       boolean NOT NULL DEFAULT false,
      noted         boolean NOT NULL DEFAULT false,
      created_at    timestamptz NOT NULL DEFAULT now(),
      updated_at    timestamptz NOT NULL DEFAULT now()
    )`);
    await pool.query(`CREATE INDEX IF NOT EXISTS returns_requests_order ON returns_requests (order_id)`);
    await pool.query(`ALTER TABLE returns_requests ADD COLUMN IF NOT EXISTS first_name text`);
    await pool.query(`ALTER TABLE returns_requests ADD COLUMN IF NOT EXISTS photos int NOT NULL DEFAULT 0`);
    await pool.query(`ALTER TABLE returns_requests ADD COLUMN IF NOT EXISTS photos_emailed boolean`);
    await pool.query(`ALTER TABLE returns_requests ADD COLUMN IF NOT EXISTS bp_started_at timestamptz`);
    await pool.query(`ALTER TABLE returns_requests ADD COLUMN IF NOT EXISTS bp_credit_id bigint`);
    await pool.query(`ALTER TABLE returns_requests ADD COLUMN IF NOT EXISTS bp_exchange_id bigint`);
    await pool.query(`ALTER TABLE returns_requests ADD COLUMN IF NOT EXISTS bp_result jsonb`);
    // ONE-OFF (Dec, 30 Sep): clear the test returns made on his unpaid test order 490003.
    // Runs once only - the marker row stops it ever running again, so returns made later
    // (even on 490003) are untouched. Brightpearl is not touched.
    await pool.query(`CREATE TABLE IF NOT EXISTS returns_maintenance (key text PRIMARY KEY, done_at timestamptz NOT NULL DEFAULT now(), detail text)`);
    const once = await pool.query(`INSERT INTO returns_maintenance (key) VALUES ('clear-test-returns-490003-2026-09-30') ON CONFLICT (key) DO NOTHING RETURNING key`);
    if (once.rowCount) {
      const gone = await pool.query(`DELETE FROM returns_requests WHERE order_id = 490003 RETURNING ref`);
      const refs = gone.rows.map((r) => r.ref).join(", ");
      await pool.query(`UPDATE returns_maintenance SET detail = $2 WHERE key = $1`, ["clear-test-returns-490003-2026-09-30", `deleted ${gone.rowCount}: ${refs}`]);
      console.log(`[returns] one-off: cleared ${gone.rowCount} test return(s) on order 490003: ${refs}`);
    }
    await pool.query(`CREATE TABLE IF NOT EXISTS returns_report_cache (days int PRIMARY KEY, built_at timestamptz NOT NULL, data jsonb NOT NULL)`);
  })().catch((e) => { ready = null; throw e; }));

  // Quantities already on an open return, per order row.
  async function alreadyRequested(orderId) {
    const r = await getPool().query(
      `SELECT lines FROM returns_requests WHERE order_id = $1 AND status = ANY($2)`, [orderId, OPEN_STATUSES]);
    const out = {};
    for (const row of r.rows) for (const l of row.lines || []) out[l.rowId] = (out[l.rowId] || 0) + Number(l.qty || 0);
    return out;
  }

  // WR + DDMMYY + two-digit sequence: "WR29092601". WR = web return, so a return the
  // customer raised is told apart from one a salesperson issued (their initials).
  async function allocateRef(orderId) {
    const day = new Date().toLocaleDateString("en-CA", { timeZone: "Europe/London" });
    for (let attempt = 0; attempt < 5; attempt++) {
      const n = await getPool().query(
        `SELECT COALESCE(MAX(seq), 0) + 1 AS next FROM sales_hub_return_refs WHERE initials = 'WR' AND day = $1`, [day]);
      const seq = n.rows[0].next;
      const ref = returnRef("WR", new Date(), seq);
      const ins = await getPool().query(
        `INSERT INTO sales_hub_return_refs (ref, initials, day, seq, order_id, created_by)
         VALUES ($1, 'WR', $2, $3, $4, 'web') ON CONFLICT (ref) DO NOTHING RETURNING ref`, [ref, day, seq, orderId]);
      if (ins.rowCount) return ref;
    }
    throw new Error("Could not allocate a returns reference");
  }

  // ---- Brightpearl ------------------------------------------------------------
  const channelNames = { at: 0, map: {} };
  async function channelName(id) {
    if (!id) return "";
    if (Date.now() - channelNames.at > 6 * 3600 * 1000) {
      try {
        const list = (await bpLive("GET", "/product-service/channel")) || [];
        channelNames.map = Object.fromEntries(list.map((c) => [c.id, c.name]));
        channelNames.at = Date.now();
      } catch (e) { console.error("[returns] channel list failed:", e.message); }
    }
    return channelNames.map[id] || "";
  }

  // The customer quotes their web order number ("000124559", often without the
  // zeros) or, for a phone/trade order, the Brightpearl order number. The postcode
  // check below is what makes either safe to accept.
  async function findOrder(token, postcode) {
    const raw = String(token || "").trim().replace(/^#/, "");
    if (!/^[A-Za-z0-9\-\/]{3,40}$/.test(raw)) return null;
    const candidates = [];
    const refs = [raw];
    if (/^\d{4,8}$/.test(raw)) refs.push(raw.padStart(9, "0"));
    for (const ref of [...new Set(refs)]) {
      try {
        const s = await bpLive("GET", `/order-service/order-search?customerRef=${encodeURIComponent(ref)}&pageSize=20`);
        const md = s && s.metaData;
        if (!md) continue;
        const ix = Object.fromEntries((md.columns || []).map((c, i) => [c.name, i]));
        for (const r of s.results || []) if (Number(r[ix.orderTypeId]) === 1) candidates.push(Number(r[ix.orderId]));
      } catch (e) { console.error("[returns] reference search failed:", e.message); }
    }
    if (/^\d{5,7}$/.test(raw)) candidates.push(Number(raw));
    const ids = [...new Set(candidates.filter(Boolean))].sort((a, b) => a - b).slice(0, 20);
    if (!ids.length) return null;
    let orders = [];
    try { orders = (await bpLive("GET", `/order-service/order/${ids.join(",")}`)) || []; }
    catch (e) {
      // One bad id in a set fails the whole set; fall back to one at a time.
      for (const id of ids) { try { orders.push(...((await bpLive("GET", `/order-service/order/${id}`)) || [])); } catch { /* not an order */ } }
    }
    // Only an order whose postcode matches counts — the newest, if several do.
    return orders
      .filter((o) => o && o.orderTypeCode === "SO" && postcodeMatches(o, postcode))
      .sort((a, b) => new Date(b.createdOn || b.placedOn) - new Date(a.createdOn || a.placedOn))[0] || null;
  }

  async function productMetaFor(order) {
    const ids = [...new Set(Object.values(order.orderRows || {}).map((r) => Number(r.productId)).filter(Boolean))].sort((a, b) => a - b);
    const meta = {};
    if (!ids.length) return meta;
    const prods = (await bpLive("GET", `/product-service/product/${ids.join(",")}`)) || [];
    for (const p of prods) meta[p.id] = { stockTracked: !!(p.stock && p.stock.stockTracked), brandId: p.brandId };
    return meta;
  }

  async function assessFor(orderNumber, postcode) {
    const order = await findOrder(orderNumber, postcode);
    if (!order) return { order: null, assessment: assessReturn(null) };
    const channelId = order.assignment && order.assignment.current && order.assignment.current.channelId;
    const [productMeta, chName, requested] = await Promise.all([
      productMetaFor(order),
      channelName(channelId),
      useDatabase && getPool() ? ensureTables().then(() => alreadyRequested(order.id)) : Promise.resolve({}),
    ]);
    return { order, assessment: assessReturn(order, { productMeta, channelName: chName, requested }) };
  }

  // ---- rate limit (per IP, in memory — one instance) ---------------------------
  const hits = new Map();
  function limited(req) {
    const ip = String(req.headers["x-forwarded-for"] || req.ip || "").split(",")[0].trim();
    const now = Date.now(), win = 15 * 60 * 1000;
    const list = (hits.get(ip) || []).filter((t) => now - t < win);
    list.push(now);
    hits.set(ip, list);
    if (hits.size > 5000) for (const [k, v] of hits) if (!v.some((t) => now - t < win)) hits.delete(k);
    return list.length > 30;
  }

  // ---- email ------------------------------------------------------------------
  // TEST MODE (Dec, 30 Sep): until RETURNS_LIVE=on is set on Render, EVERY email this
  // module sends — customer confirmations, status emails, the accounts refund request,
  // the sales-inbox notice — goes to RETURNS_TEST_EMAIL (Dec) instead, subject marked
  // with where it would have gone. Nothing reaches a customer or accounts while testing.
  const RETURNS_LIVE = () => String(process.env.RETURNS_LIVE || "").toLowerCase() === "on";
  const testAddress = () => process.env.RETURNS_TEST_EMAIL || "dec@tuffshop.co.uk";
  const redirect = (to, subject) => (RETURNS_LIVE() ? { to, subject } : { to: testAddress(), subject: `[TEST - would go to ${to}] ${subject}` });
  async function sendMail({ to, subject, html, replyTo, attachments }) {
    ({ to, subject } = redirect(to, subject));
    if (graphConfigured()) return sendNew({ to, subject, html, replyTo, attachments });
    const transporter = nodemailer.createTransport({
      host: process.env.SMTP_SERVER || "mail-eu.smtp2go.com",
      port: parseInt(process.env.SMTP_PORT || "2525", 10), secure: false,
      auth: { user: process.env.SMTP_USERNAME || "tuffshop.co.uk", pass: process.env.SMTP_PASS },
    });
    const from = process.env.SALES_SENDER_EMAIL || process.env.SENDER_EMAIL || "sales@tuffshop.co.uk";
    await transporter.sendMail({ from: `"Tuffshop" <${from}>`, replyTo, to, subject, html });
    return { via: "smtp2go" };
  }

  // ---- routes: customer --------------------------------------------------------
  const dir = path.join(rootDir, "returns-page");
  const part = (f) => fs.readFileSync(path.join(dir, f), "utf8");
  app.get(["/returns", "/returns/"], async (req, res) => {
    try {
      const chrome = await getSiteChrome();
      res.set("Cache-Control", "no-cache");
      // Only Tuffshop's own pages may frame the form (no clickjacking from other sites).
      res.set("Content-Security-Policy", "frame-ancestors 'self' https://tuffshop.co.uk https://*.tuffshop.co.uk");
      // ?embed=1 is the version framed inside tuffshop.co.uk/returns-form.
      res.type("html").send((req.query.embed ? wrapForEmbed : wrapInChrome)(chrome, {
        title: "Returns & Exchanges | Tuffshop",
        headExtra: `<style>${part("returns.css")}</style>`,
        content: part("content.html"),
        scripts: `<script>${part("returns-client.js")}</script>`,
      }));
    } catch (e) {
      console.error("[returns] page failed:", e.message);
      res.status(500).send("Sorry, the returns page isn't available right now. Please call 0113 288 7713.");
    }
  });
  app.get("/api/returns/site-chrome", (req, res) => res.json(siteChromeStatus()));

  app.post("/api/returns/lookup", async (req, res) => {
    if (limited(req)) return res.status(429).json({ ok: false, message: "Too many attempts. Please wait a few minutes and try again." });
    try {
      const b = req.body || {};
      const { order, assessment } = await assessFor(b.orderNumber, b.postcode);
      if (!order) return res.json({ ok: false, code: "not-found", message: assessment.message });
      res.json({
        ok: assessment.ok, code: assessment.code, message: assessment.message,
        orderRef: order.reference || String(order.id),
        despatchedOn: assessment.despatchedOn ? prettyDate(assessment.despatchedOn) : null,
        orderedOn: assessment.orderedOn ? prettyDate(assessment.orderedOn) : null,
        lastDay: assessment.lastDay ? prettyDate(assessment.lastDay) : null,
        lines: assessment.lines.map((l) => ({ rowId: l.rowId, name: l.name, qty: l.qty, available: l.available })),
        exchangeChoices: EXCHANGE_CHOICES, refundReasons: REFUND_REASONS, windowDays: RETURN_WINDOW_DAYS,
        // Masked: whoever typed the order number and postcode sees where the email is
        // going, never the address itself.
        emailHint: maskEmail(orderEmail(order)), needsEmail: !orderEmail(order),
      });
    } catch (e) {
      console.error("[returns] lookup failed:", e.message);
      res.status(500).json({ ok: false, message: "Something went wrong looking up your order. Please try again, or contact our sales team." });
    }
  });

  app.post("/api/returns/submit", async (req, res) => {
    if (limited(req)) return res.status(429).json({ ok: false, message: "Too many attempts. Please wait a few minutes and try again." });
    const b = req.body || {};
    try {
      if (!(useDatabase && getPool())) throw new Error("returns database unavailable");
      const typed = String(b.email || "").trim().slice(0, 200);
      const comments = String(b.comments || "").trim().slice(0, 1000);

      // Everything is re-checked here — never trust what the page sent.
      const { order, assessment } = await assessFor(b.orderNumber, b.postcode);
      if (!order) return res.status(404).json({ ok: false, message: assessment.message });
      const email = orderEmail(order) || typed;
      if (!/^[^@\s]+@[^@\s]+\.[^@\s]+$/.test(email)) return res.status(400).json({ ok: false, message: "We need an email address to send your returns reference to." });
      const sel = validateSelection(assessment, b.lines);
      if (!sel.ok) return res.status(400).json({ ok: false, message: sel.error });
      const ph = validatePhotos(b.photos, sel.lines);
      if (!ph.ok) return res.status(400).json({ ok: false, message: ph.error });

      const ref = await allocateRef(order.id);
      const cust = (order.parties && order.parties.customer) || {};
      const name = String(cust.addressFullName || "").trim().split(/\s+/)[0] || "";
      await getPool().query(
        `INSERT INTO returns_requests (ref, order_id, order_ref, customer_name, email, lines, comments, history, first_name, photos)
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)`,
        [ref, order.id, order.reference || String(order.id), cust.companyName || cust.addressFullName || "", email,
          JSON.stringify(sel.lines), comments, JSON.stringify([{ at: new Date().toISOString(), status: "requested", by: "customer" }]), name, ph.photos.length]);

      // The request is stored: from here a failure is reported, never a lost return.
      let emailed = false, noted = false;
      try {
        await sendMail({
          to: email,
          subject: `Your return ${ref} - order ${order.reference || order.id}`,
          html: returnEmailHtml({ ref, orderRef: order.reference || order.id, name, lines: sel.lines, lastDay: assessment.lastDay, address: WEB_RETURNS_ADDRESS }),
          replyTo: salesMailbox(),
        });
        emailed = true;
      } catch (e) { console.error(`[returns] ${ref} customer email failed:`, e.message); }
      try { noted = (await postBpOrderNote(order.id, returnNoteText({ ref, email, lines: sel.lines, comments }))) !== false; }
      catch (e) { console.error(`[returns] ${ref} order note failed:`, e.message); }
      await getPool().query(`UPDATE returns_requests SET emailed = $2, noted = $3 WHERE ref = $1`, [ref, emailed, noted]);

      // A heads-up in the shared sales inbox, where the team already works.
      if ((process.env.RETURNS_NOTIFY_INBOX !== "off" || ph.photos.length) && graphConfigured()) {
        const n = ph.photos.length;
        sendMail({
          to: salesMailbox(),
          subject: `${n ? "PHOTOS - " : ""}Online return ${ref} - order ${order.reference || order.id}${n ? ` (${n} photo${n === 1 ? "" : "s"} attached)` : ""}`,
          html: `<p>A customer has requested a return online.${n ? ` They've attached <b>${n} photo${n === 1 ? "" : "s"}</b> (below).` : ""}</p><pre style="font-family:Arial,sans-serif;">${
            returnNoteText({ ref, email, lines: sel.lines, comments }).replace(/</g, "&lt;")}</pre><p>Brightpearl order ${order.id}.${emailed ? "" : " <b>The confirmation email to the customer FAILED — please send them the reference.</b>"}</p>`,
          replyTo: email,
          attachments: ph.photos,
        }).then(() => getPool().query(`UPDATE returns_requests SET photos_emailed = true WHERE ref = $1`, [ref]))
          .catch((e) => {
            console.error(`[returns] ${ref} inbox notice failed:`, e.message);
            if (n) getPool().query(`UPDATE returns_requests SET photos_emailed = false WHERE ref = $1`, [ref]).catch(() => {});
          });
      }

      res.json({ ok: true, ref, emailed, emailHint: maskEmail(email), lastDay: prettyDate(assessment.lastDay), address: WEB_RETURNS_ADDRESS,
        exchanging: sel.lines.some((l) => l.outcome === "exchange"), needsContact: sel.lines.some((l) => /faulty|wrong item/i.test(l.reason)) });
    } catch (e) {
      console.error("[returns] submit failed:", e.message);
      res.status(500).json({ ok: false, message: "Something went wrong submitting your return. Please try again, or contact our sales team." });
    }
  });

  // ---- routes: staff -----------------------------------------------------------
  app.get("/api/returns/requests", requireUser, async (req, res) => {
    try {
      await ensureTables();
      const status = String(req.query.status || "");
      const r = status && STATUSES.includes(status)
        ? await getPool().query(`SELECT * FROM returns_requests WHERE status = $1 ORDER BY created_at DESC LIMIT 500`, [status])
        : await getPool().query(`SELECT * FROM returns_requests ORDER BY created_at DESC LIMIT 500`);
      res.json({ requests: r.rows, statuses: STATUSES });
    } catch (e) { res.status(500).json({ error: e.message }); }
  });

  // GET /api/returns/report?days=90 — the Brightpearl half is cached (it takes a
  // minute or two to build) and rebuilt in the background when older than 12 hours;
  // the online half is read fresh every time.
  const building = new Set();
  function rebuild(days) {
    if (building.has(days)) return;
    building.add(days);
    buildBrightpearlReport({ bpLive, days })
      .then((data) => getPool().query(
        `INSERT INTO returns_report_cache (days, built_at, data) VALUES ($1, now(), $2)
         ON CONFLICT (days) DO UPDATE SET built_at = now(), data = EXCLUDED.data`, [days, JSON.stringify(data)]))
      .catch((e) => console.error(`[returns] report build (${days}d) failed:`, e.message))
      .finally(() => building.delete(days));
  }
  // The report is Dec's only (30 Sep). Enforced HERE, not just by hiding the button:
  // RETURNS_REPORT_USERS is a comma list of Sales Hub account names (key or display name).
  const reportUsers = () => String(process.env.RETURNS_REPORT_USERS || "dec,dec clayton,declan,declan clayton")
    .toLowerCase().split(",").map((s) => s.trim()).filter(Boolean);
  const canReport = (u) => !!u && [u.key, u.name].some((v) => reportUsers().includes(String(v || "").trim().toLowerCase()));
  app.get("/api/returns/me", requireUser, (req, res) => res.json({ canReport: canReport(req.hubUser) }));
  app.get("/api/returns/report", requireUser, async (req, res) => {
    if (!canReport(req.hubUser)) return res.status(403).json({ error: "The returns report isn't available on your account." });
    try {
      await ensureTables();
      const days = [30, 90, 180, 365].includes(Number(req.query.days)) ? Number(req.query.days) : 90;
      const c = await getPool().query(`SELECT built_at, data FROM returns_report_cache WHERE days = $1`, [days]);
      const cached = c.rows[0] || null;
      const stale = !cached || Date.now() - new Date(cached.built_at).getTime() > 12 * 3600 * 1000;
      if (stale || req.query.refresh) rebuild(days);
      const bp = cached ? cached.data : null;
      const productToKey = {};
      for (const s of (bp && bp.styles) || []) for (const id of s.productIds || []) productToKey[id] = s.key;
      const online = await getPool().query(
        `SELECT status, lines FROM returns_requests WHERE created_at > now() - ($1 || ' days')::interval`, [String(days)]);
      res.json({ days, building: building.has(days), builtAt: cached && cached.built_at, bp, online: summariseOnline(online.rows, productToKey) });
    } catch (e) { res.status(500).json({ error: e.message }); }
  });

  // ---- Brightpearl: the credit, the exchange order, the money between them -------
  // GET  .../bp-plan    what would be created (reads only; sizes offered for each swap)
  // POST .../bp-create  { choices: { [line key]: productId|null } } — creates it, ONCE.
  // Only after the goods have arrived. bp_started_at is claimed atomically, so a double
  // click or two people at once cannot make two credits; every id is saved the moment it
  // exists, so a failure part-way shows what was made instead of inviting a re-run.
  const getReturn = async (ref) => (await getPool().query(`SELECT * FROM returns_requests WHERE ref = $1`, [ref])).rows[0] || null;

  app.get("/api/returns/requests/:ref/bp-plan", requireUser, async (req, res) => {
    try {
      await ensureTables();
      const row = await getReturn(req.params.ref);
      if (!row) return res.status(404).json({ error: "no such return" });
      if (row.bp_started_at) return res.json({ already: true, request: row });
      res.json({ plan: await planBrightpearl(bpLive, row) });
    } catch (e) { res.status(500).json({ error: e.message }); }
  });

  app.post("/api/returns/requests/:ref/bp-create", requireUser, async (req, res) => {
    const by = (req.hubUser && req.hubUser.name) || "staff";
    let claimed = null;
    try {
      await ensureTables();
      const row = await getReturn(req.params.ref);
      if (!row) return res.status(404).json({ error: "no such return" });
      if (row.status !== "received") return res.status(409).json({ error: "Mark the return as arrived first - credits are only raised once the goods are back." });
      const claim = await getPool().query(
        `UPDATE returns_requests SET bp_started_at = now() WHERE ref = $1 AND bp_started_at IS NULL RETURNING *`, [row.ref]);
      if (!claim.rowCount) return res.status(409).json({ error: "This return has already been put into Brightpearl.", request: await getReturn(row.ref) });
      claimed = row.ref;
      const plan = await planBrightpearl(bpLive, row);
      const choices = (req.body && req.body.choices) || {};
      const progress = async (patch) => {
        if (patch.bp_credit_id) await getPool().query(`UPDATE returns_requests SET bp_credit_id = $2 WHERE ref = $1`, [row.ref, patch.bp_credit_id]);
        if (patch.bp_exchange_id) await getPool().query(`UPDATE returns_requests SET bp_exchange_id = $2 WHERE ref = $1`, [row.ref, patch.bp_exchange_id]);
      };
      const done = await executeBrightpearl(bpLive, plan, { choices, returnRef: row.ref, progress });

      // What happened, in one line per thing, for the hub row.
      const summary = [`Credit SC#${done.creditId} (GBP ${plan.credit.gross.toFixed(2)})`];
      const swaps = [];
      for (const l of (plan.exchange && plan.exchange.lines) || []) {
        const pick = Object.prototype.hasOwnProperty.call(choices, l.key) ? choices[l.key] : l.suggested;
        const opt = pick && (l.options || []).find((o) => Number(o.productId) === Number(pick));
        swaps.push(opt ? { name: styleName(l.name), size: opt.size || opt.sku, inStock: opt.inStock > 0 }
          : { name: styleName(l.name), size: l.choice === "Something else" ? l.text : "", inStock: false });
        summary.push(opt
          ? `Exchange SO#${done.exchangeId}: sending ${opt.size || opt.sku}${l.from ? ` (${String(l.choice).toLowerCase()} from ${l.from.size})` : ""}${opt.inStock > 0 ? "" : " - NOT IN STOCK, needs ordering"}`
          : `Exchange SO#${done.exchangeId}: ADD THE ITEM - customer wants ${l.choice === "Something else" ? l.text : String(l.choice).toLowerCase()}`);
      }

      const unpaid = !/^PAID$/i.test(String(plan.order.paid || ""));
      if (unpaid) summary.push(`ORIGINAL ORDER NOT PAID (${plan.order.paid || "unknown"}) - check before any refund`);

      // Refunds: accounts pay the money back. Tell them what, how much and how it was paid.
      const refundLines = (row.lines || []).filter((l) => l.outcome === "refund");
      const unitGross = Object.fromEntries(plan.credit.rows.map((r) => [r.rowId, r.unitNet + r.unitTax]));
      const refundAmount = Math.round(refundLines.reduce((a, l) => a + (unitGross[String(l.rowId)] || 0) * Number(l.qty), 0) * 100) / 100;
      let accounts = null;
      const testUnpaid = unpaid && !RETURNS_LIVE();   // test mode: send anyway, marked, so it can be tried
      if (refundLines.length && unpaid && !testUnpaid) {
        summary.push("Accounts NOT emailed: nothing was paid on the original order, so there is nothing to refund");
      } else if (refundLines.length) {
        const amount = refundAmount;
        // accounts@ confirmed by Dec (30 Sep). While RETURNS_LIVE is off, sendMail redirects it to him.
        accounts = { to: process.env.RETURNS_ACCOUNTS_EMAIL || "accounts@tuffshop.co.uk", amount, sent: false, test: !RETURNS_LIVE() };
        let paidBy = "";
        try {
          const methods = Object.fromEntries(((await bpLive("GET", "/accounting-service/payment-method")) || []).map((m) => [m.code, m.name]));
          const s = await bpLive("GET", `/accounting-service/customer-payment-search?orderId=${row.order_id}`);
          const ix = Object.fromEntries(((s && s.metaData && s.metaData.columns) || []).map((c, i) => [c.name, i]));
          paidBy = [...new Set(((s && s.results) || []).filter((x) => x[ix.paymentType] === "RECEIPT").map((x) => methods[x[ix.paymentMethodCode]] || x[ix.paymentMethodCode]))].join(", ");
        } catch { /* the email still goes without it */ }
        const link = (id) => `<a href="https://euw1.brightpearlapp.com/patt-op.php?scode=invoice&oID=${id}">${id}</a>`;
        const td = 'style="padding:6px 10px;border:1px solid #ddd;"';
        try {
          await sendMail({
            to: accounts.to, replyTo: salesMailbox(),
            subject: `Refund to process: GBP ${amount.toFixed(2)} - return ${row.ref} - order ${row.order_ref || row.order_id}`,
            html: `<div style="font-family:Arial,sans-serif;font-size:14px;color:#222;">
              <p>Hi,</p>
              ${testUnpaid ? '<p style="background:#fff4e0;padding:8px 10px;"><b>TEST MODE:</b> the original order was never paid. Once live, this email would NOT be sent for an unpaid order.</p>' : ''}
              <p>A return has come back and needs refunding.</p>
              <table style="border-collapse:collapse;font-size:13px;">
                <tr><td ${td}><b>Refund</b></td><td ${td}><b>&pound;${amount.toFixed(2)}</b> inc VAT</td></tr>
                <tr><td ${td}>Customer</td><td ${td}>${String(row.customer_name || "").replace(/</g, "&lt;")} (${String(row.email || "").replace(/</g, "&lt;")})</td></tr>
                <tr><td ${td}>Paid by</td><td ${td}>${paidBy || "not found - check the order"}</td></tr>
                <tr><td ${td}>Original order</td><td ${td}>SO ${link(row.order_id)} (${String(row.order_ref || "").replace(/</g, "&lt;")})</td></tr>
                <tr><td ${td}>Credit</td><td ${td}>SC ${link(done.creditId)} - set to Refund requested</td></tr>
                <tr><td ${td}>Return</td><td ${td}>${row.ref}</td></tr>
              </table>
              <p>Items refunded:</p><ul>${refundLines.map((l) => `<li>${l.qty} &times; ${String(l.name).replace(/</g, "&lt;")} (${String(l.reason || "").replace(/</g, "&lt;")})</li>`).join("")}</ul>
              ${done.exchangeId ? `<p>The rest of this return was a swap - exchange order SO ${link(done.exchangeId)} has been created against the credit (no payment needed).</p>` : ""}
              <p>Please process the refund and mark the credit complete. Thanks.</p></div>`,
          });
          accounts.sent = true;
        } catch (e) { accounts.error = String(e.message).slice(0, 200); console.error(`[returns] ${row.ref} accounts email failed:`, e.message); }
        summary.push(`Refund GBP ${amount.toFixed(2)}: ${accounts.sent ? "accounts emailed (" + (accounts.test ? "TEST: sent to " + testAddress() : accounts.to) + ")" : "accounts email FAILED - tell them yourself"}`);
      }
      // The customer already said what they wanted, so Process finishes the return:
      // a swap becomes "Exchanged", a refund "Refunded" (with accounts), and the customer
      // is told. An unpaid original order with a refund is left for a person to look at.
      const holdForUnpaid = refundLines.length && unpaid && RETURNS_LIVE();
      const newStatus = holdForUnpaid ? row.status : done.exchangeId ? "exchanged" : "refunded";
      let customerEmailed = null;
      const mail = holdForUnpaid ? null : statusEmail(newStatus, row, "", { refundAmount: refundLines.length ? refundAmount : 0, swaps });
      if (mail) {
        try { await sendMail({ to: row.email, subject: mail.subject, html: mail.html, replyTo: salesMailbox() }); customerEmailed = true; }
        catch (e) { customerEmailed = false; console.error(`[returns] ${row.ref} customer email failed:`, e.message); }
        summary.push(customerEmailed ? `Customer emailed (${RETURNS_LIVE() ? row.email : "TEST: sent to " + testAddress()})` : "Customer email FAILED - let them know yourself");
      }
      const result = { ...done, by, at: new Date().toISOString(), creditGross: plan.credit.gross, creditRef: plan.credit.reference,
        summary, accounts, warnings: [...(plan.warnings || []), ...(done.warnings || [])] };
      const upd = await getPool().query(
        `UPDATE returns_requests SET bp_result = $2, status = $4, updated_at = now(), history = history || $3::jsonb WHERE ref = $1 RETURNING *`,
        [row.ref, JSON.stringify(result), JSON.stringify([{ at: result.at, status: newStatus, by, emailed: customerEmailed,
          note: `Processed: credit SC#${done.creditId}${done.exchangeId ? ", exchange order SO#" + done.exchangeId : ""}${refundLines.length ? (holdForUnpaid ? ", refund NOT requested (order unpaid)" : ", refund GBP " + refundAmount.toFixed(2) + " requested from accounts") : ""}` }]), newStatus]);
      if (newStatus !== row.status) {
        postBpOrderNote(row.order_id, `RETURN ${row.ref} processed by ${by} - ${newStatus === "exchanged" ? "exchange order SO#" + done.exchangeId + " created" : "refund requested from accounts"}` +
          (mail ? (customerEmailed ? `\nCustomer emailed: "${mail.subject}"` : "\nCustomer email FAILED") : "")).catch(() => {});
      }
      res.json({ request: upd.rows[0], result });
    } catch (e) {
      console.error(`[returns] ${req.params.ref} bp-create failed:`, e.message);
      // Keep the claim: something may already exist in Brightpearl. Record the error so
      // the hub shows exactly what was and wasn't made.
      if (claimed) await getPool().query(
        `UPDATE returns_requests SET bp_result = COALESCE(bp_result, '{}'::jsonb) || $2::jsonb WHERE ref = $1`,
        [claimed, JSON.stringify({ error: String(e.message).slice(0, 400), at: new Date().toISOString(), by })]).catch(() => {});
      res.status(500).json({ error: e.message, request: claimed ? await getReturn(claimed).catch(() => null) : null });
    }
  });

  app.post("/api/returns/requests/:ref/status", requireUser, async (req, res) => {
    try {
      await ensureTables();
      const status = String((req.body && req.body.status) || "");
      if (!STATUSES.includes(status)) return res.status(400).json({ error: "unknown status" });
      const note = String((req.body && req.body.note) || "").trim().slice(0, 500);
      const message = String((req.body && req.body.customerMessage) || "").trim().slice(0, 1500);
      const emailCustomer = req.body && req.body.emailCustomer !== false;
      if (status === "rejected" && emailCustomer && !message) {
        return res.status(400).json({ error: "Tell the customer why it's been rejected (the message box), or untick 'Email the customer'." });
      }
      const by = (req.hubUser && req.hubUser.name) || "staff";
      const r = await getPool().query(
        `UPDATE returns_requests SET status = $2, updated_at = now(),
                history = history || $3::jsonb
          WHERE ref = $1 RETURNING *`,
        [req.params.ref, status, JSON.stringify([{ at: new Date().toISOString(), status, by, note }])]);
      if (!r.rowCount) return res.status(404).json({ error: "no such return" });
      let row = r.rows[0];
      // Tell the customer. The status is saved either way; a failed email is recorded
      // on the return so the hub can show it rather than it vanishing.
      let emailed = null;
      const mail = emailCustomer ? statusEmail(status, row, message) : null;
      if (mail) {
        try { await sendMail({ to: row.email, subject: mail.subject, html: mail.html, replyTo: salesMailbox() }); emailed = true; }
        catch (e) { emailed = false; console.error(`[returns] ${row.ref} status email failed:`, e.message); }
        const upd = await getPool().query(
          `UPDATE returns_requests SET history = jsonb_set(history, ARRAY[(jsonb_array_length(history) - 1)::text, 'emailed'], $2::jsonb) WHERE ref = $1 RETURNING *`,
          [row.ref, JSON.stringify(emailed)]);
        if (upd.rowCount) row = upd.rows[0];
      }
      postBpOrderNote(row.order_id, `RETURN ${row.ref} marked ${status.toUpperCase()} by ${by}${note ? " — " + note : ""}` +
        (mail ? (emailed ? `\nCustomer emailed: "${mail.subject}"${message ? " — " + message : ""}` : "\nCustomer email FAILED") : ""))
        .catch((e) => console.error(`[returns] ${row.ref} status note failed:`, e.message));
      res.json({ request: row, emailed });
    } catch (e) { res.status(500).json({ error: e.message }); }
  });
}
