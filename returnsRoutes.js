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
  orderEmail, maskEmail, statusEmail, validatePhotos,
} from "./returns.js";
import { buildBrightpearlReport, summariseOnline } from "./returnsReport.js";


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
  async function sendMail({ to, subject, html, replyTo }) {
    if (graphConfigured()) return sendNew({ to, subject, html, replyTo });
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
        sendNew({
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
  app.get("/api/returns/report", requireUser, async (req, res) => {
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
