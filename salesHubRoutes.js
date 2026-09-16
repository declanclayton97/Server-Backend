// salesHubRoutes.js — the Sales Hub's "answer this email" endpoints.
//
// Kept out of server.js deliberately: that file is edited by several sessions at
// once, so this mounts with a single call and its dependencies are injected
// rather than reached for. Everything decidable without I/O lives in
// salesHub.js; this module only gathers facts and performs the send.

import nodemailer from "nodemailer";
import {
  SALES_INTENTS,
  detectIntent,
  extractOrderNumber,
  extractSentDate,
  notesSince,
  classifyNote,
  assessDuplication,
  buildSalesReply,
  buildSalesNote,
  dateKeys,
} from "./salesHub.js";

const num = (v) => (v == null || v === "" || isNaN(Number(v)) ? null : Number(v));

export function registerSalesHubRoutes(app, deps) {
  const { bpLive, postBpOrderNote, useDatabase, resolveSalesperson } = deps;
  // Read the pool at CALL time, not registration time. server.js declares it
  // with `let` and assigns it separately; destructuring it here would capture
  // whatever it happened to be when the routes were mounted.
  const getPool = () => (typeof deps.pool === "function" ? deps.pool() : deps.pool);

  // ---------------------------------------------------------------------
  // Gather everything we know about one order.
  //
  // Each source is fetched inside its own try: Brightpearl is the only one we
  // cannot do without. If demand_log, the blocked-line log or the email log are
  // unavailable the page still opens, with that panel saying so - a salesperson
  // waiting on a customer should not be blocked by a reporting table.
  // ---------------------------------------------------------------------
  async function gatherOrder(orderId) {
    const resp = await bpLive("GET", `/order-service/order/${orderId}`);
    const order = Array.isArray(resp) ? resp[0] : resp;
    if (!order) return null;

    const cust = (order.parties && order.parties.customer) || {};
    const salesperson = await resolveSalesperson(order.createdById).catch(() => ({ name: "", email: "" }));

    let notes = [];
    try {
      notes = (await bpLive("GET", `/order-service/order/${order.id}/note`)) || [];
    } catch (e) { console.error("[sales-hub] notes failed:", e.message); }

    // Resolve the staff names behind the notes, so the UI can say "Sarah"
    // rather than a contact id. One lookup per distinct author, cached upstream.
    const authors = {};
    for (const id of [...new Set(notes.map((n) => n.addedBy).filter(Boolean))]) {
      try { authors[id] = (await resolveSalesperson(id)).name || `Staff ${id}`; }
      catch { authors[id] = `Staff ${id}`; }
    }

    const timeline = notes
      .map((n) => ({
        addedOn: n.addedOn,
        text: n.text,
        addedBy: authors[n.addedBy] || "",
        orderStatusId: n.orderStatusId,
        kind: classifyNote(n),
      }))
      .sort((a, b) => new Date(b.addedOn) - new Date(a.addedOn));

    // Which purchase orders carry this order's lines — the only honest ETA.
    let pos = [];
    if (useDatabase && getPool()) {
      try {
        const r = await getPool().query(
          `SELECT DISTINCT po_id, supplier FROM demand_log WHERE so_id = $1 AND po_id IS NOT NULL`,
          [order.id]
        );
        const poIds = r.rows.map((x) => Number(x.po_id)).filter(Boolean).sort((a, b) => a - b);
        if (poIds.length) {
          // A Brightpearl path id-list must be ASCENDING or it 400s (CMNC-006).
          const poResp = (await bpLive("GET", `/order-service/order/${poIds.join(",")}`)) || [];
          const bySupplier = Object.fromEntries(r.rows.map((x) => [String(x.po_id), x.supplier]));
          pos = poResp.map((p) => ({
            id: p.id,
            supplier: bySupplier[String(p.id)] || (p.parties && p.parties.supplier && p.parties.supplier.companyName) || "",
            status: (p.orderStatus && p.orderStatus.name) || "",
            placedOn: p.placedOn || p.createdOn || null,
            expectedDate: (p.delivery && p.delivery.deliveryDate) || null,
          }));
        }
      } catch (e) { console.error("[sales-hub] demand_log/PO lookup failed:", e.message); }
    }

    // Lines a supplier could not supply. Promising a date on one of these is
    // the worst email we can send, so the guard needs them.
    //
    // There is no blocked-lines table: the facts live in purchasing_error_log
    // and are read out by extractBlockedLines(), the same function the
    // purchasing hub's Stuck-items tab uses. We find the rows for THIS sales
    // order by way of demand_log, which is what ties a PO back to the customer.
    //
    // Deliberately NOT reproducing that tab's "has this settled itself?" logic.
    // It is subtle, it has already been got wrong once, and the two failure
    // directions here are not equal: showing a line that has since been sorted
    // makes us decline to quote a date we could have quoted, which a
    // salesperson can override in a second. MISSING one makes us promise a
    // delivery date for goods nobody has ordered. So we show it and let them
    // judge — the same call the purchasing hub makes when it cannot tell.
    let blockedLines = [];
    if (useDatabase && getPool()) {
      try {
        const { extractBlockedLines } = await import("./blockedLines.js");
        const poRows = await getPool().query(
          `SELECT DISTINCT po_id FROM demand_log WHERE so_id = $1 AND po_id IS NOT NULL`,
          [order.id]
        );
        const myPos = new Set(poRows.rows.map((x) => Number(x.po_id)));
        if (myPos.size) {
          const errs = await getPool().query(
            `SELECT id, supplier, step, message, context, created_at
               FROM purchasing_error_log
              WHERE severity = 'error'
                AND handled_at IS NULL
                AND created_at > now() - interval '60 days'
              ORDER BY id DESC LIMIT 300`
          );
          // One SKU, one entry. A supplier that failed and retried logs the
          // same line again on every attempt, so PO 489373 listed
          // "835-39-A-XL, 835-39-A-XL, TB150922148" - the same item twice in a
          // warning a person has to read. Keep the OLDEST sighting, which is
          // when the problem actually started.
          const seen = new Map();
          for (const row of errs.rows) {
            const poId = Number((row.context && row.context.poId) || 0);
            if (!poId || !myPos.has(poId)) continue;
            for (const l of extractBlockedLines(row)) {
              const key = String(l.sku || l.name || "").toUpperCase().trim();
              if (!key) continue;
              const entry = {
                sku: l.sku, name: l.name, want: l.want,
                reason: l.reason, supplier: row.supplier,
                step: row.step, since: row.created_at, poId,
              };
              const prev = seen.get(key);
              if (!prev || new Date(entry.since) < new Date(prev.since)) seen.set(key, entry);
            }
          }
          blockedLines = [...seen.values()];
        }
      } catch (e) {
        // A reporting table being unavailable must not stop a salesperson
        // answering a customer — but say so, rather than looking clean.
        console.error("[sales-hub] blocked lines unavailable:", e.message);
        blockedLines = [];
      }
    }

    // What the automated pipeline has already sent them.
    let automatedEmails = [];
    if (useDatabase && getPool()) {
      try {
        const r = await getPool().query(`SELECT emails_sent FROM order_email_log WHERE order_id = $1`, [order.id]);
        const sent = (r.rows[0] && r.rows[0].emails_sent) || [];
        automatedEmails = (Array.isArray(sent) ? sent : []).map((e) =>
          typeof e === "string" ? { state: e, sentAt: null } : { state: e.state || e.name, sentAt: e.sentAt || e.at }
        );
      } catch (e) { console.error("[sales-hub] email log failed:", e.message); }
    }

    const rows = Object.values(order.orderRows || {}).map((r) => ({
      name: r.productName,
      sku: r.productSku,
      quantity: Number((r.quantity && r.quantity.magnitude) || 0),
      // Brightpearl exposes what has shipped per row as a magnitude too; where
      // it is absent treat it as nothing shipped rather than guessing.
      shipped: Number((r.quantity && r.quantity.shipped) || 0),
      outstanding: Math.max(0, Number((r.quantity && r.quantity.magnitude) || 0) - Number((r.quantity && r.quantity.shipped) || 0)),
    }));

    return {
      id: order.id,
      reference: order.reference || String(order.id),
      customerName: cust.companyName || cust.addressFullName || "",
      // Kept apart from customerName on purpose. customerName is usually the
      // COMPANY, and taking a first name off it greets "Airedale Boxing Club"
      // as "Hi Airedale". Only a real person's name is safe to shorten.
      contactName: cust.addressFullName || "",
      customerEmail: cust.email || "",
      status: (order.orderStatus && order.orderStatus.name) || "",
      statusId: order.orderStatus && order.orderStatus.orderStatusId,
      placedOn: order.placedOn || order.createdOn || null,
      total: (order.totalValue && order.totalValue.total) || null,
      salesperson,
      lines: rows,
      pos,
      blockedLines,
      automatedEmails,
      timeline,
    };
  }

  // POST /api/sales-hub/lookup  { query, emailDate? }
  // `query` is either a bare order number or a whole pasted email.
  app.post("/api/sales-hub/lookup", async (req, res) => {
    try {
      const raw = String((req.body && req.body.query) || "").trim();
      if (!raw) return res.status(400).json({ error: "Nothing to look up" });

      const looksLikeBareNumber = /^\d{4,8}$/.test(raw);
      const orderNumber = looksLikeBareNumber ? raw : extractOrderNumber(raw);
      if (!orderNumber) {
        return res.json({
          found: false,
          reason:
            "No order number in that email. Search Brightpearl by the customer's " +
            "address instead, then paste the number in on its own.",
        });
      }

      const order = await gatherOrder(orderNumber);
      if (!order) return res.json({ found: false, reason: `Order ${orderNumber} does not exist in Brightpearl.` });

      // The email's own date drives the whole "have we already told them" check.
      // If we cannot find one, say so rather than assuming now() — assuming now
      // would mark every note as older and silently switch the check off.
      const parsedDate = looksLikeBareNumber ? null : extractSentDate(raw);
      const emailDate = req.body.emailDate || (parsedDate ? parsedDate.toISOString() : null);

      const intent = looksLikeBareNumber ? "eta" : detectIntent(raw);

      res.json({
        found: true,
        order,
        intent,
        emailDate,
        emailDateSource: req.body.emailDate ? "given" : parsedDate ? "parsed" : "unknown",
        intents: SALES_INTENTS.map((i) => ({ key: i.key, label: i.label })),
      });
    } catch (err) {
      console.error("[sales-hub] lookup failed:", err.message);
      res.status(500).json({ error: err.message });
    }
  });

  // POST /api/sales-hub/draft  { orderId, intent, emailDate }
  app.post("/api/sales-hub/draft", async (req, res) => {
    try {
      const orderId = num(req.body && req.body.orderId);
      if (!orderId) return res.status(400).json({ error: "orderId required" });
      const order = await gatherOrder(orderId);
      if (!order) return res.status(404).json({ error: "Order not found" });

      const intent = String((req.body && req.body.intent) || "eta");
      const emailDate = (req.body && req.body.emailDate) || null;

      // Use the PO that finishes LAST: quoting the earliest would promise a
      // delivery before the rest of the order can possibly arrive.
      const po = order.pos
        .slice()
        .sort((a, b) => new Date(b.expectedDate || 0) - new Date(a.expectedDate || 0))[0] || null;

      const draft = buildSalesReply({
        intent, order, po,
        blockedLines: order.blockedLines,
        salesperson: order.salesperson,
      });

      const assessment = assessDuplication({
        notes: order.timeline,
        emailDate,
        proposedDates: draft.proposedDates,
        blockedLines: order.blockedLines,
        automatedEmails: order.automatedEmails,
      });

      res.json({ draft, assessment, po, usedNoEmailDate: !emailDate });
    } catch (err) {
      console.error("[sales-hub] draft failed:", err.message);
      res.status(500).json({ error: err.message });
    }
  });

  // POST /api/sales-hub/send
  // { orderId, to, subject, html, intent, sentBy, acknowledgedLevel }
  //
  // Always called from a human pressing Send on a draft they have read. The
  // acknowledgedLevel is what the page showed them, so overriding a warning is
  // recorded on the order rather than lost.
  app.post("/api/sales-hub/send", async (req, res) => {
    const b = req.body || {};
    try {
      const orderId = num(b.orderId);
      const to = String(b.to || "").trim();
      const subject = String(b.subject || "").trim();
      const html = String(b.html || "");
      if (!orderId || !to || !subject || !html) {
        return res.status(400).json({ error: "orderId, to, subject and html are all required" });
      }
      if (!/^[^@\s]+@[^@\s]+\.[^@\s]+$/.test(to)) {
        return res.status(400).json({ error: `"${to}" is not an email address` });
      }
      if (String(b.acknowledgedLevel) === "blocked" && !b.overrideBlocked) {
        return res.status(409).json({
          error: "This draft was blocked. Re-check it and tick the override if you are sure.",
        });
      }

      const order = await gatherOrder(orderId).catch(() => null);
      const salesperson = (order && order.salesperson) || {};

      const transporter = nodemailer.createTransport({
        host: process.env.SMTP_SERVER || "mail-eu.smtp2go.com",
        port: parseInt(process.env.SMTP_PORT || "2525", 10),
        secure: false,
        auth: { user: process.env.SMTP_USERNAME || "tuffshop.co.uk", pass: process.env.SMTP_PASS },
      });

      const fromName = salesperson.name || "Tuffshop Sales";
      const fromAddress = process.env.SALES_SENDER_EMAIL || process.env.SENDER_EMAIL || "sales@tuffshop.co.uk";

      await transporter.sendMail({
        from: `"${fromName}" <${fromAddress}>`,
        // The customer replies to the person who owns the order, not a shared
        // address nobody watches.
        replyTo: salesperson.email || fromAddress,
        to,
        subject,
        html,
      });

      const note = buildSalesNote({
        intent: b.intent || "eta",
        to, subject,
        sentBy: b.sentBy || salesperson.name || "",
        proposedDates: Array.isArray(b.proposedDates) ? b.proposedDates : dateKeys(html),
        duplicationLevel: b.acknowledgedLevel,
      });
      let noted = true;
      try { await postBpOrderNote(orderId, note); }
      catch (e) { noted = false; console.error("[sales-hub] note failed:", e.message); }

      // The email has gone. A failed note must not read as a failed send, or
      // somebody will send it a second time.
      res.json({ sent: true, noted, note });
    } catch (err) {
      console.error("[sales-hub] send failed:", err.message);
      res.status(500).json({ sent: false, error: err.message });
    }
  });

  // GET /api/sales-hub/notes/:orderId?since=ISO — the timeline panel on its own.
  app.get("/api/sales-hub/notes/:orderId", async (req, res) => {
    try {
      const order = await gatherOrder(req.params.orderId);
      if (!order) return res.status(404).json({ error: "Order not found" });
      const since = req.query.since || null;
      res.json({
        timeline: order.timeline,
        since: since ? notesSince(order.timeline, since) : [],
      });
    } catch (err) {
      res.status(500).json({ error: err.message });
    }
  });
}
