// salesHubRoutes.js — the Sales Hub's "answer this email" endpoints.
//
// Kept out of server.js deliberately: that file is edited by several sessions at
// once, so this mounts with a single call and its dependencies are injected
// rather than reached for. Everything decidable without I/O lives in
// salesHub.js; this module only gathers facts and performs the send.

import nodemailer from "nodemailer";
import { graphConfigured, salesMailbox, listInbox, getMessage, composeAndSend, saveDraft, deleteDraft, listThread, mailboxContext, currentMailbox, inboxUnread, splitAddresses, FOLDERS, moveMessage, markRead, setRead, getAttachment } from "./graphMail.js";
import { SIGNATURE_HTML } from "./emailSignature.js";
import {
  SALES_INTENTS,
  detectIntent,
  extractOrderNumber,
  looksLikeOrderId,
  extractSentDate,
  notesSince,
  classifyNote,
  classifyOrderRow,
  lastContact,
  assessDuplication,
  buildSalesReply,
  buildSalesNote,
  promisedWindow,
  emailToNoteText,
  initialsOf,
  returnRef,
} from "./salesHub.js";

const num = (v) => (v == null || v === "" || isNaN(Number(v)) ? null : Number(v));

export function registerSalesHubRoutes(app, deps) {
  // Only the Sales Hub page calls these routes, so they can require a signed-in
  // person outright — unlike the older shared endpoints, which the React app and
  // the purchasing hub also call. Registered by hubAuthRoutes, which mounts first.
  const requireUser = (req, res, next) => app.locals.requireHubUser(req, res, next);
  const { bpLive, postBpOrderNote, useDatabase, resolveSalesperson } = deps;
  // Read the pool at CALL time, not registration time. server.js declares it
  // with `let` and assigns it separately; destructuring it here would capture
  // whatever it happened to be when the routes were mounted.
  const getPool = () => (typeof deps.pool === "function" ? deps.pool() : deps.pool);

  // ── Your own mailbox as well as sales@ (Dec, 7 Oct) ────────────────────────
  // ?box=me on any /api/sales-hub/* call runs it against the SIGNED-IN person's own mailbox.
  // Never an address from the page: the server works out whose mailbox "me" is, so nobody
  // can open a colleague's. Address: HUB_MAILBOXES (JSON {hubNameKey: "x@tuffshop.co.uk"})
  // first, else Brightpearl's staff list matched on the hub name (email local part, full
  // name, or a first name only one member of staff has). The Microsoft app must also be
  // granted that mailbox in Exchange, or Graph refuses (the page says "not set up yet").
  let staffCache = { at: 0, list: [] };
  async function staffList() {
    if (Date.now() - staffCache.at < 60 * 60e3 && staffCache.list.length) return staffCache.list;
    const r = await bpLive("GET", "/contact-service/contact-search?isStaff=true&pageSize=200");
    const cols = r.metaData.columns.map((c) => c.name);
    const list = r.results.map((x) => Object.fromEntries(cols.map((c, i) => [c, x[i]])))
      .map((c) => ({ first: String(c.firstName || "").trim().toLowerCase(), last: String(c.lastName || "").trim().toLowerCase(), email: String(c.primaryEmail || "").trim().toLowerCase() }))
      .filter((c) => /@tuffshop\.co\.uk$/.test(c.email));
    staffCache = { at: Date.now(), list };
    return list;
  }
  async function myMailbox(user) {
    if (!user) return null;
    try {
      const map = JSON.parse(process.env.HUB_MAILBOXES || "{}");
      const hit = map[user.key] || map[String(user.name || "").toLowerCase()];
      if (hit) return String(hit).toLowerCase();
    } catch { /* bad JSON: fall through to the staff list */ }
    const name = String(user.name || "").trim().toLowerCase().replace(/\s+/g, " "), key = String(user.key || "").toLowerCase();
    const staff = await staffList().catch(() => []);
    const byLocal = staff.filter((s) => [name, key].includes(s.email.split("@")[0]));
    if (byLocal.length === 1) return byLocal[0].email;
    const byFull = staff.filter((s) => `${s.first} ${s.last}` === name);
    if (byFull.length === 1) return byFull[0].email;
    const byFirst = staff.filter((s) => s.first === name.split(" ")[0]);
    return byFirst.length === 1 ? byFirst[0].email : null;
  }
  app.use("/api/sales-hub", async (req, res, next) => {
    if (req.query.box !== "me") return next();
    try {
      const user = await app.locals.hubUserFromToken(req);
      if (!user) return res.status(401).json({ error: "Sign in to the Sales Hub first" });
      const box = await myMailbox(user);
      if (!box) return res.status(404).json({ error: "Couldn't work out which mailbox is yours - ask Dec to add you to HUB_MAILBOXES" });
      mailboxContext.run({ box }, next);
    } catch (e) { res.status(500).json({ error: e.message }); }
  });

  // GET /api/sales-hub/mailbox-check - Dec only: for EVERY Sales Hub user, which mailbox the hub
  // maps them to and whether Microsoft lets the app open it (unread count, or the refusal).
  // Counts only - no mail is read.
  app.get("/api/sales-hub/mailbox-check", requireUser, async (req, res) => {
    const admins = String(process.env.CALL_REPORT_USERS || "dec,dec clayton,declan,declan clayton").split(",").map((s) => s.trim().toLowerCase());
    if (![req.hubUser.key, req.hubUser.name].some((v) => admins.includes(String(v || "").toLowerCase()))) return res.status(403).json({ error: "Not available on your account" });
    try {
      const users = (await getPool().query(`SELECT name_key, display_name FROM hub_users ORDER BY display_name`)).rows;
      const out = [];
      for (const u of users) {
        const box = await myMailbox({ key: u.name_key, name: u.display_name }).catch(() => null);
        if (!box) { out.push({ user: u.display_name, mailbox: null, ok: false, why: "no mailbox mapped" }); continue; }
        try { const n = await inboxUnread(box); out.push({ user: u.display_name, mailbox: box, ok: true, unread: n.unread }); }
        catch (e) { out.push({ user: u.display_name, mailbox: box, ok: false, why: e.status === 403 ? "access denied" : e.message }); }
      }
      // ?also=a@tuffshop.co.uk,b@... tests mailboxes of people with no hub account yet (our domain only).
      for (const box of String(req.query.also || "").split(",").map((s) => s.trim().toLowerCase()).filter((s) => /^[^@s]+@tuffshop.co.uk$/.test(s))) {
        try { const n = await inboxUnread(box); out.push({ user: "(no hub account)", mailbox: box, ok: true, unread: n.unread }); }
        catch (e) { out.push({ user: "(no hub account)", mailbox: box, ok: false, why: e.status === 403 ? "access denied" : e.message }); }
      }
      res.json({ users: out });
    } catch (e) { res.status(500).json({ error: e.message }); }
  });

  // GET /api/sales-hub/mailboxes - the accounts in the folder pane, with their unread counts.
  app.get("/api/sales-hub/mailboxes", requireUser, async (req, res) => {
    const out = [];
    try { out.push({ key: "sales", label: "Tuffshop Sales", address: salesMailbox(), ...(await inboxUnread(salesMailbox())) }); }
    catch (e) { out.push({ key: "sales", label: "Tuffshop Sales", address: salesMailbox(), error: e.message }); }
    const mine = await myMailbox(req.hubUser).catch(() => null);
    if (mine && mine !== salesMailbox().toLowerCase()) {
      try { out.push({ key: "me", label: req.hubUser.name, address: mine, ...(await inboxUnread(mine)) }); }
      catch (e) { out.push({ key: "me", label: req.hubUser.name, address: mine, error: e.status === 403 || e.status === 404 ? "Not set up yet - waiting on mailbox access" : e.message }); }
    }
    res.json({ mailboxes: out });
  });

  // ---------------------------------------------------------------------
  // Gather everything we know about one order.
  //
  // Each source is fetched inside its own try: Brightpearl is the only one we
  // cannot do without. If demand_log, the blocked-line log or the email log are
  // unavailable the page still opens, with that panel saying so - a salesperson
  // waiting on a customer should not be blocked by a reporting table.
  // ---------------------------------------------------------------------
  /**
   * Turn whatever the customer quoted into a Brightpearl order id.
   *
   * A web customer quotes the number on their own confirmation — "000121305" —
   * which is the order's REFERENCE, not its id. Brightpearl can search that
   * (customerRef), so try the id first when the token looks like one, and fall
   * back to a reference search either way.
   *
   * Returns { id, via } so the UI can say how it found the order, or null.
   */
  async function resolveOrderId(token) {
    const raw = String(token || "").trim();
    if (!raw) return null;

    // A bare number is ambiguous. Magento web orders are "000123384", and people
    // drop the zeros — but 123384 is ALSO a real Brightpearl id, of an order from
    // years ago. So try it both ways and take the more recent order, rather than
    // letting the id win just because it was tried first.
    let byId = null, notSale = null;
    if (looksLikeOrderId(raw)) {
      try {
        const r = await bpLive("GET", `/order-service/order/${raw}`);
        const o = Array.isArray(r) ? r[0] : r;
        // SALES orders only (Dec, 2 Oct: a supplier's email naming a PO number linked the
        // hub to the PO). A purchase order is followed to the one sale it was raised for.
        const type = String((o && (o.orderTypeCode || o.orderTypeId)) || "");
        if (o && o.id && (type === "SO" || type === "1")) byId = { id: o.id, via: "id", createdOn: o.createdOn || o.placedOn };
        else if (o && o.id) {
          notSale = { id: o.id, type };
          if ((type === "PO" || type === "2") && useDatabase && getPool()) {
            const sos = (await getPool().query(`SELECT DISTINCT so_id FROM demand_log WHERE po_id = $1 AND so_id IS NOT NULL`, [o.id])).rows.map((x) => x.so_id);
            if (sos.length === 1) {
              const so = await bpLive("GET", `/order-service/order/${sos[0]}`).catch(() => null);
              const s = Array.isArray(so) ? so[0] : so;
              if (s && s.id) byId = { id: s.id, via: "po", poId: o.id, createdOn: s.createdOn || s.placedOn };
            } else notSale.sales = sos.length;
          }
        }
      } catch (e) { /* not an id, or gone — try it as a reference */ }
    }
    const refs = [raw];
    if (/^\d{4,8}$/.test(raw)) refs.push(raw.padStart(9, "0"));   // Magento pads to 9
    let byRef = null;
    for (const ref of [...new Set(refs)]) {
      const hit = await findByCustomerRef(ref);
      if (hit && (!byRef || new Date(hit.createdOn) > new Date(byRef.createdOn))) byRef = hit;
    }
    if (byId && byRef) {
      const [win, other] = new Date(byRef.createdOn) > new Date(byId.createdOn) ? [byRef, byId] : [byId, byRef];
      return { ...win, alsoMatched: (win.alsoMatched || 0) + 1, otherMatch: { id: other.id, via: other.via } };
    }
    return byId || byRef || (notSale ? { notSale } : null);
  }

  // Sales orders whose customer reference (the web / Magento order number) is this.
  async function findByCustomerRef(raw) {
    try {
      const s = await bpLive("GET", `/order-service/order-search?customerRef=${encodeURIComponent(raw)}&pageSize=20`);
      const md = s && s.metaData;
      if (!md) return null;
      const ix = {};
      (md.columns || []).forEach((c, i) => { ix[c.name] = i; });
      const rows = (s.results || [])
        // SALES orders only. The same reference can appear on a credit, and
        // answering a customer about their credit note is not the question
        // they asked.
        .filter((r) => Number(r[ix.orderTypeId]) === 1)
        .sort((a, b) => new Date(b[ix.createdOn]) - new Date(a[ix.createdOn]));
      if (!rows.length) return null;
      return { id: rows[0][ix.orderId], via: "reference", alsoMatched: rows.length - 1, createdOn: rows[0][ix.createdOn] };
    } catch (e) {
      console.error("[sales-hub] reference lookup failed:", e.message);
      return null;
    }
  }

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
          pos = poResp
            .map((p) => ({
              id: p.id,
              supplier: bySupplier[String(p.id)] || (p.parties && p.parties.supplier && p.parties.supplier.companyName) || "",
              status: (p.orderStatus && p.orderStatus.name) || "",
              placedOn: p.placedOn || p.createdOn || null,
              supplierContactId: (p.parties && p.parties.supplier && p.parties.supplier.contactId) || null,
              expectedDate: (p.delivery && p.delivery.deliveryDate) || null,
              stockStatus: p.stockStatusCode || null,   // POA = everything received
            }))
            // A CANCELLED PO is not a commitment to anything. demand_log keeps a
            // row for every attempt, so a rebuilt order leaves the abandoned PO
            // behind: 487877 listed 488023 "Placed with supplier" AND 487979
            // "PO Cancelled", and the cancelled one can win the ETA because the
            // draft takes the latest expected date.
            .filter((p) => !/cancel/i.test(p.status));
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

        // WHOSE line is it? A PO carries demand from many sales orders, so
        // "this line failed on a PO your order is also on" is not the same as
        // "your item failed". Order 487877 was shown 12180400006 as stuck when
        // its only garment is 60730400005 - somebody else's problem, on the
        // shared PO 488023.
        //
        // Ask demand_log who each SKU on these POs belongs to:
        //   ours      -> show it
        //   theirs    -> drop it, it is not this customer's concern
        //   unknown   -> KEEP it, flagged. A blocked line often carries the
        //                supplier's own code where the PO row carries ours, and
        //                silently dropping one of those would let us promise a
        //                date for goods nobody has ordered.
        const owner = new Map();                       // SKU -> Set(so_id)
        if (myPos.size) {
          const dl = await getPool().query(
            `SELECT sku, so_id FROM demand_log WHERE po_id = ANY($1::int[]) AND sku IS NOT NULL`,
            [[...myPos]]
          );
          for (const d of dl.rows) {
            const k = String(d.sku).toUpperCase().trim();
            if (!owner.has(k)) owner.set(k, new Set());
            owner.get(k).add(Number(d.so_id));
          }
        }
        const mine = (sku) => {
          const k = String(sku || "").toUpperCase().trim();
          if (!k || !owner.has(k)) return { ours: true, confirmed: false };   // unknown -> keep, flagged
          return { ours: owner.get(k).has(Number(order.id)), confirmed: true };
        };
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
              const own = mine(l.sku);
              if (!own.ours) continue;                 // another customer's line on a shared PO
              const entry = {
                sku: l.sku, name: l.name, want: l.want,
                reason: l.reason, supplier: row.supplier,
                step: row.step, since: row.created_at, poId,
                // false = we could not tie this SKU to any sales order, so it
                // is shown but should not be stated as fact to the customer.
                confirmed: own.confirmed,
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

    // Which rows are actually GOODS. Needs the product records, so one batch
    // lookup for the handful of distinct products on the order.
    const rawRows = Object.values(order.orderRows || {});
    const rowMeta = {};
    try {
      const ids = [...new Set(rawRows.map((r) => Number(r.productId)).filter(Boolean))].sort((a, b) => a - b);
      if (ids.length) {
        // Ascending, or Brightpearl 400s on the id-list (CMNC-006).
        const prods = (await bpLive("GET", `/product-service/product/${ids.join(",")}`)) || [];
        for (const p of prods) {
          rowMeta[p.id] = { stockTracked: !!(p.stock && p.stock.stockTracked), brandId: p.brandId };
        }
      }
    } catch (e) {
      // Unresolved products fall through as goods, which shows one row too many
      // rather than hiding something the customer paid for.
      console.error("[sales-hub] product lookup failed:", e.message);
    }

    const allRows = rawRows.map((r) => {
      const qty = Number((r.quantity && r.quantity.magnitude) || 0);
      // Brightpearl exposes what has shipped per row as a magnitude too; where
      // it is absent treat it as nothing shipped rather than guessing.
      const shipped = Number((r.quantity && r.quantity.shipped) || 0);
      return {
        name: r.productName,
        sku: r.productSku,
        quantity: qty,
        shipped,
        outstanding: Math.max(0, qty - shipped),
        kind: classifyOrderRow(r, rowMeta[Number(r.productId)]),
      };
    });
    // `lines` is what the CUSTOMER thinks they bought. Decoration, carriage and
    // the instruction rows staff type onto an order are not items.
    const rows = allRows.filter((r) => r.kind === "goods");

    // What the SUPPLIER has, for each item still waiting on a purchase order: their
    // own stock feed / portal via Alternate-Items (/api/supplier-stock). A PO's date
    // says nothing about whether the supplier can fill it — SO 491385's Portwest belt
    // read "running a little later than expected" while Portwest had none until
    // 22/01/27 (Dec, 1 Oct). Which lines: anything not yet sent that Brightpearl has NOT
    // reserved from our own stock — whatever state its PO is in. 491385's belt had been
    // DROPPED from its PO (PO 491669 arrived without it), so "lines on an open PO" missed
    // it entirely. The supplier is the one it was ordered from (demand_log). Items in our
    // own stock are never checked. Best-effort: a slow or failing feed leaves it unchecked.
    let supplierStock = [];
    if (useDatabase && getPool()) {
      try {
        const reserved = {};
        try {
          const res = (await bpLive("GET", `/warehouse-service/order/${order.id}/reservation`)) || [];
          for (const w of Array.isArray(res) ? res : [res]) {
            for (const [rowId, r] of Object.entries((w && w.orderRows) || {})) {
              const sku = String(((order.orderRows || {})[rowId] || {}).productSku || "").toUpperCase();
              if (sku) reserved[sku] = (reserved[sku] || 0) + Number(r.quantity || 0);
            }
          }
        } catch { /* no reservation read: treat nothing as reserved, the supplier check still helps */ }
        const dl = await getPool().query(
          `SELECT DISTINCT ON (upper(sku)) po_id, supplier, sku FROM demand_log WHERE so_id = $1 AND sku IS NOT NULL AND supplier IS NOT NULL ORDER BY upper(sku), po_id DESC NULLS LAST`, [order.id]);
        const waiting = dl.rows
          .map((d) => ({ d, line: rows.find((r) => String(r.sku || "").toUpperCase() === String(d.sku).toUpperCase()) }))
          .filter((x) => x.line && x.line.outstanding - (reserved[String(x.line.sku).toUpperCase()] || 0) > 0)
          .slice(0, 5);
        const open = new Map(pos.map((p) => [Number(p.id), p]));
        const ALT = process.env.ALT_ITEMS_URL || "https://alternate-items.onrender.com";
        supplierStock = (await Promise.all(waiting.map(async ({ d, line }) => {
          const ctl = new AbortController();
          const timer = setTimeout(() => ctl.abort(), 12000);
          try {
            const supplier = String(d.supplier || (open.get(Number(d.po_id)) || {}).supplier || "");
            const q = new URLSearchParams({ supplier, sku: line.sku, name: line.name || "" });
            const r = await fetch(`${ALT}/api/supplier-stock?${q}`, { signal: ctl.signal });
            const j = await r.json();
            if (!j || !j.found) return null;
            // Some feeds give only a status (Snickers: avail null, "In stock"). Number(null) is 0,
            // which read as OUT of stock on SO 491442 (Dec, 2 Oct): no number means no number.
            const avail = j.avail === null || j.avail === undefined || j.avail === "" ? (/out of stock|no stock/i.test(String(j.status || "")) ? 0 : null) : Number(j.avail);
            return { name: line.name, sku: line.sku, supplier, poId: Number(d.po_id), avail, status: j.status || null, deldate: j.deldate || null };
          } catch { return null; } finally { clearTimeout(timer); }
        }))).filter(Boolean);
      } catch (e) { console.error("[sales-hub] supplier stock check failed:", e.message); }
    }

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
      supplierStock,
      // Everything on the order, kinds included, for the UI to show in full.
      allRows,
      pos,
      blockedLines,
      automatedEmails,
      timeline,
      // The last time anyone actually spoke to this customer, machine notes
      // excluded. Shown on the page whether or not we know the email's date:
      // the salesperson can see their own email's date, so this one line tells
      // them whether their reply is already stale.
      lastContact: lastContact(timeline),
    };
  }

  // ── Who is working on which email ──────────────────────────────────────────
  // Opening an email in the hub claims it. The page renews the claim every 30s
  // while it stays open and visible, so a closed tab or a laptop lid frees it
  // within LOCK_TTL without anyone having to remember to let go. A second person
  // opening it sees who has it and cannot reply — enforced here on send, not
  // just hidden on the page.
  const LOCK_TTL_SEC = 90;
  let locksReady = null;
  const ensureLocks = () => (locksReady ||= getPool().query(`CREATE TABLE IF NOT EXISTS sales_hub_email_locks (
      message_id text PRIMARY KEY,
      user_key text NOT NULL,
      user_name text NOT NULL,
      claimed_at timestamptz NOT NULL DEFAULT now(),
      heartbeat_at timestamptz NOT NULL DEFAULT now()
    )`).catch((e) => { locksReady = null; throw e; }));

  // Live locks held by OTHER people, keyed by message id.
  async function othersLocks(me) {
    if (!(useDatabase && getPool())) return {};
    await ensureLocks();
    const r = await getPool().query(
      `SELECT message_id, user_name, claimed_at FROM sales_hub_email_locks
        WHERE heartbeat_at > now() - ($1 || ' seconds')::interval AND user_key <> $2`, [String(LOCK_TTL_SEC), me]);
    return Object.fromEntries(r.rows.map((x) => [x.message_id, { name: x.user_name, since: x.claimed_at }]));
  }

  // POST /api/sales-hub/inbox/:id/claim — take (or renew) the email, unless
  // someone else holds a live claim on it, in which case say who.
  app.post("/api/sales-hub/inbox/:id/claim", requireUser, async (req, res) => {
    if (!(useDatabase && getPool())) return res.json({ ok: true, locking: false });
    try {
      await ensureLocks();
      const me = req.hubUser;
      // One statement, so two people clicking at once cannot both win: the row is
      // only overwritten when it is ours already or its holder has gone quiet.
      const r = await getPool().query(
        `INSERT INTO sales_hub_email_locks (message_id, user_key, user_name) VALUES ($1, $2, $3)
         ON CONFLICT (message_id) DO UPDATE
           SET user_key = EXCLUDED.user_key, user_name = EXCLUDED.user_name, heartbeat_at = now(),
               claimed_at = CASE WHEN sales_hub_email_locks.user_key = EXCLUDED.user_key
                                 THEN sales_hub_email_locks.claimed_at ELSE now() END
         WHERE sales_hub_email_locks.user_key = EXCLUDED.user_key
            OR sales_hub_email_locks.heartbeat_at <= now() - ($4 || ' seconds')::interval
         RETURNING user_key`,
        [req.params.id, me.key, me.name, String(LOCK_TTL_SEC)]);
      if (r.rowCount) return res.json({ ok: true, locking: true });
      const h = await getPool().query(`SELECT user_name, claimed_at FROM sales_hub_email_locks WHERE message_id = $1`, [req.params.id]);
      res.status(409).json({ ok: false, lockedBy: h.rows[0] && h.rows[0].user_name, since: h.rows[0] && h.rows[0].claimed_at });
    } catch (err) {
      // A broken lock table must not stop anyone answering email — fail open, loudly.
      console.error("[sales-hub] claim failed:", err.message);
      res.json({ ok: true, locking: false, error: err.message });
    }
  });

  // POST /api/sales-hub/inbox/:id/release — let go (only ever of our own claim).
  app.post("/api/sales-hub/inbox/:id/release", requireUser, async (req, res) => {
    if (!(useDatabase && getPool())) return res.json({ ok: true });
    try {
      await ensureLocks();
      await getPool().query(`DELETE FROM sales_hub_email_locks WHERE message_id = $1 AND user_key = $2`, [req.params.id, req.hubUser.key]);
      res.json({ ok: true });
    } catch (err) { res.status(500).json({ error: err.message }); }
  });

  // POST /api/sales-hub/inbox/:id/read { isRead } — Outlook's read/unread toggle,
  // written to the real mailbox so everyone sees it.
  app.post("/api/sales-hub/inbox/:id/read", requireUser, async (req, res) => {
    if (!graphConfigured()) return res.status(503).json({ error: "Outlook is not connected" });
    try {
      await setRead(req.params.id, !!(req.body && req.body.isRead));
      res.json({ ok: true, isRead: !!(req.body && req.body.isRead) });
    } catch (err) { res.status(err.status === 404 ? 404 : 500).json({ error: err.message }); }
  });

  // GET /api/sales-hub/inbox?top=30&unread=1&folder=inbox|sent|drafts|deleted|junk|archive —
  // a folder of the sales mailbox, newest first, each with the order number it names (if any).
  app.get("/api/sales-hub/inbox", requireUser, async (req, res) => {
    if (!graphConfigured()) return res.status(503).json({ error: "Outlook is not connected — set MS_TENANT_ID, MS_CLIENT_ID and MS_CLIENT_SECRET" });
    try {
      // ?search= searches the whole folder on the server; ?next= is the page after the last one.
      const folder = FOLDERS[req.query.folder] ? String(req.query.folder) : "inbox";
      const page = await listInbox({ top: req.query.top, unreadOnly: req.query.unread === "1", folder,
        search: String(req.query.search || ""), next: String(req.query.next || "") });
      const msgs = page.messages;
      const locks = await othersLocks(req.hubUser.key).catch(() => ({}));
      const drafting = await draftsBySource(msgs.map((m) => m.id)).catch(() => ({}));
      res.json({
        mailbox: currentMailbox(),
        folder,
        next: page.next,
        messages: msgs.map((m) => ({
          draft: drafting[m.id] || null,
          ...m,
          orderNumber: extractOrderNumber(`${m.subject}\n${m.preview}`) || null,
          viewing: locks[m.id] || null,
        })),
      });
    } catch (err) {
      console.error("[sales-hub] inbox failed:", err.message);
      res.status(err.status === 403 ? 403 : 500).json({ error: err.message });
    }
  });

  // GET /api/sales-hub/inbox/:id — one email as plain text, ready for the lookup. Its
  // receivedAt is the email's real date, so the stale check never has to guess one.
  app.get("/api/sales-hub/inbox/:id", requireUser, async (req, res) => {
    if (!graphConfigured()) return res.status(503).json({ error: "Outlook is not connected" });
    try {
      const m = await getMessage(req.params.id);
      res.json({ ...m, orderNumber: extractOrderNumber(`${m.subject}\n${m.text}`) || null });
    } catch (err) {
      res.status(err.status === 404 ? 404 : 500).json({ error: err.message });
    }
  });

  // POST /api/sales-hub/inbox/:id/move { to: "deleted" | "inbox" | ... } — Delete is a move to
  // Deleted Items (Outlook's own behaviour, recoverable); Restore moves it back. Refused while
  // a colleague is working on it, so nobody's half-written reply loses its email.
  app.post("/api/sales-hub/inbox/:id/move", requireUser, async (req, res) => {
    if (!graphConfigured()) return res.status(503).json({ error: "Outlook is not connected" });
    const to = String((req.body && req.body.to) || "");
    if (!FOLDERS[to]) return res.status(400).json({ error: "Unknown folder" });
    try {
      const held = (await othersLocks(req.hubUser.key).catch(() => ({})))[req.params.id];
      if (held) return res.status(423).json({ error: `${held.name} is working on this email — not moved.` });
      const r = await moveMessage(req.params.id, to);
      if (useDatabase && getPool()) getPool().query(`DELETE FROM sales_hub_email_locks WHERE message_id = $1`, [req.params.id]).catch(() => {});
      console.log(`[sales-hub] ${req.hubUser.name} moved an email to ${to}`);
      res.json({ ok: true, id: r.id, to });
    } catch (err) { res.status(err.status === 404 ? 404 : 500).json({ error: err.message }); }
  });

  // GET /api/sales-hub/thread/:conversationId — the whole conversation, every folder, oldest first.
  app.get("/api/sales-hub/thread/:conversationId", requireUser, async (req, res) => {
    if (!graphConfigured()) return res.status(503).json({ error: "Outlook is not connected" });
    try { res.json({ messages: await listThread(req.params.conversationId) }); }
    catch (err) { res.status(500).json({ error: err.message }); }
  });

  // ── Drafts ─────────────────────────────────────────────────────────────────
  // A compose is saved as a real Outlook draft as it is typed (it shows in Drafts in Outlook
  // too). The row here remembers what Outlook cannot tell us back: our text apart from the
  // quoted original it sits on, which email it answers, and who is writing it.
  let draftsReady = null;
  const ensureDrafts = () => (draftsReady ||= getPool().query(`CREATE TABLE IF NOT EXISTS sales_hub_drafts (
      draft_id text PRIMARY KEY, source_id text, mode text NOT NULL, quoted text NOT NULL DEFAULT '',
      body text NOT NULL DEFAULT '', subject text, to_list text, cc_list text, bcc_list text,
      order_id bigint, intent text, user_key text, user_name text,
      updated_at timestamptz NOT NULL DEFAULT now())`)
    .then(() => getPool().query(`CREATE INDEX IF NOT EXISTS sales_hub_drafts_source ON sales_hub_drafts (source_id)`))
    .catch((e) => { draftsReady = null; throw e; }));
  const draftRow = (r) => r && ({ draftId: r.draft_id, sourceId: r.source_id, mode: r.mode, body: r.body, subject: r.subject,
    to: r.to_list, cc: r.cc_list, bcc: r.bcc_list, orderId: r.order_id ? Number(r.order_id) : null, intent: r.intent,
    by: r.user_name, byKey: r.user_key, at: r.updated_at });
  async function draftsBySource(ids) {
    if (!(useDatabase && getPool()) || !ids.length) return {};
    await ensureDrafts();
    const r = await getPool().query(`SELECT source_id, user_name, updated_at FROM sales_hub_drafts WHERE source_id = ANY($1)`, [ids]);
    return Object.fromEntries(r.rows.map((x) => [x.source_id, { by: x.user_name, at: x.updated_at }]));
  }

  // POST /api/sales-hub/drafts { draftId?, mode, messageId?, to, cc, bcc, subject, html, orderId?, intent? }
  app.post("/api/sales-hub/drafts", requireUser, async (req, res) => {
    if (!graphConfigured()) return res.status(503).json({ error: "Outlook is not connected" });
    if (!(useDatabase && getPool())) return res.status(503).json({ error: "Drafts need the database" });
    const b = req.body || {};
    try {
      await ensureDrafts();
      const prev = b.draftId ? (await getPool().query(`SELECT * FROM sales_hub_drafts WHERE draft_id = $1`, [String(b.draftId)])).rows[0] : null;
      const mode = prev ? prev.mode : (["reply", "replyAll", "forward", "new"].includes(b.mode) ? b.mode : "new");
      const sourceId = prev ? prev.source_id : (mode !== "new" && b.messageId ? String(b.messageId) : null);
      if (sourceId && (mode === "reply" || mode === "replyAll")) {
        const held = (await othersLocks(req.hubUser.key).catch(() => ({})))[sourceId];
        if (held) return res.status(423).json({ error: `${held.name} is working on this email.` });
      }
      const html = String(b.html || "");
      const saved = await saveDraft({ draftId: b.draftId || undefined, quoted: prev ? prev.quoted : "", mode, sourceId,
        to: splitAddresses(b.to), cc: splitAddresses(b.cc), bcc: splitAddresses(b.bcc), subject: String(b.subject || ""), html });
      await getPool().query(
        `INSERT INTO sales_hub_drafts (draft_id, source_id, mode, quoted, body, subject, to_list, cc_list, bcc_list, order_id, intent, user_key, user_name)
         VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13)
         ON CONFLICT (draft_id) DO UPDATE SET body = EXCLUDED.body, subject = EXCLUDED.subject, to_list = EXCLUDED.to_list,
           cc_list = EXCLUDED.cc_list, bcc_list = EXCLUDED.bcc_list, order_id = EXCLUDED.order_id, intent = EXCLUDED.intent,
           user_key = EXCLUDED.user_key, user_name = EXCLUDED.user_name, updated_at = now()`,
        [saved.draftId, sourceId, mode, saved.quoted || "", html, String(b.subject || ""), String(b.to || ""), String(b.cc || ""), String(b.bcc || ""),
         num(b.orderId) || null, b.intent ? String(b.intent) : null, req.hubUser.key, req.hubUser.name]);
      res.json({ ok: true, draftId: saved.draftId, savedAt: new Date().toISOString() });
    } catch (err) {
      console.error("[sales-hub] draft save failed:", err.message);
      res.status(err.status === 404 ? 404 : 500).json({ error: err.message });
    }
  });

  // GET /api/sales-hub/drafts?source=<messageId> | ?draft=<draftId> — the saved draft to carry on with.
  app.get("/api/sales-hub/drafts", requireUser, async (req, res) => {
    if (!(useDatabase && getPool())) return res.json({ draft: null });
    try {
      await ensureDrafts();
      const r = req.query.draft
        ? await getPool().query(`SELECT * FROM sales_hub_drafts WHERE draft_id = $1`, [String(req.query.draft)])
        : await getPool().query(`SELECT * FROM sales_hub_drafts WHERE source_id = $1 ORDER BY updated_at DESC LIMIT 1`, [String(req.query.source || "")]);
      res.json({ draft: draftRow(r.rows[0]) || null });
    } catch (err) { res.status(500).json({ error: err.message }); }
  });

  // DELETE /api/sales-hub/drafts/:draftId — Discard: deletes the Outlook draft (only ever a draft).
  app.delete("/api/sales-hub/drafts/:draftId", requireUser, async (req, res) => {
    try {
      try { await deleteDraft(req.params.draftId); }
      catch (e) { if (e.status !== 404) throw e; }   // already gone (sent or deleted in Outlook)
      if (useDatabase && getPool()) { await ensureDrafts(); await getPool().query(`DELETE FROM sales_hub_drafts WHERE draft_id = $1`, [req.params.draftId]); }
      res.json({ ok: true });
    } catch (err) { res.status(500).json({ error: err.message }); }
  });

  // GET /api/sales-hub/signature — the signature a new email or forward starts with.
  app.get("/api/sales-hub/signature", requireUser, (req, res) => res.json({ html: SIGNATURE_HTML }));

  // GET /api/sales-hub/inbox/:id/attachments/:aid — one attachment's bytes.
  // Customer-supplied files are served defensively: only real raster image types keep
  // their type (SVG can carry script, so it does not); everything else goes out as a
  // download, never rendered, and nosniff stops a browser guessing otherwise.
  const SAFE_IMAGE_TYPES = new Set(["image/png", "image/jpeg", "image/jpg", "image/gif", "image/webp", "image/bmp"]);
  app.get("/api/sales-hub/inbox/:id/attachments/:aid", requireUser, async (req, res) => {
    if (!graphConfigured()) return res.status(503).json({ error: "Outlook is not connected" });
    try {
      const a = await getAttachment(req.params.id, req.params.aid);
      const type = String(a.contentType).toLowerCase().split(";")[0].trim();
      const safe = SAFE_IMAGE_TYPES.has(type);
      const filename = String(a.name).replace(/[^\w.\- ()]+/g, "_").slice(0, 120) || "attachment";
      res.set({
        "Content-Type": safe ? type : "application/octet-stream",
        "Content-Disposition": `${safe ? "inline" : "attachment"}; filename="${filename}"`,
        "X-Content-Type-Options": "nosniff",
        "Content-Security-Policy": "default-src 'none'; sandbox",
        "Cache-Control": "private, max-age=3600",
      });
      res.send(a.buf);
    } catch (err) {
      res.status(err.status === 404 ? 404 : 500).json({ error: err.message });
    }
  });

  // POST /api/sales-hub/lookup  { query, emailDate? }
  // `query` is either a bare order number or a whole email.
  app.post("/api/sales-hub/lookup", requireUser, async (req, res) => {
    try {
      // Every answer carries the template list, so an email with no order can still pick one
      // (the page then asks for the order it needs).
      const intentList = SALES_INTENTS.map((i) => ({ key: i.key, label: i.label }));
      
      const raw = String((req.body && req.body.query) || "").trim();
      if (!raw) return res.status(400).json({ error: "Nothing to look up" });

      // A bare number typed into the box — which may be a Brightpearl id OR a
      // zero-padded web reference, so do not strip anything off it.
      const looksLikeBareNumber = /^\d{4,12}$/.test(raw);
      const orderNumber = looksLikeBareNumber ? raw : extractOrderNumber(raw);
      if (!orderNumber) {
        return res.json({
          found: false, intents: intentList,
          reason:
            "No order number in that email. Search Brightpearl by the customer's " +
            "address instead, then paste the number in on its own.",
        });
      }

      const resolved = await resolveOrderId(orderNumber);
      if (resolved && resolved.notSale) {
        const ns = resolved.notSale, po = ns.type === "PO" || ns.type === "2";
        return res.json({
          found: false, intents: intentList,
          reason: po
            ? `${orderNumber} is purchase order ${ns.id}, not a sales order` + (ns.sales > 1 ? ` (it covers ${ns.sales} sales orders)` : "") + ". Link the customer's order by hand."
            : `${orderNumber} is a ${ns.type === "SC" || ns.type === "3" ? "credit note" : "non-sales order"} in Brightpearl, not a sales order. Link the right order by hand.`,
        });
      }
      if (!resolved) {
        return res.json({
          found: false, intents: intentList,
          reason: `Nothing in Brightpearl matches ${orderNumber} — not as an order number, nor as a web order reference.`,
        });
      }

      const order = await gatherOrder(resolved.id);
      if (!order) return res.json({ found: false, intents: intentList, reason: `Order ${resolved.id} could not be loaded.` });

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
        matchedBy: resolved.via,          // "id" or "reference"
        matchedOn: orderNumber,
        alsoMatched: resolved.alsoMatched || 0,
        otherMatch: resolved.otherMatch || null,
        intents: SALES_INTENTS.map((i) => ({ key: i.key, label: i.label })),
      });
    } catch (err) {
      console.error("[sales-hub] lookup failed:", err.message);
      res.status(500).json({ error: err.message });
    }
  });

  // Returns references. One per order, per person, per day: drafting the same return
  // twice gives the same number, so switching the reply type back and forth does not
  // burn through the day's sequence. The primary key makes a duplicate impossible.
  let refsReady = null;
  const ensureRefs = () => (refsReady ||= getPool().query(`CREATE TABLE IF NOT EXISTS sales_hub_return_refs (
      ref        text PRIMARY KEY,
      initials   text NOT NULL,
      day        date NOT NULL,
      seq        int  NOT NULL,
      order_id   bigint NOT NULL,
      created_by text NOT NULL,
      created_at timestamptz NOT NULL DEFAULT now()
    )`).catch((e) => { refsReady = null; throw e; }));

  async function returnsRefFor(orderId, user) {
    if (!(useDatabase && getPool())) return null;
    const initials = initialsOf(user && user.name);
    if (!initials) return null;
    await ensureRefs();
    const day = new Date().toLocaleDateString("en-CA", { timeZone: "Europe/London" });
    const had = await getPool().query(
      `SELECT ref FROM sales_hub_return_refs WHERE order_id = $1 AND created_by = $2 AND day = $3`, [orderId, user.key, day]);
    if (had.rowCount) return had.rows[0].ref;
    for (let attempt = 0; attempt < 5; attempt++) {
      const n = await getPool().query(
        `SELECT COALESCE(MAX(seq), 0) + 1 AS next FROM sales_hub_return_refs WHERE initials = $1 AND day = $2`, [initials, day]);
      const seq = n.rows[0].next;
      const ref = returnRef(initials, new Date(), seq);
      const ins = await getPool().query(
        `INSERT INTO sales_hub_return_refs (ref, initials, day, seq, order_id, created_by)
         VALUES ($1, $2, $3, $4, $5, $6) ON CONFLICT (ref) DO NOTHING RETURNING ref`,
        [ref, initials, day, seq, orderId, user.key]);
      if (ins.rowCount) return ref;          // someone else took that number a moment ago: try the next
    }
    throw new Error("Could not allocate a returns reference");
  }

  // POST /api/sales-hub/draft  { orderId, intent, emailDate }
  app.post("/api/sales-hub/draft", requireUser, async (req, res) => {
    try {
      const orderId = num(req.body && req.body.orderId);
      const intent = String((req.body && req.body.intent) || "eta");
      // A plain reply does not need an order behind it — most of the inbox is not
      // about one, and it should still be answerable from here.
      if (!orderId && intent === "plain") {
        const draft = buildSalesReply({
          intent, order: { contactName: String((req.body && req.body.contactName) || "") },
          salesperson: {}, signedBy: (req.hubUser && req.hubUser.name) || "",
        });
        return res.json({ draft, assessment: { level: "ok", reasons: [], laterNotes: [] }, po: null, promised: null, returnsRef: null });
      }
      if (!orderId) return res.status(400).json({ error: "orderId required" });
      const order = await gatherOrder(orderId);
      if (!order) return res.status(404).json({ error: "Order not found" });

      const emailDate = (req.body && req.body.emailDate) || null;

      // Use the PO that finishes LAST: quoting the earliest would promise a
      // delivery before the rest of the order can possibly arrive.
      const po = order.pos
        .slice()
        .sort((a, b) => new Date(b.expectedDate || 0) - new Date(a.expectedDate || 0))[0] || null;

      // What this customer was last told ("advised mid next week"), from the notes.
      // The draft keeps to it unless the PO has since moved later.
      const promised = promisedWindow(order.timeline);

      const returnsRef = intent === "returns" ? await returnsRefFor(order.id, req.hubUser) : null;

      const draft = buildSalesReply({
        intent, order, po, promised, returnsRef,
        blockedLines: order.blockedLines,
        salesperson: order.salesperson,
        // The SESSION says who this is; the body is only a fallback for a
        // draft requested before sign-in existed.
        signedBy: (req.hubUser && req.hubUser.name) || (req.body && req.body.sentBy) || "",
      });

      const assessment = assessDuplication({
        notes: order.timeline,
        emailDate,
        proposedDates: draft.proposedDates,
        blockedLines: order.blockedLines,
        automatedEmails: order.automatedEmails,
        slippedFrom: draft.eta && draft.eta.slippedFrom,
      });

      res.json({ draft, assessment, po, promised, returnsRef, usedNoEmailDate: !emailDate });
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
  app.post("/api/sales-hub/send", requireUser, async (req, res) => {
    const b = req.body || {};
    try {
      // reply / replyAll / forward need the email they answer; new needs nothing else.
      // A saved draft carries its own mode and the email it answers; one written in Outlook
      // (no row here) is sent as it stands.
      let draftRec = null;
      if (b.draftId && useDatabase && getPool()) {
        await ensureDrafts();
        draftRec = (await getPool().query(`SELECT * FROM sales_hub_drafts WHERE draft_id = $1`, [String(b.draftId)])).rows[0] || null;
      }
      const mode = draftRec ? draftRec.mode : b.draftId ? "draft"
        : ["reply", "replyAll", "forward", "new"].includes(b.mode) ? b.mode : (b.messageId ? "reply" : "new");
      if (draftRec && draftRec.source_id) b.messageId = draftRec.source_id;
      const orderId = num(b.orderId);
      const toList = splitAddresses(b.to), ccList = splitAddresses(b.cc), bccList = splitAddresses(b.bcc);
      const to = toList.join("; ");
      const subject = String(b.subject || "").trim();
      const html = String(b.html || "");
      if (!toList.length || !subject || !html) return res.status(400).json({ error: "To, subject and a message are required" });
      if (mode !== "new" && mode !== "draft" && !b.messageId) return res.status(400).json({ error: "Which email is this a " + mode + " to?" });
      if (mode === "new" && !orderId && b.requireOrder) return res.status(400).json({ error: "No order linked" });
      // Files from the compose box, base64. Outlook's own limits: 20 MB a file.
      const attachments = Array.isArray(b.attachments) ? b.attachments.slice(0, 10) : [];
      let total = 0;
      for (const a of attachments) {
        const n = Buffer.byteLength(String(a.base64 || ""), "base64");
        total += n;
        if (!a.name || n === 0) return res.status(400).json({ error: "An attachment is empty or has no name" });
        if (n > 20 * 1024 * 1024) return res.status(413).json({ error: `${a.name} is over 20 MB` });
      }
      if (total > 25 * 1024 * 1024) return res.status(413).json({ error: "Attachments come to more than 25 MB" });
      const bad = [...toList, ...ccList, ...bccList].find((a) => !/^[^@\s]+@[^@\s]+\.[^@\s]+$/.test(a));
      if (bad) return res.status(400).json({ error: `"${bad}" is not an email address` });
      if (String(b.acknowledgedLevel) === "blocked" && !b.overrideBlocked) {
        return res.status(409).json({
          error: "This draft was blocked. Re-check it and tick the override if you are sure.",
        });
      }

      if (b.messageId && (mode === "reply" || mode === "replyAll")) {
        const held = (await othersLocks(req.hubUser.key).catch(() => ({})))[String(b.messageId)];
        if (held) return res.status(423).json({ error: `${held.name} is working on this email — not sent.` });
      }

      const order = orderId ? await gatherOrder(orderId).catch(() => null) : null;
      const salesperson = (order && order.salesperson) || {};

      const who = (req.hubUser && req.hubUser.name) || "";
      const fromName = who || String(b.sentBy || "").trim() || salesperson.name || "Tuffshop Sales";
      const fromAddress = process.env.SALES_SENDER_EMAIL || process.env.SENDER_EMAIL || "sales@tuffshop.co.uk";

      // The customer replies to the person who owns the order, not a shared
      // address nobody watches.
      // From a person's OWN mailbox, replies come back to them anyway - no Reply-To.
      const ownBox = currentMailbox().toLowerCase() !== salesMailbox().toLowerCase();
      const replyTo = ownBox ? undefined : (salesperson.email || fromAddress);

      // Through Outlook when connected: sent as the real sales mailbox (passes DMARC, which
      // smtp2go does not), kept in Sent Items, and — when the email came from the inbox —
      // threaded as a reply to it. smtp2go stays as the fallback so the hub still works if
      // Graph is not configured.
      let via;
      if (graphConfigured()) {
        const r = await composeAndSend({ mode, sourceId: b.messageId ? String(b.messageId) : undefined,
          draftId: b.draftId ? String(b.draftId) : undefined, quoted: draftRec ? draftRec.quoted : "",
          to: toList, cc: ccList, bcc: bccList, subject, html, replyTo, attachments });
        via = r.via;
        // Outlook marks an email read once it has been replied to; do the same so the
        // shared inbox shows it as dealt with. Never on merely OPENING it — a colleague
        // may be relying on it staying unread.
        if (b.draftId && useDatabase && getPool()) getPool().query(`DELETE FROM sales_hub_drafts WHERE draft_id = $1`, [String(b.draftId)]).catch(() => {});
        if (b.messageId && (mode === "reply" || mode === "replyAll")) markRead(String(b.messageId)).catch((e) => console.error("[sales-hub] markRead:", e.message));
      } else {
        const transporter = nodemailer.createTransport({
          host: process.env.SMTP_SERVER || "mail-eu.smtp2go.com",
          port: parseInt(process.env.SMTP_PORT || "2525", 10),
          secure: false,
          auth: { user: process.env.SMTP_USERNAME || "tuffshop.co.uk", pass: process.env.SMTP_PASS },
        });
        await transporter.sendMail({ from: `"${fromName}" <${fromAddress}>`, replyTo, to: toList, cc: ccList, bcc: bccList, subject, html,
          attachments: attachments.map((a) => ({ filename: a.name, content: Buffer.from(String(a.base64), "base64"), contentType: a.contentType })) });
        via = "smtp2go";
      }

      const note = buildSalesNote({
        intent: b.intent || "eta",
        to, subject,
        sentBy: who || b.sentBy || salesperson.name || "",
        proposedDates: Array.isArray(b.proposedDates) ? b.proposedDates : [],
        duplicationLevel: b.acknowledgedLevel,
        body: emailToNoteText(html),
        attachments: attachments.map((a) => a.name),
      });
      // postBpOrderNote reports failure by RETURNING false, not throwing — reading
      // only the catch told people "written to the order notes" when it was not.
      let noted = false;
      if (orderId) {
        try { noted = (await postBpOrderNote(orderId, note)) !== false; }
        catch (e) { console.error("[sales-hub] note failed:", e.message); }
      }

      // The email has gone. A failed note must not read as a failed send, or
      // somebody will send it a second time.
      res.json({ sent: true, via, noted, noOrder: !orderId, note });
    } catch (err) {
      console.error("[sales-hub] send failed:", err.message);
      res.status(500).json({ sent: false, error: err.message });
    }
  });

  // GET /api/sales-hub/notes/:orderId?since=ISO — the timeline panel on its own.
  app.get("/api/sales-hub/notes/:orderId", requireUser, async (req, res) => {
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
