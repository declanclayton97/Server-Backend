// travelexPo.js — Travelex/ASDA bulk-PO tracker (Matt, 29 Oct 2026).
//
// Travelex pays for ASDA uniform orders up front with a bulk PO, invoiced to them on one
// sales order (the "invoice SO", e.g. 473285 = £10,000). Every ASDA order then carries that
// PO in TRAVELEX-PO (PCF_TRAVLXPO); once processed its value is zeroed and the original
// total kept in TRAVELEX ORDER VALUE INC VAT (PCF_TRVORDVL). This adds those up against the
// invoice SO's total, so Matt can see what's left without exporting.
//
// - GET  /api/travelex-po            current status (cached; ?refresh=1 re-reads BP)
// - POST /api/travelex-po/settings   { invoiceOrderId } switch to a new bulk PO
// - Warning email when the remaining balance drops into the final £2,000 / £1,000 / £500 /
//   used up (each once per PO), and a monthly report on the 1st. Both to Matt.
//
// The PO number itself is read off the invoice SO's own TRAVELEX-PO field, so a new bulk
// PO only needs the new invoice SO number. Orders are found by the Travelex department
// (22) rather than the customer, because ASDA orders come in under several contacts.

// Switched on 30 Oct (Dec): hourly BP read + emails to Matt.
const ENABLED = true;
const DEPARTMENT_ID = 22;
const DEFAULT_INVOICE_ORDER = 473285;
const REPORT_TO = "matt@tuffshop.co.uk";
const THRESHOLDS = [2000, 1000, 500, 0];
const RATE_WINDOW_DAYS = 56;
const DAY = 24 * 60 * 60 * 1000;

const money = (n) => "£" + Number(n || 0).toLocaleString("en-GB", { minimumFractionDigits: 2, maximumFractionDigits: 2 });
const ukDate = (d) => new Date(d).toLocaleDateString("en-GB", { day: "numeric", month: "short", year: "numeric", timeZone: "Europe/London" });
const esc = (s) => String(s ?? "").replace(/[&<>"]/g, (c) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;" })[c]);
const round2 = (n) => Math.round(n * 100) / 100;

// Pure: does an order's TRAVELEX-PO field name this PO? An order split across two POs holds
// both, slash-separated ("710000000777 - Q1-2026 / 710000000946 - Q2-2026", 471790), with
// TRAVELEX ORDER VALUE holding the part charged to the newer one.
export function carriesPo(field, poRef) {
  const norm = (s) => String(s ?? "").replace(/\s+/g, " ").trim();
  return String(field ?? "").split("/").map(norm).includes(norm(poRef));
}

// One order's spend against the PO: the saved original value once processed, else (not
// yet processed, value not zeroed) its own total.
export function orderSpend(o) {
  const cf = o.customFields || {};
  const saved = Number(String(cf.PCF_TRVORDVL ?? "").replace(/[£,\s]/g, ""));
  if (Number.isFinite(saved) && saved > 0) return { value: saved, pending: false };
  return { value: Number(o.totalValue?.total || 0), pending: true };
}

// Pure: the figures shown on the overlay and in the emails.
export function summarise({ poRef, invoiceOrderId, amount, orders, now = Date.now() }) {
  const rows = orders.map((o) => ({
    orderId: o.id, reference: o.reference || "", status: o.orderStatus?.name || "",
    createdOn: o.createdOn, ...orderSpend(o),
  })).sort((a, b) => String(a.createdOn).localeCompare(String(b.createdOn)));
  const processed = round2(rows.filter((r) => !r.pending).reduce((s, r) => s + r.value, 0));
  const pending = round2(rows.filter((r) => r.pending).reduce((s, r) => s + r.value, 0));
  const used = round2(processed + pending);
  const remaining = round2(amount - used);

  // Spending rate over the last 8 weeks (or since the first order, if that's sooner).
  const first = rows.length ? new Date(rows[0].createdOn).getTime() : now;
  const windowStart = Math.max(now - RATE_WINDOW_DAYS * DAY, first);
  const windowDays = Math.max((now - windowStart) / DAY, 7);
  const recent = rows.filter((r) => new Date(r.createdOn).getTime() >= windowStart).reduce((s, r) => s + r.value, 0);
  const perDay = recent / windowDays;
  const runOut = remaining <= 0 ? now : perDay > 0 ? now + (remaining / perDay) * DAY : null;

  return {
    poRef, invoiceOrderId, amount: round2(amount), processed, pending, used, remaining,
    pctUsed: amount > 0 ? Math.round((used / amount) * 1000) / 10 : 0,
    perWeek: round2(perDay * 7), estRunOut: runOut ? new Date(runOut).toISOString() : null,
    orderCount: rows.length, pendingCount: rows.filter((r) => r.pending).length,
    firstOrder: rows[0]?.createdOn || null, orders: rows,
  };
}

// Pure: thresholds the balance is now at or under that haven't been emailed yet. Only the
// lowest one is worth an email; the rest are just marked done.
export function newThresholds(remaining, alreadySent) {
  const sent = new Set(alreadySent.map(Number));
  return THRESHOLDS.filter((t) => remaining <= t && !sent.has(t));
}

function summaryTable(s) {
  const row = (k, v, bold) => `<tr><td style="padding:4px 14px 4px 0;color:#555">${k}</td><td style="padding:4px 0;${bold ? "font-weight:bold" : ""}">${v}</td></tr>`;
  return `<table style="border-collapse:collapse;font-family:Arial,sans-serif;font-size:14px">
    ${row("Bulk PO", esc(s.poRef))}
    ${row("Invoiced on", `SO ${s.invoiceOrderId}`)}
    ${row("PO value", money(s.amount))}
    ${row("Spent (processed)", money(s.processed))}
    ${row("Not yet processed", `${money(s.pending)} (${s.pendingCount} order${s.pendingCount === 1 ? "" : "s"})`)}
    ${row("Remaining", money(s.remaining), true)}
    ${row("Used", `${s.pctUsed}%`)}
    ${row("Average spend", `${money(s.perWeek)} a week (last 8 weeks)`)}
    ${row("Estimated used up", s.estRunOut ? ukDate(s.estRunOut) : "not enough orders to tell")}
  </table>`;
}

export function alertEmail(s, threshold) {
  const subject = threshold === 0
    ? `Travelex bulk PO ${s.poRef} is used up (${money(s.remaining)} left)`
    : `Travelex bulk PO ${s.poRef}: into the final ${money(threshold).replace(".00", "")} - ${money(s.remaining)} left`;
  const html = `<div style="font-family:Arial,sans-serif;font-size:14px">
    <p>The Travelex bulk PO is down to <b>${money(s.remaining)}</b>${threshold === 0 ? " - it's used up, so they'll need a new bulk PO." : "."}</p>
    ${summaryTable(s)}
    <p style="color:#777;font-size:12px">When the new bulk PO is invoiced, click "Change PO" on the Travelex panel in Brightpearl and enter the new invoice SO number.</p></div>`;
  return { subject, html };
}

export function monthlyEmail(s, monthLabel) {
  const lines = s.orders.map((o) => `<tr>
    <td style="padding:3px 10px 3px 0">${esc(ukDate(o.createdOn))}</td>
    <td style="padding:3px 10px 3px 0">${o.orderId}</td>
    <td style="padding:3px 10px 3px 0">${esc(o.reference)}</td>
    <td style="padding:3px 10px 3px 0;text-align:right">${money(o.value)}</td>
    <td style="padding:3px 0;color:#777">${o.pending ? "not yet processed" : ""}</td></tr>`).join("");
  return {
    subject: `Travelex bulk PO report - ${monthLabel}: ${money(s.remaining)} left of ${money(s.amount)}`,
    html: `<div style="font-family:Arial,sans-serif;font-size:14px">
      <p>Travelex bulk PO spend to the start of ${esc(monthLabel)}.</p>
      ${summaryTable(s)}
      <h3 style="margin-top:22px">Orders on this PO (${s.orderCount})</h3>
      <table style="border-collapse:collapse;font-size:13px">${lines}</table></div>`,
  };
}

export function registerTravelexPo(app, { bpLive, getPool }) {
  let cache = null;
  let inflight = null;

  const ensure = () => getPool().query(`
    CREATE TABLE IF NOT EXISTS travelex_po_settings (id int PRIMARY KEY, invoice_order_id bigint NOT NULL, set_at timestamptz NOT NULL DEFAULT now());
    CREATE TABLE IF NOT EXISTS travelex_po_alerts (po_ref text NOT NULL, threshold int NOT NULL, sent_at timestamptz NOT NULL DEFAULT now(), PRIMARY KEY (po_ref, threshold));
    CREATE TABLE IF NOT EXISTS travelex_po_monthly (month text PRIMARY KEY, sent_at timestamptz NOT NULL DEFAULT now());`);

  async function invoiceOrderId() {
    if (!getPool()) return DEFAULT_INVOICE_ORDER;
    await ensure();
    const r = await getPool().query(`SELECT invoice_order_id FROM travelex_po_settings WHERE id = 1`);
    return r.rows[0] ? Number(r.rows[0].invoice_order_id) : DEFAULT_INVOICE_ORDER;
  }

  async function readInvoice(id) {
    const inv = (await bpLive("GET", `/order-service/order/${id}?includeOptional=customFields`))?.[0];
    if (!inv) throw new Error(`SO ${id} not found`);
    const poRef = String(inv.customFields?.PCF_TRAVLXPO || "").trim();
    if (!poRef) throw new Error(`SO ${id} has no TRAVELEX-PO filled in`);
    return { inv, poRef, amount: Number(inv.totalValue?.total || 0) };
  }

  async function compute() {
    const id = await invoiceOrderId();
    const { inv, poRef, amount } = await readInvoice(id);
    // ASDA orders can be placed a little before the invoice SO is raised (471791 was 26 Jun
    // for an SO raised 3 Jul), so look back 90 days from it.
    const from = new Date(new Date(inv.createdOn).getTime() - 90 * DAY).toISOString().slice(0, 10);
    const ids = [];
    for (let first = 1; ; first += 500) {
      const s = await bpLive("GET", `/order-service/order-search?departmentId=${DEPARTMENT_ID}&orderTypeId=1&createdOn=${from}/&pageSize=500&firstResult=${first}`);
      ids.push(...(s?.results || []).map((r) => r[0]));
      if (!s?.metaData?.morePagesAvailable) break;
    }
    const orders = [];
    for (let i = 0; i < ids.length; i += 100) {
      const batch = (await bpLive("GET", `/order-service/order/${ids.slice(i, i + 100).join(",")}?includeOptional=customFields`)) || [];
      for (const o of batch) {
        if (o.id === id || /cancel/i.test(o.orderStatus?.name || "")) continue;
        if (carriesPo(o.customFields?.PCF_TRAVLXPO, poRef)) orders.push(o);
      }
    }
    return summarise({ poRef, invoiceOrderId: id, amount, orders });
  }

  async function refresh() {
    if (inflight) return inflight;
    inflight = (async () => {
      try { cache = { ...(await compute()), updatedAt: new Date().toISOString(), error: null }; }
      catch (e) { console.error("[travelex-po]", e.message); cache = { ...(cache || {}), error: e.message }; }
      finally { inflight = null; }
      return cache;
    })();
    return inflight;
  }

  async function send(subject, html) {
    const { default: nodemailer } = await import("nodemailer");
    const t = nodemailer.createTransport({ host: process.env.SMTP_SERVER || "mail-eu.smtp2go.com", port: parseInt(process.env.SMTP_PORT || "2525", 10), secure: false,
      auth: { user: process.env.SMTP_USERNAME || "tuffshop.co.uk", pass: process.env.SMTP_PASS } });
    await t.sendMail({ from: '"Tuff Workwear" <noreply@tuffshop.co.uk>', to: REPORT_TO, subject, html });
  }

  async function checkAlerts(s) {
    if (!getPool() || !s?.poRef || s.error) return;
    await ensure();
    const sent = (await getPool().query(`SELECT threshold FROM travelex_po_alerts WHERE po_ref = $1`, [s.poRef])).rows.map((r) => r.threshold);
    const due = newThresholds(s.remaining, sent);
    if (!due.length) return;
    // Claim them first so a restart mid-send can't email twice.
    for (const t of due) await getPool().query(`INSERT INTO travelex_po_alerts (po_ref, threshold) VALUES ($1,$2) ON CONFLICT DO NOTHING`, [s.poRef, t]);
    const { subject, html } = alertEmail(s, Math.min(...due));
    await send(subject, html);
    console.log(`[travelex-po] alert sent: ${subject}`);
  }

  async function checkMonthly(s) {
    if (!getPool() || !s?.poRef || s.error) return;
    const now = new Date();
    const day = Number(now.toLocaleDateString("en-GB", { day: "numeric", timeZone: "Europe/London" }));
    const hour = Number(now.toLocaleTimeString("en-GB", { hour: "2-digit", hour12: false, timeZone: "Europe/London" }));
    if (day !== 1 || hour < 8) return;
    const month = now.toLocaleDateString("en-CA", { timeZone: "Europe/London" }).slice(0, 7);
    await ensure();
    const claim = await getPool().query(`INSERT INTO travelex_po_monthly (month) VALUES ($1) ON CONFLICT DO NOTHING RETURNING month`, [month]);
    if (!claim.rowCount) return;
    const label = now.toLocaleDateString("en-GB", { month: "long", year: "numeric", timeZone: "Europe/London" });
    const { subject, html } = monthlyEmail(s, label);
    await send(subject, html);
    console.log(`[travelex-po] monthly report sent for ${month}`);
  }

  async function tick() {
    const s = await refresh();
    try { await checkAlerts(s); } catch (e) { console.error("[travelex-po] alert:", e.message); }
    try { await checkMonthly(s); } catch (e) { console.error("[travelex-po] monthly:", e.message); }
  }

  app.get("/api/travelex-po", async (req, res) => {
    try {
      const stale = !cache || !cache.updatedAt || Date.now() - new Date(cache.updatedAt).getTime() > 60 * 60 * 1000;
      if (req.query.refresh === "1" || stale) await refresh();
      res.json(cache || { error: "not loaded yet" });
    } catch (e) { res.status(500).json({ error: e.message }); }
  });

  app.post("/api/travelex-po/settings", async (req, res) => {
    const id = Number(String(req.body?.invoiceOrderId || "").replace(/\D/g, ""));
    if (!id) return res.status(400).json({ error: "Enter the sales order number the bulk PO was invoiced on" });
    if (!getPool()) return res.status(503).json({ error: "Database not configured" });
    try {
      const { poRef, amount } = await readInvoice(id);   // refuses an SO with no TRAVELEX-PO
      await ensure();
      await getPool().query(`INSERT INTO travelex_po_settings (id, invoice_order_id, set_at) VALUES (1,$1,now())
        ON CONFLICT (id) DO UPDATE SET invoice_order_id = EXCLUDED.invoice_order_id, set_at = now()`, [id]);
      console.log(`[travelex-po] switched to SO ${id} (${poRef}, ${amount})`);
      res.json(await refresh());
    } catch (e) { res.status(400).json({ error: e.message }); }
  });

  if (!ENABLED) return;
  setTimeout(() => tick().catch(() => {}), 90 * 1000);
  setInterval(() => tick().catch(() => {}), 60 * 60 * 1000);
}
