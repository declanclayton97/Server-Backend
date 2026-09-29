// returns.js — the self-service returns rules. Pure: no I/O, so every decision the
// customer page shows can be tested without Brightpearl.
//
// The policy (Dec, 2026-09-29):
//   - the customer pays the return postage;
//   - logo'd / personalised items cannot be returned;
//   - nothing more than 30 days after it was sent;
//   - every channel except eBay and Amazon, which run their own returns.
//
// PERSONALISATION IS DECIDED PER ORDER, NOT PER LINE. A decoration row does not sit
// next to the garment it belongs to — SO 492358 had nine print/embroidery rows
// grouped after all eleven garments — so there is no reliable way to say which
// garment carries the logo. Rather than guess (and either refuse a plain pair of
// boots or accept an embroidered jacket), an order with ANY decoration row is not
// returnable online and the page sends the customer to the sales team, who can
// still issue a reference from the Sales Hub for the plain items.

import { classifyOrderRow } from "./salesHub.js";

export const RETURN_WINDOW_DAYS = Number(process.env.RETURN_WINDOW_DAYS || 30);
export const EXCLUDED_CHANNEL_RE = /ebay|amazon/i;

export const RETURN_REASONS = [
  "Too small",
  "Too big",
  "Changed my mind",
  "Not as described",
  "Faulty or damaged",
  "Wrong item sent",
  "Other",
];
export const RETURN_OUTCOMES = ["refund", "exchange"];

// "LS26 8LG" and "ls268lg" are the same postcode.
export const normPostcode = (p) => String(p || "").toUpperCase().replace(/[^A-Z0-9]/g, "");

// The UK calendar day of an instant, as YYYY-MM-DD.
const ukDate = (d) => new Date(d).toLocaleDateString("en-CA", { timeZone: "Europe/London" });
const addDays = (ymd, n) => {
  const d = new Date(ymd + "T12:00:00Z");
  d.setUTCDate(d.getUTCDate() + n);
  return d.toISOString().slice(0, 10);
};

// When the goods went. Web and trade orders are invoiced as they are despatched, so
// the invoice's tax date is the despatch date; an order with no invoice has not gone.
export function despatchDate(order) {
  const inv = ((order && order.invoices) || []).map((i) => i && i.taxDate).filter(Boolean).sort();
  return inv.length ? ukDate(inv[0]) : null;
}

export function postcodeMatches(order, postcode) {
  const want = normPostcode(postcode);
  if (want.length < 5) return false;
  const p = (order && order.parties) || {};
  return [p.delivery, p.billing, p.customer]
    .some((x) => x && normPostcode(x.postalCode) === want);
}

/**
 * Can this order be returned online, and which lines, how many of each?
 *
 * order        the Brightpearl order
 * productMeta  { [productId]: { stockTracked, brandId } } — what classifyOrderRow needs
 * channelName  the order's channel name
 * requested    { [rowId]: qty already on an open return } so a line cannot be returned twice
 * today        for tests
 *
 * { ok, code, message, despatchedOn, lastDay, lines: [{ rowId, name, sku, qty, available }] }
 */
export function assessReturn(order, { productMeta = {}, channelName = "", requested = {}, today = new Date() } = {}) {
  const fail = (code, message, extra = {}) => ({ ok: false, code, message, lines: [], ...extra });
  if (!order || Number(order.orderTypeId || (order.orderTypeCode === "SO" ? 1 : 0)) !== 1) {
    return fail("not-found", "We couldn't find that order. Please check the order number and postcode.");
  }
  if (EXCLUDED_CHANNEL_RE.test(channelName)) {
    const where = /amazon/i.test(channelName) ? "Amazon" : "eBay";
    return fail("marketplace", `This order was placed through ${where}, so please return it through your ${where} account.`);
  }

  const rows = Object.entries(order.orderRows || {}).map(([rowId, r]) => ({
    rowId: String(rowId), r, kind: classifyOrderRow(r, productMeta[Number(r.productId)]),
  }));
  if (rows.some((x) => x.kind === "service")) {
    return fail("personalised",
      "This order includes personalised items (printed or embroidered), which can't be returned. " +
      "If some of your items were not personalised, or something is wrong with your order, please contact our sales team and we'll help.");
  }

  const despatchedOn = despatchDate(order);
  if (!despatchedOn) {
    return fail("not-sent", "This order hasn't been sent yet. If you'd like to change or cancel it, please contact our sales team.");
  }
  const lastDay = addDays(despatchedOn, RETURN_WINDOW_DAYS);
  if (ukDate(today) > lastDay) {
    return fail("too-late",
      `This order was sent on ${prettyDate(despatchedOn)}, more than ${RETURN_WINDOW_DAYS} days ago, so it is outside our returns period.`,
      { despatchedOn, lastDay });
  }

  const lines = rows
    .filter((x) => x.kind === "goods")
    .map(({ rowId, r }) => {
      const qty = Math.round(Number((r.quantity && r.quantity.magnitude) || 0));
      const already = Math.max(0, Math.round(Number(requested[rowId] || 0)));
      return { rowId, name: String(r.productName || "").trim(), sku: r.productSku || "", qty, available: Math.max(0, qty - already) };
    })
    .filter((l) => l.qty > 0);
  if (!lines.length) return fail("nothing", "There is nothing on this order that can be returned online. Please contact our sales team.");
  if (!lines.some((l) => l.available > 0)) {
    return fail("already", "A return has already been requested for everything on this order. Check your email for your returns reference.",
      { despatchedOn, lastDay });
  }
  return { ok: true, code: "ok", message: "", despatchedOn, lastDay, lines };
}

// What the customer submitted, checked against what assessReturn allows. Never trust
// the page: quantities, reasons and outcomes are all re-validated here.
export function validateSelection(assessment, picks) {
  if (!assessment || !assessment.ok) return { ok: false, error: (assessment && assessment.message) || "Not returnable" };
  const byRow = new Map(assessment.lines.map((l) => [l.rowId, l]));
  const chosen = [];
  for (const p of Array.isArray(picks) ? picks : []) {
    const line = byRow.get(String(p && p.rowId));
    const qty = Math.round(Number(p && p.qty));
    if (!line || !(qty > 0)) continue;
    if (qty > line.available) return { ok: false, error: `You can return up to ${line.available} of "${line.name}".` };
    const reason = RETURN_REASONS.includes(p.reason) ? p.reason : null;
    if (!reason) return { ok: false, error: `Please choose a reason for "${line.name}".` };
    const outcome = RETURN_OUTCOMES.includes(p.outcome) ? p.outcome : null;
    if (!outcome) return { ok: false, error: `Please choose a refund or an exchange for "${line.name}".` };
    const exchangeFor = String(p.exchangeFor || "").trim().slice(0, 200);
    if (outcome === "exchange" && !exchangeFor) return { ok: false, error: `Please tell us what you'd like instead of "${line.name}".` };
    chosen.push({ rowId: line.rowId, name: line.name, sku: line.sku, qty, reason, outcome, exchangeFor: outcome === "exchange" ? exchangeFor : "" });
  }
  if (!chosen.length) return { ok: false, error: "Please choose at least one item to return." };
  return { ok: true, lines: chosen };
}

export function prettyDate(ymd) {
  const d = new Date(String(ymd).slice(0, 10) + "T12:00:00Z");
  return d.toLocaleDateString("en-GB", { day: "numeric", month: "long", year: "numeric", timeZone: "UTC" });
}

const esc = (s) => String(s == null ? "" : s)
  .replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;").replace(/"/g, "&quot;");

// The customer's confirmation email.
export function returnEmailHtml({ ref, orderRef, name, lines, lastDay, address }) {
  const rows = lines.map((l) => `
    <tr>
      <td style="padding:6px 8px;border:1px solid #ddd;">${esc(l.name)}</td>
      <td style="padding:6px 8px;border:1px solid #ddd;text-align:center;">${l.qty}</td>
      <td style="padding:6px 8px;border:1px solid #ddd;">${esc(l.reason)}</td>
      <td style="padding:6px 8px;border:1px solid #ddd;">${l.outcome === "exchange" ? "Exchange for: " + esc(l.exchangeFor) : "Refund"}</td>
    </tr>`).join("");
  const faulty = lines.some((l) => /faulty|wrong item/i.test(l.reason));
  return `<div style="font-family:Arial,sans-serif;font-size:14px;color:#333;max-width:640px;">
  <p>${name ? "Hi " + esc(name) + "," : "Hello,"}</p>
  <p>Thanks for letting us know. Your returns reference for order <b>${esc(orderRef)}</b> is:</p>
  <p style="font-size:22px;font-weight:bold;letter-spacing:1px;background:#F3D014;display:inline-block;padding:8px 16px;">${esc(ref)}</p>
  <p>Please write this reference on your invoice and send it back with the item(s) to:<br>${address.map(esc).join("<br>")}</p>
  <table style="border-collapse:collapse;font-size:13px;margin:12px 0;">
    <thead><tr style="background:#f2f2f2;">
      <th style="padding:6px 8px;border:1px solid #ddd;text-align:left;">Item</th>
      <th style="padding:6px 8px;border:1px solid #ddd;">Qty</th>
      <th style="padding:6px 8px;border:1px solid #ddd;text-align:left;">Reason</th>
      <th style="padding:6px 8px;border:1px solid #ddd;text-align:left;">You asked for</th>
    </tr></thead><tbody>${rows}</tbody>
  </table>
  <p>${process.env.RETURNS_CONDITION_TEXT ? esc(process.env.RETURNS_CONDITION_TEXT) + " " : ""}Return postage is paid by you, so we recommend a tracked service and keeping your proof of postage.${faulty ? " As you've told us an item is faulty or not what you ordered, our team will be in touch about the postage." : ""}</p>
  <p>Please send your return by <b>${prettyDate(lastDay)}</b>. Once it arrives and has been checked we'll process your ${lines.some((l) => l.outcome === "exchange") ? "exchange or refund" : "refund"} and email you to confirm.</p>
  <p>Kind regards,<br>Tuffshop</p>
</div>`;
}

// The note left on the original Brightpearl order, so anyone who opens it sees the return.
export function returnNoteText({ ref, email, lines, comments }) {
  return [
    `RETURN REQUESTED ONLINE — ${ref}`,
    `Customer email: ${email}`,
    ...lines.map((l) => `- ${l.qty} x ${l.name}${l.sku ? " (" + l.sku + ")" : ""} — ${l.reason} — ${l.outcome === "exchange" ? "EXCHANGE for: " + l.exchangeFor : "REFUND"}`),
    comments ? `Customer comments: ${comments}` : "",
    "Awaiting the goods. Customer pays return postage.",
  ].filter(Boolean).join("\n");
}
