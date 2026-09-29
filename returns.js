// returns.js — the self-service returns rules. Pure: no I/O, so every decision the
// customer page shows can be tested without Brightpearl.
//
// The policy is the one published at tuffshop.co.uk/returns (read 2026-09-29), which
// Dec confirmed: the customer pays return postage; personalised items cannot be
// returned; 30 days from receiving the order; exchanges go out with free standard
// delivery; every channel except eBay and Amazon, which run their own returns.
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
// The policy counts 30 days from RECEIVING the order. Brightpearl knows when it was
// sent, not when it arrived, so allow for the delivery on top.
export const DELIVERY_GRACE_DAYS = Number(process.env.RETURN_DELIVERY_GRACE_DAYS || 2);
export const EXCLUDED_CHANNEL_RE = /ebay|amazon/i;

// Exchanges are nearly always a size (the policy page says so), so those come first.
export const EXCHANGE_CHOICES = ["One size up", "One size down", "Two sizes up", "Two sizes down", "Something else"];
export const REFUND_REASONS = [
  "Too small",
  "Too big",
  "Doesn't fit right",
  "Changed my mind",
  "Not what I expected",
  "Faulty or damaged",
  "Sent the wrong item",
  "Other",
];
export const NEEDS_CONTACT_RE = /faulty|wrong item/i;

// Where the website's policy page tells customers to send returns.
export const WEB_RETURNS_ADDRESS = (process.env.WEB_RETURNS_ADDRESS ||
  "Customer Returns|Tuffshop.co.uk|144-146 Aberford Road|Woodlesford|Leeds|LS26 8LG").split("|");

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

// The address on the order, shown only masked ("ja•••@gmail.com") so the page never
// hands a full email to whoever typed in an order number and postcode.
export function orderEmail(order) {
  const p = (order && order.parties) || {};
  const e = [p.customer, p.billing, p.delivery].map((x) => x && String(x.email || "").trim()).find((x) => /^[^@\s]+@[^@\s]+\.[^@\s]+$/.test(x || ""));
  return e || null;
}
export function maskEmail(email) {
  const m = String(email || "").match(/^([^@]+)@(.+)$/);
  if (!m) return null;
  return m[1].slice(0, Math.min(2, m[1].length - 1) || 1) + "•••@" + m[2];
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
    return fail("not-found", "We can't find an order with that number and postcode. Have another look at your order confirmation email, or give us a call and we'll help.");
  }
  if (EXCLUDED_CHANNEL_RE.test(channelName)) {
    const where = /amazon/i.test(channelName) ? "Amazon" : "eBay";
    return fail("marketplace", `This order came through ${where}, so the return needs to go through your ${where} account. That way your refund comes back the same way you paid.`);
  }

  const rows = Object.entries(order.orderRows || {}).map(([rowId, r]) => ({
    rowId: String(rowId), r, kind: classifyOrderRow(r, productMeta[Number(r.productId)]),
  }));
  if (rows.some((x) => x.kind === "service")) {
    return fail("personalised",
      "This order has printed or embroidered items on it, and we can't take personalised items back unless they're faulty. " +
      "If there's something on the order that wasn't personalised, or something's wrong with it, give us a call or drop us an email and we'll sort it out.");
  }

  const despatchedOn = despatchDate(order);
  if (!despatchedOn) {
    return fail("not-sent", "This order hasn't left us yet. If you want to change or cancel it, give us a call or drop us an email.");
  }
  const lastDay = addDays(despatchedOn, RETURN_WINDOW_DAYS + DELIVERY_GRACE_DAYS);
  if (ukDate(today) > lastDay) {
    return fail("too-late",
      `We sent this order on ${prettyDate(despatchedOn)}, which is outside our ${RETURN_WINDOW_DAYS} day returns window. If something's wrong with it, give us a call and we'll see what we can do.`,
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
  if (!lines.length) return fail("nothing", "There's nothing on this order we can take back online. Give us a call or drop us an email and we'll help.");
  if (!lines.some((l) => l.available > 0)) {
    return fail("already", "You've already started a return for everything on this order. Your returns reference is in the email we sent you.",
      { despatchedOn, lastDay });
  }
  return { ok: true, code: "ok", message: "", despatchedOn, lastDay, lines };
}

// What the customer submitted, checked against what assessReturn allows. Never trust
// the page: quantities, reasons and choices are all re-validated here.
//
// A line is { rowId, qty, outcome: "exchange"|"refund", exchangeChoice, exchangeFor, reason }.
export function validateSelection(assessment, picks) {
  if (!assessment || !assessment.ok) return { ok: false, error: (assessment && assessment.message) || "Not returnable" };
  const byRow = new Map(assessment.lines.map((l) => [l.rowId, l]));
  const chosen = [];
  for (const p of Array.isArray(picks) ? picks : []) {
    const line = byRow.get(String(p && p.rowId));
    const qty = Math.round(Number(p && p.qty));
    if (!line || !(qty > 0)) continue;
    if (qty > line.available) return { ok: false, error: `You can send back up to ${line.available} of "${line.name}".` };
    if (p.outcome === "exchange") {
      const choice = EXCHANGE_CHOICES.includes(p.exchangeChoice) ? p.exchangeChoice : null;
      if (!choice) return { ok: false, error: `What would you like instead of "${line.name}"?` };
      const exchangeFor = String(p.exchangeFor || "").trim().slice(0, 200);
      if (choice === "Something else" && !exchangeFor) return { ok: false, error: `Tell us what you'd like instead of "${line.name}".` };
      chosen.push({ rowId: line.rowId, name: line.name, sku: line.sku, qty, outcome: "exchange",
        exchangeChoice: choice, exchangeFor: choice === "Something else" ? exchangeFor : "", reason: "Exchange: " + choice.toLowerCase() });
    } else if (p.outcome === "refund") {
      const reason = REFUND_REASONS.includes(p.reason) ? p.reason : null;
      if (!reason) return { ok: false, error: `Let us know why "${line.name}" is coming back.` };
      chosen.push({ rowId: line.rowId, name: line.name, sku: line.sku, qty, outcome: "refund", exchangeChoice: "", exchangeFor: "", reason });
    } else {
      return { ok: false, error: `Would you like to exchange "${line.name}" or get a refund?` };
    }
  }
  if (!chosen.length) return { ok: false, error: "Pick at least one item to send back." };
  return { ok: true, lines: chosen };
}

// "One size up", or the customer's own words for "Something else".
export const wantedText = (l) => (l.outcome === "exchange" ? (l.exchangeChoice === "Something else" ? l.exchangeFor : l.exchangeChoice) : "");

export function prettyDate(ymd) {
  const d = new Date(String(ymd).slice(0, 10) + "T12:00:00Z");
  return d.toLocaleDateString("en-GB", { weekday: "long", day: "numeric", month: "long", timeZone: "UTC" });
}

const esc = (s) => String(s == null ? "" : s)
  .replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;").replace(/"/g, "&quot;");

// The customer's confirmation email. Written the way the sales team writes.
export function returnEmailHtml({ ref, orderRef, name, lines, lastDay, address }) {
  const td = "padding:8px 10px;border-bottom:1px solid #e5e5e5;";
  const rows = lines.map((l) => `
    <tr>
      <td style="${td}">${esc(l.name)}</td>
      <td style="${td}text-align:center;">${l.qty}</td>
      <td style="${td}">${l.outcome === "exchange" ? "Exchange &ndash; " + esc(wantedText(l)) : "Refund (" + esc(l.reason.toLowerCase()) + ")"}</td>
    </tr>`).join("");
  const exchanging = lines.some((l) => l.outcome === "exchange");
  const needsContact = lines.some((l) => NEEDS_CONTACT_RE.test(l.reason));
  return `<div style="font-family:'Open Sans',Arial,sans-serif;font-size:14px;line-height:1.55;color:#222;max-width:620px;">
  <div style="background:#000;padding:14px 18px;"><img src="https://tuffshop.co.uk/media/athlete2/default/tuffshop_logo.png" alt="Tuffshop" height="56" style="display:block;height:56px;"></div>
  <div style="padding:18px 4px;">
  <p>${name ? "Hi " + esc(name) + "," : "Hi,"}</p>
  <p>Thanks for letting us know about your order <b>${esc(orderRef)}</b>. Your returns reference is:</p>
  <p style="font-size:24px;font-weight:800;letter-spacing:1px;background:#F3D014;display:inline-block;padding:8px 18px;margin:4px 0 10px;">${esc(ref)}</p>
  <p>Please write this reference on your invoice and send it back with the item(s) to:</p>
  <p style="background:#f5f5f5;padding:10px 14px;"><b>${address.map(esc).join("<br>")}</b></p>
  <table style="border-collapse:collapse;width:100%;font-size:13px;margin:6px 0 14px;">
    <thead><tr style="background:#000;color:#fff;">
      <th style="padding:8px 10px;text-align:left;">Item</th><th style="padding:8px 10px;">Qty</th><th style="padding:8px 10px;text-align:left;">What you'd like</th>
    </tr></thead><tbody>${rows}</tbody>
  </table>
  ${needsContact ? `<p><b>Sorry something's not right with your order.</b> Give us a call on 0113 288 7713 before you send it back and we'll get it sorted quickly.</p>` : ""}
  <p>Items need to be unworn and in a condition we can sell again. Return postage is down to you, so we'd recommend a tracked or signed-for service &ndash; the parcel is your responsibility until it gets to us.</p>
  <p>Please get it back to us by <b>${prettyDate(lastDay)}</b>. Once it's here and we've checked it over, we'll ${exchanging ? "send your replacement out with free standard delivery" : "sort your refund"}${exchanging && lines.some((l) => l.outcome === "refund") ? " and process your refund" : ""}, and we'll email you when it's done.</p>
  <p>Any questions, just reply to this email or give us a call on 0113 288 7713.</p>
  <p>Thanks,<br>The Tuffshop team</p>
  </div>
</div>`;
}

// The note left on the original Brightpearl order, so anyone who opens it sees the return.
export function returnNoteText({ ref, email, lines, comments }) {
  return [
    `RETURN REQUESTED ONLINE — ${ref}`,
    `Reference emailed to: ${email}`,
    ...lines.map((l) => `- ${l.qty} x ${l.name}${l.sku ? " (" + l.sku + ")" : ""} — ${l.outcome === "exchange" ? "EXCHANGE: " + wantedText(l) : "REFUND: " + l.reason}`),
    comments ? `Customer comments: ${comments}` : "",
    "Waiting for the goods to come back. Customer pays return postage.",
  ].filter(Boolean).join("\n");
}
