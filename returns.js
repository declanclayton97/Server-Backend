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
// 30 days from the ORDER date (Dec, 30 Sep: "check the order date, not the arrival date"),
// matching the returns page's "30 days from your order date".
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
 * { ok, code, message, orderedOn, despatchedOn, lastDay, lines: [{ rowId, name, sku, qty, available }] }
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
  // Still has to have been sent (above) — but the clock runs from the order date.
  const orderedOn = ukDate(order.placedOn || order.createdOn || despatchedOn);
  const lastDay = addDays(orderedOn, RETURN_WINDOW_DAYS);
  if (ukDate(today) > lastDay) {
    return fail("too-late",
      `You ordered this on ${prettyDate(orderedOn)}, which is outside our ${RETURN_WINDOW_DAYS} day returns window. If something's wrong with it, give us a call and we'll see what we can do.`,
      { orderedOn, despatchedOn, lastDay });
  }

  const lines = rows
    .filter((x) => x.kind === "goods")
    .map(({ rowId, r }) => {
      const qty = Math.round(Number((r.quantity && r.quantity.magnitude) || 0));
      const already = Math.max(0, Math.round(Number(requested[rowId] || 0)));
      return { rowId, productId: Number(r.productId) || null, name: String(r.productName || "").trim(), sku: r.productSku || "", qty, available: Math.max(0, qty - already) };
    })
    .filter((l) => l.qty > 0);
  if (!lines.length) return fail("nothing", "There's nothing on this order we can take back online. Give us a call or drop us an email and we'll help.");
  if (!lines.some((l) => l.available > 0)) {
    return fail("already", "You've already started a return for everything on this order. Your returns reference is in the email we sent you.",
      { orderedOn, despatchedOn, lastDay });
  }
  return { ok: true, code: "ok", message: "", orderedOn, despatchedOn, lastDay, lines };
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
      chosen.push({ rowId: line.rowId, productId: line.productId, name: line.name, sku: line.sku, qty, outcome: "exchange",
        exchangeChoice: choice, exchangeFor: choice === "Something else" ? exchangeFor : "", reason: "Exchange: " + choice.toLowerCase() });
    } else if (p.outcome === "refund") {
      const reason = REFUND_REASONS.includes(p.reason) ? p.reason : null;
      if (!reason) return { ok: false, error: `Let us know why "${line.name}" is coming back.` };
      chosen.push({ rowId: line.rowId, productId: line.productId, name: line.name, sku: line.sku, qty, outcome: "refund", exchangeChoice: "", exchangeFor: "", reason });
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
    `RETURN REQUESTED ONLINE - ${ref}`,
    `Reference emailed to: ${email}`,
    ...lines.map((l) => `- ${l.qty} x ${l.name}${l.sku ? " (" + l.sku + ")" : ""} - ${l.outcome === "exchange" ? "EXCHANGE: " + wantedText(l) : "REFUND: " + l.reason}`),
    comments ? `Customer comments: ${comments}` : "",
    "Waiting for the goods to come back.",
  ].filter(Boolean).join("\n");
}

// ---------------------------------------------------------------------------
// STATUS EMAILS — sent when staff move a return along in the Sales Hub.
// Cancelled / reopened send nothing: those are corrections, not news.
// `message` is what the staff member typed for the customer; a rejection needs one.
// ---------------------------------------------------------------------------
const emailShell = (name, body) => `<div style="font-family:'Open Sans',Arial,sans-serif;font-size:14px;line-height:1.55;color:#222;max-width:620px;">
  <div style="background:#000;padding:14px 18px;"><img src="https://tuffshop.co.uk/media/athlete2/default/tuffshop_logo.png" alt="Tuffshop" height="56" style="display:block;height:56px;"></div>
  <div style="padding:18px 4px;">
  <p>${name ? "Hi " + esc(name) + "," : "Hi,"}</p>
  ${body}
  <p>Any questions, just reply to this email or give us a call on 0113 288 7713.</p>
  <p>Thanks,<br>The Tuffshop team</p>
  </div></div>`;

const itemList = (lines) => "<ul>" + (lines || []).map((l) =>
  `<li>${l.qty} &times; ${esc(l.name)}${l.outcome === "exchange" ? " &ndash; swapping for " + esc(wantedText(l).toLowerCase()) : ""}</li>`).join("") + "</ul>";

export const STATUS_EMAILS = ["received", "refunded", "exchanged", "rejected"];

// ctx (sent by Process, which is what now moves a return to refunded/exchanged):
//   refundAmount  the refund requested from accounts, inc VAT
//   swaps         [{ name, size, inStock }] what is being sent in place of what
export function statusEmail(status, row, message = "", ctx = {}) {
  const ref = esc(row.ref), msg = String(message || "").trim();
  const extra = msg ? `<p style="background:#f5f5f5;padding:10px 14px;">${esc(msg).replace(/\n/g, "<br>")}</p>` : "";
  const exchanging = (row.lines || []).some((l) => l.outcome === "exchange");
  const refunding = (row.lines || []).some((l) => l.outcome === "refund");
  const name = row.first_name || "";
  if (status === "received") {
    const next = exchanging && refunding ? "send your replacement out and sort your refund"
      : exchanging ? "send your replacement out with free standard delivery" : "sort your refund";
    return { subject: `We've got your return ${row.ref}`,
      html: emailShell(name, `<p>Just to let you know your return <b>${ref}</b> has arrived with us:</p>${itemList(row.lines)}
        <p>We'll check it over and ${next} as soon as we can. We'll email you again when that's done.</p>${extra}`) };
  }
  // Sent the moment Process has run: the refund has been PASSED TO ACCOUNTS (not paid
  // yet) and the replacement has been SET UP (not necessarily sent) — worded to be true
  // at that moment.
  const amount = ctx.refundAmount ? ` of <b>&pound;${Number(ctx.refundAmount).toFixed(2)}</b>` : "";
  const refundPara = `<p>We've checked the items you sent back and passed your refund${amount} to our accounts team. It goes back the same way you paid &ndash; once it's been processed it can take a few working days to show on your account, depending on your bank.</p>`;
  if (status === "refunded") {
    return { subject: `Your refund for return ${row.ref}`,
      html: emailShell(name, `<p>Thanks for sending back return <b>${ref}</b>.</p>${refundPara}${extra}`) };
  }
  if (status === "exchanged") {
    const swaps = (ctx.swaps || []).length
      ? "<ul>" + ctx.swaps.map((s) => `<li>${esc(s.name)}${s.size ? " &ndash; <b>" + esc(s.size) + "</b>" : ""}</li>`).join("") + "</ul>"
      : itemList((row.lines || []).filter((l) => l.outcome === "exchange"));
    // Deliberately says nothing about stock or dates: the replacement may have to be
    // ordered in again (Dec, 30 Sep), so "being processed" is all that is certain.
    return { subject: `Your exchange for return ${row.ref}`,
      html: emailShell(name, `<p>Thanks for sending back return <b>${ref}</b>. We've checked it over and your exchange is now being processed:</p>${swaps}
        <p>It'll be sent out to you with free standard delivery.</p>
        ${refunding ? refundPara : ""}${extra}`) };
  }
  if (status === "rejected") {
    if (!msg) return null;
    return { subject: `About your return ${row.ref}`,
      html: emailShell(name, `<p>We've had a look at the items you sent back under return <b>${ref}</b>, and unfortunately we can't accept this return:</p>${extra}
        <p>Give us a call on 0113 288 7713 and we'll sort out what happens next.</p>`) };
  }
  return null;
}

// ---------------------------------------------------------------------------
// PHOTOS — for faulty or damaged items. Emailed to sales with the return notice.
// The page shrinks them to JPEGs before sending; these are the server's limits.
// ---------------------------------------------------------------------------
export const PHOTO_MAX = 6;
export const PHOTO_MAX_BYTES = 6 * 1024 * 1024;
export const PHOTOS_REQUIRED_RE = /faulty|damaged/i;
export function validatePhotos(photos, lines) {
  const list = Array.isArray(photos) ? photos : [];
  if (list.length > PHOTO_MAX) return { ok: false, error: `You can add up to ${PHOTO_MAX} photos.` };
  const out = [];
  for (const [i, p] of list.entries()) {
    const type = String((p && p.contentType) || "");
    if (!/^image\/(jpeg|png|webp|heic|heif)$/i.test(type)) return { ok: false, error: "Photos need to be pictures (JPG or PNG)." };
    const b64 = String((p && p.base64) || "");
    const bytes = Math.floor(b64.length * 3 / 4);
    if (!bytes) return { ok: false, error: "One of the photos didn't come through. Please try adding it again." };
    if (bytes > PHOTO_MAX_BYTES) return { ok: false, error: "One of the photos is too big. Please try a smaller one." };
    const ext = type.split("/")[1].replace("jpeg", "jpg");
    out.push({ name: `photo-${i + 1}.${ext}`, contentType: type, base64: b64 });
  }
  if (!out.length && (lines || []).some((l) => PHOTOS_REQUIRED_RE.test(l.reason || ""))) {
    return { ok: false, error: "As something's faulty or damaged, please add a photo so we can see the problem." };
  }
  return { ok: true, photos: out };
}

// ---------------------------------------------------------------------------
// REPORT — what the online returns say about fit. A swap up a size or a "too
// small" refund both mean the item came up small; the reverse means big.
// ---------------------------------------------------------------------------
export function fitVotes(line) {
  const r = String(line.reason || "").toLowerCase(), c = String(line.exchangeChoice || "").toLowerCase();
  const q = Number(line.qty || 1);
  if (/too small/.test(r) || /sizes? up/.test(c)) return { small: q, big: 0 };
  if (/too big/.test(r) || /sizes? down/.test(c)) return { small: 0, big: q };
  return { small: 0, big: 0 };
}
export function fitSignal(small, big) {
  const n = small + big;
  if (n < 3) return null;                         // too few to say anything
  if (small / n >= 0.7) return "runs small";
  if (big / n >= 0.7) return "runs big";
  return null;
}

// "Snickers 6241 AllroundWork Trousers (Black) Size-36\" Waist 194557" -> the style,
// without the size/colour tail, so every variant of a style reads as one name.
export function styleName(name) {
  let n = String(name || "");
  n = n.replace(/\bSizes?\s*[-:]?\s*[^,()]*$/i, " ");
  n = n.replace(/\)\s*[-–]\s*[A-Z0-9.\/"]{1,8}\s*$/i, ")");   // "(Black)-44" -> "(Black)"
  n = n.replace(/\([^)]*\)\s*$/, " ");
  n = n.replace(/\s\d{5,}\s*$/, " ");
  n = n.replace(/[\s,\-–(\/]+$/, "");
  return n.replace(/\s{2,}/g, " ").trim();
}

// ---------------------------------------------------------------------------
// SIZES — "one size up" within a style. Brightpearl's option values all carry
// sortOrder 0, so the order comes from the size text itself. A size is split into
// a RANK (what moves) and a SIGNATURE (what must stay the same):
//   "31 Waist 28 Leg (Snickers Size 192)" -> rank 31, sig "# waist 28 leg"   (leg stays)
//   "C52"  -> 52, "c#"        "36R" -> 36, "#r"        "UK 8 / EU42" -> 8, "uk #"
//   "XL" / "X-Large"          -> letter rank 5, sig ""
// Brackets (a brand's own size code) and anything after "/" (a conversion) are
// ignored for the signature, because they change WITH the size.
// ---------------------------------------------------------------------------
const LETTERS = [
  ["xxxs", "3xs", "xxx small", "xxx-small"], ["xxs", "2xs", "xx small", "xx-small"], ["xs", "x-small", "x small", "extra small"], ["s", "small", "sm"], ["m", "medium", "med"],
  ["l", "large", "lg"], ["xl", "x-large", "extra large"], ["xxl", "2xl", "xx-large"], ["xxxl", "3xl", "xxx-large"],
  ["4xl", "xxxxl", "xxxx-large"], ["5xl", "xxxxxl", "xxxxx-large"], ["6xl", "xxxxxxl"], ["7xl", "xxxxxxxl"], ["8xl", "xxxxxxxxl"],
];
export function sizeKey(text) {
  const raw = String(text || "").toLowerCase().replace(/\([^)]*\)/g, " ").split("/")[0].replace(/\s+/g, " ").trim();
  if (!raw) return null;
  for (let i = 0; i < LETTERS.length; i++) {
    for (const w of LETTERS[i]) {
      const re = new RegExp("(^|\s)" + w.replace(/[-]/g, "\-") + "(?=\s|$)");
      if (re.test(raw)) return { fam: "alpha", rank: i, sig: raw.replace(re, "$1#").trim() };
    }
  }
  const m = raw.match(/\d+(?:\.\d+)?/);
  if (!m) return null;
  return { fam: "num", rank: Number(m[0]), sig: (raw.slice(0, m.index) + "#" + raw.slice(m.index + m[0].length)).trim() };
}
export const SIZE_STEPS = { "One size up": 1, "One size down": -1, "Two sizes up": 2, "Two sizes down": -2 };

// siblings: [{ productId, size, colourId }]; returns the sibling `steps` sizes away
// from `current` in the same colour and fit, or null when there is no such size.
export function moveSize(current, siblings, steps) {
  const k = sizeKey(current.size);
  if (!k || !steps) return null;
  const same = siblings.filter((s) => s.colourId === current.colourId).map((s) => ({ ...s, k: sizeKey(s.size) }))
    .filter((s) => s.k && s.k.fam === k.fam && s.k.sig === k.sig);
  const ranks = [...new Set(same.map((s) => s.k.rank))].sort((a, b) => a - b);
  const at = ranks.indexOf(k.rank);
  if (at < 0) return null;
  const want = ranks[at + steps];
  if (want === undefined) return null;
  return same.find((s) => s.k.rank === want) || null;
}
