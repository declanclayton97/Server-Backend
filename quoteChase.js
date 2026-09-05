// Quote follow-up: chase a customer who has been sent a quote, three times, then
// hand the job back to the salesperson who raised it.
//
// Pure — no I/O, no DB, no clock of its own. Every function takes `now`
// explicitly so the whole schedule is testable without waiting three days.
// server.js owns the polling, the database and the sending.
//
// The sequence, counted from the moment the order entered "Quote sent":
//   1 working day   → chase 1  "did you get the quote?"
//   2 working days  → chase 2
//   3 working days  → chase 3
//   4 working days  → hand to the salesperson to chase personally
//
// Any customer response stops the sequence dead.

// Working days, not calendar days: a quote sent Friday afternoon must not be
// chased on Saturday. Sends are additionally held to office hours so nothing
// arrives at 3am — a chase that falls due outside the window waits for the next
// opening rather than being skipped.
export const QUOTE_CHASE_CONFIG = {
  stageWorkingDays: [1, 2, 3],   // chases 1..3
  handoverWorkingDay: 4,         // salesperson chases personally
  sendWindow: { startHour: 9, endHour: 17 },
};

// The reasons a customer can give for cancelling. `note` is always optional —
// the free-text box is offered on every reason, and on its own if they pick none.
export const QUOTE_CANCEL_REASONS = [
  { key: "gone_elsewhere",  label: "Gone elsewhere" },
  { key: "no_longer_needed", label: "No longer needed" },
  { key: "just_a_quote",    label: "Just wanted a quote" },
  { key: "too_expensive",   label: "Too expensive" },
  { key: "needs_requote",   label: "Needs a re-quote" },
];

export const QUOTE_ACTIONS = [
  { key: "go_ahead",   label: "Go ahead with the quote" },
  { key: "cancel",     label: "Cancel the quote" },
  { key: "more_time",  label: "I need more time" },
  { key: "call_back",  label: "Please call me" },
];

// England & Wales bank holidays, from https://www.gov.uk/bank-holidays.json.
// Without these a quote sent on 23 December gets chased on Christmas Day.
//
// This is the fallback list only — server.js refreshes it from gov.uk at boot
// via setBankHolidays(), so it stays correct without anyone editing this file.
// It matters that the baseline is here too: if that fetch fails the maths still
// skips the holidays rather than silently reverting to weekends-only.
const DEFAULT_BANK_HOLIDAYS = [
  // 2026
  "2026-01-01", "2026-04-03", "2026-04-06", "2026-05-04", "2026-05-25", "2026-08-31", "2026-12-25", "2026-12-28",
  // 2027
  "2027-01-01", "2027-03-26", "2027-03-29", "2027-05-03", "2027-05-31", "2027-08-30", "2027-12-27", "2027-12-28",
  // 2028
  "2028-01-03", "2028-04-14", "2028-04-17", "2028-05-01", "2028-05-29", "2028-08-28", "2028-12-25", "2028-12-26",
];

let bankHolidays = new Set(DEFAULT_BANK_HOLIDAYS);

// Replace the set, e.g. with a fresh pull from gov.uk. Ignores anything that is
// not a YYYY-MM-DD string, and refuses an empty list — a failed or malformed
// fetch must not quietly turn holiday handling off.
export function setBankHolidays(dates) {
  const clean = (dates || []).filter((d) => /^\d{4}-\d{2}-\d{2}$/.test(String(d)));
  if (!clean.length) return false;
  bankHolidays = new Set(clean);
  return true;
}

// Local date as YYYY-MM-DD. Not toISOString(), which converts to UTC and would
// call 00:30 on 26 December the 25th during BST.
function localDay(d) {
  return `${d.getFullYear()}-${String(d.getMonth() + 1).padStart(2, "0")}-${String(d.getDate()).padStart(2, "0")}`;
}

export function isBankHoliday(d) {
  return bankHolidays.has(localDay(d));
}

export function listBankHolidays() {
  return [...bankHolidays].sort();
}

const isWeekend = (d) => d.getDay() === 0 || d.getDay() === 6;
const isNonWorking = (d) => isWeekend(d) || isBankHoliday(d);

// Advance `n` whole working days from `from`, keeping the time of day. Counting
// starts at the NEXT day: 1 working day after Friday 16:00 is Monday 16:00.
export function addWorkingDays(from, n) {
  const d = new Date(from.getTime());
  let left = n;
  let guard = 0;
  while (left > 0) {
    d.setDate(d.getDate() + 1);
    if (!isNonWorking(d)) left--;
    // A malformed holiday set could otherwise spin forever.
    if (++guard > 400) break;
  }
  return d;
}

// Pull a timestamp into the next send window. Before the window on a working day
// → same day at startHour. After it, or on a weekend → the next working day at
// startHour. Inside the window → unchanged.
export function clampToSendWindow(when, cfg = QUOTE_CHASE_CONFIG) {
  const { startHour, endHour } = cfg.sendWindow;
  const d = new Date(when.getTime());
  if (!isNonWorking(d) && d.getHours() >= startHour && d.getHours() < endHour) return d;
  if (!isNonWorking(d) && d.getHours() < startHour) {
    d.setHours(startHour, 0, 0, 0);
    return d;
  }
  let guard = 0;
  do {
    d.setDate(d.getDate() + 1);
  } while (isNonWorking(d) && ++guard <= 400);
  d.setHours(startHour, 0, 0, 0);
  return d;
}

// When chase `stage` (1..3) is due, or the handover when stage is 4.
export function dueAtForStage(enteredStatusAt, stage, cfg = QUOTE_CHASE_CONFIG) {
  const days = stage <= cfg.stageWorkingDays.length
    ? cfg.stageWorkingDays[stage - 1]
    : cfg.handoverWorkingDay;
  return clampToSendWindow(addWorkingDays(new Date(enteredStatusAt), days), cfg);
}

/**
 * What should happen to one quote right now.
 *
 * `row` is the tracking record:
 *   { enteredStatusAt, stage, responded, seeded, customerEmail, stillQuoteSent }
 *
 * Returns one of:
 *   { action: "none",     reason }
 *   { action: "chase",    stage, dueAt }        send chase N to the customer
 *   { action: "handover", dueAt }               tell the salesperson to chase
 */
export function decideAction(row, now, cfg = QUOTE_CHASE_CONFIG) {
  if (!row) return { action: "none", reason: "no row" };

  // The backlog that existed when this went live is recorded but never chased —
  // some of those quotes are over a year old and emailing them would do harm.
  if (row.seeded) return { action: "none", reason: "seeded backlog" };

  // A quote that has left "Quote sent" is settled one way or another.
  if (row.stillQuoteSent === false) return { action: "none", reason: "no longer quote sent" };

  // Any answer ends the sequence, including "more time" and "call me" — both put
  // the ball in the salesperson's court, so chasing on would be nagging.
  if (row.responded) return { action: "none", reason: "customer responded" };

  // Stopped by hand from the dashboard. Customers reply to the email far more
  // often than they click, and those replies go to the salesperson, not to us —
  // without a way to say "I've dealt with this" we would keep chasing someone who
  // already answered.
  if (row.stopped) return { action: "none", reason: "stopped manually" };

  if (!row.customerEmail) return { action: "none", reason: "no customer email" };

  const stage = Number(row.stage) || 0;
  if (stage >= cfg.stageWorkingDays.length + 1) return { action: "none", reason: "sequence complete" };

  const next = stage + 1;
  const dueAt = dueAtForStage(row.enteredStatusAt, next, cfg);
  if (now < dueAt) return { action: "none", reason: "not due", dueAt };

  return next > cfg.stageWorkingDays.length
    ? { action: "handover", dueAt }
    : { action: "chase", stage: next, dueAt };
}

/**
 * Bundle open quotes into one chase per CUSTOMER.
 *
 * A customer with several open quotes must not get one email per quote: at build
 * time 11 customers had more than one open, and one had seven — which would have
 * been 7 emails at 24h, 7 more at 48h and 7 more at 72h. That reads as spam and
 * puts the sending domain at risk.
 *
 * Each group is driven by its least-chased quote (ties broken by the oldest), so
 * a newly added quote pulls its customer back into the sequence rather than being
 * silently skipped. Quotes already further along are carried in the same email
 * but never regressed — see applyStage below.
 *
 * Returns [{ key, quotes[], driver }], quotes ordered oldest first.
 */
export function groupQuotesForChase(rows) {
  const groups = new Map();
  for (const r of rows || []) {
    // No email means no chase; fall back to the order id so such a quote forms
    // its own group and decideAction can report why it is being skipped.
    const key = String(r.customerEmail || "").trim().toLowerCase() || `order:${r.orderId}`;
    if (!groups.has(key)) groups.set(key, []);
    groups.get(key).push(r);
  }
  return [...groups.entries()].map(([key, quotes]) => {
    quotes.sort((a, b) => new Date(a.enteredStatusAt) - new Date(b.enteredStatusAt));
    const driver = quotes.slice().sort((a, b) =>
      (Number(a.stage) || 0) - (Number(b.stage) || 0) ||
      new Date(a.enteredStatusAt) - new Date(b.enteredStatusAt)
    )[0];
    return { key, quotes, driver };
  });
}

// Which quotes in a group should move to `stage` — only those behind it. A quote
// already on chase 3 stays on 3 when it rides along in a customer's chase-1
// email, otherwise it would be chased all over again.
export function quotesToAdvance(group, stage) {
  return group.quotes.filter((q) => (Number(q.stage) || 0) < stage);
}

// ---- Email bodies -----------------------------------------------------------

const esc = (s) => String(s == null ? "" : s)
  .replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;").replace(/"/g, "&quot;");

const gbp = (n) => "£" + (Number(n) || 0).toLocaleString("en-GB", { minimumFractionDigits: 2, maximumFractionDigits: 2 });

function firstNameOf(quote) {
  const full = String(quote.customerName || "").trim();
  return (full.split(/\s+/)[0] || "").trim();
}

// The customer-facing chase. Wording escalates gently across the three sends —
// the first assumes it was missed, the last says it is the final one, so a
// customer who ignores all three is not surprised when a salesperson rings.
// `quotes` is everything open for ONE customer — one email covers the lot.
// `urlFor(quote)` returns that quote's own response link, so each quote is still
// answered individually (a customer may want one and not another).
//
// One quote gets the four buttons inline. Several get a list with a link each:
// seven quotes times four buttons is 28 buttons and unusable.
export function buildChaseEmail(quotes, stage, urlFor) {
  const list = Array.isArray(quotes) ? quotes : [quotes];
  const first = list[0] || {};
  const many = list.length > 1;
  const url = typeof urlFor === "function" ? urlFor : () => String(urlFor);

  const hi = firstNameOf(first) ? `Hi ${esc(firstNameOf(first))},` : "Hi,";
  const noun = many ? `${list.length} quotes` : `a quote`;
  const opener = stage === 1
    ? `We sent you ${noun} and wanted to check ${many ? "they" : "it"} reached you.`
    : stage === 2
      ? `Just following up on the ${many ? "quotes" : "quote"} we sent — we have not heard back yet.`
      : `This is our last reminder about the ${many ? "quotes" : "quote"} below.`;
  const subject = many
    ? (stage === 1 ? `Did you get our ${list.length} quotes?` : `Following up on your ${list.length} quotes`)
    : (stage === 1 ? `Did you get our quote? — SO${first.orderId}` : `Following up on your quote — SO${first.orderId}`);

  const button = (u, action, label, bg) => `
    <a href="${esc(u)}&action=${action}"
       style="display:inline-block;margin:4px 6px 4px 0;padding:11px 18px;border-radius:5px;
              background:${bg};color:#fff;text-decoration:none;font-weight:bold;font-size:14px;">${label}</a>`;

  const body = many
    ? `<table style="border-collapse:collapse;width:100%;font-size:14px;margin:14px 0;">
         ${list.map((q) => `
         <tr>
           <td style="padding:9px 10px 9px 0;border-bottom:1px solid #eee;">
             <strong>SO${q.orderId}</strong>${q.reference ? `<br><span style="color:#777;font-size:12px;">${esc(q.reference)}</span>` : ""}
           </td>
           <td style="padding:9px 10px;border-bottom:1px solid #eee;white-space:nowrap;">${gbp(q.netValue)} + VAT</td>
           <td style="padding:9px 0;border-bottom:1px solid #eee;text-align:right;">
             <a href="${esc(url(q))}" style="display:inline-block;padding:8px 15px;border-radius:5px;background:#0073e6;color:#fff;text-decoration:none;font-weight:bold;font-size:13px;">Reply about this one</a>
           </td>
         </tr>`).join("")}
       </table>
       <p>Tell us how you would like to proceed with each one — or just reply to this email.</p>`
    : `<table style="border-collapse:collapse;margin:14px 0;font-size:14px;">
         <tr><td style="padding:4px 12px 4px 0;color:#777;">Quote</td><td style="padding:4px 0;font-weight:bold;">SO${first.orderId}</td></tr>
         ${first.reference ? `<tr><td style="padding:4px 12px 4px 0;color:#777;">Reference</td><td style="padding:4px 0;">${esc(first.reference)}</td></tr>` : ""}
         <tr><td style="padding:4px 12px 4px 0;color:#777;">Total</td><td style="padding:4px 0;font-weight:bold;">${gbp(first.netValue)} + VAT</td></tr>
       </table>
       <p>Let us know how you would like to proceed:</p>
       <p style="margin:16px 0;">
         ${button(url(first), "go_ahead", "Go ahead with the quote", "#1e7b34")}
         ${button(url(first), "more_time", "I need more time", "#0073e6")}<br>
         ${button(url(first), "call_back", "Please call me", "#b07000")}
         ${button(url(first), "cancel", "Cancel the quote", "#8a8a8a")}
       </p>`;

  const html = `
  <div style="font-family:Arial,sans-serif;max-width:600px;color:#333;">
    <p>${hi}</p>
    <p>${opener}</p>
    ${body}
    <p style="font-size:13px;color:#777;">Or just reply to this email and it will go straight to
      ${esc(first.salespersonName || "your account manager")}.</p>
  </div>`;

  const text = `${hi}

${opener}

${many
    ? list.map((q) => `  SO${q.orderId}${q.reference ? ` (${q.reference})` : ""} — ${gbp(q.netValue)} + VAT\n    ${url(q)}`).join("\n")
    : `Quote SO${first.orderId}${first.reference ? ` (${first.reference})` : ""} — ${gbp(first.netValue)} + VAT

Let us know how you would like to proceed:
  Go ahead:       ${url(first)}&action=go_ahead
  Need more time: ${url(first)}&action=more_time
  Please call me: ${url(first)}&action=call_back
  Cancel:         ${url(first)}&action=cancel`}

Or reply to this email and it will reach ${first.salespersonName || "your account manager"}.`;

  return { subject, html, text };
}

// Sent to the salesperson the moment a customer picks an option.
export function buildResponseEmail(quote, response) {
  const actionLabel = (QUOTE_ACTIONS.find((a) => a.key === response.action) || {}).label || response.action;
  const reasonLabel = response.reason
    ? (QUOTE_CANCEL_REASONS.find((r) => r.key === response.reason) || {}).label || response.reason
    : "";
  const urgent = response.action === "go_ahead" || response.action === "call_back";
  const subject = `${actionLabel} — SO${quote.orderId} ${quote.customerName ? "· " + quote.customerName : ""}`.trim();

  const html = `
  <div style="font-family:Arial,sans-serif;max-width:640px;color:#333;">
    <h2 style="color:${urgent ? "#1e7b34" : "#333"};border-bottom:2px solid ${urgent ? "#1e7b34" : "#ccc"};padding-bottom:8px;">
      ${esc(actionLabel)}</h2>
    <table style="border-collapse:collapse;font-size:14px;margin:12px 0;">
      <tr><td style="padding:5px 14px 5px 0;color:#777;">Quote</td><td style="padding:5px 0;font-weight:bold;">SO${quote.orderId}</td></tr>
      <tr><td style="padding:5px 14px 5px 0;color:#777;">Customer</td><td style="padding:5px 0;">${esc(quote.customerName)}${quote.companyName ? " · " + esc(quote.companyName) : ""}</td></tr>
      <tr><td style="padding:5px 14px 5px 0;color:#777;">Value</td><td style="padding:5px 0;">${gbp(quote.netValue)} + VAT</td></tr>
      ${reasonLabel ? `<tr><td style="padding:5px 14px 5px 0;color:#777;">Reason</td><td style="padding:5px 0;font-weight:bold;">${esc(reasonLabel)}</td></tr>` : ""}
      <tr><td style="padding:5px 14px 5px 0;color:#777;">Chases sent</td><td style="padding:5px 0;">${response.stage || 0}</td></tr>
    </table>
    ${response.note ? `<p style="margin:12px 0;padding:12px 14px;background:#f7f7f7;border-left:4px solid #0073e6;">
      <strong>Their note:</strong><br>${esc(response.note).replace(/\n/g, "<br>")}</p>` : ""}
    ${quote.customerEmail ? `<p style="font-size:13px;color:#777;">Reply to them at
      <a href="mailto:${esc(quote.customerEmail)}">${esc(quote.customerEmail)}</a>.</p>` : ""}
  </div>`;

  return { subject, html };
}

// Sent to the salesperson when all three chases went unanswered. Takes the whole
// customer group, so one customer produces one handover, not one per quote.
export function buildHandoverEmail(quotes) {
  const list = Array.isArray(quotes) ? quotes : [quotes];
  const first = list[0] || {};
  const many = list.length > 1;
  const total = list.reduce((s, q) => s + (Number(q.netValue) || 0), 0);
  const subject = many
    ? `No reply after 3 chases — ${list.length} quotes · ${first.customerName || ""}`.trim()
    : `No reply after 3 chases — SO${first.orderId} ${first.customerName ? "· " + first.customerName : ""}`.trim();

  const rows = list.map((q) => `
      <tr>
        <td style="padding:6px 12px 6px 0;border-bottom:1px solid #eee;"><strong>SO${q.orderId}</strong>
          ${q.reference ? `<br><span style="color:#777;font-size:12px;">${esc(q.reference)}</span>` : ""}</td>
        <td style="padding:6px 12px;border-bottom:1px solid #eee;white-space:nowrap;">${gbp(q.netValue)}</td>
        <td style="padding:6px 0;border-bottom:1px solid #eee;color:#777;">sent ${esc(String(q.enteredStatusAt || "").slice(0, 10))}</td>
      </tr>`).join("");

  const html = `
  <div style="font-family:Arial,sans-serif;max-width:640px;color:#333;">
    <h2 style="color:#b07000;border-bottom:2px solid #b07000;padding-bottom:8px;">Over to you</h2>
    <p>${esc(first.customerName || "This customer")} has not answered any of the three chases on
       ${many ? `these ${list.length} quotes` : "this quote"}. Still sitting in <strong>Quote sent</strong>.</p>
    <p style="font-size:14px;">
      ${esc(first.customerName)}${first.companyName ? " · " + esc(first.companyName) : ""}
      ${first.customerEmail ? `<br><a href="mailto:${esc(first.customerEmail)}">${esc(first.customerEmail)}</a>` : ""}
    </p>
    <table style="border-collapse:collapse;font-size:14px;margin:12px 0;">${rows}
      ${many ? `<tr><td style="padding:8px 12px 8px 0;font-weight:bold;">Total</td>
        <td style="padding:8px 12px;font-weight:bold;">${gbp(total)}</td><td></td></tr>` : ""}
    </table>
    <p>Worth a phone call.</p>
  </div>`;
  return { subject, html };
}

// The internal Brightpearl note for each step, so the order's own history shows
// where the chase got to without anyone opening the dashboard.
export function buildBpNote(kind, detail) {
  if (kind === "chase") return `Quote chase ${detail.stage} of 3 emailed to ${detail.to}`;
  if (kind === "handover") return `Quote chase: no reply after 3 chases — passed to ${detail.salespersonName || "the salesperson"} to chase personally`;
  if (kind === "response") {
    const action = (QUOTE_ACTIONS.find((a) => a.key === detail.action) || {}).label || detail.action;
    const reason = detail.reason
      ? " — " + ((QUOTE_CANCEL_REASONS.find((r) => r.key === detail.reason) || {}).label || detail.reason)
      : "";
    const note = detail.note ? ` — note: ${detail.note}` : "";
    return `Quote chase: customer replied "${action}"${reason}${note} (after ${detail.stage || 0} chase${detail.stage === 1 ? "" : "s"})`;
  }
  return "Quote chase";
}
