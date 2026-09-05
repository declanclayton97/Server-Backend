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

const isWeekend = (d) => d.getDay() === 0 || d.getDay() === 6;

// Advance `n` whole working days from `from`, keeping the time of day. Counting
// starts at the NEXT day: 1 working day after Friday 16:00 is Monday 16:00.
export function addWorkingDays(from, n) {
  const d = new Date(from.getTime());
  let left = n;
  while (left > 0) {
    d.setDate(d.getDate() + 1);
    if (!isWeekend(d)) left--;
  }
  return d;
}

// Pull a timestamp into the next send window. Before the window on a working day
// → same day at startHour. After it, or on a weekend → the next working day at
// startHour. Inside the window → unchanged.
export function clampToSendWindow(when, cfg = QUOTE_CHASE_CONFIG) {
  const { startHour, endHour } = cfg.sendWindow;
  const d = new Date(when.getTime());
  if (!isWeekend(d) && d.getHours() >= startHour && d.getHours() < endHour) return d;
  if (!isWeekend(d) && d.getHours() < startHour) {
    d.setHours(startHour, 0, 0, 0);
    return d;
  }
  do {
    d.setDate(d.getDate() + 1);
  } while (isWeekend(d));
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
export function buildChaseEmail(quote, stage, responseUrl) {
  const hi = firstNameOf(quote) ? `Hi ${esc(firstNameOf(quote))},` : "Hi,";
  const ref = `SO${quote.orderId}`;
  const opener = stage === 1
    ? `We sent you a quote and wanted to check it reached you.`
    : stage === 2
      ? `Just following up on the quote we sent — we have not heard back yet.`
      : `This is our last reminder about the quote below.`;
  const subject = stage === 1
    ? `Did you get our quote? — ${ref}`
    : `Following up on your quote — ${ref}`;

  const button = (action, label, bg) => `
    <a href="${esc(responseUrl)}&action=${action}"
       style="display:inline-block;margin:4px 6px 4px 0;padding:11px 18px;border-radius:5px;
              background:${bg};color:#fff;text-decoration:none;font-weight:bold;font-size:14px;">${label}</a>`;

  const html = `
  <div style="font-family:Arial,sans-serif;max-width:600px;color:#333;">
    <p>${hi}</p>
    <p>${opener}</p>
    <table style="border-collapse:collapse;margin:14px 0;font-size:14px;">
      <tr><td style="padding:4px 12px 4px 0;color:#777;">Quote</td><td style="padding:4px 0;font-weight:bold;">${ref}</td></tr>
      ${quote.reference ? `<tr><td style="padding:4px 12px 4px 0;color:#777;">Reference</td><td style="padding:4px 0;">${esc(quote.reference)}</td></tr>` : ""}
      <tr><td style="padding:4px 12px 4px 0;color:#777;">Total</td><td style="padding:4px 0;font-weight:bold;">${gbp(quote.netValue)} + VAT</td></tr>
    </table>
    <p>Let us know how you would like to proceed:</p>
    <p style="margin:16px 0;">
      ${button("go_ahead", "Go ahead with the quote", "#1e7b34")}
      ${button("more_time", "I need more time", "#0073e6")}<br>
      ${button("call_back", "Please call me", "#b07000")}
      ${button("cancel", "Cancel the quote", "#8a8a8a")}
    </p>
    <p style="font-size:13px;color:#777;">Or just reply to this email and it will go straight to
      ${esc(quote.salespersonName || "your account manager")}.</p>
  </div>`;

  const text = `${hi}

${opener}

Quote ${ref}${quote.reference ? ` (${quote.reference})` : ""} — ${gbp(quote.netValue)} + VAT

Let us know how you would like to proceed:
  Go ahead:      ${responseUrl}&action=go_ahead
  Need more time:${responseUrl}&action=more_time
  Please call me:${responseUrl}&action=call_back
  Cancel:        ${responseUrl}&action=cancel

Or reply to this email and it will reach ${quote.salespersonName || "your account manager"}.`;

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

// Sent to the salesperson when all three chases went unanswered.
export function buildHandoverEmail(quote) {
  const subject = `No reply after 3 chases — SO${quote.orderId} ${quote.customerName ? "· " + quote.customerName : ""}`.trim();
  const html = `
  <div style="font-family:Arial,sans-serif;max-width:640px;color:#333;">
    <h2 style="color:#b07000;border-bottom:2px solid #b07000;padding-bottom:8px;">Over to you</h2>
    <p>${esc(quote.customerName || "This customer")} has not answered any of the three chases on this quote.
       It is still sitting in <strong>Quote sent</strong>.</p>
    <table style="border-collapse:collapse;font-size:14px;margin:12px 0;">
      <tr><td style="padding:5px 14px 5px 0;color:#777;">Quote</td><td style="padding:5px 0;font-weight:bold;">SO${quote.orderId}</td></tr>
      <tr><td style="padding:5px 14px 5px 0;color:#777;">Customer</td><td style="padding:5px 0;">${esc(quote.customerName)}${quote.companyName ? " · " + esc(quote.companyName) : ""}</td></tr>
      ${quote.customerEmail ? `<tr><td style="padding:5px 14px 5px 0;color:#777;">Email</td><td style="padding:5px 0;">${esc(quote.customerEmail)}</td></tr>` : ""}
      <tr><td style="padding:5px 14px 5px 0;color:#777;">Value</td><td style="padding:5px 0;">${gbp(quote.netValue)} + VAT</td></tr>
      <tr><td style="padding:5px 14px 5px 0;color:#777;">Quote sent</td><td style="padding:5px 0;">${esc(String(quote.enteredStatusAt || "").slice(0, 10))}</td></tr>
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
