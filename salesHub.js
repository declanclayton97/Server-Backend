// salesHub.js — the reasoning behind the Sales Hub's "answer this email" area.
//
// Pure functions only: no Brightpearl calls, no database, no SMTP. The server
// gathers the facts and hands them in; everything here is decidable from those
// facts alone, which is what makes it testable without credentials.
//
// The job is NOT "write a nice email". It is to work out whether we should be
// sending one at all, and to refuse to invent a date we cannot stand behind.
//
// Two failures this exists to prevent:
//
//   1. Telling a customer something a colleague already told them. They email
//      Monday, hear nothing, ring Wednesday and get an ETA, then our reply to
//      the Monday email lands Thursday repeating it. Looks careless.
//   2. CONTRADICTING that colleague. Same shape, but we give a different date
//      to the one already promised. That is far worse than repeating, and only
//      a comparison of the dates themselves catches it.

import { SIGNATURE_HTML } from "./emailSignature.js";
// The bank-holiday list the quote chase already maintains, so both agree on
// what counts as a working day rather than keeping two calendars.
import { isBankHoliday } from "./quoteChase.js";

// ---------------------------------------------------------------------------
// Intents. Ordered: the first match wins, so put the specific before the vague.
// "where is my order" is deliberately LAST because almost every chasing email
// also contains the words "order" and "when".
// ---------------------------------------------------------------------------
export const SALES_INTENTS = [
  {
    key: "proof_approval",
    label: "Waiting on proof / artwork approval",
    // Order is parked on US doing nothing because THEY have not approved.
    match: /\b(proof|artwork|mock ?-?up|approve|approval|visual)\b/i,
  },
  {
    key: "part_shipped",
    label: "Part-shipped, rest to follow",
    match: /\b(part|partial|rest of|remaining|only received|short|missing (?:items?|from))\b/i,
  },
  {
    key: "delay",
    label: "Delayed / supplier cannot supply",
    match: /\b(delay|delayed|cancel|refund|no longer|still waiting|chas(?:e|ing)|unacceptable|complain)\b/i,
  },
  {
    key: "eta",
    label: "ETA / where is my order",
    match: /\b(eta|when|where|due|expect|arrive|arriving|dispatch|despatch|deliver|delivery|update)\b/i,
  },
];

export const DEFAULT_INTENT = "eta";

// Pick the intent from the customer's own words. Falls back to ETA, because a
// sales email with no recognisable ask is nearly always "where is my stuff".
export function detectIntent(text) {
  const s = String(text || "");
  for (const intent of SALES_INTENTS) if (intent.match.test(s)) return intent.key;
  return DEFAULT_INTENT;
}

// Brightpearl order numbers as they appear in customer emails. They quote all
// sorts: "order 489373", "SO489373", "#489373", "your ref 489373".
const ORDER_NUMBER_RE = /(?:\b(?:order|ord|so|inv|invoice|ref(?:erence)?)\s*[:#]?\s*|#)(\d{4,8})\b/i;
const BARE_NUMBER_RE = /\b(\d{6})\b/;

// Pull an order number out of a pasted email. Prefers a number that is
// introduced by a word like "order", and only then falls back to any 6-digit
// number, so a phone number or a postcode does not win.
export function extractOrderNumber(text) {
  const s = String(text || "");
  const tagged = s.match(ORDER_NUMBER_RE);
  if (tagged) return tagged[1];
  const bare = s.match(BARE_NUMBER_RE);
  return bare ? bare[1] : null;
}

// A pasted email usually carries its own date in the headers. Getting this
// right is what makes the stale check trustworthy - if we cannot find it, say
// so rather than assuming "now", which would silently disable the check.
export function extractSentDate(text) {
  const s = String(text || "");
  const m = s.match(/^\s*(?:Sent|Date|On)\s*:?\s*(.+)$/im);
  if (m) {
    const d = new Date(m[1].replace(/\s+at\s+/i, " ").trim());
    if (!isNaN(d.getTime())) return d;
  }
  return null;
}

// ---------------------------------------------------------------------------
// Has somebody already answered this?
// ---------------------------------------------------------------------------

const MONTHS = { jan: 1, feb: 2, mar: 3, apr: 4, may: 5, jun: 6, jul: 7, aug: 8, sep: 9, oct: 10, nov: 11, dec: 12 };

// Every date-looking thing a colleague might have written in a note, each with
// a reader that turns it into a canonical {month, day}.
//
// These MUST reduce to a common key. A colleague types "25/09" in a note; our
// draft says "Thursday, 25 September". Comparing the raw strings makes those
// look like two DIFFERENT dates, which would flag the same date as a
// contradiction and block the most common case there is.
//
// Day-first throughout: this is a UK business, so 09/10 is 9 October. Ambiguous
// numeric dates are the one place this can be wrong, and erring day-first
// matches how everyone here writes them.
const DATE_READERS = [
  { re: /\b(\d{1,2})[\/.-](\d{1,2})(?:[\/.-]\d{2,4})?\b/g, read: (m) => ({ day: +m[1], month: +m[2] }) },
  { re: /\b(\d{1,2})(?:st|nd|rd|th)?\s+(jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)[a-z]*\b/gi, read: (m) => ({ day: +m[1], month: MONTHS[m[2].toLowerCase()] }) },
  { re: /\b(jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)[a-z]*\s+(\d{1,2})(?:st|nd|rd|th)?\b/gi, read: (m) => ({ day: +m[2], month: MONTHS[m[1].toLowerCase()] }) },
];

/**
 * Dates mentioned in a piece of text.
 *
 * Returns [{ raw, key }] - `key` is "MM-DD" for comparing one mention against
 * another regardless of how each was written; `raw` is what the person actually
 * typed, so warnings can quote them rather than showing a normalised code.
 *
 * The year is deliberately ignored. Notes rarely carry one, and an ETA is
 * always within a few weeks, so month and day identify it well enough.
 */
export function datesMentioned(text) {
  const s = String(text || "");
  const out = [];
  const seen = new Set();
  for (const { re, read } of DATE_READERS) {
    re.lastIndex = 0;
    let m;
    while ((m = re.exec(s)) !== null) {
      const { day, month } = read(m);
      if (!(month >= 1 && month <= 12) || !(day >= 1 && day <= 31)) continue;
      const key = String(month).padStart(2, "0") + "-" + String(day).padStart(2, "0");
      if (seen.has(key)) continue;
      seen.add(key);
      out.push({ raw: m[0].trim(), key });
    }
  }
  return out;
}

// The comparable keys only — what the guard actually matches on.
export const dateKeys = (text) => datesMentioned(text).map((d) => d.key);

// ---------------------------------------------------------------------------
// Not every note means somebody spoke to the customer.
//
// Brightpearl orders are full of machine chatter - the purchasing automation
// writes an "Auto-PO for CHADWICK" note on every run, and status changes leave
// their own. Counting those as contact would put a warning on virtually every
// order, and a warning that is always on is a warning nobody reads.
//
// Only two kinds count: a person typing a note, and one of OUR OWN outbound
// messages to the customer.
// ---------------------------------------------------------------------------

// Notes written by machines about internal plumbing. The customer never saw
// these, so they cannot have told them anything.
//
// Built from a survey of 403 real notes across 45 recent orders rather than
// from imagination - the first version of this guessed, matched almost nothing,
// and classified an order full of status changes and automation-rule chatter as
// four separate customer conversations.
// Notes that are machine output from end to end. Whatever follows the opening
// is more machine output, so there is nothing to look for after it.
const SYSTEM_WHOLE_NOTE = [
  /^created(\b|$)/i,                                   // "Created", "Created invoice", "Created 487600/1", "Created by Daniel Ford"
  /^updating (?:order status|shipping method)\b/i,
  /^automation rule\b/i,
  /^captured amount of\b/i,                            // card payment capture
  /^[\w .]+ tracking reference received\b/i,           // FedEx / Royal Mail / DPD
  /^this shipment did not match\b/i,
  /^there was an error when communicating with\b/i,
  /^batch purchase order \d+ was created\b/i,
  /^cloned from sale\b/i,
  /^auto-po for /i,
  /^order demand from:/i,
  /ordered on po#?\d+/i,                               // "<product> — <sku> x1 Ordered on PO#487763"
  /\bby automation with rule\b/i,                      // covers "Unallocating stock by Automation with rule ..."
  /^maximum allowed length for\b/i,                    // carrier field validation spat back at us
];

// Machine openings that a person genuinely does type on the end of - changing
// the status and adding a line about it is a habit here, e.g.
//   Updated status to "Stock needs ordering" ASKED LEE TO PARTIAL STEEL BLUE
// Only these get the leftover-text check; applying it to the list above turned
// every multi-line Auto-PO note into a "customer conversation".
const SYSTEM_MAY_HAVE_HUMAN_TAIL = [
  /^updated status to\b/i,
];

// Notes that record something the CUSTOMER was actually told. These count.
const OUTBOUND_NOTE_PATTERNS = [
  /^quote chase/i,
  /^quote refresh/i,
  /^sales hub reply sent/i,
  /^emailed\b/i,                                       // "Emailed to x@y.com", "Emailed Order Confirmation to ..."
  /^proof chased\b/i,
  /\bemail sent to\b/i,
  /\bsent proof via whatsapp\b/i,
  // A real email thread pasted into the note. Often preceded by the underscore
  // rule Outlook inserts above a quoted reply, so allow leading punctuation.
  /^[\s_>-]*from:.*\bsent:/is,
];

// System notes often carry a human afterthought: a salesperson changes the
// status and types on the end of it ("Updated status to "Stock needs ordering"
// ASKED LEE TO PARTIAL STEEL BLUE"). Matching the prefix alone would throw that
// away, so measure what is left once the machine part is removed.
const MEANINGFUL_REMAINDER = 25;

// Notes are HTML - they carry <br /> and escaped entities. Compare on text.
function plainText(raw) {
  return String(raw || "")
    .replace(/<[^>]+>/g, " ")
    .replace(/&lt;/g, "<").replace(/&gt;/g, ">").replace(/&amp;/g, "&").replace(/&nbsp;/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

export function classifyNote(note) {
  const text = plainText(note?.text);
  if (!text) return "system";
  if (OUTBOUND_NOTE_PATTERNS.some((re) => re.test(text))) return "outbound";

  if (SYSTEM_WHOLE_NOTE.some((re) => re.test(text))) return "system";

  for (const re of SYSTEM_MAY_HAVE_HUMAN_TAIL) {
    if (!re.test(text)) continue;
    // Drop the machine phrase and the status it quotes, then see whether a
    // person added anything of their own.
    const rest = text.replace(re, "").replace(/^\s*"[^"]*"/, "").replace(/^[\s:.,-]+/, "").trim();
    return rest.length >= MEANINGFUL_REMAINDER ? "human" : "system";
  }
  return "human";
}

// Does this note represent the customer being told something?
export const isContactNote = (n) => classifyNote(n) !== "system";

/**
 * Notes newer than the email we are answering - the ones that can make our
 * reply redundant or wrong.
 *
 * notes: [{ addedOn: ISO string|Date, text, addedBy }]
 * emailDate: when the CUSTOMER sent the email we are replying to.
 *
 * opts.includeSystem returns machine notes too, for showing a full timeline in
 * the UI. The guard never wants them.
 */
export function notesSince(notes, emailDate, opts = {}) {
  if (!emailDate) return [];
  const cutoff = new Date(emailDate).getTime();
  if (isNaN(cutoff)) return [];
  return (notes || [])
    .map((n) => ({ ...n, kind: classifyNote(n), _t: new Date(n.addedOn).getTime() }))
    .filter((n) => !isNaN(n._t) && n._t > cutoff)
    .filter((n) => (opts.includeSystem ? true : n.kind !== "system"))
    .sort((a, b) => b._t - a._t);
}

/**
 * The guard. Given what we are about to say and what has happened since the
 * customer wrote in, decide whether it is safe to send.
 *
 * Returns { level, reasons[], laterNotes[] } where level is:
 *   "ok"      - nothing has happened since; send normally.
 *   "warn"    - they have been contacted since, but we are not contradicting
 *               anything. Soften the wording ("further to your call...").
 *   "blocked" - we are about to give a DIFFERENT date to one already promised,
 *               or promise a date for a line the supplier has refused. A human
 *               must look at this.
 */
export function assessDuplication({ notes, emailDate, proposedDates = [], blockedLines = [], automatedEmails = [] }) {
  const laterNotes = notesSince(notes, emailDate);
  const reasons = [];
  let level = "ok";

  const bump = (to) => {
    if (to === "blocked") level = "blocked";
    else if (level !== "blocked") level = "warn";
  };

  // Promising a date for something a supplier has already refused is the worst
  // email we can send, and it does not depend on any note existing.
  if (blockedLines.length && proposedDates.length) {
    bump("blocked");
    reasons.push({
      kind: "blocked_line",
      text:
        `${blockedLines.length} line${blockedLines.length === 1 ? "" : "s"} on this order ` +
        `could not be supplied (${blockedLines.map((l) => l.sku || l.name).filter(Boolean).slice(0, 3).join(", ")}). ` +
        `Do not give a delivery date for those until they are re-sourced.`,
    });
  }

  // proposedDates arrives as the canonical keys the draft would commit us to.
  const ourKeys = proposedDates.map((d) => (typeof d === "string" && /^\d{2}-\d{2}$/.test(d) ? d : null))
    .filter(Boolean);

  for (const n of laterNotes) {
    const theirs = datesMentioned(n.text);
    if (!theirs.length) continue;
    const theirRaw = theirs.map((d) => d.raw).join(", ");

    // Same day, written differently, is NOT a contradiction.
    const agrees = theirs.some((d) => ourKeys.includes(d.key));
    if (ourKeys.length && !agrees) {
      bump("blocked");
      reasons.push({
        kind: "date_conflict",
        text:
          `${n.addedBy || "A colleague"} already gave ${theirRaw} on ` +
          `${formatWhen(n.addedOn)}. This draft says ${ourKeys.map(prettyKey).join(", ")}. ` +
          `Sending it contradicts them.`,
        note: n,
      });
    } else {
      bump("warn");
      reasons.push({
        kind: "already_told",
        text:
          `${n.addedBy || "A colleague"} already gave ${theirRaw} on ` +
          `${formatWhen(n.addedOn)} - after this email was sent. They may already know.`,
        note: n,
      });
    }
  }

  // Contact since the email that carried no date at all still matters: someone
  // has spoken to them and we do not know what was said.
  if (laterNotes.length && !reasons.some((r) => r.note)) {
    bump("warn");
    reasons.push({
      kind: "contacted_since",
      text:
        `${laterNotes.length} note${laterNotes.length === 1 ? "" : "s"} added after this email was sent. ` +
        `Read them before replying.`,
    });
  }

  for (const e of automatedEmails || []) {
    if (!emailDate || new Date(e.sentAt).getTime() <= new Date(emailDate).getTime()) continue;
    bump("warn");
    reasons.push({
      kind: "automated_email",
      text: `The system already emailed them "${e.state}" on ${formatWhen(e.sentAt)}.`,
    });
  }

  return { level, reasons, laterNotes };
}

// "09-25" -> "25 September", for warnings a person has to read.
function prettyKey(key) {
  const [m, d] = key.split("-").map(Number);
  const name = Object.keys(MONTHS).find((k) => MONTHS[k] === m) || "";
  const full = { jan: "January", feb: "February", mar: "March", apr: "April", may: "May", jun: "June", jul: "July", aug: "August", sep: "September", oct: "October", nov: "November", dec: "December" }[name];
  return `${d} ${full || name}`;
}

function formatWhen(when) {
  const d = new Date(when);
  if (isNaN(d.getTime())) return "an unknown date";
  return d.toLocaleDateString("en-GB", { weekday: "long", day: "numeric", month: "long" }) +
    " at " + d.toLocaleTimeString("en-GB", { hour: "2-digit", minute: "2-digit" });
}

// ---------------------------------------------------------------------------
// Drafting
// ---------------------------------------------------------------------------

const esc = (s) => String(s == null ? "" : s)
  .replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;").replace(/"/g, "&quot;");

const firstName = (full) => String(full || "").trim().split(/\s+/)[0] || "";

// Who to greet. Only a PERSON's name can safely be shortened to a first name:
// order.customerName is usually the company, and shortening that greets
// "Airedale Boxing Club" as "Hi Airedale". With no person on the order we say
// "Hello" rather than guess - a wrong name reads worse than no name.
export function greeting(order) {
  const person = firstName(order?.contactName);
  return person ? `Hi ${person},` : "Hello,";
}

// Phrase a delivery expectation from the PURCHASE ORDER behind the line, which
// is the only honest source we have. No PO, or a PO with no date, means we say
// we are chasing it - never a guessed date.
export function etaSentence(po) {
  if (!po) return { text: "I am chasing this with our supplier now and will come straight back to you with a date.", dates: [] };
  if (!po.expectedDate) {
    return {
      text: `This is on order with ${po.supplier || "our supplier"}${po.placedOn ? ` (placed ${formatDay(po.placedOn)})` : ""}. ` +
            `They have not confirmed a despatch date yet, so I am chasing it and will update you as soon as I hear.`,
      dates: [],
    };
  }
  const shown = formatDay(po.expectedDate);
  return {
    text: `This is on order with ${po.supplier || "our supplier"} and is currently due with us ${shown}. ` +
          `Allowing for decoration and carriage I would expect it with you shortly after that.`,
    dates: dateKeys(shown),
  };
}

// A supplier's due date lands on a Saturday often enough, and "due with us
// Saturday 19 September" promises a delivery on a day nobody delivers. Roll
// forward to the next working day - later than the raw date, never earlier,
// which is the safe direction for a promise.
function nextWorkingDay(d) {
  const out = new Date(d.getTime());
  for (let i = 0; i < 10; i++) {
    const day = out.getDay();
    if (day !== 0 && day !== 6 && !isBankHoliday(out)) return out;
    out.setDate(out.getDate() + 1);
  }
  return out;
}

function formatDay(when) {
  const d = new Date(when);
  if (isNaN(d.getTime())) return String(when);
  return nextWorkingDay(d).toLocaleDateString("en-GB", { weekday: "long", day: "numeric", month: "long" });
}

/**
 * Build the draft. Returns { subject, html, text, proposedDates }.
 *
 * Deliberately conservative: where we do not know something, the copy says we
 * are finding out. A salesperson can always add detail - they cannot un-send a
 * date that was never real.
 */
export function buildSalesReply({ intent, order, po, blockedLines = [], salesperson, tone = "warm" }) {
  // Same rule as the subject: quote the NUMBER to the customer, never the
  // internal reference. Fall back to the reference only if there is no id.
  const ref = order?.id || order?.reference;
  const lines = [];
  let proposedDates = [];

  lines.push(esc(greeting(order)));
  lines.push("Thanks for getting in touch, and sorry to keep you waiting.");

  if (intent === "proof_approval") {
    lines.push(
      `Your order ${esc(ref)} is ready to go into production - we are just waiting on your approval of the proof ` +
      `before we can start. As soon as you come back to us with a yes, it goes straight into the queue.`
    );
    lines.push("If the proof has not reached you, let me know and I will resend it.");
  } else if (intent === "part_shipped") {
    const outstanding = (order?.lines || []).filter((l) => (l.outstanding ?? 0) > 0);
    lines.push(
      `Your order ${esc(ref)} has been sent out in more than one delivery - that is why it looks short.`
    );
    if (outstanding.length) {
      lines.push(
        "Still to come:<br>" +
        outstanding.map((l) => `&bull; ${esc(l.name)}${l.outstanding ? ` &times; ${esc(l.outstanding)}` : ""}`).join("<br>")
      );
    }
    const eta = etaSentence(po);
    lines.push(eta.text);
    proposedDates = eta.dates;
  } else if (intent === "delay") {
    if (blockedLines.length) {
      lines.push(
        `I have looked into order ${esc(ref)} and I need to be straight with you - ` +
        `${blockedLines.length === 1 ? "one item" : `${blockedLines.length} items`} ` +
        `cannot be supplied by the manufacturer at the moment.`
      );
      lines.push(
        "I would rather sort this out than leave you waiting, so I can either find you the closest alternative " +
        "or take it off the order and refund that part - whichever suits you better."
      );
    } else {
      lines.push(`I am sorry order ${esc(ref)} has taken longer than it should have.`);
      const eta = etaSentence(po);
      lines.push(eta.text);
      proposedDates = eta.dates;
    }
  } else {
    const eta = etaSentence(po);
    lines.push(`I have checked order ${esc(ref)} for you.`);
    lines.push(eta.text);
    proposedDates = eta.dates;
  }

  lines.push("If there is a date you need this by, tell me and I will do what I can to work to it.");
  lines.push(`Kind regards,<br>${esc(salesperson?.name || "")}`);

  const html =
    `<div style="font-family:Arial,Helvetica,sans-serif;font-size:14px;color:#222;line-height:1.55;">` +
    lines.map((p) => `<p style="margin:0 0 14px;">${p}</p>`).join("") +
    `</div>` + SIGNATURE_HTML;

  // The SUBJECT quotes the order NUMBER, never order.reference. The reference
  // is an internal string — real ones look like
  // "#68037/130926/12 - AIREDALE BOXING" — and putting that in front of a
  // customer is both confusing and a small leak of how we file their job. The
  // number is what they were asked to quote and what they wrote in with.
  const subjectRef = order?.id ? ` - order ${order.id}` : "";
  const subject =
    intent === "proof_approval" ? `Your proof${subjectRef}`
      : intent === "delay" ? `An update on your order${subjectRef}`
        : intent === "part_shipped" ? `The rest of your order${subjectRef}`
          : `Update on your order${subjectRef}`;

  return {
    subject,
    html,
    text: lines.join("\n\n").replace(/<br>/g, "\n").replace(/<[^>]+>/g, ""),
    proposedDates,
  };
}

// What gets written to the Brightpearl order so the next person can see what
// the customer was told, without digging through a mailbox.
export function buildSalesNote({ intent, to, subject, sentBy, proposedDates = [], duplicationLevel }) {
  const label = (SALES_INTENTS.find((i) => i.key === intent) || {}).label || intent;
  const bits = [
    `Sales Hub reply sent to ${to}`,
    `Reason: ${label}`,
    `Subject: ${subject}`,
  ];
  if (proposedDates.length) bits.push(`Date given: ${proposedDates.join(", ")}`);
  if (sentBy) bits.push(`Sent by: ${sentBy}`);
  if (duplicationLevel && duplicationLevel !== "ok") bits.push(`Sent despite a "${duplicationLevel}" warning.`);
  return bits.join("\n");
}
