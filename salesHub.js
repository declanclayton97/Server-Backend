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
//
// WEB orders are the catch. A customer who ordered through the website quotes
// the number on THEIR confirmation — "000121305" — which is not a Brightpearl
// order id at all; it is the order's reference. Brightpearl can find it
// (customerRef), but only if we stop throwing the leading zeros away first.
const ORDER_NUMBER_RE = /(?:\b(?:order|ord|so|inv|invoice|ref(?:erence)?)\s*[:#]?\s*|#)(\d{4,12})\b/i;
// A zero-padded web reference. TWO or more leading zeros, deliberately: a UK
// mobile in an email signature (07960158931) has exactly one, and matching
// that would send us looking up somebody's phone number as an order.
const WEB_REF_RE = /\b(0{2,}\d{4,10})\b/;
const BARE_NUMBER_RE = /\b(\d{6})\b/g;

/**
 * Pull an order number out of a pasted email.
 *
 * Order of preference:
 *   1. a number introduced by "order" / "ref" / "#" — they told us what it is
 *   2. a zero-padded web reference, which cannot be anything else
 *   3. any bare six-digit number, which is the shape of a Brightpearl id
 *
 * Returned as a STRING, zeros intact. The caller works out whether it is an id
 * or a reference; guessing here would mean guessing twice.
 */
export function extractOrderNumber(text) {
  const s = String(text || "");
  const tagged = s.match(ORDER_NUMBER_RE);
  if (tagged) return tagged[1];
  const web = s.match(WEB_REF_RE);
  if (web) return web[1];

  // Any bare six digits — but not the back half of a phone number. "Call the
  // office on 01924 123123" was handing back 123123, and a signature with a
  // landline in it is the most ordinary thing in a customer email.
  BARE_NUMBER_RE.lastIndex = 0;
  let m;
  while ((m = BARE_NUMBER_RE.exec(s)) !== null) {
    const before = s.slice(Math.max(0, m.index - 12), m.index);
    if (/\d{4,}[\s)\-.]*$/.test(before)) continue;   // preceded by a dialling code
    return m[1];
  }
  return null;
}

// Does this look like a Brightpearl order id, or like a reference somebody was
// given by the website? Leading zeros are never an id.
export const looksLikeOrderId = (token) => /^[1-9]\d{3,8}$/.test(String(token || "").trim());

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
 * The most recent note that represents real contact with this customer.
 *
 * This is the single most useful fact on the whole page when we do not know the
 * email's date: the salesperson is looking at their email and can see its date,
 * so "last contact was Wednesday 14:32, Sarah gave an ETA" is all they need to
 * work out whether their reply is already stale.
 */
export function lastContact(notes) {
  return (notes || [])
    .filter(isContactNote)
    .map((n) => ({ ...n, _t: new Date(n.addedOn).getTime() }))
    .filter((n) => !isNaN(n._t))
    .sort((a, b) => b._t - a._t)[0] || null;
}

// Notes run long (a pasted email thread can be pages). Keep the gist.
function summarise(text, max = 120) {
  const t = String(text || "").replace(/<[^>]+>/g, " ").replace(/\s+/g, " ").trim();
  return t.length > max ? t.slice(0, max - 1) + "…" : t;
}

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
 *   "unknown" - we do not know WHEN the email was sent, so the whole check is
 *               inert. Distinct from "ok" on purpose: "ok" means we looked and
 *               found nothing, and showing that when we never looked is the
 *               one lie this module must not tell.
 *   "warn"    - they have been contacted since, but we are not contradicting
 *               anything. Soften the wording ("further to your call...").
 *   "blocked" - we are about to give a DIFFERENT date to one already promised,
 *               or promise a date for a line the supplier has refused. A human
 *               must look at this.
 */
export function assessDuplication({ notes, emailDate, proposedDates = [], blockedLines = [], automatedEmails = [], slippedFrom = null }) {
  const laterNotes = notesSince(notes, emailDate);
  const reasons = [];
  let level = emailDate ? "ok" : "unknown";

  if (!emailDate) {
    // Without a date we cannot compare anything - but the salesperson has the
    // email open in front of them, so give them the ONE fact that lets them
    // decide in a second: when this customer was last actually spoken to, and
    // what was said. They can see their own email's date; they cannot see this.
    const last = lastContact(notes);
    reasons.push({
      kind: "no_email_date",
      text: last
        ? `Last contact with this customer was ${formatWhen(last.addedOn)}` +
          (last.addedBy ? ` by ${last.addedBy}` : "") + `: "${summarise(last.text)}". ` +
          `If your email is older than that, they have already been answered.`
        : "Nobody has contacted this customer about this order yet, so there is " +
          "nothing to repeat whenever the email was sent.",
      note: last || undefined,
    });
  }

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

  // proposedDates arrives as the window keys the draft would commit us to.
  const ours = proposedDates.map(parseWindowKey).filter(Boolean);

  // The PO now says later than a colleague told them. The draft follows the PO —
  // the honest thing — but a person must see that it changes the story.
  if (slippedFrom && slippedFrom.window && ours.length) {
    bump("warn");
    reasons.push({
      kind: "window_slipped",
      text:
        `${slippedFrom.addedBy || "A colleague"} told them "${slippedFrom.raw}" on ${formatWhen(slippedFrom.addedOn)} ` +
        `(${prettyWindow(slippedFrom.window)}), but the purchase order now puts it at ${prettyWindow(ours[0])}. ` +
        `This reply moves it back — say sorry for the change.`,
    });
  }

  for (const n of laterNotes) {
    const theirs = windowsMentioned(n.text, n.addedOn);
    if (!theirs.length) continue;
    const theirRaw = theirs.map((d) => d.raw).join(", ");

    // The same window however it was worded — "mid next week" on Thursday and
    // "midweek" the following Monday — is NOT a contradiction. A different week is.
    const sameWeek = ours.length && theirs.some((t) => ours.some((o) => o.monday === t.window.monday));
    if (ours.length && !sameWeek) {
      bump("blocked");
      reasons.push({
        kind: "date_conflict",
        text:
          `${n.addedBy || "A colleague"} already told them "${theirRaw}" on ` +
          `${formatWhen(n.addedOn)} (${prettyWindow(theirs[0].window)}). This draft says ` +
          `${ours.map(prettyWindow).join(", ")}. Sending it contradicts them.`,
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
// What is actually ON the order.
//
// A Brightpearl order row is not the same thing as an item the customer bought.
// Order 487877 has FOUR rows and the customer bought ONE overall:
//
//   334405  stockTracked  brand 99   Snickers 6073 Overalls        <- goods
//   316547  non-stocked   brand 74   Embroider Right Breast        <- decoration
//   1001    non-stocked   brand 74   Shipping: Delivery            <- carriage
//   1000    non-stocked   brand 74   +++PLEASE PUT TO PROOF...+++  <- an instruction
//
// Telling a customer their order has four items when they bought one overall
// is the sort of thing that makes them stop trusting the rest of the email.
// The split is structural - stockTracked separates real goods from everything
// else - with the name only used to tell the non-stocked kinds apart.
// ---------------------------------------------------------------------------
const SHIPPING_ROW_RE = /shipping|carriage|delivery|postage|courier|p\s*&\s*p/i;
const SERVICE_ROW_RE = /embroider|\bprint\b|sticker|set ?-? ?up|personalis|personaliz|banner|dtf|digitis|digitiz|badge|transfer|vinyl|heat ?seal|artwork|origination|logo/i;

/**
 * "goods" | "service" | "shipping" | "text"
 *
 * meta is { stockTracked, brandId } from the product record, or undefined if we
 * could not look it up - in which case the row is treated as GOODS, because
 * hiding something the customer paid for is worse than showing one row too many.
 */
export function classifyOrderRow(row, meta) {
  const name = String((row && row.productName) || "");
  if (!meta) return "goods";
  if (meta.stockTracked) return "goods";
  if (SHIPPING_ROW_RE.test(name)) return "shipping";
  if (SERVICE_ROW_RE.test(name)) return "service";
  // Non-stocked, no recognisable purpose: a note somebody typed onto the order,
  // like "+++PLEASE PUT TO PROOF REQUIRED ONCE ORDERED+++".
  return "text";
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
//
// Never a date — a window ("mid next week"). Where a colleague has already
// given the customer one, that is what we repeat, re-worded for today, so the
// customer hears one story. Only if the PO now says LATER than that do we move
// it, and the guard flags that we are changing what they were told.
//
// Returns { text, dates: [windowKey], source, slippedFrom? } where source is
// "note" | "po" | "po-slipped" | "none".
export function etaSentence(po, { today = new Date(), promised = null, allowanceDays = DELIVERY_ALLOWANCE_DAYS } = {}) {
  const who = po && po.supplier ? `the ${esc(supplierLabel(po.supplier))} items` : "the stock";
  const chasing = "I am chasing this with our supplier now and will come straight back to you with an update.";
  const poWin = windowFromPo(po, allowanceDays);

  let win = null, source = "none", slippedFrom;
  if (promised && (!poWin || compareWindows(poWin, promised.window) <= 0)) { win = promised.window; source = "note"; }
  else if (poWin) {
    win = poWin;
    source = promised ? "po-slipped" : "po";
    if (promised) slippedFrom = promised;
  }

  const phrase = win ? phraseWindow(win, today) : null;
  if (!phrase) {
    // No window, or the window has already passed: the goods are late and we
    // do not know when — say we are on it rather than invent one.
    if (po && !po.expectedDate) {
      return { text: `This is on order with ${esc(supplierLabel(po.supplier) || "our supplier")}. They have not confirmed when it will be with us yet, so ${chasing.charAt(0).toLowerCase() + chasing.slice(1)}`, dates: [], source: "none" };
    }
    return { text: win ? `This is running a little later than expected. ${chasing}` : chasing, dates: [], source: win ? "overdue" : "none" };
  }
  return {
    text: `We're just waiting on ${who} to arrive, and it should be with you ${phrase}.`,
    dates: [windowKey(win)],
    phrase,
    source,
    ...(slippedFrom ? { slippedFrom } : {}),
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

// ---------------------------------------------------------------------------
// Delivery WINDOWS, not dates.
//
// The team never gives a customer an exact day. They say "early next week",
// "mid next week", "late this week" — a date is a promise the courier gets to
// break for us. So an ETA here is a window: the Monday of a week plus a part of
// it (early = Mon/Tue, mid = Wed, late = Thu/Fri, week = "sometime that week").
//
// A window is ABSOLUTE (it names a real week) and only turned back into words
// at the moment of writing, relative to the day we write. That matters: Jack's
// note on Thursday 24 Sep says "mid next week"; answering on Monday 28 Sep, the
// same window is "mid this week", and repeating his words would move it a week.
// ---------------------------------------------------------------------------
export const WINDOW_PARTS = ["early", "mid", "late"];

// A calendar day in the UK as a local-midnight Date, whatever the server's zone.
function ukDay(when) {
  const d = when instanceof Date ? when : new Date(when);
  if (isNaN(d.getTime())) return null;
  const [y, m, day] = d.toLocaleDateString("en-CA", { timeZone: "Europe/London" }).split("-").map(Number);
  return new Date(y, m - 1, day);
}
const addDays = (d, n) => new Date(d.getFullYear(), d.getMonth(), d.getDate() + n);
const isoDay = (d) => `${d.getFullYear()}-${String(d.getMonth() + 1).padStart(2, "0")}-${String(d.getDate()).padStart(2, "0")}`;
const mondayOf = (d) => addDays(d, -((d.getDay() + 6) % 7));
const partOfDay = (d) => (d.getDay() <= 2 ? "early" : d.getDay() === 3 ? "mid" : "late");

function addWorkingDays(d, n) {
  let out = nextWorkingDay(d);
  for (let i = 0; i < n; i++) out = nextWorkingDay(addDays(out, 1));
  return out;
}

// The window a given day falls in (a weekend or bank holiday rolls forward).
export function windowOf(when) {
  const d = ukDay(when);
  if (!d) return null;
  const w = nextWorkingDay(d);
  return { monday: isoDay(mondayOf(w)), part: partOfDay(w) };
}
export const windowKey = (w) => (w ? `${w.monday}/${w.part}` : null);
export function parseWindowKey(key) {
  const m = /^(\d{4}-\d{2}-\d{2})\/(early|mid|late|week)$/.exec(String(key || ""));
  return m ? { monday: m[1], part: m[2] } : null;
}

// Order two windows: negative if a is sooner. "week" sorts as the END of its week,
// the cautious reading of "sometime next week".
const PART_RANK = { early: 0, mid: 1, late: 2, week: 2 };
export function compareWindows(a, b) {
  if (a.monday !== b.monday) return a.monday < b.monday ? -1 : 1;
  return PART_RANK[a.part] - PART_RANK[b.part];
}

/**
 * Words for a window, as seen from `today`. Returns null when the window is
 * already behind us — the goods are late, and saying so is the caller's job.
 */
export function phraseWindow(w, today = new Date()) {
  const t = ukDay(today);
  const weeks = Math.round((new Date(w.monday + "T12:00:00") - mondayOf(t)) / (7 * 86400000));
  if (weeks < 0) return null;
  if (weeks === 0) {
    if (w.part !== "week" && PART_RANK[w.part] < PART_RANK[partOfDay(t)]) return null;
    return { early: "early this week", mid: "midweek", late: "by the end of this week", week: "this week" }[w.part];
  }
  if (weeks === 1) return w.part === "week" ? "next week" : `${w.part} next week`;
  if (weeks === 2) return w.part === "week" ? "the week after next" : `${w.part} the week after next`;
  return `in around ${weeks} weeks`;
}

const WEEKDAYS = ["sunday", "monday", "tuesday", "wednesday", "thursday", "friday", "saturday"];
const PART_WORDS = [
  [/^(?:early|beginning of|start of)$/i, "early"],
  [/^(?:mid|middle of)$/i, "mid"],
  [/^(?:late|end of|back end of|latter part of)$/i, "late"],
];
const partFromWords = (s) => (PART_WORDS.find(([re]) => re.test(String(s || "").trim())) || [])[1] || "week";

/**
 * Every delivery window a note gives, read relative to when the NOTE was written.
 * Covers what the team actually types — "mid next week", "early this week",
 * "end of the week", "midweek", "week after next", "Tuesday", "25/09".
 * Returns [{ raw, window }].
 */
export function windowsMentioned(text, writtenOn) {
  const s = plainText(text);
  const base = ukDay(writtenOn || new Date());
  if (!s || !base) return [];
  const out = [];
  const seen = new Set();
  const push = (raw, w) => {
    const k = windowKey(w);
    if (!w || seen.has(k)) return;
    seen.add(k);
    out.push({ raw: raw.trim(), window: w });
  };
  const inWeek = (offset, part) => ({ monday: isoDay(addDays(mondayOf(base), 7 * offset)), part });

  const PART = "(early|beginning of|start of|mid|middle of|late|end of|back end of|latter part of)";
  let m;
  const rel = new RegExp(`\\b(?:${PART}[\\s-]*)?(?:the\\s+)?(this|next)\\s+week\\b`, "gi");
  while ((m = rel.exec(s))) {
    // "next week" inside "the week after next" is handled below
    if (/after\s*$/i.test(s.slice(0, m.index))) continue;
    push(m[0], inWeek(m[2].toLowerCase() === "next" ? 1 : 0, partFromWords(m[1])));
  }
  const after = new RegExp(`\\b(?:${PART}[\\s-]*)?(?:the\\s+)?week after next\\b`, "gi");
  while ((m = after.exec(s))) push(m[0], inWeek(2, partFromWords(m[1])));
  // "midweek", "end of the week", "later this week" without this/next: this week,
  // unless that part of it has already gone — then it can only mean next week.
  const bare = /\b(mid[\s-]?week|end of (?:the )?week|later (?:on )?in the week|by the weekend)\b/gi;
  while ((m = bare.exec(s))) {
    const part = /^mid/i.test(m[1]) ? "mid" : "late";
    const gone = PART_RANK[part] < PART_RANK[partOfDay(base)];
    push(m[0], inWeek(gone ? 1 : 0, part));
  }
  // A named weekday: the next one after the note (never the note's own day).
  const wd = /\b(?:on|by|for|until|till|due|arriving|expected)\s+(monday|tuesday|wednesday|thursday|friday)\b/gi;
  while ((m = wd.exec(s))) {
    const target = WEEKDAYS.indexOf(m[1].toLowerCase());
    let d = addDays(base, 1);
    while (d.getDay() !== target) d = addDays(d, 1);
    push(m[0], windowOf(d));
  }
  if (/\btomorrow\b/i.test(s)) push("tomorrow", windowOf(addWorkingDays(base, 1)));
  // Written dates ("25/09", "Thursday 25 September"): the next such day on or after
  // the note, so a December note saying "05/01" means January.
  for (const d of datesMentioned(s)) {
    const [mm, dd] = d.key.split("-").map(Number);
    let when = new Date(base.getFullYear(), mm - 1, dd);
    if (when < addDays(base, -60)) when = new Date(base.getFullYear() + 1, mm - 1, dd);
    push(d.raw, windowOf(when));
  }
  return out;
}

/**
 * The window this customer was most recently given — the newest note that
 * represents contact (a person, or one of our own replies) and names one.
 * Machine notes are ignored: "Auto-PO ... due 25/09" was never said to anyone.
 */
export function promisedWindow(notes) {
  const withWindows = (notes || [])
    .filter(isContactNote)
    .map((n) => ({ n, t: new Date(n.addedOn).getTime(), found: windowsMentioned(n.text, n.addedOn) }))
    .filter((x) => !isNaN(x.t) && x.found.length)
    .sort((a, b) => b.t - a.t);
  if (!withWindows.length) return null;
  const { n, found } = withWindows[0];
  return { window: found[0].window, raw: found[0].raw, addedBy: n.addedBy || "", addedOn: n.addedOn, text: summarise(n.text) };
}

// "MASCOT" -> "Mascot": supplier names are stored shouting.
const supplierLabel = (s) => String(s || "").trim().toLowerCase().replace(/\b[a-z]/g, (c) => c.toUpperCase());

// Working days from the goods reaching us to reaching the customer: goods-in,
// decoration, courier. Tunable without a deploy of the logic.
export const DELIVERY_ALLOWANCE_DAYS = Number(process.env.SALES_HUB_ALLOWANCE_DAYS || 2);

/**
 * When the customer should have it, as a window, from the supplier's date plus
 * our allowance. null when the PO has no date.
 */
export function windowFromPo(po, allowanceDays = DELIVERY_ALLOWANCE_DAYS) {
  if (!po || !po.expectedDate) return null;
  const d = ukDay(po.expectedDate);
  return d ? windowOf(addWorkingDays(d, allowanceDays)) : null;
}

// "2026-09-28/mid" -> "mid week commencing 28 September", for notes and warnings.
export function prettyWindow(key) {
  const w = typeof key === "string" ? parseWindowKey(key) : key;
  if (!w) return String(key || "");
  const wc = new Date(w.monday + "T12:00:00").toLocaleDateString("en-GB", { day: "numeric", month: "long" });
  return `${w.part === "week" ? "" : w.part + " "}week commencing ${wc}`;
}

/**
 * Build the draft. Returns { subject, html, text, proposedDates }.
 *
 * Deliberately conservative: where we do not know something, the copy says we
 * are finding out. A salesperson can always add detail - they cannot un-send a
 * date that was never real.
 */
/**
 * signedBy is the person WRITING the reply — whoever is signed into the hub —
 * and it beats the order's salesperson. The order may have been raised weeks
 * ago by someone who is on holiday; the customer should hear from the person
 * who actually answered them, and that person should not be signing a
 * colleague's name to their own words.
 */
export function buildSalesReply({ intent, order, po, blockedLines = [], salesperson, signedBy, tone = "warm", today = new Date(), promised = null }) {
  const etaOpts = { today, promised };
  let eta = null;
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
    eta = etaSentence(po, etaOpts);
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
      eta = etaSentence(po, etaOpts);
      lines.push(eta.text);
      proposedDates = eta.dates;
    }
  } else {
    eta = etaSentence(po, etaOpts);
    lines.push(`I have checked order ${esc(ref)} for you.`);
    lines.push(eta.text);
    proposedDates = eta.dates;
  }

  lines.push("If there is a date you need this by, tell me and I will do what I can to work to it.");
  // Whoever is signed in signs it; the order's salesperson is only a fallback
  // for a draft built outside the hub.
  lines.push(`Kind regards,<br>${esc(String(signedBy || "").trim() || salesperson?.name || "")}`);

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
    // How the window was chosen, so the page can say "kept to what Jack told them".
    eta: eta ? { source: eta.source, phrase: eta.phrase || null, slippedFrom: eta.slippedFrom || null } : null,
  };
}

// What gets written to the Brightpearl order so the next person can see what
// the customer was told, without digging through a mailbox.
//
// The WHOLE email goes in, not just "I sent one": the next person to pick the
// order up needs to see exactly what the customer was told, in the words used.
export function buildSalesNote({ intent, to, subject, sentBy, proposedDates = [], duplicationLevel, body = "" }) {
  const label = (SALES_INTENTS.find((i) => i.key === intent) || {}).label || intent;
  const bits = [
    `Sales Hub reply sent to ${to}`,
    `Reason: ${label}`,
    `Subject: ${subject}`,
  ];
  if (proposedDates.length) bits.push(`Delivery window given: ${proposedDates.map(prettyWindow).join(", ")}`);
  if (sentBy) bits.push(`Sent by: ${sentBy}`);
  if (duplicationLevel && duplicationLevel !== "ok") bits.push(`Sent despite a "${duplicationLevel}" warning.`);
  const text = String(body || "").trim();
  return bits.join("\n") + (text ? `\n\n--- Email sent ---\n${text}` : "");
}

// The sent HTML as plain text for the order note: paragraphs and line breaks kept,
// the signature block dropped (it is the same on every email and pages long).
export function emailToNoteText(html) {
  let s = String(html || "");
  const at = s.indexOf(SIGNATURE_HTML);
  if (at >= 0) s = s.slice(0, at) + s.slice(at + SIGNATURE_HTML.length);
  return s
    .replace(/<br\s*\/?>/gi, "\n")
    .replace(/<\/(p|div|li|h\d)>/gi, "\n")
    .replace(/<li[^>]*>/gi, "• ")
    .replace(/<[^>]+>/g, "")
    .replace(/&nbsp;/g, " ").replace(/&bull;/g, "•").replace(/&times;/g, "×")
    .replace(/&lt;/g, "<").replace(/&gt;/g, ">").replace(/&quot;/g, '"').replace(/&#39;/g, "'").replace(/&amp;/g, "&")
    .replace(/[ \t]+\n/g, "\n")
    .replace(/\n{3,}/g, "\n\n")
    .trim();
}
