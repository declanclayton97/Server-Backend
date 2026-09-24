// Tests for salesHub.js.
// Run with:  node salesHub.test.js

import {
  detectIntent,
  extractOrderNumber,
  looksLikeOrderId,
  extractSentDate,
  datesMentioned,
  dateKeys,
  notesSince,
  lastContact,
  classifyNote,
  classifyOrderRow,
  assessDuplication,
  etaSentence,
  buildSalesReply,
  buildSalesNote,
  windowsMentioned,
  windowKey,
  phraseWindow,
  promisedWindow,
  emailToNoteText,
} from "./salesHub.js";

let pass = 0, fail = 0;
function assertEq(label, actual, expected) {
  const ok = JSON.stringify(actual) === JSON.stringify(expected);
  if (ok) pass++;
  else { fail++; console.error(`  x ${label}\n      expected ${JSON.stringify(expected)}\n      got      ${JSON.stringify(actual)}`); }
}
const assertTrue = (label, actual) => assertEq(label, !!actual, true);
const assertFalse = (label, actual) => assertEq(label, !!actual, false);

// --- intent -----------------------------------------------------------------
assertEq("proof beats eta", detectIntent("Can you resend the proof to approve? When will it ship?"), "proof_approval");
assertEq("part-shipped", detectIntent("We only received part of the order, where is the rest"), "part_shipped");
assertEq("delay/cancel", detectIntent("Still waiting. If it's delayed again I want to cancel"), "delay");
assertEq("plain eta", detectIntent("Any update on when this is due?"), "eta");
assertEq("unrecognised falls back to eta", detectIntent("Morning!"), "eta");

// --- order number -----------------------------------------------------------
assertEq("tagged wins", extractOrderNumber("Hi, chasing order 489373 please"), "489373");
assertEq("hash form", extractOrderNumber("ref #488976 outstanding"), "488976");
assertEq("SO prefix", extractOrderNumber("SO489433 - any news"), "489433");
assertEq("bare 6-digit fallback", extractOrderNumber("your 489448 hasn't arrived"), "489448");
assertEq("no number", extractOrderNumber("Where is my stuff?"), null);
// The order number beats an invoice number, whichever comes first (Dec, 24 Sep).
assertEq("order beats invoice", extractOrderNumber("Your Invoice #000109482 for Order #000123455"), "000123455");
assertEq("order beats invoice (plain)", extractOrderNumber("Invoice 109482 - order 489373"), "489373");
assertEq("invoice number used only when alone", extractOrderNumber("Query on invoice 000109482"), "000109482");
assertEq("bare # beats an invoice label", extractOrderNumber("Invoice #000109482 re #489373"), "489373");
assertEq("order number wording", extractOrderNumber("Order number: 489373"), "489373");
// A phone number must not beat a tagged order number.
assertEq("tagged beats phone", extractOrderNumber("call me on 01924 123456 re order 489373"), "489373");

// --- WEB order references ---------------------------------------------------
// A website customer quotes the number on THEIR confirmation, which is the
// order's reference and not a Brightpearl id at all. 000121305 is order 486086.
assertEq("zero-padded web reference survives intact",
  extractOrderNumber("Hi, chasing my order 000121305 please"), "000121305");
assertEq("web reference with no introducing word",
  extractOrderNumber("Morning,\n\n000121305 still not here\n\nThanks"), "000121305");
assertEq("leading zeros are NOT stripped", extractOrderNumber("order 000122226"), "000122226");
// The trap: a UK mobile in a signature has ONE leading zero. Matching that
// would send us looking up somebody's phone number as an order.
assertEq("a mobile number is not an order",
  extractOrderNumber("Thanks\nJack\n07960158931\njack@example.com"), null);
assertEq("a landline is not an order either",
  extractOrderNumber("Call the office on 01924 123123"), null);
// Which shape is which.
assertTrue("a plain id looks like an id", looksLikeOrderId("489373"));
assertFalse("a zero-padded ref does not", looksLikeOrderId("000121305"));
assertFalse("nor does an empty string", looksLikeOrderId(""));
assertFalse("nor does something non-numeric", looksLikeOrderId("SO489373"));

// --- sent date --------------------------------------------------------------
assertTrue("parses Sent: header", extractSentDate("From: bob@x.com\nSent: 14 September 2026 09:12\nTo: sales") instanceof Date);
assertEq("missing date returns null, not now()", extractSentDate("no headers here"), null);

// --- dates mentioned --------------------------------------------------------
assertEq("slash date -> key", dateKeys("due 25/09 we said"), ["09-25"]);
assertEq("wordy date -> same key", dateKeys("promised 25th September"), ["09-25"]);
assertEq("month-first -> same key", dateKeys("due September 25"), ["09-25"]);
assertEq("dotted date -> same key", dateKeys("due 25.09.2026"), ["09-25"]);
assertEq("no date", dateKeys("spoke to customer, all fine"), []);
assertEq("keeps what was typed", datesMentioned("due 25/09")[0].raw, "25/09");
// Day-first: a UK note saying 09/10 is 9 October, not 10 September.
assertEq("day-first reading", dateKeys("due 09/10"), ["10-09"]);
// Nonsense must not become a date.
assertEq("rejects impossible month", dateKeys("ref 25/99"), []);

// THE CASE THAT MATTERS: same day, written two different ways, must agree.
// Getting this wrong flags a repeat as a contradiction and blocks the most
// common situation there is.
const colleagueSaid = dateKeys("Rang customer, gave ETA 25/09");
const weWouldSay = dateKeys("currently due with us Thursday, 25 September");
assertEq("cross-format dates match", colleagueSaid[0], weWouldSay[0]);

// --- notesSince -------------------------------------------------------------
const emailMon = "2026-09-14T09:00:00Z";
const notes = [
  { addedOn: "2026-09-13T15:00:00Z", text: "Quote sent", addedBy: "Bob" },       // before
  { addedOn: "2026-09-16T14:32:00Z", text: "Rang customer, gave ETA 25/09", addedBy: "Sarah" }, // after
];
assertEq("only later notes", notesSince(notes, emailMon).length, 1);
assertEq("no email date disables it", notesSince(notes, null).length, 0);

// --- machine chatter must not count as contact ------------------------------
// Every string below is VERBATIM from a survey of 403 real notes across 45
// recent orders. The first version of this classifier was written from
// imagination and called all of these "human", which put a bogus "they have
// been contacted since" warning on an order nobody had spoken to.
const SYSTEM_SAMPLES = [
  "Created 487600/1",
  "Created invoice",
  "Created",
  "Created by Daniel Ford",
  'Updated status to "Invoiced and Completed"',
  'Updated status to "Stock needs ordering"<br />',
  "Captured amount of £53.48 online. Transaction ID: \"qb2xwazb\"",
  'Updating shipping method to "Royal Mail - Tracked 24" by Automation with rule "SHIPPING METHOD ROYAL"',
  "FedEx tracking reference received: 877166388598",
  "Royal Mail tracking reference received: KJ682470415GB",
  "DPD Local tracking reference received: 6962859887",
  "Automation rule 'Prints Needed New': Updating sales custom field PCF_PRINTSNEEDED",
  'Updating Order Status by Automation with rule "BANCROFT ISPPS"',
  "This shipment did not match any automatic shipping rules",
  "There was an error when communicating with FedEx: (STREETLINE1.TOO.SHORT)",
  "Batch purchase order 488050 was created using the following items from this order: 1",
  "Cloned from sale 483158",
  "Auto-PO for CHADWICK.\nOrder demand from:\n  SO#489256...",
  "Dewalt Carlisle Safety Boots (Wheat) — TB14022026 x1 Ordered on PO#487763",
];
SYSTEM_SAMPLES.forEach((t, i) =>
  assertEq(`system sample ${i}: ${t.slice(0, 34)}`, classifyNote({ text: t }), "system"));

// Things that DID reach the customer.
const OUTBOUND_SAMPLES = [
  "Emailed to amaritavares2010@icloud.com",
  "Emailed Order Confirmation to karen.hazelgrave@hubrx.co.uk Hi Karen, Bob is off sick today",
  "Quote chase 2 of 3 emailed to bob@x.com",
  "Sales Hub reply sent to dave@x.com",
  "From: Hani | Tuff Shop &lt;hani@tuffshop.co.uk&gt; Sent: 14 September 2026 12:50 To: Karen Hazelgrave",
];
OUTBOUND_SAMPLES.forEach((t, i) =>
  assertEq(`outbound sample ${i}: ${t.slice(0, 34)}`, classifyNote({ text: t }), "outbound"));

// A person typing.
assertEq("a person's note is human", classifyNote({ text: "Rang customer, gave ETA 25/09" }), "human");
assertEq("exchange note is human",
  classifyNote({ text: "ORIGINAL ORDER SO#483158 CREDITED VIA SC#487627 EXCHANGE ORDER SO#487628" }), "human");
// A status change a salesperson typed on the end of is NOT just a status change.
assertEq("status change + a human afterthought is human",
  classifyNote({ text: 'Updated status to "Stock needs ordering" ASKED LEE TO PARTIAL STEEL BLUE' }), "human");
assertEq("empty note is system", classifyNote({ text: "   " }), "system");
// Three more caught by running the classifier over the whole corpus.
assertEq("automation rule anywhere is system",
  classifyNote({ text: String.fromCharCode(34) + "Unallocating stock by Automation with rule " + String.fromCharCode(34) + "6241 SNO" + String.fromCharCode(34) }), "system");
assertEq("carrier field validation is system",
  classifyNote({ text: "Maximum allowed length for Receiver Company Name is 35. Dixon & Townsend Mechanical" }), "system");
assertEq("quoted email thread under an underscore rule is outbound",
  classifyNote({ text: "________________ From: Karen Hazelgrave &lt;karen@hubrx.co.uk&gt; Sent: 14 September 2026" }), "outbound");
assertEq("proof chase is outbound",
  classifyNote({ text: "Proof Chased - Email sent to lewis@googlemail.com on 14/09/2026" }), "outbound");
assertEq("whatsapp proof is outbound",
  classifyNote({ text: "Harry sent Proof via WhatsApp to 447402898546" }), "outbound");

assertEq("html-only note is system", classifyNote({ text: "<br /><br />" }), "system");

const noisy = [
  { addedOn: "2026-09-16T10:00:00Z", text: "Auto-PO for CHADWICK.\nOrder demand from: SO#1", addedBy: 42485 },
  { addedOn: "2026-09-16T11:00:00Z", text: "Auto-PO for PENCARRIE.\nOrder demand from: SO#2", addedBy: 42485 },
];
assertEq("a wall of auto-PO notes is not contact", notesSince(noisy, emailMon).length, 0);
assertEq("quiet despite the noise",
  assessDuplication({ notes: noisy, emailDate: emailMon, proposedDates: [] }).level, "ok");
assertEq("but the UI can still see them",
  notesSince(noisy, emailMon, { includeSystem: true }).length, 2);

// --- THE CORE GUARD ---------------------------------------------------------
// Dec's scenario: emails Monday, rings Wednesday and is given 25/09, our reply
// to the Monday email would land Thursday.
const repeat = assessDuplication({ notes, emailDate: emailMon, proposedDates: ["2026-09-21/late"] });
assertEq("same date already given -> warn", repeat.level, "warn");
assertTrue("says they already know", repeat.reasons.some((r) => r.kind === "already_told"));

// Worse: we are about to give a DIFFERENT date to the one promised.
const clash = assessDuplication({ notes, emailDate: emailMon, proposedDates: ["2026-09-28/late"] });
assertEq("different date -> blocked", clash.level, "blocked");
assertTrue("names the conflict", clash.reasons.some((r) => r.kind === "date_conflict"));
assertTrue("quotes the colleague", clash.reasons[0].text.includes("Sarah"));

// Nothing since the email at all.
assertEq("quiet order -> ok", assessDuplication({ notes: [notes[0]], emailDate: emailMon, proposedDates: ["2026-09-21/late"] }).level, "ok");

// NO EMAIL DATE: must NOT read as a clean bill of health. "ok" means we looked
// and found nothing; saying that when we never looked is the one lie this
// module must not tell.
const noDate = assessDuplication({ notes, emailDate: null, proposedDates: ["2026-09-21/late"] });
assertEq("no email date -> unknown, not ok", noDate.level, "unknown");
assertTrue("explains why the check is off", noDate.reasons.some(r => r.kind === "no_email_date"));
// The one fact that lets a salesperson decide for themselves: when this
// customer was last actually spoken to, and what was said.
assertTrue("names when they were last contacted", /Wednesday 16 September/.test(noDate.reasons[0].text));
assertTrue("names who", /Sarah/.test(noDate.reasons[0].text));
assertTrue("quotes what was said", noDate.reasons[0].text.includes("gave ETA 25/09"));
assertTrue("tells them how to use it", /older than that/.test(noDate.reasons[0].text));
const noDateQuiet = assessDuplication({ notes: [], emailDate: null, proposedDates: [] });
assertTrue("says so when there is nothing to worry about either",
  /Nobody has contacted this customer/.test(noDateQuiet.reasons[0].text));
// lastContact ignores machine notes entirely.
assertEq("lastContact skips auto-PO chatter",
  lastContact([{addedOn:"2026-09-20T10:00:00Z",text:"Auto-PO for CHADWICK. Order demand from: SO#1"},
               {addedOn:"2026-09-18T10:00:00Z",text:"Rang customer, gave ETA 25/09",addedBy:"Sarah"}]).text,
  "Rang customer, gave ETA 25/09");
assertEq("lastContact on a silent order is null", lastContact([]), null);
// A blocked line still blocks even with no date - that check needs no date.
assertEq("no date but a blocked line + a date still blocks",
  assessDuplication({ notes: [], emailDate: null, proposedDates: ["2026-09-21/late"],
    blockedLines: [{ sku: "X" }] }).level, "blocked");

// Contact since, but no date in it - still worth a look.
const vague = assessDuplication({
  notes: [{ addedOn: "2026-09-16T10:00:00Z", text: "Customer called, left message", addedBy: "Tom" }],
  emailDate: emailMon, proposedDates: [],
});
assertEq("contact with no date -> warn", vague.level, "warn");
assertTrue("tells them to read it", vague.reasons.some((r) => r.kind === "contacted_since"));

// Never promise a date on a line the supplier refused.
const blocked = assessDuplication({
  notes: [], emailDate: emailMon, proposedDates: ["2026-09-21/late"],
  blockedLines: [{ sku: "119627-271-407", name: "Fristads trousers" }],
});
assertEq("blocked line + a date -> blocked", blocked.level, "blocked");
assertTrue("names the sku", blocked.reasons[0].text.includes("119627-271-407"));

// A blocked line WITHOUT a proposed date is not itself a blocker - saying
// "I'm chasing it" is exactly what we want to allow.
assertEq("blocked line, no date -> ok",
  assessDuplication({ notes: [], emailDate: emailMon, proposedDates: [], blockedLines: [{ sku: "X" }] }).level, "ok");

// The automation counts as contact too.
const auto = assessDuplication({
  notes: [], emailDate: emailMon, proposedDates: [],
  automatedEmails: [{ state: "Despatched", sentAt: "2026-09-15T08:00:00Z" }],
});
assertEq("automated email since -> warn", auto.level, "warn");

// --- ETA honesty ------------------------------------------------------------
assertTrue("no PO -> chasing, no date", etaSentence(null).text.includes("chasing"));
assertEq("no PO proposes no date", etaSentence(null).dates, []);
assertTrue("PO without a date -> still no promise",
  etaSentence({ supplier: "Blaklader", placedOn: "2026-09-10" }).expectedDate === undefined);
assertEq("PO without a date proposes nothing",
  etaSentence({ supplier: "Blaklader", placedOn: "2026-09-10" }).dates, []);
// Windows, never dates. Today is fixed so the wording is testable.
const THU24 = new Date("2026-09-24T12:00:00+01:00"), MON28 = new Date("2026-09-28T09:00:00+01:00");
// A supplier date on a Saturday rolls to Monday, + 2 working days allowance = Wednesday.
const weekend = etaSentence({ supplier: "CHADWICK", expectedDate: "2026-09-26" }, { today: THU24 });
assertEq("Saturday PO date -> window mid next week", weekend.dates, ["2026-09-28/mid"]);
assertTrue("worded as a window", weekend.text.includes("mid next week"));
assertFalse("never names a day", /monday|tuesday|wednesday|thursday|friday|saturday|sunday/i.test(weekend.text));
assertFalse("never names a date", /\b\d{1,2}(st|nd|rd|th)?\s+(sep|oct)/i.test(weekend.text));
assertTrue("supplier name is not shouted", weekend.text.includes("Chadwick"));
// A PO date already behind us is late: say we are chasing, promise nothing.
const late = etaSentence({ supplier: "CHADWICK", expectedDate: "2026-09-10" }, { today: THU24 });
assertEq("overdue PO proposes nothing", late.dates, []);
assertTrue("overdue PO says we are chasing", /chasing/.test(late.text));
const withDate = etaSentence({ supplier: "Blaklader", expectedDate: "2026-09-25" }, { today: THU24 });
assertTrue("PO with a date names the supplier", withDate.text.includes("Blaklader"));
assertTrue("PO with a date proposes one", withDate.dates.length > 0);

// --- what is actually ON the order -----------------------------------------
// Real rows and real product records from order 487877, where the customer
// bought ONE overall and the hub reported "4 items".
const G = { stockTracked: true, brandId: 99 };          // 334405
const NS = { stockTracked: false, brandId: 74 };        // 1000 / 1001 / 316547
assertEq("a stocked garment is goods",
  classifyOrderRow({ productName: "Snickers 6073 Durable Service Overalls (Black)-M Regular" }, G), "goods");
assertEq("embroidery is a service, not an item",
  classifyOrderRow({ productName: "Embroider Right Breast" }, NS), "service");
assertEq("carriage is shipping",
  classifyOrderRow({ productName: "Shipping: Delivery - Mainland UK including Lowland Scotland" }, NS), "shipping");
assertEq("an instruction row is text",
  classifyOrderRow({ productName: "+++PLEASE PUT TO PROOF REQUIRED ONCE ORDERED+++" }, NS), "text");
// Unresolved product: show it rather than hide something they paid for.
assertEq("unknown product falls through to goods",
  classifyOrderRow({ productName: "Mystery item" }, undefined), "goods");
// A stocked product whose NAME mentions embroidery is still a garment - this is
// the trap that cost £58k of phantom decoration cost in the margin reports.
assertEq("a garment named after its decoration is still goods",
  classifyOrderRow({ productName: "Blaklader 3332 T-Shirt - INC LEFT BREAST EMBROIDERY" }, G), "goods");

// --- drafting ---------------------------------------------------------------
const order = { id: 489373, reference: "489373", customerName: "Dave Smith", contactName: "Dave Smith", lines: [] };
const draft = buildSalesReply({ intent: "eta", order, po: { supplier: "Blaklader", expectedDate: "2026-09-25" }, salesperson: { name: "Bob" }, today: THU24 });
assertTrue("greets by first name only", draft.html.includes("Hi Dave,"));
assertFalse("does not use the surname", draft.html.includes("Hi Dave Smith"));
// A COMPANY name must never be shortened into a first name.
const companyOnly = buildSalesReply({ intent:"eta", order:{ id:1, customerName:"Airedale Boxing Club", contactName:"" }, salesperson:{name:"Bob"} });
assertFalse("does not greet a company as a person", companyOnly.html.includes("Hi Airedale"));
assertTrue("falls back to a neutral greeting", companyOnly.html.includes("Hello,"));
// A person on a company order is still greeted properly.
const both = buildSalesReply({ intent:"eta", order:{ id:1, customerName:"Airedale Boxing Club", contactName:"Jack Heaps" }, salesperson:{name:"Bob"} });
assertTrue("greets the person, not the company", both.html.includes("Hi Jack,"));
assertTrue("subject carries the order NUMBER", draft.subject.includes("489373"));
// The internal reference must never reach the customer.
const internalRef = buildSalesReply({ intent: "eta", order: { id: 489256, reference: "#68037/130926/12 - AIREDALE BOXING", customerName: "Jack Heaps" }, salesperson: { name: "Bob" } });
assertEq("subject uses the number, not the internal reference", internalRef.subject, "Update on your order - order 489256");
assertFalse("internal reference is not in the subject", internalRef.subject.includes("AIREDALE"));
assertFalse("nor in the body", internalRef.html.includes("AIREDALE"));
assertTrue("body quotes the number", internalRef.html.includes("489256"));
assertTrue("draft proposes the PO date", draft.proposedDates.length > 0);
assertTrue("signature attached", draft.html.length > 400);

const delayed = buildSalesReply({
  intent: "delay", order, po: null, salesperson: { name: "Bob" },
  blockedLines: [{ sku: "X", name: "Fristads trousers" }],
});
assertTrue("blocked-line delay offers a way out", /alternative|refund/i.test(delayed.text));
assertEq("blocked-line delay promises NO date", delayed.proposedDates, []);

// The person SIGNED IN signs the reply, not whoever raised the order weeks ago.
const signed = buildSalesReply({ intent:"eta", order, salesperson:{ name:"Daniel Ford" }, signedBy:"Nicky Everall" });
assertTrue("signed by whoever is logged in", signed.html.includes("Nicky Everall"));
assertFalse("not by the order owner", signed.html.includes("Daniel Ford"));
// With nobody signed in, fall back to the order owner rather than signing blank.
const unsigned = buildSalesReply({ intent:"eta", order, salesperson:{ name:"Daniel Ford" } });
assertTrue("falls back to the order owner", unsigned.html.includes("Daniel Ford"));
const blankish = buildSalesReply({ intent:"eta", order, salesperson:{ name:"Daniel Ford" }, signedBy:"   " });
assertTrue("whitespace is not a signature", blankish.html.includes("Daniel Ford"));

const proof = buildSalesReply({ intent: "proof_approval", order, salesperson: { name: "Bob" } });
assertTrue("proof mail asks for approval", /approval|approve/i.test(proof.text));
assertEq("proof mail promises no date", proof.proposedDates, []);

// HTML escaping - a customer called "Smith & Sons <Ltd>" must not break the mail.
const nasty = buildSalesReply({ intent: "eta", order: { id: "1<script>", contactName: "A&B Smith" }, salesperson: { name: "Bob" } });
assertFalse("escapes the reference", nasty.html.includes("<script>"));
assertTrue("escapes the ampersand in a name", nasty.html.includes("A&amp;B"));

// --- the note written back --------------------------------------------------
const note = buildSalesNote({
  intent: "eta", to: "dave@x.com", subject: "Update on your order - order 489373",
  sentBy: "Bob", proposedDates: ["2026-09-28/mid"], duplicationLevel: "warn",
  body: "Hi Dave,\n\nWe're just waiting on the Mascot items to arrive, and it should be with you mid next week.",
});
assertTrue("note records the recipient", note.includes("dave@x.com"));
assertTrue("note records the window given", note.includes("mid week commencing 28 September"));
assertTrue("note carries the whole email", note.includes("should be with you mid next week"));
assertTrue("note records that a warning was overridden", note.includes("warn"));

// --- Dec's scenario, 24 Sep 2026 -------------------------------------------
// Jack noted: "customer emailed re delivery - advised mid next week". The reply
// must keep to that window, worded for the day it is sent, with no dates in it.
const jack = { addedOn: "2026-09-24T11:27:00+01:00", addedBy: "Jack Ellis-Haynes", text: "customer emailed re delivery - advised mid next week" };
const jackWin = windowsMentioned(jack.text, jack.addedOn);
assertEq("reads 'mid next week' from the note", jackWin.map((w) => windowKey(w.window)), ["2026-09-28/mid"]);
assertEq("phrases the same window on the Thursday", phraseWindow(jackWin[0].window, THU24), "mid next week");
assertEq("...and as 'midweek' the following Monday", phraseWindow(jackWin[0].window, MON28), "midweek");
const autoPo = { addedOn: "2026-09-24T12:00:00+01:00", addedBy: "", text: "Auto-PO for MASCOT.\nOrder demand from: SO#1 due 25/09" };
const promised = promisedWindow([autoPo, jack]);
assertTrue("promised window comes from Jack, not the Auto-PO", promised && promised.addedBy === "Jack Ellis-Haynes");
const mascotPo = { supplier: "MASCOT", expectedDate: "2026-09-25" };   // Fri + 2 working days = Tue 29th
const keep = buildSalesReply({ intent: "eta", order, po: mascotPo, salesperson: { name: "Bob" }, today: THU24, promised });
assertTrue("keeps Jack's window, not the sooner PO one", keep.text.includes("mid next week"));
assertTrue("names what we are waiting on", keep.text.includes("Mascot"));
assertEq("source is the note", keep.eta.source, "note");
assertFalse("no dates in the email", /\b\d{1,2}\/\d{1,2}\b|\b\d{1,2}(st|nd|rd|th)?\s+(september|october)/i.test(keep.text));
// PO slips LATER than Jack's promise: follow the PO, and the guard says so.
const slip = buildSalesReply({ intent: "eta", order, po: { supplier: "MASCOT", expectedDate: "2026-10-05" }, salesperson: { name: "Bob" }, today: THU24, promised });
assertEq("slipped PO wins", slip.eta.source, "po-slipped");
assertTrue("worded from the PO", slip.text.includes("the week after next"));
const slipCheck = assessDuplication({ notes: [jack], emailDate: "2026-09-23T09:00:00Z", proposedDates: slip.proposedDates, slippedFrom: slip.eta.slippedFrom });
assertTrue("guard flags the changed story", slipCheck.reasons.some((r) => r.kind === "window_slipped"));
// Same window worded differently by a later note is agreement, not a clash.
const agree = assessDuplication({ notes: [jack], emailDate: "2026-09-23T09:00:00Z", proposedDates: ["2026-09-28/mid"] });
assertEq("same window -> warn, not blocked", agree.level, "warn");
const clash2 = assessDuplication({ notes: [jack], emailDate: "2026-09-23T09:00:00Z", proposedDates: ["2026-10-05/mid"] });
assertEq("different week -> blocked", clash2.level, "blocked");
// Other phrasings the team uses (note written Thursday 24 Sep).
const w = (t, on = "2026-09-24T10:00:00+01:00") => windowsMentioned(t, on).map((x) => windowKey(x.window));
assertEq("early next week", w("told him early next week"), ["2026-09-28/early"]);
assertEq("late this week", w("advised late this week"), ["2026-09-21/late"]);
assertEq("end of the week", w("should be end of the week"), ["2026-09-21/late"]);
assertEq("midweek on a Thursday means next week", w("advised midweek"), ["2026-09-28/mid"]);
assertEq("week after next", w("said week after next"), ["2026-10-05/week"]);
assertEq("a weekday", w("told her by Tuesday"), ["2026-09-28/early"]);
// The note text: the email as the customer saw it, minus the signature.
const noteText = emailToNoteText(keep.html);
assertTrue("note text has the reply", noteText.includes("mid next week"));
assertFalse("note text has no tags", /<[a-z]/i.test(noteText));
assertTrue("note text drops the signature block", noteText.length < 1200);

console.log(`\n${pass} passed, ${fail} failed`);
process.exit(fail ? 1 : 0);
