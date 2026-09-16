// Tests for salesHub.js.
// Run with:  node salesHub.test.js

import {
  detectIntent,
  extractOrderNumber,
  extractSentDate,
  datesMentioned,
  dateKeys,
  notesSince,
  classifyNote,
  assessDuplication,
  etaSentence,
  buildSalesReply,
  buildSalesNote,
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
// A phone number must not beat a tagged order number.
assertEq("tagged beats phone", extractOrderNumber("call me on 01924 123456 re order 489373"), "489373");

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
const repeat = assessDuplication({ notes, emailDate: emailMon, proposedDates: dateKeys("Thursday, 25 September") });
assertEq("same date already given -> warn", repeat.level, "warn");
assertTrue("says they already know", repeat.reasons.some((r) => r.kind === "already_told"));

// Worse: we are about to give a DIFFERENT date to the one promised.
const clash = assessDuplication({ notes, emailDate: emailMon, proposedDates: dateKeys("Friday, 2 October") });
assertEq("different date -> blocked", clash.level, "blocked");
assertTrue("names the conflict", clash.reasons.some((r) => r.kind === "date_conflict"));
assertTrue("quotes the colleague", clash.reasons[0].text.includes("Sarah"));

// Nothing since the email at all.
assertEq("quiet order -> ok", assessDuplication({ notes: [notes[0]], emailDate: emailMon, proposedDates: dateKeys("25 September") }).level, "ok");

// Contact since, but no date in it - still worth a look.
const vague = assessDuplication({
  notes: [{ addedOn: "2026-09-16T10:00:00Z", text: "Customer called, left message", addedBy: "Tom" }],
  emailDate: emailMon, proposedDates: [],
});
assertEq("contact with no date -> warn", vague.level, "warn");
assertTrue("tells them to read it", vague.reasons.some((r) => r.kind === "contacted_since"));

// Never promise a date on a line the supplier refused.
const blocked = assessDuplication({
  notes: [], emailDate: emailMon, proposedDates: dateKeys("25 September"),
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
// A supplier due date on a WEEKEND must not become a promised weekend delivery.
// 19 Sept 2026 is a Saturday; the copy should say the Monday.
const weekend = etaSentence({ supplier: "CHADWICK", expectedDate: "2026-09-19" });
assertFalse("never promises a Saturday", /Saturday/.test(weekend.text));
assertTrue("rolls forward to the Monday", /Monday 21 September/.test(weekend.text));
assertEq("and proposes the rolled date", weekend.dates, ["09-21"]);
// A weekday date is left exactly as it is.
const weekday = etaSentence({ supplier: "CHADWICK", expectedDate: "2026-09-23" });
assertTrue("a Wednesday stays Wednesday", /Wednesday 23 September/.test(weekday.text));

const withDate = etaSentence({ supplier: "Blaklader", expectedDate: "2026-09-25" });
assertTrue("PO with a date names the supplier", withDate.text.includes("Blaklader"));
assertTrue("PO with a date proposes one", withDate.dates.length > 0);

// --- drafting ---------------------------------------------------------------
const order = { id: 489373, reference: "489373", customerName: "Dave Smith", contactName: "Dave Smith", lines: [] };
const draft = buildSalesReply({ intent: "eta", order, po: { supplier: "Blaklader", expectedDate: "2026-09-25" }, salesperson: { name: "Bob" } });
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
  sentBy: "Bob", proposedDates: ["25 September"], duplicationLevel: "warn",
});
assertTrue("note records the recipient", note.includes("dave@x.com"));
assertTrue("note records the date given", note.includes("25 September"));
assertTrue("note records that a warning was overridden", note.includes("warn"));

console.log(`\n${pass} passed, ${fail} failed`);
process.exit(fail ? 1 : 0);
