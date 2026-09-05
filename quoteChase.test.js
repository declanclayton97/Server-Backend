// Tests for quoteChase.js.
// Run with:  node quoteChase.test.js

import {
  addWorkingDays,
  clampToSendWindow,
  dueAtForStage,
  decideAction,
  buildChaseEmail,
  buildResponseEmail,
  buildBpNote,
  QUOTE_CHASE_CONFIG,
} from "./quoteChase.js";

let pass = 0, fail = 0;
function assertEq(label, actual, expected) {
  const ok = JSON.stringify(actual) === JSON.stringify(expected);
  if (ok) pass++;
  else { fail++; console.error(`  ✗ ${label}\n      expected ${JSON.stringify(expected)}\n      got      ${JSON.stringify(actual)}`); }
}
function assertTrue(label, actual) { assertEq(label, !!actual, true); }

// Local time, so the weekday maths matches how the server sees it.
const at = (s) => new Date(s);
const show = (d) => `${["Sun","Mon","Tue","Wed","Thu","Fri","Sat"][d.getDay()]} ${d.getFullYear()}-${String(d.getMonth()+1).padStart(2,"0")}-${String(d.getDate()).padStart(2,"0")} ${String(d.getHours()).padStart(2,"0")}:${String(d.getMinutes()).padStart(2,"0")}`;

// ── addWorkingDays ─────────────────────────────────────────────
// 2026-09-04 is a Friday.
assertEq("Fri +1 working day → Mon", show(addWorkingDays(at("2026-09-04T16:00:00"), 1)), "Mon 2026-09-07 16:00");
assertEq("Fri +2 → Tue",             show(addWorkingDays(at("2026-09-04T16:00:00"), 2)), "Tue 2026-09-08 16:00");
assertEq("Fri +3 → Wed",             show(addWorkingDays(at("2026-09-04T16:00:00"), 3)), "Wed 2026-09-09 16:00");
assertEq("Mon +1 → Tue",             show(addWorkingDays(at("2026-09-07T10:00:00"), 1)), "Tue 2026-09-08 10:00");
assertEq("Sat +1 → Mon",             show(addWorkingDays(at("2026-09-05T10:00:00"), 1)), "Mon 2026-09-07 10:00");

// ── clampToSendWindow ──────────────────────────────────────────
assertEq("inside window unchanged",  show(clampToSendWindow(at("2026-09-07T11:30:00"))), "Mon 2026-09-07 11:30");
assertEq("before window → 09:00",    show(clampToSendWindow(at("2026-09-07T06:00:00"))), "Mon 2026-09-07 09:00");
assertEq("after window → next day",  show(clampToSendWindow(at("2026-09-07T19:00:00"))), "Tue 2026-09-08 09:00");
assertEq("Saturday → Monday 09:00",  show(clampToSendWindow(at("2026-09-05T11:00:00"))), "Mon 2026-09-07 09:00");
assertEq("Fri 19:00 → Mon 09:00",    show(clampToSendWindow(at("2026-09-04T19:00:00"))), "Mon 2026-09-07 09:00");

// ── dueAtForStage — the sequence a Friday-afternoon quote gets ──
const fri = "2026-09-04T16:00:00";
assertEq("Fri quote, chase 1", show(dueAtForStage(fri, 1)), "Mon 2026-09-07 16:00");
assertEq("Fri quote, chase 2", show(dueAtForStage(fri, 2)), "Tue 2026-09-08 16:00");
assertEq("Fri quote, chase 3", show(dueAtForStage(fri, 3)), "Wed 2026-09-09 16:00");
assertEq("Fri quote, handover", show(dueAtForStage(fri, 4)), "Thu 2026-09-10 16:00");

// A late-evening quote is chased at the next morning's opening, not at 20:00.
assertEq("Tue 20:00 quote, chase 1", show(dueAtForStage("2026-09-08T20:00:00", 1)), "Thu 2026-09-10 09:00");

// ── decideAction ───────────────────────────────────────────────
const base = { enteredStatusAt: fri, stage: 0, responded: false, seeded: false, customerEmail: "c@x.co", stillQuoteSent: true };

assertEq("not due yet",
  decideAction({ ...base }, at("2026-09-07T15:59:00")).action, "none");
assertEq("due → chase 1",
  decideAction({ ...base }, at("2026-09-07T16:00:00")), { action: "chase", stage: 1, dueAt: dueAtForStage(fri, 1) });
assertEq("after chase 1, chase 2 due",
  decideAction({ ...base, stage: 1 }, at("2026-09-08T16:00:00")).stage, 2);
assertEq("after chase 3 → handover",
  decideAction({ ...base, stage: 3 }, at("2026-09-10T16:00:00")).action, "handover");
assertEq("after handover → nothing left",
  decideAction({ ...base, stage: 4 }, at("2026-09-30T16:00:00")).action, "none");

// The guards that stop a chase going out.
assertEq("seeded backlog is never chased",
  decideAction({ ...base, seeded: true }, at("2026-12-01T10:00:00")).reason, "seeded backlog");
assertEq("a reply stops the sequence",
  decideAction({ ...base, responded: true }, at("2026-12-01T10:00:00")).reason, "customer responded");
assertEq("left Quote sent → stop",
  decideAction({ ...base, stillQuoteSent: false }, at("2026-12-01T10:00:00")).reason, "no longer quote sent");
assertEq("no email → nothing to send",
  decideAction({ ...base, customerEmail: "" }, at("2026-12-01T10:00:00")).reason, "no customer email");

// A weekend quote does not start its clock until the Monday.
assertEq("Sat quote, chase 1 lands Mon+1=Tue",
  show(dueAtForStage("2026-09-05T11:00:00", 1)), "Mon 2026-09-07 11:00");

// ── emails ─────────────────────────────────────────────────────
const quote = { orderId: 484347, customerName: "Sally Sanderson", companyName: "Eton Environmental Group Ltd",
                customerEmail: "sally@eton.co.uk", netValue: 62.96, reference: "PO-99", salespersonName: "Helen Jackson",
                enteredStatusAt: "2026-08-24T15:08:54" };
const url = "https://x.co/quote/tok?e=1";

const c1 = buildChaseEmail(quote, 1, url);
assertEq("chase 1 subject", c1.subject, "Did you get our quote? — SO484347");
assertTrue("chase 1 has all four actions",
  ["go_ahead", "cancel", "more_time", "call_back"].every((a) => c1.html.includes(`action=${a}`)));
assertTrue("chase 1 shows the value", c1.html.includes("£62.96"));
assertTrue("text part carries the links too", c1.text.includes("action=go_ahead"));
assertEq("chase 3 is worded as the last", buildChaseEmail(quote, 3, url).html.includes("last reminder"), true);

const r = buildResponseEmail(quote, { action: "cancel", reason: "too_expensive", note: "Got it £40 cheaper", stage: 2 });
assertEq("response subject names the action", r.subject, "Cancel the quote — SO484347 · Sally Sanderson");
assertTrue("response shows the reason", r.html.includes("Too expensive"));
assertTrue("response shows their note", r.html.includes("Got it £40 cheaper"));

// A customer note is attacker-controlled text landing in an internal email.
const evil = buildResponseEmail(quote, { action: "cancel", note: "<script>alert(1)</script>", stage: 1 });
assertEq("customer note is escaped", evil.html.includes("<script>"), false);
assertTrue("escaped form present", evil.html.includes("&lt;script&gt;"));

// ── Brightpearl notes ──────────────────────────────────────────
assertEq("chase note", buildBpNote("chase", { stage: 2, to: "c@x.co" }), "Quote chase 2 of 3 emailed to c@x.co");
assertEq("response note",
  buildBpNote("response", { action: "cancel", reason: "gone_elsewhere", note: "used Screwfix", stage: 3 }),
  'Quote chase: customer replied "Cancel the quote" — Gone elsewhere — note: used Screwfix (after 3 chases)');
assertEq("singular chase in note",
  buildBpNote("response", { action: "go_ahead", stage: 1 }),
  'Quote chase: customer replied "Go ahead with the quote" (after 1 chase)');

console.log(`\n${pass} passed, ${fail} failed`);
process.exit(fail ? 1 : 0);
