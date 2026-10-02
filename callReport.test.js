// Tests for callReport.js.
// Run with:  node callReport.test.js

import { ukMidnight, addDays, previousWeekStart, fmtDuration, tallyCalls, personEmailHtml, managerEmailHtml } from "./callReport.js";

let pass = 0, fail = 0;
const eq = (name, got, want) => {
  const ok = JSON.stringify(got) === JSON.stringify(want);
  ok ? pass++ : fail++;
  if (!ok) console.log("FAIL", name, "\n  got ", JSON.stringify(got), "\n  want", JSON.stringify(want));
};

// UK midnight in BST is 23:00 UTC the day before; in GMT it is 00:00 UTC.
eq("bst midnight", ukMidnight("2026-10-01").toISOString(), "2026-09-30T23:00:00.000Z");
eq("gmt midnight", ukMidnight("2026-11-02").toISOString(), "2026-11-02T00:00:00.000Z");
eq("clocks-back day", ukMidnight("2026-10-25").toISOString(), "2026-10-24T23:00:00.000Z");
eq("addDays month", addDays("2026-09-29", 3), "2026-10-02");

// Monday report covers the Monday-Sunday before it, whatever day it runs.
eq("prev week from Mon", previousWeekStart(new Date("2026-10-05T07:30:00Z")), "2026-09-28");
eq("prev week from Fri", previousWeekStart(new Date("2026-10-02T10:00:00Z")), "2026-09-21");
eq("prev week from Sun", previousWeekStart(new Date("2026-10-04T12:00:00Z")), "2026-09-21");

eq("dur min", fmtDuration(125), "2m");
eq("dur hours", fmtDuration(3 * 3600 + 7 * 60), "3h 07m");

// As Webex records it: a customer rings the main number (SIP_INBOUND leg on "Holiday Check"),
// the auto attendant passes it to the hunt group, which rings each sales phone with an
// SIP_ENTERPRISE leg. Same correlation id throughout.
const rows = [
  { day: "2026-09-28", user_name: "Holiday Check", direction: "TERMINATING", answered: true, duration: 300, call_type: "SIP_INBOUND", correlation_id: "c1" },
  { day: "2026-09-28", user_name: "Jack Sales", direction: "TERMINATING", answered: true, duration: 100, call_type: "SIP_ENTERPRISE", correlation_id: "c1" },
  { day: "2026-09-28", user_name: "Helen Sales", direction: "TERMINATING", answered: false, duration: 0, call_type: "SIP_ENTERPRISE", correlation_id: "c1" },   // rang, Jack got it
  // Linked only through the interaction id (a transfer gets a new correlation id).
  { day: "2026-09-28", user_name: "Main AA", direction: "TERMINATING", answered: true, duration: 5, call_type: "SIP_INBOUND", correlation_id: "c2", interaction_id: "i2" },
  { day: "2026-09-28", user_uuid: "U1", direction: "TERMINATING", answered: true, duration: 40, call_type: "SIP_ENTERPRISE", correlation_id: "c3", interaction_id: "i2" },
  // Jack dials out: once answered, once not.
  { day: "2026-09-28", user_name: "Jack Sales", direction: "ORIGINATING", answered: true, duration: 50, call_type: "SIP_NATIONAL", correlation_id: "c4" },
  { day: "2026-09-28", user_name: "Jack Sales", direction: "ORIGINATING", answered: false, duration: 0, call_type: "SIP_MOBILE", correlation_id: "c5" },
  // Jack rings Helen: internal on both legs, counts for nobody.
  { day: "2026-09-28", user_name: "Jack Sales", direction: "ORIGINATING", answered: true, duration: 999, call_type: "SIP_ENTERPRISE", correlation_id: "c6" },
  { day: "2026-09-28", user_name: "Helen Sales", direction: "TERMINATING", answered: true, duration: 999, call_type: "SIP_ENTERPRISE", correlation_id: "c6" },
  // Someone not on the list.
  { day: "2026-09-29", user_name: "Abigail Sales", direction: "TERMINATING", answered: true, duration: 30, call_type: "SIP_ENTERPRISE", correlation_id: "c1" },
];
eq("tally", tallyCalls(rows, { u1: "jack" }, { "jack sales": "jack", "helen sales": "helen" }), {
  jack: { "2026-09-28": { callsIn: 2, callsOut: 2, talk: 190, orders: 0 } },
  helen: { "2026-09-28": { callsIn: 0, callsOut: 0, talk: 0, orders: 0 } },
});

const day = (d, x = {}) => ({ day: d, callsIn: 0, callsOut: 0, talk: 0, orders: 0, noCallData: false, ...x });
const r = {
  week: "2026-09-28", weekEnd: "2026-10-04", missingCallDays: [],
  people: [{ key: "jack", name: "Jack Ellis-Haynes", first: "Jack", webexMatched: true,
    days: [day("2026-09-28", { callsIn: 3, talk: 600, orders: 2 }), day("2026-09-29"), day("2026-09-30"), day("2026-10-01"), day("2026-10-02"), day("2026-10-03"), day("2026-10-04", { orders: 1 })],
    total: { callsIn: 3, callsOut: 0, talk: 600, orders: 3 } }],
};
const html = personEmailHtml(r, r.people[0]);
eq("person greets by first name", html.includes("Hi Jack,"), true);
eq("weekend shown only when used", [html.includes("Sat 3 Oct"), html.includes("Sun 4 Oct")], [false, true]);
eq("week total", html.includes("10m"), true);
eq("manager lists person", managerEmailHtml(r).includes("Jack Ellis-Haynes"), true);

console.log(`${pass} passed, ${fail} failed`);
if (fail) process.exit(1);
