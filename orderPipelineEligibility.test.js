// Tests for orderPipelineEligibility.js.
// Run with:  node orderPipelineEligibility.test.js

import {
  orderHasDecoration,
  workingDaysBetween,
  calendarDaysBetween,
  checkReviewEligibility,
} from "./orderPipelineEligibility.js";

let pass = 0, fail = 0;
function assertEq(label, actual, expected) {
  const ok = JSON.stringify(actual) === JSON.stringify(expected);
  if (ok) pass++;
  else { fail++; console.error(`  ✗ ${label}\n      expected ${JSON.stringify(expected)}\n      got      ${JSON.stringify(actual)}`); }
}

// ── orderHasDecoration ─────────────────────────────────────────
assertEq("Plain order → false",
  orderHasDecoration({ orderRows: { 1: { productSku: "TROUSER-32-BLK" } } }),
  false);
assertEq("OPEM- row → true",
  orderHasDecoration({ orderRows: { 1: { productSku: "TROUSER-32-BLK" }, 2: { productSku: "OPEM-LEFTBREAST" } } }),
  true);
assertEq("OPPR- row → true",
  orderHasDecoration({ orderRows: { 1: { productSku: "TSHIRT-L" }, 2: { productSku: "OPPR-FRONTBACK" } } }),
  true);
assertEq("orderRows as array → still detects",
  orderHasDecoration({ orderRows: [{ productSku: "OPEM-X" }] }),
  true);
assertEq("Empty / missing orderRows → false",
  orderHasDecoration({}), false);
assertEq("Null order → false", orderHasDecoration(null), false);

// ── workingDaysBetween (uses UTC) ──────────────────────────────
// 2026-05-11 is a Monday. Walk through a known week:
assertEq("Mon → Mon (same day) → 0",
  workingDaysBetween("2026-05-11T09:00:00Z", "2026-05-11T17:00:00Z"), 0);
assertEq("Mon → Tue → 1",
  workingDaysBetween("2026-05-11T09:00:00Z", "2026-05-12T09:00:00Z"), 1);
assertEq("Mon → Wed → 2",
  workingDaysBetween("2026-05-11T09:00:00Z", "2026-05-13T09:00:00Z"), 2);
assertEq("Fri → Mon → 1 (weekend skipped)",
  workingDaysBetween("2026-05-15T15:00:00Z", "2026-05-18T09:00:00Z"), 1);
assertEq("Sat → Mon → 0 (weekend-only gap)",
  workingDaysBetween("2026-05-16T11:00:00Z", "2026-05-18T09:00:00Z"), 0);
assertEq("Sat → Tue → 1",
  workingDaysBetween("2026-05-16T11:00:00Z", "2026-05-19T09:00:00Z"), 1);
assertEq("Sun → Mon → 0",
  workingDaysBetween("2026-05-17T11:00:00Z", "2026-05-18T09:00:00Z"), 0);
assertEq("Reversed dates → 0 (no negative counts)",
  workingDaysBetween("2026-05-15T09:00:00Z", "2026-05-11T09:00:00Z"), 0);
assertEq("Empty inputs → 0", workingDaysBetween("", ""), 0);

// ── calendarDaysBetween ────────────────────────────────────────
assertEq("Calendar days exact",
  calendarDaysBetween("2026-05-01T00:00:00Z", "2026-05-15T00:00:00Z"), 14);
assertEq("Calendar days partial",
  calendarDaysBetween("2026-05-01T00:00:00Z", "2026-05-01T12:00:00Z"), 0.5);
assertEq("Reversed → 0", calendarDaysBetween("2026-05-15", "2026-05-01"), 0);

// ── checkReviewEligibility ─────────────────────────────────────
// Plain orders — 1 working day limit
const plainOrder = (placedOn) => ({ placedOn, orderRows: { 1: { productSku: "TSHIRT-L" } } });

let r = checkReviewEligibility(
  plainOrder("2026-05-11T09:00:00Z"), "2026-05-11T17:00:00Z"
);
assertEq("Plain, same-day → eligible", r.eligible, true);

r = checkReviewEligibility(
  plainOrder("2026-05-11T09:00:00Z"), "2026-05-12T09:00:00Z"
);
assertEq("Plain, next day → eligible", r.eligible, true);

r = checkReviewEligibility(
  plainOrder("2026-05-15T15:00:00Z"), "2026-05-18T09:00:00Z"
);
assertEq("Plain, Fri→Mon (weekend gap) → eligible", r.eligible, true);

r = checkReviewEligibility(
  plainOrder("2026-05-16T11:00:00Z"), "2026-05-18T09:00:00Z"
);
assertEq("Plain, Sat→Mon → eligible (weekend doesn't count)", r.eligible, true);

r = checkReviewEligibility(
  plainOrder("2026-05-11T09:00:00Z"), "2026-05-13T09:00:00Z"
);
assertEq("Plain, 2 working days → NOT eligible", r.eligible, false);

r = checkReviewEligibility(
  plainOrder("2026-05-15T15:00:00Z"), "2026-05-19T09:00:00Z"
);
assertEq("Plain, Fri→Tue → NOT eligible (2 working days)", r.eligible, false);

// Logo orders — 14 calendar day limit
const logoOrder = (placedOn) => ({
  placedOn,
  orderRows: { 1: { productSku: "TSHIRT-L" }, 2: { productSku: "OPEM-LEFTBREAST" } },
});

r = checkReviewEligibility(logoOrder("2026-05-01T00:00:00Z"), "2026-05-08T00:00:00Z");
assertEq("Logo, 7 days → eligible", r.eligible, true);

r = checkReviewEligibility(logoOrder("2026-05-01T00:00:00Z"), "2026-05-15T00:00:00Z");
assertEq("Logo, exactly 14 days → eligible", r.eligible, true);

r = checkReviewEligibility(logoOrder("2026-05-01T00:00:00Z"), "2026-05-16T00:00:00Z");
assertEq("Logo, 15 days → NOT eligible", r.eligible, false);

r = checkReviewEligibility(logoOrder("2026-05-01T00:00:00Z"), "2026-05-22T00:00:00Z");
assertEq("Logo, 21 days → NOT eligible", r.eligible, false);

// Logo order shipped same day (rare but possible — stock embroidery)
r = checkReviewEligibility(logoOrder("2026-05-11T09:00:00Z"), "2026-05-11T17:00:00Z");
assertEq("Logo, same day → eligible", r.eligible, true);

// Edge: no placedOn → not eligible (defensive)
r = checkReviewEligibility({ orderRows: {} }, "2026-05-12");
assertEq("No placedOn → NOT eligible", r.eligible, false);

// Reason field is populated for both branches
r = checkReviewEligibility(plainOrder("2026-05-11T09:00:00Z"), "2026-05-15T09:00:00Z");
assertEq("Reason includes 'plain' for plain orders",
  r.reason.includes("plain"), true);
r = checkReviewEligibility(logoOrder("2026-05-01T00:00:00Z"), "2026-05-20T00:00:00Z");
assertEq("Reason includes 'logo' for logo orders",
  r.reason.includes("logo"), true);

console.log(`\n${pass} passed, ${fail} failed`);
process.exit(fail === 0 ? 0 : 1);
