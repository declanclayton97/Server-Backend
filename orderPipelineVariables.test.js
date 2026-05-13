// Tests for orderPipelineVariables.js.
// Run with:  node orderPipelineVariables.test.js

import {
  deriveVariables,
  parseTrackingNotes,
  buildTrackingUrl,
  firstName,
  formatOrderDate,
  pickCustomerName,
} from "./orderPipelineVariables.js";

let pass = 0, fail = 0;
function assertEq(label, actual, expected) {
  const ok = JSON.stringify(actual) === JSON.stringify(expected);
  if (ok) pass++; else {
    fail++;
    console.error(`  ✗ ${label}\n      expected ${JSON.stringify(expected)}\n      got      ${JSON.stringify(actual)}`);
  }
}

// ── firstName ──────────────────────────────────────────────────
assertEq("firstName 'John Smith'", firstName("John Smith"), "John");
assertEq("firstName trims", firstName("  Mary  Jane  Smith "), "Mary");
assertEq("firstName empty", firstName(""), "");
assertEq("firstName null", firstName(null), "");

// ── formatOrderDate ────────────────────────────────────────────
assertEq("ISO date 2026-05-13", formatOrderDate("2026-05-13T10:00:00Z"), "13 May 2026");
assertEq("Empty date", formatOrderDate(""), "");
assertEq("Garbage date", formatOrderDate("not a date"), "");

// ── pickCustomerName ───────────────────────────────────────────
assertEq("Prefers addressFullName over company",
  pickCustomerName({
    customer: { companyName: "ACME Ltd", addressFullName: "John Smith" },
  }),
  "John Smith");
assertEq("Falls back to first+last",
  pickCustomerName({
    customer: { addressFirstName: "Jane", addressLastName: "Doe" },
  }),
  "Jane Doe");
assertEq("Falls back to delivery if customer empty",
  pickCustomerName({
    customer: {},
    delivery: { addressFullName: "Site Foreman" },
  }),
  "Site Foreman");
assertEq("Returns empty if nothing", pickCustomerName({}), "");

// ── buildTrackingUrl (per carrier) ─────────────────────────────
assertEq("FedEx URL",
  buildTrackingUrl("FedEx", "871203703913"),
  "https://www.fedex.com/fedextrack/?trknbr=871203703913");
assertEq("Royal Mail URL",
  buildTrackingUrl("Royal Mail", "KJ099325504GB"),
  "https://www.royalmail.com/track-your-item#/tracking-results/KJ099325504GB");
assertEq("DPD Local URL routes to DPD",
  buildTrackingUrl("DPD Local", "6975500062"),
  "https://www.dpd.co.uk/apps/tracking/?reference=6975500062");
assertEq("Case-insensitive carrier match",
  buildTrackingUrl("FEDEX", "ABC"),
  "https://www.fedex.com/fedextrack/?trknbr=ABC");
assertEq("Unknown carrier → empty",
  buildTrackingUrl("OwlPost", "ABC"), "");

// ── parseTrackingNotes ─────────────────────────────────────────
let parsed = parseTrackingNotes([
  { text: "FedEx tracking reference received: 871203703913", addedOn: "2026-05-12T09:00:00Z" },
  { text: "Royal Mail tracking reference received: KJ099325504GB", addedOn: "2026-05-13T10:00:00Z" },
]);
assertEq("Parsed 2 tracking notes", parsed.length, 2);
assertEq("Sorted newest first", parsed[0].carrier, "Royal Mail");
assertEq("Tracking number captured", parsed[0].number, "KJ099325504GB");

parsed = parseTrackingNotes([
  { text: "DPD Local tracking reference received: 6975500062" },
]);
assertEq("DPD Local carrier captured", parsed[0].carrier, "DPD Local");

parsed = parseTrackingNotes([{ text: "Some unrelated note" }]);
assertEq("Non-tracking note ignored", parsed.length, 0);

parsed = parseTrackingNotes([
  { text: "FedEx tracking reference received: AAA\nDPD tracking reference received: BBB" },
]);
assertEq("Multi-line note returns both", parsed.length, 2);

parsed = parseTrackingNotes([]);
assertEq("Empty array → empty", parsed, []);

parsed = parseTrackingNotes(null);
assertEq("Null → empty", parsed, []);

// ── deriveVariables (full pipeline) ────────────────────────────
const bp = {
  id: 458947,
  customerRef: "TUF-12345",
  placedOn: "2026-05-13T08:30:00Z",
  parties: {
    customer: { addressFullName: "John Smith" },
  },
};
const notes = [
  { text: "FedEx tracking reference received: 871203703913", addedOn: "2026-05-13T12:00:00Z" },
];
const constants = {
  shopName: "Tuff Workwear",
  supportEmail: "info@tuffshop.co.uk",
  reviewUrl: "https://example.com/review",
  collectionAddress: "Test Yard, Test St",
};
const vars = deriveVariables(bp, { notes, constants });

assertEq("customerName", vars.customerName, "John Smith");
assertEq("customerFirstName", vars.customerFirstName, "John");
assertEq("orderNumber prefers customerRef", vars.orderNumber, "TUF-12345");
assertEq("orderId", vars.orderId, "458947");
assertEq("orderDate formatted", vars.orderDate, "13 May 2026");
assertEq("trackingNumber from notes", vars.trackingNumber, "871203703913");
assertEq("carrierName from notes", vars.carrierName, "FedEx");
assertEq("trackingUrl built", vars.trackingUrl,
  "https://www.fedex.com/fedextrack/?trknbr=871203703913");
assertEq("shopName from constants", vars.shopName, "Tuff Workwear");
assertEq("supportEmail from constants", vars.supportEmail, "info@tuffshop.co.uk");
assertEq("reviewUrl from constants", vars.reviewUrl, "https://example.com/review");
assertEq("collectionAddress from constants", vars.collectionAddress, "Test Yard, Test St");

// ── deriveVariables with no notes (in-production email use case) ─
const varsNoNotes = deriveVariables(bp, { constants });
assertEq("No notes → empty trackingNumber", varsNoNotes.trackingNumber, "");
assertEq("No notes → empty carrierName", varsNoNotes.carrierName, "");
assertEq("No notes → empty trackingUrl", varsNoNotes.trackingUrl, "");

// ── deriveVariables falls back to id when no customerRef ──────
const bp2 = { id: 12345, placedOn: "2026-05-01" };
const vars2 = deriveVariables(bp2, { constants });
assertEq("orderNumber fallback to #id", vars2.orderNumber, "#12345");

console.log(`\n${pass} passed, ${fail} failed`);
process.exit(fail === 0 ? 0 : 1);
