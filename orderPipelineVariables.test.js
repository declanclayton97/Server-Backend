// Tests for orderPipelineVariables.js.
// Run with:  node orderPipelineVariables.test.js

import {
  deriveVariables,
  parseTrackingNotes,
  buildTrackingUrl,
  firstName,
  formatOrderDate,
  pickCustomerName,
  smartTitleCase,
  isTitleToken,
  isOnlyTitles,
} from "./orderPipelineVariables.js";

let pass = 0, fail = 0;
function assertEq(label, actual, expected) {
  const ok = JSON.stringify(actual) === JSON.stringify(expected);
  if (ok) pass++; else {
    fail++;
    console.error(`  ✗ ${label}\n      expected ${JSON.stringify(expected)}\n      got      ${JSON.stringify(actual)}`);
  }
}

// ── smartTitleCase ─────────────────────────────────────────────
assertEq("ALL CAPS → title case",
  smartTitleCase("JOHN SMITH"), "John Smith");
assertEq("all lowercase → title case",
  smartTitleCase("john smith"), "John Smith");
assertEq("Mixed casing → title case",
  smartTitleCase("jOhN sMiTh"), "John Smith");
assertEq("Hyphenated name",
  smartTitleCase("ANNE-MARIE"), "Anne-Marie");
assertEq("Apostrophe name",
  smartTitleCase("O'BRIEN"), "O'Brien");
assertEq("Mc prefix",
  smartTitleCase("MCDONALD"), "McDonald");
assertEq("Mc prefix mid-name",
  smartTitleCase("john mcconnell"), "John McConnell");
assertEq("Accented chars preserved",
  smartTitleCase("MÜLLER"), "Müller");
assertEq("Multiple spaces collapsed",
  smartTitleCase("  john    smith  "), "John Smith");
assertEq("Empty input → empty string", smartTitleCase(""), "");
assertEq("Null input → empty string", smartTitleCase(null), "");

// ── isTitleToken / isOnlyTitles ────────────────────────────────
assertEq("'Mr' is a title", isTitleToken("Mr"), true);
assertEq("'MR.' is a title (punct stripped)", isTitleToken("MR."), true);
assertEq("'Dr' is a title", isTitleToken("Dr"), true);
assertEq("'John' is not a title", isTitleToken("John"), false);
assertEq("isOnlyTitles 'Mr'", isOnlyTitles("Mr"), true);
assertEq("isOnlyTitles 'Mr Dr'", isOnlyTitles("Mr Dr"), true);
assertEq("isOnlyTitles 'Mr Smith'", isOnlyTitles("Mr Smith"), false);
assertEq("isOnlyTitles ''", isOnlyTitles(""), false);

// ── firstName (greeting fallback + title handling) ─────────────
assertEq("firstName 'John Smith'", firstName("John Smith"), "John");
assertEq("firstName normalises case",
  firstName("JOHN SMITH"), "John");
assertEq("firstName 'jane doe' → 'Jane'",
  firstName("jane doe"), "Jane");
assertEq("firstName 'Mr John Smith' → 'John'",
  firstName("Mr John Smith"), "John");
assertEq("firstName 'MR JOHN SMITH' → 'John'",
  firstName("MR JOHN SMITH"), "John");
assertEq("firstName 'Mr Smith' → 'there' (surname-only is ambiguous)",
  firstName("Mr Smith"), "there");
assertEq("firstName 'Mr' alone → 'there'",
  firstName("Mr"), "there");
assertEq("firstName trims",
  firstName("  Mary  Jane  Smith "), "Mary");
assertEq("firstName empty → 'there'", firstName(""), "there");
assertEq("firstName null → 'there'", firstName(null), "there");
assertEq("firstName 'Anne-Marie Smith' → 'Anne-Marie'",
  firstName("Anne-Marie Smith"), "Anne-Marie");

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
assertEq("Normalises ALL CAPS full name",
  pickCustomerName({
    customer: { addressFullName: "JOHN SMITH" },
  }),
  "John Smith");
assertEq("Normalises all-lowercase",
  pickCustomerName({
    customer: { addressFullName: "john smith" },
  }),
  "John Smith");
assertEq("Falls back to first+last",
  pickCustomerName({
    customer: { addressFirstName: "Jane", addressLastName: "Doe" },
  }),
  "Jane Doe");
assertEq("Title-only full name → augment with addressLastName",
  pickCustomerName({
    customer: { addressFullName: "Mr", addressLastName: "Smith" },
  }),
  "Mr Smith");
assertEq("Title-only full name → augment with delivery lastName",
  pickCustomerName({
    customer: { addressFullName: "Mrs" },
    delivery: { addressLastName: "Jones" },
  }),
  "Mrs Jones");
assertEq("Title in fullName + name in first/last → 'Mr John Smith'",
  pickCustomerName({
    customer: { addressFullName: "MR", addressFirstName: "John", addressLastName: "Smith" },
  }),
  "Mr John Smith");
assertEq("Title typed into firstName box alongside name → 'Mr John Smith'",
  pickCustomerName({
    customer: { addressFirstName: "Mr John", addressLastName: "Smith" },
  }),
  "Mr John Smith");
assertEq("Title with trailing dot in firstName box",
  pickCustomerName({
    customer: { addressFirstName: "Mr.", addressLastName: "Smith" },
  }),
  "Mr Smith");
assertEq("Title + firstName combined in full name stays intact",
  pickCustomerName({
    customer: { addressFullName: "Mr John Smith" },
  }),
  "Mr John Smith");
assertEq("McDonald name title-cased",
  pickCustomerName({
    customer: { addressFullName: "JOHN MCDONALD" },
  }),
  "John McDonald");
assertEq("Falls back to delivery if customer empty",
  pickCustomerName({
    customer: {},
    delivery: { addressFullName: "Site Foreman" },
  }),
  "Site Foreman");
assertEq("Last resort: company name title-cased",
  pickCustomerName({
    customer: { companyName: "TUFF WORKWEAR LTD" },
  }),
  "Tuff Workwear Ltd");
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
  supportEmail: "sales@tuffshop.co.uk",
  reviewUrl: "https://example.com/review",
  googleReviewUrl: "https://example.com/google",
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
assertEq("supportEmail from constants", vars.supportEmail, "sales@tuffshop.co.uk");
assertEq("reviewUrl from constants", vars.reviewUrl, "https://example.com/review");
assertEq("googleReviewUrl from constants", vars.googleReviewUrl, "https://example.com/google");
assertEq("trackingButton present when trackingUrl is set",
  vars.trackingButton.includes('href="https://www.fedex.com/'), true);
assertEq("reviewButton uses Trustpilot URL",
  vars.reviewButton.includes('href="https://example.com/review"'), true);
assertEq("googleReviewButton uses Google URL",
  vars.googleReviewButton.includes('href="https://example.com/google"'), true);
assertEq("Button HTML uses table pattern (email-safe)",
  vars.trackingButton.startsWith("<table"), true);
assertEq("collectionAddress from constants", vars.collectionAddress, "Test Yard, Test St");
assertEq("signature is the branded HTML block",
  vars.signature.startsWith("<table"), true);

// ── deriveVariables with no notes (in-production email use case) ─
const varsNoNotes = deriveVariables(bp, { constants });
assertEq("No notes → empty trackingNumber", varsNoNotes.trackingNumber, "");
assertEq("No notes → empty carrierName", varsNoNotes.carrierName, "");
assertEq("No notes → empty trackingUrl", varsNoNotes.trackingUrl, "");
assertEq("No notes → empty trackingButton",
  varsNoNotes.trackingButton, "");

// ── deriveVariables falls back to id when no customerRef ──────
const bp2 = { id: 12345, placedOn: "2026-05-01" };
const vars2 = deriveVariables(bp2, { constants });
assertEq("orderNumber fallback to #id", vars2.orderNumber, "#12345");

console.log(`\n${pass} passed, ${fail} failed`);
process.exit(fail === 0 ? 0 : 1);
