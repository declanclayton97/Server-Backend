// Tests for returns.js.
// Run with:  node returns.test.js

import {
  assessReturn,
  validateSelection,
  postcodeMatches,
  despatchDate,
  normPostcode,
  returnEmailHtml,
  returnNoteText,
} from "./returns.js";

let pass = 0, fail = 0;
function assertEq(label, actual, expected) {
  const ok = JSON.stringify(actual) === JSON.stringify(expected);
  if (ok) pass++;
  else { fail++; console.error(`  x ${label}\n      expected ${JSON.stringify(expected)}\n      got      ${JSON.stringify(actual)}`); }
}
const assertTrue = (label, actual) => assertEq(label, !!actual, true);
const assertFalse = (label, actual) => assertEq(label, !!actual, false);

// A web order: two garments, carriage, and an instruction row.
const meta = {
  100: { stockTracked: true, brandId: 99 },
  101: { stockTracked: true, brandId: 99 },
  1001: { stockTracked: false, brandId: 74 },
  1000: { stockTracked: false, brandId: 74 },
  316552: { stockTracked: false, brandId: 74 },
};
const order = (over = {}) => ({
  id: 492331, orderTypeCode: "SO", reference: "000124559",
  parties: { delivery: { postalCode: "LS26 8LG" }, billing: { postalCode: "WF1 2AB" }, customer: { addressFullName: "Jo Bloggs" } },
  invoices: [{ taxDate: "2026-09-10T00:00:00.000+01:00" }],
  orderRows: {
    11: { productId: 100, productName: "Snickers 6241 Trousers 34R", productSku: "62410404034", quantity: { magnitude: "2.0000" } },
    12: { productId: 101, productName: "Snickers 2018 Sweatshirt L", productSku: "20180400006", quantity: { magnitude: "1.0000" } },
    13: { productId: 1001, productName: "Shipping: Delivery - Mainland UK", quantity: { magnitude: "1.0000" } },
    14: { productId: 1000, productName: "+++ PLEASE PUT TO PROOF +++", quantity: { magnitude: "1.0000" } },
  },
  ...over,
});
const today = new Date("2026-09-29T12:00:00Z");

// --- postcode ---------------------------------------------------------------
assertEq("postcode normalised", normPostcode(" ls26 8lg "), "LS268LG");
assertTrue("delivery postcode matches", postcodeMatches(order(), "ls268lg"));
assertTrue("billing postcode matches too", postcodeMatches(order(), "WF1 2AB"));
assertFalse("wrong postcode refused", postcodeMatches(order(), "LS1 1AA"));
assertFalse("a fragment is not a postcode", postcodeMatches(order(), "LS2"));

// --- the happy path ---------------------------------------------------------
{
  const a = assessReturn(order(), { productMeta: meta, channelName: "Magento TuffShop.co.uk", today });
  assertTrue("plain web order is returnable", a.ok);
  assertEq("only the goods are offered, not carriage or notes", a.lines.map((l) => l.rowId), ["11", "12"]);
  assertEq("quantities come through as whole numbers", a.lines.map((l) => l.qty), [2, 1]);
  assertEq("despatch = invoice tax date (UK day)", a.despatchedOn, "2026-09-10");
  assertEq("last day is 30 days after it was sent", a.lastDay, "2026-10-10");
}

// --- the policy -------------------------------------------------------------
assertEq("eBay goes back to eBay",
  assessReturn(order(), { productMeta: meta, channelName: "eBay: Tuffworkwearltd", today }).code, "marketplace");
assertEq("Amazon goes back to Amazon",
  assessReturn(order(), { productMeta: meta, channelName: "Amazon: UK", today }).code, "marketplace");
{
  const logo = order();
  logo.orderRows[15] = { productId: 316552, productName: "Print Right Arm", quantity: { magnitude: "1.0000" } };
  assertEq("any decoration row makes the whole order personalised",
    assessReturn(logo, { productMeta: meta, channelName: "Magento TuffShop.co.uk", today }).code, "personalised");
}
assertEq("not invoiced = not sent yet",
  assessReturn(order({ invoices: [] }), { productMeta: meta, today }).code, "not-sent");
assertEq("31 days after despatch is too late",
  assessReturn(order(), { productMeta: meta, today: new Date("2026-10-11T12:00:00Z") }).code, "too-late");
assertTrue("day 30 is still in time",
  assessReturn(order(), { productMeta: meta, today: new Date("2026-10-10T20:00:00Z") }).ok);
assertEq("a credit note is not an order",
  assessReturn(order({ orderTypeCode: "SC" }), { productMeta: meta, today }).code, "not-found");

// --- a line cannot be returned twice ----------------------------------------
{
  const a = assessReturn(order(), { productMeta: meta, today, requested: { 11: 1 } });
  assertEq("one of two trousers already on a return", a.lines.find((l) => l.rowId === "11").available, 1);
  const b = assessReturn(order(), { productMeta: meta, today, requested: { 11: 2, 12: 1 } });
  assertEq("everything already requested", b.code, "already");
}

// --- selection is re-checked on the server -----------------------------------
{
  const a = assessReturn(order(), { productMeta: meta, today, requested: { 11: 1 } });
  assertFalse("more than is left is refused",
    validateSelection(a, [{ rowId: "11", qty: 2, reason: "Too small", outcome: "refund" }]).ok);
  assertFalse("a reason is required",
    validateSelection(a, [{ rowId: "12", qty: 1, reason: "", outcome: "refund" }]).ok);
  assertFalse("an unknown reason is refused",
    validateSelection(a, [{ rowId: "12", qty: 1, reason: "<script>", outcome: "refund" }]).ok);
  assertFalse("an exchange needs to say what for",
    validateSelection(a, [{ rowId: "12", qty: 1, reason: "Too big", outcome: "exchange", exchangeFor: " " }]).ok);
  assertFalse("a carriage row cannot be picked",
    validateSelection(a, [{ rowId: "13", qty: 1, reason: "Other", outcome: "refund" }]).ok);
  assertFalse("nothing chosen",
    validateSelection(a, [{ rowId: "12", qty: 0, reason: "Other", outcome: "refund" }]).ok);
  const ok = validateSelection(a, [
    { rowId: "11", qty: 1, reason: "Too small", outcome: "exchange", exchangeFor: "36R" },
    { rowId: "12", qty: 1, reason: "Changed my mind", outcome: "refund", exchangeFor: "ignored" },
  ]);
  assertTrue("a valid pick passes", ok.ok);
  assertEq("exchange text kept only on exchanges", ok.lines.map((l) => l.exchangeFor), ["36R", ""]);
}

// --- what the customer and the order note say --------------------------------
{
  const lines = [{ name: "Snickers <6241>", sku: "624", qty: 1, reason: "Faulty or damaged", outcome: "refund", exchangeFor: "" }];
  const html = returnEmailHtml({ ref: "WR29092601", orderRef: "000124559", name: "Jo", lines, lastDay: "2026-10-10", address: ["Tuff Workwear Ltd", "LS26 8LG"] });
  assertTrue("email carries the reference", html.includes("WR29092601"));
  assertTrue("email uses the agreed wording", html.includes("Please write this reference on your invoice and send it back"));
  assertTrue("item names are escaped", html.includes("Snickers &lt;6241&gt;"));
  assertTrue("faulty items get the postage line", html.includes("in touch about the postage"));
  assertTrue("the send-by date is in words", html.includes("10 October 2026"));
  const note = returnNoteText({ ref: "WR29092601", email: "jo@x.com", lines, comments: "" });
  assertTrue("note starts with the reference", note.startsWith("RETURN REQUESTED ONLINE — WR29092601"));
}

assertEq("despatchDate takes the earliest invoice",
  despatchDate({ invoices: [{ taxDate: "2026-09-12T00:00:00+01:00" }, { taxDate: "2026-09-05T00:00:00+01:00" }] }), "2026-09-05");

console.log(`\n${pass} passed, ${fail} failed`);
if (fail) process.exit(1);
