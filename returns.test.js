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
  orderEmail,
  maskEmail,
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
  assertEq("last day is 30 days after it arrived (sent + 2)", a.lastDay, "2026-10-12");
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
assertEq("33 days after despatch is too late",
  assessReturn(order(), { productMeta: meta, today: new Date("2026-10-13T12:00:00Z") }).code, "too-late");
assertTrue("the last day is still in time",
  assessReturn(order(), { productMeta: meta, today: new Date("2026-10-12T20:00:00Z") }).ok);
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
  assertFalse("a refund needs a reason",
    validateSelection(a, [{ rowId: "12", qty: 1, reason: "", outcome: "refund" }]).ok);
  assertFalse("an unknown reason is refused",
    validateSelection(a, [{ rowId: "12", qty: 1, reason: "<script>", outcome: "refund" }]).ok);
  assertFalse("an exchange needs a choice",
    validateSelection(a, [{ rowId: "12", qty: 1, outcome: "exchange", exchangeChoice: "" }]).ok);
  assertFalse("an unknown exchange choice is refused",
    validateSelection(a, [{ rowId: "12", qty: 1, outcome: "exchange", exchangeChoice: "Three sizes up" }]).ok);
  assertFalse("'Something else' needs saying what",
    validateSelection(a, [{ rowId: "12", qty: 1, outcome: "exchange", exchangeChoice: "Something else", exchangeFor: " " }]).ok);
  assertFalse("neither swap nor refund chosen",
    validateSelection(a, [{ rowId: "12", qty: 1 }]).ok);
  assertFalse("a carriage row cannot be picked",
    validateSelection(a, [{ rowId: "13", qty: 1, reason: "Other", outcome: "refund" }]).ok);
  assertFalse("nothing chosen",
    validateSelection(a, [{ rowId: "12", qty: 0, reason: "Other", outcome: "refund" }]).ok);
  const ok = validateSelection(a, [
    { rowId: "11", qty: 1, outcome: "exchange", exchangeChoice: "One size up", exchangeFor: "ignored" },
    { rowId: "12", qty: 1, outcome: "refund", reason: "Changed my mind", exchangeChoice: "Two sizes up" },
  ]);
  assertTrue("a valid pick passes", ok.ok);
  assertEq("exchange choice kept only on the exchange", ok.lines.map((l) => l.exchangeChoice), ["One size up", ""]);
  assertEq("free text only kept for 'Something else'", ok.lines.map((l) => l.exchangeFor), ["", ""]);
  assertEq("an exchange's reason says what it is", ok.lines[0].reason, "Exchange: one size up");
  const other = validateSelection(a, [{ rowId: "12", qty: 1, outcome: "exchange", exchangeChoice: "Something else", exchangeFor: "Black instead" }]);
  assertEq("'Something else' keeps their words", other.lines[0].exchangeFor, "Black instead");
}

// --- the email on the order, shown masked -------------------------------------
assertEq("order email found on the customer", orderEmail({ parties: { customer: { email: "jo.bloggs@gmail.com" } } }), "jo.bloggs@gmail.com");
assertEq("falls back to the billing email", orderEmail({ parties: { customer: { email: "" }, billing: { email: "a@b.co" } } }), "a@b.co");
assertEq("no email on the order", orderEmail({ parties: { customer: {} } }), null);
assertEq("masked for the page", maskEmail("jo.bloggs@gmail.com"), "jo•••@gmail.com");
assertEq("a one-letter name still masks", maskEmail("j@x.com"), "j•••@x.com");

// --- what the customer and the order note say --------------------------------
{
  const lines = [
    { name: "Snickers <6241>", sku: "624", qty: 1, reason: "Faulty or damaged", outcome: "refund", exchangeChoice: "", exchangeFor: "" },
    { name: "Boots", sku: "B1", qty: 1, reason: "Exchange: one size up", outcome: "exchange", exchangeChoice: "One size up", exchangeFor: "" },
  ];
  const html = returnEmailHtml({ ref: "WR29092601", orderRef: "000124559", name: "Jo", lines, lastDay: "2026-10-12", address: ["Customer Returns", "LS26 8LG"] });
  assertTrue("email carries the reference", html.includes("WR29092601"));
  assertTrue("email uses the agreed wording", html.includes("Please write this reference on your invoice and send it back"));
  assertTrue("item names are escaped", html.includes("Snickers &lt;6241&gt;"));
  assertTrue("faulty items are asked to ring first", html.includes("Give us a call on 0113 288 7713 before you send it back"));
  assertTrue("the send-by date is in words", html.includes("Monday 12 October"));
  assertTrue("exchanges go out free", html.includes("free standard delivery"));
  assertTrue("the exchange says what they want", html.includes("Exchange &ndash; One size up"));
  const note = returnNoteText({ ref: "WR29092601", email: "jo@x.com", lines, comments: "" });
  assertTrue("note starts with the reference", note.startsWith("RETURN REQUESTED ONLINE — WR29092601"));
  assertTrue("note spells out the exchange", note.includes("EXCHANGE: One size up"));
}

assertEq("despatchDate takes the earliest invoice",
  despatchDate({ invoices: [{ taxDate: "2026-09-12T00:00:00+01:00" }, { taxDate: "2026-09-05T00:00:00+01:00" }] }), "2026-09-05");

console.log(`\n${pass} passed, ${fail} failed`);
if (fail) process.exit(1);
