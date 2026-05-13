// Self-contained tests for the order-pipeline status mapper.
// Run with:   node orderPipelineMapper.test.js
// No test framework — just throws on failure and prints pass/fail counts.

import {
  STATES,
  customerStateForBpStatus,
  isInternalStatus,
  isProofFlowStatus,
  listStates,
} from "./orderPipelineMapper.js";

let pass = 0;
let fail = 0;
function assert(label, actual, expected) {
  const ok = actual === expected;
  if (ok) {
    pass++;
  } else {
    fail++;
    console.error(`  ✗ ${label}\n      expected ${JSON.stringify(expected)}\n      got      ${JSON.stringify(actual)}`);
  }
}

// ── Stock Ordered (4 BP statuses) ──────────────────────────────
assert("Stock needs ordering → stock_ordered",
  customerStateForBpStatus("Stock needs ordering"), STATES.STOCK_ORDERED);
assert("Sportswear - Stock Needs Ordering → stock_ordered",
  customerStateForBpStatus("Sportswear - Stock Needs Ordering"), STATES.STOCK_ORDERED);
assert("Ordered Stock Awaiting Delivery → stock_ordered",
  customerStateForBpStatus("Ordered Stock Awaiting Delivery"), STATES.STOCK_ORDERED);
assert("Sportswear - Ordered Stock → stock_ordered",
  customerStateForBpStatus("Sportswear - Ordered Stock"), STATES.STOCK_ORDERED);

// ── In Production (14 BP statuses, spot-check) ─────────────────
assert("In stock, pick/pack/ship → in_production",
  customerStateForBpStatus("In stock, pick/pack/ship"), STATES.IN_PRODUCTION);
assert("Embroidery Room ready → in_production",
  customerStateForBpStatus("Embroidery Room ready"), STATES.IN_PRODUCTION);
assert("AVIATION - TO PICK → in_production",
  customerStateForBpStatus("AVIATION - TO PICK"), STATES.IN_PRODUCTION);

// ── Back Order ─────────────────────────────────────────────────
assert("Back Order with Supplier → back_order",
  customerStateForBpStatus("Back Order with Supplier"), STATES.BACK_ORDER);

// ── Ready for Collection ───────────────────────────────────────
assert("Collection - Paid → ready_for_collection",
  customerStateForBpStatus("Collection - Paid"), STATES.READY_FOR_COLLECTION);
assert("Sportswear Collection Paid → ready_for_collection",
  customerStateForBpStatus("Sportswear Collection Paid"), STATES.READY_FOR_COLLECTION);

// ── Final-status disambiguation (the tricky one) ───────────────
assert("Invoiced + prev=in_production → shipped",
  customerStateForBpStatus("Invoiced and Completed", STATES.IN_PRODUCTION), STATES.SHIPPED);
assert("Invoiced + prev=ready_for_collection → collected",
  customerStateForBpStatus("Invoiced and Completed", STATES.READY_FOR_COLLECTION), STATES.COLLECTED);
assert("Invoiced + no prev → shipped (default for online)",
  customerStateForBpStatus("Invoiced and Completed", null), STATES.SHIPPED);
assert("Invoiced + prev=shipped → null (don't re-email)",
  customerStateForBpStatus("Invoiced and Completed", STATES.SHIPPED), null);
assert("Invoiced + prev=collected → null (don't re-email)",
  customerStateForBpStatus("Invoiced and Completed", STATES.COLLECTED), null);
assert("Complete awaiting Payment + prev=ready → collected",
  customerStateForBpStatus("Complete awaiting Payment", STATES.READY_FOR_COLLECTION), STATES.COLLECTED);

// ── Internal-only statuses (return null) ───────────────────────
assert("Cancelled → null", customerStateForBpStatus("Cancelled"), null);
assert("Quote sent → null", customerStateForBpStatus("Quote sent"), null);
assert("LOCAL DELIVERY REQUIRED → null", customerStateForBpStatus("LOCAL DELIVERY REQUIRED"), null);
assert("G&H → null (pass-through, internal)", customerStateForBpStatus("G&H"), null);

// ── Proof flow statuses (return null) ──────────────────────────
assert("Proof Required-Send to Customer → null",
  customerStateForBpStatus("Proof Required-Send to Customer"), null);
assert("Sportswear - Proof Sent → null",
  customerStateForBpStatus("Sportswear - Proof Sent"), null);

// ── Helper predicates ──────────────────────────────────────────
assert("isInternalStatus('Cancelled') → true",
  isInternalStatus("Cancelled"), true);
assert("isInternalStatus('In stock, pick/pack/ship') → false",
  isInternalStatus("In stock, pick/pack/ship"), false);
assert("isProofFlowStatus('Sportswear - Proof Sent') → true",
  isProofFlowStatus("Sportswear - Proof Sent"), true);
assert("isProofFlowStatus('Stock needs ordering') → false",
  isProofFlowStatus("Stock needs ordering"), false);

// ── Whitespace / casing tolerance ──────────────────────────────
assert("Extra whitespace tolerated",
  customerStateForBpStatus("  Stock   Needs   Ordering  "), STATES.STOCK_ORDERED);
assert("Casing tolerated",
  customerStateForBpStatus("STOCK NEEDS ORDERING"), STATES.STOCK_ORDERED);

// ── Unknown status returns null (no false-positives) ───────────
assert("Unknown BP status → null", customerStateForBpStatus("Some Made-up Status"), null);
assert("Empty string → null", customerStateForBpStatus(""), null);
assert("null → null", customerStateForBpStatus(null), null);

// ── listStates ─────────────────────────────────────────────────
const states = listStates();
assert("listStates returns 7 entries", states.length, 7);
assert("First state has both id and label", typeof states[0].id, "string");
assert("First state has a label", typeof states[0].label, "string");

console.log(`\n${pass} passed, ${fail} failed`);
process.exit(fail === 0 ? 0 : 1);
