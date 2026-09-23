// The Portwest verify abort must distinguish "cart is empty" from "cart could not be read".
// 2026-09-23: the run aborted saying the cart was empty while it held 21 lines / 74 units.
import assert from 'node:assert/strict';

// Mirrors the branch in placePortwestOrder.
function abortMessage({ parsedLines, cartUnits, counter, poId }) {
  if (parsedLines && cartUnits !== 0) return null;                    // no abort
  const unreadable = Number(counter.totalQty) > 0 || Number(counter.cartUnits) > 0;
  return unreadable
    ? `Portwest cart could NOT BE READ — no lines parsed, but Portwest's own counter says ${counter.totalQty ?? counter.cartUnits} unit(s) are in the cart. The goods are probably sitting in the basket: check the portal before re-running, and do not assume nothing was uploaded. PO#${poId} left for review.`
    : `Portwest cart is empty after upload (their counter agrees: ${JSON.stringify(counter)}) — aborting. PO#${poId} left for review.`;
}

// The real failure: nothing parsed, but their counter reported 74 units.
const unreadable = abortMessage({ parsedLines: 0, cartUnits: 0, counter: { totalQty: 74, cartUnits: 74 }, poId: 491416 });
assert.match(unreadable, /could NOT BE READ/);
assert.match(unreadable, /74 unit\(s\) are in the cart/);
assert.match(unreadable, /do not assume nothing was uploaded/);
assert.doesNotMatch(unreadable, /is empty after upload/);

// A genuinely empty cart — both signals agree — keeps the old, correct wording.
const empty = abortMessage({ parsedLines: 0, cartUnits: 0, counter: { totalQty: 0, cartUnits: 0 }, poId: 491416 });
assert.match(empty, /is empty after upload/);
assert.match(empty, /their counter agrees/);
assert.doesNotMatch(empty, /could NOT BE READ/);

// A null counter (the read itself failed) must NOT be read as "goods are there".
const nullCounter = abortMessage({ parsedLines: 0, cartUnits: 0, counter: { totalQty: null, cartUnits: null }, poId: 491416 });
assert.match(nullCounter, /is empty after upload/);

// A healthy cart does not abort at all.
assert.equal(abortMessage({ parsedLines: 3, cartUnits: 4, counter: { totalQty: 4, cartUnits: 4 }, poId: 491416 }), null);

console.log('portwestVerify: all assertions passed');
