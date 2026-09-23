// Sterling new-portal guards. No network: the checkout form is the one captured in
// "Sterling Checkout New Portal.har" on 2026-09-23.
import assert from 'node:assert/strict';
import { sterlingShipsToUs, STERLING_OUR_POSTCODE } from './sterlingPortal.js';

assert.equal(STERLING_OUR_POSTCODE, 'LS26 8LG');
assert.equal(sterlingShipsToUs('LS26 8LG'), true);
assert.equal(sterlingShipsToUs('ls268lg'), true);
assert.equal(sterlingShipsToUs('LS26  8LG'), true);
// the saved-address picker's real entries — customers of ours, all must fail
assert.equal(sterlingShipsToUs('LS12 4BD'), false);
assert.equal(sterlingShipsToUs('IP14 3EF'), false);
assert.equal(sterlingShipsToUs(''), false);
assert.equal(sterlingShipsToUs(null), false);
assert.equal(sterlingShipsToUs(undefined), false);

// Brightpearl holds Sterling products with the EAN as the SKU, so a PO row maps straight to a
// barcode with no resolver. These are the six on the stuck POs 491127/491128.
const poRows = [
  { sku: '5055197909747', qty: 1 }, { sku: '5055160056461', qty: 1 }, { sku: '5055338425747', qty: 2 },
  { sku: '5055338425754', qty: 1 }, { sku: '5055160071419', qty: 1 }, { sku: '5055338427642', qty: 1 },
];
const items = poRows.map((r) => ({ barcode: String(r.sku), quantity: r.qty, isSale: false }));
assert.equal(items.length, 6);
assert.ok(items.every((i) => /^\d{13}$/.test(i.barcode)), 'every Sterling SKU is a 13-digit EAN');
assert.equal(items.reduce((a, i) => a + i.quantity, 0), 7);
assert.equal(JSON.stringify(items[0]), '{"barcode":"5055197909747","quantity":1,"isSale":false}');

console.log('sterlingPortal: all assertions passed');
