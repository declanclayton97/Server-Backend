// The Sterling lane's line-building: PO rows → basket lines, keyed by barcode.
// Mirrors the logic in placeSterlingOrder so the rules are pinned without a live portal.
import assert from 'node:assert/strict';

const isBarcode = (sku) => /^\d{12,14}$/.test(String(sku || '').trim());
function buildLines(poLines, isNonOrderable = () => false) {
  const out = [], skipped = [], notBarcodes = [];
  for (const l of poLines) {
    if (isNonOrderable(l.sku)) { skipped.push(l.sku); continue; }
    const code = String(l.sku || '').trim();
    if (!isBarcode(code)) { notBarcodes.push(l.sku); continue; }
    const at = out.find((x) => x.barcode === code);
    if (at) at.qty += Math.round(l.qty);
    else out.push({ barcode: code, qty: Math.round(l.qty), name: l.name });
  }
  return { out, skipped, notBarcodes };
}

// The six real codes off POs 491127/491128 — all 13-digit EANs, straight through.
const real = buildLines([
  { sku: '5055160071419', qty: 1 }, { sku: '5055197909747', qty: 2 },
  { sku: '5055338425747', qty: 1 }, { sku: '5055338425754', qty: 1 },
  { sku: '5055338427642', qty: 1 },
]);
assert.equal(real.notBarcodes.length, 0);
assert.equal(real.out.length, 5);
assert.equal(real.out.reduce((a, l) => a + l.qty, 0), 6);

// Same variant on several PO rows (two sales orders wanting it) becomes ONE basket line with the
// quantities SUMMED — adding it twice would let the basket keep the last value and under-order.
const merged = buildLines([
  { sku: '5055338425747', qty: 2 }, { sku: '5055338425747', qty: 3 }, { sku: '5055160071419', qty: 1 },
]);
assert.equal(merged.out.length, 2);
assert.equal(merged.out.find((l) => l.barcode === '5055338425747').qty, 5);

// A SKU that is not a barcode must STOP the order rather than be guessed at — the old resolver's
// job was exactly this guess, and it ordered nothing rather than the wrong thing only by luck.
const legacy = buildLines([{ sku: '5055160071419', qty: 1 }, { sku: 'Apprentice Brown 6', qty: 1 }]);
assert.equal(legacy.notBarcodes.length, 1);
assert.equal(legacy.notBarcodes[0], 'Apprentice Brown 6');

// Non-orderable rows (carriage/notes) are skipped, not refused.
const withNote = buildLines(
  [{ sku: 'CARRIAGE', qty: 1 }, { sku: '5055160071419', qty: 1 }],
  (sku) => sku === 'CARRIAGE',
);
assert.equal(withNote.skipped.length, 1);
assert.equal(withNote.notBarcodes.length, 0);
assert.equal(withNote.out.length, 1);

// Basket verification: a short or missing line is caught by comparing against the basket read back.
const want = [{ barcode: '5055160071419', qty: 2 }, { barcode: '5055197909747', qty: 2 }];
const got = [{ barcode: '5055160071419', qty: 1 }];   // one short, one absent
const missing = want.filter((l) => {
  const b = got.find((x) => x.barcode === l.barcode);
  return !b || b.qty !== l.qty;
});
assert.equal(missing.length, 2);

console.log('sterlingBarcode: all assertions passed');
