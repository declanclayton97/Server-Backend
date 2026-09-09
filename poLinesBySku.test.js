// Unit test for the shared PO line merge used by Performance Brands and Castle. Supplier baskets do
// not reliably accumulate repeat adds of one variation, so the SAME SKU MUST GO OUT ONCE, summed.
import { mergePoLinesBySku } from './purchasingSchedule.js';

let pass = true;
const check = (name, cond) => { console.log((cond ? 'PASS  ' : 'FAIL  ') + name); if (!cond) pass = false; };
const find = (rows, sku) => rows.find((r) => r.sku === sku);

// The exact shape of PO 488064, which failed on 2026-09-09: PB271-BRN-06 twice at qty 1.
const po = {
  soLines: [
    { sku: 'PB334-BLK-09', qty: 1, cost: 30, name: 'Duran Trainer', productId: 111 },
    { sku: 'PB56C-BRN-11', qty: 1, cost: 43.05, name: 'Dealer Pro', productId: 112 },
    { sku: 'PB26-BLK-10.5', qty: 1, cost: 40, name: 'Foundry', productId: 113 },
  ],
  lowLines: [
    { sku: '', qty: 1, name: '=====LOW INV====', productId: 1000 },
    { sku: 'PB271-BRN-09', qty: 1, cost: 20, name: 'Brandon 09', productId: 114 },
    { sku: 'PB271-BRN-06', qty: 1, cost: 20, name: 'Brandon 06', productId: 115 },
    { sku: 'PB271-BRN-06', qty: 1, cost: 20, name: 'Brandon 06', productId: 115 },
  ],
};
const rows = mergePoLinesBySku(po);

check('one row per SKU', rows.length === 5);
check('the duplicated SKU is sent ONCE', rows.filter((r) => r.sku === 'PB271-BRN-06').length === 1);
check('…with its quantities summed to 2', find(rows, 'PB271-BRN-06').qty === 2);
check('total units still 6', rows.reduce((a, r) => a + r.qty, 0) === 6);
check('the =====LOW INV==== separator is dropped', !rows.some((r) => String(r.name || '').includes('LOW INV')));
check('customer lines are lowInv false', find(rows, 'PB334-BLK-09').lowInv === false);
check('reorder-only lines are lowInv true', find(rows, 'PB271-BRN-06').lowInv === true);

// A SKU wanted by BOTH halves must merge as customer demand: a lowInv line may drop out silently,
// a customer line stops the run. Merging the wrong way would quietly not buy it.
const both = mergePoLinesBySku({
  soLines: [{ sku: 'PB99', qty: 1, cost: 5, name: 'x', productId: 9 }],
  lowLines: [{ sku: 'PB99', qty: 3, cost: 5, name: 'x', productId: 9 }],
});
check('SKU in both halves merges to one row', both.length === 1);
check('…quantities summed', both[0].qty === 4);
check('…and it counts as CUSTOMER demand, not droppable', both[0].lowInv === false);

// Castle sends only { sku, qty } — prove the merged shape still carries what it needs.
const castle = rows.map((l) => ({ sku: l.sku, qty: l.qty }));
check('Castle shape keeps one row per SKU', castle.length === 5);
check('…and its expectUnits still totals 6', castle.reduce((a, l) => a + l.qty, 0) === 6);

process.exit(pass ? 0 : 1);
