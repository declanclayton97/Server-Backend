// Leo pack-only styles: PO 497130 (2026-10-16) asked for S01 x2 per size and Leo's cart made it x5.
import { leoPackRound } from './purchasingSchedule.js';
let pass = true;
const check = (n, c) => { console.log((c ? "PASS  " : "FAIL  ") + n); if (!c) pass = false; };

const r = leoPackRound([
  { sku: "S01-O-LEO-M", qty: 2, cost: 3.88, productId: 209093 },
  { sku: "S01-Y-LEO-XXL", qty: 5, cost: 3.88, productId: 209105 },
  { sku: "S01-O-LEO-S", qty: 6, cost: 3.88, productId: 209094 },
  { sku: "W11-Y/O-LEO-L", qty: 2, cost: 4.58, productId: 210519 },
]);
const q = Object.fromEntries(r.lines.map((l) => [l.sku, l.qty]));
check("S01 x2 -> x5", q["S01-O-LEO-M"] === 5);
check("S01 x5 stays x5", q["S01-Y-LEO-XXL"] === 5);
check("S01 x6 -> x10", q["S01-O-LEO-S"] === 10);
check("W11 (not pack-only) untouched", q["W11-Y/O-LEO-L"] === 2);
check("extras list only the rounded lines", r.extras.length === 2 && r.extras.every((x) => x.sku.startsWith("S01-")));
check("extra units = to - from", r.extras.find((x) => x.sku === "S01-O-LEO-M").extra === 3);
process.exit(pass ? 0 : 1);
