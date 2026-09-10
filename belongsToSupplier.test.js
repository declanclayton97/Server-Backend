// The real 2026-09-10 case: Chadwick matches brandId 213; Behrens Group products carry it too.
import { belongsToSupplier } from './purchasingAuto.js';
let pass = true;
const check = (n, c) => { console.log((c ? "PASS  " : "FAIL  ") + n); if (!c) pass = false; };
const S = (...a) => new Set(a.map(String));
const M = (o) => new Map(Object.entries(o));
const CHAD = { brandNeedsOwnSupplier: true, notOurs: /^(?:NEX|HER)-/i };
const base = { nameDetect: false, supplierOwned: S(), claimed: S(), brandOwned: S(), foreignSupplier: M({}) };

check("CT line: BP names Chadwick -> ordered", belongsToSupplier(1, { ...base, ...CHAD, sku: "972-40-A-M", supplierOwned: S(1), brandOwned: S(1) }) === true);
check("NEX line: foreign primary supplier -> dropped", belongsToSupplier(2, { ...base, ...CHAD, sku: "NEX-TEE-RYL-L", brandOwned: S(2), foreignSupplier: M({ 2: "38380" }) }) === false);
check("DU028: third-party supplier, no NEX/HER shape -> dropped", belongsToSupplier(3, { ...base, ...CHAD, sku: "DU028NA-08", brandOwned: S(3), foreignSupplier: M({ 3: "33248" }) }) === false);
check("HER line with NO primary supplier -> still dropped by SKU shape", belongsToSupplier(4, { ...base, ...CHAD, sku: "HER-TEE-RYLYEL-MB", brandOwned: S(4) }) === false);
check("a genuine CT line with no primary supplier -> still ordered", belongsToSupplier(5, { ...base, ...CHAD, sku: "933-40/40-A-L", brandOwned: S(5) }) === true);
check("claimProductIds overrides both vetoes", belongsToSupplier(6, { ...base, ...CHAD, sku: "NEX-TEE-RYL-L", brandOwned: S(6), claimed: S(6), foreignSupplier: M({ 6: "38380" }) }) === true);

// PenCarrie: a DISTRIBUTOR. Brand must keep beating primarySupplierId, per SO 483237.
const PC = {};
check("PenCarrie: Anthem brand + Ralawise primary -> STILL ordered", belongsToSupplier(7, { ...base, ...PC, sku: "AM015-BLK-M", brandOwned: S(7), foreignSupplier: M({ 7: "205" }) }) === true);
process.exit(pass ? 0 : 1);
