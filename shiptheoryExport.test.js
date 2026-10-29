// Tests for shiptheoryExport.js.
// Run with:  node shiptheoryExport.test.js

import { goesAbroad, productIdsOf, wantedFor, plan } from "./shiptheoryExport.js";

let pass = 0, fail = 0;
const eq = (name, got, want) => {
  const ok = JSON.stringify(got) === JSON.stringify(want);
  if (ok) pass++; else { fail++; console.log(`FAIL ${name}\n  got:  ${JSON.stringify(got)}\n  want: ${JSON.stringify(want)}`); }
};

eq("Ireland", goesAbroad({ parties: { delivery: { countryIsoCode: "IE" } } }), true);
eq("GB", goesAbroad({ parties: { delivery: { countryIsoCode: "GB" } } }), false);
eq("no address", goesAbroad({ parties: { delivery: {} } }), false);

// SO 496090: two products plus three misc (1000) lines.
eq("rows", productIdsOf({ orderRows: { a: { productId: 330337 }, b: { productId: 170341 }, c: { productId: 1000 }, d: { productId: 1000 } } }), [330337, 170341]);

const trousers = { id: 330337, identity: { sku: "63340455150", mpn: "63340455150", barcode: "7332515421227", ean: "7332515421227" },
  salesChannels: [{ productName: "Snickers 6334 Hi-Vis Stretch Trousers Class 1\r\nAdditional Options" }] };
const w = wantedFor(trousers);
eq("wanted", [w.sku, w.mid_code, w.gtin, w.name, w.missing], ["63340455150", "63340455150", "7332515421227", "Snickers 6334 Hi-Vis Stretch Trousers Class 1", []]);
eq("ean fallback", wantedFor({ identity: { sku: "X", mpn: "", barcode: "", ean: "5012345678900" } }).gtin, "5012345678900");
eq("not a gtin", wantedFor({ identity: { sku: "X", mpn: "M1", barcode: "ABC-12" } }).missing, ['barcode "ABC-12" is not a GTIN']);
eq("nothing", wantedFor({ identity: { sku: "X" } }).missing, ["no MPN", "no barcode"]);

// Not in Shiptheory yet -> create it with both.
eq("create", plan(null, w), { method: "POST", path: "/products", body: { sku: "63340455150", export_details: { mid_code: "63340455150", gtin: "7332515421227" },
  name: "Snickers 6334 Hi-Vis Stretch Trousers Class 1", barcode: "7332515421227" } });
// In Shiptheory without them -> update, keeping what's already there.
eq("update", plan({ sku: "63340455150", export_details: { eccn: "EAR99", mid_code: "", gtin: null } }, w),
  { method: "PUT", path: "/products/update/63340455150", body: { sku: "63340455150", export_details: { eccn: "EAR99", mid_code: "63340455150", gtin: "7332515421227" } } });
eq("no export_details", plan({ sku: "63340455150", export_details: null }, w).body.export_details, { mid_code: "63340455150", gtin: "7332515421227" });
eq("already right", plan({ export_details: { mid_code: "63340455150", gtin: "7332515421227" } }, w), null);
eq("nothing to send", plan(null, wantedFor({ identity: { sku: "X" } })), null);
eq("mpn only", plan({ export_details: {} }, wantedFor({ identity: { sku: "X", mpn: "M1" } })).body.export_details, { mid_code: "M1" });

console.log(`${pass} passed, ${fail} failed`);
if (fail) process.exit(1);
