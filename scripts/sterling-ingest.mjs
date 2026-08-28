// Build sterlingProducts.json — EAN barcode -> { search, colour, size, sku, brand }
// from the Sterling product-data workbook. This is the portal-ordering map: our BP
// Sterling SKU (an EAN, same key the stock feed uses) resolves to the shop's style
// name (search term) + colour + size, which the worker uses to place the order.
// Re-run when Sterling send an updated file: `node scripts/sterling-ingest.mjs "<xlsx path>"`
import fs from 'fs';
import XLSX from 'xlsx';

const XLSX_PATH = process.argv[2] || 'C:/Users/DeclanClayton/Tuff Workwear Ltd/TuffShop - Documents/Company Folder/Purchasing/Supplier Info/Sterling/2025 & 2026 CSV File - Product Data.xlsx';
const OUT = new URL('../sterlingProducts.json', import.meta.url);

const COLOURS = ['black/grey', 'grey/black', 'navy/black', 'black', 'brown', 'honey', 'stone', 'grey', 'navy', 'khaki', 'olive', 'tan', 'beige', 'charcoal', 'blue', 'green', 'red', 'white', 'sand', 'graphite'];
const deriveColour = (name) => { const n = String(name).toLowerCase(); return (COLOURS.find((c) => n.includes(c)) || '').replace(/\b\w/g, (x) => x.toUpperCase()); };
const clean = (s) => String(s == null ? '' : s).trim();

const wb = XLSX.readFile(XLSX_PATH);
const map = {};
// [sheetName, hasHeader, cols] — main/promo are richest (own Colour col); use them first.
const sheets = [
  ['2025-2026', true, { bc: 1, sku: 2, colour: 3, brand: 4, size: 9, name: 10 }],
  ["Promo's", true, { bc: 1, sku: 2, colour: 3, brand: 4, size: 9, name: 10 }],
  ['footwear', true, { bc: 1, sku: 9, colour: null, brand: 3, size: 8, name: 9 }],
  ['workwear ', false, { bc: 1, sku: 9, colour: null, brand: 3, size: 8, name: 9, desc: 10 }],
];
for (const [sn, hasHeader, c] of sheets) {
  const ws = wb.Sheets[sn]; if (!ws) continue;
  const rows = XLSX.utils.sheet_to_json(ws, { header: 1, defval: '' });
  // find the first data row: after the "Bar Code" header if present, else row 0
  let start = 0;
  if (hasHeader) { const hi = rows.findIndex((r) => r.some((x) => /bar\s*code/i.test(String(x)))); start = hi >= 0 ? hi + 1 : 1; }
  for (const r of rows.slice(start)) {
    const bc = clean(r[c.bc]).replace(/\D/g, '');
    if (bc.length < 8) continue;
    if (map[bc]) continue;                                   // first (richest) sheet wins
    const name = clean(r[c.name]);
    if (!name) continue;
    const colour = c.colour != null ? clean(r[c.colour]) : (deriveColour(name) || deriveColour(clean(r[c.desc])));
    map[bc] = { search: name, colour, size: clean(r[c.size]), sku: clean(r[c.sku]), brand: clean(r[c.brand]) };
  }
}
// MERGE THE OVERRIDES LAST. Sterling's workbook is not complete: 5055160080800 (Dewalt Albany
// Slim, 30 Waist / 31 Leg) appears ZERO times in the 2026-01-08 file while the sizes either side
// of it are present, and that one gap stopped the whole Sterling run on 2026-08-28. Rebuilding
// from a newer workbook must not silently drop a hand-verified entry, so overrides are applied
// after the sheets and win. Each carries a _why saying how it was verified; keys starting with _
// are comments and are skipped. Delete an entry once Sterling ship a file that contains it.
const OVERRIDES = new URL('../sterlingProductsOverrides.json', import.meta.url);
let overrode = 0;
try {
  const ov = JSON.parse(fs.readFileSync(OVERRIDES, 'utf8'));
  for (const [k, v] of Object.entries(ov)) {
    if (k.startsWith('_')) continue;
    const { _why, ...entry } = v;
    map[k] = entry; overrode++;
  }
} catch (e) { console.warn('no overrides applied:', e.message); }

fs.writeFileSync(OUT, JSON.stringify(map));
if (overrode) console.log(`  + ${overrode} override(s) from sterlingProductsOverrides.json`);
console.log(`sterlingProducts.json: ${Object.keys(map).length} EANs -> ${OUT.pathname}`);
