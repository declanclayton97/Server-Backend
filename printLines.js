// Parse Brightpearl order rows into print jobs for the DTF Print Queue.
// A print row looks like e.g. "3 x Print (Our Garments) Position: Left Breast"
// (qty from the row's quantity.magnitude). We pull the decoration POSITION and
// normalise it to a canonical code; the OneDrive matcher then maps that code to
// the file named "<Customer> <Position>.eps" (no position in a name → LB).

// Canonical positions. `keys` are filename/label hints used later for matching.
// Order matters: more specific patterns first.
const POSITIONS = [
  { code: "LB",    canonical: "Left Breast",  re: /\b(left[\s-]*(breast|chest)|lb|l\/b)\b/i,  keys: ["LB", "Left Breast", "Left Chest"] },
  { code: "RB",    canonical: "Right Breast", re: /\b(right[\s-]*(breast|chest)|rb|r\/b)\b/i, keys: ["RB", "Right Breast", "Right Chest"] },
  { code: "BACK",  canonical: "Back",         re: /\b((full|large|centre|center)[\s-]*back|back)\b/i, keys: ["Back", "Full Back"] },
  { code: "FRONT", canonical: "Front",        re: /\b((full|large|centre|center)[\s-]*front|front)\b/i, keys: ["Front", "Full Front"] },
  { code: "LS",    canonical: "Left Sleeve",  re: /\b(left[\s-]*(sleeve|arm)|ls)\b/i,  keys: ["Left Sleeve", "Left Arm", "LS", "Arm"] },
  { code: "RS",    canonical: "Right Sleeve", re: /\b(right[\s-]*(sleeve|arm)|rs)\b/i, keys: ["Right Sleeve", "Right Arm", "RS"] },
  { code: "NAPE",  canonical: "Nape",         re: /\b(nape|neck)\b/i, keys: ["Nape", "Neck"] },
  { code: "LEG",   canonical: "Leg",          re: /\b(leg|thigh)\b/i, keys: ["Leg"] },
];

// Pull the decoration position out of a row's text. Prefers an explicit
// "Position: X" clause, otherwise scans the whole string for a known position.
// Returns { code, canonical, raw } — code/canonical null if unrecognised (so it
// surfaces as an exception rather than being silently dropped).
export function parsePosition(text) {
  const s = String(text || "");
  const m = s.match(/position\s*[:\-]\s*([^,;|\n]+)/i);
  const scope = m ? m[1] : s;
  for (const p of POSITIONS) {
    if (p.re.test(scope)) return { code: p.code, canonical: p.canonical, raw: (m ? m[1] : "").trim() || p.canonical };
  }
  // explicit clause but unrecognised position → keep the raw text for the operator
  if (m) return { code: null, canonical: null, raw: m[1].trim() };
  return { code: null, canonical: null, raw: null };
}

// Is this row a print line? Name/SKU mentions "print" (and we keep going even
// without a parsed position so unknown positions become exceptions).
export function isPrintRow(row) {
  const name = String(row.productName || "");
  const sku = String(row.productSku || "");
  return /\bprint\b/i.test(name) || /print/i.test(sku);
}

// Turn an order's rows into print jobs. Default position = LB when a print row
// has no recognisable position (per the filename convention).
export function printJobsFromRows(rows) {
  const list = Array.isArray(rows) ? rows : Object.values(rows || {});
  const jobs = [];
  for (const row of list) {
    if (!isPrintRow(row)) continue;
    const qty = Number(row.quantity?.magnitude ?? row.quantityMagnitude ?? 0) || 0;
    const pos = parsePosition(row.productName);
    const base = { qty, rawName: row.productName || "", sku: row.productSku || null };
    if (pos.code) {
      // recognised position
      jobs.push({ ...base, position: pos.canonical, positionCode: pos.code, assumedLB: false, unknownPosition: false });
    } else if (pos.raw) {
      // explicit but unrecognised (e.g. "Pocket") → exception for the operator
      jobs.push({ ...base, position: pos.raw, positionCode: null, assumedLB: false, unknownPosition: true });
    } else {
      // no position stated → assume Left Breast (per the filename convention)
      jobs.push({ ...base, position: "Left Breast", positionCode: "LB", assumedLB: true, unknownPosition: false });
    }
  }
  return jobs;
}
