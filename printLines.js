// Parse Brightpearl order rows into DTF print jobs for the Print Queue.
//
// The real signal (confirmed on order 467882): a print is a dedicated charge
// row whose productName is "Print (Our Garments) …" and whose decoration
// position lives in productOptions["Print Position"] (e.g. "Left Breast",
// "Back"), with the count in the row's quantity.magnitude. Embroidery rows
// ("Embroidery (Our Garments)" / productOptions["Embroidery Position"]) are a
// different process and must be excluded. Garment rows that merely mention a
// logo in their name are NOT counted (the Print charge rows are the source of
// truth, so we don't double-count).

// Canonical positions. `keys` are filename/label hints used later for matching
// the OneDrive file "<Customer> <Position>.eps".
const POSITIONS = [
  { code: "LB",    canonical: "Left Breast",  re: /\b(left[\s-]*(breast|chest)|lb|l\/b)\b/i,  keys: ["LB", "Left Breast", "Left Chest"] },
  { code: "RB",    canonical: "Right Breast", re: /\b(right[\s-]*(breast|chest)|rb|r\/b)\b/i, keys: ["RB", "Right Breast", "Right Chest"] },
  { code: "BACK",  canonical: "Back",         re: /\b((full|large|centre|center)[\s-]*back|back|rear)\b/i, keys: ["Back", "Full Back"] },
  { code: "FRONT", canonical: "Front",        re: /\b((full|large|centre|center)[\s-]*front|front)\b/i, keys: ["Front", "Full Front"] },
  { code: "LS",    canonical: "Left Sleeve",  re: /\b(left[\s-]*(sleeve|arm)|ls)\b/i,  keys: ["Left Sleeve", "Left Arm", "LS", "Arm"] },
  { code: "RS",    canonical: "Right Sleeve", re: /\b(right[\s-]*(sleeve|arm)|rs)\b/i, keys: ["Right Sleeve", "Right Arm", "RS"] },
  { code: "NAPE",  canonical: "Nape",         re: /\b(nape|neck)\b/i, keys: ["Nape", "Neck"] },
  { code: "LEG",   canonical: "Leg",          re: /\b(leg|thigh)\b/i, keys: ["Leg"] },
];

// Match a free string to a known position, or null.
export function matchPosition(text) {
  const s = String(text || "");
  for (const p of POSITIONS) if (p.re.test(s)) return { code: p.code, canonical: p.canonical };
  return null;
}

// Pull a position out of a row's name when there's no explicit option. Prefers a
// "Position: X" clause, else scans the whole string.
export function parsePosition(text) {
  const s = String(text || "");
  const m = s.match(/position\s*[:\-]\s*([^,;|\n]+)/i);
  const hit = matchPosition(m ? m[1] : s);
  if (hit) return { ...hit, raw: (m ? m[1] : "").trim() || hit.canonical };
  return { code: null, canonical: null, raw: m ? m[1].trim() : null };
}

// Is this row a DTF print charge line (not embroidery, not a plain garment)?
// Covers both trade orders ("Print (Our Garments)" + Print Position option) and
// website orders ("Print Left Breast"/"Print Large Rear" + Location option /
// Personalisation=Print).
export function isPrintRow(row) {
  const name = String(row.productName || "");
  const opts = row.productOptions || {};
  if (/embroid/i.test(name) || "Embroidery Position" in opts) return false;
  if ("Print Position" in opts) return true;
  if (String(opts.Personalisation || "").toLowerCase() === "print") return true;
  return /^\s*print\b/i.test(name); // "Print (Our Garments)", "Print Left Breast", "Print -", "Print:"
}

// Garment colour breakdown of the PRINTED items only. A garment line has a
// Colour option (and isn't a Print/Embroidery charge row); we count it when its
// description indicates printing ("...PRINTED...", "Add ... Print", etc.).
// Returns [{ colour, qty }] summed per colour.
export function extractPrintedGarments(rows) {
  const list = Array.isArray(rows) ? rows : Object.values(rows || {});
  const byColour = new Map();
  for (const row of list) {
    const opts = row.productOptions || {};
    const colour = opts.Colour || opts.Color;
    if (!colour) continue;                                  // not a garment (charge rows have no Colour)
    if ("Print Position" in opts || "Embroidery Position" in opts || "Location" in opts) continue;
    const name = String(row.productName || "");
    if (!/\bprint/i.test(name)) continue;                   // only printed garments
    const qty = Math.round(Number(row.quantity?.magnitude ?? row.quantityMagnitude ?? 0)) || 0;
    if (!qty) continue;
    byColour.set(colour, (byColour.get(colour) || 0) + qty);
  }
  return [...byColour.entries()].map(([colour, qty]) => ({ colour, qty }));
}

// Extract customer-uploaded logo URLs (website orders embed them in a garment
// row's text as "Upload Logo - <url>" / downloadCustomOption links).
export function extractLogoUrls(rows) {
  const list = Array.isArray(rows) ? rows : Object.values(rows || {});
  const urls = new Set();
  for (const row of list) {
    const text = String(row.productName || "");
    for (const m of text.matchAll(/https?:\/\/[^\s"'<>]*downloadCustomOption[^\s"'<>]*/gi)) urls.add(m[0]);
  }
  return [...urls];
}

// Turn an order's rows into print jobs. Position comes from the
// "Print Position" option first, else a name parse; no position → assume LB.
export function printJobsFromRows(rows) {
  const list = Array.isArray(rows) ? rows : Object.values(rows || {});
  const jobs = [];
  for (const row of list) {
    if (!isPrintRow(row)) continue;
    const qty = Math.round(Number(row.quantity?.magnitude ?? row.quantityMagnitude ?? 0)) || 0;
    // Logo/personalisation detail = the text after the "Print (…)" bracket
    // (with or without a -, :, + or dash separator), e.g.
    // "Print (Our Garments) + AC SECURE LOGO", "Print (Our Garments) - TABS LOGO",
    // "Print (Our Garments) CARLTON CC". Whitespace (incl. newlines) is collapsed
    // first so multi-line lines like "… BELOW TEXT '…'" are captured whole. The
    // bracket-or-separator requirement avoids mis-capturing website lines like
    // "Print Left Breast" (the position).
    const cleanName = String(row.productName || "").replace(/\s+/g, " ").trim();
    const detailM = cleanName.match(/^print\s*(?:\([^)]*\)\s*[-:+–—]?|[-:+–—])\s*(.+)$/i);
    const logoDetail = detailM ? detailM[1].trim() : null;
    const base = { qty, rawName: row.productName || "", logoDetail, sku: row.productSku || null };
    const optPos = row.productOptions?.["Print Position"] || row.productOptions?.["Location"];

    let hit, rawPos;
    if (optPos) { hit = matchPosition(optPos); rawPos = optPos; }
    else { const p = parsePosition(row.productName); hit = p.code ? p : null; rawPos = p.raw; }

    if (hit && hit.code) {
      jobs.push({ ...base, position: hit.canonical, positionCode: hit.code, assumedLB: false, unknownPosition: false });
    } else if (rawPos) {
      // a position was stated but we don't recognise it (e.g. "Miscellaneous", "Pocket")
      jobs.push({ ...base, position: rawPos, positionCode: null, assumedLB: false, unknownPosition: true });
    } else {
      // nothing stated → assume Left Breast (filename convention)
      jobs.push({ ...base, position: "Left Breast", positionCode: "LB", assumedLB: true, unknownPosition: false });
    }
  }
  return jobs;
}
