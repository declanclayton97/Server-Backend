// Tight-ish shelf nesting for DTF gang sheets. The film width is fixed; length
// grows down the roll and breaks into a new sheet at maxSheetLmm. Each item may
// be rotated 90° (if allowRotate) to fit the width / pack tighter. Shelf
// (next-fit decreasing height) — simple and decent; can be upgraded to a
// skyline/MaxRects packer later for tighter nests.
//
// items: [{ id, wmm, hmm, allowRotate }]  (already expanded by quantity)
// returns { sheets: [{ lengthMm, efficiency, placements:[{ item, xmm, ymm,
//          wmm, hmm, rotated }] }], oversized: [item] }
//   xmm/ymm = bottom-left of the footprint (PS origin bottom-left).
// maxSheetLmm defaults to 1200 — a longer roll fits more per sheet (fewer sheet
// breaks = less wasted film). The Print Queue UI will expose this so it can be
// tuned per run.
export function nestPrints({ items, sheetWmm, maxSheetLmm = 1200, gapMm = 5, marginMm = 5 }) {
  const usableW = sheetWmm - 2 * marginMm;
  const usableL = maxSheetLmm - 2 * marginMm;

  // Orient each item; drop ones too big to fit even rotated.
  const oversized = [];
  const oriented = [];
  for (const it of items) {
    const opts = [{ w: it.wmm, h: it.hmm, rotated: false }];
    if (it.allowRotate) opts.push({ w: it.hmm, h: it.wmm, rotated: true });
    const valid = opts.filter((o) => o.w <= usableW + 1e-6 && o.h <= usableL + 1e-6);
    if (valid.length === 0) { oversized.push(it); continue; }
    valid.sort((a, b) => a.h - b.h); // prefer the shorter shelf height
    oriented.push({ it, ...valid[0] });
  }
  oriented.sort((a, b) => b.h - a.h); // tallest first (classic shelf packing)

  const sheets = [];
  let s = null;
  const startSheet = () => { s = { placements: [], shelfY: marginMm, shelfH: 0, cursorX: marginMm }; sheets.push(s); };
  startSheet();

  for (const o of oriented) {
    const remW = (marginMm + usableW) - s.cursorX;
    if (s.shelfH > 0 && o.w <= remW + 1e-6) {
      // fits the open shelf (height guaranteed by the descending sort)
      s.placements.push({ item: o.it, xmm: s.cursorX, ymm: s.shelfY, wmm: o.w, hmm: o.h, rotated: o.rotated });
      s.cursorX += o.w + gapMm;
      continue;
    }
    // close the shelf and open a new one (a new sheet if it would overflow length)
    let newY = s.shelfH === 0 ? s.shelfY : s.shelfY + s.shelfH + gapMm;
    if (newY + o.h + marginMm > maxSheetLmm) { startSheet(); newY = marginMm; }
    s.shelfY = newY; s.shelfH = o.h; s.cursorX = marginMm;
    s.placements.push({ item: o.it, xmm: s.cursorX, ymm: s.shelfY, wmm: o.w, hmm: o.h, rotated: o.rotated });
    s.cursorX += o.w + gapMm;
  }

  const result = sheets
    .filter((sh) => sh.placements.length)
    .map((sh) => {
      const lengthMm = Math.max(...sh.placements.map((p) => p.ymm + p.hmm)) + marginMm;
      const usedArea = sh.placements.reduce((a, p) => a + p.wmm * p.hmm, 0);
      return { lengthMm, efficiency: usedArea / (sheetWmm * lengthMm), placements: sh.placements };
    });
  return { sheets: result, oversized };
}
