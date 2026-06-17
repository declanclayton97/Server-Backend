// DTF gang-sheet nesting. Film width is fixed; length grows down the roll and
// breaks into a new sheet at maxSheetLmm. Uses MaxRects (Best-Short-Side-Fit)
// with free 90° rotation — fills gaps and rotates whenever it packs tighter,
// far less film waste than a shelf packer. A `gap` is reserved around each
// print (added to its footprint) so prints don't touch.
//
// items: [{ id, wmm, hmm, allowRotate }]
// returns { sheets: [{ lengthMm, efficiency, placements:[{ item, xmm, ymm,
//          wmm, hmm, rotated }] }], oversized: [item] }   (xmm/ymm = bottom-left)

class MaxRects {
  constructor(W, H) { this.W = W; this.H = H; this.free = [{ x: 0, y: 0, w: W, h: H }]; }
  // Insert a w×h footprint (already gap-inclusive). Returns {x,y,rotated} or null.
  insert(w, h, allowRotate) {
    let best = null;
    const consider = (fw, fh, rot) => {
      for (const fr of this.free) {
        if (fw <= fr.w + 1e-6 && fh <= fr.h + 1e-6) {
          const score = Math.min(fr.w - fw, fr.h - fh); // best short-side fit
          if (!best || score < best.score) best = { score, x: fr.x, y: fr.y, fw, fh, rot };
        }
      }
    };
    consider(w, h, false);
    if (allowRotate) consider(h, w, true);
    if (!best) return null;
    this._place(best.x, best.y, best.fw, best.fh);
    return { x: best.x, y: best.y, rotated: best.rot };
  }
  _place(x, y, w, h) {
    const used = { x, y, w, h };
    const next = [];
    for (const fr of this.free) {
      if (this._overlap(fr, used)) {
        if (used.x > fr.x) next.push({ x: fr.x, y: fr.y, w: used.x - fr.x, h: fr.h });
        if (used.x + used.w < fr.x + fr.w) next.push({ x: used.x + used.w, y: fr.y, w: (fr.x + fr.w) - (used.x + used.w), h: fr.h });
        if (used.y > fr.y) next.push({ x: fr.x, y: fr.y, w: fr.w, h: used.y - fr.y });
        if (used.y + used.h < fr.y + fr.h) next.push({ x: fr.x, y: used.y + used.h, w: fr.w, h: (fr.y + fr.h) - (used.y + used.h) });
      } else next.push(fr);
    }
    this.free = next.filter((a, i) => !next.some((b, j) => i !== j && this._contains(b, a)));
  }
  _overlap(a, b) { return !(b.x >= a.x + a.w || b.x + b.w <= a.x || b.y >= a.y + a.h || b.y + b.h <= a.y); }
  _contains(a, b) { return a.x <= b.x + 1e-6 && a.y <= b.y + 1e-6 && a.x + a.w >= b.x + b.w - 1e-6 && a.y + a.h >= b.y + b.h - 1e-6; }
}

export function nestPrints({ items, sheetWmm, maxSheetLmm = 1200, gapMm = 5, marginMm = 0 }) {
  const usableW = sheetWmm - 2 * marginMm;
  const usableL = maxSheetLmm - 2 * marginMm;

  const oversized = [];
  let toPlace = [];
  for (const it of items) {
    const fits = it.wmm <= usableW + 1e-6 && it.hmm <= usableL + 1e-6;
    const fitsRot = it.allowRotate && it.hmm <= usableW + 1e-6 && it.wmm <= usableL + 1e-6;
    if (fits || fitsRot) toPlace.push(it); else oversized.push(it);
  }
  // Pack the biggest first — better fill.
  toPlace.sort((a, b) => (b.wmm * b.hmm) - (a.wmm * a.hmm));

  const sheets = [];
  let remaining = toPlace;
  let guard = 0;
  while (remaining.length && guard++ < 200) {
    const packer = new MaxRects(usableW, usableL);
    const placements = [];
    const leftover = [];
    for (const it of remaining) {
      const r = packer.insert(it.wmm + gapMm, it.hmm + gapMm, it.allowRotate);
      if (r) {
        const w = r.rotated ? it.hmm : it.wmm;
        const h = r.rotated ? it.wmm : it.hmm;
        placements.push({ item: it, xmm: marginMm + r.x, ymm: marginMm + r.y, wmm: w, hmm: h, rotated: r.rotated });
      } else leftover.push(it);
    }
    if (!placements.length) break; // safety
    const lengthMm = Math.max(...placements.map((p) => p.ymm + p.hmm)) + marginMm;
    const usedArea = placements.reduce((a, p) => a + p.wmm * p.hmm, 0);
    sheets.push({ lengthMm, efficiency: usedArea / (sheetWmm * lengthMm), placements });
    remaining = leftover;
  }
  return { sheets, oversized };
}
