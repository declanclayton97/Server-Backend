// Generate print-ready EPS for promo-item production jigs with the customer's
// (raster) logo embedded at exact mm size/position. Pure Node (PostScript
// Level 2) — no design software. Validated to open at true size in Illustrator/
// CorelDRAW. Transparency is flattened onto white (jigs are white surfaces).
//
// A jig = a page (mm) + one or more placement boxes. The logo is scaled to fit
// each box (aspect kept), centred, and optionally rotated about the box centre.
//   placements: [{ xmm, ymm, wmm, hmm, rotation? }]  (bottom-left origin)
import Jimp from "jimp";

const MM_TO_PT = 72 / 25.4; // 2.834645669

// Decode an image buffer, flatten transparency onto white, return RGB hex
// (wrapped so no PostScript line exceeds the DSC 255-char guideline).
async function loadLogoRgbHex(buffer) {
  const logo = await Jimp.read(buffer);
  const w = logo.bitmap.width, h = logo.bitmap.height;
  const canvas = new Jimp(w, h, 0xffffffff);
  canvas.composite(logo, 0, 0);
  const data = canvas.bitmap.data; // RGBA
  const parts = [];
  for (let i = 0; i < data.length; i += 4) {
    parts.push(
      data[i].toString(16).padStart(2, "0") +
      data[i + 1].toString(16).padStart(2, "0") +
      data[i + 2].toString(16).padStart(2, "0")
    );
  }
  // Wrap at 120 hex pairs (240 chars) per line for RIP friendliness.
  const flat = parts.join("");
  const lines = flat.match(/.{1,240}/g) || [flat];
  return { w, h, hex: lines.join("\n") };
}

/**
 * @param {Buffer} logoBuffer  raster logo (PNG/JPEG)
 * @param {number} pageWmm     jig page width (mm)
 * @param {number} pageHmm     jig page height (mm)
 * @param {Array}  placements  [{ xmm, ymm, wmm, hmm, rotation? }]
 * @returns {Promise<Buffer>}  EPS file
 */
export async function generateJigEps({ logoBuffer, pageWmm, pageHmm, placements }) {
  const { w: imgW, h: imgH, hex } = await loadLogoRgbHex(logoBuffer);
  const aspect = imgW / imgH;
  const pageWpt = pageWmm * MM_TO_PT, pageHpt = pageHmm * MM_TO_PT;

  let out =
    `%!PS-Adobe-3.0 EPSF-3.0\n` +
    `%%Creator: TuffShop jig generator\n` +
    `%%BoundingBox: 0 0 ${Math.ceil(pageWpt)} ${Math.ceil(pageHpt)}\n` +
    `%%HiResBoundingBox: 0 0 ${pageWpt.toFixed(4)} ${pageHpt.toFixed(4)}\n` +
    `%%LanguageLevel: 2\n` +
    `%%EndComments\n` +
    `/picstr ${imgW * 3} string def\n` +
    `gsave 1 1 1 setrgbcolor 0 0 ${pageWpt.toFixed(3)} ${pageHpt.toFixed(3)} rectfill grestore\n`;

  for (const p of placements || []) {
    // Fit logo inside the box, keep aspect ratio.
    let drawWmm = p.wmm, drawHmm = p.wmm / aspect;
    if (drawHmm > p.hmm) { drawHmm = p.hmm; drawWmm = p.hmm * aspect; }
    const cxPt = (p.xmm + p.wmm / 2) * MM_TO_PT;
    const cyPt = (p.ymm + p.hmm / 2) * MM_TO_PT;
    const wPt = drawWmm * MM_TO_PT, hPt = drawHmm * MM_TO_PT;
    const rot = Number(p.rotation) || 0;
    out += `gsave\n${cxPt.toFixed(3)} ${cyPt.toFixed(3)} translate\n`;
    if (rot) out += `${rot} rotate\n`;
    out += `${(-wPt / 2).toFixed(3)} ${(-hPt / 2).toFixed(3)} translate\n`;
    out += `${wPt.toFixed(3)} ${hPt.toFixed(3)} scale\n`;
    out += `${imgW} ${imgH} 8 [${imgW} 0 0 -${imgH} 0 ${imgH}]\n`;
    out += `{currentfile picstr readhexstring pop} false 3 colorimage\n${hex}\ngrestore\n`;
  }

  out += `%%EOF\n`;
  return Buffer.from(out, "latin1");
}

// Expand a saved template into concrete placements. For grid templates (pens)
// this lays out rows×cols of identical boxes across the page.
export function placementsFromTemplate(t) {
  const g = t.grid;
  // Pen jig: two interleaved facings per column. Everything is computed from a
  // single bottom-right anchor (right-facing) + the left-facing offset + gaps,
  // so editing the anchor shifts the whole bed and the rest follow.
  if (g && g.kind === 'pen') {
    const {
      cols, perColumn, colGapMm, vGapMm, boxWmm, boxHmm,
      anchorXmm, anchorYmm, leftDxMm, leftDyMm,
      rightRotation = 0, leftRotation = 180,
    } = g;
    const out = [];
    for (let col = 0; col < cols; col++) {
      const dx = -col * colGapMm; // columns go left from the rightmost
      for (let n = 0; n < perColumn; n++) {
        out.push({ xmm: anchorXmm + dx, ymm: anchorYmm + n * vGapMm, wmm: boxWmm, hmm: boxHmm, rotation: rightRotation });
        out.push({ xmm: anchorXmm + leftDxMm + dx, ymm: anchorYmm + leftDyMm + n * vGapMm, wmm: boxWmm, hmm: boxHmm, rotation: leftRotation });
      }
    }
    return out;
  }
  if (g && g.cols && g.rows) {
    const {
      cols, rows, marginXmm = 0, marginYmm = 0, cellWmm, cellHmm,
      gapXmm = 0, gapYmm = 0, rotation = 0, alternateRowRotation = false,
    } = t.grid;
    const out = [];
    // r=0 is the BOTTOM row (PS y increases upward) — matches "bottom-right is
    // position 1, right way up". With alternateRowRotation, each row up flips
    // 180° (so odd rows from the bottom are upside down).
    for (let r = 0; r < rows; r++) {
      const rot = rotation + (alternateRowRotation && r % 2 === 1 ? 180 : 0);
      for (let c = 0; c < cols; c++) {
        out.push({
          xmm: marginXmm + c * (cellWmm + gapXmm),
          ymm: marginYmm + r * (cellHmm + gapYmm),
          wmm: cellWmm, hmm: cellHmm, rotation: rot,
        });
      }
    }
    return out;
  }
  return t.placements || [];
}
