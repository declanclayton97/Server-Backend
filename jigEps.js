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

// ── Vector track ─────────────────────────────────────────────────────
// Sniff whether an uploaded file is an EPS/PostScript (vector) rather than a
// raster image, so any jig can take either: ASCII EPS starts with "%!PS",
// DOS-EPS binaries with the C5 D0 D3 C6 magic.
export function isVectorEps(buffer) {
  if (!buffer || buffer.length < 4) return false;
  if (buffer[0] === 0xc5 && buffer[1] === 0xd0 && buffer[2] === 0xd3 && buffer[3] === 0xc6) return true;
  return buffer[0] === 0x25 && buffer[1] === 0x21 && buffer[2] === 0x50 && buffer[3] === 0x53; // "%!PS"
}

// Strip a binary DOS-EPS preview header (if any) and read the BoundingBox.
function parseEps(buffer) {
  let buf = buffer;
  if (buf.length > 30 && buf[0] === 0xc5 && buf[1] === 0xd0 && buf[2] === 0xd3 && buf[3] === 0xc6) {
    // DOS EPS binary container: bytes 4-7 = PS offset, 8-11 = PS length.
    const psStart = buf.readUInt32LE(4);
    const psLen = buf.readUInt32LE(8);
    buf = buf.subarray(psStart, psStart + psLen);
  }
  const text = buf.toString("latin1");
  const hi = text.match(/%%HiResBoundingBox:\s*([-\d.]+)\s+([-\d.]+)\s+([-\d.]+)\s+([-\d.]+)/);
  const lo = text.match(/%%BoundingBox:\s*([-\d.]+)\s+([-\d.]+)\s+([-\d.]+)\s+([-\d.]+)/);
  const m = hi || lo;
  if (!m) return { text, bbox: null };
  return { text, bbox: { llx: +m[1], lly: +m[2], urx: +m[3], ury: +m[4] } };
}

// Adobe-recommended prologue to safely embed an EPS (isolates graphics state,
// neutralises showpage, balances the stack/dict stack).
const EPS_PROLOGUE = `/BeginEPSF {
  /b4_Inc_state save def
  /dict_count countdictstack def
  /op_count count 1 sub def
  userdict begin
  /showpage {} def
  0 setgray 0 setlinecap 1 setlinewidth 0 setlinejoin
  10 setmiterlimit [] 0 setdash newpath
  /languagelevel where { pop languagelevel 1 ne { false setstrokeadjust false setoverprint } if } if
} bind def
/EndEPSF {
  count op_count sub { pop } repeat
  countdictstack dict_count sub { end } repeat
  b4_Inc_state restore
} bind def
`;

/**
 * Tile an uploaded vector EPS logo across the jig placements as true vector EPS.
 * @param {Buffer} vectorBuffer  the operator's vector logo (.eps)
 */
export function tileVectorEps({ vectorBuffer, pageWmm, pageHmm, placements, logoAdjust }) {
  const { text, bbox } = parseEps(vectorBuffer);
  if (!bbox) throw new Error("Uploaded EPS has no BoundingBox — is it a valid vector EPS?");
  const bw = (bbox.urx - bbox.llx) || 1, bh = (bbox.ury - bbox.lly) || 1;
  const pageWpt = pageWmm * MM_TO_PT, pageHpt = pageHmm * MM_TO_PT;

  // Optional per-job logo nudge, applied IDENTICALLY to every placement so the
  // operator can tune one pen and have it apply to the whole bed. scale is a
  // multiplier on the fit-to-box size; offsets are in box-local mm (measured in
  // the box's own frame after rotation, so the logo sits the same on each pen
  // regardless of facing).
  const adjScale = Math.max(0.05, Number(logoAdjust?.scale) || 1);
  const adjDxPt = (Number(logoAdjust?.offsetXmm) || 0) * MM_TO_PT;
  const adjDyPt = (Number(logoAdjust?.offsetYmm) || 0) * MM_TO_PT;

  // EOD marker that ends each inlined copy of the logo's bytes. Must not occur
  // in the logo itself.
  const EOD = "%%TUFFSHOP-END-OF-LOGO-DATA";
  if (text.includes(EOD)) throw new Error("Logo EPS contains the reserved EOD marker");

  let out =
    `%!PS-Adobe-3.0 EPSF-3.0\n` +
    `%%Creator: TuffShop jig generator (vector)\n` +
    `%%BoundingBox: 0 0 ${Math.ceil(pageWpt)} ${Math.ceil(pageHpt)}\n` +
    `%%HiResBoundingBox: 0 0 ${pageWpt.toFixed(4)} ${pageHpt.toFixed(4)}\n` +
    `%%LanguageLevel: 2\n%%EndComments\n` +
    EPS_PROLOGUE;

  // Draw the logo by re-embedding its EPS bytes INLINE at each placement, run via
  // SubFileDecode+exec. Two compact alternatives were rejected: wrapping the
  // logo in one /DrawLogo procedure throws /limitcheck for any real logo
  // (>65535 tokens per PostScript array), and a ReusableStreamDecode "define
  // once, replay" stream is broken over currentfile in Ghostscript. Inlining is
  // the portable, reliable option — compact for true vector logos (the right
  // input for a pen jig), larger only for image-heavy artwork.
  // currentfile inside the SubFileDecode block reads the logo's own inline data
  // (embedded images), so image-bearing logos work too.
  const logoBlock =
    `BeginEPSF\n` +
    `currentfile 0 (${EOD}) /SubFileDecode filter cvx exec\n` +
    `${text}\n${EOD}\n` +
    `EndEPSF\n`;

  for (const p of placements || []) {
    // Fit the logo's bbox inside the placement box, keep aspect, centre it.
    let drawWmm = p.wmm, drawHmm = p.wmm * (bh / bw);
    if (drawHmm > p.hmm) { drawHmm = p.hmm; drawWmm = p.hmm * (bw / bh); }
    const cxPt = (p.xmm + p.wmm / 2) * MM_TO_PT;
    const cyPt = (p.ymm + p.hmm / 2) * MM_TO_PT;
    const s = (drawWmm * MM_TO_PT) / bw * adjScale; // fit (aspect kept) × per-job scale
    const rot = Number(p.rotation) || 0;
    out += `gsave\n${cxPt.toFixed(3)} ${cyPt.toFixed(3)} translate\n`;
    if (rot) out += `${rot} rotate\n`;
    if (adjDxPt || adjDyPt) out += `${adjDxPt.toFixed(3)} ${adjDyPt.toFixed(3)} translate\n`; // box-local nudge
    out += `${s.toFixed(6)} ${s.toFixed(6)} scale\n`;
    out += `${(-(bbox.llx + bw / 2)).toFixed(3)} ${(-(bbox.lly + bh / 2)).toFixed(3)} translate\n`;
    out += logoBlock + `grestore\n`;
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
