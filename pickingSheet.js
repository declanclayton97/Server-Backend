// pickingSheet.js — the storage-unit picking list, sent to Bob on WhatsApp every weekday
// at 15:00 (Dec, 1-2 Oct 2026).
//
// The list lives in SharePoint: "Storage Unit Picking sheet.xlsx", Sheet1, columns
//   A DATE · B SKU · C DESC · D SIZE · E COLOUR · F LOCATION · G QTY · H ORDER · I RFW · J REMOVED STW LOCATION
// Picked rows are filled GREEN and kept for the record; WHITE rows still need picking.
// At 15:00 the white rows are drawn as a table image and WhatsApped to Bob, then turned
// green (Dec's call: green, not a separate "sent" colour). No white rows -> a short
// "Nothing to pick today" message instead.
//
// Order of operations matters: the WhatsApp goes FIRST and rows are only turned green once
// it has been accepted, so a failed send never hides rows. If colouring fails after a
// send, the rows go again tomorrow (a repeat, never a loss).
//
// Access: the Microsoft app "Tuffshop CS Helpdesk" (MS_* env, same as Outlook) needs
// Files.SelectedOperations.Selected + write access granted on THIS file only.
// Config (Render env):
//   PICKING_DRIVE_ID, PICKING_ITEM_ID   the file (from Graph: search -> parentReference.driveId, id)
//   PICKING_SHEET                       default "Sheet1"
//   PICKING_WA_TO                       Bob's WhatsApp number (international, e.g. 447...)
//   PICKING_WA_CHANNEL                  which business number sends it, default "sales"
//   PICKING_TEMPLATE / PICKING_NOTHING_TEMPLATE / PICKING_TEMPLATE_LANG
//                                       approved templates, default storage_unit_picking /
//                                       storage_unit_nothing / en_GB
//   PICKING_ENABLED=on                  the 15:00 run; off until tested

import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { graphConfigured, graphRequest } from "./graphMail.js";

const COLS = ["DATE", "SKU", "DESC", "SIZE", "COLOUR", "LOCATION", "QTY", "ORDER"];   // A..H, what Bob needs
const SCAN_ROWS = 150;        // fill colours are read for the last 150 data rows (older ones are long picked)
const SENT_PURPLE = "#CCC0DA";   // Excel's light purple: dark text stays readable on it

const cfg = () => ({
  drive: process.env.PICKING_DRIVE_ID, item: process.env.PICKING_ITEM_ID,
  sheet: process.env.PICKING_SHEET || "Sheet1",
  to: (process.env.PICKING_WA_TO || "").replace(/[^\d]/g, ""),
  channel: process.env.PICKING_WA_CHANNEL || "sales",
  template: process.env.PICKING_TEMPLATE || "storage_unit_picking",
  nothingTemplate: process.env.PICKING_NOTHING_TEMPLATE || "storage_unit_nothing",
  lang: process.env.PICKING_TEMPLATE_LANG || "en_GB",
});
const wb = (c) => `/drives/${encodeURIComponent(c.drive)}/items/${encodeURIComponent(c.item)}/workbook/worksheets('${encodeURIComponent(c.sheet)}')`;

// Excel stores dates as serial numbers; the sheet types "1-Oct-26" which Excel converts.
function cellText(v, col) {
  if (v === null || v === undefined) return "";
  if (col === "DATE" && typeof v === "number" && v > 30000 && v < 80000) {
    const d = new Date(Math.round((v - 25569) * 864e5));
    return d.toLocaleDateString("en-GB", { day: "numeric", month: "short", timeZone: "UTC" });
  }
  return String(v).trim();
}
const isWhite = (color) => !color || /^#?ffffff$/i.test(String(color)) || /^none$/i.test(String(color));

// Read the sheet: every data row's values, plus the fill of the recent ones.
export async function readPickingSheet() {
  const c = cfg();
  if (!graphConfigured() || !c.drive || !c.item) throw new Error("Picking sheet not configured (PICKING_DRIVE_ID / PICKING_ITEM_ID)");
  const used = await graphRequest("GET", `${wb(c)}/usedRange(valuesOnly=true)?$select=address,values`);
  const values = (used && used.values) || [];
  const firstRow = Number(((used && used.address) || "").match(/!?[A-Z]+(\d+)/)?.[1] || 1);
  const rows = [];
  values.forEach((v, i) => {
    const rowNo = firstRow + i;
    if (rowNo === 1) return;                                   // headings
    const rec = Object.fromEntries(COLS.map((k, j) => [k, cellText(v[j], k)]));
    if (!rec.SKU && !rec.DESC) return;                         // empty row (the sheet has blank bordered rows)
    rows.push({ rowNo, ...rec });
  });
  // Fill colours, 20 per Graph $batch, for the most recent rows only.
  const recent = rows.slice(-SCAN_ROWS);
  for (let i = 0; i < recent.length; i += 20) {
    const part = recent.slice(i, i + 20);
    const res = await graphRequest("POST", "/$batch", {
      requests: part.map((r, j) => ({ id: String(j), method: "GET", url: `${wb(c)}/range(address='A${r.rowNo}')/format/fill` })),
    });
    for (const resp of (res && res.responses) || []) {
      const r = part[Number(resp.id)];
      if (resp.status === 200) r.fill = (resp.body && resp.body.color) || null;
      else r.fillError = resp.status;
    }
  }
  const unknown = recent.filter((r) => r.fillError);
  if (unknown.length) throw new Error(`Could not read the colour of ${unknown.length} row(s) - nothing sent`);
  // Rows sent to Bob are coloured PURPLE (Dec, 7 Oct; was the sheet's own green), so the team
  // can tell "sent to the unit" from "picked". Any non-white row is never sent again.
  const green = process.env.PICKING_SENT_COLOUR || SENT_PURPLE;
  return { pending: recent.filter((r) => isWhite(r.fill)), green, totalRows: rows.length };
}

// Turn the given rows green (A..J, the same span the team colours).
export async function markRowsGreen(rows, green) {
  const c = cfg();
  for (const r of rows) {
    await graphRequest("PATCH", `${wb(c)}/range(address='A${r.rowNo}:J${r.rowNo}')/format/fill`, { color: green });
  }
}

// ---- the table image -----------------------------------------------------------------
const esc = (s) => String(s).replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;");
const clip = (s, n) => (String(s).length > n ? String(s).slice(0, n - 1) + "…" : String(s));
let fonts = null;
function fontBuffers() {
  if (!fonts) {
    const here = path.dirname(fileURLToPath(import.meta.url));
    fonts = [fs.readFileSync(path.join(here, "assets", "table-font.ttf")), fs.readFileSync(path.join(here, "assets", "badge-font.ttf"))];
  }
  return fonts;
}
// Columns sized for a phone: what Bob looks for first (location) is near the front.
const LAYOUT = [
  { k: "LOCATION", w: 150, max: 14 }, { k: "QTY", w: 56, max: 4, align: "middle" }, { k: "DESC", w: 480, max: 47 },
  { k: "SIZE", w: 120, max: 11 }, { k: "COLOUR", w: 170, max: 16 }, { k: "SKU", w: 230, max: 22 }, { k: "ORDER", w: 140, max: 12 },
];
// Word-wrap to lines of at most n characters (a word longer than a line is split).
export function wrap(s, n) {
  const words = String(s || "").split(/\s+/).filter(Boolean), lines = [];
  let cur = "";
  for (let w of words) {
    while (w.length > n) { if (cur) { lines.push(cur); cur = ""; } lines.push(w.slice(0, n)); w = w.slice(n); }
    if (!cur) cur = w; else if ((cur + " " + w).length <= n) cur += " " + w; else { lines.push(cur); cur = w; }
  }
  if (cur) lines.push(cur);
  return lines.length ? lines : [""];
}
// A row with only words in it (no SKU, location or quantity) is a NOTE for Bob, e.g.
// "CHECK IF STW.1.L.5 AT UNIT - BOX IN OFFICE ..." - it gets the full width of the table.
const isNote = (r) => !r.SKU && !r.LOCATION && !r.QTY && !!(r.DESC || r.COLOUR || r.SIZE || r.ORDER);

// Long text WRAPS onto more lines in its cell (Dec, 5 Oct: a long note was cut off); the
// row grows to fit. Nothing is ever truncated.
export function pickingSvg(rows, title) {
  const pad = 16, lineH = 24, top = 64, headH = 40;
  const width = LAYOUT.reduce((a, c) => a + c.w, 0) + pad * 2;
  let x = pad;
  const xs = LAYOUT.map((c) => { const at = x; x += c.w; return at; });
  const noteMax = Math.floor((width - pad * 2 - 20) / 11);
  const laid = rows.map((r) => {
    if (isNote(r)) {
      const text = [r.DESC, r.SIZE, r.COLOUR, r.ORDER].filter(Boolean).join("  ");
      const lines = wrap(text, noteMax);
      return { r, note: lines, h: Math.max(40, lines.length * lineH + 16) };
    }
    const cells = LAYOUT.map((c) => wrap(r[c.k] || "", c.max));
    return { r, cells, h: Math.max(40, Math.max(...cells.map((l) => l.length)) * lineH + 16) };
  });
  const bodyH = laid.reduce((a, l) => a + l.h, 0);
  const height = top + headH + bodyH + pad;
  const textAt = (c, i, lines, y, bold) => {
    const tx = c.align === "middle" ? xs[i] + c.w / 2 : xs[i] + 10;
    return lines.map((ln, k) => `<text x="${tx}" y="${y + 27 + k * lineH}" font-size="19" ${bold ? 'font-weight="700"' : ""} text-anchor="${c.align === "middle" ? "middle" : "start"}" fill="#111">${esc(ln)}</text>`).join("");
  };
  let out = `<svg xmlns="http://www.w3.org/2000/svg" width="${width}" height="${height}" viewBox="0 0 ${width} ${height}" font-family="DejaVu Sans">`;
  out += `<rect width="${width}" height="${height}" fill="#ffffff"/>`;
  out += `<text x="${pad}" y="40" font-size="24" font-weight="700" fill="#111">${esc(title)}</text>`;
  out += `<rect x="${pad}" y="${top}" width="${width - pad * 2}" height="${headH}" fill="#FFFF00" stroke="#000"/>`;
  LAYOUT.forEach((c, i) => { out += textAt(c, i, [c.k], top, true); });
  xs.slice(1).forEach((cx) => { out += `<line x1="${cx}" y1="${top}" x2="${cx}" y2="${top + headH}" stroke="#999"/>`; });
  let y = top + headH;
  laid.forEach((l, n) => {
    out += `<rect x="${pad}" y="${y}" width="${width - pad * 2}" height="${l.h}" fill="${n % 2 ? "#f4f4f4" : "#ffffff"}" stroke="#999"/>`;
    if (l.note) {
      out += l.note.map((ln, k) => `<text x="${pad + 10}" y="${y + 27 + k * lineH}" font-size="19" font-weight="700" fill="#111">${esc(ln)}</text>`).join("");
    } else {
      LAYOUT.forEach((c, i) => { out += textAt(c, i, l.cells[i], y, c.k === "LOCATION" || c.k === "QTY"); });
      xs.slice(1).forEach((cx) => { out += `<line x1="${cx}" y1="${y}" x2="${cx}" y2="${y + l.h}" stroke="#999"/>`; });
    }
    y += l.h;
  });
  return out + "</svg>";
}
export async function pickingPng(rows, title) {
  const { Resvg } = await import("@resvg/resvg-js");
  const r = new Resvg(pickingSvg(rows, title), { font: { fontBuffers: fontBuffers(), defaultFontFamily: "DejaVu Sans", loadSystemFonts: false } });
  return Buffer.from(r.render().asPng());
}

// ---- WhatsApp -----------------------------------------------------------------------------
async function waUploadPng(phoneNumberId, png) {
  const v = process.env.WHATSAPP_GRAPH_VERSION || "v21.0";
  const form = new FormData();
  form.append("messaging_product", "whatsapp");
  form.append("type", "image/png");
  form.append("file", new Blob([png], { type: "image/png" }), "picking-list.png");
  const r = await fetch(`https://graph.facebook.com/${v}/${phoneNumberId}/media`, { method: "POST", headers: { Authorization: `Bearer ${process.env.WHATSAPP_TOKEN}` }, body: form });
  const j = await r.json().catch(() => ({}));
  if (!r.ok || !j.id) throw new Error(`WhatsApp image upload failed: ${(j.error && j.error.message) || r.status}`);
  return j.id;
}
async function waTemplate(phoneNumberId, to, name, lang, components) {
  const v = process.env.WHATSAPP_GRAPH_VERSION || "v21.0";
  const r = await fetch(`https://graph.facebook.com/${v}/${phoneNumberId}/messages`, {
    method: "POST", headers: { Authorization: `Bearer ${process.env.WHATSAPP_TOKEN}`, "Content-Type": "application/json" },
    body: JSON.stringify({ messaging_product: "whatsapp", to, type: "template", template: { name, language: { code: lang }, components } }),
  });
  const j = await r.json().catch(() => ({}));
  if (!r.ok) throw new Error(`WhatsApp send failed: ${(j.error && (j.error.error_user_msg || j.error.message)) || r.status}`);
  return (j.messages && j.messages[0] && j.messages[0].id) || null;
}

const ukToday = () => new Date().toLocaleDateString("en-GB", { weekday: "short", day: "numeric", month: "short", timeZone: "Europe/London" });

/**
 * One run. dryRun: read + draw only (nothing sent, nothing coloured). to: override the
 * recipient (testing). markGreen=false: send without colouring (testing on the real sheet).
 */
export async function runPicking({ waPhoneNumberId, dryRun = false, to = null, markGreen = true } = {}) {
  const c = cfg();
  const { pending, green, totalRows } = await readPickingSheet();
  const day = ukToday();
  // Notes (a row with words but no item) go to Bob too, but are not items to count.
  const items = pending.filter((r) => !isNote(r)), notes = pending.length - items.length;
  const units = items.reduce((a, r) => a + (Number(r.QTY) || 1), 0);
  const plural = (n, w) => `${n} ${w}${n === 1 ? "" : "s"}`;
  const summary = (items.length ? `${plural(items.length, "line")}, ${plural(units, "unit")}` : "no items") + (notes ? ` + ${plural(notes, "note")}` : "");
  const title = pending.length ? `Storage unit picking list - ${day} - ${summary}` : `Nothing to pick - ${day}`;
  const result = { day, totalRows, pending: pending.map(({ fill, ...r }) => r), units, green, title, sent: false, marked: 0 };
  if (dryRun) return result;
  const recipient = (to || c.to || "").replace(/[^\d]/g, "");
  if (!recipient) throw new Error("No WhatsApp number for the picking list (PICKING_WA_TO)");
  const phoneNumberId = waPhoneNumberId(c.channel);
  if (!process.env.WHATSAPP_TOKEN || !phoneNumberId) throw new Error(`WhatsApp ${c.channel} number not configured`);

  if (!pending.length) {
    result.messageId = await waTemplate(phoneNumberId, recipient, c.nothingTemplate, c.lang, [{ type: "body", parameters: [{ type: "text", text: day }] }]);
    result.sent = true;
    return result;
  }
  const png = await pickingPng(pending, title);
  const mediaId = await waUploadPng(phoneNumberId, png);
  result.messageId = await waTemplate(phoneNumberId, recipient, c.template, c.lang, [
    { type: "header", parameters: [{ type: "image", image: { id: mediaId } }] },
    { type: "body", parameters: [{ type: "text", text: day }, { type: "text", text: summary }] },
  ]);
  result.sent = true;
  if (markGreen) {
    try { await markRowsGreen(pending, green); result.marked = pending.length; }
    catch (e) { result.markError = e.message; }   // they will simply go again tomorrow
  }
  return result;
}

export function registerPickingSheet(app, { getPool, waPhoneNumberId }) {
  const requireUser = (req, res, next) => app.locals.requireHubUser(req, res, next);
  const ensure = async () => getPool().query(`CREATE TABLE IF NOT EXISTS picking_runs (
      day date PRIMARY KEY, ran_at timestamptz NOT NULL DEFAULT now(), result jsonb)`);

  // Preview / test (Sales Hub sign-in). ?send=1&to=<number> sends a real test to that
  // number WITHOUT colouring the sheet; nothing at all is sent without ?send=1.
  app.get("/api/picking/preview", requireUser, async (req, res) => {
    try {
      if (req.query.send) return res.json(await runPicking({ waPhoneNumberId, to: String(req.query.to || ""), markGreen: false }));
      const r = await runPicking({ waPhoneNumberId, dryRun: true });
      if (req.query.png) { res.type("png"); return res.send(await pickingPng(r.pending, r.title)); }
      res.json(r);
    } catch (e) { res.status(500).json({ error: e.message }); }
  });

  // Weekdays 15:00 UK, once a day even across restarts (the day's row is claimed first).
  setInterval(async () => {
    if (String(process.env.PICKING_ENABLED || "").toLowerCase() !== "on" || !getPool()) return;
    const now = new Date();
    const hm = now.toLocaleTimeString("en-GB", { hour: "2-digit", minute: "2-digit", hour12: false, timeZone: "Europe/London" });
    const dow = now.toLocaleDateString("en-GB", { weekday: "short", timeZone: "Europe/London" });
    if (/Sat|Sun/.test(dow) || hm < "15:00" || hm > "15:30") return;
    const day = now.toLocaleDateString("en-CA", { timeZone: "Europe/London" });
    try {
      await ensure();
      const claim = await getPool().query(`INSERT INTO picking_runs (day) VALUES ($1) ON CONFLICT (day) DO NOTHING RETURNING day`, [day]);
      if (!claim.rowCount) return;
      let result;
      try { result = await runPicking({ waPhoneNumberId }); }
      catch (e) {
        // Nothing was sent: release the day so it retries next minute (until 15:30).
        console.error("[picking] run failed (retrying until 15:30):", e.message);
        await getPool().query(`DELETE FROM picking_runs WHERE day = $1`, [day]);
        return;
      }
      await getPool().query(`UPDATE picking_runs SET result = $2 WHERE day = $1`, [day, JSON.stringify(result)]);
      console.log("[picking]", day, JSON.stringify({ sent: result.sent, lines: (result.pending || []).length, marked: result.marked, error: result.error || result.markError }));
    } catch (e) { console.error("[picking] scheduler:", e.message); }
  }, 60 * 1000);
}
