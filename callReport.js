// callReport.js — the weekly sales activity report (Dec, 2 Oct 2026).
//
// Monday morning, for the previous Mon-Sun, per salesperson, per day:
//   calls in      answered inbound calls from outside (Webex Calling)
//   calls out     outbound calls dialled to outside
//   time on phone talk time across both
//   orders        Brightpearl sales orders that person created by hand
//
// Calls come from the Webex Calling Detailed Call History API (cdr_feed), read by the
// Service App "Tuffshop Sales Call Report" (scopes spark-admin:calling_cdr_read +
// spark-admin:people_read, authorised in Control Hub). Webex only keeps ~30 days of call
// detail, so every day is pulled overnight and kept in webex_cdr: the report is built from
// our copy, and history grows from the day this went in.
//
// What counts (the Webex record is one row per phone per call leg). The phones are Webex
// users named "Nicky Sales", "Jack Sales"... with no personal email, so people are matched
// on that name (webexName). A customer's call arrives on the main number (leg type
// SIP_INBOUND), goes through the auto attendant "Main AA" to the "Work Wear" hunt group, and
// reaches each sales phone as an SIP_ENTERPRISE leg — the same type as a colleague calling.
// So a leg is a CUSTOMER call when any leg sharing its Correlation / Interaction ID is not
// SIP_ENTERPRISE; a call with only enterprise legs is internal and counts for nothing.
//   - calls in: answered TERMINATING legs of customer calls. The hunt group rings several
//     phones; only the one that ANSWERED gets it.
//   - calls out: ORIGINATING legs dialled outside (call type not SIP_ENTERPRISE).
//   - time on phone: answered legs of both.
//   - orders: createdById = the person, and no installedIntegrationInstanceId (web, Amazon,
//     eBay orders are created by the integrations under Tim's id and are not anyone's work).
//
// Emails go from the sales mailbox (Graph). Until CALL_REPORT_LIVE=on every email goes to
// CALL_REPORT_TEST_EMAIL (default dec@) marked "[TEST - would go to X]"; live, each person
// gets their own report only. The manager summary (everyone) always goes to
// CALL_REPORT_MANAGER (default dec@).
//
// Env: WEBEX_CLIENT_ID, WEBEX_CLIENT_SECRET, WEBEX_REFRESH_TOKEN (seed; the latest one
//      Webex hands back is kept in webex_auth), WEBEX_CDR_BASE (default
//      https://analytics-calling-eu.webexapis.com), CALL_REPORT_PEOPLE (JSON to override the
//      list below), CALL_REPORT_ENABLED=on (the overnight pull + Monday send),
//      CALL_REPORT_LIVE=on, CALL_REPORT_TEST_EMAIL, CALL_REPORT_MANAGER.

import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import crypto from "crypto";
import { graphConfigured, sendNew } from "./graphMail.js";

const DEFAULT_PEOPLE = [
  { key: "nicky", webexName: "Nicky Sales", name: "Nicky Everall", first: "Nicky", email: "nicky@tuffshop.co.uk", bpId: 61342 },
  { key: "jack", webexName: "Jack Sales", name: "Jack Ellis-Haynes", first: "Jack", email: "jack@tuffshop.co.uk", bpId: 82710 },
  { key: "helen", webexName: "Helen Sales", name: "Helen Jackson", first: "Helen", email: "helen@tuffshop.co.uk", bpId: 59339 },
  { key: "bob", webexName: "Bob Sales", name: "Robert Lodge", first: "Bob", email: "bob@tuffshop.co.uk", bpId: 445 },
  { key: "laura", webexName: "Laura Sales", name: "Laura Jackson", first: "Laura", email: "laura@tuffshop.co.uk", bpId: 137062 },
];
export function reportPeople() {
  try { if (process.env.CALL_REPORT_PEOPLE) return JSON.parse(process.env.CALL_REPORT_PEOPLE); } catch { /* fall back */ }
  return DEFAULT_PEOPLE;
}

const TZ = "Europe/London";
const INTERNAL = "SIP_ENTERPRISE";
const isLive = () => String(process.env.CALL_REPORT_LIVE || "").toLowerCase() === "on";
const testEmail = () => process.env.CALL_REPORT_TEST_EMAIL || "dec@tuffshop.co.uk";
const managerEmail = () => process.env.CALL_REPORT_MANAGER || "dec@tuffshop.co.uk";
// Tuffshop is an EU org (the global host answers 451 and names this one); a 451 also
// switches host at run time, so a region move fixes itself.
let cdrHost = null;
const cdrBase = () => cdrHost || (process.env.WEBEX_CDR_BASE || "https://analytics-calling-eu.webexapis.com").replace(/\/$/, "");
const sleep = (ms) => new Promise((s) => setTimeout(s, ms));

// ---- dates (UK days) ------------------------------------------------------------------
export const ukDay = (d) => new Date(d).toLocaleDateString("en-CA", { timeZone: TZ });
// The UTC instant of 00:00 UK on a YYYY-MM-DD day (handles BST/GMT).
export function ukMidnight(day) {
  const guess = new Date(`${day}T00:00:00Z`);
  const ukHour = Number(guess.toLocaleTimeString("en-GB", { hour: "2-digit", hour12: false, timeZone: TZ }));
  return new Date(guess.getTime() - (ukHour % 24) * 3600e3);
}
export const addDays = (day, n) => { const d = new Date(`${day}T12:00:00Z`); d.setUTCDate(d.getUTCDate() + n); return d.toISOString().slice(0, 10); };
// Monday of the week before the one `now` is in.
export function previousWeekStart(now = new Date()) {
  const today = ukDay(now);
  const dow = new Date(`${today}T12:00:00Z`).getUTCDay();   // 0 Sun .. 6 Sat
  return addDays(today, -((dow + 6) % 7) - 7);
}
const dayLabel = (day) => new Date(`${day}T12:00:00Z`).toLocaleDateString("en-GB", { weekday: "short", day: "numeric", month: "short", timeZone: "UTC" });
export function fmtDuration(sec) {
  sec = Math.round(sec || 0);
  const h = Math.floor(sec / 3600), m = Math.floor((sec % 3600) / 60);
  return h ? `${h}h ${String(m).padStart(2, "0")}m` : `${m}m`;
}

// ---- Webex auth -----------------------------------------------------------------------
let webexCache = { token: null, until: 0 };
async function webexToken(pool) {
  if (webexCache.token && Date.now() < webexCache.until) return webexCache.token;
  const { WEBEX_CLIENT_ID: id, WEBEX_CLIENT_SECRET: secret } = process.env;
  if (!id || !secret) throw new Error("Webex not configured (WEBEX_CLIENT_ID / WEBEX_CLIENT_SECRET)");
  await pool.query(`CREATE TABLE IF NOT EXISTS webex_auth (id int PRIMARY KEY, refresh_token text NOT NULL, updated_at timestamptz NOT NULL DEFAULT now())`);
  const saved = (await pool.query(`SELECT refresh_token FROM webex_auth WHERE id = 1`)).rows[0];
  // A freshly pasted env token wins over the stored one (that is how it gets re-seeded).
  const seed = process.env.WEBEX_REFRESH_TOKEN || "";
  const seedRow = (await pool.query(`CREATE TABLE IF NOT EXISTS webex_auth_seed (seed text PRIMARY KEY)`).then(() =>
    pool.query(`SELECT 1 FROM webex_auth_seed WHERE seed = $1`, [seed]))).rowCount;
  const refresh = seed && !seedRow ? seed : (saved && saved.refresh_token) || seed;
  if (!refresh) throw new Error("Webex not authorised (WEBEX_REFRESH_TOKEN)");
  const r = await fetch("https://webexapis.com/v1/access_token", {
    method: "POST",
    headers: { "Content-Type": "application/x-www-form-urlencoded" },
    body: new URLSearchParams({ grant_type: "refresh_token", client_id: id, client_secret: secret, refresh_token: refresh }),
  });
  const j = await r.json().catch(() => ({}));
  if (!j.access_token) throw new Error(`Webex sign-in failed: ${r.status} ${j.message || j.error || ""}`.trim());
  await pool.query(`INSERT INTO webex_auth (id, refresh_token) VALUES (1, $1)
                    ON CONFLICT (id) DO UPDATE SET refresh_token = EXCLUDED.refresh_token, updated_at = now()`, [j.refresh_token || refresh]);
  if (seed) await pool.query(`INSERT INTO webex_auth_seed (seed) VALUES ($1) ON CONFLICT DO NOTHING`, [seed]);
  webexCache = { token: j.access_token, until: Date.now() + (Number(j.expires_in || 3600) - 300) * 1000 };
  return webexCache.token;
}

async function webexGet(pool, url) {
  for (let attempt = 0; ; attempt++) {
    const r = await fetch(url, { headers: { Authorization: `Bearer ${await webexToken(pool)}` } });
    if ((r.status === 429 || r.status >= 500) && attempt < 6) {
      await sleep((parseInt(r.headers.get("retry-after") || "60", 10) + 2) * 1000);
      continue;
    }
    const j = await r.json().catch(() => null);
    // 451 = this org's call records live in another region; Webex names the right host.
    const region = r.status === 451 && /https:\/\/analytics-calling[\w-]*\.webexapis\.com/.exec((j && j.message) || "");
    if (region && attempt < 6) {
      cdrHost = region[0];
      url = url.replace(/^https:\/\/analytics-calling[\w-]*\.webexapis\.com/, cdrHost);
      continue;
    }
    if (!r.ok) throw new Error(`Webex ${url.split("?")[0]} -> ${r.status}: ${(j && (j.message || JSON.stringify(j.errors || ""))) || ""}`);
    const next = /<([^>]+)>;\s*rel="next"/.exec(r.headers.get("link") || "");
    return { json: j, next: next ? next[1] : null };
  }
}

// Webex person ids are base64 "ciscospark://us/PEOPLE/<uuid>"; the call records carry the uuid.
const uuidFromPersonId = (id) => { try { return Buffer.from(id, "base64").toString("utf8").split("/").pop().toLowerCase(); } catch { return ""; } };
async function webexUuids(pool, people) {
  const out = {};
  for (const p of people) {
    try {
      const { json } = await webexGet(pool, `https://webexapis.com/v1/people?email=${encodeURIComponent(p.email)}`);
      const hit = (json && json.items || [])[0];
      if (hit) out[p.key] = uuidFromPersonId(hit.id);
    } catch (e) { console.error("[call-report] people lookup", p.email, e.message); }
  }
  return out;
}

// ---- call records ---------------------------------------------------------------------
async function ensureTables(pool) {
  await pool.query(`CREATE TABLE IF NOT EXISTS webex_cdr (
      id text PRIMARY KEY, day date NOT NULL, user_uuid text, user_name text,
      direction text, answered boolean, duration int, call_type text, start_time timestamptz)`);
  await pool.query(`CREATE INDEX IF NOT EXISTS webex_cdr_day ON webex_cdr (day)`);
  await pool.query(`ALTER TABLE webex_cdr ADD COLUMN IF NOT EXISTS correlation_id text`);
  await pool.query(`ALTER TABLE webex_cdr ADD COLUMN IF NOT EXISTS interaction_id text`);
  await pool.query(`CREATE TABLE IF NOT EXISTS webex_cdr_days (day date PRIMARY KEY, fetched_at timestamptz NOT NULL DEFAULT now(), records int)`);
  // Days pulled before the call ids were kept (2 Oct) are pulled again.
  await pool.query(`DELETE FROM webex_cdr_days d WHERE EXISTS (SELECT 1 FROM webex_cdr c WHERE c.day = d.day AND c.correlation_id IS NULL AND c.interaction_id IS NULL) AND d.fetched_at < '2026-10-02T11:05:00Z'`);
  await pool.query(`CREATE TABLE IF NOT EXISTS call_report_runs (week date PRIMARY KEY, ran_at timestamptz NOT NULL DEFAULT now(), result jsonb)`);
}

const field = (rec, ...names) => { for (const n of names) if (rec[n] !== undefined && rec[n] !== null && rec[n] !== "") return rec[n]; return null; };

// Pull one UK day (two 12-hour windows; the API's limit) into webex_cdr.
export async function collectDay(pool, day) {
  await ensureTables(pool);
  const from = ukMidnight(day), to = ukMidnight(addDays(day, 1));
  if (Date.now() < to.getTime() + 30 * 60e3) throw new Error(`${day} is not over yet`);
  let n = 0;
  for (let s = from.getTime(); s < to.getTime(); s += 12 * 3600e3) {
    const e = Math.min(s + 12 * 3600e3, to.getTime());
    let url = `${cdrBase()}/v1/cdr_feed?startTime=${new Date(s).toISOString()}&endTime=${new Date(e).toISOString()}&max=500`;
    while (url) {
      const { json, next } = await webexGet(pool, url);
      for (const rec of (json && json.items) || []) {
        const id = field(rec, "Report ID", "reportId") || [field(rec, "Correlation ID"), field(rec, "Local call ID", "Call ID"), field(rec, "User UUID")].join("|");
        const start = field(rec, "Start time", "startTime");
        await pool.query(
          `INSERT INTO webex_cdr (id, day, user_uuid, user_name, direction, answered, duration, call_type, start_time, correlation_id, interaction_id)
           VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)
           ON CONFLICT (id) DO UPDATE SET correlation_id = EXCLUDED.correlation_id, interaction_id = EXCLUDED.interaction_id`,
          [id, start ? ukDay(start) : day, String(field(rec, "User UUID", "userUuid") || "").toLowerCase(), field(rec, "User", "user"),
           field(rec, "Direction", "direction"), String(field(rec, "Answered", "answered")).toLowerCase() === "true",
           Number(field(rec, "Duration", "duration") || 0), field(rec, "Call type", "callType"), start,
           field(rec, "Correlation ID", "correlationId"), field(rec, "Interaction ID", "interactionId")]);
        n++;
      }
      url = next;
      if (url) await sleep(7000);   // pagination is rate-limited too
    }
    await sleep(61000);   // the feed allows about one new query a minute
  }
  await pool.query(`INSERT INTO webex_cdr_days (day, records) VALUES ($1,$2)
                    ON CONFLICT (day) DO UPDATE SET records = EXCLUDED.records, fetched_at = now()`, [day, n]);
  return n;
}

// Every finished day in [first, last] we have not pulled yet (and Webex still has).
export async function collectMissing(pool, first, last) {
  await ensureTables(pool);
  const oldest = addDays(ukDay(new Date()), -29);
  const have = new Set((await pool.query(`SELECT to_char(day,'YYYY-MM-DD') d FROM webex_cdr_days WHERE day BETWEEN $1 AND $2`, [first, last])).rows.map((r) => r.d));
  const done = [];
  for (let d = first; d <= last; d = addDays(d, 1)) {
    if (have.has(d) || d < oldest) continue;
    try { done.push([d, await collectDay(pool, d)]); }
    catch (e) { if (!/not over yet/.test(e.message)) throw e; }
  }
  return done;
}

// ---- orders ---------------------------------------------------------------------------
async function ordersCreated(bpLive, people, first, last) {
  const ids = new Map(people.map((p) => [Number(p.bpId), p.key]));
  const counts = {}, times = {};
  let firstResult = 1;
  for (;;) {
    const r = await bpLive("GET", `/order-service/order-search?orderTypeId=1&createdOn=${first}T00:00:00/${last}T23:59:59&pageSize=500&firstResult=${firstResult}`);
    const cols = r.metaData.columns.map((c) => c.name);
    const ix = (n) => cols.indexOf(n);
    for (const row of r.results) {
      const key = ids.get(Number(row[ix("createdById")]));
      if (!key || row[ix("installedIntegrationInstanceId")] != null) continue;
      const day = String(row[ix("createdOn")]).slice(0, 10);   // Brightpearl gives UK local time
      counts[key] = counts[key] || {};
      counts[key][day] = (counts[key][day] || 0) + 1;
      // "2026-10-01T09:23:41.000+01:00" - already UK local, so the clock part is the time.
      const hm = /T(\d{2}):(\d{2})/.exec(String(row[ix("createdOn")]));
      if (hm) (times[key] = times[key] || []).push({ day, min: Number(hm[1]) * 60 + Number(hm[2]), id: row[ix("orderId")] });
    }
    if (!r.metaData.morePagesAvailable) break;
    firstResult = r.metaData.lastResult + 1;
  }
  return { counts, times };
}

// ---- the report -----------------------------------------------------------------------
const blank = () => ({ callsIn: 0, callsOut: 0, talk: 0, orders: 0 });

// Calls that touched the outside world on any leg (see the header).
function customerCallTest(rows) {
  const outside = new Set();
  for (const r of rows) if (r.call_type && r.call_type !== INTERNAL) for (const id of [r.correlation_id, r.interaction_id]) if (id) outside.add(id);
  return (r) => (r.call_type && r.call_type !== INTERNAL) || [r.correlation_id, r.interaction_id].some((id) => id && outside.has(id));
}
const ukMinutes = (d) => {
  const [h, m] = new Date(d).toLocaleTimeString("en-GB", { hour: "2-digit", minute: "2-digit", hour12: false, timeZone: TZ }).split(":").map(Number);
  return h * 60 + m;
};

// Each counted call as a timeline event: { day, min (UK clock), dur (s), kind in|out, answered }.
export function callEvents(rows, uuidToKey, nameToKey = {}) {
  const isCustomerCall = customerCallTest(rows);
  const out = {};
  for (const r of rows) {
    if (!r.start_time || !isCustomerCall(r)) continue;
    const key = uuidToKey[String(r.user_uuid || "").toLowerCase()] || nameToKey[String(r.user_name || "").trim().toLowerCase()];
    if (!key) continue;
    const dir = String(r.direction || "").toUpperCase();
    let kind = null;
    if (dir === "TERMINATING" && r.answered) kind = "in";
    else if (dir === "ORIGINATING" && r.call_type !== INTERNAL) kind = "out";
    if (!kind) continue;
    (out[key] = out[key] || []).push({ day: ukDay(r.start_time), min: ukMinutes(r.start_time), dur: r.answered ? (r.duration || 0) : 0, kind, answered: !!r.answered });
  }
  return out;
}

export function tallyCalls(rows, uuidToKey, nameToKey = {}) {
  const isCustomerCall = customerCallTest(rows);
  const out = {};
  for (const r of rows) {
    if (!isCustomerCall(r)) continue;
    const key = uuidToKey[String(r.user_uuid || "").toLowerCase()] || nameToKey[String(r.user_name || "").trim().toLowerCase()];
    if (!key) continue;
    const day = typeof r.day === "string" ? r.day.slice(0, 10) : ukDay(r.day);
    const t = ((out[key] = out[key] || {})[day] = out[key][day] || blank());
    const dir = String(r.direction || "").toUpperCase();
    if (dir === "TERMINATING") { if (r.answered) { t.callsIn++; t.talk += r.duration || 0; } }
    else if (dir === "ORIGINATING" && r.call_type !== INTERNAL) { t.callsOut++; if (r.answered) t.talk += r.duration || 0; }
  }
  return out;
}

export async function buildReport({ pool, bpLive, week, collect = true }) {
  const people = reportPeople();
  const first = week, last = addDays(week, 6);
  await ensureTables(pool);
  const collected = collect ? await collectMissing(pool, first, last) : [];
  const uuids = await webexUuids(pool, people);
  const uuidToKey = Object.fromEntries(Object.entries(uuids).map(([k, u]) => [u, k]));
  const nameToKey = Object.fromEntries(people.flatMap((p) => [[p.name.toLowerCase(), p.key], ...(p.webexName ? [[p.webexName.toLowerCase(), p.key]] : [])]));
  // A day either side, so a call's legs either side of midnight still link up.
  const rows = (await pool.query(`SELECT to_char(day,'YYYY-MM-DD') AS day, user_uuid, user_name, direction, answered, duration, call_type, correlation_id, interaction_id, start_time
                                    FROM webex_cdr WHERE day BETWEEN $1 AND $2`, [addDays(first, -1), addDays(last, 1)])).rows;
  const calls = tallyCalls(rows, uuidToKey, nameToKey);
  const events = callEvents(rows, uuidToKey, nameToKey);
  const seenNames = new Set(rows.map((r) => String(r.user_name || "").trim().toLowerCase()));
  const { counts: orders, times: orderTimes } = await ordersCreated(bpLive, people, first, last);
  const daysWithData = new Set((await pool.query(`SELECT to_char(day,'YYYY-MM-DD') d FROM webex_cdr_days WHERE day BETWEEN $1 AND $2`, [first, last])).rows.map((r) => r.d));

  const days = Array.from({ length: 7 }, (_, i) => addDays(first, i));
  const report = people.map((p) => {
    const perDay = days.map((day) => ({ day, ...blank(), ...((calls[p.key] || {})[day] || {}), orders: (orders[p.key] || {})[day] || 0, noCallData: !daysWithData.has(day) }));
    const total = perDay.reduce((a, d) => ({ callsIn: a.callsIn + d.callsIn, callsOut: a.callsOut + d.callsOut, talk: a.talk + d.talk, orders: a.orders + d.orders }), blank());
    const inWeek = (e) => e.day >= first && e.day <= last;
    return { ...p, webexMatched: !!uuids[p.key] || seenNames.has(String(p.webexName || p.name).toLowerCase()), days: perDay, total,
      timeline: { calls: (events[p.key] || []).filter(inWeek), orders: (orderTimes[p.key] || []).filter(inWeek) } };
  });
  return { week: first, weekEnd: last, people: report, collected, missingCallDays: days.filter((d) => !daysWithData.has(d)) };
}

// ---- emails ---------------------------------------------------------------------------
const esc = (s) => String(s ?? "").replace(/[&<>"]/g, (c) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;" }[c]));
const TH = 'style="text-align:left;padding:6px 10px;background:#1f2a37;color:#fff;font-weight:600;font-size:13px"';
// Number columns are right-aligned, so their headings are too (Dec, 7 Oct: they did not line up).
const THN = 'style="text-align:right;padding:6px 10px;background:#1f2a37;color:#fff;font-weight:600;font-size:13px"';
const TD = 'style="padding:6px 10px;border-bottom:1px solid #e5e7eb;font-size:13px"';
const TDN = 'style="padding:6px 10px;border-bottom:1px solid #e5e7eb;font-size:13px;text-align:right"';
const TOT = 'style="padding:7px 10px;font-weight:700;font-size:13px;border-top:2px solid #1f2a37;text-align:right"';
const weekTitle = (r) => `${dayLabel(r.week)} - ${dayLabel(r.weekEnd)}`;
const shown = (p) => p.days.filter((d, i) => i < 5 || d.callsIn || d.callsOut || d.orders);   // weekends only if used

// ---- timeline -------------------------------------------------------------------------
// One chart per person: a row per day (Mon-Fri, plus a weekend day only if used), 07:00 to
// 18:00. Each call is a bar as long as the call (green in, blue out; a call out nobody answered
// is a thin grey tick); each order created is an orange line at the time it was made.
// Sent as an image because no email client draws charts from HTML reliably.
const TL_FROM = 7 * 60, TL_TO = 18 * 60;
const TL = { left: 92, right: 16, top: 52, rowH: 34, width: 900 };
export function timelineSvg(r, p) {
  const days = shown(p).map((d) => d.day);
  const plotW = TL.width - TL.left - TL.right;
  const x = (min) => TL.left + ((Math.min(Math.max(min, TL_FROM), TL_TO) - TL_FROM) / (TL_TO - TL_FROM)) * plotW;
  const height = TL.top + days.length * TL.rowH + 40;
  let out = `<svg xmlns="http://www.w3.org/2000/svg" width="${TL.width}" height="${height}" viewBox="0 0 ${TL.width} ${height}" font-family="DejaVu Sans">`;
  out += `<rect width="${TL.width}" height="${height}" fill="#ffffff"/>`;
  out += `<text x="12" y="22" font-size="15" font-weight="700" fill="#111">${esc(p.name)} - ${esc(weekTitle(r))}</text>`;
  for (let h = 7; h <= 18; h++) {
    const hx = x(h * 60);
    out += `<line x1="${hx}" y1="${TL.top - 6}" x2="${hx}" y2="${TL.top + days.length * TL.rowH}" stroke="${h % 3 === 0 ? "#c7ccd3" : "#e7e9ec"}"/>`;
    out += `<text x="${hx}" y="${TL.top - 12}" font-size="11" text-anchor="middle" fill="#6b7280">${String(h).padStart(2, "0")}:00</text>`;
  }
  days.forEach((day, i) => {
    const y = TL.top + i * TL.rowH;
    out += `<rect x="${TL.left}" y="${y + 4}" width="${plotW}" height="${TL.rowH - 8}" fill="${i % 2 ? "#fafafa" : "#f4f6f8"}"/>`;
    out += `<text x="12" y="${y + TL.rowH / 2 + 4}" font-size="12" fill="#111">${esc(dayLabel(day))}</text>`;
    for (const c of p.timeline.calls.filter((c) => c.day === day)) {
      const x1 = x(c.min), x2 = x(c.min + c.dur / 60);
      if (!c.answered) { out += `<rect x="${x1}" y="${y + 10}" width="1.5" height="${TL.rowH - 20}" fill="#9ca3af"/>`; continue; }
      out += `<rect x="${x1}" y="${y + 8}" width="${Math.max(2, x2 - x1)}" height="${TL.rowH - 16}" rx="1" fill="${c.kind === "in" ? "#16a34a" : "#2563eb"}" fill-opacity="0.85"/>`;
    }
    for (const o of p.timeline.orders.filter((o) => o.day === day)) {
      const ox = x(o.min);
      out += `<rect x="${ox - 1.25}" y="${y + 3}" width="2.5" height="${TL.rowH - 6}" fill="#ea580c"/>`;
    }
  });
  const ly = TL.top + days.length * TL.rowH + 24;
  const key = [["#16a34a", "Call in (answered)"], ["#2563eb", "Call out"], ["#9ca3af", "Call out, no answer"]];
  let lx = TL.left;
  for (const [col, label] of key) { out += `<rect x="${lx}" y="${ly - 9}" width="14" height="10" fill="${col}"/><text x="${lx + 20}" y="${ly}" font-size="11" fill="#374151">${label}</text>`; lx += 160; }
  out += `<rect x="${lx + 5}" y="${ly - 12}" width="2.5" height="15" fill="#ea580c"/><text x="${lx + 20}" y="${ly}" font-size="11" fill="#374151">Order created</text>`;
  return out + "</svg>";
}
let tlFont = null;
export async function timelinePng(r, p) {
  const { Resvg } = await import("@resvg/resvg-js");
  if (!tlFont) tlFont = fs.readFileSync(path.join(path.dirname(fileURLToPath(import.meta.url)), "assets", "table-font.ttf"));
  const png = new Resvg(timelineSvg(r, p), { fitTo: { mode: "width", value: TL.width * 2 }, font: { fontBuffers: [tlFont], defaultFontFamily: "DejaVu Sans", loadSystemFonts: false } }).render().asPng();
  return Buffer.from(png);
}
// The picture's address in an email (cid:) or in the browser preview (data:).
const cidFor = (p) => `timeline-${p.key}@callreport`;

const dayRows = (p) => shown(p).map((d) => `<tr><td ${TD}>${esc(dayLabel(d.day))}</td>
    <td ${TDN}>${d.noCallData ? "-" : d.callsIn}</td><td ${TDN}>${d.noCallData ? "-" : d.callsOut}</td>
    <td ${TDN}>${d.noCallData ? "-" : fmtDuration(d.talk)}</td><td ${TDN}>${d.orders}</td></tr>`).join("");
const timelineImg = (p, src) => `<img src="${src(p)}" width="${TL.width}" alt="Timeline of calls and orders, 7am to 6pm" style="display:block;width:100%;max-width:${TL.width}px;height:auto;border:1px solid #e5e7eb;margin-top:10px">`;

export function personEmailHtml(r, p, { imgSrc = (q) => "cid:" + cidFor(q) } = {}) {
  return `<div style="font-family:Segoe UI,Arial,sans-serif;color:#111;max-width:${TL.width}px">
  <p>Hi ${esc(p.first)},</p>
  <p>Here's your week on the phones and in Brightpearl, ${esc(weekTitle(r))}.</p>
  <table style="border-collapse:collapse;width:100%">
    <tr><th ${TH}>Day</th><th ${THN}>Calls in</th><th ${THN}>Calls out</th><th ${THN}>Time on phone</th><th ${THN}>Orders created</th></tr>
    ${dayRows(p)}
    <tr><td style="padding:7px 10px;font-weight:700;font-size:13px;border-top:2px solid #1f2a37">Week</td>
      <td ${TOT}>${p.total.callsIn}</td><td ${TOT}>${p.total.callsOut}</td><td ${TOT}>${fmtDuration(p.total.talk)}</td><td ${TOT}>${p.total.orders}</td></tr>
  </table>
  <p style="font-size:13px;margin:18px 0 0"><b>Your days, 7am to 6pm</b></p>
  ${timelineImg(p, imgSrc)}
  <p style="color:#6b7280;font-size:12px;margin-top:14px">Calls in are calls you answered; calls out are calls you made. Calls between colleagues aren't counted.
  Orders are sales orders you created in Brightpearl (web, Amazon and eBay orders aren't included).${r.missingCallDays.length ? " A dash means there's no call data for that day." : ""}</p>
</div>`;
}

// Overview first, then each person in a section that opens on click (<details>). Outlook on the
// web / phone and Apple Mail fold them; Outlook desktop can't, and shows them all open.
// Outlook desktop cannot fold <details>, so the email links to the same report as a web page
// where it does. The link carries an HMAC of the week, so it opens that one report and cannot
// be guessed or changed to another week. Secret: CALL_REPORT_LINK_SECRET, else one generated
// once and kept in the database.
let linkSecret = null;
async function reportSecret(pool) {
  if (process.env.CALL_REPORT_LINK_SECRET) return process.env.CALL_REPORT_LINK_SECRET;
  if (linkSecret) return linkSecret;
  await pool.query(`CREATE TABLE IF NOT EXISTS call_report_secret (id int PRIMARY KEY, secret text NOT NULL)`);
  await pool.query(`INSERT INTO call_report_secret (id, secret) VALUES (1, $1) ON CONFLICT (id) DO NOTHING`, [crypto.randomBytes(32).toString("hex")]);
  linkSecret = (await pool.query(`SELECT secret FROM call_report_secret WHERE id = 1`)).rows[0].secret;
  return linkSecret;
}
const signWeek = (secret, week) => crypto.createHmac("sha256", secret).update("manager:" + week).digest("hex").slice(0, 40);
export async function viewLink(pool, week) {
  const base = (process.env.PUBLIC_BASE_URL || "https://server-backend-1i47.onrender.com").replace(/\/$/, "");
  return `${base}/call-report/view?week=${week}&sig=${signWeek(await reportSecret(pool), week)}`;
}

export function managerEmailHtml(r, { imgSrc = (q) => "cid:" + cidFor(q), viewUrl = null, web = false } = {}) {
  const people = r.people.map((p) => `<tr><td ${TD}>${esc(p.name)}${p.webexMatched ? "" : ' <span style="color:#b91c1c">(not found in Webex)</span>'}</td>
    <td ${TDN}>${p.total.callsIn}</td><td ${TDN}>${p.total.callsOut}</td><td ${TDN}>${fmtDuration(p.total.talk)}</td><td ${TDN}>${p.total.orders}</td></tr>`).join("");
  const sum = r.people.reduce((a, p) => ({ callsIn: a.callsIn + p.total.callsIn, callsOut: a.callsOut + p.total.callsOut, talk: a.talk + p.total.talk, orders: a.orders + p.total.orders }), blank());
  const sections = r.people.map((p) => `<details style="margin:10px 0;border:1px solid #d1d5db;border-radius:6px">
  <summary style="cursor:pointer;padding:9px 12px;background:#f3f4f6;font-size:14px;font-weight:600">${esc(p.name)}
    <span style="font-weight:400;color:#4b5563">&nbsp;-&nbsp;${p.total.callsIn} in, ${p.total.callsOut} out, ${fmtDuration(p.total.talk)} on the phone, ${p.total.orders} order${p.total.orders === 1 ? "" : "s"}</span></summary>
  <div style="padding:10px 12px">
  <table style="border-collapse:collapse;width:100%"><tr><th ${TH}>Day</th><th ${THN}>Calls in</th><th ${THN}>Calls out</th><th ${THN}>On phone</th><th ${THN}>Orders</th></tr>
  ${dayRows(p)}</table>
  ${timelineImg(p, imgSrc)}
  </div></details>`).join("");
  return `<div style="font-family:Segoe UI,Arial,sans-serif;color:#111;max-width:${TL.width}px">
  <p>Sales activity for ${esc(weekTitle(r))}.</p>
  ${viewUrl ? `<p style="margin:0 0 14px"><a href="${esc(viewUrl)}" style="display:inline-block;background:#0f6cbd;color:#ffffff;text-decoration:none;font-weight:600;font-size:13px;padding:8px 14px;border-radius:4px">Open the full report</a>
    <span style="color:#6b7280;font-size:12px">&nbsp;each person's days and 7am-6pm timeline</span></p>` : ""}
  <table style="border-collapse:collapse;width:100%">
    <tr><th ${TH}>Person</th><th ${THN}>Calls in</th><th ${THN}>Calls out</th><th ${THN}>Time on phone</th><th ${THN}>Orders created</th></tr>
    ${people}
    <tr><td style="padding:7px 10px;font-weight:700;font-size:13px;border-top:2px solid #1f2a37">Team</td>
      <td ${TOT}>${sum.callsIn}</td><td ${TOT}>${sum.callsOut}</td><td ${TOT}>${fmtDuration(sum.talk)}</td><td ${TOT}>${sum.orders}</td></tr>
  </table>
  ${r.missingCallDays.length ? `<p style="color:#b45309;font-size:12px">No Webex call data for: ${r.missingCallDays.map(dayLabel).join(", ")}.</p>` : ""}
  ${web ? `<p style="font-size:13px;margin:18px 0 4px"><b>Each person</b> <span style="color:#6b7280">- click a name to open their days and timeline (7am to 6pm)</span></p>
  ${sections}` : ""}
  <p style="color:#6b7280;font-size:12px;margin-top:14px">Internal calls excluded; hunt-group calls count only for whoever answered. Orders = sales orders created by that person in Brightpearl, excluding web/Amazon/eBay.</p>
</div>`;
}

// The timeline pictures, as inline attachments the HTML points at with cid:.
async function timelineAttachments(r, people) {
  const out = [];
  for (const p of people) {
    try { out.push({ name: `timeline-${p.key}.png`, contentType: "image/png", base64: (await timelinePng(r, p)).toString("base64"), isInline: true, contentId: cidFor(p) }); }
    catch (e) { console.error("[call-report] timeline image failed for", p.key, e.message); }
  }
  return out;
}

export async function sendReport(r, { onlyTo, viewUrl = null } = {}) {
  if (!graphConfigured()) throw new Error("Graph mail not configured");
  const sent = [];
  const send = async (realTo, subject, html, attachments) => {
    const to = onlyTo || (isLive() ? realTo : testEmail());
    const subj = to.toLowerCase() === realTo.toLowerCase() ? subject : `[TEST - would go to ${realTo}] ${subject}`;
    // One failure must not stop the rest, and must not make the week re-send what did go.
    try { await sendNew({ to, subject: subj, html, attachments }); sent.push({ to, realTo, subject: subj }); }
    catch (e) { sent.push({ to, realTo, subject: subj, error: e.message }); }
  };
  await send(managerEmail(), `Sales activity - week of ${dayLabel(r.week)}`, managerEmailHtml(r, { viewUrl }), []);
  for (const p of r.people) await send(p.email, `Your week - ${dayLabel(r.week)} to ${dayLabel(r.weekEnd)}`, personEmailHtml(r, p), await timelineAttachments(r, [p]));
  if (sent.every((s) => s.error)) throw new Error(`No report email could be sent: ${sent[0] && sent[0].error}`);
  return sent;
}

// ---- routes + schedule ----------------------------------------------------------------
export function registerCallReport(app, { getPool, bpLive }) {
  const requireUser = (req, res, next) => app.locals.requireHubUser(req, res, next);
  const users = () => String(process.env.CALL_REPORT_USERS || "dec,dec clayton,declan,declan clayton").split(",").map((s) => s.trim().toLowerCase());
  const allowed = (u) => !!u && [u.key, u.name].some((v) => users().includes(String(v || "").trim().toLowerCase()));
  const guard = (req, res, next) => (allowed(req.hubUser) ? next() : res.status(403).json({ error: "Not available on your account." }));
  const weekOf = (q) => (/^\d{4}-\d{2}-\d{2}$/.test(String(q || "")) ? String(q) : previousWeekStart());

  // GET /call-report/view?week=&sig= - the manager report as a web page with working fold-outs.
  app.get("/call-report/view", async (req, res) => {
    try {
      const week = String(req.query.week || "");
      if (!/^\d{4}-\d{2}-\d{2}$/.test(week)) return res.status(400).send("Bad link");
      const want = signWeek(await reportSecret(getPool()), week), got = String(req.query.sig || "");
      if (got.length !== want.length || !crypto.timingSafeEqual(Buffer.from(got), Buffer.from(want))) return res.status(403).send("This link isn't valid.");
      const r = await buildReport({ pool: getPool(), bpLive, week, collect: false });
      const pics = {};
      for (const q of r.people) pics[q.key] = "data:image/png;base64," + (await timelinePng(r, q)).toString("base64");
      res.set({ "Cache-Control": "private, no-store", "X-Robots-Tag": "noindex", "Referrer-Policy": "no-referrer" });
      res.type("html").send(`<!doctype html><html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>Sales activity - ${esc(weekTitle(r))}</title></head>
<body style="margin:0;padding:24px;background:#f6f7f9"><div style="background:#fff;padding:20px 24px;border-radius:8px;max-width:960px;margin:0 auto">${managerEmailHtml(r, { imgSrc: (q) => pics[q.key] || "", web: true })}</div></body></html>`);
    } catch (e) { res.status(500).send("Could not build the report: " + esc(e.message)); }
  });

  // Pull missing Webex days in the background (each day takes ~2 minutes of rate limit).
  // ?from=&to= (YYYY-MM-DD), default the last 7 days. GET again to see progress.
  let collecting = null;
  app.get("/api/call-report/collect", requireUser, guard, async (req, res) => {
    if (collecting && !collecting.done) return res.json(collecting);
    const today = ukDay(new Date());
    const from = /^\d{4}-\d{2}-\d{2}$/.test(String(req.query.from || "")) ? String(req.query.from) : addDays(today, -7);
    const to = /^\d{4}-\d{2}-\d{2}$/.test(String(req.query.to || "")) ? String(req.query.to) : addDays(today, -1);
    if (req.query.status) return res.json(collecting || { idle: true });
    collecting = { from, to, started: new Date().toISOString(), done: false };
    collectMissing(getPool(), from, to)
      .then((days) => Object.assign(collecting, { done: true, days }))
      .catch((e) => Object.assign(collecting, { done: true, error: e.message }));
    res.json(collecting);
  });

  // Who the stored call records belong to (counts only, no numbers), and what the Webex
  // people lookup returns for each person, so a name/email mismatch can be seen.
  app.get("/api/call-report/who", requireUser, guard, async (req, res) => {
    try {
      const pool = getPool();
      await ensureTables(pool);
      const users = (await pool.query(`SELECT user_name, user_uuid, direction, call_type, count(*)::int n FROM webex_cdr
                                        GROUP BY 1,2,3,4 ORDER BY n DESC LIMIT 200`)).rows;
      const lookups = [];
      for (const p of reportPeople()) {
        try {
          const { json } = await webexGet(pool, `https://webexapis.com/v1/people?email=${encodeURIComponent(p.email)}`);
          lookups.push({ key: p.key, found: ((json && json.items) || []).map((x) => ({ name: x.displayName, uuid: uuidFromPersonId(x.id), emails: x.emails })) });
        } catch (e) { lookups.push({ key: p.key, error: e.message }); }
      }
      res.json({ lookups, users });
    } catch (e) { res.status(500).json({ error: e.message }); }
  });

  // ?week=YYYY-MM-DD (a Monday; default last week). ?html=manager|<person key> shows an email.
  // Built from the days already pulled; ?collect=1 pulls missing ones first (slow).
  app.get("/api/call-report/preview", requireUser, guard, async (req, res) => {
    try {
      const r = await buildReport({ pool: getPool(), bpLive, week: weekOf(req.query.week), collect: req.query.collect === "1" });
      if (req.query.html) {
        const p = r.people.find((x) => x.key === req.query.html);
        const pics = {};
        for (const q of (p ? [p] : r.people)) pics[q.key] = "data:image/png;base64," + (await timelinePng(r, q)).toString("base64");
        const imgSrc = (q) => pics[q.key] || "";
        return res.type("html").send(p ? personEmailHtml(r, p, { imgSrc }) : managerEmailHtml(r, { imgSrc }));
      }
      res.json(r);
    } catch (e) { res.status(500).json({ error: e.message }); }
  });
  // Sends every email of that week's report to ONE address (default the test address).
  app.get("/api/call-report/send-test", requireUser, guard, async (req, res) => {
    try {
      const r = await buildReport({ pool: getPool(), bpLive, week: weekOf(req.query.week), collect: false });
      res.json({ sent: await sendReport(r, { onlyTo: String(req.query.to || testEmail()), viewUrl: await viewLink(getPool(), r.week) }), missingCallDays: r.missingCallDays });
    } catch (e) { res.status(500).json({ error: e.message }); }
  });

  // Overnight (01:00-06:00 UK) pull of any finished day not yet stored; Monday 07:30-10:00
  // the previous week's emails, once (the week is claimed first, released on failure).
  let busy = false;
  setInterval(async () => {
    if (busy || String(process.env.CALL_REPORT_ENABLED || "").toLowerCase() !== "on" || !getPool()) return;
    const now = new Date();
    const hm = now.toLocaleTimeString("en-GB", { hour: "2-digit", minute: "2-digit", hour12: false, timeZone: TZ });
    const dow = now.toLocaleDateString("en-GB", { weekday: "short", timeZone: TZ });
    busy = true;
    try {
      const pool = getPool();
      await ensureTables(pool);
      if (hm >= "01:00" && hm < "06:00") {
        const today = ukDay(now);
        await collectMissing(pool, addDays(today, -8), addDays(today, -1));
      } else if (dow === "Mon" && hm >= "07:30" && hm < "10:00") {
        const week = previousWeekStart(now);
        const claim = await pool.query(`INSERT INTO call_report_runs (week) VALUES ($1) ON CONFLICT (week) DO NOTHING RETURNING week`, [week]);
        if (claim.rowCount) {
          try {
            const r = await buildReport({ pool, bpLive, week });
            const sent = await sendReport(r, { viewUrl: await viewLink(pool, week) });
            await pool.query(`UPDATE call_report_runs SET result = $2 WHERE week = $1`, [week, JSON.stringify({ sent, missingCallDays: r.missingCallDays })]);
            console.log("[call-report]", week, "sent", sent.length);
          } catch (e) {
            console.error("[call-report] run failed (will retry):", e.message);
            await pool.query(`DELETE FROM call_report_runs WHERE week = $1`, [week]);
          }
        }
      }
    } catch (e) { console.error("[call-report] scheduler:", e.message); }
    finally { busy = false; }
  }, 5 * 60 * 1000);
}
