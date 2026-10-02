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
// What counts (the Webex record is one row per person per call leg):
//   - internal calls (Call type SIP_ENTERPRISE) are left out of everything;
//   - a hunt-group call rings several people: only the person who ANSWERED gets it, so the
//     unanswered legs on everyone else's phone are not counted as anything;
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

import { graphConfigured, sendNew } from "./graphMail.js";

const DEFAULT_PEOPLE = [
  { key: "nicky", name: "Nicky Everall", first: "Nicky", email: "nicky@tuffshop.co.uk", bpId: 61342 },
  { key: "jack", name: "Jack Ellis-Haynes", first: "Jack", email: "jack@tuffshop.co.uk", bpId: 82710 },
  { key: "helen", name: "Helen Jackson", first: "Helen", email: "helen@tuffshop.co.uk", bpId: 59339 },
  { key: "bob", name: "Robert Lodge", first: "Bob", email: "bob@tuffshop.co.uk", bpId: 445 },
  { key: "laura", name: "Laura Jackson", first: "Laura", email: "laura@tuffshop.co.uk", bpId: 137062 },
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
  await pool.query(`CREATE TABLE IF NOT EXISTS webex_cdr_days (day date PRIMARY KEY, fetched_at timestamptz NOT NULL DEFAULT now(), records int)`);
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
          `INSERT INTO webex_cdr (id, day, user_uuid, user_name, direction, answered, duration, call_type, start_time)
           VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9) ON CONFLICT (id) DO NOTHING`,
          [id, start ? ukDay(start) : day, String(field(rec, "User UUID", "userUuid") || "").toLowerCase(), field(rec, "User", "user"),
           field(rec, "Direction", "direction"), String(field(rec, "Answered", "answered")).toLowerCase() === "true",
           Number(field(rec, "Duration", "duration") || 0), field(rec, "Call type", "callType"), start]);
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
  const counts = {};
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
    }
    if (!r.metaData.morePagesAvailable) break;
    firstResult = r.metaData.lastResult + 1;
  }
  return counts;
}

// ---- the report -----------------------------------------------------------------------
const blank = () => ({ callsIn: 0, callsOut: 0, talk: 0, orders: 0 });

export function tallyCalls(rows, uuidToKey, nameToKey = {}) {
  const out = {};
  for (const r of rows) {
    if (r.call_type === INTERNAL) continue;
    const key = uuidToKey[String(r.user_uuid || "").toLowerCase()] || nameToKey[String(r.user_name || "").trim().toLowerCase()];
    if (!key) continue;
    const day = typeof r.day === "string" ? r.day.slice(0, 10) : ukDay(r.day);
    const t = ((out[key] = out[key] || {})[day] = out[key][day] || blank());
    const dir = String(r.direction || "").toUpperCase();
    if (dir === "TERMINATING") { if (r.answered) { t.callsIn++; t.talk += r.duration || 0; } }
    else if (dir === "ORIGINATING") { t.callsOut++; if (r.answered) t.talk += r.duration || 0; }
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
  const nameToKey = Object.fromEntries(people.map((p) => [p.name.toLowerCase(), p.key]));
  const rows = (await pool.query(`SELECT to_char(day,'YYYY-MM-DD') AS day, user_uuid, user_name, direction, answered, duration, call_type
                                    FROM webex_cdr WHERE day BETWEEN $1 AND $2`, [first, last])).rows;
  const calls = tallyCalls(rows, uuidToKey, nameToKey);
  const orders = await ordersCreated(bpLive, people, first, last);
  const daysWithData = new Set((await pool.query(`SELECT to_char(day,'YYYY-MM-DD') d FROM webex_cdr_days WHERE day BETWEEN $1 AND $2`, [first, last])).rows.map((r) => r.d));

  const days = Array.from({ length: 7 }, (_, i) => addDays(first, i));
  const report = people.map((p) => {
    const perDay = days.map((day) => ({ day, ...blank(), ...((calls[p.key] || {})[day] || {}), orders: (orders[p.key] || {})[day] || 0, noCallData: !daysWithData.has(day) }));
    const total = perDay.reduce((a, d) => ({ callsIn: a.callsIn + d.callsIn, callsOut: a.callsOut + d.callsOut, talk: a.talk + d.talk, orders: a.orders + d.orders }), blank());
    return { ...p, webexMatched: !!uuids[p.key], days: perDay, total };
  });
  return { week: first, weekEnd: last, people: report, collected, missingCallDays: days.filter((d) => !daysWithData.has(d)) };
}

// ---- emails ---------------------------------------------------------------------------
const esc = (s) => String(s ?? "").replace(/[&<>"]/g, (c) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;" }[c]));
const TH = 'style="text-align:left;padding:6px 10px;background:#1f2a37;color:#fff;font-weight:600;font-size:13px"';
const TD = 'style="padding:6px 10px;border-bottom:1px solid #e5e7eb;font-size:13px"';
const TDN = 'style="padding:6px 10px;border-bottom:1px solid #e5e7eb;font-size:13px;text-align:right"';
const TOT = 'style="padding:7px 10px;font-weight:700;font-size:13px;border-top:2px solid #1f2a37;text-align:right"';
const weekTitle = (r) => `${dayLabel(r.week)} - ${dayLabel(r.weekEnd)}`;
const shown = (p) => p.days.filter((d, i) => i < 5 || d.callsIn || d.callsOut || d.orders);   // weekends only if used

export function personEmailHtml(r, p) {
  const rows = shown(p).map((d) => `<tr><td ${TD}>${esc(dayLabel(d.day))}</td>
    <td ${TDN}>${d.noCallData ? "-" : d.callsIn}</td><td ${TDN}>${d.noCallData ? "-" : d.callsOut}</td>
    <td ${TDN}>${d.noCallData ? "-" : fmtDuration(d.talk)}</td><td ${TDN}>${d.orders}</td></tr>`).join("");
  return `<div style="font-family:Segoe UI,Arial,sans-serif;color:#111;max-width:640px">
  <p>Hi ${esc(p.first)},</p>
  <p>Here's your week on the phones and in Brightpearl, ${esc(weekTitle(r))}.</p>
  <table style="border-collapse:collapse;width:100%">
    <tr><th ${TH}>Day</th><th ${TH} align="right">Calls in</th><th ${TH} align="right">Calls out</th><th ${TH} align="right">Time on phone</th><th ${TH} align="right">Orders created</th></tr>
    ${rows}
    <tr><td style="padding:7px 10px;font-weight:700;font-size:13px;border-top:2px solid #1f2a37">Week</td>
      <td ${TOT}>${p.total.callsIn}</td><td ${TOT}>${p.total.callsOut}</td><td ${TOT}>${fmtDuration(p.total.talk)}</td><td ${TOT}>${p.total.orders}</td></tr>
  </table>
  <p style="color:#6b7280;font-size:12px;margin-top:14px">Calls in are calls you answered; calls out are calls you made. Calls between colleagues aren't counted.
  Orders are sales orders you created in Brightpearl (web, Amazon and eBay orders aren't included).${r.missingCallDays.length ? " A dash means there's no call data for that day." : ""}</p>
</div>`;
}

export function managerEmailHtml(r) {
  const people = r.people.map((p) => `<tr><td ${TD}>${esc(p.name)}${p.webexMatched ? "" : ' <span style="color:#b91c1c">(not found in Webex)</span>'}</td>
    <td ${TDN}>${p.total.callsIn}</td><td ${TDN}>${p.total.callsOut}</td><td ${TDN}>${fmtDuration(p.total.talk)}</td><td ${TDN}>${p.total.orders}</td></tr>`).join("");
  const sum = r.people.reduce((a, p) => ({ callsIn: a.callsIn + p.total.callsIn, callsOut: a.callsOut + p.total.callsOut, talk: a.talk + p.total.talk, orders: a.orders + p.total.orders }), blank());
  const daily = r.people.map((p) => `<h3 style="font-size:14px;margin:18px 0 6px">${esc(p.name)}</h3>
  <table style="border-collapse:collapse;width:100%"><tr><th ${TH}>Day</th><th ${TH}>In</th><th ${TH}>Out</th><th ${TH}>On phone</th><th ${TH}>Orders</th></tr>
  ${shown(p).map((d) => `<tr><td ${TD}>${esc(dayLabel(d.day))}</td><td ${TDN}>${d.noCallData ? "-" : d.callsIn}</td><td ${TDN}>${d.noCallData ? "-" : d.callsOut}</td><td ${TDN}>${d.noCallData ? "-" : fmtDuration(d.talk)}</td><td ${TDN}>${d.orders}</td></tr>`).join("")}</table>`).join("");
  return `<div style="font-family:Segoe UI,Arial,sans-serif;color:#111;max-width:680px">
  <p>Sales activity for ${esc(weekTitle(r))}.</p>
  <table style="border-collapse:collapse;width:100%">
    <tr><th ${TH}>Person</th><th ${TH}>Calls in</th><th ${TH}>Calls out</th><th ${TH}>Time on phone</th><th ${TH}>Orders created</th></tr>
    ${people}
    <tr><td style="padding:7px 10px;font-weight:700;font-size:13px;border-top:2px solid #1f2a37">Team</td>
      <td ${TOT}>${sum.callsIn}</td><td ${TOT}>${sum.callsOut}</td><td ${TOT}>${fmtDuration(sum.talk)}</td><td ${TOT}>${sum.orders}</td></tr>
  </table>
  ${r.missingCallDays.length ? `<p style="color:#b45309;font-size:12px">No Webex call data for: ${r.missingCallDays.map(dayLabel).join(", ")}.</p>` : ""}
  ${daily}
  <p style="color:#6b7280;font-size:12px;margin-top:14px">Internal calls excluded; hunt-group calls count only for whoever answered. Orders = sales orders created by that person in Brightpearl, excluding web/Amazon/eBay.</p>
</div>`;
}

export async function sendReport(r, { onlyTo } = {}) {
  if (!graphConfigured()) throw new Error("Graph mail not configured");
  const sent = [];
  const send = async (realTo, subject, html) => {
    const to = onlyTo || (isLive() ? realTo : testEmail());
    const subj = to.toLowerCase() === realTo.toLowerCase() ? subject : `[TEST - would go to ${realTo}] ${subject}`;
    // One failure must not stop the rest, and must not make the week re-send what did go.
    try { await sendNew({ to, subject: subj, html }); sent.push({ to, realTo, subject: subj }); }
    catch (e) { sent.push({ to, realTo, subject: subj, error: e.message }); }
  };
  await send(managerEmail(), `Sales activity - week of ${dayLabel(r.week)}`, managerEmailHtml(r));
  for (const p of r.people) await send(p.email, `Your week - ${dayLabel(r.week)} to ${dayLabel(r.weekEnd)}`, personEmailHtml(r, p));
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
        return res.type("html").send(p ? personEmailHtml(r, p) : managerEmailHtml(r));
      }
      res.json(r);
    } catch (e) { res.status(500).json({ error: e.message }); }
  });
  // Sends every email of that week's report to ONE address (default the test address).
  app.get("/api/call-report/send-test", requireUser, guard, async (req, res) => {
    try {
      const r = await buildReport({ pool: getPool(), bpLive, week: weekOf(req.query.week), collect: false });
      res.json({ sent: await sendReport(r, { onlyTo: String(req.query.to || testEmail()) }), missingCallDays: r.missingCallDays });
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
            const sent = await sendReport(r);
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
