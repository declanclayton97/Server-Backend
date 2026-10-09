// callReport.js — the weekly sales activity report (Dec, 2 Oct 2026).
//
// Monday morning, for the previous Mon-Sun, per salesperson, per day:
//   calls in      answered inbound calls from outside (Webex Calling)
//   calls out     outbound calls dialled to outside
//   time on phone talk time across both
//   orders        Brightpearl sales orders that person created by hand
//   emails        customer emails received / sent in their own mailbox (Graph, 7 Oct)
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
import { graphConfigured, sendNew, graphRequest } from "./graphMail.js";

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
// Orders and RETURNS each person created by hand (Dec, 7 Oct). A return is either:
//   - an exchange order: a SALES order whose reference starts "EXCHANGE ORDER" (it replaces goods,
//     so it is NOT a new sale and is no longer counted as an order), or
//   - a sales credit (orderTypeId 3) - the return itself.
// One person booking both halves of the same return ("EXCHANGE ORDER CREDITED VIA SC#n" where
// they also created SC n) counts once. Integration-made orders are nobody's work and are skipped.
async function ordersCreated(bpLive, people, first, last) {
  const ids = new Map(people.map((p) => [Number(p.bpId), p.key]));
  const counts = {}, times = {}, returnCounts = {}, returnTimes = {};
  const add = (cnt, tms, key, created, id) => {
    const day = String(created).slice(0, 10);                  // Brightpearl gives UK local time
    (cnt[key] = cnt[key] || {})[day] = (cnt[key][day] || 0) + 1;
    const hm = /T(\d{2}):(\d{2})/.exec(String(created));     // so the clock part is the time
    if (hm) (tms[key] = tms[key] || []).push({ day, min: Number(hm[1]) * 60 + Number(hm[2]), id });
  };
  async function each(typeId, fn) {
    let firstResult = 1;
    for (;;) {
      const r = await bpLive("GET", `/order-service/order-search?orderTypeId=${typeId}&createdOn=${first}T00:00:00/${last}T23:59:59&pageSize=500&firstResult=${firstResult}`);
      const cols = r.metaData.columns.map((c) => c.name);
      const ix = (n) => cols.indexOf(n);
      for (const row of r.results) {
        const key = ids.get(Number(row[ix("createdById")]));
        if (!key || row[ix("installedIntegrationInstanceId")] != null) continue;
        fn(key, row[ix("createdOn")], row[ix("orderId")], String(row[ix("customerRef")] || ""));
      }
      if (!r.metaData.morePagesAvailable) break;
      firstResult = r.metaData.lastResult + 1;
    }
  }
  const creditsBy = {};                                         // person -> Set of SC ids they made
  await each(3, (key, created, id) => { add(returnCounts, returnTimes, key, created, id); (creditsBy[key] = creditsBy[key] || new Set()).add(Number(id)); });
  await each(1, (key, created, id, ref) => {
    if (!/^\s*EXCHANGE\b/i.test(ref)) return add(counts, times, key, created, id);
    const sc = Number((/SC#\s*(\d+)/i.exec(ref) || [])[1]);
    if (sc && creditsBy[key] && creditsBy[key].has(sc)) return;  // same return, already counted
    add(returnCounts, returnTimes, key, created, id);
  });
  return { counts, times, returnCounts, returnTimes };
}


// ---- emails ---------------------------------------------------------------------------
// Each person's OWN mailbox (nicky@...), read through Graph with the same app as the Sales Hub
// (Exchange scoping grants it these boxes). Only customer mail counts (Dec, 7 Oct):
//   received: anything that arrived in the week, in any folder (so mail filed or deleted
//             still counts), except junk, drafts, their own sent mail, mail from a colleague
//             (@tuffshop.co.uk) and automated senders (no-reply, notifications, bounces).
//   sent:     Sent Items in the week with at least one customer recipient.
// A mailbox we cannot read gives an error for that person, never a zero.
const OURS = /@tuffshop\.co\.uk$/i;
const AUTOMATED = /^(no-?reply|do-?not-?reply|notifications?|notify|alerts?|mailer-daemon|postmaster|bounces?|newsletters?|marketing)[@.+-]|@(.+\.)?(mailchimp|mcsv|hubspot|hubspotemail|sendgrid|amazonses|mailgun|klaviyo)\./i;
const GRAPH_ROOT = "https://graph.microsoft.com/v1.0";
async function graphAll(path) {
  const out = [];
  for (let url = path, pages = 0; url && pages < 40; pages++) {
    const j = await graphRequest("GET", url);
    out.push(...((j && j.value) || []));
    url = j && j["@odata.nextLink"] ? j["@odata.nextLink"].replace(GRAPH_ROOT, "") : null;
  }
  return out;
}
export async function emailActivity(people, first, last) {
  const from = ukMidnight(first).toISOString(), to = ukMidnight(addDays(last, 1)).toISOString();
  const customer = (a) => !!a && !OURS.test(a) && !AUTOMATED.test(a);
  const out = {};
  for (const p of people) {
    if (!p.email) continue;
    const box = `/users/${encodeURIComponent(p.email)}`;
    const me = p.email.toLowerCase();
    try {
      const skip = new Set();
      for (const f of ["junkemail", "drafts", "sentitems"]) {
        try { skip.add((await graphRequest("GET", `${box}/mailFolders/${f}?$select=id`)).id); } catch { /* folder missing */ }
      }
      const inF = encodeURIComponent(`receivedDateTime ge ${from} and receivedDateTime lt ${to}`);
      const got = await graphAll(`${box}/messages?$filter=${inF}&$select=receivedDateTime,parentFolderId,from,isDraft&$top=500`);
      const outF = encodeURIComponent(`sentDateTime ge ${from} and sentDateTime lt ${to}`);
      const sent = await graphAll(`${box}/mailFolders/sentitems/messages?$filter=${outF}&$select=sentDateTime,toRecipients,ccRecipients&$top=500`);
      const r = { in: {}, out: {}, sentTimes: [] };
      for (const m of got) {
        const sender = String((m.from && m.from.emailAddress && m.from.emailAddress.address) || "").toLowerCase();
        if (m.isDraft || skip.has(m.parentFolderId) || sender === me || !customer(sender)) continue;
        const day = ukDay(m.receivedDateTime);
        r.in[day] = (r.in[day] || 0) + 1;
      }
      for (const m of sent) {
        const rcpt = [...(m.toRecipients || []), ...(m.ccRecipients || [])].map((x) => String((x.emailAddress && x.emailAddress.address) || "").toLowerCase());
        if (!rcpt.some(customer)) continue;
        const day = ukDay(m.sentDateTime);
        r.out[day] = (r.out[day] || 0) + 1;
        r.sentTimes.push({ day, min: ukMinutes(m.sentDateTime) });
      }
      out[p.key] = r;
    } catch (e) {
      out[p.key] = { error: e.status === 403 ? "no access to " + p.email : e.message };
    }
  }
  return out;
}

// ---- the report -----------------------------------------------------------------------
const blank = () => ({ callsIn: 0, callsOut: 0, talk: 0, orders: 0, returns: 0, emailsIn: 0, emailsOut: 0 });

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
  const { counts: orders, times: orderTimes, returnCounts, returnTimes } = await ordersCreated(bpLive, people, first, last);
  const mail = graphConfigured() ? await emailActivity(people, first, last).catch((e) => ({ _error: e.message })) : {};
  const daysWithData = new Set((await pool.query(`SELECT to_char(day,'YYYY-MM-DD') d FROM webex_cdr_days WHERE day BETWEEN $1 AND $2`, [first, last])).rows.map((r) => r.d));

  const days = Array.from({ length: 7 }, (_, i) => addDays(first, i));
  const report = people.map((p) => {
    const m = mail[p.key] || { error: mail._error || "mailbox not read" };
    const perDay = days.map((day) => ({ day, ...blank(), ...((calls[p.key] || {})[day] || {}), orders: (orders[p.key] || {})[day] || 0, returns: (returnCounts[p.key] || {})[day] || 0,
      emailsIn: m.error ? 0 : m.in[day] || 0, emailsOut: m.error ? 0 : m.out[day] || 0, noCallData: !daysWithData.has(day), noEmailData: !!m.error }));
    const total = perDay.reduce((a, d) => ({ callsIn: a.callsIn + d.callsIn, callsOut: a.callsOut + d.callsOut, talk: a.talk + d.talk, orders: a.orders + d.orders, returns: a.returns + d.returns, emailsIn: a.emailsIn + d.emailsIn, emailsOut: a.emailsOut + d.emailsOut }), blank());
    const inWeek = (e) => e.day >= first && e.day <= last;
    return { ...p, emailError: m.error || null, webexMatched: !!uuids[p.key] || seenNames.has(String(p.webexName || p.name).toLowerCase()), days: perDay, total,
      timeline: { calls: (events[p.key] || []).filter(inWeek), orders: (orderTimes[p.key] || []).filter(inWeek), returns: (returnTimes[p.key] || []).filter(inWeek), emails: (m.sentTimes || []).filter(inWeek) } };
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
const shown = (p) => p.days.filter((d, i) => i < 5 || d.callsIn || d.callsOut || d.orders || d.returns || d.emailsOut);   // weekends only if used

// ---- timeline -------------------------------------------------------------------------
// One chart per person: a row per day (Mon-Fri, plus a weekend day only if used), 07:00 to
// 18:00. Each call is a bar as long as the call (green in, blue out; a call out nobody answered
// is a thin grey tick); each order created is an orange line, each return/exchange a purple one.
// Sent as an image because no email client draws charts from HTML reliably.
const TL_FROM = 7 * 60, TL_TO = 18 * 60;
const TL = { left: 92, right: 26, top: 52, rowH: 34, width: 900 };
const WEB_TL_WIDTH = 1400;
export function timelineSvg(r, p, width = TL.width) {
  // The web page draws it wider (Dec, 7 Oct) with taller rows; emails keep 900 for the reading pane.
  const T = { ...TL, width, rowH: width > TL.width ? 44 : TL.rowH };
  const days = shown(p).map((d) => d.day);
  const plotW = T.width - T.left - T.right;
  const x = (min) => T.left + ((Math.min(Math.max(min, TL_FROM), TL_TO) - TL_FROM) / (TL_TO - TL_FROM)) * plotW;
  const height = T.top + days.length * T.rowH + 40;
  let out = `<svg xmlns="http://www.w3.org/2000/svg" width="${T.width}" height="${height}" viewBox="0 0 ${T.width} ${height}" font-family="DejaVu Sans">`;
  out += `<rect width="${T.width}" height="${height}" fill="#ffffff"/>`;
  out += `<text x="12" y="22" font-size="15" font-weight="700" fill="#111">${esc(p.name)} - ${esc(weekTitle(r))}</text>`;
  if (width > TL.width) for (let h = 7; h < 18; h++) {   // half-hour lines on the wide (web) version
    const hx = x(h * 60 + 30);
    out += `<line x1="${hx}" y1="${T.top}" x2="${hx}" y2="${T.top + days.length * T.rowH}" stroke="#eef0f2" stroke-dasharray="3,3"/>`;
  }
  for (let h = 7; h <= 18; h++) {
    const hx = x(h * 60);
    out += `<line x1="${hx}" y1="${T.top - 6}" x2="${hx}" y2="${T.top + days.length * T.rowH}" stroke="${h % 3 === 0 ? "#c7ccd3" : "#e7e9ec"}"/>`;
    out += `<text x="${hx}" y="${T.top - 12}" font-size="11" text-anchor="middle" fill="#6b7280">${String(h).padStart(2, "0")}:00</text>`;
  }
  days.forEach((day, i) => {
    const y = T.top + i * T.rowH;
    out += `<rect x="${T.left}" y="${y + 4}" width="${plotW}" height="${T.rowH - 8}" fill="${i % 2 ? "#fafafa" : "#f4f6f8"}"/>`;
    out += `<text x="12" y="${y + T.rowH / 2 + 4}" font-size="12" fill="#111">${esc(dayLabel(day))}</text>`;
    for (const c of p.timeline.calls.filter((c) => c.day === day)) {
      const x1 = x(c.min), x2 = x(c.min + c.dur / 60);
      if (!c.answered) { out += `<rect x="${x1}" y="${y + 10}" width="1.5" height="${T.rowH - 20}" fill="#9ca3af"/>`; continue; }
      out += `<rect x="${x1}" y="${y + 8}" width="${Math.max(2, x2 - x1)}" height="${T.rowH - 16}" rx="1" fill="${c.kind === "in" ? "#16a34a" : "#2563eb"}" fill-opacity="0.85"/>`;
    }
    for (const e of (p.timeline.emails || []).filter((e) => e.day === day)) {
      out += `<rect x="${x(e.min) - 0.75}" y="${y + T.rowH - 12}" width="2" height="9" fill="#0d9488"/>`;
    }
    for (const o of (p.timeline.returns || []).filter((o) => o.day === day)) {
      const ox = x(o.min);
      out += `<rect x="${ox - 1.25}" y="${y + 3}" width="2.5" height="${T.rowH - 6}" fill="#7c3aed"/>`;
    }
    for (const o of p.timeline.orders.filter((o) => o.day === day)) {
      const ox = x(o.min);
      out += `<rect x="${ox - 1.25}" y="${y + 3}" width="2.5" height="${T.rowH - 6}" fill="#ea580c"/>`;
    }
  });
  const ly = T.top + days.length * T.rowH + 24;
  // Each entry is spaced by its own label length (DejaVu 11px ~6.8px a character) so the
  // whole key fits the picture's width (it ran off the edge once "Email sent" was added).
  const key = [
    [`<rect x="0" y="-9" width="14" height="10" fill="#16a34a"/>`, "Call in (answered)"],
    [`<rect x="0" y="-9" width="14" height="10" fill="#2563eb"/>`, "Call out"],
    [`<rect x="0" y="-9" width="14" height="10" fill="#9ca3af"/>`, "Call out, no answer"],
    [`<rect x="6" y="-12" width="2.5" height="15" fill="#ea580c"/>`, "Order created"],
    [`<rect x="6" y="-12" width="2.5" height="15" fill="#7c3aed"/>`, "Return / exchange"],
    [`<rect x="6" y="-8" width="2" height="9" fill="#0d9488"/>`, "Email sent"],
  ];
  let lx = 12;
  for (const [mark, label] of key) {
    out += `<g transform="translate(${lx},${ly})">${mark}<text x="20" y="0" font-size="11" fill="#374151">${label}</text></g>`;
    lx += 20 + Math.ceil(label.length * 6.8) + 22;
  }
  return out + "</svg>";
}
let tlFont = null;
export async function timelinePng(r, p, width = TL.width) {
  const { Resvg } = await import("@resvg/resvg-js");
  if (!tlFont) tlFont = fs.readFileSync(path.join(path.dirname(fileURLToPath(import.meta.url)), "assets", "table-font.ttf"));
  const png = new Resvg(timelineSvg(r, p, width), { fitTo: { mode: "width", value: width * 2 }, font: { fontBuffers: [tlFont], defaultFontFamily: "DejaVu Sans", loadSystemFonts: false } }).render().asPng();
  return Buffer.from(png);
}
// The picture's address in an email (cid:) or in the browser preview (data:).
const cidFor = (p) => `timeline-${p.key}@callreport`;

const dayRows = (p) => shown(p).map((d) => `<tr><td ${TD}>${esc(dayLabel(d.day))}</td>
    <td ${TDN}>${d.noCallData ? "-" : d.callsIn}</td><td ${TDN}>${d.noCallData ? "-" : d.callsOut}</td>
    <td ${TDN}>${d.noCallData ? "-" : fmtDuration(d.talk)}</td><td ${TDN}>${d.orders}</td><td ${TDN}>${d.returns}</td>
    <td ${TDN}>${d.noEmailData ? "-" : d.emailsIn}</td><td ${TDN}>${d.noEmailData ? "-" : d.emailsOut}</td></tr>`).join("");
const mailCells = (p, s) => p.emailError ? `<td ${s}>-</td><td ${s}>-</td>` : `<td ${s}>${p.total.emailsIn}</td><td ${s}>${p.total.emailsOut}</td>`;
const timelineImg = (p, src, w = TL.width) => `<img src="${src(p)}" width="${w}" alt="Timeline of calls and orders, 7am to 6pm" style="display:block;width:100%;max-width:${w}px;height:auto;border:1px solid #e5e7eb;margin-top:10px">`;

export function personEmailHtml(r, p, { imgSrc = (q) => "cid:" + cidFor(q) } = {}) {
  return `<div style="font-family:Segoe UI,Arial,sans-serif;color:#111;max-width:${TL.width}px">
  <p>Hi ${esc(p.first)},</p>
  <p>Here's your week on the phones and in Brightpearl, ${esc(weekTitle(r))}.</p>
  <table style="border-collapse:collapse;width:100%">
    <tr><th ${TH}>Day</th><th ${THN}>Calls in</th><th ${THN}>Calls out</th><th ${THN}>Time on phone</th><th ${THN}>Orders created</th><th ${THN}>Returns</th><th ${THN}>Emails in</th><th ${THN}>Emails out</th></tr>
    ${dayRows(p)}
    <tr><td style="padding:7px 10px;font-weight:700;font-size:13px;border-top:2px solid #1f2a37">Week</td>
      <td ${TOT}>${p.total.callsIn}</td><td ${TOT}>${p.total.callsOut}</td><td ${TOT}>${fmtDuration(p.total.talk)}</td><td ${TOT}>${p.total.orders}</td><td ${TOT}>${p.total.returns}</td>${mailCells(p, TOT)}</tr>
  </table>
  <p style="font-size:13px;margin:18px 0 0"><b>Your days, 7am to 6pm</b></p>
  ${timelineImg(p, imgSrc)}
  <p style="color:#6b7280;font-size:12px;margin-top:14px">Calls in are calls you answered; calls out are calls you made. Calls between colleagues aren't counted.
  Orders are sales orders you created in Brightpearl (web, Amazon and eBay orders aren't included); returns are the exchange orders and credit notes you booked. Emails are customer emails in and out of your own mailbox (emails between colleagues and automated emails aren't counted).${r.missingCallDays.length ? " A dash means there's no call data for that day." : ""}</p>
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
const publicBase = () => (process.env.PUBLIC_BASE_URL || "https://server-backend-1i47.onrender.com").replace(/\/$/, "");
export async function viewLink(pool, week) {
  return `${publicBase()}/call-report/view?week=${week}&sig=${signWeek(await reportSecret(pool), week)}`;
}

// ---- call recordings (Dec, 8 Oct) -------------------------------------------------------
// Webex Calling records into Webex's own storage. The Service App reads them as a Compliance
// Officer: spark-admin:recordings_read lists them, but only spark-compliance:recordings_read
// gets the audio download link (and only a full admin who is ALSO a compliance officer can
// approve that scope - a full admin cannot give themselves the role; another admin must).
// Recordings are NOT part of the weekly report (Dec, 8 Oct): they exist to turn a call into
// order notes. call_recordings keeps who / when / who with; the audio stays in Webex. A replay link points
// at OUR server, signed per recording, which asks Webex for a fresh download link when it is
// opened - Webex's own links expire within hours.
async function ensureRecordingTables(pool) {
  await pool.query(`CREATE TABLE IF NOT EXISTS call_recordings (
      id text PRIMARY KEY, owner_email text, created timestamptz NOT NULL, day date NOT NULL,
      duration int, topic text, other_party text, session_id text,
      transcript text, summary text, summarized_at timestamptz, seen_at timestamptz NOT NULL DEFAULT now())`);
  await pool.query(`CREATE INDEX IF NOT EXISTS call_recordings_day ON call_recordings (day)`);
  // A recording wiped on request (e.g. card details read out, 8 Oct): kept as a row so the sync
  // never re-imports it, but with no transcript/summary and no playback.
  await pool.query(`ALTER TABLE call_recordings ADD COLUMN IF NOT EXISTS purged_at timestamptz`);
  // Which customer / order the call was about (matchRecording).
  for (const col of ["caller_number text", "contact_id bigint", "contact_name text", "order_id bigint", "match_json jsonb", "matched_at timestamptz"])
    await pool.query(`ALTER TABLE call_recordings ADD COLUMN IF NOT EXISTS ${col}`);
}
// "Call with Pelican Works 2-20261007 1543" -> "Pelican Works 2"
const otherParty = (topic) => String(topic || "").replace(/^Call with\s+/i, "").replace(/-\d{8}\s+\d{4}$/, "").trim();
export async function syncRecordings(pool, days = 3) {
  await ensureRecordingTables(pool);
  const from = new Date(Date.now() - days * 86400e3).toISOString(), to = new Date().toISOString();
  // The org-wide list only returned the AUTHORISING admin's own recordings (8 Oct: 12 of
  // Dec's, none of the sales phones', while asking per owner found Helen's). So the list is
  // asked once per owner - every report person plus CALL_RECORDING_OWNERS - and once unfiltered.
  const owners = [...new Set([...reportPeople().map((p) => p.email),
    ...String(process.env.CALL_RECORDING_OWNERS || "").split(",")].map((e) => String(e || "").trim().toLowerCase()).filter(Boolean))];
  const seen = new Set();
  let n = 0;
  for (const owner of [null, ...owners]) {
    let url = `https://webexapis.com/v1/admin/convergedRecordings?from=${from}&to=${to}&max=100${owner ? `&ownerEmail=${encodeURIComponent(owner)}` : ""}`;
    while (url) {
      const { json, next } = await webexGet(pool, url);
      for (const x of (json && json.items) || []) {
        if (seen.has(x.id)) continue;
        seen.add(x.id);
        if ((x.serviceType && x.serviceType !== "calling") || (x.status && x.status !== "available")) continue;
        await pool.query(
          `INSERT INTO call_recordings (id, owner_email, created, day, duration, topic, other_party, session_id)
           VALUES ($1,$2,$3,$4,$5,$6,$7,$8) ON CONFLICT (id) DO UPDATE SET duration = EXCLUDED.duration`,
          [x.id, String(x.ownerEmail || owner || "").toLowerCase(), x.createTime, ukDay(x.createTime), x.durationSeconds || 0,
           x.topic || "", otherParty(x.topic), (x.serviceData && x.serviceData.callSessionId) || null]);
        n++;
      }
      url = next;
    }
  }
  await transcribePending(pool).catch((e) => console.error("[call-report] transcripts:", e.message));
  return n;
}

// Webex writes the transcript some minutes after the call (Dec switched transcription on, 8
// Oct). Each recording from the last 3 days without one is re-asked at most every 10 minutes;
// once a transcript arrives, Claude turns it into a summary + an order note.
const summariesOn = () => String(process.env.CALL_SUMMARIES || "").toLowerCase() === "on";
async function transcribePending(pool, limit = 10) {
  await pool.query(`ALTER TABLE call_recordings ADD COLUMN IF NOT EXISTS transcript_checked_at timestamptz`);
  await pool.query(`ALTER TABLE call_recordings ADD COLUMN IF NOT EXISTS summary_json jsonb`);
  await pool.query(`ALTER TABLE call_recordings ADD COLUMN IF NOT EXISTS webex_notes text`);
  const todo = (await pool.query(
    `SELECT id FROM call_recordings
      WHERE transcript IS NULL AND purged_at IS NULL AND created > now() - interval '3 days' AND duration >= 15
        AND (transcript_checked_at IS NULL OR transcript_checked_at < now() - interval '10 minutes')
      ORDER BY created DESC LIMIT $1`, [limit])).rows;
  for (const { id } of todo) {
    await pool.query(`UPDATE call_recordings SET transcript_checked_at = now() WHERE id = $1`, [id]);
    const { json } = await webexGet(pool, `https://webexapis.com/v1/convergedRecordings/${encodeURIComponent(id)}`);
    const links = (json && json.temporaryDirectDownloadLinks) || {};
    const grab = async (u) => { if (!u) return ""; try { const r = await fetch(u); return r.ok ? (await r.text()).trim() : ""; } catch { return ""; } };
    // Webex's own AI summary ("suggested notes") and action items, kept alongside ours.
    const notes = await grab(links.suggestedNotesDownloadLink), actions = await grab(links.actionItemsDownloadLink);
    if (notes || actions) await pool.query(`UPDATE call_recordings SET webex_notes = $2 WHERE id = $1`, [id, [notes, actions && "Action items:\n" + actions].filter(Boolean).join("\n\n")]);
    const text = vttToText(await grab(links.transcriptDownloadLink));
    if (!text) continue;
    await pool.query(`UPDATE call_recordings SET transcript = $2 WHERE id = $1`, [id, fixKnownMishearings(text)]);
    if (summariesOn()) await summariseRecording(pool, id).catch((e) => console.error("[call-report] summary", id, e.message));
  }
  // Match calls to customers/orders: new calls, and once more when the transcript arrives.
  for (const { id } of (await pool.query(`SELECT id FROM call_recordings
      WHERE purged_at IS NULL AND created > now() - interval '3 days'
        AND (matched_at IS NULL OR (transcript IS NOT NULL AND COALESCE(match_json->>'hadTranscript', 'false') <> 'true'))
      ORDER BY created DESC LIMIT 10`)).rows) {
    await matchRecording(pool, id).catch((e) => console.error("[call-report] match", id, e.message));
  }
  // Summaries cost per call, so they are switched on separately from recording (Dec, 8 Oct:
  // record now, summarise later). Off, transcripts are still collected (free) and wait here.
  if (!summariesOn()) return;
  // Transcripts that arrived but were never summarised (Claude down, key missing at the time).
  for (const { id } of (await pool.query(`SELECT id FROM call_recordings WHERE transcript IS NOT NULL AND summary IS NULL AND created > now() - interval '3 days' LIMIT 5`)).rows) {
    await summariseRecording(pool, id).catch((e) => console.error("[call-report] summary", id, e.message));
  }
}
// WebVTT ("00:00:01.000 --> 00:00:04.000" cues, "<v Speaker>" voices) -> "Speaker: words" lines.
// Plain text passes through unchanged.
export function vttToText(raw) {
  let s = String(raw || "").replace(/\r/g, "").replace(/^﻿/, "");
  // Webex wraps its own cue headers in a WebVTT file: drop the WEBVTT line and the timings,
  // leaving the "1 "Name" (...)" headers for the parser below.
  if (/^WEBVTT/.test(s.trim()) && /^\d+\s+"[^"]+"/m.test(s)) {
    s = s.split("\n").filter((l) => !/^WEBVTT/.test(l) && !/-->/.test(l) && !/^NOTE\b/.test(l)).join("\n");
  }
  // Webex Calling's own format (seen 8 Oct): a header line per utterance -
  //   3 "Matt Lund" (2022698496) (f938d292-...)
  // - followed by the words. Turned into "Matt Lund: words", joining a speaker's runs.
  if (/^\d+\s+"[^"]+"/m.test(s) && !/^WEBVTT/.test(s.trim())) {
    const out = [];
    let who = null;
    for (const line of s.split("\n")) {
      const h = /^\d+\s+"([^"]+)"/.exec(line.trim());
      if (h) { who = h[1].trim(); continue; }
      const words = line.trim();
      if (!words) continue;
      const prev = out[out.length - 1];
      if (who && prev && prev.startsWith(who + ": ")) out[out.length - 1] = prev + " " + words;
      else out.push(who ? `${who}: ${words}` : words);
    }
    return out.join("\n").trim();
  }
  if (!/^WEBVTT/.test(s.trim())) return s.trim();
  const out = [];
  for (const block of s.split(/\n\n+/)) {
    const lines = block.split("\n").filter((l) => l && !/-->/.test(l) && !/^WEBVTT/.test(l) && !/^\d+$/.test(l.trim()) && !/^NOTE\b/.test(l));
    for (const l of lines) {
      const v = /^<v\s+([^>]+)>(.*?)(<\/v>)?$/.exec(l.trim());
      const line = v ? `${v[1].trim()}: ${v[2].trim()}` : l.trim().replace(/<[^>]+>/g, "");
      // Webex splits one person's sentence across cues; join consecutive lines from the same speaker.
      const sp = /^([^:]{1,40}):\s/.exec(line), prev = out[out.length - 1];
      if (sp && prev && prev.startsWith(sp[1] + ": ")) out[out.length - 1] = prev + " " + line.slice(sp[0].length);
      else out.push(line);
    }
  }
  return out.join("\n").trim();
}

// Claude reads the transcript and writes what a colleague would put on the order. Plain ASCII
// in the note (Brightpearl mangles anything else - see bpSafeText).
const SUMMARY_SCHEMA = {
  type: "object", additionalProperties: false,
  properties: {
    is_customer_call: { type: "boolean" },
    // Most calls are a customer chasing an order (Dec, 8 Oct); new orders are the exception.
    call_type: { type: "string", enum: ["chasing", "new_order", "change_to_order", "query", "complaint", "other"] },
    customer: { type: "string" },
    summary: { type: "string" },
    customer_asked: { type: "string" },
    told_customer: { type: "string" },
    items: { type: "array", items: { type: "object", additionalProperties: false,
      properties: { product: { type: "string" }, colour: { type: "string" }, sizes_and_quantities: { type: "string" }, decoration: { type: "string" } },
      required: ["product", "colour", "sizes_and_quantities", "decoration"] } },
    agreed: { type: "string" },
    actions: { type: "array", items: { type: "string" } },
    order_refs: { type: "array", items: { type: "string" } },
    order_note: { type: "string" },
  },
  required: ["is_customer_call", "call_type", "customer", "summary", "customer_asked", "told_customer", "items", "agreed", "actions", "order_refs", "order_note"],
};
// Brightpearl orders named in the call (any 6-digit number), so Claude can match what the
// speech-to-text garbled ("JDL five in nervy") against what is really on the order.
let bpForSummaries = null;
async function ordersMentioned(transcript) {
  if (!bpForSummaries) return [];
  const out = [];
  for (const n of [...new Set(String(transcript).match(/\b\d{6}\b/g) || [])].slice(0, 4)) {
    try {
      const r = await bpForSummaries("GET", `/order-service/order/${n}`);
      const o = Array.isArray(r) ? r[0] : r;
      if (!o || !o.id) { out.push({ number: n, found: false }); continue; }
      const cust = (o.parties && o.parties.customer) || {};
      out.push({ number: n, found: true, type: o.orderTypeCode, reference: o.reference || "", deliveryDate: ((o.delivery || {}).deliveryDate || "").slice(0, 10), customer: cust.companyName || cust.addressFullName || "",
        rows: Object.values(o.orderRows || {}).map((row) => ({ name: row.productName, sku: row.productSku, qty: Number((row.quantity || {}).magnitude || 0) })) });
    } catch (e) { out.push({ number: n, found: false, error: e.message }); }
  }
  return out;
}
// Words the speech-to-text gets wrong. Webex Calling transcription takes no custom vocabulary,
// so the correcting happens afterwards: every Brightpearl brand (refreshed daily, so a new brand
// needs no code change) plus our own trade words, plus CALL_VOCAB (comma-separated) for extras.
let vocabCache = { at: 0, words: [] };
async function tradeVocabulary() {
  if (vocabCache.words.length && Date.now() - vocabCache.at < 24 * 3600e3) return vocabCache.words;
  let brands = [];
  try {
    const r = bpForSummaries ? await bpForSummaries("GET", "/product-service/brand") : [];
    brands = (Array.isArray(r) ? r : []).map((b) => String(b.name || "").replace(/[®™]/g, "").trim())
      .filter((b) => b && !/^(other|clearance|shipping|home & living)$/i.test(b));
  } catch { /* fixed words still help */ }
  const fixed = ["Tuffshop", "Tuff Workwear", "Tuff Sportswear", "TuffKids", "embroidery", "embroidered", "printed", "DTF", "screen print",
    "left breast", "right breast", "back print", "nape", "sleeve", "hi-vis", "logo", "proof", "mock-up", "artwork", "digitising",
    "quote", "sales order", "purchase order", "Brightpearl", "dispatch", "tracking", "DPD", "Royal Mail", "exchange", "credit note",
    "navy", "royal blue", "bottle green", "charcoal", "heather grey", "black", "white", "XS", "XXL", "3XL", "4XL", "5XL"];
  const extra = String(process.env.CALL_VOCAB || "").split(",").map((s) => s.trim()).filter(Boolean);
  vocabCache = { at: Date.now(), words: [...new Set([...fixed, ...extra, ...brands])] };
  return vocabCache.words;
}
// Our own name, which the speech-to-text hears as ordinary words. Only these unambiguous swaps
// are made in the stored transcript; anything that could be a real word ("unique") is left to
// the summary step, which judges it in context.
export function fixKnownMishearings(text) {
  return String(text || "")
    .replace(/\b(tough|tuff)\s*shop\b/gi, "Tuffshop")
    .replace(/\b(tough|tuff)\s+work\s*wear\b/gi, "Tuff Workwear")
    .replace(/\btough\s+sportswear\b/gi, "Tuff Sportswear");
}
// ---- matching a call to its customer and order (Dec, 9 Oct) --------------------------------
// Tested on 8 Oct's 10 sales calls: the CALLER'S NUMBER found the right customer and order
// on 6, where speech-to-text alone got order numbers wrong (459052 for SO459194). So, in order:
//   1. an SO number or web order number said on the call that Brightpearl confirms
//      (web refs are stored 9 digits, "000125971"; the call says "0125971" or "125971");
//   2. otherwise the caller's number -> Brightpearl contact (pri / mob / sec, spacing ignored,
//      whole number only) -> that customer's latest orders.
// Read-only and free, so it runs whether or not summaries are on.
const WORD_DIGITS = { zero: "0", oh: "0", o: "0", nought: "0", one: "1", two: "2", three: "3", four: "4", five: "5", six: "6", seven: "7", eight: "8", nine: "9" };
// "four nine four seven two nine" / "4 9 4 7 2 9" / "494,729" -> "494729"
export function numbersSpoken(text) {
  // ", " or ". " ends a number (Webex punctuates between phrases); "494,729" / "49-47-29" join up.
  // Each transcript line starts "Speaker: " - and an unsaved caller's "speaker" is their phone
  // number ("+441277658517: 000-12-5510"), which ran into the order number on 9 Oct. So the
  // speaker label is dropped and every line ends a number.
  const body = String(text || "").split("\n").map((l) => l.replace(/^[^:\n]{1,40}:\s/, "")).join(" ; ");
  const tokens = body.toLowerCase().replace(/[,.]\s+/g, " ; ").replace(/[,.-]/g, "").split(/\s+/);
  const out = new Set();
  let run = "";
  const flush = () => { if (run.length >= 5) out.add(run); run = ""; };
  for (const t of tokens) {
    const w = t.replace(/[^a-z0-9]/g, "");
    if (/^\d+$/.test(w)) { run += w; continue; }
    if (WORD_DIGITS[w] !== undefined && (run || w !== "o")) { run += WORD_DIGITS[w]; continue; }
    if (w === "double" || w === "triple") continue;
    flush();
  }
  flush();
  return [...out];
}
const nationalNumber = (n) => {
  const d = String(n || "").replace(/[^\d+]/g, "");
  if (/^\+44\d{9,10}$/.test(d)) return "0" + d.slice(3);
  if (/^44\d{9,10}$/.test(d)) return "0" + d.slice(2);
  return /^0\d{9,10}$/.test(d) ? d : null;
};
async function callerNumber(pool, c) {
  const direct = nationalNumber(c.other_party);
  if (direct) return direct;
  try {   // a caller saved in Webex shows as a name; the metadata still has the number
    const { json } = await webexGet(pool, `https://webexapis.com/v1/convergedRecordings/${encodeURIComponent(c.id)}/metadata`);
    const sd = (json && json.serviceData) || {};
    for (const p of [sd.callingParty, sd.calledParty, sd.connectedParty]) {
      if (!p || (p.actor && p.actor.type === "USER" && /@tuffshop\.co\.uk$/i.test(String(p.actor.email || "")))) continue;
      const n = nationalNumber(p.number);
      if (n) return n;
    }
  } catch { /* no number */ }
  return null;
}
async function bpSearch(path) {
  const r = await bpForSummaries("GET", path);
  const cols = ((r && r.metaData && r.metaData.columns) || []).map((x) => x.name);
  return ((r && r.results) || []).map((row) => Object.fromEntries(cols.map((k, i) => [k, row[i]])));
}
export async function matchRecording(pool, id) {
  if (!bpForSummaries) return null;
  const c = (await pool.query(`SELECT id, other_party, transcript, summary_json FROM call_recordings WHERE id = $1`, [id])).rows[0];
  if (!c) return null;
  const number = await callerNumber(pool, c);
  const internal = c.summary_json && c.summary_json.internal;
  let contacts = [];
  if (number && !internal) {
    const seen = new Map();
    for (const f of ["pri", "mob", "sec"]) {
      for (const x of await bpSearch(`/contact-service/contact-search?${f}=${number}`)) {
        seen.set(x.contactId, { contactId: x.contactId, name: `${x.firstName || ""} ${x.lastName || ""}`.replace(/\s+/g, " ").trim(), company: x.companyName || "" });
      }
    }
    contacts = [...seen.values()].slice(0, 3);
  }
  // 1. numbers said on the call
  let order = null, how = null;
  for (const n of numbersSpoken(c.transcript).slice(0, 6)) {
    const tries = [];
    if (/^4\d{5}$/.test(n)) tries.push(["so", n]);
    // Web refs have 6 significant digits (000125971); padding a 5-digit fragment ("98016")
    // matched a 2025 order on 8 Oct, so shorter numbers are not tried as refs.
    if (/^0*[1-9]\d{5}$/.test(n)) tries.push(["ref", n.padStart(9, "0")]);
    for (const [kind, v] of tries) {
      try {
        const hit = kind === "so"
          ? (await bpSearch(`/order-service/order-search?orderId=${v}`))[0]
          : (await bpSearch(`/order-service/order-search?customerRef=${v}`))[0];
        if (!hit) continue;
        // An order matched only on a heard number must be recent (a stray number hits old orders).
        if (Date.now() - new Date(hit.createdOn).getTime() > 365 * 86400e3) continue;
        // A number heard on someone else's order is only trusted when we have no caller match.
        if (contacts.length && !contacts.some((x) => Number(x.contactId) === Number(hit.contactId))) continue;
        order = hit; how = kind === "so" ? `order number said on the call (${n})` : `web order number said on the call (${n})`;
        break;
      } catch { /* try the next */ }
    }
    if (order) break;
  }
  // 2. the caller's latest orders
  let candidates = [];
  for (const ct of contacts) {
    const rows = await bpSearch(`/order-service/order-search?contactId=${ct.contactId}&orderTypeId=1&sort=orderId.DESC&pageSize=3`);
    candidates.push(...rows.map((r) => ({ orderId: r.orderId, created: String(r.createdOn || "").slice(0, 10), ref: r.customerRef || "", statusId: r.orderStatusId, contact: ct.name })));
  }
  candidates = candidates.sort((a, b) => b.orderId - a.orderId).slice(0, 5);
  if (!order && candidates.length) { order = { orderId: candidates[0].orderId }; how = "caller's number - their latest order"; }
  const result = {
    number, contacts, orderId: order ? Number(order.orderId) : null, how,
    // Several recent orders = a guess; staff should confirm before a note goes on it.
    certain: !!order && (/said on the call/.test(how || "") || candidates.length <= 1),
    hadTranscript: !!c.transcript,
    candidates,
  };
  await pool.query(`UPDATE call_recordings SET caller_number = $2, contact_id = $3, contact_name = $4, order_id = $5, match_json = $6, matched_at = now() WHERE id = $1`,
    [id, number, contacts[0] ? contacts[0].contactId : null, contacts[0] ? [contacts[0].name, contacts[0].company].filter(Boolean).join(" / ") : null,
     result.orderId, JSON.stringify(result)]);
  return result;
}

export async function summariseRecording(pool, id) {
  if (!process.env.ANTHROPIC_API_KEY) throw new Error("ANTHROPIC_API_KEY not set");
  const c = (await pool.query(`SELECT owner_email, created, duration, other_party, transcript FROM call_recordings WHERE id = $1`, [id])).rows[0];
  if (!c || !c.transcript) return null;
  // A person's name, not their address ("dec@..." -> "Dec") when they are not a report person.
  const staff = (reportPeople().find((p) => String(p.email).toLowerCase() === c.owner_email) || {}).name
    || String(c.owner_email || "").split("@")[0].replace(/^./, (x) => x.toUpperCase());
  // Internal call? Webex's metadata names each end; when the other end is one of OUR
  // Webex users (a handset like "Pelican Works 2"), it is colleague-to-colleague and must
  // never become an order note. Claude alone took "Pelican Works 2" for a customer (8 Oct).
  let internal = false;
  try {
    const { json } = await webexGet(pool, `https://webexapis.com/v1/convergedRecordings/${encodeURIComponent(id)}/metadata`);
    const sd = (json && json.serviceData) || {};
    const ours = sd.managedBy && sd.managedBy.actor && sd.managedBy.actor.id;
    internal = [sd.callingParty, sd.calledParty, sd.connectedParty].some((p) => p && p.actor && p.actor.type === "USER"
      && p.actor.id !== ours && /@tuffshop\.co\.uk$/i.test(String(p.actor.email || "")));
  } catch { /* unknown - leave it to the transcript */ }
  const m = (await pool.query(`SELECT order_id FROM call_recordings WHERE id = $1`, [id])).rows[0];
  const orders = await ordersMentioned(c.transcript + (m && m.order_id ? ` ${m.order_id}` : ""));
  const vocab = await tradeVocabulary();
  const { default: Anthropic } = await import("@anthropic-ai/sdk");
  const msg = await new Anthropic().messages.create({
    model: process.env.CALL_SUMMARY_MODEL || "claude-sonnet-5",
    max_tokens: 4000,   // a 13-minute call needed more than 1500 (8 Oct)
    messages: [{ role: "user", content:
      `This is the transcript of a phone call at Tuff Shop, a UK workwear and embroidery/print company. ` +
      `Our side is ${staff}; the other party shows as "${c.other_party}". Call on ${ukDay(c.created)}, ${Math.round(c.duration / 60)} min.\n\n` +
      `Write what a colleague needs on the customer's sales order. Most calls are a customer CHASING an existing order ` +
      `(where is it, when will it arrive, has the proof been done); some are new orders, changes, queries or complaints. ` +
      `Only state what was actually said - never guess sizes, quantities, prices or dates. Empty string / empty list where nothing was said.\n` +
      `The transcript is machine speech-to-text and garbles workwear words ("left press" = left breast, "nervy" = navy, ` +
      `"JDL five" = GD05, i.e. product codes come out as words). Correct a garbled word only where the meaning is clear, ` +
      `and put what was heard in brackets when you correct a product code or number. Order numbers are often misheard: if a ` +
      `number heard is not found in Brightpearl, say so plainly ("order 595771 as heard - not found, check").\n` +
      (orders.length ? `Brightpearl orders matching numbers heard in the call - use them to identify the customer and products:\n${JSON.stringify(orders)}\n` : "") +
      (internal ? `This was an INTERNAL call: both ends are Tuff Shop phones ("${c.other_party}" is a colleague's handset, not a customer). ` +
        `is_customer_call must be false and order_note must be an empty string.\n` : "") +
      `Phone names like "Mike Production" or "Pelican Works 2" are our handsets, not people or companies - never call one the customer.\n` +
      `Names and trade words we use - spell them exactly like this wherever the transcript clearly means one (e.g. "unique" ` +
      `meaning the brand = Uneek, "tough shop" = Tuffshop, "snicker" = Snickers): ${vocab.join(", ")}.\n` +
      `Our order numbers are 6 digits (sales orders "SO", currently around 49xxxx); product codes are letters+digits like GD05, ` +
      `UC902, 2821 and are often heard as words.\n` +
      `- is_customer_call: false for a call between colleagues or with a supplier.\n` +
      `- call_type: chasing / new_order / change_to_order / query / complaint / other.\n` +
      `- summary: 1-2 plain sentences.\n` +
      `- customer_asked: what the caller wanted to know or have done.\n` +
      `- told_customer: what we told them - status, dates, promises (e.g. "will call back tomorrow", "tracking to follow").\n` +
      `- items: only when products were ordered or changed: product, colour, sizes+quantities, logo/decoration.\n` +
      `- agreed: prices, delivery dates, deadlines agreed.\n- actions: what our side must do next.\n` +
      `- order_refs: order / quote / PO numbers mentioned (as corrected, if Brightpearl confirmed one).\n` +
      `- order_note: the note to paste on the order - short, plain ASCII only (no pound sign - write GBP; no dashes ` +
      `other than -), starting "Phone call ${ukDay(c.created)} (${staff}):". For a chase, e.g. "Customer chased delivery; ` +
      `told due Friday, tracking to be emailed."\n\nTranscript:\n${c.transcript.slice(0, 60000)}` }],
    output_config: { format: { type: "json_schema", schema: SUMMARY_SCHEMA } },
  });
  const text = (msg.content || []).find((b) => b.type === "text");
  if (msg.stop_reason === "max_tokens") throw new Error(`summary was cut off at ${(msg.usage || {}).output_tokens} tokens; blocks: ${(msg.content || []).map((b) => b.type).join(",")}; text starts: ${String((text && text.text) || "").slice(0, 400)} ... ends: ${String((text && text.text) || "").slice(-300)}`);
  const parsed = JSON.parse((text && text.text) || "{}");
  if (internal) Object.assign(parsed, { is_customer_call: false, order_note: "", internal: true });
  // What this call cost, so the spend is measured rather than estimated (Dec, 8 Oct).
  parsed.usage = { model: msg.model, input_tokens: (msg.usage || {}).input_tokens || 0, output_tokens: (msg.usage || {}).output_tokens || 0 };
  // The list shows the order note; a call that is not with a customer shows its summary,
  // labelled, and has no note to post.
  const shown = parsed.is_customer_call === false ? `${internal ? "Internal call" : "Not a customer call"}: ${parsed.summary || ""}` : (parsed.order_note || parsed.summary || "");
  await pool.query(`UPDATE call_recordings SET summary = $2, summary_json = $3, summarized_at = now() WHERE id = $1`,
    [id, shown, JSON.stringify(parsed)]);
  return parsed;
}
const signRec = (secret, id) => crypto.createHmac("sha256", secret).update("rec:" + id).digest("hex").slice(0, 40);
export async function recordingLink(pool, id) {
  return `${publicBase()}/call-recording/${encodeURIComponent(id)}?sig=${signRec(await reportSecret(pool), id)}`;
}
// The matched customer and order, linked to Brightpearl; a guess (several recent orders) says so.
const BP_ORDER = "https://euw1.brightpearlapp.com/patt-op.php?scode=invoice&oID=";
const orderCell = (c) => {
  const m = c.match_json || {};
  if (!c.order_id && !c.contact_name) return '<span style="color:#9ca3af">' + (c.match_json ? "no match" : "matching...") + "</span>";
  const others = (m.candidates || []).filter((x) => Number(x.orderId) !== Number(c.order_id)).slice(0, 2)
    .map((x) => '<a href="' + BP_ORDER + x.orderId + '" target="_blank" rel="noopener" style="color:#6b7280">SO' + x.orderId + "</a>").join(", ");
  return (c.contact_name ? esc(c.contact_name) + "<br>" : "") +
    (c.order_id ? '<a href="' + BP_ORDER + c.order_id + '" target="_blank" rel="noopener" style="color:#0f6cbd;font-weight:600">SO' + c.order_id + "</a>" : "") +
    (m.how ? '<br><span style="color:#6b7280;font-size:11px">' + esc(m.how) + (m.certain ? "" : " - check") + "</span>" : "") +
    (others ? '<br><span style="color:#6b7280;font-size:11px">also: ' + others + "</span>" : "");
};
const fmtLen = (s) => `${Math.floor((s || 0) / 60)}:${String((s || 0) % 60).padStart(2, "0")}`;
const hhmm = (min) => `${String(Math.floor(min / 60)).padStart(2, "0")}:${String(min % 60).padStart(2, "0")}`;

export function managerEmailHtml(r, { imgSrc = (q) => "cid:" + cidFor(q), viewUrl = null, web = false } = {}) {
  const w = web ? WEB_TL_WIDTH : TL.width;
  const people = r.people.map((p) => `<tr><td ${TD}>${esc(p.name)}${p.webexMatched ? "" : ' <span style="color:#b91c1c">(not found in Webex)</span>'}</td>
    <td ${TDN}>${p.total.callsIn}</td><td ${TDN}>${p.total.callsOut}</td><td ${TDN}>${fmtDuration(p.total.talk)}</td><td ${TDN}>${p.total.orders}</td><td ${TDN}>${p.total.returns}</td>${mailCells(p, TDN)}</tr>`).join("");
  const sum = r.people.reduce((a, p) => ({ callsIn: a.callsIn + p.total.callsIn, callsOut: a.callsOut + p.total.callsOut, talk: a.talk + p.total.talk, orders: a.orders + p.total.orders, returns: a.returns + p.total.returns, emailsIn: a.emailsIn + p.total.emailsIn, emailsOut: a.emailsOut + p.total.emailsOut }), blank());
  const sections = r.people.map((p) => `<details style="margin:10px 0;border:1px solid #d1d5db;border-radius:6px">
  <summary style="cursor:pointer;padding:9px 12px;background:#f3f4f6;font-size:14px;font-weight:600">${esc(p.name)}
    <span style="font-weight:400;color:#4b5563">&nbsp;-&nbsp;${p.total.callsIn} in, ${p.total.callsOut} out, ${fmtDuration(p.total.talk)} on the phone, ${p.total.orders} order${p.total.orders === 1 ? "" : "s"}, ${p.total.returns} return${p.total.returns === 1 ? "" : "s"}${p.emailError ? "" : `, ${p.total.emailsOut} email${p.total.emailsOut === 1 ? "" : "s"} sent`}</span></summary>
  <div style="padding:10px 12px">
  <table style="border-collapse:collapse;width:100%"><tr><th ${TH}>Day</th><th ${THN}>Calls in</th><th ${THN}>Calls out</th><th ${THN}>On phone</th><th ${THN}>Orders</th><th ${THN}>Returns</th><th ${THN}>Emails in</th><th ${THN}>Emails out</th></tr>
  ${dayRows(p)}</table>
  ${timelineImg(p, imgSrc, w)}
  </div></details>`).join("");
  return `<div style="font-family:Segoe UI,Arial,sans-serif;color:#111;max-width:${w}px">
  <p>Sales activity for ${esc(weekTitle(r))}.</p>
  ${viewUrl ? `<p style="margin:0 0 14px"><a href="${esc(viewUrl)}" style="display:inline-block;background:#0f6cbd;color:#ffffff;text-decoration:none;font-weight:600;font-size:13px;padding:8px 14px;border-radius:4px">Open the full report</a>
    <span style="color:#6b7280;font-size:12px">&nbsp;each person's days and 7am-6pm timeline</span></p>` : ""}
  <table style="border-collapse:collapse;width:100%">
    <tr><th ${TH}>Person</th><th ${THN}>Calls in</th><th ${THN}>Calls out</th><th ${THN}>Time on phone</th><th ${THN}>Orders created</th><th ${THN}>Returns</th><th ${THN}>Emails in</th><th ${THN}>Emails out</th></tr>
    ${people}
    <tr><td style="padding:7px 10px;font-weight:700;font-size:13px;border-top:2px solid #1f2a37">Team</td>
      <td ${TOT}>${sum.callsIn}</td><td ${TOT}>${sum.callsOut}</td><td ${TOT}>${fmtDuration(sum.talk)}</td><td ${TOT}>${sum.orders}</td><td ${TOT}>${sum.returns}</td><td ${TOT}>${sum.emailsIn}</td><td ${TOT}>${sum.emailsOut}</td></tr>
  </table>
  ${r.missingCallDays.length ? `<p style="color:#b45309;font-size:12px">No Webex call data for: ${r.missingCallDays.map(dayLabel).join(", ")}.</p>` : ""}
  ${r.people.filter((p) => p.emailError).map((p) => `<p style="color:#b45309;font-size:12px">Emails not counted for ${esc(p.name)}: ${esc(p.emailError)}.</p>`).join("")}
  ${web ? `<p style="font-size:13px;margin:18px 0 4px"><b>Each person</b> <span style="color:#6b7280">- click a name to open their days and timeline (7am to 6pm)</span></p>
  ${sections}` : ""}
  <p style="color:#6b7280;font-size:12px;margin-top:14px">Internal calls excluded; hunt-group calls count only for whoever answered. Orders = sales orders created by that person in Brightpearl, excluding web/Amazon/eBay and exchange orders. Returns = exchange orders + credit notes they booked (both halves of one return count once). Emails = customer emails in and out of their own mailbox (colleagues, automated senders, junk and the shared sales@ box not included).</p>
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

export async function sendReport(r, { onlyTo, viewUrl = null, managerOnly = false } = {}) {
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
  if (!managerOnly) for (const p of r.people) await send(p.email, `Your week - ${dayLabel(r.week)} to ${dayLabel(r.weekEnd)}`, personEmailHtml(r, p), await timelineAttachments(r, [p]));
  if (sent.every((s) => s.error)) throw new Error(`No report email could be sent: ${sent[0] && sent[0].error}`);
  return sent;
}

// ---- routes + schedule ----------------------------------------------------------------
export function registerCallReport(app, { getPool, bpLive }) {
  bpForSummaries = bpLive;
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
      for (const q of r.people) pics[q.key] = "data:image/png;base64," + (await timelinePng(r, q, WEB_TL_WIDTH)).toString("base64");
      res.set({ "Cache-Control": "private, no-store", "X-Robots-Tag": "noindex", "Referrer-Policy": "no-referrer" });
      res.type("html").send(`<!doctype html><html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>Sales activity - ${esc(weekTitle(r))}</title></head>
<body style="margin:0;padding:24px;background:#f6f7f9"><div style="background:#fff;padding:20px 24px;border-radius:8px;max-width:1460px;margin:0 auto">${managerEmailHtml(r, { imgSrc: (q) => pics[q.key] || "", web: true })}</div></body></html>`);
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

  // Call recordings (Dec, 7 Oct - trying Webex recording). Lists what the Webex org holds; the
  // Service App needs spark-admin:recordings_read for this, so a 403 means that scope is missing.
  app.get("/api/call-report/recordings", requireUser, guard, async (req, res) => {
    const pool = getPool();
    if (req.query.fresh) webexCache = { token: null, until: 0 };   // pick up newly granted scopes
    let scope = null;
    try { const { json } = await webexGet(pool, "https://webexapis.com/v1/people/me"); scope = json && json.displayName; } catch (e) { scope = "me: " + e.message; }
    // ?diag=1 - why sales-phone recordings might be missing: each phone's Webex login, the
    // recordings listed for that owner, and the phone's calls in the last 2 hours of call history.
    if (req.query.diag) {
      const out = { phones: [], cdr: null };
      for (const p of reportPeople()) {
        const row = { person: p.first };
        try {
          const { json } = await webexGet(pool, `https://webexapis.com/v1/people?displayName=${encodeURIComponent(p.webexName || p.name)}`);
          const hit = ((json && json.items) || []).find((x) => String(x.displayName).toLowerCase() === String(p.webexName || p.name).toLowerCase());
          row.emails = hit && hit.emails;
          const from = new Date(Date.now() - 86400e3).toISOString(), to = new Date().toISOString();
          for (const e of (hit && hit.emails) || []) {
            try {
              const r = await webexGet(pool, `https://webexapis.com/v1/admin/convergedRecordings?ownerEmail=${encodeURIComponent(e)}&from=${from}&to=${to}&max=20`);
              row.recordingsFor = (row.recordingsFor || 0) + (((r.json && r.json.items) || []).length);
            } catch (er) { row.recordingsError = er.message; }
          }
        } catch (e) { row.error = e.message; }
        out.phones.push(row);
      }
      try {
        const s = new Date(Date.now() - (Number(req.query.mins) || 120) * 60e3).toISOString(), e = new Date(Date.now() - 5 * 60e3).toISOString();
        const { json } = await webexGet(pool, `${cdrBase()}/v1/cdr_feed?startTime=${s}&endTime=${e}&max=500`);
        const by = {};
        for (const r of (json && json.items) || []) {
          const k = r["User"] || r.user || "?";
          const t = (by[k] = by[k] || { legs: 0, answered: 0, recorded: 0 });
          t.legs++; if (String(r["Answered"]).toLowerCase() === "true") t.answered++;
          if (Object.keys(r).some((f) => /record/i.test(f) && r[f] && String(r[f]).toLowerCase() !== "false")) t.recorded++;
        }
        out.cdr = by;
        out.answeredSales = ((json && json.items) || []).filter((r) => /Sales/.test(r["User"] || "") && String(r["Answered"]).toLowerCase() === "true")
          .map((r) => ({ user: r["User"], start: r["Start time"], dir: r["Direction"], result: r["Call Recording Result"] || null, trigger: r["Call Recording Trigger"] || null, platform: r["Call Recording Platform Name"] || null }));
        out.cdrRecordFields = [...new Set(((json && json.items) || []).flatMap((r) => Object.keys(r).filter((f) => /record/i.test(f))))];
      } catch (e) { out.cdr = { error: e.message }; }
      return res.json(out);
    }
    // ?settings=1 - each sales phone's call-recording setting as Webex holds it.
    if (req.query.settings) {
      const out = [];
      for (const p of reportPeople()) {
        try {
          const { json } = await webexGet(pool, `https://webexapis.com/v1/people?displayName=${encodeURIComponent(p.webexName || p.name)}`);
          const hit = ((json && json.items) || []).find((x) => String(x.displayName).toLowerCase() === String(p.webexName || p.name).toLowerCase());
          if (!hit) { out.push({ person: p.first, error: "phone user not found" }); continue; }
          const s = (await webexGet(pool, `https://webexapis.com/v1/people/${hit.id}/features/callRecording`)).json || {};
          out.push({ person: p.first, phone: hit.displayName, ...s });
        } catch (e) { out.push({ person: p.first, error: e.message }); }
      }
      return res.json(out);
    }
    // ?purge=<id> wipes our copy of a recording (transcript, summary, notes) and stops it being
    // re-fetched or played. The audio itself must still be deleted in Webex (Control Hub).
    if (req.query.purge) {
      await ensureRecordingTables(pool);
      const r = await pool.query(`UPDATE call_recordings SET transcript = NULL, summary = NULL, summary_json = NULL, webex_notes = NULL, purged_at = now() WHERE id = $1 RETURNING id, owner_email, created, duration`, [req.query.purge]);
      return res.json({ purged: r.rows[0] || null });
    }
    // ?rematch=1 runs the customer/order matching again on the last 3 days' calls.
    if (req.query.rematch) {
      await ensureRecordingTables(pool);
      const ids = req.query.rematch !== "1" ? [String(req.query.rematch)]   // ?rematch=<id> for one call
        : (await pool.query(`SELECT id FROM call_recordings WHERE purged_at IS NULL AND created > now() - interval '3 days' ORDER BY created DESC`)).rows.map((r) => r.id);
      const out = [];
      for (const id of ids) { try { out.push({ id, ...(await matchRecording(pool, id)) }); } catch (e) { out.push({ id, error: e.message }); } }
      return res.json({ matched: out.length, results: out.map((r) => ({ id: r.id, number: r.number, orderId: r.orderId, how: r.how, certain: r.certain, error: r.error })) });
    }
    // ?redo=<id> fetches the transcript again and re-writes the summary, returning it (or the error).
    if (req.query.redo) {
      try {
        await ensureRecordingTables(pool);
        await pool.query(`UPDATE call_recordings SET transcript = NULL, summary = NULL, summary_json = NULL, transcript_checked_at = NULL WHERE id = $1`, [req.query.redo]);
        await transcribePending(pool);
        const row = (await pool.query(`SELECT transcript, summary, summary_json FROM call_recordings WHERE id = $1`, [req.query.redo])).rows[0] || {};
        if (row.transcript && !row.summary) {
          try { await summariseRecording(pool, req.query.redo); } catch (e) { return res.json({ transcript: row.transcript, error: e.message }); }
        }
        return res.json((await pool.query(`SELECT transcript, summary, summary_json FROM call_recordings WHERE id = $1`, [req.query.redo])).rows[0] || {});
      } catch (e) { return res.status(500).json({ error: e.message }); }
    }
    // ?id= one recording's details: which files Webex offers (audio, transcript...), links withheld.
    if (req.query.id) {
      try {
        const id = encodeURIComponent(req.query.id);
        const variant = { admin: `admin/convergedRecordings/${id}`, meta: `convergedRecordings/${id}/metadata` }[req.query.v] || `convergedRecordings/${id}`;
        const { json } = await webexGet(pool, `https://webexapis.com/v1/${variant}`);
        if (req.query.v === "meta") return res.json({ keys: Object.keys(json || {}), sample: JSON.stringify(json).slice(0, 1500) });
        const links = (json && json.temporaryDirectDownloadLinks) || {};
        return res.json({ keys: Object.keys(json || {}), files: Object.fromEntries(Object.entries(links).map(([k, v]) => [k, !!v])), format: json && json.format, serviceData: json && json.serviceData });
      } catch (e) { return res.json({ error: e.message }); }
    }
    // Otherwise: pull the last ?days (default 3) from Webex, then list what is stored, with replay links.
    try {
      const synced = await syncRecordings(pool, Math.min(Number(req.query.days) || 3, 30));
      const rows = (await pool.query(`SELECT id, owner_email, created, duration, other_party, summary, webex_notes, length(transcript) AS transcript_chars, transcript_checked_at, caller_number, contact_name, order_id, match_json FROM call_recordings WHERE purged_at IS NULL ORDER BY created DESC LIMIT 50`)).rows;
      for (const c of rows) c.url = await recordingLink(pool, c.id);
      res.json({ app: scope, synced, recordings: rows });
    } catch (e) { res.status(500).json({ app: scope, error: e.message }); }
  });

  // GET /call-recording/:id?sig= - a small player page for one recorded call. The signature
  // is per recording, so a link opens that call and nothing else. The audio itself comes from
  // /call-recording/:id/audio, which fetches a fresh Webex link at play time.
  const recordingOk = async (req) => {
    const id = String(req.params.id || ""), got = String(req.query.sig || "");
    const want = signRec(await reportSecret(getPool()), id);
    return got.length === want.length && crypto.timingSafeEqual(Buffer.from(got), Buffer.from(want));
  };
  const noIndex = { "Cache-Control": "private, no-store", "X-Robots-Tag": "noindex", "Referrer-Policy": "no-referrer" };
  app.get("/call-recording/:id", async (req, res) => {
    try {
      if (!(await recordingOk(req))) return res.status(403).send("This link isn't valid.");
      const pool = getPool();
      await ensureRecordingTables(pool);
      const c = (await pool.query(`SELECT owner_email, created, duration, other_party, summary, transcript, webex_notes, purged_at FROM call_recordings WHERE id = $1`, [req.params.id])).rows[0];
      if (c && c.purged_at) return res.status(410).send("This recording was deleted.");
      const who = c ? (reportPeople().find((p) => String(p.email).toLowerCase() === c.owner_email) || {}).name || c.owner_email : "";
      const when = c ? `${dayLabel(ukDay(c.created))} ${hhmm(ukMinutes(c.created))}` : "";
      const audio = `/call-recording/${encodeURIComponent(req.params.id)}/audio?sig=${encodeURIComponent(String(req.query.sig))}`;
      res.set(noIndex).type("html").send(`<!doctype html><html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>Call recording</title></head>
<body style="margin:0;padding:24px;background:#f6f7f9;font-family:Segoe UI,Arial,sans-serif;color:#111">
<div style="background:#fff;padding:20px 24px;border-radius:8px;max-width:760px;margin:0 auto">
<h2 style="margin:0 0 4px;font-size:18px">${c ? `Call with ${esc(c.other_party || "unknown")}` : "Call recording"}</h2>
<p style="margin:0 0 16px;color:#4b5563;font-size:13px">${esc(who)}${who ? " - " : ""}${esc(when)}${c ? ` - ${fmtLen(c.duration)}` : ""}</p>
<audio controls autoplay preload="auto" src="${esc(audio)}" style="width:100%"></audio>
${c && c.summary ? `<h3 style="font-size:14px;margin:20px 0 6px">Summary</h3><div style="font-size:13px;white-space:pre-wrap">${esc(c.summary)}</div>` : ""}
${c && c.webex_notes ? `<h3 style="font-size:14px;margin:20px 0 6px">Webex AI summary</h3><div style="font-size:13px;white-space:pre-wrap">${esc(c.webex_notes)}</div>` : ""}
${c && c.transcript ? `<details style="margin-top:16px"><summary style="cursor:pointer;font-size:13px;font-weight:600">Transcript</summary><div style="font-size:13px;white-space:pre-wrap;margin-top:8px">${esc(c.transcript)}</div></details>` : ""}
</div></body></html>`);
    } catch (e) { res.status(500).send("Could not open the recording: " + esc(e.message)); }
  });
  // GET /call-recordings?sig= - every recorded call of the last 7 days with its order note, for
  // checking the transcription (Dec, 8 Oct). One fixed signed link; /api/call-report/recordings-link gives it.
  app.get("/call-recordings", async (req, res) => {
    try {
      const want = signRec(await reportSecret(getPool()), "list"), got = String(req.query.sig || "");
      if (got.length !== want.length || !crypto.timingSafeEqual(Buffer.from(got), Buffer.from(want))) return res.status(403).send("This link isn't valid.");
      const pool = getPool();
      await ensureRecordingTables(pool);
      await pool.query(`ALTER TABLE call_recordings ADD COLUMN IF NOT EXISTS summary_json jsonb`);
      const rows = (await pool.query(`SELECT id, owner_email, created, duration, other_party, summary, summary_json, transcript IS NOT NULL AS has_transcript, contact_name, order_id, match_json
                                        FROM call_recordings WHERE created > now() - interval '7 days' AND purged_at IS NULL ORDER BY created DESC`)).rows;
      const body = [];
      for (const c of rows) {
        const who = (reportPeople().find((p) => String(p.email).toLowerCase() === c.owner_email) || {}).name || c.owner_email;
        const j = c.summary_json || {};
        body.push(`<tr><td ${TD}>${esc(dayLabel(ukDay(c.created)))} ${hhmm(ukMinutes(c.created))}</td><td ${TD}>${esc(who)}</td><td ${TD}>${esc(c.other_party)}</td>
          <td ${TDN}>${fmtLen(c.duration)}</td><td ${TD}>${orderCell(c)}</td><td ${TD}>${esc(j.call_type || "")}</td>
          <td ${TD}>${c.summary ? esc(c.summary) : `<span style="color:#9ca3af">${c.has_transcript ? "transcript in, note pending" : "waiting for transcript"}</span>`}</td>
          <td ${TD}><a href="${esc(await recordingLink(pool, c.id))}" target="_blank" rel="noopener" style="color:#0f6cbd;font-weight:600;text-decoration:none">&#9654; Open</a></td></tr>`);
      }
      res.set(noIndex).type("html").send(`<!doctype html><html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>Call notes</title></head>
<body style="margin:0;padding:24px;background:#f6f7f9;font-family:Segoe UI,Arial,sans-serif;color:#111"><div style="background:#fff;padding:20px 24px;border-radius:8px;max-width:1300px;margin:0 auto">
<h2 style="margin:0 0 12px;font-size:18px">Recorded calls - last 7 days</h2>
<table style="border-collapse:collapse;width:100%"><tr><th ${TH}>When</th><th ${TH}>Taken by</th><th ${TH}>With</th><th ${THN}>Length</th><th ${TH}>Customer / order</th><th ${TH}>Type</th><th ${TH}>Order note</th><th ${TH}></th></tr>
${body.join("") || `<tr><td ${TD} colspan="7">No recorded calls yet.</td></tr>`}</table>
<p style="color:#6b7280;font-size:12px;margin-top:12px">Open a call for the recording, the full transcript and the note. Notes come from Webex's speech-to-text, checked against Brightpearl where an order number was heard.</p></div></body></html>`);
    } catch (e) { res.status(500).send("Could not list the calls: " + esc(e.message)); }
  });
  app.get("/api/call-report/recordings-link", requireUser, guard, async (req, res) => {
    res.json({ url: `${publicBase()}/call-recordings?sig=${signRec(await reportSecret(getPool()), "list")}` });
  });
  app.get("/call-recording/:id/audio", async (req, res) => {
    try {
      if (!(await recordingOk(req))) return res.status(403).send("This link isn't valid.");
      if ((await getPool().query(`SELECT 1 FROM call_recordings WHERE id = $1 AND purged_at IS NOT NULL`, [req.params.id])).rowCount) return res.status(410).send("This recording was deleted.");
      const { json } = await webexGet(getPool(), `https://webexapis.com/v1/convergedRecordings/${encodeURIComponent(req.params.id)}`);
      const link = json && json.temporaryDirectDownloadLinks && json.temporaryDirectDownloadLinks.audioDownloadLink;
      if (!link) return res.status(404).send("Webex has no audio for this recording any more.");
      // Streamed through rather than redirected: Webex serves it as an octet-stream download,
      // which the browser's player will not start. Range is passed on so the player can seek.
      const up = await fetch(link, { headers: req.headers.range ? { Range: req.headers.range } : {} });
      if (!up.ok && up.status !== 206) return res.status(502).send(`Webex audio -> ${up.status}`);
      res.status(up.status).set({ ...noIndex, "Content-Type": "audio/mpeg", "Accept-Ranges": "bytes", "Content-Disposition": "inline" });
      for (const h of ["content-length", "content-range"]) if (up.headers.get(h)) res.set(h, up.headers.get(h));
      const { Readable } = await import("stream");
      Readable.fromWeb(up.body).on("error", () => res.end()).pipe(res);
    } catch (e) { res.status(502).send("Webex would not hand over the recording: " + esc(e.message)); }
  });

  // Customer emails in / out per person per day for a week, from their own mailboxes (?week=).
  app.get("/api/call-report/emails", requireUser, guard, async (req, res) => {
    try {
      const week = weekOf(req.query.week);
      const mail = await emailActivity(reportPeople(), week, addDays(week, 6));
      res.json({ week, people: Object.fromEntries(Object.entries(mail).map(([k, v]) => [k, v.error ? { error: v.error } : { in: v.in, out: v.out }])) });
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
      res.json({ sent: await sendReport(r, { onlyTo: String(req.query.to || testEmail()), viewUrl: await viewLink(getPool(), r.week), managerOnly: req.query.only === "manager" }), missingCallDays: r.missingCallDays });
    } catch (e) { res.status(500).json({ error: e.message }); }
  });

  // Overnight (01:00-06:00 UK) pull of any finished day not yet stored; Monday 07:30-10:00
  // the previous week's emails, once (the week is claimed first, released on failure).
  let busy = false, lastRecordingSync = 0;
  setInterval(async () => {
    if (busy || String(process.env.CALL_REPORT_ENABLED || "").toLowerCase() !== "on" || !getPool()) return;
    const now = new Date();
    const hm = now.toLocaleTimeString("en-GB", { hour: "2-digit", minute: "2-digit", hour12: false, timeZone: TZ });
    const dow = now.toLocaleDateString("en-GB", { weekday: "short", timeZone: TZ });
    busy = true;
    try {
      const pool = getPool();
      await ensureTables(pool);
      // New call recordings every 15 minutes through the working day.
      if (hm >= "07:00" && hm < "20:30" && Date.now() - lastRecordingSync > 14 * 60e3) {
        lastRecordingSync = Date.now();
        await syncRecordings(pool).catch((e) => console.error("[call-report] recordings sync:", e.message));
      }
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
