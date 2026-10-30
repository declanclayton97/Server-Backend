// Brightpearl API usage meter. BP throttles the whole account (~200 calls a minute shared
// by every service and every third-party app), and the 503s only tell us we ran out, not
// who spent it. Importing this file once at startup wraps global fetch: every call to the
// BP public API is counted per minute by service, calling code (from the stack) and
// endpoint, with BP's own brightpearl-requests-remaining header, and flushed once a minute
// into bp_usage on the shared database. Server-Backend's /bp-usage page reads it.
//
// The same file is copied into Tuff-WMS, Alternate-Items and Purchasing-Automation - keep
// the copies identical. It never changes a request or a response, and every failure in
// here is swallowed so metering can't break a BP call.
import pg from "pg";

const SERVICE = process.env.RENDER_SERVICE_NAME || "local";   // Render sets this per service
const FLUSH_MS = 60 * 1000;
const KEEP_DAYS = 14;

const counts = new Map();   // key -> { minute, caller, endpoint, account, calls, throttled, minRemaining }
let pool = null;
let ready = null;

/** "/order-service/order/123,456/row" -> "/order-service/order/:id/row" (query dropped). */
export function endpointOf(pathname) {
  return pathname
    .split("/")
    .map((seg) => (/^[\d,.\-]+$/.test(seg) && /\d/.test(seg) ? ":id" : seg))
    .join("/")
    .replace(/\/+$/, "") || "/";
}

/** Split a BP public-api URL into { account, endpoint }, or null for anything that isn't one. */
export function parseBpUrl(url) {
  let u;
  try { u = new URL(url); } catch { return null; }
  if (!/brightpearl/i.test(u.hostname)) return null;
  const m = u.pathname.match(/\/public-api\/([^/]+)(\/.*)?$/);
  if (!m) return null;
  return { account: m[1], endpoint: endpointOf(m[2] || "/") };
}

const GENERIC = /^(fetch\w*|bp\w*|brightpearl\w*|get|post|put|patch|request|call|send|retry\w*|withRetry|throttled\w*|live\w*|api|http\w*|j|go|run|attempt|once|anonymous|<anonymous>|async)$/i;

/** The code that asked for the call: up to two app frames above the BP wrappers. */
export function callerOf(stack) {
  const frames = [];
  for (const line of String(stack || "").split("\n").slice(1)) {
    const m = line.match(/at (?:async )?(?:(\S+) )?\(?(?:file:\/\/\/?)?(.*?):(\d+):\d+\)?$/);
    if (!m) continue;
    const [, fn = "", file, ln] = m;
    if (/node_modules|node:|internal\/|bpMeter\.js/.test(file)) continue;
    const base = file.replace(/\\/g, "/").split("/").pop();
    frames.push({ fn: fn.replace(/^Object\./, ""), at: `${base}:${ln}` });
  }
  if (!frames.length) return "unknown";
  // Skip the generic wrappers (bpGet, bpLive, request...) so the bucket names the real job.
  const i = frames.findIndex((f) => f.fn && !GENERIC.test(f.fn.split(".").pop()));
  const pick = frames.slice(i < 0 ? 0 : i, (i < 0 ? 0 : i) + 2);
  return pick.map((f) => (f.fn ? `${f.fn} ${f.at}` : f.at)).join(" < ");
}

export function record({ url, status, remaining, stack, at = Date.now() }) {
  const bp = parseBpUrl(url);
  if (!bp) return;
  const minute = Math.floor(at / 60000) * 60000;
  const caller = callerOf(stack);
  const key = `${minute}|${caller}|${bp.endpoint}|${bp.account}`;
  let c = counts.get(key);
  if (!c) counts.set(key, (c = { minute, caller, endpoint: bp.endpoint, account: bp.account, calls: 0, throttled: 0, minRemaining: null }));
  c.calls++;
  if (status === 503 || status === 429) c.throttled++;
  const r = Number(remaining);
  if (remaining != null && remaining !== "" && Number.isFinite(r)) c.minRemaining = c.minRemaining == null ? r : Math.min(c.minRemaining, r);
}

/** Take everything counted so far (the flush hands it to the database). */
export function drain() {
  const rows = [...counts.values()];
  counts.clear();
  return rows;
}

async function db() {
  if (!process.env.DATABASE_URL) return null;
  if (!ready) {
    ready = (async () => {
      pool = new pg.Pool({ connectionString: process.env.DATABASE_URL, ssl: process.env.PGSSL === "off" ? false : { rejectUnauthorized: false }, max: 1, idleTimeoutMillis: 30000 });
      pool.on("error", () => {});   // a dropped idle connection must not take the service down
      await pool.query(`CREATE TABLE IF NOT EXISTS bp_usage (
        minute timestamptz NOT NULL, service text NOT NULL, caller text NOT NULL, endpoint text NOT NULL,
        account text NOT NULL DEFAULT '', calls int NOT NULL DEFAULT 0, throttled int NOT NULL DEFAULT 0,
        min_remaining int, PRIMARY KEY (minute, service, caller, endpoint, account))`);
      await pool.query(`CREATE INDEX IF NOT EXISTS bp_usage_minute ON bp_usage (minute)`);
      return pool;
    })().catch((e) => { ready = null; throw e; });
  }
  return ready;
}

let lastPrune = 0;
export async function flush() {
  const rows = drain();
  if (!rows.length) return;
  try {
    const p = await db();
    if (!p) return;
    const vals = [], args = [];
    rows.forEach((r, i) => {
      const o = i * 8;
      vals.push(`($${o + 1},$${o + 2},$${o + 3},$${o + 4},$${o + 5},$${o + 6},$${o + 7},$${o + 8})`);
      args.push(new Date(r.minute), SERVICE, r.caller.slice(0, 300), r.endpoint.slice(0, 200), r.account, r.calls, r.throttled, r.minRemaining);
    });
    await p.query(`INSERT INTO bp_usage (minute, service, caller, endpoint, account, calls, throttled, min_remaining)
      VALUES ${vals.join(",")}
      ON CONFLICT (minute, service, caller, endpoint, account) DO UPDATE SET
        calls = bp_usage.calls + EXCLUDED.calls, throttled = bp_usage.throttled + EXCLUDED.throttled,
        min_remaining = LEAST(bp_usage.min_remaining, EXCLUDED.min_remaining)`, args);
    if (Date.now() - lastPrune > 6 * 3600e3) {
      lastPrune = Date.now();
      await p.query(`DELETE FROM bp_usage WHERE minute < now() - interval '${KEEP_DAYS} days'`);
    }
  } catch (e) {
    console.warn(`[bp-meter] flush failed (${rows.length} rows dropped): ${e.message}`);
  }
}

function install() {
  if (globalThis.__bpMeterInstalled || typeof globalThis.fetch !== "function") return;
  globalThis.__bpMeterInstalled = true;
  const realFetch = globalThis.fetch;
  globalThis.fetch = async function meteredFetch(input, init) {
    const url = typeof input === "string" ? input : input && (input.url || input.href || String(input));
    const isBp = typeof url === "string" && /brightpearl/i.test(url) && url.includes("/public-api/");
    if (!isBp) return realFetch(input, init);
    const stack = new Error().stack;
    let res;
    try {
      res = await realFetch(input, init);
    } catch (e) {
      try { record({ url, status: 0, remaining: null, stack }); } catch {}
      throw e;
    }
    try { record({ url, status: res.status, remaining: res.headers.get("brightpearl-requests-remaining"), stack }); } catch {}
    return res;
  };
  setInterval(() => { flush(); }, FLUSH_MS).unref();
  console.log(`[bp-meter] counting Brightpearl calls as "${SERVICE}"`);
}

install();
