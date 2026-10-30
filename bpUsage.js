// /bp-usage: who is spending the Brightpearl API allowance. Reads bp_usage, which
// bpMeter.js fills from every service that imports it (one row per minute, service,
// calling code and endpoint). GET /api/bp-usage?hours=N returns the numbers; /bp-usage
// draws them.

const MAX_HOURS = 24 * 14;

export function registerBpUsage(app, { getPool }) {
  app.get('/api/bp-usage', async (req, res) => {
    const pool = getPool();
    if (!pool) return res.status(503).json({ error: 'no database' });
    const hours = Math.min(Math.max(Number(req.query.hours) || 6, 0.25), MAX_HOURS);
    const since = `now() - interval '${hours * 60} minutes'`;
    // Only the live account shares the allowance (tuff-wms-test runs on the test account).
    const acct = String(req.query.account || process.env.BRIGHTPEARL_ACCOUNT_ID || 'tuffworkwear');
    try {
      const exists = await pool.query(`SELECT to_regclass('bp_usage') AS t`);
      if (!exists.rows[0].t) return res.json({ hours, perMinute: [], callers: [], busiest: [], services: [] });

      const perMinute = (await pool.query(`
        SELECT minute, service, SUM(calls)::int AS calls, SUM(throttled)::int AS throttled, MIN(min_remaining) AS min_remaining
        FROM bp_usage WHERE minute >= ${since} AND account = $1 GROUP BY minute, service ORDER BY minute`, [acct])).rows;

      const callers = (await pool.query(`
        WITH m AS (
          SELECT service, caller, minute, SUM(calls) AS calls, SUM(throttled) AS throttled
          FROM bp_usage WHERE minute >= ${since} AND account = $1 GROUP BY service, caller, minute)
        SELECT m.service, m.caller, SUM(m.calls)::int AS calls, SUM(m.throttled)::int AS throttled,
               MAX(m.calls)::int AS peak, COUNT(*)::int AS active_minutes,
               (SELECT string_agg(endpoint || ' x' || n, ', ' ORDER BY n DESC) FROM (
                  SELECT endpoint, SUM(calls)::int AS n FROM bp_usage b
                  WHERE b.minute >= ${since} AND b.account = $1 AND b.service = m.service AND b.caller = m.caller
                  GROUP BY endpoint ORDER BY n DESC LIMIT 4) e) AS endpoints
        FROM m GROUP BY m.service, m.caller ORDER BY calls DESC LIMIT 60`, [acct])).rows;

      const busiest = (await pool.query(`
        WITH t AS (
          SELECT minute, SUM(calls)::int AS calls, SUM(throttled)::int AS throttled, MIN(min_remaining) AS min_remaining
          FROM bp_usage WHERE minute >= ${since} AND account = $1 GROUP BY minute ORDER BY calls DESC LIMIT 15)
        SELECT t.*, (SELECT json_agg(x) FROM (
            SELECT service, caller, SUM(calls)::int AS calls FROM bp_usage b
            WHERE b.minute = t.minute AND b.account = $1 GROUP BY service, caller ORDER BY calls DESC LIMIT 4) x) AS top
        FROM t ORDER BY t.calls DESC`, [acct])).rows;

      const services = [...new Set(perMinute.map((r) => r.service))].sort();
      res.json({ hours, account: acct, perMinute, callers, busiest, services, now: new Date().toISOString() });
    } catch (e) {
      res.status(500).json({ error: e.message });
    }
  });

  app.get('/bp-usage', (req, res) => {
    res.set('Content-Type', 'text/html; charset=utf-8').send(PAGE);
  });
}

const PAGE = `<!doctype html>
<html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>BP API Usage</title>
<style>
  :root { --bg:#f7f7f5; --card:#fff; --ink:#1f2328; --muted:#6b7280; --line:#e5e7eb; --bad:#c2410c; --cap:#9ca3af; }
  @media (prefers-color-scheme: dark) { :root { --bg:#16181d; --card:#1f232a; --ink:#e6e8eb; --muted:#9aa3ae; --line:#2e333b; --bad:#fb923c; --cap:#6b7280; } }
  * { box-sizing:border-box; }
  body { margin:0; background:var(--bg); color:var(--ink); font:14px/1.45 system-ui,-apple-system,Segoe UI,sans-serif; }
  main { max-width:1200px; margin:0 auto; padding:20px 16px 40px; }
  h1 { font-size:20px; margin:0 0 4px; } h2 { font-size:15px; margin:0 0 10px; }
  .sub { color:var(--muted); margin:0 0 16px; }
  .card { background:var(--card); border:1px solid var(--line); border-radius:10px; padding:14px 16px; margin-bottom:14px; overflow-x:auto; }
  .row { display:flex; gap:10px; flex-wrap:wrap; align-items:center; margin-bottom:14px; }
  .row button { border:1px solid var(--line); background:var(--card); color:var(--ink); border-radius:6px; padding:5px 10px; cursor:pointer; }
  .row button.on { background:var(--ink); color:var(--bg); }
  .stats { display:grid; grid-template-columns:repeat(auto-fit,minmax(150px,1fr)); gap:10px; margin-bottom:14px; }
  .stat { background:var(--card); border:1px solid var(--line); border-radius:10px; padding:10px 14px; }
  .stat b { display:block; font-size:22px; font-variant-numeric:tabular-nums; } .stat span { color:var(--muted); font-size:12px; }
  table { border-collapse:collapse; width:100%; font-variant-numeric:tabular-nums; }
  th, td { text-align:left; padding:6px 8px; border-bottom:1px solid var(--line); vertical-align:top; }
  th { color:var(--muted); font-weight:600; font-size:12px; } td.n { text-align:right; }
  code { font-size:12px; word-break:break-word; } .muted { color:var(--muted); font-size:12px; } .bad { color:var(--bad); font-weight:600; }
  .legend { display:flex; gap:14px; flex-wrap:wrap; font-size:12px; color:var(--muted); margin-top:8px; }
  .legend i { display:inline-block; width:10px; height:10px; border-radius:2px; margin-right:5px; vertical-align:-1px; }
  svg text { fill:var(--muted); font-size:11px; }
</style></head><body><main>
<h1>Brightpearl API usage</h1>
<p class="sub">Calls our own services make to Brightpearl, counted as they happen. BP allows about 200 a minute across the whole account, including outside apps (Shiptheory, channels), which are not counted here: if BP says almost nothing is left while our count for that minute is low, the rest went to an outside app.</p>
<div class="row" id="ranges"></div>
<div class="stats" id="stats"></div>
<div class="card"><h2>Calls per minute, by service</h2><div id="chart"></div><div class="legend" id="legend"></div></div>
<div class="card"><h2>What made the calls</h2><div id="callers"></div></div>
<div class="card"><h2>Busiest minutes</h2><div id="busiest"></div></div>
<p class="muted" id="foot"></p>
</main>
<script>
const COLORS = ["#2563eb","#16a34a","#d97706","#9333ea","#0891b2","#db2777","#65a30d","#64748b"];
const RANGES = [[1,"1 hour"],[3,"3 hours"],[6,"6 hours"],[24,"24 hours"],[72,"3 days"],[168,"7 days"]];
let hours = 6; try { hours = Number(localStorage.getItem("bpUsageHours")) || 6; } catch {}
const esc = (s) => String(s ?? "").replace(/[&<>"]/g, (c) => ({"&":"&amp;","<":"&lt;",">":"&gt;",'"':"&quot;"}[c]));
const hm = (d) => new Date(d).toLocaleString("en-GB", { weekday:"short", hour:"2-digit", minute:"2-digit" });
function ranges() {
  document.getElementById("ranges").innerHTML = RANGES.map(([h,l]) => '<button data-h="'+h+'" class="'+(h===hours?"on":"")+'">'+l+'</button>').join("") + '<span class="muted">refreshes every minute</span>';
}
document.getElementById("ranges").addEventListener("click", (e) => {
  const b = e.target.closest("button"); if (!b) return;
  hours = Number(b.dataset.h); try { localStorage.setItem("bpUsageHours", hours); } catch {}
  ranges(); load();
});
function chart(d) {
  const svcs = d.services, color = (s) => COLORS[svcs.indexOf(s) % COLORS.length];
  const byMin = new Map();
  for (const r of d.perMinute) {
    const t = new Date(r.minute).getTime();
    const m = byMin.get(t) || { t, total:0, thr:0, rem:null, by:{} };
    m.by[r.service] = (m.by[r.service] || 0) + r.calls; m.total += r.calls; m.thr += r.throttled;
    if (r.min_remaining != null) m.rem = m.rem == null ? r.min_remaining : Math.min(m.rem, r.min_remaining);
    byMin.set(t, m);
  }
  const end = Date.now() - Date.now() % 60000, start = end - hours * 3600e3;
  // Wider ranges show the busiest minute in each bucket, so short bursts still show.
  const bucket = hours <= 3 ? 1 : hours <= 6 ? 2 : hours <= 24 ? 5 : hours <= 72 ? 15 : 30;
  const bars = [];
  for (let t = start; t < end; t += bucket * 60000) {
    let best = null;
    for (let k = 0; k < bucket; k++) { const m = byMin.get(t + k * 60000); if (m && (!best || m.total > best.total)) best = m; }
    bars.push({ t, m: best });
  }
  const W = 1100, H = 240, L = 36, B = 22, max = Math.max(220, ...bars.map((b) => b.m ? b.m.total : 0));
  const bw = (W - L) / bars.length, y = (v) => H - B - (v / max) * (H - B - 8);
  let s = '<svg viewBox="0 0 '+W+' '+H+'" width="100%" role="img" aria-label="Calls per minute">';
  for (const v of [0, 50, 100, 150, 200].filter((v) => v <= max)) s += '<line x1="'+L+'" x2="'+W+'" y1="'+y(v)+'" y2="'+y(v)+'" stroke="var(--line)"/><text x="'+(L-6)+'" y="'+(y(v)+4)+'" text-anchor="end">'+v+'</text>';
  s += '<line x1="'+L+'" x2="'+W+'" y1="'+y(200)+'" y2="'+y(200)+'" stroke="var(--cap)" stroke-dasharray="4 4"/><text x="'+(W-4)+'" y="'+(y(200)-4)+'" text-anchor="end">BP limit ~200/min</text>';
  bars.forEach((b, i) => {
    const x = L + i * bw;
    if (b.m) {
      let acc = 0;
      for (const sv of svcs) { const v = b.m.by[sv] || 0; if (!v) continue;
        s += '<rect x="'+(x+0.5)+'" y="'+y(acc+v)+'" width="'+Math.max(bw-1,1)+'" height="'+(y(acc)-y(acc+v))+'" fill="'+color(sv)+'"><title>'+hm(b.m.t)+' '+esc(sv)+': '+v+'</title></rect>'; acc += v; }
      if (b.m.thr) s += '<rect x="'+x+'" y="'+(H-B+3)+'" width="'+Math.max(bw,2)+'" height="4" fill="var(--bad)"><title>'+b.m.thr+' throttled</title></rect>';
    }
  });
  const ticks = 6;
  for (let k = 0; k <= ticks; k++) { const i = Math.min(bars.length - 1, Math.round(k * (bars.length - 1) / ticks)); if (bars[i]) s += '<text x="'+(L + i*bw)+'" y="'+(H-2)+'" text-anchor="middle">'+hm(bars[i].t)+'</text>'; }
  s += '</svg>';
  document.getElementById("chart").innerHTML = s;
  document.getElementById("legend").innerHTML = svcs.map((sv) => '<span><i style="background:'+color(sv)+'"></i>'+esc(sv)+'</span>').join("") + '<span><i style="background:var(--bad)"></i>throttled by BP</span>' + (bucket > 1 ? '<span>each bar = busiest minute in '+bucket+' min</span>' : '');
  const all = [...byMin.values()], total = all.reduce((a, m) => a + m.total, 0), thr = all.reduce((a, m) => a + m.thr, 0);
  const peak = all.reduce((a, m) => (m.total > (a ? a.total : -1) ? m : a), null);
  const mins = Math.max(1, Math.round(hours * 60));
  document.getElementById("stats").innerHTML = [
    [total.toLocaleString(), "calls in range"], [(total / mins).toFixed(1), "average per minute"],
    [peak ? peak.total : 0, peak ? "busiest minute (" + hm(peak.t) + ")" : "busiest minute"],
    ['<span class="'+(thr?"bad":"")+'">'+thr+'</span>', "calls throttled (503)"],
  ].map(([v, l]) => '<div class="stat"><b>'+v+'</b><span>'+l+'</span></div>').join("");
}
function callers(d) {
  const total = d.callers.reduce((a, c) => a + c.calls, 0) || 1;
  document.getElementById("callers").innerHTML = d.callers.length ? '<table><tr><th>Service</th><th>Code</th><th>Endpoints</th><th class="n">Calls</th><th class="n">Share</th><th class="n">Peak / min</th><th class="n">Minutes active</th><th class="n">Throttled</th></tr>' +
    d.callers.map((c) => '<tr><td>'+esc(c.service)+'</td><td><code>'+esc(c.caller)+'</code></td><td class="muted">'+esc(c.endpoints)+'</td><td class="n">'+c.calls.toLocaleString()+'</td><td class="n">'+(100*c.calls/total).toFixed(1)+'%</td><td class="n">'+c.peak+'</td><td class="n">'+c.active_minutes+'</td><td class="n '+(c.throttled?"bad":"")+'">'+c.throttled+'</td></tr>').join("") + '</table>'
    : '<p class="muted">Nothing counted yet. Services start reporting once they are deployed with the meter, a minute after their first BP call.</p>';
}
function busiest(d) {
  document.getElementById("busiest").innerHTML = d.busiest.length ? '<table><tr><th>Minute</th><th class="n">Our calls</th><th class="n">BP said left</th><th class="n">Throttled</th><th>Biggest callers that minute</th></tr>' +
    d.busiest.map((m) => '<tr><td>'+hm(m.minute)+'</td><td class="n">'+m.calls+'</td><td class="n">'+(m.min_remaining ?? "-")+'</td><td class="n '+(m.throttled?"bad":"")+'">'+m.throttled+'</td><td>'+(m.top||[]).map((t) => esc(t.service)+' <code>'+esc(t.caller)+'</code> x'+t.calls).join("<br>")+'</td></tr>').join("") + '</table>'
    : '<p class="muted">Nothing yet.</p>';
}
async function load() {
  try {
    const r = await fetch("/api/bp-usage?hours=" + hours); const d = await r.json();
    if (d.error) throw new Error(d.error);
    chart(d); callers(d); busiest(d);
    document.getElementById("foot").textContent = "Updated " + new Date().toLocaleTimeString("en-GB") + ". Counted services: " + (d.services.join(", ") || "none yet") + ".";
  } catch (e) { document.getElementById("foot").textContent = "Could not load: " + e.message; }
}
ranges(); load(); setInterval(load, 60000);
</script></body></html>`;
