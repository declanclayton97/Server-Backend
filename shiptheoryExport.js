// shiptheoryExport.js — MID code + GTIN onto Shiptheory products for orders going abroad (Matt, 29 Oct 2026).
//
// New EU customs rules need every product on a shipment out of GB to carry a MID code and a
// GTIN. Shiptheory keeps its own product list (by SKU), and its Brightpearl integration
// doesn't fill these yet, so this copies them from BP until it does:
//   BP identity.mpn            -> Shiptheory export_details.mid_code (Shiptheory confirmed MPN, 29 Oct)
//   BP identity.barcode / ean  -> Shiptheory export_details.gtin
//
// - Every 15 minutes: sales orders updated since the last look, delivering outside GB ->
//   each product on them is set in Shiptheory (created there by SKU if it isn't yet), so the
//   details are in place before the label is booked.
// - POST /api/shiptheory-export/backfill { days }   one-off catch-up over past orders (default 365)
// - POST /api/shiptheory-export/sku/:sku            set one SKU now
// - GET  /api/shiptheory-export                     counts, last run, products missing an MPN/barcode
//
// Done SKUs are kept in `shiptheory_export`, so a product is only sent again when its BP
// values change. Needs SHIPTHEORY_USER / SHIPTHEORY_PASS (the same API login as tuff-wms).

const API = "https://api.shiptheory.com/v1";
const POLL_MS = 15 * 60 * 1000;
const DAY = 24 * 60 * 60 * 1000;
const MISSING_RECHECK_MS = DAY;
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

export const configured = () => !!(process.env.SHIPTHEORY_USER && process.env.SHIPTHEORY_PASS);

// Pure: is this order going outside GB? (Channel Islands / IoM count: they clear customs too.)
export function goesAbroad(order) {
  const c = String(order?.parties?.delivery?.countryIsoCode || "").toUpperCase();
  return !!c && c !== "GB";
}

// Pure: real product ids on an order (not the 1000 "misc" lines for shipping and notes).
export function productIdsOf(order) {
  return Object.values(order?.orderRows || {}).map((r) => Number(r.productId)).filter((id) => id && id !== 1000);
}

// Pure: what Shiptheory should hold for a BP product. GTIN only when the barcode is one
// (8/12/13/14 digits); anything else is reported rather than sent.
export function wantedFor(p) {
  const id = p?.identity || {};
  const sku = String(id.sku || "").trim();
  const mid = String(id.mpn || "").trim();
  const code = String(id.barcode || id.ean || "").trim();
  const gtin = /^(\d{8}|\d{12,14})$/.test(code) ? code : "";
  const missing = [];
  if (!mid) missing.push("no MPN");
  if (!code) missing.push("no barcode");
  else if (!gtin) missing.push(`barcode "${code}" is not a GTIN`);
  const name = String(p?.salesChannels?.[0]?.productName || "").split(/\r?\n/)[0].trim().slice(0, 255);
  return { productId: p?.id, sku, mid_code: mid, gtin, name, missing };
}

// Pure: the Shiptheory request for this product, or null when it already matches.
// existing = the Shiptheory product (null when it isn't in Shiptheory yet).
export function plan(existing, want) {
  const set = {};
  if (want.mid_code) set.mid_code = want.mid_code;
  if (want.gtin) set.gtin = want.gtin;
  if (!Object.keys(set).length) return null;
  if (!existing) {
    const body = { sku: want.sku, export_details: set };
    if (want.name) body.name = want.name;
    if (want.gtin && want.gtin.length <= 15) body.barcode = want.gtin;
    return { method: "POST", path: "/products", body };
  }
  const cur = existing.export_details || {};
  if (Object.entries(set).every(([k, v]) => String(cur[k] ?? "") === v)) return null;
  const keep = Object.fromEntries(Object.entries(cur).filter(([, v]) => v !== null && v !== ""));
  return { method: "PUT", path: `/products/update/${encodeURIComponent(want.sku)}`, body: { sku: want.sku, export_details: { ...keep, ...set } } };
}

let tok = { value: null, at: 0 };
async function token() {
  if (tok.value && Date.now() - tok.at < 50 * 60 * 1000) return tok.value;
  if (!configured()) throw new Error("No Shiptheory login on the server (SHIPTHEORY_USER / SHIPTHEORY_PASS)");
  const r = await fetch(`${API}/token`, { method: "POST", headers: { "Content-Type": "application/json", Accept: "application/json" },
    body: JSON.stringify({ email: process.env.SHIPTHEORY_USER, password: process.env.SHIPTHEORY_PASS }) });
  const j = await r.json().catch(() => ({}));
  if (!r.ok || !j?.data?.token) throw new Error(`Shiptheory sign-in failed (${r.status})`);
  tok = { value: j.data.token, at: Date.now() };
  return tok.value;
}

async function st(method, path, body) {
  for (let attempt = 0; attempt < 3; attempt++) {
    const r = await fetch(`${API}${path}`, { method, headers: { Accept: "application/json", "Content-Type": "application/json", Authorization: `Bearer ${await token()}` },
      body: body === undefined ? undefined : JSON.stringify(body) });
    if (r.status === 401 && attempt === 0) { tok = { value: null, at: 0 }; continue; }
    if (r.status === 429) { await sleep(5000); continue; }
    const text = await r.text();
    let json = null; try { json = JSON.parse(text); } catch { /* non-JSON */ }
    return { status: r.status, json, text };
  }
  throw new Error(`Shiptheory ${method} ${path}: gave up after retries`);
}

export function registerShiptheoryExport(app, { bpLive, getPool }) {
  const state = { lastPoll: null, lastPollResult: null, backfill: null };
  let since = new Date(Date.now() - 60 * 60 * 1000);

  const ensure = () => getPool().query(`CREATE TABLE IF NOT EXISTS shiptheory_export (
    sku text PRIMARY KEY, product_id bigint, mid_code text, gtin text,
    status text NOT NULL, detail text, updated_at timestamptz NOT NULL DEFAULT now())`);

  async function record(want, status, detail) {
    await getPool().query(`INSERT INTO shiptheory_export (sku, product_id, mid_code, gtin, status, detail, updated_at)
      VALUES ($1,$2,$3,$4,$5,$6,now()) ON CONFLICT (sku) DO UPDATE SET product_id=$2, mid_code=$3, gtin=$4, status=$5, detail=$6, updated_at=now()`,
      [want.sku, want.productId, want.mid_code, want.gtin, status, detail || null]);
  }

  // One BP product into Shiptheory. Returns the status recorded.
  async function syncProduct(p, { force = false } = {}) {
    const want = wantedFor(p);
    if (!want.sku) return "no sku";
    const prev = (await getPool().query(`SELECT * FROM shiptheory_export WHERE sku = $1`, [want.sku])).rows[0];
    if (!force && prev && (prev.mid_code || "") === want.mid_code && (prev.gtin || "") === want.gtin) {
      if (prev.status !== "missing" && prev.status !== "error") return "unchanged";
      if (Date.now() - new Date(prev.updated_at).getTime() < MISSING_RECHECK_MS) return "unchanged";
    }
    if (!want.mid_code && !want.gtin) { await record(want, "missing", want.missing.join(", ")); return "missing"; }
    try {
      const view = await st("GET", `/products/view/${encodeURIComponent(want.sku)}`);
      const existing = view.status === 200 ? (view.json?.product || null) : null;
      if (view.status !== 200 && view.status !== 404) throw new Error(`view ${view.status} ${view.text.slice(0, 160)}`);
      const req = plan(existing, want);
      const note = want.missing.length ? want.missing.join(", ") : null;
      if (!req) { await record(want, "set", note); return "set"; }
      const r = await st(req.method, req.path, req.body);
      if (r.status >= 300 || r.json?.error) throw new Error(`${req.method} ${r.status} ${r.text.slice(0, 200)}`);
      const status = req.method === "POST" ? "created" : "set";
      await record(want, status, note);
      return status;
    } catch (e) {
      await record(want, "error", e.message);
      return "error";
    }
  }

  // BP products by id, 100 at a time (BP wants the ids in ascending order).
  async function bpProducts(ids) {
    const out = [];
    const sorted = [...new Set(ids)].sort((a, b) => a - b);
    for (let i = 0; i < sorted.length; i += 100) {
      const res = await bpLive("GET", `/product-service/product/${sorted.slice(i, i + 100).join(",")}`);
      out.push(...(res || []));
    }
    return out;
  }

  async function bpOrders(ids, gapMs = 0) {
    const out = [];
    const sorted = [...new Set(ids)].sort((a, b) => a - b);
    for (let i = 0; i < sorted.length; i += 100) {
      out.push(...((await bpLive("GET", `/order-service/order/${sorted.slice(i, i + 100).join(",")}`)) || []));
      if (gapMs) await sleep(gapMs);
    }
    return out;
  }

  async function searchOrderIds(filter) {
    const ids = [];
    for (let first = 1; ; first += 500) {
      const res = await bpLive("GET", `/order-service/order-search?orderTypeId=1&${filter}&columns=orderId&pageSize=500&firstResult=${first}`);
      ids.push(...(res?.results || []).map((r) => Number(r[0])));
      if (!res?.metaData?.morePagesAvailable) break;
    }
    return ids;
  }

  async function syncOrders(orders, tally) {
    const abroad = orders.filter(goesAbroad);
    tally.orders += abroad.length;
    const products = await bpProducts(abroad.flatMap(productIdsOf));
    for (const p of products) {
      const s = await syncProduct(p);
      tally[s] = (tally[s] || 0) + 1;
    }
  }

  async function poll() {
    if (!configured() || !getPool() || state.backfill?.running) return;
    const from = new Date(since.getTime() - 5 * 60 * 1000);
    const started = new Date();
    try {
      await ensure();
      const stamp = from.toISOString().slice(0, 19);
      const ids = await searchOrderIds(`updatedOn=${encodeURIComponent(stamp + "/")}`);
      const tally = { orders: 0 };
      await syncOrders(await bpOrders(ids), tally);
      since = started;
      state.lastPoll = started.toISOString();
      state.lastPollResult = { checked: ids.length, ...tally };
    } catch (e) {
      state.lastPollResult = { error: e.message };
      console.error("[shiptheory-export] poll failed:", e.message);
    }
  }

  async function backfill(days) {
    const b = state.backfill = { running: true, days, startedAt: new Date().toISOString(), orders: 0, abroad: 0, tally: { orders: 0 }, error: null };
    try {
      await ensure();
      const to = new Date();
      const from = new Date(to.getTime() - days * DAY);
      const ids = await searchOrderIds(`createdOn=${from.toISOString().slice(0, 10)}/${to.toISOString().slice(0, 10)}`);
      b.orders = ids.length;
      // Gently: one BP call a second, and products as each 1,000 orders are read.
      for (let i = 0; i < ids.length; i += 1000) {
        await syncOrders(await bpOrders(ids.slice(i, i + 1000), 1000), b.tally);
        b.done = Math.min(i + 1000, ids.length);
      }
    } catch (e) {
      b.error = e.message;
      console.error("[shiptheory-export] backfill failed:", e.message);
    }
    b.running = false;
    b.finishedAt = new Date().toISOString();
  }

  app.get("/api/shiptheory-export", async (req, res) => {
    try {
      const out = { configured: configured(), lastPoll: state.lastPoll, lastPollResult: state.lastPollResult, backfill: state.backfill };
      if (getPool()) {
        await ensure();
        out.counts = Object.fromEntries((await getPool().query(`SELECT status, count(*)::int n FROM shiptheory_export GROUP BY status`)).rows.map((r) => [r.status, r.n]));
        out.needsAttention = (await getPool().query(`SELECT sku, product_id, mid_code, gtin, status, detail, updated_at FROM shiptheory_export
          WHERE status IN ('missing','error') OR detail IS NOT NULL ORDER BY updated_at DESC LIMIT 500`)).rows;
      }
      res.json(out);
    } catch (e) { res.status(500).json({ error: e.message }); }
  });

  app.post("/api/shiptheory-export/sku/:sku", async (req, res) => {
    try {
      if (!configured()) return res.status(503).json({ error: "No Shiptheory login on the server (SHIPTHEORY_USER / SHIPTHEORY_PASS)" });
      await ensure();
      const found = await bpLive("GET", `/product-service/product-search?SKU=${encodeURIComponent(req.params.sku)}&columns=productId`);
      const id = found?.results?.[0]?.[0];
      if (!id) return res.status(404).json({ error: `SKU ${req.params.sku} not found in Brightpearl` });
      const [p] = await bpProducts([Number(id)]);
      const status = await syncProduct(p, { force: true });
      const view = await st("GET", `/products/view/${encodeURIComponent(req.params.sku)}`);
      res.json({ status, want: wantedFor(p), shiptheory: view.json?.product || view.json });
    } catch (e) { res.status(500).json({ error: e.message }); }
  });

  app.post("/api/shiptheory-export/backfill", (req, res) => {
    if (!configured()) return res.status(503).json({ error: "No Shiptheory login on the server (SHIPTHEORY_USER / SHIPTHEORY_PASS)" });
    if (state.backfill?.running) return res.status(409).json({ error: "A backfill is already running", backfill: state.backfill });
    const days = Math.min(Math.max(Number(req.body?.days) || 365, 1), 730);
    backfill(days);
    res.json({ started: true, days });
  });

  if (!configured()) { console.log("[shiptheory-export] no SHIPTHEORY_USER/PASS - not polling"); return; }
  setTimeout(poll, 60 * 1000);
  setInterval(poll, POLL_MS);
}
