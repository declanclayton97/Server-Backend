// returnsReport.js — what comes back, and how often.
//
// Two sources, because each answers a different question:
//   Brightpearl  every return on every channel: sales credits against sales, by style,
//                brand and channel. This is the RETURN RATE. Built in the background
//                (tens of API calls) and cached in the database.
//   Online form  WHY it came back: reasons and swaps, only for returns customers raised
//                at the returns page. This is where "runs small" comes from.
//
// A "sale" is counted exactly as the margin reports count one: statuses 1, 5, 18, 36
// and 60 are not sales, nor are orders whose reference says exchange, replacement or
// sample. Credits are every sales credit (order type 3) created in the window —
// exchanges included, since those goods came back too.

import { fitVotes, fitSignal, styleName } from "./returns.js";

export const NOT_SALE_STATUS_IDS = new Set([5, 36, 1, 18, 60]);
const NOT_SALE_REF_RE = /exchange|replacement|sample/i;
const ymd = (d) => d.toISOString().slice(0, 10);

async function searchIds(bpLive, typeId, from, to, keep) {
  const ids = [];
  for (let first = 1; ; ) {
    const r = await bpLive("GET", `/order-service/order-search?orderTypeId=${typeId}&createdOn=${from}T00:00:00/${to}T23:59:59&pageSize=500&firstResult=${first}`);
    const md = r && r.metaData;
    if (!md) break;
    const ix = Object.fromEntries(md.columns.map((c, i) => [c.name, i]));
    for (const row of r.results || []) if (!keep || keep(row, ix)) ids.push(Number(row[ix.orderId]));
    if (!md.morePagesAvailable && (md.lastResult || 0) >= (md.resultsAvailable || 0)) break;
    if (!(r.results || []).length) break;
    first = (md.lastResult || first + 499) + 1;
  }
  return [...new Set(ids)].sort((a, b) => a - b);
}

async function fetchInBatches(bpLive, path, ids, size = 200) {
  const out = [];
  for (let i = 0; i < ids.length; i += size) {
    const part = ids.slice(i, i + size);   // ascending, or Brightpearl 400s (CMNC-006)
    try { out.push(...((await bpLive("GET", `${path}/${part.join(",")}`)) || [])); }
    catch (e) {
      // One bad id fails the whole set — halve until the bad one is isolated.
      if (part.length === 1) continue;
      out.push(...await fetchInBatches(bpLive, path, part, Math.ceil(part.length / 2)));
    }
  }
  return out;
}

export async function buildBrightpearlReport({ bpLive, days = 90, today = new Date() }) {
  const to = ymd(today), from = ymd(new Date(today.getTime() - days * 864e5));
  const saleIds = await searchIds(bpLive, 1, from, to, (row, ix) => !NOT_SALE_STATUS_IDS.has(Number(row[ix.orderStatusId])));
  const creditIds = await searchIds(bpLive, 3, from, to);
  const sales = (await fetchInBatches(bpLive, "/order-service/order", saleIds)).filter((o) => !NOT_SALE_REF_RE.test(o.reference || ""));
  const credits = await fetchInBatches(bpLive, "/order-service/order", creditIds);

  // Every product on either side, once, for its style group, brand and whether it is
  // real stock (carriage, decoration and typed notes are not "items").
  const pids = [...new Set([...sales, ...credits].flatMap((o) => Object.values(o.orderRows || {}).map((r) => Number(r.productId))).filter(Boolean))].sort((a, b) => a - b);
  const meta = {};
  for (const p of await fetchInBatches(bpLive, "/product-service/product", pids)) {
    meta[p.id] = { group: p.productGroupId || null, brandId: p.brandId || null, stock: !!(p.stock && p.stock.stockTracked) };
  }
  const brands = Object.fromEntries(((await bpLive("GET", "/product-service/brand")) || []).map((b) => [b.id, b.name]));
  const channels = Object.fromEntries(((await bpLive("GET", "/product-service/channel")) || []).map((c) => [c.id, c.name]));

  const styles = {}, byBrand = {}, byChannel = {};
  const tally = (orders, field) => {
    for (const o of orders) {
      const ch = channels[o.assignment && o.assignment.current && o.assignment.current.channelId] || "Other";
      for (const r of Object.values(o.orderRows || {})) {
        const m = meta[Number(r.productId)];
        if (!m || !m.stock) continue;
        const qty = Math.abs(Number((r.quantity && r.quantity.magnitude) || 0));
        if (!qty) continue;
        const key = m.group ? "g" + m.group : "p" + r.productId;
        const s = styles[key] || (styles[key] = { key, group: m.group, names: {}, brand: brands[m.brandId] || "", sold: 0, returned: 0, productIds: [] });
        const nm = styleName(r.productName);
        if (nm) s.names[nm] = (s.names[nm] || 0) + qty;
        if (!s.productIds.includes(Number(r.productId))) s.productIds.push(Number(r.productId));
        s[field] += qty;
        const b = byBrand[s.brand || "Unbranded"] || (byBrand[s.brand || "Unbranded"] = { brand: s.brand || "Unbranded", sold: 0, returned: 0 });
        b[field] += qty;
        const c = byChannel[ch] || (byChannel[ch] = { channel: ch, sold: 0, returned: 0 });
        c[field] += qty;
      }
    }
  };
  tally(sales, "sold");
  tally(credits, "returned");

  const rate = (x) => (x.sold > 0 ? x.returned / x.sold : null);
  const styleRows = Object.values(styles).map((s) => ({
    key: s.key, group: s.group, brand: s.brand, sold: s.sold, returned: s.returned, rate: rate(s), productIds: s.productIds,
    // The most-sold spelling of the style's name.
    name: Object.entries(s.names).sort((a, b) => b[1] - a[1])[0]?.[0] || "(unnamed)",
  }));
  return {
    days, from, to, builtAt: new Date().toISOString(),
    totals: { orders: sales.length, credits: credits.length, sold: styleRows.reduce((a, s) => a + s.sold, 0), returned: styleRows.reduce((a, s) => a + s.returned, 0) },
    styles: styleRows,
    brands: Object.values(byBrand).map((b) => ({ ...b, rate: rate(b) })),
    channels: Object.values(byChannel).map((c) => ({ ...c, rate: rate(c) })),
  };
}

// The online returns for the same window, rolled up by reason and by style, with the
// fit signal. `productToKey` maps a product id to the Brightpearl report's style key,
// so both halves line up on the same rows.
export function summariseOnline(rows, productToKey = {}) {
  const reasons = {}, styles = {};
  let refunds = 0, swaps = 0, units = 0;
  for (const row of rows) {
    if (/cancelled/.test(row.status)) continue;
    for (const l of row.lines || []) {
      const q = Number(l.qty || 1);
      units += q;
      if (l.outcome === "exchange") swaps += q; else refunds += q;
      const label = l.outcome === "exchange" ? "Swap: " + String(l.exchangeChoice || "").toLowerCase() : String(l.reason || "Other");
      reasons[label] = (reasons[label] || 0) + q;
      const key = productToKey[l.productId] || "n:" + styleName(l.name);
      const s = styles[key] || (styles[key] = { key, name: styleName(l.name), units: 0, small: 0, big: 0, reasons: {} });
      s.units += q;
      const v = fitVotes(l); s.small += v.small; s.big += v.big;
      s.reasons[label] = (s.reasons[label] || 0) + q;
    }
  }
  for (const s of Object.values(styles)) s.fit = fitSignal(s.small, s.big);
  return {
    requests: rows.filter((r) => !/cancelled/.test(r.status)).length, units, refunds, swaps,
    reasons: Object.entries(reasons).map(([reason, n]) => ({ reason, n })).sort((a, b) => b.n - a.n),
    styles: Object.values(styles),
  };
}
