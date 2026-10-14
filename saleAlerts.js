// saleAlerts.js - "something you bought is now in our sale, in your size" emails.
//
// Items come from the website's sale section (tuffshop.co.uk/sale.html, Dec 12 Oct 2026 - replaced the STW
// warehouse source). Each weekday it features a few sale pages it has not featured before, maps every size still
// salable on the page back to the original BP product, finds everyone who bought that exact product and size
// (BP goods-out movements) and messages them: WhatsApp (template sale_item_alert) if they have a UK mobile, else email
// from info@ through smtp2go (authorised: DKIM+SPF -> DMARC pass), with a direct link to the sale item.
// No price in the message; first come, first served. Never emailed:
//   - anyone with a sales credit for that product (returned it)
//   - Amazon / eBay buyers (relay addresses), company webstores, decorated (embroidered/printed) purchases
//   - trade/rep channels unless SALE_ALERTS_TRADE=on
//   - unsubscribed addresses, staff/supplier contacts, anyone alerted in the last 7 days
// Every send, skip and failure is a row in sale_alert_sends.
//
// Env:
//   SALE_ALERTS_ENABLED  off (default) | review | live
//       review: the daily run emails SALE_ALERTS_REVIEW_TO the batch it WOULD send; no customer is emailed.
//       live:   customers are emailed.
//   SALE_ALERTS_SECRET   signs unsubscribe links (required for review and live)
//   SALE_ALERTS_FROM (info@tuffshop.co.uk), SALE_ALERTS_REVIEW_TO (dec@tuffshop.co.uk), SALE_ALERTS_PER_DAY (5),
//   SALE_ALERTS_AT (10:00, weekdays, until +60 min),
//   SALE_ALERTS_TRADE (off), SALE_ALERTS_LOOKBACK_DAYS (1095), SALE_ALERTS_PUBLIC_URL (this service's base URL)
//   SALE_ALERTS_SITE_HEADER  "Name: secret" sent to tuffshop.co.uk so a Cloudflare skip rule lets the sale pages through

import crypto from "crypto";
import { bpLiveGet } from "./purchasingAuto.js";
import { sendNew, mailboxContext, graphConfigured, salesMailbox } from "./graphMail.js";

const WAREHOUSE = 2;
const WEBSITE_CHANNELS = new Set([2, 3, 14, 17]);                                   // tuffshop.co.uk (old/online/Magento), tuffkids
const TRADE_CHANNELS = new Set([1, 4, 5, 6, 8, 10, 11, 12, 15, 16, 21, 33, 34]);    // phone, shop, email, reps, trades hub
// Everything else (Amazon 9, eBay 7, the company webstores) is never emailed.
const DECORATION = /^OP(EM|PR)/i, DECORATION_NAME = /embroid|\bprint(ed|ing)?\b|set ?up fee|upload logo|logo design/i;

const cfg = () => ({
  mode: String(process.env.SALE_ALERTS_ENABLED || "off").toLowerCase(),
  secret: process.env.SALE_ALERTS_SECRET || "",
  from: process.env.SALE_ALERTS_FROM || "info@tuffshop.co.uk",
  reviewTo: process.env.SALE_ALERTS_REVIEW_TO || "dec@tuffshop.co.uk",
  perDay: Math.max(1, Math.min(20, Number(process.env.SALE_ALERTS_PER_DAY) || 5)),
  at: process.env.SALE_ALERTS_AT || "10:00",
  trade: String(process.env.SALE_ALERTS_TRADE || "off").toLowerCase() === "on",
  whatsapp: String(process.env.SALE_ALERTS_WHATSAPP || "on").toLowerCase() !== "off",   // Dec 12 Oct: everyone with a UK mobile gets WhatsApp
  lookbackDays: Number(process.env.SALE_ALERTS_LOOKBACK_DAYS) || 1095,
  publicUrl: (process.env.SALE_ALERTS_PUBLIC_URL || "https://server-backend-1i47.onrender.com").replace(/\/$/, ""),
});

const esc = (s) => String(s ?? "").replace(/[&<>"']/g, (c) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[c]));
const chunk = (a, n) => { const o = []; for (let i = 0; i < a.length; i += n) o.push(a.slice(i, i + n)); return o; };
const idSet = (ids) => [...new Set(ids.map(Number))].sort((a, b) => a - b).join(",");   // BP 400s on unordered id-sets
const rowsOf = (resp) => { const c = resp.metaData.columns.map((x) => x.name); return resp.results.map((r) => Object.fromEntries(c.map((k, i) => [k, r[i]]))); };
const normEmail = (e) => String(e || "").trim().toLowerCase();
const ukDay = (d = new Date()) => d.toLocaleDateString("en-CA", { timeZone: "Europe/London" });

// ---- unsubscribe tokens ---------------------------------------------------------------------
export function unsubToken(email, secret = cfg().secret) {
  return crypto.createHmac("sha256", secret).update(normEmail(email)).digest("base64url").slice(0, 32);
}
const unsubUrl = (email) => { const c = cfg(); return `${c.publicUrl}/sale-alerts/unsubscribe?e=${encodeURIComponent(normEmail(email))}&t=${unsubToken(email)}`; };

// ---- BP reads ---------------------------------------------------------------------------------
async function movements(pid, type) {
  const out = [];
  for (let first = 1; ; first += 500) {
    const r = await bpLiveGet(`/warehouse-service/goods-movement-search?productId=${pid}&goodsNoteTypeCode=${type}&pageSize=500&firstResult=${first}`);
    out.push(...rowsOf(r));
    if (!r.metaData.morePagesAvailable) return out;
  }
}

async function orders(ids) {
  const out = [];
  for (const c of chunk([...new Set(ids)].sort((a, b) => a - b), 50)) out.push(...((await bpLiveGet(`/order-service/order/${c.join(",")}`)) || []));
  return out;
}

/**
 * Who should hear about this product? Every past buyer minus everyone the rules exclude.
 * Returns { product, eligible: [{contactId, email, firstName, orderId, date}], skipped: [{contactId, email, reason}] }.
 */
export async function recipientsFor(pid, { pool, cutoffDays = 7 } = {}) {
  const c = cfg();
  const since = new Date(Date.now() - c.lookbackDays * 864e5).toISOString().slice(0, 10);
  const shipped = await movements(pid, "GO");
  const sales = (await orders(shipped.map((m) => m.orderId).filter(Boolean))).filter((o) => o.orderTypeCode === "SO");
  // returns: credits that took this product back (goods-in against a credit) or credit the order it was sold on
  const returned = new Set();
  const gi = await movements(pid, "GI");
  for (const o of await orders(gi.map((m) => m.orderId).filter(Boolean))) if (o.orderTypeCode === "SC") returned.add(o.parties?.customer?.contactId);
  for (const o of sales) {
    const cr = rowsOf(await bpLiveGet(`/order-service/order-search?orderTypeId=3&parentOrderId=${o.id}&columns=orderId`));
    if (!cr.length) continue;
    for (const credit of await orders(cr.map((x) => x.orderId)))
      if (Object.values(credit.orderRows || {}).some((r) => Number(r.productId) === Number(pid))) returned.add(credit.parties?.customer?.contactId);
  }
  // one candidate per customer: their latest qualifying purchase
  const best = new Map(), skipped = [];
  for (const o of sales) {
    const cid = o.parties?.customer?.contactId, date = String(o.placedOn || o.createdOn || "").slice(0, 10), ch = Number(o.assignment?.current?.channelId);
    const why = !cid ? "no customer" : returned.has(cid) ? "returned it" : date < since ? "bought too long ago"
      : !(WEBSITE_CHANNELS.has(ch) || (c.trade && TRADE_CHANNELS.has(ch))) ? (TRADE_CHANNELS.has(ch) ? "trade channel (off)" : "marketplace / company webstore")
      : Object.values(o.orderRows || {}).some((r) => DECORATION.test(String(r.productSku || "")) || DECORATION_NAME.test(String(r.productName || ""))) ? "decorated order" : "";
    if (why) { skipped.push({ contactId: cid, orderId: o.id, reason: why }); continue; }
    if (!best.has(cid) || best.get(cid).date < date) best.set(cid, { contactId: cid, orderId: o.id, date });
  }
  const eligible = [];
  const contacts = new Map();
  for (const cc of chunk([...best.keys()].sort((a, b) => a - b), 60)) for (const x of (await bpLiveGet(`/contact-service/contact/${idSet(cc)}`)) || []) contacts.set(x.contactId, x);
  // Opt-outs and history are matched on BOTH the email and the mobile: "Stop promotions" on WhatsApp stores
  // "wa:447...", the email unsubscribe stores the address, and either one stops every channel.
  const unsub = new Set(), recent = new Set(), already = new Set();
  if (pool) {
    (await pool.query(`SELECT email FROM sale_alert_unsubs`)).rows.forEach((r) => unsub.add(r.email));
    for (const r of (await pool.query(`SELECT email, phone FROM sale_alert_sends WHERE status = 'sent' AND at > now() - ($1 || ' days')::interval`, [String(cutoffDays)])).rows) { if (r.email) recent.add(r.email); if (r.phone) recent.add("wa:" + r.phone); }
    for (const r of (await pool.query(`SELECT email, phone FROM sale_alert_sends WHERE status = 'sent' AND product_id = $1`, [pid])).rows) { if (r.email) already.add(r.email); if (r.phone) already.add("wa:" + r.phone); }
  }
  const seen = new Set(), useWa = cfg().whatsapp;
  for (const b of best.values()) {
    const x = contacts.get(b.contactId) || {}, email = normEmail(x.communication?.emails?.PRI?.email);
    const tel = x.communication?.telephones || {}, phone = ukMobile(tel.MOB) || ukMobile(tel.PRI) || ukMobile(tel.SEC);
    const keys = [email, phone && "wa:" + phone].filter(Boolean);
    const channel = useWa && phone ? "whatsapp" : email ? "email" : null;
    const why = !channel ? (useWa ? "no email or UK mobile" : "no email") : x.relationshipToAccount?.isStaff || x.relationshipToAccount?.isSupplier ? "staff/supplier"
      : keys.some((k) => unsub.has(k)) ? "unsubscribed" : keys.some((k) => already.has(k)) ? "already told about this item"
      : keys.some((k) => recent.has(k)) ? "had a sale message in the last 7 days" : keys.some((k) => seen.has(k)) ? "duplicate contact" : "";
    if (why) { skipped.push({ contactId: b.contactId, email, phone, orderId: b.orderId, reason: why }); continue; }
    keys.forEach((k) => seen.add(k));
    eligible.push({ ...b, email, phone, channel, key: channel === "whatsapp" ? "wa:" + phone : email, firstName: String(x.firstName || "").trim() });
  }
  return { eligible, skipped };
}

// ---- the sale section of the website --------------------------------------------------------
// Items come from tuffshop.co.uk/sale.html (Dec, 12 Oct): what is actually on sale, with its own link.
// A sale page is either a single-size "...-sale.html" product (SKU = original SKU + "-1") or a full
// product whose page lists the sizes still salable. Each available size maps back to the original
// BP product, whose past buyers are the audience - so "in your size" is exact.
const SITE = "https://tuffshop.co.uk";
const UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0 Safari/537.36 TuffshopSaleAlerts";
const pause = (ms) => new Promise((r) => setTimeout(r, ms));
// tuffshop.co.uk sits behind Cloudflare bot protection, which challenges server requests (403). A Cloudflare WAF
// "skip" rule matching SALE_ALERTS_SITE_HEADER ("Name: secret") lets this service read the sale pages.
function siteHeaders() {
  const h = { "User-Agent": UA, Accept: "text/html,application/xhtml+xml", "Accept-Language": "en-GB,en;q=0.9" };
  const raw = process.env.SALE_ALERTS_SITE_HEADER || "", i = raw.indexOf(":");
  if (i > 0) h[raw.slice(0, i).trim()] = raw.slice(i + 1).trim();
  return h;
}
async function getPage(url) {
  const r = await fetch(url, { headers: siteHeaders(), redirect: "follow" });
  if (r.status === 403 && r.headers.get("cf-mitigated")) throw new Error("tuffshop.co.uk blocked the request (Cloudflare challenge) - add the WAF skip rule for SALE_ALERTS_SITE_HEADER");
  if (!r.ok) { const e = new Error(`${url} -> ${r.status}`); e.status = r.status; throw e; }
  return r.text();
}

/** Every product page in the sale section, in the site's own order. */
export async function saleCatalogue({ maxPages = 80 } = {}) {
  const seen = new Set(), out = [];
  for (let p = 1; p <= maxPages; p++) {
    const h = await getPage(`${SITE}/sale.html?product_list_limit=36&p=${p}`);
    const urls = [...h.matchAll(/class="product-item-link"[^>]*href="([^"]+)"/g)].map((m) => m[1].split(/[?#]/)[0]);
    const fresh = urls.filter((u) => !seen.has(u));
    if (!fresh.length) break;                 // Magento repeats the last page past the end
    for (const u of fresh) { seen.add(u); out.push(u); }
    await pause(400);
  }
  return out;
}

const decode = (s) => String(s || "").replace(/&amp;/g, "&").replace(/&#0?39;/g, "'").replace(/&quot;/g, '"').replace(/\s+/g, " ").trim();
/** What is still buyable on one sale page: { url, name, variants: [{ sku, size }] } (variants empty = sold out). */
export async function salePage(url) {
  let h;
  try { h = await getPage(url); } catch (e) { if (e.status === 404) return { url, name: "", variants: [], gone: true }; throw e; }
  // "Carhartt K126 ... - Black - XL - SALE" reads oddly as "the ... you bought": drop the trailing "- SALE"
  const name = decode((h.match(/<h1[^>]*>\s*<span[^>]*>([^<]+)/) || h.match(/<title>([^<]+)/) || [])[1]).replace(/\s*\|.*$/, "").replace(/\s*-\s*sale$/i, "");
  const image = decode((h.match(/<meta property="og:image" content="([^"]+)"/) || [])[1]);   // product photo for the message header
  const cfgM = h.match(/"attributes":(\{.*?\}),"template"/), skuM = h.match(/"sku":(\{[^}]*\})/), salM = h.match(/"salable":(\{.*?\}\})/);
  if (cfgM && skuM) {                                          // full product: one variant per salable size
    const attrs = JSON.parse(cfgM[1]), skus = JSON.parse(skuM[1]), salable = salM ? JSON.parse(salM[1]) : null;
    const sizeAttr = Object.values(attrs).find((a) => /size/i.test(a.label)) || Object.values(attrs)[0];
    const ok = new Set(salable ? Object.values(salable).flatMap((o) => Object.values(o).flat()) : Object.keys(skus));
    const variants = [];
    for (const opt of sizeAttr?.options || []) for (const pid of opt.products) if (ok.has(String(pid)) && skus[pid]) variants.push({ sku: skus[pid], size: decode(opt.label) });
    return { url, name, image, variants };
  }
  const sku = decode((h.match(/itemprop="sku"[^>]*>([^<]*)/) || h.match(/"sku":"([^"]+)"/) || [])[1]);   // JSON-LD on single-size sale pages
  // JSON-LD writes the flag with escaped slashes: "availability":"https:\/\/schema.org\/InStock"
  const inStock = /"availability":"[^"]*InStock"|stock available|title="Availability"[\s\S]{0,200}In stock/i.test(h) && !/"availability":"[^"]*OutOfStock"|stock unavailable/i.test(h);
  return { url, name, image, variants: sku && inStock ? [{ sku, size: "" }] : [] };
}

/** The BP product a sale SKU stands for: the original. Sale copies are the original SKU + "-N" (337910429899XL-1)
 *  or + "#SALE" / "#Sale" (K126-BLKXLREG#SALE). */
export const originalSkuOf = (saleSku) => String(saleSku).replace(/#sale$/i, "").replace(/-\d+$/, "");
/** The size BP holds for a product (its "Size" variation), or "" - names the size on single-size sale pages. */
async function bpSize(pid) {
  try { const p = ((await bpLiveGet(`/product-service/product/${pid}`)) || [])[0] || {};
    return String(((p.variations || []).find((v) => /size/i.test(v.optionName || "")) || {}).optionValue || "").trim(); } catch { return ""; }
}
async function originalProductId(saleSku) {
  const base = originalSkuOf(saleSku);
  const tries = base !== saleSku ? [base, saleSku.replace(/#sale$/i, ""), saleSku] : [saleSku];
  for (const sku of tries) {
    const rows = rowsOf(await bpLiveGet(`/product-service/product-search?SKU=${encodeURIComponent(sku)}&pageSize=10`)).filter((r) => String(r.SKU).toUpperCase() === sku.toUpperCase());
    if (rows.length) return { pid: rows[0].productId, sku: rows[0].SKU, size: await bpSize(rows[0].productId) };
  }
  return null;
}

/** "Carhartt 106674 ... (Black)" + ["L"] -> "Carhartt 106674 ... (Black) in size L" (single-size pages carry no size). */
export const withSize = (name, sizes) => { const z = (sizes || []).filter(Boolean); return z.length ? `${name} in size ${z.join(" or ")}` : name; };

// ---- the message --------------------------------------------------------------------------------
// Short on purpose (Dec), and matching the WhatsApp template sale_item_alert (v2, 12 Oct): the item named in the
// subject (that is what gets an email opened), a photo, the hook first, urgency framed as "stock is limited".
export function renderEmail({ item, firstName, email }) {
  const subject = `${item.name} – now in our sale`;
  const html = `<div style="font-family:Arial,sans-serif;font-size:15px;line-height:1.5;color:#222;max-width:560px">
<p><b>Hi ${esc(firstName || "there")}, your size is in our sale 👀</b></p>
${item.image ? `<p><a href="${esc(item.url)}"><img src="${esc(item.image)}" alt="${esc(item.name)}" style="max-width:100%;width:320px;border-radius:6px"></a></p>` : ""}
<p>The ${esc(item.name)} you bought from us is now reduced. Stock is limited, so it's first come, first served.</p>
<p><a href="${esc(item.url)}" style="display:inline-block;background:#1f5fbf;color:#fff;padding:10px 18px;border-radius:6px;text-decoration:none;font-weight:bold">See it in the sale</a></p>
<p>The Tuffshop team</p>
<p style="font-size:12px;color:#888;margin-top:28px">You're getting this because you bought this item from Tuffshop. <a href="${esc(unsubUrl(email))}" style="color:#888">Unsubscribe from sale emails</a>.</p>
</div>`;
  return { subject, html };
}

// smtp2go - the relay the other system emails use. Lets us send FROM info@ (Graph cannot: info@ is not a mailbox).
async function sendSmtp2go({ to, from, subject, html, headers }) {
  const { default: nodemailer } = await import("nodemailer");
  const t = nodemailer.createTransport({ host: process.env.SMTP_SERVER || "mail-eu.smtp2go.com", port: parseInt(process.env.SMTP_PORT || "2525", 10), secure: false,
    auth: { user: process.env.SMTP_USERNAME || "tuffshop.co.uk", pass: process.env.SMTP_PASS } });
  const info = await t.sendMail({ from, to, subject, html, ...(headers ? { headers } : {}) });
  return { via: "smtp2go", messageId: info.messageId, response: info.response };
}

async function send(to, subject, html) {
  const c = cfg();
  return mailboxContext.run({ box: c.from }, () => sendNew({ to, subject, html }));
}

// ---- one day's run ------------------------------------------------------------------------------
/** dryRun: pick + check everything, send nothing, record nothing. */
export async function runSaleAlerts({ pool, dryRun = false, mode = cfg().mode, maxEvaluate = 40 } = {}) {
  const c = cfg();
  if (!dryRun && !c.secret) throw new Error("SALE_ALERTS_SECRET is not set (needed for unsubscribe links)");
  if (!dryRun && !graphConfigured()) throw new Error("Graph mail is not configured");
  const day = ukDay();
  const pages = await saleCatalogue();
  // Featured already? A page only REVIEWED is still owed its real send once live; in review mode it is not re-reviewed.
  const done = new Set((await pool.query(`SELECT url, status FROM sale_alert_pages`)).rows.filter((r) => r.status !== "review" || mode === "review").map((r) => r.url));
  const queue = pages.filter((u) => !done.has(u));
  const batch = [], evaluated = [];
  for (const url of queue) {
    if (batch.length >= c.perDay || evaluated.length >= maxEvaluate) break;
    const page = await salePage(url);
    const eligible = new Map(), skipped = [], sizes = [];
    for (const v of page.variants) {
      const orig = await originalProductId(v.sku);
      if (!orig) { skipped.push({ reason: `sale SKU ${v.sku} not matched to a product` }); continue; }
      if (!v.size && orig.size) v.size = orig.size;   // single-size "-sale" page: the size lives on the BP product
      sizes.push({ ...v, pid: orig.pid, origSku: orig.sku });
      const r = await recipientsFor(orig.pid, { pool });
      // keyed per person: someone who bought two of the sale sizes is told once, naming both
      for (const e of r.eligible) { const had = eligible.get(e.key); if (had) { if (v.size && !had.sizes.includes(v.size)) had.sizes.push(v.size); } else eligible.set(e.key, { ...e, size: v.size, sizes: v.size ? [v.size] : [], pid: orig.pid }); }
      skipped.push(...r.skipped);
    }
    evaluated.push({ url, name: page.name, sizes: sizes.length, eligible: eligible.size });
    if (!eligible.size) {
      if (!dryRun) await pool.query(`INSERT INTO sale_alert_pages (url, name, featured_on, status, recipients) VALUES ($1,$2,$3,$4,0) ON CONFLICT (url) DO NOTHING`, [url, page.name, day, page.variants.length ? "no buyers" : "sold out"]);
      continue;
    }
    batch.push({ page, sizes, eligible: [...eligible.values()], skipped });
    await pause(300);
  }
  const result = { day, mode: dryRun ? "preview" : mode, salePages: pages.length, notYetFeatured: queue.length, evaluated, products: [] };
  for (const b of batch) {
    const pid0 = b.sizes[0]?.pid || null;
    const p = { url: b.page.url, name: b.page.name, image: b.page.image || "", sizes: b.sizes.map((s) => s.size || s.origSku), sku: b.sizes.map((s) => s.origSku).join(", "),
      recipients: b.eligible.map((e) => ({ channel: e.channel, email: e.email, phone: e.phone, firstName: e.firstName, size: e.sizes.join(" / "), orderId: e.orderId, date: e.date })),
      skipped: b.skipped.map((s) => ({ email: s.email || null, orderId: s.orderId, reason: s.reason })), sent: 0, failed: 0 };
    result.products.push(p);
    if (dryRun) continue;
    await pool.query(`INSERT INTO sale_alert_pages (url, name, featured_on, status, recipients) VALUES ($1,$2,$3,$4,$5) ON CONFLICT (url) DO UPDATE SET status = EXCLUDED.status, featured_on = EXCLUDED.featured_on, recipients = EXCLUDED.recipients`,
      [b.page.url, b.page.name, day, mode, b.eligible.length]);
    for (const s of b.skipped) await logSend(pool, { day, pid: pid0, contactId: s.contactId, email: s.email, phone: s.phone, status: "skipped", reason: s.reason });
    if (mode !== "live") { for (const e of b.eligible) await logSend(pool, { day, pid: e.pid, contactId: e.contactId, email: e.email, phone: e.phone, channel: e.channel, status: "review", reason: "review mode - not sent" }); continue; }
    let live = b.page, checkedAt = Date.now();
    for (const e of b.eligible) {
      // first come, first served: re-read the page every 2 minutes and stop for a size the moment it has gone
      if (Date.now() - checkedAt > 120000) { live = await salePage(b.page.url); checkedAt = Date.now(); }
      // a single-size page has no size label on the site (it came from BP), so "still there" = still in stock
      const unlabelled = live.variants.length && live.variants.every((v) => !v.size);
      const still = unlabelled ? (e.sizes.length ? e.sizes : [""]) : e.sizes.length ? e.sizes.filter((z) => live.variants.some((v) => v.size === z)) : live.variants.length ? [""] : [];
      if (!still.length) { await logSend(pool, { day, pid: e.pid, contactId: e.contactId, email: e.email, phone: e.phone, channel: e.channel, status: "skipped", reason: "sold out before sending" }); continue; }
      // name THEIR size: the page link shows every sale size, the message must say which one is theirs
      const item = { name: withSize(b.page.name, still), url: b.page.url, image: b.page.image };
      const who = { day, pid: e.pid, contactId: e.contactId, email: e.email, phone: e.phone, channel: e.channel };
      try {
        let r;
        if (e.channel === "whatsapp") r = await sendSaleWhatsApp({ to: e.phone, firstName: e.firstName, item });
        else { const { subject, html } = renderEmail({ item, firstName: e.firstName, email: e.email }); // Customer emails go through smtp2go FROM info@: authorised for tuffshop.co.uk (DKIM s685857 + SPF em685857 -> DMARC pass,
          // verified 14 Oct). One-click unsubscribe headers help inbox placement.
          r = await sendSmtp2go({ to: e.email, from: "Tuffshop <info@tuffshop.co.uk>", subject, html,
            headers: { "List-Unsubscribe": `<${unsubUrl(e.email)}>`, "List-Unsubscribe-Post": "List-Unsubscribe=One-Click" } }); }
        p.sent++; await logSend(pool, { ...who, status: "sent", reason: r?.messageId || r?.via || e.channel });
      } catch (err) { p.failed++; await logSend(pool, { ...who, status: "failed", reason: err.message.slice(0, 300) }); }
    }
  }
  if (!dryRun && mode === "review") await sendReview(result).catch((e) => { result.reviewError = e.message; });
  return result;
}

async function logSend(pool, { day, pid, contactId, email, phone, channel, status, reason }) {
  await pool.query(`INSERT INTO sale_alert_sends (day, product_id, contact_id, email, phone, channel, status, reason) VALUES ($1,$2,$3,$4,$5,$6,$7,$8)`,
    [day, pid, contactId || null, email || null, phone || null, channel || null, status, reason || null]);
}

async function sendReview(result) {
  const c = cfg();
  const rows = result.products.map((p) => `<h3 style="margin:18px 0 4px"><a href="${esc(p.url)}">${esc(p.name)}</a> <span style="color:#888">(${esc(p.sku)}${p.sizes.some(Boolean) ? `; in the sale: ${esc(p.sizes.join(", "))}` : ""})</span></h3>
<p style="margin:0">Would message <b>${p.recipients.length}</b> (${p.recipients.filter((r) => r.channel === "whatsapp").length} WhatsApp, ${p.recipients.filter((r) => r.channel === "email").length} email):</p>${bySize(p.recipients)}
<p style="margin:0;color:#888;font-size:13px">Skipped ${p.skipped.length}: ${Object.entries(p.skipped.reduce((a, s) => ((a[s.reason] = (a[s.reason] || 0) + 1), a), {})).map(([k, v]) => `${esc(k)} ${v}`).join(", ") || "none"}</p>`).join("");
  const sample = result.products[0] ? renderEmail({ item: { name: withSize(result.products[0].name, [result.products[0].recipients[0]?.size].filter(Boolean)), url: result.products[0].url, image: result.products[0].image }, firstName: "Sam", email: c.reviewTo }) : null;
  // The review goes from the sales mailbox (known good): info@ is not a mailbox the Graph app can send as (14 Oct: "user invalid").
  await mailboxContext.run({ box: salesMailbox() }, () => sendNew({ to: c.reviewTo, subject: `Sale alerts ${result.day}: ${result.products.length} items, ${result.products.reduce((a, p) => a + p.recipients.length, 0)} customers (review - nothing sent)`,
    html: `<div style="font-family:Arial,sans-serif;font-size:14px">${rows || "<p>Nothing to send today.</p>"}${sample ? `<hr><p><b>What customers would receive:</b></p>${sample.html}` : ""}</div>` }));
}

// Review list grouped by the size each person bought - one line per size, so "only buyers of that size" is visible.
function bySize(recipients) {
  const groups = new Map();
  for (const r of recipients) { const k = r.size || "one size"; if (!groups.has(k)) groups.set(k, []); groups.get(k).push(r.channel === "whatsapp" ? "📱 0" + esc(r.phone.slice(2)) : "✉ " + esc(r.email)); }
  return [...groups].map(([k, v]) => `<p style="margin:0 0 0 12px"><b>${esc(k)}</b> (${v.length}): ${v.join(", ")}</p>`).join("");
}
// ---- WhatsApp ---------------------------------------------------------------------------------------
// Approved Marketing template sale_item_alert (id 1081768568097569): IMAGE header (the item photo),
// body "Hi {{1}}, your size is in our sale 👀 / The {{2}} you bought from us is now reduced. Stock is limited,
// so it's first come, first served.", URL button https://tuffshop.co.uk/{{1}}, quick reply "Stop promotions".
const WA_TEMPLATE = process.env.SALE_ALERTS_WA_TEMPLATE || "sale_item_alert";
const waNumberId = () => (String(process.env.SALE_ALERTS_WA_CHANNEL || "sales").toLowerCase() === "sales"
  ? process.env.WHATSAPP_SALES_PHONE_NUMBER_ID : process.env.WHATSAPP_PHONE_NUMBER_ID) || null;

/** UK mobile in WhatsApp form (447...), or null. Landlines and junk return null. */
export function ukMobile(raw) {
  let d = String(raw || "").replace(/[^\d+]/g, "");
  if (d.startsWith("+")) d = d.slice(1);
  if (d.startsWith("00")) d = d.slice(2);
  if (d.startsWith("0")) d = "44" + d.slice(1);
  return /^447\d{9}$/.test(d) ? d : null;
}

export async function sendSaleWhatsApp({ to, firstName, item }) {
  const id = waNumberId();
  if (!process.env.WHATSAPP_TOKEN || !id) throw new Error("WhatsApp is not configured (WHATSAPP_TOKEN / phone number id)");
  const path = String(item.url).replace(/^https?:\/\/(www\.)?tuffshop\.co\.uk\//i, "");
  const components = [
    ...(item.image ? [{ type: "header", parameters: [{ type: "image", image: { link: item.image } }] }] : []),
    { type: "body", parameters: [{ type: "text", text: String(firstName || "there").slice(0, 60) }, { type: "text", text: String(item.name).slice(0, 200) }] },
    { type: "button", sub_type: "url", index: "0", parameters: [{ type: "text", text: path }] },
  ];
  const v = process.env.WHATSAPP_GRAPH_VERSION || "v21.0";
  const r = await fetch(`https://graph.facebook.com/${v}/${id}/messages`, {
    method: "POST", headers: { Authorization: `Bearer ${process.env.WHATSAPP_TOKEN}`, "Content-Type": "application/json" },
    body: JSON.stringify({ messaging_product: "whatsapp", to, type: "template", template: { name: WA_TEMPLATE, language: { code: "en" }, components } }),
  });
  const j = await r.json().catch(() => ({}));
  if (!r.ok) throw new Error(`WhatsApp send failed: ${(j.error && (j.error.error_user_msg || j.error.message)) || r.status}`);
  return { via: "whatsapp", messageId: (j.messages && j.messages[0] && j.messages[0].id) || null };
}

/**
 * A customer tapped "Stop promotions" on a sale_item_alert WhatsApp. Called from the WhatsApp webhook in server.js.
 * Records the opt-out (stops email too if the same contact has an address on file) and confirms in the chat -
 * free text is allowed because they have just messaged us.
 */
export async function saleAlertsOptOut({ pool, phone, channel = "sales" }) {
  const p = ukMobile(phone) || String(phone || "").replace(/\D/g, "");
  if (!pool || !p) return false;
  await pool.query(`CREATE TABLE IF NOT EXISTS sale_alert_unsubs (email text PRIMARY KEY, at timestamptz NOT NULL DEFAULT now())`);
  await pool.query(`INSERT INTO sale_alert_unsubs (email) VALUES ($1) ON CONFLICT DO NOTHING`, ["wa:" + p]);
  console.log(`[sale-alerts] opt-out recorded for ${p}`);
  const id = channel === "sales" ? process.env.WHATSAPP_SALES_PHONE_NUMBER_ID : process.env.WHATSAPP_PHONE_NUMBER_ID;
  if (process.env.WHATSAPP_TOKEN && id) {
    const v = process.env.WHATSAPP_GRAPH_VERSION || "v21.0";
    await fetch(`https://graph.facebook.com/${v}/${id}/messages`, { method: "POST", headers: { Authorization: `Bearer ${process.env.WHATSAPP_TOKEN}`, "Content-Type": "application/json" },
      body: JSON.stringify({ messaging_product: "whatsapp", to: p, type: "text", text: { body: "Done - you won't get any more sale messages from Tuffshop. Thanks!" } }) }).catch(() => {});
  }
  return true;
}

// ---- routes + schedule ---------------------------------------------------------------------------
export function registerSaleAlerts(app, { getPool }) {
  const requireUser = (req, res, next) => app.locals.requireHubUser(req, res, next);
  const ensure = async () => getPool().query(`
    CREATE TABLE IF NOT EXISTS sale_alert_items (product_id bigint PRIMARY KEY, sku text, featured_on date NOT NULL, status text, recipients int);
    CREATE TABLE IF NOT EXISTS sale_alert_pages (url text PRIMARY KEY, name text, featured_on date NOT NULL, status text, recipients int);
    CREATE TABLE IF NOT EXISTS sale_alert_sends (id bigserial PRIMARY KEY, at timestamptz NOT NULL DEFAULT now(), day date, product_id bigint, contact_id bigint, email text, status text NOT NULL, reason text);
    ALTER TABLE sale_alert_sends ADD COLUMN IF NOT EXISTS phone text;
    ALTER TABLE sale_alert_sends ADD COLUMN IF NOT EXISTS channel text;
    CREATE INDEX IF NOT EXISTS idx_sale_alert_sends_email ON sale_alert_sends(email, at);
    CREATE TABLE IF NOT EXISTS sale_alert_unsubs (email text PRIMARY KEY, at timestamptz NOT NULL DEFAULT now());
    CREATE TABLE IF NOT EXISTS sale_alert_runs (day date PRIMARY KEY, ran_at timestamptz NOT NULL DEFAULT now(), result jsonb);`);

  // What today's run would do. Nothing is sent or recorded.
  app.get("/api/sale-alerts/preview", requireUser, async (req, res) => {
    try { await ensure(); res.json(await runSaleAlerts({ pool: getPool(), dryRun: true })); }
    catch (e) { res.status(500).json({ error: e.message }); }
  });
  // One sample email to an @tuffshop.co.uk address, to see exactly what customers get.
  app.post("/api/sale-alerts/send-sample", requireUser, async (req, res) => {
    try {
      const to = normEmail(req.query.to || req.body?.to);
      if (!/@tuffshop\.co\.uk$/.test(to)) return res.status(400).json({ error: "Samples only go to @tuffshop.co.uk addresses" });
      const { subject, html } = renderEmail({ item: { name: "Carhartt 106674 Firm Duck Traditional Coat", url: "https://tuffshop.co.uk/sale.html" }, firstName: "Sam", email: to });
      res.json(await send(to, "[SAMPLE] " + subject, html));
    } catch (e) { res.status(500).json({ error: e.message }); }
  });
  // Test the WhatsApp template with a real sale item. Locked twice: needs SALE_ALERTS_SECRET as ?key=, and only
  // ever sends to a number on SALE_ALERTS_TEST_TO (default: Dec's test numbers) - it cannot message a customer.
  app.post("/api/sale-alerts/whatsapp-test", async (req, res) => {
    try {
      const secret = cfg().secret, key = String(req.query.key || "");
      if (!secret || key.length !== secret.length || !crypto.timingSafeEqual(Buffer.from(key), Buffer.from(secret))) return res.status(403).json({ error: "forbidden" });
      // Only numbers on the test list (SALE_ALERTS_TEST_TO, comma separated) can ever receive a test.
      const allowed = String(process.env.SALE_ALERTS_TEST_TO || "447892872364,447720944723,447890562384,447854464066").split(",").map(ukMobile).filter(Boolean);
      const to = req.query.to ? ukMobile(req.query.to) : allowed[0];
      if (!to || !allowed.includes(to)) return res.status(400).json({ error: "that number is not on the test list (SALE_ALERTS_TEST_TO)" });
      const url = String(req.query.url || "https://tuffshop.co.uk/carhartt-106674-firm-duck-traditional-coat-black.html");
      if (!/^https:\/\/tuffshop\.co\.uk\//.test(url)) return res.status(400).json({ error: "url must be a tuffshop.co.uk page" });
      // ?item=&image= lets a test run before the site is readable (SALE_ALERTS_SITE_HEADER not set yet)
      const page = req.query.item ? { url, name: String(req.query.item), image: String(req.query.image || ""), variants: [] } : await salePage(url);
      if (!page.name) return res.status(400).json({ error: "could not read that page", page });
      const r = await sendSaleWhatsApp({ to, firstName: String(req.query.name || "Dec"), item: { ...page, name: withSize(page.name, req.query.size ? [String(req.query.size)] : []) } });
      res.json({ ok: true, to, item: { name: page.name, url: page.url, image: page.image, sizes: page.variants.map((v) => v.size || v.sku) }, ...r });
    } catch (e) { res.status(500).json({ error: e.message }); }
  });
  // Read-only run summary for checking from outside the Sales Hub. Same lock as the WhatsApp test (?key=secret);
  // returns counts only - no customer contact details.
  app.get("/api/sale-alerts/status", async (req, res) => {
    try {
      const secret = cfg().secret, key = String(req.query.key || "");
      if (!secret || key.length !== secret.length || !crypto.timingSafeEqual(Buffer.from(key), Buffer.from(secret))) return res.status(403).json({ error: "forbidden" });
      await ensure();
      const runs = await getPool().query(`SELECT day, ran_at, result FROM sale_alert_runs ORDER BY day DESC LIMIT 14`);
      const sends = await getPool().query(`SELECT day, channel, status, count(*)::int n FROM sale_alert_sends GROUP BY 1,2,3 ORDER BY 1 DESC`);
      const unsubs = await getPool().query(`SELECT count(*)::int n FROM sale_alert_unsubs`);
      res.json({ mode: cfg().mode, whatsapp: cfg().whatsapp, siteHeader: !!process.env.SALE_ALERTS_SITE_HEADER,
        runs: runs.rows.map((r) => ({ day: r.day, ranAt: r.ran_at, error: r.result?.error || null, mode: r.result?.mode, salePages: r.result?.salePages,
          items: (r.result?.products || []).map((p) => ({ name: p.name, recipients: p.recipients?.length || 0, whatsapp: (p.recipients || []).filter((x) => x.channel === "whatsapp").length, skipped: p.skipped?.length || 0, sent: p.sent, failed: p.failed })),
          reviewError: r.result?.reviewError || null })),
        sends: sends.rows, unsubscribed: unsubs.rows[0].n });
    } catch (e) { res.status(500).json({ error: e.message }); }
  });
  // Deliverability check: one email through smtp2go from info@ to the proofing Gmail only (fixed address), so its
  // SPF/DKIM/DMARC verdict can be read. Secret-locked; cannot email anyone else.
  app.post("/api/sale-alerts/smtp-test", async (req, res) => {
    try {
      const secret = cfg().secret, key = String(req.query.key || "");
      if (!secret || key.length !== secret.length || !crypto.timingSafeEqual(Buffer.from(key), Buffer.from(secret))) return res.status(403).json({ error: "forbidden" });
      const r = await sendSmtp2go({ to: "tuffshopproofing@gmail.com", from: "Tuffshop <info@tuffshop.co.uk>", subject: "smtp2go deliverability test " + new Date().toISOString(),
        html: "<p>Deliverability test for the sale alerts (smtp2go, from info@). No action needed.</p>" });
      res.json({ ok: true, ...r });
    } catch (e) { res.status(500).json({ error: e.message }); }
  });
  // Re-send a day's review email from its stored result (nothing is re-picked or sent to customers).
  app.post("/api/sale-alerts/resend-review", async (req, res) => {
    try {
      const secret = cfg().secret, key = String(req.query.key || "");
      if (!secret || key.length !== secret.length || !crypto.timingSafeEqual(Buffer.from(key), Buffer.from(secret))) return res.status(403).json({ error: "forbidden" });
      await ensure();
      const r = await getPool().query(`SELECT result FROM sale_alert_runs WHERE day = $1`, [String(req.query.day || ukDay())]);
      if (!r.rows[0]?.result?.products) return res.status(404).json({ error: "no stored run for that day" });
      await sendReview(r.rows[0].result);
      res.json({ ok: true, day: r.rows[0].result.day, items: r.rows[0].result.products.length, to: cfg().reviewTo });
    } catch (e) { res.status(500).json({ error: e.message }); }
  });
  app.get("/api/sale-alerts/log", requireUser, async (req, res) => {
    try {
      await ensure();
      const sends = await getPool().query(`SELECT * FROM sale_alert_sends ORDER BY id DESC LIMIT $1`, [Math.min(2000, Number(req.query.limit) || 300)]);
      const items = await getPool().query(`SELECT * FROM sale_alert_items ORDER BY featured_on DESC LIMIT 200`);
      const runs = await getPool().query(`SELECT day, ran_at, result FROM sale_alert_runs ORDER BY day DESC LIMIT 30`);
      res.json({ sends: sends.rows, items: items.rows, runs: runs.rows });
    } catch (e) { res.status(500).json({ error: e.message }); }
  });

  // Unsubscribe (public, from the email footer).
  // GET = the link in the email footer; POST = mail apps' one-click unsubscribe (List-Unsubscribe-Post, RFC 8058).
  app.all("/sale-alerts/unsubscribe", async (req, res) => {
    if (req.method !== "GET" && req.method !== "POST") return res.sendStatus(405);
    const email = normEmail(req.query.e), t = String(req.query.t || "");
    const page = (msg) => res.type("html").send(`<!doctype html><meta name="viewport" content="width=device-width,initial-scale=1"><title>Tuffshop</title><div style="font-family:Arial,sans-serif;max-width:480px;margin:60px auto;padding:0 16px;font-size:16px">${msg}</div>`);
    const secret = cfg().secret;
    if (!secret || !email || t.length !== 32 || !crypto.timingSafeEqual(Buffer.from(t), Buffer.from(unsubToken(email, secret)))) return page("<p>That unsubscribe link isn't valid. Reply to any of our emails and we'll take you off the list.</p>");
    try { await ensure(); await getPool().query(`INSERT INTO sale_alert_unsubs (email) VALUES ($1) ON CONFLICT DO NOTHING`, [email]); page(`<p>Done. <b>${esc(email)}</b> won't get any more sale emails from Tuffshop.</p>`); }
    catch (e) { console.error("[sale-alerts] unsubscribe:", e.message); page("<p>Something went wrong - please reply to the email and we'll remove you.</p>"); }
  });

  // Weekdays at SALE_ALERTS_AT (UK), once a day even across restarts.
  setInterval(async () => {
    const c = cfg();
    if (!["review", "live"].includes(c.mode) || !getPool()) return;
    const now = new Date();
    const hm = now.toLocaleTimeString("en-GB", { hour: "2-digit", minute: "2-digit", hour12: false, timeZone: "Europe/London" });
    const dow = now.toLocaleDateString("en-GB", { weekday: "short", timeZone: "Europe/London" });
    const [h, m] = c.at.split(":").map(Number), end = `${String(h + 1).padStart(2, "0")}:${String(m).padStart(2, "0")}`;
    if (/Sat|Sun/.test(dow) || hm < c.at || hm > end) return;
    const day = ukDay(now);
    try {
      await ensure();
      const claim = await getPool().query(`INSERT INTO sale_alert_runs (day) VALUES ($1) ON CONFLICT (day) DO NOTHING RETURNING day`, [day]);
      if (!claim.rowCount) return;
      let result;
      try { result = await runSaleAlerts({ pool: getPool() }); }
      catch (e) {
        console.error("[sale-alerts] run failed:", e.message);
        await getPool().query(`UPDATE sale_alert_runs SET result = $2 WHERE day = $1`, [day, JSON.stringify({ error: e.message })]);
        return;   // not retried: a half-finished send must not repeat; the log shows what went out
      }
      await getPool().query(`UPDATE sale_alert_runs SET result = $2 WHERE day = $1`, [day, JSON.stringify(result)]);
      console.log("[sale-alerts]", day, c.mode, JSON.stringify(result.products.map((p) => ({ sku: p.sku, recipients: p.recipients.length, sent: p.sent, failed: p.failed }))));
    } catch (e) { console.error("[sale-alerts] scheduler:", e.message); }
  }, 60 * 1000);
}
