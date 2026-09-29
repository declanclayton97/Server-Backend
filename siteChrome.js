// siteChrome.js — the tuffshop.co.uk header, menu and footer, borrowed from the live
// website so a page served from here looks like part of the shop.
//
// The site is Magento (Olegnax Athlete2 theme). Its header and footer are plain HTML
// styled by the theme's stylesheets, so this takes a real page from the site and keeps:
//   - the stylesheet links and the inline theme <style> from <head>;
//   - the <body> classes (the theme keys a lot of layout off them);
//   - everything from <body> to <main id="maincontent"> (top bar, logo, search, menu);
//   - the <footer>.
// Scripts are dropped: Magento's RequireJS stack expects to run on its own domain, and
// every script-driven part (mini-cart, sticky header) degrades to plain links. The
// one thing that needs a script, the mobile menu button, is handled in the page.
//
// Refreshed every 6 hours; the last good copy is kept if a refresh fails, so a website
// outage or a redesign mid-fetch never blanks the returns page.

const SITE = "https://tuffshop.co.uk";
const SOURCE = process.env.SITE_CHROME_SOURCE || `${SITE}/returns`;   // a light CMS page
const TTL = 6 * 60 * 60 * 1000;

let cache = { at: 0, chrome: null, error: null };

// Cloudflare challenges Node's own fetch (its TLS fingerprint, whatever the headers say)
// but lets curl through, so curl is tried when fetch is refused. If both fail, the copy
// saved in returns-page/site-chrome.json is used: the site's stylesheet URLs carry a
// cache-busting version that Magento ignores, so an old snapshot stays fully styled.
// Refresh the snapshot with:  node scripts/refresh-site-chrome.mjs
import { execFile } from "child_process";
import { readFileSync } from "fs";
import { fileURLToPath } from "url";
const UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0 Safari/537.36";
export async function fetchSitePage(url = SOURCE) {
  try {
    const r = await fetch(url, { headers: { "User-Agent": UA, Accept: "text/html" } });
    if (r.ok) return await r.text();
  } catch { /* try curl */ }
  return new Promise((resolve, reject) => execFile("curl", ["-sSL", "--max-time", "20", "-A", UA, "-w", "\n%{http_code}", url],
    { maxBuffer: 20 * 1024 * 1024 }, (err, out) => {
      if (err) return reject(err);
      const i = out.lastIndexOf("\n"), code = out.slice(i + 1).trim();
      if (code !== "200") return reject(new Error("HTTP " + code + " (curl)"));
      resolve(out.slice(0, i));
    }));
}
function loadSnapshot() {
  try { return JSON.parse(readFileSync(fileURLToPath(new URL("./returns-page/site-chrome.json", import.meta.url)), "utf8")); }
  catch (e) { console.error("[site-chrome] no snapshot:", e.message); return null; }
}

// Cloudflare hides emails behind "[email protected]" plus a script we are not running.
// The address is XOR-encoded in the page itself: first byte is the key.
function cfDecode(hex) {
  const key = parseInt(hex.slice(0, 2), 16);
  let out = "";
  for (let i = 2; i < hex.length; i += 2) out += String.fromCharCode(parseInt(hex.slice(i, i + 2), 16) ^ key);
  return out;
}

export function extractChrome(html) {
  const h = String(html || "");
  const bodyAt = h.search(/<body\b/i);
  const mainAt = h.search(/<main\b[^>]*id="maincontent"/i);
  const footAt = h.search(/<footer\b/i);
  const footEnd = h.search(/<\/footer>/i);
  if (bodyAt < 0 || mainAt < bodyAt || footAt < mainAt || footEnd < footAt) throw new Error("page structure not recognised");

  const head = h.slice(0, bodyAt);
  const links = [...head.matchAll(/<link\b[^>]*rel="stylesheet"[^>]*>/gi)].map((m) => m[0])
    // Deferred stylesheets load as media="print" and flip themselves on with a script.
    .map((l) => l.replace(/\smedia="print"/i, "").replace(/\sonload="[^"]*"/i, ""))
    .filter((l) => !/\/print\.css"/.test(l));
  // Stylesheets the page adds after the footer (same deferred trick).
  const tail = h.slice(footEnd);
  for (const m of tail.matchAll(/<link\b[^>]*rel="stylesheet"[^>]*>/gi)) {
    const l = m[0].replace(/\smedia="print"/i, "").replace(/\sonload="[^"]*"/i, "");
    if (!links.includes(l)) links.push(l);
  }
  const styles = [...head.matchAll(/<style\b[^>]*>[\s\S]*?<\/style>/gi)].map((m) => m[0]);
  const bodyTag = h.slice(bodyAt, h.indexOf(">", bodyAt) + 1);
  const bodyClass = ((bodyTag.match(/\bclass="([^"]*)"/i) || [])[1] || "")
    .split(/\s+/).filter((c) => c && !/^(customer-|cms-|catalog-|page-layout-|checkout-)/.test(c)).join(" ");

  const clean = (s) => s
    .replace(/<script\b[\s\S]*?<\/script>/gi, "")
    .replace(/<noscript\b[\s\S]*?<\/noscript>/gi, "")
    .replace(/<!--[\s\S]*?-->/g, "")
    .replace(/<div class="cookie-status-message"[\s\S]*?<\/div>/i, "")
    .replace(/<div data-bind="scope: 'cookie-notice-wrapper'">[\s\S]*?<\/div>/i, "")
    // Site-relative links would otherwise point at this server.
    .replace(/(\s(?:href|src|action)=")\/(?!\/)/gi, `$1${SITE}/`)
    // Cloudflare-protected emails, decoded here since its script is not.
    .replace(/<(a|span)\b[^>]*data-cfemail="([0-9a-f]+)"[^>]*>[\s\S]*?<\/\1>/gi, (_, _t, hex) => cfDecode(hex))
    .replace(/href="[^"]*\/cdn-cgi\/l\/email-protection#([0-9a-f]+)"/gi, (_, hex) => `href="mailto:${cfDecode(hex)}"`);

  const header = clean(h.slice(h.indexOf(">", bodyAt) + 1, mainAt));
  const footer = clean(h.slice(footAt, footEnd + "</footer>".length));
  return { links, styles, bodyClass, header, footer, fetchedAt: new Date().toISOString() };
}

export async function getSiteChrome({ force = false } = {}) {
  if (!force && cache.chrome && Date.now() - cache.at < TTL) return cache.chrome;
  try {
    cache = { at: Date.now(), chrome: extractChrome(await fetchSitePage()), error: null };
  } catch (e) {
    console.error("[site-chrome] refresh failed:", e.message);
    cache.error = e.message;
    cache.at = Date.now() - TTL + 10 * 60 * 1000;   // retry in 10 minutes, keep the last good copy
  }
  if (!cache.chrome) cache.chrome = loadSnapshot();
  return cache.chrome;
}

export const siteChromeStatus = () => ({ fetchedAt: cache.chrome && cache.chrome.fetchedAt, error: cache.error });

// Wrap page content in the shop's header and footer. Without a copy of the site (first
// fetch failed) the page still works, with a plain black bar and the logo.
export function wrapInChrome(chrome, { title, headExtra = "", content, scripts = "" }) {
  if (!chrome) {
    return `<!doctype html><html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1">
<title>${title}</title>${headExtra}</head><body class="no-chrome">
<div style="background:#000;padding:12px 16px;"><a href="${SITE}/"><img src="${SITE}/media/athlete2/default/tuffshop_logo.png" alt="Tuffshop" style="height:60px"></a></div>
<main id="maincontent" class="page-main">${content}</main>${scripts}</body></html>`;
  }
  return `<!doctype html><html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1">
<title>${title}</title><meta name="robots" content="noindex">
${chrome.links.join("\n")}
${chrome.styles.join("\n")}
${headExtra}
</head><body class="${chrome.bodyClass}">
${chrome.header}
<main id="maincontent" class="page-main">${content}</main>
${chrome.footer}
</div>
${scripts}
</body></html>`;
}

// The same content for embedding in a page ON tuffshop.co.uk (/returns-form): the site
// draws the header and footer there, so this carries only the theme's stylesheets.
export function wrapForEmbed(chrome, { title, headExtra = "", content, scripts = "" }) {
  const links = chrome ? chrome.links.join("\n") + "\n" + chrome.styles.join("\n") : "";
  return `<!doctype html><html lang="en" class="rt-embed"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1">
<title>${title}</title><meta name="robots" content="noindex">
${links}
${headExtra}
<style>html.rt-embed, html.rt-embed body { background: transparent; } html.rt-embed .rt-hero .page-title { display: none; } html.rt-embed .rt { padding-left: 0; padding-right: 0; max-width: none; }</style>
</head><body class="${chrome ? chrome.bodyClass : ""}">
<main id="maincontent" class="page-main">${content}</main>
${scripts}
</body></html>`;
}
