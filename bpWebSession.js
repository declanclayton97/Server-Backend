// Brightpearl legacy web-app session manager.
//
// Why this exists: BP's public REST API has no documented file-upload
// endpoint. Files can only be attached to orders via the authenticated
// web UI at /iframe_attach_file.php. This module logs in to that web UI
// using stored credentials, caches the session cookies, fetches a CSRF
// token from the form page, and POSTs files to that endpoint.
//
// Why tough-cookie: BP returns multiple cookies comma-joined inside ONE
// Set-Cookie header, AND the Expires value contains its own comma
// ("Expires=Tue, 26 May 2026..."). Naive split-on-comma corrupts both.
// tough-cookie is the canonical parser that handles this correctly.
//
// Critical detail discovered via PowerShell trace: the login flow MUST
// begin with a GET to /admin_login.php?clients_id=X first. That GET sets
// __cf_bm + _cfuvid (Cloudflare bot-management cookies) plus an initial
// pearlAdmin session-init token. Without those Cloudflare cookies, BP's
// new edge routing serves the Sage SPA login shell instead of the
// legacy iframe form.

import { CookieJar } from 'tough-cookie';

const BP_HOST = process.env.BP_WEB_HOST || 'https://euw1.brightpearlapp.com';
const BP_CLIENT = process.env.BP_WEB_CLIENT_ID || 'tuffworkwear';
const SESSION_TTL_MS = 25 * 60 * 1000; // BP sessions live ~30 min; refresh at 25

const BROWSER_HEADERS = {
  'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
  'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
  'Accept-Language': 'en-GB,en;q=0.9',
};

const sessions = new Map(); // client (clients_id) -> { jar: CookieJar, loggedInAt: number }

function extractSetCookies(res) {
  if (typeof res.headers.getSetCookie === 'function') {
    return res.headers.getSetCookie();
  }
  const arr = [];
  for (const [key, value] of res.headers.entries()) {
    if (key.toLowerCase() === 'set-cookie') arr.push(value);
  }
  return arr;
}

// Push every Set-Cookie value from a fetch response into the jar.
// tough-cookie does the heavy lifting on parsing.
async function ingestCookies(jar, res, url) {
  const setCookies = extractSetCookies(res);
  // Some servers (BP included) comma-join multiple cookies into ONE
  // header value. getSetCookie() splits multi-header but not comma-
  // joined-single-header. Detect and re-split if needed.
  const allHeaderValues = [];
  for (const raw of setCookies) {
    // Heuristic: if raw contains ", <name>=" (a real cookie boundary,
    // not a comma inside an Expires date), split. The Expires date
    // pattern is "Expires=<day>, DD-<Mon>-YYYY" — the comma is always
    // followed by " DD" (digit). A cookie boundary comma is followed
    // by " <name>=" (alphanumeric + equals before semicolon).
    const parts = raw.split(/,(?=\s*[a-zA-Z_][a-zA-Z0-9_\-]*=)/);
    for (const p of parts) allHeaderValues.push(p.trim());
  }
  for (const hv of allHeaderValues) {
    try {
      await jar.setCookie(hv, url, { ignoreError: true });
    } catch {
      // ignore unparseable cookies
    }
  }
}

async function getCookieHeader(jar, url) {
  return await jar.getCookieString(url);
}

function looksLikeLoginPage(html) {
  return (
    /name=["']email_address["']/i.test(html) ||
    /<title>Brightpearl - Login<\/title>/i.test(html) ||
    /data-theme=["']sage["']/i.test(html)
  );
}

// Walk a redirect chain manually, ingesting cookies from each hop.
// Stops on first non-3xx, hits MAX_HOPS, or missing Location.
async function followRedirects(jar, res, originUrl) {
  let current = res;
  let hops = 0;
  const MAX_HOPS = 8;
  let lastUrl = originUrl;
  while (hops < MAX_HOPS && [301, 302, 303, 307, 308].includes(current.status)) {
    let location = current.headers.get('location');
    if (!location) break;
    if (location.startsWith('//')) location = `https:${location}`;
    else if (location.startsWith('/')) location = `${BP_HOST}${location}`;
    else if (!/^https?:\/\//i.test(location)) location = `${BP_HOST}/${location}`;
    const cookieHeader = await getCookieHeader(jar, location);
    current = await fetch(location, {
      method: 'GET',
      headers: { ...BROWSER_HEADERS, Cookie: cookieHeader },
      redirect: 'manual',
    });
    await ingestCookies(jar, current, location);
    lastUrl = location;
    hops++;
  }
  return { finalRes: current, finalUrl: lastUrl, hops };
}

const TEST_CLIENT = process.env.BP_WEB_TEST_CLIENT || 'tuffbsitc';

async function login(client = BP_CLIENT, trace = null) {
  // Sandbox (test) account uses its own login; live uses the uploader account.
  // Sandbox (tuffbsitc) = the user's own login (DEC_USER/DEC_PASS) for safe testing.
  // >>> GO-LIVE TODO: the LIVE reference-write (tuffworkwear) uses the FILE UPLOADER
  //     account (BP_WEB_EMAIL/BP_WEB_PASSWORD). Confirm that env is the fileuploader@
  //     account and verify a reference-write works on live before switching on. <<<
  const isTest = client === TEST_CLIENT;
  const email = isTest ? (process.env.DEC_USER || process.env.BP_WEB_TEST_EMAIL || process.env.BP_WEB_EMAIL) : process.env.BP_WEB_EMAIL;
  const password = isTest ? (process.env.DEC_PASS || process.env.BP_WEB_TEST_PASSWORD || process.env.BP_WEB_PASSWORD) : process.env.BP_WEB_PASSWORD;
  if (!email || !password) {
    throw new Error(`BP web credentials not configured for client ${client}`);
  }
  const tstep = (o) => { if (trace) trace.push(o); };

  const jar = new CookieJar();

  // Step 1: GET the login page first. This is where Cloudflare sets
  // __cf_bm + _cfuvid (bot-management cookies) and BP sets the initial
  // pearlAdmin session-init token. Without these, the post-login flow
  // is routed to the new Sage SPA.
  const loginPageUrl = `${BP_HOST}/admin_login.php?clients_id=${encodeURIComponent(client)}`;
  const getRes = await fetch(loginPageUrl, {
    method: 'GET',
    headers: { ...BROWSER_HEADERS },
    redirect: 'manual',
  });
  await ingestCookies(jar, getRes, loginPageUrl);

  // Step 2: POST credentials, carrying the cookies from Step 1.
  const body = new URLSearchParams({
    email_address: email,
    password,
    action: 'login',
    redirect: `//${BP_HOST.replace(/^https?:\/\//, '')}/index.php`,
    clients_id: client,
  });

  const postCookies = await getCookieHeader(jar, BP_HOST);
  const postRes = await fetch(`${BP_HOST}/admin_login.php`, {
    method: 'POST',
    headers: {
      ...BROWSER_HEADERS,
      'Content-Type': 'application/x-www-form-urlencoded',
      'Referer': loginPageUrl,
      'Cookie': postCookies,
    },
    body,
    redirect: 'manual',
  });
  await ingestCookies(jar, postRes, `${BP_HOST}/admin_login.php`);
  tstep({ step: 'postLogin', client, emailUsed: email, status: postRes.status, location: (postRes.headers.get('location') || '').slice(0, 120) });

  // Step 3: Follow the redirect chain (/p.php?p=dash → /report.php → ...).
  // Each hop may re-issue pearlAdmin with HttpOnly+Secure flags upgraded.
  const { finalRes, finalUrl, hops } = await followRedirects(jar, postRes, `${BP_HOST}/admin_login.php`);

  // Sanity check: jar should now contain pearlAdmin. If it doesn't, our
  // credentials were wrong or BP changed the cookie name.
  const finalCookieHeader = await getCookieHeader(jar, BP_HOST);
  const cookieNames = finalCookieHeader
    .split('; ')
    .map((c) => c.split('=')[0])
    .filter(Boolean);
  tstep({ step: 'afterRedirects', hops, finalUrl: (finalUrl || '').slice(0, 120), finalStatus: finalRes.status, cookies: cookieNames });
  if (!cookieNames.includes('pearlAdmin')) {
    throw new Error(
      `BP login: pearlAdmin not found in jar after login. ` +
      `Cookies present: ${cookieNames.join(',') || 'none'}. ` +
      `Final URL: ${finalUrl} (status ${finalRes.status}). ` +
      `Likely cause: wrong email/password, or BP renamed the session cookie.`
    );
  }

  console.log(`[bp-web] login OK (${client}) after ${hops} redirect(s), cookies in jar: ${cookieNames.join(',')}`);
  const session = { jar, loggedInAt: Date.now() };
  sessions.set(client, session);
  return session;
}

async function getSession(client = BP_CLIENT) {
  const s = sessions.get(client);
  if (s && Date.now() - s.loggedInAt < SESSION_TTL_MS) return s;
  return await login(client);
}

function invalidateSession(client = BP_CLIENT) {
  sessions.delete(client);
}

async function fetchCsrfToken(jar, orderId) {
  const url = `${BP_HOST}/iframe_attach_file.php?e_name=orders_id&e_val=${encodeURIComponent(orderId)}`;
  const cookieHeader = await getCookieHeader(jar, url);
  const res = await fetch(url, {
    headers: {
      ...BROWSER_HEADERS,
      Cookie: cookieHeader,
      'Referer': `${BP_HOST}/patt-op.php?scode=invoice&oID=${encodeURIComponent(orderId)}`,
    },
  });
  await ingestCookies(jar, res, url);
  const html = await res.text();
  if (looksLikeLoginPage(html)) {
    const err = new Error('SESSION_EXPIRED');
    err.code = 'SESSION_EXPIRED';
    throw err;
  }
  // BP exposes the token as <meta name="__fc_csrf_token" content="...">
  // (the legacy form's JS reads it and posts it as a form field). It may
  // also appear as <input name="..." value="..."> in some templates.
  // Handle both attribute names (content / value) and both orders.
  const patterns = [
    /name=["']__fc_csrf_token["'][^>]*(?:content|value)=["']([^"']+)["']/i,
    /(?:content|value)=["']([^"']+)["'][^>]*name=["']__fc_csrf_token["']/i,
  ];
  for (const p of patterns) {
    const m = html.match(p);
    if (m && m[1]) return m[1];
  }

  const themeMatch = html.match(/data-theme=["']([^"']+)["']/);
  const titleMatch = html.match(/<title>([^<]+)<\/title>/i);
  const tokenIdx = html.indexOf('__fc_csrf_token');
  const tokenContext = tokenIdx >= 0
    ? `csrf-context: ...${html.slice(Math.max(0, tokenIdx - 100), tokenIdx + 300)}...`
    : 'csrf-context: token name string not present in body';
  throw new Error(
    `CSRF token not found in iframe form for order ${orderId}. ` +
    `status=${res.status} theme=${themeMatch?.[1] || 'none'} ` +
    `title="${titleMatch?.[1] || 'none'}" bodyLen=${html.length} ` +
    tokenContext
  );
}

// Attach a single file to a Brightpearl sales order via the legacy web
// upload endpoint. Re-logs in once if the cached session has expired.
async function attachFileToOrder(orderId, filename, buffer, mimeType = 'application/octet-stream') {
  let attempts = 0;
  while (attempts < 2) {
    attempts++;
    let session;
    try {
      session = await getSession();
      const csrf = await fetchCsrfToken(session.jar, orderId);

      const form = new FormData();
      form.append('MAX_FILE_SIZE', '20971520');
      form.append('userfile', new Blob([buffer], { type: mimeType }), filename);
      form.append('e_name', 'orders_id');
      form.append('e_val', String(orderId));
      form.append('Submit', 'Upload');
      form.append('__fc_csrf_token', csrf);

      const url = `${BP_HOST}/iframe_attach_file.php?&e_name=orders_id&e_val=${encodeURIComponent(orderId)}&filePosted=1`;
      const cookieHeader = await getCookieHeader(session.jar, url);
      const res = await fetch(url, {
        method: 'POST',
        headers: {
          ...BROWSER_HEADERS,
          Cookie: cookieHeader,
          'Referer': `${BP_HOST}/iframe_attach_file.php?e_name=orders_id&e_val=${encodeURIComponent(orderId)}`,
        },
        body: form,
        redirect: 'manual',
      });
      await ingestCookies(session.jar, res, url);
      const html = await res.text();

      if (res.ok && /<title>File store<\/title>/i.test(html)) {
        return { success: true };
      }
      if (looksLikeLoginPage(html) && attempts < 2) {
        invalidateSession();
        continue;
      }
      return {
        success: false,
        status: res.status,
        body: html.slice(0, 500),
      };
    } catch (err) {
      if (err.code === 'SESSION_EXPIRED' && attempts < 2) {
        invalidateSession();
        continue;
      }
      throw err;
    }
  }
  return { success: false, error: 'Max retry attempts exceeded' };
}

// GET an authenticated Brightpearl web page using the cached session (re-logs
// in once if expired). Follows redirects. Returns { status, html, finalUrl }.
async function fetchAuthed(url, { client = BP_CLIENT, method = 'GET', body, headers } = {}) {
  for (let attempt = 0; attempt < 2; attempt++) {
    const session = await getSession(client);
    const cookieHeader = await getCookieHeader(session.jar, url);
    let res = await fetch(url, { method, body, headers: { ...BROWSER_HEADERS, Cookie: cookieHeader, ...(headers || {}) }, redirect: 'manual' });
    await ingestCookies(session.jar, res, url);
    let finalUrl = url;
    if (method === 'GET' && [301, 302, 303, 307, 308].includes(res.status)) {
      const r = await followRedirects(session.jar, res, url);
      res = r.finalRes; finalUrl = r.finalUrl;
    }
    const html = await res.text();
    if (looksLikeLoginPage(html) && attempt === 0) { invalidateSession(client); continue; }
    return { status: res.status, html, finalUrl, location: res.headers.get('location') || null };
  }
  return { status: 0, html: '', finalUrl: url };
}

// Update a Brightpearl order's Reference box (the "orders_customer_ref" field).
// The public API can't edit an order's reference post-create; the legacy web UI
// auto-saves each field via ajaxData.php?op=order:validateOrder with an
// x-csrf-token header (token = the __fc_csrf_token meta on the order page).
// Used to write a supplier's order number onto our PO for two-way linkage.
// NOTE: web session only authenticates on the LIVE account — no sandbox login.
// Low-level: POST to the legacy order ajax endpoint with CSRF + session.
// op e.g. "order:validateOrder", data = form fields object.
async function orderAjaxPost(orderId, op, data, { client = BP_CLIENT } = {}) {
  for (let attempt = 0; attempt < 2; attempt++) {
    const session = await getSession(client);
    const pageUrl = `${BP_HOST}/patt-op.php?scode=invoice&oID=${encodeURIComponent(orderId)}`;
    let cookie = await getCookieHeader(session.jar, pageUrl);
    const pageRes = await fetch(pageUrl, { headers: { ...BROWSER_HEADERS, Cookie: cookie }, redirect: 'manual' });
    await ingestCookies(session.jar, pageRes, pageUrl);
    const html = await pageRes.text();
    if (looksLikeLoginPage(html) && attempt === 0) { invalidateSession(client); continue; }
    const token = (html.match(/name=["']__fc_csrf_token["'][^>]*content=["']([^"']+)["']/i)
      || html.match(/content=["']([^"']+)["'][^>]*name=["']__fc_csrf_token["']/i) || [])[1];
    if (!token) throw new Error(`__fc_csrf_token not found on order page ${orderId} (status ${pageRes.status})`);

    const url = `${BP_HOST}/ajaxData.php?op=${op}&oID=${encodeURIComponent(orderId)}`;
    cookie = await getCookieHeader(session.jar, url);
    const res = await fetch(url, {
      method: 'POST',
      headers: {
        ...BROWSER_HEADERS,
        'Content-Type': 'application/x-www-form-urlencoded',
        'X-Requested-With': 'XMLHttpRequest',
        'X-CSRF-Token': token,
        Accept: 'application/json, text/javascript, */*; q=0.01',
        Origin: BP_HOST,
        Referer: pageUrl,
        Cookie: cookie,
      },
      body: new URLSearchParams(data || {}).toString(),
    });
    await ingestCookies(session.jar, res, url);
    const text = await res.text();
    if (looksLikeLoginPage(text) && attempt === 0) { invalidateSession(client); continue; }
    let json = null; try { json = JSON.parse(text); } catch {}
    return { ok: res.ok, status: res.status, op, json, text: json ? undefined : text.slice(0, 400) };
  }
  return { ok: false, error: 'session expired' };
}

// One authed ajaxData request with CSRF + session cookies.
async function ajaxReq(session, url, token, method = 'POST', body) {
  const cookie = await getCookieHeader(session.jar, url);
  const res = await fetch(url, {
    method,
    headers: {
      ...BROWSER_HEADERS,
      'Content-Type': 'application/x-www-form-urlencoded',
      'X-Requested-With': 'XMLHttpRequest',
      'X-CSRF-Token': token,
      Accept: 'application/json, text/javascript, */*; q=0.01',
      Origin: BP_HOST,
      Referer: `${BP_HOST}/patt-op.php?scode=invoice`,
      Cookie: cookie,
    },
    body,
  });
  await ingestCookies(session.jar, res, url);
  const text = await res.text();
  let json = null; try { json = JSON.parse(text); } catch {}
  return { status: res.status, json, text };
}

// Save order-header fields via the legacy editor: pageLock -> order:validateOrder
// (persists while locked) -> release lock. `fields` = object of form fields.
// Live account only.
async function lockedValidateOrder(orderId, fields, { client = BP_CLIENT } = {}) {
  for (let attempt = 0; attempt < 2; attempt++) {
    const session = await getSession(client);
    const pageUrl = `${BP_HOST}/patt-op.php?scode=invoice&oID=${encodeURIComponent(orderId)}`;
    const pageCookie = await getCookieHeader(session.jar, pageUrl);
    const pageRes = await fetch(pageUrl, { headers: { ...BROWSER_HEADERS, Cookie: pageCookie }, redirect: 'manual' });
    await ingestCookies(session.jar, pageRes, pageUrl);
    const html = await pageRes.text();
    if (looksLikeLoginPage(html) && attempt === 0) { invalidateSession(client); continue; }
    const token = (html.match(/name=["']__fc_csrf_token["'][^>]*content=["']([^"']+)["']/i)
      || html.match(/content=["']([^"']+)["'][^>]*name=["']__fc_csrf_token["']/i) || [])[1];
    if (!token) throw new Error(`__fc_csrf_token not found on order page ${orderId} (status ${pageRes.status})`);

    const lockUrl = `${BP_HOST}/ajaxData.php?op=pageLock&resourceId=${encodeURIComponent(orderId)}&resourceTypeId=2`;
    const saveUrl = `${BP_HOST}/ajaxData.php?op=order:validateOrder&oID=${encodeURIComponent(orderId)}`;
    const lock = await ajaxReq(session, lockUrl, token, 'POST');
    const save = await ajaxReq(session, saveUrl, token, 'POST', new URLSearchParams(fields).toString());
    const unlock = await ajaxReq(session, lockUrl, token, 'DELETE');
    return {
      ok: save.status === 200, status: save.status,
      lock: { status: lock.status, body: (lock.json != null ? lock.json : (lock.text || '').slice(0, 150)) },
      save: { status: save.status, body: (save.json != null ? save.json : (save.text || '').slice(0, 250)) },
      unlock: { status: unlock.status, body: (unlock.json != null ? unlock.json : (unlock.text || '').slice(0, 150)) },
    };
  }
  return { ok: false, error: 'session expired' };
}

const decodeHtml = (s) => String(s || '')
  .replace(/&amp;/g, '&').replace(/&lt;/g, '<').replace(/&gt;/g, '>').replace(/&quot;/g, '"')
  .replace(/&#0?39;/g, "'").replace(/&#x([0-9a-f]+);/gi, (_, h) => String.fromCharCode(parseInt(h, 16))).replace(/&#(\d+);/g, (_, d) => String.fromCharCode(Number(d)));

// Parse the ENTIRE order edit form (the one holding orders_customer_ref) into an
// ordered list of [name, value] pairs — inputs (skipping submit/button/file and
// unchecked checkboxes/radios), textareas, and the selected <option> of selects.
// Array fields (name[]) are preserved in order. The whole form is re-submitted to
// save, so this must round-trip faithfully.
function parseOrderForm(html) {
  const idx = html.indexOf('name="orders_customer_ref"');
  if (idx < 0) return null;
  const start = html.lastIndexOf('<form', idx);
  const end = html.indexOf('</form>', idx);
  if (start < 0 || end < 0) return null;
  const form = html.slice(start, end);
  const fields = [];
  const seen = []; // track (start index) to interleave selects/textareas roughly in order is not needed for BP
  for (const m of form.matchAll(/<input\b[^>]*>/gi)) {
    const tag = m[0];
    const name = (tag.match(/\bname\s*=\s*"([^"]*)"/i) || [])[1];
    if (!name) continue;
    const type = ((tag.match(/\btype\s*=\s*"([^"]*)"/i) || [])[1] || 'text').toLowerCase();
    if (['submit', 'button', 'image', 'file', 'reset'].includes(type)) continue;
    if ((type === 'checkbox' || type === 'radio') && !/\bchecked\b/i.test(tag)) continue;
    const value = (tag.match(/\bvalue\s*=\s*"([^"]*)"/i) || [, ''])[1];
    fields.push([name, decodeHtml(value)]);
  }
  for (const m of form.matchAll(/<textarea\b[^>]*>([\s\S]*?)<\/textarea>/gi)) {
    const name = (m[0].match(/\bname\s*=\s*"([^"]*)"/i) || [])[1];
    if (name) fields.push([name, decodeHtml(m[1])]);
  }
  for (const m of form.matchAll(/<select\b[^>]*>([\s\S]*?)<\/select>/gi)) {
    const name = (m[0].match(/\bname\s*=\s*"([^"]*)"/i) || [])[1];
    if (!name) continue;
    const opts = [...m[1].matchAll(/<option\b([^>]*)>/gi)];
    let sel = opts.find((o) => /\bselected\b/i.test(o[1])) || opts[0];
    const val = sel ? (sel[1].match(/\bvalue\s*=\s*"([^"]*)"/i) || [, ''])[1] : '';
    fields.push([name, decodeHtml(val)]);
  }
  return fields;
}

// Save order-header changes by re-submitting the whole legacy edit form.
// overrides = { orders_customer_ref: '...' } etc. Refreshes the CSRF token from
// the live page. Live + sandbox (per client).
async function saveOrderForm(orderId, overrides = {}, { client = BP_CLIENT } = {}) {
  for (let attempt = 0; attempt < 2; attempt++) {
    const session = await getSession(client);
    const pageUrl = `${BP_HOST}/patt-op.php?scode=invoice&oID=${encodeURIComponent(orderId)}`;
    const cookie = await getCookieHeader(session.jar, pageUrl);
    const pageRes = await fetch(pageUrl, { headers: { ...BROWSER_HEADERS, Cookie: cookie }, redirect: 'manual' });
    await ingestCookies(session.jar, pageRes, pageUrl);
    const html = await pageRes.text();
    if (looksLikeLoginPage(html) && attempt === 0) { invalidateSession(client); continue; }
    const fields = parseOrderForm(html);
    if (!fields) throw new Error(`order form not found for ${orderId} (status ${pageRes.status})`);
    // The __fc_csrf_token form INPUT is empty in the raw HTML — the page JS fills
    // it from the <meta name="__fc_csrf_token"> at submit time. Do the same.
    const metaToken = (html.match(/name=["']__fc_csrf_token["'][^>]*content=["']([^"']+)["']/i)
      || html.match(/content=["']([^"']+)["'][^>]*name=["']__fc_csrf_token["']/i) || [])[1];
    const applied = { ...overrides };
    if (metaToken && !('__fc_csrf_token' in applied)) applied.__fc_csrf_token = metaToken;
    for (const [k, v] of Object.entries(applied)) {
      const f = fields.find((x) => x[0] === k);
      if (f) f[1] = String(v); else fields.push([k, String(v)]);
    }
    const fd = new FormData();
    for (const [k, v] of fields) fd.append(k, v);
    const postCookie = await getCookieHeader(session.jar, pageUrl);
    const res = await fetch(pageUrl, { method: 'POST', headers: { ...BROWSER_HEADERS, Cookie: postCookie, Origin: BP_HOST, Referer: pageUrl, 'Sec-Fetch-Dest': 'document', 'Sec-Fetch-Mode': 'navigate', 'Sec-Fetch-Site': 'same-origin', 'Sec-Fetch-User': '?1' }, body: fd, redirect: 'manual' });
    await ingestCookies(session.jar, res, pageUrl);
    return { ok: [200, 302].includes(res.status), status: res.status, location: res.headers.get('location'), fieldCount: fields.length };
  }
  return { ok: false, error: 'session expired' };
}

// Set the order's Reference box (orders_customer_ref) via a full form re-submit.
async function updateOrderReference(orderId, reference, opts = {}) {
  return saveOrderForm(orderId, { orders_customer_ref: reference }, opts);
}

export { attachFileToOrder, login, invalidateSession, fetchAuthed, getSession, getCookieHeader, updateOrderReference, lockedValidateOrder, orderAjaxPost, BP_HOST };
