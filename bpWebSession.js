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

async function login(client = BP_CLIENT) {
  const email = process.env.BP_WEB_EMAIL;
  const password = process.env.BP_WEB_PASSWORD;
  if (!email || !password) {
    throw new Error('BP_WEB_EMAIL/BP_WEB_PASSWORD not configured');
  }

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

export { attachFileToOrder, login, invalidateSession, fetchAuthed, getSession, getCookieHeader, BP_HOST };
