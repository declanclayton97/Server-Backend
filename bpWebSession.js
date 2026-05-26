// Brightpearl legacy web-app session manager.
//
// Why this exists: BP's public REST API has no documented file-upload
// endpoint. Files can only be attached to orders via the authenticated
// web UI at /iframe_attach_file.php. This module logs in to that web UI
// using stored credentials, caches the session cookies, fetches a CSRF
// token from the form page, and POSTs files to that endpoint.
//
// Fragility note: this is undocumented BP internals. If BP changes their
// login flow, CSRF mechanism, or upload endpoint, this breaks silently.
// Watch for [bp-web] error logs.

const BP_HOST = process.env.BP_WEB_HOST || 'https://euw1.brightpearlapp.com';
const BP_CLIENT = process.env.BP_WEB_CLIENT_ID || 'tuffworkwear';
const SESSION_TTL_MS = 25 * 60 * 1000; // BP sessions live ~30 min; refresh at 25

let cachedSession = null; // { cookies: string, loggedInAt: number }

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

function cookieJarToHeader(setCookieArr) {
  return setCookieArr.map((h) => h.split(';')[0]).join('; ');
}

function looksLikeLoginPage(html) {
  return /name=["']email_address["']/i.test(html) && /name=["']password["']/i.test(html);
}

async function login() {
  const email = process.env.BP_WEB_EMAIL;
  const password = process.env.BP_WEB_PASSWORD;
  if (!email || !password) {
    throw new Error('BP_WEB_EMAIL/BP_WEB_PASSWORD not configured');
  }

  const body = new URLSearchParams({
    email_address: email,
    password,
    action: 'login',
    redirect: `//${BP_HOST.replace(/^https?:\/\//, '')}/index.php`,
    clients_id: BP_CLIENT,
  });

  const res = await fetch(`${BP_HOST}/admin_login.php`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
    body,
    redirect: 'manual',
  });

  const setCookies = extractSetCookies(res);
  if (!setCookies.length) {
    const peek = await res.text().catch(() => '');
    throw new Error(`BP login: no cookies returned (status ${res.status}). Body start: ${peek.slice(0, 200)}`);
  }

  cachedSession = {
    cookies: cookieJarToHeader(setCookies),
    loggedInAt: Date.now(),
  };
  console.log('[bp-web] logged in, cached session');
  return cachedSession;
}

async function getSession() {
  if (cachedSession && Date.now() - cachedSession.loggedInAt < SESSION_TTL_MS) {
    return cachedSession;
  }
  return await login();
}

function invalidateSession() {
  cachedSession = null;
}

async function fetchCsrfToken(orderId, cookies) {
  const url = `${BP_HOST}/iframe_attach_file.php?e_name=orders_id&e_val=${encodeURIComponent(orderId)}`;
  const res = await fetch(url, { headers: { Cookie: cookies } });
  const html = await res.text();
  if (looksLikeLoginPage(html)) {
    const err = new Error('SESSION_EXPIRED');
    err.code = 'SESSION_EXPIRED';
    throw err;
  }
  const match = html.match(/name=["']__fc_csrf_token["']\s+value=["']([^"']+)["']/i);
  if (!match) {
    throw new Error(`CSRF token not found in iframe form for order ${orderId}. Body start: ${html.slice(0, 200)}`);
  }
  return match[1];
}

// Attach a single file to a Brightpearl sales order via the legacy web
// upload endpoint. Re-logs in once if the cached session has expired.
//
// Returns { success: true } on confirmed upload, or
// { success: false, status, body } on failure.
async function attachFileToOrder(orderId, filename, buffer, mimeType = 'application/octet-stream') {
  let attempts = 0;
  while (attempts < 2) {
    attempts++;
    let session;
    try {
      session = await getSession();
      const csrf = await fetchCsrfToken(orderId, session.cookies);

      const form = new FormData();
      form.append('MAX_FILE_SIZE', '20971520');
      form.append('userfile', new Blob([buffer], { type: mimeType }), filename);
      form.append('e_name', 'orders_id');
      form.append('e_val', String(orderId));
      form.append('Submit', 'Upload');
      form.append('__fc_csrf_token', csrf);

      const url = `${BP_HOST}/iframe_attach_file.php?&e_name=orders_id&e_val=${encodeURIComponent(orderId)}&filePosted=1`;
      const res = await fetch(url, {
        method: 'POST',
        headers: { Cookie: session.cookies },
        body: form,
        redirect: 'manual',
      });
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

export { attachFileToOrder, login, invalidateSession };
