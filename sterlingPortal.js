// Sterling Safetywear — the NEW B2B portal (b2b.sterlingsafetywear.co.uk).
//
// Sterling moved their trade site on or before 2026-09-23: sterling.famlive.net now answers 503
// and serves a "OUR TRADE SITE HAS MOVED" notice, which is why the Playwright worker's login found
// no form ("attempted=false, inputs:[]") and the 13:00 lane failed with nothing ordered.
//
// The replacement is an ASP.NET Razor Pages app (with Blazor Server components for the interactive
// bits). The ORDERING path is plain HTML forms and one JSON handler, so it does NOT need a browser:
//
//   login   OIDC:  GET  login.sterlingsafetywear.co.uk/Account/Login?ReturnUrl=…   (antiforgery)
//                  POST same URL with Email/Password           → 302 to /connect/authorize/callback
//                  POST b2b…/signin-oidc                       → 302, session cookie set
//   basket  POST  /detail/{style}?handler=Basket
//                 header requestverificationtoken: <token>, JSON body
//                 [{ "barcode": "5055160071440", "quantity": 1, "isSale": false }]   → "1"
//   read    GET   /detail/_?handler=BasketPartial               → HTML fragment
//   place   GET   /Checkout  → antiforgery token + a form pre-filled with OUR address
//           POST  /Checkout  (urlencoded) with TermsAccepted=true and OrderReference=<PO#>
//
// Shapes taken from "Sterling Checkout New Portal.har" (user recording, 2026-09-23).
//
// ⚠️ THE BASKET IS KEYED BY BARCODE, which is a large simplification: Sterling products in
// Brightpearl carry the EAN AS THEIR SKU (product 119066 = "5055160056461"), so a PO row's SKU is
// the barcode and the whole search → result-click → colour/size-box resolver that sterlingResolve.js
// exists for is no longer needed. That resolver is what failed on the three new DeWalt codes on
// 2026-09-18.
//
// ⚠️ DELIVERY ADDRESS. The checkout carries a `SelectedOption` picker whose entries are OTHER
// companies' addresses saved on our account ("Josh Witcombe", "J V Price Ltd", "Gary Perry
// Plumbing"…) — drop-ship destinations. Picking one sends our goods to a customer. This module
// therefore never sets SelectedOption, forces UseInvoiceAddressForDelivery, and REFUSES to submit
// unless the form's delivery postcode is ours. Blaklader taught this the same morning: an order
// body built from someone else's order was addressed to Power Engineering Services Ltd and only a
// 400 from Blaklader stopped it going out.

const BASE = process.env.STERLING_B2B_BASE || 'https://b2b.sterlingsafetywear.co.uk';
const LOGIN_BASE = process.env.STERLING_LOGIN_BASE || 'https://login.sterlingsafetywear.co.uk';
const UA = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36';
export const STERLING_OUR_POSTCODE = process.env.STERLING_DELIVERY_POSTCODE || 'LS26 8LG';

const zipKey = (z) => String(z || '').replace(/[^A-Z0-9]/gi, '').toUpperCase();
export const sterlingShipsToUs = (zip) => !!zipKey(zip) && zipKey(zip) === zipKey(STERLING_OUR_POSTCODE);

// PER-HOST cookie jars. The login runs across TWO hosts — the IdentityServer at
// login.sterlingsafetywear.co.uk and the app at b2b… — and each sets its own session cookie.
// One flat jar would send each host the other's session, which is both wrong and needless
// exposure of a session identifier.
const jarFor = (jar, url) => (jar[new URL(url).host] = jar[new URL(url).host] || {});
function readCookies(res, jar, url) {
  const bag = jarFor(jar, url);
  const set = typeof res.headers.getSetCookie === 'function' ? res.headers.getSetCookie() : [res.headers.get('set-cookie')].filter(Boolean);
  for (const s of set) {
    const m = String(s).match(/^\s*([^=;]+)=([^;]*)/);
    if (!m) continue;
    const name = m[1].trim();
    // An empty value is a DELETION (sign-out clears the cookie by setting it empty + expired);
    // storing it would keep sending a dead session and every later call would bounce to login.
    if (m[2] === '' || /expires=thu, 01 jan 1970/i.test(String(s))) delete bag[name];
    else bag[name] = `${name}=${m[2]}`;
  }
}
const cookieHeader = (jar, url) => Object.values(jarFor(jar, url)).join('; ');
const tokenFrom = (html) => (String(html).match(/name="__RequestVerificationToken"[^>]*value="([^"]+)"/i) || [])[1] || null;

// Follow 302s by hand so every hop's Set-Cookie lands in one jar — the OIDC dance crosses two
// hosts and `redirect: "follow"` would drop the cookies set on the intermediate hops.
async function hop(url, opts, jar, { max = 12, trace = null } = {}) {
  let current = url, res = null;
  for (let i = 0; i < max; i++) {
    res = await fetch(current, {
      ...opts,
      redirect: 'manual',
      headers: { 'User-Agent': UA, Accept: 'text/html,application/xhtml+xml', Cookie: cookieHeader(jar, current), ...(opts.headers || {}) },
    });
    readCookies(res, jar, current);
    if (trace) trace.push({ kind: opts.method === 'POST' && i === 0 ? 'post' : 'get', status: res.status, url: current.replace(BASE, 'b2b:').replace(LOGIN_BASE, 'login:').slice(0, 140) });
    if (res.status < 300 || res.status > 399) return Object.assign(res, { finalUrl: current });
    const loc = res.headers.get('location');
    if (!loc) return Object.assign(res, { finalUrl: current });
    current = new URL(loc, current).toString();
    opts = { method: 'GET' };                 // a redirect is always followed as a GET with no body
  }
  return Object.assign(res, { finalUrl: current });
}

// THIS OIDC FLOW MOVES BY SELF-SUBMITTING FORMS, NOT REDIRECTS — in BOTH directions.
// `GET /SignIn` answers 200 with a page titled "Working..." holding a form that posts client_id,
// nonce, state, code_challenge… to login…/connect/authorize; the authorization code comes back the
// same way (response_mode=form_post) as a form posting to b2b…/signin-oidc. A redirect-follower
// stops dead on both, which is why the first attempts collected Nonce/Correlation cookies — the
// handshake had started — and never an authentication cookie.
const isAutoPost = (html) => /<form[^>]*action="[^"]+"/i.test(html) && !/type="password"/i.test(html)
  && (/Working\.\.\./i.test(html) || /document\.forms\[0\]\.submit|onload="document\.forms/i.test(html) || /<title>\s*Working/i.test(html));

function autoPostBody(html) {
  const body = new URLSearchParams();
  for (const m of html.matchAll(/<input\b[^>]*>/gi)) {
    const tag = m[0];
    const type = ((tag.match(/type="([^"]*)"/i) || [])[1] || '').toLowerCase();
    if (type === 'submit' || type === 'button' || type === 'password') continue;
    const name = (tag.match(/name="([^"]+)"/i) || [])[1];
    if (!name) continue;
    const value = (tag.match(/value="([^"]*)"/i) || [])[1];
    body.set(name, value != null ? value.replace(/&amp;/g, '&') : '');
  }
  return body;
}

// Follow redirects AND auto-submitting forms until a real page comes back.
async function hopAuto(url, opts, jar, { maxForms = 4, trace = null } = {}) {
  let res = await hop(url, opts, jar, { trace });
  for (let i = 0; i < maxForms; i++) {
    const html = await res.text();
    if (!isAutoPost(html)) return Object.assign(res, { pageHtml: html });
    const action = (html.match(/<form[^>]*action="([^"]+)"/i) || [])[1];
    const body = autoPostBody(html);
    if (!action || ![...body.keys()].length) return Object.assign(res, { pageHtml: html });
    const next = new URL(action.replace(/&amp;/g, '&'), res.finalUrl || url).toString();
    if (trace) trace.push({ kind: 'auto-form', to: next.replace(BASE, 'b2b:').replace(LOGIN_BASE, 'login:').slice(0, 140), fields: [...body.keys()].slice(0, 12) });
    res = await hop(next, {
      method: 'POST', body: body.toString(),
      headers: { 'Content-Type': 'application/x-www-form-urlencoded', Referer: res.finalUrl || url },
    }, jar, { trace });
  }
  return Object.assign(res, { pageHtml: await res.text() });
}

// The ONLY cookie that means "authenticated". Everything else on this host is handshake state.
const hasAuthCookie = (jar) => Object.keys(jarFor(jar, BASE)).some((n) => /^\.AspNetCore\.Cookies/i.test(n));

let session = { jar: null, at: 0 };
const TTL = 15 * 60 * 1000;

export async function sterlingLogin({ force = false } = {}) {
  if (!force && session.jar && Date.now() - session.at < TTL) return session.jar;
  const user = process.env.STERLING_USER, pass = process.env.STERLING_PASS;
  if (!user || !pass) throw new Error('STERLING_USER / STERLING_PASS are not set on this service');
  const jar = {};
  // START AT /SignIn, not at "/". The root is a PUBLIC landing page that answers 200 and carries an
  // antiforgery token of its own, so starting there looks like a login page while being nothing of
  // the sort — the first version of this posted the credentials into that page's form and then
  // reported success on an empty cookie jar. /SignIn is the app's OIDC challenge and bounces to
  // login.sterlingsafetywear.co.uk/connect/authorize → /Account/Login?ReturnUrl=…
  const start = await hopAuto(`${BASE}/SignIn?returnUrl=%2F`, { method: 'GET' }, jar);
  const loginUrl = start.finalUrl || '';
  const html = start.pageHtml;
  const passField = (html.match(/<input[^>]*type="password"[^>]*name="([^"]+)"/i) || [])[1]
    || (html.match(/<input[^>]*name="([^"]+)"[^>]*type="password"/i) || [])[1];
  if (!passField) {
    // No password box means this is not the login form, whatever it returned. NEVER post
    // credentials into a page we have not positively identified.
    //
    // "Do we already have a session?" must be answered by the AUTH cookie and nothing else. This
    // used to accept any cookie on the app host, and the OIDC challenge sets .AspNetCore.Nonce and
    // .Correlation before a single credential is checked — so a handshake that never completed
    // reported a healthy login, and only the empty basket and the missing antiforgery tokens
    // further down gave it away.
    if (hasAuthCookie(jar)) { session = { jar, at: Date.now() }; return jar; }
    throw new Error(`Sterling login: landed on ${loginUrl.slice(0, 120)} with no password field and no auth cookie `
      + `(held: ${Object.keys(jarFor(jar, BASE)).map((n) => n.replace(/\.CfDJ8.*/, '')).join(', ') || 'none'}) — not posting credentials`);
  }
  const userField = (html.match(/<input[^>]*name="((?:[^"]*\.)?(?:Username|UserName|Email)[^"]*)"/i) || [])[1] || 'Username';
  const token = tokenFrom(html);
  const returnUrl = (html.match(/name="ReturnUrl"[^>]*value="([^"]*)"/i) || [])[1];
  const body = new URLSearchParams();
  if (token) body.set('__RequestVerificationToken', token);
  if (returnUrl) body.set('ReturnUrl', returnUrl.replace(/&amp;/g, '&'));
  body.set(userField, user);
  body.set(passField, pass);
  body.set('RememberLogin', 'false');
  body.set('button', 'login');
  const res = await hopAuto(loginUrl, {
    method: 'POST', body: body.toString(),
    headers: { 'Content-Type': 'application/x-www-form-urlencoded', Referer: loginUrl },
  }, jar);
  // The authorize endpoint answers with another form_post page; hopAuto replays it, which is what
  // hands the authorization code to the app and finally sets the session cookie.
  const after = res.pageHtml;
  // Trust the SESSION, not the status code: a refused login re-renders the form with a 200.
  if (!hasAuthCookie(jar) || /type="password"/i.test(after)) {
    // Report SterlingS OWN words. IdentityServer renders the reason in an alert/validation block,
    // and "invalid credentials" needs a very different response from "your password must be reset"
    // — which their move notice says existing passwords may require on first use of the new site.
    const err = (after.match(/validation-summary-errors[\s\S]{0,300}?<li>([^<]+)</i) || [])[1]
      || (after.match(/field-validation-error[^>]*>([^<]+)</i) || [])[1]
      || (after.match(/class="[^"]*alert[^"]*"[^>]*>\s*([^<]{4,160})/i) || [])[1]
      || (after.match(/<div[^>]*validation-summary[^>]*>[\s\S]{0,200}?([A-Z][^<]{6,160})/i) || [])[1];
    throw new Error(`Sterling login refused${err ? `: "${err.trim().replace(/\s+/g, ' ')}"` : ' (no message on the page)'}`
      + ` — landed on ${(res.finalUrl || '').replace(LOGIN_BASE, 'login:').replace(BASE, 'b2b:').slice(0, 80)}`
      + `, no .AspNetCore.Cookies auth cookie`);
  }
  session = { jar, at: Date.now() };
  return jar;
}

// A page from the app, used both to read state and to harvest a fresh antiforgery token.
async function appGet(path, jar) {
  const res = await hop(`${BASE}${path}`, { method: 'GET' }, jar);
  const html = await res.text();
  // `res.url` is empty on a manually-followed redirect chain, so the hop's own final URL is what
  // says where we ended up — without it an expired session reads as a successful fetch of the
  // login page, and every later parse fails somewhere less obvious.
  const at = res.finalUrl || res.url || '';
  if (/\/Account\/Login/i.test(at)) throw new Error(`session expired fetching ${path} (bounced to login)`);
  return { status: res.status, url: at, html };
}

// Trace the OIDC challenge WITHOUT posting credentials: every hop, where it ended, and whether a
// password box is there at the end. Needed because "login succeeded" was being decided by the
// presence of any app cookie, and the OIDC handshake sets Nonce/Correlation cookies before any
// authentication happens — so a challenge that never reached the login form looked like a session.
export async function sterlingLoginTrace() {
  const jar = {}, trace = [];
  const res = await hopAuto(`${BASE}/SignIn?returnUrl=%2F`, { method: 'GET' }, jar, { trace });
  const html = res.pageHtml || '';
  return {
    trace,
    landedOn: (res.finalUrl || '').replace(BASE, 'b2b:').replace(LOGIN_BASE, 'login:').slice(0, 160),
    bytes: html.length,
    title: (html.match(/<title>([^<]*)</i) || [])[1] || null,
    hasPasswordField: /type="password"/i.test(html),
    hasToken: /__RequestVerificationToken/.test(html),
    formAction: (html.match(/<form[^>]*action="([^"]{0,140})"/i) || [])[1] || null,
    fieldNames: [...html.matchAll(/<input[^>]*name="([^"]+)"/gi)].map((m) => m[1]).filter((n) => !/Verification/i.test(n)).slice(0, 14),
    errorText: (html.match(/validation-summary-errors[\s\S]{0,200}?<li>([^<]+)</i) || [])[1] || null,
    cookies: Object.fromEntries(Object.entries(jar).map(([h, b]) => [h, Object.keys(b).map((k) => k.replace(/\.CfDJ8.*/, '.<id>'))])),
  };
}

// What does the site actually hand US? Facts only — where the request ended up, how big the page
// is, and which markers it carries — because "no antiforgery token" has at least three causes
// (not signed in, signed in but a different page, or the markup changed) and they look identical
// from the call site. Cookie NAMES only, never values.
export async function sterlingDiag(paths = ['/', '/detail/_', '/Checkout', '/Account/Manage'], { jar = null } = {}) {
  const j = jar || (await sterlingLogin());
  const out = [];
  for (const p of paths) {
    try {
      const res = await hop(`${BASE}${p}`, { method: 'GET' }, j);
      const html = await res.text();
      out.push({
        path: p, status: res.status, endedAt: (res.finalUrl || '').replace(BASE, '').slice(0, 120), bytes: html.length,
        hasToken: /__RequestVerificationToken/.test(html),
        looksSignedIn: /\/SignOut|Logout|Sign\s*out/i.test(html),
        looksLoggedOut: /\/SignIn\b|Sign\s*in<|type="password"/i.test(html),
        title: (html.match(/<title>([^<]*)</i) || [])[1] || null,
      });
    } catch (e) { out.push({ path: p, error: e.message }); }
  }
  return { cookies: Object.fromEntries(Object.entries(j).map(([host, bag]) => [host, Object.keys(bag)])), pages: out };
}

// BASKET — add barcodes. `[{ barcode, quantity, isSale:false }]`, answered with the new line count.
// The style in the path is only the page the handler hangs off; "_" is what the site's own basket
// widget uses for the non-product pages, so it works for a bulk add.
export async function sterlingAddToBasket(items, { jar = null, page = '_' } = {}) {
  const j = jar || (await sterlingLogin());
  // The token is per-page and NOT on every page: /detail/_ is a 1.5KB stub with none, while the
  // home page carries one. The HAR's add was posted from a real product page, which also has one.
  // Try in order and use the first that actually yields a token rather than assuming.
  let token = null, tokenFromPage = null;
  for (const p of [`/detail/${encodeURIComponent(page)}`, '/', '/Checkout']) {
    try {
      const got = tokenFrom((await appGet(p, j)).html);
      if (got) { token = got; tokenFromPage = p; break; }
    } catch { /* try the next page */ }
  }
  if (!token) throw new Error('no antiforgery token on /detail, / or /Checkout — cannot add to basket');
  const body = items.map((i) => ({ barcode: String(i.barcode || i.sku), quantity: Math.round(Number(i.qty ?? i.quantity) || 0), isSale: false }))
    .filter((i) => i.barcode && i.quantity > 0);
  if (!body.length) return { ok: false, reason: 'no lines with a barcode and a quantity' };
  const res = await fetch(`${BASE}/detail/${encodeURIComponent(page)}?handler=Basket`, {
    method: 'POST',
    headers: {
      'User-Agent': UA, 'Content-Type': 'application/json', Accept: '*/*',
      requestverificationtoken: token, Cookie: cookieHeader(j, BASE), Referer: `${BASE}/detail/${encodeURIComponent(page)}`,
    },
    body: JSON.stringify(body),
  });
  readCookies(res, j, BASE);
  const text = (await res.text()).trim();
  return { ok: res.ok, status: res.status, sent: body, tokenFrom: tokenFromPage, response: text.slice(0, 200) };
}

// Read the basket back. Their cart accepts codes it cannot resolve, so what was ASKED FOR is never
// proof of what is in it — every other lane here learned that the expensive way.
export async function sterlingBasket({ jar = null } = {}) {
  const j = jar || (await sterlingLogin());
  const { html } = await appGet('/detail/_?handler=BasketPartial', j);
  const lines = [];
  for (const m of html.matchAll(/data-barcode="(\d+)"[\s\S]{0,400}?data-quantity="(\d+)"/gi)) lines.push({ barcode: m[1], qty: Number(m[2]) });
  const total = (html.match(/(?:basket|cart)[^£]{0,40}£\s*([\d,]+\.\d{2})/i) || [])[1] || null;
  return { lines, count: lines.length, units: lines.reduce((a, l) => a + l.qty, 0), total, raw: html.length };
}

// CHECKOUT. Reads the form, checks WHERE it is addressed, and only then posts it.
// execute:false returns exactly what would be sent, which is how this gets tested without buying.
export async function sterlingCheckout({ orderRef, orderText = '', jar = null, execute = false } = {}) {
  const j = jar || (await sterlingLogin());
  const { html } = await appGet('/Checkout', j);
  const start = html.indexOf('<form id="checkout-form"');
  if (start < 0) return { ok: false, step: 'checkout-form', reason: 'no checkout form on /Checkout — basket empty, or the page changed' };
  const form = html.slice(start, html.indexOf('</form>', start));
  const token = tokenFrom(form) || tokenFrom(html);
  if (!token) return { ok: false, step: 'checkout-form', reason: 'no antiforgery token in the checkout form' };

  // Start from the fields the server itself rendered, so anything we do not understand is carried
  // through untouched rather than dropped. Checkboxes contribute only their "true" value, and the
  // hidden "false" companion ASP.NET pairs them with is added back explicitly below.
  const fields = new URLSearchParams();
  for (const m of form.matchAll(/<(input|textarea)\b[^>]*>/gi)) {
    const tag = m[0];
    const at = (k) => (tag.match(new RegExp(`${k}="([^"]*)"`, 'i')) || [])[1];
    const name = at('name');
    if (!name || /^__RequestVerificationToken$/i.test(name)) continue;
    const type = (at('type') || '').toLowerCase();
    if (type === 'checkbox') continue;                       // handled explicitly
    if (type === 'hidden' && /UseInvoiceAddressForDelivery|TermsAccepted/i.test(name)) continue;
    if (fields.has(name)) continue;
    fields.set(name, at('value') || '');
  }
  fields.set('__RequestVerificationToken', token);
  fields.set('OrderReference', String(orderRef == null ? '' : orderRef));
  fields.set('OrderText', String(orderText || ''));
  // OUR address, always. SelectedOption is the saved-address picker and its entries are other
  // companies — leaving it empty is what keeps the goods coming here.
  fields.set('SelectedOption', '');
  fields.set('UseInvoiceAddressForDelivery', 'true');
  fields.append('UseInvoiceAddressForDelivery', 'false');
  fields.set('TermsAccepted', 'true');
  fields.append('TermsAccepted', 'false');

  const postcode = fields.get('DeliveryAddress.Postcode');
  const company = fields.get('DeliveryAddress.Company');
  if (!sterlingShipsToUs(postcode)) {
    return { ok: false, step: 'delivery-address', sent: false,
      reason: `REFUSED: the checkout is addressed to "${company || '?'}" at ${postcode || '?'}, not our ${STERLING_OUR_POSTCODE}. Nothing was submitted.`,
      deliveryAddress: { company, postcode } };
  }
  const preview = { orderRef, company, postcode, fields: [...fields.keys()].filter((k) => !/Verification/i.test(k)) };
  if (!execute) return { ok: true, sent: false, dryRun: true, ...preview };

  const res = await fetch(`${BASE}/Checkout`, {
    method: 'POST', redirect: 'manual',
    headers: { 'User-Agent': UA, 'Content-Type': 'application/x-www-form-urlencoded', Cookie: cookieHeader(j, BASE), Referer: `${BASE}/Checkout` },
    body: fields.toString(),
  });
  readCookies(res, j, BASE);
  const loc = res.headers.get('location');
  const text = res.status >= 300 && res.status <= 399 ? '' : await res.text();
  // A 302 away from /Checkout is the success shape; a 200 means it re-rendered the form, which is a
  // validation failure however healthy the status code looks.
  const placed = !!(loc && !/\/Checkout$/i.test(loc));
  const orderNo = (loc && (loc.match(/(?:order|confirmation)\/?([A-Z0-9-]+)/i) || [])[1])
    || (text.match(/Order\s*(?:number|ref(?:erence)?)\s*[:#]?\s*([A-Z0-9-]{4,})/i) || [])[1] || null;
  return { ok: placed, sent: true, status: res.status, location: loc, orderNo, ...preview,
    ...(placed ? {} : { reason: 'the checkout did not redirect — treat as NOT placed and check the portal', bodySample: String(text).slice(0, 400) }) };
}
