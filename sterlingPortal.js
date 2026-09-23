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

function readCookies(res, jar) {
  const set = typeof res.headers.getSetCookie === 'function' ? res.headers.getSetCookie() : [res.headers.get('set-cookie')].filter(Boolean);
  for (const s of set) {
    const m = String(s).match(/^\s*([^=;]+)=([^;]*)/);
    if (!m) continue;
    const name = m[1].trim();
    // An empty value is a DELETION (sign-out clears the cookie by setting it empty + expired);
    // storing it would keep sending a dead session and every later call would 302 to login.
    if (m[2] === '' || /expires=thu, 01 jan 1970/i.test(String(s))) delete jar[name];
    else jar[name] = `${name}=${m[2]}`;
  }
}
const cookieHeader = (jar) => Object.values(jar).join('; ');
const tokenFrom = (html) => (String(html).match(/name="__RequestVerificationToken"[^>]*value="([^"]+)"/i) || [])[1] || null;

// Follow 302s by hand so every hop's Set-Cookie lands in one jar — the OIDC dance crosses two
// hosts and `redirect: "follow"` would drop the cookies set on the intermediate hops.
async function hop(url, opts, jar, { max = 10 } = {}) {
  let current = url, res = null;
  for (let i = 0; i < max; i++) {
    res = await fetch(current, {
      ...opts,
      redirect: 'manual',
      headers: { 'User-Agent': UA, Accept: 'text/html,application/xhtml+xml', Cookie: cookieHeader(jar), ...(opts.headers || {}) },
    });
    readCookies(res, jar);
    if (res.status < 300 || res.status > 399) return res;
    const loc = res.headers.get('location');
    if (!loc) return res;
    current = new URL(loc, current).toString();
    opts = { method: 'GET' };                 // a redirect is always followed as a GET with no body
  }
  return res;
}

let session = { jar: null, at: 0 };
const TTL = 15 * 60 * 1000;

export async function sterlingLogin({ force = false } = {}) {
  if (!force && session.jar && Date.now() - session.at < TTL) return session.jar;
  const user = process.env.STERLING_USER, pass = process.env.STERLING_PASS;
  if (!user || !pass) throw new Error('STERLING_USER / STERLING_PASS are not set on this service');
  const jar = {};
  // Hitting the app unauthenticated bounces through OIDC and lands on the login form, which is
  // where the antiforgery token and the exact ReturnUrl come from — both are per-request.
  const start = await hop(`${BASE}/`, { method: 'GET' }, jar);
  const html = await start.text();
  const token = tokenFrom(html);
  const loginUrl = start.url || `${LOGIN_BASE}/Account/Login`;
  if (!/\/Account\/Login/i.test(loginUrl) && !/name="__RequestVerificationToken"/i.test(html)) {
    // Already signed in (a warm jar) — nothing to post.
    session = { jar, at: Date.now() };
    return jar;
  }
  const body = new URLSearchParams();
  if (token) body.set('__RequestVerificationToken', token);
  // Field names come from the login page itself rather than being guessed, because ASP.NET Identity
  // templates differ ("Email"/"Input.Email"/"Username") and a wrong name silently re-renders the
  // form with a 200, which looks like success to anything that only checks the status code.
  const emailField = (html.match(/<input[^>]*name="([^"]*(?:Email|UserName|Username)[^"]*)"/i) || [])[1] || 'Email';
  const passField = (html.match(/<input[^>]*type="password"[^>]*name="([^"]+)"/i) || [])[1]
    || (html.match(/<input[^>]*name="([^"]*Password[^"]*)"[^>]*type="password"/i) || [])[1] || 'Password';
  body.set(emailField, user);
  body.set(passField, pass);
  const res = await hop(loginUrl, {
    method: 'POST', body: body.toString(),
    headers: { 'Content-Type': 'application/x-www-form-urlencoded', Referer: loginUrl },
  }, jar);
  const after = await res.text();
  // The only trustworthy signal is that we ended up back on the app with a session — a 200 that is
  // still the login page means the credentials were refused.
  if (/name="__RequestVerificationToken"/i.test(after) && /\/Account\/Login/i.test(res.url || '')) {
    const err = (after.match(/validation-summary-errors[\s\S]{0,200}?<li>([^<]+)</i) || [])[1];
    throw new Error(`Sterling login refused${err ? `: ${err.trim()}` : ' (still on the login page)'}`);
  }
  session = { jar, at: Date.now() };
  return jar;
}

// A page from the app, used both to read state and to harvest a fresh antiforgery token.
async function appGet(path, jar) {
  const res = await hop(`${BASE}${path}`, { method: 'GET' }, jar);
  const html = await res.text();
  if (/\/Account\/Login/i.test(res.url || '')) throw new Error(`session expired fetching ${path}`);
  return { status: res.status, url: res.url, html };
}

// BASKET — add barcodes. `[{ barcode, quantity, isSale:false }]`, answered with the new line count.
// The style in the path is only the page the handler hangs off; "_" is what the site's own basket
// widget uses for the non-product pages, so it works for a bulk add.
export async function sterlingAddToBasket(items, { jar = null, page = '_' } = {}) {
  const j = jar || (await sterlingLogin());
  const { html } = await appGet(`/detail/${encodeURIComponent(page)}`, j);
  const token = tokenFrom(html);
  if (!token) throw new Error('no antiforgery token on the detail page — cannot add to basket');
  const body = items.map((i) => ({ barcode: String(i.barcode || i.sku), quantity: Math.round(Number(i.qty ?? i.quantity) || 0), isSale: false }))
    .filter((i) => i.barcode && i.quantity > 0);
  if (!body.length) return { ok: false, reason: 'no lines with a barcode and a quantity' };
  const res = await fetch(`${BASE}/detail/${encodeURIComponent(page)}?handler=Basket`, {
    method: 'POST',
    headers: {
      'User-Agent': UA, 'Content-Type': 'application/json', Accept: '*/*',
      requestverificationtoken: token, Cookie: cookieHeader(j), Referer: `${BASE}/detail/${encodeURIComponent(page)}`,
    },
    body: JSON.stringify(body),
  });
  readCookies(res, j);
  const text = (await res.text()).trim();
  return { ok: res.ok, status: res.status, sent: body, response: text.slice(0, 200) };
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
    headers: { 'User-Agent': UA, 'Content-Type': 'application/x-www-form-urlencoded', Cookie: cookieHeader(j), Referer: `${BASE}/Checkout` },
    body: fields.toString(),
  });
  readCookies(res, j);
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
