// Brightpearl's order screen, worked the way a member of staff works it.
//
// Why this exists: the BP API allowance is 200 calls a minute for the WHOLE
// account (every app shares it), and the purchasing runs already use all of it
// at 09:42 and 09:45 (measured 26 Oct 2026). BP's own web screens are not
// counted against that allowance: 20 order-screen loads through the web login
// left the API allowance untouched. The automation-rule replacement therefore
// reads and changes orders here, through the screen, as a dedicated BP user.
//
// How BP saves an order: the whole order screen is ONE form (#form) holding
// every row, price, address, custom field, the status and the shipping method.
// "Save changes" posts that whole form back to the same URL. So a change here is:
// read the screen -> take the form exactly as a browser would send it -> change
// only the named fields -> post it -> read the screen again and check that every
// change stuck. Nothing is assumed saved until the re-read shows it.
//
// Two things a careless resubmit would do, and this refuses:
//   - notify_customer: a ticked box emails the customer. It is never sent.
//   - a select value the screen doesn't offer (a shipping method or status id
//     that doesn't exist): BP may store it or drop it silently. Refused before
//     anything is posted.
//   - the rows' VAT as read (0.00 in the raw HTML): put back from the API first,
//     see "the order rows' VAT" below.
//
// The transport is injected so the tests can run on a saved screen with no
// network. `webTransport(client)` is the real one, on bpWebSession's login.

const decode = (s) => String(s ?? '')
  .replace(/&quot;/g, '"').replace(/&#0?39;/g, "'").replace(/&lt;/g, '<').replace(/&gt;/g, '>')
  .replace(/&#x([0-9a-f]+);/gi, (_, h) => String.fromCharCode(parseInt(h, 16)))
  .replace(/&#(\d+);/g, (_, d) => String.fromCharCode(Number(d)))
  .replace(/&nbsp;/g, ' ').replace(/&amp;/g, '&');

// Attributes of one start tag: double, single or unquoted values, bare flags.
function attrs(tag) {
  const out = {};
  const body = tag.replace(/^<\s*[a-z0-9]+/i, '').replace(/\/?>$/, '');
  for (const m of body.matchAll(/([^\s=/>]+)(?:\s*=\s*(?:"([^"]*)"|'([^']*)'|([^\s"'>]+)))?/g)) {
    out[m[1].toLowerCase()] = m[2] ?? m[3] ?? m[4] ?? '';
  }
  return out;
}

// The order form's HTML: the <form> that actually holds the order id (oID) or customer ref, to its </form>.
// Not "the first form that looks like id=form": on 3 Nov ~10:15 BP's global-search form started matching that,
// and Tuff-WMS read the search box as the order (its fix cc39bc0, copied here).
function orderFormHtml(html) {
  const holds = /name\s*=\s*["']?(oID|orders_customer_ref)\b/i;
  for (const m of html.matchAll(/<form\b/gi)) {
    const end = html.indexOf('</form>', m.index);
    if (end < 0) break;
    const form = html.slice(m.index, end);
    if (holds.test(form)) return form;
  }
  return null;
}

const SKIP_TYPES = new Set(['submit', 'button', 'image', 'file', 'reset']);

// Every control in document order, with what a browser would send for it.
function controls(form) {
  const list = [];
  const rx = /<input\b[^>]*>|<textarea\b[^>]*>[\s\S]*?<\/textarea>|<select\b[^>]*>[\s\S]*?<\/select>/gi;
  for (const m of form.matchAll(rx)) {
    const raw = m[0];
    const tag = raw.match(/^<[^>]*>/)[0];
    const a = attrs(tag);
    if (!a.name) continue;
    const kind = raw.slice(1, 7).toLowerCase().startsWith('input') ? 'input'
      : raw.slice(1, 9).toLowerCase() === 'textarea' ? 'textarea' : 'select';
    const c = { name: a.name, kind, disabled: 'disabled' in a };
    if (kind === 'input') {
      c.type = (a.type || 'text').toLowerCase();
      c.value = decode(a.value ?? (c.type === 'checkbox' || c.type === 'radio' ? 'on' : ''));
      c.checked = 'checked' in a;
    } else if (kind === 'textarea') {
      c.value = decode(raw.replace(/^<[^>]*>/, '').replace(/<\/textarea>$/i, ''));
    } else {
      c.multiple = 'multiple' in a;
      c.options = [...raw.matchAll(/<option\b[^>]*>([\s\S]*?)(?=<option\b|<\/select>)/gi)].map((o) => {
        const oa = attrs(o[0].match(/^<[^>]*>/)[0]);
        const text = decode(o[1].replace(/<[^>]*>/g, '')).trim();
        return { value: oa.value != null ? decode(oa.value) : text, text, selected: 'selected' in oa };
      });
    }
    list.push(c);
  }
  return list;
}

// What a browser posts: [name, value] pairs in document order.
function submission(ctrls) {
  const pairs = [];
  for (const c of ctrls) {
    if (c.disabled) continue;
    if (c.kind === 'input') {
      if (SKIP_TYPES.has(c.type)) continue;
      if ((c.type === 'checkbox' || c.type === 'radio') && !c.checked) continue;
      pairs.push([c.name, c.value]);
    } else if (c.kind === 'textarea') {
      pairs.push([c.name, c.value]);
    } else {
      const sel = c.options.filter((o) => o.selected);
      if (c.multiple) sel.forEach((o) => pairs.push([c.name, o.value]));
      else if (sel.length) pairs.push([c.name, sel.at(-1).value]);
      else if (c.options.length) pairs.push([c.name, c.options[0].value]);
    }
  }
  return pairs;
}

// BP's yes/no custom fields are three radios: 1 = yes, -1 = no, 0 = not set.
const YESNO = { '1': true, '-1': false, '0': null };
const yesnoValue = (v) => (v === true ? '1' : v === false ? '-1' : '0');

// Read one order screen into the state the rules need, plus the form to re-post.
function parseOrderPage(html) {
  const form = orderFormHtml(html || '');
  if (!form) return null;
  const ctrls = controls(form);
  const pairs = submission(ctrls);
  const one = (name) => { const p = pairs.find((x) => x[0] === name); return p ? p[1] : null; };

  const customFields = {};
  const fieldKinds = {};
  for (const c of ctrls) {
    if (!/^PCF_/.test(c.name)) continue;
    if (c.kind === 'input' && c.type === 'radio') {
      fieldKinds[c.name] = 'yesno';
      if (!(c.name in customFields)) customFields[c.name] = null;
      if (c.checked) customFields[c.name] = c.value in YESNO ? YESNO[c.value] : c.value;
    } else {
      fieldKinds[c.name] = c.kind === 'select' ? 'select' : 'text';
      customFields[c.name] = one(c.name) ?? '';
    }
  }

  const selects = {};
  for (const c of ctrls) if (c.kind === 'select') selects[c.name] = c.options.map((o) => ({ value: o.value, text: o.text }));

  const csrf = (html.match(/<meta[^>]*name=["']__fc_csrf_token["'][^>]*content=["']([^"']+)["']/i)
    || html.match(/<meta[^>]*content=["']([^"']+)["'][^>]*name=["']__fc_csrf_token["']/i)
    || [])[1] || one('__fc_csrf_token') || null;

  // The status picker sits outside the form (#titleOrderStatus); BP's own
  // script copies it into the hidden orders_status input before posting.
  const statusPicker = (html.match(/<select\b[^>]*id=["']titleOrderStatus["'][^>]*>[\s\S]*?<\/select>/i) || [])[0];
  const statuses = statusPicker
    ? controls(statusPicker.replace(/^<select\b/i, '<select name="titleOrderStatus"'))[0].options.map((o) => ({ value: o.value, text: o.text }))
    : null;

  return {
    orderId: one('oID'),
    statusId: one('orders_status'),
    shipMethodId: one('ship_method'),
    customFields,
    fieldKinds,
    selects,
    statuses,
    csrf,
    notifyCustomerTicked: pairs.some((p) => p[0] === 'notify_customer'),
    pairs,
  };
}

// ---- the order rows' VAT ------------------------------------------------------------
// A form holding order rows is NOT safe to re-post as read (3 Nov): the server-read HTML has 0.00 in
// rwTotalTaxUser[] for every row BP calculates VAT on (the page's script fills the real figure in after
// load), so posting it back saves 0.00 as a manual VAT - Tuff-WMS's hub invoicing zeroed 18 orders that
// way. Every real order has rows, so each row's VAT is put back from an API read of the same order,
// matched on row id, and the rows must read back unchanged after the save. (Same as Tuff-WMS bpInvoice.)
const ROW_FIELD = /^(rwTotalTaxUser|itemnet|itemnet4dp|tax_code|listPrice|orders_products_id)\[\]$/;
const hasRows = (page) => !!page && page.pairs.some((p) => ROW_FIELD.test(p[0]));

const num = (v) => Number(v) || 0;
const pence = (v) => Math.round(num(v) * 100);

// {rowId: {net, tax, code}} from an API order, in pence.
function apiRows(order) {
  return Object.fromEntries(Object.entries((order && order.orderRows) || {}).map(([id, r]) =>
    [String(id), { net: pence(r.rowValue?.rowNet?.value), tax: pence(r.rowValue?.rowTax?.value), code: String(r.rowValue?.taxClassId ?? '') }]));
}

// The form's pairs with every row's rwTotalTaxUser[] set to the API's VAT for that row. Throws (nothing
// is sent) unless the form's rows and the API's rows are exactly the same rows, nets and VAT codes.
function withRowTax(pairs, order) {
  const out = pairs.map((p) => [...p]);
  const at = (name) => out.map((p, i) => (p[0] === name ? i : -1)).filter((i) => i >= 0);
  const ids = at('orders_products_id[]'), taxes = at('rwTotalTaxUser[]'), nets = at('itemnet[]'), qtys = at('qty[]'), codes = at('tax_code[]');
  if (!ids.length) throw new Error('no order rows on the order screen - BP has changed it, not saving');
  if (![taxes, nets, qtys, codes].every((a) => a.length === ids.length)) throw new Error("the order screen's rows don't line up (row id / VAT / net / qty counts differ) - not saving");
  const api = apiRows(order), seen = new Set();
  ids.forEach((pos, i) => {
    const id = String(out[pos][1]);
    const row = api[id];
    if (!row) throw new Error(`row ${id} is on the order screen but not in the API - not saving`);
    if (seen.has(id)) throw new Error(`row ${id} is on the order screen twice - not saving`);
    seen.add(id);
    const formNet = num(out[nets[i]][1]) * num(out[qtys[i]][1]);
    if (Math.abs(Math.round(formNet * 100) - row.net) > Math.max(2, num(out[qtys[i]][1]))) {
      throw new Error(`row ${id}: the screen's net ${formNet.toFixed(2)} isn't the API's ${(row.net / 100).toFixed(2)} - not saving`);
    }
    // A different VAT code (T0 export / T20...) would re-rate the row - refuse rather than send it.
    if (row.code && String(out[codes[i]][1]) !== row.code) {
      throw new Error(`row ${id}: the screen's VAT code ${out[codes[i]][1]} isn't the API's ${row.code} - not saving`);
    }
    out[taxes[i]][1] = (row.tax / 100).toFixed(2);
  });
  const missing = Object.keys(api).filter((id) => !seen.has(id));
  if (missing.length) throw new Error(`row(s) ${missing.join(', ')} are in the API but not on the order screen - not saving`);
  return out;
}

// Rows whose net, VAT or VAT code differ between two API reads.
function rowChanges(before, after) {
  const a = apiRows(before), b = apiRows(after);
  return [...new Set([...Object.keys(a), ...Object.keys(b)])].filter((id) => !a[id] || !b[id] || a[id].net !== b[id].net || a[id].tax !== b[id].tax || a[id].code !== b[id].code)
    .map((id) => ({ rowId: id, before: a[id] || null, after: b[id] || null }));
}

// Turn a wanted change into the exact pairs to post. Throws rather than guess.
// changes = { statusId, shipMethodId, customFields: { PCF_X: value }, note }
// apiOrder: the same order read from the API, REQUIRED when the form holds rows (see above).
// planOnly: work out the diff without the rows' VAT - the pairs must not be posted.
function planSave(page, changes = {}, { apiOrder = null, planOnly = false } = {}) {
  if (!page) throw new Error('no order form on the page');
  const pairs = page.pairs.filter((p) => p[0] !== 'notify_customer').map((p) => [...p]);
  const diff = [];
  const setOne = (name, value, from) => {
    const at = pairs.findIndex((p) => p[0] === name);
    if (at < 0) pairs.push([name, value]); else pairs[at][1] = value;
    if (String(from ?? '') !== String(value)) diff.push({ field: name, from: from ?? null, to: value });
  };
  const offered = (list, value, what) => {
    if (list && !list.some((o) => String(o.value) === String(value))) {
      throw new Error(`${what} ${value} is not offered on this order screen`);
    }
  };

  if (changes.statusId != null) {
    offered(page.statuses, changes.statusId, 'status');
    setOne('orders_status', String(changes.statusId), page.statusId);
  }
  if (changes.shipMethodId != null) {
    offered(page.selects.ship_method, changes.shipMethodId, 'shipping method');
    setOne('ship_method', String(changes.shipMethodId), page.shipMethodId);
  }
  for (const [name, value] of Object.entries(changes.customFields || {})) {
    const kind = page.fieldKinds[name];
    if (!kind) throw new Error(`custom field ${name} is not on this order screen`);
    const from = page.customFields[name];
    if (kind === 'yesno') {
      if (![true, false, null].includes(value)) throw new Error(`${name} is yes/no; got ${JSON.stringify(value)}`);
      if (from !== value) diff.push({ field: name, from, to: value });
      const at = pairs.findIndex((p) => p[0] === name);
      if (at >= 0) pairs[at][1] = yesnoValue(value); else pairs.push([name, yesnoValue(value)]);
    } else {
      if (kind === 'select') offered(page.selects[name], value, name);
      setOne(name, String(value ?? ''), from);
    }
  }
  if (changes.note) {
    if (!/^[\x20-\x7e\r\n\t]*$/.test(changes.note)) throw new Error('order notes must be plain ASCII (BP mangles anything else)');
    setOne('note_text', changes.note, '');
  }
  if (page.csrf) setOne('__fc_csrf_token', page.csrf, page.csrf);
  if (planOnly || !hasRows(page)) return { pairs, diff };
  if (!apiOrder) throw new Error('the order screen form holds order rows - re-posting it without their VAT from the API zeroes it, refusing');
  return { pairs: withRowTax(pairs, apiOrder), diff };
}

// After a save: did every change stick? Returns the fields that didn't.
function verifySaved(after, changes) {
  const misses = [];
  if (!after) return [{ field: '(page)', want: 'order screen', got: 'no order form' }];
  if (changes.statusId != null && String(after.statusId) !== String(changes.statusId)) misses.push({ field: 'status', want: String(changes.statusId), got: after.statusId });
  if (changes.shipMethodId != null && String(after.shipMethodId) !== String(changes.shipMethodId)) misses.push({ field: 'ship_method', want: String(changes.shipMethodId), got: after.shipMethodId });
  for (const [name, value] of Object.entries(changes.customFields || {})) {
    const got = after.customFields[name];
    const want = after.fieldKinds[name] === 'yesno' ? value : String(value ?? '');
    if (got !== want) misses.push({ field: name, want, got });
  }
  return misses;
}

const ORDER_URL = (host, id) => `${host}/patt-op.php?scode=invoice&oID=${encodeURIComponent(id)}`;

async function readOrder(orderId, { transport }) {
  const html = await transport.get(ORDER_URL(transport.host, orderId));
  const page = parseOrderPage(html);
  if (!page) throw new Error(`order ${orderId}: no order form on the screen (signed out, or the order doesn't exist)`);
  return page;
}

// Change an order and prove it. Already-true changes are not posted again, so
// a repeat webhook or a sweep repeat is harmless.
// getApiOrder(orderId) -> the API's order (GET /order-service/order/{id}, first result). Needed for any
// order with rows: one API read before the save (the rows' VAT) and one after (rows unchanged). Only an
// order that actually needs changing costs those two calls.
async function updateOrder(orderId, changes, { transport, dryRun = false, getApiOrder = null }) {
  const before = await readOrder(orderId, { transport });
  const real = planSave(before, changes, { planOnly: true }).diff.filter((d) => d.field !== '__fc_csrf_token');
  if (!real.length) return { ok: true, orderId, skipped: 'already set', diff: [] };
  if (dryRun) return { ok: true, orderId, dryRun: true, diff: real };
  const rows = hasRows(before);
  if (rows && !getApiOrder) throw new Error(`order ${orderId} has rows - updateOrder needs getApiOrder to keep their VAT, refusing`);
  const apiBefore = rows ? await getApiOrder(orderId) : null;
  const { pairs } = planSave(before, changes, { apiOrder: apiBefore });   // throws before anything is sent if the rows don't match
  await transport.post(ORDER_URL(transport.host, orderId), pairs);
  const after = await readOrder(orderId, { transport });
  const misses = verifySaved(after, changes);
  if (rows) {
    const changed = rowChanges(apiBefore, await getApiOrder(orderId));
    if (changed.length) {
      const say = changed.map((c) => `row ${c.rowId}: net ${c.before ? (c.before.net / 100).toFixed(2) : '-'} -> ${c.after ? (c.after.net / 100).toFixed(2) : '-'}, VAT ${c.before ? (c.before.tax / 100).toFixed(2) : '-'} -> ${c.after ? (c.after.tax / 100).toFixed(2) : '-'}`).join('; ');
      console.error(`[bpScreen] ${orderId} ROWS CHANGED BY THE SAVE: ${say}`);
      misses.push({ field: '(order rows)', want: 'unchanged', got: say });
    }
  }
  return { ok: misses.length === 0, orderId, diff: real, misses, after };
}

// The real transport: bpWebSession's cookie session for `client`.
async function webTransport(client) {
  const ws = await import('./bpWebSession.js');
  const host = ws.BP_HOST;
  const ingest = async (jar, res, url) => {
    const list = typeof res.headers.getSetCookie === 'function' ? res.headers.getSetCookie() : [];
    for (const raw of list) for (const v of raw.split(/,(?=\s*[a-zA-Z_][a-zA-Z0-9_\-]*=)/)) {
      try { await jar.setCookie(v.trim(), url, { ignoreError: true }); } catch {}
    }
  };
  return {
    host,
    client,
    async get(url) {
      const r = await ws.fetchAuthed(url, { client });
      return r.html || '';
    },
    async post(url, pairs) {
      const { jar } = await ws.getSession(client);
      const fd = new FormData();
      for (const [k, v] of pairs) fd.append(k, v);
      const res = await fetch(url, {
        method: 'POST',
        headers: { Cookie: await ws.getCookieHeader(jar, url), Origin: host, Referer: url },
        body: fd,
        redirect: 'manual',
      });
      await ingest(jar, res, url);
      if (![200, 302, 303].includes(res.status)) throw new Error(`order save answered ${res.status}`);
      return res.status;
    },
  };
}

export { parseOrderPage, planSave, verifySaved, readOrder, updateOrder, webTransport, attrs, controls, submission, hasRows, apiRows, withRowTax, rowChanges };
