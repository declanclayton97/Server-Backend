// Delete a LINE from a Brightpearl order.
//
// Why this exists: the public API cannot remove a row from a PAID order — it returns
// ORDC-053 "Order has payments" (and ORDC-108 "Order is a parent order" once invoiced),
// and every eBay order arrives paid. The web UI can, but not through any endpoint: its
// deleteRow() only removes the <tr> from the DOM, and the change is persisted by
// re-submitting the WHOLE order form without that line. So that is what this does,
// reusing the authenticated session in bpWebSession.js.
//
// THE DANGEROUS PART. Line fields come in two shapes:
//   keyed      ids[2042135], reserved[2042135]        - tied to the order-row id
//   positional sku[], qty[], details[], itemnet[] ... - the Nth entry IS the Nth line
// Removing a line means dropping its keyed fields AND the Nth element of every per-line
// array. Miss one array and every column below shears up by a row — the order still
// saves, silently wrong. So a name is only treated as per-line when TWO independent
// tests agree: it appears inside a detailsTr block, and it occurs exactly once per line.
// Anything failing either test is passed through untouched.
//
// Dry run by default. Nothing is posted unless dryRun:false is passed explicitly.

import { getSession, getCookieHeader, BP_HOST } from './bpWebSession.js';

const BROWSER_HEADERS = {
  'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
  Accept: 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
  'Accept-Language': 'en-GB,en;q=0.9',
};

const decodeHtml = (s) => String(s || '')
  .replace(/&amp;/g, '&').replace(/&lt;/g, '<').replace(/&gt;/g, '>').replace(/&quot;/g, '"')
  .replace(/&#0?39;/g, "'")
  .replace(/&#x([0-9a-f]+);/gi, (_, h) => String.fromCharCode(parseInt(h, 16)))
  .replace(/&#(\d+);/g, (_, d) => String.fromCharCode(Number(d)));

const looksLikeLoginPage = (html) => /name=["']email_address["']/i.test(html)
  || /<title>Brightpearl - Login<\/title>/i.test(html);

// Every submittable field of the order form, in document order. Skips submit/button/
// file/reset inputs and unchecked checkboxes/radios, exactly as a browser would.
function parseOrderForm(html) {
  const idx = html.indexOf('name="orders_customer_ref"');
  if (idx < 0) return null;
  const start = html.lastIndexOf('<form', idx);
  const end = html.indexOf('</form>', idx);
  if (start < 0 || end < 0) return null;
  const form = html.slice(start, end);
  const fields = [];
  for (const m of form.matchAll(/<input\b[^>]*>/gi)) {
    const tag = m[0];
    const name = (tag.match(/\bname\s*=\s*"([^"]*)"/i) || [])[1];
    if (!name) continue;
    const type = ((tag.match(/\btype\s*=\s*"([^"]*)"/i) || [])[1] || 'text').toLowerCase();
    if (['submit', 'button', 'image', 'file', 'reset'].includes(type)) continue;
    if ((type === 'checkbox' || type === 'radio') && !/\bchecked\b/i.test(tag)) continue;
    fields.push([name, decodeHtml((tag.match(/\bvalue\s*=\s*"([^"]*)"/i) || [, ''])[1])]);
  }
  for (const m of form.matchAll(/<textarea\b[^>]*>([\s\S]*?)<\/textarea>/gi)) {
    const name = (m[0].match(/\bname\s*=\s*"([^"]*)"/i) || [])[1];
    if (name) fields.push([name, decodeHtml(m[1])]);
  }
  for (const m of form.matchAll(/<select\b[^>]*>([\s\S]*?)<\/select>/gi)) {
    const name = (m[0].match(/\bname\s*=\s*"([^"]*)"/i) || [])[1];
    if (!name) continue;
    const opts = [...m[1].matchAll(/<option\b([^>]*)>/gi)];
    const sel = opts.find((o) => /\bselected\b/i.test(o[1])) || opts[0];
    fields.push([name, decodeHtml(sel ? (sel[1].match(/\bvalue\s*=\s*"([^"]*)"/i) || [, ''])[1] : '')]);
  }
  return { fields, formHtml: form };
}

// The form HTML sliced into one chunk per order line. The final chunk over-reaches to
// the end of the form, which is why membership alone never decides "per-line".
function splitDetailRows(formHtml) {
  const starts = [];
  const rx = /<tr\b[^>]*\bclass\s*=\s*"[^"]*\bdetailsTr\b[^"]*"[^>]*>/gi;
  let m;
  while ((m = rx.exec(formHtml)) !== null) starts.push(m.index);
  return starts.map((s, i) => formHtml.slice(s, i + 1 < starts.length ? starts[i + 1] : formHtml.length));
}

/** Work out exactly which fields removing this line implies. Pure — touches nothing. */
export function planRowDeletion(html, orderRowId) {
  const parsed = parseOrderForm(html);
  if (!parsed) throw new Error('order form not found on the page');
  const rows = splitDetailRows(parsed.formHtml);
  if (!rows.length) throw new Error('no order lines found in the form');

  const keyRx = new RegExp(`name="(?:ids|reserved)\\[${orderRowId}\\]"`);
  const rowIndex = rows.findIndex((r) => keyRx.test(r));
  if (rowIndex < 0) throw new Error(`order row ${orderRowId} is not one of the ${rows.length} lines on this order`);

  const inRows = new Set();
  for (const r of rows) for (const m of r.matchAll(/\bname\s*=\s*"([^"]+)"/gi)) inRows.add(m[1]);

  const counts = {};
  for (const [n] of parsed.fields) counts[n] = (counts[n] || 0) + 1;

  // both tests must agree — see the header comment
  const perLine = Object.keys(counts).filter((n) => n.endsWith('[]') && counts[n] === rows.length && inRows.has(n));
  const keyedRx = new RegExp(`^(?:ids|reserved)\\[${orderRowId}\\]$`);

  const seen = {}, kept = [], dropped = [];
  for (const [n, v] of parsed.fields) {
    if (keyedRx.test(n)) { dropped.push([n, v]); continue; }
    if (perLine.indexOf(n) !== -1 && ((seen[n] = (seen[n] || 0) + 1) - 1) === rowIndex) { dropped.push([n, v]); continue; }
    kept.push([n, v]);
  }
  // Arrays that look per-line but sit outside the rows are where a shear would come
  // from, so surface them rather than silently deciding.
  const suspicious = Object.keys(counts).filter((n) => n.endsWith('[]') && counts[n] === rows.length && !inRows.has(n));

  return { fields: parsed.fields, kept, dropped, perLine, suspicious, rowCount: rows.length, rowIndex };
}

/**
 * Remove one line from an order by re-submitting the form without it.
 * dryRun defaults to TRUE — it reports the plan and posts nothing.
 */
export async function deleteOrderRow(orderId, orderRowId, { client, dryRun = true } = {}) {
  const pageUrl = `${BP_HOST}/patt-op.php?scode=invoice&oID=${encodeURIComponent(orderId)}`;
  for (let attempt = 0; attempt < 2; attempt++) {
    const session = client ? await getSession(client) : await getSession();
    const cookie = await getCookieHeader(session.jar, pageUrl);
    const pageRes = await fetch(pageUrl, { headers: { ...BROWSER_HEADERS, Cookie: cookie }, redirect: 'manual' });
    const html = await pageRes.text();
    if (looksLikeLoginPage(html)) {
      if (attempt === 0) continue;
      throw new Error('not authenticated to the Brightpearl web UI');
    }

    const plan = planRowDeletion(html, orderRowId);
    const summary = {
      orderId, orderRowId, client: client || '(default/live)',
      lines: plan.rowCount, deletingLineIndex: plan.rowIndex,
      fieldsBefore: plan.fields.length, fieldsAfter: plan.kept.length,
      perLineFieldNames: plan.perLine,
      arraysNotTreatedAsPerLine: plan.suspicious,
      dropping: plan.dropped.map(([n, v]) => `${n}=${String(v).slice(0, 40)}`),
    };
    if (plan.rowCount < 2) throw new Error('refusing: an order must keep at least one line');
    if (dryRun) return { dryRun: true, ...summary };

    // The form's __fc_csrf_token input is empty in the raw HTML; the page JS copies it
    // from the <meta> at submit time, so do the same.
    const token = (html.match(/name=["']__fc_csrf_token["'][^>]*content=["']([^"']+)["']/i)
      || html.match(/content=["']([^"']+)["'][^>]*name=["']__fc_csrf_token["']/i) || [])[1];
    const fd = new FormData();
    for (const [k, v] of plan.kept) fd.append(k, v);
    if (token) fd.set('__fc_csrf_token', token);
    // The form carries a HIDDEN submit, <input type="submit" name="submit_form" value="1">,
    // and BP only processes the save when it is present. parseOrderForm skips submit inputs
    // (a browser only sends the one that was clicked), so without this the POST returns a
    // perfectly happy 302 and changes nothing — which is exactly what it did first time.
    fd.set('submit_form', '1');

    const postCookie = await getCookieHeader(session.jar, pageUrl);
    const res = await fetch(pageUrl, {
      method: 'POST',
      headers: { ...BROWSER_HEADERS, Cookie: postCookie, Origin: BP_HOST, Referer: pageUrl,
        'Sec-Fetch-Dest': 'document', 'Sec-Fetch-Mode': 'navigate', 'Sec-Fetch-Site': 'same-origin', 'Sec-Fetch-User': '?1' },
      body: fd, redirect: 'manual',
    });
    return { ok: [200, 302].includes(res.status), status: res.status, location: res.headers.get('location'), ...summary };
  }
  return { ok: false, error: 'session expired' };
}
