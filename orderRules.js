// Our replacement for Brightpearl's automation rules - WATCH ONLY for now.
//
// orderRules.rules.json is our own copy of BP's 60 rules (copied 5 Nov 2026, in BP's priority order, BP's own
// format), because the plan is to delete them from BP. Every 2 minutes this reads the orders BP says were
// created or changed since the last look, runs the WATCHED rules over each one and logs what they would do.
// It never writes to Brightpearl: there is no save code in this file.
//
// BP runs each rule ONCE per order: on 5 Nov four Travelex orders had Name Badges ticked by the rule, set back to
// No by a person once the badges were done, and BP never ticked them again. So ours fires once per order and rule:
// any earlier row for the pair (below) means it has already fired.
//
// What a log row means:
//   would  - first time the rule's conditions match and the order is NOT yet as the rule leaves it. While BP's own
//            rule is still on, BP normally gets there within seconds, so the order then shows up as `done` or `bp`.
//            A `would` that never gets either is a disagreement to look at.
//   done   - the order is already as the rule leaves it and the rule's other conditions match - BP's rule (or a
//            person) did what ours would have. Conditions on the fields the rule itself changes are skipped here,
//            because the change has already moved them (e.g. status New order -> pick/pack/ship).
//   bp     - BP's note "Automation rule '<name>': Updating sales custom field ..." - BP's rule definitely ran.
//            Only custom-field actions leave these notes; status and shipping-method changes don't name the rule.
//
// API cost: 1 order-search + 1 note-search per poll (plus pages), 1 batched order GET per 200 changed orders, and
// 1 note read for each first-time `would` on a rule that leaves notes - about 2 calls a minute.
// Off with ORDER_RULES_WATCH=off.
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const HERE = path.dirname(fileURLToPath(import.meta.url));
const RULES = JSON.parse(fs.readFileSync(path.join(HERE, 'orderRules.rules.json'), 'utf8')).rules;

// The rules being watched first: simple conditions we can read straight off the order.
export const WATCHED = [
  'Bob Custom Fields',
  'Ebay Automation - DPD',
  'Ebay Automation - Royal Mail',
  'Ebay Automation - FedEx',
  'Payment Link Sent Paid',
  'Name Badges',
  'BT Postcode Catcher',
  'International Shipping',
];

const POLL_MS = 2 * 60 * 1000;
const OVERLAP_MS = 60 * 1000;   // re-read the last minute of each window: BP's updatedOn can land a little late

const ids = (c) => (c.options?.ids || []).map(String);
const has = (list, v) => v != null && list.includes(String(v));
const addressText = (d = {}) => [d.addressFullName, d.companyName, d.addressLine1, d.addressLine2, d.addressLine3, d.addressLine4, d.postalCode]
  .filter(Boolean).join(' ').toUpperCase();

// The order's value for a custom field, in the shape the rule's action writes ("1"/"0" for yes/no).
function customValue(order, code) {
  const v = (order.customFields || {})[code];
  if (v === true) return '1';
  if (v === false) return '0';
  if (v && typeof v === 'object') return String(v.id ?? v.value ?? '');
  return v == null ? '' : String(v);
}

// Each condition type: (condition, order) -> true/false. A type missing here makes the rule unsupported.
const CONDITIONS = {
  'order-status': (c, o) => has(ids(c), o.orderStatus?.orderStatusId),
  'channel': (c, o) => has(ids(c), o.assignment?.current?.channelId),
  'payment-status': (c, o) => has(ids(c), o.orderPaymentStatus),
  'stock-allocation-status': (c, o) => has(ids(c), o.allocationStatusCode),
  'fulfilment-status': (c, o) => has(ids(c), o.stockStatusCode),
  'delivery-country': (c, o) => has(ids(c), o.parties?.delivery?.countryId),
  'shipping-method': (c, o) => has(ids(c), o.delivery?.shippingMethodId),
  'created-by': (c, o) => has(ids(c), o.createdById),
  'contains-item': (c, o) => {
    const want = new Set(ids(c).map((s) => s.toUpperCase()));
    return Object.values(o.orderRows || {}).some((r) => want.has(String(r.productSku || '').toUpperCase()));
  },
  'delivery-zip-code': (c, o) => {
    const pc = String(o.parties?.delivery?.postalCode || '').toUpperCase().replace(/\s+/g, '');
    if (!pc) return false;
    // partialMatches = "starts with", as BP does (the BT rule lists BT1..BT89, so BT1 also catching BT10 is harmless)
    return ids(c).some((p) => { const q = p.toUpperCase().replace(/\s+/g, ''); return c.options?.partialMatches ? pc.startsWith(q) : pc === q; });
  },
  'delivery-address': (c, o) => addressText(o.parties?.delivery).includes(String(c.options?.stringContains || '').toUpperCase()),
  'invoiced': (c, o) => Boolean(o.invoices?.[0]?.invoiceReference) === Boolean(c.options?.invoiced),
};

// The order field each action sets, the value it sets it to, and the condition type that reads that field.
function actionTarget(a) {
  const o = a.options || {};
  if (a['action-type'] === 'update-status') return { field: 'status', value: String(o.ids?.[0]), label: o.optionsLabel?.[0], reads: 'order-status' };
  if (a['action-type'] === 'update-shipping-method') return { field: 'shipMethod', value: String(o.ids?.[0]), label: o.optionsLabel?.[0], reads: 'shipping-method' };
  if (a['action-type'] === 'update-custom-field') return { field: `cf:${o.ids?.[0]}`, value: String(o.value ?? ''), label: `${o.ids?.[0]} = ${o.value}`, reads: null };
  return null;   // invoice-order, unallocate-stock: not done by ours yet
}
const currentValue = (field, o) => (field === 'status' ? String(o.orderStatus?.orderStatusId)
  : field === 'shipMethod' ? String(o.delivery?.shippingMethodId)
  : customValue(o, field.slice(3)));

export function unsupported(rule) {
  const missing = rule.conditions.filter((c) => !CONDITIONS[c['condition-type']]).map((c) => c['condition-type']);
  const acts = rule.actions.filter((a) => !actionTarget(a)).map((a) => a['action-type']);
  return [...missing, ...acts];
}

function conditionHolds(c, o) {
  const hit = CONDITIONS[c['condition-type']](c, o);
  return c.predicate === 'IS_NOT' ? !hit : hit;
}

// One rule against one order: null (doesn't apply), or { verdict: 'would'|'done', changes: [...] }.
export function evaluate(rule, order) {
  if (unsupported(rule).length) return null;
  const targets = rule.actions.map(actionTarget);
  const changes = targets.filter((t) => currentValue(t.field, order) !== t.value)
    .map((t) => ({ field: t.field, from: currentValue(t.field, order), to: t.value, label: t.label }));
  if (changes.length) {
    return rule.conditions.every((c) => conditionHolds(c, order)) ? { verdict: 'would', changes } : null;
  }
  // Already as the rule leaves it: did the rule's other conditions hold? Skip the ones on fields it sets.
  const setsFields = new Set(targets.map((t) => t.reads).filter(Boolean));
  const others = rule.conditions.filter((c) => !setsFields.has(c['condition-type']));
  return others.every((c) => conditionHolds(c, order)) ? { verdict: 'done', changes: [] } : null;
}

// All watched rules over one order, in BP's priority order. Each `would` is applied to a copy before the next rule
// looks, the way BP's later rules see the earlier ones' changes.
export function runRules(order, rules = RULES.filter((r) => WATCHED.includes(r.name))) {
  const o = structuredClone(order);
  const out = [];
  for (const rule of rules) {
    const r = evaluate(rule, o);
    if (!r) continue;
    out.push({ rule, ...r });
    for (const ch of r.changes) {
      if (ch.field === 'status') o.orderStatus = { ...o.orderStatus, orderStatusId: Number(ch.to) };
      else if (ch.field === 'shipMethod') o.delivery = { ...o.delivery, shippingMethodId: Number(ch.to) };
      else o.customFields = { ...o.customFields, [ch.field.slice(3)]: ch.to === '1' ? true : ch.to === '0' ? false : ch.to };
    }
  }
  return out;
}

// --- the 24/7 watcher ---------------------------------------------------------------------------------------

function bpApi() {
  const host = `https://${process.env.BRIGHTPEARL_DATACENTER || 'euw1'}.brightpearlconnect.com/public-api/${process.env.BRIGHTPEARL_ACCOUNT_ID}`;
  const headers = { 'brightpearl-app-ref': process.env.BRIGHTPEARL_APP_REF, 'brightpearl-account-token': process.env.BRIGHTPEARL_API_TOKEN };
  return async (p) => {
    for (let attempt = 1; ; attempt++) {
      const r = await fetch(host + p, { headers });
      if ((r.status === 503 || r.status === 429) && attempt < 4) { await new Promise((ok) => setTimeout(ok, 3000 * attempt)); continue; }
      if (!r.ok) throw new Error(`BP GET ${p.slice(0, 80)} -> ${r.status}`);
      return (await r.json()).response;
    }
  };
}

async function changedOrderIds(get, since) {
  const out = [];
  for (let first = 1; ; ) {
    const s = await get(`/order-service/order-search?orderTypeId=1&updatedOn=${encodeURIComponent(since.toISOString().slice(0, 19) + '/')}&pageSize=500&firstResult=${first}`);
    out.push(...s.results.map((r) => r[0]));
    if (!s.metaData.morePagesAvailable) return out;
    first += s.metaData.resultsReturned;
  }
}

// The rule a BP note says ran. Custom-field rules write "Automation rule 'X': Updating ...", shipping and status
// rules write "Updating shipping method to "Y" by Automation with rule "X"".
export const noteRuleName = (text) => String(text || '').match(/^Automation rule '([^']+)'|by Automation with rule "([^"]+)"/)?.slice(1).find(Boolean) || null;

// BP's own rule notes added since `since`: [{ orderId, ruleName }].
async function bpRuleNotes(get, since) {
  const out = [];
  for (let first = 1; ; ) {
    const s = await get(`/order-service/order-note-search?dateAdded=${encodeURIComponent(since.toISOString().slice(0, 19) + '/')}&pageSize=500&firstResult=${first}`);
    const col = s.metaData.columns.map((c) => c.name);
    for (const r of s.results) {
      const n = Object.fromEntries(col.map((k, i) => [k, r[i]]));
      const ruleName = noteRuleName(n.noteText);
      if (ruleName) out.push({ orderId: n.orderId, ruleName });
    }
    if (!s.metaData.morePagesAvailable) return out;
    first += s.metaData.resultsReturned;
  }
}

async function ensureTables(pool) {
  await pool.query(`CREATE TABLE IF NOT EXISTS order_rules_state (k text PRIMARY KEY, v text NOT NULL)`);
  await pool.query(`CREATE TABLE IF NOT EXISTS order_rules_log (
    order_id bigint NOT NULL, rule_id text NOT NULL, rule_name text NOT NULL, verdict text NOT NULL,
    changes jsonb, order_status int, first_seen timestamptz NOT NULL DEFAULT now(), last_seen timestamptz NOT NULL DEFAULT now(),
    seen int NOT NULL DEFAULT 1, PRIMARY KEY (order_id, rule_id, verdict))`);
}

export async function pollOnce({ pool, get = bpApi(), now = new Date() } = {}) {
  await ensureTables(pool);
  const st = (await pool.query(`SELECT v FROM order_rules_state WHERE k = 'cursor'`)).rows[0];
  const since = st ? new Date(new Date(st.v).getTime() - OVERLAP_MS) : new Date(now.getTime() - POLL_MS);
  const orderIds = await changedOrderIds(get, since);
  let logged = 0;
  const log = (orderId, rule, verdict, changes = [], status = null) => pool.query(`INSERT INTO order_rules_log (order_id, rule_id, rule_name, verdict, changes, order_status)
      VALUES ($1,$2,$3,$4,$5,$6)
      ON CONFLICT (order_id, rule_id, verdict) DO UPDATE SET last_seen = now(), seen = order_rules_log.seen + 1`,
    [orderId, rule.id, rule.name, verdict, JSON.stringify(changes), status]).then(() => { logged++; });

  for (const n of await bpRuleNotes(get, since)) {
    const rule = RULES.find((r) => r.name === n.ruleName && WATCHED.includes(r.name));
    if (rule) await log(n.orderId, rule, 'bp');
  }
  for (let i = 0; i < orderIds.length; i += 200) {
    const orders = await get(`/order-service/order/${orderIds.slice(i, i + 200).join(',')}?includeOptional=customFields`);
    for (const o of orders) {
      for (const r of runRules(o)) {
        if (r.verdict === 'would') {
          // Once per order: anything already logged for this order and rule means it has fired.
          const fired = (await pool.query(`SELECT 1 FROM order_rules_log WHERE order_id = $1 AND rule_id = $2 AND verdict <> 'would'`, [o.id, r.rule.id])).rows.length;
          if (fired) continue;
          // Fired before we started watching and someone has since undone it? BP's note says so, and BP won't redo it.
          const s = await get(`/order-service/order-note-search?orderId=${o.id}&pageSize=500`);
          const col = s.metaData.columns.map((c) => c.name).indexOf('noteText');
          if (s.results.some((row) => noteRuleName(row[col]) === r.rule.name)) { await log(o.id, r.rule, 'already'); continue; }
        }
        await log(o.id, r.rule, r.verdict, r.changes, o.orderStatus?.orderStatusId ?? null);
      }
    }
  }
  await pool.query(`INSERT INTO order_rules_state (k, v) VALUES ('cursor', $1) ON CONFLICT (k) DO UPDATE SET v = EXCLUDED.v`, [now.toISOString()]);
  return { since: since.toISOString(), orders: orderIds.length, logged };
}

// Per rule: how many orders ours would have changed, how many were already done, how many BP's notes show it ran
// on, and the disagreements: oursOnly = a `would` with no `done`/`bp` after `graceMin` minutes; bpOnly = BP's rule
// ran (its note) but ours never matched the order.
export async function watchReport(pool, { hours = 24, graceMin = 15 } = {}) {
  await ensureTables(pool);
  const rows = (await pool.query(`
    SELECT w.rule_name, w.order_id, w.verdict, w.changes, w.first_seen,
           (SELECT array_agg(o.verdict) FROM order_rules_log o WHERE o.order_id = w.order_id AND o.rule_id = w.rule_id AND o.verdict <> w.verdict) AS other
    FROM order_rules_log w
    WHERE w.first_seen > now() - ($1 || ' hours')::interval`, [String(hours)])).rows;
  const blank = () => ({ would: 0, done: 0, bp: 0, already: 0, agreed: 0, oursOnly: [], bpOnly: [] });
  const rules = Object.fromEntries(WATCHED.map((n) => [n, blank()]));
  const late = (r) => Date.now() - new Date(r.first_seen).getTime() > graceMin * 60000;
  for (const r of rows) {
    const s = rules[r.rule_name] || (rules[r.rule_name] = blank());
    const other = r.other || [];
    s[r.verdict]++;
    if (r.verdict === 'would') {
      if (other.includes('bp')) continue; // counted on the bp row
      if (other.length) s.agreed++;
      else if (late(r)) s.oursOnly.push({ orderId: Number(r.order_id), since: r.first_seen, changes: r.changes });
    } else if (r.verdict === 'bp') {
      if (other.length) s.agreed++;
      else if (late(r)) s.bpOnly.push({ orderId: Number(r.order_id), since: r.first_seen });
    }
  }
  const last = (await pool.query(`SELECT v FROM order_rules_state WHERE k = 'cursor'`)).rows[0]?.v || null;
  return { hours, graceMin, lastPoll: last, watched: WATCHED, rules };
}

export function registerOrderRules(app, { getPool }) {
  for (const n of WATCHED) if (!RULES.some((r) => r.name === n)) console.error(`[order-rules] watched rule "${n}" isn't in orderRules.rules.json`);
  for (const r of RULES.filter((x) => WATCHED.includes(x.name))) {
    const u = unsupported(r);
    if (u.length) console.error(`[order-rules] "${r.name}" can't be watched yet: ${u.join(', ')}`);
  }

  app.get('/api/order-rules/watch', async (req, res) => {
    const pool = getPool();
    if (!pool) return res.status(503).json({ error: 'no database' });
    try {
      res.json(await watchReport(pool, { hours: Math.min(Number(req.query.hours) || 24, 24 * 14) }));
    } catch (e) { res.status(500).json({ error: e.message }); }
  });

  if (String(process.env.ORDER_RULES_WATCH || 'on').toLowerCase() === 'off') {
    console.log('[order-rules] watcher off (ORDER_RULES_WATCH=off)');
    return;
  }
  let running = false;
  const tick = async () => {
    const pool = getPool();
    if (!pool || running) return;
    running = true;
    try {
      const r = await pollOnce({ pool });
      if (r.logged) console.log(`[order-rules] ${r.orders} changed orders since ${r.since}, ${r.logged} rule results logged (watch only)`);
    } catch (e) {
      console.error('[order-rules] poll failed:', e.message);
    } finally { running = false; }
  };
  setTimeout(tick, 30 * 1000);
  setInterval(tick, POLL_MS);
}
