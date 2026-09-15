// Which line stopped the order — tested against REAL rows captured from the live error log
// (blockedLines.fixtures.json, 2026-09-14), not invented ones. The shapes differ per supplier and
// several carry their detail as JSON inside the message, so fixtures are the only honest test.
//
// The failure that matters most here is a FALSE one. A wrong SKU sends someone hunting a product
// that was never the problem, so the "must yield nothing" cases below are as important as the rest.
import { readFileSync } from 'node:fs';
import { extractBlockedLines } from './blockedLines.js';

const rows = JSON.parse(readFileSync(new URL('./blockedLines.fixtures.json', import.meta.url), 'utf8'));
const find = (supplier, step, msgPart) => rows.find((r) => r.supplier === supplier && r.step === step
  && (!msgPart || String(r.message).includes(msgPart)));
const skus = (row) => extractBlockedLines(row).map((l) => l.sku);

let pass = true;
const check = (name, cond) => { console.log((cond ? 'PASS  ' : 'FAIL  ') + name); if (!cond) pass = false; };

// ── structured context ──────────────────────────────────────────────────────
const fri = extractBlockedLines(find('FRISTADS', 'out-of-stock-dropped'));
check('Fristads drop names the item',        fri.length === 1 && fri[0].sku === '125949-171-406');
check('…with its size and quantity',         fri[0].size === 'Medium' && fri[0].qty === 1);
check('…its name',                           /FLAME HIGH VIS COVERALL/.test(fri[0].name || ''));
check('…and what they can supply',           fri[0].avail === 0 && fri[0].deldate === '2026-10-01');

const pen = extractBlockedLines(find('PENCARRIE', 'resolve'));
check('PenCarrie unresolved objects',        pen.length === 3 && pen[0].sku === '152818');
check('…carry size and product name',        pen[0].size === 'Medium' && /RG139/.test(pen[0].name || ''));

const sn = extractBlockedLines(find('SNICKERS', 'checkout', '32235804046'));
check('Snickers missing line',               sn.length === 1 && sn[0].sku === '32235804046');
check('…with the qty wanted and a reason',   sn[0].qty === 1 && /not in basket/.test(sn[0].reason || ''));

check('Helly Hansen unresolved strings',     skus(find('HELLY HANSEN', 'preflight')).join() === '78359_991_42');

// Chadwick: the offender plus the lines its abort took down with it.
const ch = skus(find('CHADWICK', 'checkout', 'expected 7'));
check('Chadwick names the rejected code',    ch.includes('CT894-35-A-S'));
check('…and the collateral lines',           ch.filter((s) => s.startsWith('925-')).length === 4);

// ── detail buried in the message ────────────────────────────────────────────
const frc = extractBlockedLines(find('FRISTADS', 'cart'));
check('JSON inside a message is read',       frc.length === 3);
check('…with the portal\'s own reason',      /portal rejected/.test(frc[0].reason || ''));

check('Sterling code followed by prose',     skus(find('STERLING', 'resolve')).join() === '5063777009268');
check('Helly Hansen trailing code list',     skus(find('HELLY HANSEN', 'checkout')).length === 3);

// Sterling's worker identifies by style + size, never a SKU — without that, its failures (the most
// common resolution failures we get) would yield nothing at all.
const st = extractBlockedLines(find('STERLING', 'checkout'));
check('Sterling style+size is usable',       st.length === 1 && /Sudbury/.test(st[0].sku) && st[0].size === 'L31W36');

// ── must yield NOTHING: not every failure is a line failure ─────────────────
check('an auth failure names no item',       skus(find('SCRUFFS', 'unknown')).length === 0);
check('an empty storefront cart does not',   skus(find('BLAKLADER', 'checkout')).length === 0);
check('a bad request date does not',         skus(find('CHADWICK', 'checkout', 'Invalid Request Date')).length === 0);

// The two false positives this guard exists for, both seen before it was added.
const snFull = find('SNICKERS', 'checkout', 'The Cart contains');
check('a price in scraped page text is NOT an item', !skus(snFull).some((s) => /^\d+\.\d\d$/.test(s) || s === '084.60'));
check('…that row yields nothing at all',     skus(snFull).length === 0);
const pc = find('PENCARRIE', 'checkout');
check('our own order id is NOT an item',     !skus(pc).includes('TUWO_TW486420'));
check('…that row yields nothing at all',     skus(pc).length === 0);

// Never throws, whatever it is handed.
for (const bad of [null, {}, { message: null }, { context: null }, { context: { dropped: [{}] } }]) {
  try { extractBlockedLines(bad); } catch { check('survives ' + JSON.stringify(bad), false); }
}
check('survives malformed rows', true);

// Portwest drops a line the cart refused and names the quantity `want`, not `qty` — SO 487469
// (CD883DKR40) went eight days unnoticed because nothing raised this at all.
const pw = extractBlockedLines({ supplier: 'PORTWEST', step: 'customer-line-dropped', severity: 'error',
  message: 'Portwest would not take 1 line(s); they were removed from PO#489300',
  context: { poId: 489300, dropped: [{ sku: 'CD883DKR40', want: 1 }],
             droppedForCustomers: [{ sku: 'CD883DKR40', want: 1, soIds: ['487469'] }] } });
check('a Portwest dropped line is surfaced',  pw.some((l) => l.sku === 'CD883DKR40'));
check('…with its quantity, from want',        pw.find((l) => l.sku === 'CD883DKR40').qty === 1);
check('…and is not duplicated per shape',     pw.filter((l) => l.sku === 'CD883DKR40').length === 1);

// A PO number used to EXPLAIN a refusal is not a third item to go and find. Chadwick, 15 Sept.
const cw = extractBlockedLines({ supplier: 'CHADWICK', step: 'checkout',
  message: 'Chadwick refused 2 line(s) a customer is waiting for — NOT ordering without them: '
    + 'TB150922148 x1, ML110722012 x1. Check the item code against their catalogue '
    + '(a Brightpearl SKU carrying a stray "CT" prefix did this on PO 488574).',
  context: { poId: 489373 } });
check('the two refused codes are found',        ['TB150922148','ML110722012'].every((c) => cw.some((l) => l.sku === c)));
check('…and the PO in the aside is NOT an item', !cw.some((l) => l.sku === '488574'));
check('…so exactly two lines come back',        cw.length === 2);

// A substituted line must read as a substitution, not a shortfall. Chadwick PO 489373.
const sub = extractBlockedLines({ supplier: 'CHADWICK', step: 'checkout',
  message: 'basket does not match what was sent (39 line(s), expected 40) — NOT ordering.',
  context: { poId: 489373, response: { ok: false, step: 'basket',
    missing: [{ sku: '835-39-Y-XL', qty: 3 }],
    wrongQty: [{ sku: '835-39-A-XL', want: 4, got: 7 }],
    merged: [{ from: '835-39-Y-XL', into: '835-39-A-XL', qty: 3, nowAt: 7, asked: 4 }] } } });
const youth = sub.find((l) => l.sku === '835-39-Y-XL');
check('the substituted line is surfaced',       !!youth);
check('…and reads as a SUBSTITUTION',          /SUBSTITUTED/.test(youth.reason || ''));
check('…naming what it was merged into',       /835-39-A-XL/.test(youth.reason || ''));
check('…and is listed once, not twice',        sub.filter((l) => l.sku === '835-39-Y-XL').length === 1);
check('the inflated line is surfaced too',     sub.some((l) => l.sku === '835-39-A-XL'));

console.log(pass ? '\nALL PASS' : '\nFAILURES');
process.exit(pass ? 0 : 1);
