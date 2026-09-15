// triageSignature — what makes two purchasing failures "the same failure", so a bot is not sent
// at one it has already failed to fix. The cooldown/max-fires rule is pinned alongside it.
import { triageSignature } from './purchasingSchedule.js';
import { extractBlockedLines } from './blockedLines.js';

let pass = true;
const check = (name, ok) => { console.log((ok ? 'PASS  ' : 'FAIL  ') + name); if (!ok) pass = false; };

const CHADWICK = {
  supplier: 'CHADWICK', step: 'checkout', severity: 'error',
  message: 'Chadwick refused 2 line(s) a customer is waiting for — NOT ordering without them: '
    + 'TB150922148 x1, ML110722012 x1. Check the item code against their catalogue '
    + '(a Brightpearl SKU carrying a stray "CT" prefix did this on PO 488574).',
  context: { poId: 489373 },
};
const sigOf = (row) => triageSignature(row, extractBlockedLines(row));

// ── the same failure ─────────────────────────────────────────────────────────
const a = sigOf(CHADWICK);
const retry = sigOf({ ...CHADWICK, context: { poId: 489401 } });   // tomorrow, new PO, same codes
check('the same refused codes are the same failure', a === retry);

const qtyChanged = sigOf({ ...CHADWICK, message: CHADWICK.message.replace('x1, ML110722012 x1', 'x3, ML110722012 x2') });
check('a quantity change is still the same failure', a === qtyChanged);

// ── a different failure ──────────────────────────────────────────────────────
const other = sigOf({ ...CHADWICK, message: CHADWICK.message.replace('TB150922148', 'ZZ999999999') });
check('a different refused code is a different failure', a !== other);
check('a different supplier is a different failure',    a !== sigOf({ ...CHADWICK, supplier: 'CASTLE' }));
check('a different step is a different failure',        a !== sigOf({ ...CHADWICK, step: 'resolve' }));

// ── failures that name no item fall back to the message, numbers masked ──────
const noItem = { supplier: 'SCRUFFS', step: 'unknown', message: 'auth failed: 401 after 3 attempts', context: { poId: 1 } };
const noItem2 = { ...noItem, message: 'auth failed: 401 after 7 attempts', context: { poId: 2 } };
check('a failure naming no item still gets a signature', !!sigOf(noItem));
check('…and a changed count is the same failure',        sigOf(noItem) === sigOf(noItem2));
check('…but different wording is not',                   sigOf(noItem) !== sigOf({ ...noItem, message: 'connection reset' }));

// ── the skip rule ────────────────────────────────────────────────────────────
// Mirrors triageAlreadyChasing: cooldown first, then a hard stop, and a resolved one starts over.
const COOLDOWN = 20, MAX = 3;
const decide = ({ fires, hoursAgo, everHandled }) => {
  if (!fires) return 'fire';
  if (everHandled) return 'fire';
  if (hoursAgo < COOLDOWN) return 'skip';
  if (fires >= MAX) return 'skip';
  return 'fire';
};
check('never seen before → fire',                decide({ fires: 0 }) === 'fire');
check('same failure an hour ago → skip',         decide({ fires: 1, hoursAgo: 1 }) === 'skip');
check('…which is what the 3 daily retries hit',  decide({ fires: 1, hoursAgo: 0.2 }) === 'skip');
check('a day later, 2nd attempt → fire',         decide({ fires: 1, hoursAgo: 24 }) === 'fire');
check('a day later, 3rd attempt → fire',         decide({ fires: 2, hoursAgo: 24 }) === 'fire');
check('after 3 unresolved fires → stop',         decide({ fires: 3, hoursAgo: 48 }) === 'skip');
check('…and stays stopped',                      decide({ fires: 9, hoursAgo: 200 }) === 'skip');
check('resolved before, now back → fire again',  decide({ fires: 5, hoursAgo: 48, everHandled: true }) === 'fire');

// ── never throws ─────────────────────────────────────────────────────────────
for (const bad of [{}, { supplier: null, message: null }, { message: undefined }]) {
  try { triageSignature(bad, null); } catch (e) { check('survives ' + JSON.stringify(bad) + ': ' + e.message, false); }
}
check('survives malformed rows', true);

console.log(pass ? '\nALL PASS' : '\nFAILURES');
process.exit(pass ? 0 : 1);
