// PCF_SUPPLIER tag parsing: splitting suppliers, and reading a parenthetical scope.
//
// Asked for on 2026-09-14: "RALAWISE / PENCARRIE (ONLY K241DKN6, K241DKN8, K241DKN12, 157619)" —
// four named items from PenCarrie, the rest from Ralawise. Three things were in the way, and the
// first two broke tags that already existed.
import { splitOutsideBrackets, parseTagScope, isCodeLikeTerm, rowMatchesTerm } from './purchasingAuto.js';

let pass = true;
const check = (name, cond) => { console.log((cond ? 'PASS  ' : 'FAIL  ') + name); if (!cond) pass = false; };
// productOptions is a plain object keyed by option name — optValue does Object.keys(opts) over it,
// not an array walk. Getting this wrong makes every colour/size term silently unmatchable.
const row = (sku, name, productOptions = {}) => ({
  productSku: sku, productName: name, productOptions, quantity: { magnitude: '1' },
});

// ── 1. separators inside a bracket are not separators ───────────────────────
// A two-tone colour carries a slash and this catalogue is full of them (RS237-BK/RD, LV873-BK/WH).
// Splitting blind tore the note in half and the supplier stopped matching its own key — the order
// was skipped for that supplier with no error, and the tag looked perfectly correct in Brightpearl.
check('slash inside a scope does not split the tag',
  JSON.stringify(splitOutsideBrackets('PENCARRIE (RS237 BK/RD ONLY)', '/')) === '["PENCARRIE (RS237 BK/RD ONLY)"]');
check('slash BETWEEN suppliers still splits',
  JSON.stringify(splitOutsideBrackets('RALAWISE / PENCARRIE (LV873 BK/WH ONLY)', '/')) === '["RALAWISE","PENCARRIE (LV873 BK/WH ONLY)"]');
check('comma inside a scope does not split alternatives',
  JSON.stringify(splitOutsideBrackets('PENCARRIE (K241DKN6, K241DKN8)', ',')) === '["PENCARRIE (K241DKN6, K241DKN8)"]');
check('comma BETWEEN alternatives still splits',
  JSON.stringify(splitOutsideBrackets('PENCARRIE, RALAWISE, PRESTIGE', ',')) === '["PENCARRIE","RALAWISE","PRESTIGE"]');
check('unclosed bracket does not swallow the rest',
  splitOutsideBrackets('A / B (oops', '/').length === 2);

// ── 2. the scope keeps its comma structure ──────────────────────────────────
const list = parseTagScope('PENCARRIE (ONLY K241DKN6, K241DKN8, K241DKN12, 157619)');
check('four items parse as four groups',      list.groups.length === 4);
check('…ONLY is noise wherever it sits',      !list.terms.includes('ONLY'));
check('…and all four terms survive',          list.terms.join() === 'K241DKN6,K241DKN8,K241DKN12,157619');

const narrow = parseTagScope('PENCARRIE (RG165 NAVY, M X1 ONLY)');
check('one narrowed item keeps its groups',   JSON.stringify(narrow.groups) === '[["RG165","NAVY"],["M"]]');
check('…and its quantity cap',                narrow.qty === 1);

// ── 3. the gate that decides list-vs-narrowed ───────────────────────────────
// Both readings are spelled the same way, so the list reading is allowed ONLY when there are 2+
// groups and EVERY group names a product code. This is the rule that stops "(RG165 NAVY, M X1)"
// widening into "every medium on the order" — which would buy the wrong things.
const gate = (s) => (s.groups || []).length > 1 && s.groups.every((g) => g.some(isCodeLikeTerm));
check('four product codes → list reading allowed',  gate(list) === true);
check('RG165 NAVY, M X1 → list reading REFUSED',    gate(narrow) === false);
check('BACK ORDER → refused',                       gate(parseTagScope('HELLY HANSEN (BACK ORDER)')) === false);
check('a single code is not a list',                gate(parseTagScope('PENCARRIE (JC020 ONLY)')) === false);
check('numeric SKU counts as a code',               isCodeLikeTerm('157619') === true);
check('a size does not',                            isCodeLikeTerm('M') === false);
check('a colour does not',                          isCodeLikeTerm('NAVY') === false);

// ── 4. row selection, using the real matcher ────────────────────────────────
// NOTE: the decision itself lives inline in gatherLiveDemand and cannot be called directly, so the
// rule is restated here. That makes this a check on INTENT, not on that code path — the pieces
// below (matcher, parser, gate) are the real ones.
const rows = [
  row('K241DKN6', 'Kariban Sweatshirt Dark Navy 6'),
  row('K241DKN8', 'Kariban Sweatshirt Dark Navy 8'),
  row('K241DKN12', 'Kariban Sweatshirt Dark Navy 12'),
  row('157619', 'Regatta Softshell'),
  row('TR010-BLK-M', 'Tridri Performance Tee', { Colour: 'Black', Size: 'Medium' }),
  row('UC301-NVY-L', 'Uneek Polo', { Colour: 'Navy', Size: 'Large' }),
];
const andRows = (s) => rows.filter((r) => s.terms.every((t) => rowMatchesTerm(r, t)));
const orRows = (s) => rows.filter((r) => s.groups.some((g) => g.every((t) => rowMatchesTerm(r, t))));

check('AND reading matches nothing (no row is all four)', andRows(list).length === 0);
const picked = orRows(list).map((r) => r.productSku).sort().join();
check('list reading picks exactly the four named',        picked === '157619,K241DKN12,K241DKN6,K241DKN8');
check('…and leaves the Ralawise/Uneek rows alone',        !orRows(list).some((r) => /TR010|UC301/.test(r.productSku)));

// The hazard the gate exists to prevent, stated as a test: if the list reading were allowed here it
// would drag in an unrelated medium row.
const risky = parseTagScope('PENCARRIE (TR010 BLACK, M ONLY)');
check('narrowed note: AND picks the one right row',  andRows(risky).map((r) => r.productSku).join() === 'TR010-BLK-M');
check('…and the gate refuses to widen it',           gate(risky) === false);

// ── A ROW KEEPS THE CODE IT WAS RAISED WITH ──────────────────────────────────
// Brightpearl stamps the SKU onto the order row at creation; renaming the product never rewrites
// it. Invisible until a bulk SKU migration runs — then open orders carry stale codes while tags
// are written with the new ones. SO 485033 (2026-09-15): tag "RX500F NAV XS", row still reading
// ML271019011, product long since renamed RX500F-NAV-XS. altCodes carries the current name.
const frozenRow = {
  productSku: 'ML271019011',
  productName: "RX500F Women's Soft Shell Jacket - Navy-8",
  productOptions: { 1: { optionName: 'Colour', optionValue: 'Navy' }, 2: { optionName: 'Size', optionValue: 'XS - 8' } },
};
const LIVE_SKU = ['RX500F-NAV-XS'];
check('the frozen row matches RX500F by name',   rowMatchesTerm(frozenRow, 'RX500F'));
check('…but NAV cannot match Navy on its own',   !rowMatchesTerm(frozenRow, 'NAV'));
check('…so the group fails, as it did live',     !['RX500F', 'NAV', 'XS'].every((t) => rowMatchesTerm(frozenRow, t)));
check('NAV matches through the current sku',     rowMatchesTerm(frozenRow, 'NAV', LIVE_SKU));
check('XS matches through the current sku',      rowMatchesTerm(frozenRow, 'XS', LIVE_SKU));
check('…so the whole group matches',             ['RX500F', 'NAV', 'XS'].every((t) => rowMatchesTerm(frozenRow, t, LIVE_SKU)));
const blouseRow = {
  productSku: 'K241DKN6',
  productName: "K241 Women's Short Sleeve Blouse - Navy-6",
  productOptions: { 1: { optionName: 'Colour', optionValue: 'Dark Navy' }, 2: { optionName: 'Size', optionValue: '6' } },
};
check('a different row is not widened into the scope', !rowMatchesTerm(blouseRow, 'RX500F', ['K241-DKN-6']));
check('an empty alt list behaves as before',     rowMatchesTerm(frozenRow, 'RX500F', []) === rowMatchesTerm(frozenRow, 'RX500F'));
check('undefined alts are safe',                 rowMatchesTerm(frozenRow, 'RX500F', undefined) === true);

console.log(pass ? '\nALL PASS' : '\nFAILURES');
process.exit(pass ? 0 : 1);
