// "(+ 1x SKU)" on a PCF_SUPPLIER tag: an ADDITION for that sales order.
//
// "(ONLY 1x T53545)" can only narrow what an order's rows already ask for, and a shipped order's
// rows ask for nothing — SO 479368 sat tagged and unordered that way on 2026-09-21. "+" says
// "order this as well, for this SO", and must never be mistaken for a scope.
import { parseTagPlus, parseTagScope } from './purchasingAuto.js';

let pass = true;
const check = (name, ok) => { console.log((ok ? 'PASS  ' : 'FAIL  ') + name); if (!ok) pass = false; };
const eq = (a, b) => JSON.stringify(a) === JSON.stringify(b);

check('(+ 1x SKU)',                    eq(parseTagPlus('SCRUFFS (+ 1x T53545)'), { sku: 'T53545', qty: 1 }));
check('(+ SKU) defaults to one',       eq(parseTagPlus('SCRUFFS (+ T53545)'), { sku: 'T53545', qty: 1 }));
check('(+SKU x2) qty after',           eq(parseTagPlus('SCRUFFS (+T53545 x2)'), { sku: 'T53545', qty: 2 }));
check('multiplication sign',           eq(parseTagPlus('SCRUFFS (+ 2 × T53545)'), { sku: 'T53545', qty: 2 }));
check('hyphenated Fristads code',      eq(parseTagPlus('FRISTADS (+ 3 x 119627-271-407)'), { sku: '119627-271-407', qty: 3 }));
check('lower case is upper-cased',     eq(parseTagPlus('scruffs (+ t53545)'), { sku: 'T53545', qty: 1 }));
check('ONLY is not a plus',            parseTagPlus('SCRUFFS (ONLY 1x T53545)') === null);
check('a bare supplier is not a plus', parseTagPlus('PORTWEST') === null);
check('an annotation is not a plus',   parseTagPlus('HELLY HANSEN (BACK ORDER)') === null);
check('a scope is not a plus',         parseTagPlus('PENCARRIE (RG165 NAVY, M X1 ONLY)') === null);
check('zero quantity is refused',      parseTagPlus('SCRUFFS (+ 0x T53545)') === null);

// and the scope parser must leave a "+" note alone, while still reading a real scope
check('scope ignores a + note',        parseTagScope('SCRUFFS (+ 1x T53545)') === null);
check('scope still reads ONLY',        eq(parseTagScope('PENCARRIE (RG165 NAVY, M X1 ONLY)'), { terms: ['RG165', 'NAVY', 'M'], groups: [['RG165', 'NAVY'], ['M']], qty: 1 }));

console.log(pass ? '\nALL PASS' : '\nFAILURES');
process.exit(pass ? 0 : 1);
