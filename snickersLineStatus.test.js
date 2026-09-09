// Offline test for the discontinued lookup. A local stub stands in for Alt-Items, so this
// asserts the REQUEST we build as well as how we read the answer — the bug was entirely in the
// request, and a test that only stubbed the response would have passed against the broken code.
import http from 'node:http';
import { snickersLineStatus } from './purchasingSchedule.js';

const seen = [];
const server = http.createServer((req, res) => {
  seen.push(req.url);
  const u = new URL(req.url, 'http://x');
  // The real portal answers on sku + size TEXT. Anything else is a variant it cannot match,
  // which is what the old numeric-size request always produced.
  const ok = u.searchParams.get('sku') === '12180400006' && u.searchParams.get('sizeLabel') === 'L';
  res.setHeader('content-type', 'application/json');
  res.end(JSON.stringify(ok
    ? { found: true, avail: null, status: 'Discontinued', barcode: '7332515399571', size: 'L' }
    : { found: false, reason: 'no colour/size match' }));
});
await new Promise((r) => server.listen(0, r));
const base = `http://127.0.0.1:${server.address().port}`;
let pass = true;
const check = (name, cond) => { console.log((cond ? 'PASS  ' : 'FAIL  ') + name); if (!cond) pass = false; };

// 1. the real failing line, with the size label Brightpearl actually holds
const hit = await snickersLineStatus(base, '12180400006', 'L');
check('discontinued line reports its status', !!hit && /discontinued/i.test(hit.status || ''));

// 2. the request must carry sku + sizeLabel, and must NOT reconstruct a numeric size
const url = seen[0] || '';
check('request uses sku=',            url.includes('sku=12180400006'));
check('request uses sizeLabel=L',     url.includes('sizeLabel=L'));
check('request drops the old size= derivation', !/[?&]size=/.test(url) && !/[?&]code=/.test(url));

// 3. this is the regression: the old code sent size="6" for this SKU. Prove that shape fails,
//    so nobody reintroduces it thinking the portal is tolerant.
const before = seen.length;
const numeric = await fetch(`${base}/api/supplier-stock?supplier=SNICKERS&code=1218&colour=0400&size=6&live=1`).then((r) => r.json());
check('the old numeric-size request still finds nothing', numeric.found === false);
check('…and it did reach the stub', seen.length === before + 1);

// 4. no size label = no answerable question. Must return null WITHOUT calling out.
const n = seen.length;
const noSize = await snickersLineStatus(base, '12180400006', '');
check('missing size label returns null', noSize === null);
check('…and makes no request', seen.length === n);

server.close();
process.exit(pass ? 0 : 1);
