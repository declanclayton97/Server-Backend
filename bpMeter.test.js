import { test } from 'node:test';
import assert from 'node:assert/strict';
import { endpointOf, parseBpUrl, callerOf, record, drain } from './bpMeter.js';

test('endpointOf collapses ids and id sets, drops trailing slash', () => {
  assert.equal(endpointOf('/order-service/order/123,456/row'), '/order-service/order/:id/row');
  assert.equal(endpointOf('/warehouse-service/product-availability/61000-120999'), '/warehouse-service/product-availability/:id');
  assert.equal(endpointOf('/warehouse-service/warehouse/2/location/'), '/warehouse-service/warehouse/:id/location');
  assert.equal(endpointOf('/order-service/order-search'), '/order-service/order-search');
});

test('parseBpUrl only counts the BP public API', () => {
  assert.deepEqual(parseBpUrl('https://euw1.brightpearlconnect.com/public-api/tuffworkwear/order-service/order/5/custom-field?x=1'),
    { account: 'tuffworkwear', endpoint: '/order-service/order/:id/custom-field' });
  assert.equal(parseBpUrl('https://euw1.brightpearlconnect.com/report.php?x=1'), null);   // BP web UI
  assert.equal(parseBpUrl('https://api.shiptheory.com/v1/public-api/x'), null);
  assert.equal(parseBpUrl('not a url'), null);
});

test('callerOf skips generic BP wrappers and names the job', () => {
  const stack = `Error
    at globalThis.fetch.meteredFetch (file:///opt/render/project/src/bpMeter.js:120:17)
    at bpGet (file:///opt/render/project/src/server.js:900:22)
    at async pollUrgentMockups (file:///opt/render/project/src/server.js:1500:9)
    at async Timeout._onTimeout (file:///opt/render/project/src/server.js:1600:3)`;
  assert.equal(callerOf(stack), 'pollUrgentMockups server.js:1500 < Timeout._onTimeout server.js:1600');
});

test('callerOf handles Windows paths and anonymous frames', () => {
  const stack = `Error
    at C:\\app\\lib\\bp.js:30:5
    at node:internal/process/task_queues:95:5`;
  assert.equal(callerOf(stack), 'bp.js:30');
  assert.equal(callerOf(''), 'unknown');
});

test('record buckets per minute and tracks throttles and the lowest remaining', () => {
  drain();
  const at = Date.UTC(2026, 9, 30, 12, 0, 10);
  const url = 'https://euw1.brightpearlconnect.com/public-api/tuffworkwear/order-service/order/1';
  const stack = 'Error\n    at async job (file:///x/server.js:1:1)';
  record({ url, status: 200, remaining: '120', stack, at });
  record({ url: url.replace('/1', '/2'), status: 503, remaining: '0', stack, at: at + 20000 });
  record({ url, status: 200, remaining: '199', stack, at: at + 60000 });   // next minute
  record({ url: 'https://example.com/', status: 200, remaining: null, stack, at });   // ignored
  const rows = drain().sort((a, b) => a.minute - b.minute);
  assert.equal(rows.length, 2);
  assert.deepEqual({ calls: rows[0].calls, throttled: rows[0].throttled, min: rows[0].minRemaining }, { calls: 2, throttled: 1, min: 0 });
  assert.equal(rows[0].caller, 'job server.js:1');
  assert.equal(rows[1].minRemaining, 199);
  assert.equal(drain().length, 0);
});
