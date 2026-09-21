// Exercises the REAL bpFetchThrottled, not a copy of it.
//
// Run: node bpThrottle.test.mjs
import assert from 'node:assert';
import { bpFetchThrottled, bpThrottleWaitMs, BP_THROTTLE_DEFAULT_MS, BP_THROTTLE_MAX_MS } from './bpThrottle.js';

const res = (status, throttle) => ({
  status,
  ok: status >= 200 && status < 300,
  headers: { get: (h) => (h === 'brightpearl-next-throttle-period' && throttle != null ? String(throttle) : null) },
});

// Returns each response in turn, then repeats the last one forever.
const scripted = (...queue) => {
  let calls = 0;
  const f = async () => queue[Math.min(calls++, queue.length - 1)];
  f.calls = () => calls;
  return f;
};

const run = async (opts, ...queue) => {
  const slept = [];
  const f = scripted(...queue);
  const r = await bpFetchThrottled('https://bp/test', {}, {
    fetchImpl: f, sleepImpl: async (ms) => { slept.push(ms); }, log: () => {}, ...opts,
  });
  return { r, slept, calls: f.calls() };
};

// --- the wait itself -------------------------------------------------------
// A zero-length wait is the dangerous answer: it burns every attempt inside the
// same throttle window and comes back 503 anyway, which looks like BP being
// down rather than us not waiting.
assert.equal(bpThrottleWaitMs('1500'), 1500);
assert.equal(bpThrottleWaitMs(1500), 1500);
assert.equal(bpThrottleWaitMs(undefined), BP_THROTTLE_DEFAULT_MS);
assert.equal(bpThrottleWaitMs(null), BP_THROTTLE_DEFAULT_MS);
assert.equal(bpThrottleWaitMs(''), BP_THROTTLE_DEFAULT_MS);
assert.equal(bpThrottleWaitMs('not-a-number'), BP_THROTTLE_DEFAULT_MS);
assert.equal(bpThrottleWaitMs('0'), BP_THROTTLE_DEFAULT_MS);
assert.equal(bpThrottleWaitMs('-5'), BP_THROTTLE_DEFAULT_MS);
assert.equal(bpThrottleWaitMs('999999'), BP_THROTTLE_MAX_MS, 'an absurd header must not park the request');
assert.equal(bpThrottleWaitMs('2000ms'), 2000, 'parseInt tolerates a unit suffix');

// --- the retry -------------------------------------------------------------
// 1. The common case, and the one that lost order 484881's status change: a
//    throttle that clears on the next attempt.
{
  const { r, slept, calls } = await run({}, res(503, 1500), res(200));
  assert.equal(r.status, 200, 'succeeds once the throttle clears');
  assert.deepEqual(slept, [1500], 'waits exactly what BP advised');
  assert.equal(calls, 2);
}

// 2. Still throttled after every attempt: hand the 503 back so the caller can
//    fall back to posting the note. Must not loop forever.
{
  const { r, slept, calls } = await run({}, res(503, 800));
  assert.equal(r.status, 503, 'gives the 503 back rather than throwing');
  assert.equal(calls, 4, 'four attempts by default');
  assert.deepEqual(slept, [800, 800, 800], 'sleeps BETWEEN attempts, not after the last');
}

// 3. 429 behaves like 503.
{
  const { r, calls } = await run({}, res(429, 1000), res(200));
  assert.equal(r.status, 200);
  assert.equal(calls, 2);
}

// 4. A real error is not retried — retrying a 404 wastes the allowance BP has
//    just told us we are short of, and delays the note that matters more.
for (const status of [400, 401, 403, 404, 500]) {
  const { r, calls, slept } = await run({}, res(status));
  assert.equal(r.status, status);
  assert.equal(calls, 1, `${status} must not be retried`);
  assert.deepEqual(slept, [], `${status} must not sleep`);
}

// 5. attempts:1 means try once, so a caller can opt out of waiting entirely.
{
  const { r, calls, slept } = await run({ attempts: 1 }, res(503, 500));
  assert.equal(r.status, 503);
  assert.equal(calls, 1);
  assert.deepEqual(slept, []);
}

// 6. A response with no usable headers must not throw — some fetch mocks and
//    error paths hand back a bare object.
{
  const bare = { status: 503, ok: false };
  const { r, slept } = await run({ attempts: 2 }, bare, res(200));
  assert.equal(r.status, 200);
  assert.deepEqual(slept, [BP_THROTTLE_DEFAULT_MS], 'falls back to the default wait');
}

console.log('bpThrottle: all assertions passed');
