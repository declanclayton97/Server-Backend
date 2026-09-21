// Brightpearl rate limiting.
//
// BP answers 503 (occasionally 429) when the account's API allowance is spent,
// and puts the milliseconds to wait in brightpearl-next-throttle-period. It
// means "come back in a moment", not "this failed".
//
// This lived inline in server.js as one bare fetch per call, which is how the
// proof-send path lost a status change to a single throttled second:
//
//   [bp-status] GET order 484881 returned 503
//
// It is its own module so the test can exercise the real function instead of a
// copy of it — a copy drifts, and then the test passes while the code is wrong.
export const BP_THROTTLE_DEFAULT_MS = 2000;
export const BP_THROTTLE_MAX_MS = 30000;

// How long to wait after a throttled response. Exported for the test, because
// this is where the sharp edges are: a missing, empty, non-numeric, zero or
// negative header must not turn into a zero-length wait (that just burns the
// remaining attempts inside the same throttle window), and an absurd one must
// not park the request for an hour.
export function bpThrottleWaitMs(headerValue) {
  const advised = parseInt(headerValue == null ? '' : String(headerValue), 10);
  if (!Number.isFinite(advised) || advised <= 0) return BP_THROTTLE_DEFAULT_MS;
  return Math.min(BP_THROTTLE_MAX_MS, advised);
}

// Retries ONLY 429/503. A 400/401/404 is a real answer and retrying it wastes
// the allowance we were just told had run out.
export async function bpFetchThrottled(url, init = {}, { label = 'bp', attempts = 4, fetchImpl, sleepImpl, log } = {}) {
  const doFetch = fetchImpl || fetch;
  const doSleep = sleepImpl || ((ms) => new Promise((r) => setTimeout(r, ms)));
  const warn = log || console.warn;
  for (let attempt = 1; ; attempt++) {
    const res = await doFetch(url, init);
    if ((res.status !== 503 && res.status !== 429) || attempt >= attempts) return res;
    const wait = bpThrottleWaitMs(res.headers && res.headers.get ? res.headers.get('brightpearl-next-throttle-period') : null);
    warn(`[${label}] BP ${res.status} (rate limited) — waiting ${wait}ms, attempt ${attempt}/${attempts}`);
    await doSleep(wait);
  }
}
