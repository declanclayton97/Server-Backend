import test from 'node:test';
import assert from 'node:assert/strict';
import { workingHoursBetween, enteredStatusAt, overdueProofs } from './proofOverdue.js';

const H = 3600 * 1000;
const at = (s) => new Date(s).getTime();
const none = new Set();

test('weekday hours count in full', () => {
  // Tue 10 Nov 09:00 to Wed 11 Nov 21:00 GMT = 36 h
  assert.equal(workingHoursBetween(at('2026-11-10T09:00:00Z'), at('2026-11-11T21:00:00Z'), none), 36);
});

test('a weekend is left out', () => {
  // Fri 13 Nov 12:00 to Mon 16 Nov 12:00: Fri 12 h + Mon 12 h
  assert.equal(workingHoursBetween(at('2026-11-13T12:00:00Z'), at('2026-11-16T12:00:00Z'), none), 24);
});

test('a bank holiday is left out', () => {
  // Thu 24 Dec 12:00 to Tue 29 Dec 12:00 2026: Thu 12 h, Fri 25 + Mon 28 holidays, Tue 12 h
  const hols = new Set(['2026-12-25', '2026-12-28']);
  assert.equal(workingHoursBetween(at('2026-12-24T12:00:00Z'), at('2026-12-29T12:00:00Z'), hols), 24);
});

test('UK days, not UTC days, during summer time', () => {
  // Sat 4 Jul 2026 starts at 23:00 UTC on Fri 3 Jul: Fri 22:00-23:00 UTC counts, 23:00 onwards does not
  assert.equal(workingHoursBetween(at('2026-07-03T22:00:00Z'), at('2026-07-04T02:00:00Z'), none), 1);
});

test('entered status is the first note of the latest run in 34', () => {
  const notes = [
    { addedOn: '2026-10-21T13:52:00+01:00', orderStatusId: 34 },
    { addedOn: '2026-10-21T15:38:00+01:00', orderStatusId: 124 },
    { addedOn: '2026-11-09T13:58:00Z', orderStatusId: 34 },
    { addedOn: '2026-11-10T09:00:00Z', orderStatusId: 34 },
  ];
  assert.equal(enteredStatusAt(notes.slice().reverse()), '2026-11-09T13:58:00Z');
  assert.equal(enteredStatusAt(notes.slice(0, 2)), null); // not in 34 now
  assert.equal(enteredStatusAt([]), null);
});

function fakePool(sessions = []) {
  const rows = new Map();
  return {
    rows,
    async query(sql, params = []) {
      if (/^CREATE/.test(sql.trim())) return { rows: [] };
      if (/^DELETE/.test(sql.trim())) { for (const k of [...rows.keys()]) if (!params[0].includes(k)) rows.delete(k); return { rows: [] }; }
      if (/^SELECT order_id/.test(sql.trim())) return { rows: [...rows].map(([order_id, since]) => ({ order_id, since })) };
      if (/^INSERT/.test(sql.trim())) { if (!rows.has(params[0])) rows.set(params[0], params[1]); return { rows: [] }; }
      if (/FROM approval_sessions/.test(sql)) return { rows: sessions.filter((s) => params[0].includes(s.order_number)).map((s) => ({ order_number: s.order_number, last: s.created_at })) };
      throw new Error('unexpected sql ' + sql);
    },
  };
}

test('overdue after 36 working hours unless a proof went since', async () => {
  const now = at('2026-11-12T10:00:00Z'); // Thu
  const notes = {
    1: [{ addedOn: '2026-11-10T09:00:00Z', orderStatusId: 34 }],          // 49 h: overdue
    2: [{ addedOn: '2026-11-11T09:00:00Z', orderStatusId: 34 }],          // 25 h: not yet
    3: [{ addedOn: '2026-11-09T09:00:00Z', orderStatusId: 34 }],          // overdue but proof sent
    4: [{ addedOn: '2026-11-01T09:00:00Z', orderStatusId: 23 }],          // no 34 note: counts from now
  };
  const pool = fakePool([{ order_number: '3', created_at: '2026-11-11T08:00:00Z' }]);
  pool.rows.set(9, new Date('2026-10-01T00:00:00Z')); // left 34 since: dropped
  const calls = [];
  const r = await overdueProofs({ pool, orderIds: [1, 2, 3, 4], getNotes: async (id) => { calls.push(id); return notes[id]; }, now });
  assert.deepEqual([...r].map(([id, o]) => [id, o.overdue]), [[1, true], [2, false], [3, false], [4, false]]);
  assert.equal(r.get(1).hours, 49);
  assert.equal(r.get(4).hours, 0);
  assert.equal(pool.rows.has(9), false);
  // second pass: no more note lookups
  await overdueProofs({ pool, orderIds: [1, 2, 3, 4], getNotes: async (id) => { calls.push(id); return notes[id]; }, now });
  assert.deepEqual(calls, [1, 2, 3, 4]);
});
