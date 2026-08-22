#!/usr/bin/env node
// Is it safe to deploy Server-Backend right now?
//
// WHY THIS EXISTS: on 2026-08-19 two deploys landed inside the Fristads poller window. The first
// orphaned PO 483226; the second restarted the service mid-run so `last_run_date` never saved, the
// poller ran a THIRD time and placed a DUPLICATE live order (2597326, £539.85) that had to be
// cancelled with the supplier.
//
// The cause of the mistiming was trusting the shell clock: container tzdata often has no DST, so
// `TZ=Europe/London date` returns GMT and reads an HOUR EARLY through British Summer Time. UTC is
// reliable; UK local is not. So UK time is computed from UTC here, never read from the environment.
//
// This lives IN THE REPO (not just in the local CLAUDE-HUB) so an automated triage run in a cloud
// environment can honour it too — it is the single rule that stops an unattended fix from causing
// a duplicate live order.
//
// Usage:  node scripts/deploy-window.mjs          → human readable
//         node scripts/deploy-window.mjs --json    → { safe, reason, ... } for scripted callers
// Exit 0 = clear to deploy, exit 1 = inside/near a poller window, DO NOT deploy or push.

const asJson = process.argv.includes('--json');
// --at "Mon 10:35" pins the UK day/time so the BLOCKING branch can actually be tested. Without it
// the refusal path is only exercised during a real window, i.e. the one moment you must not be
// experimenting. Affects the answer only, never the poller list.
const atArg = (process.argv.find((a) => a.startsWith('--at=')) || '').slice(5)
  || (process.argv.includes('--at') ? process.argv[process.argv.indexOf('--at') + 1] : '');
const now = new Date();

// UK = UTC+1 during BST (last Sunday of March → last Sunday of October), else UTC.
const lastSunday = (year, monthIdx) => { const d = new Date(Date.UTC(year, monthIdx + 1, 0)); d.setUTCDate(d.getUTCDate() - d.getUTCDay()); return d; };
const y = now.getUTCFullYear();
const bstStart = lastSunday(y, 2); bstStart.setUTCHours(1, 0, 0, 0);
const bstEnd = lastSunday(y, 9); bstEnd.setUTCHours(1, 0, 0, 0);
const isBST = now >= bstStart && now < bstEnd;
const uk = new Date(now.getTime() + (isBST ? 3600000 : 0));
let hh = uk.getUTCHours(), mm = uk.getUTCMinutes();
let day = ['Sun', 'Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat'][uk.getUTCDay()];
if (atArg) {
  const m = String(atArg).match(/^\s*(Mon|Tue|Wed|Thu|Fri|Sat|Sun)[\s,]+(\d{1,2}):(\d{2})\s*$/i);
  if (!m) { console.error(`--at needs the form "Mon 10:35", got ${JSON.stringify(atArg)}`); process.exit(2); }
  day = m[1][0].toUpperCase() + m[1].slice(1, 3).toLowerCase();
  hh = Number(m[2]); mm = Number(m[3]);
}

// Every poller window (start hour:minute, 30 minutes long). KEEP IN STEP WITH server.js — if you
// add a poller there and not here, this will cheerfully clear a deploy into its window.
const POLLERS = [
  { name: 'Blaklader', h: 9, m: 30, days: 'Mon-Fri' },
  { name: 'Snickers', h: 10, m: 0, days: 'Mon-Fri' },
  { name: 'Fristads', h: 10, m: 30, days: 'Mon-Fri' },
  { name: 'Helly Hansen', h: 11, m: 0, days: 'Mon-Fri' },
  { name: 'Mascot', h: 11, m: 30, days: 'Mon-Fri' },
  { name: 'Castle', h: 12, m: 0, days: 'Mon-Fri' },
  { name: 'Sterling', h: 13, m: 0, days: 'Mon-Fri' },
  { name: 'Carhartt', h: 13, m: 30, days: 'Mon/Wed/Fri' },
  { name: 'Scruffs', h: 14, m: 0, days: 'Mon-Fri' },
  { name: 'Performance Brands', h: 14, m: 30, days: 'Mon-Fri' },
  { name: 'Portwest', h: 15, m: 0, days: 'Mon-Fri' },
  { name: 'PenCarrie', h: 15, m: 40, days: 'Mon-Fri' },
  { name: 'Uneek', h: 16, m: 0, days: 'Mon-Fri' },
];
const runsToday = (p) => (p.days === 'Mon/Wed/Fri' ? ['Mon', 'Wed', 'Fri'] : ['Mon', 'Tue', 'Wed', 'Thu', 'Fri']).includes(day);
const mins = hh * 60 + mm;
// A run can overrun its 30-minute window (portal workers take minutes), so treat a window as
// unsafe from its start until 15 minutes AFTER it closes, and refuse within 10 minutes before one.
const BUFFER_AFTER = 15, BUFFER_BEFORE = 10;
const pad = (n) => String(n).padStart(2, '0');

let blocking = null, next = null;
for (const p of POLLERS) {
  if (!runsToday(p)) continue;
  const start = p.h * 60 + p.m, end = start + 30 + BUFFER_AFTER;
  if (mins >= start && mins < end) blocking = { ...p, why: 'window open (or a run may still be finishing)' };
  else if (mins >= start - BUFFER_BEFORE && mins < start) blocking = { ...p, why: `window opens within ${start - mins} min` };
  if (mins < start && (!next || start < next.start)) next = { ...p, start };
}

// Walk FORWARD past every window that would still block, rather than reporting only when THIS one
// closes — "clear again at 10:45" is a time you are still blocked at, and on a bad day you believe it.
let clearAt = null;
if (blocking) {
  const windows = POLLERS.filter(runsToday).map((p) => ({ name: p.name, start: p.h * 60 + p.m })).sort((a, b) => a.start - b.start);
  let clear = blocking.h * 60 + blocking.m + 30 + BUFFER_AFTER, moved = true;
  while (moved) {
    moved = false;
    for (const w of windows) {
      if (clear >= w.start - BUFFER_BEFORE && clear < w.start + 30 + BUFFER_AFTER) { clear = w.start + 30 + BUFFER_AFTER; moved = true; }
    }
  }
  clearAt = clear >= 24 * 60 ? 'tomorrow' : `${pad(Math.floor(clear / 60))}:${pad(clear % 60)}`;
}

const result = {
  safe: !blocking,
  ukNow: `${day} ${pad(hh)}:${pad(mm)}`,
  zone: isBST ? 'BST' : 'GMT',
  computedFromUtc: `${pad(now.getUTCHours())}:${pad(now.getUTCMinutes())}`,
  blocking: blocking ? { supplier: blocking.name, window: `${pad(blocking.h)}:${pad(blocking.m)}`, why: blocking.why } : null,
  clearAt,
  next: next ? { supplier: next.name, window: `${pad(next.h)}:${pad(next.m)}`, inMinutes: next.start - mins } : null,
  afterDeploy: 'Re-check that supplier for a duplicate PO before reporting all-clear.',
};

if (asJson) {
  console.log(JSON.stringify(result, null, 2));
} else {
  console.log(`UK now: ${result.ukNow} (${result.zone}) — computed from UTC ${result.computedFromUtc}, NOT the shell clock`);
  if (blocking) {
    console.log(`DO NOT DEPLOY — ${blocking.name} ${pad(blocking.h)}:${pad(blocking.m)}: ${blocking.why}`);
    console.log(clearAt === 'tomorrow' ? 'clear again tomorrow — every remaining window today is blocking' : `clear again at ${clearAt} UK (past every window after it)`);
  } else {
    console.log('CLEAR TO DEPLOY' + (next ? ` — next window is ${next.supplier} at ${next.window} UK, in ${next.inMinutes} min` : ' — no further windows today'));
    console.log(result.afterDeploy);
  }
}
process.exit(blocking ? 1 : 0);
