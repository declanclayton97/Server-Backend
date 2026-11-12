// Overdue proofs (Dec, 12 Nov 2026): an order that has sat in Proof Required (status 34) for
// more than 36 hours, not counting weekends or bank holidays, goes URGENT on Harry's list and
// locks his mock-up screens to the overdue orders until a proof has been sent for each one.
//
// BP has no "entered this status at" field, but every order note records the order's status
// when it was written. The first note of the order's latest unbroken run in status 34 is when
// it went in (or just after, if that change left no note of its own). Each order's time is
// looked up once and kept in proof_required_since; it is dropped when the order leaves 34.

export const PROOF_REQUIRED_STATUS = 34;
export const OVERDUE_HOURS = 36;

// England and Wales, used when gov.uk can't be reached.
const FALLBACK_BANK_HOLIDAYS = [
  "2026-01-01", "2026-04-03", "2026-04-06", "2026-05-04", "2026-05-25", "2026-08-31", "2026-12-25", "2026-12-28",
  "2027-01-01", "2027-03-26", "2027-03-29", "2027-05-03", "2027-05-31", "2027-08-30", "2027-12-27", "2027-12-28",
];

let bankHolidays = { dates: new Set(FALLBACK_BANK_HOLIDAYS), fetchedAt: 0 };

export async function loadBankHolidays(fetchImpl = fetch) {
  if (Date.now() - bankHolidays.fetchedAt < 24 * 3600 * 1000) return bankHolidays.dates;
  try {
    const r = await fetchImpl("https://www.gov.uk/bank-holidays.json");
    if (!r.ok) throw new Error(`gov.uk ${r.status}`);
    const events = (await r.json())["england-and-wales"]?.events || [];
    if (!events.length) throw new Error("no england-and-wales events");
    bankHolidays = { dates: new Set([...FALLBACK_BANK_HOLIDAYS, ...events.map((e) => e.date)]), fetchedAt: Date.now() };
  } catch (e) {
    console.error("[proof-overdue] bank holidays from gov.uk failed, using the built-in list:", e.message);
    bankHolidays = { ...bankHolidays, fetchedAt: Date.now() };
  }
  return bankHolidays.dates;
}

const londonDay = new Intl.DateTimeFormat("en-CA", { timeZone: "Europe/London", year: "numeric", month: "2-digit", day: "2-digit", weekday: "short" });

// "2026-11-12" and whether it is a working day, for the UK date a moment falls on.
function ukDay(ms) {
  const p = Object.fromEntries(londonDay.formatToParts(new Date(ms)).map((x) => [x.type, x.value]));
  return { date: `${p.year}-${p.month}-${p.day}`, weekend: p.weekday === "Sat" || p.weekday === "Sun" };
}

// Hours between two moments, leaving out any time that falls on a UK weekend or bank holiday.
// UK midnights always land on a whole UTC hour, so stepping hour by hour splits exactly on them.
export function workingHoursBetween(fromMs, toMs, holidays = bankHolidays.dates, capHours = 24 * 400) {
  if (!(toMs > fromMs)) return 0;
  const HOUR = 3600 * 1000;
  let total = 0;
  let t = fromMs;
  while (t < toMs && total < capHours) {
    const next = Math.min(toMs, Math.floor(t / HOUR) * HOUR + HOUR);
    const d = ukDay(t);
    if (!d.weekend && !holidays.has(d.date)) total += (next - t) / HOUR;
    t = next;
  }
  return total;
}

// When the order went into status 34 this time, from its notes (oldest first or not).
export function enteredStatusAt(notes, statusId = PROOF_REQUIRED_STATUS) {
  const sorted = [...(notes || [])].sort((a, b) => String(a.addedOn).localeCompare(String(b.addedOn)));
  if (!sorted.length || sorted[sorted.length - 1].orderStatusId !== statusId) return null;
  let i = sorted.length - 1;
  while (i > 0 && sorted[i - 1].orderStatusId === statusId) i--;
  return sorted[i].addedOn;
}

// Keeps proof_required_since in step with the orders now in status 34 and returns
// Map(orderId -> { since, hours, overdue }). getNotes(id) returns BP's note list for an order.
// At most `maxLookups` new orders are looked up per call; the rest wait for the next one.
export async function overdueProofs({ pool, orderIds, getNotes, now = Date.now(), maxLookups = 15 }) {
  const out = new Map();
  if (!pool) return out;
  await pool.query(`CREATE TABLE IF NOT EXISTS proof_required_since (
      order_id INTEGER PRIMARY KEY, since TIMESTAMPTZ NOT NULL, from_notes BOOLEAN NOT NULL, seen_at TIMESTAMPTZ NOT NULL DEFAULT NOW())`);
  const ids = [...new Set(orderIds.map(Number))];
  await pool.query(`DELETE FROM proof_required_since WHERE NOT (order_id = ANY($1::int[]))`, [ids]);
  const known = new Map((await pool.query(`SELECT order_id, since FROM proof_required_since`)).rows
    .map((r) => [Number(r.order_id), new Date(r.since).getTime()]));

  let lookups = 0;
  for (const id of ids) {
    if (known.has(id) || lookups >= maxLookups) continue;
    lookups++;
    let since = null;
    try { since = enteredStatusAt(await getNotes(id)); }
    catch (e) { console.error("[proof-overdue] notes for", id, "failed:", e.message); continue; }
    // No note written in status 34 yet: count from now, the first time we have seen it.
    const at = since ? new Date(since).getTime() : now;
    await pool.query(`INSERT INTO proof_required_since (order_id, since, from_notes) VALUES ($1, $2, $3) ON CONFLICT (order_id) DO NOTHING`,
      [id, new Date(at), !!since]);
    known.set(id, at);
  }

  // A proof sent since the order went in clears it, even if the order hasn't been moved on yet.
  const proofed = new Map((await pool.query(
    `SELECT order_number, MAX(created_at) AS last FROM approval_sessions WHERE order_number = ANY($1::text[]) GROUP BY order_number`,
    [ids.map(String)])).rows.map((r) => [Number(r.order_number), new Date(r.last).getTime()]));

  const holidays = await loadBankHolidays();
  for (const [id, since] of known) {
    if (!ids.includes(id)) continue;
    const hours = workingHoursBetween(since, now, holidays);
    out.set(id, { since: new Date(since).toISOString(), hours: Math.round(hours * 10) / 10,
      overdue: hours > OVERDUE_HOURS && !((proofed.get(id) || 0) > since) });
  }
  return out;
}
