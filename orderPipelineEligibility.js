// Decides whether a given BP order qualifies for a customer-facing
// review-request email (the "collected" and "delivered" states). Pure — no
// I/O, no DB. The rules favour skipping when in doubt: a missed review
// opportunity is cheap, a bad review from a frustrated customer is not.

// True when the order has any decoration service row (embroidery / print).
// BP attaches these as separate order rows with SKUs prefixed OPEM- (for
// embroidery) or OPPR- (for print). Their presence means the order required
// in-house production work and gets a longer eligibility window.
function orderHasDecoration(order) {
  if (!order || !order.orderRows) return false;
  const rows = Array.isArray(order.orderRows)
    ? order.orderRows
    : Object.values(order.orderRows);
  return rows.some((r) => {
    const sku = String(r?.productSku || "");
    return sku.startsWith("OPEM-") || sku.startsWith("OPPR-");
  });
}

// Count working days (Mon-Fri) between two timestamps, computed on date
// boundaries — partial days don't matter, only whether the dates fall on a
// weekday or weekend. Inclusive of `from`, exclusive of `to`, so same-day
// returns 0 ("shipped same working day").
//
// Examples:
//   Mon 9am → Mon 5pm  → 0  (same working day)
//   Mon 9am → Tue 9am  → 1  (next working day)
//   Mon 9am → Wed 9am  → 2
//   Fri 4pm → Mon 9am  → 1  (Sat/Sun skipped)
//   Sat 11am → Mon 9am → 0  (came in over weekend, Mon is "same working day")
//   Sat → Tue          → 1
function workingDaysBetween(fromIso, toIso) {
  if (!fromIso || !toIso) return 0;
  const from = new Date(fromIso);
  const to = new Date(toIso);
  if (isNaN(from.getTime()) || isNaN(to.getTime())) return 0;
  // Strip times so we operate on date boundaries — UTC for stability across
  // BST transitions etc.
  const start = Date.UTC(from.getUTCFullYear(), from.getUTCMonth(), from.getUTCDate());
  const end = Date.UTC(to.getUTCFullYear(), to.getUTCMonth(), to.getUTCDate());
  if (end <= start) return 0;
  let count = 0;
  for (let ms = start; ms < end; ms += 86400000) {
    const day = new Date(ms).getUTCDay(); // 0=Sun, 6=Sat
    if (day !== 0 && day !== 6) count++;
  }
  return count;
}

// Calendar-day difference (used for logo orders where weekends already
// count as production time — embroidery / print orders don't usually run on
// weekends but the lead-time promise to the customer counts all days).
function calendarDaysBetween(fromIso, toIso) {
  if (!fromIso || !toIso) return 0;
  const from = new Date(fromIso);
  const to = new Date(toIso);
  if (isNaN(from.getTime()) || isNaN(to.getTime())) return 0;
  return Math.max(0, (to.getTime() - from.getTime()) / 86400000);
}

const LOGO_MAX_CALENDAR_DAYS = 14;
const PLAIN_MAX_WORKING_DAYS = 1;

// Returns { eligible, reason, deco, daysElapsed } so callers can log the
// reasoning into the activity log.
function checkReviewEligibility(order, finishedAtIso) {
  const placedOn = order?.placedOn || order?.createdOn;
  const finishedAt = finishedAtIso || new Date().toISOString();
  if (!placedOn) {
    return { eligible: false, reason: "no placedOn timestamp on order", deco: null };
  }
  const deco = orderHasDecoration(order);
  if (deco) {
    const days = calendarDaysBetween(placedOn, finishedAt);
    const rounded = Math.round(days * 10) / 10;
    if (days <= LOGO_MAX_CALENDAR_DAYS) {
      return {
        eligible: true,
        reason: `logo order, ${rounded} calendar days (limit ${LOGO_MAX_CALENDAR_DAYS})`,
        deco: true,
        daysElapsed: rounded,
      };
    }
    return {
      eligible: false,
      reason: `logo order took ${rounded} calendar days (limit ${LOGO_MAX_CALENDAR_DAYS})`,
      deco: true,
      daysElapsed: rounded,
    };
  }
  const wd = workingDaysBetween(placedOn, finishedAt);
  if (wd <= PLAIN_MAX_WORKING_DAYS) {
    return {
      eligible: true,
      reason: `plain order, ${wd} working day${wd === 1 ? "" : "s"} (limit ${PLAIN_MAX_WORKING_DAYS})`,
      deco: false,
      daysElapsed: wd,
    };
  }
  return {
    eligible: false,
    reason: `plain order took ${wd} working days (limit ${PLAIN_MAX_WORKING_DAYS})`,
    deco: false,
    daysElapsed: wd,
  };
}

export {
  orderHasDecoration,
  workingDaysBetween,
  calendarDaysBetween,
  checkReviewEligibility,
  LOGO_MAX_CALENDAR_DAYS,
  PLAIN_MAX_WORKING_DAYS,
};
