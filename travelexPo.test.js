// Tests for travelexPo.js.
// Run with:  node travelexPo.test.js

import { orderSpend, summarise, newThresholds, alertEmail, monthlyEmail } from "./travelexPo.js";

let pass = 0, fail = 0;
const eq = (name, got, want) => {
  const ok = JSON.stringify(got) === JSON.stringify(want);
  if (ok) pass++; else { fail++; console.log(`FAIL ${name}\n  got:  ${JSON.stringify(got)}\n  want: ${JSON.stringify(want)}`); }
};

const order = (id, created, saved, total) => ({
  id, reference: `ASDA-${id}`, orderStatus: { name: "Accounts Complete" }, createdOn: created,
  customFields: { PCF_TRAVLXPO: "710000000946 - Q2-2026", ...(saved != null ? { PCF_TRVORDVL: saved } : {}) },
  totalValue: { total: total },
});

// Processed orders count their saved value; unprocessed ones their own total.
eq("saved value", orderSpend(order(1, "2026-10-01", "28.74", "0.00")), { value: 28.74, pending: false });
eq("not processed", orderSpend(order(2, "2026-10-01", null, "64.19")), { value: 64.19, pending: true });
eq("saved with £", orderSpend(order(3, "2026-10-01", "£1,203.50", "0.00")), { value: 1203.5, pending: false });
eq("blank saved", orderSpend(order(4, "2026-10-01", "", "12.00")), { value: 12, pending: true });

const now = new Date("2026-10-29T12:00:00Z").getTime();
const s = summarise({
  poRef: "710000000946 - Q2-2026", invoiceOrderId: 473285, amount: 10000, now,
  orders: [
    order(10, "2026-07-01T10:00:00Z", "8000", "0"),
    order(11, "2026-10-01T10:00:00Z", "560", "0"),
    order(12, "2026-10-20T10:00:00Z", null, "100"),
  ],
});
eq("processed", s.processed, 8560);
eq("pending", s.pending, 100);
eq("used", s.used, 8660);
eq("remaining", s.remaining, 1340);
eq("pct", s.pctUsed, 86.6);
// Last 8 weeks: 560 + 100 = 660 over 56 days = 82.50 a week.
eq("per week", s.perWeek, 82.5);
eq("run out ~16 weeks", Math.round((new Date(s.estRunOut).getTime() - now) / 86400000), Math.round(1340 / (660 / 56)));
eq("counts", [s.orderCount, s.pendingCount], [3, 1]);

// Only thresholds not yet emailed.
eq("into final 2000", newThresholds(1340, []), [2000]);
eq("already sent 2000", newThresholds(1340, [2000]), []);
eq("jumped past two", newThresholds(400, [2000]), [1000, 500]);
eq("used up", newThresholds(-5, [2000, 1000, 500]), [0]);
eq("plenty left", newThresholds(5000, []), []);

eq("alert subject", alertEmail(s, 2000).subject, "Travelex bulk PO 710000000946 - Q2-2026: into the final £2,000 - £1,340.00 left");
eq("used-up subject", alertEmail({ ...s, remaining: 0 }, 0).subject, "Travelex bulk PO 710000000946 - Q2-2026 is used up (£0.00 left)");
eq("monthly lists orders", (monthlyEmail(s, "November 2026").html.match(/ASDA-1\d/g) || []).length, 3);

console.log(`${pass} passed, ${fail} failed`);
if (fail) process.exit(1);
