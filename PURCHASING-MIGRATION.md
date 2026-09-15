# Purchasing → Purchasing-Automation: what to DELETE from this repo

Purchasing is moving to its own service (repo `Purchasing-Automation`, host
`purchasing-automation.onrender.com`). This backend was for backend tasks and the purchasing
estate grew inside it until it was straining under load during busy periods.

**Nothing in this file has been deleted yet.** This is the map, written while the new service runs
in parallel on dry runs. Delete only when the new service has been ordering for real, unaided, and
the hub and triage runbook point at it.

## ORDER OF OPERATIONS — the one irreversible step

`MASTER_ENABLED = true` on the new service and removing the pollers HERE must be the SAME change.
If both services run the pollers, both claim the same day and the same order is placed twice. That
is not hypothetical: two deploys inside the Fristads window on 2026-08-19 produced orphan PO 483226,
real order 2597307, and a DUPLICATE live order 2597326 — £539.85 ordered twice, returnable only
with a restocking fee.

1. New service answers every route below, verified against this one.
2. Repoint the consumers (see CONSUMERS).
3. Same commit: flip MASTER_ENABLED there **and** delete the POLLERS here.
4. Then delete the ROUTES and HELPERS here.

## 1. POLLERS AND SWEEPS — delete at step 3 (238 lines)

| lines | what |
|---|---|
| 9883–9894 | CASTLE |
| 9900–9911 | CASTLE |
| 9918–9929 | STERLING |
| 9937–9948 | UNEEK |
| 9955–9966 | SNICKERS |
| 9974–9985 | CARHARTT |
| 9993–10004 | HELLY HANSEN |
| 10013–10024 | PORTWEST |
| 10034–10045 | PENCARRIE |
| 10069–10081 | BLAKLADER_LOW (reorder half) |
| 10085–10097 | SNICKERS_LOW (reorder half) |
| 10167–10182 | V12 |
| 10190–10201 | BUCKLER |
| 10217–10228 | CHADWICK |
| 10235–10246 | CHADWICK |
| 10254–10265 | SCRUFFS |
| 10275–10286 | PERFORMANCE BRANDS |
| 10299–10316 | RETRY SWEEP |
| 10324–10333 | RETRY SWEEP |

⚠ Several suppliers appear TWICE — the customer half and the reorder half run in separate windows
(Castle, Chadwick, the retry sweep, and Blaklader/Snickers as `*_LOW`). Delete both, or a half
keeps running here and double-claims.

## 2. ROUTE REGIONS — delete at step 4 (2337 lines, 83 routes)

| lines | routes |
|---|---|
| 7628–8251 | 34 |
| 8297–8769 | 11 |
| 8874–9862 | 33 |
| 10104–10108 | 1 |
| 10152–10160 | 1 |
| 10337–10350 | 2 |
| 10672–10894 | 1 |

Every route, in file order:

- `/api/purchasing/suppliers` (line 7628, 4 lines)
- `/api/purchasing/preview` (line 7634, 6 lines)
- `/api/purchasing/preview-live` (line 7644, 9 lines)
- `/api/purchasing/debug-lowstock` (line 7658, 40 lines)
- `/api/purchasing/detect-compare` (line 7701, 6 lines)
- `/api/purchasing/pending-lines` (line 7711, 7 lines)
- `/api/purchasing/pending-lines` (line 7718, 7 lines)
- `/api/purchasing/pending-lines/:id` (line 7727, 5 lines)
- `/api/purchasing/add-po-product-row-live` (line 7746, 6 lines)
- `/api/purchasing/add-po-misc-row-live` (line 7752, 6 lines)
- `/api/purchasing/remove-po-row-live` (line 7758, 6 lines)
- `/api/purchasing/set-po-row-cost-live` (line 7764, 6 lines)
- `/api/purchasing/create-combo-po-live` (line 7774, 15 lines)
- `/api/purchasing/email-po-preview` (line 7793, 15 lines)
- `/api/purchasing/discontinued-resend` (line 7816, 8 lines)
- `/api/purchasing/blocked-lines` (line 7837, 90 lines)
- `/api/purchasing/demand-log` (line 7928, 18 lines)
- `/api/purchasing/po-cart-lines` (line 7950, 8 lines)
- `/api/purchasing/clear-po-field-live` (line 7962, 14 lines)
- `/api/purchasing/email-po-bp` (line 7981, 8 lines)
- `/api/purchasing/finalize-tags-live` (line 7992, 15 lines)
- `/api/purchasing/absorb-discount-po` (line 8012, 14 lines)
- `/api/purchasing/reprice-po-live` (line 8027, 8 lines)
- `/api/purchasing/mark-po-placed-live` (line 8039, 22 lines)
- `/api/purchasing/debug-order-page` (line 8064, 21 lines)
- `/api/purchasing/debug-live-get` (line 8087, 6 lines)
- `/api/purchasing/debug-stock` (line 8095, 7 lines)
- `/api/purchasing/debug-fields` (line 8105, 6 lines)
- `/api/purchasing/ebay-row-sku-probe` (line 8116, 38 lines)
- `/api/purchasing/ebay-row-split` (line 8162, 42 lines)
- `/api/purchasing/create-po` (line 8207, 7 lines)
- `/api/purchasing/finalize` (line 8217, 7 lines)
- `/api/purchasing/debug-bp` (line 8226, 5 lines)
- `/api/purchasing/product-identity` (line 8237, 15 lines)
- `/api/purchasing/product-identity-live` (line 8297, 17 lines)
- `/api/purchasing/product-status-live` (line 8323, 49 lines)
- `/api/purchasing/product-supplier-live` (line 8385, 42 lines)
- `/api/purchasing/bp-import-upload` (line 8438, 60 lines)
- `/api/purchasing/product-name-append-live` (line 8504, 33 lines)
- `/api/purchasing/weight-put-test` (line 8541, 21 lines)
- `/api/purchasing/set-primary-supplier-live` (line 8574, 42 lines)
- `/api/purchasing/bp-live-get` (line 8619, 10 lines)
- `/api/purchasing/product-fields-live` (line 8638, 36 lines)
- `/api/purchasing/product-price-test` (line 8695, 36 lines)
- `/api/purchasing/product-price-live` (line 8737, 33 lines)
- `/api/purchasing/price-approvals` (line 8874, 113 lines)
- `/api/purchasing/price-approvals/apply` (line 8991, 49 lines)
- `/api/purchasing/gildan-plan` (line 9044, 77 lines)
- `/api/purchasing/product-find` (line 9124, 11 lines)
- `/api/purchasing/price-lists` (line 9138, 10 lines)
- `/api/purchasing/price-inspect` (line 9151, 34 lines)
- `/api/purchasing/price-probe` (line 9188, 24 lines)
- `/api/purchasing/price-test` (line 9217, 47 lines)
- `/api/purchasing/identity-test` (line 9269, 41 lines)
- `/api/purchasing/seed-test-order` (line 9315, 23 lines)
- `/api/purchasing/empty-po-rows-live` (line 9358, 23 lines)
- `/api/purchasing/set-order-status-live` (line 9383, 22 lines)
- `/api/purchasing/cancel-order` (line 9407, 9 lines)
- `/api/purchasing/fristads-scheduled-run` (line 9426, 16 lines)
- `/api/purchasing/stamp-po-field-live` (line 9445, 10 lines)
- `/api/purchasing/sterling-resolve-test` (line 9459, 16 lines)
- `/api/purchasing/sterling-resolve-po` (line 9487, 27 lines)
- `/api/purchasing/sterling-worker-dry` (line 9518, 17 lines)
- `/api/purchasing/worker-run` (line 9540, 18 lines)
- `/api/purchasing/sterling-worker-job/:id` (line 9559, 8 lines)
- `/api/purchasing/supplier-scheduled-run` (line 9571, 8 lines)
- `/api/purchasing/consolidate-po` (line 9581, 7 lines)
- `/api/purchasing/price-heal` (line 9596, 11 lines)
- `/api/purchasing/reconcile-po` (line 9615, 8 lines)
- `/api/purchasing/portwest-prepare` (line 9626, 6 lines)
- `/api/purchasing/portwest-place` (line 9634, 8 lines)
- `/api/purchasing/error-log` (line 9644, 30 lines)
- `/api/purchasing/triage-fire-test` (line 9678, 8 lines)
- `/api/purchasing/triage-fire-test` (line 9686, 25 lines)
- `/api/purchasing/run-state` (line 9716, 5 lines)
- `/api/purchasing/clear-supplier-basket` (line 9733, 18 lines)
- `/api/purchasing/force-run-safety` (line 9752, 75 lines)
- `/api/purchasing/error-log/:id/handled` (line 9833, 30 lines)
- `/api/purchasing/tag-audit` (line 10104, 5 lines)
- `/api/purchasing/blaklader-cart-probe` (line 10152, 9 lines)
- `/api/purchasing/stuck-claims` (line 10337, 5 lines)
- `/api/purchasing/retry-sweep` (line 10344, 7 lines)
- `/api/purchasing/prepare-supplier-order` (line 10672, 223 lines)

## 3. HELPERS these routes need — delete at step 4, AFTER the routes

Check each for use elsewhere in this file before removing it; some may be shared.

| declared | name |
|---|---|
| 140 | `BRIGHTPEARL_ACCOUNT_ID` |
| 141 | `BRIGHTPEARL_API_TOKEN` |
| 8694 | `COST_LIST` |
| 10407 | `EMAIL_SUPPLIERS` |
| 9357 | `EMPTIABLE_PO_STATUSES` |
| 9382 | `LIVE_STATUS_ALLOWED` |
| 7836 | `PLACED_PO_STATUSES` |
| 8322 | `PRODUCT_STATUSES` |
| 8736 | `RETAIL_LIST` |
| 8266 | `bpLive` |
| 8793 | `ensurePriceDecisionTable` |
| 8635 | `liveGetCustomFields` |
| 8282 | `liveGetIdentity` |
| 8287 | `liveSetIdentity` |
| 7626 | `parseOrderIds` |
| 8836 | `parsePriceRows` |
| 7619 | `requirePurchasing` |
| 8677 | `rrpFromCost` |
| 10353 | `sendOutOfStockEmail` |
| 10443 | `sendPurchaseOrderEmail` |
| 10416 | `supplierOrderEmail` |
| 8813 | `writeCostPrice` |

## CONSUMERS that must be repointed first

- **Purchasing hub** — `Mock Up Creator/public/purchasing-hub.html`, `const API` (~line 205)
  hardcodes this host. Every panel breaks the moment the routes go.
- **Triage runbook** — `PURCHASING-TRIAGE.md` / `TRIAGE-ROUTINE-PROMPT.md` reference this host
  6 times. The triage routine follows those literally; pointed at a service that no longer owns
  purchasing, it cannot recover anything.

## What does NOT move

`purchasingSchedule.js`, `purchasingAuto.js`, `bpWebSession.js`, `bpOrderRow.js`,
`blockedLines.js`, `sterlingResolve.js`, `lowInventory.js` and both Sterling data files are
ALREADY byte-identical in both repos. The supplier-portal logic lives in Alternate-Items, which both
services call — it needs no copying and is already current for both.

Generated 2026-09-15 from the live file. Line numbers move as this file
is edited; re-run the mapping before deleting rather than trusting these.
