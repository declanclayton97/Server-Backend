// Pure status → customer-state mapping for the order tracking email
// pipeline. No I/O, no DB — exported as plain functions so this can be
// unit-tested in isolation and reused by the admin panel for previews.
//
// Source of truth: SPEC.md in the Order-Tracking-Pipeline project.
// If a BP status is renamed or added, edit STATE_FOR_STATUS_NAME below.

// ── Customer-facing states ──────────────────────────────────────
// State #1 (order placed) is BP-native and not tracked here — the
// rest are the states we own.
const STATES = {
  STOCK_ORDERED: "stock_ordered",
  IN_PRODUCTION: "in_production",
  BACK_ORDER: "back_order",
  READY_FOR_COLLECTION: "ready_for_collection",
  SHIPPED: "shipped",
  COLLECTED: "collected",
  DELIVERED: "delivered",
};

// Stable customer-facing labels (for admin panel display).
const STATE_LABELS = {
  [STATES.STOCK_ORDERED]: "Stock Ordered",
  [STATES.IN_PRODUCTION]: "In Production",
  [STATES.BACK_ORDER]: "Back Order Delay",
  [STATES.READY_FOR_COLLECTION]: "Ready for Collection",
  [STATES.SHIPPED]: "Shipped",
  [STATES.COLLECTED]: "Collected",
  [STATES.DELIVERED]: "Delivered (review request)",
};

// ── Normalisation ───────────────────────────────────────────────
// BP status names sometimes have inconsistent casing/whitespace. Normalise
// once at the boundary so the maps below are predictable.
function norm(name) {
  return String(name || "").trim().toLowerCase().replace(/\s+/g, " ");
}

// ── BP status → customer state ──────────────────────────────────
// Built from the table in SPEC.md. Values are state constants from STATES.
// Keys are normalised BP status names.
const STATE_FOR_STATUS_NAME = new Map([
  // Stock Ordered — BP statuses where we've placed a PO with our supplier
  ["stock needs ordering", STATES.STOCK_ORDERED],
  ["sportswear - stock needs ordering", STATES.STOCK_ORDERED],
  ["ordered stock awaiting delivery", STATES.STOCK_ORDERED],
  ["sportswear - ordered stock", STATES.STOCK_ORDERED],

  // In Production — the order is actively being processed in-house. Only
  // the production-room statuses live here; pick-stage statuses below are
  // explicitly silent so we don't email "being prepared" before the picker
  // has actually confirmed the stock exists (see PICK_STAGE_STATUS_NAMES).
  ["banners/stickers/signs need printing", STATES.IN_PRODUCTION],
  ["embroidery room ready", STATES.IN_PRODUCTION],
  ["emb - awaiting tidying", STATES.IN_PRODUCTION],
  ["print room ready", STATES.IN_PRODUCTION],
  ["personalisation correction", STATES.IN_PRODUCTION],
  ["print - mimaki printer", STATES.IN_PRODUCTION],

  // Back Order — supplier delay
  ["back order with supplier", STATES.BACK_ORDER],

  // Ready for Collection — finished, awaiting customer pickup
  ["collection - paid", STATES.READY_FOR_COLLECTION],
  ["collection - credit account", STATES.READY_FOR_COLLECTION],
  ["sportswear collection paid", STATES.READY_FOR_COLLECTION],
]);

// Pick-stage statuses — BP says "this can be picked" but it hasn't been
// physically pulled off the shelf yet. The Pick-Pack unfound workflow shows
// these are over-optimistic often enough that emailing "your order is being
// prepared" here risks misleading the customer when the stock turns out to
// be missing and the order has to wait days for a restock. Deliberately
// silent — the customer's next email fires either at a production-room
// status (logo orders) or at Invoiced and Completed (non-logo orders).
const PICK_STAGE_STATUS_NAMES = new Set([
  "in stock, pick/pack/ship",
  "instock - pick & put in workshop",
  "pending room - awaiting instructions",
  "order needs files adding",
  "sports - files need adding",
  "sportswear orders to pick",
  "aviation - to pick",
].map(norm));

// "Final" BP statuses — these route to either Shipped or Collected based
// on the previous customer state captured in order_email_log.
const FINAL_STATUS_NAMES = new Set([
  "invoiced and completed",
  "complete awaiting payment",
]);

// Internal-only statuses — orders sit in these without triggering any
// customer email. Includes the "G&H" pass-through (G&H is internal-only;
// the next transition to Stock Needs Ordering is what emails the customer).
const INTERNAL_STATUS_NAMES = new Set([
  "ignore – awaiting deletion",
  "ignore - awaiting deletion", // hyphen variant
  "pending magento orders",
  "new order",
  "belgrade pending orders",
  "quote sent",
  "sample orders",
  "cancellation pending",
  "cancelled",
  "draft / quote",
  "draft/quote",
  "query",
  "await payment before ordering in",
  "awaiting payment before despatch",
  "awaiting payment before workshop",
  "on stop do not process without a",
  "collection - to pay",
  "sportswear collection to pay",
  "accounts - query",
  "accounts complete",
  "exch. orders - await return",
  "ptsg exchange order",
  "g&h",
  "local delivery required",
].map(norm));

// Proof flow — owned by the separate proof approval system, never
// emailed from this pipeline.
const PROOF_STATUS_NAMES = new Set([
  "proof required-send to customer",
  "proof sent to customer",
  "sportswear - proof required",
  "sportswear - proof sent",
].map(norm));

// ── Public API ──────────────────────────────────────────────────

// Returns true if the BP status should never trigger any pipeline email.
// Used by the poller as a fast-skip — don't even look the order up.
function isInternalStatus(bpStatusName) {
  const n = norm(bpStatusName);
  return INTERNAL_STATUS_NAMES.has(n);
}

// Returns true if the BP status is owned by the proof approval system.
// Also a skip — proofs have their own customer emails.
function isProofFlowStatus(bpStatusName) {
  const n = norm(bpStatusName);
  return PROOF_STATUS_NAMES.has(n);
}

// Returns true if the order is in a pick-stage status — BP indicates the
// items are ready to pick but no physical pick has been confirmed. Useful
// for the admin "inspect order" panel so users can distinguish "deliberately
// silent" from "unknown status" when reviewing an order's email history.
function isPickStageStatus(bpStatusName) {
  const n = norm(bpStatusName);
  return PICK_STAGE_STATUS_NAMES.has(n);
}

// Returns the customer-facing state for a given BP status name, taking the
// previously-emailed state into account so that "Invoiced and Completed"
// routes correctly between Shipped and Collected.
//
// Returns null when no email should fire (internal status, proof status,
// or a final status reached without the prerequisite previous state).
function customerStateForBpStatus(bpStatusName, prevCustomerState = null) {
  const n = norm(bpStatusName);
  if (!n) return null;
  if (INTERNAL_STATUS_NAMES.has(n)) return null;
  if (PROOF_STATUS_NAMES.has(n)) return null;
  if (PICK_STAGE_STATUS_NAMES.has(n)) return null;

  if (FINAL_STATUS_NAMES.has(n)) {
    // Disambiguate based on the previous customer-facing state.
    if (prevCustomerState === STATES.READY_FOR_COLLECTION) return STATES.COLLECTED;
    if (prevCustomerState === STATES.IN_PRODUCTION) return STATES.SHIPPED;
    // Already past the shipping touchpoint — don't re-email.
    if (prevCustomerState === STATES.SHIPPED) return null;
    if (prevCustomerState === STATES.COLLECTED) return null;
    if (prevCustomerState === STATES.DELIVERED) return null;
    // No previous state recorded (order zoomed straight to Invoiced without
    // us ever seeing it in In Production / Ready). Default to Shipped — it's
    // the much more common path for online orders.
    return STATES.SHIPPED;
  }

  return STATE_FOR_STATUS_NAME.get(n) || null;
}

// Convenience accessor for the admin panel — list every customer state
// alongside its label so the UI can render the template editor.
function listStates() {
  return Object.values(STATES).map((id) => ({ id, label: STATE_LABELS[id] }));
}

export {
  STATES,
  STATE_LABELS,
  customerStateForBpStatus,
  isInternalStatus,
  isProofFlowStatus,
  isPickStageStatus,
  listStates,
  // Exposed for test introspection / admin panel "which BP statuses map
  // here" display. Not meant for callers to mutate.
  STATE_FOR_STATUS_NAME as _STATE_FOR_STATUS_NAME,
  FINAL_STATUS_NAMES as _FINAL_STATUS_NAMES,
  INTERNAL_STATUS_NAMES as _INTERNAL_STATUS_NAMES,
  PROOF_STATUS_NAMES as _PROOF_STATUS_NAMES,
  PICK_STAGE_STATUS_NAMES as _PICK_STAGE_STATUS_NAMES,
};
