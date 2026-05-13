// Derives the variables map for a single BP order. Pure — caller fetches
// the BP order detail and the order notes, this module just shapes them.
//
// Brand constants (shopName / supportEmail / reviewUrl / collectionAddress)
// are read from env at load time so changes don't need code edits. They can
// be overridden per-call via the `constants` option for preview testing.

import { SIGNATURE_HTML } from "./emailSignature.js";

const BRAND_CONSTANTS = {
  shopName: process.env.ORDER_PIPELINE_SHOP_NAME || "Tuff Workwear",
  supportEmail: process.env.ORDER_PIPELINE_SUPPORT_EMAIL || "sales@tuffshop.co.uk",
  reviewUrl: process.env.ORDER_PIPELINE_REVIEW_URL || "https://uk.trustpilot.com/review/tuffshop.co.uk",
  collectionAddress: process.env.ORDER_PIPELINE_COLLECTION_ADDRESS ||
    "Tuff Workwear, [collection address — set ORDER_PIPELINE_COLLECTION_ADDRESS env var]",
};

// ── Tracking note parsing ──────────────────────────────────────
// Shiptheory writes BP order notes like:
//   "FedEx tracking reference received: 871203703913"
//   "Royal Mail tracking reference received: KJ099325504GB"
// We capture the carrier name (group 1) and the tracking number (group 2),
// then route to the right carrier URL. Multiple notes (e.g. partial
// shipments) return all — caller picks "latest" semantics.
const TRACKING_RE = /^(.+?)\s+tracking reference received:\s+(\S+)/i;

const CARRIER_URL_BUILDERS = {
  "fedex":        (n) => `https://www.fedex.com/fedextrack/?trknbr=${encodeURIComponent(n)}`,
  "royal mail":   (n) => `https://www.royalmail.com/track-your-item#/tracking-results/${encodeURIComponent(n)}`,
  "dpd":          (n) => `https://www.dpd.co.uk/apps/tracking/?reference=${encodeURIComponent(n)}`,
  "dpd local":    (n) => `https://www.dpd.co.uk/apps/tracking/?reference=${encodeURIComponent(n)}`,
  "ups":          (n) => `https://www.ups.com/track?tracknum=${encodeURIComponent(n)}`,
  "evri":         (n) => `https://www.evri.com/track/parcel/${encodeURIComponent(n)}`,
  "hermes":       (n) => `https://www.evri.com/track/parcel/${encodeURIComponent(n)}`,
  "parcelforce":  (n) => `https://www.parcelforce.com/portal/pw/track?trackNumber=${encodeURIComponent(n)}`,
};

function buildTrackingUrl(carrier, number) {
  const key = String(carrier || "").trim().toLowerCase();
  const builder = CARRIER_URL_BUILDERS[key];
  return builder ? builder(number) : "";
}

// Accepts BP's note shape (or any object with .text). Returns the list of
// extracted tracking events sorted newest-first.
function parseTrackingNotes(notes) {
  if (!Array.isArray(notes)) return [];
  const results = [];
  for (const note of notes) {
    const text = note?.text || note?.body || note?.message || note?.note || "";
    const lines = String(text).split(/\r?\n/);
    for (const line of lines) {
      const m = line.match(TRACKING_RE);
      if (m) {
        results.push({
          carrier: m[1].trim(),
          number: m[2].trim(),
          addedOn: note?.addedOn || note?.createdOn || null,
        });
      }
    }
  }
  results.sort((a, b) => {
    const ta = a.addedOn ? Date.parse(a.addedOn) : 0;
    const tb = b.addedOn ? Date.parse(b.addedOn) : 0;
    return tb - ta;
  });
  return results;
}

// ── Helpers ────────────────────────────────────────────────────
function firstName(fullName) {
  if (!fullName) return "";
  const trimmed = String(fullName).trim();
  return trimmed.split(/\s+/)[0] || "";
}

function formatOrderDate(isoDate) {
  if (!isoDate) return "";
  const d = new Date(isoDate);
  if (isNaN(d.getTime())) return "";
  const months = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"];
  return `${d.getDate()} ${months[d.getMonth()]} ${d.getFullYear()}`;
}

function pickCustomerName(parties) {
  const c = parties?.customer || {};
  const d = parties?.delivery || {};
  return (
    c.addressFullName ||
    [c.addressFirstName, c.addressLastName].filter(Boolean).join(" ") ||
    c.contactName ||
    d.addressFullName ||
    [d.addressFirstName, d.addressLastName].filter(Boolean).join(" ") ||
    c.companyName ||
    d.companyName ||
    ""
  ).trim();
}

// ── Public: derive variables ───────────────────────────────────
// `bp`     — the BP order detail object (response[0] shape from
//            /order-service/order/{id}).
// `notes`  — optional array of BP note objects fetched from
//            /order/{id}/note — needed for tracking-bearing emails.
// `constants` — optional brand-constants override (used by the admin
//            panel preview when staff want to see how a template would
//            render with the live brand vs a custom one).
function deriveVariables(bp, { notes = [], constants = BRAND_CONSTANTS } = {}) {
  const customerName = pickCustomerName(bp?.parties);
  const tracking = parseTrackingNotes(notes);
  const latest = tracking[0] || {};
  const trackingNumber = latest.number || "";
  const carrierName = latest.carrier || "";
  const trackingUrl = trackingNumber ? buildTrackingUrl(carrierName, trackingNumber) : "";

  return {
    customerName,
    customerFirstName: firstName(customerName),
    orderNumber: bp?.customerRef || bp?.purchaseOrderNumber || (bp?.id ? `#${bp.id}` : ""),
    orderId: bp?.id ? String(bp.id) : "",
    orderDate: formatOrderDate(bp?.placedOn || bp?.createdOn),
    collectionAddress: constants.collectionAddress,
    trackingNumber,
    carrierName,
    trackingUrl,
    reviewUrl: constants.reviewUrl,
    shopName: constants.shopName,
    supportEmail: constants.supportEmail,
    // Branded HTML signature block — same one the proof-chase emails use.
    // Templates can drop {{signature}} wherever they want it to land.
    signature: SIGNATURE_HTML,
  };
}

export {
  BRAND_CONSTANTS,
  CARRIER_URL_BUILDERS,
  deriveVariables,
  parseTrackingNotes,
  buildTrackingUrl,
  pickCustomerName,
  firstName,
  formatOrderDate,
};
