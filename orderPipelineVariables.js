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
  // {{reviewUrl}} stays as the Trustpilot link so any template that already
  // uses it keeps working unchanged. {{googleReviewUrl}} is a sibling
  // variable so templates can offer both options.
  reviewUrl: process.env.ORDER_PIPELINE_REVIEW_URL || "https://uk.trustpilot.com/review/tuffshop.co.uk",
  googleReviewUrl: process.env.ORDER_PIPELINE_GOOGLE_REVIEW_URL ||
    "https://www.google.com/search?newwindow=1&sca_esv=23181f9f9c0f0df0&rlz=1C1YTUH_en-GBGB1087GB1087&si=AL3DRZFIhG6pAqfNLal55wUTwygCG0fClF3UxiOmgw9Hq7nbWSabA4-IiFJbXPux_EoC-BKpHe_SPFRI3IuymQlPHZEUvxA6fNcvOfrZzqJ2_ztJd4ma9whaaWi8xH668oAofC8WuuoN&q=Tuffshop.co.uk+Reviews&sa=X&ved=2ahUKEwjEjKH5rbiUAxW5UkEAHXhFKO8Q0bkNegQIIxAH",
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

// Render an email-safe HTML "button" for a URL. Uses a single-cell table —
// most resilient pattern across email clients (Outlook ignores
// padding/background on bare <a> tags). Returns empty string when href is
// missing so templates that use {{trackingButton}} for an order with no
// tracking number just collapse the section silently.
function makeEmailButton(href, label, { bg = "#1f1f1f", color = "#ffffff" } = {}) {
  if (!href || !label) return "";
  return (
    `<table cellpadding="0" cellspacing="0" border="0" style="margin:16px 0;">` +
    `<tr><td style="border-radius:6px;background:${bg};">` +
    `<a href="${href}" style="display:inline-block;padding:12px 24px;color:${color};` +
    `text-decoration:none;font-family:Arial,Helvetica,sans-serif;font-weight:bold;font-size:14px;">` +
    `${label}</a></td></tr></table>`
  );
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

// ── Name normalisation ────────────────────────────────────────
// Customers type their names all sorts of ways — ALL CAPS, all lowercase,
// surname only with a title in the first-name box, the full title in the
// "first name" field, etc. The helpers below try to reach a presentable
// "Mr John Smith" / "John" form regardless of input shape so the rendered
// email doesn't shout the recipient's name at them.

const TITLES = new Set([
  "mr", "mrs", "ms", "miss", "mx",
  "dr", "doctor", "prof", "professor",
  "sir", "dame", "lord", "lady",
  "rev", "reverend",
  "fr", "father",
  "capt", "captain",
  "maj", "major",
  "lt", "lieutenant",
  "sgt", "sergeant",
  "col", "colonel",
]);

function stripTrailingPunct(token) {
  return String(token || "").replace(/[.,;:!?]+$/, "");
}

function isTitleToken(token) {
  if (!token) return false;
  return TITLES.has(stripTrailingPunct(token).toLowerCase());
}

// Returns true when every token in the string is a salutation/title (e.g.
// the customer typed "Mr" alone in the full-name box). Used to detect
// degenerate inputs so we can try to recover a usable name elsewhere.
function isOnlyTitles(s) {
  if (!s) return false;
  const tokens = String(s).trim().split(/\s+/).filter(Boolean);
  if (tokens.length === 0) return false;
  return tokens.every(isTitleToken);
}

// Best-effort title case — lowercase then capitalise the first letter
// after the start of the string, a space, hyphen, or apostrophe. Handles
// "MCDONALD" → "Mcdonald" → "McDonald" via a second pass. Accented
// characters are preserved as-typed (the regex only operates on the
// first letter of each segment so naming patterns like "Müller" survive).
function smartTitleCase(name) {
  if (!name) return "";
  let s = String(name).trim().replace(/\s+/g, " ").toLowerCase();
  // Capitalise after string-start, space, hyphen, apostrophe.
  s = s.replace(/(^|[\s'\-])([a-zà-ÿ])/g, (_, sep, ch) => sep + ch.toUpperCase());
  // Mc prefix — covers McDonald, McLeod, McConnell etc. Skipped Mac
  // because it generates false positives on names like Macy / Mackenzie.
  s = s.replace(/\bMc([a-zà-ÿ])/g, (_, c) => "Mc" + c.toUpperCase());
  return s;
}

// firstName for "Hi {customerFirstName}" greetings. Strips titles, falls
// back to "there" when the input is unusable (title-only, or "Mr Smith"
// where the only non-title word is almost certainly a surname).
function firstName(fullName) {
  if (!fullName) return "there";
  const trimmed = String(fullName).trim().replace(/\s+/g, " ");
  if (!trimmed) return "there";
  const parts = trimmed.split(/\s+/);
  if (parts.length === 0) return "there";

  if (isTitleToken(parts[0])) {
    // "Mr John Smith" → "John" (token after title is the first name)
    if (parts.length >= 3) return smartTitleCase(parts[1]);
    // "Mr" alone or "Mr Smith" — only one non-title word and it's almost
    // certainly the surname. "Hi Smith" reads strangely, so default to a
    // friendly fallback rather than guessing wrong.
    return "there";
  }

  return smartTitleCase(parts[0]);
}

function formatOrderDate(isoDate) {
  if (!isoDate) return "";
  const d = new Date(isoDate);
  if (isNaN(d.getTime())) return "";
  const months = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"];
  return `${d.getDate()} ${months[d.getMonth()]} ${d.getFullYear()}`;
}

// Pick the most usable customer name from BP's parties shape. The approach
// is to first detect a salutation/title from any field the customer might
// have typed it into, then strip leading titles from each name candidate
// so we end up with a clean "body" name. The two get recombined at the end
// so the result reads as e.g. "Mr John Smith" / "Mrs Jones" / "John
// Smith" — correctly title-cased — regardless of how the customer entered
// their details.
function pickCustomerName(parties) {
  const c = parties?.customer || {};
  const d = parties?.delivery || {};
  const norm = (s) => String(s || "").trim().replace(/\s+/g, " ");

  // Look at every field that could plausibly carry a title and grab the
  // first one we find. Most BP customers put the title (if any) in the
  // full-name or first-name field.
  const detectTitle = (s) => {
    const tokens = norm(s).split(/\s+/).filter(Boolean);
    if (tokens.length > 0 && isTitleToken(tokens[0])) {
      return stripTrailingPunct(tokens[0]);
    }
    return "";
  };
  let title = "";
  for (const src of [c.addressFullName, c.contactName, c.addressFirstName, d.addressFullName, d.contactName, d.addressFirstName]) {
    title = detectTitle(src);
    if (title) break;
  }

  // Strip any leading title from a candidate so we don't double the title
  // when combining at the end.
  const stripLeadingTitle = (s) => {
    const tokens = norm(s).split(/\s+/).filter(Boolean);
    if (tokens.length > 0 && isTitleToken(tokens[0])) {
      return tokens.slice(1).join(" ");
    }
    return tokens.join(" ");
  };

  // Build name-body candidates in priority order, with titles stripped.
  const bodyCandidates = [
    stripLeadingTitle(c.addressFullName),
    [stripLeadingTitle(c.addressFirstName), norm(c.addressLastName)].filter(Boolean).join(" "),
    stripLeadingTitle(c.contactName),
    stripLeadingTitle(d.addressFullName),
    [stripLeadingTitle(d.addressFirstName), norm(d.addressLastName)].filter(Boolean).join(" "),
  ]
    .map((s) => norm(s))
    .filter(Boolean);

  const body = bodyCandidates[0] || "";

  if (body) {
    return smartTitleCase(title ? `${title} ${body}` : body);
  }

  // No body name found — try the title alone, then the company name as a
  // last resort.
  if (title) return smartTitleCase(title);
  const company = norm(c.companyName || d.companyName);
  return company ? smartTitleCase(company) : "";
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
    googleReviewUrl: constants.googleReviewUrl,
    shopName: constants.shopName,
    supportEmail: constants.supportEmail,
    // Branded HTML signature block — same one the proof-chase emails use.
    // Templates can drop {{signature}} wherever they want it to land.
    signature: SIGNATURE_HTML,
    // Pre-styled email-safe buttons. Use these instead of {{trackingUrl}}
    // / {{reviewUrl}} when you want a clickable button with friendly link
    // text rather than a raw URL spelled out in the body.
    trackingButton: makeEmailButton(trackingUrl, "Track your order"),
    reviewButton: makeEmailButton(constants.reviewUrl, "Leave a Trustpilot review", { bg: "#00b67a" }),
    googleReviewButton: makeEmailButton(constants.googleReviewUrl, "Leave a Google review", { bg: "#4285f4" }),
  };
}

export {
  BRAND_CONSTANTS,
  CARRIER_URL_BUILDERS,
  deriveVariables,
  parseTrackingNotes,
  buildTrackingUrl,
  makeEmailButton,
  pickCustomerName,
  firstName,
  formatOrderDate,
  smartTitleCase,
  isTitleToken,
  isOnlyTitles,
};
