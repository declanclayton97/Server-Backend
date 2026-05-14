// Pure template renderer for the order tracking email pipeline. Takes a
// stored template ({ subject, body_html }) and a variables map, returns
// the rendered output. No I/O — testable in isolation and safe to call
// from the admin panel for live preview.
//
// Placeholder syntax: {{varName}}  (whitespace inside braces is tolerated:
// {{ varName }} works the same). Unknown variables render as the empty
// string and are reported back in the result for admin UI hinting.

// ── Variable schema — the locked list from SPEC.md ──────────────
// Keep this in sync with the inline comment in server.js's
// initializeOrderPipelineTables(). The admin panel reads this list to
// render the "available variables" help section.
const VARIABLE_SCHEMA = [
  { name: "customerName",      label: "Customer full name",                example: "John Smith" },
  { name: "customerFirstName", label: "Customer first name (greetings)",  example: "John" },
  { name: "orderNumber",       label: "BP order reference / PO number",   example: "TUF-12345" },
  { name: "orderId",           label: "Numeric BP order ID",              example: "458947" },
  { name: "orderDate",         label: "Date placed (formatted)",          example: "13 May 2026" },
  { name: "collectionAddress", label: "Pickup address (ready-to-collect)", example: "Tuff Shop, 12 High St, Bristol" },
  { name: "trackingNumber",    label: "Carrier tracking number (shipped)", example: "871203703913" },
  { name: "carrierName",       label: "Carrier name (shipped)",            example: "FedEx" },
  { name: "trackingUrl",       label: "Carrier tracking URL (raw)",        example: "https://www.fedex.com/..." },
  { name: "trackingButton",    label: "Carrier tracking BUTTON (pre-styled HTML)", example: "[ Track your order ]" },
  { name: "reviewUrl",         label: "Trustpilot review URL (raw)",       example: "https://uk.trustpilot.com/review/tuffshop.co.uk" },
  { name: "reviewButton",      label: "Trustpilot review BUTTON (pre-styled HTML)", example: "[ Leave a Trustpilot review ]" },
  { name: "googleReviewUrl",   label: "Google reviews URL (raw)",          example: "https://www.google.com/search?...Tuffshop.co.uk+Reviews" },
  { name: "googleReviewButton", label: "Google review BUTTON (pre-styled HTML)",   example: "[ Leave a Google review ]" },
  { name: "shopName",          label: "Shop name (brand constant)",        example: "Tuff Workwear" },
  { name: "supportEmail",      label: "Support email (brand constant)",    example: "sales@tuffshop.co.uk" },
  { name: "signature",         label: "Brand signature (HTML — logo + socials + legal)", example: "(rendered block — same as proof-chase emails)" },
];

const VALID_VAR_NAMES = new Set(VARIABLE_SCHEMA.map((v) => v.name));

// Replace every {{xxx}} placeholder in `text`. Returns { rendered, used,
// missing, unknown }:
//   used     — variable names that were referenced AND had a value
//   missing  — referenced variables with a falsy/undefined value (empty
//              string substituted; admin UI surfaces these as warnings)
//   unknown  — placeholder names that aren't in VARIABLE_SCHEMA at all
//              (typo in the template — also empty-substituted but flagged
//              as an error in the UI)
function renderText(text, variables) {
  const used = new Set();
  const missing = new Set();
  const unknown = new Set();
  const out = String(text || "").replace(/\{\{\s*([a-zA-Z_][a-zA-Z0-9_]*)\s*\}\}/g, (_, rawName) => {
    const name = rawName;
    if (!VALID_VAR_NAMES.has(name)) {
      unknown.add(name);
      return "";
    }
    const value = variables ? variables[name] : undefined;
    if (value === undefined || value === null || value === "") {
      missing.add(name);
      return "";
    }
    used.add(name);
    return String(value);
  });
  return {
    rendered: out,
    used: Array.from(used),
    missing: Array.from(missing),
    unknown: Array.from(unknown),
  };
}

// Render a full template (subject + body) against a variables map. Diagnostic
// arrays are unioned across subject and body so the admin UI surfaces the
// complete set of issues regardless of which field they appeared in.
function renderTemplate(template, variables) {
  const subject = renderText(template?.subject, variables);
  const body = renderText(template?.body_html, variables);
  const union = (a, b) => Array.from(new Set([...a, ...b]));
  return {
    subject: subject.rendered,
    body_html: body.rendered,
    diagnostics: {
      used: union(subject.used, body.used),
      missing: union(subject.missing, body.missing),
      unknown: union(subject.unknown, body.unknown),
    },
  };
}

export {
  VARIABLE_SCHEMA,
  VALID_VAR_NAMES,
  renderText,
  renderTemplate,
};
