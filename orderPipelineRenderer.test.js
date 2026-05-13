// Tests for the order-pipeline template renderer.
// Run with:  node orderPipelineRenderer.test.js

import { renderText, renderTemplate, VARIABLE_SCHEMA } from "./orderPipelineRenderer.js";

let pass = 0, fail = 0;
function assertEq(label, actual, expected) {
  const ok = JSON.stringify(actual) === JSON.stringify(expected);
  if (ok) pass++; else {
    fail++;
    console.error(`  ✗ ${label}\n      expected ${JSON.stringify(expected)}\n      got      ${JSON.stringify(actual)}`);
  }
}

const vars = {
  customerName: "John Smith",
  customerFirstName: "John",
  orderNumber: "TUF-12345",
  orderId: "458947",
  trackingNumber: "871203703913",
  carrierName: "FedEx",
  trackingUrl: "https://example.com/track",
  shopName: "Tuff Workwear",
  supportEmail: "sales@tuffshop.co.uk",
};

// ── Basic substitution ─────────────────────────────────────────
let r = renderText("Hi {{customerFirstName}}", vars);
assertEq("Basic substitution", r.rendered, "Hi John");
assertEq("used populated", r.used.includes("customerFirstName"), true);
assertEq("no missing", r.missing.length, 0);
assertEq("no unknown", r.unknown.length, 0);

// ── Whitespace inside braces ───────────────────────────────────
r = renderText("Hello {{ customerName }}!", vars);
assertEq("Whitespace tolerated", r.rendered, "Hello John Smith!");

// ── Multiple substitutions, order preserved ────────────────────
r = renderText("{{customerName}} ordered {{orderNumber}}", vars);
assertEq("Multiple substitutions", r.rendered, "John Smith ordered TUF-12345");

// ── Missing variable → empty string, flagged in missing[] ──────
r = renderText("Hi {{customerFirstName}}, review {{reviewUrl}} please", vars);
assertEq("Missing variable rendered as empty",
  r.rendered, "Hi John, review  please");
assertEq("Missing variable reported", r.missing, ["reviewUrl"]);

// ── Unknown variable → empty string, flagged in unknown[] ──────
r = renderText("Hi {{customerFirstName}}, your {{notARealVar}} is here", vars);
assertEq("Unknown variable empty-substituted",
  r.rendered, "Hi John, your  is here");
assertEq("Unknown variable reported", r.unknown, ["notARealVar"]);

// ── Subject + body rendered together, diagnostics union ────────
const template = {
  subject: "Your order {{orderNumber}}",
  body_html: "<p>Hi {{customerFirstName}}, tracking {{trackingNumber}}.</p><p>{{reviewUrl}}</p>",
};
const tpl = renderTemplate(template, vars);
assertEq("Template subject rendered", tpl.subject, "Your order TUF-12345");
assertEq("Template body rendered",
  tpl.body_html, "<p>Hi John, tracking 871203703913.</p><p></p>");
assertEq("Diagnostics aggregate missing",
  tpl.diagnostics.missing, ["reviewUrl"]);
assertEq("Diagnostics no unknown when all valid",
  tpl.diagnostics.unknown.length, 0);

// ── Empty / null inputs ────────────────────────────────────────
r = renderText("", vars);
assertEq("Empty text → empty rendered", r.rendered, "");
r = renderText(null, vars);
assertEq("Null text → empty rendered", r.rendered, "");
r = renderText("static text only", vars);
assertEq("No placeholders → unchanged", r.rendered, "static text only");

// ── Empty value treated as missing ─────────────────────────────
r = renderText("hi {{customerFirstName}}", { customerFirstName: "" });
assertEq("Empty string value → missing[]", r.missing, ["customerFirstName"]);

// ── Variable schema integrity ──────────────────────────────────
assertEq("Schema has 13 variables", VARIABLE_SCHEMA.length, 13);
assertEq("Schema entries have name/label/example",
  VARIABLE_SCHEMA.every((v) => v.name && v.label && v.example), true);
const names = VARIABLE_SCHEMA.map((v) => v.name);
assertEq("Schema names unique", new Set(names).size, names.length);

console.log(`\n${pass} passed, ${fail} failed`);
process.exit(fail === 0 ? 0 : 1);
