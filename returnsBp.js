// returnsBp.js — turn an online return into Brightpearl records, the way the team books
// them by hand (read off real pairs, e.g. SC 489699 + SO 489700 on SO 486999):
//
//   CREDIT     order type SC, parentOrderId = the original sale, reference
//              "<original ref> / EXCHANGE" (or "/ REFUND"; the credits report reads the
//              reason off that suffix), rows copied from the sale at the price paid.
//   EXCHANGE   a new SO, parentOrderId = the original sale, reference
//              "EXCHANGE ORDER CREDITED VIA SC#<credit>", same channel / price list /
//              delivery address / delivery method, the new size AT THE SAME PRICE, plus a
//              £0 line "EXCHANGE ORDER CREDITED VIA SC#…".
//   MONEY      no payments at all. The credit and the exchange order are both left
//              owing and cancel each other out against the original paid sale — Dec,
//              30 Sep: payments between them are "just extra noise". (The older hand-made
//              pairs did carry an OTHER payment/receipt; that is deliberately not copied.)
//   STATUS     credit 121 "Refund requested" when anything is being refunded (accounts
//              are emailed to pay it), 11 "complete" for a swap whose replacement is on
//              the exchange order, otherwise left at 10 for staff.
//
// "Something else" swaps get the exchange order with a text line saying what the
// customer asked for and nothing priced — staff add the item themselves.
//
// Every write was proven on the TEST account (tuffbsitc) first, 29 Sep 2026.
// `bp(method, path, body)` is either the live or the test writer; both behave the same.

import { SIZE_STEPS, moveSize, sizeKey } from "./returns.js";
import { bpSafeText } from "./bpText.js";

const round2 = (n) => Math.round(Number(n) * 100) / 100;
const money = (n) => round2(n).toFixed(2);
async function one(bp, id) { const r = await bp("GET", `/order-service/order/${id}`); return Array.isArray(r) ? r[0] : r; }
const opt = (p, id) => ((p && p.variations) || []).find((v) => v.optionId === id) || {};

async function productsById(bp, ids) {
  const out = {};
  const list = [...new Set(ids.map(Number).filter(Boolean))].sort((a, b) => a - b);
  for (let i = 0; i < list.length; i += 200) {
    for (const p of (await bp("GET", `/product-service/product/${list.slice(i, i + 200).join(",")}`)) || []) out[p.id] = p;
  }
  return out;
}
async function stockFor(bp, ids) {
  const list = [...new Set(ids.map(Number).filter(Boolean))].sort((a, b) => a - b);
  const out = {};
  for (let i = 0; i < list.length; i += 200) {
    try {
      const r = (await bp("GET", `/warehouse-service/product-availability/${list.slice(i, i + 200).join(",")}`)) || {};
      // onHand = free to sell. inStock counts stock already ALLOCATED to other orders (SO 494767:
      // inStock 22, onHand 0) - using it promised a T-shirt we did not have.
      for (const [id, a] of Object.entries(r)) out[id] = Math.max(0, Number((a && a.total && a.total.onHand) || 0));
    } catch { /* stock is advice for the dropdown, not a blocker */ }
  }
  return out;
}

// Every live variant of the product's style, with its size, colour and stock.
async function styleVariants(bp, productId) {
  const me = (await productsById(bp, [productId]))[productId];
  if (!me) return { me: null, variants: [] };
  const group = me.productGroupId;
  let ids = [productId];
  if (group) {
    const s = await bp("GET", `/product-service/product-search?productGroupId=${group}&pageSize=500`);
    const ix = Object.fromEntries(((s && s.metaData && s.metaData.columns) || []).map((c, i) => [c.name, i]));
    ids = ((s && s.results) || []).filter((r) => Number(r[ix.productGroupId]) === Number(group)).map((r) => Number(r[ix.productId]));
    if (!ids.includes(productId)) ids.push(productId);
  }
  const prods = await productsById(bp, ids);
  const stock = await stockFor(bp, ids);
  const variants = Object.values(prods)
    .filter((p) => p.id === productId || !/archiv|discontinu/i.test(String(p.status || "")))
    .map((p) => ({
      productId: p.id, sku: (p.identity && p.identity.sku) || "",
      size: opt(p, 2).optionValue || "", colour: opt(p, 1).optionValue || "", colourId: opt(p, 1).optionValueId || null,
      inStock: stock[p.id] || 0,
    }));
  const mine = variants.find((v) => v.productId === productId) || { productId, size: opt(me, 2).optionValue || "", colourId: opt(me, 1).optionValueId || null };
  return { me: mine, variants };
}

/**
 * What would be created for this return. Reads only.
 * ret: a returns_requests row. Returns { order, credit, exchange, warnings }.
 */
export async function planBrightpearl(bp, ret) {
  const order = await one(bp, ret.order_id);
  if (!order) throw new Error(`order ${ret.order_id} not found`);
  const warnings = [];
  const lines = ret.lines || [];

  // One credit row per ORDER row: two return lines on the same row (one swapped, one
  // refunded) are credited together, so splitting 27.97 VAT gives 27.97, not 13.99 + 13.99.
  const merged = [];
  for (const l of lines) {
    const m = merged.find((x) => String(x.rowId) === String(l.rowId));
    if (m) { m.qty += Number(l.qty); m.outcomes.add(l.outcome); } else merged.push({ ...l, qty: Number(l.qty), outcomes: new Set([l.outcome]) });
  }
  const creditRows = [];
  for (const l of merged) {
    const r = (order.orderRows || {})[l.rowId];
    if (!r) { warnings.push(`"${l.name}" is no longer on order ${order.id} (row ${l.rowId}), so it can't be credited automatically.`); continue; }
    const soldQty = Number((r.quantity && r.quantity.magnitude) || 0) || 1;
    const rowNet = Number((r.rowValue && r.rowValue.rowNet && r.rowValue.rowNet.value) || 0);
    const rowTax = Number((r.rowValue && r.rowValue.rowTax && r.rowValue.rowTax.value) || 0);
    const whole = Number(l.qty) >= soldQty;
    creditRows.push({
      rowId: String(l.rowId), productId: Number(r.productId), name: r.productName, sku: r.productSku || "", qty: Number(l.qty),
      net: whole ? round2(rowNet) : round2(rowNet / soldQty * l.qty),
      tax: whole ? round2(rowTax) : round2(rowTax / soldQty * l.qty),
      unitNet: rowNet / soldQty, unitTax: rowTax / soldQty,
      taxCode: (r.rowValue && r.rowValue.taxCode) || "T20", nominalCode: r.nominalCode || "4000",
      outcome: l.outcomes.has("refund") ? "refund" : "exchange", outcomes: [...l.outcomes],
    });
  }
  const creditNet = round2(creditRows.reduce((a, r) => a + r.net, 0));
  const creditTax = round2(creditRows.reduce((a, r) => a + r.tax, 0));
  const hasRefund = lines.some((l) => l.outcome === "refund");
  const hasExchange = lines.some((l) => l.outcome === "exchange");
  const ref = order.reference || String(order.id);

  // For each swap: the style's sizes in the same colour, with the one the customer
  // asked for picked out. Staff confirm or change it before anything is created.
  const exchangeLines = [];
  for (const x of lines.filter((l) => l.outcome === "exchange")) {
    const cr = creditRows.find((r) => r.rowId === String(x.rowId));
    if (!cr) continue;
    const c = { ...cr, qty: Number(x.qty), exchangeChoice: x.exchangeChoice || "", exchangeFor: x.exchangeFor || "" };
    const steps = SIZE_STEPS[c.exchangeChoice];
    const line = { key: String(lines.indexOf(x)), rowId: c.rowId, name: c.name, qty: c.qty, choice: c.exchangeChoice, text: c.exchangeFor,
      unitNet: c.unitNet, unitTax: c.unitTax, taxCode: c.taxCode, nominalCode: c.nominalCode, from: null, options: [], suggested: null };
    if (steps) {
      try {
        const { me, variants } = await styleVariants(bp, c.productId);
        line.from = me && { productId: me.productId, size: me.size, colour: me.colour };
        const same = variants.filter((v) => !me || v.colourId === me.colourId);
        same.sort((a, b) => { const x = sizeKey(a.size), y = sizeKey(b.size); return ((x && x.rank) || 0) - ((y && y.rank) || 0) || a.size.localeCompare(b.size); });
        line.options = same.map((v) => ({ productId: v.productId, size: v.size, sku: v.sku, inStock: v.inStock, current: me && v.productId === me.productId }));
        const target = me ? moveSize(me, variants, steps) : null;
        line.suggested = target ? target.productId : null;
        if (!target) warnings.push(`Couldn't work out "${c.exchangeChoice.toLowerCase()}" from ${me ? me.size || "this size" : "this item"} for "${c.name}" - pick the size.`);
      } catch (e) { warnings.push(`Couldn't read the sizes for "${c.name}": ${e.message}`); }
    }
    exchangeLines.push(line);
  }

  return {
    order: {
      id: order.id, reference: ref, customerId: order.parties && order.parties.customer && order.parties.customer.contactId,
      channelId: order.assignment && order.assignment.current && order.assignment.current.channelId,
      priceListId: order.priceListId, priceModeCode: order.priceModeCode, warehouseId: order.warehouseId,
      currency: (order.currency && order.currency.orderCurrencyCode) || "GBP",
      delivery: order.parties && order.parties.delivery, shippingMethodId: order.delivery && order.delivery.shippingMethodId,
      paid: order.orderPaymentStatus,
    },
    credit: {
      reference: `${ref} / ${hasRefund && hasExchange ? "REFUND & EXCHANGE" : hasExchange ? "EXCHANGE" : "REFUND"}`,
      rows: creditRows, net: creditNet, tax: creditTax, gross: round2(creditNet + creditTax),
      status: hasRefund ? 121 : 11,
    },
    exchange: hasExchange ? { lines: exchangeLines } : null,
    warnings,
  };
}

// PCF_CRDTRSN is a SELECT: its option ids come from the field's own definition
// (182 "Return for Exchange", 183 "Return for Refund" when read on 30 Sep), looked up
// by NAME so a renumbered list cannot set the wrong reason. Brightpearl's write format
// for a select is not something we have proven, so each form is tried and the value is
// read back — it only counts once Brightpearl shows the right option.
let reasonOptions = null;
async function setCreditReason(bp, sc, name) {
  try {
    if (!reasonOptions) {
      const meta = (await bp("GET", "/order-service/sale/custom-field-meta-data")) || [];
      const f = meta.find((x) => x.code === "PCF_CRDTRSN");
      reasonOptions = Object.fromEntries(Object.values((f && f.options) || {}).map((o) => [o.value, Number(o.id)]));
    }
  } catch { reasonOptions = null; }
  const id = (reasonOptions && reasonOptions[name]) || { "Return for Exchange": 182, "Return for Refund": 183 }[name];
  if (!id) return false;
  for (const value of [{ id }, id, String(id)]) {
    try { await bp("PATCH", `/order-service/order/${sc}/custom-field`, [{ op: "add", path: "/PCF_CRDTRSN", value }]); }
    catch { continue; }
    try {
      const cf = (await bp("GET", `/order-service/order/${sc}/custom-field`)) || {};
      if (cf.PCF_CRDTRSN && Number(cf.PCF_CRDTRSN.id) === Number(id)) return true;
    } catch { /* try the next form */ }
  }
  return false;
}

const addressOf = (d) => d && {
  addressFullName: d.addressFullName || "", companyName: d.companyName || "",
  addressLine1: d.addressLine1 || "", addressLine2: d.addressLine2 || "", addressLine3: d.addressLine3 || "", addressLine4: d.addressLine4 || "",
  postalCode: d.postalCode || "", countryIsoCode: d.countryIsoCode3 || d.countryIsoCode || "GBR",
  telephone: d.telephone || "", mobileTelephone: d.mobileTelephone || "", email: d.email || "",
};
const rowBody = (r) => ({
  ...(r.productId ? { productId: r.productId } : {}),
  ...(r.productName ? { productName: bpSafeText(r.productName).slice(0, 250) } : {}),
  quantity: { magnitude: String(r.qty) }, nominalCode: String(r.nominalCode || "4000"),
  rowValue: { taxCode: r.taxCode || "T20", rowNet: { currency: "GBP", value: money(r.net) }, rowTax: { currency: "GBP", value: money(r.tax) } },
});

// ── The exchange order, raised the moment the customer asks (Dec + team, 7 Oct) ──────────────
// So the new item can be ordered in / got ready while the old one is in the post:
//   in stock      allocated, status 125 "Pick - Exch Order *DO NOT SHIP*"
//   not in stock  status 23 "Stock needs ordering" + PCF_SUPPLIER = the purchasing key(s) of the
//                 item's supplier, so the next purchasing run orders it ("/" between suppliers)
// The status is set in the SAME call that creates the order, so it never passes through a
// default status (which could trigger a Brightpearl customer email). Nothing here emails anyone.
// The order is flagged in the Sales Hub for a person to check the size before it ships; the
// credit is still only raised once the goods are back (executeBrightpearl reuses this order).
export const EXCH_HOLD_STATUS = 125;     // Pick - Exch Order *DO NOT SHIP*
export const EXCH_ORDER_STATUS = 23;     // Stock needs ordering
export const EXCH_RELEASE_STATUS = 48;   // In stock, pick/pack/ship - once the return is back

// Brightpearl supplier contact -> the purchasing run's key (its PCF_SUPPLIER vocabulary).
let supplierKeys = null;
async function supplierKeyFor(bp, contactId, productName) {
  if (!supplierKeys) {
    supplierKeys = {};
    try {
      const { SUPPLIERS } = await import("./purchasingAuto.js");
      for (const [key, s] of Object.entries(SUPPLIERS || {})) {
        if (!s || !s.contactId) continue;
        // Snickers and Hellberg share contact 331: the name decides.
        if (s.sharesContactWith && supplierKeys[s.contactId]) continue;
        supplierKeys[s.contactId] = key;
      }
    } catch { /* fall back to the contact's own name */ }
  }
  if (Number(contactId) === 331 && /hellberg/i.test(productName || "")) return "HELLBERG";
  if (supplierKeys[contactId]) return supplierKeys[contactId];
  try {   // a supplier we don't automate: its company name is what staff would type
    const c = await bp("GET", `/contact-service/contact/${contactId}`);
    const one = Array.isArray(c) ? c[0] : c;
    const name = (one && (one.organisation && one.organisation.name)) || [one && one.firstName, one && one.lastName].filter(Boolean).join(" ");
    return String(name || "").trim().toUpperCase() || null;
  } catch { return null; }
}

export async function raiseExchangeEarly(bp, ret, { returnRef } = {}) {
  const plan = await planBrightpearl(bp, ret);
  if (!plan.exchange || !plan.exchange.lines.length) return null;
  const o = plan.order, out = { warnings: [...plan.warnings], lines: [] };
  const label = `EXCHANGE ORDER - AWAITING RETURN ${returnRef}`;
  const header = {
    orderTypeCode: "SO", reference: bpSafeText(label),
    priceListId: o.priceListId, priceModeCode: o.priceModeCode, warehouseId: o.warehouseId,
    currency: { orderCurrencyCode: o.currency }, parentOrderId: o.id,
    parties: { customer: { contactId: o.customerId }, ...(o.delivery ? { delivery: addressOf(o.delivery) } : {}) },
    ...(o.channelId ? { assignment: { current: { channelId: o.channelId } } } : {}),
    ...(o.shippingMethodId ? { delivery: { shippingMethodId: o.shippingMethodId } } : {}),
  };

  // What goes on it, and whether we have it.
  const picks = plan.exchange.lines.map((l) => {
    const opt = l.suggested && (l.options || []).find((x) => Number(x.productId) === Number(l.suggested));
    return { l, opt };
  });
  const priced = picks.filter((p) => p.opt);
  const stock = priced.length ? await stockFor(bp, priced.map((p) => p.opt.productId)) : {};
  const short = priced.filter((p) => (stock[p.opt.productId] || 0) < p.l.qty);
  const allInStock = priced.length === picks.length && !short.length;
  const status = allInStock ? EXCH_HOLD_STATUS : EXCH_ORDER_STATUS;

  // Created straight into its status (no default status in between).
  const ex = await bp("POST", "/order-service/order", { ...header, orderStatusId: status });
  out.exchangeId = ex;
  await bp("POST", `/order-service/order/${ex}/row`, rowBody({ productId: 1000, productName: label, qty: 1, net: 0, tax: 0, taxCode: "T20", nominalCode: "4000" }));
  const reserve = [];
  for (const { l, opt } of picks) {
    if (opt) {
      const rowId = await bp("POST", `/order-service/order/${ex}/row`, rowBody({ productId: Number(opt.productId), qty: l.qty,
        net: round2(l.unitNet * l.qty), tax: round2(l.unitTax * l.qty), taxCode: l.taxCode, nominalCode: l.nominalCode }));
      const have = (stock[opt.productId] || 0) >= l.qty;
      if (have && rowId) reserve.push({ productId: String(opt.productId), salesOrderRowId: String(rowId), quantity: String(l.qty) });
      out.lines.push({ name: l.name, from: l.from && l.from.size, size: opt.size || opt.sku, sku: opt.sku, inStock: have });
    } else {
      const wants = l.choice === "Something else" ? l.text : `${String(l.choice || "").toLowerCase()} from ${l.from ? l.from.size : "the one returned"}`;
      await bp("POST", `/order-service/order/${ex}/row`, rowBody({ productId: 1000, productName: `TO ADD: customer wants ${wants} (instead of ${l.name})`,
        qty: l.qty, net: 0, tax: 0, taxCode: l.taxCode, nominalCode: l.nominalCode }));
      out.lines.push({ name: l.name, from: l.from && l.from.size, size: null, wants, inStock: false });
    }
  }
  // Belt and braces: if Brightpearl ignored the status on create, set it now.
  try {
    const now = await one(bp, ex);
    if (Number(now && now.orderStatus && now.orderStatus.orderStatusId) !== status) await bp("PUT", `/order-service/order/${ex}/status`, { orderStatusId: status });
  } catch (e) { out.warnings.push(`Status not confirmed on SO#${ex}: ${String(e.message).slice(0, 120)}`); }
  out.status = status;

  // Allocate what we have.
  if (reserve.length && o.warehouseId) {
    try { await bp("POST", `/warehouse-service/order/${ex}/reservation/warehouse/${o.warehouseId}`, { products: reserve }); out.allocated = reserve.length; }
    catch (e) { out.warnings.push(`Couldn't allocate stock on SO#${ex} - allocate it by hand (${String(e.message).slice(0, 120)})`); }
  }

  // Not in stock: tell the purchasing run who to order it from.
  if (!allInStock) {
    const keys = [];
    const short2 = priced.filter((p) => (stock[p.opt.productId] || 0) < p.l.qty);
    if (short2.length) {
      try {
        const sup = (await bp("GET", `/product-service/product/${[...new Set(short2.map((p) => Number(p.opt.productId)))].sort((a, b) => a - b).join(",")}/supplier`)) || {};
        for (const p of short2) {
          const contact = ((sup[p.opt.productId] || [])[0]);
          const key = contact ? await supplierKeyFor(bp, contact, p.l.name) : null;
          if (key && !keys.includes(key)) keys.push(key);
          if (!key) out.warnings.push(`No supplier on ${p.opt.sku || p.opt.productId} - fill in PCF_SUPPLIER on SO#${ex} by hand`);
        }
      } catch (e) { out.warnings.push(`Couldn't read the supplier - fill in PCF_SUPPLIER on SO#${ex} by hand`); }
    }
    if (keys.length) {
      const tag = keys.join(" / ");
      try { await bp("PATCH", `/order-service/order/${ex}/custom-field`, [{ op: "add", path: "/PCF_SUPPLIER", value: tag }]); out.supplierTag = tag; }
      catch (e) { out.warnings.push(`PCF_SUPPLIER not set on SO#${ex} - set it to "${tag}" by hand`); }
    }
    if (priced.length < picks.length) out.warnings.push(`SO#${ex} has a "TO ADD" line - add the item the customer wants`);
  }

  const note = (id, text) => bp("POST", `/order-service/order/${id}/note`, { text: bpSafeText(text) }).catch(() => null);
  const what = out.lines.map((x) => `${x.name}: ${x.size ? `send ${x.size}${x.from ? ` (was ${x.from})` : ""}${x.inStock ? " - allocated" : " - TO ORDER"}` : `TO ADD - ${x.wants}`}`).join("\n");
  await note(ex, `Exchange raised automatically from online return ${returnRef}, BEFORE the return has arrived.\nORIGINAL ORDER SO#${o.id}\n${what}\nDO NOT SHIP until the return is back - check the size first.`);
  await note(o.id, `Exchange order SO#${ex} raised early from online return ${returnRef} (awaiting the return).`);
  return out;
}

/**
 * Create it. `choices` = { [exchange line key]: productId | null } for the swaps (null = staff will
 * add the item). `progress(patch)` is called after every write so a failure part-way
 * leaves a record of what exists — a retry must never make a second credit.
 * `existingExchangeId`: the exchange order was raised early (raiseExchangeEarly) - it is reused,
 * not duplicated: it gets the credit's marker line and, if it was held, is released to ship.
 */
export async function executeBrightpearl(bp, plan, { choices = {}, returnRef, progress = async () => {}, existingExchangeId = null } = {}) {
  const o = plan.order, done = { steps: [] };
  const header = (extra) => ({
    priceListId: o.priceListId, priceModeCode: o.priceModeCode, warehouseId: o.warehouseId,
    currency: { orderCurrencyCode: o.currency }, parentOrderId: o.id,
    parties: { customer: { contactId: o.customerId }, ...(o.delivery ? { delivery: addressOf(o.delivery) } : {}) },
    ...(o.channelId ? { assignment: { current: { channelId: o.channelId } } } : {}),
    ...extra,
  });

  // 1. The credit.
  const sc = await bp("POST", "/order-service/order", header({ orderTypeCode: "SC", reference: bpSafeText(plan.credit.reference) }));
  done.creditId = sc; await progress({ bp_credit_id: sc });
  // First line says what kind of credit it is, so nobody refunds a swap by mistake
  // (Dec, 30 Sep). £0, product 1000, like the exchange order's own marker line.
  const kinds = new Set(plan.credit.rows.flatMap((r) => r.outcomes || [r.outcome]));
  const kind = kinds.has("refund") && kinds.has("exchange") ? "REFUND & EXCHANGE" : kinds.has("exchange") ? "EXCHANGE" : "REFUND";
  await bp("POST", `/order-service/order/${sc}/row`, rowBody({ productId: 1000, productName: kind, qty: 1, net: 0, tax: 0, taxCode: "T20", nominalCode: "4000" }));
  for (const r of plan.credit.rows) await bp("POST", `/order-service/order/${sc}/row`, rowBody(r));
  // Credit Reason (PCF_CRDTRSN, a dropdown) as the team sets it by hand. Mixed returns
  // count as a refund: money is owed back, and the first line says REFUND & EXCHANGE.
  const reasonName = kinds.has("refund") ? "Return for Refund" : "Return for Exchange";
  if (!(await setCreditReason(bp, sc, reasonName))) {
    (done.warnings = done.warnings || []).push(`Credit Reason not set on SC#${sc} - set it to "${reasonName}" by hand`);
  } else done.creditReason = reasonName;
  done.steps.push("credit");

  // 2. The exchange order.
  let exchangePriced = 0, allPriced = true;
  if (plan.exchange && existingExchangeId) {
    // Raised early: tie it to the credit and, if it was waiting on the return, release it.
    const ex = Number(existingExchangeId);
    done.exchangeId = ex; done.exchangeEarly = true;
    const marker = `EXCHANGE ORDER CREDITED VIA SC#${sc}`;
    await bp("POST", `/order-service/order/${ex}/row`, rowBody({ productId: 1000, productName: marker, qty: 1, net: 0, tax: 0, taxCode: "T20", nominalCode: "4000" }));
    try {
      const cur = await one(bp, ex);
      const st = Number(cur && cur.orderStatus && cur.orderStatus.orderStatusId);
      if (st === EXCH_HOLD_STATUS) { await bp("PUT", `/order-service/order/${ex}/status`, { orderStatusId: EXCH_RELEASE_STATUS }); done.exchangeReleased = true; }
      else done.exchangeLeftAt = st;
      exchangePriced = round2(Object.values((cur && cur.orderRows) || {}).reduce((a, r) =>
        a + Number((r.rowValue && r.rowValue.rowNet && r.rowValue.rowNet.value) || 0) + Number((r.rowValue && r.rowValue.rowTax && r.rowValue.rowTax.value) || 0), 0));
      allPriced = !Object.values((cur && cur.orderRows) || {}).some((r) => /^TO ADD:/i.test(String(r.productName || "")));
    } catch (e) { (done.warnings = done.warnings || []).push(`Couldn't release SO#${ex}: ${String(e.message).slice(0, 120)}`); }
    done.steps.push("exchange order (raised early)");
  } else if (plan.exchange) {
    const note = `EXCHANGE ORDER CREDITED VIA SC#${sc}`;
    const ex = await bp("POST", "/order-service/order", header({
      orderTypeCode: "SO", reference: note, ...(o.shippingMethodId ? { delivery: { shippingMethodId: o.shippingMethodId } } : {}),
    }));
    done.exchangeId = ex; await progress({ bp_exchange_id: ex });
    // First line, as on the team's own exchange orders (SO 492360): which credit pays for it.
    await bp("POST", `/order-service/order/${ex}/row`, rowBody({ productId: 1000, productName: note, qty: 1, net: 0, tax: 0, taxCode: "T20", nominalCode: "4000" }));
    let exGross = 0;
    for (const l of plan.exchange.lines) {
      const pick = Object.prototype.hasOwnProperty.call(choices, l.key) ? choices[l.key] : l.suggested;
      const ok = pick && (l.options || []).some((x) => Number(x.productId) === Number(pick));
      if (ok) {
        const net = round2(l.unitNet * l.qty), tax = round2(l.unitTax * l.qty);
        await bp("POST", `/order-service/order/${ex}/row`, rowBody({ productId: Number(pick), qty: l.qty, net, tax, taxCode: l.taxCode, nominalCode: l.nominalCode }));
        exGross += net + tax;
      } else {
        allPriced = false;
        // Nothing to price: say what they want, at £0, for staff to turn into an item.
        const wants = l.choice === "Something else" ? l.text : `${String(l.choice || "").toLowerCase()} from ${l.from ? l.from.size : "the one returned"}`;
        await bp("POST", `/order-service/order/${ex}/row`, rowBody({ productId: 1000, productName: `TO ADD: customer wants ${wants} (instead of ${l.name})`, qty: l.qty, net: 0, tax: 0, taxCode: l.taxCode, nominalCode: l.nominalCode }));
      }
    }
    done.steps.push("exchange order");

    // No payments between the two (Dec, 30 Sep): the credit (money owed back) and the
    // exchange order (money owed in) are left unpaid and cancel each other out against the
    // original, already-paid sale. A PAYMENT/RECEIPT pair is just two more transactions.
    exchangePriced = round2(exGross);
  }
  done.exchangeGross = exchangePriced;

  // 3. Credit status: a refund still has money to pay back (accounts are emailed); a
  // swap whose replacement is fully on the exchange order is finished; anything with a
  // "TO ADD" line is left open for staff.
  const refund = plan.credit.rows.some((r) => (r.outcomes || [r.outcome]).includes("refund"));
  const status = refund ? 121 : (plan.exchange && allPriced ? 11 : null);
  if (status) {
    // Everything exists by now; a status that will not set is reported, not fatal.
    try { await bp("PUT", `/order-service/order/${sc}/status`, { orderStatusId: status }); done.creditStatus = status; }
    catch (e) { (done.warnings = done.warnings || []).push(`Credit status ${status} not set: ${String(e.message).slice(0, 160)}`); }
  }

  // 4. Notes: the cross-reference the team writes by hand (SO 492360), on all three
  // orders, then where it came from.
  const xref = [`ORIGINAL ORDER SO#${o.id}`, ...(done.exchangeId ? [`REPLACED VIA SO#${done.exchangeId}`] : []), `CREDITED VIA SC#${sc}`].join("\n");
  const from = `Online return ${returnRef} (GBP ${money(plan.credit.gross)} credited)`;
  const note = (id, text) => bp("POST", `/order-service/order/${id}/note`, { text: bpSafeText(text) }).catch(() => null);
  await note(o.id, `${xref}\n${from}`);
  await note(sc, `${xref}\n${from}${refund ? "\nREFUND STILL TO BE PAID to the customer." : ""}`);
  if (done.exchangeId) await note(done.exchangeId, `${xref}\n${from}\nReplacement for the customer - free standard delivery.`);
  return done;
}
