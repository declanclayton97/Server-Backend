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
//   MONEY      the credit pays for the exchange: a PAYMENT out of the credit and a
//              RECEIPT into the exchange order, method OTHER, same amount. Both then read
//              PAID, exactly as the hand-made ones do.
//   STATUS     credit 121 "Refund requested" when anything is being refunded (the money
//              still has to go back), 11 "complete" when the exchange used all of it,
//              otherwise left at 10 for staff.
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
      for (const [id, a] of Object.entries(r)) out[id] = Number((a && a.total && a.total.inStock) || 0);
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

  const creditRows = [];
  for (const l of lines) {
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
      outcome: l.outcome, exchangeChoice: l.exchangeChoice || "", exchangeFor: l.exchangeFor || "", reason: l.reason || "",
    });
  }
  const creditNet = round2(creditRows.reduce((a, r) => a + r.net, 0));
  const creditTax = round2(creditRows.reduce((a, r) => a + r.tax, 0));
  const hasRefund = creditRows.some((r) => r.outcome === "refund");
  const hasExchange = creditRows.some((r) => r.outcome === "exchange");
  const ref = order.reference || String(order.id);

  // For each swap: the style's sizes in the same colour, with the one the customer
  // asked for picked out. Staff confirm or change it before anything is created.
  const exchangeLines = [];
  for (const c of creditRows.filter((r) => r.outcome === "exchange")) {
    const steps = SIZE_STEPS[c.exchangeChoice];
    const line = { rowId: c.rowId, name: c.name, qty: c.qty, choice: c.exchangeChoice, text: c.exchangeFor,
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

/**
 * Create it. `choices` = { [rowId]: productId | null } for the swaps (null = staff will
 * add the item). `progress(patch)` is called after every write so a failure part-way
 * leaves a record of what exists — a retry must never make a second credit.
 */
export async function executeBrightpearl(bp, plan, { choices = {}, returnRef, progress = async () => {} } = {}) {
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
  for (const r of plan.credit.rows) await bp("POST", `/order-service/order/${sc}/row`, rowBody(r));
  done.steps.push("credit");

  // 2. The exchange order.
  let transferred = 0;
  if (plan.exchange) {
    const note = `EXCHANGE ORDER CREDITED VIA SC#${sc}`;
    const ex = await bp("POST", "/order-service/order", header({
      orderTypeCode: "SO", reference: note, ...(o.shippingMethodId ? { delivery: { shippingMethodId: o.shippingMethodId } } : {}),
    }));
    done.exchangeId = ex; await progress({ bp_exchange_id: ex });
    let exGross = 0;
    for (const l of plan.exchange.lines) {
      const pick = Object.prototype.hasOwnProperty.call(choices, l.rowId) ? choices[l.rowId] : l.suggested;
      const ok = pick && (l.options || []).some((x) => Number(x.productId) === Number(pick));
      if (ok) {
        const net = round2(l.unitNet * l.qty), tax = round2(l.unitTax * l.qty);
        await bp("POST", `/order-service/order/${ex}/row`, rowBody({ productId: Number(pick), qty: l.qty, net, tax, taxCode: l.taxCode, nominalCode: l.nominalCode }));
        exGross += net + tax;
      } else {
        // Nothing to price: say what they want, at £0, for staff to turn into an item.
        const wants = l.choice === "Something else" ? l.text : `${String(l.choice || "").toLowerCase()} from ${l.from ? l.from.size : "the one returned"}`;
        await bp("POST", `/order-service/order/${ex}/row`, rowBody({ productId: 1000, productName: `TO ADD: customer wants ${wants} (instead of ${l.name})`, qty: l.qty, net: 0, tax: 0, taxCode: l.taxCode, nominalCode: l.nominalCode }));
      }
    }
    await bp("POST", `/order-service/order/${ex}/row`, rowBody({ productId: 1000, productName: note, qty: 1, net: 0, tax: 0, taxCode: "T20", nominalCode: "4000" }));
    done.steps.push("exchange order");

    // 3. The credit pays for the exchange — never more than the credit is worth.
    transferred = round2(Math.min(exGross, plan.credit.gross));
    if (transferred > 0) {
      const today = new Date().toISOString().slice(0, 10);
      const journalRef = bpSafeText(`Online return ${returnRef || ""} exchange`).trim();
      await bp("POST", "/accounting-service/customer-payment", { paymentMethodCode: "OTHER", paymentType: "PAYMENT", orderId: sc, currencyIsoCode: "GBP", exchangeRate: 1, amountPaid: transferred, paymentDate: today, journalRef });
      await progress({ bp_transfer_out: true });
      await bp("POST", "/accounting-service/customer-payment", { paymentMethodCode: "OTHER", paymentType: "RECEIPT", orderId: ex, currencyIsoCode: "GBP", exchangeRate: 1, amountPaid: transferred, paymentDate: today, journalRef });
      done.steps.push(`moved GBP ${money(transferred)} from the credit to the exchange order`);
    }
  }
  done.transferred = transferred;

  // 4. Credit status: refunds still owe money; a fully-used exchange credit is finished.
  const refund = plan.credit.rows.some((r) => r.outcome === "refund");
  const status = refund ? 121 : (transferred >= plan.credit.gross - 0.005 ? 11 : null);
  if (status) { await bp("PUT", `/order-service/order/${sc}/status`, { orderStatusId: status }); done.creditStatus = status; }

  // 5. Notes, so each record says where it came from.
  const summary = `Online return ${returnRef}: credit SC#${sc} (GBP ${money(plan.credit.gross)})${done.exchangeId ? `, exchange order SO#${done.exchangeId}` : ""}.`;
  const note = (id, text) => bp("POST", `/order-service/order/${id}/note`, { text: bpSafeText(text) }).catch(() => null);
  await note(o.id, summary);
  await note(sc, `${summary}${refund ? " REFUND STILL TO BE PAID to the customer." : ""}`);
  if (done.exchangeId) await note(done.exchangeId, `${summary} Replacement for the customer - free standard delivery.`);
  return done;
}
