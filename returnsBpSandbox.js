// TEMPORARY — proves the Brightpearl writes the returns flow needs, on the TEST account
// (tuffbsitc) only. Never touches live. Remove once returnsBp.js is proven.
//   GET /api/returns/bp-sandbox?step=all
export function registerReturnsBpSandbox(app, { bpTest }) {
  app.get("/api/returns/bp-sandbox", async (req, res) => {
    const log = [];
    const tryStep = async (name, fn) => {
      try { const out = await fn(); log.push({ step: name, ok: true, out }); return out; }
      catch (e) { log.push({ step: name, ok: false, error: String(e.message || e).slice(0, 600) }); return null; }
    };
    const one = async (id) => { const r = await bpTest("GET", `/order-service/order/${id}`); return Array.isArray(r) ? r[0] : r; };
    const brief = (o) => o && ({ id: o.id, type: o.orderTypeCode, parent: o.parentOrderId, ref: o.reference, status: o.orderStatus && o.orderStatus.orderStatusId,
      statusName: o.orderStatus && o.orderStatus.name, pay: o.orderPaymentStatus, channel: o.assignment && o.assignment.current && o.assignment.current.channelId,
      total: o.totalValue && o.totalValue.total, rows: Object.values(o.orderRows || {}).map((r) => [r.productId, r.productName && r.productName.slice(0, 40), r.quantity && r.quantity.magnitude, r.rowValue && r.rowValue.rowNet && r.rowValue.rowNet.value, r.nominalCode]),
      delivery: o.parties && o.parties.delivery && o.parties.delivery.postalCode, shipMethod: o.delivery && o.delivery.shippingMethodId });
    try {
      const customerId = Number(req.query.customer || 128071);
      const pid = Number(req.query.product || 143061);
      if (req.query.step === "addr") {
        // An order delivered to an address that is NOT the customer's default, with a delivery method.
        const id = await tryStep("create SO with delivery address + method", () => bpTest("POST", "/order-service/order", {
          orderTypeCode: "SO", reference: "RETURNS SANDBOX ADDR", priceListId: 3, priceModeCode: "EXC", warehouseId: 2,
          currency: { orderCurrencyCode: "GBP" }, assignment: { current: { channelId: 17 } },
          parties: { customer: { contactId: customerId }, delivery: { addressFullName: "Test Person", companyName: "Test Co", addressLine1: "1 Test Street",
            addressLine2: "Rothwell", addressLine3: "Leeds", addressLine4: "West Yorkshire", postalCode: "LS26 8LG", countryIsoCode: "GBR", telephone: "0113 000 0000", email: "test@example.com" } },
          delivery: { shippingMethodId: 104 },
        }));
        if (id) await tryStep("read", async () => { const o = await one(id); return { ...brief(o), deliveryParty: o.parties.delivery, deliveryBlock: o.delivery }; });
        return res.json({ account: "TEST (tuffbsitc)", log });
      }
      // 1. a sale to return against
      const soId = await tryStep("create SO", () => bpTest("POST", "/order-service/order", {
        orderTypeCode: "SO", reference: "RETURNS SANDBOX", priceListId: 3, priceModeCode: "EXC", warehouseId: 2,
        currency: { orderCurrencyCode: "GBP" }, parties: { customer: { contactId: customerId } },
        assignment: { current: { channelId: 17 } },
      }));
      if (soId) await tryStep("SO row", () => bpTest("POST", `/order-service/order/${soId}/row`, {
        productId: pid, quantity: { magnitude: "2" }, nominalCode: "4000",
        rowValue: { taxCode: "T20", rowNet: { currency: "GBP", value: "139.84" }, rowTax: { currency: "GBP", value: "27.97" } },
      }));
      // 2a. credit through the generic order endpoint, with parentOrderId
      const scA = soId && await tryStep("create SC (generic, parentOrderId)", () => bpTest("POST", "/order-service/order", {
        orderTypeCode: "SC", parentOrderId: soId, reference: "RETURNS SANDBOX / EXCHANGE", priceListId: 3, priceModeCode: "EXC", warehouseId: 2,
        currency: { orderCurrencyCode: "GBP" }, parties: { customer: { contactId: customerId } },
        assignment: { current: { channelId: 17 } },
      }));
      // 2b. credit through the sales-credit endpoint, with parentId
      const scB = soId && await tryStep("create SC (sales-credit, parentId)", () => bpTest("POST", "/order-service/sales-credit", {
        ref: "RETURNS SANDBOX B / EXCHANGE", parentId: soId, channelId: 17, priceListId: 3, priceModeCode: "EXC", warehouseId: 2,
        currency: { code: "GBP", fixedExchangeRate: true, exchangeRate: "1.0" }, customer: { id: customerId },
      }));
      const sc = scA || scB;
      if (sc) await tryStep("SC row", () => bpTest("POST", `/order-service/order/${sc}/row`, {
        productId: pid, quantity: { magnitude: "1" }, nominalCode: "4000",
        rowValue: { taxCode: "T20", rowNet: { currency: "GBP", value: "69.92" }, rowTax: { currency: "GBP", value: "13.98" } },
      }));
      // 3. exchange order
      const ex = soId && await tryStep("create exchange SO (parentOrderId)", () => bpTest("POST", "/order-service/order", {
        orderTypeCode: "SO", parentOrderId: soId, reference: `EXCHANGE ORDER CREDITED VIA SC#${sc}`, priceListId: 3, priceModeCode: "EXC", warehouseId: 2,
        currency: { orderCurrencyCode: "GBP" }, parties: { customer: { contactId: customerId } },
        assignment: { current: { channelId: 17 } },
      }));
      if (ex) {
        await tryStep("exchange row", () => bpTest("POST", `/order-service/order/${ex}/row`, {
          productId: Number(req.query.product2 || 144545), quantity: { magnitude: "1" }, nominalCode: "4000",
          rowValue: { taxCode: "T20", rowNet: { currency: "GBP", value: "69.92" }, rowTax: { currency: "GBP", value: "13.98" } },
        }));
        await tryStep("exchange note row (product 1000)", () => bpTest("POST", `/order-service/order/${ex}/row`, {
          productId: 1000, productName: `EXCHANGE ORDER CREDITED VIA SC#${sc}`, quantity: { magnitude: "1" }, nominalCode: "4000",
          rowValue: { taxCode: "T20", rowNet: { currency: "GBP", value: "0.00" }, rowTax: { currency: "GBP", value: "0.00" } },
        }));
        await tryStep("exchange free-text row (no productId)", () => bpTest("POST", `/order-service/order/${ex}/row`, {
          productName: "Customer wants: same trousers in navy", quantity: { magnitude: "1" }, nominalCode: "4000",
          rowValue: { taxCode: "T20", rowNet: { currency: "GBP", value: "0.00" }, rowTax: { currency: "GBP", value: "0.00" } },
        }));
      }
      // 4. move the money: payment out of the credit, receipt into the exchange order
      const today = new Date().toISOString().slice(0, 10);
      if (sc && ex) {
        await tryStep("payment methods", async () => ((await bpTest("GET", "/accounting-service/payment-method")) || []).map((m) => [m.id, m.code, m.name]));
        for (const code of String(req.query.method || "OTHER").split(",")) {
          await tryStep(`PAYMENT on SC (${code})`, () => bpTest("POST", "/accounting-service/customer-payment", {
            paymentMethodCode: code, paymentType: "PAYMENT", orderId: sc, currencyIsoCode: "GBP", exchangeRate: 1, amountPaid: 83.90, paymentDate: today, journalRef: "Returns sandbox transfer",
          }));
          await tryStep(`RECEIPT on exchange (${code})`, () => bpTest("POST", "/accounting-service/customer-payment", {
            paymentMethodCode: code, paymentType: "RECEIPT", orderId: ex, currencyIsoCode: "GBP", exchangeRate: 1, amountPaid: 83.90, paymentDate: today, journalRef: "Returns sandbox transfer",
          }));
        }
      }
      // 5. statuses
      if (sc) await tryStep("SC status 11", () => bpTest("PUT", `/order-service/order/${sc}/status`, { orderStatusId: 11 }));
      if (scB && scB !== sc) await tryStep("SC-B status 121", () => bpTest("PUT", `/order-service/order/${scB}/status`, { orderStatusId: 121 }));
      if (sc) await tryStep("SC note", () => bpTest("POST", `/order-service/order/${sc}/note`, { text: "Returns sandbox note", isPublic: false }));
      // 6. read it all back
      for (const id of [soId, scA, scB, ex].filter(Boolean)) await tryStep(`read ${id}`, async () => brief(await one(id)));
      if (sc && ex) await tryStep("payments", async () => {
        const out = [];
        for (const id of [sc, ex]) {
          const r = await bpTest("GET", `/accounting-service/customer-payment-search?orderId=${id}`);
          const ix = Object.fromEntries(r.metaData.columns.map((c, i) => [c.name, i]));
          for (const x of r.results) out.push([id, x[ix.paymentType], x[ix.paymentMethodCode], x[ix.amountPaid], x[ix.journalId]]);
        }
        return out;
      });
      res.json({ account: "TEST (tuffbsitc)", ids: { soId, scA, scB, ex }, log });
    } catch (e) { res.status(500).json({ error: e.message, log }); }
  });
}
