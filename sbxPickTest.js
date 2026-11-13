// sbxPickTest.js - SANDBOX ONLY (tuffbsitc, via purchasingAuto.bpApi). One-off test for the warehouse hub (Dec, 13 Nov):
// when a product sits in two locations, which one does a goods-out note take it from, and does a pick message
// (POST .../goods-out/{id}/pick) move it to the location really picked from? Puts stock in location A, then B, makes
// a sales order + allocation + goods-out note, reads the note's location, sends a pick for B, reads it again.
// Nothing here can reach the live account: bpApi is the TEST account's client.

const iso = () => new Date().toISOString().replace(/\.\d+Z$/, '+00:00');
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

export async function runPickTest(bpApi, { productId = null } = {}) {
  const log = [];
  const say = (k, v) => { log.push({ step: k, ...(v === undefined ? {} : { value: v }) }); return v; };
  const warehouses = await bpApi('GET', '/warehouse-service/warehouse');
  const W = Number(warehouses[0].id);
  say('warehouse', W);
  const def = Number(await bpApi('GET', `/warehouse-service/warehouse/${W}/location/default`));
  let locs = (await bpApi('GET', `/warehouse-service/warehouse/${W}/location`)) || [];
  // Two test locations of our own, made once and reused.
  const want = [['PICKTEST', 'A'], ['PICKTEST', 'B']];
  const ids = [];
  for (const [a, b] of want) {
    let l = locs.find((x) => x.groupingA === a && x.groupingB === b);
    if (!l) {
      const id = await bpApi('POST', `/warehouse-service/warehouse/${W}/location`, { groupingA: a, groupingB: b, groupingC: '1', groupingD: '1' });
      l = { id: Number(Array.isArray(id) ? id[0] : id) };
    }
    ids.push(Number(l.id));
  }
  const [A, B] = ids;
  say('locations', { default: def, A, B });

  // A recent sales order: its customer and price list for the test order, and a stock-tracked product.
  const search = await bpApi('GET', '/order-service/order-search?orderTypeId=1&pageSize=20&sort=orderId.DESC');
  const tplId = search.results[0][0];
  const [tpl] = await bpApi('GET', `/order-service/order/${tplId}`);
  if (!productId) {
    const ps = await bpApi('GET', '/product-service/product-search?pageSize=50');
    for (const row of ps.results) {
      const [p] = await bpApi('GET', `/product-service/product/${row[0]}`);
      if (p && p.stock && p.stock.stockTracked && !p.composition?.bundle) { productId = p.id; break; }
    }
  }
  say('product', productId);
  const avail = async (label) => {
    const r = await bpApi('GET', `/warehouse-service/product-availability/${productId}?includeOptional=breakDownByLocation`);
    return say(label, r?.[productId]?.warehouses?.[W]?.byLocation || {});
  };
  await avail('stock before');

  // Stock: 2 into A first (older), then 2 into B.
  await bpApi('POST', `/warehouse-service/warehouse/${W}/stock-correction`, { corrections: [{ quantity: 2, productId, locationId: A, reason: 'pick test A (older)', cost: { currency: 'GBP', value: '1.00' } }] });
  await sleep(2000);
  await bpApi('POST', `/warehouse-service/warehouse/${W}/stock-correction`, { corrections: [{ quantity: 2, productId, locationId: B, reason: 'pick test B (newer)', cost: { currency: 'GBP', value: '1.00' } }] });
  await avail('stock after adding 2 to A then 2 to B');

  // An order for 1, allocated, with a goods-out note.
  const so = await bpApi('POST', '/order-service/order', {
    orderTypeCode: 'SO', reference: 'PICK TEST (hub)', priceListId: tpl.priceListId, priceModeCode: tpl.priceModeCode,
    warehouseId: W, currency: { orderCurrencyCode: tpl.currency?.orderCurrencyCode || 'GBP' },
    parties: { customer: { contactId: tpl.parties.customer.contactId } },
  });
  say('sales order', so);
  const rowId = await bpApi('POST', `/order-service/order/${so}/row`, {
    productId, quantity: { magnitude: '1' }, nominalCode: '4000',
    rowValue: { taxCode: 'T20', rowNet: { currency: 'GBP', value: '1.00' }, rowTax: { currency: 'GBP', value: '0.20' } },
  });
  say('row', rowId);
  await bpApi('POST', `/warehouse-service/order/${so}/reservation/warehouse/${W}`, { products: [{ productId: String(productId), salesOrderRowId: String(rowId), quantity: '1' }] });
  const gon = await bpApi('POST', `/warehouse-service/order/${so}/goods-note/goods-out`, {
    warehouses: [{ releaseDate: iso(), warehouseId: W, transfer: false, products: [{ productId, salesOrderRowId: rowId, quantity: 1 }] }], priority: false,
  });
  const noteId = Number(Array.isArray(gon) ? gon[0] : gon);
  say('goods-out note', noteId);
  const where = async (label) => {
    const n = (await bpApi('GET', `/warehouse-service/order/${so}/goods-note/goods-out`))[noteId];
    const locsOn = Object.values(n.orderRows || {}).flat().map((x) => ({ locationId: x.locationId, quantity: x.quantity, name: x.locationId === A ? 'A (older stock)' : x.locationId === B ? 'B (newer stock)' : x.locationId === def ? 'default' : 'other' }));
    return say(label, locsOn);
  };
  await where('note takes it from (BP chose)');
  await avail('stock after the note');

  // The pick message: it came from B.
  let pickErr = null;
  try { await bpApi('POST', `/warehouse-service/order/${so}/goods-note/goods-out/${noteId}/pick`, { items: [{ salesOrderRowId: rowId, productId, locationId: B, quantity: 1 }] }); }
  catch (e) { pickErr = e.message; }
  say('pick message', pickErr ? { error: pickErr } : 'accepted');
  await where('note takes it from (after pick = B)');
  await avail('stock after the pick');
  return { ok: !pickErr, so, noteId, productId, A, B, log };
}
