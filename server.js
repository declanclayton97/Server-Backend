// SFTP HTTPS Proxy for Codesandbox React mockup workflow
import express from "express";
import SFTPClient from "ssh2-sftp-client";
import dotenv from "dotenv";
import { Readable } from "stream";
import DocuSignService from './docusignService.js';
import cors from 'cors';
import docusign from 'docusign-esign';
import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';
import pkg from 'pg';
import { PDFDocument, rgb } from 'pdf-lib';
import nodemailer from 'nodemailer';
const { Pool } = pkg;

// Get directory name for ES modules
const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

dotenv.config();

// Initialize PostgreSQL connection (for production) with fallback to JSON (for local dev)
const useDatabase = !!process.env.DATABASE_URL;
let pool;

if (useDatabase) {
  pool = new Pool({
    connectionString: process.env.DATABASE_URL,
    ssl: process.env.DATABASE_URL?.includes('localhost') ? false : {
      rejectUnauthorized: false
    }
  });
  console.log('📊 Using PostgreSQL database for DocuSign logs');
} else {
  console.log('📝 Using JSON file storage for DocuSign logs (local development)');
}

const app = express();
const PORT = process.env.PORT || 3000;

app.use(express.json({ limit: '50mb' }));
app.use(express.urlencoded({ limit: '50mb', extended: true }));
app.use(cors({
  origin: true,
  credentials: true
}));
app.use(express.raw({ limit: '50mb', type: 'application/octet-stream' }));

// Static email assets (logo, social icons) referenced from chase-email HTML.
// Served from https://<host>/email-assets/imageXXX.png
app.use('/email-assets', express.static(path.join(__dirname, 'email-assets'), {
  maxAge: '7d',
  immutable: true,
}));

// Brightpearl configuration
const BRIGHTPEARL_DATACENTER = process.env.BRIGHTPEARL_DATACENTER || 'use1';
const BRIGHTPEARL_ACCOUNT_ID = process.env.BRIGHTPEARL_ACCOUNT_ID;
const BRIGHTPEARL_API_TOKEN = process.env.BRIGHTPEARL_API_TOKEN;


// CORS middleware
app.use((req, res, next) => {
  res.setHeader("Access-Control-Allow-Origin", "*");
  res.setHeader("Access-Control-Allow-Methods", "GET, POST, OPTIONS, PUT, DELETE");
  res.setHeader("Access-Control-Allow-Headers", "Content-Type, Authorization, X-Requested-With");
  res.setHeader("Access-Control-Max-Age", "86400"); // 24 hours
  if (req.method === "OPTIONS") {
    return res.sendStatus(200);
  }
  next();
});

// Home route
app.get("/", (req, res) => {
  res.send("SFTP Proxy for Mockup Sheets is running");
});

// SFTP image route
app.get("/image", async (req, res) => {
  const productCode = req.query.code;
  if (!productCode) {
    return res.status(400).send('Missing "code" query parameter');
  }
  
  const filePath = `/LOW_RES_JPG/${productCode}.jpg`;
  const sftp = new SFTPClient();
  
  try {
    await sftp.connect({
      host: "prodinfrargftp.blob.core.windows.net",
      port: process.env.SFTP_PORT || 22,
      username: "prodinfrargftp.internal.prodimage",
      password: process.env.SFTP_PASSWORD,
    });
    
    const streamOrBuffer = await sftp.get(filePath);
    res.setHeader("Content-Type", "image/jpeg");
    
    if (streamOrBuffer.pipe) {
      streamOrBuffer.pipe(res);
      streamOrBuffer.on("end", async () => {
        await sftp.end();
      });
    } else {
      res.end(streamOrBuffer);
      await sftp.end();
    }
  } catch (error) {
    console.error("Error fetching image from SFTP:", error.message);
    if (!res.headersSent) {
      res.status(500).send("Error fetching image from SFTP.");
    }
    try {
      await sftp.end();
    } catch {}
  }
});

// Helly Hansen returns a 26829-byte placeholder image (HTTP 200) for missing
// product/colour combinations. Treat that as a 404 so frontend probing moves on.
const HH_PLACEHOLDER_BYTES = 26829;

// Image proxy route
app.get("/fetch-image", async (req, res) => {
  const imageUrl = req.query.url;
  if (!imageUrl) {
    return res.status(400).send('Missing "url" query parameter');
  }

  try {
    const response = await fetch(imageUrl);
    if (!response.ok) throw new Error(`Failed to fetch ${imageUrl}`);

    if (imageUrl.includes("hhworkwear.com")) {
      const len = parseInt(response.headers.get("content-length") || "0", 10);
      if (len === HH_PLACEHOLDER_BYTES) {
        return res.status(404).send("HH placeholder image — treated as missing");
      }
    }

    const contentType = response.headers.get("content-type") || "image/jpeg";
    res.setHeader("Content-Type", contentType);
    
    const reader = response.body.getReader();
    const stream = new Readable({
      async read() {
        try {
          while (true) {
            const { done, value } = await reader.read();
            if (done) break;
            this.push(value);
          }
          this.push(null);
        } catch (err) {
          this.destroy(err);
        }
      },
    });
    stream.pipe(res);
  } catch (error) {
    console.error(`Error proxying image from ${imageUrl}:`, error.message);
    res.status(500).send(`Error proxying image: ${error.message}`);
  }
});

app.get("/api/brightpearl/order/:orderId/custom-fields", async (req, res) => {
  try {
    const { orderId } = req.params;
    console.log('Fetching Brightpearl custom fields for order:', orderId);

    if (!BRIGHTPEARL_API_TOKEN || !BRIGHTPEARL_ACCOUNT_ID) {
      return res.status(500).json({ error: 'Brightpearl credentials not configured' });
    }

    const baseUrl = BRIGHTPEARL_DATACENTER === 'euw1'
      ? 'https://euw1.brightpearlconnect.com'
      : 'https://use1.brightpearlconnect.com';

    const url = `${baseUrl}/public-api/${BRIGHTPEARL_ACCOUNT_ID}/order-service/order/${orderId}/custom-field`;
    console.log('Fetching custom fields from:', url);

    const response = await fetch(url, {
      method: 'GET',
      headers: {
        'brightpearl-app-ref': process.env.BRIGHTPEARL_APP_REF,
        'brightpearl-account-token': BRIGHTPEARL_API_TOKEN,
        'Content-Type': 'application/json'
      }
    });

    const responseText = await response.text();

    if (!response.ok) {
      console.error('Brightpearl custom fields response:', responseText);
      return res.status(response.status).json({
        error: responseText
      });
    }

    const data = JSON.parse(responseText);
    res.json(data);
  } catch (error) {
    console.error('Custom fields fetch error:', error);
    res.status(500).json({ error: error.message });
  }
});

app.get("/api/brightpearl/order/:orderId", async (req, res) => {
  try {
    const { orderId } = req.params;
    console.log('Fetching Brightpearl order:', orderId);
    
    if (!BRIGHTPEARL_API_TOKEN || !BRIGHTPEARL_ACCOUNT_ID) {
      return res.status(500).json({ error: 'Brightpearl credentials not configured' });
    }
    
    // Use the correct Brightpearl Connect URL format
    const baseUrl = BRIGHTPEARL_DATACENTER === 'euw1' 
      ? 'https://euw1.brightpearlconnect.com'
      : 'https://use1.brightpearlconnect.com';
    
    // Correct path structure as confirmed by Brightpearl support
    const url = `${baseUrl}/public-api/${BRIGHTPEARL_ACCOUNT_ID}/order-service/order/${orderId}`;
    console.log('Using correct URL:', url);
    
    const response = await fetch(url, {
      method: 'GET',
      headers: {
        'brightpearl-app-ref': process.env.BRIGHTPEARL_APP_REF,
        'brightpearl-account-token': BRIGHTPEARL_API_TOKEN,
        'Content-Type': 'application/json'
      }
    });
    
    const responseText = await response.text();
    
    if (!response.ok) {
      console.error('Brightpearl response:', responseText);
      return res.status(response.status).json({ 
        error: responseText 
      });
    }
    
    const data = JSON.parse(responseText);
    res.json(data);
  } catch (error) {
    console.error('Order fetch error:', error);
    res.status(500).json({ error: error.message });
  }
});

// Also update the product endpoint
app.get("/api/brightpearl/product/:productId", async (req, res) => {
  try {
    const { productId } = req.params;
    
    if (!BRIGHTPEARL_API_TOKEN || !BRIGHTPEARL_ACCOUNT_ID) {
      return res.status(500).json({ error: 'Brightpearl credentials not configured' });
    }
    
    const baseUrl = BRIGHTPEARL_DATACENTER === 'euw1' 
      ? 'https://euw1.brightpearlconnect.com'
      : 'https://use1.brightpearlconnect.com';
    
    const url = `${baseUrl}/public-api/${BRIGHTPEARL_ACCOUNT_ID}/product-service/product/${productId}`;
    
    const response = await fetch(url, {
      method: 'GET',
      headers: {
        'brightpearl-app-ref': process.env.BRIGHTPEARL_APP_REF,
        'brightpearl-account-token': BRIGHTPEARL_API_TOKEN,
        'Content-Type': 'application/json'
      }
    });
    
    if (!response.ok) {
      const errorText = await response.text();
      console.error('Product API error:', errorText);
      return res.status(response.status).json({ 
        error: errorText 
      });
    }
    
    const data = await response.json();
    res.json(data);
  } catch (error) {
    console.error('Product fetch error:', error);
    res.status(500).json({ error: error.message });
  }
});

app.get("/api/brightpearl/order/:orderId/availability", async (req, res) => {
  try {
    const { orderId } = req.params;
    
    if (!BRIGHTPEARL_API_TOKEN || !BRIGHTPEARL_ACCOUNT_ID) {
      return res.status(500).json({ error: 'Brightpearl credentials not configured' });
    }
    
    const baseUrl = BRIGHTPEARL_DATACENTER === 'euw1' 
      ? 'https://euw1.brightpearlconnect.com'
      : 'https://use1.brightpearlconnect.com';
    
    // Get product availability/allocation
    const url = `${baseUrl}/public-api/${BRIGHTPEARL_ACCOUNT_ID}/warehouse-service/product-availability`;
    
    const response = await fetch(url, {
      method: 'GET',
      headers: {
        'brightpearl-app-ref': process.env.BRIGHTPEARL_APP_REF,
        'brightpearl-account-token': BRIGHTPEARL_API_TOKEN,
        'Content-Type': 'application/json'
      }
    });
    
    const responseText = await response.text();
    
    if (!response.ok) {
      console.error('Brightpearl response:', responseText);
      return res.status(response.status).json({ 
        error: responseText 
      });
    }
    
    const data = JSON.parse(responseText);
    res.json(data);
  } catch (error) {
    console.error('Availability fetch error:', error);
    res.status(500).json({ error: error.message });
  }
});

app.get('/api/brightpearl/proof-required', async (req, res) => {
  try {
    if (!BRIGHTPEARL_API_TOKEN || !BRIGHTPEARL_ACCOUNT_ID) {
      return res.status(500).json({ error: 'Brightpearl credentials not configured' });
    }

    const proofRequiredStatusId = '34';
    
    const baseUrl = BRIGHTPEARL_DATACENTER === 'euw1' 
      ? 'https://euw1.brightpearlconnect.com'
      : 'https://use1.brightpearlconnect.com';
    
    // Search for orders with the specific status
    const url = `${baseUrl}/public-api/${BRIGHTPEARL_ACCOUNT_ID}/order-service/order-search?orderStatusId=${proofRequiredStatusId}&pageSize=50&firstResult=1`;
    
    const response = await fetch(url, {
      method: 'GET',
      headers: {
        'brightpearl-app-ref': process.env.BRIGHTPEARL_APP_REF,
        'brightpearl-account-token': BRIGHTPEARL_API_TOKEN,
        'Content-Type': 'application/json'
      }
    });
    
    if (!response.ok) {
      const errorText = await response.text();
      console.error('Order search error:', errorText);
      return res.status(response.status).json({ error: errorText });
    }
    
    const data = await response.json();
    
    // Check if we have results
    if (!data.response || !data.response.results || data.response.results.length === 0) {
      return res.json([]);
    }
    
    // The results array contains order IDs in a specific format
    // We need to extract just the order IDs (first column of each result)
    let orderIds = [];
    
    // If results is an array of arrays (each order as an array of fields)
    if (Array.isArray(data.response.results[0])) {
      orderIds = data.response.results.map(row => row[0]); // First element is usually the order ID
    } 
    // If results is just an array of order IDs
    else if (typeof data.response.results[0] === 'number' || typeof data.response.results[0] === 'string') {
      orderIds = data.response.results;
    }
    // Handle the case where it might be returning full records
    else {
      console.log('Unexpected result format, attempting to extract IDs');
      // Try to extract IDs from the comma-separated string if that's what we got
      const resultString = data.response.results.toString();
      const parts = resultString.split(',');
      // Take every nth element that looks like an order ID
      orderIds = parts.filter((part, index) => {
        return index % 20 === 0 && /^\d+$/.test(part.trim());
      }).map(id => id.trim());
    }
    
    console.log('Extracted order IDs:', orderIds);
    
    if (orderIds.length === 0) {
      return res.json([]);
    }
    
    // Now fetch the details for these orders
    // Limit to first 10 orders to avoid URL being too long
    const limitedOrderIds = orderIds.slice(0, 10);
    const orderRange = limitedOrderIds.join(',');
    const detailsUrl = `${baseUrl}/public-api/${BRIGHTPEARL_ACCOUNT_ID}/order-service/order/${orderRange}`;
    
    console.log('Fetching order details from:', detailsUrl);
    
    const detailsResponse = await fetch(detailsUrl, {
      method: 'GET',
      headers: {
        'brightpearl-app-ref': process.env.BRIGHTPEARL_APP_REF,
        'brightpearl-account-token': BRIGHTPEARL_API_TOKEN,
        'Content-Type': 'application/json'
      }
    });
    
    if (!detailsResponse.ok) {
      const errorText = await detailsResponse.text();
      console.error('Order details error:', errorText);
      return res.status(detailsResponse.status).json({ error: errorText });
    }
    
    const detailsData = await detailsResponse.json();
    
    // Transform the data to match what your frontend expects
    const orders = detailsData.response.map(order => ({
      orderId: order.id,
      orderReference: order.reference,
      customerName: order.parties?.customer?.contactName || 
                    order.parties?.delivery?.addressFullName || 
                    order.parties?.customer?.addressFullName ||
                    'Unknown',
      placedOn: order.placedOn,
      deliveryDate: order.delivery?.deliveryDate || null
    }));
    
    res.json(orders);
  } catch (error) {
    console.error('Error fetching proof required orders:', error);
    res.status(500).json({ error: error.message });
  }
});

// Helper: does a custom-field value count as "Yes"?
function isBadgeYes(value) {
  if (value === true) return true;
  if (typeof value === 'number') return value === 1;
  if (typeof value === 'string') {
    const v = value.trim().toLowerCase();
    return v === 'yes' || v === 'y' || v === 'true' || v === '1';
  }
  return false;
}

// Name Badges: list orders in channel 22 where custom field PCF_BADGE = "Yes"
// ?debug=1                    → raw diagnostic output (existing search + verify)
// ?debugOrder=<orderId>       → inspect one specific order's custom-fields + channel
// ?probe=1                    → try multiple filter syntaxes, report which Brightpearl honors
app.get('/api/brightpearl/name-badges', async (req, res) => {
  try {
    const debug = req.query.debug === '1';
    const debugOrder = req.query.debugOrder;
    const probe = req.query.probe === '1';

    if (!BRIGHTPEARL_API_TOKEN || !BRIGHTPEARL_ACCOUNT_ID) {
      return res.status(500).json({ error: 'Brightpearl credentials not configured' });
    }

    const baseUrl = BRIGHTPEARL_DATACENTER === 'euw1'
      ? 'https://euw1.brightpearlconnect.com'
      : 'https://use1.brightpearlconnect.com';

    const headers = {
      'brightpearl-app-ref': process.env.BRIGHTPEARL_APP_REF,
      'brightpearl-account-token': BRIGHTPEARL_API_TOKEN,
      'Content-Type': 'application/json'
    };

    // ?probeContact=<orderId> — test contactId-based filtering: pull contactId
    // from a known order, then count how many other orders share it. If that
    // contactId covers all Travelex (or Asda) orders, contactId becomes the
    // selective filter we need.
    if (req.query.probeContact) {
      const seedOrderId = req.query.probeContact;
      const out = { seedOrderId };

      // Get the seed order's contactId + customer info
      const r1 = await fetch(`${baseUrl}/public-api/${BRIGHTPEARL_ACCOUNT_ID}/order-service/order/${seedOrderId}`, { method: 'GET', headers });
      const order = (await r1.json()).response?.[0];
      const contactId =
        order?.parties?.customer?.contactId ||
        order?.parties?.customer?.id ||
        order?.contactId;
      out.seedOrderContactId = contactId;
      out.seedOrderCustomer = {
        companyName: order?.parties?.customer?.companyName,
        contactName: order?.parties?.customer?.contactName,
        email: order?.parties?.customer?.email,
        addressFullName: order?.parties?.customer?.addressFullName,
      };
      out.seedOrderChannelId = order?.assignment?.current?.channelId;
      await new Promise((r) => setTimeout(r, 500));

      // Count orders that share the same contactId (newest first if possible)
      if (contactId) {
        const url = `${baseUrl}/public-api/${BRIGHTPEARL_ACCOUNT_ID}/order-service/order-search?contactId=${contactId}&pageSize=20&firstResult=1`;
        out.searchUrl = url;
        const r2 = await fetch(url, { method: 'GET', headers });
        const data = await r2.json().catch(() => null);
        out.searchStatus = r2.status;
        out.totalOrdersForContact = data?.response?.metaData?.resultsAvailable;
        const rows = data?.response?.results || [];
        out.first20OrderIds = Array.isArray(rows[0]) ? rows.map(x => x[0]) : rows;
      }

      return res.json({ probeContact: true, ...out });
    }

    // Probe mode: dump search metadata + try alternate channel filter syntaxes,
    // and fetch a returned order directly to confirm its actual channel.
    if (probe) {
      const out = {};

      // 1) Get the search metadata — tells us every column Brightpearl exposes,
      //    including which operators each column supports for filtering.
      const metaUrl = `${baseUrl}/public-api/${BRIGHTPEARL_ACCOUNT_ID}/order-service/order-search?pageSize=1&firstResult=1`;
      try {
        const r = await fetch(metaUrl, { method: 'GET', headers });
        const body = await r.json().catch(() => null);
        out.metaData = body?.response?.metaData || null;
      } catch (err) {
        out.metaDataError = err.message;
      }
      await new Promise((r) => setTimeout(r, 600));

      // 2) Fetch order 100086 directly — what does it ACTUALLY say its channel is?
      //    If channelId !== 22, the prior filter was being ignored.
      try {
        const r = await fetch(`${baseUrl}/public-api/${BRIGHTPEARL_ACCOUNT_ID}/order-service/order/100086`, { method: 'GET', headers });
        const body = await r.json().catch(() => null);
        const order = body?.response?.[0];
        out.order100086 = {
          status: r.status,
          orderId: order?.id,
          reference: order?.reference,
          channelId: order?.assignment?.current?.channelId,
          orderStatusId: order?.orderStatusId,
          placedOn: order?.placedOn,
        };
      } catch (err) {
        out.order100086Error = err.message;
      }
      await new Promise((r) => setTimeout(r, 600));

      // 3) Try alternate channel-related filter syntaxes
      const channelVariants = [
        { label: 'channelId=22',          qs: 'channelId=22' },
        { label: 'salesChannelId=22',     qs: 'salesChannelId=22' },
        { label: 'channelTypeId=22',      qs: 'channelTypeId=22' },
        { label: 'siteCode=22',           qs: 'siteCode=22' },
        { label: 'orderStatusId=34 (control)', qs: 'orderStatusId=34' },
      ];
      const variantResults = [];
      for (const v of channelVariants) {
        const url = `${baseUrl}/public-api/${BRIGHTPEARL_ACCOUNT_ID}/order-service/order-search?${v.qs}&pageSize=5&firstResult=1`;
        try {
          const r = await fetch(url, { method: 'GET', headers });
          const body = await r.json().catch(() => null);
          const rows = body?.response?.results || [];
          const ids = Array.isArray(rows[0]) ? rows.map(x => x[0]) : rows;
          variantResults.push({
            label: v.label,
            status: r.status,
            resultCount: rows.length,
            firstFiveIds: ids.slice(0, 5),
            errorBody: r.ok ? undefined : body,
          });
        } catch (err) {
          variantResults.push({ label: v.label, error: err.message });
        }
        await new Promise((r) => setTimeout(r, 600));
      }
      out.channelFilterVariants = variantResults;

      return res.json({ probe: true, ...out });
    }

    // Inspect one specific order — bypasses search/pagination entirely
    if (debugOrder) {
      const cfUrl = `${baseUrl}/public-api/${BRIGHTPEARL_ACCOUNT_ID}/order-service/order/${debugOrder}/custom-field`;
      const orderUrl = `${baseUrl}/public-api/${BRIGHTPEARL_ACCOUNT_ID}/order-service/order/${debugOrder}`;
      const [cfResp, orderResp] = await Promise.all([
        fetch(cfUrl, { method: 'GET', headers }),
        fetch(orderUrl, { method: 'GET', headers }),
      ]);
      const cfText = await cfResp.text();
      let cfParsed;
      try { cfParsed = JSON.parse(cfText); } catch { cfParsed = cfText; }
      const orderData = await orderResp.json().catch(() => null);
      const order = orderData?.response?.[0];
      const fields = cfParsed?.response || cfParsed || {};
      return res.json({
        orderId: debugOrder,
        customFieldStatus: cfResp.status,
        customFieldKeys: typeof fields === 'object' ? Object.keys(fields) : null,
        rawCustomFieldResponse: cfParsed,
        orderChannelId: order?.assignment?.current?.channelId,
        orderReference: order?.reference,
        pcfBadgeValue: fields?.PCF_BADGE,
        pcfBadgePassesYesCheck: isBadgeYes(fields?.PCF_BADGE),
      });
    }

    // Filter by Brightpearl department — confirmed via the user's admin report
    // URL (department_id[]=22) and metaData (departmentId is filterable). All
    // Asda/Travelex badge orders sit in department 22, regardless of which
    // contactId the order is on, so this avoids maintaining a contact list.
    const departmentId = req.query.departmentId || process.env.BADGE_DEPARTMENT_ID || '22';

    // Narrow further by updatedOn=last30days. Flipping PCF_BADGE refreshes
    // the order's updatedOn, so any currently-flagged order is in the window.
    const days = Math.min(parseInt(req.query.days, 10) || 30, 90);
    const fromIso = new Date(Date.now() - days * 24 * 60 * 60 * 1000).toISOString();
    const updatedFilter = encodeURIComponent(`${fromIso}/`);

    const searchUrl = `${baseUrl}/public-api/${BRIGHTPEARL_ACCOUNT_ID}/order-service/order-search?departmentId=${departmentId}&updatedOn=${updatedFilter}&pageSize=500&firstResult=1`;
    const searchUrls = [searchUrl];

    const searchResp = await fetch(searchUrl, { method: 'GET', headers });
    if (!searchResp.ok) {
      const errorText = await searchResp.text();
      console.error('Name badges order-search error:', errorText);
      if (searchResp.status === 503 || searchResp.status === 429) {
        return res.status(searchResp.status).json({ error: 'Brightpearl rate limit hit — please wait 30 seconds before refreshing.' });
      }
      return res.status(searchResp.status).json({ error: errorText });
    }

    const searchData = await searchResp.json();
    const rows = searchData.response?.results || [];
    const allOrderIds = rows.length === 0
      ? []
      : Array.isArray(rows[0]) ? rows.map((r) => r[0]) : rows;

    if (allOrderIds.length === 0) {
      if (debug) return res.json({ debug: true, searchUrls, departmentId, windowDays: days, ordersInDepartment: 0 });
      return res.json([]);
    }

    // Bulk-fetch order details in batches of 50 — gives us customer info for
    // the queue display. Order detail does NOT include custom fields directly,
    // so PCF_BADGE has to come from a separate /custom-field call (next step).
    const detailMap = new Map();
    for (let i = 0; i < allOrderIds.length; i += 50) {
      const batch = allOrderIds.slice(i, i + 50);
      const detailUrl = `${baseUrl}/public-api/${BRIGHTPEARL_ACCOUNT_ID}/order-service/order/${batch.join(',')}`;
      const r = await fetch(detailUrl, { method: 'GET', headers });
      if (!r.ok) continue;
      const data = await r.json();
      for (const order of (data.response || [])) {
        detailMap.set(order.id, order);
      }
    }

    // Custom-field check on the filtered subset (department-22 + recent
    // updates is already small, so per-order /custom-field calls are fine).
    const matches = [];
    const debugSamples = [];
    await Promise.all(allOrderIds.map(async (orderId) => {
      try {
        const cfUrl = `${baseUrl}/public-api/${BRIGHTPEARL_ACCOUNT_ID}/order-service/order/${orderId}/custom-field`;
        const cfResp = await fetch(cfUrl, { method: 'GET', headers });
        if (!cfResp.ok) return;
        const cfData = await cfResp.json();
        const fields = cfData.response || cfData || {};
        if (debug && debugSamples.length < 5) {
          debugSamples.push({ orderId, badgeValue: fields.PCF_BADGE });
        }
        if (isBadgeYes(fields.PCF_BADGE)) matches.push(orderId);
      } catch (err) {
        console.error(`PCF_BADGE check failed for order ${orderId}:`, err.message);
      }
    }));

    if (debug) {
      return res.json({
        debug: true,
        departmentId,
        windowDays: days,
        searchUrls,
        ordersInDepartment: allOrderIds.length,
        sampleBadgeValues: debugSamples,
        matchedOrderIds: matches,
      });
    }

    if (matches.length === 0) {
      return res.json([]);
    }

    const orders = matches
      .map((id) => detailMap.get(id))
      .filter(Boolean)
      .map((order) => ({
        orderId: order.id,
        orderReference: order.reference,
        customerName: order.parties?.customer?.companyName ||
                      order.parties?.customer?.contactName ||
                      order.parties?.delivery?.addressFullName ||
                      order.parties?.customer?.addressFullName ||
                      'Unknown',
        placedOn: order.placedOn,
        deliveryDate: order.delivery?.deliveryDate || null,
      }));

    res.json(orders);
  } catch (error) {
    console.error('Error fetching name badge orders:', error);
    res.status(500).json({ error: error.message });
  }
});

// Update a custom field on a Brightpearl sales order.
// Frontend sends a simple object: { "PCF_BADGE": "No" }
// Brightpearl expects JSON Patch format: [{"op":"replace","path":"/PCF_BADGE","value":"No"}]
app.patch('/api/brightpearl/order/:orderId/custom-fields', async (req, res) => {
  try {
    const { orderId } = req.params;

    if (!BRIGHTPEARL_API_TOKEN || !BRIGHTPEARL_ACCOUNT_ID) {
      return res.status(500).json({ error: 'Brightpearl credentials not configured' });
    }

    if (!req.body || typeof req.body !== 'object' || Array.isArray(req.body)) {
      return res.status(400).json({ error: 'Body must be an object of { PCF_CODE: value }' });
    }

    const baseUrl = BRIGHTPEARL_DATACENTER === 'euw1'
      ? 'https://euw1.brightpearlconnect.com'
      : 'https://use1.brightpearlconnect.com';

    const url = `${baseUrl}/public-api/${BRIGHTPEARL_ACCOUNT_ID}/order-service/order/${orderId}/custom-field`;

    // Convert flat { PCF_CODE: value } object to JSON Patch operations
    const operations = Object.entries(req.body).map(([key, value]) => ({
      op: 'replace',
      path: `/${key}`,
      value,
    }));

    const response = await fetch(url, {
      method: 'PATCH',
      headers: {
        'brightpearl-app-ref': process.env.BRIGHTPEARL_APP_REF,
        'brightpearl-account-token': BRIGHTPEARL_API_TOKEN,
        'Content-Type': 'application/json'
      },
      body: JSON.stringify(operations)
    });

    if (!response.ok) {
      const errorText = await response.text();
      console.error(`Custom field update failed for order ${orderId}:`, errorText);
      console.error('Sent body:', JSON.stringify(operations));
      return res.status(response.status).json({ error: errorText, sentOperations: operations });
    }

    res.json({ success: true });
  } catch (error) {
    console.error('Custom field update error:', error);
    res.status(500).json({ error: error.message });
  }
});

// ===========================================================================
// URGENT JOBS — backed by Postgres `urgent_orders`. Background poller scans
// recently-updated Brightpearl orders for the PCF_URGENT custom field (a
// date) and keeps the table in sync. Frontend reads the table directly.
// ===========================================================================

// Brightpearl order detail puts the current status at order.orderStatus
// as an object (not at order.orderStatusId or assignment.current.orderStatusId).
// Try every shape we've seen so the code is robust if BP changes endpoints.
function getOrderStatusId(order) {
  if (!order) return null;
  return (
    order.orderStatus?.orderStatusId ??
    order.orderStatus?.id ??
    order.assignment?.current?.orderStatusId ??
    order.orderStatusId ??
    null
  );
}

// Hard-coded staff name map — extracted from Brightpearl admin report URLs.
// Add new staff here as one line; takes precedence over any contact lookup.
const KNOWN_STAFF_NAMES = {
  4: 'Tim Banks',
  445: 'Robert Lodge',
  6433: 'Keith Taylor',
  49220: 'Ryan Wilson',
  56618: 'Abigail Birtwhistle',
  59339: 'Helen Jackson',
  82710: 'Jack Ellis-Haynes',
  124562: 'Dan Ford',
  137062: 'Laura Jackson',
};

// In-memory cache for any IDs not in the hard-coded map (looked up via API)
const staffNameCache = new Map();

async function resolveStaffName(contactId) {
  if (!contactId) return null;
  if (KNOWN_STAFF_NAMES[contactId]) return KNOWN_STAFF_NAMES[contactId];
  if (staffNameCache.has(contactId)) return staffNameCache.get(contactId);
  if (!BRIGHTPEARL_API_TOKEN || !BRIGHTPEARL_ACCOUNT_ID) return null;
  const baseUrl = BRIGHTPEARL_DATACENTER === 'euw1'
    ? 'https://euw1.brightpearlconnect.com'
    : 'https://use1.brightpearlconnect.com';
  try {
    const r = await fetch(
      `${baseUrl}/public-api/${BRIGHTPEARL_ACCOUNT_ID}/contact-service/contact/${contactId}`,
      { method: 'GET', headers: {
        'brightpearl-app-ref': process.env.BRIGHTPEARL_APP_REF,
        'brightpearl-account-token': BRIGHTPEARL_API_TOKEN,
        'Content-Type': 'application/json',
      }}
    );
    if (!r.ok) {
      staffNameCache.set(contactId, null); // negative-cache so we don't retry
      return null;
    }
    const data = await r.json();
    const c = data?.response?.[0];
    const name = [c?.firstName, c?.lastName].filter(Boolean).join(' ').trim() || null;
    staffNameCache.set(contactId, name);
    return name;
  } catch {
    return null;
  }
}

// Inspect order rows to detect EMBROIDERY / PRINT / BOTH / null.
// Checks SKU prefixes (OPEM-/OPPR-), the row's productName, and every value
// in the row's productOptions object — Brightpearl often stores decoration
// info as "Print Position: Left Breast Embroidery" inside productOptions
// rather than on the SKU itself.
function detectDecorationType(order) {
  const rows = order?.orderRows;
  if (!rows) return null;
  const rowList = Array.isArray(rows) ? rows : Object.values(rows);
  let hasEmb = false;
  let hasPrint = false;
  for (const row of rowList) {
    const sku = (row.productSku || '').toUpperCase();
    const name = (row.productName || '').toLowerCase();
    const optionValues = row.productOptions
      ? Object.values(row.productOptions).map((v) => String(v ?? '').toLowerCase())
      : [];
    const optionsBlob = optionValues.join(' | ');

    if (sku.startsWith('OPEM-') || /\bemb(roider|\b)/.test(name) || /\bemb(roider|\b)/.test(optionsBlob)) {
      hasEmb = true;
    }
    if (sku.startsWith('OPPR-') || /\bprint/.test(name) || /\bprint/.test(optionsBlob)) {
      hasPrint = true;
    }
  }
  if (hasEmb && hasPrint) return 'BOTH';
  if (hasEmb) return 'EMBROIDERY';
  if (hasPrint) return 'PRINT';
  return null;
}

// Extract YYYY-MM-DD without ever going through a Date object — avoids any
// timezone shift when BP returns "2026-04-28T00:00:00+01:00" style values.
function extractIsoDate(value) {
  if (value == null || value === '') return null;
  if (typeof value === 'string') {
    const m = value.match(/^(\d{4})-(\d{2})-(\d{2})/);
    if (m) return `${m[1]}-${m[2]}-${m[3]}`;
  }
  // Last-resort fallback for non-string inputs (Date, number) — use UTC components
  const d = value instanceof Date ? value : new Date(value);
  if (isNaN(d)) return null;
  return `${d.getUTCFullYear()}-${String(d.getUTCMonth() + 1).padStart(2, '0')}-${String(d.getUTCDate()).padStart(2, '0')}`;
}

// Kept for any other callers that need a Date instance
function parseDateLike(value) {
  if (!value) return null;
  if (value instanceof Date) return isNaN(value) ? null : value;
  const d = new Date(value);
  return isNaN(d) ? null : d;
}

async function initializeUrgentOrdersTable() {
  if (!useDatabase) return;
  try {
    await pool.query(`
      CREATE TABLE IF NOT EXISTS urgent_orders (
        order_id BIGINT PRIMARY KEY,
        order_reference TEXT,
        urgent_by_date DATE,
        is_asap BOOLEAN NOT NULL DEFAULT FALSE,
        customer_name TEXT,
        business_name TEXT,
        decoration_type TEXT,
        order_total NUMERIC(12, 2),
        created_by_id INTEGER,
        created_by_name TEXT,
        placed_on TIMESTAMPTZ,
        last_checked_at TIMESTAMPTZ DEFAULT NOW()
      );
      CREATE INDEX IF NOT EXISTS idx_urgent_orders_date ON urgent_orders(urgent_by_date);
    `);
    // Add is_asap column to existing tables (migration for old deploys)
    await pool.query(`
      ALTER TABLE urgent_orders
      ADD COLUMN IF NOT EXISTS is_asap BOOLEAN NOT NULL DEFAULT FALSE
    `);
    // Source: 'flag' (user set PCF_URGENT/PCF_ASAP) vs 'stale' (auto-detected
    // by being in status 24/25 with no activity for >14 days)
    await pool.query(`
      ALTER TABLE urgent_orders
      ADD COLUMN IF NOT EXISTS source TEXT NOT NULL DEFAULT 'flag'
    `);
    await pool.query(`
      ALTER TABLE urgent_orders
      ADD COLUMN IF NOT EXISTS stale_status_id INTEGER
    `);
    await pool.query(`
      ALTER TABLE urgent_orders
      ADD COLUMN IF NOT EXISTS stale_days INTEGER
    `);
    console.log('✅ urgent_orders table initialized');
  } catch (err) {
    console.error('❌ Error initializing urgent_orders table:', err.message);
  }
}

// Lock so concurrent scans (e.g. periodic poll firing while a manual rescan
// is mid-flight) don't double-hit Brightpearl's rate limit.
let urgentPollInFlight = false;
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

// Global Brightpearl polling lock + circuit breaker — shared by every
// background scanner (urgent + stale). User-driven endpoints (Mark Complete,
// Force Add, Inspect) still go through normally, but the heavyweight pollers
// serialize against each other and back off after a rate-limit hit.
let bpPollLockHeld = false;
let bpRateLimitedUntil = 0;
let bpPollLockOwner = null;        // label of whatever is holding the lock
let bpPollLockAcquiredAt = 0;       // ms timestamp when lock was acquired

// Live progress for the currently-running scan, surfaced in /status so the
// frontend can render a progress bar while the user waits.
let bpPollProgress = null;
function startPollProgress(label, total) {
  bpPollProgress = { label, total, processed: 0, startedAt: Date.now() };
}
function tickPollProgress(by = 1) {
  if (bpPollProgress) bpPollProgress.processed += by;
}
function endPollProgress() {
  bpPollProgress = null;
}
async function acquireBpPollLock(label, { waitMs = 0 } = {}) {
  const deadline = Date.now() + waitMs;
  while (true) {
    if (Date.now() < bpRateLimitedUntil) {
      console.log(`[bp-lock/${label}] skipped — circuit breaker open`);
      return false;
    }
    if (!bpPollLockHeld) {
      bpPollLockHeld = true;
      bpPollLockOwner = label;
      bpPollLockAcquiredAt = Date.now();
      return true;
    }
    if (Date.now() >= deadline) {
      console.log(`[bp-lock/${label}] timed out waiting for lock (held by ${bpPollLockOwner} for ${Math.round((Date.now() - bpPollLockAcquiredAt) / 1000)}s)`);
      return false;
    }
    await sleep(2000);
  }
}
function releaseBpPollLock() {
  bpPollLockHeld = false;
  bpPollLockOwner = null;
  bpPollLockAcquiredAt = 0;
}
function tripBpCircuitBreaker(seconds = 120) {
  bpRateLimitedUntil = Date.now() + seconds * 1000;
  console.warn(`[bp-lock] circuit breaker tripped — pausing all background polls for ${seconds}s`);
}

// Scan one batch of recently-updated orders, syncing PCF_URGENT into the table
async function pollUrgentOrders({ sinceMs, label = 'incremental', refreshAllCached = false } = {}) {
  if (!useDatabase || !BRIGHTPEARL_API_TOKEN || !BRIGHTPEARL_ACCOUNT_ID) return;
  if (urgentPollInFlight) {
    console.log(`[urgent-poll/${label}] skipped — another scan is in progress`);
    return;
  }
  if (!(await acquireBpPollLock(`urgent/${label}`))) return;
  urgentPollInFlight = true;
  try {
    return await pollUrgentOrdersInner({ sinceMs, label, refreshAllCached });
  } finally {
    urgentPollInFlight = false;
    releaseBpPollLock();
  }
}

async function pollUrgentOrdersInner({ sinceMs, label, refreshAllCached = false } = {}) {

  const baseUrl = BRIGHTPEARL_DATACENTER === 'euw1'
    ? 'https://euw1.brightpearlconnect.com'
    : 'https://use1.brightpearlconnect.com';
  const headers = {
    'brightpearl-app-ref': process.env.BRIGHTPEARL_APP_REF,
    'brightpearl-account-token': BRIGHTPEARL_API_TOKEN,
    'Content-Type': 'application/json',
  };

  // Default: orders updated in the last 15 minutes (poller runs every 5)
  const fromMs = sinceMs ?? Date.now() - 15 * 60 * 1000;
  const fromIso = new Date(fromMs).toISOString();
  const updatedFilter = encodeURIComponent(`${fromIso}/`);

  // BP sometimes returns fewer rows than the requested pageSize. Paginate by
  // incrementing firstResult by the ACTUAL row count and stop only when an
  // empty page comes back. Hard cap at 500 pages purely as a runaway guard.
  // orderTypeId=1 limits to sales orders (excludes POs / credit notes / etc).
  // Additionally filter createdOn to exclude ancient orders that BP's
  // background processes occasionally touch — urgent flags are only ever
  // set on recent orders anyway.
  const createdSinceDays = parseInt(process.env.URGENT_CREATED_SINCE_DAYS, 10) || 365;
  const createdSinceIso = new Date(Date.now() - createdSinceDays * 24 * 60 * 60 * 1000).toISOString();
  const createdFilter = encodeURIComponent(`${createdSinceIso}/`);

  const orderIds = [];
  let firstResult = 1;
  startPollProgress(`urgent/${label}/searching`, 0); // visible during pagination phase
  for (let page = 0; page < 500; page++) {
    const url = `${baseUrl}/public-api/${BRIGHTPEARL_ACCOUNT_ID}/order-service/order-search?orderTypeId=1&updatedOn=${updatedFilter}&createdOn=${createdFilter}&pageSize=500&firstResult=${firstResult}`;
    const r = await fetch(url, { method: 'GET', headers });
    if (!r.ok) {
      console.error(`[urgent-poll/${label}] order-search failed:`, r.status);
      return;
    }
    const data = await r.json();
    const rows = data.response?.results || [];
    if (rows.length === 0) break;
    const ids = Array.isArray(rows[0]) ? rows.map((row) => row[0]) : rows;
    orderIds.push(...ids);
    firstResult += rows.length; // advance by what BP actually returned
  }
  console.log(`[urgent-poll/${label}] order-search returned ${orderIds.length} order IDs across the window`);

  // Always merge in currently-cached order IDs that need re-checking. This is
  // what makes manual Re-scan self-correcting: existing entries with wrong
  // dates (or stale data) get re-fetched even if their updatedOn is outside
  // the search window. For the every-5-min poll we only refresh cache entries
  // older than 1 hour to keep load light; manual rescan refreshes them all.
  try {
    const cachedQuery = refreshAllCached
      ? 'SELECT order_id FROM urgent_orders'
      : "SELECT order_id FROM urgent_orders WHERE last_checked_at < NOW() - INTERVAL '1 hour'";
    const cachedResult = await pool.query(cachedQuery);
    const cachedIds = cachedResult.rows.map((r) => Number(r.order_id));
    for (const id of cachedIds) {
      if (!orderIds.includes(id)) orderIds.push(id);
    }
    if (cachedIds.length > 0) {
      console.log(`[urgent-poll/${label}] including ${cachedIds.length} cached order(s) for re-check`);
    }
  } catch (err) {
    console.error(`[urgent-poll/${label}] cached-id query failed:`, err.message);
  }

  if (orderIds.length === 0) {
    endPollProgress();
    return;
  }

  // Replace the "searching" progress bar with the actual "processing" total
  startPollProgress(`urgent/${label}`, orderIds.length);
  let added = 0;
  let removed = 0;
  let consecutiveRateLimits = 0;
  // Process serially with spacing to stay under Brightpearl's rate limit
  // (BP allows ~25 req/sec on the public API; we do at most ~6/sec)
  for (const orderId of orderIds) {
    tickPollProgress();
    await sleep(250); // ~4 requests/sec — conservative since BP budget is shared

    try {
      const cfResp = await fetch(
        `${baseUrl}/public-api/${BRIGHTPEARL_ACCOUNT_ID}/order-service/order/${orderId}/custom-field`,
        { method: 'GET', headers }
      );
      if (cfResp.status === 503 || cfResp.status === 429) {
        consecutiveRateLimits++;
        if (consecutiveRateLimits >= 3) {
          tripBpCircuitBreaker(120);
          console.error(`[urgent-poll/${label}] aborting after 3 rate-limit responses; circuit breaker tripped`);
          break;
        }
        const wait = consecutiveRateLimits * 3000;
        console.warn(`[urgent-poll/${label}] rate limited on ${orderId}, sleeping ${wait}ms`);
        await sleep(wait);
        continue;
      }
      consecutiveRateLimits = 0;
      if (!cfResp.ok) continue;
      const cfData = await cfResp.json();
      const fields = cfData.response || cfData || {};
      const urgentByDateStr = extractIsoDate(fields.PCF_URGENT);
      const isAsap = isBadgeYes(fields.PCF_ASAP); // reuse the same yes/true/1 matcher

      if (!urgentByDateStr && !isAsap) {
        // Neither urgent nor ASAP — drop from cache if present
        const del = await pool.query('DELETE FROM urgent_orders WHERE order_id = $1', [orderId]);
        if (del.rowCount > 0) removed++;
        continue;
      }

      // Urgent or ASAP — fetch order details and upsert
      await sleep(150);
      const orderResp = await fetch(
        `${baseUrl}/public-api/${BRIGHTPEARL_ACCOUNT_ID}/order-service/order/${orderId}`,
        { method: 'GET', headers }
      );
      if (orderResp.status === 503 || orderResp.status === 429) {
        console.warn(`[urgent-poll/${label}] rate limited on order ${orderId} details, sleeping 5s`);
        await sleep(5000);
        continue;
      }
      if (!orderResp.ok) continue;
      const orderData = await orderResp.json();
      const order = orderData.response?.[0];
      if (!order) continue;

      const customer = order.parties?.customer || {};
      const customerName = customer.contactName || customer.addressFullName || null;
      const businessName = customer.companyName || null;
      const orderTotal = parseFloat(order.totalValue?.total ?? order.total ?? 0) || null;
      const createdById = order.createdById || order.createdBy?.contactId || null;
      const createdByName = await resolveStaffName(createdById);
      const decorationType = detectDecorationType(order);
      const placedOn = order.placedOn ? new Date(order.placedOn) : null;
      const urgentByDate = urgentByDateStr;

      await pool.query(`
        INSERT INTO urgent_orders (
          order_id, order_reference, urgent_by_date, is_asap, customer_name, business_name,
          decoration_type, order_total, created_by_id, created_by_name, placed_on,
          source, stale_status_id, stale_days, last_checked_at
        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, 'flag', NULL, NULL, NOW())
        ON CONFLICT (order_id) DO UPDATE SET
          order_reference = EXCLUDED.order_reference,
          urgent_by_date = EXCLUDED.urgent_by_date,
          is_asap = EXCLUDED.is_asap,
          customer_name = EXCLUDED.customer_name,
          business_name = EXCLUDED.business_name,
          decoration_type = EXCLUDED.decoration_type,
          order_total = EXCLUDED.order_total,
          created_by_id = EXCLUDED.created_by_id,
          created_by_name = EXCLUDED.created_by_name,
          placed_on = EXCLUDED.placed_on,
          source = 'flag',
          stale_status_id = NULL,
          stale_days = NULL,
          last_checked_at = NOW()
      `, [
        orderId, order.reference, urgentByDate, isAsap, customerName, businessName,
        decorationType, orderTotal, createdById, createdByName, placedOn,
      ]);
      added++;
    } catch (err) {
      console.error(`[urgent-poll/${label}] error on order ${orderId}:`, err.message);
    }
  }

  console.log(`[urgent-poll/${label}] scanned ${orderIds.length}, added/updated ${added}, removed ${removed}`);
  endPollProgress();
}

// ---------------------------------------------------------------------------
// Stale-order poller — auto-flags orders sitting in production statuses for
// too long. Configurable via env:
//   BADGE_STALE_STATUS_IDS = comma-separated, default "24,25" (Embroidery, Print)
//   BADGE_STALE_DAYS       = integer, default 14
// Runs every 30 minutes. Orders that match are added with source='stale';
// orders that no longer match are removed (unless the row has been promoted
// to source='flag' by a user setting PCF_URGENT/PCF_ASAP, which always wins).
// ---------------------------------------------------------------------------
let stalePollInFlight = false;

async function pollStaleOrders({ waitForLockMs = 0 } = {}) {
  if (!useDatabase || !BRIGHTPEARL_API_TOKEN || !BRIGHTPEARL_ACCOUNT_ID) return;
  if (stalePollInFlight) {
    console.log('[stale-poll] skipped — another stale scan is in progress');
    return;
  }
  if (!(await acquireBpPollLock('stale', { waitMs: waitForLockMs }))) return;
  stalePollInFlight = true;
  try {
    return await pollStaleOrdersInner();
  } finally {
    stalePollInFlight = false;
    releaseBpPollLock();
  }
}

async function pollStaleOrdersInner() {
  const statusIds = (process.env.BADGE_STALE_STATUS_IDS || '24,25')
    .split(',').map((s) => parseInt(s.trim(), 10)).filter((n) => Number.isFinite(n));
  const staleDays = Math.max(1, parseInt(process.env.BADGE_STALE_DAYS, 10) || 14);

  const baseUrl = BRIGHTPEARL_DATACENTER === 'euw1'
    ? 'https://euw1.brightpearlconnect.com'
    : 'https://use1.brightpearlconnect.com';
  const headers = {
    'brightpearl-app-ref': process.env.BRIGHTPEARL_APP_REF,
    'brightpearl-account-token': BRIGHTPEARL_API_TOKEN,
    'Content-Type': 'application/json',
  };

  // Cutoff: any order whose updatedOn is BEFORE this date is "stale" if also
  // sitting in one of the watched statuses
  const cutoff = new Date(Date.now() - staleDays * 24 * 60 * 60 * 1000);
  const cutoffIso = cutoff.toISOString();
  const updatedFilter = encodeURIComponent(`2010-01-01T00:00:00Z/${cutoffIso}`);

  startPollProgress('stale/searching', 0);
  const candidatesByStatus = new Map(); // statusId → [orderId, ...]
  for (const statusId of statusIds) {
    const ids = [];
    let firstResult = 1;
    for (let page = 0; page < 500; page++) {
      const url = `${baseUrl}/public-api/${BRIGHTPEARL_ACCOUNT_ID}/order-service/order-search?orderTypeId=1&orderStatusId=${statusId}&updatedOn=${updatedFilter}&pageSize=500&firstResult=${firstResult}`;
      const r = await fetch(url, { method: 'GET', headers });
      if (!r.ok) {
        console.error(`[stale-poll] order-search status=${statusId} failed:`, r.status);
        return;
      }
      const data = await r.json();
      const rows = data.response?.results || [];
      if (rows.length === 0) break;
      const pageIds = Array.isArray(rows[0]) ? rows.map((row) => row[0]) : rows;
      ids.push(...pageIds);
      firstResult += rows.length;
    }
    candidatesByStatus.set(statusId, ids);
  }

  // All currently-stale order IDs across watched statuses
  const allStaleIds = new Set();
  for (const ids of candidatesByStatus.values()) {
    for (const id of ids) allStaleIds.add(Number(id));
  }

  // Drop any source='stale' rows whose order is no longer stale
  const existingStale = await pool.query(
    `SELECT order_id FROM urgent_orders WHERE source = 'stale'`
  );
  const toRemove = existingStale.rows
    .map((r) => Number(r.order_id))
    .filter((id) => !allStaleIds.has(id));
  if (toRemove.length > 0) {
    await pool.query(
      `DELETE FROM urgent_orders WHERE source = 'stale' AND order_id = ANY($1::bigint[])`,
      [toRemove]
    );
    console.log(`[stale-poll] removed ${toRemove.length} no-longer-stale row(s)`);
  }

  // Add / refresh stale rows. Bulk-fetch details (50 per call) for the candidates.
  let added = 0;
  let skipped = 0;
  const allIdsArr = Array.from(allStaleIds);
  startPollProgress('stale', allIdsArr.length);
  for (let i = 0; i < allIdsArr.length; i += 50) {
    const batch = allIdsArr.slice(i, i + 50);
    const r = await fetch(
      `${baseUrl}/public-api/${BRIGHTPEARL_ACCOUNT_ID}/order-service/order/${batch.join(',')}`,
      { method: 'GET', headers }
    );
    if (!r.ok) {
      if (r.status === 503 || r.status === 429) {
        tripBpCircuitBreaker(120);
        console.warn(`[stale-poll] rate limited — tripping circuit breaker for 2 min`);
        return; // bail; next scheduled cycle will resume
      }
      console.error(`[stale-poll] batch fetch failed:`, r.status);
      continue;
    }
    const data = await r.json();
    for (const order of (data.response || [])) {
      tickPollProgress();
      try {
        const customer = order.parties?.customer || {};
        const customerName = customer.contactName || customer.addressFullName || null;
        const businessName = customer.companyName || null;
        const orderTotal = parseFloat(order.totalValue?.total ?? order.total ?? 0) || null;
        const createdById = order.createdById || order.createdBy?.contactId || null;
        const createdByName = await resolveStaffName(createdById);
        const decorationType = detectDecorationType(order);
        const placedOn = order.placedOn ? new Date(order.placedOn) : null;
        const updatedOn = order.updatedOn ? new Date(order.updatedOn) : null;
        const days = updatedOn
          ? Math.floor((Date.now() - updatedOn.getTime()) / (24 * 60 * 60 * 1000))
          : staleDays;
        const orderStatusId = getOrderStatusId(order);

        // INSERT ... ON CONFLICT DO NOTHING — never overwrites a 'flag' row.
        // For source='stale' rows we still want to refresh display fields, so
        // we do a follow-up UPDATE that only fires for stale rows.
        const ins = await pool.query(
          `INSERT INTO urgent_orders (
            order_id, order_reference, urgent_by_date, is_asap, customer_name,
            business_name, decoration_type, order_total, created_by_id,
            created_by_name, placed_on, source, stale_status_id, stale_days,
            last_checked_at
          ) VALUES ($1, $2, NULL, TRUE, $3, $4, $5, $6, $7, $8, $9, 'stale', $10, $11, NOW())
          ON CONFLICT (order_id) DO NOTHING`,
          [
            order.id, order.reference, customerName, businessName, decorationType,
            orderTotal, createdById, createdByName, placedOn, orderStatusId, days,
          ]
        );
        if (ins.rowCount > 0) {
          added++;
        } else {
          // Row already exists. Refresh stats only if it's still source='stale'.
          await pool.query(
            `UPDATE urgent_orders
              SET stale_status_id = $2, stale_days = $3, last_checked_at = NOW()
              WHERE order_id = $1 AND source = 'stale'`,
            [order.id, orderStatusId, days]
          );
          skipped++;
        }
      } catch (err) {
        console.error(`[stale-poll] error on order ${order.id}:`, err.message);
      }
    }
  }

  console.log(`[stale-poll] stale orders found: ${allIdsArr.length}, added new: ${added}, refreshed/skipped: ${skipped}, removed: ${toRemove.length}`);
  endPollProgress();
}

// Force-release the global BP poll lock. Use only if a scan has clearly
// wedged (lock has been held for many minutes with no progress).
app.post('/api/urgent-orders/release-lock', (req, res) => {
  const heldFor = bpPollLockHeld ? Math.round((Date.now() - bpPollLockAcquiredAt) / 1000) : 0;
  const owner = bpPollLockOwner;
  bpPollLockHeld = false;
  bpPollLockOwner = null;
  bpPollLockAcquiredAt = 0;
  urgentPollInFlight = false;
  stalePollInFlight = false;
  console.warn(`[bp-lock] force-released after ${heldFor}s (was held by ${owner})`);
  res.json({ released: true, wasHeldBy: owner, wasHeldForSeconds: heldFor });
});
app.get('/api/urgent-orders/release-lock', (req, res) => {
  const heldFor = bpPollLockHeld ? Math.round((Date.now() - bpPollLockAcquiredAt) / 1000) : 0;
  const owner = bpPollLockOwner;
  bpPollLockHeld = false;
  bpPollLockOwner = null;
  bpPollLockAcquiredAt = 0;
  urgentPollInFlight = false;
  stalePollInFlight = false;
  console.warn(`[bp-lock] force-released after ${heldFor}s (was held by ${owner})`);
  res.json({ released: true, wasHeldBy: owner, wasHeldForSeconds: heldFor });
});

// Test the search query the poller uses, so we can see exactly what BP
// returns. ?orderTypeId=N&days=N to vary, defaults match the production poller.
app.get('/api/urgent-orders/test-search', async (req, res) => {
  if (!BRIGHTPEARL_API_TOKEN || !BRIGHTPEARL_ACCOUNT_ID) {
    return res.status(500).json({ error: 'Brightpearl credentials not configured' });
  }
  const baseUrl = BRIGHTPEARL_DATACENTER === 'euw1'
    ? 'https://euw1.brightpearlconnect.com'
    : 'https://use1.brightpearlconnect.com';
  const headers = {
    'brightpearl-app-ref': process.env.BRIGHTPEARL_APP_REF,
    'brightpearl-account-token': BRIGHTPEARL_API_TOKEN,
    'Content-Type': 'application/json',
  };
  const days = parseInt(req.query.days, 10) || 30;
  const orderTypeId = req.query.orderTypeId != null ? req.query.orderTypeId : '1';
  const useTypeFilter = orderTypeId !== 'none';
  const createdSinceDays = req.query.createdSinceDays != null
    ? parseInt(req.query.createdSinceDays, 10)
    : 365;
  const useCreatedFilter = req.query.createdSinceDays !== 'none' && createdSinceDays > 0;
  const fromIso = new Date(Date.now() - days * 24 * 60 * 60 * 1000).toISOString();
  const updatedFilter = encodeURIComponent(`${fromIso}/`);
  const typeQs = useTypeFilter ? `orderTypeId=${orderTypeId}&` : '';
  const createdQs = useCreatedFilter
    ? `createdOn=${encodeURIComponent(`${new Date(Date.now() - createdSinceDays * 24 * 60 * 60 * 1000).toISOString()}/`)}&`
    : '';
  const url = `${baseUrl}/public-api/${BRIGHTPEARL_ACCOUNT_ID}/order-service/order-search?${typeQs}${createdQs}updatedOn=${updatedFilter}&pageSize=5&firstResult=1`;

  try {
    const r = await fetch(url, { method: 'GET', headers });
    const body = await r.text();
    let json;
    try { json = JSON.parse(body); } catch { json = null; }
    res.json({
      requestUrl: url,
      status: r.status,
      resultsAvailable: json?.response?.metaData?.resultsAvailable ?? null,
      resultsReturned: json?.response?.metaData?.resultsReturned ?? null,
      firstFiveResults: (json?.response?.results || []).slice(0, 5),
      rawError: r.ok ? null : body,
    });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// Probe BP for order-email endpoints — tries common candidate URLs with empty
// bodies. 404 means the endpoint doesn't exist; 400 / 405 / 422 means the
// path is valid but the body's wrong (which is fine, we're just mapping).
app.get('/api/bp-email-probe', async (req, res) => {
  if (!BRIGHTPEARL_API_TOKEN || !BRIGHTPEARL_ACCOUNT_ID) {
    return res.status(500).json({ error: 'Brightpearl credentials not configured' });
  }
  const orderId = req.query.orderId || '442282'; // default to a known order
  const baseUrl = BRIGHTPEARL_DATACENTER === 'euw1'
    ? 'https://euw1.brightpearlconnect.com'
    : 'https://use1.brightpearlconnect.com';
  const headers = {
    'brightpearl-app-ref': process.env.BRIGHTPEARL_APP_REF,
    'brightpearl-account-token': BRIGHTPEARL_API_TOKEN,
    'Content-Type': 'application/json',
  };

  const candidates = [
    { path: `/order-service/order/${orderId}/email`,             method: 'POST', body: {} },
    { path: `/order-service/order/${orderId}/send-email`,        method: 'POST', body: {} },
    { path: `/order-service/order/${orderId}/notification`,      method: 'POST', body: {} },
    { path: `/order-service/order/${orderId}/document`,          method: 'POST', body: {} },
    { path: `/order-service/order/${orderId}/document-email`,    method: 'POST', body: {} },
    { path: `/communication-service/notification`,               method: 'POST', body: {} },
    { path: `/communication-service/email`,                      method: 'POST', body: {} },
    { path: `/messaging-service/message`,                        method: 'POST', body: {} },
    { path: `/workflow-service/email`,                           method: 'POST', body: {} },
    { path: `/workflow-service/document/63/send`,                method: 'POST', body: { orderId: parseInt(orderId, 10) } },
    { path: `/document-service/document/63/email`,               method: 'POST', body: { orderId: parseInt(orderId, 10) } },
    { path: `/contact-service/contact-search`,                   method: 'GET',  body: null }, // sanity check (we know this works)
  ];

  const results = [];
  for (const c of candidates) {
    const url = `${baseUrl}/public-api/${BRIGHTPEARL_ACCOUNT_ID}${c.path}`;
    try {
      const init = { method: c.method, headers };
      if (c.body !== null) init.body = JSON.stringify(c.body);
      const r = await fetch(url, init);
      const text = await r.text();
      // Cap snippet to 200 chars so order-list responses don't flood the output
      const snippet = text.length > 200 ? `${text.slice(0, 200)}…` : text;
      results.push({
        method: c.method,
        path: c.path,
        status: r.status,
        verdict:
          r.status === 404 ? 'PATH NOT FOUND'
          : r.status === 405 ? 'METHOD NOT ALLOWED (path exists)'
          : r.status === 401 ? 'AUTH ISSUE'
          : r.status === 400 || r.status === 422 ? 'PATH EXISTS — bad body shape (good sign)'
          : r.status === 200 || r.status === 201 || r.status === 204 ? 'SUCCESS — be careful, may have actually fired'
          : `OTHER (${r.status})`,
        responseSnippet: snippet,
        responseLength: text.length,
      });
    } catch (err) {
      results.push({ method: c.method, path: c.path, error: err.message });
    }
    await new Promise((r) => setTimeout(r, 1500)); // generous spacing — keeps total well under BP per-minute budget even if other pollers are active
  }
  res.json({ orderId, candidates: results });
});

// Status endpoint — see what the pollers are doing right now
app.get('/api/urgent-orders/status', async (req, res) => {
  try {
    const counts = useDatabase
      ? await pool.query(`
          SELECT source, COUNT(*)::int AS n
          FROM urgent_orders
          GROUP BY source
        `).then((r) => {
          const out = { flag: 0, stale: 0 };
          for (const row of r.rows) out[row.source] = row.n;
          return out;
        })
      : null;
    res.json({
      bpPollLockHeld,
      bpPollLockOwner,
      bpPollLockHeldForSeconds: bpPollLockHeld
        ? Math.round((Date.now() - bpPollLockAcquiredAt) / 1000)
        : 0,
      urgentPollInFlight,
      stalePollInFlight,
      bpRateLimitedUntil: bpRateLimitedUntil
        ? new Date(bpRateLimitedUntil).toISOString()
        : null,
      circuitBreakerOpen: Date.now() < bpRateLimitedUntil,
      circuitBreakerSecondsRemaining: Math.max(0, Math.ceil((bpRateLimitedUntil - Date.now()) / 1000)),
      pollProgress: bpPollProgress
        ? {
            label: bpPollProgress.label,
            processed: bpPollProgress.processed,
            total: bpPollProgress.total,
            percent: bpPollProgress.total
              ? Math.round((bpPollProgress.processed / bpPollProgress.total) * 100)
              : 0,
            elapsedSeconds: Math.round((Date.now() - bpPollProgress.startedAt) / 1000),
          }
        : null,
      cacheCounts: counts,
      thresholds: {
        watchedStatusIds: (process.env.BADGE_STALE_STATUS_IDS || '24,25').split(',').map((s) => s.trim()),
        staleDays: parseInt(process.env.BADGE_STALE_DAYS, 10) || 14,
      },
    });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// Manual stale-rescan trigger + diagnostics
const staleRescanHandler = async (req, res) => {
  if (stalePollInFlight) {
    return res.status(409).json({ error: 'A stale scan is already running — wait a minute.' });
  }
  // Wait up to 5 min for the global BP lock if the urgent poller is busy
  pollStaleOrders({ waitForLockMs: 5 * 60 * 1000 })
    .catch((err) => console.error('Manual stale rescan failed:', err.message));
  res.json({
    accepted: true,
    note: bpPollLockHeld
      ? 'Urgent poll currently in flight — stale scan will start as soon as it finishes (up to 5 min wait).'
      : 'Stale scan started.',
  });
};
app.post('/api/urgent-orders/stale-rescan', staleRescanHandler);
app.get('/api/urgent-orders/stale-rescan', staleRescanHandler);

// Inspect why a specific order does or doesn't qualify as stale
app.get('/api/urgent-orders/stale-check/:orderId', async (req, res) => {
  if (!BRIGHTPEARL_API_TOKEN || !BRIGHTPEARL_ACCOUNT_ID) {
    return res.status(500).json({ error: 'Brightpearl credentials not configured' });
  }
  const baseUrl = BRIGHTPEARL_DATACENTER === 'euw1'
    ? 'https://euw1.brightpearlconnect.com'
    : 'https://use1.brightpearlconnect.com';
  const headers = {
    'brightpearl-app-ref': process.env.BRIGHTPEARL_APP_REF,
    'brightpearl-account-token': BRIGHTPEARL_API_TOKEN,
    'Content-Type': 'application/json',
  };
  try {
    const r = await fetch(
      `${baseUrl}/public-api/${BRIGHTPEARL_ACCOUNT_ID}/order-service/order/${req.params.orderId}`,
      { method: 'GET', headers }
    );
    if (!r.ok) return res.status(r.status).json({ error: await r.text() });
    const order = (await r.json()).response?.[0];
    if (!order) return res.status(404).json({ error: 'order not found' });

    const statusIds = (process.env.BADGE_STALE_STATUS_IDS || '24,25')
      .split(',').map((s) => parseInt(s.trim(), 10));
    const staleDays = Math.max(1, parseInt(process.env.BADGE_STALE_DAYS, 10) || 14);
    const updatedOn = order.updatedOn ? new Date(order.updatedOn) : null;
    const daysSinceUpdate = updatedOn
      ? Math.floor((Date.now() - updatedOn.getTime()) / (24 * 60 * 60 * 1000))
      : null;
    const cutoffDate = new Date(Date.now() - staleDays * 24 * 60 * 60 * 1000);

    const orderStatusId = getOrderStatusId(order);
    const inWatchedStatus = statusIds.includes(orderStatusId);
    const oldEnough = updatedOn && updatedOn.getTime() < cutoffDate.getTime();
    const wouldQualify = inWatchedStatus && oldEnough;

    res.json({
      orderId: order.id,
      reference: order.reference,
      orderStatusId: orderStatusId ?? null,
      orderStatusObject: order.orderStatus ?? null,
      orderTopLevelKeys: Object.keys(order || {}),
      assignmentCurrentKeys: order?.assignment?.current
        ? Object.keys(order.assignment.current)
        : null,
      updatedOn: order.updatedOn,
      placedOn: order.placedOn,
      daysSinceUpdate,
      thresholds: { watchedStatusIds: statusIds, staleDays, cutoffDate: cutoffDate.toISOString() },
      checks: { inWatchedStatus, oldEnough },
      wouldQualifyAsStale: wouldQualify,
      currentlyInUrgentCache: useDatabase
        ? (await pool.query('SELECT source FROM urgent_orders WHERE order_id = $1', [order.id])).rows[0]?.source || null
        : null,
    });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// Read endpoint — frontend reads from Postgres directly, no Brightpearl call
app.get('/api/urgent-orders', async (req, res) => {
  if (!useDatabase) return res.json([]);
  try {
    const result = await pool.query(`
      SELECT order_id, order_reference, urgent_by_date, is_asap, customer_name, business_name,
             decoration_type, order_total, created_by_id, created_by_name, placed_on,
             source, stale_status_id, stale_days, last_checked_at
      FROM urgent_orders
      ORDER BY is_asap DESC, urgent_by_date ASC NULLS LAST, order_id ASC
    `);
    const orders = result.rows.map((r) => ({
      orderId: Number(r.order_id),
      orderReference: r.order_reference,
      urgentByDate: extractIsoDate(r.urgent_by_date),
      isAsap: !!r.is_asap,
      customerName: r.customer_name,
      businessName: r.business_name,
      decorationType: r.decoration_type,
      orderTotal: r.order_total ? Number(r.order_total) : null,
      createdById: r.created_by_id,
      createdByName: r.created_by_name,
      placedOn: r.placed_on,
      source: r.source || 'flag',
      staleStatusId: r.stale_status_id,
      staleDays: r.stale_days,
      lastCheckedAt: r.last_checked_at,
    }));
    res.json(orders);
  } catch (err) {
    console.error('Error reading urgent_orders:', err.message);
    res.status(500).json({ error: err.message });
  }
});

// Inspect one order's raw row data + what the detector decides.
// Use this to debug why decorationType comes back null for a specific order.
app.get('/api/urgent-orders/inspect/:orderId', async (req, res) => {
  if (!BRIGHTPEARL_API_TOKEN || !BRIGHTPEARL_ACCOUNT_ID) {
    return res.status(500).json({ error: 'Brightpearl credentials not configured' });
  }
  const baseUrl = BRIGHTPEARL_DATACENTER === 'euw1'
    ? 'https://euw1.brightpearlconnect.com'
    : 'https://use1.brightpearlconnect.com';
  const headers = {
    'brightpearl-app-ref': process.env.BRIGHTPEARL_APP_REF,
    'brightpearl-account-token': BRIGHTPEARL_API_TOKEN,
    'Content-Type': 'application/json',
  };
  try {
    const orderResp = await fetch(
      `${baseUrl}/public-api/${BRIGHTPEARL_ACCOUNT_ID}/order-service/order/${req.params.orderId}`,
      { method: 'GET', headers }
    );
    if (!orderResp.ok) {
      return res.status(orderResp.status).json({ error: await orderResp.text() });
    }
    const orderData = await orderResp.json();
    const order = orderData?.response?.[0];
    if (!order) return res.status(404).json({ error: 'order not found' });

    const rows = order.orderRows;
    const rowList = Array.isArray(rows) ? rows : Object.values(rows || {});
    const rowSummaries = rowList.map((row) => ({
      productSku: row.productSku,
      productName: row.productName,
      productOptions: row.productOptions,
      quantityMagnitude: row.quantity?.magnitude,
    }));

    const cfResp = await fetch(
      `${baseUrl}/public-api/${BRIGHTPEARL_ACCOUNT_ID}/order-service/order/${req.params.orderId}/custom-field`,
      { method: 'GET', headers }
    );
    const cfData = cfResp.ok ? await cfResp.json() : null;

    res.json({
      orderId: order.id,
      orderReference: order.reference,
      channelId: order?.assignment?.current?.channelId,
      orderStatusId: order.orderStatusId,
      detectedDecorationType: detectDecorationType(order),
      rows: rowSummaries,
      customFields: cfData?.response || cfData,
      pcfUrgent: cfData?.response?.PCF_URGENT,
      pcfAsap: cfData?.response?.PCF_ASAP,
    });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// Force-process a single order: fetches its custom fields + details and
// upserts/deletes its row in urgent_orders. Useful for "I set PCF_ASAP but
// the page didn't pick it up" cases — bypasses the time-window search.
// Both GET and POST so it's trivially testable from a browser address bar
const checkOrderHandler = async (req, res) => {
  const orderId = req.params.orderId;
  if (!useDatabase) return res.status(500).json({ error: 'Database disabled' });
  if (!BRIGHTPEARL_API_TOKEN || !BRIGHTPEARL_ACCOUNT_ID) {
    return res.status(500).json({ error: 'Brightpearl credentials not configured' });
  }
  const baseUrl = BRIGHTPEARL_DATACENTER === 'euw1'
    ? 'https://euw1.brightpearlconnect.com'
    : 'https://use1.brightpearlconnect.com';
  const headers = {
    'brightpearl-app-ref': process.env.BRIGHTPEARL_APP_REF,
    'brightpearl-account-token': BRIGHTPEARL_API_TOKEN,
    'Content-Type': 'application/json',
  };

  // Retry helper — survives transient 503/429 by waiting and trying again
  const fetchWithRetry = async (url, attempt = 0) => {
    const r = await fetch(url, { method: 'GET', headers });
    if ((r.status === 503 || r.status === 429) && attempt < 3) {
      const wait = (attempt + 1) * 3000; // 3s, 6s, 9s
      console.warn(`[urgent/check] ${r.status} on ${url}, retry ${attempt + 1} in ${wait}ms`);
      await sleep(wait);
      return fetchWithRetry(url, attempt + 1);
    }
    return r;
  };

  try {
    // 1. Custom fields
    const cfResp = await fetchWithRetry(
      `${baseUrl}/public-api/${BRIGHTPEARL_ACCOUNT_ID}/order-service/order/${orderId}/custom-field`
    );
    if (!cfResp.ok) {
      return res.status(cfResp.status).json({ error: 'cf-fetch-failed', detail: await cfResp.text() });
    }
    const cfData = await cfResp.json();
    const fields = cfData.response || cfData || {};
    const urgentByDateStr = extractIsoDate(fields.PCF_URGENT);
    const isAsap = isBadgeYes(fields.PCF_ASAP);

    if (!urgentByDateStr && !isAsap) {
      const del = await pool.query('DELETE FROM urgent_orders WHERE order_id = $1', [orderId]);
      return res.json({
        action: del.rowCount > 0 ? 'removed' : 'skipped',
        reason: 'neither PCF_URGENT nor PCF_ASAP is set',
        rawPcfUrgent: fields.PCF_URGENT,
        rawPcfAsap: fields.PCF_ASAP,
      });
    }

    // 2. Order details
    const orderResp = await fetchWithRetry(
      `${baseUrl}/public-api/${BRIGHTPEARL_ACCOUNT_ID}/order-service/order/${orderId}`
    );
    if (!orderResp.ok) {
      return res.status(orderResp.status).json({ error: 'order-fetch-failed', detail: await orderResp.text() });
    }
    const orderData = await orderResp.json();
    const order = orderData.response?.[0];
    if (!order) return res.status(404).json({ error: 'order not found in detail response' });

    const customer = order.parties?.customer || {};
    const customerName = customer.contactName || customer.addressFullName || null;
    const businessName = customer.companyName || null;
    const orderTotal = parseFloat(order.totalValue?.total ?? order.total ?? 0) || null;
    const createdById = order.createdById || order.createdBy?.contactId || null;
    const createdByName = await resolveStaffName(createdById);
    const decorationType = detectDecorationType(order);
    const placedOn = order.placedOn ? new Date(order.placedOn) : null;

    // 3. Upsert
    await pool.query(`
      INSERT INTO urgent_orders (
        order_id, order_reference, urgent_by_date, is_asap, customer_name, business_name,
        decoration_type, order_total, created_by_id, created_by_name, placed_on,
        source, stale_status_id, stale_days, last_checked_at
      ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, 'flag', NULL, NULL, NOW())
      ON CONFLICT (order_id) DO UPDATE SET
        order_reference = EXCLUDED.order_reference,
        urgent_by_date = EXCLUDED.urgent_by_date,
        is_asap = EXCLUDED.is_asap,
        customer_name = EXCLUDED.customer_name,
        business_name = EXCLUDED.business_name,
        decoration_type = EXCLUDED.decoration_type,
        order_total = EXCLUDED.order_total,
        created_by_id = EXCLUDED.created_by_id,
        created_by_name = EXCLUDED.created_by_name,
        placed_on = EXCLUDED.placed_on,
        source = 'flag',
        stale_status_id = NULL,
        stale_days = NULL,
        last_checked_at = NOW()
    `, [
      orderId, order.reference, urgentByDateStr, isAsap, customerName, businessName,
      decorationType, orderTotal, createdById, createdByName, placedOn,
    ]);

    res.json({
      action: 'upserted',
      stored: {
        orderId,
        urgentByDate: urgentByDateStr,
        isAsap,
        decorationType,
        customerName,
        businessName,
        orderTotal,
        createdByName,
      },
      rawPcfUrgent: fields.PCF_URGENT,
      rawPcfAsap: fields.PCF_ASAP,
    });
  } catch (err) {
    console.error(`[urgent/check] error for ${orderId}:`, err.message);
    res.status(500).json({ error: err.message, stack: err.stack });
  }
};
app.post('/api/urgent-orders/check/:orderId', checkOrderHandler);
app.get('/api/urgent-orders/check/:orderId', checkOrderHandler);

// Mark an urgent order complete: clears PCF_URGENT in Brightpearl and
// removes the row from the local cache. Sales can also clear the field
// directly in Brightpearl — the next poll cycle will catch that too.
app.post('/api/urgent-orders/complete/:orderId', async (req, res) => {
  const { orderId } = req.params;
  if (!BRIGHTPEARL_API_TOKEN || !BRIGHTPEARL_ACCOUNT_ID) {
    return res.status(500).json({ error: 'Brightpearl credentials not configured' });
  }
  const baseUrl = BRIGHTPEARL_DATACENTER === 'euw1'
    ? 'https://euw1.brightpearlconnect.com'
    : 'https://use1.brightpearlconnect.com';
  const url = `${baseUrl}/public-api/${BRIGHTPEARL_ACCOUNT_ID}/order-service/order/${orderId}/custom-field`;

  // Try JSON Patch "remove" first; if Brightpearl rejects, fall back to "replace" with null
  const tryPatch = async (operations) =>
    fetch(url, {
      method: 'PATCH',
      headers: {
        'brightpearl-app-ref': process.env.BRIGHTPEARL_APP_REF,
        'brightpearl-account-token': BRIGHTPEARL_API_TOKEN,
        'Content-Type': 'application/json',
      },
      body: JSON.stringify(operations),
    });

  try {
    // Stale orders have no PCF to clear — they're auto-detected, not user-flagged.
    // The only way to resolve a stale order is to actually progress it in BP.
    if (useDatabase) {
      const existing = await pool.query(
        'SELECT source FROM urgent_orders WHERE order_id = $1',
        [orderId]
      );
      if (existing.rows[0]?.source === 'stale') {
        return res.status(400).json({
          error: 'This order is auto-flagged as stale. Move it to a different status in Brightpearl to clear it.',
        });
      }
    }

    // Fetch current custom fields so we only patch fields that actually exist
    // — JSON Patch "remove" / "replace" both fail with CMNC-038 if the path
    // is not present (and Brightpearl orders don't carry every PCF unless
    // they've been set at least once).
    const cfResp = await fetch(
      `${baseUrl}/public-api/${BRIGHTPEARL_ACCOUNT_ID}/order-service/order/${orderId}/custom-field`,
      { method: 'GET', headers: {
        'brightpearl-app-ref': process.env.BRIGHTPEARL_APP_REF,
        'brightpearl-account-token': BRIGHTPEARL_API_TOKEN,
        'Content-Type': 'application/json',
      }}
    );
    const cfData = cfResp.ok ? await cfResp.json() : null;
    const fields = cfData?.response || {};

    const operations = [];
    if (fields.PCF_URGENT != null && fields.PCF_URGENT !== '') {
      operations.push({ op: 'remove', path: '/PCF_URGENT' });
    }
    if (fields.PCF_ASAP === true || fields.PCF_ASAP === 'true' || fields.PCF_ASAP === 1) {
      operations.push({ op: 'replace', path: '/PCF_ASAP', value: false });
    }

    if (operations.length > 0) {
      let r = await tryPatch(operations);
      if (!r.ok) {
        const errText = await r.text();
        // Last-ditch fallback — convert any "remove" to "replace with null"
        const fallback = operations.map((op) =>
          op.op === 'remove' ? { op: 'replace', path: op.path, value: null } : op
        );
        console.warn(`[urgent/complete] first patch rejected for ${orderId} — retrying with replace-null. Error:`, errText);
        r = await tryPatch(fallback);
      }
      if (!r.ok) {
        const errorText = await r.text();
        console.error(`[urgent/complete] patch failed for ${orderId}:`, errorText);
        return res.status(r.status).json({ error: errorText });
      }
    }

    // Drop from local cache so the queue updates immediately
    if (useDatabase) {
      await pool.query('DELETE FROM urgent_orders WHERE order_id = $1', [orderId]);
    }
    res.json({ success: true, clearedOps: operations });
  } catch (err) {
    console.error('[urgent/complete] error:', err.message);
    res.status(500).json({ error: err.message });
  }
});

// Manual re-scan trigger — useful for "force refresh" buttons / debugging.
// ?days=N expands the lookback window (default 1 day).
app.post('/api/urgent-orders/rescan', async (req, res) => {
  if (urgentPollInFlight) {
    return res.status(409).json({ error: 'A scan is already in progress — try again in a minute.' });
  }
  if (Date.now() < bpRateLimitedUntil) {
    const seconds = Math.ceil((bpRateLimitedUntil - Date.now()) / 1000);
    return res.status(503).json({
      error: `Brightpearl rate-limit cooldown active — pollers paused for ${seconds} more seconds. Try again then.`,
      circuitBreakerSecondsRemaining: seconds,
    });
  }
  if (bpPollLockHeld) {
    return res.status(409).json({
      error: `Another Brightpearl scan is already running (${bpPollLockOwner}). Wait for it to finish.`,
      heldBy: bpPollLockOwner,
      heldForSeconds: Math.round((Date.now() - bpPollLockAcquiredAt) / 1000),
    });
  }
  const days = Math.min(parseInt(req.query.days, 10) || 1, 30);
  const sinceMs = Date.now() - days * 24 * 60 * 60 * 1000;
  pollUrgentOrders({ sinceMs, label: `manual-${days}d`, refreshAllCached: true })
    .catch((err) => console.error('Manual rescan failed:', err.message));
  res.json({ accepted: true, days });
});

// Boot the urgent orders system
(async () => {
  await initializeUrgentOrdersTable();
  if (useDatabase && BRIGHTPEARL_API_TOKEN) {
    // Adaptive seed window: scan only the gap since we last polled. Keeps
    // frequent redeploys cheap (seconds, not minutes) while still falling
    // back to a full 7-day catch-up after long downtime or first-ever boot.
    const sevenDaysMs = 7 * 24 * 60 * 60 * 1000;
    let sinceMs = Date.now() - sevenDaysMs;
    let seedLabel = 'initial-seed';
    try {
      const r = await pool.query('SELECT MAX(last_checked_at) AS t FROM urgent_orders');
      const lastCheck = r.rows[0]?.t;
      if (lastCheck) {
        // 15 min slack to overlap with the incremental poll window
        const gapStart = new Date(lastCheck).getTime() - 15 * 60 * 1000;
        if (gapStart > sinceMs) {
          sinceMs = gapStart;
          const gapMin = Math.round((Date.now() - gapStart) / 60000);
          seedLabel = `gap-seed-${gapMin}m`;
        }
      }
    } catch (err) {
      console.warn('[urgent-poll] could not determine last check, doing full 7-day seed:', err.message);
    }
    console.log(`[urgent-poll] boot seed window: ${new Date(sinceMs).toISOString()} → now (${seedLabel})`);
    pollUrgentOrders({ sinceMs, label: seedLabel })
      .catch((err) => console.error('Initial urgent seed failed:', err.message));
    // Recurring poll every 5 minutes — covers any update in the last 15 min window
    setInterval(() => {
      pollUrgentOrders().catch((err) => console.error('Urgent poll failed:', err.message));
    }, 5 * 60 * 1000);
    console.log('✅ Urgent orders poller scheduled (every 5 min)');

    // Stale-orders poller every 30 minutes — staggered 90 sec after the
    // urgent seed scan so they don't compete for BP rate budget at boot
    setTimeout(() => {
      pollStaleOrders().catch((err) => console.error('Initial stale poll failed:', err.message));
    }, 90 * 1000);
    setInterval(() => {
      pollStaleOrders().catch((err) => console.error('Stale poll failed:', err.message));
    }, 30 * 60 * 1000);
    console.log('✅ Stale orders poller scheduled (every 30 min, first run +90s)');
  }
})();

// ===========================================================================
// PROOF CHASE — auto-emails customers whose proof has been sent and not
// responded to for >3 days. Built standalone, defaults to DRY-RUN (no real
// emails fired) until env var PROOF_CHASE_DRY_RUN=false is set.
//
// Config (Render env):
//   PROOF_CHASE_STATUS_IDS  — comma-separated orderStatusIds of "Proof Sent"
//                             statuses (e.g. "35,92" — find via the
//                             /api/proof-chase/find-statuses endpoint)
//   PROOF_CHASE_DAYS        — days in status before chasing (default 3)
//   PROOF_CHASE_DRY_RUN     — "false" to actually send (default "true")
//   PROOF_CHASE_SENDER      — From: address (default noreply@tuffshop.co.uk)
// ===========================================================================

async function initializeProofChaseTable() {
  if (!useDatabase) return;
  try {
    await pool.query(`
      CREATE TABLE IF NOT EXISTS proof_chase_log (
        order_id BIGINT PRIMARY KEY,
        first_seen_in_proof_status_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        chase_sent_at TIMESTAMPTZ,
        chase_sent_to TEXT,
        chase_dry_run BOOLEAN,
        last_status_id INTEGER,
        last_checked_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
      );
      CREATE INDEX IF NOT EXISTS idx_proof_chase_first_seen ON proof_chase_log(first_seen_in_proof_status_at);
    `);
    console.log('✅ proof_chase_log table initialized');
  } catch (err) {
    console.error('❌ Error initializing proof_chase_log table:', err.message);
  }
}

// Build the chase email body. Quicknote text + customer/order merge.
const EMAIL_ASSETS_BASE = process.env.EMAIL_ASSETS_BASE_URL || 'https://server-backend-1i47.onrender.com/email-assets';

const PROOF_CHASE_SIGNATURE_TEXT = `--

Tuff Workwear Ltd
Tuffshop.co.uk
144-146 Aberford Road, Leeds, LS26 8LG

Follow us:
Facebook: https://facebook.com/tuffshop.co.uk/
Twitter: https://twitter.com/tuffshop
LinkedIn: https://www.linkedin.com/company/tuff-workwear-ltd
YouTube: https://youtube.com/channel/UCjI55--M7y8397npIA-BAdw
Instagram: https://instagram.com/tuff.shop/
Pinterest: https://pinterest.com/tuffshop/

This email and any attachments to it may be confidential and are intended solely for the use of the individual to whom it is addressed. Any views or opinions expressed are solely those of the author and do not necessarily represent those of Tuff Workwear Ltd.
If you are not the intended recipient of this email, you must neither take any action based upon its contents, nor copy or show it to anyone.
Please contact the sender if you believe you have received this email in error.`;

const PROOF_CHASE_SIGNATURE_HTML = `<table cellpadding="0" cellspacing="0" border="0" style="margin-top:24px;font-family:'Lucida Grande',Arial,Helvetica,sans-serif;font-size:12px;color:#222;">
  <tr>
    <td style="padding:6px 0;">
      <a href="https://tuffshop.co.uk" style="text-decoration:none;border:0;">
        <img src="${EMAIL_ASSETS_BASE}/image001.png" alt="Tuff Workwear" width="113" height="80" style="display:block;border:0;outline:none;">
      </a>
    </td>
  </tr>
  <tr>
    <td style="padding:6px 0;">
      <a href="https://maps.apple.com/?q=144-146+Aberford+Road,Leeds,LS26+8LG" style="color:#222;text-decoration:none;">144-146 Aberford Road, Leeds, LS26 8LG</a>
    </td>
  </tr>
  <tr>
    <td style="padding:6px 0;">
      <table cellpadding="0" cellspacing="0" border="0">
        <tr>
          <td style="padding:0 6px 0 0;"><a href="https://facebook.com/tuffshop.co.uk/" style="border:0;"><img src="${EMAIL_ASSETS_BASE}/image002.png" alt="Facebook" width="32" height="32" style="display:block;border:0;outline:none;"></a></td>
          <td style="padding:0 6px;"><a href="https://twitter.com/tuffshop?lang=en" style="border:0;"><img src="${EMAIL_ASSETS_BASE}/image003.png" alt="Twitter" width="32" height="32" style="display:block;border:0;outline:none;"></a></td>
          <td style="padding:0 6px;"><a href="https://www.linkedin.com/company/tuff-workwear-ltd" style="border:0;"><img src="${EMAIL_ASSETS_BASE}/image004.png" alt="LinkedIn" width="32" height="32" style="display:block;border:0;outline:none;"></a></td>
          <td style="padding:0 6px;"><a href="https://youtube.com/channel/UCjI55--M7y8397npIA-BAdw" style="border:0;"><img src="${EMAIL_ASSETS_BASE}/image005.png" alt="YouTube" width="32" height="32" style="display:block;border:0;outline:none;"></a></td>
          <td style="padding:0 6px;"><a href="https://instagram.com/tuff.shop/?hl=en" style="border:0;"><img src="${EMAIL_ASSETS_BASE}/image006.png" alt="Instagram" width="32" height="32" style="display:block;border:0;outline:none;"></a></td>
          <td style="padding:0 6px;"><a href="https://pinterest.com/tuffshop/" style="border:0;"><img src="${EMAIL_ASSETS_BASE}/image007.png" alt="Pinterest" width="32" height="32" style="display:block;border:0;outline:none;"></a></td>
          <td style="padding:0 6px;"><a href="https://wa.me/" style="border:0;"><img src="${EMAIL_ASSETS_BASE}/image008.png" alt="WhatsApp" width="32" height="32" style="display:block;border:0;outline:none;"></a></td>
          <td style="padding:0 0 0 6px;"><a href="https://www.google.com/maps/place/Tuffshop.co.uk/@53.7529091,-1.4490469,17z/data=!3m1!4b1!4m5!3m4!1s0x48795d81c9d2d0df:0x51b05d91e1ea6be9!8m2!3d53.7529091!4d-1.4468582" style="border:0;"><img src="${EMAIL_ASSETS_BASE}/image009.png" alt="Google" width="32" height="32" style="display:block;border:0;outline:none;"></a></td>
        </tr>
      </table>
    </td>
  </tr>
  <tr>
    <td style="padding:6px 0;font-size:12px;">To follow us on social media or leave a review click the icons - we would love your feedback!</td>
  </tr>
</table>
<p style="color:#888;font-family:'Lucida Grande',Arial,Helvetica,sans-serif;font-size:11px;line-height:1.4;margin-top:18px;">
  This email and any attachments to it may be confidential and are intended solely for the use of the individual to whom it is addressed. Any views or opinions expressed are solely those of the author and do not necessarily represent those of Tuff Workwear Ltd.<br>
  If you are not the intended recipient of this email, you must neither take any action based upon its contents, nor copy or show it to anyone.<br>
  Please contact the sender if you believe you have received this email in error.
</p>`;

function buildProofChaseEmail(order) {
  const customer = order.parties?.customer || {};
  const fullName = customer.contactName || customer.addressFullName || '';
  const firstName = (fullName.split(/\s+/)[0] || '').trim();
  const subject = `Following up on your proofs — SO${order.id}`;
  const greeting = firstName ? `Hi ${firstName},` : `Hi,`;

  const text = `${greeting}

Did you get the mock ups through all ok and is this something you still require?

We send proofs on both WhatsApp and Email so if you can check both of those if possible.

Sometimes our e-mails do go into spam folders so if you can please have a check in there too for the mock up email it would be much appreciated.

If you no longer require this, it is not an issue just please let us know.

Thank you

${PROOF_CHASE_SIGNATURE_TEXT}`;

  // Light HTML version — same wording, basic formatting.
  const html = `<div style="font-family:Arial,Helvetica,sans-serif;font-size:14px;line-height:1.5;color:#222;">
<p>${greeting}</p>
<p>Did you get the mock ups through all ok and is this something you still require?</p>
<p>We send proofs on both WhatsApp and Email so if you can check both of those if possible.</p>
<p>Sometimes our e-mails do go into spam folders so if you can please have a check in there too for the mock up email it would be much appreciated.</p>
<p>If you no longer require this, it is not an issue just please let us know.</p>
<p>Thank you</p>
${PROOF_CHASE_SIGNATURE_HTML}
</div>`;

  return { subject, text, html };
}

async function postBpOrderNote(orderId, noteText) {
  const baseUrl = BRIGHTPEARL_DATACENTER === 'euw1'
    ? 'https://euw1.brightpearlconnect.com'
    : 'https://use1.brightpearlconnect.com';
  const url = `${baseUrl}/public-api/${BRIGHTPEARL_ACCOUNT_ID}/order-service/order/${orderId}/note`;
  const r = await fetch(url, {
    method: 'POST',
    headers: {
      'brightpearl-app-ref': process.env.BRIGHTPEARL_APP_REF,
      'brightpearl-account-token': BRIGHTPEARL_API_TOKEN,
      'Content-Type': 'application/json',
    },
    body: JSON.stringify({
      contactId: 0, // 0 = system note
      text: noteText,
      addedOn: new Date().toISOString(),
    }),
  });
  if (!r.ok) {
    const err = await r.text();
    console.error(`[proof-chase] BP note post failed for ${orderId}: ${r.status} ${err}`);
    return false;
  }
  return true;
}

async function sendProofChaseEmail(order, dryRun) {
  const customer = order.parties?.customer || {};
  const recipient = customer.email;
  if (!recipient) {
    console.warn(`[proof-chase] no customer email on order ${order.id}, skipping`);
    return { skipped: true, reason: 'no email' };
  }

  const { subject, text, html } = buildProofChaseEmail(order);
  const sender = process.env.PROOF_CHASE_SENDER || 'noreply@tuffshop.co.uk';

  if (dryRun) {
    console.log(`[proof-chase] DRY RUN — would send to ${recipient} for order ${order.reference || order.id}`);
    console.log(`[proof-chase] DRY RUN — subject: ${subject}`);
    console.log(`[proof-chase] DRY RUN — body:\n${text}`);
    return { dryRun: true, recipient, subject };
  }

  if (!process.env.SMTP_PASS) {
    console.error('[proof-chase] SMTP_PASS not configured, cannot send');
    return { skipped: true, reason: 'smtp not configured' };
  }
  const transporter = nodemailer.createTransport({
    host: process.env.SMTP_SERVER || 'mail-eu.smtp2go.com',
    port: parseInt(process.env.SMTP_PORT || '2525'),
    secure: false,
    auth: {
      user: process.env.SMTP_USERNAME || 'tuffshop.co.uk',
      pass: process.env.SMTP_PASS,
    },
  });
  await transporter.sendMail({
    from: `"Tuff Workwear" <${sender}>`,
    to: recipient,
    subject,
    text,
    html,
  });
  await postBpOrderNote(
    order.id,
    `AUTO-CHASE: 3-day proof follow-up email sent to ${recipient} on ${new Date().toLocaleString('en-GB')}`
  );
  return { sent: true, recipient, subject };
}

async function pollProofChase({ daysOverride } = {}) {
  if (!useDatabase || !BRIGHTPEARL_API_TOKEN || !BRIGHTPEARL_ACCOUNT_ID) return;
  const statusIdsRaw = (process.env.PROOF_CHASE_STATUS_IDS || '').trim();
  if (!statusIdsRaw) {
    console.log('[proof-chase] PROOF_CHASE_STATUS_IDS not set — poller idle');
    return;
  }
  const statusIds = statusIdsRaw.split(',').map((s) => parseInt(s.trim(), 10)).filter(Boolean);
  const days = daysOverride != null
    ? Math.max(0, parseInt(daysOverride, 10) || 0)
    : (parseInt(process.env.PROOF_CHASE_DAYS, 10) || 3);
  const dryRun = process.env.PROOF_CHASE_DRY_RUN !== 'false';

  if (!(await acquireBpPollLock('proof-chase'))) return;
  try {
    const baseUrl = BRIGHTPEARL_DATACENTER === 'euw1'
      ? 'https://euw1.brightpearlconnect.com'
      : 'https://use1.brightpearlconnect.com';
    const headers = {
      'brightpearl-app-ref': process.env.BRIGHTPEARL_APP_REF,
      'brightpearl-account-token': BRIGHTPEARL_API_TOKEN,
      'Content-Type': 'application/json',
    };

    const seenInThisCycle = new Set();

    for (const statusId of statusIds) {
      let firstResult = 1;
      for (let page = 0; page < 20; page++) {
        const url = `${baseUrl}/public-api/${BRIGHTPEARL_ACCOUNT_ID}/order-service/order-search?orderStatusId=${statusId}&pageSize=500&firstResult=${firstResult}`;
        await sleep(500);
        const r = await fetch(url, { method: 'GET', headers });
        if (!r.ok) {
          if (r.status === 503 || r.status === 429) {
            tripBpCircuitBreaker(120);
            console.warn(`[proof-chase] rate limited — bailing this cycle`);
            return;
          }
          console.error(`[proof-chase] order-search failed for status ${statusId}: ${r.status}`);
          break;
        }
        const data = await r.json();
        const rows = data.response?.results || [];
        if (rows.length === 0) break;
        for (const row of rows) {
          const id = Array.isArray(row) ? row[0] : row;
          seenInThisCycle.add(Number(id));

          // For NEW orders only, fetch the order detail to get a real
          // updatedOn (BP's default order-search response doesn't include
          // it). For orders we've already seen, just refresh status/check.
          const existing = await pool.query(
            'SELECT 1 FROM proof_chase_log WHERE order_id = $1',
            [id]
          );
          if (existing.rowCount === 0) {
            let firstSeen = new Date().toISOString();
            try {
              await sleep(250);
              const detailResp = await fetch(
                `${baseUrl}/public-api/${BRIGHTPEARL_ACCOUNT_ID}/order-service/order/${id}`,
                { method: 'GET', headers }
              );
              if (detailResp.ok) {
                const detail = (await detailResp.json()).response?.[0];
                const updatedOn = detail?.updatedOn || detail?.placedOn;
                if (updatedOn) firstSeen = new Date(updatedOn).toISOString();
              }
            } catch (err) {
              console.warn(`[proof-chase] could not fetch detail for ${id}: ${err.message}`);
            }
            await pool.query(`
              INSERT INTO proof_chase_log (order_id, first_seen_in_proof_status_at, last_status_id, last_checked_at)
              VALUES ($1, $2, $3, NOW())
              ON CONFLICT (order_id) DO UPDATE SET
                last_status_id = EXCLUDED.last_status_id,
                last_checked_at = NOW()
            `, [id, firstSeen, statusId]);
          } else {
            await pool.query(`
              UPDATE proof_chase_log
              SET last_status_id = $2, last_checked_at = NOW()
              WHERE order_id = $1
            `, [id, statusId]);
          }
        }
        firstResult += rows.length;
      }
    }

    // Find orders ready to chase: been in proof status >N days and not yet chased
    const ready = await pool.query(`
      SELECT order_id, first_seen_in_proof_status_at
      FROM proof_chase_log
      WHERE chase_sent_at IS NULL
        AND first_seen_in_proof_status_at < NOW() - ($1 || ' days')::interval
        AND last_checked_at >= NOW() - INTERVAL '15 minutes'
    `, [days]);

    console.log(`[proof-chase] ${ready.rows.length} order(s) ready to chase (dryRun=${dryRun})`);

    for (const row of ready.rows) {
      const orderId = Number(row.order_id);
      try {
        await sleep(500);
        const orderResp = await fetch(
          `${baseUrl}/public-api/${BRIGHTPEARL_ACCOUNT_ID}/order-service/order/${orderId}`,
          { method: 'GET', headers }
        );
        if (!orderResp.ok) {
          console.error(`[proof-chase] order detail fetch failed for ${orderId}: ${orderResp.status}`);
          continue;
        }
        const order = (await orderResp.json()).response?.[0];
        if (!order) continue;

        const result = await sendProofChaseEmail(order, dryRun);
        if (result.sent || result.dryRun) {
          await pool.query(`
            UPDATE proof_chase_log
            SET chase_sent_at = NOW(), chase_sent_to = $2, chase_dry_run = $3
            WHERE order_id = $1
          `, [orderId, result.recipient || null, !!result.dryRun]);
        }
      } catch (err) {
        console.error(`[proof-chase] error processing order ${orderId}: ${err.message}`);
      }
    }

    // Cleanup: orders no longer in any proof-sent status (they advanced or
    // got cancelled). Drop them from the log so a future re-entry restarts
    // the 3-day clock cleanly.
    const cleanup = await pool.query(
      `DELETE FROM proof_chase_log WHERE last_checked_at < NOW() - INTERVAL '1 hour'`
    );
    if (cleanup.rowCount > 0) {
      console.log(`[proof-chase] cleaned up ${cleanup.rowCount} stale row(s)`);
    }
  } finally {
    releaseBpPollLock();
  }
}

// Discover status IDs by name — helps the user find what to put in PROOF_CHASE_STATUS_IDS
app.get('/api/proof-chase/find-statuses', async (req, res) => {
  if (!BRIGHTPEARL_API_TOKEN || !BRIGHTPEARL_ACCOUNT_ID) {
    return res.status(500).json({ error: 'Brightpearl credentials not configured' });
  }
  const baseUrl = BRIGHTPEARL_DATACENTER === 'euw1'
    ? 'https://euw1.brightpearlconnect.com'
    : 'https://use1.brightpearlconnect.com';
  try {
    const r = await fetch(
      `${baseUrl}/public-api/${BRIGHTPEARL_ACCOUNT_ID}/order-service/order-status`,
      { method: 'GET', headers: {
        'brightpearl-app-ref': process.env.BRIGHTPEARL_APP_REF,
        'brightpearl-account-token': BRIGHTPEARL_API_TOKEN,
        'Content-Type': 'application/json',
      }}
    );
    if (!r.ok) return res.status(r.status).json({ error: await r.text() });
    const data = await r.json();
    const all = data.response || [];
    const filter = (req.query.q || 'proof').toLowerCase();
    const matching = all.filter((s) => (s.name || '').toLowerCase().includes(filter));
    // BP's order-status response varies in field name — try every shape we've seen
    const idOf = (s) => s.id ?? s.orderStatusId ?? s.statusId ?? s.code ?? null;
    res.json({
      filter,
      matching: matching.map((s) => ({ id: idOf(s), name: s.name, raw: s })),
      total: all.length,
      sampleRawShape: all[0] ? Object.keys(all[0]) : null,
    });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// Status / counts
app.get('/api/proof-chase/status', async (req, res) => {
  if (!useDatabase) return res.json({ enabled: false });
  try {
    const cfg = {
      statusIdsConfigured: !!process.env.PROOF_CHASE_STATUS_IDS,
      days: parseInt(process.env.PROOF_CHASE_DAYS, 10) || 3,
      dryRun: process.env.PROOF_CHASE_DRY_RUN !== 'false',
      sender: process.env.PROOF_CHASE_SENDER || 'noreply@tuffshop.co.uk',
    };
    const counts = await pool.query(`
      SELECT
        COUNT(*) FILTER (WHERE chase_sent_at IS NULL) AS pending,
        COUNT(*) FILTER (WHERE chase_sent_at IS NOT NULL AND chase_dry_run = true) AS dry_run_logged,
        COUNT(*) FILTER (WHERE chase_sent_at IS NOT NULL AND chase_dry_run = false) AS sent_for_real
      FROM proof_chase_log
    `);
    const recent = await pool.query(`
      SELECT order_id, first_seen_in_proof_status_at, chase_sent_at, chase_sent_to, chase_dry_run, last_status_id
      FROM proof_chase_log
      ORDER BY COALESCE(chase_sent_at, first_seen_in_proof_status_at) DESC
      LIMIT 20
    `);
    res.json({
      config: cfg,
      counts: counts.rows[0],
      recent: recent.rows,
    });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// Preview what would be sent for a specific order — safe, no actual send
app.get('/api/proof-chase/preview/:orderId', async (req, res) => {
  if (!BRIGHTPEARL_API_TOKEN || !BRIGHTPEARL_ACCOUNT_ID) {
    return res.status(500).json({ error: 'Brightpearl credentials not configured' });
  }
  const baseUrl = BRIGHTPEARL_DATACENTER === 'euw1'
    ? 'https://euw1.brightpearlconnect.com'
    : 'https://use1.brightpearlconnect.com';
  try {
    const r = await fetch(
      `${baseUrl}/public-api/${BRIGHTPEARL_ACCOUNT_ID}/order-service/order/${req.params.orderId}`,
      { method: 'GET', headers: {
        'brightpearl-app-ref': process.env.BRIGHTPEARL_APP_REF,
        'brightpearl-account-token': BRIGHTPEARL_API_TOKEN,
        'Content-Type': 'application/json',
      }}
    );
    if (!r.ok) return res.status(r.status).json({ error: await r.text() });
    const order = (await r.json()).response?.[0];
    if (!order) return res.status(404).json({ error: 'order not found' });
    const customer = order.parties?.customer || {};
    const { subject, text, html } = buildProofChaseEmail(order);
    res.json({
      orderId: order.id,
      orderReference: order.reference,
      currentOrderStatusId: order.orderStatus?.orderStatusId,
      currentOrderStatusName: order.orderStatus?.name,
      recipient: customer.email || null,
      recipientName: customer.contactName || customer.addressFullName || null,
      subject,
      bodyText: text,
      bodyHtml: html,
      wouldSend: !!customer.email,
      reasonNotSent: customer.email ? null : 'no customer email on order',
    });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// Manual trigger for the next poll cycle (useful for testing without waiting 30 min).
// ?days=N overrides PROOF_CHASE_DAYS for this run only — set 0 to chase every
// order currently in proof-sent status, regardless of how long it's been there.
const proofChaseRunHandler = async (req, res) => {
  if (bpPollLockHeld) {
    return res.status(409).json({ error: `Another scan is in progress (${bpPollLockOwner})` });
  }
  const daysOverride = req.query.days;
  pollProofChase({ daysOverride }).catch((err) => console.error('Manual proof-chase run failed:', err.message));
  res.json({ accepted: true, daysOverride: daysOverride ?? null });
};
app.post('/api/proof-chase/run-now', proofChaseRunHandler);
app.get('/api/proof-chase/run-now', proofChaseRunHandler);

// One-shot: re-seed first_seen_in_proof_status_at by fetching the order detail
// from BP for every row in proof_chase_log. Useful after a deploy where the
// previous seed used the wrong source for the timestamp. Preserves chase_sent_at.
const proofChaseReseedHandler = async (req, res) => {
  if (!BRIGHTPEARL_API_TOKEN || !BRIGHTPEARL_ACCOUNT_ID) {
    return res.status(500).json({ error: 'Brightpearl credentials not configured' });
  }
  if (bpPollLockHeld) {
    return res.status(409).json({ error: `Another scan is in progress (${bpPollLockOwner})` });
  }
  const baseUrl = BRIGHTPEARL_DATACENTER === 'euw1'
    ? 'https://euw1.brightpearlconnect.com'
    : 'https://use1.brightpearlconnect.com';
  const headers = {
    'brightpearl-app-ref': process.env.BRIGHTPEARL_APP_REF,
    'brightpearl-account-token': BRIGHTPEARL_API_TOKEN,
    'Content-Type': 'application/json',
  };
  if (!(await acquireBpPollLock('proof-chase-reseed'))) {
    return res.status(409).json({ error: 'could not acquire BP lock' });
  }
  // Optional: also clear any DRY-RUN chase_sent_at marks so the orders
  // re-qualify on the next run. Useful when re-testing after a bad seed.
  const clearDryRunChase = req.query.clearDryRunChase === '1' || req.query.clearDryRunChase === 'true';
  res.json({ accepted: true, clearDryRunChase });
  (async () => {
    try {
      if (clearDryRunChase) {
        const cleared = await pool.query(
          `UPDATE proof_chase_log
           SET chase_sent_at = NULL, chase_sent_to = NULL, chase_dry_run = NULL
           WHERE chase_dry_run = true`
        );
        console.log(`[proof-chase-reseed] cleared ${cleared.rowCount} dry-run chase marks`);
      }
      const rows = await pool.query('SELECT order_id FROM proof_chase_log');
      let updated = 0;
      for (const row of rows.rows) {
        try {
          await sleep(300);
          const r = await fetch(
            `${baseUrl}/public-api/${BRIGHTPEARL_ACCOUNT_ID}/order-service/order/${row.order_id}`,
            { method: 'GET', headers }
          );
          if (!r.ok) continue;
          const order = (await r.json()).response?.[0];
          const updatedOn = order?.updatedOn || order?.placedOn;
          if (!updatedOn) continue;
          await pool.query(
            `UPDATE proof_chase_log SET first_seen_in_proof_status_at = $2 WHERE order_id = $1`,
            [row.order_id, new Date(updatedOn).toISOString()]
          );
          updated++;
        } catch (err) {
          console.warn(`[proof-chase-reseed] error on ${row.order_id}: ${err.message}`);
        }
      }
      console.log(`[proof-chase-reseed] updated ${updated} of ${rows.rows.length} rows`);
    } finally {
      releaseBpPollLock();
    }
  })();
};
app.post('/api/proof-chase/reseed', proofChaseReseedHandler);
app.get('/api/proof-chase/reseed', proofChaseReseedHandler);

// Boot the proof-chase system
(async () => {
  await initializeProofChaseTable();
  if (useDatabase && BRIGHTPEARL_API_TOKEN) {
    // Stagger initial run 3 minutes after boot so other pollers settle first
    setTimeout(() => {
      pollProofChase().catch((err) => console.error('Initial proof-chase run failed:', err.message));
    }, 3 * 60 * 1000);
    // Periodic every 30 minutes
    setInterval(() => {
      pollProofChase().catch((err) => console.error('Proof-chase poll failed:', err.message));
    }, 30 * 60 * 1000);
    console.log('✅ Proof-chase poller scheduled (every 30 min, first run +3min). Dry-run defaults ON.');
  }
})();

const docuSignService = new DocuSignService();

// DocuSign logging functionality
const DOCUSIGN_LOG_FILE = path.join(__dirname, 'docusign-logs.json');

// Initialize database table if using PostgreSQL
async function initializeDatabase() {
  if (!useDatabase) return;

  try {
    await pool.query(`
      CREATE TABLE IF NOT EXISTS docusign_logs (
        id SERIAL PRIMARY KEY,
        timestamp TIMESTAMPTZ NOT NULL,
        envelope_id VARCHAR(255) NOT NULL,
        status VARCHAR(50),
        recipient_email VARCHAR(255),
        recipient_name VARCHAR(255),
        signature_count INTEGER,
        pdf_size_bytes BIGINT,
        user_agent TEXT,
        ip_address VARCHAR(100),
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
      );
      CREATE INDEX IF NOT EXISTS idx_docusign_logs_timestamp ON docusign_logs(timestamp DESC);
      CREATE INDEX IF NOT EXISTS idx_docusign_logs_envelope_id ON docusign_logs(envelope_id);
    `);
    console.log('✅ DocuSign logs table initialized');
  } catch (error) {
    console.error('❌ Error initializing database:', error.message);
  }
}

// Initialize log file if it doesn't exist (for JSON fallback)
function initializeLogFile() {
  if (!fs.existsSync(DOCUSIGN_LOG_FILE)) {
    fs.writeFileSync(DOCUSIGN_LOG_FILE, JSON.stringify({ logs: [] }, null, 2));
    console.log('📝 Created DocuSign log file:', DOCUSIGN_LOG_FILE);
  }
}

// Log a DocuSign send (supports both PostgreSQL and JSON)
async function logDocuSignSend(logEntry) {
  try {
    if (useDatabase) {
      // Use PostgreSQL
      await pool.query(
        `INSERT INTO docusign_logs
         (timestamp, envelope_id, status, recipient_email, recipient_name, signature_count, pdf_size_bytes, user_agent, ip_address)
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)`,
        [
          logEntry.timestamp,
          logEntry.envelopeId,
          logEntry.status,
          logEntry.recipientEmail,
          logEntry.recipientName,
          logEntry.signatureCount,
          logEntry.pdfSizeBytes,
          logEntry.userAgent,
          logEntry.ipAddress
        ]
      );
      console.log('✅ Logged DocuSign send to database:', logEntry.envelopeId);
    } else {
      // Use JSON file
      initializeLogFile();
      const data = JSON.parse(fs.readFileSync(DOCUSIGN_LOG_FILE, 'utf-8'));
      data.logs.unshift(logEntry);

      // Keep only the last 1000 entries
      if (data.logs.length > 1000) {
        data.logs = data.logs.slice(0, 1000);
      }

      fs.writeFileSync(DOCUSIGN_LOG_FILE, JSON.stringify(data, null, 2));
      console.log('✅ Logged DocuSign send to file:', logEntry.envelopeId);
    }
  } catch (error) {
    console.error('❌ Error logging DocuSign send:', error.message);
  }
}

// Get DocuSign logs endpoint (supports both PostgreSQL and JSON)
app.get('/api/docusign-logs', async (req, res) => {
  try {
    const { startDate, endDate, limit = 50 } = req.query;

    if (useDatabase) {
      // Use PostgreSQL
      let query = 'SELECT * FROM docusign_logs WHERE 1=1';
      const params = [];
      let paramIndex = 1;

      if (startDate) {
        query += ` AND timestamp >= $${paramIndex}`;
        params.push(startDate);
        paramIndex++;
      }

      if (endDate) {
        query += ` AND timestamp <= $${paramIndex}`;
        params.push(endDate);
        paramIndex++;
      }

      query += ' ORDER BY timestamp DESC';
      query += ` LIMIT $${paramIndex}`;
      params.push(parseInt(limit));

      const result = await pool.query(query, params);
      const countResult = await pool.query('SELECT COUNT(*) FROM docusign_logs');

      // Transform database results to match frontend format
      const logs = result.rows.map(row => ({
        timestamp: row.timestamp,
        envelopeId: row.envelope_id,
        status: row.status,
        recipientEmail: row.recipient_email,
        recipientName: row.recipient_name,
        signatureCount: row.signature_count,
        pdfSizeBytes: row.pdf_size_bytes,
        userAgent: row.user_agent,
        ipAddress: row.ip_address
      }));

      res.json({
        success: true,
        total: parseInt(countResult.rows[0].count),
        returned: logs.length,
        logs: logs
      });
    } else {
      // Use JSON file
      initializeLogFile();
      const data = JSON.parse(fs.readFileSync(DOCUSIGN_LOG_FILE, 'utf-8'));
      let logs = data.logs;

      if (startDate) {
        logs = logs.filter(log => new Date(log.timestamp) >= new Date(startDate));
      }

      if (endDate) {
        logs = logs.filter(log => new Date(log.timestamp) <= new Date(endDate));
      }

      logs = logs.slice(0, parseInt(limit));

      res.json({
        success: true,
        total: data.logs.length,
        returned: logs.length,
        logs: logs
      });
    }
  } catch (error) {
    console.error('Error reading DocuSign logs:', error);
    res.status(500).json({
      success: false,
      error: error.message
    });
  }
});

// Initialize database on startup
initializeDatabase();

app.get('/check-limits', (req, res) => {
  res.json({ 
    message: 'Server is configured',
    limits: '50mb'
  });
});

// Main DocuSign endpoint
app.post('/send-to-docusign', async (req, res) => {
  console.log('📥 DocuSign endpoint hit! Version: 2025-01-17-v4-CORRECT-REPO');
  console.log('  - Request body exists:', !!req.body);
  console.log('  - Request body keys:', req.body ? Object.keys(req.body) : 'NO BODY');

  try {
    // Support both logoPositions (old) and signaturePositions (new frontend)
    const { pdfBase64, recipientEmail, recipientName, signaturePositions, logoPositions } = req.body;

    console.log('📋 Received data:');
    console.log('  - recipientEmail:', recipientEmail);
    console.log('  - recipientName:', recipientName);
    console.log('  - pdfBase64 length:', pdfBase64?.length);
    console.log('  - signaturePositions:', signaturePositions);
    console.log('  - logoPositions (legacy):', logoPositions);

    if (!pdfBase64 || !recipientEmail || !recipientName) {
      return res.status(400).json({
        success: false,
        error: 'Missing required fields: pdfBase64, recipientEmail, recipientName'
      });
    }

    // Use signaturePositions if available, otherwise fall back to logoPositions
    const positions = signaturePositions || logoPositions;

    if (!positions || !Array.isArray(positions)) {
      return res.status(400).json({
        success: false,
        error: 'signaturePositions must be an array'
      });
    }

    const pdfBytes = Buffer.from(pdfBase64, 'base64');

    const formattedPositions = positions.map(pos => ({
      page: pos.page || 1,
      x: pos.x || 100,
      y: pos.y || 100
    }));
    
    const result = await docuSignService.createEnvelopeWithSignatureFields(
      pdfBytes,
      recipientEmail,
      recipientName,
      formattedPositions
    );

    // Log the DocuSign send
    const logEntry = {
      timestamp: new Date().toISOString(),
      envelopeId: result.envelopeId,
      status: result.status,
      recipientEmail,
      recipientName,
      signatureCount: formattedPositions.length,
      pdfSizeBytes: pdfBytes.length,
      userAgent: req.headers['user-agent'] || 'unknown',
      ipAddress: req.ip || req.connection.remoteAddress || 'unknown'
    };

    logDocuSignSend(logEntry);

    res.json({
      success: true,
      envelopeId: result.envelopeId,
      status: result.status
    });
  } catch (error) {
    console.error('DocuSign error:', error);
    res.status(500).json({ 
      success: false,
      error: error.message 
    });
  }
});

// Search contacts by company name
app.get("/api/brightpearl/contact-search", async (req, res) => {
  try {
    const { company } = req.query;
    if (!company) return res.status(400).json({ error: "company query param required" });

    const baseUrl = BRIGHTPEARL_DATACENTER === 'euw1'
      ? 'https://euw1.brightpearlconnect.com'
      : 'https://use1.brightpearlconnect.com';

    const url = `${baseUrl}/public-api/${BRIGHTPEARL_ACCOUNT_ID}/contact-service/contact-search?companyName=${encodeURIComponent(company)}&pageSize=50`;

    const response = await fetch(url, {
      headers: {
        'brightpearl-app-ref': process.env.BRIGHTPEARL_APP_REF,
        'brightpearl-account-token': BRIGHTPEARL_API_TOKEN,
        'Content-Type': 'application/json'
      }
    });

    const data = await response.json();
    res.json({ success: true, results: data?.response?.results || [], columns: data?.response?.metaData?.columns });
  } catch (error) {
    console.error("Contact search error:", error);
    res.status(500).json({ error: error.message });
  }
});

// Search orders by customer contact ID
app.get("/api/brightpearl/orders-by-contact/:contactId", async (req, res) => {
  try {
    const { contactId } = req.params;
    const { fromDate, toDate } = req.query;
    const baseUrl = BRIGHTPEARL_DATACENTER === 'euw1'
      ? 'https://euw1.brightpearlconnect.com'
      : 'https://use1.brightpearlconnect.com';

    // Build placedOn filter (Brightpearl format: "fromDate/toDate" or just "fromDate/")
    let placedOnFilter = "";
    if (fromDate || toDate) {
      placedOnFilter = `&placedOn=${fromDate || ""}/${toDate || ""}`;
    }
    const url = `${baseUrl}/public-api/${BRIGHTPEARL_ACCOUNT_ID}/order-service/order-search?contactId=${contactId}&pageSize=200&firstResult=1${placedOnFilter}`;

    const response = await fetch(url, {
      method: 'GET',
      headers: {
        'brightpearl-app-ref': process.env.BRIGHTPEARL_APP_REF,
        'brightpearl-account-token': BRIGHTPEARL_API_TOKEN,
        'Content-Type': 'application/json'
      }
    });

    if (!response.ok) {
      return res.status(response.status).json({ error: `Brightpearl returned ${response.status}` });
    }

    const data = await response.json();
    const orderIds = data?.response?.results?.map(r => r[0]) || [];

    if (orderIds.length === 0) {
      return res.json({ success: true, orders: [] });
    }

    // Fetch order details in batches
    const batchSize = 20;
    const allOrders = [];

    for (let i = 0; i < orderIds.length; i += batchSize) {
      const batch = orderIds.slice(i, i + batchSize);
      const orderRange = batch.join(',');
      const detailsUrl = `${baseUrl}/public-api/${BRIGHTPEARL_ACCOUNT_ID}/order-service/order/${orderRange}`;

      const detailsResponse = await fetch(detailsUrl, {
        headers: {
          'brightpearl-app-ref': process.env.BRIGHTPEARL_APP_REF,
          'brightpearl-account-token': BRIGHTPEARL_API_TOKEN,
          'Content-Type': 'application/json'
        }
      });

      if (detailsResponse.ok) {
        const detailsData = await detailsResponse.json();
        const orders = detailsData?.response || [];
        allOrders.push(...(Array.isArray(orders) ? orders : [orders]));
      }
    }

    res.json({ success: true, orders: allOrders, totalFound: orderIds.length });
  } catch (error) {
    console.error("Orders by contact error:", error);
    res.status(500).json({ error: error.message });
  }
});

// ============================================================
// Customer Mockup Configs (logo placements for customer portal)
// ============================================================

async function initMockupConfigsDB() {
  if (!pool) return;
  try {
    await pool.query(`
      CREATE TABLE IF NOT EXISTS customer_mockup_configs (
        id SERIAL PRIMARY KEY,
        customer VARCHAR(50) NOT NULL,
        product_code VARCHAR(20) NOT NULL,
        colour VARCHAR(100) NOT NULL,
        position VARCHAR(50) NOT NULL,
        logo_id VARCHAR(50) NOT NULL,
        side VARCHAR(10) DEFAULT 'front',
        placement JSONB NOT NULL,
        created_at TIMESTAMP DEFAULT NOW(),
        updated_at TIMESTAMP DEFAULT NOW(),
        UNIQUE(customer, product_code, colour, position, logo_id, side)
      );
    `);
    console.log("✅ Mockup configs table ready");
  } catch (err) {
    console.error("❌ Mockup configs DB init error:", err.message);
  }
}

initMockupConfigsDB();

// Save or update a placement config
app.put("/api/mockup-configs", async (req, res) => {
  if (!pool) return res.status(503).json({ error: "Database not configured" });
  try {
    const { customer, productCode, colour, position, logoId, side, placement } = req.body;
    if (!customer || !productCode || !colour || !position || !logoId || !placement) {
      return res.status(400).json({ error: "Missing required fields" });
    }

    const result = await pool.query(`
      INSERT INTO customer_mockup_configs (customer, product_code, colour, position, logo_id, side, placement)
      VALUES ($1, $2, $3, $4, $5, $6, $7)
      ON CONFLICT (customer, product_code, colour, position, logo_id, side)
      DO UPDATE SET placement = $7, updated_at = NOW()
      RETURNING id
    `, [customer, productCode, colour, position, logoId, side || 'front', JSON.stringify(placement)]);

    res.json({ success: true, id: result.rows[0].id });
  } catch (err) {
    console.error("Save mockup config error:", err);
    res.status(500).json({ error: err.message });
  }
});

// Get all configs for a customer
app.get("/api/mockup-configs/:customer", async (req, res) => {
  if (!pool) return res.status(503).json({ error: "Database not configured" });
  try {
    const { customer } = req.params;
    const { productCode, colour } = req.query;

    let query = `SELECT * FROM customer_mockup_configs WHERE customer = $1`;
    const params = [customer];

    if (productCode) {
      params.push(productCode);
      query += ` AND product_code = $${params.length}`;
    }
    if (colour) {
      params.push(colour);
      query += ` AND colour = $${params.length}`;
    }

    query += ` ORDER BY product_code, colour, position`;
    const result = await pool.query(query, params);

    res.json({ success: true, configs: result.rows });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// Delete a config
app.delete("/api/mockup-configs/:id", async (req, res) => {
  if (!pool) return res.status(503).json({ error: "Database not configured" });
  try {
    await pool.query(`DELETE FROM customer_mockup_configs WHERE id = $1`, [req.params.id]);
    res.json({ success: true });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// ============================================================
// Customer Order Portal
// ============================================================

async function initCustomerOrdersDB() {
  if (!pool) return;
  try {
    await pool.query(`
      CREATE TABLE IF NOT EXISTS customer_orders (
        id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
        customer_name VARCHAR(255),
        contact_name VARCHAR(255),
        order_data JSONB NOT NULL,
        notes TEXT,
        status VARCHAR(20) DEFAULT 'new',
        created_at TIMESTAMP DEFAULT NOW()
      );
    `);
    console.log("✅ Customer orders table ready");
  } catch (err) {
    console.error("❌ Customer orders DB init error:", err.message);
  }
}

initCustomerOrdersDB();

app.post("/api/customer-orders", async (req, res) => {
  if (!pool) return res.status(503).json({ error: "Database not configured" });
  try {
    const { customer, contactName, items, notes } = req.body;
    const result = await pool.query(
      `INSERT INTO customer_orders (customer_name, contact_name, order_data, notes)
       VALUES ($1, $2, $3, $4) RETURNING id, created_at`,
      [customer || null, contactName || null, JSON.stringify({ items: items || [] }), notes || null]
    );
    res.json({ success: true, orderId: result.rows[0].id });
  } catch (err) {
    console.error("Customer order error:", err);
    res.status(500).json({ error: err.message });
  }
});

// Send order confirmation — summary email with PDF attached
app.post("/api/customer-orders/send-confirmation", async (req, res) => {
  if (!process.env.SMTP_PASS) return res.json({ success: false, error: "SMTP not configured" });
  try {
    const { pdfBase64, customer, contactName, items, notes } = req.body;
    if (!pdfBase64) return res.status(400).json({ error: "pdfBase64 required" });

    const transporter = nodemailer.createTransport({
      host: process.env.SMTP_SERVER || "mail-eu.smtp2go.com",
      port: parseInt(process.env.SMTP_PORT || "2525"),
      secure: false,
      auth: {
        user: process.env.SMTP_USERNAME || "tuffshop.co.uk",
        pass: process.env.SMTP_PASS,
      },
    });

    const itemRows = (items || []).map((item) => {
      const sizes = (item.sizes || []).filter((s) => s.qty > 0).map((s) => `${s.qty}x ${s.size}`).join(", ");
      const logos = (item.logos || []).map((l) => `${l.position}: ${l.logo}`).join(" | ");
      let customHtml = "";
      if (item.isCustom && item.customLogos) {
        const entries = Object.entries(item.customLogos);
        const logoLines = entries.map(([pos, cl]) => {
          const cmyk = cl.tintCmyk ? `C${cl.tintCmyk.c} M${cl.tintCmyk.m} Y${cl.tintCmyk.y} K${cl.tintCmyk.k}` : null;
          const ink = cl.tintHex
            ? `<br><span style="font-size:11px">Ink: <span style="display:inline-block;width:10px;height:10px;background:${cl.tintHex};border:1px solid #ccc;vertical-align:middle"></span> <code>${cl.tintHex}</code>${cmyk ? ` &nbsp;|&nbsp; CMYK <strong>${cmyk}</strong>` : ""}</span>`
            : "";
          return `<div style="margin-top:4px"><strong>${pos}:</strong> ${cl.logoName}${ink}</div>`;
        }).join("");
        const frontImg = item.customPreviews && item.customPreviews.front
          ? `<img src="${item.customPreviews.front}" alt="front" width="100" style="width:100px;max-width:100px;height:auto;margin:4px 4px 0 0;border:1px solid #e0e0e0;border-radius:4px"/>`
          : "";
        const backImg = item.customPreviews && item.customPreviews.back
          ? `<img src="${item.customPreviews.back}" alt="back" width="100" style="width:100px;max-width:100px;height:auto;margin:4px 0 0 0;border:1px solid #e0e0e0;border-radius:4px"/>`
          : "";
        customHtml = `
          <div style="margin-top:6px;padding:6px 8px;background:#fffbe6;border-left:3px solid #F3D014;font-size:12px">
            <strong style="color:#000">CUSTOM</strong>
            ${logoLines}
            ${(frontImg || backImg) ? `<div style="margin-top:4px">${frontImg}${backImg}</div>` : ""}
          </div>`;
      }
      return `<tr>
        <td style="padding:8px;border-bottom:1px solid #eee"><strong>${item.product}</strong><br><span style="color:#888">${item.colour}</span>${customHtml}</td>
        <td style="padding:8px;border-bottom:1px solid #eee;vertical-align:top">${sizes}</td>
        <td style="padding:8px;border-bottom:1px solid #eee;font-size:12px;vertical-align:top">${logos || "None"}</td>
      </tr>`;
    }).join("");

    const totalQty = (items || []).reduce((s, item) =>
      s + (item.sizes || []).reduce((a, sz) => a + (sz.qty || 0), 0), 0
    );

    const filename = `Fitness-Inc-Order-Confirmation-${new Date().toLocaleDateString("en-GB").replace(/\//g, "-")}.pdf`;

    await transporter.sendMail({
      from: `"Fitness Inc Orders" <${process.env.SENDER_EMAIL || "fitnessincorders@tuffshop.co.uk"}>`,
      to: process.env.RECIPIENT_EMAILS || "dec@tuffshop.co.uk",
      subject: `Fitness Inc Order - ${contactName || customer || "Unknown"} - ${totalQty} items`,
      html: `
        <div style="font-family:Arial,sans-serif;max-width:600px;margin:0 auto">
          <div style="background:#000;color:#fff;padding:16px 20px;border-bottom:3px solid #F3D014">
            <h2 style="margin:0;font-size:18px">New Fitness Inc Order</h2>
          </div>
          <div style="padding:20px">
            <p><strong>Customer:</strong> ${customer || "Fitness Inc"}</p>
            <p><strong>Contact:</strong> ${contactName || "N/A"}</p>
            <p><strong>Total Items:</strong> ${totalQty}</p>
            ${notes ? `<p><strong>Notes:</strong> ${notes}</p>` : ""}
            <table style="width:100%;border-collapse:collapse;margin-top:16px">
              <thead><tr style="background:#f5f5f5">
                <th style="padding:8px;text-align:left">Product</th>
                <th style="padding:8px;text-align:left">Sizes</th>
                <th style="padding:8px;text-align:left">Logos</th>
              </tr></thead>
              <tbody>${itemRows}</tbody>
            </table>
            <p style="color:#888;font-size:12px;margin-top:20px">
              Submitted: ${new Date().toLocaleString("en-GB", { timeZone: "Europe/London" })}
            </p>
            <p style="color:#888;font-size:12px">Order confirmation PDF attached.</p>
          </div>
        </div>
      `,
      attachments: [{ filename, content: Buffer.from(pdfBase64, "base64"), contentType: "application/pdf" }],
    });

    console.log("Order confirmation email sent with PDF");
    res.json({ success: true });
  } catch (err) {
    console.error("Failed to email confirmation:", err.message);
    res.status(500).json({ error: err.message });
  }
});

app.get("/api/customer-orders", async (req, res) => {
  if (!pool) return res.status(503).json({ error: "Database not configured" });
  try {
    const result = await pool.query(
      `SELECT id, customer_name, contact_name, order_data, notes, status, created_at
       FROM customer_orders ORDER BY created_at DESC LIMIT 50`
    );
    res.json({ success: true, orders: result.rows });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// DELETE all customer orders (admin)
app.delete("/api/customer-orders", async (req, res) => {
  if (!pool) return res.status(503).json({ error: "Database not configured" });
  try {
    const result = await pool.query("DELETE FROM customer_orders RETURNING id");
    res.json({ success: true, deleted: result.rowCount });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// PATCH order status
app.patch("/api/customer-orders/:orderId/status", async (req, res) => {
  if (!pool) return res.status(503).json({ error: "Database not configured" });
  try {
    const { orderId } = req.params;
    const { status } = req.body;
    if (!status) return res.status(400).json({ success: false, error: "status required" });
    const result = await pool.query(
      "UPDATE customer_orders SET status = $1 WHERE id = $2 RETURNING id, status",
      [status, orderId]
    );
    if (result.rowCount === 0) return res.status(404).json({ success: false, error: "Order not found" });
    res.json({ success: true, id: result.rows[0].id, status: result.rows[0].status });
  } catch (err) {
    console.error("Failed to update order status:", err);
    res.status(500).json({ success: false, error: err.message });
  }
});

// ============================================================
// Proof Approval System
// ============================================================

async function initApprovalDB() {
  if (!pool) {
    console.log("⚠️  No DATABASE_URL — approval system disabled");
    return;
  }
  try {
    await pool.query(`
      CREATE TABLE IF NOT EXISTS approval_sessions (
        id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
        order_number VARCHAR(50),
        customer_name VARCHAR(255),
        recipient_name VARCHAR(255),
        pdf_data BYTEA,
        logo_positions JSONB NOT NULL,
        status VARCHAR(20) DEFAULT 'pending',
        created_at TIMESTAMP DEFAULT NOW(),
        completed_at TIMESTAMP,
        submitter_ip VARCHAR(100),
        approver_name VARCHAR(255),
        signature_data TEXT
      );
      -- Allow existing NOT NULL column to be nullable
      ALTER TABLE approval_sessions ALTER COLUMN pdf_data DROP NOT NULL;
      -- Add columns if missing
      ALTER TABLE approval_sessions ADD COLUMN IF NOT EXISTS submitter_ip VARCHAR(100);
      ALTER TABLE approval_sessions ADD COLUMN IF NOT EXISTS approver_name VARCHAR(255);
      ALTER TABLE approval_sessions ADD COLUMN IF NOT EXISTS signature_data TEXT;
      ALTER TABLE approval_sessions ADD COLUMN IF NOT EXISTS pdf_delete_after TIMESTAMP;
      ALTER TABLE approval_sessions ADD COLUMN IF NOT EXISTS created_by VARCHAR(100);
      ALTER TABLE approval_sessions ADD COLUMN IF NOT EXISTS opened_at TIMESTAMP;
      CREATE TABLE IF NOT EXISTS approval_items (
        id SERIAL PRIMARY KEY,
        session_id UUID REFERENCES approval_sessions(id) ON DELETE CASCADE,
        position_index INTEGER NOT NULL,
        label VARCHAR(255),
        page_number INTEGER NOT NULL,
        status VARCHAR(20) DEFAULT 'pending',
        rejection_reason TEXT,
        reviewed_at TIMESTAMP,
        UNIQUE(session_id, position_index)
      );
    `);
    console.log("✅ Approval tables ready");
  } catch (err) {
    console.error("❌ Failed to init approval DB:", err.message);
  }
}

initApprovalDB();

// Create approval session
app.post("/api/approval-sessions", async (req, res) => {
  if (!pool) return res.status(503).json({ error: "Database not configured" });

  try {
    const { pdfBase64, logoPositions, orderNumber, customerName, recipientName, createdBy } = req.body;

    if (!pdfBase64 || !logoPositions?.length) {
      return res.status(400).json({ error: "pdfBase64 and logoPositions are required" });
    }

    const pdfBuffer = Buffer.from(pdfBase64, "base64");

    const sessionResult = await pool.query(
      `INSERT INTO approval_sessions (order_number, customer_name, recipient_name, pdf_data, logo_positions, created_by)
       VALUES ($1, $2, $3, $4, $5, $6)
       RETURNING id, status, created_at`,
      [orderNumber || null, customerName || null, recipientName || null, pdfBuffer, JSON.stringify(logoPositions), createdBy || null]
    );

    const session = sessionResult.rows[0];

    for (let i = 0; i < logoPositions.length; i++) {
      const pos = logoPositions[i];
      await pool.query(
        `INSERT INTO approval_items (session_id, position_index, label, page_number)
         VALUES ($1, $2, $3, $4)`,
        [session.id, i, pos.label || `Position ${i + 1}`, pos.page || 1]
      );
    }

    const frontendUrl = process.env.FRONTEND_URL || "https://mock-up-creator-hosted-web.onrender.com";

    res.json({
      success: true,
      sessionId: session.id,
      approvalUrl: `${frontendUrl}/approve/${session.id}`,
    });
  } catch (err) {
    console.error("Create approval session error:", err);
    res.status(500).json({ error: err.message });
  }
});

// Get approval session metadata
app.get("/api/approval-sessions/:sessionId", async (req, res) => {
  if (!pool) return res.status(503).json({ error: "Database not configured" });

  try {
    const { sessionId } = req.params;

    const sessionResult = await pool.query(
      `SELECT id, order_number, customer_name, recipient_name, logo_positions,
              status, created_at, completed_at, opened_at
       FROM approval_sessions WHERE id = $1`,
      [sessionId]
    );

    if (sessionResult.rows.length === 0) {
      return res.status(404).json({ error: "Session not found" });
    }

    const session = sessionResult.rows[0];

    // Record first-open timestamp (only if not already set, and only for pending sessions)
    if (!session.opened_at && session.status === "pending") {
      try {
        await pool.query(`UPDATE approval_sessions SET opened_at = NOW() WHERE id = $1 AND opened_at IS NULL`, [sessionId]);
      } catch {}
    }

    const itemsResult = await pool.query(
      `SELECT id, position_index, label, page_number, status, rejection_reason, reviewed_at
       FROM approval_items WHERE session_id = $1 ORDER BY position_index`,
      [sessionId]
    );

    res.json({
      success: true,
      session: {
        id: session.id,
        orderNumber: session.order_number,
        customerName: session.customer_name,
        recipientName: session.recipient_name,
        logoPositions: session.logo_positions,
        status: session.status,
        createdAt: session.created_at,
        completedAt: session.completed_at,
        items: itemsResult.rows.map((item) => ({
          id: item.id,
          positionIndex: item.position_index,
          label: item.label,
          pageNumber: item.page_number,
          status: item.status,
          rejectionReason: item.rejection_reason,
          reviewedAt: item.reviewed_at,
        })),
      },
    });
  } catch (err) {
    console.error("Get approval session error:", err);
    res.status(500).json({ error: err.message });
  }
});

// Stream PDF for approval session
app.get("/api/approval-sessions/:sessionId/pdf", async (req, res) => {
  if (!pool) return res.status(503).json({ error: "Database not configured" });

  try {
    const { sessionId } = req.params;
    const result = await pool.query(
      `SELECT pdf_data FROM approval_sessions WHERE id = $1`,
      [sessionId]
    );

    if (result.rows.length === 0) {
      return res.status(404).json({ error: "Session not found" });
    }

    if (!result.rows[0].pdf_data) {
      return res.status(410).json({ error: "PDF has been removed after approval" });
    }

    res.setHeader("Content-Type", "application/pdf");
    res.setHeader("Content-Disposition", "inline");
    res.send(result.rows[0].pdf_data);
  } catch (err) {
    console.error("Get approval PDF error:", err);
    res.status(500).json({ error: err.message });
  }
});

// Submit all reviews
app.post("/api/approval-sessions/:sessionId/submit", async (req, res) => {
  if (!pool) return res.status(503).json({ error: "Database not configured" });

  try {
    const { sessionId } = req.params;
    const { items, approverName, signatureData } = req.body;

    if (!items?.length) {
      return res.status(400).json({ error: "items array is required" });
    }

    const sessionCheck = await pool.query(
      `SELECT status FROM approval_sessions WHERE id = $1`,
      [sessionId]
    );
    if (sessionCheck.rows.length === 0) {
      return res.status(404).json({ error: "Session not found" });
    }
    if (sessionCheck.rows[0].status !== "pending") {
      return res.status(400).json({ error: "Session already completed" });
    }

    for (const item of items) {
      await pool.query(
        `UPDATE approval_items
         SET status = $1, rejection_reason = $2, reviewed_at = NOW()
         WHERE id = $3 AND session_id = $4`,
        [item.status, item.rejectionReason || null, item.itemId, sessionId]
      );
    }

    const allItems = await pool.query(
      `SELECT status FROM approval_items WHERE session_id = $1`,
      [sessionId]
    );
    const hasRejected = allItems.rows.some((i) => i.status === "rejected");
    const allReviewed = allItems.rows.every((i) => i.status !== "pending");

    let sessionStatus = "pending";
    if (allReviewed) {
      sessionStatus = hasRejected ? "changes_requested" : "approved";
      // Set status + clear the PDF data + capture submitter IP + approver info
      const submitterIp = req.headers["x-forwarded-for"]?.split(",")[0]?.trim() || req.socket?.remoteAddress || null;
      await pool.query(
        `UPDATE approval_sessions SET status = $1, completed_at = NOW(), submitter_ip = $3, approver_name = $4, signature_data = $5 WHERE id = $2`,
        [sessionStatus, sessionId, submitterIp, approverName || null, signatureData || null]
      );
    }

    res.json({ success: true, sessionStatus });

    // Send email notification to the person who created this proof (async, don't block response)
    if (allReviewed && process.env.SMTP_PASS) {
      try {
        // Get session details including created_by and PDF
        const sessionData = await pool.query(
          `SELECT customer_name, order_number, created_by, pdf_data, logo_positions, approver_name, signature_data, submitter_ip FROM approval_sessions WHERE id = $1`,
          [sessionId]
        );
        const sess = sessionData.rows[0];
        if (!sess) throw new Error("Session not found");

        // Route email based on who created the proof
        const emailMap = {
          "Dec": "dec@tuffshop.co.uk",
          "Harry": "harry.b@tuffshop.co.uk",
        };
        const recipientEmail = emailMap[sess.created_by] || "dec@tuffshop.co.uk";

        // Get item results
        const itemResults = await pool.query(
          `SELECT label, status, rejection_reason FROM approval_items WHERE session_id = $1 ORDER BY position_index`,
          [sessionId]
        );

        const statusLabel = sessionStatus === "approved" ? "APPROVED" : "CHANGES REQUESTED";
        const statusColor = sessionStatus === "approved" ? "#48C549" : "#dc0032";

        const itemRows = itemResults.rows.map((item) => {
          const icon = item.status === "approved" ? "&#10003;" : "&#10007;";
          const color = item.status === "approved" ? "#48C549" : "#dc0032";
          return `<tr>
            <td style="padding:6px 8px;border-bottom:1px solid #eee;color:${color};font-size:16px">${icon}</td>
            <td style="padding:6px 8px;border-bottom:1px solid #eee">${item.label}</td>
            <td style="padding:6px 8px;border-bottom:1px solid #eee;color:#888;font-size:12px">${item.rejection_reason || ""}</td>
          </tr>`;
        }).join("");

        // Generate signed PDF for attachment if PDF data exists
        let attachments = [];
        if (sess.pdf_data) {
          try {
            const pdfDoc = await PDFDocument.load(sess.pdf_data);
            const pages = pdfDoc.getPages();
            const positions = typeof sess.logo_positions === 'string' ? JSON.parse(sess.logo_positions) : sess.logo_positions;

            // Add signature stamp (same logic as download endpoint)
            let sigImage = null;
            if (sess.signature_data) {
              try {
                const sigBase64 = sess.signature_data.replace(/^data:image\/png;base64,/, '');
                sigImage = await pdfDoc.embedPng(Buffer.from(sigBase64, 'base64'));
              } catch {}
            }

            const stampW = 150, stampH = 50;
            const stampX = 179 + 373 - stampW, stampY = 296;
            const yellow = rgb(0.95, 0.82, 0.08), black = rgb(0, 0, 0);

            const stampedPages = new Set();
            for (const pos of (positions || [])) {
              const pageIdx = (pos.page || 1) - 1;
              if (pageIdx >= pages.length || stampedPages.has(pageIdx)) continue;
              stampedPages.add(pageIdx);
              const page = pages[pageIdx];
              page.drawRectangle({ x: stampX, y: stampY, width: stampW, height: stampH, borderColor: yellow, borderWidth: 2, color: rgb(1, 1, 1), opacity: 0.25 });
              if (sigImage) {
                const sigDims = sigImage.scale(1);
                const sigScale = Math.min((stampW - 8) / sigDims.width, (stampH - 14) / sigDims.height);
                page.drawImage(sigImage, { x: stampX + (stampW - sigDims.width * sigScale) / 2, y: stampY + 14 + ((stampH - 14) - sigDims.height * sigScale) / 2, width: sigDims.width * sigScale, height: sigDims.height * sigScale });
              }
              if (sess.approver_name) page.drawText(sess.approver_name, { x: stampX + 4, y: stampY + 4, size: 6, color: black });
              if (sess.submitter_ip) page.drawText(sess.submitter_ip, { x: stampX + stampW - 4 - (sess.submitter_ip.length * 3.2), y: stampY + 4, size: 6, color: black });
            }

            const signedPdfBytes = await pdfDoc.save();
            const filename = `${sess.customer_name || "Customer"}-${sess.order_number || "proof"}-Signed.pdf`;
            attachments = [{ filename, content: Buffer.from(signedPdfBytes), contentType: "application/pdf" }];
          } catch (pdfErr) {
            console.error("Failed to generate signed PDF for email:", pdfErr.message);
          }
        }

        const transporter = nodemailer.createTransport({
          host: process.env.SMTP_SERVER || "mail-eu.smtp2go.com",
          port: parseInt(process.env.SMTP_PORT || "2525"),
          secure: false,
          auth: { user: process.env.SMTP_USERNAME || "tuffshop.co.uk", pass: process.env.SMTP_PASS },
        });

        await transporter.sendMail({
          from: `"Tuffshop Proof Approvals" <${process.env.PROOF_SENDER_EMAIL || "proofapprovals@tuffshop.co.uk"}>`,
          to: recipientEmail,
          subject: `Proof ${statusLabel} — ${sess.customer_name || "Customer"}${sess.order_number ? ` (Order ${sess.order_number})` : ""}`,
          html: `
            <div style="font-family:Arial,sans-serif;max-width:600px;margin:0 auto">
              <div style="background:#000;color:#fff;padding:16px 20px;border-bottom:3px solid ${statusColor}">
                <h2 style="margin:0;font-size:18px">Proof ${statusLabel}</h2>
              </div>
              <div style="padding:20px">
                <p><strong>Customer:</strong> ${sess.customer_name || "N/A"}</p>
                ${sess.order_number ? `<p><strong>Order:</strong> ${sess.order_number}</p>` : ""}
                ${sess.approver_name ? `<p><strong>Approved by:</strong> ${sess.approver_name}</p>` : ""}
                ${sess.submitter_ip ? `<p><strong>IP:</strong> ${sess.submitter_ip}</p>` : ""}
                <table style="width:100%;border-collapse:collapse;margin-top:16px">
                  <thead><tr style="background:#f5f5f5">
                    <th style="padding:6px 8px;text-align:left;width:30px"></th>
                    <th style="padding:6px 8px;text-align:left">Logo</th>
                    <th style="padding:6px 8px;text-align:left">Notes</th>
                  </tr></thead>
                  <tbody>${itemRows}</tbody>
                </table>
                ${attachments.length > 0 ? '<p style="color:#888;font-size:12px;margin-top:16px">Signed PDF attached.</p>' : ""}
              </div>
            </div>
          `,
          attachments,
        });
        console.log(`Proof approval email sent to ${recipientEmail} (created by ${sess.created_by})`);
      } catch (emailErr) {
        console.error("Failed to send approval notification email:", emailErr.message);
      }
    }
  } catch (err) {
    console.error("Submit approval error:", err);
    res.status(500).json({ error: err.message });
  }
});

// Clean up: purge PDF data from signed/completed sessions older than 14 days
// Pending sessions keep their PDF indefinitely (customer hasn't responded yet)
app.post("/api/approval-sessions/cleanup", async (req, res) => {
  if (!pool) return res.status(503).json({ error: "Database not configured" });
  try {
    const result = await pool.query(
      `UPDATE approval_sessions SET pdf_data = NULL, pdf_delete_after = NULL
       WHERE pdf_data IS NOT NULL AND (
         (status IN ('approved', 'changes_requested', 'archived') AND completed_at < NOW() - INTERVAL '14 days')
         OR (pdf_delete_after IS NOT NULL AND pdf_delete_after < NOW())
       )
       RETURNING id`
    );
    res.json({ success: true, cleaned: result.rowCount });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// Download signed PDF — overlays signature stamp on each logo box
app.get("/api/approval-sessions/:sessionId/download", async (req, res) => {
  if (!pool) return res.status(503).json({ error: "Database not configured" });
  try {
    const { sessionId } = req.params;
    const result = await pool.query(
      `SELECT pdf_data, customer_name, order_number, approver_name, signature_data, submitter_ip, logo_positions FROM approval_sessions WHERE id = $1`,
      [sessionId]
    );
    if (result.rows.length === 0) return res.status(404).json({ error: "Session not found" });
    if (!result.rows[0].pdf_data) return res.status(410).json({ error: "PDF no longer available" });

    const { pdf_data, customer_name, order_number, approver_name, signature_data, submitter_ip, logo_positions } = result.rows[0];
    const filename = `${customer_name || "Customer"}-${order_number || "proof"}-Signed.pdf`;

    // Overlay signature stamp on each logo box
    let finalPdfBytes = pdf_data;
    if (approver_name || signature_data) {
      try {
        const pdfDoc = await PDFDocument.load(pdf_data);
        const pages = pdfDoc.getPages();
        const positions = typeof logo_positions === 'string' ? JSON.parse(logo_positions) : logo_positions;

        // Embed signature image if available
        let sigImage = null;
        if (signature_data) {
          try {
            const sigBase64 = signature_data.replace(/^data:image\/png;base64,/, '');
            const sigBytes = Buffer.from(sigBase64, 'base64');
            sigImage = await pdfDoc.embedPng(sigBytes);
          } catch {}
        }

        // Stamp dimensions and position — bottom-right of the product image area
        // Product image area: x:179, y:296, width:373, height:373
        const stampW = 150;
        const stampH = 50;
        const stampX = 179 + 373 - stampW; // right-aligned within image area
        const stampY = 296; // bottom of image area
        const borderW = 2;
        const yellow = rgb(0.95, 0.82, 0.08);
        const black = rgb(0, 0, 0);

        // Draw stamp once per page
        const stampedPages = new Set();
        for (const pos of (positions || [])) {
          const pageIdx = (pos.page || 1) - 1;
          if (pageIdx >= pages.length || stampedPages.has(pageIdx)) continue;
          stampedPages.add(pageIdx);
          const page = pages[pageIdx];

          // Yellow border rectangle
          page.drawRectangle({
            x: stampX, y: stampY, width: stampW, height: stampH,
            borderColor: yellow, borderWidth: borderW,
            color: rgb(1, 1, 1), opacity: 0.25,
          });

          // Signature image (top portion)
          if (sigImage) {
            const sigAreaH = stampH - 14;
            const sigDims = sigImage.scale(1);
            const sigScale = Math.min((stampW - 8) / sigDims.width, sigAreaH / sigDims.height);
            const sigW = sigDims.width * sigScale;
            const sigH = sigDims.height * sigScale;
            page.drawImage(sigImage, {
              x: stampX + (stampW - sigW) / 2,
              y: stampY + 14 + (sigAreaH - sigH) / 2,
              width: sigW, height: sigH,
            });
          }

          // Name (bottom-left) and IP (bottom-right)
          if (approver_name) {
            page.drawText(approver_name, {
              x: stampX + 4, y: stampY + 4, size: 6, color: black,
            });
          }
          if (submitter_ip) {
            const ipText = submitter_ip;
            const ipX = stampX + stampW - 4 - (ipText.length * 3.2);
            page.drawText(ipText, {
              x: ipX, y: stampY + 4, size: 6, color: black,
            });
          }
        }

        finalPdfBytes = await pdfDoc.save();
      } catch (stampErr) {
        console.error("Stamp overlay failed, serving unsigned PDF:", stampErr);
        // Fallback: serve original PDF without stamp
      }
    }

    res.setHeader("Content-Type", "application/pdf");
    res.setHeader("Content-Disposition", `attachment; filename="${filename}"`);
    res.send(Buffer.from(finalPdfBytes));

    // Mark for deletion in 24 hours
    await pool.query(`UPDATE approval_sessions SET pdf_delete_after = NOW() + INTERVAL '24 hours' WHERE id = $1`, [sessionId]);
  } catch (err) {
    console.error("Download signed PDF error:", err);
    res.status(500).json({ error: err.message });
  }
});

// Archive a session
app.put("/api/approval-sessions/:sessionId/archive", async (req, res) => {
  if (!pool) return res.status(503).json({ error: "Database not configured" });
  try {
    const { sessionId } = req.params;
    await pool.query(
      `UPDATE approval_sessions SET status = 'archived' WHERE id = $1`,
      [sessionId]
    );
    res.json({ success: true });
  } catch (err) {
    console.error("Archive session error:", err);
    res.status(500).json({ error: err.message });
  }
});

// List sessions
app.get("/api/approval-sessions", async (req, res) => {
  if (!pool) return res.status(503).json({ error: "Database not configured" });

  try {
    const { orderNumber, includeArchived } = req.query;
    let query = `SELECT id, order_number, customer_name, recipient_name, status, created_at, completed_at, opened_at, submitter_ip, approver_name, signature_data, created_by, (pdf_data IS NOT NULL) AS has_pdf
                 FROM approval_sessions`;
    const conditions = [];
    const params = [];

    if (!includeArchived) {
      conditions.push(`status != 'archived'`);
    }
    if (orderNumber) {
      params.push(orderNumber);
      conditions.push(`order_number = $${params.length}`);
    }
    if (conditions.length) {
      query += ` WHERE ${conditions.join(" AND ")}`;
    }

    query += ` ORDER BY created_at DESC LIMIT 200`;

    const result = await pool.query(query, params);

    const sessions = await Promise.all(
      result.rows.map(async (session) => {
        const itemsResult = await pool.query(
          `SELECT id, label, status, rejection_reason FROM approval_items WHERE session_id = $1 ORDER BY position_index`,
          [session.id]
        );
        return {
          id: session.id,
          orderNumber: session.order_number,
          customerName: session.customer_name,
          recipientName: session.recipient_name,
          status: session.status,
          createdAt: session.created_at,
          completedAt: session.completed_at,
          openedAt: session.opened_at,
          submitterIp: session.submitter_ip,
          approverName: session.approver_name,
          signatureData: session.signature_data,
          createdBy: session.created_by,
          hasPdf: session.has_pdf,
          items: itemsResult.rows,
        };
      })
    );

    res.json({ success: true, sessions });
  } catch (err) {
    console.error("List approval sessions error:", err);
    res.status(500).json({ error: err.message });
  }
});

app.listen(PORT, () => {
  console.log(`✅ SFTP Proxy running on port ${PORT}`);
});






































