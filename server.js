// SFTP HTTPS Proxy for Codesandbox React mockup workflow
import express from "express";
import SFTPClient from "ssh2-sftp-client";
import dotenv from "dotenv";
import { Readable } from "stream";
import DocuSignService from './docusignService.js';
import cors from 'cors';
import docusign from 'docusign-esign';
import fs from 'fs';
import crypto from 'crypto';
import path from 'path';
import { fileURLToPath } from 'url';
import pkg from 'pg';
import { PDFDocument, rgb, degrees } from 'pdf-lib';
import nodemailer from 'nodemailer';
import { listStates, customerStateForBpStatus } from './orderPipelineMapper.js';
import { VARIABLE_SCHEMA, renderTemplate } from './orderPipelineRenderer.js';
import { deriveVariables, firstName as deriveFirstName, pickCustomerName } from './orderPipelineVariables.js';
import { checkReviewEligibility } from './orderPipelineEligibility.js';
import { SIGNATURE_HTML, SIGNATURE_TEXT } from './emailSignature.js';
import { attachFileToOrder as bpAttachFileToOrder, login as bpWebLogin, invalidateSession as bpWebInvalidate, fetchAuthed as bpWebFetch } from './bpWebSession.js';
import * as purchasingAuto from './purchasingAuto.js';
import { convertDesignToPng } from './wilcomClient.js';
import { generateJigEps, tileVectorEps, placementsFromTemplate, isVectorEps, buildGangSheetEps, parseEps, epsSizeMm } from './jigEps.js';
import { nestPrints } from './gangNest.js';
import { printJobsFromRows, extractLogoUrls, extractPrintedGarments } from './printLines.js';
import Anthropic from '@anthropic-ai/sdk';
import { spawn } from 'child_process';
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
    },
    keepAlive: true,               // keep sockets warm so Render doesn't drop idle ones
    idleTimeoutMillis: 30000,      // close idle clients ourselves before the DB does
    connectionTimeoutMillis: 10000,
    max: 10,
  });
  // CRITICAL: without an 'error' listener, a pooled connection dying in the
  // background (Render idle timeout, DB restart, network blip) is emitted as an
  // uncaught exception and crashes the process. This handler lets the pool quietly
  // discard the dead client and carry on — the next query gets a fresh connection.
  pool.on('error', (err) => {
    console.warn('[pg pool] idle client error (recovered):', err?.message || err);
  });
  console.log('📊 Using PostgreSQL database for DocuSign logs');
} else {
  console.log('📝 Using JSON file storage for DocuSign logs (local development)');
}

const app = express();
const PORT = process.env.PORT || 3000;

// Targeted safety net: only swallow EPIPE/ECONNRESET on writes, which
// just means a client (or upstream proxy) disconnected mid-response.
// Anything else still crashes so genuine bugs stay visible. Without
// this, a single mid-flight client abort can take the whole server
// down on Render.
process.on('uncaughtException', (err) => {
  if (err && (err.code === 'EPIPE' || err.code === 'ECONNRESET') && err.syscall === 'write') {
    console.warn(`[uncaught] swallowed ${err.code} on socket write — client disconnected`);
    return;
  }
  // A background Postgres connection dropping is recoverable — the pool discards
  // the dead client and reconnects on the next query. Don't crash over it.
  if (err && /Connection terminated unexpectedly|terminating connection|Client has encountered a connection error/i.test(err.message || '')) {
    console.warn('[uncaught] swallowed DB connection drop — pool will reconnect:', err.message);
    return;
  }
  console.error('[uncaught] FATAL:', err);
  // Let Node take down the process — Render will restart cleanly.
  process.exit(1);
});

// CORS MUST be registered before any body parser. If express.json runs
// first and throws (e.g. payload too large, malformed JSON, memory
// pressure), the error response goes out without CORS headers and the
// browser reports it as a CORS failure — masking the real cause.
app.use(cors({
  origin: true,
  credentials: true,
}));
// Stash the raw request body so the WhatsApp webhook can verify Meta's
// X-Hub-Signature-256 HMAC (which is computed over the exact bytes sent).
app.use(express.json({
  limit: '50mb',
  verify: (req, _res, buf) => { req.rawBody = buf; },
}));
app.use(express.urlencoded({ limit: '50mb', extended: true }));
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

// Convert a Wilcom .EMB embroidery file to a transparent TrueView PNG.
// Body: { embBase64: string, dpi?: number (96-300), inputExt?: string }
// Returns: { png: <base64>, widthMm, heightMm, numColours, numStitches }
app.post("/api/emb-to-png", async (req, res) => {
  try {
    const { embBase64, dpi, inputExt } = req.body || {};
    if (!embBase64 || typeof embBase64 !== "string") {
      return res.status(400).json({ error: 'Missing "embBase64" (base64-encoded design file)' });
    }
    const buf = Buffer.from(embBase64, "base64");
    if (!buf.length) return res.status(400).json({ error: "embBase64 did not decode to any bytes" });

    const { png, widthMm, heightMm, designInfo } = await convertDesignToPng(buf, {
      dpi: Number(dpi) || 300,
      inputExt: inputExt || "EMB",
    });

    res.json({
      png: png.toString("base64"),
      widthMm,
      heightMm,
      numColours: designInfo.num_colours ?? null,
      numStitches: designInfo.num_stitches ?? null,
    });
  } catch (err) {
    console.error("[emb-to-png] failed:", err.message);
    const isAuth = /WILCOM_USERNAME|login failed|MFA|new password/i.test(err.message);
    res.status(isAuth ? 502 : 500).json({ error: err.message });
  }
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
    // Approval workflow: new orders default to 'pending' until Mike (or any
    // approver) signs them off. Backfill — pre-existing rows on first deploy
    // were trusted, so they become 'approved'. After this column exists, the
    // default flips to 'pending' for all subsequent inserts.
    await pool.query(`
      ALTER TABLE urgent_orders
      ADD COLUMN IF NOT EXISTS approval_status TEXT
    `);
    await pool.query(`
      UPDATE urgent_orders SET approval_status = 'approved' WHERE approval_status IS NULL
    `);
    await pool.query(`
      ALTER TABLE urgent_orders ALTER COLUMN approval_status SET DEFAULT 'pending'
    `);
    await pool.query(`
      ALTER TABLE urgent_orders ALTER COLUMN approval_status SET NOT NULL
    `);
    await pool.query(`
      ALTER TABLE urgent_orders ADD COLUMN IF NOT EXISTS approved_by TEXT
    `);
    await pool.query(`
      ALTER TABLE urgent_orders ADD COLUMN IF NOT EXISTS approved_at TIMESTAMPTZ
    `);
    // Notification tracking — set when Mike has been emailed about a pending
    // order so the digest doesn't re-send the same items.
    await pool.query(`
      ALTER TABLE urgent_orders ADD COLUMN IF NOT EXISTS mike_notified_at TIMESTAMPTZ
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
  // Email Mike if any pending orders are waiting (cooldown enforced inside).
  maybeNotifyMike().catch((err) => console.error('[mike-notify] error:', err.message));
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
  // BP requires comma-separated IDs in path to be ascending (CMNC-008)
  allIdsArr.sort((a, b) => a - b);
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
      const errBody = await r.text().catch(() => '');
      console.error(
        `[stale-poll] batch fetch failed: ${r.status} — batch=${JSON.stringify(batch)} body=${errBody.slice(0, 500)}`
      );
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

// Read endpoint — frontend reads from Postgres directly, no Brightpearl call.
// Default returns only 'approved' rows (so standard / view-only users never
// see un-approved jobs). Pass ?include=pending to also see rows awaiting Mike.
app.get('/api/urgent-orders', async (req, res) => {
  if (!useDatabase) return res.json([]);
  try {
    const includePending = req.query.include === 'pending' || req.query.include === 'all';
    const where = includePending
      ? `WHERE approval_status IN ('pending', 'approved')`
      : `WHERE approval_status = 'approved'`;
    const result = await pool.query(`
      SELECT order_id, order_reference, urgent_by_date, is_asap, customer_name, business_name,
             decoration_type, order_total, created_by_id, created_by_name, placed_on,
             source, stale_status_id, stale_days, last_checked_at,
             approval_status, approved_by, approved_at
      FROM urgent_orders
      ${where}
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
      approvalStatus: r.approval_status || 'approved',
      approvedBy: r.approved_by,
      approvedAt: r.approved_at,
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

// ============================================================
// DTF Print Queue — orders flagged PCF_PRINTSNE ("Prints Needed") + their print
// lines (position/qty), to feed the gang-sheet builder. Custom fields aren't
// searchable in BP order-search, so we scan recent sales orders and check the
// flag in batches.
// ============================================================
const PRINTS_NEEDED_PCF = process.env.PRINTS_NEEDED_PCF || 'PCF_PRINTSNE';
const TUFF_SPORTSWEAR_CHANNEL_ID = Number(process.env.TUFF_SPORTSWEAR_CHANNEL_ID || 18);
const sleepMs = (ms) => new Promise((r) => setTimeout(r, ms));

// Garment-colour → which LOGO variant to print. Dark garments need the WHITE
// logo; light/hi-vis garments need the BLACK logo. Unknown colours return null
// so the operator is prompted to classify them (saved in colour_variants).
const DARK_COLOUR_KW = ['black', 'navy', 'bottle', 'forest', 'racing green', 'charcoal', 'maroon', 'burgundy', 'purple', 'royal', 'brown', 'olive', 'slate', 'graphite', 'anthracite', 'dark'];
const LIGHT_COLOUR_KW = ['white', 'hi vis', 'hi-vis', 'hivis', 'high vis', 'yellow', 'orange', 'natural', 'ivory', 'cream', 'sky', 'heather', 'grey', 'gray', 'silver', 'beige', 'sand', 'lime', 'pink', 'light'];
const colourOverrides = new Map(); // normalised colour -> isDark (true/false)
// Normalise a colour for matching: lowercase, strip ()[] and collapse spaces, so
// "(Steel Grey)" and "Steel Grey" classify the same.
const normColour = (c) => String(c || '').toLowerCase().replace(/[()[\]]/g, '').replace(/\s+/g, ' ').trim();

function classifyGarmentLogo(colour) {
  const lc = normColour(colour);
  if (!lc) return null;
  if (colourOverrides.has(lc)) return colourOverrides.get(lc) ? 'white' : 'black';
  if (DARK_COLOUR_KW.some((k) => lc.includes(k))) return 'white';
  if (LIGHT_COLOUR_KW.some((k) => lc.includes(k))) return 'black';
  return null; // unknown → prompt operator
}

async function loadColourOverrides() {
  if (!pool) return;
  try {
    const r = await pool.query('SELECT colour, is_dark FROM colour_variants');
    colourOverrides.clear();
    for (const row of r.rows) colourOverrides.set(normColour(row.colour), row.is_dark);
  } catch (e) { console.error('[print-queue] loadColourOverrides:', e.message); }
}
function bpBase() {
  const baseUrl = BRIGHTPEARL_DATACENTER === 'euw1'
    ? 'https://euw1.brightpearlconnect.com'
    : 'https://use1.brightpearlconnect.com';
  const headers = {
    'brightpearl-app-ref': process.env.BRIGHTPEARL_APP_REF,
    'brightpearl-account-token': BRIGHTPEARL_API_TOKEN,
    'Content-Type': 'application/json',
  };
  return { baseUrl, headers };
}
// Run async fn over items, `size` concurrently at a time.
async function mapPool(items, size, fn) {
  const out = [];
  for (let i = 0; i < items.length; i += size) out.push(...await Promise.all(items.slice(i, i + size).map(fn)));
  return out;
}
const orderToQueueItem = (o) => {
  const prints = printJobsFromRows(o.orderRows);
  if (!prints.length) return null;
  const cust = o.parties?.customer || {};
  const channelId = o.assignment?.current?.channelId ?? null;
  return {
    orderId: o.id,
    reference: o.reference,
    // companyName for trade; addressFullName (person) for individuals/website.
    customer: cust.companyName || cust.addressFullName || cust.contactName || null,
    contactName: cust.addressFullName || cust.contactName || null,
    channelId,
    // Sportswear orders: the logo per print line is in its description (logoDetail),
    // not the customer's OneDrive file.
    sportswear: channelId === TUFF_SPORTSWEAR_CHANNEL_ID,
    logoUrls: extractLogoUrls(o.orderRows), // website orders embed the artwork URL
    // Printed-garment colour breakdown + which logo variant each colour needs.
    garments: extractPrintedGarments(o.orderRows).map((g) => ({ ...g, logo: classifyGarmentLogo(g.colour) })),
    prints,
    totalPrints: prints.reduce((a, p) => a + p.qty, 0),
  };
};

// Custom fields for a set of order IDs. Tries BP's id-set batch; if the account
// returns a flat map (batch unsupported) it falls back to per-order. Returns
// { [orderId]: { PCF_...: value } }.
async function bpCustomFieldsForIds(ids, baseUrl, headers) {
  if (!ids.length) return {};
  try {
    const url = `${baseUrl}/public-api/${BRIGHTPEARL_ACCOUNT_ID}/order-service/order/${ids.join(',')}/custom-field`;
    const r = await fetch(url, { headers });
    if (r.ok) {
      const resp = (await r.json().catch(() => null))?.response || {};
      const keys = Object.keys(resp);
      const keyedById = keys.length > 0 && keys.every((k) => /^\d+$/.test(k));
      if (keyedById) return resp;                       // batch worked
      if (ids.length === 1) return { [ids[0]]: resp };  // single order
      // multi-id but flat map → batch not supported; fall through to per-order
    }
  } catch { /* fall through */ }
  // per-order, 6 concurrent (id-set batch isn't supported on this account)
  const pairs = await mapPool(ids, 6, async (id) => {
    try {
      const rr = await fetch(`${baseUrl}/public-api/${BRIGHTPEARL_ACCOUNT_ID}/order-service/order/${id}/custom-field`, { headers });
      return [id, rr.ok ? ((await rr.json().catch(() => null))?.response || {}) : {}];
    } catch { return [id, {}]; }
  });
  return Object.fromEntries(pairs);
}

// Persistent queue table — kept live by the BP webhook (per-order), with the
// scan only as an occasional reconcile backstop.
async function initializePrintQueueTable() {
  if (!pool) return;
  try {
    await pool.query(`
      CREATE TABLE IF NOT EXISTS print_queue (
        order_id BIGINT PRIMARY KEY,
        reference TEXT,
        customer TEXT,
        contact_name TEXT,
        channel_id BIGINT,
        logo_urls JSONB NOT NULL DEFAULT '[]'::jsonb,
        garments JSONB NOT NULL DEFAULT '[]'::jsonb,
        prints JSONB NOT NULL DEFAULT '[]'::jsonb,
        total_prints INTEGER NOT NULL DEFAULT 0,
        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
      );
      ALTER TABLE print_queue ADD COLUMN IF NOT EXISTS contact_name TEXT;
      ALTER TABLE print_queue ADD COLUMN IF NOT EXISTS channel_id BIGINT;
      ALTER TABLE print_queue ADD COLUMN IF NOT EXISTS logo_urls JSONB NOT NULL DEFAULT '[]'::jsonb;
      ALTER TABLE print_queue ADD COLUMN IF NOT EXISTS garments JSONB NOT NULL DEFAULT '[]'::jsonb;
      CREATE TABLE IF NOT EXISTS colour_variants (
        colour TEXT PRIMARY KEY,
        is_dark BOOLEAN NOT NULL,
        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
      );
    `);
    await loadColourOverrides();
  } catch (err) {
    console.error('[print-queue] table init failed:', err.message);
  }
}

// Upsert one queue item (shared by the filter refresh, scan, and webhook).
async function upsertPrintQueueRow(item) {
  await pool.query(
    `INSERT INTO print_queue (order_id, reference, customer, contact_name, channel_id, logo_urls, garments, prints, total_prints, updated_at)
     VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9, NOW())
     ON CONFLICT (order_id) DO UPDATE SET
       reference = EXCLUDED.reference, customer = EXCLUDED.customer, contact_name = EXCLUDED.contact_name,
       channel_id = EXCLUDED.channel_id, logo_urls = EXCLUDED.logo_urls, garments = EXCLUDED.garments,
       prints = EXCLUDED.prints, total_prints = EXCLUDED.total_prints, updated_at = NOW()`,
    [item.orderId, item.reference, item.customer, item.contactName || null, item.channelId ?? null,
     JSON.stringify(item.logoUrls || []), JSON.stringify(item.garments || []), JSON.stringify(item.prints), item.totalPrints]
  );
}

// Re-evaluate ONE order: if flagged (PCF_PRINTSNE) and it has print lines, upsert
// it into print_queue; otherwise remove it. This is what the webhook calls.
async function evaluatePrintOrder(orderId) {
  if (!pool) return { orderId, error: 'no db' };
  const { baseUrl, headers } = bpBase();
  const cf = await bpCustomFieldsForIds([orderId], baseUrl, headers);
  const flagged = isBadgeYes(cf[orderId]?.[PRINTS_NEEDED_PCF]);
  if (!flagged) {
    await pool.query('DELETE FROM print_queue WHERE order_id = $1', [orderId]);
    return { orderId, flagged: false, removed: true };
  }
  const od = await fetch(`${baseUrl}/public-api/${BRIGHTPEARL_ACCOUNT_ID}/order-service/order/${orderId}`, { headers });
  const o = (await od.json().catch(() => null))?.response?.[0];
  const item = o ? orderToQueueItem(o) : null;
  if (!item) {
    await pool.query('DELETE FROM print_queue WHERE order_id = $1', [orderId]);
    return { orderId, flagged: true, noPrints: true, removed: true };
  }
  await upsertPrintQueueRow(item);
  return { orderId, flagged: true, upserted: true, item };
}

// Background reconcile cache state (the scan is the backstop, not the primary).
let printQueueCache = { generatedAt: null, refreshing: false, error: null, scanned: 0, flagged: 0, jobs: 0, queue: [], tookMs: null, days: null };

async function refreshPrintQueue({ days = 21, maxOrders = 10000 } = {}) {
  if (printQueueCache.refreshing) return printQueueCache;
  if (!BRIGHTPEARL_API_TOKEN || !BRIGHTPEARL_ACCOUNT_ID) { printQueueCache.error = 'no BP creds'; return printQueueCache; }
  printQueueCache = { ...printQueueCache, refreshing: true };
  const started = Date.now();
  try {
    const { baseUrl, headers } = bpBase();
    const searchBase = `${baseUrl}/public-api/${BRIGHTPEARL_ACCOUNT_ID}/order-service/order-search?orderTypeId=1`;
    const fromIso = new Date(Date.now() - days * 24 * 60 * 60 * 1000).toISOString();
    const updatedFilter = encodeURIComponent(`${fromIso}/`);

    // 1) Collect ALL order IDs updated in the window (a flagged order can have
    // any orderId — it's recently UPDATED, not necessarily recently created —
    // so we scan the whole window, not a newest-by-id subset).
    const ids = [];
    let total = 0;
    let firstResult = 1;
    while (ids.length < maxOrders) {
      const r = await fetch(`${searchBase}&updatedOn=${updatedFilter}&pageSize=500&firstResult=${firstResult}`, { headers });
      if (!r.ok) break;
      const d = await r.json().catch(() => null);
      const results = d?.response?.results || [];
      total = d?.response?.metaData?.resultsAvailable || total;
      results.forEach((row) => ids.push(Number(Array.isArray(row) ? row[0] : row)));
      if (results.length < 500) break;
      firstResult += 500;
      await sleepMs(80);
    }
    const scanIds = [...new Set(ids)].slice(0, maxOrders);

    // 2) check the Prints Needed flag (per-order, concurrent inside batches)
    const flaggedIds = [];
    for (let i = 0; i < scanIds.length; i += 60) {
      const batch = scanIds.slice(i, i + 60);
      const cf = await bpCustomFieldsForIds(batch, baseUrl, headers);
      for (const oid of batch) if (isBadgeYes(cf[oid]?.[PRINTS_NEEDED_PCF])) flaggedIds.push(oid);
    }

    // 3) details for flagged orders → parse print lines. Fetch per-order (the
    // single GET reliably includes orderRows; the id-set batch GET does not).
    const items = await mapPool(flaggedIds, 6, async (id) => {
      try {
        const r = await fetch(`${baseUrl}/public-api/${BRIGHTPEARL_ACCOUNT_ID}/order-service/order/${id}`, { headers });
        const o = (await r.json().catch(() => null))?.response?.[0];
        return o ? orderToQueueItem(o) : null;
      } catch { return null; }
    });
    const queue = items.filter(Boolean).sort((a, b) => String(a.customer || '').localeCompare(String(b.customer || '')));

    // Sync the table: upsert the flagged-with-prints orders, and drop any rows
    // in the scanned window that are no longer flagged (caught an unflag the
    // webhook may have missed).
    if (pool) {
      for (const it of queue) await upsertPrintQueueRow(it);
      const keepIds = queue.map((q) => q.orderId);
      await pool.query(
        'DELETE FROM print_queue WHERE order_id = ANY($1::bigint[]) AND NOT (order_id = ANY($2::bigint[]))',
        [scanIds, keepIds.length ? keepIds : [-1]]
      );
    }

    printQueueCache = {
      generatedAt: new Date().toISOString(), refreshing: false, error: null,
      windowTotal: total, scanned: scanIds.length, flagged: flaggedIds.length, jobs: queue.length, queue,
      tookMs: Date.now() - started, days,
    };
  } catch (err) {
    console.error('[print-queue] refresh failed:', err.message);
    printQueueCache = { ...printQueueCache, refreshing: false, error: err.message };
  }
  return printQueueCache;
}

// PRIMARY refresh: read the operator's curated "Prints Needed" saved report via
// the fileuploader@ web session, then sync the table to exactly those orders.
// Cheap (~one report fetch + one GET per listed order) — no scanning.
async function refreshPrintQueueFromFilter() {
  if (printQueueCache.refreshing) return printQueueCache;
  if (!pool) { printQueueCache.error = 'no db'; return printQueueCache; }
  printQueueCache = { ...printQueueCache, refreshing: true };
  const started = Date.now();
  try {
    const { baseUrl, headers } = bpBase();
    const { status, html } = await bpWebFetch(PRINTS_REPORT_URL);
    if (status !== 200 || /name=["']email_address["']/i.test(html)) {
      throw new Error(`filter fetch failed (status ${status}${/email_address/i.test(html) ? ', got login page' : ''})`);
    }
    const ids = [...new Set([...html.matchAll(/oID=(\d+)/gi)].map((m) => Number(m[1])))];

    const items = await mapPool(ids, 6, async (id) => {
      try {
        const r = await fetch(`${baseUrl}/public-api/${BRIGHTPEARL_ACCOUNT_ID}/order-service/order/${id}`, { headers });
        const o = (await r.json().catch(() => null))?.response?.[0];
        return o ? orderToQueueItem(o) : null;
      } catch { return null; }
    });
    const queue = items.filter(Boolean).sort((a, b) => String(a.customer || '').localeCompare(String(b.customer || '')));

    // Sync the table to exactly the filter result.
    for (const it of queue) await upsertPrintQueueRow(it);
    const keepIds = queue.map((q) => q.orderId);
    await pool.query('DELETE FROM print_queue WHERE NOT (order_id = ANY($1::bigint[]))', [keepIds.length ? keepIds : [-1]]);

    printQueueCache = {
      generatedAt: new Date().toISOString(), refreshing: false, error: null, source: 'filter',
      filterOrders: ids.length, jobs: queue.length, queue, tookMs: Date.now() - started,
    };
  } catch (err) {
    console.error('[print-queue] filter refresh failed:', err.message);
    printQueueCache = { ...printQueueCache, refreshing: false, error: err.message };
  }
  return printQueueCache;
}

// Serve the cached queue instantly. ?orderId=NNN is a live single-order test.
app.get('/api/print-queue', async (req, res) => {
  if (!BRIGHTPEARL_API_TOKEN || !BRIGHTPEARL_ACCOUNT_ID) {
    return res.status(500).json({ error: 'Brightpearl credentials not configured' });
  }
  // ?count=N → just the order count in an N-day updatedOn window (diagnostic).
  if (req.query.count) {
    const { baseUrl, headers } = bpBase();
    const d = Math.min(parseInt(req.query.count, 10) || 30, 365);
    const fromIso = new Date(Date.now() - d * 24 * 60 * 60 * 1000).toISOString();
    const r = await fetch(`${baseUrl}/public-api/${BRIGHTPEARL_ACCOUNT_ID}/order-service/order-search?orderTypeId=1&updatedOn=${encodeURIComponent(`${fromIso}/`)}&pageSize=1&firstResult=1`, { headers });
    const total = (await r.json().catch(() => null))?.response?.metaData?.resultsAvailable ?? null;
    return res.json({ days: d, ordersUpdatedInWindow: total });
  }
  if (req.query.orderId) {
    const { baseUrl, headers } = bpBase();
    const id = req.query.orderId;
    const cf = await bpCustomFieldsForIds([id], baseUrl, headers);
    const od = await fetch(`${baseUrl}/public-api/${BRIGHTPEARL_ACCOUNT_ID}/order-service/order/${id}`, { headers });
    const order = (await od.json().catch(() => null))?.response?.[0];
    return res.json({ orderId: id, printsNeeded: isBadgeYes(cf[id]?.[PRINTS_NEEDED_PCF]), pcfValue: cf[id]?.[PRINTS_NEEDED_PCF] ?? null, item: order ? orderToQueueItem(order) : null });
  }
  // Normal path: read the live queue from the table (kept current by the
  // webhook + reconcile). No scanning on request.
  if (!pool) return res.status(503).json({ error: 'Database not configured' });
  try {
    const r = await pool.query('SELECT order_id, reference, customer, contact_name, channel_id, logo_urls, garments, prints, total_prints, updated_at FROM print_queue ORDER BY customer NULLS LAST, order_id');
    const queue = r.rows.map((row) => ({
      orderId: Number(row.order_id), reference: row.reference, customer: row.customer,
      contactName: row.contact_name, channelId: row.channel_id != null ? Number(row.channel_id) : null,
      sportswear: row.channel_id != null && Number(row.channel_id) === TUFF_SPORTSWEAR_CHANNEL_ID,
      logoUrls: row.logo_urls, garments: row.garments || [], prints: row.prints, totalPrints: row.total_prints, updatedAt: row.updated_at,
    }));
    res.json({
      source: 'table', jobs: queue.length, queue,
      reconcile: { generatedAt: printQueueCache.generatedAt, refreshing: printQueueCache.refreshing, scanned: printQueueCache.scanned, error: printQueueCache.error },
    });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// Trigger a refresh; returns immediately. Default reads the saved filter;
// ?scan=1 forces the heavy full-window scan fallback.
app.post('/api/print-queue/refresh', (req, res) => {
  if (printQueueCache.refreshing) return res.json({ started: false, refreshing: true });
  if (req.query.scan === '1' || req.body?.scan) {
    const days = Math.min(parseInt(req.body?.days, 10) || 21, 365);
    refreshPrintQueue({ days }).catch(() => {});
    return res.json({ started: true, mode: 'scan' });
  }
  refreshPrintQueueFromFilter().catch(() => {});
  res.json({ started: true, mode: 'filter' });
});

// Classify an unknown garment colour as dark (→ white logo) or light (→ black
// logo). Saves it, reloads the map, and re-runs the filter so the queue updates.
app.post('/api/print-queue/colours', async (req, res) => {
  if (!pool) return res.status(503).json({ error: 'Database not configured' });
  const { colour, isDark } = req.body || {};
  if (!colour || typeof isDark !== 'boolean') return res.status(400).json({ error: 'colour + isDark (boolean) required' });
  try {
    await pool.query(
      `INSERT INTO colour_variants (colour, is_dark, updated_at) VALUES ($1,$2,NOW())
       ON CONFLICT (colour) DO UPDATE SET is_dark = EXCLUDED.is_dark, updated_at = NOW()`,
      [colour.trim(), isDark]
    );
    await loadColourOverrides();
    refreshPrintQueueFromFilter().catch(() => {});
    res.json({ success: true, colour: colour.trim(), isDark });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.get('/api/print-queue/colours', async (req, res) => {
  if (!pool) return res.status(503).json({ error: 'Database not configured' });
  try {
    const r = await pool.query('SELECT colour, is_dark FROM colour_variants ORDER BY colour');
    res.json({ overrides: r.rows });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// Identify the company/brand in a customer-uploaded logo image (website orders
// with no company name to match a file on). Uses Claude vision to read the text
// in the logo. Results are cached by image URL so the same logo is never billed
// twice. Costs a fraction of a penny per new logo; only called for jobs that
// have nothing else to match on. Requires ANTHROPIC_API_KEY on the backend.
const LOGO_IDENT_PROMPT =
  'This is a company logo a customer uploaded for printing. Reply with ONLY the ' +
  'company or brand name as written in the logo (read the text). If the logo has ' +
  'no readable company/brand name, reply with exactly: UNKNOWN. No quotes, no ' +
  'punctuation, no extra words — just the name or UNKNOWN.';
const SUPPORTED_IMG = ['image/jpeg', 'image/png', 'image/gif', 'image/webp'];

async function ensureLogoIdentTable() {
  await pool.query(`CREATE TABLE IF NOT EXISTS logo_ident (
    url_hash TEXT PRIMARY KEY, url TEXT, name TEXT, created_at TIMESTAMPTZ DEFAULT NOW())`);
}

app.post('/api/identify-logo', async (req, res) => {
  const imageUrl = req.body?.imageUrl;
  if (!imageUrl || typeof imageUrl !== 'string') return res.status(400).json({ error: 'imageUrl required' });
  if (!process.env.ANTHROPIC_API_KEY) return res.status(501).json({ error: 'ANTHROPIC_API_KEY not configured on the backend' });
  if (!pool) return res.status(503).json({ error: 'Database not configured' });
  try {
    await ensureLogoIdentTable();
    const key = crypto.createHash('md5').update(imageUrl).digest('hex');
    const hit = await pool.query('SELECT name FROM logo_ident WHERE url_hash = $1', [key]);
    if (hit.rows[0]) return res.json({ name: hit.rows[0].name || '', cached: true });

    const ir = await fetch(imageUrl);
    if (!ir.ok) return res.status(502).json({ error: `could not fetch image (${ir.status})` });
    const ct = (ir.headers.get('content-type') || '').split(';')[0].trim().toLowerCase();
    if (!SUPPORTED_IMG.includes(ct)) return res.status(415).json({ error: `unsupported image type${ct ? ` (${ct})` : ''}` });
    const b64 = Buffer.from(await ir.arrayBuffer()).toString('base64');

    const client = new Anthropic();
    const msg = await client.messages.create({
      model: 'claude-haiku-4-5',
      max_tokens: 60,
      messages: [{
        role: 'user',
        content: [
          { type: 'image', source: { type: 'base64', media_type: ct, data: b64 } },
          { type: 'text', text: LOGO_IDENT_PROMPT },
        ],
      }],
    });
    let name = (msg.content?.find((b) => b.type === 'text')?.text || '').trim();
    if (!name || /^unknown$/i.test(name)) name = '';
    await pool.query(
      `INSERT INTO logo_ident (url_hash, url, name, created_at) VALUES ($1,$2,$3,NOW())
       ON CONFLICT (url_hash) DO UPDATE SET name = EXCLUDED.name`,
      [key, imageUrl, name]
    );
    res.json({ name });
  } catch (e) {
    console.error('[identify-logo] failed:', e.message);
    res.status(500).json({ error: e.message });
  }
});

// Mark a job as printed: clear PCF_PRINTSNE in Brightpearl (so it leaves the
// filter) and remove it from the queue table.
app.post('/api/print-queue/mark-printed', async (req, res) => {
  if (!pool) return res.status(503).json({ error: 'Database not configured' });
  if (!BRIGHTPEARL_API_TOKEN || !BRIGHTPEARL_ACCOUNT_ID) return res.status(500).json({ error: 'Brightpearl credentials not configured' });
  const orderId = req.body?.orderId;
  if (!orderId) return res.status(400).json({ error: 'orderId required' });
  try {
    const { baseUrl, headers } = bpBase();
    const url = `${baseUrl}/public-api/${BRIGHTPEARL_ACCOUNT_ID}/order-service/order/${orderId}/custom-field`;
    const r = await fetch(url, {
      method: 'PATCH', headers,
      body: JSON.stringify([{ op: 'replace', path: `/${PRINTS_NEEDED_PCF}`, value: false }]),
    });
    if (!r.ok) { const t = await r.text(); return res.status(r.status).json({ error: `Brightpearl update failed: ${t.slice(0, 300)}` }); }
    await pool.query('DELETE FROM print_queue WHERE order_id = $1', [orderId]);
    res.json({ success: true, orderId });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// Build DTF gang sheets from uploaded artwork + quantities. Each item is one
// EPS (base64) with a qty; we nest all copies onto 570mm-wide sheets (full
// width, no edge margin) and return the sheet EPS(s).
app.post('/api/print-queue/build', async (req, res) => {
  const { items, sheetWmm = 570, maxSheetLmm = 1200, gapMm = 5, marginMm = 0 } = req.body || {};
  if (!Array.isArray(items) || !items.length) return res.status(400).json({ error: 'items required' });
  try {
    const parsed = items.map((it) => {
      try {
        const buf = Buffer.from(it.epsBase64 || '', 'base64');
        const { text, bbox } = parseEps(buf);
        if (!bbox) return null;
        const { wmm, hmm } = epsSizeMm(bbox);
        return { text, bbox, wmm, hmm, qty: Math.max(0, Math.round(Number(it.qty) || 0)), label: it.label || '' };
      } catch { return null; }
    }).filter(Boolean);
    if (!parsed.length) return res.status(400).json({ error: 'no valid EPS items' });

    const nestItems = [];
    parsed.forEach((p, pi) => { for (let i = 0; i < p.qty; i++) nestItems.push({ id: `${pi}-${i}`, wmm: p.wmm, hmm: p.hmm, allowRotate: true, _p: pi }); });
    if (!nestItems.length) return res.status(400).json({ error: 'no prints (all quantities 0)' });

    const { sheets, oversized } = nestPrints({ items: nestItems, sheetWmm, maxSheetLmm, gapMm, marginMm });
    const out = sheets.map((sh, si) => {
      const placements = sh.placements.map((pl) => {
        const p = parsed[pl.item._p];
        return { text: p.text, bbox: p.bbox, xmm: pl.xmm, ymm: pl.ymm, scale: 1, rotation: pl.rotated ? 90 : 0 };
      });
      const eps = buildGangSheetEps({ pageWmm: sheetWmm, pageHmm: sh.lengthMm, placements });
      return { sheet: si + 1, lengthMm: Math.round(sh.lengthMm), efficiencyPct: Math.round(sh.efficiency * 100), prints: sh.placements.length, epsBase64: eps.toString('base64') };
    });
    res.json({ sheets: out, sheetWmm, totalPrints: nestItems.length, oversized: oversized.map((o) => parsed[o._p]?.label || o.id) });
  } catch (e) {
    console.error('[print-queue] build failed:', e.message);
    res.status(500).json({ error: e.message });
  }
});

// Diagnostic: test the BP web-session login (used for proof/EMB/colour-sheet
// uploads). Echoes which account it's using + the exact login result/error.
app.get('/api/bp-web/login-test', async (req, res) => {
  const info = { email: process.env.BP_WEB_EMAIL || null, client: process.env.BP_WEB_CLIENT_ID || 'tuffworkwear', host: process.env.BP_WEB_HOST || 'https://euw1.brightpearlapp.com' };
  try {
    bpWebInvalidate();
    const s = await bpWebLogin();
    const cookies = await s.jar.getCookieString(info.host);
    res.json({ ok: true, ...info, cookieNames: cookies.split('; ').map((c) => c.split('=')[0]).filter(Boolean) });
  } catch (err) {
    res.json({ ok: false, ...info, error: err.message });
  }
});

// Diagnostic: fetch the BP "Prints Needed" saved report via the fileuploader@
// web session and surface its structure + candidate order ids, so we can write
// the parser. ?url= overrides the default report.
const PRINTS_REPORT_URL = process.env.PRINTS_REPORT_URL || 'https://euw1.brightpearlapp.com/report.php?report_type=sales&preset_id=422';
app.get('/api/print-queue/filter-debug', async (req, res) => {
  try {
    const url = req.query.url || PRINTS_REPORT_URL;
    const { status, html, finalUrl } = await bpWebFetch(url);
    const uniq = (re) => [...new Set([...html.matchAll(re)].map((m) => m[1]))];
    const oIDs = uniq(/oID=(\d+)/gi);
    const ordersId = uniq(/orders?[_-]?id["'=\/:\s]+(\d+)/gi);
    const ordNum = uniq(/order(?:Number|_number|num)["'=:\s]+(\d+)/gi);
    res.json({
      status, finalUrl, htmlLength: html.length,
      looksLikeLogin: /name=["']email_address["']/i.test(html),
      oIDcount: oIDs.length, oIDsample: oIDs.slice(0, 30),
      ordersIdSample: ordersId.slice(0, 30),
      ordNumSample: ordNum.slice(0, 30),
      htmlHead: html.slice(0, 2000),
    });
  } catch (err) {
    res.json({ error: err.message });
  }
});

// BP webhook receiver: BP calls this when an order changes (set up a BP
// automation: when PCF_PRINTSNE changes → call this URL with the order id).
// Accepts the id from ?orderId=, body.id/orderId/resourceId, or an array of
// such events. Re-evaluates each order → upsert/remove in print_queue. Always
// 200 so BP doesn't retry-storm on our errors.
app.all('/api/print-queue/webhook', async (req, res) => {
  try {
    // Optional shared-secret: if PRINT_QUEUE_WEBHOOK_TOKEN is set, require it.
    const want = process.env.PRINT_QUEUE_WEBHOOK_TOKEN;
    if (want && req.query.token !== want) return res.status(401).json({ ok: false, error: 'bad token' });
    const body = req.body;
    const ids = new Set();
    const pick = (o) => { const v = o?.id ?? o?.orderId ?? o?.resourceId ?? o?.resourceKey; if (v != null && /^\d+$/.test(String(v))) ids.add(String(v)); };
    if (req.query.orderId) String(req.query.orderId).split(',').forEach((v) => /^\d+$/.test(v.trim()) && ids.add(v.trim()));
    if (Array.isArray(body)) body.forEach(pick);
    else if (body && typeof body === 'object') { pick(body); if (Array.isArray(body.events)) body.events.forEach(pick); }
    const idList = [...ids];
    if (!idList.length) return res.json({ ok: true, processed: 0, note: 'no order id found in webhook' });
    const results = await mapPool(idList, 4, (id) => evaluatePrintOrder(id).catch((e) => ({ orderId: id, error: e.message })));
    res.json({ ok: true, processed: idList.length, results });
  } catch (err) {
    console.error('[print-queue] webhook error:', err.message);
    res.json({ ok: false, error: err.message });
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

// Approval workflow ---------------------------------------------------------
// Approve a pending order — flips approval_status to 'approved' so it shows
// up on the standard / view-only boards. ?approvedBy=Name optional.
app.post('/api/urgent-orders/approve/:orderId', async (req, res) => {
  if (!useDatabase) return res.status(500).json({ error: 'Database unavailable' });
  try {
    const orderId = parseInt(req.params.orderId, 10);
    const approvedBy = (req.body?.approvedBy || req.query.approvedBy || 'Mike').slice(0, 64);
    const r = await pool.query(`
      UPDATE urgent_orders
      SET approval_status = 'approved', approved_by = $2, approved_at = NOW()
      WHERE order_id = $1
      RETURNING order_id, approval_status
    `, [orderId, approvedBy]);
    if (r.rowCount === 0) return res.status(404).json({ error: 'Order not found in cache' });
    res.json({ success: true, approvalStatus: r.rows[0].approval_status });
  } catch (err) {
    console.error('[urgent/approve] error:', err.message);
    res.status(500).json({ error: err.message });
  }
});

// Reject a pending order — clears the urgent flag in Brightpearl (so the
// poller doesn't immediately re-add it) and removes the row. The card on
// Mike's board shows the createdByName so he knows who to email manually
// for a new date.
app.post('/api/urgent-orders/reject/:orderId', async (req, res) => {
  const { orderId } = req.params;
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
    let isStale = false;
    if (useDatabase) {
      const existing = await pool.query(
        'SELECT source FROM urgent_orders WHERE order_id = $1',
        [orderId]
      );
      isStale = existing.rows[0]?.source === 'stale';
    }

    // Stale orders have no PCF flag to clear — they're auto-detected by the
    // stale poller. Rejecting one just dismisses it from the cache; the next
    // stale-poll cycle will re-add it if it's still genuinely stale.
    if (!isStale) {
      const cfResp = await fetch(
        `${baseUrl}/public-api/${BRIGHTPEARL_ACCOUNT_ID}/order-service/order/${orderId}/custom-field`,
        { method: 'GET', headers }
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
        const url = `${baseUrl}/public-api/${BRIGHTPEARL_ACCOUNT_ID}/order-service/order/${orderId}/custom-field`;
        let r = await fetch(url, { method: 'PATCH', headers, body: JSON.stringify(operations) });
        if (!r.ok) {
          // Some BP accounts reject 'remove' — retry with replace-null
          const fallback = operations.map((op) =>
            op.op === 'remove' ? { op: 'replace', path: op.path, value: null } : op
          );
          r = await fetch(url, { method: 'PATCH', headers, body: JSON.stringify(fallback) });
        }
        if (!r.ok) {
          const errText = await r.text();
          console.error(`[urgent/reject] patch failed for ${orderId}:`, errText);
          return res.status(r.status).json({ error: errText });
        }
      }
    }

    // Remove from cache. Mike will manually email the createdBy person to get
    // a new date — the order will reappear on next poll if BP gets re-flagged.
    if (useDatabase) {
      await pool.query('DELETE FROM urgent_orders WHERE order_id = $1', [orderId]);
    }
    res.json({ success: true, wasStale: isStale });
  } catch (err) {
    console.error('[urgent/reject] error:', err.message);
    res.status(500).json({ error: err.message });
  }
});

// Mike approval-digest emailer ----------------------------------------------
// Sends a single email listing all pending orders that haven't been notified
// about yet. Called after each urgent-poll cycle.
const MIKE_NOTIFY_EMAIL = process.env.MIKE_NOTIFY_EMAIL || 'michael.hodgkins@tuffshop.co.uk';
const MIKE_NOTIFY_COOLDOWN_MS = 15 * 60 * 1000;
let lastMikeNotifyAt = 0;

async function maybeNotifyMike() {
  if (!useDatabase) return;
  if (!process.env.SMTP_PASS) return; // SMTP not configured, silently skip
  const now = Date.now();
  if (now - lastMikeNotifyAt < MIKE_NOTIFY_COOLDOWN_MS) return;

  try {
    const pending = await pool.query(`
      SELECT order_id, order_reference, urgent_by_date, is_asap, customer_name, business_name, created_by_name
      FROM urgent_orders
      WHERE approval_status = 'pending' AND mike_notified_at IS NULL
      ORDER BY is_asap DESC, urgent_by_date ASC NULLS LAST, order_id ASC
    `);
    if (pending.rows.length === 0) return;

    const rows = pending.rows;
    const subject = `Urgent Jobs: ${rows.length} order${rows.length === 1 ? '' : 's'} need approval`;
    const lines = rows.map((r) => {
      const ref = r.order_reference || `#${r.order_id}`;
      const due = r.is_asap
        ? 'ASAP'
        : (r.urgent_by_date ? new Date(r.urgent_by_date).toLocaleDateString('en-GB') : 'No date');
      const who = r.created_by_name || 'Unknown';
      const cust = r.business_name || r.customer_name || '';
      return `• ${ref} — ${due} — ${cust} — flagged by ${who}`;
    });
    const text = `${rows.length} urgent order${rows.length === 1 ? '' : 's'} awaiting approval:\n\n${lines.join('\n')}\n\nApprove at: https://urgent-jobs.onrender.com/?User=Mike`;

    const htmlList = rows.map((r) => {
      const ref = r.order_reference || `#${r.order_id}`;
      const due = r.is_asap
        ? '<strong style="color:#c62828">ASAP</strong>'
        : (r.urgent_by_date ? new Date(r.urgent_by_date).toLocaleDateString('en-GB') : 'No date');
      const who = r.created_by_name || 'Unknown';
      const cust = r.business_name || r.customer_name || '';
      return `<li><strong>${ref}</strong> — ${due} — ${cust} — flagged by ${who}</li>`;
    }).join('');
    const html = `
      <p>${rows.length} urgent order${rows.length === 1 ? '' : 's'} need your approval:</p>
      <ul>${htmlList}</ul>
      <p><a href="https://urgent-jobs.onrender.com/?User=Mike">Open the approval board</a></p>
    `;

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
      from: '"Tuff Workwear" <noreply@tuffshop.co.uk>',
      to: MIKE_NOTIFY_EMAIL,
      subject,
      text,
      html,
    });

    await pool.query(
      `UPDATE urgent_orders SET mike_notified_at = NOW() WHERE order_id = ANY($1::bigint[])`,
      [rows.map((r) => Number(r.order_id))]
    );
    lastMikeNotifyAt = now;
    console.log(`[mike-notify] sent digest of ${rows.length} pending order(s) to ${MIKE_NOTIFY_EMAIL}`);
  } catch (err) {
    console.error('[mike-notify] failed:', err.message);
  }
}

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
//   PROOF_CHASE_DAYS        — business days in status before chasing
//                             (default 3, weekends excluded — no sends Sat/Sun)
//   PROOF_CHASE_DRY_RUN     — "false" to actually send (default "true")
//   PROOF_CHASE_SENDER      — From: address (default noreply@tuffshop.co.uk)
// ===========================================================================

// Count Mon–Fri day boundaries crossed between two timestamps. Used so
// PROOF_CHASE_DAYS reflects working days, not calendar days — a proof
// sent Friday afternoon shouldn't trigger a chase Monday morning.
function businessDaysBetween(start, end) {
  const dayMs = 24 * 60 * 60 * 1000;
  const startDay = Math.floor(start.getTime() / dayMs);
  const endDay = Math.floor(end.getTime() / dayMs);
  let count = 0;
  for (let d = startDay + 1; d <= endDay; d++) {
    const dow = new Date(d * dayMs).getUTCDay();
    if (dow !== 0 && dow !== 6) count++;
  }
  return count;
}

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
// Signature constants moved to emailSignature.js so the order tracking
// pipeline can reuse the same block. Aliases below keep the existing
// proof-chase references intact without touching that module.
const PROOF_CHASE_SIGNATURE_TEXT = SIGNATURE_TEXT;
const PROOF_CHASE_SIGNATURE_HTML = SIGNATURE_HTML;

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

// Operator → email map. Used in two places:
//   (a) routing approval-result emails to the operator who created the
//       proof (existing flow at /api/approval-sessions/:id/respond).
//   (b) setting the From address when an operator manually sends a proof
//       via the new /api/proof/send-email endpoint.
// Configurable via OPERATOR_EMAIL_MAP env var (JSON) to add operators
// without a deploy.
const DEFAULT_OPERATOR_EMAIL_MAP = {
  "Dec": "dec@tuffshop.co.uk",
  "Harry": "harry.b@tuffshop.co.uk",
};
function loadOperatorEmailMap() {
  let map = { ...DEFAULT_OPERATOR_EMAIL_MAP };
  if (process.env.OPERATOR_EMAIL_MAP) {
    try {
      const parsed = JSON.parse(process.env.OPERATOR_EMAIL_MAP);
      if (parsed && typeof parsed === 'object') map = { ...map, ...parsed };
    } catch (err) {
      console.warn('[operator-email-map] OPERATOR_EMAIL_MAP env not valid JSON:', err.message);
    }
  }
  return map;
}
const OPERATOR_EMAIL_MAP = loadOperatorEmailMap();
function operatorEmailFor(operator) {
  return OPERATOR_EMAIL_MAP[operator] || OPERATOR_EMAIL_MAP['Dec'] || 'dec@tuffshop.co.uk';
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

// Status IDs configurable so they can be changed without a deploy if BP
// is reconfigured. Defaults match the operator's current setup:
//   34 = Proof Required (set when sales books a proof)
//   35 = Proof Sent     (transition target once we've sent the proof)
const BP_STATUS_PROOF_REQUIRED = parseInt(process.env.BP_STATUS_PROOF_REQUIRED || '34', 10);
const BP_STATUS_PROOF_SENT = parseInt(process.env.BP_STATUS_PROOF_SENT || '35', 10);

// Conditional status transition: only flips an order from "Proof Required"
// to "Proof Sent" — if it's already in a later state (sent, approved, in
// production, etc.) we leave it alone. Safe to call on any order; will
// no-op when not in the source status. Fires a BP system note alongside
// the transition so the audit trail shows what triggered it.
// Lookup a Brightpearl product by SKU. Returns the product ID or null
// if not found / lookup failed.
async function bpLookupProductBySku(sku) {
  if (!BRIGHTPEARL_API_TOKEN || !BRIGHTPEARL_ACCOUNT_ID || !sku) return null;
  const baseUrl = BRIGHTPEARL_DATACENTER === 'euw1'
    ? 'https://euw1.brightpearlconnect.com'
    : 'https://use1.brightpearlconnect.com';
  const url = `${baseUrl}/public-api/${BRIGHTPEARL_ACCOUNT_ID}/product-service/product-search?SKU=${encodeURIComponent(sku)}&pageSize=1`;
  try {
    const r = await fetch(url, {
      headers: {
        'brightpearl-app-ref': process.env.BRIGHTPEARL_APP_REF,
        'brightpearl-account-token': BRIGHTPEARL_API_TOKEN,
      },
    });
    if (!r.ok) {
      console.warn(`[bp-product-lookup] SKU ${sku} → HTTP ${r.status}`);
      return null;
    }
    const data = await r.json();
    const results = data.response?.results || [];
    if (results.length === 0) {
      console.warn(`[bp-product-lookup] no product for SKU ${sku}`);
      return null;
    }
    // BP returns results as positional arrays. The metadata describes
    // which column is which. Find productId by column name.
    const fields = data.response?.metaData?.resultDescription || data.response?.metaData?.columns || [];
    let idx = 0;
    for (let i = 0; i < fields.length; i++) {
      const fname = (fields[i]?.name || fields[i] || '').toString().toLowerCase();
      if (fname === 'productid' || fname === 'id') { idx = i; break; }
    }
    return Number(results[0][idx]);
  } catch (err) {
    console.warn(`[bp-product-lookup] SKU ${sku} error: ${err.message}`);
    return null;
  }
}

// Add a row to an existing BP sales order. rowNetExVat is the ex-VAT
// line total (qty * unit price after discount). Tax computed at 20%
// VAT to match the customer-facing presentation.
async function bpAddOrderRow(orderId, productId, quantity, rowNetExVat, productName) {
  if (!BRIGHTPEARL_API_TOKEN || !BRIGHTPEARL_ACCOUNT_ID) return { ok: false, reason: 'no-creds' };
  const baseUrl = BRIGHTPEARL_DATACENTER === 'euw1'
    ? 'https://euw1.brightpearlconnect.com'
    : 'https://use1.brightpearlconnect.com';
  const url = `${baseUrl}/public-api/${BRIGHTPEARL_ACCOUNT_ID}/order-service/order/${orderId}/row`;
  const tax = rowNetExVat * 0.20;
  // taxCode MUST be inside rowValue — at top level BP ignores it and
  // returns ORDC-040 "you have not supplied a tax code".
  // Optional productName overrides the row's displayed name (still linked to
  // productId for stock) — used to suffix promo lines with "+ PRINTED LOGO".
  const body = {
    productId,
    ...(productName ? { productName } : {}),
    quantity: { magnitude: quantity },
    rowValue: {
      taxCode: process.env.BRIGHTPEARL_DEFAULT_TAX_CODE || 'T1',
      rowNet: { currency: 'GBP', value: rowNetExVat.toFixed(2) },
      rowTax: { currency: 'GBP', value: tax.toFixed(2) },
    },
  };
  try {
    const r = await fetch(url, {
      method: 'POST',
      headers: {
        'brightpearl-app-ref': process.env.BRIGHTPEARL_APP_REF,
        'brightpearl-account-token': BRIGHTPEARL_API_TOKEN,
        'Content-Type': 'application/json',
      },
      body: JSON.stringify(body),
    });
    if (!r.ok) {
      const errBody = await r.text();
      console.error(`[bp-add-row] order ${orderId} product ${productId} → ${r.status}: ${errBody.slice(0, 300)}`);
      return { ok: false, status: r.status, body: errBody };
    }
    return { ok: true };
  } catch (err) {
    console.error(`[bp-add-row] order ${orderId} product ${productId} error: ${err.message}`);
    return { ok: false, error: err.message };
  }
}

async function transitionBpStatusProofRequiredToSent(orderId, noteText = 'Proof sent') {
  if (!BRIGHTPEARL_API_TOKEN || !BRIGHTPEARL_ACCOUNT_ID) {
    console.warn('[bp-status] BP creds missing; cannot transition');
    return { ok: false, reason: 'no-creds' };
  }
  const baseUrl = BRIGHTPEARL_DATACENTER === 'euw1'
    ? 'https://euw1.brightpearlconnect.com'
    : 'https://use1.brightpearlconnect.com';
  const headers = {
    'brightpearl-app-ref': process.env.BRIGHTPEARL_APP_REF,
    'brightpearl-account-token': BRIGHTPEARL_API_TOKEN,
    'Content-Type': 'application/json',
  };

  // Probe current status. BP shapes the response slightly differently
  // depending on endpoint version — try both common locations.
  let currentStatusId = null;
  try {
    const getRes = await fetch(
      `${baseUrl}/public-api/${BRIGHTPEARL_ACCOUNT_ID}/order-service/order/${orderId}`,
      { headers }
    );
    if (!getRes.ok) {
      console.warn(`[bp-status] GET order ${orderId} returned ${getRes.status}`);
      return { ok: false, reason: 'get-failed', status: getRes.status };
    }
    const data = await getRes.json();
    const order = Array.isArray(data.response) ? data.response[0] : data.response;
    currentStatusId = order?.orderStatus?.orderStatusId ?? order?.orderStatusId ?? null;
  } catch (err) {
    console.error(`[bp-status] GET order ${orderId} error:`, err.message);
    return { ok: false, reason: 'get-error', error: err.message };
  }

  if (currentStatusId !== BP_STATUS_PROOF_REQUIRED) {
    // Status isn't "Proof Required" — don't transition, but still post
    // the note so there's an audit trail of the send in BP regardless
    // of where the order is in its lifecycle.
    console.log(`[bp-status] order ${orderId} currently status ${currentStatusId}, not ${BP_STATUS_PROOF_REQUIRED} — skipping transition, still posting note`);
    try {
      await postBpOrderNote(orderId, noteText);
    } catch (err) {
      console.error(`[bp-status] note post failed for ${orderId}:`, err.message);
    }
    return { ok: true, skipped: true, currentStatusId, notePosted: true };
  }

  try {
    const putRes = await fetch(
      `${baseUrl}/public-api/${BRIGHTPEARL_ACCOUNT_ID}/order-service/order/${orderId}/status`,
      {
        method: 'PUT',
        headers,
        body: JSON.stringify({
          orderStatusId: BP_STATUS_PROOF_SENT,
          orderNote: { text: noteText, isPublic: false },
        }),
      }
    );
    if (!putRes.ok) {
      const errBody = await putRes.text();
      console.error(`[bp-status] PUT ${orderId} status returned ${putRes.status}: ${errBody.slice(0, 300)}`);
      return { ok: false, reason: 'put-failed', status: putRes.status, body: errBody };
    }
    console.log(`[bp-status] order ${orderId} transitioned ${BP_STATUS_PROOF_REQUIRED} → ${BP_STATUS_PROOF_SENT}`);
    return { ok: true, transitioned: true };
  } catch (err) {
    console.error(`[bp-status] PUT ${orderId} status error:`, err.message);
    return { ok: false, reason: 'put-error', error: err.message };
  }
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
    `Proof Chased - Email sent to ${recipient} on ${new Date().toLocaleString('en-GB')}`
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

  if (!(await acquireBpPollLock('proof-chase', { waitMs: 5 * 60 * 1000 }))) return;
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

    // Don't fire chase emails on weekends — customers reading "your proof's
    // been waiting 3 days" on a Saturday looks bad, and they can't act on it
    // until Monday anyway. The order-search above still runs so first_seen
    // gets recorded for anything that lands in proof status over the weekend.
    const todayDow = new Date().getUTCDay();
    if (todayDow === 0 || todayDow === 6) {
      console.log('[proof-chase] weekend — skipping send phase, will resume Monday');
      return;
    }

    // Find orders ready to chase: been in proof status >N calendar days and
    // not yet chased. Calendar-days is a loose prefilter — we re-check using
    // business-days in JS below so weekends don't count toward the threshold.
    const ready = await pool.query(`
      SELECT order_id, first_seen_in_proof_status_at
      FROM proof_chase_log
      WHERE chase_sent_at IS NULL
        AND first_seen_in_proof_status_at < NOW() - ($1 || ' days')::interval
        AND last_checked_at >= NOW() - INTERVAL '15 minutes'
    `, [days]);

    const nowDate = new Date();
    const dueRows = ready.rows.filter((row) => {
      const firstSeen = new Date(row.first_seen_in_proof_status_at);
      return businessDaysBetween(firstSeen, nowDate) >= days;
    });

    console.log(`[proof-chase] ${ready.rows.length} candidate(s), ${dueRows.length} ready to chase after business-day filter (dryRun=${dryRun})`);

    for (const row of dueRows) {
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

// Send a real test email to an arbitrary address using the chase template,
// bypassing dry-run and the >N-days filter. Useful for previewing what
// customers will see. Defaults `to` to dec@tuffshop.co.uk.
//   ?to=<email>      destination address (default dec@tuffshop.co.uk)
//   ?orderId=<id>    optional — if given, uses real order data; otherwise
//                    sends a synthetic test order
const proofChaseTestSendHandler = async (req, res) => {
  const to = (req.query.to || 'dec@tuffshop.co.uk').toString();
  const orderId = req.query.orderId ? parseInt(req.query.orderId, 10) : null;

  if (!process.env.SMTP_PASS) {
    return res.status(500).json({ error: 'SMTP_PASS not configured on backend' });
  }

  let order;
  if (orderId && BRIGHTPEARL_API_TOKEN && BRIGHTPEARL_ACCOUNT_ID) {
    const baseUrl = BRIGHTPEARL_DATACENTER === 'euw1'
      ? 'https://euw1.brightpearlconnect.com'
      : 'https://use1.brightpearlconnect.com';
    try {
      const r = await fetch(
        `${baseUrl}/public-api/${BRIGHTPEARL_ACCOUNT_ID}/order-service/order/${orderId}`,
        {
          headers: {
            'brightpearl-app-ref': process.env.BRIGHTPEARL_APP_REF,
            'brightpearl-account-token': BRIGHTPEARL_API_TOKEN,
          },
        }
      );
      if (r.ok) order = (await r.json()).response?.[0];
    } catch (err) {
      console.warn(`[proof-chase-test] failed to fetch order ${orderId}: ${err.message}`);
    }
  }
  if (!order) {
    order = {
      id: 999999,
      reference: 'TEST-EMAIL-PREVIEW',
      parties: { customer: { contactName: 'Test Recipient', email: to } },
    };
  }

  const { subject, text, html } = buildProofChaseEmail(order);
  const sender = process.env.PROOF_CHASE_SENDER || 'noreply@tuffshop.co.uk';
  try {
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
      to,
      subject: `[TEST] ${subject}`,
      text,
      html,
    });
    res.json({ sent: true, to, subject: `[TEST] ${subject}`, basedOnOrderId: order.id });
  } catch (err) {
    console.error('[proof-chase-test] send failed:', err.message);
    res.status(500).json({ error: err.message });
  }
};
app.get('/api/proof-chase/send-test', proofChaseTestSendHandler);
app.post('/api/proof-chase/send-test', proofChaseTestSendHandler);

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

// ── Order tracking email pipeline ─────────────────────────────────
// Customer-facing transactional emails across the lifecycle of an online
// order (departmentId=17). Phase 1: schema + status mapper only — no
// poller, no sends. See Order-Tracking-Pipeline/SPEC.md.
//
// Two tables:
//   order_email_log         — per-order send history, the idempotency guard
//                             that stops the same state firing twice.
//   order_pipeline_templates — admin-editable subject/body per customer
//                             state. Seeded with placeholders on first boot.
async function initializeOrderPipelineTables() {
  if (!useDatabase) return;
  try {
    await pool.query(`
      CREATE TABLE IF NOT EXISTS order_email_log (
        order_id BIGINT PRIMARY KEY,
        last_customer_state TEXT,
        emails_sent JSONB NOT NULL DEFAULT '[]'::jsonb,
        last_checked_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
      );
      CREATE INDEX IF NOT EXISTS idx_order_email_log_checked
        ON order_email_log(last_checked_at);
      -- Per-order skip list — admin-managed via the "Skip these states"
      -- panel. The poller short-circuits on any state in this array so an
      -- order can be excluded from any subset of pipeline emails (typical
      -- use: skip collected + delivered for an order we know went badly).
      ALTER TABLE order_email_log
        ADD COLUMN IF NOT EXISTS excluded_states JSONB NOT NULL DEFAULT '[]'::jsonb;
      -- Mirror of emails_sent but for dry-run mode. While dry-run is on we
      -- deliberately don't touch emails_sent (so flipping live starts with
      -- a clean slate), but without dry_run_states the poller would re-log
      -- the same "would-send" decision every 5 minutes for as long as the
      -- order is in the updatedOn window. Resetting this column safely
      -- re-runs dry-run testing.
      ALTER TABLE order_email_log
        ADD COLUMN IF NOT EXISTS dry_run_states JSONB NOT NULL DEFAULT '[]'::jsonb;

      CREATE TABLE IF NOT EXISTS order_pipeline_templates (
        state TEXT PRIMARY KEY,
        subject TEXT NOT NULL,
        body_html TEXT NOT NULL,
        enabled BOOLEAN NOT NULL DEFAULT TRUE,
        updated_by TEXT,
        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
      );

      -- sender_email is per-template so states that invite replies (e.g.
      -- the back-order delay email, where a customer may want to ask for
      -- a substitute or cancel) come from a monitored inbox, while the
      -- informational states come from an unmonitored ordertracking@
      -- address. Editable in the admin panel — these are only defaults.
      ALTER TABLE order_pipeline_templates
        ADD COLUMN IF NOT EXISTS sender_email TEXT NOT NULL
        DEFAULT 'ordertracking@tuffshop.co.uk';

      -- Per-event activity log for the poller. Server stdout would clog
      -- under 5-10k orders/day; querying this table from the admin panel
      -- gives operators a paginated, filterable view of every decision
      -- the poller made (dry_run / sent / skipped / error).
      CREATE TABLE IF NOT EXISTS order_pipeline_send_log (
        id BIGSERIAL PRIMARY KEY,
        order_id BIGINT NOT NULL,
        state TEXT NOT NULL,
        status TEXT NOT NULL,
        recipient TEXT,
        sender TEXT,
        subject TEXT,
        error_message TEXT,
        bp_status_name TEXT,
        prev_state TEXT,
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
      );
      CREATE INDEX IF NOT EXISTS idx_op_send_log_created_at
        ON order_pipeline_send_log(created_at DESC);
      CREATE INDEX IF NOT EXISTS idx_op_send_log_order_id
        ON order_pipeline_send_log(order_id);
      CREATE INDEX IF NOT EXISTS idx_op_send_log_status
        ON order_pipeline_send_log(status);

      -- Singleton holding the timestamp of the last successful poll.
      -- The poller uses this for the adaptive window — on next run it
      -- scans NOW() - max(15min, gap-since-last-poll), capped at 7 days
      -- so the first ever poll doesn't try to backfill years of orders.
      CREATE TABLE IF NOT EXISTS order_pipeline_poll_state (
        id INT PRIMARY KEY DEFAULT 1,
        last_polled_at TIMESTAMPTZ,
        last_poll_count INT,
        last_poll_duration_ms INT,
        last_poll_error TEXT,
        CHECK (id = 1)
      );
      INSERT INTO order_pipeline_poll_state (id) VALUES (1) ON CONFLICT DO NOTHING;
    `);

    // First-deploy backfill for the back-order template — only flips the
    // sender if the row is still on the column default (i.e. nobody has
    // edited via the admin panel yet). Safe to run on every boot.
    await pool.query(`
      UPDATE order_pipeline_templates
      SET sender_email = 'sales@tuffshop.co.uk', updated_at = NOW()
      WHERE state = 'back_order' AND sender_email = 'ordertracking@tuffshop.co.uk'
    `);

    // Seed placeholder templates on first boot. Uses ON CONFLICT DO NOTHING
    // so existing rows are never overwritten — once the user edits a
    // template in the admin panel, redeploys leave their version alone.
    //
    // Variables locked for v1 (admin panel must document these for users):
    //   {{customerName}}        — full name
    //   {{customerFirstName}}   — first name only (for greetings)
    //   {{orderNumber}}         — BP order ref / PO number
    //   {{orderId}}             — numeric BP id (for support reference)
    //   {{orderDate}}           — placed-on date, formatted DD MMM YYYY
    //   {{collectionAddress}}   — for ready-for-collection emails
    //   {{trackingNumber}}      — for shipped emails
    //   {{carrierName}}         — for shipped emails (FedEx / Royal Mail / DPD)
    //   {{trackingUrl}}         — carrier tracking page link
    //   {{reviewUrl}}           — for collected / delivered review emails
    //   {{shopName}}            — constant: Tuff Workwear
    //   {{supportEmail}}        — constant: sales@tuffshop.co.uk
    // Reusable footer fragments so the "monitored vs unmonitored sender"
    // language stays consistent across templates without copy-pasting.
    const noReplyFooter =
      "<p style=\"color:#888;font-size:12px;margin-top:24px;\">" +
      "This email comes from an unmonitored address — please don't reply. " +
      "If you need help, email <a href=\"mailto:{{supportEmail}}\">{{supportEmail}}</a>." +
      "</p>";
    const replyOkFooter =
      "<p style=\"color:#888;font-size:12px;margin-top:24px;\">" +
      "You can reply to this email and we'll get back to you, or email " +
      "<a href=\"mailto:{{supportEmail}}\">{{supportEmail}}</a>." +
      "</p>";

    // Every template ends with {{signature}} — the renderer expands it to
    // the shared branded HTML signature (logo, socials, legal disclaimer).
    // Same block proof-chase emails use, so customers see consistent
    // branding across every Tuff Workwear email.
    const seeds = [
      // [state, sender_email, subject, body_html]
      ['stock_ordered', 'ordertracking@tuffshop.co.uk',
        "We've ordered the stock for your order {{orderNumber}}",
        "<p>Hi {{customerFirstName}},</p>" +
        "<p>Just a quick update — we've placed the order with our supplier for the items on your order <strong>{{orderNumber}}</strong>. " +
        "As soon as they arrive with us we'll start preparing your order and email you again.</p>" +
        "<p>Thanks,<br/>{{shopName}}</p>" + noReplyFooter + "{{signature}}"],
      ['in_production', 'ordertracking@tuffshop.co.uk',
        "Your order {{orderNumber}} is being prepared",
        "<p>Hi {{customerFirstName}},</p>" +
        "<p>Good news — we've started work on your order <strong>{{orderNumber}}</strong>. " +
        "We'll let you know as soon as it's ready.</p>" +
        "<p>Thanks,<br/>{{shopName}}</p>" + noReplyFooter + "{{signature}}"],
      ['back_order', 'sales@tuffshop.co.uk',
        "There's a delay with your order {{orderNumber}}",
        "<p>Hi {{customerFirstName}},</p>" +
        "<p>We wanted to let you know about a delay with your order <strong>{{orderNumber}}</strong> — our supplier has the stock on back order. " +
        "We'll keep you updated and let you know as soon as it lands with us.</p>" +
        "<p>If you'd rather not wait, just reply to this email and we'll sort it for you.</p>" +
        "<p>Thanks for your patience,<br/>{{shopName}}</p>" + replyOkFooter + "{{signature}}"],
      ['ready_for_collection', 'ordertracking@tuffshop.co.uk',
        "Your order {{orderNumber}} is ready to collect",
        "<p>Hi {{customerFirstName}},</p>" +
        "<p>Your order <strong>{{orderNumber}}</strong> is all ready for you to collect from:</p>" +
        "<p>{{collectionAddress}}</p>" +
        "<p>We're open Monday–Friday 9–5. See you soon!</p>" +
        "<p>Thanks,<br/>{{shopName}}</p>" + noReplyFooter + "{{signature}}"],
      ['shipped', 'ordertracking@tuffshop.co.uk',
        "Your order {{orderNumber}} is on its way",
        "<p>Hi {{customerFirstName}},</p>" +
        "<p>Your order <strong>{{orderNumber}}</strong> has been shipped with {{carrierName}}.</p>" +
        "<p>Tracking number: <strong>{{trackingNumber}}</strong></p>" +
        "{{trackingButton}}" +
        "<p>Thanks,<br/>{{shopName}}</p>" + noReplyFooter + "{{signature}}"],
      ['collected', 'ordertracking@tuffshop.co.uk',
        "Thanks for picking up your order, {{customerFirstName}}",
        "<p>Hi {{customerFirstName}},</p>" +
        "<p>Just wanted to say thanks for collecting your order <strong>{{orderNumber}}</strong>. " +
        "If you have a moment, we'd hugely appreciate a quick review:</p>" +
        "<p><a href=\"{{reviewUrl}}\">Leave a review</a></p>" +
        "<p>Thanks,<br/>{{shopName}}</p>" + noReplyFooter + "{{signature}}"],
      ['delivered', 'ordertracking@tuffshop.co.uk',
        "How was your order, {{customerFirstName}}?",
        "<p>Hi {{customerFirstName}},</p>" +
        "<p>We hope your order <strong>{{orderNumber}}</strong> arrived safely and you're happy with it. " +
        "If you have a moment, we'd really appreciate a quick review:</p>" +
        "<p><a href=\"{{reviewUrl}}\">Leave a review</a></p>" +
        "<p>Thanks,<br/>{{shopName}}</p>" + noReplyFooter + "{{signature}}"],
    ];

    for (const [state, sender, subject, body] of seeds) {
      await pool.query(
        `INSERT INTO order_pipeline_templates (state, subject, body_html, sender_email, enabled, updated_by)
         VALUES ($1, $2, $3, $4, TRUE, 'system-seed')
         ON CONFLICT (state) DO NOTHING`,
        [state, subject, body, sender]
      );
    }

    // Refresh never-edited templates so wording improvements (e.g. the
    // monitored-vs-unmonitored sender footers) propagate to existing
    // deploys. We only touch rows still tagged `updated_by = 'system-seed'`
    // — once anyone saves via the admin panel, updated_by changes and we
    // leave their version alone.
    for (const [state, sender, subject, body] of seeds) {
      await pool.query(
        `UPDATE order_pipeline_templates
         SET subject = $1, body_html = $2, sender_email = $3, updated_at = NOW()
         WHERE state = $4 AND updated_by = 'system-seed'`,
        [subject, body, sender, state]
      );
    }

    // The "Stock Ordered" email caused more confusion than it solved
    // ("oh, you don't have it yet?" panic from customers who'd expected
    // it to ship). Disabled by default; an admin can re-enable via the
    // panel if they want it back. Only touches rows still tagged
    // updated_by='system-seed' AND currently enabled — won't override an
    // admin who has explicitly switched it back on.
    await pool.query(`
      UPDATE order_pipeline_templates
      SET enabled = FALSE, updated_at = NOW()
      WHERE state = 'stock_ordered'
        AND updated_by = 'system-seed'
        AND enabled = TRUE
    `);

    console.log('✅ order_email_log + order_pipeline_templates initialized');
  } catch (err) {
    console.error('❌ Error initializing order pipeline tables:', err.message);
  }
}

// whatsapp_messages — every inbound/outbound WhatsApp message for the
// proof number, plus delivery status. Conversations are derived by
// grouping on peer_number (the customer's E.164-without-plus number).
// raw keeps the full Graph webhook payload for debugging / future media.
async function initializeWhatsAppTables() {
  if (!useDatabase) return;
  try {
    await pool.query(`
      CREATE TABLE IF NOT EXISTS whatsapp_messages (
        id BIGSERIAL PRIMARY KEY,
        wa_message_id TEXT,
        direction TEXT NOT NULL,            -- 'in' | 'out'
        peer_number TEXT NOT NULL,          -- the customer's number
        body TEXT,
        msg_type TEXT NOT NULL DEFAULT 'text',
        status TEXT,                        -- sent|delivered|read|failed (out)
        order_number TEXT,
        read_at TIMESTAMPTZ,                -- when staff read an inbound msg
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        raw JSONB
      );
      CREATE INDEX IF NOT EXISTS idx_wa_messages_peer
        ON whatsapp_messages(peer_number, created_at);
      CREATE INDEX IF NOT EXISTS idx_wa_messages_wamid
        ON whatsapp_messages(wa_message_id);
      -- Dismissing a conversation stamps every current row for that peer.
      -- A new inbound message (dismissed_at NULL) makes it resurface.
      ALTER TABLE whatsapp_messages
        ADD COLUMN IF NOT EXISTS dismissed_at TIMESTAMPTZ;
      -- Graph media id for image/document/video/audio/sticker messages, so the
      -- chat can fetch the media on demand via the proxy (no media stored).
      ALTER TABLE whatsapp_messages
        ADD COLUMN IF NOT EXISTS media_id TEXT;
      -- Which staff member sent an outbound message (operator identity from the
      -- app's ?user= param). Null for automated sends (auto-reply, cross-sell).
      ALTER TABLE whatsapp_messages
        ADD COLUMN IF NOT EXISTS sent_by TEXT;
    `);
    // Back-fill media_id for media rows received before the column existed —
    // the id was always kept in the raw webhook payload. Idempotent (only
    // touches rows still missing it). Media older than ~30d may have expired
    // on Meta's side, but the id costs nothing to store and recent ones load.
    await pool.query(`
      UPDATE whatsapp_messages
         SET media_id = COALESCE(
               raw->'message'->'image'->>'id',
               raw->'message'->'document'->>'id',
               raw->'message'->'video'->>'id',
               raw->'message'->'audio'->>'id',
               raw->'message'->'sticker'->>'id')
       WHERE media_id IS NULL
         AND msg_type IN ('image','document','video','audio','sticker')
         AND raw IS NOT NULL
    `);
    console.log('✅ whatsapp_messages initialized');
  } catch (err) {
    console.error('❌ Error initializing whatsapp_messages table:', err.message);
  }
}

// bp_order_attachments — supplementary files (scanned thread-colour sheet,
// embroidery digitised file) staged client-side at mockup creation and
// pushed to the Brightpearl order along with the signed PDF when the
// operator clicks "Attach to BP". Rows are deleted after a successful
// upload to BP — this table is temporary staging, not an archive.
async function initializeBpOrderAttachments() {
  if (!useDatabase) return;
  try {
    await pool.query(`
      CREATE TABLE IF NOT EXISTS bp_order_attachments (
        id SERIAL PRIMARY KEY,
        bp_order_id VARCHAR(50) NOT NULL,
        kind VARCHAR(50) NOT NULL,           -- 'scanned_sheet' | 'embroidery_file'
        filename VARCHAR(255) NOT NULL,
        mime_type VARCHAR(100),
        data BYTEA NOT NULL,
        uploaded_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
      );
      CREATE INDEX IF NOT EXISTS idx_bp_order_attachments_order
        ON bp_order_attachments(bp_order_id);
      -- Old single-row-per-kind constraint replaced with filename-aware
      -- dedup so orders can stage multiple emb files (different positions,
      -- per-employee personalised names, etc.). scanned_sheet is still
      -- enforced as single via the POST handler (DELETE-before-INSERT).
      DROP INDEX IF EXISTS uniq_bp_order_attachments_kind;
      CREATE UNIQUE INDEX IF NOT EXISTS uniq_bp_order_attachments_kind_file
        ON bp_order_attachments(bp_order_id, kind, filename);
    `);
    console.log('✅ bp_order_attachments initialized');
  } catch (err) {
    console.error('❌ Error initializing bp_order_attachments table:', err.message);
  }
}

// fitness_inc_colour_additions — operator-added catalogue colours for
// Fitness Inc products. Merged at runtime with the hardcoded
// FITNESS_INC_PRODUCTS array so admins can add colours without a code
// change. front_url/back_url are optional — null means fall back to the
// product's _default placeholder in fitnessIncProductImages.json.
async function initializeFitnessIncColourAdditions() {
  if (!useDatabase) return;
  try {
    await pool.query(`
      CREATE TABLE IF NOT EXISTS fitness_inc_colour_additions (
        id SERIAL PRIMARY KEY,
        product_id VARCHAR(50) NOT NULL,
        colour_name VARCHAR(100) NOT NULL,
        front_url TEXT,
        back_url TEXT,
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        created_by VARCHAR(100)
      );
      CREATE UNIQUE INDEX IF NOT EXISTS uniq_fitness_inc_colour_addition
        ON fitness_inc_colour_additions(product_id, colour_name);

      -- Whole operator-added products (added via the admin UI, no code change).
      -- Merged with the hardcoded catalogue at runtime. images is a JSON map
      -- { "<colour>": { "front": url, "back": url } }.
      CREATE TABLE IF NOT EXISTS fitness_inc_product_additions (
        id SERIAL PRIMARY KEY,
        product_id VARCHAR(60) NOT NULL UNIQUE,
        name TEXT NOT NULL,
        code VARCHAR(60) NOT NULL,
        colours JSONB NOT NULL DEFAULT '[]'::jsonb,
        positions JSONB NOT NULL DEFAULT '[]'::jsonb,
        one_size BOOLEAN NOT NULL DEFAULT FALSE,
        images JSONB NOT NULL DEFAULT '{}'::jsonb,
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        created_by VARCHAR(100)
      );
    `);
    console.log('✅ fitness_inc_colour_additions initialized');
  } catch (err) {
    console.error('❌ Error initializing fitness_inc_colour_additions:', err.message);
  }
}

// promo_offer_items — the configurable set of promotional items shown
// on the proof-approval page as a time-limited up-sell. Operator-edited
// via the admin UI (no code deploy needed to add/remove items or change
// prices). promo_offer_uptake records each customer submission so we
// can track conversion + audit what was actually requested.
async function initializePromoOfferTables() {
  if (!useDatabase) return;
  try {
    await pool.query(`
      CREATE TABLE IF NOT EXISTS promo_offer_items (
        id SERIAL PRIMARY KEY,
        name TEXT NOT NULL,
        image_url TEXT,
        regular_price NUMERIC(10,2) NOT NULL,
        deal_price NUMERIC(10,2) NOT NULL,
        deal_window_minutes INT NOT NULL DEFAULT 10,
        sort_order INT NOT NULL DEFAULT 0,
        enabled BOOLEAN NOT NULL DEFAULT TRUE,
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
      );
      -- Added later: min qty + logo zone definition (x/y/width/height as
      -- percentages of image + rotation in degrees).
      ALTER TABLE promo_offer_items ADD COLUMN IF NOT EXISTS min_qty INT NOT NULL DEFAULT 1;
      ALTER TABLE promo_offer_items ADD COLUMN IF NOT EXISTS logo_zone JSONB;
      -- Which logo variant this item should render: 'dark' for mugs/pens
      -- /coasters (need a dark logo to be visible on light items), 'light'
      -- for notepads, 'auto' (default) falls back to the primary logo.
      ALTER TABLE promo_offer_items ADD COLUMN IF NOT EXISTS logo_variant VARCHAR(10) NOT NULL DEFAULT 'auto';
      -- Brightpearl SKU — if set, the item is auto-added as an order row
      -- to the customer's BP order on successful up-sell submit.
      ALTER TABLE promo_offer_items ADD COLUMN IF NOT EXISTS bp_sku VARCHAR(100);
      CREATE TABLE IF NOT EXISTS promo_offer_uptake (
        id SERIAL PRIMARY KEY,
        approval_session_id UUID REFERENCES approval_sessions(id) ON DELETE SET NULL,
        order_number VARCHAR(50),
        customer_name VARCHAR(255),
        items JSONB NOT NULL,
        submitted_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        customer_ip TEXT
      );
      CREATE INDEX IF NOT EXISTS idx_promo_offer_uptake_session
        ON promo_offer_uptake(approval_session_id);
    `);
    console.log('✅ promo_offer tables initialized');
  } catch (err) {
    console.error('❌ Error initializing promo_offer tables:', err.message);
  }
}

// WhatsApp post-approval cross-sell. crosssell_rules maps what a customer
// ordered (by product code / prefix / brand) to a matching companion item;
// crosssell_sends logs each message so the inbound "Yes, add it" reply can be
// matched back to the offer and auto-added to the BP order.
async function initializeCrossSellTables() {
  if (!useDatabase) return;
  try {
    await pool.query(`
      CREATE TABLE IF NOT EXISTS crosssell_rules (
        id SERIAL PRIMARY KEY,
        source_match_type VARCHAR(20) NOT NULL DEFAULT 'code_prefix',
        source_match_value TEXT,
        ordered_label TEXT,
        companion_sku VARCHAR(100),
        companion_name TEXT NOT NULL,
        companion_image_url TEXT,
        match_colour BOOLEAN NOT NULL DEFAULT TRUE,
        logo_variant VARCHAR(10) NOT NULL DEFAULT 'auto',
        logo_zone JSONB,
        priority INT NOT NULL DEFAULT 0,
        enabled BOOLEAN NOT NULL DEFAULT TRUE,
        origin VARCHAR(10) NOT NULL DEFAULT 'manual',
        approved BOOLEAN NOT NULL DEFAULT TRUE,
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
      );
      -- Brand + garment-type keyed matching (the chosen model): a rule matches
      -- an ordered line by brand + garment type and offers a companion product.
      -- companion_product_code drives the image grab (brand-specific resolver);
      -- companion_sku drives the Brightpearl add (falls back to the code).
      ALTER TABLE crosssell_rules ALTER COLUMN source_match_value DROP NOT NULL;
      ALTER TABLE crosssell_rules ALTER COLUMN companion_sku DROP NOT NULL;
      ALTER TABLE crosssell_rules ADD COLUMN IF NOT EXISTS source_brand TEXT;
      ALTER TABLE crosssell_rules ADD COLUMN IF NOT EXISTS source_garment_type TEXT;
      ALTER TABLE crosssell_rules ADD COLUMN IF NOT EXISTS companion_product_code TEXT;
      -- Optional indicative price shown as a "from £X each" badge on the mockup.
      ALTER TABLE crosssell_rules ADD COLUMN IF NOT EXISTS price_each NUMERIC(10,2);
      CREATE INDEX IF NOT EXISTS idx_crosssell_rules_brand_garment
        ON crosssell_rules(source_brand, source_garment_type);

      CREATE TABLE IF NOT EXISTS crosssell_sends (
        id SERIAL PRIMARY KEY,
        approval_session_id UUID REFERENCES approval_sessions(id) ON DELETE SET NULL,
        order_number VARCHAR(50),
        customer_phone VARCHAR(32),
        rule_id INT REFERENCES crosssell_rules(id) ON DELETE SET NULL,
        companion_sku VARCHAR(100),
        companion_name TEXT,
        mockup_url TEXT,
        wa_message_id TEXT,
        sent_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        response VARCHAR(10),
        responded_at TIMESTAMPTZ,
        bp_added BOOLEAN NOT NULL DEFAULT FALSE,
        bp_order_row_id BIGINT
      );
      CREATE INDEX IF NOT EXISTS idx_crosssell_sends_phone
        ON crosssell_sends(customer_phone, sent_at);
      -- After a YES, the customer's size/colour/qty follow-up is captured here;
      -- the sales notification is held until then so it's ONE complete email.
      ALTER TABLE crosssell_sends ADD COLUMN IF NOT EXISTS details_text TEXT;
      ALTER TABLE crosssell_sends ADD COLUMN IF NOT EXISTS details_at TIMESTAMPTZ;
      -- When the sales notification was sent (combined email, or the safety-net
      -- "said yes, no details yet" email). Stops duplicate / repeated emails.
      ALTER TABLE crosssell_sends ADD COLUMN IF NOT EXISTS notified_at TIMESTAMPTZ;
      -- Mark all pre-existing answered rows as already-notified so the new
      -- safety-net sweep can't fire for stale rows from before this flow existed.
      UPDATE crosssell_sends SET notified_at = COALESCE(responded_at, sent_at, NOW())
        WHERE notified_at IS NULL AND response IS NOT NULL
          AND responded_at < NOW() - INTERVAL '6 hours';
    `);
    console.log('✅ crosssell tables initialized');
  } catch (err) {
    console.error('❌ Error initializing crosssell tables:', err.message);
  }
}

// Promo-item production jig templates: page size (mm) + logo placement(s) per
// item, used to generate print-ready EPS. Validated specs below are the
// defaults; the admin can fine-tune. Seeded with DO NOTHING so re-deploys don't
// clobber edits; the reset-defaults endpoint re-applies these exactly.
const JIG_DEFAULTS = [
  // Notepad: A5, logo fits a 100×100mm box centred.
  { item_key: 'notepad', label: 'Notepad (A5)', page_w_mm: 148, page_h_mm: 210, vector_required: false,
    placements: [{ xmm: 24, ymm: 55, wmm: 100, hmm: 100, rotation: 0 }], grid: null },
  // Coaster: logo fits a 60×60mm box centred inside the 90mm circle.
  { item_key: 'coaster', label: 'Coaster (90mm circle)', page_w_mm: 90, page_h_mm: 90, vector_required: false,
    placements: [{ xmm: 15, ymm: 15, wmm: 60, hmm: 60, rotation: 0 }], grid: null },
  // Mug: 80×200 sheet, two 65×65 boxes rotated 90°, centred across the 80mm
  // width, 3mm from the top edge and 3mm from the bottom edge.
  { item_key: 'mug', label: 'Mug (80×200 sublimation)', page_w_mm: 80, page_h_mm: 200, vector_required: false,
    placements: [{ xmm: 7.5, ymm: 132, wmm: 65, hmm: 65, rotation: 90 }, { xmm: 7.5, ymm: 3, wmm: 65, hmm: 65, rotation: 90 }], grid: null },
  // Pen jig: 710×510 bed, 72-up. Two interleaved facings per column computed
  // from a single bottom-right anchor (validated against the real jig). VECTOR.
  { item_key: 'pen', label: 'Pen jig (72-up)', page_w_mm: 710, page_h_mm: 510, vector_required: true,
    placements: null, grid: {
      kind: 'pen', cols: 4, perColumn: 9, colGapMm: 188.918, vGapMm: 58.0207,
      boxWmm: 69.019, boxHmm: 12.085, anchorXmm: 584.85, anchorYmm: 0.733,
      leftDxMm: 37.2, leftDyMm: 28.911, rightRotation: 0, leftRotation: 180,
    } },
  // Banner: 1000×500mm, logo as large as possible centred inside a 50mm border
  // (box 900×400 → the logo fits keeping aspect). Border adjustable in the Jig
  // Templates admin. Takes a raster image OR a vector .eps.
  { item_key: 'banner', label: 'Banner (1000×500mm)', page_w_mm: 1000, page_h_mm: 500, vector_required: false,
    placements: [{ xmm: 50, ymm: 50, wmm: 900, hmm: 400, rotation: 0 }], grid: null },
];

async function upsertJigDefaults(overwrite) {
  const conflict = overwrite
    ? `ON CONFLICT (item_key) DO UPDATE SET label=EXCLUDED.label, page_w_mm=EXCLUDED.page_w_mm,
         page_h_mm=EXCLUDED.page_h_mm, placements=EXCLUDED.placements, grid=EXCLUDED.grid,
         vector_required=EXCLUDED.vector_required, updated_at=NOW()`
    : `ON CONFLICT (item_key) DO NOTHING`;
  for (const d of JIG_DEFAULTS) {
    await pool.query(
      `INSERT INTO jig_templates (item_key, label, page_w_mm, page_h_mm, placements, grid, vector_required)
       VALUES ($1,$2,$3,$4,$5,$6,$7) ${conflict}`,
      [d.item_key, d.label, d.page_w_mm, d.page_h_mm,
       d.placements ? JSON.stringify(d.placements) : null,
       d.grid ? JSON.stringify(d.grid) : null, d.vector_required]
    );
  }
}

async function initializeJigTemplates() {
  if (!useDatabase) return;
  try {
    await pool.query(`
      CREATE TABLE IF NOT EXISTS jig_templates (
        id SERIAL PRIMARY KEY,
        item_key TEXT UNIQUE NOT NULL,
        label TEXT NOT NULL,
        page_w_mm NUMERIC(8,2) NOT NULL,
        page_h_mm NUMERIC(8,2) NOT NULL,
        placements JSONB,
        grid JSONB,
        vector_required BOOLEAN NOT NULL DEFAULT FALSE,
        enabled BOOLEAN NOT NULL DEFAULT TRUE,
        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
      );
    `);
    await upsertJigDefaults(false);
    console.log('✅ jig_templates initialized');
  } catch (err) {
    console.error('❌ Error initializing jig_templates:', err.message);
  }
}

// Kill-switch: set ORDER_PIPELINE_ENABLED=false on Render to fully stop the
// order-pipeline poller (no boot run, no interval, manual trigger 503s).
// Even in dry-run mode the poller hits Brightpearl, so this is the way to
// stop burning BP quota while the pipeline is parked.
function isOrderPipelineEnabled() {
  return String(process.env.ORDER_PIPELINE_ENABLED ?? 'true').toLowerCase() !== 'false';
}

(async () => {
  await initializeOrderPipelineTables();
  await initializeWhatsAppTables();
  await initializeBpOrderAttachments();
  await initializeFitnessIncColourAdditions();
  await initializePromoOfferTables();
  await initializeCrossSellTables();
  await initializeJigTemplates();
  await initializePrintQueueTable();
  if (!isOrderPipelineEnabled()) {
    console.log('⏸️  Order-pipeline poller DISABLED via ORDER_PIPELINE_ENABLED=false. No polls will run.');
    return;
  }
  if (useDatabase && BRIGHTPEARL_API_TOKEN) {
    // Stagger initial run 4 min after boot so the urgent/stale/proof-chase
    // pollers settle first and we don't elbow them at the BP rate limit.
    setTimeout(() => {
      pollOrderTrackingPipeline({ label: 'boot' })
        .catch((err) => console.error('Initial order-pipeline run failed:', err.message));
    }, 4 * 60 * 1000);
    setInterval(() => {
      pollOrderTrackingPipeline()
        .catch((err) => console.error('Order-pipeline poll failed:', err.message));
    }, ORDER_PIPELINE_POLL_INTERVAL_MS);
    console.log(`✅ Order-pipeline poller scheduled (every ${ORDER_PIPELINE_POLL_INTERVAL_MS / 60000} min, first run +4min). Mode: ${isDryRun() ? 'DRY-RUN' : 'LIVE'}.`);
  }
})();

// ── Order pipeline: admin / inspect endpoints ─────────────────────
// Read-only for the admin panel + a single test-send. No poller endpoints
// here yet — that's Phase 3. None of these touch real customer emails.

// Helper: fetch a BP order detail + its notes in parallel. Returns
// { order, notes } in the shapes orderPipelineVariables.js expects.
async function fetchBpOrderForPipeline(orderId) {
  if (!BRIGHTPEARL_API_TOKEN || !BRIGHTPEARL_ACCOUNT_ID) {
    throw new Error('Brightpearl credentials not configured on this backend');
  }
  const baseUrl = BRIGHTPEARL_DATACENTER === 'euw1'
    ? 'https://euw1.brightpearlconnect.com'
    : 'https://use1.brightpearlconnect.com';
  const headers = {
    'brightpearl-app-ref': process.env.BRIGHTPEARL_APP_REF,
    'brightpearl-account-token': BRIGHTPEARL_API_TOKEN,
  };

  const [orderRes, notesRes] = await Promise.all([
    fetch(`${baseUrl}/public-api/${BRIGHTPEARL_ACCOUNT_ID}/order-service/order/${orderId}`, { headers }),
    fetch(`${baseUrl}/public-api/${BRIGHTPEARL_ACCOUNT_ID}/order-service/order/${orderId}/note`, { headers }),
  ]);

  if (!orderRes.ok) {
    const t = await orderRes.text().catch(() => '');
    throw new Error(`Failed to fetch BP order ${orderId}: ${orderRes.status} ${t.slice(0, 200)}`);
  }
  const orderJson = await orderRes.json();
  const order = (orderJson.response || [])[0];
  if (!order) throw new Error(`BP order ${orderId} not found`);

  let notes = [];
  if (notesRes.ok) {
    const notesJson = await notesRes.json().catch(() => ({ response: [] }));
    notes = notesJson.response || [];
  }
  return { order, notes };
}

// GET /api/order-pipeline/states — for the admin panel sidebar + variable
// reference card. Static data, doesn't touch the DB.
app.get('/api/order-pipeline/states', (req, res) => {
  res.json({
    states: listStates(),
    variables: VARIABLE_SCHEMA,
  });
});

// GET /api/order-pipeline/templates — return all 7 stored templates.
app.get('/api/order-pipeline/templates', async (req, res) => {
  if (!useDatabase) return res.status(503).json({ error: 'DATABASE_URL not configured' });
  try {
    const r = await pool.query(
      `SELECT state, subject, body_html, sender_email, enabled, updated_by, updated_at
       FROM order_pipeline_templates
       ORDER BY state`
    );
    res.json({ templates: r.rows });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// PUT /api/order-pipeline/templates/:state — update subject / body /
// sender_email / enabled. Any field omitted from the body preserves its
// existing value so the UI can save partial edits.
app.put('/api/order-pipeline/templates/:state', async (req, res) => {
  if (!useDatabase) return res.status(503).json({ error: 'DATABASE_URL not configured' });
  const { state } = req.params;
  const { subject, body_html, sender_email, enabled, updatedBy } = req.body || {};
  if (typeof subject !== 'string' || typeof body_html !== 'string') {
    return res.status(400).json({ error: 'subject and body_html are required strings' });
  }
  if (sender_email !== undefined && sender_email !== null) {
    if (typeof sender_email !== 'string' || !/^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(sender_email)) {
      return res.status(400).json({ error: 'sender_email must be a valid email address' });
    }
  }
  const known = listStates().some((s) => s.id === state);
  if (!known) return res.status(400).json({ error: `unknown state '${state}'` });
  try {
    const r = await pool.query(
      `UPDATE order_pipeline_templates
       SET subject = $1,
           body_html = $2,
           sender_email = COALESCE($3, sender_email),
           enabled = COALESCE($4, enabled),
           updated_by = $5,
           updated_at = NOW()
       WHERE state = $6
       RETURNING state, subject, body_html, sender_email, enabled, updated_by, updated_at`,
      [
        subject,
        body_html,
        typeof sender_email === 'string' ? sender_email : null,
        typeof enabled === 'boolean' ? enabled : null,
        updatedBy || null,
        state,
      ]
    );
    if (r.rowCount === 0) return res.status(404).json({ error: `template '${state}' not seeded` });
    res.json({ template: r.rows[0] });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// GET /api/order-pipeline/preview?orderId=X&state=Y — renders the stored
// template against a real BP order. Returns rendered subject/body + the
// resolved variables (for the side panel) + diagnostics (missing/unknown
// placeholders so the admin can spot typos before saving).
app.get('/api/order-pipeline/preview', async (req, res) => {
  if (!useDatabase) return res.status(503).json({ error: 'DATABASE_URL not configured' });
  const { orderId, state } = req.query;
  if (!orderId || !state) {
    return res.status(400).json({ error: 'orderId and state query params required' });
  }
  try {
    const tplRes = await pool.query(
      'SELECT state, subject, body_html, sender_email, enabled FROM order_pipeline_templates WHERE state = $1',
      [state]
    );
    if (tplRes.rowCount === 0) {
      return res.status(404).json({ error: `template for state '${state}' not found` });
    }
    const template = tplRes.rows[0];
    const { order, notes } = await fetchBpOrderForPipeline(orderId);
    const variables = deriveVariables(order, { notes });
    const rendered = renderTemplate(template, variables);
    res.json({
      state: template.state,
      enabled: template.enabled,
      sender_email: template.sender_email,
      subject: rendered.subject,
      body_html: rendered.body_html,
      variables,
      diagnostics: rendered.diagnostics,
    });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// POST /api/order-pipeline/test-send — same as preview, but actually emails
// the result to the requester (NOT the customer). Subject is prefixed with
// [TEST] and a yellow info banner is injected at the top of the body so
// recipients are never confused about whether it's the real thing.
app.post('/api/order-pipeline/test-send', async (req, res) => {
  if (!useDatabase) return res.status(503).json({ error: 'DATABASE_URL not configured' });
  const { state, orderId, recipientEmail, requestedBy } = req.body || {};
  if (!state || !orderId || !recipientEmail) {
    return res.status(400).json({ error: 'state, orderId and recipientEmail required' });
  }
  if (!/^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(recipientEmail)) {
    return res.status(400).json({ error: 'recipientEmail looks invalid' });
  }
  try {
    const tplRes = await pool.query(
      'SELECT state, subject, body_html, sender_email FROM order_pipeline_templates WHERE state = $1',
      [state]
    );
    if (tplRes.rowCount === 0) {
      return res.status(404).json({ error: `template for state '${state}' not found` });
    }
    const template = tplRes.rows[0];
    const { order, notes } = await fetchBpOrderForPipeline(orderId);
    const variables = deriveVariables(order, { notes });
    const rendered = renderTemplate(template, variables);

    if (!process.env.SMTP_PASS) {
      return res.status(503).json({ error: 'SMTP_PASS not configured on backend' });
    }
    const sender = template.sender_email || 'ordertracking@tuffshop.co.uk';
    const transporter = nodemailer.createTransport({
      host: process.env.SMTP_SERVER || 'mail-eu.smtp2go.com',
      port: parseInt(process.env.SMTP_PORT || '2525', 10),
      secure: false,
      auth: {
        user: process.env.SMTP_USERNAME || 'tuffshop.co.uk',
        pass: process.env.SMTP_PASS,
      },
    });
    const testBanner = `<div style="background:#fff8d6;border:1px solid #f0c419;padding:10px 14px;margin:0 0 16px;font-family:Arial,sans-serif;font-size:12px;color:#7a5b00;">
<strong>[TEST EMAIL]</strong> Rendered for state <code>${state}</code> against BP order <strong>#${orderId}</strong>.
Sender: <code>${sender}</code>. Triggered by ${requestedBy || 'unknown'}. The customer would have received the content below.
</div>`;
    await transporter.sendMail({
      from: `${variables.shopName} <${sender}>`,
      to: recipientEmail,
      subject: `[TEST] ${rendered.subject}`,
      html: testBanner + rendered.body_html,
    });
    res.json({
      sent: true,
      sender,
      recipient: recipientEmail,
      subject: rendered.subject,
      diagnostics: rendered.diagnostics,
    });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// GET /api/order-pipeline/inspect/:orderId — what (if anything) we've
// emailed for this order. Drives the "is this customer up to date?"
// section of the admin panel.
app.get('/api/order-pipeline/inspect/:orderId', async (req, res) => {
  if (!useDatabase) return res.status(503).json({ error: 'DATABASE_URL not configured' });
  try {
    const r = await pool.query(
      `SELECT order_id, last_customer_state, emails_sent, excluded_states,
              dry_run_states, last_checked_at
       FROM order_email_log
       WHERE order_id = $1`,
      [req.params.orderId]
    );
    if (r.rowCount === 0) {
      return res.json({ orderId: req.params.orderId, found: false, log: null });
    }
    res.json({ orderId: req.params.orderId, found: true, log: r.rows[0] });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// POST /api/order-pipeline/reset-dry-run
// Body: { orderId? }  (omit for all orders)
// Clears dry_run_states so the dry-run loop will re-log decisions next
// poll. Useful after editing a template — lets you see fresh "would
// send" entries with the new copy. Doesn't touch emails_sent or
// excluded_states, so live-mode behaviour is unaffected.
app.post('/api/order-pipeline/reset-dry-run', async (req, res) => {
  if (!useDatabase) return res.status(503).json({ error: 'DATABASE_URL not configured' });
  const { orderId } = req.body || {};
  try {
    const q = orderId
      ? await pool.query(
          `UPDATE order_email_log SET dry_run_states = '[]'::jsonb WHERE order_id = $1`,
          [Number(orderId)]
        )
      : await pool.query(
          `UPDATE order_email_log SET dry_run_states = '[]'::jsonb WHERE dry_run_states <> '[]'::jsonb`
        );
    res.json({ ok: true, rowsAffected: q.rowCount, scope: orderId ? `order ${orderId}` : 'all orders' });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// POST /api/order-pipeline/exclude-state
// Body: { orderId, state, action: 'add' | 'remove', by? }
// Manages the per-order skip list. Idempotent — adding a state that's
// already excluded does nothing; removing a state that isn't excluded
// does nothing.
app.post('/api/order-pipeline/exclude-state', async (req, res) => {
  if (!useDatabase) return res.status(503).json({ error: 'DATABASE_URL not configured' });
  const { orderId, state, action, by } = req.body || {};
  if (!orderId || !state || !['add', 'remove'].includes(action)) {
    return res.status(400).json({ error: 'orderId, state and action ("add"|"remove") required' });
  }
  if (!listStates().some((s) => s.id === state)) {
    return res.status(400).json({ error: `unknown state '${state}'` });
  }
  try {
    if (action === 'add') {
      // Ensure the row exists first, then add the state to the JSONB array
      // if it isn't already in there. The CASE NOT-IN guard makes the
      // operation idempotent.
      await pool.query(
        `INSERT INTO order_email_log (order_id, excluded_states, last_checked_at)
         VALUES ($1, $2::jsonb, NOW())
         ON CONFLICT (order_id) DO UPDATE SET
           excluded_states = CASE
             WHEN order_email_log.excluded_states @> $2::jsonb
               THEN order_email_log.excluded_states
             ELSE order_email_log.excluded_states || $2::jsonb
           END,
           last_checked_at = NOW()`,
        [Number(orderId), JSON.stringify([state])]
      );
    } else {
      // Remove via jsonb_array_elements + filter
      await pool.query(
        `UPDATE order_email_log
         SET excluded_states = COALESCE(
           (SELECT jsonb_agg(elem) FROM jsonb_array_elements(excluded_states) elem WHERE elem <> $2::jsonb),
           '[]'::jsonb
         ),
         last_checked_at = NOW()
         WHERE order_id = $1`,
        [Number(orderId), JSON.stringify(state)]
      );
    }
    await writeActivityLog({
      orderId: Number(orderId), state, status: 'skipped',
      errorMessage: `manual ${action === 'add' ? 'EXCLUDE' : 'INCLUDE'} from skip list by ${by || 'admin'}`,
    });
    const r = await pool.query(
      `SELECT excluded_states FROM order_email_log WHERE order_id = $1`,
      [Number(orderId)]
    );
    res.json({ ok: true, excluded_states: r.rows[0]?.excluded_states || [] });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// ── Order tracking pipeline POLLER ────────────────────────────────
// Scans BP for online-channel orders that have moved status in the last
// 15 min (or the gap since our last successful poll, whichever is wider)
// and decides whether each one is due a customer email. In dry-run mode
// (default) every "would-send" is written to order_pipeline_send_log
// with status='dry_run' and no SMTP traffic is generated. Activity log
// is the source of truth for what the system would do — server stdout
// only carries a one-line summary per poll cycle.

const ORDER_PIPELINE_POLL_INTERVAL_MS = 5 * 60 * 1000;          // 5 min
const ORDER_PIPELINE_WINDOW_MIN_MS = 15 * 60 * 1000;            // 15 min
const ORDER_PIPELINE_WINDOW_MAX_MS = 7 * 24 * 60 * 60 * 1000;   // 7 days cap
const ORDER_PIPELINE_BP_SPACING_MS = 250;                       // per-order pause
const ORDER_PIPELINE_DEPARTMENT_ID = parseInt(process.env.ORDER_PIPELINE_DEPARTMENT_ID || '17', 10);
// Default-on dry-run — must set ORDER_PIPELINE_DRY_RUN=false to flip live.
function isDryRun() {
  const v = String(process.env.ORDER_PIPELINE_DRY_RUN ?? 'true').toLowerCase();
  return v !== 'false' && v !== '0' && v !== 'no';
}

let orderPipelinePollInFlight = false;

async function writeActivityLog(entry) {
  if (!useDatabase) return;
  try {
    await pool.query(
      `INSERT INTO order_pipeline_send_log
       (order_id, state, status, recipient, sender, subject, error_message, bp_status_name, prev_state, created_at)
       VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, NOW())`,
      [
        Number(entry.orderId) || 0,
        entry.state || '',
        entry.status,
        entry.recipient || null,
        entry.sender || null,
        entry.subject || null,
        entry.errorMessage || null,
        entry.bpStatusName || null,
        entry.prevState || null,
      ]
    );
  } catch (err) {
    console.error('[order-pipeline] failed to write activity log:', err.message);
  }
}

async function fetchOrderNotesSafe(orderId) {
  try {
    const baseUrl = BRIGHTPEARL_DATACENTER === 'euw1'
      ? 'https://euw1.brightpearlconnect.com'
      : 'https://use1.brightpearlconnect.com';
    const r = await fetch(`${baseUrl}/public-api/${BRIGHTPEARL_ACCOUNT_ID}/order-service/order/${orderId}/note`, {
      headers: {
        'brightpearl-app-ref': process.env.BRIGHTPEARL_APP_REF,
        'brightpearl-account-token': BRIGHTPEARL_API_TOKEN,
      },
    });
    if (!r.ok) return [];
    const data = await r.json().catch(() => ({ response: [] }));
    return data.response || [];
  } catch {
    return [];
  }
}

// Decide + log what should happen for a single BP order detail. Mutates
// order_email_log (last_customer_state + last_checked_at). Only writes
// emails_sent in real-send mode — leaving it empty during dry-run keeps
// the de-dup guard a clean slate for when the switch flips live.
async function processOrderForPipeline(order) {
  const orderId = order?.id;
  if (!orderId) return { decision: 'no-id' };
  const bpStatusName = order.orderStatus?.name || '';

  const logRes = await pool.query(
    `SELECT last_customer_state, emails_sent, excluded_states, dry_run_states
     FROM order_email_log WHERE order_id = $1`,
    [orderId]
  );
  const prevLog = logRes.rows[0];
  const prevState = prevLog?.last_customer_state || null;
  const emailsSent = Array.isArray(prevLog?.emails_sent) ? prevLog.emails_sent : [];
  const excludedStates = Array.isArray(prevLog?.excluded_states) ? prevLog.excluded_states : [];
  const dryRunStates = Array.isArray(prevLog?.dry_run_states) ? prevLog.dry_run_states : [];

  const currentState = customerStateForBpStatus(bpStatusName, prevState);

  // Touch last_checked_at regardless of outcome so the inspect view shows
  // we've seen this order recently.
  const touchSql = (state) => state
    ? `INSERT INTO order_email_log (order_id, last_customer_state, last_checked_at)
       VALUES ($1, $2, NOW())
       ON CONFLICT (order_id) DO UPDATE SET
         last_customer_state = EXCLUDED.last_customer_state,
         last_checked_at = NOW()`
    : `INSERT INTO order_email_log (order_id, last_checked_at)
       VALUES ($1, NOW())
       ON CONFLICT (order_id) DO UPDATE SET last_checked_at = NOW()`;

  if (!currentState) {
    // Status doesn't trigger any email (internal / proof flow / pick-stage).
    await pool.query(touchSql(null), [orderId]);
    return { decision: 'no-state', bpStatusName };
  }

  if (emailsSent.some((e) => e && e.state === currentState)) {
    // Already emailed this state for this order — guard against re-fires.
    await pool.query(touchSql(currentState), [orderId, currentState]);
    return { decision: 'already-emailed', state: currentState };
  }

  // Look up the template for this state
  const tplRes = await pool.query(
    `SELECT subject, body_html, sender_email, enabled FROM order_pipeline_templates WHERE state = $1`,
    [currentState]
  );
  const template = tplRes.rows[0];
  if (!template) {
    await pool.query(touchSql(currentState), [orderId, currentState]);
    await writeActivityLog({
      orderId, state: currentState, status: 'error',
      errorMessage: `no template seeded for state '${currentState}'`,
      bpStatusName, prevState,
    });
    return { decision: 'no-template' };
  }
  if (!template.enabled) {
    await pool.query(touchSql(currentState), [orderId, currentState]);
    await writeActivityLog({
      orderId, state: currentState, status: 'skipped',
      errorMessage: 'template disabled in admin panel',
      bpStatusName, prevState,
      sender: template.sender_email,
    });
    return { decision: 'template-disabled' };
  }

  // Per-order skip list — populated via the admin panel for orders we know
  // shouldn't receive a particular email (e.g. a botched experience that
  // we don't want to ask for a review on).
  if (excludedStates.includes(currentState)) {
    await pool.query(touchSql(currentState), [orderId, currentState]);
    await writeActivityLog({
      orderId, state: currentState, status: 'skipped',
      errorMessage: 'state in per-order skip list (excluded_states)',
      bpStatusName, prevState,
      sender: template.sender_email,
    });
    return { decision: 'excluded' };
  }

  // Review-request gating — only for the collected / delivered states.
  // Plain orders need to be out the door same or next working day; logo
  // orders get 14 calendar days. Anything slower than that, skip the
  // review email — a bad review from a frustrated customer costs more
  // than the missed opportunity.
  if (currentState === 'collected' || currentState === 'delivered') {
    const eligibility = checkReviewEligibility(order, order.updatedOn || new Date().toISOString());
    if (!eligibility.eligible) {
      await pool.query(touchSql(currentState), [orderId, currentState]);
      await writeActivityLog({
        orderId, state: currentState, status: 'skipped',
        errorMessage: `review skipped — ${eligibility.reason}`,
        bpStatusName, prevState,
        sender: template.sender_email,
      });
      return { decision: 'review-ineligible', reason: eligibility.reason };
    }
  }

  // Build the variables (notes only needed for shipped emails but cheap to
  // always fetch and it keeps the dry-run preview accurate).
  const notes = await fetchOrderNotesSafe(orderId);
  const variables = deriveVariables(order, { notes });
  const rendered = renderTemplate(
    { subject: template.subject, body_html: template.body_html },
    variables
  );
  const recipient = order.parties?.customer?.email || '';
  const sender = template.sender_email;

  if (!recipient) {
    await pool.query(touchSql(currentState), [orderId, currentState]);
    await writeActivityLog({
      orderId, state: currentState, status: 'skipped',
      errorMessage: 'no customer email on order',
      bpStatusName, prevState,
      sender, subject: rendered.subject,
    });
    return { decision: 'no-email' };
  }

  if (isDryRun()) {
    // If we've already logged a dry-run decision for this (order, state)
    // pair, don't re-log it on subsequent polls. Updates last_checked_at
    // so the inspect view still shows we've seen it.
    if (dryRunStates.includes(currentState)) {
      await pool.query(touchSql(currentState), [orderId, currentState]);
      return { decision: 'dry_run_already_logged', state: currentState };
    }
    // First time we've decided this combo. Update last_customer_state so
    // the Invoiced/Completed disambiguation keeps working, and append the
    // state to dry_run_states so we don't re-log next poll. emails_sent
    // stays untouched — flipping out of dry-run starts with a clean slate.
    await pool.query(
      `INSERT INTO order_email_log (order_id, last_customer_state, dry_run_states, last_checked_at)
       VALUES ($1, $2, $3::jsonb, NOW())
       ON CONFLICT (order_id) DO UPDATE SET
         last_customer_state = EXCLUDED.last_customer_state,
         dry_run_states = order_email_log.dry_run_states || EXCLUDED.dry_run_states,
         last_checked_at = NOW()`,
      [orderId, currentState, JSON.stringify([currentState])]
    );
    await writeActivityLog({
      orderId, state: currentState, status: 'dry_run',
      recipient, sender, subject: rendered.subject,
      bpStatusName, prevState,
    });
    return { decision: 'dry_run', state: currentState, recipient };
  }

  // Real send path — currently locked off behind ORDER_PIPELINE_DRY_RUN.
  // Will be enabled in Phase 4 after the dry-run logs have been audited.
  await writeActivityLog({
    orderId, state: currentState, status: 'error',
    errorMessage: 'real-send path not yet implemented (Phase 4)',
    recipient, sender, subject: rendered.subject,
    bpStatusName, prevState,
  });
  return { decision: 'real-send-not-implemented' };
}

async function pollOrderTrackingPipeline({ sinceMs, label = 'incremental' } = {}) {
  if (!useDatabase || !BRIGHTPEARL_API_TOKEN || !BRIGHTPEARL_ACCOUNT_ID) return;
  if (orderPipelinePollInFlight) {
    console.log(`[order-pipeline/${label}] skipped — another scan is in progress`);
    return { skipped: 'in-flight' };
  }
  if (!(await acquireBpPollLock(`order-pipeline/${label}`, { waitMs: 60 * 1000 }))) {
    return { skipped: 'bp-lock' };
  }
  orderPipelinePollInFlight = true;
  const start = Date.now();
  try {
    return await pollOrderTrackingPipelineInner({ sinceMs, label });
  } catch (err) {
    console.error(`[order-pipeline/${label}] uncaught:`, err.message);
    try {
      await pool.query(
        `UPDATE order_pipeline_poll_state SET last_poll_error = $1, last_poll_duration_ms = $2 WHERE id = 1`,
        [err.message?.slice(0, 500) || 'unknown', Date.now() - start]
      );
    } catch {}
    return { error: err.message };
  } finally {
    orderPipelinePollInFlight = false;
    releaseBpPollLock();
  }
}

async function pollOrderTrackingPipelineInner({ sinceMs, label }) {
  const baseUrl = BRIGHTPEARL_DATACENTER === 'euw1'
    ? 'https://euw1.brightpearlconnect.com'
    : 'https://use1.brightpearlconnect.com';
  const headers = {
    'brightpearl-app-ref': process.env.BRIGHTPEARL_APP_REF,
    'brightpearl-account-token': BRIGHTPEARL_API_TOKEN,
  };

  // Adaptive window — if we polled successfully recently, scan the gap
  // since then (plus a 1-minute overlap for safety). Otherwise fall
  // back to the 15-min default. Capped at 7 days so a long outage
  // doesn't trigger a year-long historical scan.
  let fromMs = sinceMs;
  if (fromMs === undefined) {
    try {
      const r = await pool.query(`SELECT last_polled_at FROM order_pipeline_poll_state WHERE id = 1`);
      const last = r.rows[0]?.last_polled_at;
      if (last) {
        const gap = Date.now() - new Date(last).getTime() + 60 * 1000;
        fromMs = Date.now() - Math.min(ORDER_PIPELINE_WINDOW_MAX_MS, Math.max(ORDER_PIPELINE_WINDOW_MIN_MS, gap));
      } else {
        fromMs = Date.now() - ORDER_PIPELINE_WINDOW_MIN_MS;
      }
    } catch {
      fromMs = Date.now() - ORDER_PIPELINE_WINDOW_MIN_MS;
    }
  }
  const fromIso = new Date(fromMs).toISOString();
  const updatedFilter = encodeURIComponent(`${fromIso}/`);
  const start = Date.now();

  // Paginate order-search until BP returns an empty page. Filter by
  // departmentId so we only get online-channel orders.
  const orderIds = [];
  let firstResult = 1;
  for (let page = 0; page < 100; page++) {
    const url = `${baseUrl}/public-api/${BRIGHTPEARL_ACCOUNT_ID}/order-service/order-search?orderTypeId=1&departmentId=${ORDER_PIPELINE_DEPARTMENT_ID}&updatedOn=${updatedFilter}&pageSize=500&firstResult=${firstResult}`;
    const r = await fetch(url, { method: 'GET', headers });
    if (!r.ok) {
      if (r.status === 503 || r.status === 429) tripBpCircuitBreaker(120);
      throw new Error(`order-search ${r.status}`);
    }
    const data = await r.json();
    const rows = data.response?.results || [];
    if (rows.length === 0) break;
    const ids = Array.isArray(rows[0]) ? rows.map((row) => row[0]) : rows;
    orderIds.push(...ids);
    firstResult += rows.length;
  }

  if (orderIds.length === 0) {
    await pool.query(
      `UPDATE order_pipeline_poll_state
       SET last_polled_at = NOW(), last_poll_count = 0,
           last_poll_duration_ms = $1, last_poll_error = NULL
       WHERE id = 1`,
      [Date.now() - start]
    );
    console.log(`[order-pipeline/${label}] window from=${fromIso}, 0 orders updated`);
    return { processed: 0, errors: 0, fromIso };
  }

  // Fetch details in BP-supported batches of up to ~100 IDs.
  let processed = 0;
  let errors = 0;
  const BATCH = 100;
  for (let i = 0; i < orderIds.length; i += BATCH) {
    const range = orderIds.slice(i, i + BATCH).join(',');
    const r = await fetch(`${baseUrl}/public-api/${BRIGHTPEARL_ACCOUNT_ID}/order-service/order/${range}`, {
      method: 'GET', headers,
    });
    if (!r.ok) {
      if (r.status === 503 || r.status === 429) tripBpCircuitBreaker(120);
      throw new Error(`order-detail batch ${r.status}`);
    }
    const data = await r.json();
    const orders = data.response || [];
    for (const order of orders) {
      try {
        await processOrderForPipeline(order);
        processed++;
      } catch (err) {
        errors++;
        await writeActivityLog({
          orderId: order?.id || 0, state: '', status: 'error',
          errorMessage: err.message,
          bpStatusName: order?.orderStatus?.name,
        });
      }
      await sleep(ORDER_PIPELINE_BP_SPACING_MS);
    }
  }

  await pool.query(
    `UPDATE order_pipeline_poll_state
     SET last_polled_at = NOW(), last_poll_count = $1,
         last_poll_duration_ms = $2, last_poll_error = NULL
     WHERE id = 1`,
    [processed, Date.now() - start]
  );
  console.log(`[order-pipeline/${label}] processed ${processed}/${orderIds.length} updated orders (${errors} errors) in ${Math.round((Date.now() - start) / 1000)}s. mode=${isDryRun() ? 'dry-run' : 'LIVE'}`);
  return { processed, errors, fromIso, total: orderIds.length };
}

// GET /api/order-pipeline/poll-status — last poll meta + dry-run flag
app.get('/api/order-pipeline/poll-status', async (req, res) => {
  if (!useDatabase) return res.status(503).json({ error: 'DATABASE_URL not configured' });
  try {
    const r = await pool.query(`SELECT last_polled_at, last_poll_count, last_poll_duration_ms, last_poll_error FROM order_pipeline_poll_state WHERE id = 1`);
    res.json({
      ...(r.rows[0] || {}),
      dryRun: isDryRun(),
      inFlight: orderPipelinePollInFlight,
      pollIntervalMs: ORDER_PIPELINE_POLL_INTERVAL_MS,
      windowMinMs: ORDER_PIPELINE_WINDOW_MIN_MS,
      departmentId: ORDER_PIPELINE_DEPARTMENT_ID,
    });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// GET /api/order-pipeline/activity — paginated activity log
// Skipped events are excluded from the default view (they'd dominate the
// table once the eligibility / skip-list rules are in play). Pass
// ?status=skipped explicitly to see them.
app.get('/api/order-pipeline/activity', async (req, res) => {
  if (!useDatabase) return res.status(503).json({ error: 'DATABASE_URL not configured' });
  const limit = Math.min(parseInt(req.query.limit, 10) || 100, 500);
  const offset = Math.max(parseInt(req.query.offset, 10) || 0, 0);
  const status = req.query.status; // optional filter: dry_run / sent / skipped / error
  const orderId = req.query.orderId; // optional filter
  const conditions = [];
  const params = [];
  if (status) {
    conditions.push(`status = $${params.length + 1}`);
    params.push(status);
  } else {
    // Default view hides skipped — most of those are routine (template
    // disabled, eligibility rule fired, manual exclude) and would bury
    // the dry_run / sent / error rows the operator actually cares about.
    conditions.push(`status <> 'skipped'`);
  }
  if (orderId) { conditions.push(`order_id = $${params.length + 1}`); params.push(Number(orderId)); }
  const where = conditions.length ? `WHERE ${conditions.join(' AND ')}` : '';
  try {
    const r = await pool.query(
      `SELECT id, order_id, state, status, recipient, sender, subject,
              error_message, bp_status_name, prev_state, created_at
       FROM order_pipeline_send_log
       ${where}
       ORDER BY created_at DESC
       LIMIT ${limit} OFFSET ${offset}`,
      params
    );
    const countRes = await pool.query(
      `SELECT COUNT(*)::int AS c FROM order_pipeline_send_log ${where}`,
      params
    );
    res.json({
      events: r.rows,
      total: countRes.rows[0].c,
      limit, offset,
    });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// POST /api/order-pipeline/poll/run — manual trigger
// Body: { windowMins?: number } — optional wider window for audit runs
app.post('/api/order-pipeline/poll/run', async (req, res) => {
  if (!useDatabase) return res.status(503).json({ error: 'DATABASE_URL not configured' });
  if (!isOrderPipelineEnabled()) {
    return res.status(503).json({ error: 'order pipeline disabled (ORDER_PIPELINE_ENABLED=false)' });
  }
  if (orderPipelinePollInFlight) {
    return res.status(409).json({ error: 'poll already in flight' });
  }
  const windowMins = Math.min(Math.max(parseInt(req.body?.windowMins, 10) || 0, 0), 60 * 24 * 7);
  const sinceMs = windowMins > 0 ? Date.now() - windowMins * 60 * 1000 : undefined;
  // Fire and forget — return immediately, results land in the activity log
  pollOrderTrackingPipeline({ sinceMs, label: `manual-${windowMins || 'auto'}` })
    .catch((err) => console.error('Manual order-pipeline run failed:', err.message));
  res.json({ accepted: true, windowMins: windowMins || 'auto', dryRun: isDryRun() });
});

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
      to: process.env.RECIPIENT_EMAILS || "bob@tuffshop.co.uk",
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
      `UPDATE customer_orders SET status = $1 WHERE id = $2
       RETURNING id, status, customer_name, contact_name, order_data, notes, created_at`,
      [status, orderId]
    );
    if (result.rowCount === 0) return res.status(404).json({ success: false, error: "Order not found" });

    const row = result.rows[0];
    res.json({ success: true, id: row.id, status: row.status });

    // Fire-and-forget: notify Fitness Inc when a REQUEST (notes-only, no items)
    // is marked complete. Regular product orders are intentionally excluded.
    if (status === "completed") {
      const items = row.order_data?.items || [];
      const isFitnessInc = (row.customer_name || "").toLowerCase().includes("fitness inc");
      const isRequest = items.length === 0 && !!row.notes;
      if (isFitnessInc && isRequest && process.env.SMTP_PASS) {
        const to = process.env.FITNESS_INC_NOTIFY_EMAIL || "info@fitnessincleeds.co.uk";
        const requestText = String(row.notes).replace(/^REQUEST:\s*/i, "").replace(/^LOCATION REQUEST:\s*/i, "");
        const submitted = new Date(row.created_at).toLocaleString("en-GB", { timeZone: "Europe/London" });
        try {
          const transporter = nodemailer.createTransport({
            host: process.env.SMTP_SERVER || "mail-eu.smtp2go.com",
            port: parseInt(process.env.SMTP_PORT || "2525"),
            secure: false,
            auth: { user: process.env.SMTP_USERNAME || "tuffshop.co.uk", pass: process.env.SMTP_PASS },
          });
          await transporter.sendMail({
            from: `"Fitness Inc Orders" <${process.env.SENDER_EMAIL || "fitnessincorders@tuffshop.co.uk"}>`,
            to,
            subject: "Your Fitness Inc request has been completed",
            text: `Hi ${row.contact_name || "Fitness Inc"},

Your request has been completed.

Original request: ${requestText}
Submitted: ${submitted}

If you have any questions just reply to this email.

Thanks,
Tuffshop`,
            html: `
              <div style="font-family:Arial,sans-serif;max-width:600px;margin:0 auto">
                <div style="background:#000;color:#fff;padding:16px 20px;border-bottom:3px solid #F3D014">
                  <h2 style="margin:0;font-size:18px">Request Completed</h2>
                </div>
                <div style="padding:20px;color:#222">
                  <p>Hi ${row.contact_name || "Fitness Inc"},</p>
                  <p>Your request has been completed.</p>
                  <p style="margin:16px 0;padding:12px;background:#fffbe6;border-left:3px solid #F3D014">
                    <strong>Original request:</strong><br>${requestText.replace(/\n/g, "<br>")}
                  </p>
                  <p style="color:#888;font-size:12px">Submitted: ${submitted}</p>
                  <p>If you have any questions just reply to this email.</p>
                  <p>Thanks,<br>Tuffshop</p>
                </div>
              </div>`,
          });
          console.log(`[fitness-inc] request-complete email sent to ${to} for order ${row.id}`);
        } catch (mailErr) {
          console.error(`[fitness-inc] failed to send request-complete email for order ${row.id}:`, mailErr.message);
        }
      }
    }
  } catch (err) {
    console.error("Failed to update order status:", err);
    res.status(500).json({ success: false, error: err.message });
  }
});

// ============================================================
// Fitness Inc — operator-added catalogue colours
// ============================================================
// Lets admins add new colours to existing FI products without a code
// change. Merged into the hardcoded catalogue at runtime by a frontend
// hook (useFitnessIncCatalogue).

// GET all additions — frontend merges into FITNESS_INC_PRODUCTS at runtime.
app.get('/api/fitness-inc/colour-additions', async (req, res) => {
  if (!pool) return res.status(503).json({ error: 'Database not configured' });
  try {
    const r = await pool.query(
      `SELECT id, product_id, colour_name, front_url, back_url, created_at, created_by
         FROM fitness_inc_colour_additions
        ORDER BY product_id, colour_name`
    );
    res.json({ additions: r.rows });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// POST a new colour addition. Body: { productId, colourName, frontUrl?, backUrl?, createdBy? }
// Upserts on (product_id, colour_name) so editing a colour's URLs just re-posts.
app.post('/api/fitness-inc/colour-additions', async (req, res) => {
  if (!pool) return res.status(503).json({ error: 'Database not configured' });
  try {
    const { productId, colourName, frontUrl, backUrl, createdBy } = req.body || {};
    if (!productId || !colourName) {
      return res.status(400).json({ error: 'productId and colourName are required' });
    }
    const r = await pool.query(
      `INSERT INTO fitness_inc_colour_additions (product_id, colour_name, front_url, back_url, created_by)
       VALUES ($1, $2, $3, $4, $5)
       ON CONFLICT (product_id, colour_name)
       DO UPDATE SET front_url = EXCLUDED.front_url,
                     back_url = EXCLUDED.back_url,
                     created_by = COALESCE(EXCLUDED.created_by, fitness_inc_colour_additions.created_by)
       RETURNING id, product_id, colour_name, front_url, back_url, created_at, created_by`,
      [String(productId), String(colourName).trim(), frontUrl || null, backUrl || null, createdBy || null]
    );
    res.json({ success: true, addition: r.rows[0] });
  } catch (err) {
    console.error('[fitness-inc-colours] add failed:', err.message);
    res.status(500).json({ error: err.message });
  }
});

// DELETE one — for removing colours added in error.
app.delete('/api/fitness-inc/colour-additions/:id', async (req, res) => {
  if (!pool) return res.status(503).json({ error: 'Database not configured' });
  try {
    const r = await pool.query(
      `DELETE FROM fitness_inc_colour_additions WHERE id = $1`,
      [req.params.id]
    );
    res.json({ success: true, deleted: r.rowCount });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// Whole operator-added products. The frontend merges these into the catalogue.
app.get('/api/fitness-inc/products', async (req, res) => {
  if (!pool) return res.status(503).json({ error: 'Database not configured' });
  try {
    const r = await pool.query(
      `SELECT id, product_id, name, code, colours, positions, one_size, images, created_at, created_by
         FROM fitness_inc_product_additions ORDER BY created_at DESC`
    );
    res.json({ products: r.rows });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// POST a new/edited product. Body: { code, name, colours[], positions[],
// oneSize?, images{colour:{front,back}}, productId?, createdBy? }.
// Upserts on product_id (defaults to a slug of the code) so re-saving edits.
app.post('/api/fitness-inc/products', async (req, res) => {
  if (!pool) return res.status(503).json({ error: 'Database not configured' });
  try {
    const { code, name, colours, positions, oneSize, images, productId, createdBy } = req.body || {};
    if (!code?.trim() || !name?.trim()) return res.status(400).json({ error: 'code and name are required' });
    const colArr = Array.isArray(colours) ? colours.map((c) => String(c).trim()).filter(Boolean) : [];
    const posArr = Array.isArray(positions) ? positions.map((p) => String(p).trim()).filter(Boolean) : [];
    if (colArr.length === 0) return res.status(400).json({ error: 'at least one colour is required' });
    if (posArr.length === 0) return res.status(400).json({ error: 'at least one position is required' });
    const pid = (productId || code).toString().toLowerCase().replace(/[^a-z0-9]/g, '') || `p${Date.now()}`;
    const r = await pool.query(
      `INSERT INTO fitness_inc_product_additions (product_id, name, code, colours, positions, one_size, images, created_by)
       VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
       ON CONFLICT (product_id) DO UPDATE SET
         name=EXCLUDED.name, code=EXCLUDED.code, colours=EXCLUDED.colours,
         positions=EXCLUDED.positions, one_size=EXCLUDED.one_size, images=EXCLUDED.images
       RETURNING id, product_id, name, code, colours, positions, one_size, images, created_at, created_by`,
      [pid, name.trim(), code.trim(), JSON.stringify(colArr), JSON.stringify(posArr),
       oneSize === true, JSON.stringify(images && typeof images === 'object' ? images : {}), createdBy || null]
    );
    res.json({ success: true, product: r.rows[0] });
  } catch (err) {
    console.error('[fitness-inc-products] save failed:', err.message);
    res.status(500).json({ error: err.message });
  }
});

app.delete('/api/fitness-inc/products/:id', async (req, res) => {
  if (!pool) return res.status(503).json({ error: 'Database not configured' });
  try {
    const r = await pool.query(`DELETE FROM fitness_inc_product_additions WHERE id = $1`, [req.params.id]);
    res.json({ success: true, deleted: r.rowCount });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// ============================================================
// Promo offer — up-sell on the proof-approval page
// ============================================================
// Operator configures a set of promo items + dual pricing. Customer
// sees them on the approval page right after approving and can add
// items at the deal price within a short window. Submission emails
// sales@tuffshop.co.uk + attaches a PDF summary to the BP order.

// Master switch: customer-facing promo offer is OFF by default until
// PROMO_OFFER_ENABLED=true is set on Render. `?preview=1` on the
// request bypasses the gate so the team can test the customer flow
// while real customers see nothing.
function isPromoOfferEnabled() {
  return String(process.env.PROMO_OFFER_ENABLED ?? 'false').toLowerCase() === 'true';
}

// Public: get the active offer items. Returns only enabled rows.
// Gated by PROMO_OFFER_ENABLED env var; preview=1 bypasses the gate.
app.get('/api/promo-offer-items', async (req, res) => {
  if (!pool) return res.status(503).json({ error: 'Database not configured' });
  const previewBypass = req.query.preview === '1' || req.query.preview === 'true';
  if (!isPromoOfferEnabled() && !previewBypass) {
    return res.json({ items: [], gated: true });
  }
  try {
    const r = await pool.query(
      `SELECT id, name, image_url, regular_price, deal_price, deal_window_minutes, sort_order, min_qty, logo_zone, logo_variant, bp_sku
         FROM promo_offer_items
        WHERE enabled = TRUE
        ORDER BY sort_order, id`
    );
    res.json({ items: r.rows, preview: previewBypass });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// Admin: list everything including disabled.
app.get('/api/promo-offer-items/admin', async (req, res) => {
  if (!pool) return res.status(503).json({ error: 'Database not configured' });
  try {
    const r = await pool.query(
      `SELECT id, name, image_url, regular_price, deal_price, deal_window_minutes, sort_order, enabled, created_at, min_qty, logo_zone, logo_variant, bp_sku
         FROM promo_offer_items
        ORDER BY sort_order, id`
    );
    res.json({ items: r.rows });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// Admin: create or update by id.
app.post('/api/promo-offer-items', async (req, res) => {
  if (!pool) return res.status(503).json({ error: 'Database not configured' });
  try {
    const { id, name, imageUrl, regularPrice, dealPrice, dealWindowMinutes, sortOrder, enabled, minQty, logoZone, logoVariant, bpSku } = req.body || {};
    if (!name) return res.status(400).json({ error: 'name required' });
    if (regularPrice == null || dealPrice == null) return res.status(400).json({ error: 'regularPrice and dealPrice required' });
    const minQtyClean = Math.max(1, parseInt(minQty, 10) || 1);
    const logoZoneJson = logoZone ? JSON.stringify(logoZone) : null;
    const variantClean = ['dark', 'light', 'auto'].includes(logoVariant) ? logoVariant : 'auto';
    const skuClean = bpSku?.trim() || null;
    if (id) {
      const r = await pool.query(
        `UPDATE promo_offer_items
            SET name = $1, image_url = $2, regular_price = $3, deal_price = $4,
                deal_window_minutes = $5, sort_order = $6, enabled = $7,
                min_qty = $8, logo_zone = $9, logo_variant = $10, bp_sku = $11
          WHERE id = $12
          RETURNING *`,
        [name, imageUrl || null, regularPrice, dealPrice, dealWindowMinutes ?? 10, sortOrder ?? 0, enabled ?? true, minQtyClean, logoZoneJson, variantClean, skuClean, id]
      );
      return res.json({ success: true, item: r.rows[0] });
    }
    const r = await pool.query(
      `INSERT INTO promo_offer_items (name, image_url, regular_price, deal_price, deal_window_minutes, sort_order, enabled, min_qty, logo_zone, logo_variant, bp_sku)
       VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
       RETURNING *`,
      [name, imageUrl || null, regularPrice, dealPrice, dealWindowMinutes ?? 10, sortOrder ?? 0, enabled ?? true, minQtyClean, logoZoneJson, variantClean, skuClean]
    );
    res.json({ success: true, item: r.rows[0] });
  } catch (err) {
    console.error('[promo-offer] save failed:', err.message);
    res.status(500).json({ error: err.message });
  }
});

app.delete('/api/promo-offer-items/:id', async (req, res) => {
  if (!pool) return res.status(503).json({ error: 'Database not configured' });
  try {
    const r = await pool.query(`DELETE FROM promo_offer_items WHERE id = $1`, [req.params.id]);
    res.json({ success: true, deleted: r.rowCount });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// Build the Brightpearl/Bolt "Pay Now Online" link for an order. Confirmed the
// order-only form works (no invoice needed) — pays the order balance, which now
// includes the just-added promo items. Returns null if creds/contactId missing.
async function buildOrderPaymentLink(orderId) {
  if (!BRIGHTPEARL_API_TOKEN || !BRIGHTPEARL_ACCOUNT_ID || !orderId) return null;
  const baseUrl = BRIGHTPEARL_DATACENTER === 'euw1'
    ? 'https://euw1.brightpearlconnect.com'
    : 'https://use1.brightpearlconnect.com';
  const headers = {
    'brightpearl-app-ref': process.env.BRIGHTPEARL_APP_REF,
    'brightpearl-account-token': BRIGHTPEARL_API_TOKEN,
    'Content-Type': 'application/json',
  };
  try {
    const r = await fetch(`${baseUrl}/public-api/${BRIGHTPEARL_ACCOUNT_ID}/order-service/order/${orderId}`, { headers });
    if (!r.ok) return null;
    const data = await r.json();
    const order = Array.isArray(data.response) ? data.response[0] : data.response;
    const contactId = order?.parties?.customer?.contactId ?? order?.contactId ?? null;
    if (!contactId) return null;
    const ACCOUNT = process.env.BOLT_ACCOUNT_CODE || 'tuffworkwear';
    return `https://bpp.withbolt.com/c/bpp/s/invoice.html?accountCode=${ACCOUNT}&channelKey=bpp&salesOrderId=${orderId}&contactId=${contactId}`;
  } catch (err) {
    console.error('[promo-offer] buildOrderPaymentLink failed:', err.message);
    return null;
  }
}

// Diagnostic: build candidate Brightpearl/Bolt "Pay Now" payment links for an
// order, so we can test which form the portal accepts (order-only vs needs an
// invoice). Read-only. GET /api/promo-offer/payment-link/:orderId
app.get('/api/promo-offer/payment-link/:orderId', async (req, res) => {
  if (!BRIGHTPEARL_API_TOKEN || !BRIGHTPEARL_ACCOUNT_ID) {
    return res.status(503).json({ error: 'Brightpearl credentials missing' });
  }
  const orderId = String(req.params.orderId).replace(/[^0-9]/g, '');
  if (!orderId) return res.status(400).json({ error: 'numeric orderId required' });
  const baseUrl = BRIGHTPEARL_DATACENTER === 'euw1'
    ? 'https://euw1.brightpearlconnect.com'
    : 'https://use1.brightpearlconnect.com';
  const headers = {
    'brightpearl-app-ref': process.env.BRIGHTPEARL_APP_REF,
    'brightpearl-account-token': BRIGHTPEARL_API_TOKEN,
    'Content-Type': 'application/json',
  };
  try {
    const r = await fetch(`${baseUrl}/public-api/${BRIGHTPEARL_ACCOUNT_ID}/order-service/order/${orderId}`, { headers });
    if (!r.ok) return res.status(502).json({ error: `BP order fetch returned ${r.status}` });
    const data = await r.json();
    const order = Array.isArray(data.response) ? data.response[0] : data.response;
    if (!order) return res.status(404).json({ error: 'order not found' });

    const contactId = order?.parties?.customer?.contactId ?? order?.contactId ?? null;
    // Best-effort: surface any invoice reference the order object exposes so we
    // can see whether an invoice exists at this stage.
    const invoiceRef =
      order?.invoices?.[0]?.invoiceReference ||
      order?.invoiceReference ||
      order?.invoice?.invoiceReference ||
      null;

    const ACCOUNT = process.env.BOLT_ACCOUNT_CODE || 'tuffworkwear';
    const base = `https://bpp.withbolt.com/c/bpp/s/invoice.html?accountCode=${ACCOUNT}&channelKey=bpp`;
    const links = {
      orderOnly: `${base}&salesOrderId=${orderId}&contactId=${contactId ?? ''}`,
      invoiceAsOrderRef: `${base}&salesInvoiceId=${orderId}&salesOrderId=${orderId}&contactId=${contactId ?? ''}`,
      withInvoice: invoiceRef ? `${base}&salesInvoiceId=${invoiceRef}&salesOrderId=${orderId}&contactId=${contactId ?? ''}` : null,
    };
    res.json({
      orderId, contactId, invoiceRef,
      hasInvoicesField: order?.invoices !== undefined,
      links,
    });
  } catch (err) {
    console.error('[promo-offer/payment-link]', err.message);
    res.status(500).json({ error: err.message });
  }
});

// ============================================================
// WhatsApp cross-sell — admin CRUD for the source→companion rules.
// ============================================================

// Master switch: auto-send of the post-approval cross-sell message is OFF
// until CROSSSELL_ENABLED=true on Render. Building/admin works regardless;
// this only gates actually messaging customers (marketing opt-in required).
function isCrossSellEnabled() {
  return String(process.env.CROSSSELL_ENABLED ?? 'false').toLowerCase() === 'true';
}

// Admin: list every rule (including disabled / unapproved Claude suggestions).
app.get('/api/crosssell/rules', async (req, res) => {
  if (!pool) return res.status(503).json({ error: 'Database not configured' });
  try {
    const r = await pool.query(
      `SELECT id, source_brand, source_garment_type, ordered_label, companion_product_code,
              companion_sku, companion_name, companion_image_url, price_each, match_colour,
              logo_variant, logo_zone, priority, enabled, origin, approved, created_at
         FROM crosssell_rules
        ORDER BY priority DESC, id`
    );
    res.json({ rules: r.rows, enabled: isCrossSellEnabled() });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// Admin: create or update a rule by id.
app.post('/api/crosssell/rules', async (req, res) => {
  if (!pool) return res.status(503).json({ error: 'Database not configured' });
  try {
    const {
      id, sourceBrand, sourceGarmentType, orderedLabel, companionProductCode, companionSku,
      companionName, companionImageUrl, priceEach, matchColour, logoVariant, logoZone, priority, enabled, origin, approved,
    } = req.body || {};
    if (!sourceBrand?.trim()) return res.status(400).json({ error: 'sourceBrand required' });
    if (!sourceGarmentType?.trim()) return res.status(400).json({ error: 'sourceGarmentType required' });
    if (!companionName?.trim()) return res.status(400).json({ error: 'companionName required' });
    if (!companionProductCode?.trim() && !companionSku?.trim()) {
      return res.status(400).json({ error: 'companionProductCode or companionSku required' });
    }
    const variantClean = ['dark', 'light', 'auto'].includes(logoVariant) ? logoVariant : 'auto';
    const originClean = origin === 'claude' ? 'claude' : 'manual';
    const logoZoneJson = logoZone ? JSON.stringify(logoZone) : null;
    const priceClean = (priceEach == null || priceEach === '' || isNaN(parseFloat(priceEach))) ? null : parseFloat(priceEach);
    const cols = [sourceBrand.trim(), sourceGarmentType.trim().toLowerCase(), orderedLabel?.trim() || null,
      companionProductCode?.trim() || null, companionSku?.trim() || null, companionName.trim(),
      companionImageUrl?.trim() || null, priceClean, matchColour ?? true, variantClean, logoZoneJson,
      priority ?? 0, enabled ?? true, originClean, approved ?? true];
    if (id) {
      const r = await pool.query(
        `UPDATE crosssell_rules SET
           source_brand=$1, source_garment_type=$2, ordered_label=$3, companion_product_code=$4,
           companion_sku=$5, companion_name=$6, companion_image_url=$7, price_each=$8, match_colour=$9,
           logo_variant=$10, logo_zone=$11, priority=$12, enabled=$13, origin=$14, approved=$15
         WHERE id=$16 RETURNING *`,
        [...cols, id]
      );
      return res.json({ success: true, rule: r.rows[0] });
    }
    const r = await pool.query(
      `INSERT INTO crosssell_rules
         (source_brand, source_garment_type, ordered_label, companion_product_code, companion_sku,
          companion_name, companion_image_url, price_each, match_colour, logo_variant, logo_zone, priority,
          enabled, origin, approved)
       VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15) RETURNING *`,
      cols
    );
    res.json({ success: true, rule: r.rows[0] });
  } catch (err) {
    console.error('[crosssell] save failed:', err.message);
    res.status(500).json({ error: err.message });
  }
});

app.delete('/api/crosssell/rules/:id', async (req, res) => {
  if (!pool) return res.status(503).json({ error: 'Database not configured' });
  try {
    const r = await pool.query(`DELETE FROM crosssell_rules WHERE id = $1`, [req.params.id]);
    res.json({ success: true, deleted: r.rowCount });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// Diagnostic: list the account's WhatsApp message templates (name + language +
// status) so we can see the EXACT name/locale to use. Needs WHATSAPP_WABA_ID
// (the WhatsApp Business Account id) — set it on Render once; it's in WhatsApp
// Manager → Account tools / API setup.
app.get('/api/whatsapp/templates', async (req, res) => {
  const token = process.env.WHATSAPP_TOKEN;
  const wabaId = process.env.WHATSAPP_WABA_ID;
  if (!token) return res.status(503).json({ error: 'WhatsApp not configured' });
  if (!wabaId) return res.status(400).json({ error: 'Set WHATSAPP_WABA_ID env (WhatsApp Business Account id) to list templates' });
  const graphVersion = process.env.WHATSAPP_GRAPH_VERSION || 'v21.0';
  try {
    const r = await fetch(
      `https://graph.facebook.com/${graphVersion}/${wabaId}/message_templates?fields=name,language,status,category&limit=200`,
      { headers: { Authorization: `Bearer ${token}` } }
    );
    const data = await r.json().catch(() => ({}));
    if (!r.ok) return res.status(502).json({ error: data?.error?.message || `Graph API ${r.status}`, details: data?.error || null });
    const templates = (data.data || []).map((t) => ({ name: t.name, language: t.language, status: t.status, category: t.category }));
    res.json({ count: templates.length, templates });
  } catch (err) {
    console.error('[whatsapp/templates] failed:', err.message);
    res.status(500).json({ error: err.message });
  }
});

// Bundled bold font for the price badge (so server-side text always renders).
let _badgeFontBuf = null;
function badgeFontBuffer() {
  if (_badgeFontBuf === null) {
    try { _badgeFontBuf = fs.readFileSync(path.join(__dirname, 'assets', 'badge-font.ttf')); }
    catch (e) { _badgeFontBuf = false; console.warn('[crosssell/badge] font missing:', e.message); }
  }
  return _badgeFontBuf || null;
}

// High-fidelity "from £X each" badge: SVG (yellow circle, slight tilt, size
// hierarchy) rendered to PNG via resvg — matches the admin PriceBadge much more
// closely than jimp's bitmap font. Returns a PNG buffer, or null on any failure
// (caller falls back to the jimp badge).
async function renderPriceBadgePng(price, diameter) {
  const fontBuf = badgeFontBuffer();
  if (!fontBuf) return null;
  const D = Math.max(120, Math.round(diameter));
  const val = Number(price);
  const display = Number.isInteger(val) ? String(val) : val.toFixed(2);
  const priceStr = `£${display}`;
  // Shrink the price font for longer amounts so it never overflows the circle.
  const priceFont = Math.min(D * 0.27, (D * 0.82) / (priceStr.length * 0.56));
  const c = (D / 2).toFixed(1);
  const svg =
    `<svg xmlns="http://www.w3.org/2000/svg" width="${D}" height="${D}" viewBox="0 0 ${D} ${D}">` +
    `<g transform="rotate(-8 ${c} ${c})" font-family="DejaVu Sans" font-weight="700" fill="#111827" text-anchor="middle">` +
    `<circle cx="${c}" cy="${c}" r="${c}" fill="#eab308"/>` +
    `<text x="${c}" y="${(D * 0.40).toFixed(1)}" font-size="${(D * 0.15).toFixed(1)}">from</text>` +
    `<text x="${c}" y="${(D * 0.61).toFixed(1)}" font-size="${priceFont.toFixed(1)}">${priceStr}</text>` +
    `<text x="${c}" y="${(D * 0.77).toFixed(1)}" font-size="${(D * 0.15).toFixed(1)}">each</text>` +
    `</g></svg>`;
  try {
    const { Resvg } = await import('@resvg/resvg-js');
    const r = new Resvg(svg, { font: { fontBuffers: [fontBuf], defaultFontFamily: 'DejaVu Sans', loadSystemFonts: false } });
    return r.render().asPng();
  } catch (e) {
    console.warn('[crosssell/badge] resvg render failed:', e.message);
    return null;
  }
}

// Build a circular "from £X each" price badge (brand yellow, black text) to
// composite onto the cross-sell mockup — jimp fallback for renderPriceBadgePng.
async function buildCrossSellPriceBadge(Jimp, price, diameter) {
  const D = Math.max(120, Math.round(diameter));
  const badge = new Jimp(D, D, 0xeab308ff); // brand yellow
  badge.circle();                            // round mask (transparent corners)
  const val = Number(price);
  const display = Number.isInteger(val) ? String(val) : val.toFixed(2);
  // Size hierarchy like the admin badge: small "from", big price, small "each".
  const fontBig = await Jimp.loadFont(D >= 240 ? Jimp.FONT_SANS_64_BLACK : Jimp.FONT_SANS_32_BLACK);
  const fontSm = await Jimp.loadFont(D >= 240 ? Jimp.FONT_SANS_32_BLACK : Jimp.FONT_SANS_16_BLACK);
  const lines = [
    { font: fontSm, text: 'from' },
    { font: fontBig, text: `£${display}` },
    { font: fontSm, text: 'each' },
  ];
  const heights = lines.map((l) => Jimp.measureTextHeight(l.font, l.text, D));
  const gap = Math.round(D * 0.015);
  const totalH = heights.reduce((a, b) => a + b, 0) + gap * (lines.length - 1);
  let y = Math.round((D - totalH) / 2);
  for (let i = 0; i < lines.length; i++) {
    const w = Jimp.measureText(lines[i].font, lines[i].text);
    badge.print(lines[i].font, Math.round((D - w) / 2), y, lines[i].text);
    y += heights[i] + gap;
  }
  return badge;
}

// Composite a customer's logo onto a companion product image in the rule's
// logo zone — the real cross-sell mockup. Served at a PUBLIC url so WhatsApp
// can fetch it as the template's image header. Generated on demand (not stored).
//   GET /api/crosssell/mockup?sessionId=<uuid>&ruleId=<n>&variant=primary|dark|light
app.get('/api/crosssell/mockup', async (req, res) => {
  if (!pool) return res.status(503).send('Database not configured');
  const { sessionId, ruleId, variant } = req.query;
  if (!sessionId) return res.status(400).send('sessionId required');
  try {
    const sr = await pool.query(
      `SELECT primary_logo_data, promo_logo_dark_data, promo_logo_light_data, crosssell_candidate
         FROM approval_sessions WHERE id = $1`,
      [sessionId]
    );
    const s = sr.rows[0];
    if (!s) return res.status(404).send('Session not found');

    // Prefer the session's stored candidate (companion already assembled in the
    // ORDERED colour). Fall back to a rule lookup (default colour) for legacy
    // sessions / explicit ruleId tests.
    const cand = s.crosssell_candidate || null;
    let companionUrl = cand?.companionImageUrl || null;
    let companionUrlFallback = cand?.companionImageUrlDefault || null;
    let zone = cand?.logoZone || null;
    let price = cand?.priceEach ?? null;
    if (!companionUrl && ruleId) {
      const rr = await pool.query(`SELECT companion_image_url, logo_zone, price_each FROM crosssell_rules WHERE id = $1`, [ruleId]);
      const rule = rr.rows[0];
      if (rule?.companion_image_url) { companionUrl = rule.companion_image_url; zone = rule.logo_zone; price = rule.price_each; }
    }
    if (!companionUrl) return res.status(404).send('No companion image (no candidate and no usable rule)');

    const logoBuffer =
      variant === 'dark' ? (s.promo_logo_dark_data || s.primary_logo_data)
      : variant === 'light' ? (s.promo_logo_light_data || s.primary_logo_data)
      : s.primary_logo_data;
    if (!logoBuffer) return res.status(404).send('No logo on this session');

    const { default: Jimp } = await import('jimp');
    // Try the colour-matched image; if that exact colour isn't stocked (our
    // /image route 500s on a missing code), fall back to the default colour.
    let companion;
    try {
      companion = await Jimp.read(companionUrl);
    } catch (e) {
      if (!companionUrlFallback) throw e;
      console.warn(`[crosssell/mockup] colour image failed (${companionUrl}); using default`);
      companion = await Jimp.read(companionUrlFallback);
    }
    const logo = await Jimp.read(Buffer.from(logoBuffer));
    const W = companion.bitmap.width, H = companion.bitmap.height;
    const z = zone || { x: 50, y: 30, width: 14, height: 10, rotation: 0 };
    const zw = (z.width / 100) * W, zh = (z.height / 100) * H;
    const zx = (z.x / 100) * W, zy = (z.y / 100) * H;
    if (z.rotation) logo.rotate(z.rotation); // degrees; zones are usually 0
    const scale = Math.min(zw / logo.bitmap.width, zh / logo.bitmap.height);
    if (scale > 0 && isFinite(scale)) logo.scale(scale);
    const lx = Math.round(zx + (zw - logo.bitmap.width) / 2);
    const ly = Math.round(zy + (zh - logo.bitmap.height) / 2);
    companion.composite(logo, lx, ly);

    // "from £X each" yellow price badge, top-right — matches the admin preview.
    if (price != null && price !== '' && !isNaN(Number(price))) {
      try {
        const D = Math.round(W * 0.30);
        const png = await renderPriceBadgePng(price, D);          // crisp SVG version
        const badge = png ? await Jimp.read(Buffer.from(png)) : await buildCrossSellPriceBadge(Jimp, price, D); // jimp fallback
        const m = Math.round(W * 0.02);
        companion.composite(badge, W - badge.bitmap.width - m, m);
      } catch (e) { console.warn('[crosssell/mockup] price badge failed:', e.message); }
    }

    const out = await companion.getBufferAsync(Jimp.MIME_PNG);
    res.setHeader('Content-Type', 'image/png');
    res.setHeader('Cache-Control', 'public, max-age=600');
    res.send(out);
  } catch (err) {
    console.error('[crosssell/mockup] failed:', err.message);
    res.status(500).send('Mockup generation failed');
  }
});

// Admin TEST SEND: fire the real proof_approved_crosssell WhatsApp template to
// an EXPLICIT test number (never a customer), using a chosen rule's companion
// image + wording. Confirms the template/image/buttons render on WhatsApp
// before the full auto-send pipeline exists. NOT gated by CROSSSELL_ENABLED —
// the recipient is explicit. Logs a crosssell_sends row so the "Yes" reply
// loop can also be tested from the test phone.
app.post('/api/crosssell/test-send', async (req, res) => {
  if (!pool) return res.status(503).json({ error: 'Database not configured' });
  const token = process.env.WHATSAPP_TOKEN;
  const phoneNumberId = process.env.WHATSAPP_PHONE_NUMBER_ID;
  if (!token || !phoneNumberId) return res.status(503).json({ error: 'WhatsApp not configured' });

  const { toPhone, ruleId, firstName, templateName: tplOverride, templateLang: langOverride, sessionId, logoVariant } = req.body || {};
  const to = normaliseWhatsAppNumber(toPhone);
  if (!to) return res.status(400).json({ error: 'A valid test phone number is required' });

  try {
    // If a session with a stored candidate is given, use it (companion already
    // colour-matched + the matched wording). Otherwise fall back to a rule.
    let candidate = null;
    if (sessionId) {
      const cr = await pool.query(`SELECT crosssell_candidate FROM approval_sessions WHERE id = $1`, [sessionId]);
      candidate = cr.rows[0]?.crosssell_candidate || null;
    }
    const rRes = ruleId
      ? await pool.query(`SELECT * FROM crosssell_rules WHERE id = $1`, [ruleId])
      : candidate?.ruleId
      ? await pool.query(`SELECT * FROM crosssell_rules WHERE id = $1`, [candidate.ruleId])
      : await pool.query(`SELECT * FROM crosssell_rules WHERE enabled = true AND companion_image_url IS NOT NULL ORDER BY id LIMIT 1`);
    const rule = rRes.rows[0];
    if (!rule && !candidate) return res.status(400).json({ error: 'No usable cross-sell rule or session candidate' });

    const name = (firstName || 'there').toString().slice(0, 60);
    const ordered = (candidate?.orderedLabel || rule?.ordered_label || 'your order').toString().slice(0, 60);
    const companion = (candidate?.companionName || rule?.companion_name || 'matching item').toString().slice(0, 60);

    // Header image: if a session is given, composite that customer's logo onto
    // the (colour-matched) companion via the mockup endpoint. Otherwise the bare
    // companion product image from the rule.
    const publicBackend = process.env.PUBLIC_BACKEND_URL || 'https://server-backend-1i47.onrender.com';
    const headerImageUrl = sessionId
      ? `${publicBackend}/api/crosssell/mockup?sessionId=${encodeURIComponent(sessionId)}${rule ? `&ruleId=${rule.id}` : ''}&variant=${encodeURIComponent(logoVariant || 'primary')}&cb=${Date.now()}`
      : rule.companion_image_url;
    const templateName = (tplOverride || '').trim() || process.env.CROSSSELL_TEMPLATE_NAME || 'proof_approved_crosssell';
    // The cross-sell template was approved in plain English ("en"), NOT en_GB
    // (unlike the proof template). Confirmed by the user 2026-06-22.
    const templateLang = (langOverride || '').trim() || process.env.CROSSSELL_TEMPLATE_LANG || 'en';
    const graphVersion = process.env.WHATSAPP_GRAPH_VERSION || 'v21.0';

    const gRes = await fetch(`https://graph.facebook.com/${graphVersion}/${phoneNumberId}/messages`, {
      method: 'POST',
      headers: { Authorization: `Bearer ${token}`, 'Content-Type': 'application/json' },
      body: JSON.stringify({
        messaging_product: 'whatsapp',
        to,
        type: 'template',
        template: {
          name: templateName,
          language: { code: templateLang },
          components: [
            { type: 'header', parameters: [{ type: 'image', image: { link: headerImageUrl } }] },
            { type: 'body', parameters: [
              { type: 'text', text: name },
              { type: 'text', text: ordered },
              { type: 'text', text: companion },
            ] },
          ],
        },
      }),
    });
    const data = await gRes.json().catch(() => ({}));
    if (!gRes.ok) {
      console.error('[crosssell/test-send] Graph error:', JSON.stringify(data?.error || data));
      return res.status(502).json({ error: data?.error?.message || `Graph API returned ${gRes.status}`, details: data?.error || null });
    }
    const messageId = data?.messages?.[0]?.id || null;

    // Record for visibility in the chat thread + enable the reply-loop test.
    const renderedBody =
      `Hi ${name}, thanks for approving your proof! As you've ordered ${ordered}, ` +
      `we've popped your logo onto a matching ${companion} reply YES and we'll add it to your order. 🙌`;
    const ruleIdForLog = rule?.id || candidate?.ruleId || null;
    await recordWhatsAppMessage({
      waMessageId: messageId, direction: 'out', peerNumber: to, body: renderedBody,
      msgType: 'template', status: 'sent', sentBy: 'cross-sell test',
      raw: { template: templateName, test: true, ruleId: ruleIdForLog },
    });
    await pool.query(
      `INSERT INTO crosssell_sends (order_number, customer_phone, rule_id, companion_sku, companion_name, mockup_url, wa_message_id)
       VALUES ($1,$2,$3,$4,$5,$6,$7)`,
      ['TEST', to, ruleIdForLog, rule?.companion_sku || null, companion, headerImageUrl, messageId]
    );

    console.log(`[crosssell/test-send] sent to ${to} id=${messageId} (rule ${ruleIdForLog}, colour ${candidate?.orderedColour || 'default'})`);
    res.json({ success: true, messageId, to, ruleUsed: { id: ruleIdForLog, companion, colour: candidate?.orderedColour || 'default (no candidate)' } });
  } catch (err) {
    console.error('[crosssell/test-send] failed:', err.message);
    res.status(500).json({ error: err.message });
  }
});

// Admin: ask Claude to PROPOSE cross-sell rules (brand + garment → companion).
// Returns suggestions only — nothing is saved; the operator reviews each, adds
// the real product code, and clicks Add. Used occasionally, so cost is trivial.
const CROSSSELL_GARMENT_TYPES = [
  'softshell', 'bodywarmer', 'hoodie', 'sweatshirt', 'fleece', 'polo', 't-shirt',
  'jacket', 'trousers', 'shorts', 'shirt', 'beanie', 'cap', 'hi-vis', 'overall', 'apron',
];
app.post('/api/crosssell/suggest', async (req, res) => {
  if (!process.env.ANTHROPIC_API_KEY) return res.status(501).json({ error: 'ANTHROPIC_API_KEY not configured on the backend' });
  const context = (req.body?.context || '').toString().slice(0, 300);
  const count = Math.min(Math.max(parseInt(req.body?.count, 10) || 6, 1), 12);
  try {
    let existing = [];
    if (pool) {
      const r = await pool.query('SELECT source_brand, source_garment_type, companion_name FROM crosssell_rules');
      existing = r.rows.map((x) => `${x.source_brand} ${x.source_garment_type} → ${x.companion_name}`);
    }
    const prompt =
      `You propose WhatsApp cross-sell rules for Tuff Shop, a UK workwear & promotional ` +
      `decorating company. After a customer approves a proof, we offer them ONE complementary ` +
      `item from the SAME brand with their logo printed/embroidered on it.\n\n` +
      `Garment-type tokens to use for sourceGarmentType and companionGarmentType (use these exact strings): ` +
      `${CROSSSELL_GARMENT_TYPES.join(', ')}.\n\n` +
      `Existing rules — do NOT duplicate these brand+garment pairings:\n` +
      `${existing.length ? existing.map((e) => `- ${e}`).join('\n') : '(none yet)'}\n\n` +
      (context ? `Operator focus: ${context}\n\n` : '') +
      `Propose up to ${count} NEW, sensible rules. Only decoratable garments, companion from the SAME brand, ` +
      `natural pairings (e.g. polo→sweatshirt, jacket→fleece, t-shirt→hoodie). Favour brands common in UK ` +
      `workwear (Blaklader, Snickers, Uneek, Portwest, Regatta, Result, Fruit of the Loom, Gildan). ` +
      `For suggestedCompanionCode give the real style code ONLY if you are confident (e.g. Blaklader 3340 ` +
      `sweatshirt); otherwise return an empty string — never invent a code. priceEach: indicative GBP per ` +
      `item if known, else 0. orderedLabel: a natural phrase like "a Blaklader polo". rationale: one short sentence.`;

    const schema = {
      type: 'object', additionalProperties: false,
      properties: {
        suggestions: {
          type: 'array',
          items: {
            type: 'object', additionalProperties: false,
            properties: {
              sourceBrand: { type: 'string' },
              sourceGarmentType: { type: 'string' },
              companionGarmentType: { type: 'string' },
              companionName: { type: 'string' },
              suggestedCompanionCode: { type: 'string' },
              orderedLabel: { type: 'string' },
              priceEach: { type: 'number' },
              rationale: { type: 'string' },
            },
            required: ['sourceBrand', 'sourceGarmentType', 'companionGarmentType', 'companionName', 'suggestedCompanionCode', 'orderedLabel', 'priceEach', 'rationale'],
          },
        },
      },
      required: ['suggestions'],
    };

    const client = new Anthropic();
    const msg = await client.messages.create({
      model: 'claude-opus-4-8',
      max_tokens: 2000,
      messages: [{ role: 'user', content: prompt }],
      output_config: { format: { type: 'json_schema', schema } },
    });
    const text = msg.content?.find((b) => b.type === 'text')?.text || '{}';
    let parsed = {};
    try { parsed = JSON.parse(text); } catch { return res.status(502).json({ error: 'Claude returned malformed JSON' }); }
    res.json({ suggestions: Array.isArray(parsed.suggestions) ? parsed.suggestions : [] });
  } catch (e) {
    console.error('[crosssell/suggest] failed:', e.message);
    res.status(500).json({ error: e.message });
  }
});

// ============================================================
// Promo-item production jigs — templates + EPS generation.
// ============================================================

// List jig templates (notepad / coaster / mug / pen).
app.get('/api/jig/templates', async (req, res) => {
  if (!pool) return res.status(503).json({ error: 'Database not configured' });
  try {
    const r = await pool.query(
      `SELECT id, item_key, label, page_w_mm, page_h_mm, placements, grid, vector_required, enabled, updated_at
         FROM jig_templates ORDER BY item_key`
    );
    res.json({ templates: r.rows });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// Re-apply the built-in validated defaults (overwrites current templates).
app.post('/api/jig/templates/reset-defaults', async (req, res) => {
  if (!pool) return res.status(503).json({ error: 'Database not configured' });
  try {
    await upsertJigDefaults(true);
    const r = await pool.query(
      `SELECT id, item_key, label, page_w_mm, page_h_mm, placements, grid, vector_required, enabled, updated_at
         FROM jig_templates ORDER BY item_key`
    );
    res.json({ success: true, templates: r.rows });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// Upsert a jig template by item_key (admin fine-tuning).
app.post('/api/jig/templates', async (req, res) => {
  if (!pool) return res.status(503).json({ error: 'Database not configured' });
  try {
    const { itemKey, label, pageWmm, pageHmm, placements, grid, vectorRequired, enabled } = req.body || {};
    if (!itemKey?.trim()) return res.status(400).json({ error: 'itemKey required' });
    if (pageWmm == null || pageHmm == null) return res.status(400).json({ error: 'pageWmm and pageHmm required' });
    const r = await pool.query(
      `INSERT INTO jig_templates (item_key, label, page_w_mm, page_h_mm, placements, grid, vector_required, enabled, updated_at)
       VALUES ($1,$2,$3,$4,$5,$6,$7,$8, NOW())
       ON CONFLICT (item_key) DO UPDATE SET
         label=EXCLUDED.label, page_w_mm=EXCLUDED.page_w_mm, page_h_mm=EXCLUDED.page_h_mm,
         placements=EXCLUDED.placements, grid=EXCLUDED.grid, vector_required=EXCLUDED.vector_required,
         enabled=EXCLUDED.enabled, updated_at=NOW()
       RETURNING *`,
      [itemKey.trim(), label || itemKey, pageWmm, pageHmm,
       placements ? JSON.stringify(placements) : null,
       grid ? JSON.stringify(grid) : null,
       vectorRequired ?? false, enabled ?? true]
    );
    res.json({ success: true, template: r.rows[0] });
  } catch (err) {
    console.error('[jig] save failed:', err.message);
    res.status(500).json({ error: err.message });
  }
});

// Generate a print-ready EPS for an item from a (raster) logo.
// Body: { itemKey, logoBase64 } → EPS download.
app.post('/api/jig/generate', async (req, res) => {
  if (!pool) return res.status(503).json({ error: 'Database not configured' });
  try {
    const { itemKey, logoBase64, logoAdjust } = req.body || {};
    if (!itemKey || !logoBase64) return res.status(400).json({ error: 'itemKey and logoBase64 required' });
    const tRes = await pool.query(`SELECT * FROM jig_templates WHERE item_key = $1`, [itemKey]);
    if (tRes.rowCount === 0) return res.status(404).json({ error: `no jig template for '${itemKey}'` });
    const t = tRes.rows[0];
    const fileBuffer = Buffer.from(logoBase64, 'base64');
    const placements = placementsFromTemplate({ placements: t.placements, grid: t.grid });
    const pageWmm = Number(t.page_w_mm), pageHmm = Number(t.page_h_mm);
    // Route on the actual uploaded file, not the template flag: a vector .eps is
    // tiled as TRUE vector for any item (best quality); a raster image is
    // embedded. vector_required (pen) still rejects a raster upload.
    const uploadIsVector = isVectorEps(fileBuffer);
    if (t.vector_required && !uploadIsVector) {
      return res.status(422).json({ error: `'${itemKey}' needs a vector .eps logo` });
    }
    // The white substrate box (item-sized) belongs on single-item jigs
    // (notepad/coaster/mug) but NOT the pen bed, whose "page" is the whole jig.
    const isPenBed = t.grid && t.grid.kind === 'pen';
    const eps = uploadIsVector
      ? tileVectorEps({ vectorBuffer: fileBuffer, pageWmm, pageHmm, placements, logoAdjust, drawBackground: !isPenBed })
      : await generateJigEps({ logoBuffer: fileBuffer, pageWmm, pageHmm, placements });
    res.setHeader('Content-Type', 'application/postscript');
    res.setHeader('Content-Disposition', `attachment; filename="jig-${itemKey}.eps"`);
    res.send(eps);
  } catch (err) {
    console.error('[jig] generate failed:', err.message);
    res.status(500).json({ error: err.message });
  }
});

// Generate a jig EPS for a specific order using that order's stored logo —
// driven from the promo uptake report ("click the job, generate the jigs").
// Body: { sessionId, itemKey } → EPS download.
app.post('/api/jig/generate-for-session', async (req, res) => {
  if (!pool) return res.status(503).json({ error: 'Database not configured' });
  try {
    const { sessionId, itemKey } = req.body || {};
    if (!sessionId || !itemKey) return res.status(400).json({ error: 'sessionId and itemKey required' });
    const tRes = await pool.query(`SELECT * FROM jig_templates WHERE item_key = $1`, [itemKey]);
    if (tRes.rowCount === 0) return res.status(404).json({ error: `no jig template for '${itemKey}'` });
    const t = tRes.rows[0];
    if (t.vector_required) return res.status(422).json({ error: `'${itemKey}' needs vector artwork — not supported yet` });
    const sRes = await pool.query(`SELECT primary_logo_data FROM approval_sessions WHERE id = $1`, [sessionId]);
    if (sRes.rowCount === 0) return res.status(404).json({ error: 'order/session not found' });
    const logoBuffer = sRes.rows[0].primary_logo_data;
    if (!logoBuffer) return res.status(422).json({ error: 'no logo stored for this order' });
    const placements = placementsFromTemplate({ placements: t.placements, grid: t.grid });
    const eps = await generateJigEps({
      logoBuffer, pageWmm: Number(t.page_w_mm), pageHmm: Number(t.page_h_mm), placements,
    });
    res.setHeader('Content-Type', 'application/postscript');
    res.setHeader('Content-Disposition', `attachment; filename="jig-${itemKey}-${String(sessionId).slice(0, 8)}.eps"`);
    res.send(eps);
  } catch (err) {
    console.error('[jig] generate-for-session failed:', err.message);
    res.status(500).json({ error: err.message });
  }
});

// Customer submits their selection from the approval page. Records to
// promo_offer_uptake, emails sales@tuffshop.co.uk with the line items
// + customer ref, and attaches a PDF summary to the BP order if we can.
app.post('/api/promo-offer/submit', async (req, res) => {
  if (!pool) return res.status(503).json({ error: 'Database not configured' });
  try {
    const previewBypass = req.query.preview === '1' || req.query.preview === 'true' || req.body?.preview === true;
    if (!isPromoOfferEnabled() && !previewBypass) {
      return res.status(503).json({ error: 'Promo offer not currently enabled' });
    }
    const { approvalSessionId, items, testEmail } = req.body || {};
    if (!approvalSessionId) return res.status(400).json({ error: 'approvalSessionId required' });
    if (!Array.isArray(items) || items.length === 0) return res.status(400).json({ error: 'items[] required' });

    // Pull the session for context (customer/order ref + the captured
    // logos so we can overlay them on the PDF preview).
    const sessRes = await pool.query(
      `SELECT customer_name, order_number, recipient_name, approver_name,
              primary_logo_data, primary_logo_mime,
              promo_logo_dark_data, promo_logo_dark_mime,
              promo_logo_light_data, promo_logo_light_mime
         FROM approval_sessions WHERE id = $1`,
      [approvalSessionId]
    );
    if (sessRes.rowCount === 0) return res.status(404).json({ error: 'approval session not found' });
    const sess = sessRes.rows[0];

    // Hydrate item details from the DB so we trust prices server-side
    // rather than whatever the client posted. Also pull image_url +
    // logo_zone + logo_variant so we can render the PDF preview.
    const ids = items.map((i) => Number(i.itemId)).filter(Boolean);
    const itemDetailsRes = await pool.query(
      `SELECT id, name, regular_price, deal_price, image_url, logo_zone, logo_variant, bp_sku
         FROM promo_offer_items WHERE id = ANY($1::int[])`,
      [ids]
    );
    const detailById = new Map(itemDetailsRes.rows.map((r) => [r.id, r]));
    // Stored prices are inc-VAT (UK 20%). Internal & customer-facing
    // figures are ex-VAT with a "+ VAT" note on the total.
    const VAT_RATE = 0.20;
    const exVat = (incVat) => Number(incVat) / (1 + VAT_RATE);

    const lineItems = items
      .map((i) => {
        const d = detailById.get(Number(i.itemId));
        if (!d) return null;
        const qty = Math.max(1, parseInt(i.qty, 10) || 1);
        const dealExVat = exVat(d.deal_price);
        return {
          itemId: d.id,
          name: d.name,
          qty,
          dealPriceExVat: dealExVat,
          regularPriceExVat: exVat(d.regular_price),
          dealPriceIncVat: Number(d.deal_price),
          lineTotalExVat: dealExVat * qty,
          imageUrl: d.image_url,
          logoZone: d.logo_zone,
          logoVariant: d.logo_variant || 'auto',
          bpSku: d.bp_sku || null,
        };
      })
      .filter(Boolean);

    if (lineItems.length === 0) return res.status(400).json({ error: 'no valid items selected' });

    // Bundle discount tiers — mirror frontend exactly so totals match.
    //   1 unique item  →  no discount
    //   2 unique items →  10% off
    //   3 unique items →  15% off
    //   4+             →  20% off
    const uniqueItemCount = lineItems.length;
    const bundleDiscountPct = uniqueItemCount >= 4 ? 0.20 : uniqueItemCount === 3 ? 0.15 : uniqueItemCount === 2 ? 0.10 : 0;
    // Round each figure to the penny and derive the next from the rounded
    // value so the displayed breakdown reconciles (subtotal − discount =
    // total), matching the customer-facing card. Display-only — the BP row
    // amounts below are computed per-line independently (and BP rounds each
    // row itself), so this doesn't change what's actually charged.
    const round2 = (n) => Math.round((n + Number.EPSILON) * 100) / 100;
    const subtotalExVat = round2(lineItems.reduce((s, l) => s + l.lineTotalExVat, 0));
    const bundleDiscount = round2(subtotalExVat * bundleDiscountPct);
    const orderTotalExVat = round2(subtotalExVat - bundleDiscount);
    // Keep `orderTotal` name as an alias so the rest of the handler /
    // PDF code (further down) doesn't need rewriting — but it's now
    // ex-VAT after bundle discount.
    const orderTotal = orderTotalExVat;
    const customerIp = req.headers['x-forwarded-for']?.split(',')[0]?.trim() || req.socket?.remoteAddress || null;

    // Record the uptake — but never for preview/test submissions, so the
    // reported order numbers / revenue stay clean.
    if (!previewBypass) {
      await pool.query(
        `INSERT INTO promo_offer_uptake (approval_session_id, order_number, customer_name, items, customer_ip)
         VALUES ($1, $2, $3, $4, $5)`,
        [approvalSessionId, sess.order_number, sess.customer_name, JSON.stringify(lineItems), customerIp]
      );
    }

    // Auto-add rows to the BP order at the agreed deal price (with the
    // bundle discount distributed proportionally per line). Only fires
    // for items that have a BP SKU configured in the admin AND when not
    // in preview mode. Each failure is logged but doesn't block the
    // rest of the response — the email still goes out so the operator
    // sees what should have been added if any addition failed.
    const bpRowResults = [];
    if (sess.order_number && !previewBypass) {
      for (const l of lineItems) {
        if (!l.bpSku) {
          bpRowResults.push({ name: l.name, sku: null, ok: false, reason: 'no SKU configured' });
          continue;
        }
        const productId = await bpLookupProductBySku(l.bpSku);
        if (!productId) {
          bpRowResults.push({ name: l.name, sku: l.bpSku, ok: false, reason: 'SKU not found in BP' });
          continue;
        }
        const lineExVatAfterDiscount = l.lineTotalExVat * (1 - bundleDiscountPct);
        const addRes = await bpAddOrderRow(sess.order_number, productId, l.qty, lineExVatAfterDiscount, `${l.name} + PRINTED LOGO`);
        bpRowResults.push({
          name: l.name, sku: l.bpSku, productId, ok: addRes.ok,
          reason: addRes.ok ? null : (addRes.body || addRes.error || `status ${addRes.status}`),
        });
        if (addRes.ok) {
          console.log(`[promo-offer] added ${l.qty}x ${l.name} (SKU ${l.bpSku}, productId ${productId}) to BP order ${sess.order_number} @ £${lineExVatAfterDiscount.toFixed(2)} ex VAT`);
        }
      }
    }

    // Build the email + the PDF summary in parallel.
    const itemRows = lineItems.map((l) =>
      `<tr>
        <td style="padding:8px;border-bottom:1px solid #eee">${l.name}</td>
        <td style="padding:8px;border-bottom:1px solid #eee;text-align:center">${l.qty}</td>
        <td style="padding:8px;border-bottom:1px solid #eee;text-align:right">£${l.dealPriceExVat.toFixed(2)}</td>
        <td style="padding:8px;border-bottom:1px solid #eee;text-align:right"><strong>£${l.lineTotalExVat.toFixed(2)}</strong></td>
      </tr>`
    ).join('');

    // Discount + totals row (rendered separately so the discount line
    // only appears when applicable).
    const totalsRows = `
      <tr>
        <td colspan="3" style="padding:8px 8px 4px;text-align:right">Subtotal</td>
        <td style="padding:8px 8px 4px;text-align:right">£${subtotalExVat.toFixed(2)}</td>
      </tr>
      ${bundleDiscountPct > 0 ? `<tr>
        <td colspan="3" style="padding:4px 8px;text-align:right;color:#48C549">Bundle discount (${Math.round(bundleDiscountPct * 100)}%)</td>
        <td style="padding:4px 8px;text-align:right;color:#48C549">−£${bundleDiscount.toFixed(2)}</td>
      </tr>` : ''}
      <tr>
        <td colspan="3" style="padding:8px;text-align:right;font-weight:bold;border-top:1px solid #eee">Total + VAT</td>
        <td style="padding:8px;text-align:right;font-weight:bold;border-top:1px solid #eee">£${orderTotalExVat.toFixed(2)}</td>
      </tr>`;

    const subject = `${previewBypass ? '[PREVIEW] ' : ''}Promo add-on for ${sess.customer_name || 'customer'}${sess.order_number ? ` (order ${sess.order_number})` : ''}`;
    const html = `
      <div style="font-family:Arial,sans-serif;max-width:600px">
        <div style="background:#000;color:#fff;padding:16px 20px;border-bottom:3px solid #F3D014">
          <h2 style="margin:0;font-size:18px">Promo Add-On Request</h2>
        </div>
        <div style="padding:20px">
          <p><strong>Customer:</strong> ${sess.customer_name || '?'}</p>
          ${sess.order_number ? `<p><strong>Order:</strong> ${sess.order_number}</p>` : ''}
          ${sess.approver_name ? `<p><strong>Approved by:</strong> ${sess.approver_name}</p>` : ''}
          <p><strong>Submitted:</strong> ${new Date().toLocaleString('en-GB', { timeZone: 'Europe/London' })}</p>
          <table style="width:100%;border-collapse:collapse;margin-top:16px">
            <thead><tr style="background:#f5f5f5">
              <th style="padding:8px;text-align:left">Item</th>
              <th style="padding:8px;text-align:center">Qty</th>
              <th style="padding:8px;text-align:right">Deal £ (ex VAT)</th>
              <th style="padding:8px;text-align:right">Line £ (ex VAT)</th>
            </tr></thead>
            <tbody>${itemRows}</tbody>
            <tfoot>${totalsRows}</tfoot>
          </table>
          ${bpRowResults.length > 0 ? `<p style="color:${bpRowResults.every(r => r.ok) ? '#48C549' : '#dc0032'};font-size:12px;margin-top:12px"><strong>Auto-added to BP order:</strong> ${bpRowResults.filter(r => r.ok).length}/${bpRowResults.length} item(s).${bpRowResults.some(r => !r.ok) ? '<br>Failed:<br>' + bpRowResults.filter(r => !r.ok).map(r => `• ${r.name}${r.sku ? ` (SKU ${r.sku})` : ''}: ${r.reason}`).join('<br>') : ''}</p>` : ''}
          <p style="color:#666;font-size:12px;margin-top:20px">All prices ex-VAT. PDF summary attached.</p>
        </div>
      </div>`;
    const textBody = `Promo Add-On Request

Customer: ${sess.customer_name || '?'}${sess.order_number ? `\nOrder: ${sess.order_number}` : ''}
Submitted: ${new Date().toISOString()}

${lineItems.map((l) => `- ${l.name} x ${l.qty} @ £${l.dealPriceExVat.toFixed(2)} = £${l.lineTotalExVat.toFixed(2)} (ex VAT)`).join('\n')}

Subtotal: £${subtotalExVat.toFixed(2)}
${bundleDiscountPct > 0 ? `Bundle discount (${Math.round(bundleDiscountPct * 100)}%): -£${bundleDiscount.toFixed(2)}\n` : ''}Total + VAT: £${orderTotalExVat.toFixed(2)}`;

    // Generate a PDF summary with per-row thumbnails (item image + the
    // customer's logo overlaid in the configured zone, with rotation).
    // The thumbnail mirrors what the customer saw on the approval page.
    let pdfBuffer = null;
    try {
      const pdfDoc = await PDFDocument.create();
      const page = pdfDoc.addPage([595, 842]); // A4 portrait
      const font = await pdfDoc.embedFont('Helvetica');
      const bold = await pdfDoc.embedFont('Helvetica-Bold');

      // Helpers for image fetch + embed. Detailed logging so we can
      // diagnose which item's image failed and why.
      const fetchImageBuffer = async (url) => {
        if (!url) return null;
        try {
          const r = await fetch(url, {
            signal: AbortSignal.timeout(10000),
            redirect: 'follow',
            headers: { 'User-Agent': 'Mozilla/5.0 (compatible; TuffshopBot/1.0)' },
          });
          if (!r.ok) {
            console.warn(`[promo-offer-pdf] fetch ${url} → HTTP ${r.status}`);
            return null;
          }
          const buf = Buffer.from(await r.arrayBuffer());
          const ct = r.headers.get('content-type') || 'unknown';
          console.log(`[promo-offer-pdf] fetched ${url} (${buf.length}B, content-type=${ct})`);
          return buf;
        } catch (err) {
          console.warn(`[promo-offer-pdf] fetch ${url} failed: ${err.message}`);
          return null;
        }
      };
      const embedImageGuess = async (buf, sourceUrl) => {
        if (!buf || buf.length < 4) {
          console.warn(`[promo-offer-pdf] embed: empty/short buffer for ${sourceUrl || '?'}`);
          return null;
        }
        const magic = `${buf[0].toString(16)} ${buf[1].toString(16)} ${buf[2].toString(16)} ${buf[3].toString(16)}`;
        // WebP starts "RIFF ???? WEBP" → pdf-lib can't embed natively.
        const isWebp = buf.slice(0, 4).toString('ascii') === 'RIFF' && buf.slice(8, 12).toString('ascii') === 'WEBP';
        if (isWebp) {
          console.warn(`[promo-offer-pdf] embed: WebP not supported by pdf-lib (${sourceUrl || '?'}). Use a PNG/JPG URL instead.`);
          return null;
        }
        try {
          if (buf[0] === 0x89 && buf[1] === 0x50) return await pdfDoc.embedPng(buf);
          if (buf[0] === 0xFF && buf[1] === 0xD8) return await pdfDoc.embedJpg(buf);
          try { return await pdfDoc.embedPng(buf); }
          catch (pngErr) {
            try { return await pdfDoc.embedJpg(buf); }
            catch (jpgErr) {
              console.warn(`[promo-offer-pdf] embed failed (${sourceUrl || '?'}, magic=${magic}): PNG=${pngErr.message}; JPG=${jpgErr.message}`);
              return null;
            }
          }
        } catch (err) {
          console.warn(`[promo-offer-pdf] embed ${sourceUrl || '?'}: ${err.message}`);
          return null;
        }
      };

      // Compute the (x, y) bottom-left to pass to drawImage so that
      // rotation around the bottom-left lands the image's centre at
      // (cx, cy). pdf-lib rotates around bottom-left, so we offset.
      const rotatedDrawCoords = (cx, cy, w, h, rotDeg) => {
        if (!rotDeg) return { x: cx - w / 2, y: cy - h / 2 };
        const r = (rotDeg * Math.PI) / 180;
        const cosR = Math.cos(r), sinR = Math.sin(r);
        return {
          x: cx - (w / 2) * cosR + (h / 2) * sinR,
          y: cy - (w / 2) * sinR - (h / 2) * cosR,
        };
      };

      // Pre-embed customer logos (only the variants we need).
      const variantsUsed = new Set(lineItems.map((l) => l.logoVariant || 'auto'));
      const logosByVariant = {};
      const pickLogoBuf = (v) => {
        if (v === 'dark' && sess.promo_logo_dark_data) return sess.promo_logo_dark_data;
        if (v === 'light' && sess.promo_logo_light_data) return sess.promo_logo_light_data;
        return sess.primary_logo_data || null;
      };
      for (const v of variantsUsed) {
        const buf = pickLogoBuf(v);
        if (buf) logosByVariant[v] = await embedImageGuess(Buffer.from(buf), `customer-logo-${v}`);
      }

      // Pre-fetch + embed item images (dedup by URL across line items).
      const itemImageByUrl = {};
      for (const l of lineItems) {
        if (l.imageUrl && !(l.imageUrl in itemImageByUrl)) {
          const buf = await fetchImageBuffer(l.imageUrl);
          itemImageByUrl[l.imageUrl] = buf ? await embedImageGuess(buf, l.imageUrl) : null;
        }
      }

      let y = 800;
      page.drawText('Promo Add-On Request', { x: 40, y, size: 18, font: bold });
      y -= 26;
      page.drawText(`Customer: ${sess.customer_name || '?'}`, { x: 40, y, size: 11, font });
      y -= 14;
      if (sess.order_number) { page.drawText(`Order: ${sess.order_number}`, { x: 40, y, size: 11, font }); y -= 14; }
      page.drawText(`Submitted: ${new Date().toLocaleString('en-GB', { timeZone: 'Europe/London' })}`, { x: 40, y, size: 11, font });
      y -= 22;

      // Column headers.
      page.drawText('Preview', { x: 40, y, size: 10, font: bold });
      page.drawText('Item', { x: 130, y, size: 10, font: bold });
      page.drawText('Qty', { x: 350, y, size: 10, font: bold });
      page.drawText('Deal £', { x: 410, y, size: 10, font: bold });
      page.drawText('Line £', { x: 480, y, size: 10, font: bold });
      y -= 10;
      page.drawLine({ start: { x: 40, y, }, end: { x: 555, y }, thickness: 0.5, color: rgb(0.8, 0.8, 0.8) });
      y -= 10;

      const ROW_HEIGHT = 75;
      const THUMB_SIZE = 60;
      for (const l of lineItems) {
        // y here is the TOP of the row; draw thumbnail from top-down.
        const rowTop = y;
        const rowBottom = y - ROW_HEIGHT;
        const thumbX = 40;
        const thumbY = rowBottom + (ROW_HEIGHT - THUMB_SIZE) / 2;

        // Draw item image, scaled to fit THUMB_SIZE×THUMB_SIZE.
        const itemImg = itemImageByUrl[l.imageUrl];
        let imgDims = { width: THUMB_SIZE, height: THUMB_SIZE };
        let imgDrawX = thumbX, imgDrawY = thumbY;
        if (itemImg) {
          imgDims = itemImg.scaleToFit(THUMB_SIZE, THUMB_SIZE);
          imgDrawX = thumbX + (THUMB_SIZE - imgDims.width) / 2;
          imgDrawY = thumbY + (THUMB_SIZE - imgDims.height) / 2;
          page.drawImage(itemImg, { x: imgDrawX, y: imgDrawY, width: imgDims.width, height: imgDims.height });
        } else {
          // Stub box if image couldn't be fetched.
          page.drawRectangle({ x: thumbX, y: thumbY, width: THUMB_SIZE, height: THUMB_SIZE, borderColor: rgb(0.8, 0.8, 0.8), borderWidth: 0.5 });
        }

        // Overlay customer logo at the zone (with rotation, swapped
        // dimensions for 90/270, same as the customer-facing CSS).
        const z = l.logoZone;
        const logoImg = logosByVariant[l.logoVariant || 'auto'];
        if (logoImg && z && z.width > 0 && z.height > 0) {
          // Zone in PDF coords (Y flipped vs CSS: image's visual top
          // sits at imgDrawY + imgDims.height in PDF space).
          const zoneW = (z.width / 100) * imgDims.width;
          const zoneH = (z.height / 100) * imgDims.height;
          const zoneX = imgDrawX + (z.x / 100) * imgDims.width;
          const zoneY = imgDrawY + imgDims.height - ((z.y + z.height) / 100) * imgDims.height;
          const isQuarter = z.rotation === 90 || z.rotation === 270;
          const containerW = isQuarter ? zoneH : zoneW;
          const containerH = isQuarter ? zoneW : zoneH;
          const logoFit = logoImg.scaleToFit(containerW, containerH);
          const cx = zoneX + zoneW / 2;
          const cy = zoneY + zoneH / 2;
          const { x: lx, y: ly } = rotatedDrawCoords(cx, cy, logoFit.width, logoFit.height, z.rotation || 0);
          try {
            page.drawImage(logoImg, {
              x: lx, y: ly, width: logoFit.width, height: logoFit.height,
              rotate: z.rotation ? degrees(z.rotation) : undefined,
            });
          } catch (drawErr) {
            console.warn(`[promo-offer-pdf] logo draw: ${drawErr.message}`);
          }
        }

        // Text columns aligned to thumbnail vertical centre. Prices ex-VAT.
        const textY = thumbY + THUMB_SIZE / 2 - 4;
        page.drawText(l.name.slice(0, 36), { x: 130, y: textY, size: 10, font });
        page.drawText(String(l.qty), { x: 350, y: textY, size: 10, font });
        page.drawText(l.dealPriceExVat.toFixed(2), { x: 410, y: textY, size: 10, font });
        page.drawText(l.lineTotalExVat.toFixed(2), { x: 480, y: textY, size: 10, font });

        y -= ROW_HEIGHT;
        if (y < 100) break; // ran out of page; could add multi-page later if needed
      }

      // Totals block — subtotal, optional bundle discount, final + VAT.
      y -= 6;
      page.drawLine({ start: { x: 350, y }, end: { x: 555, y }, thickness: 0.5, color: rgb(0.8, 0.8, 0.8) });
      y -= 14;
      page.drawText('Subtotal (ex VAT)', { x: 350, y, size: 10, font });
      page.drawText(`£${subtotalExVat.toFixed(2)}`, { x: 480, y, size: 10, font });
      if (bundleDiscountPct > 0) {
        y -= 14;
        page.drawText(`Bundle discount (${Math.round(bundleDiscountPct * 100)}%)`, { x: 350, y, size: 10, font, color: rgb(0.28, 0.77, 0.29) });
        page.drawText(`-£${bundleDiscount.toFixed(2)}`, { x: 480, y, size: 10, font, color: rgb(0.28, 0.77, 0.29) });
      }
      y -= 16;
      page.drawLine({ start: { x: 350, y: y + 6 }, end: { x: 555, y: y + 6 }, thickness: 0.5, color: rgb(0.8, 0.8, 0.8) });
      page.drawText('Total + VAT', { x: 380, y, size: 12, font: bold });
      page.drawText(`£${orderTotalExVat.toFixed(2)}`, { x: 480, y, size: 12, font: bold });

      pdfBuffer = Buffer.from(await pdfDoc.save());
    } catch (pdfErr) {
      console.warn('[promo-offer] PDF build failed:', pdfErr.message);
    }

    // In preview mode the operator can redirect this test email to a chosen
    // address (the "send test to" box on a ?preview=1 link); otherwise it
    // always goes to the configured notify inbox. The override is ignored
    // for real customer submissions so production always notifies sales.
    const emailLooksValid = (e) => typeof e === 'string' && /^[^@\s]+@[^@\s]+\.[^@\s]+$/.test(e.trim());
    const promoEmailTo = (previewBypass && emailLooksValid(testEmail))
      ? testEmail.trim()
      : (process.env.PROMO_OFFER_NOTIFY_EMAIL || 'sales@tuffshop.co.uk');

    // Send email. Best-effort: failure doesn't block the customer's response.
    if (process.env.SMTP_PASS) {
      try {
        const transporter = nodemailer.createTransport({
          host: process.env.SMTP_SERVER || 'mail-eu.smtp2go.com',
          port: parseInt(process.env.SMTP_PORT || '2525'),
          secure: false,
          auth: { user: process.env.SMTP_USERNAME || 'tuffshop.co.uk', pass: process.env.SMTP_PASS },
        });
        const filename = `Promo-AddOn-${sess.order_number || 'order'}.pdf`;
        await transporter.sendMail({
          from: `"Tuffshop Proofs" <${process.env.SENDER_EMAIL || 'noreply@tuffshop.co.uk'}>`,
          to: promoEmailTo,
          subject,
          html,
          text: textBody,
          attachments: pdfBuffer ? [{ filename, content: pdfBuffer, contentType: 'application/pdf' }] : [],
        });
        console.log(`[promo-offer] uptake email sent for ${sess.order_number || approvalSessionId} → ${promoEmailTo}`);
      } catch (mailErr) {
        console.error('[promo-offer] email send failed:', mailErr.message);
      }
    }

    // Attach the same PDF to the BP order if we have one. Skipped in
    // preview mode so test submissions don't pollute real BP orders.
    if (pdfBuffer && sess.order_number && !previewBypass) {
      try {
        const filename = `Promo-AddOn-${sess.order_number}.pdf`;
        const r = await bpAttachFileToOrder(sess.order_number, filename, pdfBuffer, 'application/pdf');
        if (r.success) console.log(`[promo-offer] PDF attached to BP order ${sess.order_number}`);
        else console.warn(`[promo-offer] BP attach failed:`, r);
      } catch (attachErr) {
        console.warn('[promo-offer] BP attach error:', attachErr.message);
      }
    }

    // "Pay Now Online" link — OFF for real customers until PROMO_PAYMENT_ENABLED=true,
    // but always built for preview submits so the team can test the journey.
    // Real submits only offer it if a promo item actually got added to the order.
    // PROMO_PAYMENT_TEST_ORDERS: comma-separated order numbers that get the live
    // link even while the global flag is off — for testing one real order at a time.
    let paymentUrl = null;
    const paymentEnabled = String(process.env.PROMO_PAYMENT_ENABLED ?? 'false').toLowerCase() === 'true';
    const testOrders = String(process.env.PROMO_PAYMENT_TEST_ORDERS ?? '')
      .split(',')
      .map((s) => s.trim())
      .filter(Boolean);
    const isTestOrder = sess.order_number && testOrders.includes(String(sess.order_number));
    if (sess.order_number && (paymentEnabled || isTestOrder || previewBypass)) {
      const anyAdded = previewBypass ? true : bpRowResults.some((r) => r.ok);
      if (anyAdded) paymentUrl = await buildOrderPaymentLink(sess.order_number);
    }

    res.json({ success: true, total: orderTotal, lineCount: lineItems.length, emailedTo: process.env.SMTP_PASS ? promoEmailTo : null, paymentUrl });
  } catch (err) {
    console.error('[promo-offer] submit failed:', err.message);
    if (!res.headersSent) res.status(500).json({ error: err.message });
  }
});

// GET /api/promo-offer/uptake — report of real (non-preview) submissions.
// Returns per-item totals (units + value at deal price ex VAT, before any
// bundle discount) and the most recent submissions, for the admin view.
app.get('/api/promo-offer/uptake', async (req, res) => {
  if (!pool) return res.status(503).json({ error: 'Database not configured' });
  try {
    const r = await pool.query(
      `SELECT id, approval_session_id, order_number, customer_name, items, submitted_at
         FROM promo_offer_uptake
        ORDER BY submitted_at DESC`
    );

    // Aggregate per item across every submission. `items` is the JSONB
    // array of line items recorded at submit time.
    const byItem = new Map(); // name -> { itemId, name, units, valueExVat, orders }
    let totalUnits = 0;
    let totalValueExVat = 0;
    for (const row of r.rows) {
      const lines = Array.isArray(row.items) ? row.items : [];
      for (const l of lines) {
        const key = l.name || `#${l.itemId}`;
        const qty = Number(l.qty) || 0;
        const lineValue = Number(l.lineTotalExVat) || 0;
        const agg = byItem.get(key) || { itemId: l.itemId ?? null, name: key, units: 0, valueExVat: 0, orders: 0 };
        agg.units += qty;
        agg.valueExVat += lineValue;
        agg.orders += 1;
        byItem.set(key, agg);
        totalUnits += qty;
        totalValueExVat += lineValue;
      }
    }

    const totals = Array.from(byItem.values()).sort((a, b) => b.units - a.units);
    const recent = r.rows.slice(0, 50).map((row) => ({
      id: row.id,
      sessionId: row.approval_session_id,
      orderNumber: row.order_number,
      customerName: row.customer_name,
      submittedAt: row.submitted_at,
      items: (Array.isArray(row.items) ? row.items : []).map((l) => ({
        name: l.name, qty: Number(l.qty) || 0, lineTotalExVat: Number(l.lineTotalExVat) || 0,
      })),
    }));

    res.json({
      submissionCount: r.rows.length,
      totalUnits,
      totalValueExVat,
      totals,
      recent,
    });
  } catch (err) {
    console.error('[promo-offer] uptake report failed:', err.message);
    res.status(500).json({ error: err.message });
  }
});

// GET /api/promo-offer/send-log?days=14 — for each proof sent in the window,
// whether it carried the promo offer (offer enabled) or not (operator ticked
// "disable promo offer"), plus whether the customer actually bought (uptake).
// Lets the operator see the reach/hit rate of the up-sell.
app.get('/api/promo-offer/send-log', async (req, res) => {
  if (!pool) return res.status(503).json({ error: 'Database not configured' });
  try {
    const days = Math.min(Math.max(parseInt(req.query.days, 10) || 14, 1), 90);
    const r = await pool.query(
      `SELECT s.id, s.order_number, s.customer_name, s.created_at, s.status,
              s.promo_offer_disabled,
              EXISTS (SELECT 1 FROM promo_offer_uptake u WHERE u.approval_session_id = s.id) AS converted
         FROM approval_sessions s
        WHERE s.created_at > NOW() - ($1 || ' days')::interval
        ORDER BY s.created_at DESC`,
      [String(days)]
    );
    const rows = r.rows;
    const total = rows.length;
    const sent = rows.filter((x) => !x.promo_offer_disabled).length;
    const converted = rows.filter((x) => x.converted).length;
    const approved = rows.filter((x) => x.status === 'approved').length;
    const pct = (n, d) => (d ? Math.round((n / d) * 1000) / 10 : 0);
    res.json({
      days,
      generatedAt: new Date().toISOString(),
      summary: {
        total,
        sent,
        notSent: total - sent,
        sentRatePct: pct(sent, total),     // % of proofs that carried the offer
        approved,
        converted,                          // sent AND customer bought a promo item
        convertRateOfSentPct: pct(converted, sent),
      },
      jobs: rows.map((x) => ({
        sessionId: x.id,
        orderNumber: x.order_number,
        customerName: x.customer_name,
        createdAt: x.created_at,
        status: x.status,
        sent: !x.promo_offer_disabled,
        converted: !!x.converted,
      })),
    });
  } catch (err) {
    console.error('[promo-offer] send-log failed:', err.message);
    res.status(500).json({ error: err.message });
  }
});

// DELETE /api/promo-offer/uptake/:id — remove a single uptake row (e.g. a
// test submission added during setup). Internal admin tool; no undo.
app.delete('/api/promo-offer/uptake/:id', async (req, res) => {
  if (!pool) return res.status(503).json({ error: 'Database not configured' });
  const id = parseInt(req.params.id, 10);
  if (!Number.isInteger(id)) return res.status(400).json({ error: 'invalid id' });
  try {
    const r = await pool.query('DELETE FROM promo_offer_uptake WHERE id = $1', [id]);
    res.json({ success: true, deleted: r.rowCount });
  } catch (err) {
    console.error('[promo-offer] uptake delete failed:', err.message);
    res.status(500).json({ error: err.message });
  }
});

// ============================================================
// EPS → PNG conversion (skip the manual Illustrator step)
// ============================================================
// POST /api/convert-eps with { dataBase64 } returns { pngBase64, bytes }.
// Pipes the EPS into Ghostscript via stdin, captures the PNG from stdout.
// Requires `gs` on PATH on the backend host — Render's Node images do
// include it. If we ever see "ENOENT" / "Ghostscript not installed",
// the deploy environment changed.
// Ghostscript can spike memory on rasterisation. On small Render
// instances, 4+ concurrent gs processes will OOM/stall and produce
// the "fetches never complete" symptom we saw. Single-slot queue:
// requests wait their turn instead of fighting for resources.
let epsConvertBusy = false;
const epsConvertWaiters = [];
async function acquireEpsSlot() {
  if (!epsConvertBusy) { epsConvertBusy = true; return; }
  return new Promise((resolve) => epsConvertWaiters.push(resolve));
}
function releaseEpsSlot() {
  const next = epsConvertWaiters.shift();
  if (next) next();
  else epsConvertBusy = false;
}

app.post('/api/convert-eps', async (req, res) => {
  const reqId = Math.random().toString(36).slice(2, 8);
  try {
    const { dataBase64, dpi } = req.body || {};
    if (!dataBase64) return res.status(400).json({ error: 'dataBase64 required' });
    const epsBuffer = Buffer.from(dataBase64, 'base64');
    if (epsBuffer.length === 0) return res.status(400).json({ error: 'empty file' });
    const resolution = Math.min(Math.max(parseInt(dpi, 10) || 300, 72), 600);

    console.log(`[convert-eps:${reqId}] received, eps=${epsBuffer.length}B, dpi=${resolution}, queue=${epsConvertWaiters.length}`);
    const acquireStart = Date.now();
    await acquireEpsSlot();
    const waitedMs = Date.now() - acquireStart;
    if (waitedMs > 50) console.log(`[convert-eps:${reqId}] waited ${waitedMs}ms in queue`);

    // If the client gave up while we were queued, no point starting gs.
    if (res.writableEnded || res.destroyed) {
      console.warn(`[convert-eps:${reqId}] client gone before slot, skipping`);
      releaseEpsSlot();
      return;
    }

    const gs = spawn('gs', [
      '-dNOPAUSE',
      '-dBATCH',
      '-dQUIET',
      '-dEPSCrop',           // crop to bounding box, no whitespace
      '-sDEVICE=pngalpha',   // transparent PNG output
      `-r${resolution}`,
      '-sOutputFile=-',      // stdout
      '-',                    // stdin
    ]);

    const chunks = [];
    const errChunks = [];
    gs.stdout.on('data', (c) => chunks.push(c));
    gs.stderr.on('data', (c) => errChunks.push(c));
    // Drain gs.stdout/stderr to /dev/null even after we've decided to
    // ignore them, otherwise an unconsumed pipe can backpressure gs.
    gs.stdout.on('error', (err) => console.warn('[convert-eps] stdout error:', err.message));
    gs.stderr.on('error', (err) => console.warn('[convert-eps] stderr error:', err.message));

    let responded = false;
    const startedAt = Date.now();
    // Guarded response write: if the client has already disconnected
    // (writableEnded / destroyed), res.json() throws EPIPE — which
    // bubbles up as an uncaught socket error and can crash the Node
    // process. Skip the write in that case.
    const finish = (status, payload) => {
      if (responded) return;
      responded = true;
      clearTimeout(timeoutHandle);
      try { if (!gs.killed) gs.kill('SIGKILL'); } catch {}
      releaseEpsSlot();
      const tookMs = Date.now() - startedAt;
      console.log(`[convert-eps:${reqId}] done in ${tookMs}ms, status=${status}`);
      if (res.writableEnded || res.destroyed) {
        console.warn(`[convert-eps:${reqId}] client gone, skipping response write`);
        return;
      }
      try {
        res.status(status).json(payload);
      } catch (err) {
        console.warn(`[convert-eps:${reqId}] response write failed:`, err.message);
      }
    };

    // Hard timeout. EPS rendering rarely takes more than a few seconds;
    // anything longer almost certainly means a malformed file is making
    // gs hang waiting for input.
    const TIMEOUT_MS = 30000;
    const timeoutHandle = setTimeout(() => {
      console.warn(`[convert-eps] timeout after ${TIMEOUT_MS}ms — killing gs`);
      finish(504, { error: `Ghostscript timed out after ${TIMEOUT_MS}ms — EPS may be malformed.` });
    }, TIMEOUT_MS);

    // Client disconnected before we finished (closed tab, navigated
    // away, fetch aborted). Kill gs so we don't waste CPU and mark the
    // request as already-handled so finish() won't try to write to a
    // dead socket — that's what was producing the EPIPE Socket errors
    // in the Render logs.
    req.on('close', () => {
      if (!responded) {
        console.warn('[convert-eps] client disconnected mid-request — aborting gs');
        responded = true;
        clearTimeout(timeoutHandle);
        try { if (!gs.killed) gs.kill('SIGKILL'); } catch {}
      }
    });

    res.on('error', (err) => {
      console.warn('[convert-eps] response stream error:', err.message);
    });

    gs.on('error', (err) => {
      if (err.code === 'ENOENT') {
        return finish(503, { error: 'Ghostscript not installed on backend (gs binary missing).' });
      }
      return finish(500, { error: err.message });
    });

    gs.stdin.on('error', (err) => {
      // EPIPE here means gs exited before we finished writing — close
      // handler will fire shortly with the real exit code, so just log.
      console.warn('[convert-eps] stdin error:', err.message);
    });

    gs.on('close', (code) => {
      if (code !== 0) {
        const stderr = Buffer.concat(errChunks).toString().slice(0, 500);
        return finish(500, { error: `Ghostscript exit ${code}: ${stderr || '(no stderr)'}` });
      }
      const png = Buffer.concat(chunks);
      if (png.length === 0) {
        return finish(500, { error: 'Ghostscript produced empty output' });
      }
      return finish(200, { success: true, pngBase64: png.toString('base64'), bytes: png.length });
    });

    gs.stdin.write(epsBuffer, (writeErr) => {
      if (writeErr) console.warn('[convert-eps] stdin write callback error:', writeErr.message);
    });
    gs.stdin.end();
  } catch (err) {
    console.error(`[convert-eps:${reqId}] handler error:`, err.message);
    // Best-effort slot release in case we crashed after acquiring.
    if (epsConvertBusy) {
      try { releaseEpsSlot(); } catch {}
    }
    if (!res.headersSent) {
      try { res.status(500).json({ error: err.message }); } catch {}
    }
  }
});

// ============================================================
// BP Order Attachments — staging + push to Brightpearl
// ============================================================
// Operator uploads scanned thread-colour sheet and embroidery digitised
// file during mockup creation; both stage here until the "Attach to BP"
// button fires. On successful BP upload, rows are deleted (this table is
// temporary staging, not an archive — the signed PDF in approval_sessions
// is the audit copy).

const VALID_ATTACHMENT_KINDS = ['scanned_sheet', 'embroidery_file'];

// Stamp the customer's signature + name + IP onto every product page of
// a proof PDF, mirroring /api/approval-sessions/:id/download. Returns
// the stamped bytes, or the original on any stamp failure. No stamp
// applied if there's nothing to stamp (returns original unchanged).
async function stampSignatureOntoPdf(pdfBytes, { approverName, signatureData, submitterIp, logoPositions }) {
  if (!approverName && !signatureData) return pdfBytes;
  try {
    const pdfDoc = await PDFDocument.load(pdfBytes);
    const pages = pdfDoc.getPages();
    const positions = typeof logoPositions === 'string' ? JSON.parse(logoPositions) : logoPositions;

    let sigImage = null;
    if (signatureData) {
      try {
        const sigBase64 = signatureData.replace(/^data:image\/png;base64,/, '');
        const sigBytes = Buffer.from(sigBase64, 'base64');
        sigImage = await pdfDoc.embedPng(sigBytes);
      } catch {}
    }

    const stampW = 150;
    const stampH = 50;
    const stampX = 179 + 373 - stampW;
    const stampY = 296;
    const borderW = 2;
    const yellow = rgb(0.95, 0.82, 0.08);
    const black = rgb(0, 0, 0);

    const stampedPages = new Set();
    for (const pos of (positions || [])) {
      const pageIdx = (pos.page || 1) - 1;
      if (pageIdx >= pages.length || stampedPages.has(pageIdx)) continue;
      stampedPages.add(pageIdx);
      const page = pages[pageIdx];

      page.drawRectangle({
        x: stampX, y: stampY, width: stampW, height: stampH,
        borderColor: yellow, borderWidth: borderW,
        color: rgb(1, 1, 1), opacity: 0.25,
      });

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

      if (approverName) {
        page.drawText(approverName, { x: stampX + 4, y: stampY + 4, size: 6, color: black });
      }
      if (submitterIp) {
        const ipText = submitterIp;
        const ipX = stampX + stampW - 4 - (ipText.length * 3.2);
        page.drawText(ipText, { x: ipX, y: stampY + 4, size: 6, color: black });
      }
    }

    return Buffer.from(await pdfDoc.save());
  } catch (err) {
    console.error('[stamp] signature overlay failed, returning unsigned PDF:', err.message);
    return pdfBytes;
  }
}

// Stage a single supplementary file for a BP order.
// Body: { bpOrderId, kind, filename, mimeType, dataBase64 }
app.post('/api/bp-order-attachments', async (req, res) => {
  if (!pool) return res.status(503).json({ error: 'Database not configured' });
  try {
    const { bpOrderId, kind, filename, mimeType, dataBase64 } = req.body;
    if (!bpOrderId || !kind || !filename || !dataBase64) {
      return res.status(400).json({ error: 'bpOrderId, kind, filename, dataBase64 are required' });
    }
    if (!VALID_ATTACHMENT_KINDS.includes(kind)) {
      return res.status(400).json({ error: `kind must be one of ${VALID_ATTACHMENT_KINDS.join(', ')}` });
    }
    const buf = Buffer.from(dataBase64, 'base64');
    // All kinds (scanned_sheet, embroidery_file) dedup by filename —
    // same name at multiple positions = one row = one BP upload;
    // different filenames coexist as separate rows.
    await pool.query(
      `INSERT INTO bp_order_attachments (bp_order_id, kind, filename, mime_type, data)
       VALUES ($1, $2, $3, $4, $5)
       ON CONFLICT (bp_order_id, kind, filename)
       DO UPDATE SET mime_type = EXCLUDED.mime_type,
                     data = EXCLUDED.data, uploaded_at = NOW()`,
      [String(bpOrderId), kind, filename, mimeType || null, buf]
    );
    res.json({ success: true, bytes: buf.length });
  } catch (err) {
    console.error('[bp-attachments] upload failed:', err.message);
    res.status(500).json({ error: err.message });
  }
});

// List staged attachments for an order (metadata only — no file bytes).
app.get('/api/bp-order-attachments/:bpOrderId', async (req, res) => {
  if (!pool) return res.status(503).json({ error: 'Database not configured' });
  try {
    const r = await pool.query(
      `SELECT id, kind, filename, mime_type, octet_length(data) AS size_bytes, uploaded_at
         FROM bp_order_attachments WHERE bp_order_id = $1 ORDER BY uploaded_at`,
      [String(req.params.bpOrderId)]
    );
    res.json({ attachments: r.rows });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// Delete a single staged attachment (e.g. operator re-uploads).
app.delete('/api/bp-order-attachments/:id', async (req, res) => {
  if (!pool) return res.status(503).json({ error: 'Database not configured' });
  try {
    const r = await pool.query(`DELETE FROM bp_order_attachments WHERE id = $1`, [req.params.id]);
    res.json({ success: true, deleted: r.rowCount });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// Push everything to Brightpearl.
// Body: { bpOrderId, approvalSessionId }
// Fetches signed PDF from approval_sessions + all staged attachments from
// bp_order_attachments, uploads each to BP via the legacy web endpoint,
// and on full success deletes the staged rows. Returns per-file status.
app.post('/api/brightpearl/attach-files', async (req, res) => {
  if (!pool) return res.status(503).json({ error: 'Database not configured' });
  try {
    const { bpOrderId, approvalSessionId } = req.body;
    if (!bpOrderId) return res.status(400).json({ error: 'bpOrderId required' });

    const files = [];

    // 1. Proof PDF from approval_sessions (optional — not every attach
    //    is post-approval; we accept the call without it). If the
    //    customer has signed (approver_name or signature_data present),
    //    overlay the signature stamp before attaching so production gets
    //    the signed-off version. Otherwise the raw unsigned proof goes up.
    if (approvalSessionId) {
      const sessRes = await pool.query(
        `SELECT pdf_data, order_number, customer_name, status,
                approver_name, signature_data, submitter_ip, logo_positions
           FROM approval_sessions WHERE id = $1`,
        [approvalSessionId]
      );
      if (sessRes.rowCount === 0) {
        return res.status(404).json({ error: 'approval session not found' });
      }
      const sess = sessRes.rows[0];
      if (sess.pdf_data) {
        const orderRef = sess.order_number || bpOrderId;
        const isSigned = !!(sess.approver_name || sess.signature_data);
        const pdfBuffer = await stampSignatureOntoPdf(sess.pdf_data, {
          approverName: sess.approver_name,
          signatureData: sess.signature_data,
          submitterIp: sess.submitter_ip,
          logoPositions: sess.logo_positions,
        });
        files.push({
          source: 'approval_session',
          kind: isSigned ? 'signed_pdf' : 'unsigned_proof',
          filename: `${orderRef}-${isSigned ? 'Signed-Proof' : 'Proof'}.pdf`,
          mimeType: 'application/pdf',
          buffer: pdfBuffer,
        });
      }
    }

    // 2. Staged attachments from bp_order_attachments.
    const stagedRes = await pool.query(
      `SELECT id, kind, filename, mime_type, data
         FROM bp_order_attachments WHERE bp_order_id = $1 ORDER BY uploaded_at`,
      [String(bpOrderId)]
    );
    for (const row of stagedRes.rows) {
      files.push({
        source: 'staged',
        stagedId: row.id,
        kind: row.kind,
        filename: row.filename,
        mimeType: row.mime_type || 'application/octet-stream',
        buffer: row.data,
      });
    }

    if (files.length === 0) {
      return res.status(400).json({ error: 'no files to attach (no signed PDF and no staged attachments)' });
    }

    // 3. Upload each to BP. Continue past individual failures so partial
    //    success is reported back rather than aborting the whole batch.
    const results = [];
    for (const f of files) {
      try {
        const r = await bpAttachFileToOrder(bpOrderId, f.filename, f.buffer, f.mimeType);
        results.push({ ...stripBuffer(f), ...r });
      } catch (err) {
        console.error(`[bp-attach] ${f.filename} failed:`, err.message);
        results.push({ ...stripBuffer(f), success: false, error: err.message });
      }
    }

    // 4. Clean up staged rows that uploaded successfully. Failures stay
    //    in place so the operator can retry.
    const successfulStagedIds = results
      .filter((r) => r.source === 'staged' && r.success)
      .map((r) => r.stagedId);
    if (successfulStagedIds.length) {
      await pool.query(
        `DELETE FROM bp_order_attachments WHERE id = ANY($1::int[])`,
        [successfulStagedIds]
      );
    }

    const allOk = results.every((r) => r.success);
    res.json({ success: allOk, results });
  } catch (err) {
    console.error('[bp-attach] orchestrator failed:', err.message);
    res.status(500).json({ error: err.message });
  }
});

function stripBuffer({ buffer, ...rest }) {
  return rest;
}

// ============================================================
// Purchasing automation (Phase A) — build supplier POs from Brightpearl demand.
// Runs against the TEST account (env BP_TEST_ACCOUNT/APP_REF/TOKEN). See
// purchasingAuto.js. Sequencing: create-po builds a "Pending PO"; finalize
// (post supplier-portal placement) strips the tag + flips status.
// ============================================================
function requirePurchasing(res) {
  if (!purchasingAuto.isConfigured()) {
    res.status(503).json({ error: 'Purchasing automation not configured (set BP_TEST_APP_REF and BP_TEST_TOKEN)' });
    return false;
  }
  return true;
}
const parseOrderIds = (v) => (v ? String(v).split(',').map((s) => parseInt(s.trim(), 10)).filter(Boolean) : null);

app.get('/api/purchasing/suppliers', (req, res) => {
  if (!requirePurchasing(res)) return;
  res.json({ suppliers: Object.keys(purchasingAuto.SUPPLIERS) });
});

// Read-only: what would go on this supplier's PO right now.
app.get('/api/purchasing/preview', async (req, res) => {
  if (!requirePurchasing(res)) return;
  try {
    res.json(await purchasingAuto.preview(req.query.supplier, parseOrderIds(req.query.orderIds)));
  } catch (e) { res.status(400).json({ error: e.message }); }
});

// Create the Pending PO (+ source note, + stamp PO number on each order).
// Pass { dryRun:true } to preview via POST, or { orderIds:[...] } to scope.
app.post('/api/purchasing/create-po', async (req, res) => {
  if (!requirePurchasing(res)) return;
  try {
    const { supplier, orderIds, dryRun } = req.body || {};
    res.json(await purchasingAuto.createPO(supplier, { orderIds, dryRun }));
  } catch (e) { res.status(400).json({ error: e.message }); }
});

// Post-placement: strip the supplier tag, flip status when it was the last
// supplier, add "ordered via PO N" notes. { supplier, poId, orderIds:[...] }.
app.post('/api/purchasing/finalize', async (req, res) => {
  if (!requirePurchasing(res)) return;
  try {
    const { supplier, poId, orderIds } = req.body || {};
    res.json(await purchasingAuto.finalizePO(supplier, { poId, orderIds }));
  } catch (e) { res.status(400).json({ error: e.message }); }
});

// Prepare a supplier order: take the demand, check LIVE supplier stock per line,
// import the in-stock qty into the supplier basket, and email the out-of-stock
// shortfall (for back-order / alternative). Leaves the basket for human review;
// does NOT place the order. Stock + basket run on the Alternate-Items service.
const ALT_ITEMS_URL = process.env.ALT_ITEMS_URL || 'https://alternate-items.onrender.com';
const OOS_EMAIL_TO = process.env.PURCHASING_OOS_EMAIL || 'dec@tuffshop.co.uk';

async function sendOutOfStockEmail(supplier, lines) {
  const rows = lines.map((l) => `<tr>
    <td style="padding:6px;border:1px solid #ddd">${l.orderId}</td>
    <td style="padding:6px;border:1px solid #ddd">${l.ref || ''}</td>
    <td style="padding:6px;border:1px solid #ddd;font-family:monospace">${l.sku}</td>
    <td style="padding:6px;border:1px solid #ddd">${(l.name || '').slice(0, 60)}</td>
    <td style="padding:6px;border:1px solid #ddd;text-align:right">${l.qty}</td>
    <td style="padding:6px;border:1px solid #ddd">${(l.stock && l.stock.status) || 'Unknown'}</td>
    <td style="padding:6px;border:1px solid #ddd">${(l.stock && l.stock.deldate) || ''}</td>
  </tr>`).join('');
  const html = `<div style="font-family:Arial,sans-serif;max-width:900px;color:#333">
    <h2 style="color:#cc0000">${supplier} — ${lines.length} line${lines.length === 1 ? '' : 's'} not available to order now</h2>
    <p>These lines were <strong>not</strong> added to the ${supplier} basket — they're out of stock or low/restocking. They need a back-order or an alternative.</p>
    <table style="border-collapse:collapse;font-size:13px">
      <tr style="background:#f2f2f2">
        <th style="padding:6px;border:1px solid #ddd;text-align:left">Order</th>
        <th style="padding:6px;border:1px solid #ddd;text-align:left">Reference</th>
        <th style="padding:6px;border:1px solid #ddd;text-align:left">SKU</th>
        <th style="padding:6px;border:1px solid #ddd;text-align:left">Item</th>
        <th style="padding:6px;border:1px solid #ddd;text-align:right">Qty</th>
        <th style="padding:6px;border:1px solid #ddd;text-align:left">Status</th>
        <th style="padding:6px;border:1px solid #ddd;text-align:left">Restock</th>
      </tr>${rows}</table>
    <p style="font-size:11px;color:#888;margin-top:16px">Automated by the purchasing flow • live stock from the supplier portal.</p>
  </div>`;
  const smtpHost = process.env.SMTP_SERVER || 'mail-eu.smtp2go.com';
  const ports = [...new Set([parseInt(process.env.SMTP_PORT || '2525', 10), 2525, 587, 8025])];
  let lastErr;
  for (const port of ports) {
    try {
      const t = nodemailer.createTransport({ host: smtpHost, port, secure: false, auth: { user: process.env.SMTP_USERNAME || 'tuffshop.co.uk', pass: process.env.SMTP_PASS }, connectionTimeout: 15000, greetingTimeout: 15000, socketTimeout: 20000 });
      await t.sendMail({ from: `"Tuffshop Purchasing" <${OOS_EMAIL_TO}>`, to: OOS_EMAIL_TO, subject: `${supplier} order — ${lines.length} line(s) not available now`, html });
      return true;
    } catch (e) { lastErr = e; }
  }
  throw lastErr;
}

app.post('/api/purchasing/prepare-supplier-order', async (req, res) => {
  if (!requirePurchasing(res)) return;
  try {
    const { supplier, orderIds, dryRun, simulateOosSkus } = req.body || {};
    const simOos = new Set((simulateOosSkus || []).map((x) => String(x))); // test hook: force these SKUs OOS
    const plan = await purchasingAuto.preview(supplier, parseOrderIds(orderIds));
    const lines = [];
    for (const o of plan.orders) for (const l of o.lines) lines.push({ orderId: o.orderId, ref: o.ref, sku: l.sku, name: l.name, qty: l.qty });

    // live stock per unique SKU
    const stockBySku = {};
    for (const l of lines) {
      if (!(l.sku in stockBySku)) {
        try { const r = await fetch(`${ALT_ITEMS_URL}/api/supplier-stock?sku=${encodeURIComponent(l.sku)}`); stockBySku[l.sku] = await r.json(); }
        catch (e) { stockBySku[l.sku] = { found: false, reason: e.message }; }
      }
      l.stock = simOos.has(String(l.sku)) ? { found: true, status: 'Out of stock', simulated: true } : stockBySku[l.sku];
    }
    // Only cleanly "In stock" lines are orderable. "Low stock" (with a future
    // restock date) and "Out of stock" read as unavailable on the portal, so
    // they go to the shortfall (email / back-order) — don't import them.
    const isAvailable = (l) => l.stock && /^\s*in stock\s*$/i.test(l.stock.status || '');
    const inStock = lines.filter((l) => isAvailable(l));
    const outOfStock = lines.filter((l) => !isAvailable(l));

    // aggregate in-stock lines by SKU for the basket import
    const agg = {};
    for (const l of inStock) agg[l.sku] = (agg[l.sku] || 0) + l.qty;
    const importLines = Object.entries(agg).map(([stockCode, qty]) => ({ stockCode, qty }));

    let basket = null, emailed = false;
    if (!dryRun) {
      if (importLines.length) {
        const r = await fetch(`${ALT_ITEMS_URL}/api/basket-import`, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ lines: importLines }) });
        basket = await r.json();
      }
      if (outOfStock.length) { await sendOutOfStockEmail(supplier, outOfStock); emailed = true; }
    }

    res.json({
      supplier: plan.supplier, dryRun: !!dryRun,
      totalLines: lines.length, inStockLines: inStock.length, outOfStockLines: outOfStock.length,
      importedSkus: importLines.length, basket, emailed,
      outOfStock: outOfStock.map((l) => ({ orderId: l.orderId, ref: l.ref, sku: l.sku, name: l.name, qty: l.qty, status: l.stock && l.stock.status })),
    });
  } catch (e) { res.status(400).json({ error: e.message }); }
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
      -- Captured at proof-send time so the customer-facing promo panel
      -- can overlay their actual logo onto promo item images.
      ALTER TABLE approval_sessions ADD COLUMN IF NOT EXISTS primary_logo_data BYTEA;
      ALTER TABLE approval_sessions ADD COLUMN IF NOT EXISTS primary_logo_mime VARCHAR(100);
      -- Dedicated dark + light promo logos uploaded by the operator.
      -- These take precedence over primary_logo_data on the promo panel.
      -- Operator picks the right variant per item type (dark for mugs/
      -- pens/coasters, light for notepads etc.).
      ALTER TABLE approval_sessions ADD COLUMN IF NOT EXISTS promo_logo_dark_data BYTEA;
      ALTER TABLE approval_sessions ADD COLUMN IF NOT EXISTS promo_logo_dark_mime VARCHAR(100);
      ALTER TABLE approval_sessions ADD COLUMN IF NOT EXISTS promo_logo_light_data BYTEA;
      ALTER TABLE approval_sessions ADD COLUMN IF NOT EXISTS promo_logo_light_mime VARCHAR(100);
      -- Tracks operator resends so the Approval History timeline shows
      -- "Resent on email/whatsapp at <date>" alongside Sent / Opened.
      -- Each entry: { channel, recipient, sentBy, ts }
      ALTER TABLE approval_sessions ADD COLUMN IF NOT EXISTS resend_log JSONB NOT NULL DEFAULT '[]'::jsonb;
      -- When true, the customer-facing promo item up-sell panel is
      -- suppressed on this session's approval page. Set per-link by the
      -- operator at proof-send time.
      ALTER TABLE approval_sessions ADD COLUMN IF NOT EXISTS promo_offer_disabled BOOLEAN NOT NULL DEFAULT FALSE;
      -- WhatsApp cross-sell candidate computed at proof-send (matched rule +
      -- companion image assembled in the ORDERED colour + logo zone/variant).
      ALTER TABLE approval_sessions ADD COLUMN IF NOT EXISTS crosssell_candidate JSONB;
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
    const {
      pdfBase64, logoPositions, orderNumber, customerName, recipientName, createdBy,
      primaryLogoBase64, primaryLogoMime,
      promoLogoDarkBase64, promoLogoDarkMime,
      promoLogoLightBase64, promoLogoLightMime,
      disablePromoOffer, crossSellCandidate,
    } = req.body;

    if (!pdfBase64 || !logoPositions?.length) {
      return res.status(400).json({ error: "pdfBase64 and logoPositions are required" });
    }

    const pdfBuffer = Buffer.from(pdfBase64, "base64");
    const primaryLogoBuffer = primaryLogoBase64 ? Buffer.from(primaryLogoBase64, "base64") : null;
    const promoDarkBuffer = promoLogoDarkBase64 ? Buffer.from(promoLogoDarkBase64, "base64") : null;
    const promoLightBuffer = promoLogoLightBase64 ? Buffer.from(promoLogoLightBase64, "base64") : null;

    const sessionResult = await pool.query(
      `INSERT INTO approval_sessions
         (order_number, customer_name, recipient_name, pdf_data, logo_positions, created_by,
          primary_logo_data, primary_logo_mime,
          promo_logo_dark_data, promo_logo_dark_mime,
          promo_logo_light_data, promo_logo_light_mime,
          promo_offer_disabled, crosssell_candidate)
       VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)
       RETURNING id, status, created_at`,
      [
        orderNumber || null, customerName || null, recipientName || null, pdfBuffer,
        JSON.stringify(logoPositions), createdBy || null,
        primaryLogoBuffer, primaryLogoMime || null,
        promoDarkBuffer, promoLogoDarkMime || null,
        promoLightBuffer, promoLogoLightMime || null,
        disablePromoOffer === true,
        crossSellCandidate ? JSON.stringify(crossSellCandidate) : null,
      ]
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

// Normalise a UK-centric phone string to WhatsApp's E.164-without-plus format.
// Accepts "07911 123456", "+44 7911 123456", "0044...", "447911123456",
// and bare mobiles missing the leading 0 like "7911123456".
function normaliseWhatsAppNumber(raw) {
  if (!raw) return null;
  let digits = String(raw).replace(/[^\d+]/g, "");
  if (digits.startsWith("+")) digits = digits.slice(1);
  else if (digits.startsWith("00")) digits = digits.slice(2);
  // Bare UK national number (leading 0) -> prepend country code 44.
  if (digits.startsWith("0")) digits = "44" + digits.slice(1);
  // UK mobile entered without the leading 0 (10 digits starting 7,
  // e.g. "7892872364") -> prepend country code. WhatsApp will accept a
  // malformed number and return a messageId, then silently fail to
  // deliver, so we must fix this before sending.
  else if (/^7\d{9}$/.test(digits)) digits = "44" + digits;
  // Must be all digits, plausible length (8-15 per E.164).
  if (!/^\d{8,15}$/.test(digits)) return null;
  return digits;
}

// Fetch a Brightpearl order's parties so the WhatsApp greeting can use the
// real customer name — same source the order-tracking emails use. Returns
// null on any problem (creds missing, non-BP order id, API error) so the
// caller falls back to "there".
async function fetchBrightpearlParties(orderId) {
  if (!orderId || !BRIGHTPEARL_API_TOKEN || !BRIGHTPEARL_ACCOUNT_ID) return null;
  try {
    const baseUrl = BRIGHTPEARL_DATACENTER === 'euw1'
      ? 'https://euw1.brightpearlconnect.com'
      : 'https://use1.brightpearlconnect.com';
    const url = `${baseUrl}/public-api/${BRIGHTPEARL_ACCOUNT_ID}/order-service/order/${orderId}`;
    const response = await fetch(url, {
      method: 'GET',
      headers: {
        'brightpearl-app-ref': process.env.BRIGHTPEARL_APP_REF,
        'brightpearl-account-token': BRIGHTPEARL_API_TOKEN,
        'Content-Type': 'application/json',
      },
    });
    if (!response.ok) return null;
    const data = await response.json();
    const order = Array.isArray(data?.response)
      ? data.response[0]
      : (Array.isArray(data) ? data[0] : data);
    return order?.parties || null;
  } catch {
    return null;
  }
}

// Insert one message row into whatsapp_messages. Best-effort: a logging
// failure must never break the actual send/receive path, so we swallow.
async function recordWhatsAppMessage({
  waMessageId = null,
  direction,
  peerNumber,
  body = null,
  msgType = "text",
  status = null,
  orderNumber = null,
  mediaId = null,
  sentBy = null,
  raw = null,
}) {
  if (!useDatabase) return;
  try {
    await pool.query(
      `INSERT INTO whatsapp_messages
         (wa_message_id, direction, peer_number, body, msg_type, status, order_number, media_id, sent_by, raw)
       VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)`,
      [waMessageId, direction, peerNumber, body, msgType, status, orderNumber, mediaId, sentBy,
       raw ? JSON.stringify(raw) : null]
    );
  } catch (err) {
    console.error("[whatsapp] recordWhatsAppMessage failed:", err.message);
  }
}

// The 24h customer-service window is open iff the customer sent us an
// inbound message within the last 24h. Returns { open, expiresAt }.
async function whatsAppWindow(peerNumber) {
  if (!useDatabase) return { open: false, expiresAt: null };
  const r = await pool.query(
    `SELECT MAX(created_at) AS last_in FROM whatsapp_messages
       WHERE peer_number = $1 AND direction = 'in'`,
    [peerNumber]
  );
  const lastIn = r.rows[0]?.last_in ? new Date(r.rows[0].last_in) : null;
  if (!lastIn) return { open: false, expiresAt: null };
  const expiresAt = new Date(lastIn.getTime() + 24 * 60 * 60 * 1000);
  return { open: expiresAt.getTime() > Date.now(), expiresAt };
}

// Default auto-reply body, sent once per cooldown when a customer messages
// the proof number. Wording overridable via env without a deploy.
const DEFAULT_AUTO_REPLY =
  "This is a proof only service. For sales or general enquiries please email " +
  "sales@tuffshop.co.uk. If your message is about your proof, please wait and " +
  "we will be in touch shortly.";

// Send a one-shot auto-reply to a customer who just messaged us. Skips:
//  - if WHATSAPP_AUTO_REPLY_DISABLED is set (kill switch)
//  - if an auto-reply or human-typed reply went out in the cooldown window
//    (default 60 min) — prevents spam on rapid messages and stays silent
//    while a human is actively chatting
// Best-effort: a failure here must NOT break the webhook ack path.
async function maybeSendAutoReply(peer) {
  if (!peer) return;
  if (process.env.WHATSAPP_AUTO_REPLY_DISABLED === "1") return;
  const token = process.env.WHATSAPP_TOKEN;
  const phoneNumberId = process.env.WHATSAPP_PHONE_NUMBER_ID;
  if (!token || !phoneNumberId || !useDatabase) return;

  const cooldownMin = Number(process.env.WHATSAPP_AUTO_REPLY_COOLDOWN_MIN) || 60;
  try {
    const r = await pool.query(
      `SELECT 1 FROM whatsapp_messages
        WHERE peer_number = $1
          AND direction = 'out'
          AND msg_type IN ('auto_reply','text')
          AND created_at > NOW() - ($2 || ' minutes')::interval
        LIMIT 1`,
      [peer, String(cooldownMin)]
    );
    if (r.rowCount > 0) return; // recent outbound — stay quiet
  } catch (err) {
    console.error("[whatsapp/auto-reply] lookup failed:", err.message);
    return;
  }

  const body = process.env.WHATSAPP_AUTO_REPLY_TEXT || DEFAULT_AUTO_REPLY;
  const graphVersion = process.env.WHATSAPP_GRAPH_VERSION || "v21.0";
  try {
    const gRes = await fetch(
      `https://graph.facebook.com/${graphVersion}/${phoneNumberId}/messages`,
      {
        method: "POST",
        headers: {
          Authorization: `Bearer ${token}`,
          "Content-Type": "application/json",
        },
        body: JSON.stringify({
          messaging_product: "whatsapp",
          to: peer,
          type: "text",
          text: { body },
        }),
      }
    );
    const data = await gRes.json().catch(() => ({}));
    if (!gRes.ok) {
      console.error(
        "[whatsapp/auto-reply] Graph error:",
        JSON.stringify(data?.error || data)
      );
      return;
    }
    const messageId = data?.messages?.[0]?.id || null;
    await recordWhatsAppMessage({
      waMessageId: messageId,
      direction: "out",
      peerNumber: peer,
      body,
      msgType: "auto_reply",
      status: "sent",
    });
    console.log(`[whatsapp/auto-reply] sent to ${peer}`);
  } catch (err) {
    console.error("[whatsapp/auto-reply] error:", err.message);
  }
}

// Send a free-typed WhatsApp text to a customer (only valid inside the 24h
// service window opened by their inbound message). Records it as an outbound
// 'text' so the generic auto-reply cooldown also suppresses the proof-only
// message during an active conversation. Returns the wa message id or null.
async function sendWhatsAppText(peer, body, msgType = "text") {
  const token = process.env.WHATSAPP_TOKEN;
  const phoneNumberId = process.env.WHATSAPP_PHONE_NUMBER_ID;
  if (!token || !phoneNumberId || !peer || !body) return null;
  const graphVersion = process.env.WHATSAPP_GRAPH_VERSION || "v21.0";
  try {
    const gRes = await fetch(`https://graph.facebook.com/${graphVersion}/${phoneNumberId}/messages`, {
      method: "POST",
      headers: { Authorization: `Bearer ${token}`, "Content-Type": "application/json" },
      body: JSON.stringify({ messaging_product: "whatsapp", to: peer, type: "text", text: { body } }),
    });
    const data = await gRes.json().catch(() => ({}));
    if (!gRes.ok) { console.error("[whatsapp/text] Graph error:", JSON.stringify(data?.error || data)); return null; }
    const messageId = data?.messages?.[0]?.id || null;
    await recordWhatsAppMessage({ waMessageId: messageId, direction: "out", peerNumber: peer, body, msgType, status: "sent" });
    return messageId;
  } catch (err) {
    console.error("[whatsapp/text] send failed:", err.message);
    return null;
  }
}

// Classify a reply to the cross-sell offer. Button titles are definitive; free
// text is matched so a customer who types "yes please" instead of tapping the
// button is still understood.
function classifyCrossSellReply(text) {
  const t = (text || "").trim().toLowerCase();
  if (!t) return "other";
  if (t === "yes, add it") return "yes";
  if (t === "no thanks") return "no";
  if (/\b(yes|yeah|yep|yup|yup|sure|ok|okay|please do|go on|go ahead|add it|add one|i'?ll take|sounds good|definitely|love it)\b/.test(t)) return "yes";
  if (/\b(no|nah|nope|not interested|no thanks|don'?t|do not|leave it)\b/.test(t)) return "no";
  return "other";
}

const DEFAULT_CROSSSELL_REPLY =
  "Great! 🙌 What size, colour and quantity would you like? A member of the team " +
  "will get it added to your order.";

// Handle an inbound message as a reply to a recent cross-sell offer. Returns
// true if it was a cross-sell reply (so the generic proof-only auto-reply is
// suppressed). Flow:
//   1st reply = YES   → ask for size/colour/qty; DON'T email yet (wait).
//   1st reply = NO    → just logged.
//   1st reply = other → email (unclear, a human should look).
//   next message after a YES = the size details → send ONE combined email.
async function handleCrossSellReply(peer, text, contactName) {
  if (!useDatabase || !peer) return false;

  // 1) Is this the FIRST reply to a pending offer?
  let send;
  try {
    const r = await pool.query(
      `SELECT * FROM crosssell_sends
        WHERE customer_phone = $1 AND response IS NULL
          AND sent_at > NOW() - INTERVAL '7 days'
        ORDER BY sent_at DESC LIMIT 1`,
      [peer]
    );
    send = r.rows[0];
  } catch (err) {
    console.error("[crosssell/reply] lookup failed:", err.message);
    return false;
  }

  if (send) {
    const verdict = classifyCrossSellReply(text); // 'yes' | 'no' | 'other'
    try {
      await pool.query(
        `UPDATE crosssell_sends SET response = $1, responded_at = NOW() WHERE id = $2`,
        [verdict, send.id]
      );
    } catch (err) { console.error("[crosssell/reply] update failed:", err.message); }

    if (verdict === "yes" && process.env.CROSSSELL_AUTO_REPLY_DISABLED !== "1") {
      // Ask for the details; the email waits until they reply with them.
      const body = process.env.CROSSSELL_REPLY_TEXT || DEFAULT_CROSSSELL_REPLY;
      await sendWhatsAppText(peer, body, "crosssell_reply");
    }
    // Only an UNCLEAR first reply emails now (no size question was asked, so no
    // follow-up to wait for). A clear yes waits for details; a clear no is logged.
    if (verdict === "other") {
      notifyCrossSellReply(send, contactName, text, verdict).catch((e) =>
        console.error("[crosssell/reply] notify failed:", e.message)
      );
    }
    console.log(`[crosssell/reply] ${peer} → ${verdict} (order ${send.order_number || "?"})`);
    return true;
  }

  // 2) Not a first reply — is this the size/colour/qty FOLLOW-UP to a recent
  // YES that hasn't been emailed yet? If so, send the single combined email.
  let pendingDetails;
  try {
    const r = await pool.query(
      `SELECT * FROM crosssell_sends
        WHERE customer_phone = $1 AND response = 'yes' AND details_at IS NULL
          AND responded_at > NOW() - INTERVAL '24 hours'
        ORDER BY responded_at DESC LIMIT 1`,
      [peer]
    );
    pendingDetails = r.rows[0];
  } catch (err) {
    console.error("[crosssell/reply] details lookup failed:", err.message);
    return false;
  }
  if (!pendingDetails) return false; // not cross-sell related

  try {
    await pool.query(
      `UPDATE crosssell_sends SET details_text = $1, details_at = NOW(), notified_at = NOW() WHERE id = $2`,
      [text || null, pendingDetails.id]
    );
  } catch (err) { console.error("[crosssell/reply] details update failed:", err.message); }

  notifyCrossSellReply(pendingDetails, contactName, text, "yes").catch((e) =>
    console.error("[crosssell/reply] notify failed:", e.message)
  );
  // Acknowledge the customer so they know it's in hand.
  if (process.env.CROSSSELL_AUTO_REPLY_DISABLED !== "1") {
    const ack = process.env.CROSSSELL_ACK_TEXT ||
      "Thanks! 👍 We'll get that added to your order and confirm shortly.";
    await sendWhatsAppText(peer, ack, "crosssell_reply");
  }
  console.log(`[crosssell/reply] ${peer} → size details captured (order ${pendingDetails.order_number || "?"})`);
  return true;
}

// Safety net: a customer who said YES but never sent their size/colour/qty.
// After a grace period (default 2h), email anyway so the lead isn't lost.
// Runs periodically; each row is emailed at most once (notified_at).
async function sweepCrossSellAwaitingDetails() {
  if (!useDatabase) return;
  const graceMin = Number(process.env.CROSSSELL_DETAILS_GRACE_MIN) || 120;
  try {
    const r = await pool.query(
      `SELECT * FROM crosssell_sends
        WHERE response = 'yes' AND details_at IS NULL AND notified_at IS NULL
          AND responded_at < NOW() - ($1 || ' minutes')::interval
          AND responded_at > NOW() - INTERVAL '24 hours'
        ORDER BY responded_at ASC LIMIT 20`,
      [String(graceMin)]
    );
    for (const send of r.rows) {
      try {
        await pool.query(`UPDATE crosssell_sends SET notified_at = NOW() WHERE id = $1`, [send.id]);
        await notifyCrossSellReply(send, null, null, "yes_nodetails");
        console.log(`[crosssell/sweep] chased order ${send.order_number || "?"} (yes, no details after ${graceMin}m)`);
      } catch (e) {
        console.error("[crosssell/sweep] row failed:", e.message);
      }
    }
  } catch (err) {
    console.error("[crosssell/sweep] query failed:", err.message);
  }
}
setInterval(() => { sweepCrossSellAwaitingDetails().catch(() => {}); }, 15 * 60 * 1000);

// Email sales when a customer responds to a cross-sell offer (yes, or an
// unclear reply). They confirm size/colour/qty with the customer and add it to
// Brightpearl manually.
async function notifyCrossSellReply(send, contactName, rawText, verdict) {
  if (!process.env.SMTP_PASS) { console.warn("[crosssell/reply] SMTP_PASS unset — not emailed"); return; }
  // Test sends (logged with order "TEST") go to the tester so real sales never
  // see test traffic; real sends go to the configured sales address.
  const isTest = send?.order_number === "TEST";
  const to = isTest
    ? (process.env.CROSSSELL_TEST_NOTIFY_EMAIL || "dec@tuffshop.co.uk")
    : (process.env.CROSSSELL_NOTIFY_EMAIL || process.env.PROMO_OFFER_NOTIFY_EMAIL || "sales@tuffshop.co.uk");
  const transporter = nodemailer.createTransport({
    host: process.env.SMTP_SERVER || "mail-eu.smtp2go.com",
    port: parseInt(process.env.SMTP_PORT || "2525", 10),
    secure: false,
    auth: { user: process.env.SMTP_USERNAME || "tuffshop.co.uk", pass: process.env.SMTP_PASS },
  });
  const name = contactName || "Customer";
  const isYes = verdict === "yes";
  const isAwaiting = verdict === "yes_nodetails";
  const orderNo = send.order_number || "—";
  const item = [send.companion_name, send.companion_sku && `(${send.companion_sku})`].filter(Boolean).join(" ") || "—";
  const sizes = isAwaiting
    ? "<em>not sent yet — chase the customer</em>"
    : isYes
    ? `<strong>${(rawText || "—").replace(/</g, "&lt;")}</strong>`
    : (rawText || "—").replace(/</g, "&lt;");
  const intro = isAwaiting
    ? `<strong>${name}</strong> (order ${orderNo}) would like to add some items to their order but hasn't sent their sizes yet — please give them a nudge.`
    : isYes
    ? `<strong>${name}</strong> (order ${orderNo}) would like to add some items to their order, see the information below.`
    : `<strong>${name}</strong> (order ${orderNo}) replied to an add-on offer but it wasn't a clear yes/no — please take a look.`;
  const td = 'style="padding:4px 12px 4px 0;color:#666;vertical-align:top"';
  const html = `<div style="font-family:Arial,sans-serif;font-size:14px;color:#111">
    <p>${intro}</p>
    <table style="border-collapse:collapse;font-size:14px;margin-top:8px">
      <tr><td ${td}>Customer Name</td><td><strong>${name}</strong></td></tr>
      <tr><td ${td}>Order Number</td><td><strong>${orderNo}</strong></td></tr>
      <tr><td ${td}>Phone</td><td><a href="https://wa.me/${(send.customer_phone || "").replace(/[^\d]/g, "")}">${send.customer_phone || "—"}</a></td></tr>
      <tr><td ${td}>Item</td><td>${item}</td></tr>
      <tr><td ${td}>Size / colour / qty</td><td>${sizes}</td></tr>
    </table>
    ${send.mockup_url ? `<p style="margin-top:12px"><img src="${send.mockup_url}" alt="mockup" style="max-width:280px;border:1px solid #e5e7eb;border-radius:6px"></p>` : ""}
  </div>`;
  await transporter.sendMail({
    from: "Tuff Shop <ordertracking@tuffshop.co.uk>",
    to,
    subject: `Customer Order ${orderNo} Add On${isAwaiting ? " — awaiting sizes" : ""}`,
    html,
  });
  console.log(`[crosssell/reply] notified ${to} (order ${orderNo})`);
}

// GET webhook — Meta's one-time verification handshake. Echoes
// hub.challenge only when the verify token matches.
app.get("/api/whatsapp/webhook", (req, res) => {
  const mode = req.query["hub.mode"];
  const token = req.query["hub.verify_token"];
  const challenge = req.query["hub.challenge"];
  if (mode === "subscribe" && token && token === process.env.WHATSAPP_VERIFY_TOKEN) {
    return res.status(200).send(challenge);
  }
  return res.sendStatus(403);
});

// POST webhook — inbound messages + delivery-status callbacks from Meta.
// Always 200 quickly (Meta retries on non-2xx); do work after responding.
app.post("/api/whatsapp/webhook", async (req, res) => {
  // Verify X-Hub-Signature-256 over the exact raw bytes.
  const appSecret = process.env.WHATSAPP_APP_SECRET;
  const sigHeader = req.get("x-hub-signature-256") || "";
  if (appSecret && req.rawBody) {
    const expected =
      "sha256=" +
      crypto.createHmac("sha256", appSecret).update(req.rawBody).digest("hex");
    const a = Buffer.from(sigHeader);
    const b = Buffer.from(expected);
    if (a.length !== b.length || !crypto.timingSafeEqual(a, b)) {
      console.warn("[whatsapp/webhook] signature mismatch — rejected");
      return res.sendStatus(403);
    }
  }
  res.sendStatus(200);

  try {
    const entries = req.body?.entry || [];
    for (const entry of entries) {
      for (const change of entry.changes || []) {
        const value = change.value || {};
        const contacts = value.contacts || [];
        const nameByWaId = {};
        for (const c of contacts) nameByWaId[c.wa_id] = c.profile?.name;

        // Inbound customer messages
        for (const m of value.messages || []) {
          const peer = m.from;
          // Media messages (image/document/video/audio/sticker) carry a Graph
          // media id + optional caption. We store the id (not the bytes) and
          // fetch on demand via /api/whatsapp/media/:id.
          const media = m.image || m.document || m.video || m.audio || m.sticker || null;
          const mediaId = media?.id || null;
          let text = null;
          if (m.type === "text") text = m.text?.body || null;
          else if (m.type === "button") text = m.button?.text || null;
          else if (m.type === "interactive")
            text =
              m.interactive?.button_reply?.title ||
              m.interactive?.list_reply?.title ||
              null;
          else if (mediaId)
            // Use the caption as the bubble text; the image itself renders from media_id.
            text = media?.caption || media?.filename || null;
          else text = `[${m.type} message]`;
          await recordWhatsAppMessage({
            waMessageId: m.id,
            direction: "in",
            peerNumber: peer,
            body: text,
            msgType: m.type || "text",
            mediaId,
            raw: { message: m, contactName: nameByWaId[peer] || null },
          });
          console.log(`[whatsapp/webhook] inbound from ${peer}: ${text?.slice(0, 80)}`);
          // If this is a reply to a cross-sell offer, handle it (auto-reply +
          // notify sales) and skip the generic proof-only auto-reply. Otherwise
          // fall back to the rate-limited generic auto-reply.
          handleCrossSellReply(peer, text, nameByWaId[peer] || null)
            .then((handled) => { if (!handled) return maybeSendAutoReply(peer); })
            .catch((err) => console.error("[whatsapp/reply] unhandled:", err.message));
        }

        // Delivery-status updates for our outbound messages
        for (const s of value.statuses || []) {
          // Log every status so delivery problems are visible. A 'failed'
          // status carries an errors[] array with the real reason (e.g.
          // recipient has no WhatsApp account, not in allowed list, etc).
          if (s.status === "failed") {
            console.error(
              `[whatsapp/webhook] message ${s.id} to ${s.recipient_id} FAILED:`,
              JSON.stringify(s.errors || s)
            );
          } else {
            console.log(
              `[whatsapp/webhook] status ${s.status} for ${s.id} (to ${s.recipient_id})`
            );
          }
          if (!useDatabase) continue;
          try {
            await pool.query(
              `UPDATE whatsapp_messages
                 SET status = $1
               WHERE wa_message_id = $2 AND direction = 'out'`,
              [s.status, s.id]
            );
          } catch (err) {
            console.error("[whatsapp/webhook] status update failed:", err.message);
          }
        }
      }
    }
  } catch (err) {
    console.error("[whatsapp/webhook] processing error:", err.message);
  }
});

// Send a proof-approval link to a customer via the WhatsApp Cloud API,
// using the pre-approved "proof_approval_request" message template.
app.post("/api/whatsapp/send-proof", async (req, res) => {
  const token = process.env.WHATSAPP_TOKEN;
  const phoneNumberId = process.env.WHATSAPP_PHONE_NUMBER_ID;
  if (!token || !phoneNumberId) {
    return res.status(503).json({
      error: "WhatsApp not configured (set WHATSAPP_TOKEN and WHATSAPP_PHONE_NUMBER_ID)",
    });
  }

  const { phone, customerName, orderNumber, approvalUrl, sentBy } = req.body || {};

  const to = normaliseWhatsAppNumber(phone);
  if (!to) {
    return res.status(400).json({ error: "A valid phone number is required" });
  }
  if (!approvalUrl) {
    return res.status(400).json({ error: "approvalUrl is required" });
  }

  const templateName = process.env.WHATSAPP_TEMPLATE_NAME || "proof_approval_request";
  const templateLang = process.env.WHATSAPP_TEMPLATE_LANG || "en_GB";
  const graphVersion = process.env.WHATSAPP_GRAPH_VERSION || "v21.0";

  // Template body: "Hi {{1}}, your proof for order {{2}} is ready... tap the
  // button below." The link is a dynamic URL button, NOT a body variable.
  //
  // Greeting name priority:
  //   1. an explicit recipient name typed in the UI (customerName)
  //   2. otherwise the customer's name from the Brightpearl order
  //   3. deriveFirstName() handles title-casing + first-name extraction,
  //      and returns "there" when there's genuinely no usable name —
  //      identical to the order-tracking emails. So named orders greet
  //      "Hi Glyn" and only nameless ones get "Hi there".
  let nameSource = customerName;
  if (!nameSource && orderNumber) {
    const parties = await fetchBrightpearlParties(orderNumber);
    if (parties) nameSource = pickCustomerName(parties);
  }
  const greetingName = deriveFirstName(nameSource);
  const nameParam = (greetingName || "there").toString().slice(0, 60);
  const orderParam = (orderNumber || "your order").toString().slice(0, 60);
  const bodyParams = [
    { type: "text", text: nameParam },
    { type: "text", text: orderParam },
  ];

  // The button's URL is configured in WhatsApp Manager as
  // https://mock-up-creator-hosted-web.onrender.com/approve/{{1}}
  // so we pass only the path suffix after /approve/ as the variable.
  const fullUrl = approvalUrl.toString();
  const suffixMatch = fullUrl.match(/\/approve\/(.+)$/);
  const urlSuffix = suffixMatch ? suffixMatch[1] : fullUrl;

  try {
    const gRes = await fetch(
      `https://graph.facebook.com/${graphVersion}/${phoneNumberId}/messages`,
      {
        method: "POST",
        headers: {
          Authorization: `Bearer ${token}`,
          "Content-Type": "application/json",
        },
        body: JSON.stringify({
          messaging_product: "whatsapp",
          to,
          type: "template",
          template: {
            name: templateName,
            language: { code: templateLang },
            components: [
              { type: "body", parameters: bodyParams },
              {
                type: "button",
                sub_type: "url",
                index: "0",
                parameters: [{ type: "text", text: urlSuffix }],
              },
            ],
          },
        }),
      }
    );

    const data = await gRes.json().catch(() => ({}));

    if (!gRes.ok) {
      const fbErr = data?.error?.message || `Graph API returned ${gRes.status}`;
      console.error("[whatsapp/send-proof] Graph API error:", JSON.stringify(data?.error || data));
      return res.status(502).json({ error: fbErr, details: data?.error || null });
    }

    const messageId = data?.messages?.[0]?.id || null;
    console.log(`[whatsapp/send-proof] sent to ${to} (order ${orderNumber || "?"}) id=${messageId}`);
    // Store the exact rendered message (matching the approved template
    // body + button) so staff can verify in the inbox what the customer
    // actually received. Keep this wording in sync with the WhatsApp
    // Manager template.
    const renderedBody =
      `Hi ${nameParam}, your proof for order ${orderParam} is ready to ` +
      `review. Tap the button below to approve or request changes.\n\n` +
      `🔗 Review proof: ${fullUrl}`;
    await recordWhatsAppMessage({
      waMessageId: messageId,
      direction: "out",
      peerNumber: to,
      body: renderedBody,
      msgType: "template",
      status: "sent",
      orderNumber: orderNumber ? String(orderNumber) : null,
      raw: { template: templateName, approvalUrl: fullUrl },
    });
    res.json({ success: true, messageId, to });

    // Best-effort: transition BP order from "Proof Required" (34) to
    // "Proof Sent" (35) now the proof's gone out. Fires async after the
    // response so it doesn't delay the UI. No-op if already past 34.
    if (orderNumber) {
      const noteText = sentBy
        ? `${sentBy} sent Proof via WhatsApp to ${to}`
        : `Proof sent via WhatsApp to ${to}`;
      transitionBpStatusProofRequiredToSent(String(orderNumber), noteText)
        .catch((err) => console.error(`[bp-status] ${orderNumber} transition unexpected error:`, err.message));
    }
  } catch (err) {
    console.error("[whatsapp/send-proof] error:", err.message);
    res.status(500).json({ error: err.message });
  }
});

// Send a proof approval link to a customer via email. Mirrors the
// WhatsApp send flow: From is the operator's email (so replies route
// to them), display name is "Tuffshop Proofs". Fires the same BP
// status 34 → 35 transition + BP note as the WhatsApp path so the
// downstream automation behaves identically regardless of channel.
//
// Body: { to: string|string[], orderNumber, customerName, approvalUrl, sentBy }
app.post('/api/proof/send-email', async (req, res) => {
  if (!process.env.SMTP_PASS) {
    return res.status(503).json({ error: 'SMTP not configured (SMTP_PASS missing)' });
  }
  try {
    const { to, orderNumber, customerName, approvalUrl, sentBy } = req.body || {};
    if (!to) return res.status(400).json({ error: 'to required' });
    if (!approvalUrl) return res.status(400).json({ error: 'approvalUrl required' });

    const recipients = Array.isArray(to) ? to.filter(Boolean) : [to];
    if (recipients.length === 0) return res.status(400).json({ error: 'to is empty' });

    const fromEmail = operatorEmailFor(sentBy);

    // Greeting name resolution mirrors the WhatsApp template logic.
    let nameSource = customerName;
    if (!nameSource && orderNumber) {
      const parties = await fetchBrightpearlParties(orderNumber);
      if (parties) nameSource = pickCustomerName(parties);
    }
    const greeting = deriveFirstName(nameSource) || 'there';
    const orderRef = orderNumber || 'your order';
    const subject = `Your proof for order ${orderRef} is ready to review`;

    const ctaButton = `
      <table cellpadding="0" cellspacing="0" border="0" style="margin:24px 0">
        <tr>
          <td style="background:#F3D014;border-radius:8px;padding:0;text-align:center">
            <a href="${approvalUrl}" target="_blank"
               style="display:inline-block;padding:16px 32px;color:#000;text-decoration:none;font-weight:bold;font-size:16px;font-family:Arial,sans-serif;letter-spacing:0.5px">
              ► REVIEW YOUR PROOF
            </a>
          </td>
        </tr>
      </table>`;

    const html = `
      <div style="font-family:Arial,sans-serif;max-width:600px;margin:0 auto;color:#222;background:#fff">
        <div style="background:#000;color:#fff;padding:16px 20px;border-bottom:3px solid #F3D014">
          <h2 style="margin:0;font-size:18px">Your proof is ready</h2>
        </div>
        <div style="padding:20px;font-size:14px;line-height:1.5">
          <p>Hi ${greeting},</p>
          <p>Your proof for order <strong>${orderRef}</strong> is ready to review.</p>
          <p>Click the button below to open your <strong>proof confirmation page</strong>, where you can approve each logo or request changes.</p>
          ${ctaButton}
          <p style="color:#666;font-size:12px;margin-top:24px">
            Button not working? Copy and paste this link into your browser:<br>
            <a href="${approvalUrl}" style="color:#666;word-break:break-all">${approvalUrl}</a>
          </p>
          ${SIGNATURE_HTML || ''}
        </div>
      </div>`;

    const text = `Hi ${greeting},

Your proof for order ${orderRef} is ready to review.

Click here to open your proof confirmation page, where you can approve each logo or request changes:

${approvalUrl}

${SIGNATURE_TEXT || ''}`;

    const smtpHost = process.env.SMTP_SERVER || 'mail-eu.smtp2go.com';
    const primaryPort = parseInt(process.env.SMTP_PORT || '2525');
    // smtp2go accepts submission on several ports (2525/587/8025). If one is
    // briefly throttled/blocked from Render's IP, a same-port retry just times
    // out again — so rotate through known-good ports across attempts.
    const portRotation = [...new Set([primaryPort, 2525, 587, 8025])];
    const makeTransport = (port) => nodemailer.createTransport({
      host: smtpHost,
      port,
      secure: false,
      auth: { user: process.env.SMTP_USERNAME || 'tuffshop.co.uk', pass: process.env.SMTP_PASS },
      connectionTimeout: 15000,
      greetingTimeout: 15000,
      socketTimeout: 20000,
    });

    // Retry transient SMTP failures ("Greeting never received", timeouts, resets)
    // — smtp2go occasionally doesn't complete the handshake in time. Each attempt
    // opens a fresh connection (non-pooled) on the next port in the rotation.
    const sendWithRetry = async (mail, tries = 4) => {
      let lastErr;
      for (let i = 0; i < tries; i++) {
        const port = portRotation[i % portRotation.length];
        try { return await makeTransport(port).sendMail(mail); }
        catch (e) {
          lastErr = e;
          const transient = /greeting never received|timeout|ETIMEDOUT|ECONNRESET|ECONNECTION|ESOCKET|EAI_AGAIN|connection closed/i.test(e.message || '');
          if (!transient || i === tries - 1) throw e;
          console.warn(`[proof-email] transient send error on port ${port} (retry ${i + 1}/${tries - 1}):`, e.message);
          await new Promise((r) => setTimeout(r, 1500 * (i + 1)));
        }
      }
      throw lastErr;
    };

    await sendWithRetry({
      from: `"Tuffshop Proofs" <${fromEmail}>`,
      to: recipients.join(', '),
      replyTo: fromEmail,
      subject,
      text,
      html,
    });

    console.log(`[proof-email] sent to ${recipients.join(', ')} from ${fromEmail} (order ${orderNumber || '?'})`);
    res.json({ success: true, to: recipients });

    // Best-effort BP status transition 34 → 35, same as WhatsApp path.
    if (orderNumber) {
      const noteText = sentBy
        ? `${sentBy} sent Proof via Email to ${recipients.join(', ')}`
        : `Proof sent via Email to ${recipients.join(', ')}`;
      transitionBpStatusProofRequiredToSent(String(orderNumber), noteText)
        .catch((err) => console.error(`[bp-status] ${orderNumber} transition unexpected error:`, err.message));
    }
  } catch (err) {
    console.error('[proof-email] error:', err.message);
    if (!res.headersSent) res.status(500).json({ error: err.message });
  }
});

// Send a free-form reply to a customer. Only allowed inside the 24h
// customer-service window (i.e. they messaged us within 24h); outside it
// WhatsApp requires a template, so we 409 with windowClosed so the UI can
// explain and offer the proof template instead.
app.post("/api/whatsapp/send-message", async (req, res) => {
  const token = process.env.WHATSAPP_TOKEN;
  const phoneNumberId = process.env.WHATSAPP_PHONE_NUMBER_ID;
  if (!token || !phoneNumberId) {
    return res.status(503).json({ error: "WhatsApp not configured" });
  }
  const { phone, body, sentBy } = req.body || {};
  const to = normaliseWhatsAppNumber(phone);
  if (!to) return res.status(400).json({ error: "A valid phone number is required" });
  const text = (body || "").toString().trim();
  if (!text) return res.status(400).json({ error: "Message body is required" });

  const win = await whatsAppWindow(to);
  if (!win.open) {
    return res.status(409).json({
      error: "The 24-hour reply window has closed for this customer.",
      windowClosed: true,
    });
  }

  const graphVersion = process.env.WHATSAPP_GRAPH_VERSION || "v21.0";
  try {
    const gRes = await fetch(
      `https://graph.facebook.com/${graphVersion}/${phoneNumberId}/messages`,
      {
        method: "POST",
        headers: {
          Authorization: `Bearer ${token}`,
          "Content-Type": "application/json",
        },
        body: JSON.stringify({
          messaging_product: "whatsapp",
          to,
          type: "text",
          text: { body: text.slice(0, 4096) },
        }),
      }
    );
    const data = await gRes.json().catch(() => ({}));
    if (!gRes.ok) {
      const fbErr = data?.error?.message || `Graph API returned ${gRes.status}`;
      console.error("[whatsapp/send-message] Graph error:", JSON.stringify(data?.error || data));
      return res.status(502).json({ error: fbErr });
    }
    const messageId = data?.messages?.[0]?.id || null;
    await recordWhatsAppMessage({
      waMessageId: messageId,
      direction: "out",
      peerNumber: to,
      body: text,
      msgType: "text",
      status: "sent",
      sentBy: (sentBy || "").toString().trim().slice(0, 120) || null,
    });
    res.json({ success: true, messageId, to });
  } catch (err) {
    console.error("[whatsapp/send-message] error:", err.message);
    res.status(500).json({ error: err.message });
  }
});

// Send an image to a customer (within the 24h service window). The operator
// uploads the bytes; we upload them to WhatsApp media, send an image message,
// and record it with the returned media id so the chat can render it back via
// the media proxy.
const WA_SEND_IMAGE_TYPES = ["image/jpeg", "image/png"];
app.post("/api/whatsapp/send-image", async (req, res) => {
  const token = process.env.WHATSAPP_TOKEN;
  const phoneNumberId = process.env.WHATSAPP_PHONE_NUMBER_ID;
  if (!token || !phoneNumberId) return res.status(503).json({ error: "WhatsApp not configured" });

  const { phone, imageBase64, mimeType, caption, sentBy } = req.body || {};
  const to = normaliseWhatsAppNumber(phone);
  if (!to) return res.status(400).json({ error: "A valid phone number is required" });
  const mime = (mimeType || "").toLowerCase();
  if (!imageBase64 || !WA_SEND_IMAGE_TYPES.includes(mime)) {
    return res.status(400).json({ error: "A JPEG or PNG image is required" });
  }
  const buf = Buffer.from(imageBase64, "base64");
  if (!buf.length) return res.status(400).json({ error: "Empty image" });
  if (buf.length > 5 * 1024 * 1024) return res.status(413).json({ error: "Image too large (max 5MB)" });

  const win = await whatsAppWindow(to);
  if (!win.open) {
    return res.status(409).json({ error: "The 24-hour reply window has closed for this customer.", windowClosed: true });
  }

  const graphVersion = process.env.WHATSAPP_GRAPH_VERSION || "v21.0";
  const cap = (caption || "").toString().trim().slice(0, 1024);
  try {
    // 1) Upload the media to get a reusable id.
    const form = new FormData();
    form.append("messaging_product", "whatsapp");
    form.append("type", mime);
    form.append("file", new Blob([buf], { type: mime }), mime === "image/png" ? "image.png" : "image.jpg");
    const upRes = await fetch(`https://graph.facebook.com/${graphVersion}/${phoneNumberId}/media`, {
      method: "POST",
      headers: { Authorization: `Bearer ${token}` },
      body: form,
    });
    const upData = await upRes.json().catch(() => ({}));
    if (!upRes.ok || !upData?.id) {
      console.error("[whatsapp/send-image] upload error:", JSON.stringify(upData?.error || upData));
      return res.status(502).json({ error: upData?.error?.message || `Media upload failed (${upRes.status})` });
    }
    const mediaId = upData.id;

    // 2) Send the image message referencing the uploaded media.
    const gRes = await fetch(`https://graph.facebook.com/${graphVersion}/${phoneNumberId}/messages`, {
      method: "POST",
      headers: { Authorization: `Bearer ${token}`, "Content-Type": "application/json" },
      body: JSON.stringify({
        messaging_product: "whatsapp",
        to,
        type: "image",
        image: { id: mediaId, ...(cap ? { caption: cap } : {}) },
      }),
    });
    const data = await gRes.json().catch(() => ({}));
    if (!gRes.ok) {
      console.error("[whatsapp/send-image] Graph error:", JSON.stringify(data?.error || data));
      return res.status(502).json({ error: data?.error?.message || `Graph API returned ${gRes.status}` });
    }
    const messageId = data?.messages?.[0]?.id || null;
    await recordWhatsAppMessage({
      waMessageId: messageId,
      direction: "out",
      peerNumber: to,
      body: cap || null,
      msgType: "image",
      status: "sent",
      mediaId,
      sentBy: (sentBy || "").toString().trim().slice(0, 120) || null,
    });
    res.json({ success: true, messageId, to });
  } catch (err) {
    console.error("[whatsapp/send-image] error:", err.message);
    res.status(500).json({ error: err.message });
  }
});

// Send a PDF document to a customer (within the 24h service window). Mirrors
// send-image: the operator uploads the bytes, we upload them to WhatsApp media,
// send a document message (with the original filename + any typed caption), and
// record it with the returned media id so the chat shows a "📎 Open document"
// link via the media proxy.
const WA_SEND_DOC_TYPES = ["application/pdf"];
app.post("/api/whatsapp/send-document", async (req, res) => {
  const token = process.env.WHATSAPP_TOKEN;
  const phoneNumberId = process.env.WHATSAPP_PHONE_NUMBER_ID;
  if (!token || !phoneNumberId) return res.status(503).json({ error: "WhatsApp not configured" });

  const { phone, fileBase64, mimeType, filename, caption, sentBy } = req.body || {};
  const to = normaliseWhatsAppNumber(phone);
  if (!to) return res.status(400).json({ error: "A valid phone number is required" });
  const mime = (mimeType || "").toLowerCase();
  if (!fileBase64 || !WA_SEND_DOC_TYPES.includes(mime)) {
    return res.status(400).json({ error: "A PDF document is required" });
  }
  const buf = Buffer.from(fileBase64, "base64");
  if (!buf.length) return res.status(400).json({ error: "Empty document" });
  if (buf.length > 25 * 1024 * 1024) return res.status(413).json({ error: "Document too large (max 25MB)" });

  const win = await whatsAppWindow(to);
  if (!win.open) {
    return res.status(409).json({ error: "The 24-hour reply window has closed for this customer.", windowClosed: true });
  }

  // Sanitise the display filename; WhatsApp shows it to the customer. Always
  // end in .pdf so it opens as a document on the recipient's device.
  let docName = (filename || "document.pdf").toString().trim().replace(/[\r\n"]/g, "").slice(0, 240) || "document.pdf";
  if (!/\.pdf$/i.test(docName)) docName += ".pdf";

  const graphVersion = process.env.WHATSAPP_GRAPH_VERSION || "v21.0";
  const cap = (caption || "").toString().trim().slice(0, 1024);
  try {
    // 1) Upload the media to get a reusable id.
    const form = new FormData();
    form.append("messaging_product", "whatsapp");
    form.append("type", mime);
    form.append("file", new Blob([buf], { type: mime }), docName);
    const upRes = await fetch(`https://graph.facebook.com/${graphVersion}/${phoneNumberId}/media`, {
      method: "POST",
      headers: { Authorization: `Bearer ${token}` },
      body: form,
    });
    const upData = await upRes.json().catch(() => ({}));
    if (!upRes.ok || !upData?.id) {
      console.error("[whatsapp/send-document] upload error:", JSON.stringify(upData?.error || upData));
      return res.status(502).json({ error: upData?.error?.message || `Media upload failed (${upRes.status})` });
    }
    const mediaId = upData.id;

    // 2) Send the document message referencing the uploaded media.
    const gRes = await fetch(`https://graph.facebook.com/${graphVersion}/${phoneNumberId}/messages`, {
      method: "POST",
      headers: { Authorization: `Bearer ${token}`, "Content-Type": "application/json" },
      body: JSON.stringify({
        messaging_product: "whatsapp",
        to,
        type: "document",
        document: { id: mediaId, filename: docName, ...(cap ? { caption: cap } : {}) },
      }),
    });
    const data = await gRes.json().catch(() => ({}));
    if (!gRes.ok) {
      console.error("[whatsapp/send-document] Graph error:", JSON.stringify(data?.error || data));
      return res.status(502).json({ error: data?.error?.message || `Graph API returned ${gRes.status}` });
    }
    const messageId = data?.messages?.[0]?.id || null;
    await recordWhatsAppMessage({
      waMessageId: messageId,
      direction: "out",
      peerNumber: to,
      body: cap || docName,
      msgType: "document",
      status: "sent",
      mediaId,
      sentBy: (sentBy || "").toString().trim().slice(0, 120) || null,
    });
    res.json({ success: true, messageId, to });
  } catch (err) {
    console.error("[whatsapp/send-document] error:", err.message);
    res.status(500).json({ error: err.message });
  }
});

// Conversation list — one row per customer number, newest activity first.
app.get("/api/whatsapp/conversations", async (req, res) => {
  if (!useDatabase) return res.status(503).json({ error: "Database not configured" });
  try {
    const r = await pool.query(`
      SELECT m.peer_number,
             MAX(m.created_at) AS last_at,
             COUNT(*) FILTER (
               WHERE m.direction = 'in' AND m.read_at IS NULL
                 AND m.dismissed_at IS NULL
             ) AS unread,
             MAX(m.created_at) FILTER (WHERE m.direction = 'in') AS last_in_at,
             -- Exclude auto-replies from "last message" derivation so the
             -- inbox preview and the Approval-History WhatsApp pill reflect
             -- the last human exchange, not the canned "proof-only" reply
             -- that fires after every inbound message.
             (SELECT body FROM whatsapp_messages x
                WHERE x.peer_number = m.peer_number
                  AND x.msg_type <> 'auto_reply'
                ORDER BY x.created_at DESC LIMIT 1) AS last_body,
             (SELECT direction FROM whatsapp_messages x
                WHERE x.peer_number = m.peer_number
                  AND x.msg_type <> 'auto_reply'
                ORDER BY x.created_at DESC LIMIT 1) AS last_direction,
             (SELECT order_number FROM whatsapp_messages x
                WHERE x.peer_number = m.peer_number AND x.order_number IS NOT NULL
                ORDER BY x.created_at DESC LIMIT 1) AS order_number
        FROM whatsapp_messages m
       GROUP BY m.peer_number
      HAVING bool_or(m.dismissed_at IS NULL)   -- hide fully-dismissed convos
       ORDER BY last_at DESC
    `);
    const now = Date.now();
    const conversations = r.rows.map((row) => {
      const lastIn = row.last_in_at ? new Date(row.last_in_at).getTime() : null;
      const expiresAt = lastIn ? new Date(lastIn + 24 * 3600 * 1000) : null;
      return {
        phone: row.peer_number,
        lastAt: row.last_at,
        lastBody: row.last_body,
        lastDirection: row.last_direction,
        unread: Number(row.unread) || 0,
        orderNumber: row.order_number || null,
        windowOpen: expiresAt ? expiresAt.getTime() > now : false,
        windowExpiresAt: expiresAt,
      };
    });
    res.json({ conversations });
  } catch (err) {
    console.error("[whatsapp/conversations] error:", err.message);
    res.status(500).json({ error: err.message });
  }
});

// Proxy a WhatsApp media object so the chat can show images without storing
// them. Graph media ids aren't directly fetchable: first resolve the id to a
// short-lived signed URL, then fetch THAT with the bearer token. Streams the
// bytes straight back (and lets the browser cache for a few minutes).
app.get("/api/whatsapp/media/:mediaId", async (req, res) => {
  const token = process.env.WHATSAPP_TOKEN;
  if (!token) return res.status(503).send("WhatsApp not configured");
  const mediaId = req.params.mediaId;
  if (!/^\d+$/.test(mediaId)) return res.status(400).send("Bad media id");
  const graphVersion = process.env.WHATSAPP_GRAPH_VERSION || "v21.0";
  try {
    const metaRes = await fetch(
      `https://graph.facebook.com/${graphVersion}/${mediaId}`,
      { headers: { Authorization: `Bearer ${token}` } }
    );
    if (!metaRes.ok) {
      return res.status(metaRes.status === 404 ? 404 : 502).send("Media lookup failed");
    }
    const meta = await metaRes.json();
    if (!meta?.url) return res.status(404).send("No media url");
    const binRes = await fetch(meta.url, { headers: { Authorization: `Bearer ${token}` } });
    if (!binRes.ok) return res.status(502).send("Media fetch failed");
    res.setHeader("Content-Type", meta.mime_type || binRes.headers.get("content-type") || "application/octet-stream");
    res.setHeader("Cache-Control", "private, max-age=300");
    const buf = Buffer.from(await binRes.arrayBuffer());
    res.send(buf);
  } catch (err) {
    console.error("[whatsapp/media] error:", err.message);
    res.status(500).send("Media proxy error");
  }
});

// Full message thread for one customer number.
app.get("/api/whatsapp/conversations/:phone/messages", async (req, res) => {
  if (!useDatabase) return res.status(503).json({ error: "Database not configured" });
  try {
    const phone = req.params.phone;
    const r = await pool.query(
      `SELECT id, wa_message_id, direction, peer_number, body, msg_type,
              status, order_number, read_at, created_at, media_id, sent_by
         FROM whatsapp_messages
        WHERE peer_number = $1
        ORDER BY created_at ASC`,
      [phone]
    );
    const win = await whatsAppWindow(phone);
    res.json({ phone, messages: r.rows, windowOpen: win.open, windowExpiresAt: win.expiresAt });
  } catch (err) {
    console.error("[whatsapp/messages] error:", err.message);
    res.status(500).json({ error: err.message });
  }
});

// Mark all inbound messages from a customer as read.
app.post("/api/whatsapp/conversations/:phone/read", async (req, res) => {
  if (!useDatabase) return res.status(503).json({ error: "Database not configured" });
  try {
    await pool.query(
      `UPDATE whatsapp_messages
          SET read_at = NOW()
        WHERE peer_number = $1 AND direction = 'in' AND read_at IS NULL`,
      [req.params.phone]
    );
    res.json({ success: true });
  } catch (err) {
    console.error("[whatsapp/read] error:", err.message);
    res.status(500).json({ error: err.message });
  }
});

// Dismiss a conversation — stamps every current row for the peer so it
// drops out of the conversation list (and the Unmatched panel). A later
// inbound message has dismissed_at NULL, so the conversation resurfaces.
app.post("/api/whatsapp/conversations/:phone/dismiss", async (req, res) => {
  if (!useDatabase) return res.status(503).json({ error: "Database not configured" });
  try {
    await pool.query(
      `UPDATE whatsapp_messages
          SET dismissed_at = NOW()
        WHERE peer_number = $1 AND dismissed_at IS NULL`,
      [req.params.phone]
    );
    res.json({ success: true });
  } catch (err) {
    console.error("[whatsapp/dismiss] error:", err.message);
    res.status(500).json({ error: err.message });
  }
});

// Lightweight: every order number that has ever had an approval session,
// regardless of age or status. Used by the Approval-History UI to decide
// whether an incoming WhatsApp conversation is "matched" — the full
// /api/approval-sessions list is capped at LIMIT 200, so old-but-genuine
// orders would otherwise be mis-classed as Unmatched once they age out of
// that window. Returns just the distinct order numbers (no PDFs, items or
// row payload), so it stays cheap even with thousands of sessions.
//
// NOTE: must be registered BEFORE the "/:sessionId" route below, or Express
// would capture "order-numbers" as a sessionId and try to load it as a UUID.
app.get("/api/approval-sessions/order-numbers", async (req, res) => {
  if (!pool) return res.status(503).json({ error: "Database not configured" });
  try {
    const r = await pool.query(
      `SELECT DISTINCT order_number
         FROM approval_sessions
        WHERE order_number IS NOT NULL AND order_number <> ''`
    );
    res.json({ success: true, orderNumbers: r.rows.map((row) => row.order_number) });
  } catch (err) {
    console.error("[approval] order-numbers failed:", err.message);
    res.status(500).json({ error: err.message });
  }
});

// Read-only DB bloat diagnostics: per-table sizes + what's actually taking
// space inside approval_sessions (PDFs vs logos vs signatures, and how much is
// on pending sessions the purge can't touch). Defined BEFORE /:sessionId so
// "db-stats" isn't parsed as a session UUID.
app.get("/api/approval-sessions/db-stats", async (req, res) => {
  if (!pool) return res.status(503).json({ error: "Database not configured" });
  try {
    const db = await pool.query(
      `SELECT pg_size_pretty(pg_database_size(current_database())) AS db_size`
    );
    const tables = await pool.query(
      `SELECT relname AS table,
              pg_size_pretty(pg_total_relation_size(c.oid)) AS total,
              pg_size_pretty(pg_total_relation_size(c.oid) - pg_relation_size(c.oid)) AS toast_and_index
         FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = 'public' AND c.relkind = 'r'
        ORDER BY pg_total_relation_size(c.oid) DESC
        LIMIT 12`
    );
    const breakdown = await pool.query(
      `SELECT count(*) AS rows,
              count(pdf_data) AS rows_with_pdf,
              count(*) FILTER (WHERE pdf_data IS NOT NULL AND status = 'pending') AS pending_with_pdf,
              count(*) FILTER (WHERE pdf_data IS NOT NULL AND status IN ('approved','changes_requested','archived')) AS completed_with_pdf,
              pg_size_pretty(COALESCE(sum(length(pdf_data)), 0)) AS pdf_live_total,
              pg_size_pretty(COALESCE(sum(COALESCE(length(primary_logo_data),0)
                                        + COALESCE(length(promo_logo_dark_data),0)
                                        + COALESCE(length(promo_logo_light_data),0)), 0)) AS logo_live_total,
              pg_size_pretty(COALESCE(sum(COALESCE(length(signature_data),0)), 0)) AS signature_live_total,
              min(created_at) AS oldest, max(created_at) AS newest
         FROM approval_sessions`
    );
    const deadrows = await pool.query(
      `SELECT relname AS table, n_live_tup AS live_rows, n_dead_tup AS dead_rows, last_autovacuum
         FROM pg_stat_user_tables
        ORDER BY n_dead_tup DESC
        LIMIT 8`
    );
    // The Render "storage" metric = whole Postgres disk (all DBs + WAL), not
    // just this DB. Surface those so a gap vs db_size is explained (usually a
    // WAL spike after VACUUM FULL, which recycles on the next checkpoint).
    let databases = null, wal = null;
    try {
      const r = await pool.query(
        `SELECT datname, pg_size_pretty(pg_database_size(datname)) AS size, pg_database_size(datname) AS bytes
           FROM pg_database ORDER BY pg_database_size(datname) DESC`
      );
      databases = r.rows;
    } catch (e) { databases = `unavailable: ${e.message}`; }
    try {
      const r = await pool.query(
        `SELECT pg_size_pretty(COALESCE(sum(size),0)) AS wal_total, count(*) AS wal_files FROM pg_ls_waldir()`
      );
      wal = r.rows[0];
    } catch (e) { wal = `unavailable: ${e.message}`; }
    res.json({ db_size: db.rows[0].db_size, databases, wal, approval_breakdown: breakdown.rows[0], tables: tables.rows, dead_tuples: deadrows.rows });
  } catch (err) {
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
              status, created_at, completed_at, opened_at, promo_offer_disabled
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
        promoOfferDisabled: session.promo_offer_disabled === true,
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
    let completedAt = null;
    if (allReviewed) {
      sessionStatus = hasRejected ? "changes_requested" : "approved";
      // Set status + clear the PDF data + capture submitter IP + approver info
      const submitterIp = req.headers["x-forwarded-for"]?.split(",")[0]?.trim() || req.socket?.remoteAddress || null;
      const upd = await pool.query(
        `UPDATE approval_sessions SET status = $1, completed_at = NOW(), submitter_ip = $3, approver_name = $4, signature_data = $5 WHERE id = $2 RETURNING completed_at`,
        [sessionStatus, sessionId, submitterIp, approverName || null, signatureData || null]
      );
      completedAt = upd.rows[0]?.completed_at || null;
    }

    res.json({ success: true, sessionStatus, completedAt });

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

        // Route email based on who created the proof (uses the shared
        // operator email map so adding new operators only needs one edit).
        const recipientEmail = operatorEmailFor(sess.created_by);

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

// Purge PDF data from signed/completed sessions older than 14 days (or past an
// explicit pdf_delete_after). Pending sessions keep their PDF indefinitely
// (customer hasn't responded yet). NOTE: nulling the BYTEA frees space for reuse
// inside Postgres but does NOT shrink the on-disk files — run VACUUM FULL
// approval_sessions once to actually return that space to the OS.
async function purgeOldApprovalPdfs() {
  if (!pool) return 0;
  // Purge the PDF blob from any NON-pending session older than 14 days. We use
  // COALESCE(completed_at, created_at) because completed_at is NULL on archived
  // / older sessions, which previously let them escape the purge forever (the
  // root cause of the DB bloat). Pending sessions keep their PDF (customer
  // hasn't responded yet). The pdf_delete_after branch handles the 24h-after-
  // archive fast path.
  const result = await pool.query(
    `UPDATE approval_sessions SET pdf_data = NULL, pdf_delete_after = NULL
     WHERE pdf_data IS NOT NULL AND (
       (status <> 'pending' AND COALESCE(completed_at, created_at) < NOW() - INTERVAL '14 days')
       OR (pdf_delete_after IS NOT NULL AND pdf_delete_after < NOW())
     )
     RETURNING id`
  );
  return result.rowCount;
}

app.post("/api/approval-sessions/cleanup", async (req, res) => {
  if (!pool) return res.status(503).json({ error: "Database not configured" });
  try {
    const cleaned = await purgeOldApprovalPdfs();
    res.json({ success: true, cleaned });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// One-off disk reclaim: VACUUM FULL rewrites approval_sessions (+ its TOAST blob
// storage) into fresh files, returning dead space to the OS — the only way to
// actually shrink the on-disk size after purging PDFs. Takes an ACCESS EXCLUSIVE
// lock (approvals pause for the duration) and needs free disk ~= the live data
// size to write the new copy. Run after a purge, during a quiet period.
app.post("/api/approval-sessions/vacuum", async (req, res) => {
  if (!pool) return res.status(503).json({ error: "Database not configured" });
  const client = await pool.connect();
  try {
    await client.query("SET statement_timeout = 0"); // VACUUM FULL may take minutes
    const before = await client.query(`SELECT pg_size_pretty(pg_total_relation_size('approval_sessions')) AS s`);
    await client.query("VACUUM (FULL, ANALYZE) approval_sessions");
    const after = await client.query(`SELECT pg_size_pretty(pg_total_relation_size('approval_sessions')) AS s`);
    res.json({ success: true, before: before.rows[0].s, after: after.rows[0].s });
  } catch (err) {
    res.status(500).json({ error: err.message });
  } finally {
    client.release();
  }
});

// Public: serve a logo variant for the approval session so the
// customer-facing promo panel can overlay it onto promo item images.
// Variant 'dark', 'light', or 'primary' (fallback). For dark/light,
// falls back to primary if the requested variant wasn't uploaded.
app.get("/api/approval-sessions/:sessionId/promo-logo/:variant", async (req, res) => {
  if (!pool) return res.status(503).json({ error: "Database not configured" });
  const variant = req.params.variant;
  if (!['dark', 'light', 'primary', 'auto'].includes(variant)) {
    return res.status(400).json({ error: 'variant must be dark, light, primary or auto' });
  }
  try {
    const result = await pool.query(
      `SELECT primary_logo_data, primary_logo_mime,
              promo_logo_dark_data, promo_logo_dark_mime,
              promo_logo_light_data, promo_logo_light_mime
         FROM approval_sessions WHERE id = $1`,
      [req.params.sessionId]
    );
    if (result.rowCount === 0) return res.status(404).end();
    const row = result.rows[0];
    let data = null, mime = null;
    if (variant === 'dark' && row.promo_logo_dark_data) {
      data = row.promo_logo_dark_data; mime = row.promo_logo_dark_mime;
    } else if (variant === 'light' && row.promo_logo_light_data) {
      data = row.promo_logo_light_data; mime = row.promo_logo_light_mime;
    }
    // Fallback to primary if the dedicated variant wasn't uploaded.
    if (!data && row.primary_logo_data) {
      data = row.primary_logo_data; mime = row.primary_logo_mime;
    }
    if (!data) return res.status(404).end();
    res.setHeader("Content-Type", mime || "image/png");
    res.setHeader("Cache-Control", "public, max-age=3600");
    res.send(Buffer.from(data));
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// Backwards-compatible primary endpoint.
app.get("/api/approval-sessions/:sessionId/primary-logo", async (req, res) => {
  if (!pool) return res.status(503).json({ error: "Database not configured" });
  try {
    const result = await pool.query(
      `SELECT primary_logo_data, primary_logo_mime FROM approval_sessions WHERE id = $1`,
      [req.params.sessionId]
    );
    if (result.rowCount === 0 || !result.rows[0].primary_logo_data) {
      return res.status(404).end();
    }
    const { primary_logo_data, primary_logo_mime } = result.rows[0];
    res.setHeader("Content-Type", primary_logo_mime || "image/png");
    res.setHeader("Cache-Control", "public, max-age=3600");
    res.send(Buffer.from(primary_logo_data));
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// Admin-only: allow setting/updating the primary logo on an existing
// session (so we can backfill or replace later if needed). Body:
// { primaryLogoBase64, primaryLogoMime }
app.post("/api/approval-sessions/:sessionId/primary-logo", async (req, res) => {
  if (!pool) return res.status(503).json({ error: "Database not configured" });
  try {
    const { primaryLogoBase64, primaryLogoMime } = req.body || {};
    if (!primaryLogoBase64) return res.status(400).json({ error: "primaryLogoBase64 required" });
    const buf = Buffer.from(primaryLogoBase64, "base64");
    await pool.query(
      `UPDATE approval_sessions SET primary_logo_data = $1, primary_logo_mime = $2 WHERE id = $3`,
      [buf, primaryLogoMime || "image/png", req.params.sessionId]
    );
    res.json({ success: true });
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
// Append a resend event to the session's resend_log. Frontend calls
// this after a successful /api/whatsapp/send-proof or /api/proof/send-email
// triggered from the Approval History "Resend" button so the timeline
// metadata gets updated.
// Body: { channel: 'whatsapp' | 'email', recipient: string, sentBy?: string }
app.post("/api/approval-sessions/:sessionId/log-resend", async (req, res) => {
  if (!pool) return res.status(503).json({ error: "Database not configured" });
  try {
    const { sessionId } = req.params;
    const { channel, recipient, sentBy } = req.body || {};
    if (!channel || !recipient) {
      return res.status(400).json({ error: "channel and recipient required" });
    }
    if (!['whatsapp', 'email'].includes(channel)) {
      return res.status(400).json({ error: "channel must be 'whatsapp' or 'email'" });
    }
    const entry = { channel, recipient, sentBy: sentBy || null, ts: new Date().toISOString() };
    const r = await pool.query(
      `UPDATE approval_sessions
          SET resend_log = COALESCE(resend_log, '[]'::jsonb) || $1::jsonb
        WHERE id = $2
        RETURNING resend_log`,
      [JSON.stringify(entry), sessionId]
    );
    if (r.rowCount === 0) return res.status(404).json({ error: "session not found" });
    res.json({ success: true, resendLog: r.rows[0].resend_log });
  } catch (err) {
    console.error("[approval] log-resend failed:", err.message);
    res.status(500).json({ error: err.message });
  }
});

app.get("/api/approval-sessions", async (req, res) => {
  if (!pool) return res.status(503).json({ error: "Database not configured" });

  try {
    const { orderNumber, includeArchived } = req.query;
    let query = `SELECT id, order_number, customer_name, recipient_name, status, created_at, completed_at, opened_at, submitter_ip, approver_name, signature_data, created_by, resend_log, (pdf_data IS NOT NULL) AS has_pdf
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

    // Active sessions BEFORE archived so the 200-row cap can never crowd out a
    // live approved/changes_requested/pending session behind a pile of archived
    // rows (the frontend "Needs attention" filter reads this list and was
    // under-counting because archived rows filled the window).
    query += ` ORDER BY (status = 'archived') ASC, created_at DESC LIMIT 200`;

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
          resendLog: session.resend_log || [],
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

// Auto-purge old proof PDFs so approval_sessions doesn't grow unbounded.
// Runs ~30s after boot, then every 12h. (Disk only shrinks after a one-off
// VACUUM FULL approval_sessions — nulling alone just frees space for reuse.)
if (pool) {
  const runPdfPurge = () =>
    purgeOldApprovalPdfs()
      .then((n) => { if (n > 0) console.log(`[approval-cleanup] purged PDF data from ${n} old session(s)`); })
      .catch((err) => console.error('[approval-cleanup] failed:', err.message));
  setTimeout(runPdfPurge, 30 * 1000);
  setInterval(runPdfPurge, 12 * 60 * 60 * 1000);
}

// Keep print_queue in sync with the operator's saved "Prints Needed" filter via
// the web session — cheap, so refresh on boot then every 10 min. (The webhook,
// if set up, gives instant updates between refreshes.)
if (BRIGHTPEARL_API_TOKEN && BRIGHTPEARL_ACCOUNT_ID) {
  setTimeout(() => refreshPrintQueueFromFilter().catch((e) => console.error('[print-queue] boot filter refresh failed:', e.message)), 60 * 1000);
  setInterval(() => refreshPrintQueueFromFilter().catch((e) => console.error('[print-queue] filter refresh failed:', e.message)), 10 * 60 * 1000);
}

app.listen(PORT, () => {
  console.log(`✅ SFTP Proxy running on port ${PORT}`);
});






































