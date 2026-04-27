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

// Name Badges: list orders in channel 22 where custom field PCF_BADGE = "Yes"
app.get('/api/brightpearl/name-badges', async (req, res) => {
  try {
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

    // Search orders in channel 22 (matches proof-required pattern)
    const searchUrl = `${baseUrl}/public-api/${BRIGHTPEARL_ACCOUNT_ID}/order-service/order-search?channelId=22&pageSize=50&firstResult=1`;
    const searchResp = await fetch(searchUrl, { method: 'GET', headers });

    if (!searchResp.ok) {
      const errorText = await searchResp.text();
      console.error('Name badges order-search error:', errorText);
      return res.status(searchResp.status).json({ error: errorText });
    }

    const searchData = await searchResp.json();
    if (!searchData.response?.results?.length) {
      return res.json([]);
    }

    // Brightpearl returns results as arrays of column values; first column is order ID
    const rows = searchData.response.results;
    const orderIds = Array.isArray(rows[0]) ? rows.map(r => r[0]) : rows;

    // Fetch custom fields for each order and filter to PCF_BADGE = "Yes"
    const matches = [];
    await Promise.all(orderIds.map(async (orderId) => {
      try {
        const cfUrl = `${baseUrl}/public-api/${BRIGHTPEARL_ACCOUNT_ID}/order-service/order/${orderId}/custom-field`;
        const cfResp = await fetch(cfUrl, { method: 'GET', headers });
        if (!cfResp.ok) return;
        const cfData = await cfResp.json();
        const fields = cfData.response || cfData || {};
        const badgeValue = fields.PCF_BADGE;
        if (typeof badgeValue === 'string' && badgeValue.toLowerCase() === 'yes') {
          matches.push(orderId);
        }
      } catch (err) {
        console.error(`Error checking PCF_BADGE for order ${orderId}:`, err.message);
      }
    }));

    if (matches.length === 0) {
      return res.json([]);
    }

    // Fetch order details for the matching orders
    const detailsUrl = `${baseUrl}/public-api/${BRIGHTPEARL_ACCOUNT_ID}/order-service/order/${matches.join(',')}`;
    const detailsResp = await fetch(detailsUrl, { method: 'GET', headers });

    if (!detailsResp.ok) {
      const errorText = await detailsResp.text();
      console.error('Name badges details error:', errorText);
      return res.status(detailsResp.status).json({ error: errorText });
    }

    const detailsData = await detailsResp.json();
    const orders = (detailsData.response || []).map(order => ({
      orderId: order.id,
      orderReference: order.reference,
      customerName: order.parties?.customer?.companyName ||
                    order.parties?.customer?.contactName ||
                    order.parties?.delivery?.addressFullName ||
                    order.parties?.customer?.addressFullName ||
                    'Unknown',
      placedOn: order.placedOn,
      deliveryDate: order.delivery?.deliveryDate || null
    }));

    res.json(orders);
  } catch (error) {
    console.error('Error fetching name badge orders:', error);
    res.status(500).json({ error: error.message });
  }
});

// Update a custom field on a Brightpearl sales order
// Body: { "PCF_BADGE": "No" } — keys are PCF codes, values are the new field values
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

    const response = await fetch(url, {
      method: 'PATCH',
      headers: {
        'brightpearl-app-ref': process.env.BRIGHTPEARL_APP_REF,
        'brightpearl-account-token': BRIGHTPEARL_API_TOKEN,
        'Content-Type': 'application/json'
      },
      body: JSON.stringify(req.body)
    });

    if (!response.ok) {
      const errorText = await response.text();
      console.error(`Custom field update failed for order ${orderId}:`, errorText);
      return res.status(response.status).json({ error: errorText });
    }

    res.json({ success: true });
  } catch (error) {
    console.error('Custom field update error:', error);
    res.status(500).json({ error: error.message });
  }
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






































