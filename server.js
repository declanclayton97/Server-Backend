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

// Image proxy route
app.get("/fetch-image", async (req, res) => {
  const imageUrl = req.query.url;
  if (!imageUrl) {
    return res.status(400).send('Missing "url" query parameter');
  }
  
  try {
    const response = await fetch(imageUrl);
    if (!response.ok) throw new Error(`Failed to fetch ${imageUrl}`);
    
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
    const baseUrl = BRIGHTPEARL_DATACENTER === 'euw1'
      ? 'https://euw1.brightpearlconnect.com'
      : 'https://use1.brightpearlconnect.com';

    const url = `${baseUrl}/public-api/${BRIGHTPEARL_ACCOUNT_ID}/order-service/order-search?contactId=${contactId}&pageSize=200&firstResult=1`;

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
        pdf_data BYTEA NOT NULL,
        logo_positions JSONB NOT NULL,
        status VARCHAR(20) DEFAULT 'pending',
        created_at TIMESTAMP DEFAULT NOW(),
        completed_at TIMESTAMP
      );
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
    const { pdfBase64, logoPositions, orderNumber, customerName, recipientName } = req.body;

    if (!pdfBase64 || !logoPositions?.length) {
      return res.status(400).json({ error: "pdfBase64 and logoPositions are required" });
    }

    const pdfBuffer = Buffer.from(pdfBase64, "base64");

    const sessionResult = await pool.query(
      `INSERT INTO approval_sessions (order_number, customer_name, recipient_name, pdf_data, logo_positions)
       VALUES ($1, $2, $3, $4, $5)
       RETURNING id, status, created_at`,
      [orderNumber || null, customerName || null, recipientName || null, pdfBuffer, JSON.stringify(logoPositions)]
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
              status, created_at, completed_at
       FROM approval_sessions WHERE id = $1`,
      [sessionId]
    );

    if (sessionResult.rows.length === 0) {
      return res.status(404).json({ error: "Session not found" });
    }

    const session = sessionResult.rows[0];

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
    const { items } = req.body;

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
      await pool.query(
        `UPDATE approval_sessions SET status = $1, completed_at = NOW() WHERE id = $2`,
        [sessionStatus, sessionId]
      );
    }

    res.json({ success: true, sessionStatus });
  } catch (err) {
    console.error("Submit approval error:", err);
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
    let query = `SELECT id, order_number, customer_name, recipient_name, status, created_at, completed_at
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

    query += ` ORDER BY created_at DESC LIMIT 50`;

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






































