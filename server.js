// SFTP HTTPS Proxy for Codesandbox React mockup workflow
import express from "express";
import SFTPClient from "ssh2-sftp-client";
import dotenv from "dotenv";
import { Readable } from "stream";
import DocuSignService from './docusignService.js';
import cors from 'cors';

dotenv.config();

const app = express();
const PORT = process.env.PORT || 3000;

// Brightpearl configuration
const BRIGHTPEARL_DATACENTER = process.env.BRIGHTPEARL_DATACENTER || 'use1';
const BRIGHTPEARL_ACCOUNT_ID = process.env.BRIGHTPEARL_ACCOUNT_ID;
const BRIGHTPEARL_API_TOKEN = process.env.BRIGHTPEARL_API_TOKEN;

app.use(express.json({ limit: '50mb' }));
app.use(express.urlencoded({ limit: '50mb', extended: true }));

// CORS middleware
app.use((req, res, next) => {
  res.setHeader("Access-Control-Allow-Origin", "*");
  res.setHeader("Access-Control-Allow-Methods", "GET, POST, OPTIONS");
  res.setHeader("Access-Control-Allow-Headers", "Content-Type");
  if (req.method === "OPTIONS") {
    return res.sendStatus(200);
  }
  next();
});

app.use(cors());

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

// Test endpoint
app.get('/test-docusign', (req, res) => {
  res.json({ 
    message: 'DocuSign endpoint is reachable',
    envVarsSet: {
      accountId: !!process.env.DOCUSIGN_ACCOUNT_ID,
      integrationKey: !!process.env.DOCUSIGN_INTEGRATION_KEY,
      userId: !!process.env.DOCUSIGN_USER_ID,
      privateKey: !!process.env.DOCUSIGN_PRIVATE_KEY
    }
  });
});

// Main DocuSign endpoint
app.post('/send-to-docusign', async (req, res) => {
  console.log('DocuSign endpoint hit!');
  
  try {
    const { pdfBase64, recipientEmail, recipientName, logoPositions } = req.body;
    
    if (!pdfBase64 || !recipientEmail || !recipientName) {
      return res.status(400).json({ 
        success: false,
        error: 'Missing required fields' 
      });
    }
    
    const pdfBytes = Buffer.from(pdfBase64, 'base64');
    
    const signaturePositions = logoPositions.map(pos => ({
      page: pos.page || 1,
      x: pos.x || 100,
      y: pos.y || 100
    }));
    
    const result = await docuSignService.createEnvelopeWithSignatureFields(
      pdfBytes,
      recipientEmail,
      recipientName,
      signaturePositions
    );
    
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

app.listen(PORT, () => {
  console.log(`✅ SFTP Proxy running on port ${PORT}`);
});






























