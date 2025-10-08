// SFTP HTTPS Proxy for Codesandbox React mockup workflow
import express from "express";
import SFTPClient from "ssh2-sftp-client";
import dotenv from "dotenv";
import { Readable } from "stream";

dotenv.config();

const app = express();
const PORT = process.env.PORT || 3000;

// Brightpearl configuration
const BRIGHTPEARL_DATACENTER = process.env.BRIGHTPEARL_DATACENTER || 'use1';
const BRIGHTPEARL_ACCOUNT_ID = process.env.BRIGHTPEARL_ACCOUNT_ID;
const BRIGHTPEARL_API_TOKEN = process.env.BRIGHTPEARL_API_TOKEN;

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

    const proofRequiredStatusId = '34'; // Replace with your actual status ID
    
    const baseUrl = BRIGHTPEARL_DATACENTER === 'euw1' 
      ? 'https://euw1.brightpearlconnect.com'
      : 'https://use1.brightpearlconnect.com';
    
    const url = `${baseUrl}/public-api/${BRIGHTPEARL_ACCOUNT_ID}/order-service/order-search`;
    
    const response = await fetch(url, {
      method: 'POST',
      headers: {
        'brightpearl-app-ref': process.env.BRIGHTPEARL_APP_REF,
        'brightpearl-account-token': BRIGHTPEARL_API_TOKEN,
        'Content-Type': 'application/json'
      },
      body: JSON.stringify({
        filters: {
          orderStatusId: [proofRequiredStatusId] // Note: changed from { in: [...] } to just array
        },
        columns: ['orderId', 'orderReference', 'customerName', 'placedOn', 'deliveryDate'],
        sort: 'placedOn',
        sortDirection: 'DESC',
        pageSize: 50
      })
    });
    
    if (!response.ok) {
      const errorText = await response.text();
      console.error('Order search error:', errorText);
      return res.status(response.status).json({ error: errorText });
    }
    
    const data = await response.json();
    res.json(data.response);
  } catch (error) {
    console.error('Error fetching proof required orders:', error);
    res.status(500).json({ error: error.message });
  }
});

app.listen(PORT, () => {
  console.log(`✅ SFTP Proxy running on port ${PORT}`);
});























