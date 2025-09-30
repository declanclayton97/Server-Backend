// SFTP HTTPS Proxy for Codesandbox React mockup workflow
// Configured for prodinfrargftp.blob.core.windows.net
import express from "express";
import SFTPClient from "ssh2-sftp-client";
import dotenv from "dotenv";
import path from "path";
import axios from "axios"; // Add this import
import { Readable } from "stream";

dotenv.config();

const app = express();
const PORT = process.env.PORT || 3000;

// Add Brightpearl configuration
const BRIGHTPEARL_DATACENTER = process.env.BRIGHTPEARL_DATACENTER || 'use1';
const BRIGHTPEARL_ACCOUNT_ID = process.env.BRIGHTPEARL_ACCOUNT_ID;
const BRIGHTPEARL_API_TOKEN = process.env.BRIGHTPEARL_API_TOKEN;

// Add CORS middleware for all routes
app.use((req, res, next) => {
  res.setHeader("Access-Control-Allow-Origin", "*");
  res.setHeader("Access-Control-Allow-Methods", "GET, POST, OPTIONS");
  res.setHeader("Access-Control-Allow-Headers", "Content-Type");
  if (req.method === "OPTIONS") {
    return res.sendStatus(200);
  }
  next();
});

// Your existing SFTP route
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

app.get("/", (req, res) => {
  res.send("SFTP Proxy for Mockup Sheets is running");
});

// Your existing image proxy
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
        while (true) {
          const { done, value } = await reader.read();
          if (done) break;
          this.push(value);
        }
        this.push(null);
      },
    });
    stream.pipe(res);
  } catch (error) {
    console.error(`Error proxying image from ${imageUrl}:`, error.message);
    res.status(500).send(`Error proxying image: ${error.message}`);
  }
});

// ADD NEW BRIGHTPEARL ENDPOINTS HERE

// Test endpoint
app.get("/api/brightpearl/test", (req, res) => {
  res.json({ 
    status: 'connected',
    configured: !!BRIGHTPEARL_API_TOKEN,
    datacenter: BRIGHTPEARL_DATACENTER,
    accountId: BRIGHTPEARL_ACCOUNT_ID ? 'Set' : 'Not set'
  });
});

// Fetch order endpoint
app.get("/api/brightpearl/order/:orderId", async (req, res) => {
  try {
    const { orderId } = req.params;
    console.log('Fetching Brightpearl order:', orderId);
    
    if (!BRIGHTPEARL_API_TOKEN || !BRIGHTPEARL_ACCOUNT_ID) {
      throw new Error('Brightpearl credentials not configured');
    }
    
    const url = `https://api-${BRIGHTPEARL_DATACENTER}.brightpearl.com/public-api/${BRIGHTPEARL_ACCOUNT_ID}/order/${orderId}`;
    
    const response = await axios.get(url, {
      headers: {
        'Authorization': `Bearer ${BRIGHTPEARL_API_TOKEN}`,
        'brightpearl-app-ref': 'mockup-sheets',
        'Content-Type': 'application/json'
      }
    });
    
    console.log('Order fetched successfully');
    res.json(response.data);
  } catch (error) {
    console.error('Brightpearl error:', error.response?.data || error.message);
    res.status(error.response?.status || 500).json({
      error: error.response?.data?.errors?.[0]?.message || error.message
    });
  }
});

// Fetch product details endpoint
app.get("/api/brightpearl/product/:productId", async (req, res) => {
  try {
    const { productId } = req.params;
    
    if (!BRIGHTPEARL_API_TOKEN || !BRIGHTPEARL_ACCOUNT_ID) {
      throw new Error('Brightpearl credentials not configured');
    }
    
    const url = `https://api-${BRIGHTPEARL_DATACENTER}.brightpearl.com/public-api/${BRIGHTPEARL_ACCOUNT_ID}/product/${productId}`;
    
    const response = await axios.get(url, {
      headers: {
        'Authorization': `Bearer ${BRIGHTPEARL_API_TOKEN}`,
        'brightpearl-app-ref': 'mockup-sheets',
        'Content-Type': 'application/json'
      }
    });
    
    res.json(response.data);
  } catch (error) {
    console.error('Product fetch error:', error.response?.data || error.message);
    res.status(error.response?.status || 500).json({
      error: error.response?.data?.errors?.[0]?.message || error.message
    });
  }
});

app.listen(PORT, () => {
  console.log(`✅ SFTP Proxy running on port ${PORT}`);
});
