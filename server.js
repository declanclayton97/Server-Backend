// SFTP HTTPS Proxy for Codesandbox React mockup workflow
// Configured for prodinfrargftp.blob.core.windows.net

import express from "express";
import SFTPClient from "ssh2-sftp-client";
import dotenv from "dotenv";
import path from "path";

dotenv.config();

const app = express();
const PORT = process.env.PORT || 3000;

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
    res.setHeader("Access-Control-Allow-Origin", "*");

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

app.listen(PORT, () => {
  console.log(`✅ SFTP Proxy running on port ${PORT}`);
});
