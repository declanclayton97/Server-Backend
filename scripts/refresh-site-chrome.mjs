// Saves the tuffshop.co.uk header/footer used around the returns page, as a fallback
// for when the live site cannot be fetched from the server (Cloudflare challenges).
// Run after a website redesign:  node scripts/refresh-site-chrome.mjs
import fs from "fs";
import { fetchSitePage, extractChrome } from "../siteChrome.js";
const chrome = extractChrome(await fetchSitePage());
fs.writeFileSync(new URL("../returns-page/site-chrome.json", import.meta.url), JSON.stringify(chrome));
console.log(`saved: ${chrome.links.length} stylesheets, header ${chrome.header.length} chars, footer ${chrome.footer.length} chars`);
