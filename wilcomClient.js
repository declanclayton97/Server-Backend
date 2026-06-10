// Wilcom WorkspaceTrueSizer client.
//
// Converts Wilcom .EMB embroidery files to transparent TrueView PNGs by calling
// the same backend the TrueSizer web app uses (prod.api.wilcomworkspace.com).
// Authenticates against the WorkspaceCognito user pool with stored credentials
// (WILCOM_USERNAME / WILCOM_PASSWORD) and caches the ID token until it nears
// expiry, re-authenticating automatically.
//
// Env:
//   WILCOM_USERNAME   - Wilcom WorkspaceWorkspaceaccount email
//   WILCOM_PASSWORD   - that account's password
//   WILCOM_POOL_ID    - (optional) Cognito user pool id   [default us-west-2_RVg6q6lhC]
//   WILCOM_CLIENT_ID  - (optional) Cognito app client id   [default 62idsq3lkfp5o3s7bpgt4h199a]
import axios from "axios";
import pkg from "amazon-cognito-identity-js";

const { CognitoUserPool, CognitoUser, AuthenticationDetails } = pkg;

const POOL_ID = process.env.WILCOM_POOL_ID || "us-west-2_RVg6q6lhC";
const CLIENT_ID = process.env.WILCOM_CLIENT_ID || "62idsq3lkfp5o3s7bpgt4h199a";
const API_BASE = process.env.WILCOM_API_BASE || "https://prod.api.wilcomworkspace.com/prod";

// amazon-cognito-identity-js reaches for window.localStorage; provide an
// in-memory shim so it runs cleanly under Node.
class MemoryStorage {
  constructor() { this.store = {}; }
  setItem(k, v) { this.store[k] = String(v); }
  getItem(k) { return Object.prototype.hasOwnProperty.call(this.store, k) ? this.store[k] : null; }
  removeItem(k) { delete this.store[k]; }
  clear() { this.store = {}; }
}

const pool = new CognitoUserPool({ UserPoolId: POOL_ID, ClientId: CLIENT_ID, Storage: new MemoryStorage() });

let cached = null; // { idToken, expMs, session }

function jwtExpMs(jwt) {
  try {
    const payload = JSON.parse(Buffer.from(jwt.split(".")[1], "base64").toString("utf8"));
    return (payload.exp || 0) * 1000;
  } catch { return 0; }
}

function authenticate() {
  const Username = process.env.WILCOM_USERNAME;
  const Password = process.env.WILCOM_PASSWORD;
  if (!Username || !Password) {
    return Promise.reject(new Error("WILCOM_USERNAME / WILCOM_PASSWORD not set"));
  }
  const user = new CognitoUser({ Username, Pool: pool, Storage: new MemoryStorage() });
  const details = new AuthenticationDetails({ Username, Password });
  return new Promise((resolve, reject) => {
    user.authenticateUser(details, {
      onSuccess: (session) => resolve({ user, session }),
      onFailure: (err) => reject(new Error(`Wilcom login failed: ${err.message || err}`)),
      // A brand-new password / MFA challenge would land here — surface it clearly.
      newPasswordRequired: () => reject(new Error("Wilcom account requires a new password (set it in the web app first)")),
      mfaRequired: () => reject(new Error("Wilcom account has MFA enabled — token auto-login not supported")),
    });
  });
}

// Returns a valid ID token, refreshing/re-authenticating as needed.
export async function getIdToken() {
  const now = Date.now();
  if (cached && cached.expMs - now > 120_000) return cached.idToken; // >2 min headroom

  // Try a refresh-token exchange first (cheap); fall back to full SRP login.
  if (cached && cached.session && cached.user) {
    try {
      const refreshed = await new Promise((resolve, reject) => {
        cached.user.refreshSession(cached.session.getRefreshToken(), (err, s) =>
          err ? reject(err) : resolve(s)
        );
      });
      const idToken = refreshed.getIdToken().getJwtToken();
      cached = { idToken, expMs: jwtExpMs(idToken), session: refreshed, user: cached.user };
      return idToken;
    } catch { /* fall through to full login */ }
  }

  const { user, session } = await authenticate();
  const idToken = session.getIdToken().getJwtToken();
  cached = { idToken, expMs: jwtExpMs(idToken), session, user };
  return idToken;
}

/**
 * Convert a Wilcom .EMB (or other supported design file) to a transparent PNG.
 * @param {Buffer} designBuffer  raw design file bytes
 * @param {object} [opts]
 * @param {number} [opts.dpi=300]            output resolution, 96-300
 * @param {string} [opts.inputExt="EMB"]     source file extension
 * @returns {Promise<{png: Buffer, designInfo: object, widthMm: number, heightMm: number}>}
 */
export async function convertDesignToPng(designBuffer, opts = {}) {
  const dpi = Math.min(300, Math.max(96, opts.dpi || 300));
  const inputExt = (opts.inputExt || "EMB").toUpperCase();

  const payload = {
    DesignFileBase64: designBuffer.toString("base64"),
    InputFileExtension: inputExt,
    OutputFileExtension: "PNG",
    OutputFileName: "out.PNG",
    DPI: dpi,
    design: { transform: {}, recolor: {}, colorway: "" },
  };

  // Wilcom's gateway occasionally returns a transient 502/503/504 (cold/slow
  // backend); retry a few times with backoff before giving up.
  let data, lastErr;
  for (let attempt = 0; attempt < 3; attempt++) {
    try {
      const token = await getIdToken();
      const resp = await axios.post(`${API_BASE}/transform`, payload, {
        headers: {
          "content-type": "application/json; charset=UTF-8",
          authorization: `Bearer ${token}`,
          accept: "application/json",
          origin: "https://apps.wilcom.com",
          referer: "https://apps.wilcom.com/",
        },
        timeout: 90_000,
        maxBodyLength: Infinity,
        maxContentLength: Infinity,
      });
      data = resp.data;
      break;
    } catch (err) {
      lastErr = err;
      const status = err.response && err.response.status;
      const transient = !status || [429, 500, 502, 503, 504].includes(status) || err.code === "ECONNABORTED";
      if (!transient) throw err;
      await new Promise((r) => setTimeout(r, 1500 * (attempt + 1)));
    }
  }
  if (!data) throw new Error(`Wilcom transform failed: ${lastErr && lastErr.message}`);

  if (!data.ResultFileBase64) {
    throw new Error("Wilcom transform returned no image");
  }
  const png = Buffer.from(data.ResultFileBase64, "base64");
  const di = (data.designInfo && data.designInfo.design_info) || {};
  return { png, designInfo: di, widthMm: di.width || null, heightMm: di.height || null };
}

// CLI test:  node wilcomClient.js path/to/design.EMB [dpi]
if (import.meta.url === `file://${process.argv[1].replace(/\\/g, "/")}`) {
  try { (await import("dotenv")).default.config(); } catch { /* dotenv optional */ }
  const fs = await import("node:fs");
  const file = process.argv[2];
  const dpi = parseInt(process.argv[3] || "300", 10);
  if (!file) { console.error("usage: node wilcomClient.js <design.EMB> [dpi]"); process.exit(1); }
  try {
    const buf = fs.readFileSync(file);
    const ext = (file.split(".").pop() || "EMB").toUpperCase();
    const t0 = Date.now();
    const { png, widthMm, heightMm, designInfo } = await convertDesignToPng(buf, { dpi, inputExt: ext });
    const out = file.replace(/\.[^.]+$/, "") + `.${dpi}dpi.png`;
    fs.writeFileSync(out, png);
    console.log(`OK in ${Date.now() - t0}ms -> ${out}`);
    console.log(`  ${png.length} bytes, ${widthMm}x${heightMm}mm, ${designInfo.num_colours} colour(s), ${designInfo.num_stitches} stitches`);
  } catch (e) {
    console.error("FAILED:", e.message);
    process.exit(1);
  }
}
