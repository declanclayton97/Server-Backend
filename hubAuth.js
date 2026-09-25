// hubAuth.js — who is using the Sales Hub.
//
// The point is identification, not security: eight salespeople share one URL,
// every WhatsApp send is stamped with a name, and each of them wants to see
// their own work rather than everyone's. A shared "type your name in this box"
// header field did that badly — it was blank half the time and anyone could
// type anyone.
//
// What this is NOT: protection for the backend. Every /api/* route stays open,
// because the purchasing hub and the React app call the same backend and would
// break. Treat a session as a claim about WHO, not permission to do anything.
//
// Pure functions here; the storage and routes live in hubAuthRoutes.js.

import crypto from "node:crypto";

// Names are matched loosely so "Dan", "dan" and "Dan " are one person, while
// the display name keeps whatever capitalisation they typed - it goes out on
// customer messages.
export const nameKey = (s) => String(s || "").trim().toLowerCase().replace(/\s+/g, " ");

export function validateName(name) {
  const clean = String(name || "").trim().replace(/\s+/g, " ");
  if (clean.length < 2) return { ok: false, error: "Enter your name (at least 2 characters)" };
  if (clean.length > 40) return { ok: false, error: "That name is too long" };
  // It ends up in emails and WhatsApp messages, so keep it to something that
  // reads as a name rather than markup.
  if (!/^[A-Za-z][A-Za-z '\-.]*$/.test(clean)) {
    return { ok: false, error: "Letters, spaces, apostrophes and hyphens only" };
  }
  return { ok: true, name: clean, key: nameKey(clean) };
}

export function validatePassword(pw) {
  const s = String(pw == null ? "" : pw);
  // Deliberately gentle. This is a name badge, and a rule nobody can satisfy
  // just means eight people all choose "Password1".
  if (s.length < 6) return { ok: false, error: "Password must be at least 6 characters" };
  if (s.length > 200) return { ok: false, error: "That password is too long" };
  return { ok: true };
}

// scrypt, from node's own crypto. No dependency, and far better than the
// "it doesn't need to be secure" version of this, which is a plaintext column
// that later turns up in a database export.
export function hashPassword(password, salt = crypto.randomBytes(16).toString("hex")) {
  const hash = crypto.scryptSync(String(password), salt, 64).toString("hex");
  return { salt, hash };
}

export function verifyPassword(password, salt, expectedHash) {
  if (!salt || !expectedHash) return false;
  const actual = Buffer.from(crypto.scryptSync(String(password), salt, 64).toString("hex"));
  const expected = Buffer.from(String(expectedHash));
  // Lengths must match before timingSafeEqual, and it throws if they do not.
  if (actual.length !== expected.length) return false;
  return crypto.timingSafeEqual(actual, expected);
}

// Sessions are a random token; only its hash is stored, so a leaked database
// does not hand over working logins. Long-lived on purpose - being asked to log
// in twice a day is how people end up sharing one account.
export const SESSION_DAYS = 60;
export const newSessionToken = () => crypto.randomBytes(32).toString("hex");
export const hashToken = (t) => crypto.createHash("sha256").update(String(t)).digest("hex");
export const sessionExpiry = (from = new Date()) =>
  new Date(from.getTime() + SESSION_DAYS * 24 * 3600 * 1000);

// "Bearer <token>" or a bare token, from either the header or a query string.
export function tokenFromRequest(req) {
  const auth = String((req && req.get && req.get("authorization")) || "");
  const m = auth.match(/^Bearer\s+(.+)$/i);
  if (m) return m[1].trim();
  const q = req && req.query && req.query.token;
  return q ? String(q) : null;
}

// ---------------------------------------------------------------------------
// Authenticator codes (TOTP, RFC 6238) — what Microsoft / Google Authenticator
// show. The hub reads a customer mailbox and sends as sales@, so a password on
// its own is no longer enough. Standard parameters every app understands:
// SHA-1, 6 digits, 30-second steps.
// ---------------------------------------------------------------------------
const B32 = "ABCDEFGHIJKLMNOPQRSTUVWXYZ234567";
export function base32Encode(buf) {
  let bits = 0, value = 0, out = "";
  for (const byte of buf) {
    value = (value << 8) | byte; bits += 8;
    while (bits >= 5) { out += B32[(value >>> (bits - 5)) & 31]; bits -= 5; }
  }
  if (bits > 0) out += B32[(value << (5 - bits)) & 31];
  return out;
}
export function base32Decode(str) {
  const s = String(str || "").toUpperCase().replace(/[^A-Z2-7]/g, "");
  let bits = 0, value = 0; const out = [];
  for (const c of s) {
    value = (value << 5) | B32.indexOf(c); bits += 5;
    if (bits >= 8) { out.push((value >>> (bits - 8)) & 255); bits -= 8; }
  }
  return Buffer.from(out);
}

export const TOTP_STEP_SEC = 30;
export const newTotpSecret = () => base32Encode(crypto.randomBytes(20));
export const totpStep = (now = Date.now()) => Math.floor(now / 1000 / TOTP_STEP_SEC);

export function totpCode(secret, step) {
  const msg = Buffer.alloc(8);
  msg.writeBigUInt64BE(BigInt(step));
  const h = crypto.createHmac("sha1", base32Decode(secret)).update(msg).digest();
  const o = h[h.length - 1] & 15;
  const n = ((h[o] & 127) << 24) | (h[o + 1] << 16) | (h[o + 2] << 8) | h[o + 3];
  return String(n % 1e6).padStart(6, "0");
}

/**
 * The step a code matches, or null. One step either side is accepted, for
 * phone clocks that drift. The caller refuses any step at or below the last one
 * used, so a code read over someone's shoulder cannot be replayed.
 */
export function verifyTotp(secret, code, now = Date.now(), window = 1) {
  const c = String(code || "").replace(/\s+/g, "");
  if (!/^\d{6}$/.test(c) || !secret) return null;
  const cur = totpStep(now);
  for (let d = -window; d <= window; d++) {
    const exp = Buffer.from(totpCode(secret, cur + d)), got = Buffer.from(c);
    if (crypto.timingSafeEqual(exp, got)) return cur + d;
  }
  return null;
}

export const otpauthUrl = (name, secret, issuer = "Tuffshop Sales Hub") =>
  `otpauth://totp/${encodeURIComponent(issuer)}:${encodeURIComponent(name)}` +
  `?secret=${secret}&issuer=${encodeURIComponent(issuer)}&algorithm=SHA1&digits=6&period=${TOTP_STEP_SEC}`;

// Secrets at rest: AES-256-GCM under HUB_TOTP_KEY when it is set, so a copy of
// the database alone does not give away everyone's codes. Without the key they
// are stored as-is (and a warning is logged) rather than locking everyone out.
function totpKey() {
  const k = process.env.HUB_TOTP_KEY;
  return k ? crypto.createHash("sha256").update(k).digest() : null;
}
export function sealSecret(secret) {
  const key = totpKey();
  if (!key) return secret;
  const iv = crypto.randomBytes(12);
  const c = crypto.createCipheriv("aes-256-gcm", key, iv);
  const enc = Buffer.concat([c.update(secret, "utf8"), c.final()]);
  return `enc:${iv.toString("base64")}:${c.getAuthTag().toString("base64")}:${enc.toString("base64")}`;
}
export function openSecret(stored) {
  const s = String(stored || "");
  if (!s.startsWith("enc:")) return s || null;
  const key = totpKey();
  if (!key) throw new Error("HUB_TOTP_KEY is not set, so the stored authenticator secrets cannot be read");
  const [, iv, tag, data] = s.split(":");
  const d = crypto.createDecipheriv("aes-256-gcm", key, Buffer.from(iv, "base64"));
  d.setAuthTag(Buffer.from(tag, "base64"));
  return Buffer.concat([d.update(Buffer.from(data, "base64")), d.final()]).toString("utf8");
}
