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
