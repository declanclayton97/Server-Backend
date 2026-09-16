// Tests for hubAuth.js.  Run with:  node hubAuth.test.js
import {
  nameKey, validateName, validatePassword,
  hashPassword, verifyPassword,
  newSessionToken, hashToken, sessionExpiry, tokenFromRequest,
} from "./hubAuth.js";

let pass = 0, fail = 0;
function assertEq(label, actual, expected) {
  const ok = JSON.stringify(actual) === JSON.stringify(expected);
  if (ok) pass++;
  else { fail++; console.error(`  x ${label}\n      expected ${JSON.stringify(expected)}\n      got      ${JSON.stringify(actual)}`); }
}
const assertTrue = (l, a) => assertEq(l, !!a, true);
const assertFalse = (l, a) => assertEq(l, !!a, false);

// --- names ------------------------------------------------------------------
assertEq("same person however they type it", nameKey(" Dan  Ford "), "dan ford");
assertEq("case folded", nameKey("DAN"), nameKey("dan"));
assertEq("display name keeps their capitals", validateName("  dan  Ford ").name, "dan Ford");
assertEq("but the key does not", validateName("  dan  Ford ").key, "dan ford");
assertTrue("apostrophes are names too", validateName("Siobhan O'Neill").ok);
assertTrue("hyphens too", validateName("Jack Ellis-Haynes").ok);
assertFalse("too short", validateName("D").ok);
assertFalse("no markup in a name", validateName("<script>alert(1)</script>").ok);
assertFalse("no digits", validateName("Dan2").ok);
assertFalse("empty", validateName("   ").ok);

// --- passwords --------------------------------------------------------------
assertFalse("too short", validatePassword("abc12").ok);
assertTrue("six is enough for a name badge", validatePassword("abc123").ok);
assertFalse("absurdly long is refused", validatePassword("x".repeat(500)).ok);

// --- hashing ----------------------------------------------------------------
const h = hashPassword("correct horse");
assertTrue("right password verifies", verifyPassword("correct horse", h.salt, h.hash));
assertFalse("wrong password does not", verifyPassword("Correct horse", h.salt, h.hash));
assertFalse("wrong salt does not", verifyPassword("correct horse", "0".repeat(32), h.hash));
assertFalse("missing hash is a refusal, not a crash", verifyPassword("x", h.salt, null));
assertFalse("missing salt is a refusal", verifyPassword("x", null, h.hash));
// A stored hash of a different length must not throw inside timingSafeEqual.
assertFalse("short stored hash does not throw", verifyPassword("correct horse", h.salt, "abc"));
// Same password, different salt, different hash — so one leaked row tells you
// nothing about anyone else who picked the same password.
assertFalse("salts differ per user", hashPassword("same").hash === hashPassword("same").hash);

// --- sessions ---------------------------------------------------------------
const t = newSessionToken();
assertEq("token is 64 hex chars", t.length, 64);
assertFalse("two tokens are not the same", newSessionToken() === newSessionToken());
assertEq("hash is stable", hashToken(t), hashToken(t));
assertFalse("the token is not its own hash", hashToken(t) === t);
assertTrue("expiry is in the future", sessionExpiry() > new Date());

// --- reading the token off a request ---------------------------------------
const req = (headers, query) => ({ get: (k) => headers[k.toLowerCase()], query: query || {} });
assertEq("Bearer header", tokenFromRequest(req({ authorization: "Bearer abc123" })), "abc123");
assertEq("case-insensitive scheme", tokenFromRequest(req({ authorization: "bearer abc123" })), "abc123");
assertEq("query fallback", tokenFromRequest(req({}, { token: "abc123" })), "abc123");
assertEq("nothing at all", tokenFromRequest(req({}, {})), null);

console.log(`\n${pass} passed, ${fail} failed`);
process.exit(fail ? 1 : 0);
