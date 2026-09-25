// hubAuthRoutes.js — register / log in / who am I, for the Sales Hub.
//
// Mounted from server.js with one call, like salesHubRoutes, so several
// sessions can edit that file without colliding.
//
// Again: this identifies a person, it does not protect the API. Every other
// /api/* route stays open because the purchasing hub and the React app share
// this backend.

import {
  validateName, validatePassword, nameKey,
  hashPassword, verifyPassword,
  newSessionToken, hashToken, sessionExpiry, tokenFromRequest, SESSION_DAYS,
  newTotpSecret, verifyTotp, otpauthUrl, sealSecret, openSecret,
} from "./hubAuth.js";

export function registerHubAuthRoutes(app, deps) {
  const { useDatabase } = deps;
  const getPool = () => (typeof deps.pool === "function" ? deps.pool() : deps.pool);

  let ready = null;
  async function ensureTables() {
    if (ready) return ready;
    ready = getPool().query(`
      CREATE TABLE IF NOT EXISTS hub_users (
        name_key    TEXT PRIMARY KEY,
        display_name TEXT NOT NULL,
        pw_salt     TEXT NOT NULL,
        pw_hash     TEXT NOT NULL,
        created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        last_seen_at TIMESTAMPTZ
      );
      -- Only the HASH of a session token is stored, so a database dump does not
      -- hand anyone a working login.
      CREATE TABLE IF NOT EXISTS hub_sessions (
        token_hash TEXT PRIMARY KEY,
        name_key   TEXT NOT NULL REFERENCES hub_users(name_key) ON DELETE CASCADE,
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        expires_at TIMESTAMPTZ NOT NULL
      );
      CREATE INDEX IF NOT EXISTS idx_hub_sessions_user ON hub_sessions(name_key);
      -- Authenticator (TOTP). A session only counts once its code step is done.
      ALTER TABLE hub_users ADD COLUMN IF NOT EXISTS totp_secret TEXT;
      ALTER TABLE hub_users ADD COLUMN IF NOT EXISTS totp_last_step BIGINT;
      ALTER TABLE hub_sessions ADD COLUMN IF NOT EXISTS mfa BOOLEAN NOT NULL DEFAULT FALSE;
      -- Between "password right" and "code right". Short-lived, few attempts.
      CREATE TABLE IF NOT EXISTS hub_pending_logins (
        token_hash TEXT PRIMARY KEY,
        name_key   TEXT NOT NULL REFERENCES hub_users(name_key) ON DELETE CASCADE,
        purpose    TEXT NOT NULL,            -- 'code' | 'enrol'
        secret     TEXT,                     -- the new secret while enrolling (sealed)
        attempts   INT NOT NULL DEFAULT 0,
        expires_at TIMESTAMPTZ NOT NULL
      );
    `);
    // Sessions issued under a longer limit are held to the current one, counted from
    // when they signed in — shortening SESSION_DAYS applies to everyone, not just new sign-ins.
    ready.then(() => getPool().query(
      `UPDATE hub_sessions SET expires_at = created_at + ($1 || ' days')::interval
        WHERE expires_at > created_at + ($1 || ' days')::interval`, [String(SESSION_DAYS)]))
      .catch((e) => console.error("[hub-auth] session cap failed:", e.message));
    if (!process.env.HUB_TOTP_KEY) console.warn("[hub-auth] HUB_TOTP_KEY not set — authenticator secrets are stored unencrypted");
    return ready;
  }

  const needDb = (res) => {
    if (useDatabase && getPool()) return true;
    res.status(503).json({ error: "The hub database is not configured, so sign-in is unavailable" });
    return false;
  };

  async function userFromToken(req) {
    const token = tokenFromRequest(req);
    if (!token) return null;
    await ensureTables();
    const r = await getPool().query(
      `SELECT u.name_key, u.display_name
         FROM hub_sessions s JOIN hub_users u ON u.name_key = s.name_key
        WHERE s.token_hash = $1 AND s.expires_at > NOW() AND s.mfa`,
      [hashToken(token)]
    );
    if (!r.rowCount) return null;
    getPool().query(`UPDATE hub_users SET last_seen_at = NOW() WHERE name_key = $1`, [r.rows[0].name_key])
      .catch(() => {});
    return { key: r.rows[0].name_key, name: r.rows[0].display_name };
  }
  // Exposed so other routes can stamp "who did this" without re-implementing it.
  app.locals.hubUserFromToken = userFromToken;

  /**
   * Express middleware: refuse unless a real hub session is presented.
   *
   * Used on the routes only the Sales Hub calls. It is NOT applied to the older
   * shared endpoints — the React app and the purchasing hub call those and have
   * no session — which is why the WhatsApp routes gate on channel=sales instead
   * of blanket-refusing.
   *
   * Fails closed: if the database is down we cannot tell who this is, and the
   * right answer to "I do not know who you are" is no.
   */
  app.locals.requireHubUser = async function requireHubUser(req, res, next) {
    try {
      const u = await userFromToken(req);
      if (!u) return res.status(401).json({ error: "Sign in to the Sales Hub first" });
      req.hubUser = u;
      next();
    } catch (e) {
      console.error("[hub-auth] session check failed:", e.message);
      res.status(503).json({ error: "Could not verify your sign-in" });
    }
  };

  // Only ever called once the authenticator code has been checked.
  async function issueSession(key) {
    const token = newSessionToken();
    await getPool().query(
      `INSERT INTO hub_sessions (token_hash, name_key, expires_at, mfa) VALUES ($1, $2, $3, TRUE)`,
      [hashToken(token), key, sessionExpiry()]
    );
    // Tidy expired rows opportunistically rather than running a sweeper.
    getPool().query(`DELETE FROM hub_sessions WHERE expires_at < NOW()`).catch(() => {});
    return token;
  }

  const PENDING_MINUTES = 5, PENDING_ATTEMPTS = 5;

  // Password was right: hand back a short-lived ticket for the code step. Someone
  // with no authenticator yet gets a fresh secret to scan instead.
  async function startSecondStep(user) {
    const ticket = newSessionToken();
    const enrolling = !user.totp_secret;
    const secret = enrolling ? newTotpSecret() : null;
    getPool().query(`DELETE FROM hub_pending_logins WHERE expires_at < NOW()`).catch(() => {});
    await getPool().query(
      `INSERT INTO hub_pending_logins (token_hash, name_key, purpose, secret, expires_at)
       VALUES ($1, $2, $3, $4, NOW() + ($5 || ' minutes')::interval)`,
      [hashToken(ticket), user.name_key, enrolling ? "enrol" : "code", secret ? sealSecret(secret) : null, String(PENDING_MINUTES)]
    );
    return enrolling
      ? { mfa: "enrol", ticket, name: user.display_name, secret, otpauth: otpauthUrl(user.display_name, secret) }
      : { mfa: "code", ticket, name: user.display_name };
  }

  // Who has an account. Lets the sign-in screen offer names instead of making
  // people guess their own spelling, and shows Dec who has registered.
  app.get("/api/hub/users", async (req, res) => {
    if (!needDb(res)) return;
    try {
      await ensureTables();
      const r = await getPool().query(
        `SELECT display_name, created_at, last_seen_at FROM hub_users ORDER BY display_name`
      );
      res.json({ users: r.rows.map((u) => ({ name: u.display_name, createdAt: u.created_at, lastSeenAt: u.last_seen_at })) });
    } catch (e) { res.status(500).json({ error: e.message }); }
  });

  // Tells the sign-in screen whether it may offer "create an account" at all,
  // so somebody without the code is not invited to fill in a form that cannot
  // succeed.
  app.get("/api/hub/signup-open", (req, res) => {
    res.json({ open: !!process.env.HUB_SIGNUP_CODE });
  });

  // First time someone opens the hub.
  app.post("/api/hub/register", async (req, res) => {
    if (!needDb(res)) return;
    const b = req.body || {};

    // Registration needs the shared code. FAIL CLOSED: with no HUB_SIGNUP_CODE
    // set, nobody can register at all. That is deliberate — this hub will soon
    // read a mailbox and send as sales@, and "anyone with the link can make an
    // account" stops being acceptable the moment that is true.
    const code = process.env.HUB_SIGNUP_CODE;
    if (!code) {
      return res.status(503).json({
        error: "Sign-up is closed. Set HUB_SIGNUP_CODE on the backend to let new people register.",
      });
    }
    if (String(b.authCode || "").trim() !== code) {
      return res.status(403).json({ error: "That sign-up code is not right. Ask whoever sent you the link." });
    }

    const n = validateName(b.name);
    if (!n.ok) return res.status(400).json({ error: n.error });
    const p = validatePassword(b.password);
    if (!p.ok) return res.status(400).json({ error: p.error });
    try {
      await ensureTables();
      const { salt, hash } = hashPassword(b.password);
      const r = await getPool().query(
        `INSERT INTO hub_users (name_key, display_name, pw_salt, pw_hash)
         VALUES ($1, $2, $3, $4) ON CONFLICT (name_key) DO NOTHING RETURNING name_key`,
        [n.key, n.name, salt, hash]
      );
      if (!r.rowCount) {
        // Taken. Say so plainly — the alternative is somebody quietly
        // overwriting a colleague's account by picking the same name.
        return res.status(409).json({ error: `"${n.name}" already has an account. Sign in instead, or add your surname.` });
      }
      res.json(await startSecondStep({ name_key: n.key, display_name: n.name, totp_secret: null }));
    } catch (e) { res.status(500).json({ error: e.message }); }
  });

  app.post("/api/hub/login", async (req, res) => {
    if (!needDb(res)) return;
    const b = req.body || {};
    const key = nameKey(b.name);
    if (!key) return res.status(400).json({ error: "Enter your name" });
    try {
      await ensureTables();
      const r = await getPool().query(
        `SELECT name_key, display_name, pw_salt, pw_hash, totp_secret FROM hub_users WHERE name_key = $1`, [key]
      );
      const u = r.rows[0];
      // Same message whether the name is unknown or the password is wrong.
      // Not for secrecy — this is a name badge — but because "no such user"
      // invites people to register a second account under a variant spelling.
      if (!u || !verifyPassword(b.password, u.pw_salt, u.pw_hash)) {
        return res.status(401).json({ error: "That name and password do not match" });
      }
      // Right password is only half of it now.
      res.json(await startSecondStep(u));
    } catch (e) { res.status(500).json({ error: e.message }); }
  });

  // POST /api/hub/mfa { ticket, code } — the authenticator step. On enrolment the
  // first good code is what switches the authenticator on.
  app.post("/api/hub/mfa", async (req, res) => {
    if (!needDb(res)) return;
    const b = req.body || {};
    if (!b.ticket) return res.status(400).json({ error: "Sign in again" });
    try {
      await ensureTables();
      // Count the attempt BEFORE checking, in the same statement that reads the
      // ticket, so parallel guesses cannot all slip under the limit.
      const r = await getPool().query(
        `UPDATE hub_pending_logins p SET attempts = attempts + 1
          FROM hub_users u
         WHERE p.token_hash = $1 AND u.name_key = p.name_key AND p.expires_at > NOW()
         RETURNING p.name_key, p.purpose, p.secret, p.attempts, u.display_name, u.totp_secret, u.totp_last_step`,
        [hashToken(b.ticket)]);
      const t = r.rows[0];
      if (!t) return res.status(401).json({ error: "That took too long — sign in again", restart: true });
      if (t.attempts > PENDING_ATTEMPTS) {
        await getPool().query(`DELETE FROM hub_pending_logins WHERE token_hash = $1`, [hashToken(b.ticket)]);
        return res.status(429).json({ error: "Too many wrong codes — sign in again", restart: true });
      }
      const secret = openSecret(t.purpose === "enrol" ? t.secret : t.totp_secret);
      const step = verifyTotp(secret, b.code);
      if (step == null) return res.status(401).json({ error: "That code is not right — check the app and try the current one" });
      if (t.totp_last_step != null && step <= Number(t.totp_last_step)) {
        return res.status(401).json({ error: "That code has already been used — wait for the next one" });
      }
      await getPool().query(
        `UPDATE hub_users SET totp_last_step = $2${t.purpose === "enrol" ? ", totp_secret = $3" : ""} WHERE name_key = $1`,
        t.purpose === "enrol" ? [t.name_key, step, t.secret] : [t.name_key, step]);
      await getPool().query(`DELETE FROM hub_pending_logins WHERE token_hash = $1`, [hashToken(b.ticket)]);
      res.json({ token: await issueSession(t.name_key), name: t.display_name, enrolled: t.purpose === "enrol" });
    } catch (e) { res.status(500).json({ error: e.message }); }
  });

  // Lost phone: an admin clears the authenticator and the person sets it up again
  // at their next sign-in. Their sessions end with it.
  app.post("/api/hub/reset-authenticator", async (req, res) => {
    if (!needDb(res)) return;
    const b = req.body || {};
    const admin = process.env.HUB_ADMIN_KEY;
    if (!admin) return res.status(503).json({ error: "Set HUB_ADMIN_KEY on the backend to reset authenticators" });
    if (String(b.adminKey || "") !== admin) return res.status(403).json({ error: "Wrong admin key" });
    const key = nameKey(b.name);
    if (!key) return res.status(400).json({ error: "Which name?" });
    try {
      await ensureTables();
      const r = await getPool().query(`UPDATE hub_users SET totp_secret = NULL, totp_last_step = NULL WHERE name_key = $1`, [key]);
      if (!r.rowCount) return res.status(404).json({ error: "No account with that name" });
      await getPool().query(`DELETE FROM hub_sessions WHERE name_key = $1`, [key]);
      res.json({ success: true });
    } catch (e) { res.status(500).json({ error: e.message }); }
  });

  app.get("/api/hub/me", async (req, res) => {
    if (!needDb(res)) return;
    try {
      const u = await userFromToken(req);
      if (!u) return res.status(401).json({ error: "Not signed in" });
      res.json({ name: u.name });
    } catch (e) { res.status(500).json({ error: e.message }); }
  });

  app.post("/api/hub/logout", async (req, res) => {
    if (!needDb(res)) return;
    try {
      const token = tokenFromRequest(req);
      if (token) {
        await ensureTables();
        await getPool().query(`DELETE FROM hub_sessions WHERE token_hash = $1`, [hashToken(token)]);
      }
      res.json({ success: true });
    } catch (e) { res.status(500).json({ error: e.message }); }
  });

  // Remove an account entirely — somebody who has left, or a test row.
  // Admin-gated for the same reason as the password reset: there is no identity
  // check here beyond a name, so anyone could otherwise delete anyone.
  app.post("/api/hub/delete-user", async (req, res) => {
    if (!needDb(res)) return;
    const b = req.body || {};
    const admin = process.env.HUB_ADMIN_KEY;
    if (!admin) return res.status(503).json({ error: "Set HUB_ADMIN_KEY on the backend to remove accounts" });
    if (String(b.adminKey || "") !== admin) return res.status(403).json({ error: "Wrong admin key" });
    const key = nameKey(b.name);
    if (!key) return res.status(400).json({ error: "Which name?" });
    try {
      await ensureTables();
      // Sessions go with them (the foreign key cascades, but be explicit).
      await getPool().query(`DELETE FROM hub_sessions WHERE name_key = $1`, [key]);
      const r = await getPool().query(`DELETE FROM hub_users WHERE name_key = $1`, [key]);
      if (!r.rowCount) return res.status(404).json({ error: "No account with that name" });
      res.json({ success: true, removed: b.name });
    } catch (e) { res.status(500).json({ error: e.message }); }
  });

  // Changing a forgotten password needs someone who can see the database.
  // Deliberately not a self-serve reset: there is no email verification here,
  // so a self-serve reset would let anyone take over any account by name.
  app.post("/api/hub/set-password", async (req, res) => {
    if (!needDb(res)) return;
    const b = req.body || {};
    const admin = process.env.HUB_ADMIN_KEY;
    if (!admin) return res.status(503).json({ error: "Set HUB_ADMIN_KEY on the backend to reset passwords" });
    if (String(b.adminKey || "") !== admin) return res.status(403).json({ error: "Wrong admin key" });
    const key = nameKey(b.name);
    const p = validatePassword(b.password);
    if (!key) return res.status(400).json({ error: "Which name?" });
    if (!p.ok) return res.status(400).json({ error: p.error });
    try {
      await ensureTables();
      const { salt, hash } = hashPassword(b.password);
      const r = await getPool().query(
        `UPDATE hub_users SET pw_salt = $2, pw_hash = $3 WHERE name_key = $1`, [key, salt, hash]
      );
      if (!r.rowCount) return res.status(404).json({ error: "No account with that name" });
      // Their old sessions must die with the old password.
      await getPool().query(`DELETE FROM hub_sessions WHERE name_key = $1`, [key]);
      res.json({ success: true });
    } catch (e) { res.status(500).json({ error: e.message }); }
  });
}
