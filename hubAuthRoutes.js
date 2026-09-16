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
  newSessionToken, hashToken, sessionExpiry, tokenFromRequest,
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
    `);
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
        WHERE s.token_hash = $1 AND s.expires_at > NOW()`,
      [hashToken(token)]
    );
    if (!r.rowCount) return null;
    getPool().query(`UPDATE hub_users SET last_seen_at = NOW() WHERE name_key = $1`, [r.rows[0].name_key])
      .catch(() => {});
    return { key: r.rows[0].name_key, name: r.rows[0].display_name };
  }
  // Exposed so other routes can stamp "who did this" without re-implementing it.
  app.locals.hubUserFromToken = userFromToken;

  async function issueSession(key) {
    const token = newSessionToken();
    await getPool().query(
      `INSERT INTO hub_sessions (token_hash, name_key, expires_at) VALUES ($1, $2, $3)`,
      [hashToken(token), key, sessionExpiry()]
    );
    // Tidy expired rows opportunistically rather than running a sweeper.
    getPool().query(`DELETE FROM hub_sessions WHERE expires_at < NOW()`).catch(() => {});
    return token;
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

  // First time someone opens the hub.
  app.post("/api/hub/register", async (req, res) => {
    if (!needDb(res)) return;
    const b = req.body || {};
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
      res.json({ token: await issueSession(n.key), name: n.name });
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
        `SELECT name_key, display_name, pw_salt, pw_hash FROM hub_users WHERE name_key = $1`, [key]
      );
      const u = r.rows[0];
      // Same message whether the name is unknown or the password is wrong.
      // Not for secrecy — this is a name badge — but because "no such user"
      // invites people to register a second account under a variant spelling.
      if (!u || !verifyPassword(b.password, u.pw_salt, u.pw_hash)) {
        return res.status(401).json({ error: "That name and password do not match" });
      }
      res.json({ token: await issueSession(u.name_key), name: u.display_name });
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
