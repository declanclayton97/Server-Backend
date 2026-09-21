// ── Carry-forward lines ───────────────────────────────────────────────────────
// Things that must go on a supplier's NEXT order but which Brightpearl demand will never produce,
// because the sales order they belong to is already finalised, shipped or short-delivered:
//   • PO 483480 bought ONE 3625 shirt where the BP unit is a 5-pack, so four were owed to SO 483415
//     — closed, so no demand scan would ever ask for them (Blaklader, 2026-08).
//   • SO 479368 shipped two 3-packs of Scruffs socks and one has to be bought again; the SO is
//     fully allocated so its rows read as ordered, and re-tagging it only made the tag audit nag
//     (2026-09-21). "ONLY 1x T53545" in a tag cannot conjure a quantity the rows do not have.
//
// A note in a PO or an email does not order anything. This does: createPo appends every unconsumed
// line for that supplier to the order as ordinary lines (a PO row, the supplier basket, the price
// check, all of it), reserves them against that PO, and runSupplierScheduled marks them consumed
// only once the order is actually PLACED. If the run aborts they stay pending and go on the next.
//
// Lives in its own module because BOTH purchasingAuto (createPo) and purchasingSchedule (the run,
// the routes) need it, and purchasingSchedule already imports purchasingAuto — importing the other
// way round would be a cycle.
//
// qty is in BRIGHTPEARL units — the same unit as a PO row — except on the Blaklader lane, which
// keeps its own older path: there qty is Blaklader's PIECES and goes straight to the cart, raw,
// because its whole reason to exist is a partial pack that a BP unit cannot express.

async function ensurePendingTable(pool) {
  await pool.query(`CREATE TABLE IF NOT EXISTS purchasing_pending_lines (
    id serial PRIMARY KEY,
    supplier text NOT NULL,
    sku text NOT NULL,
    qty int NOT NULL,
    note text,
    created_at timestamptz DEFAULT now(),
    consumed_at timestamptz,
    consumed_po int
  )`);
  // so_id: the sales order the line is owed to, so the PO note and the drop notice can name it.
  // reserved_po: the PO createPo put it on; consumed only when THAT PO places.
  await pool.query(`ALTER TABLE purchasing_pending_lines
    ADD COLUMN IF NOT EXISTS so_id int,
    ADD COLUMN IF NOT EXISTS reserved_po int,
    ADD COLUMN IF NOT EXISTS added_by text`);
}

export async function addPendingLine(pool, { supplier, sku, qty, note, soId, addedBy }) {
  if (!pool) return { error: 'no database' };
  await ensurePendingTable(pool);
  const r = await pool.query(
    `INSERT INTO purchasing_pending_lines (supplier, sku, qty, note, so_id, added_by) VALUES ($1,$2,$3,$4,$5,$6) RETURNING *`,
    [String(supplier).toUpperCase(), String(sku).trim(), Math.round(Number(qty) || 0), note || null, soId ? Number(soId) : null, addedBy ? String(addedBy).slice(0, 80) : null],
  );
  return r.rows[0];
}

export async function listPendingLines(pool, supplier, { includeConsumed = false } = {}) {
  if (!pool) return [];
  await ensurePendingTable(pool);
  const r = await pool.query(
    `SELECT * FROM purchasing_pending_lines WHERE supplier=$1 ${includeConsumed ? '' : 'AND consumed_at IS NULL'} ORDER BY id`,
    [String(supplier).toUpperCase()],
  );
  return r.rows;
}

// Every supplier's open lines at once — what the hub shows.
export async function listAllPendingLines(pool, { includeConsumed = false, days = 60 } = {}) {
  if (!pool) return [];
  await ensurePendingTable(pool);
  const r = includeConsumed
    ? await pool.query(`SELECT * FROM purchasing_pending_lines WHERE created_at > now() - ($1 || ' days')::interval ORDER BY supplier, id`, [String(days)])
    : await pool.query('SELECT * FROM purchasing_pending_lines WHERE consumed_at IS NULL ORDER BY supplier, id');
  return r.rows;
}

export async function updatePendingLine(pool, id, { qty, note, remove, consumedPoId } = {}) {
  if (!pool) return { error: 'no database' };
  await ensurePendingTable(pool);
  // Mark a line FULFILLED rather than deleting it. When PO 483751 was placed by hand the only
  // option was `remove`, which threw away the record of what was owed and why; consumed_at +
  // consumed_po keep it. `remove` stays for lines added in error.
  if (consumedPoId) {
    const c = await pool.query(
      'UPDATE purchasing_pending_lines SET consumed_at=now(), consumed_po=$2 WHERE id=$1 AND consumed_at IS NULL RETURNING *',
      [id, consumedPoId],
    );
    return c.rows[0] || { error: 'not found or already consumed' };
  }
  if (remove) { await pool.query('DELETE FROM purchasing_pending_lines WHERE id=$1 AND consumed_at IS NULL', [id]); return { removed: id }; }
  const r = await pool.query(
    'UPDATE purchasing_pending_lines SET qty=COALESCE($2,qty), note=COALESCE($3,note) WHERE id=$1 AND consumed_at IS NULL RETURNING *',
    [id, qty != null ? Math.round(Number(qty)) : null, note != null ? String(note) : null],
  );
  return r.rows[0] || { error: 'not found or already consumed' };
}

// createPo put these lines on PO `poId`. Recorded so the run can consume exactly the lines that
// went on the PO that placed — not whatever is pending by the time it finishes.
export async function reservePendingLines(pool, ids, poId) {
  if (!pool || !ids.length) return;
  await ensurePendingTable(pool);
  await pool.query('UPDATE purchasing_pending_lines SET reserved_po=$2 WHERE id = ANY($1::int[]) AND consumed_at IS NULL', [ids, poId]);
}

export async function consumePendingLines(pool, ids, poId) {
  if (!pool || !ids.length) return;
  await ensurePendingTable(pool);
  await pool.query('UPDATE purchasing_pending_lines SET consumed_at=now(), consumed_po=$2 WHERE id = ANY($1::int[]) AND consumed_at IS NULL', [ids, poId]);
}

// The order placed: everything reserved against its PO is now bought. Returns the rows consumed
// so the run report can say so.
export async function consumeReservedPendingLines(pool, poId) {
  if (!pool || !poId) return [];
  await ensurePendingTable(pool);
  const r = await pool.query(
    'UPDATE purchasing_pending_lines SET consumed_at=now(), consumed_po=$1 WHERE reserved_po=$1 AND consumed_at IS NULL RETURNING *',
    [Number(poId)],
  );
  return r.rows;
}
