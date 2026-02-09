/**
 * netlify/functions/candidates.js
 *
 * Speed-optimized: uses json_each(?) to avoid chunked IN lists.
 * Falls back to chunked IN if json_each is unavailable.
 *
 * Request (POST JSON):
 * {
 *   district: "dumka",
 *   state: "S27",
 *   ac: 7,
 *   table: "idx_voter_strict" | "idx_voter_exact" | "idx_voter_loose" | "idx_relative_strict" | "idx_relative_exact" | "idx_relative_loose",
 *   keys: ["...","..."]
 * }
 *
 * Response:
 * { ok:true, rows:[{row_id, hit_count, and_hit}, ...] }
 */

const {
  getClient,
  ok,
  badRequest,
  serverError,
  readJsonBody,
  asInt,
  asString,
  decodeRowIds,
} = require("./_turso");

const ALLOWED_TABLES = new Set([
  "idx_voter_strict",
  "idx_voter_exact",
  "idx_voter_loose",
  "idx_relative_strict",
  "idx_relative_exact",
  "idx_relative_loose",
]);

async function queryChunkedIn(client, { table, state, ac, keys }) {
  const CHUNK = 900; // keep under SQLite variable limit
  const hitCount = new Map();

  for (let i = 0; i < keys.length; i += CHUNK) {
    const chunk = keys.slice(i, i + CHUNK);
    const ph = chunk.map(() => "?").join(",");

    const sql = `
      SELECT key, row_ids, n
      FROM ${table}
      WHERE "State Code" = ?
        AND "AC No" = ?
        AND key IN (${ph});
    `;

    const args = [state, ac, ...chunk];
    const rs = await client.execute({ sql, args });

    for (const row of rs.rows || []) {
      const n = row.n ?? row["n"];
      const ids = decodeRowIds(row.row_ids ?? row["row_ids"], n);
      for (const rid of ids) {
        const k = Number(rid);
        hitCount.set(k, (hitCount.get(k) || 0) + 1);
      }
    }
  }

  return hitCount;
}

async function queryJsonEach(client, { table, state, ac, keys }) {
  // One query, no chunking:
  // ks(key) is built from json_each(?) over the JSON array of keys.
  // Join uses the PK ("State Code","AC No",key) efficiently.
  const sql = `
    WITH ks AS (
      SELECT CAST(value AS TEXT) AS key
      FROM json_each(?)
    )
    SELECT t.key, t.row_ids, t.n
    FROM ${table} t
    JOIN ks ON ks.key = t.key
    WHERE t."State Code" = ?
      AND t."AC No" = ?;
  `;

  const args = [JSON.stringify(keys), state, ac];
  const rs = await client.execute({ sql, args });

  const hitCount = new Map();
  for (const row of rs.rows || []) {
    const n = row.n ?? row["n"];
    const ids = decodeRowIds(row.row_ids ?? row["row_ids"], n);
    for (const rid of ids) {
      const k = Number(rid);
      hitCount.set(k, (hitCount.get(k) || 0) + 1);
    }
  }
  return hitCount;
}

exports.handler = async (event) => {
  try {
    if (event.httpMethod === "OPTIONS") return ok({});
    if (event.httpMethod !== "POST") return badRequest("POST required");

    const body = await readJsonBody(event);
    if (!body) return badRequest("Invalid JSON");

    const district = asString(body.district);
    const state = asString(body.state, "S27");
    const ac = asInt(body.ac);
    const table = asString(body.table);
    const keysIn = Array.isArray(body.keys) ? body.keys.map((x) => asString(x)).filter(Boolean) : [];

    if (!district) return badRequest("Missing district");
    if (!state) return badRequest("Missing state");
    if (!Number.isFinite(ac)) return badRequest("Missing/invalid ac");
    if (!ALLOWED_TABLES.has(table)) return badRequest("Invalid table");
    if (!keysIn.length) return ok({ rows: [] });

    // De-dupe keys (important: duplicates would inflate hit_count)
    const keys = Array.from(new Set(keysIn));

    const client = await getClient(district);

    let hitCount;
    try {
      hitCount = await queryJsonEach(client, { table, state, ac, keys });
    } catch (e) {
      const msg = String(e?.message || e || "");
      // If JSON1 isn't available for some reason, fallback
      if (msg.includes("json_each") || msg.includes("no such table")) {
        hitCount = await queryChunkedIn(client, { table, state, ac, keys });
      } else {
        throw e;
      }
    }

    const totalKeys = keys.length;
    const out = [];
    for (const [row_id, c] of hitCount.entries()) {
      out.push({ row_id, hit_count: c, and_hit: c === totalKeys });
    }

    out.sort((a, b) => (b.hit_count - a.hit_count) || (a.row_id - b.row_id));

    return ok({ rows: out });
  } catch (err) {
    return serverError(err);
  }
};
