/**
 * netlify/functions/candidates.js
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
    const keys = Array.isArray(body.keys) ? body.keys.map((x) => asString(x)).filter(Boolean) : [];

    if (!district) return badRequest("Missing district");
    if (!state) return badRequest("Missing state");
    if (!Number.isFinite(ac)) return badRequest("Missing/invalid ac");
    if (!ALLOWED_TABLES.has(table)) return badRequest("Invalid table");
    if (!keys.length) return ok({ rows: [] });

    const client = await getClient(district);

    // SQLite max variables is commonly 999; keep some headroom for state+ac.
    const CHUNK = 900;
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
      const rows = rs.rows || [];

      for (const row of rows) {
        const n = row.n ?? row["n"];
        const ids = decodeRowIds(row.row_ids ?? row["row_ids"], n);

        for (const rid of ids) {
          const k = Number(rid);
          hitCount.set(k, (hitCount.get(k) || 0) + 1);
        }
      }
    }

    const totalKeys = keys.length;
    const out = [];
    for (const [row_id, c] of hitCount.entries()) {
      out.push({ row_id, hit_count: c, and_hit: c === totalKeys });
    }

    // Stable-ish ordering: higher hit_count first, then row_id asc
    out.sort((a, b) => (b.hit_count - a.hit_count) || (a.row_id - b.row_id));

    return ok({ rows: out });
  } catch (err) {
    return serverError(err);
  }
};
