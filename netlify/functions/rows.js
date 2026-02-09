/**
 * netlify/functions/rows.js
 *
 * Speed-optimized: uses json_each(?) to avoid chunked IN lists.
 * Preserves input order (important for stable paging display).
 * Falls back to chunked IN if json_each is unavailable.
 *
 * Request (POST JSON):
 * {
 *   district: "dumka",
 *   state: "S27",
 *   ac: 7,
 *   kind: "score" | "display" | "age" | "gender_age",
 *   row_ids: [1,2,3]
 * }
 *
 * Response:
 * { ok:true, rows:[{...}, ...] }
 */

const {
  getClient,
  ok,
  badRequest,
  serverError,
  readJsonBody,
  asInt,
  asString,
} = require("./_turso");

const DISPLAY_COLS = [
  "Voter Name",
  "Relative Name",
  "Relation",
  "Gender",
  "Age",
  "House No",
  "Serial No",
  "Page No",
  "Part No",
  "ID",
  "Source PDF",
  "PDF Path",
];

function quoteIdent(name) {
  return `"${String(name).replace(/"/g, '""')}"`;
}

async function queryChunkedIn(client, { state, ac, rowIds, sqlCols }) {
  const CHUNK = 900;
  const rows = [];
  for (let i = 0; i < rowIds.length; i += CHUNK) {
    const chunk = rowIds.slice(i, i + CHUNK);
    const ph = chunk.map(() => "?").join(",");
    const sql = `
      SELECT ${sqlCols}
      FROM voters v
      WHERE v."State Code" = ?
        AND v."AC No" = ?
        AND v.row_id IN (${ph});
    `;
    const args = [state, ac, ...chunk];
    const rs = await client.execute({ sql, args });
    if (rs.rows && rs.rows.length) rows.push(...rs.rows);
  }

  // Reorder to match input order
  const byId = new Map();
  for (const r of rows) byId.set(Number(r.row_id), r);

  const ordered = [];
  for (const rid of rowIds) {
    const r = byId.get(Number(rid));
    if (r) ordered.push(r);
  }
  return ordered;
}

async function queryJsonEach(client, { state, ac, rowIds, sqlCols }) {
  // ids.ord preserves order of the JSON array input
  const sql = `
    WITH ids AS (
      SELECT CAST(key AS INTEGER) AS ord,
             CAST(value AS INTEGER) AS row_id
      FROM json_each(?)
    )
    SELECT ${sqlCols}
    FROM ids
    JOIN voters v ON v.row_id = ids.row_id
    WHERE v."State Code" = ?
      AND v."AC No" = ?
    ORDER BY ids.ord;
  `;

  const args = [JSON.stringify(rowIds), state, ac];
  const rs = await client.execute({ sql, args });
  return rs.rows || [];
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
    const kind = asString(body.kind);
    const rowIdsIn = Array.isArray(body.row_ids) ? body.row_ids.map((x) => asInt(x)).filter(Number.isFinite) : [];

    if (!district) return badRequest("Missing district");
    if (!state) return badRequest("Missing state");
    if (!Number.isFinite(ac)) return badRequest("Missing/invalid ac");
    if (!rowIdsIn.length) return ok({ rows: [] });

    // Keep original order, but normalize to integers
    const rowIds = rowIdsIn.map((x) => Number(x));

    const client = await getClient(district);

    let sqlCols = "";

    if (kind === "score") {
      sqlCols = [
        "v.row_id AS row_id",
        "v.voter_name_raw AS voter_name_raw",
        "v.relative_name_raw AS relative_name_raw",
        "v.voter_name_norm AS voter_name_norm",
        "v.relative_name_norm AS relative_name_norm",
        `${quoteIdent("Serial No")} AS serial_no`,
      ].join(", ");
    } else if (kind === "age") {
      sqlCols = [
        "v.row_id AS row_id",
        `${quoteIdent("Age")} AS Age`,
      ].join(", ");
    } else if (kind === "gender_age") {
      sqlCols = [
        "v.row_id AS row_id",
        `${quoteIdent("Age")} AS Age`,
        `${quoteIdent("Gender")} AS Gender`,
      ].join(", ");
    } else if (kind === "display") {
      const cols = [
        "v.row_id AS row_id",
        `${quoteIdent("State Code")} AS ${quoteIdent("State Code")}`,
        `${quoteIdent("AC No")} AS ${quoteIdent("AC No")}`,
        ...DISPLAY_COLS.map(quoteIdent),
      ];
      sqlCols = cols.join(", ");
    } else {
      return badRequest(`Invalid kind: ${kind}`);
    }

    let rows;
    try {
      rows = await queryJsonEach(client, { state, ac, rowIds, sqlCols });
    } catch (e) {
      const msg = String(e?.message || e || "");
      if (msg.includes("json_each") || msg.includes("no such table")) {
        rows = await queryChunkedIn(client, { state, ac, rowIds, sqlCols });
      } else {
        throw e;
      }
    }

    return ok({ rows });
  } catch (err) {
    return serverError(err);
  }
};
