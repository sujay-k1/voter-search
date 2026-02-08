/**
 * netlify/functions/rows.js
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

async function selectInChunks(client, { sqlBase, argsBase, rowIds }) {
  // SQLite max variables is commonly 999. Keep room for state+ac in argsBase.
  const CHUNK = 900;
  const all = [];

  for (let i = 0; i < rowIds.length; i += CHUNK) {
    const chunk = rowIds.slice(i, i + CHUNK);
    const ph = chunk.map(() => "?").join(",");
    const sql = `${sqlBase} AND row_id IN (${ph})`;
    const args = [...argsBase, ...chunk];

    const rs = await client.execute({ sql, args });
    if (rs.rows && rs.rows.length) all.push(...rs.rows);
  }
  return all;
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
    const rowIds = Array.isArray(body.row_ids) ? body.row_ids.map((x) => asInt(x)).filter(Number.isFinite) : [];

    if (!district) return badRequest("Missing district");
    if (!state) return badRequest("Missing state");
    if (!Number.isFinite(ac)) return badRequest("Missing/invalid ac");
    if (!rowIds.length) return ok({ rows: [] });

    const client = await getClient(district);

    if (kind === "score") {
      const sqlBase = `
        SELECT
          row_id,
          voter_name_raw,
          relative_name_raw,
          voter_name_norm,
          relative_name_norm,
          ${quoteIdent("Serial No")} AS serial_no
        FROM voters
        WHERE "State Code" = ?
          AND "AC No" = ?
      `;
      const rows = await selectInChunks(client, { sqlBase, argsBase: [state, ac], rowIds });
      return ok({ rows });
    }

    if (kind === "age") {
      const sqlBase = `
        SELECT row_id, ${quoteIdent("Age")} AS Age
        FROM voters
        WHERE "State Code" = ?
          AND "AC No" = ?
      `;
      const rows = await selectInChunks(client, { sqlBase, argsBase: [state, ac], rowIds });
      return ok({ rows });
    }

    if (kind === "gender_age") {
      const sqlBase = `
        SELECT row_id,
               ${quoteIdent("Age")} AS Age,
               ${quoteIdent("Gender")} AS Gender
        FROM voters
        WHERE "State Code" = ?
          AND "AC No" = ?
      `;
      const rows = await selectInChunks(client, { sqlBase, argsBase: [state, ac], rowIds });
      return ok({ rows });
    }

    if (kind === "display") {
      const cols = [
        "row_id",
        quoteIdent("State Code"),
        quoteIdent("AC No"),
        ...DISPLAY_COLS.map(quoteIdent),
      ].join(", ");

      const sqlBase = `
        SELECT ${cols}
        FROM voters
        WHERE "State Code" = ?
          AND "AC No" = ?
      `;

      const rows = await selectInChunks(client, { sqlBase, argsBase: [state, ac], rowIds });
      return ok({ rows });
    }

    return badRequest(`Invalid kind: ${kind}`);
  } catch (err) {
    return serverError(err);
  }
};
