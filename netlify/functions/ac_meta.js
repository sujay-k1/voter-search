/**
 * netlify/functions/ac_meta.js
 *
 * Request (POST JSON):
 * { district:"dumka", state:"S27", ac:7 }
 *
 * Response:
 * { ok:true, voters:<count> }
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

exports.handler = async (event) => {
  try {
    if (event.httpMethod === "OPTIONS") return ok({});
    if (event.httpMethod !== "POST") return badRequest("POST required");

    const body = await readJsonBody(event);
    if (!body) return badRequest("Invalid JSON");

    const district = asString(body.district);
    const state = asString(body.state, "S27");
    const ac = asInt(body.ac);

    if (!district) return badRequest("Missing district");
    if (!state) return badRequest("Missing state");
    if (!Number.isFinite(ac)) return badRequest("Missing/invalid ac");

    const client = await getClient(district);

    const rs = await client.execute({
      sql: `SELECT COUNT(*) AS voters FROM voters WHERE "State Code" = ? AND "AC No" = ?;`,
      args: [state, ac],
    });

    const voters = rs.rows && rs.rows[0] ? Number(rs.rows[0].voters) : 0;

    return ok({ voters });
  } catch (err) {
    return serverError(err);
  }
};
