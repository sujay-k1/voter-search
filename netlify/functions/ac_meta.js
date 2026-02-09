/**
 * netlify/functions/ac_meta.js
 *
 * Fast ping for a given district+AC.
 *
 * Request (POST JSON):
 * { district:"dumka", state:"S27", ac:7 }
 *
 * Response:
 * { ok:true, voters:null, has_any:true|false }
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

    // ✅ Fast "ping": does NOT scan/count the entire AC
    const rs = await client.execute({

      sql: `SELECT 1 AS ok
            FROM voters
            WHERE "State Code" = ? AND "AC No" = ?
            LIMIT 1;`,

      args: [state, ac],
    });

    const hasAny = !!(rs.rows && rs.rows.length);

    // voters:null tells frontend "don’t show voters count"
    return ok({ voters: null, has_any: hasAny });
  } catch (err) {
    return serverError(err);
  }
};
