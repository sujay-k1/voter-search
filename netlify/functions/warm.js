/**
 * netlify/functions/warm.js
 *
 * Best-effort warm-up for a district DB:
 * - warms Netlify function instance + libsql client
 * - touches the main tables used by search (voters + idx tables)
 *
 * Request (POST JSON):
 * { district:"dumka", state:"S27" }
 *
 * Response:
 * { ok:true, district, ms_total, steps:[{name, ms, ok}] }
 */

const {
  getClient,
  ok,
  badRequest,
  serverError,
  readJsonBody,
  asString,
} = require("./_turso");

async function timed(name, fn) {
  const t0 = Date.now();
  try {
    await fn();
    return { name, ms: Date.now() - t0, ok: true };
  } catch (e) {
    return { name, ms: Date.now() - t0, ok: false, error: String(e?.message || e || "error") };
  }
}

exports.handler = async (event) => {
  const t0 = Date.now();
  try {
    if (event.httpMethod === "OPTIONS") return ok({});
    if (event.httpMethod !== "POST") return badRequest("POST required");

    const body = await readJsonBody(event);
    if (!body) return badRequest("Invalid JSON");

    const district = asString(body.district);
    const state = asString(body.state, "S27");

    if (!district) return badRequest("Missing district");
    if (!state) return badRequest("Missing state");

    const client = await getClient(district);

    const steps = [];

    // 1) Basic DB round-trip
    steps.push(
      await timed("select_1", async () => {
        await client.execute({ sql: "SELECT 1 AS ok;", args: [] });
      })
    );

    // 2) Touch voters table (state range seek on PK prefix)
    steps.push(
      await timed("voters_state_limit1", async () => {
        await client.execute({
          sql: `SELECT 1 AS ok
                FROM voters
                WHERE "State Code" = ?
                LIMIT 1;`,
          args: [state],
        });
      })
    );

    // 3) Touch idx tables that search relies on (PK prefix seek on "State Code")
    const idxTables = [
      "idx_voter_strict",
      "idx_voter_exact",
      "idx_voter_loose",
      "idx_relative_strict",
      "idx_relative_exact",
      "idx_relative_loose",
    ];

    for (const t of idxTables) {
      steps.push(
        await timed(`${t}_state_limit1`, async () => {
          await client.execute({
            sql: `SELECT 1 AS ok
                  FROM ${t}
                  WHERE "State Code" = ?
                  LIMIT 1;`,
            args: [state],
          });
        })
      );
    }

    const msTotal = Date.now() - t0;

    // helpful log in Netlify
    const okCount = steps.filter((s) => s.ok).length;
    console.log(`[warm] district=${district} state=${state} ok=${okCount}/${steps.length} total=${msTotal}ms`);

    return ok({
      district,
      state,
      ms_total: msTotal,
      steps,
    });
  } catch (err) {
    return serverError(err);
  }
};
