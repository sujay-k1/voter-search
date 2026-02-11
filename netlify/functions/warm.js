/**
 * netlify/functions/warm.js
 *
 * FULL WARM (per AC) for a district DB:
 * - warms Netlify function instance + libsql client
 * - touches main tables used by search (voters + idx tables)
 * - for EVERY AC in the district: runs very cheap LIMIT 1 queries
 *
 * Request (POST JSON):
 *  { district:"dumka", state:"S27", acs:[7,10,11,12] }   // preferred
 *  { district:"dumka", state:"S27" }                     // fallback: derive ACs from voters table
 *
 * Response:
 * { ok:true, district, state, acs, ms_total, summary:{...}, steps:[...] }
 */

const {
  getClient,
  ok,
  badRequest,
  serverError,
  readJsonBody,
  asString,
} = require("./_turso");

function asAcsArray(x) {
  if (!Array.isArray(x)) return null;
  const out = [];
  for (const v of x) {
    const n = Number(v);
    if (Number.isFinite(n)) out.push(n);
  }
  // unique + sorted
  return Array.from(new Set(out)).sort((a, b) => a - b);
}

async function timed(name, fn) {
  const t0 = Date.now();
  try {
    await fn();
    return { name, ms: Date.now() - t0, ok: true };
  } catch (e) {
    return {
      name,
      ms: Date.now() - t0,
      ok: false,
      error: String(e?.message || e || "error"),
    };
  }
}

// simple promise pool
async function runPool(items, concurrency, worker) {
  const results = new Array(items.length);
  let idx = 0;

  async function runOne() {
    while (true) {
      const i = idx++;
      if (i >= items.length) return;
      results[i] = await worker(items[i], i);
    }
  }

  const n = Math.max(1, Math.min(concurrency || 1, items.length || 1));
  const runners = [];
  for (let i = 0; i < n; i++) runners.push(runOne());
  await Promise.all(runners);
  return results;
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

    // idx tables used by search
    const idxTables = [
      "idx_voter_strict",
      "idx_voter_exact",
      "idx_voter_loose",
      "idx_relative_strict",
      "idx_relative_exact",
      "idx_relative_loose",
    ];

    const mode = String(body.mode || "").toLowerCase();

    // LITE warm: avoid per-AC warming (can compete with the first search and slow it down).
    // Touch each table once for this state so Turso + the function instance are "awake".
    if (mode === "lite") {
      steps.push(
        await timed("voters_limit1_state", async () => {
          await client.execute({
            sql: `SELECT 1 AS ok
                  FROM voters
                  WHERE "State Code" = ?
                  LIMIT 1;`,
            args: [state],
          });
        })
      );

      for (const t of idxTables) {
        steps.push(
          await timed(`${t}_limit1_state`, async () => {
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
      console.log(`[warm] mode=lite district=${district} state=${state} total=${msTotal}ms`);
      return ok({
        district,
        state,
        mode: "lite",
        ms_total: msTotal,
        summary: { ok_steps: steps.filter((s) => s.ok).length, total_steps: steps.length },
        steps,
      });
    }


    // Resolve AC list: prefer caller-provided list (fastest).
    let acs = asAcsArray(body.acs);

    // Fallback: derive AC list from voters table if not provided.
    // NOTE: this can be heavier, but it's correct.
    if (!acs || !acs.length) {
      const distinct = await timed("derive_acs_distinct_voters", async () => {
        await client.execute({
          sql: `SELECT DISTINCT "AC No" AS ac
                FROM voters
                WHERE "State Code" = ?
                ORDER BY "AC No";`,
          args: [state],
        });
      });
      steps.push(distinct);

      if (distinct.ok) {
        // We need the actual rows for ac list, so run again but capture rows.
        const rs = await client.execute({
          sql: `SELECT DISTINCT "AC No" AS ac
                FROM voters
                WHERE "State Code" = ?
                ORDER BY "AC No";`,
          args: [state],
        });
        acs = (rs.rows || [])
          .map((r) => Number(r.ac ?? r["ac"] ?? r["AC No"]))
          .filter((n) => Number.isFinite(n));
      } else {
        acs = [];
      }
    }

    // If still empty, nothing more to warm.
    if (!acs.length) {
      const msTotal = Date.now() - t0;
      console.log(`[warm] district=${district} state=${state} acs=0 total=${msTotal}ms`);
      return ok({
        district,
        state,
        acs: [],
        ms_total: msTotal,
        summary: { acs: 0, per_ac_steps: 0, ok_steps: steps.filter((s) => s.ok).length, total_steps: steps.length },
        steps,
      });
    }

    const concurrency = Number.isFinite(Number(body.concurrency)) ? Math.max(1, Math.min(12, Number(body.concurrency))) : 6;

    // Per-AC warm: touch voters + each idx table with (state, ac) predicate.
    const perAcResults = await runPool(acs, concurrency, async (ac) => {
      const per = [];

      // Touch voters at this AC
      per.push(
        await timed(`ac_${ac}_voters_limit1`, async () => {
          await client.execute({
            sql: `SELECT 1 AS ok
                  FROM voters
                  WHERE "State Code" = ?
                    AND "AC No" = ?
                  LIMIT 1;`,
            args: [state, ac],
          });
        })
      );

      // Touch each idx table at this AC
      for (const t of idxTables) {
        per.push(
          await timed(`ac_${ac}_${t}_limit1`, async () => {
            await client.execute({
              sql: `SELECT 1 AS ok
                    FROM ${t}
                    WHERE "State Code" = ?
                      AND "AC No" = ?
                    LIMIT 1;`,
              args: [state, ac],
            });
          })
        );
      }

      // Summarize per AC (don’t explode response size too much)
      const okCount = per.filter((x) => x.ok).length;
      const ms = per.reduce((s, x) => s + (x.ms || 0), 0);

      return {
        ac,
        ok_steps: okCount,
        total_steps: per.length,
        ms_sum: ms,
        // include detailed per-step timings only if requested
        steps: body.verbose ? per : undefined,
      };
    });

    // Add a compact summary step (and optionally detailed steps)
    steps.push({
      name: "per_ac_warm_summary",
      ok: true,
      ms: 0,
      acs: acs.length,
      concurrency,
      per_ac: perAcResults,
    });

    const msTotal = Date.now() - t0;

    const okBase = steps.filter((s) => s.ok).length;
    const perAcOkTotal = perAcResults.reduce((s, x) => s + (x.ok_steps || 0), 0);
    const perAcStepsTotal = perAcResults.reduce((s, x) => s + (x.total_steps || 0), 0);

    console.log(
      `[warm] district=${district} state=${state} acs=${acs.length} perAcOk=${perAcOkTotal}/${perAcStepsTotal} total=${msTotal}ms`
    );

    return ok({
      district,
      state,
      acs,
      ms_total: msTotal,
      summary: {
        acs: acs.length,
        per_ac_steps: perAcStepsTotal,
        per_ac_ok_steps: perAcOkTotal,
        base_ok_steps: okBase,
        base_total_steps: steps.length,
        concurrency,
      },
      steps,
    });
  } catch (err) {
    return serverError(err);
  }
};
