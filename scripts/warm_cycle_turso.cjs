// scripts/warm_cycle_turso.cjs
const fs = require("fs");
const path = require("path");

function slugifyDistrictId(id) {
  return String(id ?? "")
    .trim()
    .toLowerCase()
    .replace(/[_\s]+/g, "-")
    .replace(/[^a-z0-9-]/g, "-")
    .replace(/-+/g, "-")
    .replace(/^-+|-+$/g, "");
}

async function getLibsql() {
  return await import("@libsql/client");
}

function buildDbUrl({ prefix, slug, user, suffix }) {
  return `libsql://${prefix}${slug}-${user}.${suffix}`;
}

async function runPool(items, concurrency, worker) {
  const out = new Array(items.length);
  let idx = 0;
  const n = Math.max(1, Math.min(concurrency || 1, items.length || 1));
  async function runner() {
    while (true) {
      const i = idx++;
      if (i >= items.length) return;
      out[i] = await worker(items[i], i);
    }
  }
  await Promise.all(Array.from({ length: n }, () => runner()));
  return out;
}

(async () => {
  const state = process.env.WARM_STATE || "S27";

  const token = process.env.TURSO_TOKEN_C || process.env.TURSO_TOKEN || "";
  if (!token) throw new Error("Missing TURSO_TOKEN_C or TURSO_TOKEN (set as GitHub Secret).");

  const user = process.env.TURSO_USER || "sujay-k3";
  const suffix = process.env.TURSO_URL_SUFFIX || "aws-ap-south-1.turso.io";
  const prefix = process.env.TURSO_DB_PREFIX || "s27-";

  const batchSize = Math.max(1, Math.min(50, Number(process.env.WARM_BATCH || 6)));
  const concurrency = Math.max(1, Math.min(8, Number(process.env.WARM_CONCURRENCY || 3)));
  const intervalMin = Math.max(5, Number(process.env.WARM_INTERVAL_MIN || 10));
  const warmAcsPerDistrict = Math.max(0, Math.min(2, Number(process.env.WARM_ACS_PER_DISTRICT || 0)));

  const manifestPath = path.join(process.cwd(), "data", state, "district_manifest.json");
  const manifest = JSON.parse(fs.readFileSync(manifestPath, "utf8"));

  const districts = (manifest.districts || [])
    .map((d) => ({
      id: String(d.id || ""),
      slug: slugifyDistrictId(d.id),
      acs: Array.isArray(d.acs) ? d.acs.filter((x) => Number.isFinite(Number(x))).map(Number) : [],
    }))
    .filter((d) => d.slug);

  if (!districts.length) {
    console.log(`[warm-cycle] no districts found in ${manifestPath}`);
    return;
  }

  const slot = Math.floor(Date.now() / (intervalMin * 60 * 1000));
  const start = slot % districts.length;

  const batch = [];
  for (let i = 0; i < Math.min(batchSize, districts.length); i++) {
    batch.push(districts[(start + i) % districts.length]);
  }

  const { createClient } = await getLibsql();
  const idxTables = [
    "idx_voter_strict",
    "idx_voter_exact",
    "idx_voter_loose",
    "idx_relative_strict",
    "idx_relative_exact",
    "idx_relative_loose",
  ];

  console.log(`[warm-cycle] state=${state} slot=${slot} batch=${batch.length}/${districts.length}`);

  const results = await runPool(batch, concurrency, async (d) => {
    const t0 = Date.now();
    const url = buildDbUrl({ prefix, slug: d.slug, user, suffix });
    const client = createClient({ url, authToken: token });

    try {
      await client.execute({ sql: "SELECT 1 AS ok;", args: [] });

      await client.execute({
        sql: `SELECT 1 AS ok FROM voters WHERE "State Code" = ? LIMIT 1;`,
        args: [state],
      });

      for (const t of idxTables) {
        await client.execute({
          sql: `SELECT 1 AS ok FROM ${t} WHERE "State Code" = ? LIMIT 1;`,
          args: [state],
        });
      }

      if (warmAcsPerDistrict > 0 && d.acs.length) {
        for (let k = 0; k < warmAcsPerDistrict; k++) {
          const ac = d.acs[(slot + k) % d.acs.length];
          await client.execute({
            sql: `SELECT 1 AS ok FROM voters WHERE "State Code" = ? AND "AC No" = ? LIMIT 1;`,
            args: [state, ac],
          });
          for (const t of idxTables) {
            await client.execute({
              sql: `SELECT 1 AS ok FROM ${t} WHERE "State Code" = ? AND "AC No" = ? LIMIT 1;`,
              args: [state, ac],
            });
          }
        }
      }

      return { district: d.slug, ok: true, ms: Date.now() - t0 };
    } catch (e) {
      return { district: d.slug, ok: false, ms: Date.now() - t0, error: String(e?.message || e) };
    } finally {
      try { client.close?.(); } catch {}
    }
  });

  const okCount = results.filter((r) => r.ok).length;
  const failCount = results.length - okCount;

  console.log(`[warm-cycle] done ok=${okCount} fail=${failCount}`);
  if (failCount) {
    console.log("[warm-cycle] failures:", results.filter((r) => !r.ok));
    process.exitCode = 1;
  }
})();
