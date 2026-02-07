// netlify/functions/candidates.js
//
// Phase 2A (parity-first):
// - Client sends precomputed keys (strict/exact/loose) exactly as app.js does today.
// - Server unions row_ids from index tables and returns minimal voter rows.
//
// Expected request JSON:
// {
//   "state_code": "S27",
//   "ac_no": 1,
//   "exact_on": true,
//   "scope": "voter" | "relative" | "anywhere",
//   "keys": {
//     "strict_voter": ["0इम", "..."],
//     "exact_voter":  ["0I", "..."],
//     "loose_voter":  ["0I", "..."],
//     "strict_relative": [...],
//     "exact_relative": [...],
//     "loose_relative": [...]
//   },
//   "max_candidates": 50000
// }
//
// Response JSON:
// {
//   "ac_no": 1,
//   "row_ids": [...],              // distinct candidate row_ids
//   "rows": [                      // minimal worker payload rows
//     { "row_id": 45274, "voter_name_raw":"...", "relative_name_raw":"...", "serial_no":"1" },
//     ...
//   ]
// }

import { createClient } from "@libsql/client";

const DB_URL = process.env.TURSO_DATABASE_URL;
const DB_AUTH_TOKEN = process.env.TURSO_AUTH_TOKEN;

function json(statusCode, obj) {
  return {
    statusCode,
    headers: {
      "content-type": "application/json; charset=utf-8",
      "cache-control": "no-store",
      "access-control-allow-origin": "*",
      "access-control-allow-headers": "content-type",
      "access-control-allow-methods": "POST,OPTIONS",
    },
    body: JSON.stringify(obj),
  };
}

function uniq(arr) {
  return Array.from(new Set(arr));
}

function chunk(arr, size) {
  const out = [];
  for (let i = 0; i < arr.length; i += size) out.push(arr.slice(i, i + size));
  return out;
}

function requireEnv() {
  if (!DB_URL || !DB_AUTH_TOKEN) {
    throw new Error("Missing TURSO_DATABASE_URL or TURSO_AUTH_TOKEN");
  }
}

function tableFor(kind) {
  // kind examples: "strict_voter", "exact_relative", "loose_voter"
  if (kind === "strict_voter") return "idx_prefix3_voter";
  if (kind === "strict_relative") return "idx_prefix3_relative";
  if (kind === "exact_voter") return "idx_exact2_voter";
  if (kind === "exact_relative") return "idx_exact2_relative";
  if (kind === "loose_voter") return "idx_loose2_voter";
  if (kind === "loose_relative") return "idx_loose2_relative";
  return null;
}

function kindsForScope(scope) {
  if (scope === "voter") return ["strict_voter", "exact_voter", "loose_voter"];
  if (scope === "relative") return ["strict_relative", "exact_relative", "loose_relative"];
  // anywhere
  return [
    "strict_voter", "exact_voter", "loose_voter",
    "strict_relative", "exact_relative", "loose_relative",
  ];
}

export async function handler(event) {
  if (event.httpMethod === "OPTIONS") return json(200, { ok: true });
  if (event.httpMethod !== "POST") return json(405, { error: "POST only" });

  try {
    requireEnv();
    const client = createClient({ url: DB_URL, authToken: DB_AUTH_TOKEN });

    const body = JSON.parse(event.body || "{}");

    const state_code = String(body.state_code || "S27");
    const ac_no = Number(body.ac_no);
    const exact_on = !!body.exact_on;
    const scope = String(body.scope || "anywhere");
    const keys = body.keys || {};
    const max_candidates = Number(body.max_candidates || 50000);

    if (!Number.isFinite(ac_no) || ac_no <= 0) {
      return json(400, { error: "ac_no must be a positive number" });
    }

    const wantKinds = kindsForScope(scope);

    // Parity rule: if exact_on === true, current app.js skips loose stage
    // (your earlier contract: loose skipped when exactOn=true).
    const effectiveKinds = exact_on
      ? wantKinds.filter(k => !k.startsWith("loose_"))
      : wantKinds;

    // Collect row_ids by querying index tables for each kind
    const rowIdSet = new Set();

    for (const kind of effectiveKinds) {
      const t = tableFor(kind);
      if (!t) continue;
      const klist = Array.isArray(keys[kind]) ? keys[kind] : [];
      if (klist.length === 0) continue;

      // libsql parameter limits are safer with chunking
      for (const kchunk of chunk(uniq(klist), 200)) {
        const placeholders = kchunk.map(() => "?").join(",");
        const sql = `
          SELECT row_id
          FROM ${t}
          WHERE ac_no = ?
            AND key IN (${placeholders})
        `;
        const params = [ac_no, ...kchunk];
        const rs = await client.execute({ sql, args: params });
        for (const r of rs.rows) {
          rowIdSet.add(Number(r.row_id));
          if (rowIdSet.size >= max_candidates) break;
        }
      }

      if (rowIdSet.size >= max_candidates) break;
    }

    const row_ids = Array.from(rowIdSet);

    // Fetch minimal worker payload rows
    const rows = [];
    for (const idChunk of chunk(row_ids, 200)) {
      const placeholders = idChunk.map(() => "?").join(",");
      const sql = `
        SELECT row_id, voter_name_raw, relative_name_raw, serial_no
        FROM voters
        WHERE ac_no = ?
          AND row_id IN (${placeholders})
      `;
      const params = [ac_no, ...idChunk];
      const rs = await client.execute({ sql, args: params });
      for (const r of rs.rows) {
        rows.push({
          row_id: Number(r.row_id),
          voter_name_raw: r.voter_name_raw ?? "",
          relative_name_raw: r.relative_name_raw ?? "",
          serial_no: r.serial_no ?? "",
        });
      }
    }

    return json(200, { ac_no, row_ids, rows });
  } catch (e) {
    return json(500, { error: String(e?.message || e) });
  }
}
