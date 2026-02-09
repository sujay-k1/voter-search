/**
 * netlify/functions/candidates.js
 *
 * Speed-optimized candidate aggregation for blob-encoded row_ids:
 * - Avoids per-row Map increments (very slow for huge lists)
 * - Uses multi-way merge over sorted row_id lists to compute hit_count
 * - Adds in-memory cache for decoded (table,state,ac,key)->row_id array
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

// ---------- Tiny LRU for decoded arrays ----------
const DECODE_CACHE = new Map(); // key -> { arr, ts }
const DECODE_CACHE_MAX = 600;   // tune later; warm instance only
function cacheKey(table, state, ac, key) {
  return `${table}|${state}|${ac}|${key}`;
}
function cacheGet(k) {
  const v = DECODE_CACHE.get(k);
  if (!v) return null;
  v.ts = Date.now();
  return v.arr;
}
function cacheSet(k, arr) {
  DECODE_CACHE.set(k, { arr, ts: Date.now() });
  if (DECODE_CACHE.size <= DECODE_CACHE_MAX) return;

  // evict ~20% oldest
  const entries = Array.from(DECODE_CACHE.entries());
  entries.sort((a, b) => a[1].ts - b[1].ts);
  const evictN = Math.max(1, Math.floor(DECODE_CACHE_MAX * 0.2));
  for (let i = 0; i < evictN; i++) DECODE_CACHE.delete(entries[i][0]);
}

// ---------- Min-heap (value, listIndex) ----------
class MinHeap {
  constructor() { this.a = []; }
  push(x) {
    const a = this.a;
    a.push(x);
    let i = a.length - 1;
    while (i > 0) {
      const p = (i - 1) >> 1;
      if (a[p].v <= a[i].v) break;
      [a[p], a[i]] = [a[i], a[p]];
      i = p;
    }
  }
  pop() {
    const a = this.a;
    if (!a.length) return null;
    const top = a[0];
    const last = a.pop();
    if (a.length) {
      a[0] = last;
      let i = 0;
      while (true) {
        const l = i * 2 + 1;
        const r = l + 1;
        let m = i;
        if (l < a.length && a[l].v < a[m].v) m = l;
        if (r < a.length && a[r].v < a[m].v) m = r;
        if (m === i) break;
        [a[i], a[m]] = [a[m], a[i]];
        i = m;
      }
    }
    return top;
  }
  peek() { return this.a.length ? this.a[0] : null; }
  get size() { return this.a.length; }
}

/**
 * Multi-way merge over sorted arrays to compute counts:
 * Input: lists = [{arr, idx, li}, ...] for non-empty arrays only
 * totalKeys = number of keys requested (including missing keys)
 */
function mergeCounts(lists, totalKeys) {
  const heap = new MinHeap();

  // Push first element of each list
  for (let li = 0; li < lists.length; li++) {
    const arr = lists[li].arr;
    if (arr.length) {
      heap.push({ v: arr[0] >>> 0, li });
      lists[li].idx = 0;
    }
  }

  const out = [];
  while (heap.size) {
    const first = heap.pop();
    const v = first.v >>> 0;

    // count how many lists contain this value
    let count = 1;

    // advance the list that produced this value
    {
      const L = lists[first.li];
      L.idx++;
      if (L.idx < L.arr.length) heap.push({ v: L.arr[L.idx] >>> 0, li: first.li });
    }

    // collect any other lists with the same v
    while (heap.peek() && (heap.peek().v >>> 0) === v) {
      const x = heap.pop();
      count++;

      const L = lists[x.li];
      L.idx++;
      if (L.idx < L.arr.length) heap.push({ v: L.arr[L.idx] >>> 0, li: x.li });
    }

    out.push({
      row_id: v,
      hit_count: count,
      and_hit: (count === totalKeys), // will be false if any key missing (because count <= lists.length < totalKeys)
    });
  }

  return out;
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
    const ac = asInt(body.ac);
    const table = asString(body.table);
    const keysIn = Array.isArray(body.keys) ? body.keys.map((x) => asString(x)).filter(Boolean) : [];

    if (!district) return badRequest("Missing district");
    if (!state) return badRequest("Missing state");
    if (!Number.isFinite(ac)) return badRequest("Missing/invalid ac");
    if (!ALLOWED_TABLES.has(table)) return badRequest("Invalid table");
    if (!keysIn.length) return ok({ rows: [] });

    // De-dupe keys; important for correct hit_count semantics
    const keys = Array.from(new Set(keysIn));
    const totalKeys = keys.length;

    const client = await getClient(district);

    // Fetch blobs for keys (single query using json_each)
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

    const rs = await client.execute({
      sql,
      args: [JSON.stringify(keys), state, ac],
    });

    // Map returned rows by key so we can detect missing keys
    const got = new Map();
    for (const row of (rs.rows || [])) {
      got.set(String(row.key ?? row["key"]), row);
    }

    // Decode only the keys that exist; missing keys imply and_hit always false (correct)
    const lists = [];
    let decodedTotal = 0;

    for (const k of keys) {
      const row = got.get(k);
      if (!row) continue;

      const ck = cacheKey(table, state, ac, k);
      let arr = cacheGet(ck);
      if (!arr) {
        const n = row.n ?? row["n"];
        arr = decodeRowIds(row.row_ids ?? row["row_ids"], n);
        // Ensure numbers
        // Assume sorted (highly likely; delta encoding implies monotonic). Do NOT sort (would be too slow).
        cacheSet(ck, arr);
      }

      if (arr && arr.length) {
        lists.push({ arr, idx: 0 });
        decodedTotal += arr.length;
      }
    }

    if (!lists.length) {
      const ms = Date.now() - t0;
      console.log(`[candidates] ${district} ac=${ac} table=${table} keys=${totalKeys} -> 0 rows (${ms}ms)`);
      return ok({ rows: [] });
    }

    const rowsOut = mergeCounts(lists, totalKeys);

    const ms = Date.now() - t0;
    console.log(
      `[candidates] ${district} ac=${ac} table=${table} keys=${totalKeys} lists=${lists.length} decoded_total=${decodedTotal} out=${rowsOut.length} (${ms}ms)`
    );

    return ok({ rows: rowsOut });
  } catch (err) {
    const ms = Date.now() - t0;
    console.log(`[candidates] ERROR after ${ms}ms`, err && err.message ? err.message : err);
    return serverError(err);
  }
};
