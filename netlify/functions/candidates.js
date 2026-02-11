/**
 * netlify/functions/candidates.js
 *
 * Fetch candidate row_ids from prefix indexes, per (district,state,ac).
 *
 * Supports TWO request shapes:
 *
 * 1) Single query (backwards compatible):
 *    { district, state, ac, table, keys }
 *    -> { ok:true, rows:[{row_id,hit_count,and_hit}] }
 *
 * 2) Batched queries (preferred):
 *    { district, state, ac, queries:[{ tag, table, keys }] }
 *    -> { ok:true, results:{ [tag]: { rows:[...] } } }
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

// ---------- Tiny LRU for decoded arrays (warm instance only) ----------
const DECODE_CACHE = new Map(); // key -> { arr, ts }
const DECODE_CACHE_MAX = 1200; // higher since we now batch
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

  // evict ~25% oldest
  const entries = Array.from(DECODE_CACHE.entries());
  entries.sort((a, b) => a[1].ts - b[1].ts);
  const evictN = Math.max(1, Math.floor(DECODE_CACHE_MAX * 0.25));
  for (let i = 0; i < evictN; i++) DECODE_CACHE.delete(entries[i][0]);
}

// ---------- Min-heap merge (counts across sorted lists) ----------
class MinHeap {
  constructor() {
    this.a = [];
  }
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
  peek() {
    return this.a.length ? this.a[0] : null;
  }
  get size() {
    return this.a.length;
  }
}

function mergeCounts(lists, totalKeys) {
  const heap = new MinHeap();

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

    let count = 1;

    {
      const L = lists[first.li];
      L.idx++;
      if (L.idx < L.arr.length) heap.push({ v: L.arr[L.idx] >>> 0, li: first.li });
    }

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
      and_hit: count === totalKeys,
    });
  }

  return out;
}


function mergeUnion(lists) {
  // Merge sorted uint32 arrays into a single sorted unique list.
  const heap = new MinHeap();

  for (let li = 0; li < lists.length; li++) {
    const arr = lists[li].arr;
    if (arr.length) {
      heap.push({ v: arr[0] >>> 0, li });
      lists[li].idx = 0;
    }
  }

  const out = [];
  let last = null;

  while (heap.size) {
    const first = heap.pop();
    const v = first.v >>> 0;

    const L = lists[first.li];
    L.idx++;
    if (L.idx < L.arr.length) heap.push({ v: L.arr[L.idx] >>> 0, li: first.li });

    if (last === null || v !== last) {
      out.push(v);
      last = v;
    }
  }

  return out;
}

function normalizeKeys(keysIn) {
  if (!Array.isArray(keysIn)) return [];
  const keys = keysIn
    .map((x) => asString(x))
    .map((s) => s.trim())
    .filter(Boolean);
  return Array.from(new Set(keys));
}

async function fetchRowsForKeys(client, { table, state, ac, keys }) {
  if (!keys.length) return new Map();

  // NOTE: json_each preserves input order but we don't need it here.
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

  const got = new Map();
  for (const row of rs.rows || []) {
    got.set(String(row.key ?? row["key"]), row);
  }
  return got;
}

function decodeKeyRow(table, state, ac, key, row) {
  const ck = cacheKey(table, state, ac, key);
  let arr = cacheGet(ck);
  if (!arr) {
    const n = row.n ?? row["n"];
    arr = decodeRowIds(row.row_ids ?? row["row_ids"], n);
    cacheSet(ck, arr);
  }
  return arr || [];
}

async function runSingleQuery(client, { table, state, ac, keys }) {
  const totalKeys = keys.length;
  if (!totalKeys) return [];

  const got = await fetchRowsForKeys(client, { table, state, ac, keys });

  const lists = [];
  let decodedTotal = 0;

  for (const k of keys) {
    const row = got.get(k);
    if (!row) continue;

    const arr = decodeKeyRow(table, state, ac, k, row);
    if (arr.length) {
      lists.push({ arr, idx: 0 });
      decodedTotal += arr.length;
    }
  }

  if (!lists.length) return [];

  const rowsOut = mergeCounts(lists, totalKeys);
  return { rowsOut, decodedTotal, lists: lists.length, totalKeys };
}

exports.handler = async (event) => {
  const t0 = Date.now();
  try {
    if (event.httpMethod === "OPTIONS") return ok({});
    if (event.httpMethod !== "POST") return badRequest("POST required");

    const body = await readJsonBody(event);
    if (!body) return badRequest("Invalid JSON");

    const district = asString(body.district).trim();
    const state = asString(body.state, "S27").trim();
    const ac = asInt(body.ac);

    if (!district) return badRequest("Missing district");
    if (!state) return badRequest("Missing state");
    if (!Number.isFinite(ac)) return badRequest("Missing/invalid ac");

    const client = await getClient(district);

    // ------------------------------
    // Batched queries
    // ------------------------------
    if (Array.isArray(body.queries) && body.queries.length) {
      const queriesIn = body.queries;
      const ret = asString(body.ret, "ids").trim().toLowerCase(); // "ids" (default) | "full"

      // Validate + normalize; group by table to minimize DB round-trips.
      const byTable = new Map();
      for (let i = 0; i < queriesIn.length; i++) {
        const q = queriesIn[i] || {};
        const tag = asString(q.tag || `q${i}`).trim() || `q${i}`;
        const table = asString(q.table).trim();
        const keys = normalizeKeys(q.keys);

        if (!ALLOWED_TABLES.has(table)) return badRequest(`Invalid table: ${table}`);

        if (!byTable.has(table)) byTable.set(table, { allKeys: new Set(), items: [] });
        const bucket = byTable.get(table);

        // Keep items only for "full" mode; in "ids" mode we only need the union.
        if (ret === "full") bucket.items.push({ tag, table, keys });

        keys.forEach((k) => bucket.allKeys.add(k));
      }

      // ---- Fast path: ids-only (no hit_count/and_hit) ----
      if (ret !== "full") {
        let tablesTouched = 0;
        let decodedTotalAll = 0;

        const lists = [];

        // We could parallelize per-table queries, but keeping it sequential is
        // usually fine and avoids overloading libsql on cold starts.
        for (const [table, bucket] of byTable.entries()) {
          const allKeys = Array.from(bucket.allKeys);
          if (!allKeys.length) continue;

          const got = await fetchRowsForKeys(client, { table, state, ac, keys: allKeys });
          tablesTouched++;

          for (const k of allKeys) {
            const row = got.get(k);
            if (!row) continue;
            const arr = decodeKeyRow(table, state, ac, k, row);
            if (arr && arr.length) {
              lists.push({ arr, idx: 0 });
              decodedTotalAll += arr.length;
            }
          }
        }

        const ids = lists.length ? mergeUnion(lists) : [];

        const ms = Date.now() - t0;
        console.log(
          `[candidates] IDS district=${district} ac=${ac} tables=${tablesTouched} keys_union=${Array.from(byTable.values()).reduce((s,b)=>s+b.allKeys.size,0)} decoded_total=${decodedTotalAll} out=${ids.length} (${ms}ms)`
        );

        return ok({ ids });
      }

      // ---- Full mode: per-tag rows with hit_count + and_hit (debug/back-compat) ----
      const results = {};
      let tablesTouched = 0;
      let decodedTotalAll = 0;

      for (const [table, bucket] of byTable.entries()) {
        const allKeys = Array.from(bucket.allKeys);

        // Query once per table for the union of keys.
        let got = new Map();
        if (allKeys.length) {
          got = await fetchRowsForKeys(client, { table, state, ac, keys: allKeys });
          tablesTouched++;
        }

        // Decode per-key once, reuse for all tags.
        const decodedByKey = new Map();
        for (const k of allKeys) {
          const row = got.get(k);
          if (!row) continue;
          decodedByKey.set(k, decodeKeyRow(table, state, ac, k, row));
        }

        for (const item of bucket.items) {
          const totalKeys = item.keys.length;
          if (!totalKeys) {
            results[item.tag] = { rows: [] };
            continue;
          }

          const lists2 = [];
          let decodedTotal = 0;

          for (const k of item.keys) {
            const arr = decodedByKey.get(k) || [];
            if (arr.length) {
              lists2.push({ arr, idx: 0 });
              decodedTotal += arr.length;
            }
          }

          if (!lists2.length) {
            results[item.tag] = { rows: [] };
            continue;
          }

          const rowsOut = mergeCounts(lists2, totalKeys);
          results[item.tag] = { rows: rowsOut };

          decodedTotalAll += decodedTotal;
        }
      }

      const ms = Date.now() - t0;
      console.log(
        `[candidates] FULL district=${district} ac=${ac} tags=${Object.keys(results).length} tables=${tablesTouched} decoded_total=${decodedTotalAll} (${ms}ms)`
      );

      return ok({ results });
    }

    // ------------------------------
    // Single query (backwards compatible)
    // ------------------------------
    const table = asString(body.table).trim();
    const keys = normalizeKeys(body.keys);

    if (!ALLOWED_TABLES.has(table)) return badRequest("Invalid table");
    if (!keys.length) return ok({ rows: [] });

    const out = await runSingleQuery(client, { table, state, ac, keys });
    if (!out || !out.rowsOut || !out.rowsOut.length) {
      const ms = Date.now() - t0;
      console.log(`[candidates] ${district} ac=${ac} table=${table} keys=${keys.length} -> 0 (${ms}ms)`);
      return ok({ rows: [] });
    }

    const ms = Date.now() - t0;
    console.log(
      `[candidates] ${district} ac=${ac} table=${table} keys=${keys.length} lists=${out.lists} decoded_total=${out.decodedTotal} out=${out.rowsOut.length} (${ms}ms)`
    );

    return ok({ rows: out.rowsOut });
  } catch (err) {
    const ms = Date.now() - t0;
    console.log(`[candidates] ERROR after ${ms}ms`, err && err.message ? err.message : err);
    return serverError(err);
  }
};
