
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
      and_hit: (count === totalKeys),
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


    const keys = Array.from(new Set(keysIn));
    const totalKeys = keys.length;

    const client = await getClient(district);


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
    for (const row of (rs.rows || [])) {
      got.set(String(row.key ?? row["key"]), row);
    }


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
