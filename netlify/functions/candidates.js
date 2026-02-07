// netlify/functions/candidates.js
// Returns candidate row_ids for a given district + AC, using precomputed prefix index tables in Turso.
// This keeps ALL fuzzy-ranking logic in worker.js (client-side). Server only does candidate generation.

function json(statusCode, obj) {
  return {
    statusCode,
    headers: {
      "content-type": "application/json; charset=utf-8",
      "cache-control": "no-store",
    },
    body: JSON.stringify(obj),
  };
}

function text(statusCode, msg) {
  return {
    statusCode,
    headers: {
      "content-type": "text/plain; charset=utf-8",
      "cache-control": "no-store",
    },
    body: String(msg || ""),
  };
}

function safeParseJson(str) {
  try {
    return JSON.parse(str);
  } catch {
    return null;
  }
}

function districtToDbSlug(idOrLabel) {
  const s = String(idOrLabel ?? "").trim();
  if (!s) return "";
  return s
    .toLowerCase()
    .replace(/_/g, "-")
    .replace(/\s+/g, "-")
    .replace(/[^a-z0-9-]/g, "-")
    .replace(/-+/g, "-")
    .replace(/^-|-$/g, "");
}

function qIdent(name) {
  // SQLite identifier quoting
  return `"${String(name).replace(/"/g, '""')}"`;
}

function isUint8Array(v) {
  return v && (v instanceof Uint8Array || (typeof Buffer !== "undefined" && Buffer.isBuffer(v)));
}

function decodeRowIds(val) {
  if (val == null) return [];

  // Already an array (rare)
  if (Array.isArray(val)) {
    return val.map((x) => Number(x)).filter(Number.isFinite);
  }

  // JSON/text
  if (typeof val === "string") {
    const s = val.trim();
    if (!s) return [];
    if (s.startsWith("[")) {
      try {
        const a = JSON.parse(s);
        return Array.isArray(a) ? a.map((x) => Number(x)).filter(Number.isFinite) : [];
      } catch {
        return [];
      }
    }
    return s
      .split(",")
      .map((x) => Number(String(x).trim()))
      .filter(Number.isFinite);
  }

  // BLOB
  if (isUint8Array(val) || val instanceof ArrayBuffer) {
    const u8 = val instanceof ArrayBuffer ? new Uint8Array(val) : new Uint8Array(val);
    if (!u8.length) return [];

    // If it looks like text, try text decode
    const b0 = u8[0];
    if (b0 === 0x5b /* [ */ || b0 === 0x2d /* - */ || (b0 >= 0x30 && b0 <= 0x39)) {
      try {
        const txt = new TextDecoder("utf-8").decode(u8).trim();
        if (txt.startsWith("[")) {
          const a = JSON.parse(txt);
          return Array.isArray(a) ? a.map((x) => Number(x)).filter(Number.isFinite) : [];
        }
        return txt
          .split(",")
          .map((x) => Number(String(x).trim()))
          .filter(Number.isFinite);
      } catch {
        // fall through to binary decode
      }
    }

    // Binary decode: try 64-bit little endian, else 32-bit little endian
    try {
      if (u8.length % 8 === 0) {
        const dv = new DataView(u8.buffer, u8.byteOffset, u8.byteLength);
        const out = [];
        for (let i = 0; i < u8.length; i += 8) {
          const n = dv.getBigInt64(i, true);
          const num = Number(n);
          if (Number.isFinite(num)) out.push(num);
        }
        return out;
      }
      if (u8.length % 4 === 0) {
        const dv = new DataView(u8.buffer, u8.byteOffset, u8.byteLength);
        const out = [];
        for (let i = 0; i < u8.length; i += 4) {
          const num = dv.getInt32(i, true);
          if (Number.isFinite(num)) out.push(num);
        }
        return out;
      }
    } catch {
      return [];
    }
  }

  return [];
}

async function listTables(client) {
  const rs = await client.execute(`SELECT name FROM sqlite_master WHERE type='table'`);
  const set = new Set();
  for (const r of rs.rows || []) set.add(String(r.name));
  return set;
}

function pickExistingTable(tablesSet, candidates) {
  for (const name of candidates) if (tablesSet.has(name)) return name;
  return "";
}

async function getIndexTableNames(client) {
  const tables = await listTables(client);

  // Most likely (from our build scripts)
  const strictVoter = pickExistingTable(tables, ["idx_voter", "idx_voter_strict", "index_prefix_3_voter"]);
  const strictRel = pickExistingTable(tables, ["idx_relative", "idx_relative_strict", "index_prefix_3_relative"]);

  const exactVoter = pickExistingTable(tables, ["idx_exact_voter", "idx_voter_exact", "index_exact_prefix_2_voter"]);
  const exactRel = pickExistingTable(tables, ["idx_exact_relative", "idx_relative_exact", "index_exact_prefix_2_relative"]);

  const looseVoter = pickExistingTable(tables, ["idx_loose_voter", "idx_voter_loose", "index_loose_prefix_2_voter"]);
  const looseRel = pickExistingTable(tables, ["idx_loose_relative", "idx_relative_loose", "index_loose_prefix_2_relative"]);

  return { strictVoter, strictRel, exactVoter, exactRel, looseVoter, looseRel };
}

async function getIndexCols(client, tableName) {
  const rs = await client.execute(`PRAGMA table_info(${qIdent(tableName)})`);
  const cols = new Set((rs.rows || []).map((r) => String(r.name)));

  const acCol = cols.has("ac") ? "ac" : (cols.has("AC No") ? "AC No" : (cols.has("ac_no") ? "ac_no" : "ac"));
  const keyCol = cols.has("key") ? "key" : (cols.has("Key") ? "Key" : "key");
  const rowIdsCol = cols.has("row_ids") ? "row_ids" : (cols.has("rowIds") ? "rowIds" : (cols.has("rowids") ? "rowids" : "row_ids"));

  return { acCol, keyCol, rowIdsCol };
}

async function postingsToHitCountMap(client, tableName, ac, keys) {
  const out = new Map();
  if (!tableName || !keys || !keys.length) return out;

  // Hard limits to protect the function
  if (keys.length > 200) keys = keys.slice(0, 200);

  const { acCol, keyCol, rowIdsCol } = await getIndexCols(client, tableName);

  const placeholders = keys.map(() => "?").join(",");
  const sql = `SELECT ${qIdent(keyCol)} AS k, ${qIdent(rowIdsCol)} AS r FROM ${qIdent(tableName)} WHERE ${qIdent(acCol)} = ? AND ${qIdent(keyCol)} IN (${placeholders})`;
  const args = [ac, ...keys];

  const rs = await client.execute({ sql, args });

  for (const row of rs.rows || []) {
    const rids = decodeRowIds(row.r);
    for (const rid of rids) {
      out.set(rid, (out.get(rid) || 0) + 1);
    }
  }

  return out;
}

function mapToMeta(hitMap, keysLen) {
  const meta = new Map();
  const need = Number(keysLen) || 0;
  for (const [rid, hit] of hitMap.entries()) {
    meta.set(rid, { hit_count: hit, and_hit: need > 0 ? hit === need : false });
  }
  return meta;
}

function unionKeys(...maps) {
  const s = new Set();
  for (const m of maps) for (const k of m.keys()) s.add(k);
  return s;
}

let CLIENT_CACHE = new Map(); // key -> { client, idxNames }

async function getClientForDistrict(districtSlug) {
  const groupMap = {
    // Account A (sujay-k1)
    "chatra": "A",
    "hazaribagh": "A",
    "deoghar": "A",
    "jamtara": "A",
    "dumka": "A",
    "kodarma": "A",
    "pakur": "A",
    "ramgarh": "A",
    "giridih": "A",
    "sahebganj": "A",
    "godda": "A",

    // Account B (sujay-k2)
    "bokaro": "B",
    "khunti": "B",
    "dhanbad": "B",
    "ranchi": "B",
    "east-singhbhum": "B",
    "saraikela-kharswan": "B",
    "gumla": "B",
    "west-singhbhum": "B",

    // Account C (sujay-k3)
    "garhwa": "C",
    "lohardaga": "C",
    "simdega": "C",
    "latehar": "C",
    "palamu": "C",
  };

  const group = groupMap[districtSlug];
  if (!group) throw new Error(`Unknown district group for "${districtSlug}"`);

  const org = group === "A" ? (process.env.TURSO_ORG_A || "sujay-k1")
    : group === "B" ? (process.env.TURSO_ORG_B || "sujay-k2")
    : (process.env.TURSO_ORG_C || "sujay-k3");

  const token = group === "A" ? process.env.TURSO_TOKEN_A
    : group === "B" ? process.env.TURSO_TOKEN_B
    : process.env.TURSO_TOKEN_C;

  if (!token) throw new Error(`Missing TURSO_TOKEN_${group}`);

  const region = process.env.TURSO_REGION || "aws-ap-south-1";
  const hostSuffix = process.env.TURSO_HOST_SUFFIX || "turso.io";

  const dbName = `s27-${districtSlug}`;
  const url = `libsql://${dbName}-${org}.${region}.${hostSuffix}`;

  const cacheKey = `${group}:${url}`;
  const cached = CLIENT_CACHE.get(cacheKey);
  if (cached) return cached;

  const { createClient } = await import("@libsql/client");
  const client = createClient({ url, authToken: token });

  const idxNames = await getIndexTableNames(client);

  const entry = { client, idxNames };
  CLIENT_CACHE.set(cacheKey, entry);
  return entry;
}

exports.handler = async (event) => {
  try {
    if (event.httpMethod === "OPTIONS") {
      return {
        statusCode: 204,
        headers: {
          "access-control-allow-origin": "*",
          "access-control-allow-methods": "POST,OPTIONS",
          "access-control-allow-headers": "content-type",
        },
        body: "",
      };
    }

    if (event.httpMethod !== "POST") return text(405, "Method Not Allowed");

    const body = safeParseJson(event.body || "");
    if (!body) return text(400, "Invalid JSON");

    const district = districtToDbSlug(body.district ?? body.districtId);
    const ac = Number(body.ac);
    const scope = String(body.scope || "voter");
    const exactOn = !!body.exactOn;

    const strictKeys = Array.isArray(body.strictKeys) ? body.strictKeys.map(String) : [];
    const exactKeys = Array.isArray(body.exactKeys) ? body.exactKeys.map(String) : [];
    const looseKeys = Array.isArray(body.looseKeys) ? body.looseKeys.map(String) : [];

    if (!district) return text(400, "Missing district");
    if (!Number.isFinite(ac)) return text(400, "Invalid ac");

    const { client, idxNames } = await getClientForDistrict(district);

    const keysStrict = strictKeys.filter(Boolean);
    const keysExact = exactKeys.filter(Boolean);
    const keysLoose = looseKeys.filter(Boolean);

    let strictVoterHits = new Map(), strictRelHits = new Map();
    let exactVoterHits = new Map(), exactRelHits = new Map();
    let looseVoterHits = new Map(), looseRelHits = new Map();

    if (scope === "voter") {
      if (keysStrict.length && idxNames.strictVoter) strictVoterHits = await postingsToHitCountMap(client, idxNames.strictVoter, ac, keysStrict);
      if (keysExact.length && idxNames.exactVoter) exactVoterHits = await postingsToHitCountMap(client, idxNames.exactVoter, ac, keysExact);
      if (keysLoose.length && idxNames.looseVoter) looseVoterHits = await postingsToHitCountMap(client, idxNames.looseVoter, ac, keysLoose);
    } else if (scope === "relative") {
      if (keysStrict.length && idxNames.strictRel) strictRelHits = await postingsToHitCountMap(client, idxNames.strictRel, ac, keysStrict);
      if (keysExact.length && idxNames.exactRel) exactRelHits = await postingsToHitCountMap(client, idxNames.exactRel, ac, keysExact);
      if (keysLoose.length && idxNames.looseRel) looseRelHits = await postingsToHitCountMap(client, idxNames.looseRel, ac, keysLoose);
    } else {
      if (keysStrict.length) {
        if (idxNames.strictVoter) strictVoterHits = await postingsToHitCountMap(client, idxNames.strictVoter, ac, keysStrict);
        if (idxNames.strictRel) strictRelHits = await postingsToHitCountMap(client, idxNames.strictRel, ac, keysStrict);
      }
      if (keysExact.length) {
        if (idxNames.exactVoter) exactVoterHits = await postingsToHitCountMap(client, idxNames.exactVoter, ac, keysExact);
        if (idxNames.exactRel) exactRelHits = await postingsToHitCountMap(client, idxNames.exactRel, ac, keysExact);
      }
      if (keysLoose.length) {
        if (idxNames.looseVoter) looseVoterHits = await postingsToHitCountMap(client, idxNames.looseVoter, ac, keysLoose);
        if (idxNames.looseRel) looseRelHits = await postingsToHitCountMap(client, idxNames.looseRel, ac, keysLoose);
      }
    }

    const strictVoterMeta = mapToMeta(strictVoterHits, keysStrict.length);
    const strictRelMeta = mapToMeta(strictRelHits, keysStrict.length);
    const exactVoterMeta = mapToMeta(exactVoterHits, keysExact.length);
    const exactRelMeta = mapToMeta(exactRelHits, keysExact.length);
    const looseVoterMeta = mapToMeta(looseVoterHits, keysLoose.length);
    const looseRelMeta = mapToMeta(looseRelHits, keysLoose.length);

    const all = unionKeys(strictVoterHits, strictRelHits, exactVoterHits, exactRelHits, looseVoterHits, looseRelHits);
    const candidates = Array.from(all).sort((a, b) => a - b);

    const metaByRow = {};
    for (const rid of candidates) {
      const sv = strictVoterMeta.get(rid);
      const sr = strictRelMeta.get(rid);
      const ev = exactVoterMeta.get(rid);
      const er = exactRelMeta.get(rid);
      const lv = looseVoterMeta.get(rid);
      const lr = looseRelMeta.get(rid);

      const merged_hit_count =
        (sv?.hit_count || 0) +
        (sr?.hit_count || 0) +
        (ev?.hit_count || 0) +
        (er?.hit_count || 0) +
        (lv?.hit_count || 0) +
        (lr?.hit_count || 0);

      metaByRow[String(rid)] = {
        strict_voter_hit_count: sv?.hit_count || 0,
        strict_voter_and_hit: !!sv?.and_hit,
        strict_rel_hit_count: sr?.hit_count || 0,
        strict_rel_and_hit: !!sr?.and_hit,

        exact_voter_hit_count: ev?.hit_count || 0,
        exact_voter_and_hit: !!ev?.and_hit,
        exact_rel_hit_count: er?.hit_count || 0,
        exact_rel_and_hit: !!er?.and_hit,

        loose_voter_hit_count: lv?.hit_count || 0,
        loose_voter_and_hit: !!lv?.and_hit,
        loose_rel_hit_count: lr?.hit_count || 0,
        loose_rel_and_hit: !!lr?.and_hit,

        merged_hit_count,
        merged_and_hit: (sv?.and_hit || sr?.and_hit || ev?.and_hit || er?.and_hit || lv?.and_hit || lr?.and_hit) ? true : false,
      };
    }

    return json(200, { candidates, metaByRow });
  } catch (e) {
    console.error("candidates error:", e);
    return text(500, e && e.message ? e.message : "Server error");
  }
};
