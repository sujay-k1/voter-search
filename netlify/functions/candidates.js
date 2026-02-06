
// netlify/functions/candidates.js
// Returns candidate rows + per-row meta in the SAME SHAPE your current client uses,
// so your existing worker.js ranking can remain unchanged.
//
// Query params:
// - state: e.g. S27 (optional, currently informational)
// - ac: integer (required)
// - scope: voter | relative | anywhere (required)
// - q: query string (required)
// - exactOn: "1" means strict ∪ exact (NO loose). "0" means strict ∪ exact ∪ loose.
//          This matches your current exactOnFromIncludeTyping() behavior.
// Response:
// {
//   ac: 1,
//   strictKeys: [...],
//   exactKeys: [...],
//   looseKeys: [...],
//   rows: [ { row_id, voter_name_raw, relative_name_raw, voter_name_norm, relative_name_norm, serial_no, _meta } ]
// }

import { createClient } from "@libsql/client";

// ---- constants (mirror client) ----
const PREFIX_LEN_STRICT = 3;
const PREFIX_LEN_LOOSE = 2;
const PREFIX_LEN_EXACT = 2;

// ---- init Turso client ----
function getDb() {
  const url = process.env.TURSO_DATABASE_URL;
  const authToken = process.env.TURSO_AUTH_TOKEN;
  if (!url || !authToken) {
    throw new Error("Missing TURSO_DATABASE_URL / TURSO_AUTH_TOKEN");
  }
  return createClient({ url, authToken });
}

// ---- helpers copied from app.js (minimal + identical behavior) ----
function norm(s) {
  if (s == null) return "";
  s = String(s).replace(/\u00a0/g, " ").trim();
  s = s.replace(/[.,;:|/\\()[\]{}<>"'~!@#$%^&*_+=?-]/g, " ");
  s = s.replace(/\s+/g, " ").trim();
  return s;
}
function tokenize(s) {
  s = norm(s);
  if (!s) return [];
  return s.split(" ").filter(Boolean);
}
function prefixN(token, n) {
  token = (token || "").replace(/\s+/g, "");
  if (!token) return "";
  return token.length >= n ? token.slice(0, n) : token;
}

const INDEP_VOWEL_MAP = new Map(
  Object.entries({
    अ: "A", आ: "A",
    इ: "I", ई: "I",
    उ: "U", ऊ: "U",
    ए: "E", ऐ: "E",
    ओ: "O", औ: "O",
    ऋ: "R", ॠ: "R",
    ऌ: "L", ॡ: "L",
  })
);

const MATRA_MAP = new Map(
  Object.entries({
    "ा": "A",
    "ि": "I", "ी": "I",
    "ु": "U", "ू": "U",
    "े": "E", "ै": "E",
    "ो": "O", "ौ": "O",
    "ृ": "R", "ॄ": "R",
    "ॢ": "L", "ॣ": "L",
  })
);

const REMOVE_MARKS = new Set(["ँ", "ं", "ः", "़", "्"]);

function normExactIndex(s) {
  s = norm(s);
  if (!s) return "";
  let out = "";
  for (const ch of s) {
    if (REMOVE_MARKS.has(ch)) continue;
    if (INDEP_VOWEL_MAP.has(ch)) out += INDEP_VOWEL_MAP.get(ch);
    else if (MATRA_MAP.has(ch)) out += MATRA_MAP.get(ch);
    else out += ch;
  }
  out = out.replace(/\s+/g, " ").trim();
  return out;
}
function tokenizeExactIndex(s) {
  s = normExactIndex(s);
  if (!s) return [];
  return s.split(" ").filter(Boolean);
}

const CONFUSABLE_SETS = [
  ["द", "ढ", "ह"],
  ["ब", "व"],
  ["स", "श"],
  ["त", "न"],
  ["ड", "ढ"],
];
const CONF_MAP = (() => {
  const m = new Map();
  for (const set of CONFUSABLE_SETS) {
    const rep = set[0];
    for (const ch of set) m.set(ch, rep);
  }
  return m;
})();
function applyConfusableFoldLoose(s) {
  if (!s) return "";
  let out = "";
  for (const ch of s) out += CONF_MAP.get(ch) || ch;
  // client special-case
  out = out.replace(/रव/g, "ख");
  return out;
}
function normLoose(s) {
  s = norm(s);
  if (!s) return "";
  let out = "";
  for (const ch of s) {
    if (INDEP_VOWEL_MAP.has(ch)) out += INDEP_VOWEL_MAP.get(ch);
    else if (MATRA_MAP.has(ch)) out += MATRA_MAP.get(ch);
    else if (REMOVE_MARKS.has(ch)) continue;
    else out += ch;
  }
  out = applyConfusableFoldLoose(out);
  out = out.replace(/\s+/g, " ").trim();
  return out;
}
function tokenizeLoose(s) {
  s = normLoose(s);
  if (!s) return [];
  return s.split(" ").filter(Boolean);
}

// join variants identical to app.js
function joinVariantsTokens(tokens) {
  const toks = tokens.slice().filter(Boolean);
  const n = toks.length;
  if (n <= 1) return [];
  const out = new Set();

  if (n <= 3) {
    for (let i = 0; i < n - 1; i++) {
      const merged = toks
        .slice(0, i)
        .concat([toks[i] + toks[i + 1]])
        .concat(toks.slice(i + 2));
      out.add(merged.join(" "));
    }
    out.add(toks.join(""));
    const final = new Set();
    for (const s of out) final.add(s.replace(/\s+/g, ""));
    return Array.from(final);
  }

  for (let i = 0; i < n - 1; i++) {
    const merged = toks
      .slice(0, i)
      .concat([toks[i] + toks[i + 1]])
      .concat(toks.slice(i + 2));
    out.add(merged.join(" ").replace(/\s+/g, ""));
  }
  out.add(toks.join(""));
  return Array.from(out);
}

function buildKeysFromTokens(tokens, prefixLen) {
  const keys = tokens.map((t) => prefixN(t, prefixLen)).filter(Boolean);
  const joins = joinVariantsTokens(tokens);
  for (const j of joins) {
    const k = prefixN(j, prefixLen);
    if (k) keys.push(k);
  }
  return Array.from(new Set(keys));
}

// ---- Turso index query helpers ----
// Expected schema (we’ll generate later in schema.sql):
// idx_voter(ac INTEGER, key TEXT, row_id INTEGER)
// idx_relative(ac INTEGER, key TEXT, row_id INTEGER)
// idx_exact_voter(ac INTEGER, key TEXT, row_id INTEGER)
// idx_exact_relative(ac INTEGER, key TEXT, row_id INTEGER)
// idx_loose_voter(ac INTEGER, key TEXT, row_id INTEGER)
// idx_loose_relative(ac INTEGER, key TEXT, row_id INTEGER)
//
// Each table should have INDEX(ac, key) and INDEX(ac, row_id).

async function queryIndexCandidates(db, tableName, ac, keys) {
  if (!keys.length) return new Map();

  // IN list via bound params
  const params = { ac: Number(ac) };
  const ph = [];
  for (let i = 0; i < keys.length; i++) {
    const p = `k${i}`;
    params[p] = keys[i];
    ph.push(`@${p}`);
  }

  const sql = `
    WITH hits AS (
      SELECT key, row_id
      FROM ${tableName}
      WHERE ac = @ac AND key IN (${ph.join(",")})
    )
    SELECT
      row_id AS row_id,
      COUNT(DISTINCT key) AS hit_count,
      CASE WHEN COUNT(DISTINCT key) = ${keys.length} THEN 1 ELSE 0 END AS and_hit
    FROM hits
    GROUP BY row_id;
  `;

  const rs = await db.execute({ sql, args: params });
  const m = new Map();
  for (const r of rs.rows) {
    const rid = Number(r.row_id);
    m.set(rid, {
      hit_count: Number(r.hit_count),
      and_hit: Number(r.and_hit) === 1,
    });
  }
  return m;
}

function emptyMeta() {
  return {
    voter_hit_count: 0,
    voter_and_hit: false,
    relative_hit_count: 0,
    relative_and_hit: false,
    voter_exact_hit_count: 0,
    voter_exact_and_hit: false,
    relative_exact_hit_count: 0,
    relative_exact_and_hit: false,
    voter_loose_hit_count: 0,
    voter_loose_and_hit: false,
    relative_loose_hit_count: 0,
    relative_loose_and_hit: false,
  };
}

// Fetch only the columns your worker ranking needs (same as fetchRowsByIds)
async function fetchRowsByIds(db, ac, rowIds) {
  if (!rowIds.length) return [];
  const params = { ac: Number(ac) };
  const ph = [];
  for (let i = 0; i < rowIds.length; i++) {
    const p = `r${i}`;
    params[p] = Number(rowIds[i]);
    ph.push(`@${p}`);
  }

  const sql = `
    SELECT
      row_id,
      voter_name_raw,
      relative_name_raw,
      voter_name_norm,
      relative_name_norm,
      serial_no
    FROM voters
    WHERE ac = @ac AND row_id IN (${ph.join(",")});
  `;

  const rs = await db.execute({ sql, args: params });
  return rs.rows.map((r) => ({
    row_id: Number(r.row_id),
    voter_name_raw: r.voter_name_raw ?? "",
    relative_name_raw: r.relative_name_raw ?? "",
    voter_name_norm: r.voter_name_norm ?? "",
    relative_name_norm: r.relative_name_norm ?? "",
    serial_no: r.serial_no ?? "",
  }));
}

async function getCandidatesForQuery(db, ac, q, scope, exactOn) {
  const strictTokens = tokenize(q);
  const strictKeys = buildKeysFromTokens(strictTokens, PREFIX_LEN_STRICT);

  const exactTokens = tokenizeExactIndex(q);
  const exactKeys = buildKeysFromTokens(exactTokens, PREFIX_LEN_EXACT);

  const looseTokens = tokenizeLoose(q);
  const looseKeys = buildKeysFromTokens(looseTokens, PREFIX_LEN_LOOSE);

  if (!strictKeys.length && !exactKeys.length && !looseKeys.length) {
    return { candidates: [], metaByRow: new Map(), strictKeys, exactKeys, looseKeys };
  }

  const wantLoose = !exactOn;

  let strictVoterMap = new Map(), strictRelMap = new Map();
  let exactVoterMap = new Map(), exactRelMap = new Map();
  let looseVoterMap = new Map(), looseRelMap = new Map();

  const jobs = [];

  if (scope === "voter") {
    if (strictKeys.length) jobs.push(queryIndexCandidates(db, "idx_voter", ac, strictKeys).then(m => (strictVoterMap = m)));
    if (exactKeys.length) jobs.push(queryIndexCandidates(db, "idx_exact_voter", ac, exactKeys).then(m => (exactVoterMap = m)));
    if (wantLoose && looseKeys.length) jobs.push(queryIndexCandidates(db, "idx_loose_voter", ac, looseKeys).then(m => (looseVoterMap = m)));
  } else if (scope === "relative") {
    if (strictKeys.length) jobs.push(queryIndexCandidates(db, "idx_relative", ac, strictKeys).then(m => (strictRelMap = m)));
    if (exactKeys.length) jobs.push(queryIndexCandidates(db, "idx_exact_relative", ac, exactKeys).then(m => (exactRelMap = m)));
    if (wantLoose && looseKeys.length) jobs.push(queryIndexCandidates(db, "idx_loose_relative", ac, looseKeys).then(m => (looseRelMap = m)));
  } else {
    // anywhere
    if (strictKeys.length) {
      jobs.push(queryIndexCandidates(db, "idx_voter", ac, strictKeys).then(m => (strictVoterMap = m)));
      jobs.push(queryIndexCandidates(db, "idx_relative", ac, strictKeys).then(m => (strictRelMap = m)));
    }
    if (exactKeys.length) {
      jobs.push(queryIndexCandidates(db, "idx_exact_voter", ac, exactKeys).then(m => (exactVoterMap = m)));
      jobs.push(queryIndexCandidates(db, "idx_exact_relative", ac, exactKeys).then(m => (exactRelMap = m)));
    }
    if (wantLoose && looseKeys.length) {
      jobs.push(queryIndexCandidates(db, "idx_loose_voter", ac, looseKeys).then(m => (looseVoterMap = m)));
      jobs.push(queryIndexCandidates(db, "idx_loose_relative", ac, looseKeys).then(m => (looseRelMap = m)));
    }
  }

  await Promise.all(jobs);

  // Build metaByRow exactly like app.js upsert logic
  const metaByRow = new Map();

  function upsert(row_id, patch) {
    const cur = metaByRow.get(row_id) || emptyMeta();
    metaByRow.set(row_id, { ...cur, ...patch });
  }

  for (const [rid, m] of strictVoterMap.entries()) upsert(rid, { voter_hit_count: m.hit_count, voter_and_hit: m.and_hit });
  for (const [rid, m] of strictRelMap.entries()) upsert(rid, { relative_hit_count: m.hit_count, relative_and_hit: m.and_hit });

  for (const [rid, m] of exactVoterMap.entries()) upsert(rid, { voter_exact_hit_count: m.hit_count, voter_exact_and_hit: m.and_hit });
  for (const [rid, m] of exactRelMap.entries()) upsert(rid, { relative_exact_hit_count: m.hit_count, relative_exact_and_hit: m.and_hit });

  for (const [rid, m] of looseVoterMap.entries()) upsert(rid, { voter_loose_hit_count: m.hit_count, voter_loose_and_hit: m.and_hit });
  for (const [rid, m] of looseRelMap.entries()) upsert(rid, { relative_loose_hit_count: m.hit_count, relative_loose_and_hit: m.and_hit });

  const candidates = Array.from(metaByRow.keys());
  return { candidates, metaByRow, strictKeys, exactKeys, looseKeys };
}

// ---- Netlify handler ----
export async function handler(event) {
  try {
    const qs = event.queryStringParameters || {};

    const ac = Number(qs.ac);
    const scope = String(qs.scope || "voter");
    const q = norm(qs.q || "");
    const exactOn = String(qs.exactOn || "0") === "1";

    if (!Number.isFinite(ac) || ac <= 0) {
      return json(400, { error: "Missing/invalid ac" });
    }
    if (!q) {
      return json(400, { error: "Missing q" });
    }
    if (!["voter", "relative", "anywhere"].includes(scope)) {
      return json(400, { error: "Invalid scope" });
    }

    const db = getDb();

    const { candidates, metaByRow, strictKeys, exactKeys, looseKeys } =
      await getCandidatesForQuery(db, ac, q, scope, exactOn);

    if (!candidates.length) {
      return json(200, {
        ac,
        strictKeys,
        exactKeys,
        looseKeys,
        rows: [],
      });
    }

    // Fetch the exact columns your worker consumes
    const rows = await fetchRowsByIds(db, ac, candidates);

    // Attach _meta
    const rowsWithMeta = rows.map((r) => ({
      ...r,
      _meta: metaByRow.get(r.row_id) || emptyMeta(),
    }));

    return json(200, {
      ac,
      strictKeys,
      exactKeys,
      looseKeys,
      rows: rowsWithMeta,
    });
  } catch (e) {
    return json(500, { error: String(e?.message || e) });
  }
}

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