// netlify/functions/rows.js
// Fetch rows by row_id for a given district + AC from Turso.
// mode="score" returns minimal fields used by worker ranking + filters.
// mode="display" returns columns used for the results table (keys match UI headers).

function json(statusCode, obj) {
  return {
    statusCode,
    headers: {
      "content-type": "application/json; charset=utf-8",
      "cache-control": "no-store",
      "access-control-allow-origin": "*",
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
      "access-control-allow-origin": "*",
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
  // SQLite identifier quoting (supports spaces)
  return `"${String(name).replace(/"/g, '""')}"`;
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

async function findVotersTable(client) {
  const tables = await listTables(client);

  // Most likely names (from build scripts / older experiments)
  const direct = pickExistingTable(tables, ["voters", "voters_min", "voters_slim", "voters_s27", "voter"]);
  if (direct) return direct;

  // Fallback: pick any table that looks like voter data
  for (const t of tables) {
    const tl = String(t).toLowerCase();
    if (tl.includes("voter")) return t;
  }

  // If nothing obvious, return empty (will error later with a clear message)
  return "";
}

async function getTableCols(client, tableName) {
  const rs = await client.execute(`PRAGMA table_info(${qIdent(tableName)})`);
  return new Set((rs.rows || []).map((r) => String(r.name)));
}

function pickCol(cols, candidates) {
  for (const c of candidates) if (cols.has(c)) return c;
  return "";
}

function chunkArray(arr, size) {
  const out = [];
  for (let i = 0; i < arr.length; i += size) out.push(arr.slice(i, i + size));
  return out;
}

let CLIENT_CACHE = new Map(); // key -> { client }

async function getClientForDistrict(districtSlug) {
  // Keep this mapping identical to candidates.js
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

  const org =
    group === "A" ? (process.env.TURSO_ORG_A || "sujay-k1")
    : group === "B" ? (process.env.TURSO_ORG_B || "sujay-k2")
    : (process.env.TURSO_ORG_C || "sujay-k3");

  const token =
    group === "A" ? process.env.TURSO_TOKEN_A
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

  const entry = { client };
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
    const mode = String(body.mode || "score");

    const rowIdsRaw = Array.isArray(body.rowIds) ? body.rowIds : [];
    const rowIds = rowIdsRaw.map((x) => Number(x)).filter(Number.isFinite);

    if (!district) return text(400, "Missing district");
    if (!Number.isFinite(ac)) return text(400, "Invalid ac");
    if (!rowIds.length) return json(200, { rows: [] });

    // Hard limit to protect function
    if (rowIds.length > 50000) return text(400, "rowIds too large");

    const { client } = await getClientForDistrict(district);

    const votersTable = await findVotersTable(client);
    if (!votersTable) throw new Error("Could not find voters table in DB");

    const cols = await getTableCols(client, votersTable);

    // Column candidates (handles both snake_case and spaced names)
    const acCol = pickCol(cols, ["ac", "AC No", "ac_no"]) || "ac";
    const rowIdCol = pickCol(cols, ["row_id", "Row ID", "rowid", "rowId"]) || "row_id";

    // score-mode fields (used by worker ranking + filters)
    const voterRawCol = pickCol(cols, ["voter_name_raw", "Voter Name", "voter_name"]);
    const relRawCol = pickCol(cols, ["relative_name_raw", "Relative Name", "relative_name"]);
    const voterNormCol = pickCol(cols, ["voter_name_norm", "voter_name_raw", "Voter Name", "voter_name"]);
    const relNormCol = pickCol(cols, ["relative_name_norm", "relative_name_raw", "Relative Name", "relative_name"]);
    const serialCol = pickCol(cols, ["serial_no", "Serial No", "serial"]);
    const ageCol = pickCol(cols, ["age", "Age"]);
    const genderCol = pickCol(cols, ["gender", "Gender"]);

    // display-mode columns (keys must match UI headers)
    const displayMap = {
      "State Code": pickCol(cols, ["State Code", "state_code", "state"]),
      "AC No": pickCol(cols, ["AC No", "ac_no", "ac"]),
      "Voter Name": pickCol(cols, ["Voter Name", "voter_name", "voter_name_raw"]),
      "Relative Name": pickCol(cols, ["Relative Name", "relative_name", "relative_name_raw"]),
      "Relation": pickCol(cols, ["Relation", "relation"]),
      "Gender": pickCol(cols, ["Gender", "gender"]),
      "Age": pickCol(cols, ["Age", "age"]),
      "House No": pickCol(cols, ["House No", "house_no", "house"]),
      "Serial No": pickCol(cols, ["Serial No", "serial_no", "serial"]),
      "Page No": pickCol(cols, ["Page No", "page_no", "page"]),
      "Part No": pickCol(cols, ["Part No", "part_no", "part"]),
      "ID": pickCol(cols, ["ID", "id"]),
    };

    // IMPORTANT: SQLite has a "max variables" limit (~999). Keep chunk comfortably lower.
    const CHUNK = 800;
    const chunks = chunkArray(rowIds, CHUNK);

    const outRows = [];

    for (const ids of chunks) {
      const placeholders = ids.map(() => "?").join(",");
      const whereSql = `WHERE ${qIdent(acCol)} = ? AND ${qIdent(rowIdCol)} IN (${placeholders})`;
      const args = [ac, ...ids];

      let selectSql = "";
      if (mode === "display") {
        const parts = [
          `${qIdent(rowIdCol)} AS row_id`,
        ];

        // Always include these keys, even if missing in DB (we'll fill later)
        for (const key of Object.keys(displayMap)) {
          const col = displayMap[key];
          if (col) parts.push(`${qIdent(col)} AS ${qIdent(key)}`);
          else parts.push(`'' AS ${qIdent(key)}`);
        }

        selectSql = `SELECT ${parts.join(", ")} FROM ${qIdent(votersTable)} ${whereSql}`;
      } else {
        // default: score
        const parts = [
          `${qIdent(rowIdCol)} AS row_id`,
          `${voterRawCol ? qIdent(voterRawCol) : "''"} AS voter_name_raw`,
          `${relRawCol ? qIdent(relRawCol) : "''"} AS relative_name_raw`,
          `${voterNormCol ? qIdent(voterNormCol) : "''"} AS voter_name_norm`,
          `${relNormCol ? qIdent(relNormCol) : "''"} AS relative_name_norm`,
          `${serialCol ? qIdent(serialCol) : "''"} AS serial_no`,
          `${ageCol ? qIdent(ageCol) : "''"} AS age`,
          `${genderCol ? qIdent(genderCol) : "''"} AS gender`,
        ];
        selectSql = `SELECT ${parts.join(", ")} FROM ${qIdent(votersTable)} ${whereSql}`;
      }

      const rs = await client.execute({ sql: selectSql, args });

      for (const r of rs.rows || []) {
        // libsql client returns plain objects
        outRows.push(r);
      }
    }

    return json(200, { rows: outRows });
  } catch (e) {
    console.error("rows error:", e);
    // Return JSON so frontend can show the real error if needed
    return json(500, { error: e?.message || "Server error" });
  }
};
