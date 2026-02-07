// netlify/functions/rows.js
// Fetches voter rows by row_id list for a given district + AC.
// mode="score" returns only the fields needed for worker ranking + filters.
// mode="display" returns all DISPLAY_COLS used in the UI table.

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

async function getVotersMeta(client) {
  const tables = await listTables(client);
  const votersTable = pickExistingTable(tables, ["voters", "Voters"]);
  if (!votersTable) throw new Error("Table 'voters' not found in DB");

  const rs = await client.execute(`PRAGMA table_info(${qIdent(votersTable)})`);
  const cols = new Set((rs.rows || []).map((r) => String(r.name)));

  const pickCol = (cands) => {
    for (const c of cands) if (cols.has(c)) return c;
    return "";
  };

  // Core keys
  const rowIdCol = pickCol(["row_id", "rowid"]);
  const acCol = pickCol(["AC No", "ac_no", "ac", "acno"]);
  if (!rowIdCol) throw new Error("voters.row_id column not found");
  if (!acCol) throw new Error("voters AC column not found (expected 'AC No' or 'ac_no' etc.)");

  // Names used by worker
  const voterRawCol = pickCol(["voter_name_raw", "voter_raw", "voter_raw_name"]);
  const relRawCol = pickCol(["relative_name_raw", "rel_name_raw", "relative_raw_name"]);
  const voterNormCol = pickCol(["voter_name_norm", "voter_norm"]);
  const relNormCol = pickCol(["relative_name_norm", "relative_norm"]);

  // Filters / sort
  const genderCol = pickCol(["Gender", "gender"]);
  const ageCol = pickCol(["Age", "age"]);

  // UI (display)
  const stateCol = pickCol(["State Code", "state_code"]);
  const partCol = pickCol(["Part No", "part_no"]);
  const pageCol = pickCol(["Page No", "page_no"]);
  const serialCol = pickCol(["Serial No", "serial_no"]);
  const voterNameCol = pickCol(["Voter Name", "voter_name"]);
  const relNameCol = pickCol(["Relative Name", "relative_name"]);
  const relationCol = pickCol(["Relation", "relation"]);
  const houseCol = pickCol(["House No", "house_no"]);
  const idCol = pickCol(["ID", "id"]);

  return {
    votersTable,
    cols,
    rowIdCol,
    acCol,
    voterRawCol,
    relRawCol,
    voterNormCol,
    relNormCol,
    genderCol,
    ageCol,
    stateCol,
    partCol,
    pageCol,
    serialCol,
    voterNameCol,
    relNameCol,
    relationCol,
    houseCol,
    idCol,
  };
}

let CLIENT_CACHE = new Map(); // key -> { client, votersMeta }

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

  const votersMeta = await getVotersMeta(client);

  const entry = { client, votersMeta };
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

    const district = districtToDbSlug(body.district);
    const ac = Number(body.ac);
    const mode = String(body.mode || "score");
    let rowIds = Array.isArray(body.rowIds) ? body.rowIds.map((x) => Number(x)).filter(Number.isFinite) : [];

    if (!district) return text(400, "Missing district");
    if (!Number.isFinite(ac)) return text(400, "Invalid ac");
    if (!rowIds.length) return json(200, { rows: [] });

    // Hard limits (protect function)
    if (rowIds.length > 5000) rowIds = rowIds.slice(0, 5000);

    const { client, votersMeta } = await getClientForDistrict(district);

    const { votersTable, rowIdCol, acCol } = votersMeta;

    const placeholders = rowIds.map(() => "?").join(",");

    let sql = "";
    let args = [ac, ...rowIds];

    if (mode === "display") {
      const cols = [];

      cols.push(`${qIdent(rowIdCol)} AS row_id`);

      if (votersMeta.stateCol) cols.push(`${qIdent(votersMeta.stateCol)} AS ${qIdent("State Code")}`);
      else cols.push(`'' AS ${qIdent("State Code")}`);

      cols.push(`${qIdent(acCol)} AS ${qIdent("AC No")}`);

      const add = (colName, outName) => {
        if (colName) cols.push(`${qIdent(colName)} AS ${qIdent(outName)}`);
        else cols.push(`'' AS ${qIdent(outName)}`);
      };

      add(votersMeta.voterNameCol, "Voter Name");
      add(votersMeta.relNameCol, "Relative Name");
      add(votersMeta.relationCol, "Relation");
      add(votersMeta.genderCol, "Gender");
      add(votersMeta.ageCol, "Age");
      add(votersMeta.houseCol, "House No");
      add(votersMeta.serialCol, "Serial No");
      add(votersMeta.pageCol, "Page No");
      add(votersMeta.partCol, "Part No");
      add(votersMeta.idCol, "ID");

      sql = `SELECT ${cols.join(", ")} FROM ${qIdent(votersTable)} WHERE ${qIdent(acCol)} = ? AND ${qIdent(rowIdCol)} IN (${placeholders})`;
    } else {
      const cols = [];
      cols.push(`${qIdent(rowIdCol)} AS row_id`);

      if (votersMeta.voterRawCol) cols.push(`${qIdent(votersMeta.voterRawCol)} AS voter_name_raw`);
      else cols.push(`'' AS voter_name_raw`);

      if (votersMeta.relRawCol) cols.push(`${qIdent(votersMeta.relRawCol)} AS relative_name_raw`);
      else cols.push(`'' AS relative_name_raw`);

      if (votersMeta.voterNormCol) cols.push(`${qIdent(votersMeta.voterNormCol)} AS voter_name_norm`);
      else cols.push(`'' AS voter_name_norm`);

      if (votersMeta.relNormCol) cols.push(`${qIdent(votersMeta.relNormCol)} AS relative_name_norm`);
      else cols.push(`'' AS relative_name_norm`);

      if (votersMeta.genderCol) cols.push(`${qIdent(votersMeta.genderCol)} AS gender`);
      else cols.push(`'' AS gender`);

      if (votersMeta.ageCol) cols.push(`${qIdent(votersMeta.ageCol)} AS age`);
      else cols.push(`'' AS age`);

      if (votersMeta.serialCol) cols.push(`${qIdent(votersMeta.serialCol)} AS serial`);
      else cols.push(`'' AS serial`);

      sql = `SELECT ${cols.join(", ")} FROM ${qIdent(votersTable)} WHERE ${qIdent(acCol)} = ? AND ${qIdent(rowIdCol)} IN (${placeholders})`;
    }

    const rs = await client.execute({ sql, args });

    const rows = (rs.rows || []).map((r) => ({ ...r, row_id: Number(r.row_id) }));

    return json(200, { rows });
  } catch (e) {
    console.error("rows error:", e);
    return text(500, e && e.message ? e.message : "Server error");
  }
};
