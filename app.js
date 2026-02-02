// app.js (ES module)
import * as duckdb from "./duckdb/duckdb-browser.mjs";

const STATE_CODE_DEFAULT = "S27";
const PREFIX_LEN = 3;

const PAGE_SIZE_DEFAULT = 50;
const FETCH_ID_CHUNK = 4000;
const SCORE_BATCH = 2000;

const DISPLAY_COLS = [
  "Voter Name",
  "Relative Name",
  "Relation",
  "Gender",
  "Age",
  "House No",
  "Serial No",
  "Page No",
  "Part No",
  "ID",
];

const STICKY_COL = "Voter Name";

// NEW: search scope state
const SCOPE = {
  VOTER: "voter",
  RELATIVE: "relative",
  ANYWHERE: "anywhere",
};
let searchScope = SCOPE.VOTER;

let db, conn;
let current = {
  state: STATE_CODE_DEFAULT,
  ac: null,
  meta: null,
  loaded: false,
  lastQuery: "",
};

let rankedByRelevance = []; // [{row_id, score}]
let rankedView = [];        // sorted view as per dropdown

let page = 1;
let pageSize = PAGE_SIZE_DEFAULT;

let ageMap = null;            // Map(row_id -> numeric age or null)
let displayCache = new Map(); // row_id -> display row for current ordering

const $ = (id) => document.getElementById(id);

function setStatus(msg) { $("status").textContent = msg; }
function setBar(pct) { $("bar").style.width = `${Math.max(0, Math.min(100, pct))}%`; }
function setMeta(msg) { $("meta").textContent = msg; }

async function populateACDropdown(stateCode) {
  const acSel = $("acSel");
  acSel.innerHTML = `<option value="">Loading…</option>`;

  const manifestUrl = new URL(`/data/${stateCode}/ac_manifest.json`, window.location.origin).toString();
  const resp = await fetch(manifestUrl);
  if (!resp.ok) throw new Error(`Missing manifest: ${manifestUrl} (HTTP ${resp.status})`);

  const manifest = await resp.json();
  const acs = manifest.acs || [];

  acSel.innerHTML = acs.map(a => {
    const label = `AC ${String(a.ac_no).padStart(2,"0")} • ${a.row_count.toLocaleString()} rows`;
    return `<option value="${a.ac_no}">${label}</option>`;
  }).join("");

  if (acs.length > 0 && !acSel.value) acSel.value = String(acs[0].ac_no);
}

function norm(s) {
  if (s == null) return "";
  s = String(s).replace(/\u00A0/g, " ").trim();
  s = s.replace(/[.,;:|/\\()[\]{}<>"'`~!@#$%^&*_+=?-]/g, " ");
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

function escapeHtml(s) {
  return String(s)
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#039;");
}

function qIdent(colName) {
  const safe = String(colName).replace(/"/g, '""');
  return `"${safe}"`;
}

function formatCell(v) {
  if (v === null || v === undefined) return "";
  if (typeof v === "bigint") return v.toString();
  return String(v);
}

// ---------- DuckDB init ----------
async function initDuckDB() {
  if (db) return;

  const bundles = {
    mvp: {
      mainModule: "/duckdb/duckdb-mvp.wasm",
      mainWorker: "/duckdb/duckdb-browser-mvp.worker.js",
      pthreadWorker: null,
    },
    eh: {
      mainModule: "/duckdb/duckdb-eh.wasm",
      mainWorker: "/duckdb/duckdb-browser-eh.worker.js",
      pthreadWorker: null,
    },
  };

  const features = await duckdb.getPlatformFeatures();
  const bundle = await duckdb.selectBundle(bundles, features);

  console.log("DuckDB selected bundle:", bundle);

  const worker = new Worker(bundle.mainWorker);
  const logger = new duckdb.ConsoleLogger();

  db = new duckdb.AsyncDuckDB(logger, worker);
  await db.instantiate(bundle.mainModule, bundle.pthreadWorker);

  conn = await db.connect();
}

// ---------- Load AC ----------
async function loadAC(stateCode, acNo) {
  await initDuckDB();

  current.state = stateCode;
  current.ac = acNo;

  const acSlug = `ac=${String(acNo).padStart(2, "0")}`;
  const baseRel = `/data/${stateCode}/${acSlug}/`;
  const baseAbs = new URL(baseRel, window.location.origin).toString();

  const metaUrl   = new URL("meta.json", baseAbs).toString();
  const votersUrl = new URL("voters.parquet", baseAbs).toString();

  setStatus(`Loading AC${acNo}…`);
  setBar(5);

  const metaResp = await fetch(metaUrl);
  if (!metaResp.ok) throw new Error(`meta.json not found: ${metaUrl} (HTTP ${metaResp.status})`);
  const meta = await metaResp.json();
  current.meta = meta;

  // NEW: read index filenames from meta.json if present; otherwise fall back to convention
  const idxVFile = meta?.index_files?.voter || `index_prefix_${PREFIX_LEN}_voter.parquet`;
  const idxRFile = meta?.index_files?.relative || `index_prefix_${PREFIX_LEN}_relative.parquet`;

  const indexVUrl = new URL(idxVFile, baseAbs).toString();
  const indexRUrl = new URL(idxRFile, baseAbs).toString();

  await conn.query(`DROP VIEW IF EXISTS voters;`);
  await conn.query(`DROP VIEW IF EXISTS idx_voter;`);
  await conn.query(`DROP VIEW IF EXISTS idx_relative;`);

  setBar(15);

  await conn.query(`
    CREATE VIEW voters AS
    SELECT * FROM read_parquet('${votersUrl}');
  `);

  setBar(40);

  // NEW: two index views
  await conn.query(`
    CREATE VIEW idx_voter AS
    SELECT * FROM read_parquet('${indexVUrl}');
  `);

  await conn.query(`
    CREATE VIEW idx_relative AS
    SELECT * FROM read_parquet('${indexRUrl}');
  `);

  setBar(70);

  const rc = await conn.query(`SELECT COUNT(*)::INT AS c FROM voters;`);
  const iv = await conn.query(`SELECT COUNT(*)::INT AS c FROM idx_voter;`);
  const ir = await conn.query(`SELECT COUNT(*)::INT AS c FROM idx_relative;`);

  setBar(100);
  current.loaded = true;

  // reset search state
  rankedByRelevance = [];
  rankedView = [];
  ageMap = null;
  displayCache.clear();
  page = 1;

  setMeta(
    `Loaded AC${acNo} • voters: ${rc.toArray()[0].c} • idx_voter keys: ${iv.toArray()[0].c} • idx_relative keys: ${ir.toArray()[0].c} • prefix_len: ${meta.prefix_len}`
  );
  setStatus(`Ready. Enter query and Search.`);
  $("results").innerHTML = "";
  $("pager").style.display = "none";
}

// ---------- Candidate generation (OR-always, plus metadata for ranking) ----------
async function queryIndexCandidates(viewName, keys) {
  if (!keys.length) return new Map();
  const keyListSql = keys.map(k => `'${k.replace(/'/g, "''")}'`).join(",");

  // OR-always: return all rows matching any key + how many keys matched (hit_count)
  // and_hit indicates row matched all keys (useful for boosts, not filtering).
  const sql = `
    WITH hits AS (
      SELECT key, UNNEST(row_ids) AS row_id
      FROM ${viewName}
      WHERE key IN (${keyListSql})
    )
    SELECT
      row_id::INT AS row_id,
      COUNT(DISTINCT key)::INT AS hit_count,
      (COUNT(DISTINCT key) = ${keys.length}) AS and_hit
    FROM hits
    GROUP BY row_id
  `;

  const rs = await conn.query(sql);
  const m = new Map();
  for (const r of rs.toArray()) {
    m.set(Number(r.row_id), {
      hit_count: Number(r.hit_count),
      and_hit: Boolean(r.and_hit),
    });
  }
  return m;
}

async function getCandidatesForQuery(q, scope) {
  const toks = tokenize(q);
  if (toks.length === 0) return { keys: [], candidates: [], metaByRow: new Map() };

  const keys = toks.map(t => prefixN(t, PREFIX_LEN)).filter(Boolean);
  if (keys.length === 0) return { keys: [], candidates: [], metaByRow: new Map() };

  // Gather from selected index(es)
  let voterMap = new Map();
  let relMap = new Map();

  if (scope === SCOPE.VOTER) {
    voterMap = await queryIndexCandidates("idx_voter", keys);
  } else if (scope === SCOPE.RELATIVE) {
    relMap = await queryIndexCandidates("idx_relative", keys);
  } else {
    // ANYWHERE: union of both fields (not "must match both")
    [voterMap, relMap] = await Promise.all([
      queryIndexCandidates("idx_voter", keys),
      queryIndexCandidates("idx_relative", keys),
    ]);
  }

  // Merge into a single metaByRow with field-specific metadata
  const metaByRow = new Map();

  function upsert(row_id, patch) {
    const cur = metaByRow.get(row_id) || {
      voter_hit_count: 0,
      voter_and_hit: false,
      relative_hit_count: 0,
      relative_and_hit: false,
    };
    metaByRow.set(row_id, { ...cur, ...patch });
  }

  for (const [rid, m] of voterMap.entries()) {
    upsert(rid, { voter_hit_count: m.hit_count, voter_and_hit: m.and_hit });
  }
  for (const [rid, m] of relMap.entries()) {
    upsert(rid, { relative_hit_count: m.hit_count, relative_and_hit: m.and_hit });
  }

  const candidates = Array.from(metaByRow.keys());

  return { keys, candidates, metaByRow, tokenCount: keys.length };
}

// ---------- Fetch scoring rows ----------
async function fetchRowsByIds(rowIds) {
  const cols = [
    "row_id",
    "voter_name_raw",
    "relative_name_raw",
    "voter_name_norm",
    "relative_name_norm",
  ];

  await conn.query(`DROP TABLE IF EXISTS cand_ids;`);
  await conn.query(`CREATE TEMP TABLE cand_ids(row_id INTEGER);`);

  for (let i = 0; i < rowIds.length; i += FETCH_ID_CHUNK) {
    const chunk = rowIds.slice(i, i + FETCH_ID_CHUNK);
    const valuesSql = chunk.map(id => `(${Number(id)})`).join(",");
    await conn.query(`INSERT INTO cand_ids VALUES ${valuesSql};`);
  }

  const sql = `
    SELECT ${cols.map(qIdent).join(", ")}
    FROM voters v
    JOIN cand_ids c USING(row_id)
  `;

  const rs = await conn.query(sql);
  return rs.toArray().map(r => ({
    row_id: Number(r.row_id),
    voter_name_raw: r.voter_name_raw ?? "",
    relative_name_raw: r.relative_name_raw ?? "",
    voter_name_norm: r.voter_name_norm ?? "",
    relative_name_norm: r.relative_name_norm ?? ""
  }));
}

// ---------- Worker ----------
let worker;
function initWorker() {
  if (worker) return;

  worker = new Worker("./worker.js", { type: "module" });

  worker.onmessage = async (ev) => {
    const msg = ev.data;

    if (msg.type === "progress") {
      const { done, total, phase, candidates } = msg;
      const pct = total > 0 ? (done / total) * 100 : 0;
      setBar(pct);
      setStatus(`${phase} • candidates: ${candidates} • scored: ${done}/${total}`);
      return;
    }

    if (msg.type === "done") {
      rankedByRelevance = msg.ranked || [];
      rankedView = rankedByRelevance.slice(); // default relevance
      ageMap = null;
      displayCache.clear();
      page = 1;
      setBar(100);
      await applySortAndRender();
      return;
    }

    if (msg.type === "error") {
      setBar(0);
      setStatus(`Worker error: ${msg.message}`);
      return;
    }
  };
}

// ---------- PDF link ----------
function buildPdfUrl(row) {
  const state = formatCell(row["State Code"]);
  const ac = formatCell(row["AC No"]);
  const part = formatCell(row["Part No"]);
  if (!state || !ac || !part) return "";
  return `https://www.eci.gov.in/sir/f3/${state}/data/OLDSIRROLL/${state}/${ac}/${state}_${ac}_${part}.pdf`;
}

// ---------- Display fetch (BigInt safe) ----------
async function fetchDisplayRowsByIds(rowIds) {
  await conn.query(`DROP TABLE IF EXISTS page_ids;`);
  await conn.query(`CREATE TEMP TABLE page_ids(row_id INTEGER);`);

  const valuesSql = rowIds.map(id => `(${Number(id)})`).join(",");
  await conn.query(`INSERT INTO page_ids VALUES ${valuesSql};`);

  const displayExprs = DISPLAY_COLS.map(col => {
    if (col === "Part No") return `CAST(v.${qIdent(col)} AS VARCHAR) AS ${qIdent(col)}`;
    return `v.${qIdent(col)} AS ${qIdent(col)}`;
  });

  const selectSql = `
    SELECT
      CAST(v.${qIdent("row_id")} AS VARCHAR) AS ${qIdent("row_id")},
      v.${qIdent("State Code")} AS ${qIdent("State Code")},
      CAST(v.${qIdent("AC No")} AS VARCHAR) AS ${qIdent("AC No")},
      ${displayExprs.join(",\n      ")}
    FROM voters v
    JOIN page_ids p ON v.row_id = p.row_id
  `;

  const rs = await conn.query(selectSql);

  const expectedKeys = ["row_id", "State Code", "AC No", ...DISPLAY_COLS];

  return rs.toArray().map(r => {
    const out = {};
    for (const k of expectedKeys) out[k] = r[k];
    out.row_id = Number(out.row_id);
    return out;
  });
}

// ---------- Sorting (unchanged) ----------
function parseAgeValue(v) {
  if (v === null || v === undefined) return null;
  const s = String(v).trim();
  const m = s.match(/(\d+)/);
  if (!m) return null;
  const n = Number(m[1]);
  return Number.isFinite(n) ? n : null;
}

async function ensureAgeMapLoaded() {
  if (ageMap) return;
  ageMap = new Map();

  if (!rankedByRelevance.length) return;

  setStatus("Preparing Age sort…");
  setBar(5);

  await conn.query(`DROP TABLE IF EXISTS sort_ids;`);
  await conn.query(`CREATE TEMP TABLE sort_ids(row_id INTEGER);`);

  const allIds = rankedByRelevance.map(x => x.row_id);

  for (let i = 0; i < allIds.length; i += FETCH_ID_CHUNK) {
    const chunk = allIds.slice(i, i + FETCH_ID_CHUNK);
    const valuesSql = chunk.map(id => `(${Number(id)})`).join(",");
    await conn.query(`INSERT INTO sort_ids VALUES ${valuesSql};`);

    const pct = (i / allIds.length) * 100;
    setBar(Math.min(90, Math.max(5, pct)));
  }

  const sql = `
    SELECT
      CAST(v.${qIdent("row_id")} AS VARCHAR) AS row_id,
      v.${qIdent("Age")} AS age
    FROM voters v
    JOIN sort_ids s ON v.row_id = s.row_id
  `;

  const rs = await conn.query(sql);
  for (const r of rs.toArray()) {
    const rid = Number(r.row_id);
    ageMap.set(rid, parseAgeValue(r.age));
  }

  setBar(100);
}

async function applySortAndRender() {
  const sortMode = $("sortSel").value || "relevance";

  if (sortMode === "relevance") {
    rankedView = rankedByRelevance.slice();
  } else {
    await ensureAgeMapLoaded();
    const dir = (sortMode === "age_desc") ? -1 : 1;

    rankedView = rankedByRelevance.slice().sort((a, b) => {
      const aa = ageMap.get(a.row_id) ?? null;
      const bb = ageMap.get(b.row_id) ?? null;

      const aMissing = (aa === null);
      const bMissing = (bb === null);

      if (aMissing && bMissing) {
        if (b.score !== a.score) return b.score - a.score;
        return a.row_id - b.row_id;
      }
      if (aMissing) return 1;
      if (bMissing) return -1;

      if (aa !== bb) return (aa - bb) * dir;

      if (b.score !== a.score) return b.score - a.score;
      return a.row_id - b.row_id;
    });
  }

  page = 1;
  displayCache.clear();
  await renderPage();
}

// ---------- Search ----------
async function runSearch() {
  if (!current.loaded) {
    setStatus("Load an AC first.");
    return;
  }

  const q = $("q").value || "";
  const qn = norm(q);
  current.lastQuery = q;

  rankedByRelevance = [];
  rankedView = [];
  ageMap = null;
  displayCache.clear();
  page = 1;

  $("pager").style.display = "none";
  $("results").innerHTML = "";
  setBar(0);

  if (!qn) {
    setStatus("Enter a query.");
    return;
  }

  const exactOn = Boolean($("exactToggle")?.checked);

  setStatus("Stage 1: candidate generation…");
  const { keys, candidates, metaByRow, tokenCount } = await getCandidatesForQuery(qn, searchScope);

  if (!candidates.length) {
    setStatus(`No matches in index (keys: ${keys.join(", ") || "—"}).`);
    return;
  }

  // helpful status, no behavior change
  const andHits = Array.from(metaByRow.values()).filter(m => {
    if (searchScope === SCOPE.VOTER) return m.voter_and_hit;
    if (searchScope === SCOPE.RELATIVE) return m.relative_and_hit;
    // ANYWHERE: and-hit in either field
    return m.voter_and_hit || m.relative_and_hit;
  }).length;

  setStatus(`Stage 2: fetching ${candidates.length} candidate rows… (AND-hits: ${andHits}/${candidates.length})`);
  setBar(2);

  const rows = await fetchRowsByIds(candidates);

  // Attach index metadata for ranking boosts (does not filter)
  const rowsWithMeta = rows.map(r => ({
    ...r,
    _meta: metaByRow.get(r.row_id) || {
      voter_hit_count: 0, voter_and_hit: false,
      relative_hit_count: 0, relative_and_hit: false
    }
  }));

  setStatus(`Stage 3: scoring ${rowsWithMeta.length} rows…`);
  setBar(1);

  initWorker();

  worker.postMessage({
    type: "start",
    query: qn,
    scope: searchScope,
    exactOn,
    tokenCount: Number(tokenCount || tokenize(qn).length || 0),
    total: rowsWithMeta.length
  });

  for (let i = 0; i < rowsWithMeta.length; i += SCORE_BATCH) {
    const batch = rowsWithMeta.slice(i, i + SCORE_BATCH);
    worker.postMessage({ type: "batch", rows: batch });
  }

  worker.postMessage({ type: "finish" });
}

// ---------- Render table (unchanged) ----------
async function renderPage() {
  pageSize = Number($("pageSizeSel").value) || PAGE_SIZE_DEFAULT;

  const total = rankedView.length;
  const totalPages = Math.max(1, Math.ceil(total / pageSize));
  page = Math.max(1, Math.min(page, totalPages));

  const start = (page - 1) * pageSize;
  const end = Math.min(total, start + pageSize);

  const slice = rankedView.slice(start, end);
  const rowIds = slice.map(x => x.row_id);

  const missing = rowIds.filter(id => !displayCache.has(id));
  if (missing.length) {
    setStatus(`Loading page ${page} rows…`);
    const rows = await fetchDisplayRowsByIds(missing);
    for (const r of rows) displayCache.set(r.row_id, r);
  }

  const scoreMap = new Map(slice.map(x => [x.row_id, x.score]));
  const orderedRows = rowIds.map(id => displayCache.get(id)).filter(Boolean);

  $("results").innerHTML = renderTable(orderedRows, scoreMap);

  $("pager").style.display = total > 0 ? "flex" : "none";
  $("pageInfo").textContent = `Showing ${start + 1}-${end} of ${total} • Page ${page}/${totalPages}`;
  setStatus(`Showing ${start + 1}-${end} of ${total}`);
}

function renderTable(rows, scoreMap) {
  const finalHeaders = [
    "Voter Name",
    "Relative Name",
    "Relation",
    "Gender",
    "Age",
    "House No",
    "Serial No",
    "Page No",
    "Part No",
    "ID",
    "PDF"
  ];

  const thead = `
    <thead>
      <tr>
        ${finalHeaders.map(h => {
          const sticky = (h === STICKY_COL) ? "stickyCol" : "";
          return `<th class="${sticky}">${escapeHtml(h)}</th>`;
        }).join("")}
      </tr>
    </thead>
  `;

  const tbody = `
    <tbody>
      ${rows.map(r => {
        const pdfUrl = buildPdfUrl(r);
        const score = scoreMap.get(r.row_id);
        return `
          <tr>
            ${finalHeaders.map(h => {
              const sticky = (h === STICKY_COL) ? "stickyCol" : "";

              if (h === "PDF") {
                return `<td>${pdfUrl ? `<a href="${escapeHtml(pdfUrl)}" target="_blank" rel="noopener noreferrer">Open PDF</a>` : ""}</td>`;
              }

              const val = formatCell(r[h]);
              const title = (h === "Voter Name" && typeof score === "number")
                ? `title="score: ${score.toFixed(3)}"`
                : "";

              return `<td class="${sticky}" ${title}>${escapeHtml(val)}</td>`;
            }).join("")}
          </tr>
        `;
      }).join("")}
    </tbody>
  `;

  return `
    <div class="tableWrap">
      <table>
        ${thead}
        ${tbody}
      </table>
    </div>
  `;
}

// ---------- NEW: Chip helpers ----------
function setActiveChip(scope) {
  searchScope = scope;

  $("chipVoter").classList.toggle("active", scope === SCOPE.VOTER);
  $("chipRelative").classList.toggle("active", scope === SCOPE.RELATIVE);
  $("chipAnywhere").classList.toggle("active", scope === SCOPE.ANYWHERE);

  // If a query is present and results exist, rerun search to reflect scope change
  // (safe and deterministic; no background caching changes).
}

function getScopeLabel() {
  if (searchScope === SCOPE.VOTER) return "Voter Name";
  if (searchScope === SCOPE.RELATIVE) return "Relative Name";
  return "Anywhere";
}

// ---------- Wire UI ----------
$("loadBtn").onclick = async () => {
  const state = $("stateSel").value;
  const ac = Number($("acSel").value);
  try {
    await loadAC(state, ac);
  } catch (e) {
    console.error(e);
    setStatus(`Load failed: ${e?.message || e}`);
    setBar(0);
  }
};

$("searchBtn").onclick = () => runSearch();
$("q").addEventListener("keydown", (e) => {
  if (e.key === "Enter") runSearch();
});

$("clearBtn").onclick = () => {
  $("q").value = "";
  rankedByRelevance = [];
  rankedView = [];
  ageMap = null;
  displayCache.clear();
  $("results").innerHTML = "";
  $("pager").style.display = "none";
  setBar(0);
  setStatus("Cleared.");
};

$("prevBtn").onclick = async () => { page--; await renderPage(); };
$("nextBtn").onclick = async () => { page++; await renderPage(); };
$("pageSizeSel").onchange = async () => { page = 1; await renderPage(); };

$("sortSel").onchange = async () => {
  if (!rankedByRelevance.length) return;
  try {
    await applySortAndRender();
  } catch (e) {
    console.error(e);
    setStatus(`Sort failed: ${e?.message || e}`);
  }
};

// NEW: chip wiring
$("chipVoter").onclick = () => setActiveChip(SCOPE.VOTER);
$("chipRelative").onclick = () => setActiveChip(SCOPE.RELATIVE);
$("chipAnywhere").onclick = () => setActiveChip(SCOPE.ANYWHERE);

// Optional: if user changes exact toggle and already has query typed,
// they can just hit Search again. No auto-run to keep behavior predictable.
$("exactToggle").onchange = () => {
  setStatus(`Exact is now ${$("exactToggle").checked ? "ON" : "OFF"} • scope: ${getScopeLabel()} • press Search`);
};

// Boot
setMeta("Not loaded.");
setStatus("Loading AC list…");

populateACDropdown($("stateSel").value)
  .then(() => setStatus("Select AC and click Load AC."))
  .catch((e) => {
    console.error(e);
    setStatus(`Failed to load AC list: ${e?.message || e}`);
  });
