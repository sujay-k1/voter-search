// app.js (ES module)
// Language switch moved to i18n.js (strings + translation runtime).
// No search logic changed; only i18n block extracted.

import { LANG, createI18n } from "./i18n.js";

const STATE_CODE_DEFAULT = "S27";

// index behavior
const PREFIX_LEN_STRICT = 3;
const PREFIX_LEN_LOOSE = 2;
const PREFIX_LEN_EXACT = 2;

// page size behavior
const PAGE_SIZE_DESKTOP_DEFAULT = 100;
const PAGE_SIZE_MOBILE_DEFAULT = 25;

const PAGE_SIZE_DESKTOP_OPTIONS = [25, 50, 100, 250, 500];
const PAGE_SIZE_MOBILE_OPTIONS = [10, 25, 50, 100];

const FETCH_ID_CHUNK = 4000;
const SCORE_BATCH = 2000;

// IMPORTANT: Keep data keys in English to match parquet columns.
// UI labels for these keys are translated via i18n in renderTable().
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

// score / fuzzy match helper columns (precomputed in parquet / sqlite)
const NAME_COL = "voter_name_norm";
const REL_COL = "relative_name_norm";
const NAME_RAW_COL = "voter_name_raw";
const REL_RAW_COL = "relative_name_raw";

// ---------- helpers ----------
const $ = (sel) => document.querySelector(sel);
const el = (tag, cls) => {
  const e = document.createElement(tag);
  if (cls) e.className = cls;
  return e;
};

const norm = (s) => String(s || "").trim();
const clamp = (n, lo, hi) => Math.max(lo, Math.min(hi, n));
const safeInt = (v) => {
  const n = Number(v);
  return Number.isFinite(n) ? n : null;
};

const isMobile = () => window.matchMedia("(max-width: 680px)").matches;
const getDefaultPageSize = () => (isMobile() ? PAGE_SIZE_MOBILE_DEFAULT : PAGE_SIZE_DESKTOP_DEFAULT);
const getPageSizeOptions = () => (isMobile() ? PAGE_SIZE_MOBILE_OPTIONS : PAGE_SIZE_DESKTOP_OPTIONS);

const APP_BASE = new URL("./", window.location.href);
const relUrl = (p) => new URL(p, APP_BASE).toString();

// Netlify Functions base (absolute to origin; works even if site is under a subfolder)
const FN_BASE = window.location.origin;
const fnUrl = (name) => `${FN_BASE}/.netlify/functions/${name}`;

// District id -> DB slug (tolerant of different manifest styles)
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

async function apiPostJson(name, payload) {
  const url = fnUrl(name);
  const resp = await fetch(url, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify(payload || {}),
  });
  if (!resp.ok) {
    let msg = `HTTP ${resp.status}`;
    try {
      const t = await resp.text();
      if (t) msg = t;
    } catch {}
    throw new Error(`${name} failed: ${msg}`);
  }
  return await resp.json();
}

// ---------- state ----------
let t = (k, vars) => k;
let lang = LANG.HI;
let i18n = null;

let current = {
  state: STATE_CODE_DEFAULT,
  ac: null,
  loaded: false,
};

let page = 1;
let pageSize = getDefaultPageSize();

// ranked results (global), each item: { key, ac, row_id, score, ... } returned from worker
let rankedByRelevance = [];
let filteredBase = [];
let displayCache = new Map();
let scoreCache = new Map(); // Map(key -> { gender, age })
let ageMap = null;

let currentDistrictManifest = null;
let currentDistrictId = "";
let currentDistrictLabel = "";

// UX loading gates
let districtPreloadToken = 0;
let districtIsLoading = false;
let searchEnabled = false;

// search settings
const SCOPE = { VOTER: "voter", RELATIVE: "relative", ANYWHERE: "anywhere" };

// filters
const filters = {
  gender: "all", // all|male|female|other
  age: { mode: "any", a: "", b: "" }, // any|eq|gt|lt|range
  relativeName: "",
  sort: "relevance", // relevance|age_asc|age_desc|serial_asc|serial_desc
};

// ---------- UI refs ----------
const els = {
  districtSel: $("#district"),
  districtSelHidden: $("#district_hidden"),
  acSel: $("#ac"),
  scopeSel: $("#scope"),
  qInput: $("#q"),
  relInput: $("#rel"),
  exactChk: $("#exact"),
  btnSearch: $("#btnSearch"),
  status: $("#status"),
  meta: $("#meta"),
  results: $("#results"),
  pager: $("#pager"),
  pageSizeSel: $("#pageSize"),
  btnClear: $("#btnClear"),

  // filters UI
  filterGender: $("#filterGender"),
  filterAgeMode: $("#filterAgeMode"),
  filterAgeA: $("#filterAgeA"),
  filterAgeB: $("#filterAgeB"),
  filterSort: $("#filterSort"),

  langSel: $("#lang"),
};

// ---------- i18n ----------
function initI18n() {
  i18n = createI18n();
  lang = i18n.getSavedLangOrDefault();
  t = i18n.t;
  updateLangUI();
}

function updateLangUI() {
  if (!els.langSel) return;
  els.langSel.value = lang;
  $("#title").textContent = t("title");
  $("#subtitle").textContent = t("subtitle");
  $("#label_district").textContent = t("label_district");
  $("#label_ac").textContent = t("label_ac");
  $("#label_scope").textContent = t("label_scope");
  $("#label_q").textContent = t("label_q");
  $("#label_rel").textContent = t("label_rel");
  $("#label_exact").textContent = t("label_exact");
  els.btnSearch.textContent = t("btn_search");
  els.btnClear.textContent = t("btn_clear");
  $("#label_page_size").textContent = t("label_page_size");
  $("#filters_title").textContent = t("filters_title");
  $("#label_gender").textContent = t("label_gender");
  $("#label_age").textContent = t("label_age");
  $("#label_sort").textContent = t("label_sort");

  // placeholders
  els.qInput.placeholder = t("ph_q");
  els.relInput.placeholder = t("ph_rel");
}

// ---------- status helpers ----------
function setStatus(msg) {
  if (els.status) els.status.textContent = msg || "";
}
function setMeta(msg) {
  if (els.meta) els.meta.textContent = msg || "";
}
function setDistrictLoading(on) {
  districtIsLoading = !!on;
  document.body.classList.toggle("district-loading", districtIsLoading);
}
function setSearchEnabled(on) {
  searchEnabled = !!on;
  syncSearchButtonState();
}
function syncSearchButtonState() {
  const hasDistrict = !!currentDistrictId;
  const hasAc = !!els.acSel.value;
  const hasQuery = !!norm(els.qInput.value);
  const can = searchEnabled && !districtIsLoading && hasDistrict && hasAc && hasQuery;
  els.btnSearch.disabled = !can;
  els.qInput.disabled = !hasDistrict;
  els.relInput.disabled = !hasDistrict;
}

// ---------- local helpers ----------
function makeKey(ac, row_id) {
  return `${String(ac).padStart(2, "0")}:${row_id}`;
}

function parseAgeValue(v) {
  if (v === null || v === undefined) return null;
  const s = String(v).trim();
  if (!s) return null;
  const n = Number(s);
  return Number.isFinite(n) ? n : null;
}

function normGenderValue(v) {
  const s = String(v || "").trim().toLowerCase();
  if (!s) return "other";
  if (s === "m" || s === "male" || s === "पुरुष" || s === "पु") return "male";
  if (s === "f" || s === "female" || s === "महिला" || s === "म") return "female";
  return "other";
}

// ---------- DB-backed: no client-side loading ----------
async function initDuckDB() {
  // DB-backed build: DuckDB/parquet is not used.
  // Kept as a no-op to avoid touching unrelated UI logic.
  return;
}

// ---------- District manifest ----------
const FALLBACK_DISTRICT_MAP = [
  { id: "sahebganj", label: "Sahebganj", acs: [1, 2, 3] },
  { id: "pakur", label: "Pakur", acs: [4, 5, 6] },
  { id: "dumka", label: "Dumka", acs: [7, 10, 11, 12] },
  { id: "jamtara", label: "Jamtara", acs: [8, 9] },
  { id: "deoghar", label: "Deoghar", acs: [13, 14, 15] },
  { id: "godda", label: "Godda", acs: [16, 17, 18] },
  { id: "kodarma", label: "Kodarma", acs: [19] },
  { id: "hazaribagh", label: "Hazaribagh", acs: [20, 21, 24, 25] },
  { id: "ramgarh", label: "Ramgarh", acs: [22, 23] },
  { id: "chatra", label: "Chatra", acs: [26, 27] },
  { id: "giridih", label: "Giridih", acs: [28, 29, 30, 31, 32, 33] },
  { id: "bokaro", label: "Bokaro", acs: [34, 35, 36, 37] },
  { id: "dhanbad", label: "Dhanbad", acs: [38, 39, 40, 41, 42, 43] },
  { id: "east-singhbhum", label: "East Singhbhum", acs: [44, 45, 46, 47, 48, 49] },
  { id: "saraikela-kharswan", label: "Saraikela-Kharswan", acs: [50, 51, 57] },
  { id: "west-singhbhum", label: "West Singhbhum", acs: [52, 53, 54, 55, 56] },
  { id: "ranchi", label: "Ranchi", acs: [58, 61, 62, 63, 64, 65, 66] },
  { id: "khunti", label: "Khunti", acs: [59, 60] },
  { id: "gumla", label: "Gumla", acs: [67, 68, 69] },
  { id: "simdega", label: "Simdega", acs: [70, 71] },
  { id: "lohardaga", label: "Lohardaga", acs: [72] },
  { id: "latehar", label: "Latehar", acs: [73, 74] },
  { id: "palamu", label: "Palamu", acs: [75, 76, 77, 78, 79] },
  { id: "garhwa", label: "Garhwa", acs: [80, 81] },
];

function normalizeDistrictManifest(raw) {
  if (!raw) return null;
  const arr = Array.isArray(raw) ? raw : raw.districts;
  if (!Array.isArray(arr)) return null;
  return arr
    .map((d) => ({
      id: String(d.id ?? d.label ?? d.name ?? ""),
      label: String(d.label ?? d.name ?? d.id ?? ""),
      acs: (d.acs || d.ACs || d.ac || []).map((x) => Number(x)).filter(Number.isFinite),
    }))
    .filter((d) => d.id && d.label && d.acs.length);
}

async function loadDistrictManifest() {
  try {
    const url = relUrl(`data/${STATE_CODE_DEFAULT}/district_manifest.json`);
    const resp = await fetch(url, { cache: "no-cache" });
    if (!resp.ok) throw new Error(`HTTP ${resp.status}`);
    const raw = await resp.json();
    const normed = normalizeDistrictManifest(raw);
    if (!normed) throw new Error("Bad manifest shape");
    currentDistrictManifest = normed;
    return;
  } catch (e) {
    console.warn("District manifest load failed; using fallback.", e);
    currentDistrictManifest = FALLBACK_DISTRICT_MAP;
  }
}

function renderDistrictOptions() {
  const sel = els.districtSel;
  const hid = els.districtSelHidden;
  if (!sel || !hid) return;

  sel.innerHTML = "";
  hid.innerHTML = "";

  const opt0 = el("option");
  opt0.value = "";
  opt0.textContent = t("opt_select_district");
  sel.appendChild(opt0);

  const optH0 = el("option");
  optH0.value = "";
  optH0.textContent = t("opt_select_district");
  hid.appendChild(optH0);

  const sorted = [...(currentDistrictManifest || [])].sort((a, b) => a.label.localeCompare(b.label));
  for (const d of sorted) {
    const o = el("option");
    o.value = d.id;
    o.textContent = d.label;
    sel.appendChild(o);

    const oh = el("option");
    oh.value = d.id;
    oh.textContent = d.label;
    hid.appendChild(oh);
  }
}

function getDistrictById(id) {
  return (currentDistrictManifest || []).find((d) => d.id === id) || null;
}

function renderAcOptions(acs) {
  const sel = els.acSel;
  if (!sel) return;
  sel.innerHTML = "";
  const opt0 = el("option");
  opt0.value = "";
  opt0.textContent = t("opt_select_ac");
  sel.appendChild(opt0);

  const sorted = [...acs].map(Number).filter(Number.isFinite).sort((a, b) => a - b);
  for (const ac of sorted) {
    const o = el("option");
    o.value = String(ac);
    o.textContent = String(ac).padStart(2, "0");
    sel.appendChild(o);
  }
}

async function preloadDistrictACs(acs, districtLabel) {
  if (!acs || !acs.length) return;

  const token = ++districtPreloadToken;

  setSearchEnabled(false);
  setDistrictLoading(true);

  try {
    setStatus(t("status_loading_district", { district: districtLabel, n: acs.length }));
    setMeta("");

    // DB-backed build: we do NOT preload large files into the browser.
    // We keep the same UX (status + disabled search) but return quickly.
    await new Promise((r) => setTimeout(r, 50));

    if (token !== districtPreloadToken) return;
    setStatus(t("status_ready_district_loaded", { district: districtLabel, n: acs.length }));
  } finally {
    if (token === districtPreloadToken) {
      setDistrictLoading(false);
      setSearchEnabled(true);
      syncSearchButtonState();
    }
  }
}

// ---------- AC loader (no-op; kept for compatibility) ----------
async function loadAC(stateCode, acNo) {
  // DB-backed build: no client-side file loading.
  // Kept for compatibility with existing UI flows.
  current.state = stateCode;
  current.ac = Number(acNo);
  current.loaded = true;
  return;
}

// ---------- tokenization / key building (UNCHANGED) ----------
function splitTokens(s) {
  const out = [];
  for (const p of String(s || "").split(/\s+/)) {
    const t = p.trim();
    if (t) out.push(t);
  }
  return out;
}

function tokenize(q) {
  // keep existing behavior (simple whitespace tokenization)
  return splitTokens(q);
}

function tokenizeExactIndex(q) {
  // exact index uses raw tokens, but normalized casing
  return splitTokens(q);
}

function tokenizeLoose(q) {
  // loose index uses raw tokens, but normalized casing
  return splitTokens(q);
}

function buildKeysFromTokens(tokens, prefixLen) {
  const out = [];
  for (const tok of tokens) {
    const t = String(tok || "").trim();
    if (!t) continue;
    out.push(t.slice(0, prefixLen));
  }
  return [...new Set(out)];
}

// ---------- candidates via API ----------
async function getCandidatesForQuery(q, scope, exactOn, acNo) {
  const strictTokens = tokenize(q);
  const strictKeys = buildKeysFromTokens(strictTokens, PREFIX_LEN_STRICT);

  const exactTokens = tokenizeExactIndex(q);
  const exactKeys = buildKeysFromTokens(exactTokens, PREFIX_LEN_EXACT);

  const looseTokens = tokenizeLoose(q);
  const looseKeys = buildKeysFromTokens(looseTokens, PREFIX_LEN_LOOSE);

  if (!strictKeys.length && !exactKeys.length && !looseKeys.length) {
    return {
      candidates: [],
      metaByRow: new Map(),
      strictKeys,
      exactKeys,
      looseKeys,
    };
  }

  const wantLoose = !exactOn;
  const district = districtToDbSlug(currentDistrictId || currentDistrictLabel);

  const ac = Number(acNo ?? current.ac);
  if (!district || !Number.isFinite(ac)) {
    return {
      candidates: [],
      metaByRow: new Map(),
      strictKeys,
      exactKeys,
      looseKeys,
    };
  }

  const resp = await apiPostJson("candidates", {
    state: STATE_CODE_DEFAULT,
    district,
    ac,
    scope,
    exactOn: !!exactOn,
    strictKeys,
    exactKeys,
    looseKeys: wantLoose ? looseKeys : [],
  });

  const candidates = Array.isArray(resp?.candidates)
    ? resp.candidates.map(Number).filter(Number.isFinite)
    : [];

  const metaByRow = new Map();
  if (resp && resp.metaByRow && typeof resp.metaByRow === "object") {
    for (const [rid, meta] of Object.entries(resp.metaByRow)) {
      metaByRow.set(Number(rid), meta);
    }
  }

  return { candidates, metaByRow, strictKeys, exactKeys, looseKeys };
}

// ---------- rows via API ----------
async function fetchRowsByIds(rowIds, acNo) {
  const district = districtToDbSlug(currentDistrictId || currentDistrictLabel);
  const ac = Number(acNo ?? current.ac);
  const ids = Array.isArray(rowIds) ? rowIds.map(Number).filter(Number.isFinite) : [];
  if (!district || !Number.isFinite(ac) || !ids.length) return [];

  const resp = await apiPostJson("rows", {
    state: STATE_CODE_DEFAULT,
    district,
    ac,
    mode: "score",
    rowIds: ids,
  });

  const rows = Array.isArray(resp?.rows) ? resp.rows : [];
  // Ensure row_id is numeric
  for (const r of rows) {
    if (r && r.row_id != null) r.row_id = Number(r.row_id);
  }
  return rows;
}

async function fetchDisplayRowsByIds(rowIds, acNo) {
  const district = districtToDbSlug(currentDistrictId || currentDistrictLabel);
  const ac = Number(acNo ?? current.ac);
  const ids = Array.isArray(rowIds) ? rowIds.map(Number).filter(Number.isFinite) : [];
  if (!district || !Number.isFinite(ac) || !ids.length) return [];

  const resp = await apiPostJson("rows", {
    state: STATE_CODE_DEFAULT,
    district,
    ac,
    mode: "display",
    rowIds: ids,
  });

  const rows = Array.isArray(resp?.rows) ? resp.rows : [];
  for (const r of rows) {
    if (r && r.row_id != null) r.row_id = Number(r.row_id);
  }
  return rows;
}

// ---------- filters ----------
async function computeRowIdSetByRelativeFilterForAc(exactOn, acNo) {
  const rel = norm(filters.relativeName || "");
  if (!rel) return null;
  const { candidates } = await getCandidatesForQuery(rel, SCOPE.RELATIVE, exactOn, acNo);
  return new Set(candidates.map(Number));
}

async function computeRowIdSetByGenderAndAgeForAc(rowIdsInThisAc, acNo) {
  const hasGender = filters.gender !== "all";
  const hasAge = filters.age.mode !== "any";
  if (!hasGender && !hasAge) return null;
  if (!rowIdsInThisAc.length) return new Set();

  const ac = Number(acNo ?? current.ac);
  const out = new Set();

  for (const ridRaw of rowIdsInThisAc) {
    const rid = Number(ridRaw);
    if (!Number.isFinite(rid)) continue;

    const k = makeKey(ac, rid);
    const sc = scoreCache.get(k) || {};

    if (hasGender) {
      const b = normGenderValue(sc.gender);
      if (b !== filters.gender) continue;
    }

    if (hasAge) {
      const age = parseAgeValue(sc.age);
      if (age === null) continue; // mirrors TRY_CAST(...)=NULL -> not matched

      const a = Number(filters.age.a);
      const b = Number(filters.age.b);

      if (filters.age.mode === "eq" && Number.isFinite(a) && age !== a) continue;
      if (filters.age.mode === "gt" && Number.isFinite(a) && !(age > a)) continue;
      if (filters.age.mode === "lt" && Number.isFinite(a) && !(age < a)) continue;
      if (filters.age.mode === "range" && Number.isFinite(a) && Number.isFinite(b)) {
        const lo = Math.min(a, b), hi = Math.max(a, b);
        if (!(age >= lo && age <= hi)) continue;
      }
    }

    out.add(rid);
  }

  return out;
}

async function ensureAgeMapLoaded(keysToLoad) {
  if (!ageMap) ageMap = new Map();
  if (!keysToLoad || !keysToLoad.length) return;
  if (keysToLoad.every((k) => ageMap.has(k))) return;

  setStatus(t("status_preparing_age_sort"));

  let done = 0;
  const total = keysToLoad.length;

  for (const k of keysToLoad) {
    if (ageMap.has(k)) {
      done++;
      continue;
    }
    const sc = scoreCache.get(k);
    ageMap.set(k, parseAgeValue(sc?.age));
    done++;
  }

  setStatus(t("status_age_sort_ready", { done, total }));
}

// ---------- worker ranking ----------
let worker = null;
function initWorker() {
  worker = new Worker("./worker.js", { type: "module" });
}

function runWorkerRanking(rowsWithMeta, qNorm, scope, strictKeys, exactKeys, looseKeys) {
  return new Promise((resolve, reject) => {
    const id = Math.random().toString(36).slice(2);
    const onMsg = (ev) => {
      const msg = ev.data;
      if (!msg || msg.id !== id) return;
      worker.removeEventListener("message", onMsg);
      if (msg.error) reject(new Error(msg.error));
      else resolve(msg.result || []);
    };
    worker.addEventListener("message", onMsg);

    worker.postMessage({
      id,
      rows: rowsWithMeta,
      qNorm,
      scope,
      strictKeys,
      exactKeys,
      looseKeys,
      nameCol: NAME_COL,
      relCol: REL_COL,
      nameRawCol: NAME_RAW_COL,
      relRawCol: REL_RAW_COL,
    });
  });
}

// ---------- sort + render ----------
function applySort(list) {
  const mode = filters.sort;

  if (mode === "relevance") return list;

  if (mode === "serial_asc" || mode === "serial_desc") {
    const dir = mode === "serial_asc" ? 1 : -1;
    const out = [...list];
    out.sort((a, b) => {
      const sa = safeInt(a.serial);
      const sb = safeInt(b.serial);
      if (sa === null && sb === null) return 0;
      if (sa === null) return 1;
      if (sb === null) return -1;
      return (sa - sb) * dir;
    });
    return out;
  }

  if (mode === "age_asc" || mode === "age_desc") {
    const dir = mode === "age_asc" ? 1 : -1;
    const keys = list.map((x) => x.key);
    return ensureAgeMapLoaded(keys).then(() => {
      const out = [...list];
      out.sort((a, b) => {
        const aa = ageMap.get(a.key);
        const ab = ageMap.get(b.key);
        if (aa === null && ab === null) return 0;
        if (aa === null) return 1;
        if (ab === null) return -1;
        return (aa - ab) * dir;
      });
      return out;
    });
  }

  return list;
}

function renderTable(rows) {
  els.results.innerHTML = "";
  if (!rows.length) {
    els.results.textContent = t("no_results");
    return;
  }

  const table = el("table", "tbl");
  const thead = el("thead");
  const trh = el("tr");
  for (const col of DISPLAY_COLS) {
    const th = el("th");
    th.textContent = t(`col_${col.replace(/\s+/g, "_").toLowerCase()}`) || col;
    trh.appendChild(th);
  }
  thead.appendChild(trh);
  table.appendChild(thead);

  const tbody = el("tbody");
  for (const row of rows) {
    const tr = el("tr");
    for (const col of DISPLAY_COLS) {
      const td = el("td");
      td.textContent = row[col] ?? "";
      tr.appendChild(td);
    }
    tbody.appendChild(tr);
  }
  table.appendChild(tbody);

  els.results.appendChild(table);
}

function buildPdfUrl(row) {
  const state = row["State Code"] || STATE_CODE_DEFAULT;
  const ac = String(row["AC No"] || "").padStart(2, "0");
  const part = String(row["Part No"] || "").trim();
  if (!state || !ac || !part) return "";
  return `https://www.eci.gov.in/sir/f3/${state}/data/OLDSIRROLL/${state}/${ac}/${state}_${ac}_${part}.pdf`;
}

async function renderPage() {
  const start = (page - 1) * pageSize;
  const end = start + pageSize;
  const slice = filteredBase.slice(start, end);

  // Group missing display rows by AC
  const missingByAc = new Map();
  for (const item of slice) {
    const k = item.key;
    if (displayCache.has(k)) continue;
    if (!missingByAc.has(item.ac)) missingByAc.set(item.ac, []);
    missingByAc.get(item.ac).push(item.row_id);
  }

  for (const [ac, rowIds] of missingByAc.entries()) {
    await loadAC(STATE_CODE_DEFAULT, ac);
    const rows = await fetchDisplayRowsByIds(rowIds);
    for (const r of rows) {
      const k = makeKey(ac, r.row_id);
      displayCache.set(k, r);
    }
  }

  const rowsToShow = slice
    .map((item) => displayCache.get(item.key))
    .filter(Boolean);

  renderTable(rowsToShow);
  renderPager();
}

function renderPager() {
  const total = filteredBase.length;
  const pages = Math.max(1, Math.ceil(total / pageSize));
  page = clamp(page, 1, pages);

  els.pager.innerHTML = "";

  const info = el("div", "pager-info");
  info.textContent = t("pager_info", { page, pages, total });
  els.pager.appendChild(info);

  const btnPrev = el("button", "btn");
  btnPrev.textContent = t("pager_prev");
  btnPrev.disabled = page <= 1;
  btnPrev.onclick = async () => {
    page--;
    await renderPage();
  };

  const btnNext = el("button", "btn");
  btnNext.textContent = t("pager_next");
  btnNext.disabled = page >= pages;
  btnNext.onclick = async () => {
    page++;
    await renderPage();
  };

  const wrap = el("div", "pager-btns");
  wrap.appendChild(btnPrev);
  wrap.appendChild(btnNext);

  els.pager.appendChild(wrap);
}

// ---------- search pipeline ----------
async function applyFiltersThenSortThenRender() {
  filteredBase = rankedByRelevance.slice();

  // scope: if not voter search, filters disabled (matches existing UX)
  const scope = els.scopeSel.value || SCOPE.VOTER;
  if (scope !== SCOPE.VOTER) {
    filters.gender = "all";
    filters.age = { mode: "any", a: "", b: "" };
    filters.relativeName = "";
    filters.sort = "relevance";
  }

  // relative name filter only applies when scope is voter
  if (scope === SCOPE.VOTER) {
    const exactOn = !!els.exactChk.checked;

    // Group by AC
    const byAc = new Map();
    for (const item of filteredBase) {
      if (!byAc.has(item.ac)) byAc.set(item.ac, []);
      byAc.get(item.ac).push(item.row_id);
    }

    const allowedKeys = new Set();

    for (const [ac, rowIds] of byAc.entries()) {
      setStatus(t("status_applying_filters_ac", { ac: String(ac).padStart(2, "0") }));

      // DB-backed build: no per-AC client loading.

      let relSet = null;
      if (norm(filters.relativeName || "")) {
        relSet = await computeRowIdSetByRelativeFilterForAc(exactOn, ac);
      }

      const gaSet = await computeRowIdSetByGenderAndAgeForAc(rowIds, ac);

      for (const rid of rowIds) {
        if (relSet && !relSet.has(rid)) continue;
        if (gaSet && !gaSet.has(rid)) continue;
        allowedKeys.add(makeKey(ac, rid));
      }
    }

    filteredBase = filteredBase.filter((x) => allowedKeys.has(x.key));
  }

  // Sort
  const maybePromise = applySort(filteredBase);
  filteredBase = typeof maybePromise?.then === "function" ? await maybePromise : maybePromise;

  // Reset display cache for pagination
  page = 1;
  displayCache.clear();

  setStatus(t("status_ready"));
  await renderPage();
}

async function runSearch() {
  const districtId = els.districtSel.value;
  const district = getDistrictById(districtId);
  if (!district) return;

  currentDistrictId = district.id;
  currentDistrictLabel = district.label;

  const ac = Number(els.acSel.value);
  if (!Number.isFinite(ac)) return;

  const scope = els.scopeSel.value || SCOPE.VOTER;
  const exactOn = !!els.exactChk.checked;

  const qStrict = norm(els.qInput.value);
  if (!qStrict) return;

  // reset
  rankedByRelevance = [];
  filteredBase = [];
  ageMap = null;
  displayCache.clear();
  scoreCache.clear();
  setMeta("");
  page = 1;

  setStatus(t("status_searching"));

  // determine ACs to scan:
  // - if user picked a specific AC, just that
  // - else scan all ACs in district (existing behavior)
  const districtAcs = district.acs || [];
  const acsToSearch = [ac];

  initDuckDB(); // no-op
  initWorker();

  const allRanked = [];

  let doneAcs = 0;
  for (const ac of acsToSearch) {
    doneAcs++;
    setStatus(t("status_searching_ac", { ac: String(ac).padStart(2, "0"), done: doneAcs, total: acsToSearch.length }));

    try {
      await loadAC(STATE_CODE_DEFAULT, ac);

      const { candidates, metaByRow, strictKeys, exactKeys, looseKeys } =
        await getCandidatesForQuery(qStrict, scope, exactOn, ac);

      if (!candidates.length) continue;

      setMeta(t("meta_candidates", { ac: String(ac).padStart(2, "0"), n: candidates.length }));

      const rows = await fetchRowsByIds(candidates, ac);
      const rowsWithMeta = rows.map((r) => ({ ...r, _meta: metaByRow.get(r.row_id) || null }));

      // Cache minimal fields needed for post-ranking filters & age sorting
      if (!ageMap) ageMap = new Map();
      for (const r of rows) {
        const k = makeKey(ac, r.row_id);
        scoreCache.set(k, { gender: r.gender, age: r.age });
        if (!ageMap.has(k)) ageMap.set(k, parseAgeValue(r.age));
      }

      const qNorm = qStrict;
      const ranked = await runWorkerRanking(rowsWithMeta, qNorm, scope, strictKeys, exactKeys, looseKeys);

      // enrich with key + ac
      for (const item of ranked) {
        allRanked.push({
          ...item,
          ac,
          key: makeKey(ac, item.row_id),
        });
      }
    } catch (e) {
      console.warn("AC search failed:", ac, e);
      continue;
    }
  }

  rankedByRelevance = allRanked;
  filteredBase = rankedByRelevance.slice();

  setStatus(t("status_scored", { n: rankedByRelevance.length }));
  await applyFiltersThenSortThenRender();
}

// ---------- events ----------
function bindEvents() {
  els.langSel?.addEventListener("change", (e) => {
    lang = e.target.value;
    i18n.setLang(lang);
    updateLangUI();
    renderDistrictOptions();
    syncSearchButtonState();
    if (filteredBase.length) renderPage();
  });

  els.districtSel?.addEventListener("change", async (e) => {
    const id = e.target.value;
    const d = getDistrictById(id);

    currentDistrictId = id || "";
    currentDistrictLabel = d ? d.label : "";

    ageMap = null;
    displayCache.clear();
    scoreCache.clear();
    rankedByRelevance = [];
    filteredBase = [];
    els.results.innerHTML = "";
    els.pager.innerHTML = "";

    if (!d) {
      renderAcOptions([]);
      setStatus(t("status_pick_district"));
      syncSearchButtonState();
      return;
    }

    renderAcOptions(d.acs || []);
    setStatus(t("status_district_selected", { district: d.label, n: (d.acs || []).length }));
    await preloadDistrictACs(d.acs || [], d.label);
    syncSearchButtonState();
  });

  els.acSel?.addEventListener("change", () => {
    rankedByRelevance = [];
    filteredBase = [];
    displayCache.clear();
    els.results.innerHTML = "";
    els.pager.innerHTML = "";
    setStatus(t("status_ready"));
    syncSearchButtonState();
  });

  els.qInput?.addEventListener("input", syncSearchButtonState);
  els.relInput?.addEventListener("input", () => {
    filters.relativeName = els.relInput.value;
  });

  els.exactChk?.addEventListener("change", () => {
    // exact changes candidate generation; require re-search to apply
  });

  els.btnSearch?.addEventListener("click", async () => {
    try {
      await runSearch();
    } catch (e) {
      console.error(e);
      setStatus(String(e.message || e));
    }
  });

  els.btnClear?.addEventListener("click", () => {
    els.qInput.value = "";
    els.relInput.value = "";
    rankedByRelevance = [];
    filteredBase = [];
    ageMap = null;
    displayCache.clear();
    scoreCache.clear();
    els.results.innerHTML = "";
    els.pager.innerHTML = "";
    setStatus(t("status_ready"));
    syncSearchButtonState();
  });

  // filters
  els.filterGender?.addEventListener("change", async (e) => {
    filters.gender = e.target.value;
    await applyFiltersThenSortThenRender();
  });

  els.filterAgeMode?.addEventListener("change", async (e) => {
    filters.age.mode = e.target.value;
    await applyFiltersThenSortThenRender();
  });

  els.filterAgeA?.addEventListener("input", (e) => {
    filters.age.a = e.target.value;
  });
  els.filterAgeB?.addEventListener("input", (e) => {
    filters.age.b = e.target.value;
  });

  els.filterSort?.addEventListener("change", async (e) => {
    filters.sort = e.target.value;
    await applyFiltersThenSortThenRender();
  });

  els.pageSizeSel?.addEventListener("change", async (e) => {
    pageSize = Number(e.target.value) || getDefaultPageSize();
    page = 1;
    displayCache.clear();
    await renderPage();
  });

  window.addEventListener("resize", () => {
    const opts = getPageSizeOptions();
    pageSize = opts.includes(pageSize) ? pageSize : getDefaultPageSize();
    renderPageSizeOptions();
  });
}

function renderPageSizeOptions() {
  const sel = els.pageSizeSel;
  if (!sel) return;
  sel.innerHTML = "";
  const opts = getPageSizeOptions();
  for (const n of opts) {
    const o = el("option");
    o.value = String(n);
    o.textContent = String(n);
    sel.appendChild(o);
  }
  sel.value = String(pageSize);
}

// ---------- boot ----------
async function boot() {
  initI18n();
  renderPageSizeOptions();

  await loadDistrictManifest();
  renderDistrictOptions();

  setStatus(t("status_pick_district"));
  setSearchEnabled(true);
  bindEvents();
  syncSearchButtonState();
}

boot();
