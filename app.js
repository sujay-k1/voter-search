// app.js (ES module)
// Language switch moved to i18n.js (strings + translation runtime).
// No search logic changed; only i18n block extracted.

import { LANG, createI18n } from "./i18n.js";

/**
 * IMPORTANT (hosting under subfolder like /voter-search/):
 * Never use absolute paths like "/data/..." or "/duckdb/...".
 * Always resolve relative to the folder that contains this app.
 */
const APP_BASE = new URL("./", window.location.href); // e.g. https://sujaykumar.net/voter-search/
const relUrl = (p) => new URL(String(p).replace(/^\/+/, ""), APP_BASE).toString();

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
  "ID",
  "Part No",
  "Page No",
  "Source PDF",
];

// scopes
const SCOPE = {
  VOTER: "voter",
  RELATIVE: "relative",
  ANYWHERE: "anywhere",
};

// ---------- i18n ----------
let i18n = createI18n();
function t(key, vars) {
  return i18n.t(key, vars);
}

// ---------- DOM ----------
const $ = (id) => document.getElementById(id);

const landing = $("landing");
const results = $("results");

const districtSelect = $("districtSelect");
const acSelect = $("acSelect"); // multi-select (or single)
const qLanding = $("qLanding");
const qResults = $("qResults");

const searchBtnLanding = $("searchBtnLanding");
const searchBtnResults = $("searchBtnResults");

const statusEl = $("status");
const barWrap = $("barWrap");
const bar = $("bar");

const scopeTabs = document.querySelectorAll("[data-scope]");
const includeTypingLanding = $("includeTypingLanding");
const includeTypingResults = $("includeTypingResults");

const langBtnHi = $("langHi");
const langBtnHinglish = $("langHinglish");
const langBtnEn = $("langEn");

const filtersWrap = $("filtersWrap");
const filterGender = $("filterGender");
const filterMinAge = $("filterMinAge");
const filterMaxAge = $("filterMaxAge");
const filterQuery = $("filterQuery");

const sortSelect = $("sortSelect");
const pageSizeSelect = $("pageSizeSelect");
const pagerPrev = $("pagerPrev");
const pagerNext = $("pagerNext");
const pagerInfo = $("pagerInfo");

const tableWrap = $("tableWrap");
const tableHead = $("tableHead");
const tableBody = $("tableBody");

const backBtn = $("backBtn");

// ---------- State ----------
let manifest = null; // district_manifest.json
let current = {
  districtId: "",
  districtLabel: "",
  ac: null,
};

let searchScope = SCOPE.VOTER;

let rankedByRelevance = []; // [{ key, ac, row_id, score }]
let filteredBase = []; // subset of rankedByRelevance after filters
let rankedView = []; // current sorted view (may be filtered & sorted)
let page = 1;

let pageSize = PAGE_SIZE_DESKTOP_DEFAULT;

let searchEnabled = false;
let districtLoading = false;
let searchInFlight = false;
let searchRunToken = 0;
let searchAbortCtrl = null;

// Score cache: minimal info needed for filters + worker ranking
// key: `${ac}:${row_id}` -> row
const scoreCache = new Map();

// Display cache: full display row for the table
const displayCache = new Map();

// ---------- Utils ----------
function setStatus(msg) {
  if (statusEl) statusEl.textContent = msg || "";
}

function setBar(pct) {
  if (!barWrap || !bar) return;
  if (pct == null) {
    barWrap.style.display = "none";
    return;
  }
  barWrap.style.display = "block";
  const clamped = Math.max(0, Math.min(100, Number(pct) || 0));
  bar.style.width = `${clamped}%`;
}

function showLanding() {
  landing.style.display = "block";
  results.style.display = "none";
}

function showResults() {
  landing.style.display = "none";
  results.style.display = "block";
}

function escapeHtml(s) {
  return String(s ?? "")
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;");
}

function isMobile() {
  return window.matchMedia && window.matchMedia("(max-width: 720px)").matches;
}

function makeKey(ac, rowId) {
  return `${Number(ac)}:${Number(rowId)}`;
}

function getActiveQueryInput() {
  return results.style.display !== "none" ? qResults : qLanding;
}

function exactOnFromIncludeTyping() {
  // checkbox label = "Including typing mistakes"
  // When checked => include typos => exactOn = false
  const el = results.style.display !== "none" ? includeTypingResults : includeTypingLanding;
  return el ? !el.checked : false;
}

function getCurrentDistrictSlug() {
  const v = districtSelect?.value || "";
  return String(v || "").trim();
}

function getActiveACs() {
  // If you have a multi-select: take selected options.
  // If single select: returns [Number(value)].
  const districtId = getCurrentDistrictSlug();
  if (!districtId || !manifest) return [];

  const district = (manifest.districts || []).find((d) => d.id === districtId);
  if (!district) return [];

  // If AC selector exists and has selections, respect it; otherwise use all district ACs.
  if (!acSelect) return district.acs.map(Number);

  const selected = Array.from(acSelect.selectedOptions || []).map((o) => Number(o.value)).filter(Number.isFinite);

  if (selected.length) return selected;

  // default: all in district
  return (district.acs || []).map(Number).filter(Number.isFinite);
}

function syncSearchButtonState() {
  const hasDistrict = !!getCurrentDistrictSlug();
  const canSearch = searchEnabled && !districtLoading && hasDistrict && !searchInFlight;

  if (searchBtnLanding) searchBtnLanding.disabled = !canSearch;
  if (searchBtnResults) searchBtnResults.disabled = !canSearch;

  if (qLanding) qLanding.disabled = !canSearch;
  if (qResults) qResults.disabled = !canSearch;

  if (districtSelect) districtSelect.disabled = districtLoading || searchInFlight;
  if (acSelect) acSelect.disabled = districtLoading || searchInFlight;

  if (includeTypingLanding) includeTypingLanding.disabled = districtLoading || searchInFlight || !hasDistrict;
  if (includeTypingResults) includeTypingResults.disabled = districtLoading || searchInFlight || !hasDistrict;
}

function cancelInFlightSearch() {
  // Invalidate any in-flight runSearch loops + abort network calls + reset worker
  searchRunToken++;
  if (searchAbortCtrl) {
    try {
      searchAbortCtrl.abort();
    } catch {}
  }
  searchAbortCtrl = null;

  if (worker) {
    try {
      worker.terminate();
    } catch {}
    worker = null;
  }
  pendingResolve = null;
  pendingReject = null;
  searchInFlight = false;
  syncSearchButtonState();
}

// ---------- Backend (Netlify Functions) ----------
function fnUrl(name) {
  return relUrl(`.netlify/functions/${name}`);
}

async function postJson(url, payload, signal) {
  const resp = await fetch(url, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify(payload || {}),
    signal,
  });

  const text = await resp.text();
  let json;
  try {
    json = text ? JSON.parse(text) : {};
  } catch {
    json = { raw: text };
  }

  if (!resp.ok) {
    const msg = json?.error || json?.message || `HTTP ${resp.status}`;
    throw new Error(msg);
  }
  return json;
}

async function callFn(name, payload, signal) {
  return postJson(fnUrl(name), payload, signal);
}

// ---------- District manifest + selectors ----------
async function loadManifest() {
  const url = relUrl(`data/${STATE_CODE_DEFAULT}/district_manifest.json`);
  const resp = await fetch(url, { cache: "no-store" });
  if (!resp.ok) throw new Error(`Failed to load district manifest (${resp.status})`);
  manifest = await resp.json();
}

function populateDistrictSelect() {
  if (!districtSelect || !manifest) return;

  districtSelect.innerHTML = "";
  const opt0 = document.createElement("option");
  opt0.value = "";
  opt0.textContent = t("district_placeholder");
  districtSelect.appendChild(opt0);

  for (const d of manifest.districts || []) {
    const opt = document.createElement("option");
    opt.value = d.id;
    opt.textContent = d.label;
    districtSelect.appendChild(opt);
  }
}

function populateACSelectForDistrict(districtId) {
  if (!acSelect || !manifest) return;
  acSelect.innerHTML = "";

  const district = (manifest.districts || []).find((d) => d.id === districtId);
  if (!district) return;

  // Optional placeholder if single-select
  if (!acSelect.multiple) {
    const opt0 = document.createElement("option");
    opt0.value = "";
    opt0.textContent = t("ac_placeholder");
    acSelect.appendChild(opt0);
  }

  for (const ac of district.acs || []) {
    const opt = document.createElement("option");
    opt.value = String(ac);
    opt.textContent = String(ac).padStart(2, "0");
    acSelect.appendChild(opt);
  }

  // Default: select all if multi-select
  if (acSelect.multiple) {
    for (const o of acSelect.options) o.selected = true;
  }
}

// ---------- Score + display cache ----------
function cacheScoreRow(ac, r) {
  const key = makeKey(ac, r.row_id);
  const prev = scoreCache.get(key);

  const row = {
    row_id: Number(r.row_id),
    ac: Number(ac),
    serial_no: r.serial_no ?? r["Serial No"] ?? "",
    voter_name_raw: r.voter_name_raw ?? r["Voter Name"] ?? "",
    relative_name_raw: r.relative_name_raw ?? r["Relative Name"] ?? "",
    voter_name_norm: r.voter_name_norm ?? "",
    relative_name_norm: r.relative_name_norm ?? "",
    gender: r.gender ?? r["Gender"] ?? "",
    age: r.age ?? r["Age"] ?? "",
  };

  if (!prev) scoreCache.set(key, row);
}

function getScoreRow(ac, rowId) {
  return scoreCache.get(makeKey(ac, rowId)) || null;
}

// ---------- Strict / Exact / Loose query helpers (RESTORED FROM ORIGINAL) ----------

// ---------------- Strict normalization ----------------
// Minimal strict folding to match your prefix_3 index
function norm(s) {
  return String(s ?? "")
    .trim()
    .replace(/\s+/g, " ");
}

function tokenize(s) {
  const q = norm(s);
  if (!q) return [];
  // split on whitespace; keep Devanagari tokens and ASCII
  return q.split(" ").filter(Boolean);
}

function prefixN(s, n) {
  const t = String(s ?? "");
  if (!t) return "";
  return t.length <= n ? t : t.slice(0, n);
}

// ---------------- Exact index normalization ----------------
// exact_prefix_2 index uses a vowel-bucket + matra folding for OCR-ish exactness
const INDEP_VOWEL_MAP = new Map([
  ["अ", "A"],
  ["आ", "A"],
  ["इ", "I"],
  ["ई", "I"],
  ["उ", "U"],
  ["ऊ", "U"],
  ["ए", "E"],
  ["ऐ", "E"],
  ["ओ", "O"],
  ["औ", "O"],
  ["ऋ", "R"],
]);

const MATRA_MAP = new Map([
  ["ा", "A"],
  ["ि", "I"],
  ["ी", "I"],
  ["ु", "U"],
  ["ू", "U"],
  ["े", "E"],
  ["ै", "E"],
  ["ो", "O"],
  ["ौ", "O"],
  ["ृ", "R"],
]);

const REMOVE_MARKS = /[ँंः़्]/g;

// Fold a single token into an "exact index" representation.
// Goal: handle common OCR vowel/matra confusions while staying fairly strict.
function normExactIndexToken(tok) {
  const s = String(tok || "");
  if (!s) return "";

  let out = "";
  for (let i = 0; i < s.length; i++) {
    const ch = s[i];
    if (REMOVE_MARKS.test(ch)) continue;

    // independent vowels -> buckets
    if (INDEP_VOWEL_MAP.has(ch)) {
      out += INDEP_VOWEL_MAP.get(ch);
      continue;
    }
    // matras -> buckets
    if (MATRA_MAP.has(ch)) {
      out += MATRA_MAP.get(ch);
      continue;
    }

    out += ch;
  }
  return out;
}

function tokenizeExactIndex(s) {
  const toks = tokenize(s);
  return toks.map(normExactIndexToken).filter(Boolean);
}

// ---------------- Loose normalization ----------------
// loose_prefix_2 index uses visually/phonetically confusable folding.
// This is intentionally broad (for maximum recall when "include typing mistakes" is ON).
const CONFUSABLE_SETS = [
  // Common OCR/visual confusions
  ["क", "र", "ख"],
  ["स", "श"],
  ["द", "ध", "ढ"],
  ["त", "ट", "थ", "ठ"],
  ["व", "ब"],
  ["प", "फ"],
  ["ग", "घ"],
  ["ज", "झ"],
  ["इ", "ई"],
  ["उ", "ऊ"],
  ["अ", "आ"],
  ["ए", "ऐ"],
  ["ओ", "औ"],
];

const CONF_MAP = (() => {
  const m = new Map();
  // Assign stable bucket letters for each group
  // (must match how your DB was built)
  const buckets = "abcdefghijklmnopqrstuvwxyz";
  let bi = 0;
  for (const group of CONFUSABLE_SETS) {
    const b = buckets[bi++] || "z";
    for (const ch of group) m.set(ch, b);
  }
  return m;
})();

function applyConfusableFoldLoose(s) {
  const inStr = String(s || "");
  if (!inStr) return "";

  let out = "";
  for (let i = 0; i < inStr.length; i++) {
    const ch = inStr[i];
    if (REMOVE_MARKS.test(ch)) continue;

    // vowels/matras still bucketed
    if (INDEP_VOWEL_MAP.has(ch)) {
      out += INDEP_VOWEL_MAP.get(ch);
      continue;
    }
    if (MATRA_MAP.has(ch)) {
      out += MATRA_MAP.get(ch);
      continue;
    }

    out += CONF_MAP.get(ch) ?? ch;
  }
  return out;
}

function tokenizeLoose(s) {
  const toks = tokenize(s);
  return toks.map(applyConfusableFoldLoose).filter(Boolean);
}

// ---------------- Join variants ----------------
// For multi-part names, we also generate concatenated variants (no spaces).
function joinVariantsTokens(tokens) {
  const toks = Array.isArray(tokens) ? tokens.filter(Boolean) : [];
  if (toks.length <= 1) return [];

  const joined = [];

  // full join (all tokens)
  joined.push(toks.join(""));

  // progressive joins: 2..N tokens from the start
  for (let i = 2; i <= toks.length; i++) {
    joined.push(toks.slice(0, i).join(""));
  }

  // adjacent pair joins
  for (let i = 0; i < toks.length - 1; i++) {
    joined.push(toks[i] + toks[i + 1]);
  }

  // de-dup
  return Array.from(new Set(joined)).filter(Boolean);
}

function buildKeysFromTokens(tokens, prefixLen) {
  const keys = new Set();

  for (const tok of tokens) {
    const p = prefixN(tok, prefixLen);
    if (p) keys.add(p);
  }

  // join variants
  for (const j of joinVariantsTokens(tokens)) {
    const p = prefixN(j, prefixLen);
    if (p) keys.add(p);
  }

  return Array.from(keys);
}

// ---------- Candidates + Rows (server) ----------
async function getCandidatesForQuery(q, scope, exactOn, signal) {
  const strictTokens = tokenize(q);
  const strictKeys = buildKeysFromTokens(strictTokens, PREFIX_LEN_STRICT);

  const exactTokens = tokenizeExactIndex(q);
  const exactKeys = buildKeysFromTokens(exactTokens, PREFIX_LEN_EXACT);

  const wantLoose = !exactOn;
  const looseTokens = wantLoose ? tokenizeLoose(q) : [];
  const looseKeys = wantLoose ? buildKeysFromTokens(looseTokens, PREFIX_LEN_LOOSE) : [];

  const districtId = getCurrentDistrictSlug();
  const ac = Number(current.ac);

  if (!districtId || !Number.isFinite(ac)) {
    return { candidates: [], metaByRow: new Map() };
  }

  const json = await callFn(
    "candidates",
    {
      state: STATE_CODE_DEFAULT,
      districtId,
      district: districtId,
      ac,
      scope,
      exactOn,
      strictKeys,
      exactKeys,
      looseKeys,
    },
    signal
  );

  const candidates = Array.isArray(json.candidates) ? json.candidates.map(Number).filter(Number.isFinite) : [];

  const metaByRow = new Map();
  if (json.metaByRow && typeof json.metaByRow === "object") {
    for (const [k, v] of Object.entries(json.metaByRow)) {
      const rid = Number(k);
      if (Number.isFinite(rid)) metaByRow.set(rid, v);
    }
  }

  return { candidates, metaByRow };
}

async function fetchRowsByIds(rowIds, signal) {
  const districtId = getCurrentDistrictSlug();
  const ac = Number(current.ac);
  if (!districtId || !Number.isFinite(ac) || !rowIds?.length) return [];

  const ids = rowIds.map(Number).filter(Number.isFinite);
  const out = [];

  for (let i = 0; i < ids.length; i += FETCH_ID_CHUNK) {
    const chunk = ids.slice(i, i + FETCH_ID_CHUNK);

    const json = await callFn(
      "rows",
      {
        state: STATE_CODE_DEFAULT,
        districtId,
        district: districtId,
        ac,
        mode: "score",
        rowIds: chunk,
      },
      signal
    );

    const rows = Array.isArray(json.rows) ? json.rows : [];
    for (const r of rows) cacheScoreRow(ac, r);

    for (const r of rows) {
      out.push({
        row_id: Number(r.row_id),
        voter_name_raw: r.voter_name_raw ?? "",
        relative_name_raw: r.relative_name_raw ?? "",
        voter_name_norm: r.voter_name_norm ?? "",
        relative_name_norm: r.relative_name_norm ?? "",
        serial_no: r.serial_no ?? "",
      });
    }
  }

  return out;
}

async function fetchDisplayRowsByIds(rowIds, signal) {
  const districtId = getCurrentDistrictSlug();
  const ac = Number(current.ac);
  if (!districtId || !Number.isFinite(ac) || !rowIds?.length) return [];

  const ids = rowIds.map(Number).filter(Number.isFinite);
  const outRows = [];

  for (let i = 0; i < ids.length; i += FETCH_ID_CHUNK) {
    const chunk = ids.slice(i, i + FETCH_ID_CHUNK);

    const json = await callFn(
      "rows",
      {
        state: STATE_CODE_DEFAULT,
        districtId,
        district: districtId,
        ac,
        mode: "display",
        rowIds: chunk,
      },
      signal
    );

    const rows = Array.isArray(json.rows) ? json.rows : [];
    for (const r of rows) outRows.push(r);
  }

  return outRows.map((r) => {
    const out = {};
    out.row_id = Number(r.row_id);
    out["State Code"] = r["State Code"] ?? r.state_code ?? STATE_CODE_DEFAULT;
    out["AC No"] = r["AC No"] ?? r.ac_no ?? String(ac);
    for (const k of DISPLAY_COLS) out[k] = r[k] ?? "";
    return out;
  });
}

// ---------- Worker ranking ----------
let worker;
let pendingResolve = null;
let pendingReject = null;

function initWorker() {
  if (worker) return;
  worker = new Worker(new URL("./worker.js", import.meta.url), { type: "module" });

  worker.onmessage = (ev) => {
    const msg = ev.data || {};
    if (msg.type === "progress") {
      if (msg.total > 0) {
        setBar((100 * msg.done) / msg.total);
      }
      const ac = current.ac != null ? String(current.ac).padStart(2, "0") : "";
      setStatus(t("status_ranking", { done: msg.done, total: msg.total, ac }));
      return;
    }

    if (msg.type === "done") {
      const ranked = (msg.ranked || []).map((x, i) => ({
        row_id: x.row_id,
        score:
          typeof x.score === "number"
            ? x.score
            : typeof x.rank === "number"
            ? 1000000 - x.rank
            : 1000000 - i,
      }));
      const resolve = pendingResolve;
      pendingResolve = null;
      pendingReject = null;
      if (resolve) resolve(ranked);
      return;
    }

    if (msg.type === "error") {
      const reject = pendingReject;
      pendingResolve = null;
      pendingReject = null;
      if (reject) reject(new Error(msg.message || "Worker error"));
      return;
    }
  };

  worker.onerror = (e) => {
    const reject = pendingReject;
    pendingResolve = null;
    pendingReject = null;
    if (reject) reject(new Error(e?.message || "Worker crashed"));
  };
}

function runWorkerRanking(rowsWithMeta, query, exactOn, scope) {
  initWorker();
  return new Promise((resolve, reject) => {
    pendingResolve = resolve;
    pendingReject = reject;
    worker.postMessage({ type: "start", query: String(query || ""), exactOn: !!exactOn, scope: String(scope || "voter") });
    for (let i = 0; i < rowsWithMeta.length; i += SCORE_BATCH) {
      worker.postMessage({ type: "batch", rows: rowsWithMeta.slice(i, i + SCORE_BATCH) });
    }
    worker.postMessage({ type: "finish" });
  });
}

// ---------- Chips ----------
function setScopeUI(scope) {
  for (const el of scopeTabs) {
    const s = el.getAttribute("data-scope");
    el.classList.toggle("active", s === scope);
  }
}

function setScope(scope) {
  searchScope = scope;
  setScopeUI(scope);
}

function setLanguage(lang) {
  i18n.setLang(lang);
  // update UI labels/text
  document.documentElement.lang = lang === LANG.EN ? "en" : "hi";
  renderStaticText();
  renderDistrictUI();
  renderResultsUI();
}

function renderStaticText() {
  // landing
  $("titleText").textContent = t("title");
  $("subtitleText").textContent = t("subtitle");

  // buttons
  if (searchBtnLanding) searchBtnLanding.textContent = t("search");
  if (searchBtnResults) searchBtnResults.textContent = t("search");

  // labels
  $("districtLabel").textContent = t("district");
  $("acLabel").textContent = t("ac");

  if (includeTypingLanding) $("includeTypingLabelLanding").textContent = t("include_typos");
  if (includeTypingResults) $("includeTypingLabelResults").textContent = t("include_typos");

  // filters
  $("filtersTitle").textContent = t("filters");
  $("filterGenderLabel").textContent = t("gender");
  $("filterAgeLabel").textContent = t("age");
  $("filterQueryLabel").textContent = t("filter_query");
  $("sortLabel").textContent = t("sort");
  $("pageSizeLabel").textContent = t("page_size");

  // back button
  if (backBtn) backBtn.textContent = t("back");
}

function renderDistrictUI() {
  // placeholder option text depends on i18n; rebuild list
  populateDistrictSelect();
  if (districtSelect) districtSelect.value = current.districtId || "";

  const did = getCurrentDistrictSlug();
  if (did) populateACSelectForDistrict(did);

  syncSearchButtonState();
}

function renderResultsUI() {
  // scope tabs labels
  const tabVoter = $("tabVoter");
  const tabRelative = $("tabRelative");
  const tabAnywhere = $("tabAnywhere");
  if (tabVoter) tabVoter.textContent = t("scope_voter");
  if (tabRelative) tabRelative.textContent = t("scope_relative");
  if (tabAnywhere) tabAnywhere.textContent = t("scope_anywhere");

  // if already in results, rerender table headers
  if (results.style.display !== "none") renderTable();
}

// ---------- Filters + sorting ----------
function parseNum(x) {
  const n = Number(String(x ?? "").trim());
  return Number.isFinite(n) ? n : null;
}

function applyFilters() {
  let base = rankedByRelevance.slice();

  // gender
  const g = (filterGender?.value || "").trim();
  if (g) {
    base = base.filter((it) => {
      const r = getScoreRow(it.ac, it.row_id);
      const rg = String(r?.gender || "").trim();
      return rg === g;
    });
  }

  // age
  const minA = parseNum(filterMinAge?.value);
  const maxA = parseNum(filterMaxAge?.value);
  if (minA != null || maxA != null) {
    base = base.filter((it) => {
      const r = getScoreRow(it.ac, it.row_id);
      const a = parseNum(r?.age);
      if (a == null) return false;
      if (minA != null && a < minA) return false;
      if (maxA != null && a > maxA) return false;
      return true;
    });
  }

  // local filter query (client-side)
  const fq = norm(filterQuery?.value || "");
  if (fq) {
    const fqLower = fq.toLowerCase();
    base = base.filter((it) => {
      const r = getScoreRow(it.ac, it.row_id);
      const vn = String(r?.voter_name_raw || "").toLowerCase();
      const rn = String(r?.relative_name_raw || "").toLowerCase();
      return vn.includes(fqLower) || rn.includes(fqLower);
    });
  }

  filteredBase = base;
}

function applySort() {
  const mode = sortSelect?.value || "relevance";
  const arr = filteredBase.slice();

  if (mode === "serial") {
    arr.sort((a, b) => {
      const ra = getScoreRow(a.ac, a.row_id);
      const rb = getScoreRow(b.ac, b.row_id);
      const sa = parseNum(ra?.serial_no) ?? 0;
      const sb = parseNum(rb?.serial_no) ?? 0;
      if (sa !== sb) return sa - sb;
      if (a.ac !== b.ac) return a.ac - b.ac;
      return a.row_id - b.row_id;
    });
  } else if (mode === "age") {
    arr.sort((a, b) => {
      const ra = getScoreRow(a.ac, a.row_id);
      const rb = getScoreRow(b.ac, b.row_id);
      const aa = parseNum(ra?.age) ?? 0;
      const ab = parseNum(rb?.age) ?? 0;
      if (aa !== ab) return aa - ab;
      if (a.ac !== b.ac) return a.ac - b.ac;
      return a.row_id - b.row_id;
    });
  } else {
    // relevance (already sorted)
    // keep stable ordering
    arr.sort((a, b) => (b.score !== a.score ? b.score - a.score : a.ac !== b.ac ? a.ac - b.ac : a.row_id - b.row_id));
  }

  rankedView = arr;
}

async function applyFiltersThenSortThenRender() {
  applyFilters();
  applySort();
  page = 1;
  await renderPage();
}

// ---------- Table rendering ----------
function getPageSizeOptions() {
  return isMobile() ? PAGE_SIZE_MOBILE_OPTIONS : PAGE_SIZE_DESKTOP_OPTIONS;
}

function populatePageSizeSelect() {
  if (!pageSizeSelect) return;
  pageSizeSelect.innerHTML = "";
  const opts = getPageSizeOptions();

  for (const n of opts) {
    const opt = document.createElement("option");
    opt.value = String(n);
    opt.textContent = String(n);
    pageSizeSelect.appendChild(opt);
  }
  // keep current if possible
  if (!opts.includes(pageSize)) pageSize = opts[opts.length - 1];
  pageSizeSelect.value = String(pageSize);
}

function renderTableHead() {
  if (!tableHead) return;
  const cols = ["AC No", ...DISPLAY_COLS];
  tableHead.innerHTML = `<tr>${cols
    .map((c) => `<th>${escapeHtml(i18n.header(c))}</th>`)
    .join("")}</tr>`;
}

function renderTableBody(rows) {
  if (!tableBody) return;
  tableBody.innerHTML = rows
    .map((r) => {
      const ac = r["AC No"] ?? "";
      const tds = [`<td>${escapeHtml(ac)}</td>`].concat(DISPLAY_COLS.map((c) => `<td>${escapeHtml(r[c] ?? "")}</td>`));
      return `<tr>${tds.join("")}</tr>`;
    })
    .join("");
}

async function renderPage() {
  renderTableHead();
  if (!pagerInfo) return;

  const total = rankedView.length;
  const totalPages = Math.max(1, Math.ceil(total / pageSize));
  page = Math.min(page, totalPages);

  const start = (page - 1) * pageSize;
  const end = Math.min(total, start + pageSize);

  pagerInfo.textContent = t("pager_info", { page, totalPages, total });

  if (pagerPrev) pagerPrev.disabled = page <= 1;
  if (pagerNext) pagerNext.disabled = page >= totalPages;

  const slice = rankedView.slice(start, end);

  // Fetch display rows for this page (group by AC)
  const missingByAc = new Map();
  for (const it of slice) {
    const key = makeKey(it.ac, it.row_id);
    if (!displayCache.has(key)) {
      if (!missingByAc.has(it.ac)) missingByAc.set(it.ac, []);
      missingByAc.get(it.ac).push(it.row_id);
    }
  }

  for (const [ac, ids] of missingByAc.entries()) {
    current.ac = ac;
    const rows = await fetchDisplayRowsByIds(ids);
    for (const r of rows) {
      const key = makeKey(ac, r.row_id);
      displayCache.set(key, r);
    }
  }

  const displayRows = slice
    .map((it) => displayCache.get(makeKey(it.ac, it.row_id)))
    .filter(Boolean);

  renderTableBody(displayRows);
}

function renderTable() {
  renderTableHead();
  // body will be rendered in renderPage()
}

// ---------- Main search ----------
async function runSearch() {
  // Cancel any previous in-flight search (network + worker)
  cancelInFlightSearch();

  // After cancelInFlightSearch(), searchRunToken has been incremented.
  const runToken = searchRunToken;

  searchAbortCtrl = new AbortController();
  const signal = searchAbortCtrl.signal;

  searchInFlight = true;
  syncSearchButtonState();

  try {
    const qEl = getActiveQueryInput();
    const q = norm(qEl.value);
    if (!q) {
      setStatus(t("status_enter_query"));
      return;
    }

    const districtId = getCurrentDistrictSlug();
    if (!districtId) {
      setStatus(t("status_select_district"));
      return;
    }

    const acs = getActiveACs();
    if (!acs.length) {
      setStatus(t("status_no_acs_selected"));
      return;
    }

    rankedByRelevance = [];
    filteredBase = [];
    rankedView = [];
    displayCache.clear();
    scoreCache.clear();
    page = 1;

    showResults();
    setBar(0);

    const exactOn = exactOnFromIncludeTyping();
    const scopeForWorker = searchScope; // voter | relative | anywhere

    let merged = [];

    for (let i = 0; i < acs.length; i++) {
      const ac = acs[i];
      current.ac = ac;

      setStatus(t("status_stage1_loose", { ac }));

      const { candidates, metaByRow } = await getCandidatesForQuery(q, searchScope, exactOn, signal);
      if (runToken !== searchRunToken) return;

      if (!candidates.length) continue;

      // Fetch score rows in chunks to avoid Netlify/Turso timeouts and large payloads
      const rows = await fetchRowsByIds(candidates, signal);
      if (runToken !== searchRunToken) return;

      const rowsWithMeta = rows.map((r) => ({ ...r, _meta: metaByRow.get(r.row_id) || null }));

      const ranked = await runWorkerRanking(rowsWithMeta, q, exactOn, scopeForWorker);
      if (runToken !== searchRunToken) return;

      for (const r of ranked) merged.push({ key: makeKey(ac, r.row_id), ac, row_id: r.row_id, score: r.score });
    }

    // Stable ordering: score desc, then AC asc, then row_id asc
    rankedByRelevance = merged.sort((a, b) =>
      b.score !== a.score ? b.score - a.score : a.ac !== b.ac ? a.ac - b.ac : a.row_id - b.row_id
    );

    await applyFiltersThenSortThenRender();
    setStatus(t("status_ready_results", { n: rankedView.length }));
  } catch (e) {
    if (e && (e.name === "AbortError" || String(e.message || "").includes("aborted"))) return;
    console.error(e);
    setStatus(String(e && e.message ? e.message : e));
  } finally {
    // Only clear if this is still the latest run
    if (runToken === searchRunToken) {
      searchInFlight = false;
      searchAbortCtrl = null;
      syncSearchButtonState();
    }
  }
}

// ---------- Events ----------
function wireEvents() {
  // scopes
  for (const el of scopeTabs) {
    el.addEventListener("click", () => {
      const s = el.getAttribute("data-scope");
      if (s) setScope(s);
    });
  }
  setScopeUI(searchScope);

  // language
  if (langBtnHi) langBtnHi.onclick = () => setLanguage(LANG.HI);
  if (langBtnHinglish) langBtnHinglish.onclick = () => setLanguage(LANG.HINGLISH);
  if (langBtnEn) langBtnEn.onclick = () => setLanguage(LANG.EN);

  // district change
  if (districtSelect) {
    districtSelect.addEventListener("change", () => {
      const did = getCurrentDistrictSlug();
      current.districtId = did;
      populateACSelectForDistrict(did);

      // landing input enabled only after district
      syncSearchButtonState();

      // Clear results state
      rankedByRelevance = [];
      rankedView = [];
      filteredBase = [];
      displayCache.clear();
      scoreCache.clear();
      setBar(null);
      setStatus("");
      showLanding();
    });
  }

  // page size
  if (pageSizeSelect) {
    pageSizeSelect.addEventListener("change", async () => {
      pageSize = Number(pageSizeSelect.value) || pageSize;
      page = 1;
      await renderPage();
    });
  }

  // pager
  if (pagerPrev) {
    pagerPrev.addEventListener("click", async () => {
      page = Math.max(1, page - 1);
      await renderPage();
    });
  }
  if (pagerNext) {
    pagerNext.addEventListener("click", async () => {
      page = page + 1;
      await renderPage();
    });
  }

  // filters
  const onFilter = async () => {
    await applyFiltersThenSortThenRender();
  };
  if (filterGender) filterGender.addEventListener("change", onFilter);
  if (filterMinAge) filterMinAge.addEventListener("input", onFilter);
  if (filterMaxAge) filterMaxAge.addEventListener("input", onFilter);
  if (filterQuery) filterQuery.addEventListener("input", onFilter);
  if (sortSelect) sortSelect.addEventListener("change", onFilter);

  // search buttons
  if (searchBtnLanding) searchBtnLanding.onclick = () => runSearch();
  if (searchBtnResults) searchBtnResults.onclick = () => runSearch();

  // enter key
  if (qLanding) {
    qLanding.addEventListener("keydown", (e) => {
      if (e.key === "Enter") runSearch();
    });
  }
  if (qResults) {
    qResults.addEventListener("keydown", (e) => {
      if (e.key === "Enter") runSearch();
    });
  }

  // back
  if (backBtn) {
    backBtn.addEventListener("click", () => {
      setBar(null);
      showLanding();
    });
  }

  // responsive page size options
  window.addEventListener("resize", () => {
    populatePageSizeSelect();
  });
}

// ---------- Boot ----------
async function boot() {
  try {
    setStatus(t("status_loading_manifest"));
    districtLoading = true;
    syncSearchButtonState();

    // init i18n
    if (typeof i18n.loadSavedLanguageOrDefault === "function") {
      await i18n.loadSavedLanguageOrDefault();
    }

    renderStaticText();

    await loadManifest();
    populateDistrictSelect();

    populatePageSizeSelect();
    renderTableHead();

    districtLoading = false;
    searchEnabled = true;
    syncSearchButtonState();

    setStatus(t("status_ready"));
  } catch (e) {
    console.error(e);
    setStatus(String(e && e.message ? e.message : e));
  }
}

wireEvents();
boot();
