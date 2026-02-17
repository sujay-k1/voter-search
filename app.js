// app.js (ES module)
// Language switch moved to i18n.js (strings + translation runtime).
// No search logic changed; only i18n block extracted. 
//temp comment

import * as duckdb from "./duckdb/duckdb-browser.mjs";
import { LANG, createI18n } from "./i18n.js";

/**
 * IMPORTANT (hosting under subfolder like /voter-search/):
 * Never use absolute paths like "/data/..." or "/duckdb/...".
 * Always resolve relative to the folder that contains this app.
 */
const APP_BASE = new URL("./", window.location.href); // e.g. https://sujaykumar.net/voter-search/
const relUrl = (p) => new URL(String(p).replace(/^\/+/, ""), APP_BASE).toString();

// Debug mode: add ?debug=1 (or ?debug=true) to URL
const DEBUG = (() => {
  const v = new URLSearchParams(window.location.search).get("debug");
  if (v == null) return false;
  if (v === "" || v === "1" || v === "true" || v === "yes") return true;
  return false;
})();

// ---------------- Netlify Functions (Turso backend) ----------------
// Functions live at: https://<site>/.netlify/functions/<name>
// NOTE: use window.location.origin (NOT APP_BASE) because functions are always rooted at origin.
const FN_BASE = new URL("/.netlify/functions/", window.location.origin);
const fnUrl = (name) => new URL(String(name).replace(/^\/+/, ""), FN_BASE).toString();

function slugifyDistrictId(id) {
  return String(id ?? "")
    .trim()
    .toLowerCase()
    .replace(/[_\s]+/g, "-")
    .replace(/[^a-z0-9-]/g, "-")
    .replace(/-+/g, "-")
    .replace(/^-+|-+$/g, "");
}

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

function isRetryableFetchError(e) {
  const msg = String(e?.message || e || "").toLowerCase();
  // network hiccups / abort / transient gateways
  return (
    msg.includes("abort") ||
    msg.includes("network") ||
    msg.includes("failed to fetch") ||
    msg.includes("timeout") ||
    msg.includes("gateway") ||
    msg.includes("fetch")
  );
}

async function postJson(url, data, { timeoutMs = 60000, retries = 0, retryDelayMs = 200 } = {}) {
  let attempt = 0;
  // eslint-disable-next-line no-constant-condition
  while (true) {
    attempt++;
    const ctrl = new AbortController();
    const t = setTimeout(() => ctrl.abort(), timeoutMs);
    try {
      const resp = await fetch(url, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify(data ?? {}),
        signal: ctrl.signal,
      });
      const text = await resp.text();
      let json = null;
      try {
        json = text ? JSON.parse(text) : null;
      } catch {
        json = null;
      }

      if (!resp.ok) {
        const msg = (json && (json.error || json.message)) || text || `HTTP ${resp.status}`;
        // Retry 502/503/504 once if configured.
        if (retries > 0 && (resp.status === 502 || resp.status === 503 || resp.status === 504)) {
          retries--;
          await sleep(retryDelayMs);
          retryDelayMs = Math.min(1000, retryDelayMs * 2);
          continue;
        }
        throw new Error(msg);
      }
      return json;
    } catch (e) {
      if (retries > 0 && isRetryableFetchError(e)) {
        retries--;
        await sleep(retryDelayMs);
        retryDelayMs = Math.min(1000, retryDelayMs * 2);
        continue;
      }
      throw e;
    } finally {
      clearTimeout(t);
    }
  }
}

async function callFn(name, payload, opts) {
  return await postJson(fnUrl(name), payload, opts);
}

// Simple async pool to limit concurrency
async function runPool(items, concurrency, workerFn) {
  const out = new Array(items.length);
  let idx = 0;
  const n = Math.max(1, Math.min(concurrency || 1, items.length || 1));
  async function runner() {
    while (true) {
      const i = idx++;
      if (i >= items.length) return;
      out[i] = await workerFn(items[i], i);
    }
  }
  const ps = [];
  for (let i = 0; i < n; i++) ps.push(runner());
  await Promise.all(ps);
  return out;
}

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

// Larger chunks reduce round-trips to rows() on big result sets.
// (Still small enough to keep request/response sizes safe.)
const FETCH_ID_CHUNK = 8000;
const SCORE_BATCH = 2000;

// Concurrency knobs (tune for speed vs backend load)
const MAX_PREP_IN_FLIGHT_AC = 4;      // candidates+score-rows fetch pipeline
const MAX_DISPLAY_IN_FLIGHT_AC = 4;   // display rows fetch during paging
const MAX_FILTER_IN_FLIGHT_AC = 4;    // relative candidates + gender/age fetch during filtering

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

const STICKY_COL_KEY = "Voter Name";

// search scope state
const SCOPE = {
  VOTER: "voter",
  RELATIVE: "relative",
  ANYWHERE: "anywhere",
};

let searchScope = SCOPE.VOTER;

let db, conn;

// district + AC selection state
let districtManifest = null;
let currentDistrictId = "";
let currentDistrictLabel = "";
let districtACsAll = []; // all ACs for district
let selectedACs = new Set(); // subset selected (empty => all)

// per-AC loaded state (views point to current AC)
let current = {
  state: STATE_CODE_DEFAULT,
  ac: null,
  meta: null,
  loaded: false, // "AC loaded" (views ready)
  lastQuery: "",
};

// Ranking results use composite key (ac:row_id) so row_id collisions across ACs are safe
// Now uses `sortKey` (array) from worker — NO arbitrary numeric score.
let rankedByRelevance = []; // full [{key, ac, row_id, sortKey, explain?, match_field?}]
let masterRankedAll = []; // last computed full results (AC filter uses this)
let lastSearchCtx = { districtSlug: "", query: "", scope: "", exactOn: false, searchedACs: new Set() };
let filteredBase = []; // after filters, in relevance order
let rankedView = []; // after sort, for paging
let page = 1;

let pageSize = PAGE_SIZE_DESKTOP_DEFAULT;

// Used to cancel district-preload if user switches district quickly
let districtPreloadToken = 0;

let ageMap = null; // Map(key -> ageNumber|null)
let displayCache = new Map(); // Map(key -> rowObject)

// Speed caches (persist across filter/sort changes; cleared on new search/district)
let scoreRowCache = new Map(); // Map(key -> {row_id, voter_name_norm, relative_name_norm})
let genderAgeCache = new Map(); // Map(key -> { age:Number|null, gender:String })
let relativeCandidatesCache = new Map(); // Map(cacheKey -> Set(row_id))

// Used to cancel an in-flight search when user starts a new one.
let activeSearchToken = 0;

// Gender domain discovery per loaded AC (used only when filtering within that AC)
let genderBuckets = { male: new Set(), female: new Set(), other: new Set() };

// Sort mode (popover)
const SORT = {
  RELEVANCE: "relevance",
  AGE_ASC: "age_asc",
  AGE_DESC: "age_desc",
};
let sortMode = SORT.RELEVANCE;

// District popover search
let districtQuery = "";
function getDistrictById(id) {
  if (!id) return null;
  return (districtManifest?.districts || []).find((x) => x.id === id) || null;
}

function getActiveLang() {
  try {
    return i18n?.getLang?.() || LANG.HI;
  } catch {
    return LANG.HI;
  }
}

function getDistrictLabelForLang(d, lang) {
  if (!d) return "";
  const base = String(d.label ?? d.id ?? "").trim();

  const labels = d && typeof d.labels === "object" && d.labels ? d.labels : null;
  if (!lang) lang = getActiveLang();

  if (labels) {
    if (lang === LANG.EN) return String(labels.en ?? labels.english ?? base).trim() || base;
    if (lang === LANG.HINGLISH) return String(labels.hinglish ?? labels.en ?? base).trim() || base;
    return String(labels.hi ?? labels.hindi ?? base).trim() || base;
  }

  // legacy per-field labels (optional)
  if (lang === LANG.EN) return String(d.label_en ?? d.label ?? d.name ?? d.id ?? "").trim() || base;
  if (lang === LANG.HINGLISH)
    return String(d.label_hinglish ?? d.label_en ?? d.label ?? d.name ?? d.id ?? "").trim() || base;
  return String(d.label_hi ?? d.label_hindi ?? d.label ?? d.name ?? d.id ?? "").trim() || base;
}

function getDistrictLabelCurrent() {
  const d = getDistrictById(currentDistrictId);
  if (!d) return "";
  return getDistrictLabelForLang(d, getActiveLang());
}

// Jharkhand AC labels by AC number (fallback when manifest does not provide AC names).
const FALLBACK_AC_LABELS = {
  1: { en: "Rajmahal", hi: "राजमहल", hinglish: "Rajmahal" },
  2: { en: "Borio", hi: "बोरियो", hinglish: "Borio" },
  3: { en: "Barhait", hi: "बरहेट", hinglish: "Barhait" },
  4: { en: "Litipara", hi: "लिट्टीपाड़ा", hinglish: "Litipara" },
  5: { en: "Pakur", hi: "पाकुड़", hinglish: "Pakur" },
  6: { en: "Maheshpur", hi: "महेशपुर", hinglish: "Maheshpur" },
  7: { en: "Sikaripara", hi: "शिकारीपाड़ा", hinglish: "Sikaripara" },
  8: { en: "Nala", hi: "नाला", hinglish: "Nala" },
  9: { en: "Jamtara", hi: "जामताड़ा", hinglish: "Jamtara" },
  10: { en: "Dumka", hi: "दुमका", hinglish: "Dumka" },
  11: { en: "Jama", hi: "जामा", hinglish: "Jama" },
  12: { en: "Jarmundi", hi: "जरमुंडी", hinglish: "Jarmundi" },
  13: { en: "Madhupur", hi: "मधुपुर", hinglish: "Madhupur" },
  14: { en: "Sarath", hi: "सारठ", hinglish: "Sarath" },
  15: { en: "Deoghar", hi: "देवघर", hinglish: "Deoghar" },
  16: { en: "Poreyahat", hi: "पोरेयाहाट", hinglish: "Poreyahat" },
  17: { en: "Godda", hi: "गोड्डा", hinglish: "Godda" },
  18: { en: "Mahagama", hi: "महगामा", hinglish: "Mahagama" },
  19: { en: "Koderma", hi: "कोडरमा", hinglish: "Koderma" },
  20: { en: "Barkatha", hi: "बरकट्ठा", hinglish: "Barkatha" },
  21: { en: "Barhi", hi: "बरही", hinglish: "Barhi" },
  22: { en: "Barkagaon", hi: "बड़कागांव", hinglish: "Barkagaon" },
  23: { en: "Ramgarh", hi: "रामगढ़", hinglish: "Ramgarh" },
  24: { en: "Mandu", hi: "मांडू", hinglish: "Mandu" },
  25: { en: "Hazaribagh", hi: "हजारीबाग", hinglish: "Hazaribagh" },
  26: { en: "Simaria", hi: "सिमरिया", hinglish: "Simaria" },
  27: { en: "Chatra", hi: "चतरा", hinglish: "Chatra" },
  28: { en: "Dhanwar", hi: "धनवार", hinglish: "Dhanwar" },
  29: { en: "Bagodar", hi: "बगोदर", hinglish: "Bagodar" },
  30: { en: "Jamua", hi: "जमुआ", hinglish: "Jamua" },
  31: { en: "Gandey", hi: "गांडेय", hinglish: "Gandey" },
  32: { en: "Giridih", hi: "गिरिडीह", hinglish: "Giridih" },
  33: { en: "Dumri", hi: "डुमरी", hinglish: "Dumri" },
  34: { en: "Gomia", hi: "गोमिया", hinglish: "Gomia" },
  35: { en: "Bermo", hi: "बेरमो", hinglish: "Bermo" },
  36: { en: "Bokaro", hi: "बोकारो", hinglish: "Bokaro" },
  37: { en: "Chandankiyari", hi: "चंदनकियारी", hinglish: "Chandankiyari" },
  38: { en: "Sindri", hi: "सिंदरी", hinglish: "Sindri" },
  39: { en: "Nirsa", hi: "निरसा", hinglish: "Nirsa" },
  40: { en: "Dhanbad", hi: "धनबाद", hinglish: "Dhanbad" },
  41: { en: "Jharia", hi: "झरिया", hinglish: "Jharia" },
  42: { en: "Tundi", hi: "टुंडी", hinglish: "Tundi" },
  43: { en: "Baghmara", hi: "बाघमारा", hinglish: "Baghmara" },
  44: { en: "Baharagora", hi: "बहरागोड़ा", hinglish: "Baharagora" },
  45: { en: "Ghatsila", hi: "घाटशिला", hinglish: "Ghatsila" },
  46: { en: "Potka", hi: "पोटका", hinglish: "Potka" },
  47: { en: "Jugsalai", hi: "जुगसलाई", hinglish: "Jugsalai" },
  48: { en: "Jamshedpur East", hi: "जमशेदपुर पूर्व", hinglish: "Jamshedpur East" },
  49: { en: "Jamshedpur West", hi: "जमशेदपुर पश्चिम", hinglish: "Jamshedpur West" },
  50: { en: "Ichagarh", hi: "ईचागढ़", hinglish: "Ichagarh" },
  51: { en: "Seraikella", hi: "सरायकेला", hinglish: "Seraikella" },
  52: { en: "Chaibasa", hi: "चाईबासा", hinglish: "Chaibasa" },
  53: { en: "Majhgaon", hi: "मझगांव", hinglish: "Majhgaon" },
  54: { en: "Jagannathpur", hi: "जगन्नाथपुर", hinglish: "Jagannathpur" },
  55: { en: "Manoharpur", hi: "मनोहरपुर", hinglish: "Manoharpur" },
  56: { en: "Chakradharpur", hi: "चक्रधरपुर", hinglish: "Chakradharpur" },
  57: { en: "Kharsawan", hi: "खरसावां", hinglish: "Kharsawan" },
  58: { en: "Tamar", hi: "तमाड़", hinglish: "Tamar" },
  59: { en: "Torpa", hi: "तोरपा", hinglish: "Torpa" },
  60: { en: "Khunti", hi: "खूंटी", hinglish: "Khunti" },
  61: { en: "Silli", hi: "सिल्ली", hinglish: "Silli" },
  62: { en: "Khijri", hi: "खिजरी", hinglish: "Khijri" },
  63: { en: "Ranchi", hi: "रांची", hinglish: "Ranchi" },
  64: { en: "Hatia", hi: "हटिया", hinglish: "Hatia" },
  65: { en: "Kanke", hi: "कांके", hinglish: "Kanke" },
  66: { en: "Mandar", hi: "मांडर", hinglish: "Mandar" },
  67: { en: "Sisai", hi: "सिसई", hinglish: "Sisai" },
  68: { en: "Gumla", hi: "गुमला", hinglish: "Gumla" },
  69: { en: "Bishunpur", hi: "बिशुनपुर", hinglish: "Bishunpur" },
  70: { en: "Simdega", hi: "सिमडेगा", hinglish: "Simdega" },
  71: { en: "Kolebira", hi: "कोलेबिरा", hinglish: "Kolebira" },
  72: { en: "Lohardaga", hi: "लोहरदगा", hinglish: "Lohardaga" },
  73: { en: "Manika", hi: "मनिका", hinglish: "Manika" },
  74: { en: "Latehar", hi: "लातेहार", hinglish: "Latehar" },
  75: { en: "Panki", hi: "पांकी", hinglish: "Panki" },
  76: { en: "Daltonganj", hi: "डालटनगंज", hinglish: "Daltonganj" },
  77: { en: "Bishrampur", hi: "विश्रामपुर", hinglish: "Bishrampur" },
  78: { en: "Chhatarpur", hi: "छतरपुर", hinglish: "Chhatarpur" },
  79: { en: "Hussainabad", hi: "हुसैनाबाद", hinglish: "Hussainabad" },
  80: { en: "Garhwa", hi: "गढ़वा", hinglish: "Garhwa" },
  81: { en: "Bhawanathpur", hi: "भवनाथपुर", hinglish: "Bhawanathpur" },
};

function getAcLabelForLang(acNo, lang) {
  const n = Number(acNo);
  if (!Number.isFinite(n)) return "";
  if (!lang) lang = getActiveLang();

  const d = getDistrictById(currentDistrictId);
  const acLabels = d && typeof d.acLabels === "object" && d.acLabels ? d.acLabels : null;
  const raw = (acLabels && (acLabels[n] ?? acLabels[String(n)])) || FALLBACK_AC_LABELS[n];
  if (!raw) return "";

  if (typeof raw === "string") return raw.trim();
  const base = String(raw.en ?? raw.label ?? raw.name ?? "").trim();
  if (lang === LANG.EN) return String(raw.en ?? base).trim() || base;
  if (lang === LANG.HINGLISH) return String(raw.hinglish ?? raw.en ?? base).trim() || base;
  return String(raw.hi ?? raw.hindi ?? base).trim() || base;
}

function getAcOptionLabel(acNo) {
  const n = Number(acNo);
  if (!Number.isFinite(n)) return "";
  const name = getAcLabelForLang(n, getActiveLang());
  return name ? `${n} - ${name}` : `AC ${n}`;
}

function getDistrictSearchStrings(d) {
  const out = [];
  const seen = new Set();
  const push = (x) => {
    const s = String(x ?? "").trim();
    if (!s) return;
    const k = s.toLowerCase();
    if (seen.has(k)) return;
    seen.add(k);
    out.push(s);
  };

  push(d?.id);
  push(d?.label);
  push(d?.name);
  push(d?.label_en);
  push(d?.label_hinglish);
  push(d?.label_hi);
  push(d?.label_hindi);

  const labels = d && typeof d.labels === "object" && d.labels ? d.labels : {};
  push(labels.en);
  push(labels.hinglish);
  push(labels.hi);
  push(labels.hindi);

  return out;
}

function normDistrictSearchStr(s) {
  return norm(String(s || "")).toLowerCase();
}

function districtMatchesQuery(d, qNorm, qNoSpace) {
  if (!qNorm) return true;
  const cands = getDistrictSearchStrings(d);
  for (const c of cands) {
    const cn = normDistrictSearchStr(c);
    if (!cn) continue;
    if (cn.includes(qNorm)) return true;
    if (qNoSpace && cn.replace(/\s+/g, "").includes(qNoSpace)) return true;
  }
  return false;
}

/* ----------------------------- rank key compare ----------------------------- */
function cmpRankKey(a, b) {
  const aa = Array.isArray(a) ? a : [];
  const bb = Array.isArray(b) ? b : [];
  const n = Math.max(aa.length, bb.length);
  for (let i = 0; i < n; i++) {
    const av = aa[i] ?? 0;
    const bv = bb[i] ?? 0;
    if (av !== bv) return av < bv ? -1 : 1; // smaller is better
  }
  return 0;
}
function cmpRelevance(a, b) {
  const c = cmpRankKey(a.sortKey, b.sortKey);
  if (c !== 0) return c;
  if (a.ac !== b.ac) return a.ac - b.ac;
  return a.row_id - b.row_id;
}

// ---------------- Transliteration + Voice (NEW, non-breaking) ----------------
const TRANSLIT = {
  endpoint: "https://inputtools.google.com/request",
  itc: "mr-t-i0-und",
  num: 5,
  debounceMs: 120,
};

function isDevanagariChar(ch) {
  if (!ch) return false;
  const cp = ch.codePointAt(0);
  return cp >= 0x0900 && cp <= 0x097f;
}
function containsDevanagari(s) {
  if (!s) return false;
  return /[\u0900-\u097F]/.test(String(s));
}
function isLatinChar(ch) {
  return /^[A-Za-z]$/.test(ch || "");
}
function detectScriptModeFromText(s) {
  s = String(s || "");
  if (!s) return "nonlatin";
  if (containsDevanagari(s)) return "nonlatin";
  // treat pure ASCII letters/spaces/punct as latin-intent if it has at least one A-Z
  if (/[A-Za-z]/.test(s)) return "latin";
  return "nonlatin";
}

function isIOS() {
  const ua = navigator.userAgent || "";
  const iOS = /iP(hone|od|ad)/.test(ua);
  // iPadOS 13+ reports as Mac; detect touch
  const iPadOS = navigator.platform === "MacIntel" && navigator.maxTouchPoints > 1;
  return iOS || iPadOS;
}
function isSafari() {
  const ua = navigator.userAgent || "";
  const isWebKit = /AppleWebKit/.test(ua);
  const isChrome = /CriOS|Chrome/.test(ua);
  const isFirefox = /FxiOS|Firefox/.test(ua);
  return isWebKit && !isChrome && !isFirefox;
}
function isIOSSafari() {
  return isIOS() && isSafari();
}

function setInputValueNoRerender(inputEl, v) {
  // Do not replace node; just set .value
  inputEl.value = v;
  // keep existing enable/disable logic synced
  syncSearchButtonState();
}

async function fetchGoogleSuggestions(text) {
  const params = new URLSearchParams();
  params.set("text", text);
  params.set("itc", TRANSLIT.itc);
  params.set("num", String(TRANSLIT.num));
  params.set("cp", "0");
  params.set("cs", "1");
  params.set("ie", "utf-8");
  params.set("oe", "utf-8");
  params.set("app", "test");

  const url = `${TRANSLIT.endpoint}?${params.toString()}`;

  const resp = await fetch(url, { method: "GET" });
  if (!resp.ok) throw new Error(`HTTP ${resp.status}`);
  const json = await resp.json();
  // Expected: ["SUCCESS", [[input, [cands...], ...]]]
  if (!Array.isArray(json) || json[0] !== "SUCCESS") return [];
  const payload = json[1];
  if (!Array.isArray(payload) || !payload.length) return [];
  const first = payload[0];
  if (!Array.isArray(first) || first.length < 2) return [];
  const cands = first[1];
  if (!Array.isArray(cands)) return [];
  return cands.map((x) => String(x || "").trim()).filter(Boolean);
}

function ensureTranslitPopoverSkeleton(popEl) {
  if (!popEl || popEl.dataset.built === "1") return;

  popEl.innerHTML = "";

  const list = document.createElement("div");
  list.dataset.role = "translit-list";
  popEl.appendChild(list);

  popEl.dataset.built = "1";
}

function openTranslitPopover(popEl, anchorWrapEl) {
  if (!popEl) return;

  // keep same style and anchoring behavior as other popovers
  popEl.style.display = "block";
  popEl.setAttribute("aria-hidden", "false");

  // ensure width matches input wrap
  if (anchorWrapEl) {
    const w = anchorWrapEl.getBoundingClientRect().width;
    if (w && Number.isFinite(w))
      popEl.style.minWidth = `${Math.max(240, Math.floor(w))}px`;
  }
}

function closeTranslitPopover(popEl) {
  if (!popEl) return;
  popEl.style.display = "none";
  popEl.setAttribute("aria-hidden", "true");
  // clear highlights but keep skeleton
  const list = popEl.querySelector("div[data-role='translit-list']");
  if (list) list.innerHTML = "";
  popEl.dataset.activeIndex = "-1";
  popEl.dataset.items = "[]";
}

function renderTranslitSuggestions(popEl, suggestions, { onPick, activeIndex = -1 } = {}) {
  if (!popEl) return;

  ensureTranslitPopoverSkeleton(popEl);
  const listEl = popEl.querySelector("div[data-role='translit-list']");
  if (!listEl) return;

  listEl.innerHTML = "";

  const items = suggestions.slice(0, 5);
  popEl.dataset.items = JSON.stringify(items);
  popEl.dataset.activeIndex = String(activeIndex);

  for (let i = 0; i < items.length; i++) {
    const s = items[i];
    const row = popRow({
      left: s,
      right: "",
      chevron: false,
      selected: i === activeIndex,
      onClick: () => onPick?.(s),
    });

    // highlight styling uses existing .popSelected class; we piggyback on that
    if (i === activeIndex) row.classList.add("popSelected");

    // IMPORTANT: use mousedown so input doesn't blur
    row.addEventListener("mousedown", (e) => {
      e.preventDefault();
      e.stopPropagation();
      onPick?.(s);
    });

    listEl.appendChild(row);
  }

  if (!items.length) {
    closeTranslitPopover(popEl);
  }
}

function getItemsFromPopover(popEl) {
  try {
    const arr = JSON.parse(popEl?.dataset?.items || "[]");
    return Array.isArray(arr) ? arr : [];
  } catch {
    return [];
  }
}
function getActiveIndexFromPopover(popEl) {
  const n = Number(popEl?.dataset?.activeIndex);
  return Number.isFinite(n) ? n : -1;
}
function setActiveIndexOnPopover(popEl, idx) {
  if (!popEl) return;
  popEl.dataset.activeIndex = String(idx);
}

function attachNameEnhancements({
  inputEl,
  wrapEl,
  micBtnEl,
  popEl,
  iosHintEl,
  iosHintCloseEl,
  onCommit,
  getDisabledState,
}) {
  if (!inputEl || !wrapEl || !micBtnEl || !popEl) return;

  // State per-field
  let inputMode = "nonlatin"; // latin | nonlatin
  let lastReqId = 0;
  let debounceTimer = null;
  let ignoreUntil = 0; // used to suppress suggestions briefly after programmatic changes
  let isComposing = false;

  // SpeechRecognition per field
  let recognizer = null;
  let isListening = false;
  let spokenFinal = "";
  let pendingFinalCommit = "";
  let lastPreview = "";
  let lastCommittedFinal = "";
  let suppressSuggestDuringSpeech = false;

  function now() {
    return Date.now();
  }

  function joinSpeechParts(a, b) {
    const aa = String(a || "").trim();
    const bb = String(b || "").trim();
    if (!aa) return bb;
    if (!bb) return aa;
    return `${aa} ${bb}`.replace(/\s+/g, " ").trim();
  }

  function setListeningUI(on) {
    isListening = !!on;
    micBtnEl.classList.toggle("listening", !!on);
    wrapEl.classList.toggle("isListening", !!on);
  }

  function closeAll() {
    closeTranslitPopover(popEl);
  }

  function syncDisabled() {
    const disabled = getDisabledState ? !!getDisabledState() : !!inputEl.disabled;
    micBtnEl.disabled = disabled;
    if (disabled) {
      // stop listening if any
      if (recognizer && isListening) {
        try {
          recognizer.abort();
        } catch {}
      }
      setListeningUI(false);
      closeAll();
    }
  }

  // keep mic disabled in sync when input disabled toggles
  const mo = new MutationObserver(syncDisabled);
  mo.observe(inputEl, {
    attributes: true,
    attributeFilter: ["disabled", "aria-disabled", "class"],
  });
  syncDisabled();

  // iOS hint controls
  function showIOSSafariHint() {
    if (!iosHintEl) return;
    iosHintEl.classList.add("show");
  }
  function hideIOSSafariHint() {
    if (!iosHintEl) return;
    iosHintEl.classList.remove("show");
  }
  if (iosHintCloseEl && iosHintEl) {
    iosHintCloseEl.onclick = () => hideIOSSafariHint();
  }

  // Mode determination based on last typed char (as per spec)
  function updateModeFromLastChar(ch) {
    if (isLatinChar(ch)) inputMode = "latin";
    else if (isDevanagariChar(ch)) inputMode = "nonlatin";
    // else keep prior mode
  }

  function setModeFromText(text) {
    inputMode = detectScriptModeFromText(text);
  }

  function commitSuggestion(chosen) {
    ignoreUntil = now() + 80;
    setInputValueNoRerender(inputEl, chosen);
    closeAll();
    if (typeof onCommit === "function") onCommit(chosen);
  }

  async function requestSuggestions(text) {
    const q = String(text || "");
    const trimmed = q.trim();
    if (!trimmed) {
      closeAll();
      return;
    }
    if (inputMode !== "latin") {
      closeAll();
      return;
    }
    if (suppressSuggestDuringSpeech) return;
    if (now() < ignoreUntil) return;

    const reqId = ++lastReqId;
    try {
      const candsRaw = await fetchGoogleSuggestions(trimmed);
      if (reqId !== lastReqId) return; // stale

      // filter: remove empties, identical to input, dedupe
      const seen = new Set();
      const out = [];
      for (const c of candsRaw) {
        const cand = String(c || "").trim();
        if (!cand) continue;
        if (cand === trimmed) continue;
        if (seen.has(cand)) continue;
        seen.add(cand);
        out.push(cand);
        if (out.length >= 5) break;
      }

      if (!out.length) {
        closeAll();
        return;
      }

      openTranslitPopover(popEl, wrapEl);
      renderTranslitSuggestions(popEl, out, {
        activeIndex: -1,
        onPick: (chosen) => commitSuggestion(chosen),
      });
    } catch (_e) {
      // silent failure per spec
      closeAll();
    }
  }

  function scheduleSuggest(text) {
    if (debounceTimer) clearTimeout(debounceTimer);
    debounceTimer = setTimeout(() => {
      debounceTimer = null;
      requestSuggestions(text);
    }, TRANSLIT.debounceMs);
  }

  // IME composition tracking
  inputEl.addEventListener("compositionstart", () => {
    isComposing = true;
  });
  inputEl.addEventListener("compositionend", () => {
    isComposing = false;
    // compositionend produces committed text; set mode from last char in full value
    const v = String(inputEl.value || "");
    setModeFromText(v);
    // spec: IME output -> nonlatin; avoid showing suggestions
    closeAll();
  });

  // Typing/paste tracking
  inputEl.addEventListener("beforeinput", (e) => {
    // Helps detect inserted char before value changes
    // For IME, data can be null; we don't want to flip latin mode incorrectly.
    const t = e?.inputType || "";
    const data = e?.data;

    if (t.startsWith("insert") && typeof data === "string" && data.length) {
      // use last char of inserted data
      const ch = data[data.length - 1];
      updateModeFromLastChar(ch);
    }
  });

  inputEl.addEventListener("paste", (e) => {
    try {
      const txt = e.clipboardData?.getData("text") ?? "";
      setModeFromText(txt);
    } catch {}
  });

  inputEl.addEventListener("input", () => {
    if (isComposing) return; // don't interfere mid-IME
    if (suppressSuggestDuringSpeech) return;

    const v = String(inputEl.value || "");
    if (!v.trim()) {
      closeAll();
      return;
    }

    if (inputMode !== "latin") {
      closeAll();
      return;
    }
    scheduleSuggest(v);
  });

  // Keyboard support for suggestion dropdown (up/down/enter/esc)
  // IMPORTANT: use capture + stopImmediatePropagation so wireIMEEnter cannot steal Enter.
  inputEl.addEventListener(
    "keydown",
    (e) => {
      const isOpen = popEl.style.display === "block";
      if (!isOpen) return;

      const items = getItemsFromPopover(popEl);
      const hasItems = items.length > 0;

      if (e.key === "Escape" || e.key === "ArrowDown" || e.key === "ArrowUp" || e.key === "Enter") {
        e.preventDefault();
        e.stopPropagation();
        if (typeof e.stopImmediatePropagation === "function") e.stopImmediatePropagation();
      }

      if (e.key === "Escape") {
        closeAll();
        return;
      }

      if (!hasItems) {
        closeAll();
        return;
      }

      let idx = getActiveIndexFromPopover(popEl);

      if (e.key === "ArrowDown") {
        idx = Math.min(items.length - 1, idx + 1);
        setActiveIndexOnPopover(popEl, idx);
        renderTranslitSuggestions(popEl, items, {
          activeIndex: idx,
          onPick: (chosen) => commitSuggestion(chosen),
        });
        return;
      }

      if (e.key === "ArrowUp") {
        idx = Math.max(0, idx - 1);
        setActiveIndexOnPopover(popEl, idx);
        renderTranslitSuggestions(popEl, items, {
          activeIndex: idx,
          onPick: (chosen) => commitSuggestion(chosen),
        });
        return;
      }

      if (e.key === "Enter") {
        const idxNow = getActiveIndexFromPopover(popEl);
        const pickIdx = idxNow >= 0 && idxNow < items.length ? idxNow : 0;
        const chosen = items[pickIdx];
        commitSuggestion(chosen);
        return;
      }
    },
    true
  );

  // Mic button behavior
  function initRecognizerIfPossible() {
    const SpeechRecognition = window.SpeechRecognition || window.webkitSpeechRecognition;
    if (!SpeechRecognition) return null;
    const r = new SpeechRecognition();
    r.continuous = false;
    r.interimResults = true;
    r.maxAlternatives = 5;
    r.lang = "hi-IN";
    return r;
  }

  function stopListening() {
    if (!recognizer) return;
    try {
      recognizer.stop();
    } catch {}
  }

  function startListening() {
    if (!recognizer) return;
    suppressSuggestDuringSpeech = true;
    spokenFinal = "";
    pendingFinalCommit = "";
    lastPreview = "";
    lastCommittedFinal = "";
    setListeningUI(true);
    closeAll();
    try {
      recognizer.start();
    } catch {}
  }

  function handleSpeechFinal(text) {
    const transcript = String(text || "").trim();
    if (!transcript) return;

    ignoreUntil = now() + 120;
    setInputValueNoRerender(inputEl, transcript);

    if (containsDevanagari(transcript)) {
      inputMode = "nonlatin";
      closeAll();
      if (typeof onCommit === "function") onCommit(transcript);
      return;
    }

    inputMode = "latin";
    (async () => {
      try {
        const candsRaw = await fetchGoogleSuggestions(transcript);
        const seen = new Set();
        const out = [];
        for (const c of candsRaw) {
          const cand = String(c || "").trim();
          if (!cand) continue;
          if (cand === transcript) continue;
          if (seen.has(cand)) continue;
          seen.add(cand);
          out.push(cand);
          if (out.length >= 5) break;
        }

        if (out.length) {
          openTranslitPopover(popEl, wrapEl);
          renderTranslitSuggestions(popEl, out, {
            activeIndex: -1,
            onPick: (chosen) => commitSuggestion(chosen),
          });

          if (typeof onCommit === "function") onCommit(transcript);
        } else {
          closeAll();
          if (typeof onCommit === "function") onCommit(transcript);
        }
      } catch {
        closeAll();
        if (typeof onCommit === "function") onCommit(transcript);
      }
    })();
  }

  micBtnEl.addEventListener("click", () => {
    syncDisabled();
    if (micBtnEl.disabled) return;

    if (isIOSSafari()) {
      try {
        inputEl.focus();
      } catch {}
      showIOSSafariHint();
      return;
    }

    if (!recognizer) {
      recognizer = initRecognizerIfPossible();
      if (!recognizer) return;

      recognizer.onresult = (ev) => {
        try {
          let interim = "";
          let finalChunk = "";

          for (let i = ev.resultIndex; i < ev.results.length; i++) {
            const res = ev.results[i];
            const txt = res?.[0]?.transcript ?? "";
            if (res.isFinal) finalChunk = joinSpeechParts(finalChunk, txt);
            else interim += txt;
          }

          interim = String(interim || "").trim();
          finalChunk = String(finalChunk || "").trim();

          if (finalChunk) {
            spokenFinal = joinSpeechParts(spokenFinal, finalChunk);
            pendingFinalCommit = spokenFinal;
          }

          const livePreview = joinSpeechParts(spokenFinal, interim);
          if (livePreview && livePreview !== lastPreview) {
            lastPreview = livePreview;
            ignoreUntil = now() + 60;
            setInputValueNoRerender(inputEl, livePreview);
          }

          if (pendingFinalCommit && !interim && pendingFinalCommit !== lastCommittedFinal) {
            lastCommittedFinal = pendingFinalCommit;
            handleSpeechFinal(pendingFinalCommit);
            pendingFinalCommit = "";
          }
        } catch (e) {
          console.warn(e);
        }
      };

      recognizer.onerror = (_ev) => {
        setListeningUI(false);
        suppressSuggestDuringSpeech = false;
      };

      recognizer.onend = () => {
        const finalToCommit = String(pendingFinalCommit || spokenFinal || "").trim();
        if (finalToCommit && finalToCommit !== lastCommittedFinal) {
          lastCommittedFinal = finalToCommit;
          handleSpeechFinal(finalToCommit);
        }
        pendingFinalCommit = "";
        setListeningUI(false);
        suppressSuggestDuringSpeech = false;
      };
    }

    if (isListening) {
      stopListening();
      return;
    }
    startListening();
  });

  return {
    close: () => closeAll(),
    closePopover: () => closeTranslitPopover(),
    syncDisabled,
    getPopover: () => popEl,
    getWrap: () => wrapEl,
    getMic: () => micBtnEl,
  };
}

// ---------------- i18n ----------------
const i18n = createI18n({ storageKey: "sir_lang", defaultLang: LANG.HI });
const t = i18n.t;

function applyTranslationsToDOM() {
  document.querySelectorAll("[data-i18n]").forEach((el) => {
    const k = el.getAttribute("data-i18n");
    if (!k) return;
    el.textContent = t(k);
  });

  document.querySelectorAll("[data-i18n-placeholder]").forEach((el) => {
    const k = el.getAttribute("data-i18n-placeholder");
    if (!k) return;
    el.setAttribute("placeholder", t(k));
  });

  document.querySelectorAll("[data-i18n-title]").forEach((el) => {
    const k = el.getAttribute("data-i18n-title");
    if (!k) return;
    el.setAttribute("title", t(k));
  });

  document.querySelectorAll("[data-i18n-aria-label]").forEach((el) => {
    const k = el.getAttribute("data-i18n-aria-label");
    if (!k) return;
    el.setAttribute("aria-label", t(k));
  });

  updateDistrictUI();
  updateSelectedAcText();
  setSortMode(sortMode);
  renderSortPopover();
  renderPageSizePopover();
  renderFiltersPopoverRoot();

  try {
    if (districtPopover && districtPopover.style.display !== "none") updateDistrictPopoverList(districtPopover);
  } catch {}
  try {
    if (districtPopoverLanding && districtPopoverLanding.style.display !== "none") updateDistrictPopoverList(districtPopoverLanding);
  } catch {}
  try {
    if (acPopover && acPopover.style.display !== "none") renderAcPopover();
  } catch {}

  if (pageSizeBtn) {
    const label = document.querySelector("[data-i18n='page_size_label']");
    if (label) label.textContent = t("page_size_label");
  }
}

function setLanguage(lang) {
  const active = i18n.setLang(lang);

  document.documentElement.lang = active === LANG.EN ? "en" : "hi";

  const btnHi = $("langHi");
  const btnHing = $("langHinglish");
  const btnEn = $("langEn");

  const all = [
    { el: btnHi, lang: LANG.HI },
    { el: btnHing, lang: LANG.HINGLISH },
    { el: btnEn, lang: LANG.EN },
  ];

  for (const item of all) {
    if (!item.el) continue;
    const isActive = item.lang === active;
    item.el.classList.toggle("active", isActive);
    item.el.setAttribute("aria-pressed", isActive ? "true" : "false");
    const check = item.el.querySelector(".langCheck");
    if (check) check.textContent = isActive ? "✓" : "";
  }

  refreshChipLabels();
  applyTranslationsToDOM();

  if (!isResultsVisible() && (!current.lastQuery || !String(current.lastQuery).trim())) {
    setStatus(t("status_select_district"));
  }
}

function loadSavedLanguageOrDefault() {
  return i18n.loadSavedLanguageOrDefault();
}

function headerLabelForKey(k) {
  switch (k) {
    case "Voter Name":
      return t("h_voter_name");
    case "Relative Name":
      return t("h_relative_name");
    case "Relation":
      return t("h_relation");
    case "Gender":
      return t("h_gender");
    case "Age":
      return t("h_age");
    case "House No":
      return t("h_house_no");
    case "Serial No":
      return t("h_serial_no");
    case "Page No":
      return t("h_page_no");
    case "Part No":
      return t("h_part_no");
    case "AC No":
      return t("h_ac_no");
    case "ID":
      return t("h_id");
    default:
      return k;
  }
}

// ------- helpers -------
const $ = (id) => document.getElementById(id);

// ------- UI helpers -------
const landingSection = $("landingSection");
const resultsSection = $("resultsSection");

// Landing widgets
const qLanding = $("q");
const searchBtnLanding = $("searchBtn");
const districtSelHidden = $("districtSel"); // hidden select (kept for compatibility)
const districtBtnLanding = $("districtBtnLanding");
const districtMirrorLanding = $("districtMirrorLanding");
const districtPopoverLanding = $("districtPopoverLanding");
const exactToggleLanding = $("exactToggle");

// Results widgets
const qResults = $("qResults");
const searchBtnResults = $("searchBtnResults");
const exactToggleResults = $("exactToggleResults");
const resultsCountEl = $("resultsCount");
const moreFiltersBtn = $("moreFiltersBtn");

// District header switcher
const districtBtn = $("districtBtn");
const districtMirror = $("districtMirror");
const districtPopover = $("districtPopover");

// AC popover filter
const selectedAcBtn = $("selectedAcBtn");
const selectedAcText = $("selectedAcText");
const acPopover = $("acPopover");

// Filters UI elements
const filtersPopover = $("filtersPopover");
const modalOverlay = $("modalOverlay");
const modalTitle = $("modalTitle");
const modalSubtitle = $("modalSubtitle");
const modalFields = $("modalFields");
const modalCancel = $("modalCancel");
const modalDone = $("modalDone");

// Sort popover
const sortBtn = $("sortBtn");
const sortText = $("sortText");
const sortPopover = $("sortPopover");

// Pager
const pagerEl = $("pager");
const prevBtn = $("prevBtn");
const nextBtn = $("nextBtn");
const currentPageCount = $("currentPageCount");
const pageInfo = $("pageInfo");

const clearBtn = $("clearBtn");
const clearBtnTop = $("clearBtnTop");

// Results table scroll container (for mobile-only compact header behavior)
const tableRegion = document.querySelector("#resultsSection .tableRegion");
const tableHeaderShadow = $("tableHeaderShadow");
const scrollTopFab = $("scrollTopFab");
const resultsTopEl = resultsSection?.querySelector?.(".resultsTop") || null;
const resultsBodyEl = resultsSection?.querySelector?.(".resultsBody") || null;

// Page-size popover
const pageSizeBtn = $("pageSizeBtn");
const pageSizeText = $("pageSizeText");
const pageSizePopover = $("pageSizePopover");

// status/meta (both landing + results)
const statusLanding = $("statusLanding");
const metaLanding = $("metaLanding");
const statusResults = $("statusResults");
const metaResults = $("metaResults");
const progressOverlayResults = $("progressOverlayResults");
const progressPanelResults = $("progressPanelResults");
const progressRingResults = $("progressRingResults");
const progressPctResults = $("progressPctResults");
const progressStageResults = $("progressStageResults");
const progressSubResults = $("progressSubResults");

const qDisabledHintLanding = $("qDisabledHintLanding");
const qDisabledHintResults = $("qDisabledHintResults");
const landingInfoBanner = $("landingInfoBanner");
const landingInfoBannerDesktop = $("landingInfoBannerDesktop");
const landingKnowMoreBtn = $("landingKnowMoreBtn");
const landingKnowMoreBtnMobile = $("landingKnowMoreBtnMobile");
const landingInfoBannerCloseBtn = $("landingInfoBannerCloseBtn");
const landingInfoBannerCloseBtnMobile = $("landingInfoBannerCloseBtnMobile");
const landingFaqBtn = $("landingFaqBtn");
const resultsInfoToast = $("resultsInfoToast");
const resultsToastRingFg = $("resultsToastRingFg");
const announcementSection = $("announcementSection");
const announcementBackBtn = $("announcementBackBtn");

const SEEN_LANDING_BANNER_KEY = "sir_seen_landing_banner_v1";
const SEEN_RESULTS_BANNER_KEY = "sir_seen_results_banner_v1";

let resultsToastTimer = null;
let resultsToastRaf = null;

function setStatus(msg) {
  if (statusLanding) statusLanding.textContent = msg ?? "";
  if (statusResults) statusResults.textContent = msg ?? "";
}


function setResultsProgressVisible(visible) {
  if (progressOverlayResults) {
    progressOverlayResults.style.display = visible ? "flex" : "none";
    progressOverlayResults.setAttribute("aria-hidden", visible ? "false" : "true");
  }
  if (progressPanelResults) progressPanelResults.style.display = visible ? "flex" : "none";
}

// Backward-compatible loader API used by older callsites.
function hideLoader() {
  setResultsProgressVisible(false);
  setProgressStage("");
  setProgressSub("");
  setBar(0);
}

function showLoader(stageMsg = "") {
  setResultsProgressVisible(true);
  setProgressStage(stageMsg || "");
}

function setProgressStage(msg) {
  if (progressStageResults) progressStageResults.textContent = msg ?? "";
}

function setProgressSub(msg) {
  if (progressSubResults) progressSubResults.textContent = msg ?? "";
}

function phaseLabelForWorker(phase) {
  if (phase === "scoring") return t("progress_stage_rank");
  return String(phase || "").trim();
}

function formatWorkerStage(phase, ac, i, n) {
  const p = phaseLabelForWorker(phase);
  const acPart = t("progress_ac_context", { ac, i, n });
  return p ? `${p} • ${acPart}` : acPart;
}

function setBar(pct) {
  const safe = Number.isFinite(Number(pct)) ? Number(pct) : 0;
  const clamped = Math.max(0, Math.min(100, safe));
  const rounded = Math.round(clamped);
  if (progressRingResults) progressRingResults.setAttribute("stroke-dasharray", `${clamped}, 100`);
  if (progressPctResults) progressPctResults.textContent = `${rounded}%`;
}

function setMeta(msg) {
  if (metaLanding) metaLanding.textContent = msg ?? "";
  if (metaResults) metaResults.textContent = msg ?? "";
}

// ===== VIEW SWITCHING =====

function showLanding() {
  clearLandingScrollRestoreHandlers();
  landingSection.style.display = "flex";
  resultsSection.style.display = "none";
  if (announcementSection) announcementSection.style.display = "none";
  // Ensure results UI is restored when user returns.
  resetMobileTableCompactUI();
}

function showResults() {
  clearLandingScrollRestoreHandlers();
  landingSection.style.display = "none";
  resultsSection.style.display = "block";
  if (announcementSection) announcementSection.style.display = "none";
  // Sync compact UI state (in case user navigates back to results).
  if (tableRegion) {
    setMobileTableCompact(tableRegion.scrollTop > 8);
    syncStickyColScrollShadow();
  }
}

function showAnnouncementPage() {
  clearLandingScrollRestoreHandlers();
  landingSection.style.display = "none";
  resultsSection.style.display = "none";
  if (announcementSection) announcementSection.style.display = "block";
}

function hasSeenLandingBanner() {
  let seen = false;
  try {
    seen = localStorage.getItem(SEEN_LANDING_BANNER_KEY) === "1";
  } catch {}
  return seen;
}

function syncLandingFaqVisibility(seen = hasSeenLandingBanner()) {
  if (!landingFaqBtn) return;
  landingFaqBtn.style.display = seen ? "inline-flex" : "none";
}

function markLandingBannerSeenAndHide() {
  try {
    localStorage.setItem(SEEN_LANDING_BANNER_KEY, "1");
  } catch {}
  if (landingInfoBanner) landingInfoBanner.style.display = "none";
  if (landingInfoBannerDesktop) landingInfoBannerDesktop.style.display = "none";
  syncLandingFaqVisibility(true);
}

function maybeShowLandingBanner() {
  const seen = hasSeenLandingBanner();
  if (!landingInfoBanner) {
    syncLandingFaqVisibility(seen);
    return;
  }
  landingInfoBanner.style.display = seen ? "none" : "";
  syncLandingFaqVisibility(seen);
}

function maybeShowLandingBannerDesktop() {
  const seen = hasSeenLandingBanner();
  if (!landingInfoBannerDesktop) {
    syncLandingFaqVisibility(seen);
    return;
  }
  landingInfoBannerDesktop.style.display = seen ? "none" : "";
  syncLandingFaqVisibility(seen);
}

function hideResultsToast() {
  if (resultsToastTimer) {
    clearTimeout(resultsToastTimer);
    resultsToastTimer = null;
  }
  if (resultsToastRaf) {
    cancelAnimationFrame(resultsToastRaf);
    resultsToastRaf = null;
  }
  if (resultsInfoToast) resultsInfoToast.style.display = "none";
  if (resultsToastRingFg) resultsToastRingFg.setAttribute("stroke-dasharray", "0 100");
}

function maybeShowResultsToastOnce() {
  if (!resultsInfoToast || !resultsToastRingFg) return;

  let seen = false;
  try {
    seen = localStorage.getItem(SEEN_RESULTS_BANNER_KEY) === "1";
  } catch {}
  if (seen) return;

  resultsInfoToast.style.display = "block";

  const path = resultsToastRingFg;

  // --- CRITICAL: use real path length ---
  const length = path.getTotalLength();

  // Force reset state every time
  path.style.transition = "none";
  path.style.strokeDasharray = length;
  path.style.strokeDashoffset = length;

  // Force layout so browser applies reset immediately
  path.getBoundingClientRect();

  const ms = 6000;
  const t0 = performance.now();

  const tick = (now) => {
    const p = Math.max(0, Math.min(1, (now - t0) / ms));
    const offset = length * (1 - p);
    path.style.strokeDashoffset = offset;

    // DEBUG (remove later)
    // console.log("progress:", p, "offset:", offset);

    if (p < 1) {
      resultsToastRaf = requestAnimationFrame(tick);
    }
  };

  resultsToastRaf = requestAnimationFrame(tick);

  resultsToastTimer = setTimeout(() => {
    hideResultsToast();
  }, ms);

  try {
    localStorage.setItem(SEEN_RESULTS_BANNER_KEY, "1");
  } catch {}
}

function isResultsVisible() {
  return window.getComputedStyle(resultsSection).display !== "none";
}

function isLandingVisible() {
  return window.getComputedStyle(landingSection).display !== "none";
}

const LANDING_INPUT_TOP_GAP_PX = 96;
const KEYBOARD_OPEN_DELTA_PX = 110;
const KEYBOARD_CLOSE_DELTA_PX = 40;
const AUTO_SCROLL_RESTORE_TOLERANCE_PX = 120;

let landingScrollRestoreCleanup = null;

function clearLandingScrollRestoreHandlers() {
  const cleanup = landingScrollRestoreCleanup;
  landingScrollRestoreCleanup = null;
  if (typeof cleanup === "function") {
    try {
      cleanup();
    } catch {}
  }
}

function setupLandingKeyboardCloseRestore({ inputEl, prevScrollY, targetScrollY, baselineViewportH }) {
  clearLandingScrollRestoreHandlers();
  if (!inputEl) return;

  const vv = window.visualViewport || null;
  let keyboardWasOpen = false;
  let done = false;

  const cleanup = () => {
    vv?.removeEventListener?.("resize", onViewportResize);
    inputEl.removeEventListener("blur", onInputBlur);
    window.removeEventListener("pagehide", onPageHide);
  };

  const maybeRestore = () => {
    if (done) return;
    if (!isLandingVisible()) {
      done = true;
      clearLandingScrollRestoreHandlers();
      return;
    }

    const currentViewportH = vv?.height || window.innerHeight;
    if (currentViewportH <= baselineViewportH - KEYBOARD_OPEN_DELTA_PX) {
      keyboardWasOpen = true;
    }

    const focusGone = document.activeElement !== inputEl;
    if (focusGone && !keyboardWasOpen) {
      done = true;
      clearLandingScrollRestoreHandlers();
      return;
    }

    const keyboardClosed = keyboardWasOpen && currentViewportH >= baselineViewportH - KEYBOARD_CLOSE_DELTA_PX;
    if (!keyboardClosed && !(keyboardWasOpen && focusGone)) return;

    const currentY = window.scrollY || window.pageYOffset || 0;
    const stillNearAutoPosition = Math.abs(currentY - targetScrollY) <= AUTO_SCROLL_RESTORE_TOLERANCE_PX;
    if (stillNearAutoPosition) {
      window.scrollTo({ top: prevScrollY, behavior: "smooth" });
    }

    done = true;
    clearLandingScrollRestoreHandlers();
  };

  const onViewportResize = () => {
    requestAnimationFrame(maybeRestore);
  };

  const onInputBlur = () => {
    setTimeout(maybeRestore, 90);
  };

  const onPageHide = () => {
    done = true;
    clearLandingScrollRestoreHandlers();
  };

  vv?.addEventListener?.("resize", onViewportResize);
  inputEl.addEventListener("blur", onInputBlur);
  window.addEventListener("pagehide", onPageHide);

  landingScrollRestoreCleanup = cleanup;
}

function scrollLandingInputToTopGap(inputEl, gapPx = LANDING_INPUT_TOP_GAP_PX) {
  if (!inputEl || !isLandingVisible()) return;

  const prevScrollY = window.scrollY || window.pageYOffset || 0;
  const rect = inputEl.getBoundingClientRect();
  const targetScrollY = Math.max(0, Math.round(prevScrollY + rect.top - gapPx));
  if (Math.abs(targetScrollY - prevScrollY) < 2) return;

  const baselineViewportH = window.visualViewport?.height || window.innerHeight;
  setupLandingKeyboardCloseRestore({
    inputEl,
    prevScrollY,
    targetScrollY,
    baselineViewportH,
  });

  window.scrollTo({ top: targetScrollY, behavior: "smooth" });
}


// ------- Mobile-only: collapse header rows when the results table is scrolled -------
function isNarrowMobileForTableCompact() {
  return window.matchMedia("(max-width: 680px)").matches;
}

function applyMobileCompactResultsBodyHeight() {
  if (!resultsTopEl || !resultsBodyEl) return;

  // Only override height in compact state (so default layout remains unchanged).
  const vh = window.visualViewport?.height || window.innerHeight;
  const topH = resultsTopEl.getBoundingClientRect().height;

  // Keep a small gutter so borders/shadows don't clip.
  const h = Math.max(260, Math.round(vh - topH - 8));
  resultsBodyEl.style.height = `${h}px`;
}

function clearMobileCompactResultsBodyHeight() {
  if (!resultsBodyEl) return;
  resultsBodyEl.style.height = "";
}

function syncStickyColScrollShadow() {
  if (!tableRegion) return;
  tableRegion.classList.toggle("scrolled-x", tableRegion.scrollLeft > 1);
  tableRegion.classList.toggle("scrolled-y", tableRegion.scrollTop > 0);
}

function syncTableHeaderShadowOffset() {
  if (!tableRegion || !tableHeaderShadow) return;
  const firstTh = tableRegion.querySelector("thead th");
  const h = Math.round(firstTh?.getBoundingClientRect?.().height || 0);
  const safe = Math.max(36, h || 44);
  tableRegion.style.setProperty("--table-head-h", `${safe}px`);
}

function initStickyColScrollShadowSync() {
  if (!tableRegion) return;

  let raf = null;
  const sync = () => {
    if (raf) return;
    raf = requestAnimationFrame(() => {
      raf = null;
      syncTableHeaderShadowOffset();
      syncStickyColScrollShadow();
    });
  };

  tableRegion.addEventListener("scroll", sync, { passive: true });
  window.addEventListener("resize", sync);
  window.visualViewport?.addEventListener?.("resize", sync);

  syncTableHeaderShadowOffset();
  syncStickyColScrollShadow();
}

function setMobileTableCompact(on) {
  if (!isNarrowMobileForTableCompact() || !isResultsVisible()) {
    resultsSection.classList.remove("mobileCompact");
    clearMobileCompactResultsBodyHeight();
    return;
  }

  resultsSection.classList.toggle("mobileCompact", !!on);

  if (resultsSection.classList.contains("mobileCompact")) {
    applyMobileCompactResultsBodyHeight();
  } else {
    clearMobileCompactResultsBodyHeight();
  }
}

function resetMobileTableCompactUI() {
  resultsSection.classList.remove("mobileCompact");
  clearMobileCompactResultsBodyHeight();
}

function initMobileTableScrollCompactUI() {
  if (!tableRegion) return;

  let raf = null;

  const syncFromScroll = () => {
    syncStickyColScrollShadow();
    // Only act when results are visible.
    if (!isResultsVisible() || !isNarrowMobileForTableCompact()) {
      resetMobileTableCompactUI();
      return;
    }
    setMobileTableCompact(tableRegion.scrollTop > 8);
  };

  tableRegion.addEventListener(
    "scroll",
    () => {
      if (raf) return;
      raf = requestAnimationFrame(() => {
        raf = null;
        syncFromScroll();
      });
    },
    { passive: true }
  );

  // Recompute height on viewport changes while compact.
  window.addEventListener("resize", () => {
    if (!isResultsVisible() || !isNarrowMobileForTableCompact()) {
      resetMobileTableCompactUI();
      return;
    }
    if (resultsSection.classList.contains("mobileCompact")) applyMobileCompactResultsBodyHeight();
  });
  window.visualViewport?.addEventListener?.("resize", () => {
    if (!isResultsVisible() || !isNarrowMobileForTableCompact()) return;
    if (resultsSection.classList.contains("mobileCompact")) applyMobileCompactResultsBodyHeight();
  });

  // Scroll-to-top FAB
  scrollTopFab?.addEventListener("click", () => {
    tableRegion.scrollTo({ top: 0, behavior: "smooth" });
  });

  // Ensure correct state when coming back to results.
  syncFromScroll();
}

function getActiveQueryInput() {
  return isResultsVisible() ? qResults : qLanding;
}

const DISABLED_QUERY_HINT_MS = 2200;
const disabledQueryHintTimers = new Map();

function hideDisabledQueryHint(hintEl) {
  if (!hintEl) return;
  const timer = disabledQueryHintTimers.get(hintEl);
  if (timer) clearTimeout(timer);
  disabledQueryHintTimers.delete(hintEl);
  hintEl.classList.remove("show");
  hintEl.setAttribute("aria-hidden", "true");
}

function showDisabledQueryHint(hintEl) {
  if (!hintEl) return;
  hideDisabledQueryHint(hintEl);
  hintEl.classList.add("show");
  hintEl.setAttribute("aria-hidden", "false");
  const timer = setTimeout(() => {
    hintEl.classList.remove("show");
    hintEl.setAttribute("aria-hidden", "true");
    disabledQueryHintTimers.delete(hintEl);
  }, DISABLED_QUERY_HINT_MS);
  disabledQueryHintTimers.set(hintEl, timer);
}

function wireDisabledQueryTooltip({ wrapEl, inputEl, hintEl }) {
  if (!wrapEl || !inputEl || !hintEl) return;
  if (wrapEl.dataset.disabledHintWired === "1") return;

  wrapEl.dataset.disabledHintWired = "1";
  wrapEl.addEventListener("click", (e) => {
    if (!inputEl.disabled) return;
    if (districtACsAll.length) return;
    e.preventDefault();
    e.stopPropagation();
    showDisabledQueryHint(hintEl);
    setStatus(t("hint_select_district_then_type_name"));
  });
}

function setSearchEnabled(enabled) {
  qLanding.disabled = !enabled;
  qResults.disabled = !enabled;
  hideDisabledQueryHint(qDisabledHintLanding);
  hideDisabledQueryHint(qDisabledHintResults);
  syncSearchButtonState();

  try {
    enhLanding?.syncDisabled?.();
  } catch {}
  try {
    enhResults?.syncDisabled?.();
  } catch {}
  try {
    enhRel?.syncDisabled?.();
  } catch {}
}

function hasQueryableState() {
  const q = norm(getActiveQueryInput().value || "");
  return Boolean(districtACsAll.length) && q.length > 0;
}

function syncSearchButtonState() {
  const q = (getActiveQueryInput().value || "").trim();
  const canSearch = Boolean(districtACsAll.length) && q.length > 0 && !qLanding.disabled && !qResults.disabled;
  searchBtnLanding.disabled = !canSearch;
  searchBtnResults.disabled = !canSearch;
}

function isMobileUI() {
  return window.matchMedia("(max-width: 980px)").matches;
}

function setDistrictLoading(isLoading) {
  const landingShell = districtBtnLanding?.closest(".acShell");
  if (landingShell) landingShell.classList.toggle("loading", !!isLoading);

  const resultsShell = districtBtn?.closest(".acShell");
  if (resultsShell) resultsShell.classList.toggle("loading", !!isLoading);
}

// ---------------- Filters state ----------------
const filters = {
  gender: "all", // all | male | female | other
  age: { mode: "any", a: null, b: null }, // any | eq | gt | lt | range
  relativeName: "", // string (applies only when scope=voter)
};

function clearFilters() {
  filters.gender = "all";
  filters.age = { mode: "any", a: null, b: null };
  filters.relativeName = "";
  renderFiltersPopoverRoot();
}

function ageLabel() {
  const m = filters.age.mode;
  if (m === "any") return t("any");
  if (m === "eq") return `${t("equal_to")} ${(filters.age.a ?? "").toString().trim()}`;
  if (m === "gt") return `${t("greater_than")} ${(filters.age.a ?? "").toString().trim()}`;
  if (m === "lt") return `${t("less_than")} ${(filters.age.a ?? "").toString().trim()}`;
  if (m === "range")
    return t("between_a_b", {
      a: (filters.age.a ?? "").toString().trim(),
      b: (filters.age.b ?? "").toString().trim(),
    });
  return t("any");
}

function genderLabel() {
  if (filters.gender === "male") return t("male");
  if (filters.gender === "female") return t("female");
  if (filters.gender === "other") return t("other");
  return t("all");
}

function relativeFilterLabel() {
  return (filters.relativeName || "").trim() ? filters.relativeName.trim() : "";
}

// ---------------- Scope chips + rules ----------------
function refreshChipLabels() {
  const chipVoter = $("chipVoter");
  const chipRelative = $("chipRelative");
  const chipAnywhere = $("chipAnywhere");
  if (!chipVoter || !chipRelative || !chipAnywhere) return;

  if (searchScope === SCOPE.VOTER) chipVoter.textContent = t("chip_voter");
  else chipVoter.textContent = t("chip_voter_plain");

  chipRelative.textContent = t("chip_relative_plain");
  chipAnywhere.textContent = t("chip_anywhere_plain");
}

function setActiveChip(scope) {
  searchScope = scope;

  $("chipVoter").classList.toggle("active", scope === SCOPE.VOTER);
  $("chipRelative").classList.toggle("active", scope === SCOPE.RELATIVE);
  $("chipAnywhere").classList.toggle("active", scope === SCOPE.ANYWHERE);

  refreshChipLabels();

  const enabled = scope === SCOPE.VOTER;
  moreFiltersBtn.disabled = !enabled;

  if (!enabled) {
    closeFiltersPopover();
    clearFilters();
  }

  refreshOnStateChange("scope");
}

// ---------------- Strict normalization ----------------
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

// ---------------- Exact index normalization (vowel/matra tolerant) ----------------
const INDEP_VOWEL_MAP = new Map(
  Object.entries({
    अ: "A",
    आ: "A",
    इ: "I",
    ई: "I",
    उ: "U",
    ऊ: "U",
    ए: "E",
    ऐ: "E",
    ओ: "O",
    औ: "O",
    ऋ: "R",
    ॠ: "R",
    ऌ: "L",
    ॡ: "L",
  })
);

const MATRA_MAP = new Map(
  Object.entries({
    "ा": "A",
    "ि": "I",
    "ी": "I",
    "ु": "U",
    "ू": "U",
    "े": "E",
    "ै": "E",
    "ो": "O",
    "ौ": "O",
    "ृ": "R",
    "ॄ": "R",
    "ॢ": "L",
    "ॣ": "L",
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

// ---------------- Loose normalization (candidate recall only) ----------------
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

// ---------------- Join variants (query side too) ----------------
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

// ---------------- helpers ----------------
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

function makeKey(ac, row_id) {
  return `${Number(ac)}:${Number(row_id)}`;
}

function isAllACsSelected() {
  return selectedACs.size === 0 || selectedACs.size === districtACsAll.length;
}

function getActiveACs() {
  return isAllACsSelected()
    ? districtACsAll.slice()
    : Array.from(selectedACs)
        .slice()
        .sort((a, b) => a - b);
}

// ---------- DuckDB init ----------
async function initDuckDB() {
  if (db) return;

  // IMPORTANT: host-safe paths under /voter-search/
  const bundles = {
    mvp: {
      mainModule: relUrl("duckdb/duckdb-mvp.wasm"),
      mainWorker: relUrl("duckdb/duckdb-browser-mvp.worker.js"),
      pthreadWorker: null,
    },
    eh: {
      mainModule: relUrl("duckdb/duckdb-eh.wasm"),
      mainWorker: relUrl("duckdb/duckdb-browser-eh.worker.js"),
      pthreadWorker: null,
    },
  };

  const features = await duckdb.getPlatformFeatures();
  const bundle = await duckdb.selectBundle(bundles, features);

  const worker = new Worker(bundle.mainWorker, { type: "module" });
  const logger = new duckdb.ConsoleLogger();
  db = new duckdb.AsyncDuckDB(logger, worker);

  await db.instantiate(bundle.mainModule, bundle.pthreadWorker);
  conn = await db.connect();
}

// ---------- District manifest ----------
const FALLBACK_DISTRICT_MAP = [
  { id: "Sahebganj", label: "Sahebganj", acs: [1, 2, 3] },
  { id: "Pakur", label: "Pakur", acs: [4, 5, 6] },
  { id: "Dumka", label: "Dumka", acs: [7, 10, 11, 12] },
  { id: "Jamtara", label: "Jamtara", acs: [8, 9] },
  { id: "Deoghar", label: "Deoghar", acs: [13, 14, 15] },
  { id: "Godda", label: "Godda", acs: [16, 17, 18] },
  { id: "Kodarma", label: "Kodarma", acs: [19] },
  { id: "Hazaribagh", label: "Hazaribagh", acs: [20, 21, 24, 25] },
  { id: "Ramgarh", label: "Ramgarh", acs: [22, 23] },
  { id: "Chatra", label: "Chatra", acs: [26, 27] },
  { id: "Giridih", label: "Giridih", acs: [28, 29, 30, 31, 32, 33] },
  { id: "Bokaro", label: "Bokaro", acs: [34, 35, 36, 37] },
  { id: "Dhanbad", label: "Dhanbad", acs: [38, 39, 40, 41, 42, 43] },
  { id: "East Singhbhum", label: "East Singhbhum", acs: [44, 45, 46, 47, 48, 49] },
  { id: "Saraikela-Kharswan", label: "Saraikela-Kharswan", acs: [50, 51, 57] },
  { id: "West Singhbhum", label: "West Singhbhum", acs: [52, 53, 54, 55, 56] },
  { id: "Ranchi", label: "Ranchi", acs: [58, 61, 62, 63, 64, 65, 66] },
  { id: "Khunti", label: "Khunti", acs: [59, 60] },
  { id: "Gumla", label: "Gumla", acs: [67, 68, 69] },
  { id: "Simdega", label: "Simdega", acs: [70, 71] },
  { id: "Lohardaga", label: "Lohardaga", acs: [72] },
  { id: "Latehar", label: "Latehar", acs: [73, 74] },
  { id: "Palamu", label: "Palamu", acs: [75, 76, 77, 78, 79] },
  { id: "Garhwa", label: "Garhwa", acs: [80, 81] },
];

function normalizeDistrictManifest(raw) {
  const buildFallbackAcLabels = (acs) => {
    const out = {};
    for (const ac of acs || []) {
      const n = Number(ac);
      if (!Number.isFinite(n)) continue;
      const rec = FALLBACK_AC_LABELS[n];
      if (!rec) continue;
      out[n] = {
        en: String(rec.en ?? "").trim(),
        hi: String(rec.hi ?? rec.en ?? "").trim(),
        hinglish: String(rec.hinglish ?? rec.en ?? "").trim(),
      };
    }
    return out;
  };

  const normalizeAcLabels = (rawMap, acs) => {
    const out = buildFallbackAcLabels(acs);
    if (!rawMap || typeof rawMap !== "object") return out;

    for (const [k, v] of Object.entries(rawMap)) {
      const n = Number(k);
      if (!Number.isFinite(n)) continue;

      if (typeof v === "string") {
        const s = String(v || "").trim();
        if (!s) continue;
        out[n] = { en: s, hi: s, hinglish: s };
        continue;
      }

      if (v && typeof v === "object") {
        const en = String(v.en ?? v.label ?? v.name ?? "").trim();
        if (!en) continue;
        out[n] = {
          en,
          hi: String(v.hi ?? v.hindi ?? en).trim() || en,
          hinglish: String(v.hinglish ?? en).trim() || en,
        };
      }
    }

    return out;
  };

  const fallbackWithLabels = () => {
    return {
      districts: (FALLBACK_DISTRICT_MAP || []).map((d) => {
        const base = String(d.label ?? d.id ?? "").trim() || "District";
        const acs = Array.isArray(d.acs) ? d.acs.slice().map(Number).filter(Number.isFinite) : [];
        return {
          id: String(d.id ?? base).trim() || base,
          label: base,
          labels: { en: base, hi: base, hinglish: base },
          acs,
          acLabels: normalizeAcLabels(null, acs),
        };
      }),
    };
  };

  if (!raw) return fallbackWithLabels();

  const normalizeOne = (d) => {
    const id = String(d?.id ?? d?.label ?? d?.name ?? "").trim() || String(d?.name ?? d?.label ?? "District");
    const label = String(d?.label ?? d?.name ?? d?.id ?? "District").trim() || "District";

    const labelsObj = d && typeof d.labels === "object" && d.labels ? d.labels : {};
    const en = String(labelsObj.en ?? d?.label_en ?? label ?? id).trim() || label || id;
    const hi =
      String(labelsObj.hi ?? labelsObj.hindi ?? d?.label_hi ?? d?.label_hindi ?? "").trim() || label || id;
    const hinglish = String(labelsObj.hinglish ?? d?.label_hinglish ?? "").trim() || en || label || id;

    const acs = Array.isArray(d?.acs) ? d.acs.map(Number).filter(Number.isFinite) : [];
    const acLabels = normalizeAcLabels(d?.ac_labels ?? d?.acLabels ?? null, acs);

    return { id, label, labels: { en, hi, hinglish }, acs, acLabels };
  };

  if (Array.isArray(raw.districts)) {
    return {
      districts: raw.districts.map(normalizeOne).filter((d) => d.acs.length > 0),
    };
  }

  if (typeof raw === "object") {
    const districts = [];

    // shape A: raw is { districts: [...] } already handled above
    // shape B: raw is a mapping { "Ranchi": [1,2,...], ... }
    for (const [k, v] of Object.entries(raw)) {
      if (!Array.isArray(v)) continue;
      const acs = v.map(Number).filter(Number.isFinite);
      if (!acs.length) continue;
      const label = String(k || "District").trim() || "District";
      districts.push({
        id: label,
        label,
        labels: { en: label, hi: label, hinglish: label },
        acs,
        acLabels: normalizeAcLabels(null, acs),
      });
    }

    if (districts.length) return { districts };
  }

  return fallbackWithLabels();
}

async function loadDistrictManifest(stateCode) {
  // IMPORTANT: host-safe path under /voter-search/
  const url = relUrl(`data/${stateCode}/district_manifest.json`);
  try {
    const resp = await fetch(url);
    if (!resp.ok) throw new Error(`HTTP ${resp.status}`);
    const raw = await resp.json();
    districtManifest = normalizeDistrictManifest(raw);
  } catch (e) {
    console.warn("Using fallback district mapping because manifest load failed:", e);
    districtManifest = { districts: FALLBACK_DISTRICT_MAP };
  }
}

// Keep hidden select populated for compatibility (not used for UI)
function populateDistrictHiddenSelect() {
  if (!districtSelHidden) return;
  districtSelHidden.innerHTML =
    `<option value="">${escapeHtml(t("select_district"))}</option>` +
    (districtManifest?.districts || [])
      .map((d) => `<option value="${escapeHtml(d.id)}">${escapeHtml(getDistrictLabelForLang(d, getActiveLang()) || d.label)}</option>`)
      .join("");
}

function updateDistrictUI() {
  const label = currentDistrictId ? getDistrictLabelCurrent() || currentDistrictLabel : t("select_district");
  if (districtMirror) districtMirror.textContent = label || t("select_district");
  if (districtMirrorLanding) districtMirrorLanding.textContent = label || t("select_district");
}

function updateSelectedAcText() {
  if (!selectedAcText) return;

  if (!districtACsAll.length) {
    selectedAcText.textContent = t("selected_acs_none");
    return;
  }
  if (isAllACsSelected()) {
    selectedAcText.textContent = t("selected_acs_all");
    return;
  }
  const arr = getActiveACs();
  if (arr.length <= 4)
    selectedAcText.textContent = t("selected_acs_list", {
      list: arr.join(", "),
    });
  else selectedAcText.textContent = t("selected_acs_n", { n: arr.length });
}

// NEW: preload all ACs for selected district (as requested).
// CHANGED: no longer blocks typing/search and no longer calls ac_meta/loadAC per-AC.
// It now triggers warm() in background and immediately enables inputs.
async function preloadDistrictACs(acs, districtLabel) {
  if (!acs || !acs.length) return;

  const token = ++districtPreloadToken;

  // Do NOT block input anymore.
  setDistrictLoading(true);
  try {
    // Immediately mark district as ready from UI standpoint.
    setStatus(t("status_ready_district_loaded", { district: districtLabel, n: acs.length }));
    setMeta("");

    // Warm in background (best-effort). Do not await; do not block UI.
    const districtSlug = slugifyDistrictId(currentDistrictId || "");
    if (districtSlug) {
      // Warm-lite to avoid competing with the first search (full warm can be expensive).
      // If you ever want full warm again, set mode:"full" and pass acs.
      setTimeout(() => {
        callFn(
          "warm",
          { district: districtSlug, state: STATE_CODE_DEFAULT, mode: "lite" },
          { timeoutMs: 12000, retries: 0 }
        ).catch(() => {});
      }, 600);
    }
  } finally {
    // Ensure UI is enabled immediately.
    if (token === districtPreloadToken) {
      setDistrictLoading(false);
      setSearchEnabled(true);
      syncSearchButtonState();

      // iOS opens keyboard only when focus runs in the same trusted tap/click stack.
      try {
        const el = getActiveQueryInput();
        if (el && !el.disabled) {
          try {
            el.focus({ preventScroll: true });
          } catch {
            try {
              el.focus();
            } catch {}
          }

          if (el === qLanding && isLandingVisible()) {
            scrollLandingInputToTopGap(el, LANDING_INPUT_TOP_GAP_PX);
          }
        }
      } catch {}
    }
  }
}

function setDistrictById(id) {
  const d = (districtManifest?.districts || []).find((x) => x.id === id);
  if (!d) return;

  currentDistrictId = d.id;
  currentDistrictLabel = getDistrictLabelForLang(d, getActiveLang());
  districtACsAll = d.acs.slice().map(Number).filter(Number.isFinite);
  selectedACs.clear(); // default = all

  if (districtSelHidden) districtSelHidden.value = d.id;

  updateDistrictUI();
  updateSelectedAcText();

  // Reset view state
  rankedByRelevance = [];
  masterRankedAll = [];
  lastSearchCtx = { districtSlug: "", query: "", scope: "", exactOn: false, searchedACs: new Set() };

  filteredBase = [];
  rankedView = [];
  ageMap = null;
  page = 1;

  // Clear caches (district change => different DB)
  displayCache.clear();
  scoreRowCache.clear();
  genderAgeCache.clear();
  relativeCandidatesCache.clear();

  clearFilters();
  closeFiltersPopover();
  closeDistrictPopovers();
  closeAcPopover();
  closeSortPopover();
  closePageSizePopover();

  // Warm-lite in background (does not block UI)
  preloadDistrictACs(districtACsAll.slice(), currentDistrictLabel);

  syncSearchButtonState();
}

// ---------- Gender domain discovery (per loaded AC) ----------
function normGenderValue(v) {
  const s = String(v ?? "").trim().toLowerCase();
  if (!s) return "other";
  if (s === "m" || s.includes("male") || s.includes("पुरुष") || s.includes("पु") || s.includes("man")) return "male";
  if (s === "f" || s.includes("female") || s.includes("महिला") || s.includes("स्त्री") || s.includes("woman"))
    return "female";
  if (s.includes("other") || s.includes("अन्य") || s === "o") return "other";
  return "other";
}

async function loadGenderDomain() {
  // Turso-backed version: do not query DuckDB. Filters will fetch Gender/Age for candidate ids as needed.
  genderBuckets = { male: new Set(), female: new Set(), other: new Set() };
}

// ---------- Load AC (views swap to that AC) ----------
// CHANGED: decommissioned ac_meta. This is now local-only and never hits the backend.
async function loadAC(stateCode, acNo) {
  current.state = stateCode;
  current.ac = acNo;
  current.meta = null;
  current.loaded = true;

  setMeta(`Loaded AC${String(acNo).padStart(2, "0")}`);
}

// ---------- Candidate generation ----------
async function queryIndexCandidatesBatchForAc(acNo, querySpecs) {
  const districtSlug = slugifyDistrictId(currentDistrictId || "");
  if (!districtSlug) throw new Error("District not selected");
  if (!querySpecs || !querySpecs.length) return [];

  // Batched backend call: 1 request per AC.
  // Use ids-only mode for speed (server skips hit_count/and_hit aggregation).
  const resp = await callFn(
    "candidates",
    {
      district: districtSlug,
      state: current.state || STATE_CODE_DEFAULT,
      ac: Number(acNo),
      ret: "ids",
      queries: querySpecs.map((q) => ({ table: q.table, keys: q.keys })),
    },
    { timeoutMs: 45000, retries: 1 }
  );

  const ids = Array.isArray(resp?.ids) ? resp.ids : [];
  // Ensure numeric + unique (server already returns unique sorted, but stay defensive).
  const out = [];
  const seen = new Set();
  for (const x of ids) {
    const n = Number(x);
    if (!Number.isFinite(n)) continue;
    if (seen.has(n)) continue;
    seen.add(n);
    out.push(n);
  }
  return out;
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

async function getCandidatesForQueryForAc(acNo, q, scope, exactOn) {
  const strictTokens = tokenize(q);
  const strictKeys = buildKeysFromTokens(strictTokens, PREFIX_LEN_STRICT);

  const exactTokens = tokenizeExactIndex(q);
  const exactKeys = buildKeysFromTokens(exactTokens, PREFIX_LEN_EXACT);

  const looseTokens = tokenizeLoose(q);
  const looseKeys = buildKeysFromTokens(looseTokens, PREFIX_LEN_LOOSE);

  if (!strictKeys.length && !exactKeys.length && !looseKeys.length) {
    return { candidates: [], strictKeys, exactKeys, looseKeys };
  }

  const wantLoose = !exactOn;
  const querySpecs = [];
  const push = (tag, table, keys) => {
    if (keys && keys.length) querySpecs.push({ tag, table, keys });
  };

  if (scope === SCOPE.VOTER) {
    push("sv", "idx_voter_strict", strictKeys);
    push("ev", "idx_voter_exact", exactKeys);
    if (wantLoose) push("lv", "idx_voter_loose", looseKeys);
  } else if (scope === SCOPE.RELATIVE) {
    push("sr", "idx_relative_strict", strictKeys);
    push("er", "idx_relative_exact", exactKeys);
    if (wantLoose) push("lr", "idx_relative_loose", looseKeys);
  } else {
    push("sv", "idx_voter_strict", strictKeys);
    push("sr", "idx_relative_strict", strictKeys);
    push("ev", "idx_voter_exact", exactKeys);
    push("er", "idx_relative_exact", exactKeys);
    if (wantLoose) {
      push("lv", "idx_voter_loose", looseKeys);
      push("lr", "idx_relative_loose", looseKeys);
    }
  }

  const candidates = await queryIndexCandidatesBatchForAc(acNo, querySpecs);
  return { candidates, strictKeys, exactKeys, looseKeys };
}
/*
// ---------- Fetch scoring rows (per AC) ----------
function makeKey(ac, row_id) {
  return `${Number(ac)}:${Number(row_id)}`;
}
*/

async function fetchScoreRowsByIdsForAc(acNo, rowIds) {
  if (!rowIds || !rowIds.length) return [];

  const districtSlug = slugifyDistrictId(currentDistrictId || "");
  if (!districtSlug) throw new Error("District not selected");

  const want = rowIds.map((x) => Number(x)).filter(Number.isFinite);
  if (!want.length) return [];

  // Only fetch missing rows.
  const missing = [];
  for (const rid of want) {
    const k = makeKey(acNo, rid);
    if (!scoreRowCache.has(k)) missing.push(rid);
  }

  if (missing.length) {
    for (let i = 0; i < missing.length; i += FETCH_ID_CHUNK) {
      const chunk = missing.slice(i, i + FETCH_ID_CHUNK).map(Number);

      const resp = await callFn(
        "rows",
        {
          district: districtSlug,
          state: current.state || STATE_CODE_DEFAULT,
          ac: Number(acNo),
          kind: "score",
          row_ids: chunk,
        },
        { timeoutMs: 45000, retries: 1 }
      );

      for (const r of resp?.rows || []) {
        const rid = Number(r.row_id);
        const k = makeKey(acNo, rid);
        scoreRowCache.set(k, {
          row_id: rid,
          voter_name_norm: r.voter_name_norm ?? "",
          relative_name_norm: r.relative_name_norm ?? "",
        });
      }
    }
  }

  // Return in input order.
  const out = [];
  for (const rid of want) {
    const k = makeKey(acNo, rid);
    const r = scoreRowCache.get(k);
    if (r) out.push(r);
  }
  return out;
}

// ---------- Worker ----------
let worker;
let pendingResolve = null;
let pendingReject = null;
let pendingProgress = null;

function initWorker() {
  if (worker) return;

  // Harden worker URL resolution (safe under subfolders / different base href)
  worker = new Worker(new URL("./worker.js", import.meta.url), { type: "module" });

  worker.onmessage = async (ev) => {
    const msg = ev.data;

    if (msg.type === "progress") {
      const { done, total, phase, candidates } = msg;
      if (typeof pendingProgress === "function") {
        pendingProgress({ done, total, phase, candidates });
      }
      return;
    }

    if (msg.type === "done") {
      const ranked = (msg.ranked || []).map((x, i) => ({
        row_id: Number(x.row_id),
        sortKey: Array.isArray(x.key) ? x.key : [999999999, i],
        explain: x.explain ?? "",
        match_field: x.match_field ?? "",
      }));

      if (pendingResolve) {
        const r = pendingResolve;
        pendingResolve = null;
        pendingReject = null;
        pendingProgress = null;
        r(ranked);
      }
      return;
    }

    if (msg.type === "error") {
      setStatus(`Worker error: ${msg.message}`);
      if (pendingReject) {
        const rej = pendingReject;
        pendingResolve = null;
        pendingReject = null;
        pendingProgress = null;
        rej(new Error(msg.message));
      }
      return;
    }
  };
}

function runWorkerRanking(rowsWithMeta, qStrict, exactOn, scopeForWorker, onProgress) {
  initWorker();
  return new Promise((resolve, reject) => {
    pendingResolve = resolve;
    pendingReject = reject;
    pendingProgress = typeof onProgress === "function" ? onProgress : null;

    worker.postMessage({
      type: "start",
      query: qStrict,
      scope: scopeForWorker,
      exactOn,
      total: rowsWithMeta.length,
    });

    for (let i = 0; i < rowsWithMeta.length; i += SCORE_BATCH) {
      const batch = rowsWithMeta.slice(i, i + SCORE_BATCH);
      worker.postMessage({ type: "batch", rows: batch });
    }
    worker.postMessage({ type: "finish" });
  });
}

// ---------- PDF link ----------
function buildPdfUrl(row) {
  const state = formatCell(row["State Code"]);
  const ac = formatCell(row["AC No"]);
  const part = formatCell(row["Part No"]);
  if (!state || !ac || !part) return "";
  return `https://www.eci.gov.in/sir/f3/${state}/data/OLDSIRROLL/${state}/${ac}/${state}_${ac}_${part}.pdf`;
}

// ---------- Display fetch (per AC, cached) ----------
async function fetchDisplayRowsByIdsForAc(acNo, rowIds) {
  if (!rowIds || !rowIds.length) return [];

  const districtSlug = slugifyDistrictId(currentDistrictId || "");
  if (!districtSlug) throw new Error("District not selected");

  const want = rowIds.map((x) => Number(x)).filter(Number.isFinite);
  if (!want.length) return [];

  const missing = [];
  for (const rid of want) {
    const k = makeKey(acNo, rid);
    if (!displayCache.has(k)) missing.push(rid);
  }

  if (missing.length) {
    // Display pages are small, but still chunk for safety.
    for (let i = 0; i < missing.length; i += 1500) {
      const chunk = missing.slice(i, i + 1500);
      const resp = await callFn(
        "rows",
        {
          district: districtSlug,
          state: current.state || STATE_CODE_DEFAULT,
          ac: Number(acNo),
          kind: "display",
          row_ids: chunk,
        },
        { timeoutMs: 45000, retries: 1 }
      );

      const rows = resp?.rows || [];
      for (const r of rows) {
        const out = { ...r };
        out.row_id = Number(out.row_id);
        const k = makeKey(acNo, out.row_id);
        displayCache.set(k, out);
      }
    }
  }

  const out = [];
  for (const rid of want) {
    const k = makeKey(acNo, rid);
    const r = displayCache.get(k);
    if (r) out.push(r);
  }
  return out;
}

// ---------- Sorting ----------
function parseAgeValue(v) {
  if (v === null || v === undefined) return null;
  const s = String(v).trim();
  const m = s.match(/(\d+)/);
  if (!m) return null;
  const n = Number(m[1]);
  return Number.isFinite(n) ? n : null;
}

async function ensureAgeMapLoaded(keysToLoad) {
  if (!ageMap) ageMap = new Map();
  if (!keysToLoad || !keysToLoad.length) return;
  if (keysToLoad.every((k) => ageMap.has(k))) return;

  const districtSlug = slugifyDistrictId(currentDistrictId || "");
  if (!districtSlug) throw new Error("District not selected");

  const byAc = new Map();
  for (const key of keysToLoad) {
    if (ageMap.has(key)) continue;
    // If we already loaded Gender/Age for this row during filtering, reuse it.
    if (genderAgeCache.has(key)) {
      const g = genderAgeCache.get(key);
      if (g && "age" in g) ageMap.set(key, g.age);
      continue;
    }

    const [acStr, ridStr] = String(key).split(":");
    const ac = Number(acStr);
    const rid = Number(ridStr);
    if (!Number.isFinite(ac) || !Number.isFinite(rid)) continue;
    if (!byAc.has(ac)) byAc.set(ac, []);
    byAc.get(ac).push(rid);
  }

  let done = 0;
  const total = keysToLoad.length;
  setStatus(t("status_preparing_age_sort"));

  for (const [ac, rids] of byAc.entries()) {
    for (let i = 0; i < rids.length; i += FETCH_ID_CHUNK) {
      const chunk = rids.slice(i, i + FETCH_ID_CHUNK).map(Number);

      const resp = await callFn(
        "rows",
        {
          district: districtSlug,
          state: current.state || STATE_CODE_DEFAULT,
          ac: Number(ac),
          kind: "age",
          row_ids: chunk,
        },
        { timeoutMs: 45000, retries: 1 }
      );

      for (const r of resp?.rows || []) {
        const rid = Number(r.row_id);
        const k = makeKey(ac, rid);
        const age = parseAgeValue(r.Age ?? r.age);
        ageMap.set(k, age);
        // Keep in unified cache too.
        if (!genderAgeCache.has(k)) genderAgeCache.set(k, { age, gender: "" });
        done++;
      }
    }
  }

  setStatus(t("status_age_sort_ready", { done, total }));
}

function setSortMode(mode) {
  sortMode = mode || SORT.RELEVANCE;

  sortText.textContent =
    sortMode === SORT.AGE_ASC
      ? t("sort_by_age_up")
      : sortMode === SORT.AGE_DESC
      ? t("sort_by_age_down")
      : t("sort_by_relevance");
}

async function applySort() {
  if (sortMode === SORT.RELEVANCE) {
    rankedView = filteredBase.slice(); // already ordered by relevance
    return;
  }

  const keys = filteredBase.map((x) => x.key);
  await ensureAgeMapLoaded(keys);

  const dir = sortMode === SORT.AGE_DESC ? -1 : 1;

  rankedView = filteredBase.slice().sort((a, b) => {
    const aa = ageMap.get(a.key) ?? null;
    const bb = ageMap.get(b.key) ?? null;

    const aMissing = aa === null;
    const bMissing = bb === null;

    if (aMissing && bMissing) return cmpRelevance(a, b);
    if (aMissing) return 1;
    if (bMissing) return -1;

    if (aa !== bb) return (aa - bb) * dir;
    return cmpRelevance(a, b);
  });
}

// ---------- Filters application (post-ranking) ----------
function getIncludeTypingChecked() {
  return isResultsVisible() ? exactToggleResults.checked : exactToggleLanding.checked;
}

function setIncludeTypingChecked(v) {
  exactToggleLanding.checked = !!v;
  exactToggleResults.checked = !!v;
}

function exactOnFromIncludeTyping() {
  // include typing mistakes ON => exact OFF
  return !Boolean(getIncludeTypingChecked());
}

function updateMoreFiltersEnabled() {
  moreFiltersBtn.disabled = !(searchScope === SCOPE.VOTER);
}

// Compute row-id set by Gender/Age for ONE AC
// Uses a cache so repeated filter/sort changes do NOT re-hit rows().
async function computeRowIdSetByGenderAndAgeForAc(acNo, rowIdsInThisAc) {
  const hasGender = filters.gender !== "all";
  const hasAge = filters.age.mode !== "any";
  if (!hasGender && !hasAge) return null;
  if (!rowIdsInThisAc || !rowIdsInThisAc.length) return new Set();

  const districtSlug = slugifyDistrictId(currentDistrictId || "");
  if (!districtSlug) throw new Error("District not selected");

  const want = rowIdsInThisAc.map((x) => Number(x)).filter(Number.isFinite);
  const missing = [];
  for (const rid of want) {
    const k = makeKey(acNo, rid);
    const ga = genderAgeCache.get(k);
    if (!ga) {
      missing.push(rid);
      continue;
    }
    if (hasGender && !ga.gender) {
      missing.push(rid);
      continue;
    }
    if (hasAge && !("age" in ga)) {
      missing.push(rid);
      continue;
    }
  }

  if (missing.length) {
    for (let i = 0; i < missing.length; i += FETCH_ID_CHUNK) {
      const chunk = missing.slice(i, i + FETCH_ID_CHUNK).map(Number);
      const resp = await callFn(
        "rows",
        {
          district: districtSlug,
          state: current.state || STATE_CODE_DEFAULT,
          ac: Number(acNo),
          kind: "gender_age",
          row_ids: chunk,
        },
        { timeoutMs: 45000, retries: 1 }
      );

      for (const r of resp?.rows || []) {
        const rid = Number(r.row_id);
        const k = makeKey(acNo, rid);
        const gender = normGenderValue(r.Gender ?? r.gender);
        const age = parseAgeValue(r.Age ?? r.age);
        genderAgeCache.set(k, { gender, age });
      }
    }
  }

  const out = new Set();
  for (const rid of want) {
    const k = makeKey(acNo, rid);
    const ga = genderAgeCache.get(k);
    if (!ga) continue;

    if (hasGender) {
      if (filters.gender !== ga.gender) continue;
    }

    if (hasAge) {
      const age = ga.age;
      const a = Number(filters.age.a);
      const b = Number(filters.age.b);

      if (filters.age.mode === "eq" && Number.isFinite(a)) {
        if (age === null || age !== a) continue;
      }
      if (filters.age.mode === "gt" && Number.isFinite(a)) {
        if (age === null || age <= a) continue;
      }
      if (filters.age.mode === "lt" && Number.isFinite(a)) {
        if (age === null || age >= a) continue;
      }
      if (filters.age.mode === "range" && Number.isFinite(a) && Number.isFinite(b)) {
        const lo = Math.min(a, b),
          hi = Math.max(a, b);
        if (age === null || age < lo || age > hi) continue;
      }
    }

    out.add(rid);
  }

  return out;
}

async function computeRowIdSetByRelativeFilterForAc(acNo, exactOn) {
  const rel = norm(filters.relativeName || "");
  if (!rel) return null;

  const districtSlug = slugifyDistrictId(currentDistrictId || "");
  if (!districtSlug) throw new Error("District not selected");
  const cacheKey = `${districtSlug}|${acNo}|${exactOn ? 1 : 0}|${rel}`;
  if (relativeCandidatesCache.has(cacheKey)) return relativeCandidatesCache.get(cacheKey);

  const { candidates } = await getCandidatesForQueryForAc(acNo, rel, SCOPE.RELATIVE, exactOn);
  const s = new Set(candidates.map(Number));
  relativeCandidatesCache.set(cacheKey, s);
  return s;
}

async function applyFiltersThenSortThenRender() {
  filteredBase = rankedByRelevance.slice();

  if (searchScope !== SCOPE.VOTER) {
    clearFilters();
  }

  if (searchScope === SCOPE.VOTER && rankedByRelevance.length) {
    const exactOn = exactOnFromIncludeTyping();

    const hasRel = Boolean(norm(filters.relativeName || ""));
    const hasGender = filters.gender !== "all";
    const hasAge = filters.age.mode !== "any";

    if (hasRel || hasGender || hasAge) {
      const byAc = new Map();
      for (const x of rankedByRelevance) {
        if (!byAc.has(x.ac)) byAc.set(x.ac, []);
        byAc.get(x.ac).push(x.row_id);
      }

      const entries = Array.from(byAc.entries());
      const total = entries.length;
      let done = 0;

      setStatus(t("status_applying_filters"));

      const perAcKeeps = await runPool(entries, MAX_FILTER_IN_FLIGHT_AC, async ([ac, rowIds]) => {
        // Best-effort: if a backend call fails, DO NOT drop rows; keep everything for that AC.
        let relSet = null;
        if (hasRel) {
          try {
            relSet = await computeRowIdSetByRelativeFilterForAc(ac, exactOn);
          } catch (e) {
            console.warn("Relative filter failed for AC", ac, e);
            relSet = null; // keep all
          }
        }

        let gaSet = null;
        if (hasGender || hasAge) {
          try {
            gaSet = await computeRowIdSetByGenderAndAgeForAc(ac, rowIds);
          } catch (e) {
            console.warn("Gender/Age filter fetch failed for AC", ac, e);
            gaSet = null; // keep all
          }
        }

        const keep = [];
        for (const rid of rowIds) {
          if (relSet && !relSet.has(rid)) continue;
          if (gaSet && !gaSet.has(rid)) continue;
          keep.push(rid);
        }

        done++;
        setStatus(t("status_applying_filters_ac", { ac, i: done, n: total }));
        return { ac, keep };
      });

      const allowedKeys = new Set();
      for (const item of perAcKeeps || []) {
        if (!item) continue;
        const ac = item.ac;
        for (const rid of item.keep) allowedKeys.add(makeKey(ac, rid));
      }

      filteredBase = allowedKeys.size ? rankedByRelevance.filter((x) => allowedKeys.has(x.key)) : [];
    }
  }

  await applySort();

  page = 1;
  // IMPORTANT: don't clear displayCache here.
  // Filtering/sorting should not re-fetch rows if we already fetched them for paging.

  await renderPage();
  resultsCountEl.textContent = String(rankedView.length || 0);

  updateMoreFiltersEnabled();
  renderFiltersPopoverRoot();
  renderSortPopover();
  renderPageSizePopover();
}

// ---------- Search ----------
async function computeMergedForAcs(acList, qStrict, exactOn, scopeForWorker, myToken) {
  const merged = [];
  if (!acList || !acList.length) return merged;

  const totalAcs = acList.length;

  let prepared = 0;  // candidates+score-rows fetched (or failed)
  let processed = 0; // fully handled (worker ran or skipped)
  let started = 0;

  const prepareAc = async (ac) => {
    setProgressStage(exactOn ? t("status_stage1_exact", { ac }) : t("status_stage1_loose", { ac }));
    const { candidates } = await getCandidatesForQueryForAc(ac, qStrict, searchScope, exactOn);
    if (!candidates.length) return { ac, rows: [], candidates: 0 };

    setProgressStage(t("status_stage2", { n: candidates.length, ac }));
    const rows = await fetchScoreRowsByIdsForAc(ac, candidates);
    return { ac, rows, candidates: candidates.length };
  };

  let nextIndex = 0;
  let inFlight = 0;

  const ready = [];
  let notifyResolve = null;
  const notify = () => {
    if (notifyResolve) {
      const r = notifyResolve;
      notifyResolve = null;
      r();
    }
  };
  const waitReady = () => new Promise((res) => (notifyResolve = res));

  const pushReady = (item) => {
    prepared++;
    ready.push(item);
    notify();
  };

  const scheduleMore = () => {
    while (inFlight < MAX_PREP_IN_FLIGHT_AC && nextIndex < acList.length) {
      const ac = acList[nextIndex++];
      started++;
      setProgressStage(t("status_stage0", { ac, i: started, n: totalAcs }));
      setBar((processed / totalAcs) * 100);

      inFlight++;
      prepareAc(ac)
        .then((val) => pushReady({ ok: true, ac, val }))
        .catch((err) => pushReady({ ok: false, ac, err }))
        .finally(() => {
          inFlight--;
          scheduleMore();
          notify();
        });
    }
  };

  scheduleMore();

  while (processed < totalAcs) {
    if (myToken !== activeSearchToken) return null; // cancelled

    if (!ready.length) {
      await waitReady();
      continue;
    }

    const settled = ready.shift();

    if (!settled || !settled.ok) {
      processed++;
      setBar((processed / totalAcs) * 100);
      setProgressStage(t("status_stage0", { ac: settled?.ac ?? "?", i: processed, n: totalAcs }));
      console.warn("Skipping AC due to prepare error:", settled?.ac, settled?.err);
      continue;
    }

    const { ac, rows } = settled.val;

    if (rows.length) {
      setProgressStage(t("status_stage3", { n: rows.length, ac }));
      const ranked = await runWorkerRanking(rows, qStrict, exactOn, scopeForWorker, ({ done, total, phase }) => {
        const local = total > 0 ? Math.max(0, Math.min(1, done / total)) : 0;
        const overall = ((processed + local) / totalAcs) * 100;
        setBar(overall);
        setProgressStage(formatWorkerStage(phase, ac, processed + 1, totalAcs));
      });
      for (const r of ranked) {
        merged.push({
          key: makeKey(ac, r.row_id),
          ac,
          row_id: r.row_id,
          sortKey: r.sortKey,
          explain: DEBUG ? r.explain : "",
          match_field: DEBUG ? r.match_field : "",
        });
      }
      resultsCountEl.textContent = String(merged.length);
    }

    processed++;
    setBar((processed / totalAcs) * 100);
    setProgressStage(t("status_stage0", { ac, i: processed, n: totalAcs }));
  }
  return merged;
}

async function runSearch() {
  const q = getActiveQueryInput().value || "";
  const qStrict = norm(q);

  current.lastQuery = qStrict;
  qLanding.value = qStrict;
  qResults.value = qStrict;

  rankedByRelevance = [];
  filteredBase = [];
  rankedView = [];
  ageMap = null;
  page = 1;

  pagerEl.style.display = "none";
  $("results").innerHTML = "";
  
  showLoader();

  setResultsProgressVisible(false);
  setBar(0);
  setProgressStage("");
  setProgressSub("");

  if (!qStrict) {
    setStatus(t("status_enter_query"));
    syncSearchButtonState();
    return;
  }

  if (!districtACsAll.length) {
    setStatus(t("status_select_district_first"));
    return;
  }

  const exactOn = exactOnFromIncludeTyping();

  // IMPORTANT FIX: pass actual scope including ANYWHERE to worker
  const scopeForWorker = searchScope;

  const acList = getActiveACs();
  if (!acList.length) {
    setStatus(t("status_no_acs_selected"));
    return;
  }

  showResults();
  resultsCountEl.textContent = "0";
  maybeShowResultsToastOnce();
  setResultsProgressVisible(true);

  // Cancel/ignore older searches.
  const myToken = ++activeSearchToken;

  const districtSlug = slugifyDistrictId(currentDistrictId || "");
  masterRankedAll = [];
  lastSearchCtx = { districtSlug, query: qStrict, scope: scopeForWorker, exactOn, searchedACs: new Set() };

  let merged;
  try {
    merged = await computeMergedForAcs(acList, qStrict, exactOn, scopeForWorker, myToken);
  } finally {
    setBar(100);
    setResultsProgressVisible(false);
    setProgressStage("");
    setProgressSub("");
  }
  if (merged === null) return;

  // GLOBAL merge sort: compare worker rank keys (NOT per-AC arbitrary scores)
  masterRankedAll = merged.sort(cmpRelevance);
  lastSearchCtx.searchedACs = new Set(acList);

  rankedByRelevance = masterRankedAll.slice(); // current view = searched ACs

  await applyFiltersThenSortThenRender();
  setStatus(t("status_ready_results", { n: rankedView.length }));

  if (DEBUG && rankedView.length) {
    console.log("DEBUG top 20:");
    for (const x of rankedView.slice(0, 20)) {
      console.log(x.key, x.sortKey, x.explain);
    }
  }
}

// ---------- Render table ----------
async function renderPage() {
  const total = rankedView.length;
  const totalPages = Math.max(1, Math.ceil(total / pageSize));
  page = Math.max(1, Math.min(page, totalPages));

  const start = (page - 1) * pageSize;
  const end = Math.min(total, start + pageSize);
  const slice = rankedView.slice(start, end);

  const missingByAc = new Map();
  for (const x of slice) {
    if (!displayCache.has(x.key)) {
      if (!missingByAc.has(x.ac)) missingByAc.set(x.ac, []);
      missingByAc.get(x.ac).push(x.row_id);
    }
  }

  if (missingByAc.size) {
    const entries = Array.from(missingByAc.entries());
    const totalAcs = entries.length;
    let done = 0;

    await runPool(entries, MAX_DISPLAY_IN_FLIGHT_AC, async ([ac, rowIds]) => {
      setStatus(t("status_loading_page_rows", { page, ac, i: done + 1, n: totalAcs }));
      await fetchDisplayRowsByIdsForAc(ac, rowIds);
      done++;
      setStatus(t("status_loading_page_rows", { page, ac, i: done, n: totalAcs }));
      return true;
    });
  }

  const infoMap = new Map(slice.map((x) => [x.key, x]));
  const orderedRows = slice.map((x) => displayCache.get(x.key)).filter(Boolean);

  $("results").innerHTML = renderTable(orderedRows, infoMap);

  pagerEl.style.display = total > 0 ? "flex" : "none";
  currentPageCount.textContent = t("page_x_of_y", { p: page, t: totalPages });
  pageInfo.textContent = t("showing_prefix", { from: total ? start + 1 : 0, to: end });
  resultsCountEl.textContent = String(total);

  if (total) setStatus(t("status_showing_range", { from: start + 1, to: end, total }));
  else setStatus(t("status_ready_results", { n: 0 }));

  syncTableHeaderShadowOffset();
  syncStickyColScrollShadow();
}

function renderTable(rows, infoMap) {
  const headerDefs = [
    { key: "Voter Name", label: headerLabelForKey("Voter Name") },
    { key: "Relative Name", label: headerLabelForKey("Relative Name") },
    { key: "Relation", label: headerLabelForKey("Relation") },
    { key: "Gender", label: headerLabelForKey("Gender") },
    { key: "Age", label: headerLabelForKey("Age") },
    { key: "House No", label: headerLabelForKey("House No") },
    { key: "Serial No", label: headerLabelForKey("Serial No") },
    { key: "Page No", label: headerLabelForKey("Page No") },
    { key: "Part No", label: headerLabelForKey("Part No") },
    { key: "AC No", label: headerLabelForKey("AC No") },
    { key: "ID", label: headerLabelForKey("ID") },
    { key: "__PDF__", label: t("h_pdf") },
  ];

  const thead = `
    <thead>
      <tr>
        ${headerDefs
          .map((h) => {
            const sticky = h.key === STICKY_COL_KEY ? "stickyCol" : "";
            return `<th class="${sticky}">${escapeHtml(h.label)}</th>`;
          })
          .join("")}
      </tr>
    </thead>
  `;

  const tbody = `
    <tbody>
      ${rows
        .map((r) => {
          const pdfUrl = buildPdfUrl(r);
          const k = makeKey(Number(r["AC No"] || 0), Number(r.row_id));
          const info = infoMap.get(k);

          return `
          <tr>
            ${headerDefs
              .map((h) => {
                const sticky = h.key === STICKY_COL_KEY ? "stickyCol" : "";

                if (h.key === "__PDF__") {
                  return `<td>${
                    pdfUrl
                      ? `<a href="${escapeHtml(pdfUrl)}" target="_blank" rel="noopener noreferrer">${escapeHtml(
                          t("open_pdf")
                        )}</a>`
                      : ""
                  }</td>`;
                }

                const val = formatCell(r[h.key]);

                let title = "";
                if (h.key === "Voter Name" && DEBUG && info) {
                  const ttxt =
                    `rankKey: ${JSON.stringify(info.sortKey)} | ` +
                    (info.match_field ? `field=${info.match_field} | ` : "") +
                    (info.explain || "");
                  title = `title="${escapeHtml(ttxt)}"`;
                }

                return `<td class="${sticky}" ${title}>${escapeHtml(val)}</td>`;
              })
              .join("")}
          </tr>
        `;
        })
        .join("")}
    </tbody>
  `;

  return `<table>${thead}${tbody}</table>`;
}

// ---------- IME-safe Enter ----------
function wireIMEEnter(inputEl, onEnter) {
  let isComposing = false;
  inputEl.addEventListener("compositionstart", () => {
    isComposing = true;
  });
  inputEl.addEventListener("compositionend", () => {
    isComposing = false;
  });

  inputEl.addEventListener("keydown", (e) => {
    if (e.key === "Enter" && !isComposing && !e.isComposing) {
      e.preventDefault();
      onEnter();
    }
  });
}

// ---------- Clear ----------
function clearAll() {
  qLanding.value = "";
  qResults.value = "";
  current.lastQuery = "";

  rankedByRelevance = [];
  masterRankedAll = [];
  lastSearchCtx = { districtSlug: "", query: "", scope: "", exactOn: false, searchedACs: new Set() };

  filteredBase = [];
  rankedView = [];
  ageMap = null;
  displayCache.clear();

  clearFilters();
  closeFiltersPopover();

  $("results").innerHTML = "";
  pagerEl.style.display = "none";
  resultsCountEl.textContent = "0";

  setBar(0);
  setStatus(t("status_cleared"));
  syncSearchButtonState();

  showLanding();
}

// ---------- Auto refresh behavior ----------
let refreshTimer = null;

function refreshOnStateChange(reason) {
  if (refreshTimer) clearTimeout(refreshTimer);
  refreshTimer = setTimeout(async () => {
    refreshTimer = null;

    // AC selection changes should NOT rerun the whole search if we already have results
    // for the same (district, query, scope, exactOn). We just filter/expand incrementally.
    if (reason === "acs") {
      const qStrict = current.lastQuery || lastSearchCtx?.query || norm(getActiveQueryInput().value || "");
      const districtSlug = slugifyDistrictId(currentDistrictId || "");
      const exactOn = exactOnFromIncludeTyping();
      const scopeForWorker = searchScope;

      // Fast path: reuse already-ranked rows and only expand for newly-selected ACs.
      if (
        qStrict &&
        lastSearchCtx &&
        masterRankedAll &&
        masterRankedAll.length &&
        districtSlug === lastSearchCtx.districtSlug &&
        qStrict === lastSearchCtx.query &&
        scopeForWorker === lastSearchCtx.scope &&
        exactOn === lastSearchCtx.exactOn
      ) {
        const targetAcs = getActiveACs();
        const searched = lastSearchCtx.searchedACs || new Set();
        const missing = targetAcs.filter((ac) => !searched.has(ac));

        // If we need to expand to new ACs, do it incrementally (no rework for already-scored ACs)
        if (missing.length) {
          showResults();
          const myToken = ++activeSearchToken;

          setBar(0);
          setStatus(t("status_stage0", { ac: missing[0] || 0, i: 1, n: missing.length }));

          const merged = await computeMergedForAcs(missing, qStrict, exactOn, scopeForWorker, myToken);
          if (merged !== null && merged.length) {
            masterRankedAll = masterRankedAll.concat(merged).sort(cmpRelevance);
            for (const ac of missing) searched.add(ac);
            lastSearchCtx.searchedACs = searched;
          }
        }

        // Now just filter the already-ranked master list by selected ACs.
        const targetSet = new Set(getActiveACs());
        rankedByRelevance = masterRankedAll.filter((x) => targetSet.has(x.ac));

        await applyFiltersThenSortThenRender();
        setStatus(t("status_ready_results", { n: rankedView.length }));
        return;
      }

      // Safe fallback: force a rerun for the current displayed query.
      if (qStrict) {
        current.lastQuery = qStrict;
        qLanding.value = qStrict;
        qResults.value = qStrict;
        await runSearch();
      }
      return;
    }

    // Default behavior for query/scope/exact changes: rerun
    if (current.lastQuery) {
      await runSearch();
    }
  }, 250);
}

// ---------- Popover helpers ----------
function popRow({ left, right, chevron = true, selected = false, onClick }) {
  const div = document.createElement("div");
  div.className = `popRow${selected ? " popSelected" : ""}`;
  div.innerHTML = `
    <div class="popLeft">${escapeHtml(left)}</div>
    <div class="popRight">
      ${right ? `<span>${escapeHtml(right)}</span>` : ""}
      ${chevron ? `<span class="popChevron" aria-hidden="true"></span>` : ""}
    </div>
  `;
  div.onclick = onClick;
  return div;
}

/* ----------------- the rest of your UI popovers + modal code -----------------
   UNCHANGED from your paste (filters popover, district popovers, AC popover,
   sort popover, page size popover, enhancements init, outside click handling,
   and boot logic).
----------------------------------------------------------------------------- */

// ---------- Filters popover ----------
let popView = "root"; // root | gender | age

function openFiltersPopover() {
  if (moreFiltersBtn.disabled) return;
  filtersPopover.style.display = "block";
  filtersPopover.setAttribute("aria-hidden", "false");
  popView = "root";
  renderFiltersPopoverRoot();
}

function closeFiltersPopover() {
  filtersPopover.style.display = "none";
  filtersPopover.setAttribute("aria-hidden", "true");
  popView = "root";
}

function renderFiltersPopoverRoot() {
  if (filtersPopover.style.display === "none") return;
  popView = "root";
  filtersPopover.innerHTML = "";

  const g = popRow({
    left: t("filter_gender"),
    right: genderLabel(),
    chevron: true,
    onClick: () => renderFiltersPopoverGender(),
  });

  const a = popRow({
    left: t("filter_age"),
    right: filters.age.mode === "any" ? t("any") : ageLabel(),
    chevron: true,
    onClick: () => renderFiltersPopoverAge(),
  });

  const r = popRow({
    left: t("filter_relative_name"),
    right: relativeFilterLabel(),
    chevron: false,
    onClick: () => openRelativeNameModal(),
  });

  filtersPopover.appendChild(g);
  filtersPopover.appendChild(document.createElement("div")).className = "popSep";
  filtersPopover.appendChild(a);
  filtersPopover.appendChild(document.createElement("div")).className = "popSep";
  filtersPopover.appendChild(r);
}

function renderFiltersPopoverGender() {
  popView = "gender";
  filtersPopover.innerHTML = "";

  const back = popRow({
    left: t("back"),
    right: "",
    chevron: false,
    onClick: () => renderFiltersPopoverRoot(),
  });
  back.classList.add("popBack");
  filtersPopover.appendChild(back);
  filtersPopover.appendChild(document.createElement("div")).className = "popSep";

  const opts = [
    { k: "male", label: t("male") },
    { k: "female", label: t("female") },
    { k: "other", label: t("other") },
    { k: "all", label: t("all") },
  ];

  for (const o of opts) {
    const row = popRow({
      left: o.label,
      right: "",
      chevron: false,
      selected: filters.gender === o.k,
      onClick: () => {
        filters.gender = o.k;
        renderFiltersPopoverGender();
        refreshOnStateChange("filters");
      },
    });
    filtersPopover.appendChild(row);
  }
}

function renderFiltersPopoverAge() {
  popView = "age";
  filtersPopover.innerHTML = "";

  const back = popRow({
    left: t("back"),
    right: "",
    chevron: false,
    onClick: () => renderFiltersPopoverRoot(),
  });
  back.classList.add("popBack");
  filtersPopover.appendChild(back);
  filtersPopover.appendChild(document.createElement("div")).className = "popSep";

  const opts = [
    { k: "eq", label: t("equal_to") },
    { k: "gt", label: t("greater_than") },
    { k: "lt", label: t("less_than") },
    { k: "range", label: t("range") },
  ];

  for (const o of opts) {
    const right =
      filters.age.mode === o.k
        ? o.k === "range"
          ? t("between_a_b", {
              a: (filters.age.a ?? "").toString().trim(),
              b: (filters.age.b ?? "").toString().trim(),
            })
          : `${(filters.age.a ?? "").toString().trim()}`
        : "";

    const row = popRow({
      left: o.label,
      right,
      chevron: false,
      selected: filters.age.mode === o.k,
      onClick: () => openAgeModal(o.k),
    });
    filtersPopover.appendChild(row);
  }

  filtersPopover.appendChild(document.createElement("div")).className = "popSep";

  filtersPopover.appendChild(
    popRow({
      left: t("any"),
      right: "",
      chevron: false,
      selected: filters.age.mode === "any",
      onClick: () => {
        filters.age = { mode: "any", a: null, b: null };
        renderFiltersPopoverAge();
        refreshOnStateChange("filters");
      },
    })
  );
}

// ---------- Modal helpers ----------
let modalState = null;
let modalEnhancers = [];
let modalEnhancerOutsideHandler = null;

function openModal({ title, subtitle, fields, onDone }) {
  // cleanup any previous modal enhancers
  try {
    (modalEnhancers || []).forEach((e) => e?.close?.());
  } catch {}
  modalEnhancers = [];
  if (modalEnhancerOutsideHandler) {
    try {
      document.removeEventListener("mousedown", modalEnhancerOutsideHandler, true);
    } catch {}
    modalEnhancerOutsideHandler = null;
  }

  modalState = { onDone, fields };

  modalTitle.textContent = title;
  modalSubtitle.textContent = subtitle;

  modalFields.innerHTML = "";

  for (const f of fields) {
    if (f.type === "andLabel") {
      const div = document.createElement("div");
      div.className = "andLabel";
      div.textContent = f.label || t("and");
      modalFields.appendChild(div);
      continue;
    }

    const wrap = document.createElement("div");
    wrap.className = "field";

    if (f.enhanceName) {
      const enhWrap = document.createElement("div");
      enhWrap.className = "enhancedInputWrap";
      enhWrap.style.flex = "1 1 auto";
      enhWrap.style.minWidth = "0";

      const chip = document.createElement("div");
      chip.className = "listeningChip";
      chip.textContent = t("mic_listening");

      const input = document.createElement("input");
      input.type = f.inputType || "text";
      input.placeholder = f.placeholder || "";
      input.value = f.value || "";
      input.inputMode = f.inputMode || "text";
      input.autocomplete = "off";
      input.spellcheck = false;
      input.className = "qInput";

      const mic = document.createElement("button");
      mic.type = "button";
      mic.className = "micBtn";
      mic.setAttribute("aria-label", "Voice input");
      const micIcon = document.createElement("span");
      micIcon.className = "micIcon";
      mic.appendChild(micIcon);

      const pop = document.createElement("div");
      pop.className = "popover";
      pop.setAttribute("aria-hidden", "true");
      pop.style.display = "none";

      enhWrap.appendChild(chip);
      enhWrap.appendChild(input);
      enhWrap.appendChild(mic);
      enhWrap.appendChild(pop);

      const x = document.createElement("button");
      x.type = "button";
      x.className = "xBtn";
      x.textContent = "×";
      x.onclick = () => {
        input.value = "";
        input.focus();
      };

      wrap.appendChild(enhWrap);
      wrap.appendChild(x);
      modalFields.appendChild(wrap);

      f._el = input;

      try {
        const enh = attachNameEnhancements({
          inputEl: input,
          wrapEl: enhWrap,
          micBtnEl: mic,
          popEl: pop,
          onCommit: () => {},
        });
        modalEnhancers.push(enh);
      } catch {}

      continue;
    }

    // default: simple input (existing behavior)
    const input = document.createElement("input");
    input.type = f.inputType || "text";
    input.placeholder = f.placeholder || "";
    input.value = f.value || "";
    input.inputMode = f.inputMode || (f.inputType === "number" ? "numeric" : "text");
    input.autocomplete = "off";
    input.spellcheck = false;

    const x = document.createElement("button");
    x.type = "button";
    x.className = "xBtn";
    x.textContent = "×";
    x.onclick = () => {
      input.value = "";
      input.focus();
    };

    wrap.appendChild(input);
    wrap.appendChild(x);
    modalFields.appendChild(wrap);

    f._el = input;
  }

  // close transliteration popovers in modal on outside click
  modalEnhancerOutsideHandler = (e) => {
    const target = e.target;
    // ignore clicks inside the modal itself
    if (modalOverlay && modalOverlay.contains(target)) {
      // if click is inside any enhanced field, do nothing
      for (const enh of modalEnhancers) {
        const w = enh?.getWrap?.();
        const p = enh?.getPopover?.();
        const m = enh?.getMic?.();
        if ((w && w.contains(target)) || (p && p.contains(target)) || (m && m.contains(target))) return;
      }
      // otherwise close popovers
      for (const enh of modalEnhancers) enh?.closePopover?.();
      return;
    }
    // click outside modal: close popovers too
    for (const enh of modalEnhancers) enh?.closePopover?.();
  };
  try {
    document.addEventListener("mousedown", modalEnhancerOutsideHandler, true);
  } catch {}

  modalOverlay.style.display = "flex";
  modalOverlay.setAttribute("aria-hidden", "false");

  const first = fields.find((f) => f._el)?._el;
  if (first) setTimeout(() => first.focus(), 0);
}

function closeModal() {
  modalOverlay.style.display = "none";
  modalOverlay.setAttribute("aria-hidden", "true");

  if (modalEnhancerOutsideHandler) {
    try {
      document.removeEventListener("mousedown", modalEnhancerOutsideHandler, true);
    } catch {}
    modalEnhancerOutsideHandler = null;
  }

  try {
    (modalEnhancers || []).forEach((e) => e?.close?.());
  } catch {}
  modalEnhancers = [];

  modalState = null;
}

modalCancel.onclick = () => closeModal();
modalOverlay.addEventListener("click", (e) => {
  if (e.target === modalOverlay) closeModal();
});
modalDone.onclick = () => {
  if (!modalState) return;
  try {
    const values = modalState.fields
      .filter((f) => f._el)
      .map((f) => f._el.value ?? "");
    modalState.onDone(values);
    closeModal();
  } catch (e) {
    console.error(e);
  }
};

function openRelativeNameModal() {
  openModal({
    title: t("modal_rel_title"),
    subtitle: t("modal_rel_sub"),
    fields: [{ inputType: "text", inputMode: "text", placeholder: t("modal_enter_name"), value: filters.relativeName || "", enhanceName: true }],
    onDone: ([v]) => {
      filters.relativeName = norm(v || "");
      renderFiltersPopoverRoot();
      refreshOnStateChange("filters");
    },
  });
}

function openAgeModal(mode) {
  const title = mode === "range" ? t("modal_age_title_range") : t("modal_age_title_eq");

  const subtitle =
    mode === "eq"
      ? t("modal_age_sub_eq")
      : mode === "gt"
        ? t("modal_age_sub_gt")
        : mode === "lt"
          ? t("modal_age_sub_lt")
          : t("modal_age_sub_range");

  if (mode === "range") {
    openModal({
      title,
      subtitle,
      fields: [
        { inputType: "number", placeholder: t("modal_enter_number"), value: filters.age.mode === "range" ? filters.age.a ?? "" : "" },
        { type: "andLabel", label: t("and") },
        { inputType: "number", placeholder: t("modal_enter_number"), value: filters.age.mode === "range" ? filters.age.b ?? "" : "" },
      ],
      onDone: ([a, b]) => {
        const aa = Number(String(a || "").trim());
        const bb = Number(String(b || "").trim());
        if (Number.isFinite(aa) && Number.isFinite(bb)) {
          filters.age = { mode: "range", a: aa, b: bb };
        } else {
          filters.age = { mode: "any", a: null, b: null };
        }
        renderFiltersPopoverAge();
        refreshOnStateChange("filters");
      },
    });
    return;
  }

  openModal({
    title,
    subtitle,
    fields: [{ inputType: "number", placeholder: t("modal_enter_number"), value: filters.age.mode === mode ? filters.age.a ?? "" : "" }],
    onDone: ([a]) => {
      const aa = Number(String(a || "").trim());
      if (Number.isFinite(aa)) filters.age = { mode, a: aa, b: null };
      else filters.age = { mode: "any", a: null, b: null };
      renderFiltersPopoverAge();
      refreshOnStateChange("filters");
    },
  });
}

// ---------- District popovers (landing + results) ----------
function openDistrictPopover(popEl, btnEl) {
  if (!districtManifest?.districts?.length) return;
  popEl.style.display = "block";
  popEl.setAttribute("aria-hidden", "false");
  if (btnEl) btnEl.setAttribute("aria-expanded", "true");

  ensureDistrictPopoverSkeleton(popEl);
  updateDistrictPopoverList(popEl);

  const input = popEl.querySelector("input[data-role='district-search']");
  if (input) setTimeout(() => input.focus(), 0);
}

function closeDistrictPopover(popEl, btnEl) {
  popEl.style.display = "none";
  popEl.setAttribute("aria-hidden", "true");
  if (btnEl) btnEl.setAttribute("aria-expanded", "false");
}

function closeDistrictPopovers() {
  closeDistrictPopover(districtPopover, districtBtn);
  closeDistrictPopover(districtPopoverLanding, districtBtnLanding);
}

// build popover skeleton once, keep input stable to prevent caret jumping
function ensureDistrictPopoverSkeleton(popEl) {
  if (popEl.dataset.built === "1") return;

  popEl.innerHTML = "";

  const s = document.createElement("div");
  s.className = "popSearch";
  s.innerHTML = `<input data-role="district-search" type="search" placeholder="${escapeHtml(
    t("district_search_placeholder")
  )}" autocomplete="off" spellcheck="false">`;
  popEl.appendChild(s);

  const list = document.createElement("div");
  list.dataset.role = "district-list";
  popEl.appendChild(list);

  const input = s.querySelector("input");
  input.value = districtQuery || "";

  input.addEventListener("input", () => {
    districtQuery = input.value || "";
    updateDistrictPopoverList(popEl);
  });

  popEl.dataset.built = "1";
}

function updateDistrictPopoverList(popEl) {
  const listEl = popEl.querySelector("div[data-role='district-list']");
  const inputEl = popEl.querySelector("input[data-role='district-search']");
  if (!listEl) return;

  // keep placeholder translated even when language switches (rebuild placeholder if needed)
  if (inputEl) {
    if (inputEl.getAttribute("placeholder") !== t("district_search_placeholder")) {
      inputEl.setAttribute("placeholder", t("district_search_placeholder"));
    }
    if (inputEl.value !== (districtQuery || "")) inputEl.value = districtQuery || "";
  }

  listEl.innerHTML = "";

  const qRaw = (districtQuery || "").trim();
  const qNorm = normDistrictSearchStr(qRaw);
  const qNoSpace = qNorm.replace(/\s+/g, "");
  const list = (districtManifest?.districts || []).filter((d) => districtMatchesQuery(d, qNorm, qNoSpace));

  for (const d of list) {
    const isSel = d.id === currentDistrictId;
    const row = popRow({
      left: getDistrictLabelForLang(d, getActiveLang()) || d.label,
      right: "",
      chevron: false,
      selected: isSel,
      onClick: () => {
        closeDistrictPopovers();
        setDistrictById(d.id);
        refreshOnStateChange("district");
      },
    });
    listEl.appendChild(row);
  }
}

// ---------- AC filter popover ----------
function openAcPopover() {
  if (!districtACsAll.length) return;
  acPopover.style.display = "block";
  acPopover.setAttribute("aria-hidden", "false");
  renderAcPopover();
}

function closeAcPopover() {
  acPopover.style.display = "none";
  acPopover.setAttribute("aria-hidden", "true");
}

function renderAcPopover() {
  acPopover.innerHTML = "";

  acPopover.appendChild(
    popRow({
      left: t("all"),
      right: "",
      chevron: false,
      selected: isAllACsSelected(),
      onClick: () => {
        selectedACs.clear();
        updateSelectedAcText();
        renderAcPopover();
        refreshOnStateChange("acs");
      },
    })
  );

  acPopover.appendChild(document.createElement("div")).className = "popSep";

  for (const ac of districtACsAll) {
    const checked = selectedACs.has(ac);
    const effectiveSelected = isAllACsSelected() ? false : checked;

    const row = popRow({
      left: getAcOptionLabel(ac),
      right: "",
      chevron: false,
      selected: effectiveSelected,
      onClick: () => {
        if (isAllACsSelected()) {
          // From "All ACs", first click should select only the clicked AC.
          selectedACs = new Set([ac]);
        } else {
          if (selectedACs.has(ac)) {
            selectedACs.delete(ac);
            // If user deselects the last selected AC, return to "All ACs".
            if (selectedACs.size === 0) selectedACs.clear();
          } else {
            selectedACs.add(ac);
          }

          // If user ends up selecting every AC individually, collapse to "All ACs".
          if (selectedACs.size === districtACsAll.length) selectedACs.clear();
        }

        updateSelectedAcText();
        renderAcPopover();
        refreshOnStateChange("acs");
      },
    });

    acPopover.appendChild(row);
  }
}

// ---------- Sort popover ----------
function openSortPopover() {
  sortPopover.style.display = "block";
  sortPopover.setAttribute("aria-hidden", "false");
  sortBtn.setAttribute("aria-expanded", "true");
  renderSortPopover();
}

function closeSortPopover() {
  sortPopover.style.display = "none";
  sortPopover.setAttribute("aria-hidden", "true");
  sortBtn.setAttribute("aria-expanded", "false");
}

function renderSortPopover() {
  sortPopover.innerHTML = "";

  const opts = [
    { k: SORT.RELEVANCE, label: t("sort_row_relevance") },
    { k: SORT.AGE_ASC, label: t("sort_row_age_up") },
    { k: SORT.AGE_DESC, label: t("sort_row_age_down") },
  ];

  for (const o of opts) {
    sortPopover.appendChild(
      popRow({
        left: o.label,
        right: "",
        chevron: false,
        selected: sortMode === o.k,
        onClick: () => {
          closeSortPopover();
          setSortMode(o.k);
          refreshOnStateChange("sort");
        },
      })
    );
  }
}

// ---------- Page size popover ----------
function getPageSizeOptionsAndDefault() {
  const mobile = isMobileUI();
  return {
    opts: mobile ? PAGE_SIZE_MOBILE_OPTIONS : PAGE_SIZE_DESKTOP_OPTIONS,
    def: mobile ? PAGE_SIZE_MOBILE_DEFAULT : PAGE_SIZE_DESKTOP_DEFAULT,
  };
}

function setupPageSizeDefaultIfNeeded() {
  const { opts, def } = getPageSizeOptionsAndDefault();
  if (!opts.includes(Number(pageSize))) pageSize = def;
  if (pageSizeText) pageSizeText.textContent = String(pageSize);
}

function openPageSizePopover() {
  pageSizePopover.style.display = "block";
  pageSizePopover.setAttribute("aria-hidden", "false");
  pageSizeBtn.setAttribute("aria-expanded", "true");
  renderPageSizePopover();
}

function closePageSizePopover() {
  pageSizePopover.style.display = "none";
  pageSizePopover.setAttribute("aria-hidden", "true");
  pageSizeBtn.setAttribute("aria-expanded", "false");
}

function renderPageSizePopover() {
  const { opts } = getPageSizeOptionsAndDefault();

  pageSizePopover.innerHTML = "";

  for (const n of opts) {
    pageSizePopover.appendChild(
      popRow({
        left: String(n),
        right: "",
        chevron: false,
        selected: Number(pageSize) === Number(n),
        onClick: async () => {
          closePageSizePopover();
          pageSize = Number(n);
          if (pageSizeText) pageSizeText.textContent = String(pageSize);
          page = 1;
          await renderPage();
        },
      })
    );
  }
}

// ---------- Enhancements init (NEW) ----------
let enhLanding = null;
let enhResults = null;
let enhRel = null; // reserved for relative field if/when added in DOM

function initNameEnhancements() {
  // Landing
  const wrapLanding = $("enhancedWrapLanding");
  const popLanding = $("translitPopoverLanding");
  const micLanding = $("micBtnLanding");
  const iosHintLanding = $("iosHintLanding");
  const iosHintCloseLanding = $("iosHintCloseLanding");

  enhLanding = attachNameEnhancements({
    inputEl: qLanding,
    wrapEl: wrapLanding,
    micBtnEl: micLanding,
    popEl: popLanding,
    iosHintEl: iosHintLanding,
    iosHintCloseEl: iosHintCloseLanding,
    // For primary fields: commit should trigger existing search now.
    onCommit: (_text) => runSearch(),
    getDisabledState: () => qLanding.disabled,
  });
  wireDisabledQueryTooltip({
    wrapEl: wrapLanding,
    inputEl: qLanding,
    hintEl: qDisabledHintLanding,
  });

  // Results
  const wrapResults = $("enhancedWrapResults");
  const popResults = $("translitPopoverResults");
  const micResults = $("micBtnResults");
  const iosHintResults = $("iosHintResults");
  const iosHintCloseResults = $("iosHintCloseResults");

  enhResults = attachNameEnhancements({
    inputEl: qResults,
    wrapEl: wrapResults,
    micBtnEl: micResults,
    popEl: popResults,
    iosHintEl: iosHintResults,
    iosHintCloseEl: iosHintCloseResults,
    onCommit: (_text) => runSearch(),
    getDisabledState: () => qResults.disabled,
  });
  wireDisabledQueryTooltip({
    wrapEl: wrapResults,
    inputEl: qResults,
    hintEl: qDisabledHintResults,
  });

  // Relative field: your current UI stores relative name filter inside modal (not a persistent field).
  // So there is nothing to attach yet without changing UI behavior.
  // If you later add a relative input field in DOM, we can attach here (enhRel).
}

// ---------- Close popovers on outside click / Esc ----------
document.addEventListener(
  "pointerdown",
  (e) => {
    const path = e.composedPath?.() || [];

    const clickInsideFilters = path.includes(filtersPopover) || path.includes(moreFiltersBtn);
    const clickInsideDistrictResults = path.includes(districtPopover) || path.includes(districtBtn);
    const clickInsideDistrictLanding = path.includes(districtPopoverLanding) || path.includes(districtBtnLanding);
    const clickInsideAc = path.includes(acPopover) || path.includes(selectedAcBtn);
    const clickInsideSort = path.includes(sortPopover) || path.includes(sortBtn);
    const clickInsidePageSize = path.includes(pageSizePopover) || path.includes(pageSizeBtn);

    // NEW: translit popovers (landing/results)
    const translitPopLanding = $("translitPopoverLanding");
    const translitPopResults = $("translitPopoverResults");
    const wrapLanding = $("enhancedWrapLanding");
    const wrapResults = $("enhancedWrapResults");

    const clickInsideTranslitLanding =
      (translitPopLanding && path.includes(translitPopLanding)) || (wrapLanding && path.includes(wrapLanding));
    const clickInsideTranslitResults =
      (translitPopResults && path.includes(translitPopResults)) || (wrapResults && path.includes(wrapResults));

    if (filtersPopover.style.display !== "none" && !clickInsideFilters) closeFiltersPopover();
    if (districtPopover.style.display !== "none" && !clickInsideDistrictResults) closeDistrictPopover(districtPopover, districtBtn);
    if (districtPopoverLanding.style.display !== "none" && !clickInsideDistrictLanding)
      closeDistrictPopover(districtPopoverLanding, districtBtnLanding);
    if (acPopover.style.display !== "none" && !clickInsideAc) closeAcPopover();
    if (sortPopover.style.display !== "none" && !clickInsideSort) closeSortPopover();
    if (pageSizePopover.style.display !== "none" && !clickInsidePageSize) closePageSizePopover();

    // close translit popovers on outside click
    if (translitPopLanding?.style?.display === "block" && !clickInsideTranslitLanding) closeTranslitPopover(translitPopLanding);
    if (translitPopResults?.style?.display === "block" && !clickInsideTranslitResults) closeTranslitPopover(translitPopResults);
  },
  true
);

document.addEventListener("keydown", (e) => {
  if (e.key === "Escape") {
    closeFiltersPopover();
    closeDistrictPopovers();
    closeAcPopover();
    closeSortPopover();
    closePageSizePopover();
    closeModal();

    // NEW: translit popovers
    closeTranslitPopover($("translitPopoverLanding"));
    closeTranslitPopover($("translitPopoverResults"));
  }
});

// ---------- Wire UI ----------
searchBtnLanding.onclick = () => runSearch();
searchBtnResults.onclick = () => runSearch();

qLanding.addEventListener("input", syncSearchButtonState);
qResults.addEventListener("input", syncSearchButtonState);

wireIMEEnter(qLanding, runSearch);
wireIMEEnter(qResults, runSearch);

clearBtn.onclick = () => clearAll();
clearBtnTop?.addEventListener("click", () => clearAll());

prevBtn.onclick = async () => {
  page--;
  await renderPage();
};
nextBtn.onclick = async () => {
  page++;
  await renderPage();
};

// Scope chips
$("chipVoter").onclick = () => setActiveChip(SCOPE.VOTER);
$("chipRelative").onclick = () => setActiveChip(SCOPE.RELATIVE);
$("chipAnywhere").onclick = () => setActiveChip(SCOPE.ANYWHERE);

// Include typing mistakes toggles auto-refresh search
exactToggleLanding.onchange = () => {
  setIncludeTypingChecked(exactToggleLanding.checked);
  setStatus(t("toggle_include_typing_refresh", { state: exactToggleLanding.checked ? t("on") : t("off") }));
  refreshOnStateChange("exact");
};
exactToggleResults.onchange = () => {
  setIncludeTypingChecked(exactToggleResults.checked);
  setStatus(t("toggle_include_typing_refresh", { state: exactToggleResults.checked ? t("on") : t("off") }));
  refreshOnStateChange("exact");
};

// More filters button
moreFiltersBtn.onclick = () => {
  if (filtersPopover.style.display === "block") closeFiltersPopover();
  else openFiltersPopover();
};

// District button (landing)
districtBtnLanding.onclick = () => {
  if (districtPopoverLanding.style.display === "block") closeDistrictPopover(districtPopoverLanding, districtBtnLanding);
  else {
    closeDistrictPopover(districtPopover, districtBtn);
    openDistrictPopover(districtPopoverLanding, districtBtnLanding);
  }
};

// District button (results)
districtBtn.onclick = () => {
  if (districtPopover.style.display === "block") closeDistrictPopover(districtPopover, districtBtn);
  else {
    closeDistrictPopover(districtPopoverLanding, districtBtnLanding);
    openDistrictPopover(districtPopover, districtBtn);
  }
};

// AC filter button
selectedAcBtn.onclick = () => {
  if (acPopover.style.display === "block") closeAcPopover();
  else openAcPopover();
};

// Sort button
sortBtn.onclick = () => {
  if (sortPopover.style.display === "block") closeSortPopover();
  else openSortPopover();
};

// Page size button
pageSizeBtn.onclick = () => {
  if (pageSizePopover.style.display === "block") closePageSizePopover();
  else openPageSizePopover();
};

landingKnowMoreBtnMobile?.addEventListener("click", () => {
  markLandingBannerSeenAndHide();
  showAnnouncementPage();
});

landingKnowMoreBtn?.addEventListener("click", () => {
  markLandingBannerSeenAndHide();
  showAnnouncementPage();
});

landingInfoBannerCloseBtnMobile?.addEventListener("click", () => {
  markLandingBannerSeenAndHide();
});

landingInfoBannerCloseBtn?.addEventListener("click", () => {
  markLandingBannerSeenAndHide();
});

landingFaqBtn?.addEventListener("click", () => {
  try {
    localStorage.setItem(SEEN_LANDING_BANNER_KEY, "1");
  } catch {}
  showAnnouncementPage();
});

announcementBackBtn?.addEventListener("click", () => {
  showLanding();
  maybeShowLandingBanner();
  maybeShowLandingBannerDesktop();
});

// Language buttons
$("langHi")?.addEventListener("click", () => setLanguage(LANG.HI));
$("langHinglish")?.addEventListener("click", () => setLanguage(LANG.HINGLISH));
$("langEn")?.addEventListener("click", () => setLanguage(LANG.EN));

// ---------- Boot ----------
setMeta(t("status_not_loaded"));
setStatus(t("status_select_district"));

setActiveChip(SCOPE.VOTER);
setIncludeTypingChecked(true);

setSearchEnabled(false);
showLanding();
maybeShowLandingBanner();
maybeShowLandingBannerDesktop();

updateMoreFiltersEnabled();
renderFiltersPopoverRoot();

updateDistrictUI();
updateSelectedAcText();

setSortMode(SORT.RELEVANCE);
renderSortPopover();

setupPageSizeDefaultIfNeeded();
renderPageSizePopover();

// init transliteration + voice enhancers (NEW)
initNameEnhancements();

// Mobile-only: auto-collapse header rows when table is scrolled
initMobileTableScrollCompactUI();
initStickyColScrollShadowSync();

// update page size options if user resizes across breakpoint
let resizeTimer = null;
window.addEventListener("resize", () => {
  if (resizeTimer) clearTimeout(resizeTimer);
  resizeTimer = setTimeout(async () => {
    resizeTimer = null;

    const before = pageSize;
    setupPageSizeDefaultIfNeeded();

    if (rankedView.length) {
      const totalPages = Math.max(1, Math.ceil(rankedView.length / pageSize));
      page = Math.max(1, Math.min(page, totalPages));
      await renderPage();
    }

    renderPageSizePopover();

    if (before !== pageSize && pageSizeText) pageSizeText.textContent = String(pageSize);
  }, 150);
});

(async () => {
  // set language first (so initial UI text is correct)
  setLanguage(loadSavedLanguageOrDefault());

  await loadDistrictManifest(STATE_CODE_DEFAULT);
  populateDistrictHiddenSelect();
  setStatus(t("status_select_district"));
})();
