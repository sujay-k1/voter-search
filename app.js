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
let rankedByRelevance = []; // full [{key, ac, row_id, score}]
let filteredBase = []; // after filters, in relevance order
let rankedView = []; // after sort, for paging
let page = 1;

let pageSize = PAGE_SIZE_DESKTOP_DEFAULT;

// Used to cancel district-preload if user switches district quickly
let districtPreloadToken = 0;

let ageMap = null; // Map(key -> ageNumber|null)
let displayCache = new Map(); // Map(key -> rowObject)

// score cache for age/gender filters
let scoreCache = new Map(); // Map(key -> { ageRaw, ageParsed, genderRaw, genderBucket })

// Sort mode (popover)
const SORT = {
  RELEVANCE: "relevance",
  AGE_ASC: "age_asc",
  AGE_DESC: "age_desc",
};
let sortMode = SORT.RELEVANCE;

// District popover search
let districtQuery = "";

// ---------------- Transliteration + Voice (kept) ----------------
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
  if (/[A-Za-z]/.test(s)) return "latin";
  return "nonlatin";
}

function isIOS() {
  const ua = navigator.userAgent || "";
  const iOS = /iP(hone|od|ad)/.test(ua);
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
  inputEl.value = v;
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

  popEl.style.display = "block";
  popEl.setAttribute("aria-hidden", "false");

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
  const list = popEl.querySelector("div[data-role='translit-list']");
  if (list) list.innerHTML = "";
  popEl.dataset.activeIndex = "-1";
}

function renderTranslitList(popEl, items) {
  ensureTranslitPopoverSkeleton(popEl);
  const list = popEl.querySelector("div[data-role='translit-list']");
  if (!list) return;

  list.innerHTML = "";

  items.forEach((txt, idx) => {
    const btn = document.createElement("button");
    btn.type = "button";
    btn.className = "option";
    btn.textContent = txt;
    btn.dataset.idx = String(idx);
    list.appendChild(btn);
  });

  popEl.dataset.activeIndex = items.length ? "0" : "-1";
}

function clamp(n, a, b) {
  return Math.max(a, Math.min(b, n));
}

function highlightActiveOption(popEl) {
  const list = popEl.querySelector("div[data-role='translit-list']");
  if (!list) return;
  const idx = Number(popEl.dataset.activeIndex || -1);
  const btns = Array.from(list.querySelectorAll("button.option"));
  btns.forEach((b, i) => b.classList.toggle("active", i === idx));
  if (idx >= 0 && idx < btns.length) btns[idx].scrollIntoView({ block: "nearest" });
}

function createEnhancerForInput(kind /* "landing" | "results" */, inputEl, wrapEl, micBtnEl, chipEl, popEl) {
  if (!inputEl || !wrapEl || !popEl) return null;

  let lastMode = "nonlatin";
  let debounceTimer = null;
  let lastQueryText = "";
  let suggestions = [];
  let suppressSuggestDuringSpeech = false;

  let recognizer = null;
  let isListening = false;

  function syncDisabled() {
    const disabled = inputEl.disabled || inputEl.hasAttribute("disabled");
    if (micBtnEl) micBtnEl.disabled = disabled;
    if (disabled) {
      closeTranslitPopover(popEl);
      setListeningUI(false);
    }
  }

  function setListeningUI(on) {
    isListening = !!on;
    if (!chipEl) return;
    chipEl.style.display = on ? "inline-flex" : "none";
  }

  async function refreshSuggestions(force = false) {
    if (suppressSuggestDuringSpeech) return;

    const text = String(inputEl.value || "");
    const mode = detectScriptModeFromText(text);
    lastMode = mode;

    if (mode !== "latin") {
      closeTranslitPopover(popEl);
      suggestions = [];
      lastQueryText = "";
      return;
    }

    const trimmed = text.trim();
    if (!trimmed) {
      closeTranslitPopover(popEl);
      suggestions = [];
      lastQueryText = "";
      return;
    }

    if (!force && trimmed === lastQueryText) return;

    lastQueryText = trimmed;

    try {
      suggestions = await fetchGoogleSuggestions(trimmed);
    } catch (_e) {
      suggestions = [];
    }

    if (!suggestions.length) {
      closeTranslitPopover(popEl);
      return;
    }

    renderTranslitList(popEl, suggestions);
    openTranslitPopover(popEl, wrapEl);
    highlightActiveOption(popEl);
  }

  function scheduleSuggest() {
    if (debounceTimer) clearTimeout(debounceTimer);
    debounceTimer = setTimeout(() => refreshSuggestions(false), TRANSLIT.debounceMs);
  }

  inputEl.addEventListener("input", () => {
    scheduleSuggest();
  });

  inputEl.addEventListener("keydown", (ev) => {
    const open = popEl.style.display === "block";
    if (!open) return;

    const idx = Number(popEl.dataset.activeIndex || -1);
    const max = suggestions.length - 1;

    if (ev.key === "ArrowDown") {
      ev.preventDefault();
      popEl.dataset.activeIndex = String(clamp(idx + 1, 0, max));
      highlightActiveOption(popEl);
    } else if (ev.key === "ArrowUp") {
      ev.preventDefault();
      popEl.dataset.activeIndex = String(clamp(idx - 1, 0, max));
      highlightActiveOption(popEl);
    } else if (ev.key === "Enter") {
      if (idx >= 0 && idx < suggestions.length) {
        ev.preventDefault();
        setInputValueNoRerender(inputEl, suggestions[idx]);
        closeTranslitPopover(popEl);
      }
    } else if (ev.key === "Escape") {
      ev.preventDefault();
      closeTranslitPopover(popEl);
    }
  });

  popEl.addEventListener("click", (ev) => {
    const btn = ev.target.closest("button.option");
    if (!btn) return;
    const idx = Number(btn.dataset.idx || -1);
    if (idx >= 0 && idx < suggestions.length) {
      setInputValueNoRerender(inputEl, suggestions[idx]);
      closeTranslitPopover(popEl);
    }
  });

  document.addEventListener("click", (ev) => {
    if (popEl.style.display !== "block") return;
    if (wrapEl.contains(ev.target) || popEl.contains(ev.target)) return;
    closeTranslitPopover(popEl);
  });

  // voice
  function supportsSpeech() {
    return "webkitSpeechRecognition" in window || "SpeechRecognition" in window;
  }

  function startListening() {
    const Ctor = window.SpeechRecognition || window.webkitSpeechRecognition;
    if (!Ctor) return;

    recognizer = new Ctor();
    recognizer.continuous = false;
    recognizer.interimResults = true;
    recognizer.lang = "hi-IN";

    suppressSuggestDuringSpeech = true;
    setListeningUI(true);

    recognizer.onresult = (ev) => {
      let finalText = "";
      let interimText = "";
      for (let i = ev.resultIndex; i < ev.results.length; i++) {
        const res = ev.results[i];
        if (res.isFinal) finalText += res[0].transcript;
        else interimText += res[0].transcript;
      }
      const combined = (finalText || interimText || "").trim();
      if (combined) {
        setInputValueNoRerender(inputEl, combined);
        if (finalText) {
          setTimeout(() => {
            suppressSuggestDuringSpeech = false;
            refreshSuggestions(true);
          }, 10);
        }
      }
    };

    recognizer.onerror = (_ev) => {
      setListeningUI(false);
      suppressSuggestDuringSpeech = false;
    };

    recognizer.onend = () => {
      setListeningUI(false);
      suppressSuggestDuringSpeech = false;
    };

    recognizer.start();
  }

  function stopListening() {
    try {
      recognizer?.stop?.();
    } catch (_e) {}
    setListeningUI(false);
    suppressSuggestDuringSpeech = false;
  }

  if (micBtnEl) {
    micBtnEl.addEventListener("click", () => {
      if (inputEl.disabled) return;

      if (!supportsSpeech()) return;

      if (isListening) {
        stopListening();
        return;
      }
      startListening();
    });
  }

  return {
    close: () => closeTranslitPopover(popEl),
    syncDisabled,
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

  updateDistrictUI();
  updateSelectedAcText();
  setSortMode(sortMode);
  renderSortPopover();
  renderPageSizePopover();
  renderFiltersPopoverRoot();

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
  // i18n.js API has differed across builds. Avoid hard-crash if helper name changed.
  try {
    if (i18n && typeof i18n.loadSavedLanguageOrDefault === "function") {
      return i18n.loadSavedLanguageOrDefault();
    }
    if (i18n && typeof i18n.getSavedLangOrDefault === "function") {
      return i18n.getSavedLangOrDefault();
    }
    if (i18n && typeof i18n.getSavedLanguageOrDefault === "function") {
      return i18n.getSavedLanguageOrDefault();
    }
  } catch (_e) {}

  // Fallback: read from localStorage using the same key passed into createI18n().
  const key = "sir_lang";
  try {
    const v = localStorage.getItem(key);
    if (v === LANG.HI || v === LANG.HINGLISH || v === LANG.EN) return v;
  } catch (_e) {}
  return LANG.HI;
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
const districtBtnLanding = $("districtBtnLanding");
const districtPopoverLanding = $("districtPopoverLanding");
const districtMirrorLanding = $("districtMirrorLanding");
const qLanding = $("q");
const searchBtnLanding = $("searchBtn");
const exactToggleLanding = $("exactToggle");

// Results widgets
const districtBtn = $("districtBtn");
const districtPopover = $("districtPopover");
const districtMirror = $("districtMirror");
const qResults = $("qResults");
const searchBtnResults = $("searchBtnResults");
const exactToggleResults = $("exactToggleResults");

const chipVoter = $("chipVoter");
const chipRelative = $("chipRelative");
const chipAnywhere = $("chipAnywhere");

const selectedAcBtn = $("selectedAcBtn");
const selectedAcText = $("selectedAcText");
const acPopover = $("acPopover");

const moreFiltersBtn = $("moreFiltersBtn");
const filtersPopover = $("filtersPopover");

const sortBtn = $("sortBtn");
const sortText = $("sortText");
const sortPopover = $("sortPopover");

const pageSizeBtn = $("pageSizeBtn");
const pageSizeText = $("pageSizeText");
const pageSizePopover = $("pageSizePopover");

const resultsEl = $("results");
const pager = $("pager");
const prevBtn = $("prevBtn");
const nextBtn = $("nextBtn");
const pageInfo = $("pageInfo");
const resultsCount = $("resultsCount");
const currentPageCount = $("currentPageCount");

const statusLanding = $("statusLanding");
const statusResults = $("statusResults");
const metaLine = $("metaLine");
const bar = $("bar");

const clearBtn = $("clearBtn");

const districtSelHidden = $("districtSel");

// iOS hint
const iosHintLanding = $("iosHintLanding");
const iosHintCloseLanding = $("iosHintCloseLanding");
const iosHintResults = $("iosHintResults");
const iosHintCloseResults = $("iosHintCloseResults");

// Modal (filters)
const modalOverlay = $("modalOverlay");
const modalTitle = $("modalTitle");
const modalSubtitle = $("modalSubtitle");
const modalFields = $("modalFields");
const modalCancel = $("modalCancel");
const modalDone = $("modalDone");

// Filters UI fields
const filtersRoot = $("filtersRoot");
const relNameInput = $("relNameInput");
const genderSel = $("genderSel");
const ageModeSel = $("ageModeSel");
const ageA = $("ageA");
const ageB = $("ageB");
const clearFiltersBtn = $("clearFiltersBtn");
const applyFiltersBtn = $("applyFiltersBtn");

// enhanced input elements
const enhancerLanding = createEnhancerForInput(
  "landing",
  qLanding,
  $("enhancedWrapLanding"),
  $("micBtnLanding"),
  $("listenChipLanding"),
  $("translitPopoverLanding")
);

const enhancerResults = createEnhancerForInput(
  "results",
  qResults,
  $("enhancedWrapResults"),
  $("micBtnResults"),
  $("listenChipResults"),
  $("translitPopoverResults")
);

// -------- state helpers --------
let searchEnabled = false;
let districtLoading = false;

function setBar(pct) {
  if (!bar) return;
  const x = Math.max(0, Math.min(100, Number(pct) || 0));
  bar.style.width = `${x}%`;
}

function setMeta(msg) {
  if (metaLine) metaLine.textContent = msg || "";
}

function setStatus(msg) {
  if (statusLanding) statusLanding.textContent = msg || "";
  if (statusResults) statusResults.textContent = msg || "";
}

function showLanding() {
  if (landingSection) landingSection.style.display = "flex";
  if (resultsSection) resultsSection.style.display = "none";
}

function showResults() {
  if (landingSection) landingSection.style.display = "none";
  if (resultsSection) resultsSection.style.display = "block";
}

function isResultsVisible() {
  return resultsSection && resultsSection.style.display !== "none";
}

function setSearchEnabled(v) {
  searchEnabled = !!v;
  syncSearchButtonState();
  enhancerLanding?.syncDisabled();
  enhancerResults?.syncDisabled();
}

function setDistrictLoading(v) {
  districtLoading = !!v;
  document.body.classList.toggle("loading", districtLoading);
  syncSearchButtonState();
}

function sanitizeDistrictId(id) {
  return String(id || "")
    .trim()
    .toLowerCase()
    .replace(/[_\s]+/g, "-")
    .replace(/[^a-z0-9-]/g, "-")
    .replace(/-+/g, "-")
    .replace(/^-|-$/g, "");
}

function getCurrentDistrictSlug() {
  return sanitizeDistrictId(currentDistrictId);
}

function updateDistrictUI() {
  const label = currentDistrictLabel || t("select_district");
  if (districtMirror) districtMirror.textContent = label;
  if (districtMirrorLanding) districtMirrorLanding.textContent = label;
}

function isAllACsSelected() {
  return selectedACs.size === 0 || selectedACs.size === districtACsAll.length;
}

function getActiveACs() {
  return isAllACsSelected()
    ? districtACsAll.slice()
    : Array.from(selectedACs).slice().sort((a, b) => a - b);
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
      list: arr.map((x) => String(x).padStart(2, "0")).join(", "),
    });
  else selectedAcText.textContent = t("selected_acs_count", { n: arr.length });
}

function closeDistrictPopover(popEl, btnEl) {
  if (!popEl) return;
  popEl.style.display = "none";
  popEl.setAttribute("aria-hidden", "true");
  if (btnEl) btnEl.setAttribute("aria-expanded", "false");
}

function openDistrictPopover(popEl, btnEl) {
  if (!popEl) return;
  popEl.style.display = "block";
  popEl.setAttribute("aria-hidden", "false");
  if (btnEl) btnEl.setAttribute("aria-expanded", "true");
}

function closeAcPopover() {
  if (!acPopover) return;
  acPopover.style.display = "none";
  acPopover.setAttribute("aria-hidden", "true");
}

function openAcPopover() {
  if (!acPopover) return;
  closeDistrictPopover(districtPopoverLanding, districtBtnLanding);
  closeDistrictPopover(districtPopover, districtBtn);
  closeFiltersPopover();
  closeSortPopover();
  closePageSizePopover();

  acPopover.style.display = "block";
  acPopover.setAttribute("aria-hidden", "false");
  renderAcPopover();
}

function closeFiltersPopover() {
  if (!filtersPopover) return;
  filtersPopover.style.display = "none";
  filtersPopover.setAttribute("aria-hidden", "true");
}

function openFiltersPopover() {
  if (!filtersPopover) return;
  closeDistrictPopover(districtPopoverLanding, districtBtnLanding);
  closeDistrictPopover(districtPopover, districtBtn);
  closeAcPopover();
  closeSortPopover();
  closePageSizePopover();

  filtersPopover.style.display = "block";
  filtersPopover.setAttribute("aria-hidden", "false");
  renderFiltersPopoverRoot();
}

function closeSortPopover() {
  if (!sortPopover) return;
  sortPopover.style.display = "none";
  sortPopover.setAttribute("aria-hidden", "true");
  sortBtn?.setAttribute("aria-expanded", "false");
}

function openSortPopover() {
  if (!sortPopover) return;
  closeDistrictPopover(districtPopoverLanding, districtBtnLanding);
  closeDistrictPopover(districtPopover, districtBtn);
  closeAcPopover();
  closeFiltersPopover();
  closePageSizePopover();

  sortPopover.style.display = "block";
  sortPopover.setAttribute("aria-hidden", "false");
  sortBtn?.setAttribute("aria-expanded", "true");
  renderSortPopover();
}

function closePageSizePopover() {
  if (!pageSizePopover) return;
  pageSizePopover.style.display = "none";
  pageSizePopover.setAttribute("aria-hidden", "true");
  pageSizeBtn?.setAttribute("aria-expanded", "false");
}

function openPageSizePopover() {
  if (!pageSizePopover) return;
  closeDistrictPopover(districtPopoverLanding, districtBtnLanding);
  closeDistrictPopover(districtPopover, districtBtn);
  closeAcPopover();
  closeFiltersPopover();
  closeSortPopover();

  pageSizePopover.style.display = "block";
  pageSizePopover.setAttribute("aria-hidden", "false");
  pageSizeBtn?.setAttribute("aria-expanded", "true");
  renderPageSizePopover();
}

function syncSearchButtonState() {
  const hasDistrict = !!getCurrentDistrictSlug() && districtACsAll.length > 0;
  const canSearch = searchEnabled && !districtLoading && hasDistrict;

  if (qLanding) qLanding.disabled = !canSearch;
  if (qResults) qResults.disabled = !canSearch;

  if (searchBtnLanding) searchBtnLanding.disabled = !canSearch;
  if (searchBtnResults) searchBtnResults.disabled = !canSearch;

  if (selectedAcBtn) selectedAcBtn.disabled = !canSearch;

  if (moreFiltersBtn) moreFiltersBtn.disabled = !(canSearch && searchScope === SCOPE.VOTER);

  if (sortBtn) sortBtn.disabled = !canSearch;
  if (pageSizeBtn) pageSizeBtn.disabled = !canSearch;

  enhancerLanding?.syncDisabled();
  enhancerResults?.syncDisabled();
}

// ---------- Backend (Netlify Functions) ----------
const FN_BASE = `${window.location.origin}/netlify/functions/`;
const fnUrl = (name) => `${FN_BASE}${name}`;

async function postJson(url, payload) {
  const resp = await fetch(url, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify(payload || {}),
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

async function callFn(name, payload) {
  return postJson(fnUrl(name), payload);
}

function makeKey(ac, row_id) {
  return `${Number(ac)}:${Number(row_id)}`;
}

function parseAgeValue(v) {
  if (v === null || v === undefined) return null;
  const s = String(v).trim();
  const m = s.match(/(\d+)/);
  if (!m) return null;
  const n = Number(m[1]);
  return Number.isFinite(n) ? n : null;
}

function normGenderValue(v) {
  const s = String(v ?? "").trim().toLowerCase();
  if (!s) return "other";
  if (s === "m" || s.includes("male") || s.includes("पुरुष") || s.includes("पु") || s.includes("man")) return "male";
  if (s === "f" || s.includes("female") || s.includes("महिला") || s.includes("स्त्री") || s.includes("woman")) return "female";
  if (s.includes("other") || s.includes("अन्य") || s === "o") return "other";
  return "other";
}

function cacheScoreRow(ac, r) {
  const k = makeKey(ac, Number(r.row_id));
  const ageRaw = r.age ?? r.Age ?? r["Age"] ?? null;
  const genderRaw = r.gender ?? r.Gender ?? r["Gender"] ?? null;
  const ageParsed = parseAgeValue(ageRaw);
  const genderBucket = normGenderValue(genderRaw);
  scoreCache.set(k, { ageRaw, ageParsed, genderRaw, genderBucket });
}
// -------- District manifest --------
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
  if (!raw) return { districts: FALLBACK_DISTRICT_MAP };
  if (Array.isArray(raw.districts)) {
    return {
      districts: raw.districts
        .map((d) => ({
          id: String(d.id ?? d.label ?? d.name ?? "").trim(),
          label: String(d.label ?? d.name ?? d.id ?? "District"),
          acs: Array.isArray(d.acs) ? d.acs.map(Number).filter(Number.isFinite) : [],
        }))
        .filter((d) => d.id && d.acs.length > 0),
    };
  }
  return { districts: FALLBACK_DISTRICT_MAP };
}

async function loadDistrictManifest(stateCode) {
  const url = relUrl(`data/${stateCode}/district_manifest.json`);
  try {
    const resp = await fetch(url);
    if (!resp.ok) throw new Error(`HTTP ${resp.status}`);
    const raw = await resp.json();
    districtManifest = normalizeDistrictManifest(raw);
  } catch (_e) {
    districtManifest = { districts: FALLBACK_DISTRICT_MAP };
  }
}

function populateDistrictHiddenSelect() {
  if (!districtSelHidden) return;
  districtSelHidden.innerHTML =
    `<option value="">${t("select_district")}</option>` +
    (districtManifest?.districts || [])
      .map((d) => `<option value="${d.id}">${d.label}</option>`)
      .join("");
}

// ---------- AC selection ----------
function renderAcPopover() {
  if (!acPopover) return;

  const all = districtACsAll.slice().sort((a, b) => a - b);
  const isAll = isAllACsSelected();

  const wrap = document.createElement("div");

  const allBtn = document.createElement("button");
  allBtn.type = "button";
  allBtn.className = "option";
  allBtn.textContent = isAll ? `✓ ${t("selected_acs_all")}` : t("selected_acs_all");
  allBtn.onclick = () => {
    selectedACs.clear();
    updateSelectedAcText();
    renderAcPopover();
    if (rankedByRelevance.length) applyFiltersThenSortThenRender();
  };
  wrap.appendChild(allBtn);

  all.forEach((ac) => {
    const b = document.createElement("button");
    b.type = "button";
    b.className = "option";
    const on = selectedACs.has(ac);
    b.textContent = `${on ? "✓ " : ""}AC ${String(ac).padStart(2, "0")}`;
    b.onclick = () => {
      if (isAllACsSelected()) {
        selectedACs = new Set(all);
      }
      if (selectedACs.has(ac)) selectedACs.delete(ac);
      else selectedACs.add(ac);

      if (selectedACs.size === all.length) selectedACs.clear();

      updateSelectedAcText();
      renderAcPopover();
      if (rankedByRelevance.length) applyFiltersThenSortThenRender();
    };
    wrap.appendChild(b);
  });

  acPopover.innerHTML = "";
  acPopover.appendChild(wrap);
}

async function preloadDistrictACs(acs, _districtLabel) {
  if (!acs || !acs.length) return;
  const token = ++districtPreloadToken;
  setSearchEnabled(false);
  setDistrictLoading(true);
  try {
    // DB-backed mode: no client preload needed
  } finally {
    if (token === districtPreloadToken) {
      setDistrictLoading(false);
      setSearchEnabled(true);
    }
  }
}

function setDistrictById(id) {
  const d = (districtManifest?.districts || []).find((x) => x.id === id);
  if (!d) return;

  currentDistrictId = d.id;
  currentDistrictLabel = d.label;
  districtACsAll = d.acs.slice();
  selectedACs.clear();

  if (districtSelHidden) districtSelHidden.value = d.id;

  updateDistrictUI();
  updateSelectedAcText();

  rankedByRelevance = [];
  filteredBase = [];
  rankedView = [];
  ageMap = null;
  displayCache.clear();
  scoreCache.clear();
  page = 1;

  preloadDistrictACs(districtACsAll.slice(), currentDistrictLabel);

  if (isResultsVisible()) {
    resultsEl.innerHTML = "";
    pager.style.display = "none";
    pageInfo.textContent = "0";
    resultsCount.textContent = "0";
  }

  setStatus(t("status_ready_district_loaded", { district: currentDistrictLabel, n: districtACsAll.length }));
}

// ---------- Query helpers ----------
function norm(s) {
  return String(s || "").trim();
}

function prefixN(s, n) {
  s = String(s || "").trim();
  if (!s) return "";
  return s.slice(0, n);
}

function tokenize(s) {
  s = norm(s);
  if (!s) return [];
  return s.split(/\s+/g).filter(Boolean);
}

function tokenizeExactIndex(s) {
  return tokenize(s);
}

function tokenizeLoose(s) {
  return tokenize(s);
}

function joinVariantsTokens(tokens) {
  // keep as before (safe fallback)
  if (!Array.isArray(tokens)) return [];
  if (tokens.length < 2) return [];
  const out = [];
  for (let i = 0; i < tokens.length - 1; i++) out.push(tokens[i] + tokens[i + 1]);
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

// ---------- Candidates + Rows (server) ----------
async function getCandidatesForQuery(q, scope, exactOn) {
  const strictTokens = tokenize(q);
  const strictKeys = buildKeysFromTokens(strictTokens, PREFIX_LEN_STRICT);

  const exactTokens = tokenizeExactIndex(q);
  const exactKeys = buildKeysFromTokens(exactTokens, PREFIX_LEN_EXACT);

  const looseTokens = tokenizeLoose(q);
  const looseKeys = buildKeysFromTokens(looseTokens, PREFIX_LEN_LOOSE);

  const districtId = getCurrentDistrictSlug();
  const ac = Number(current.ac);

  if (!districtId || !Number.isFinite(ac)) {
    return { candidates: [], metaByRow: new Map() };
  }

  const json = await callFn("candidates", {
    state: STATE_CODE_DEFAULT,
    districtId,
    ac,
    scope,
    exactOn,
    strictKeys,
    exactKeys,
    looseKeys,
  });

  const candidates = Array.isArray(json.candidates) ? json.candidates.map(Number).filter(Number.isFinite) : [];
  const metaByRow = new Map();
  if (Array.isArray(json.metaByRowEntries)) {
    for (const [rid, meta] of json.metaByRowEntries) {
      const id = Number(rid);
      if (Number.isFinite(id)) metaByRow.set(id, meta || null);
    }
  } else if (json.metaByRow && typeof json.metaByRow === "object") {
    for (const [rid, meta] of Object.entries(json.metaByRow)) {
      const id = Number(rid);
      if (Number.isFinite(id)) metaByRow.set(id, meta || null);
    }
  }
  const outCandidates = candidates.length ? candidates : Array.from(metaByRow.keys());
  return { candidates: outCandidates, metaByRow };
}

async function fetchRowsByIds(rowIds) {
  const districtId = getCurrentDistrictSlug();
  const ac = Number(current.ac);
  if (!districtId || !Number.isFinite(ac) || !rowIds?.length) return [];

  const json = await callFn("rows", {
    state: STATE_CODE_DEFAULT,
    districtId,
    ac,
    mode: "score",
    rowIds: rowIds.map(Number).filter(Number.isFinite),
  });

  const rows = Array.isArray(json.rows) ? json.rows : [];
  for (const r of rows) cacheScoreRow(ac, r);

  return rows.map((r) => ({
    row_id: Number(r.row_id),
    voter_name_raw: r.voter_name_raw ?? "",
    relative_name_raw: r.relative_name_raw ?? "",
    voter_name_norm: r.voter_name_norm ?? "",
    relative_name_norm: r.relative_name_norm ?? "",
    serial_no: r.serial_no ?? "",
  }));
}

async function fetchDisplayRowsByIds(rowIds) {
  const districtId = getCurrentDistrictSlug();
  const ac = Number(current.ac);
  if (!districtId || !Number.isFinite(ac) || !rowIds?.length) return [];

  const json = await callFn("rows", {
    state: STATE_CODE_DEFAULT,
    districtId,
    ac,
    mode: "display",
    rowIds: rowIds.map(Number).filter(Number.isFinite),
  });

  const rows = Array.isArray(json.rows) ? json.rows : [];

  return rows.map((r) => {
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
    const msg = ev.data;
    if (msg.type === "progress") {
      const { done, total, phase, candidates } = msg;
      setStatus(`${phase} • candidates: ${candidates} • scored: ${done}/${total}`);
      return;
    }
    if (msg.type === "done") {
      const ranked = (msg.ranked || []).map((x, i) => ({
        row_id: x.row_id,
        score: typeof x.score === "number" ? x.score : 1000000 - i,
      }));
      pendingResolve?.(ranked);
      pendingResolve = null;
      pendingReject = null;
      return;
    }
    if (msg.type === "error") {
      setStatus(`Worker error: ${msg.message}`);
      pendingReject?.(new Error(msg.message));
      pendingResolve = null;
      pendingReject = null;
    }
  };
}

function runWorkerRanking(rowsWithMeta, qStrict, exactOn, scopeForWorker) {
  initWorker();
  return new Promise((resolve, reject) => {
    pendingResolve = resolve;
    pendingReject = reject;

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

// ---------- Rendering ----------
function escapeHtml(s) {
  return String(s)
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#039;");
}

function renderTable(rows) {
  const cols = DISPLAY_COLS;
  const header = `<thead><tr>${cols
    .map((c) => `<th${c === STICKY_COL_KEY ? ' class="stickyCol"' : ""}>${escapeHtml(headerLabelForKey(c))}</th>`)
    .join("")}</tr></thead>`;

  const body = `<tbody>${rows
    .map((r) => {
      const tds = cols
        .map((c) => `<td${c === STICKY_COL_KEY ? ' class="stickyCol"' : ""}>${escapeHtml(r[c] ?? "")}</td>`)
        .join("");
      return `<tr>${tds}</tr>`;
    })
    .join("")}</tbody>`;

  return `<table class="table">${header}${body}</table>`;
}

function setSortMode(mode) {
  sortMode = mode || SORT.RELEVANCE;
  if (!sortText) return;
  sortText.textContent =
    sortMode === SORT.AGE_ASC
      ? t("sort_by_age_up")
      : sortMode === SORT.AGE_DESC
      ? t("sort_by_age_down")
      : t("sort_by_relevance");
}

function renderSortPopover() {
  if (!sortPopover) return;
  const items = [
    { mode: SORT.RELEVANCE, label: t("sort_by_relevance") },
    { mode: SORT.AGE_ASC, label: t("sort_by_age_up") },
    { mode: SORT.AGE_DESC, label: t("sort_by_age_down") },
  ];
  sortPopover.innerHTML = items
    .map((it) => `<button type="button" class="option${it.mode === sortMode ? " active" : ""}" data-mode="${it.mode}">${escapeHtml(it.label)}</button>`)
    .join("");
  sortPopover.querySelectorAll("button.option").forEach((b) => {
    b.onclick = async () => {
      setSortMode(b.dataset.mode);
      closeSortPopover();
      if (rankedByRelevance.length) await applyFiltersThenSortThenRender();
    };
  });
}

function setupPageSizeDefaultIfNeeded() {
  const mobile = window.matchMedia && window.matchMedia("(max-width: 640px)").matches;
  const desired = mobile ? PAGE_SIZE_MOBILE_DEFAULT : PAGE_SIZE_DESKTOP_DEFAULT;
  if (!pageSize || !Number.isFinite(pageSize)) pageSize = desired;
}

function renderPageSizePopover() {
  if (!pageSizePopover) return;
  const mobile = window.matchMedia && window.matchMedia("(max-width: 640px)").matches;
  const opts = mobile ? PAGE_SIZE_MOBILE_OPTIONS : PAGE_SIZE_DESKTOP_OPTIONS;
  pageSizePopover.innerHTML = opts
    .map((n) => `<button type="button" class="option${n === pageSize ? " active" : ""}" data-n="${n}">${n}</button>`)
    .join("");
  pageSizePopover.querySelectorAll("button.option").forEach((b) => {
    b.onclick = async () => {
      pageSize = Number(b.dataset.n);
      if (pageSizeText) pageSizeText.textContent = String(pageSize);
      closePageSizePopover();
      if (rankedView.length) {
        const totalPages = Math.max(1, Math.ceil(rankedView.length / pageSize));
        page = Math.max(1, Math.min(page, totalPages));
        await renderPage();
      }
      renderPageSizePopover();
    };
  });
  if (pageSizeText) pageSizeText.textContent = String(pageSize);
}

// ---------- Filters (minimal) ----------
let filters = {
  relativeName: "",
  gender: "all",
  age: { mode: "any", a: "", b: "" },
};

function updateMoreFiltersEnabled() {
  if (moreFiltersBtn) moreFiltersBtn.disabled = !(searchEnabled && searchScope === SCOPE.VOTER);
}

function renderFiltersPopoverRoot() {
  if (!filtersPopover || !filtersRoot) return;

  filtersRoot.innerHTML = `
    <div class="row">
      <label>${escapeHtml(t("filter_relative_name"))}</label>
      <input id="relNameInput" type="text" value="${escapeHtml(filters.relativeName || "")}" />
    </div>
  `;

  // wire to existing ids if they exist in DOM
}

function clearFilters() {
  filters = { relativeName: "", gender: "all", age: { mode: "any", a: "", b: "" } };
}

async function applyFiltersThenSortThenRender() {
  filteredBase = rankedByRelevance.slice();

  // sort
  if (sortMode === SORT.RELEVANCE) rankedView = filteredBase.slice();
  else {
    const dir = sortMode === SORT.AGE_DESC ? -1 : 1;
    rankedView = filteredBase.slice().sort((a, b) => {
      const aa = scoreCache.get(a.key)?.ageParsed ?? null;
      const bb = scoreCache.get(b.key)?.ageParsed ?? null;
      const am = aa === null;
      const bm = bb === null;
      if (am && bm) return b.score - a.score;
      if (am) return 1;
      if (bm) return -1;
      if (aa !== bb) return (aa - bb) * dir;
      return b.score - a.score;
    });
  }

  await renderPage();
}

async function renderPage() {
  if (!rankedView.length) {
    resultsEl.innerHTML = "";
    pager.style.display = "none";
    pageInfo.textContent = "0";
    resultsCount.textContent = "0";
    return;
  }

  const total = rankedView.length;
  const totalPages = Math.max(1, Math.ceil(total / pageSize));
  page = Math.max(1, Math.min(page, totalPages));

  const start = (page - 1) * pageSize;
  const end = Math.min(total, start + pageSize);

  pageInfo.textContent = `${start + 1}-${end} / `;
  resultsCount.textContent = String(total);

  const slice = rankedView.slice(start, end);

  // group by AC for display fetch
  const byAc = new Map();
  for (const item of slice) {
    if (!byAc.has(item.ac)) byAc.set(item.ac, []);
    byAc.get(item.ac).push(item.row_id);
  }

  const rowsOut = [];
  for (const [ac, ids] of byAc.entries()) {
    current.ac = ac;
    const rows = await fetchDisplayRowsByIds(ids);
    for (const r of rows) rowsOut.push(r);
  }

  resultsEl.innerHTML = renderTable(rowsOut);

  pager.style.display = totalPages > 1 ? "flex" : "none";
  currentPageCount.textContent = `${page} / ${totalPages}`;

  prevBtn.disabled = page <= 1;
  nextBtn.disabled = page >= totalPages;
}

// ---------- Search ----------
function getActiveQueryInput() {
  return isResultsVisible() ? qResults : qLanding;
}

function setIncludeTypingChecked(v) {
  exactToggleLanding.checked = !!v;
  exactToggleResults.checked = !!v;
}
function getIncludeTypingChecked() {
  return isResultsVisible() ? exactToggleResults.checked : exactToggleLanding.checked;
}
function exactOnFromIncludeTyping() {
  return !Boolean(getIncludeTypingChecked());
}

async function runSearch() {
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
  const scopeForWorker = searchScope === SCOPE.ANYWHERE ? SCOPE.VOTER : searchScope;

  let merged = [];

  for (let i = 0; i < acs.length; i++) {
    const ac = acs[i];
    current.ac = ac;

    setStatus(t("status_stage1_loose", { ac }));

    const { candidates, metaByRow } = await getCandidatesForQuery(q, searchScope, exactOn);
    if (!candidates.length) continue;

    const rows = await fetchRowsByIds(candidates);
    const rowsWithMeta = rows.map((r) => ({ ...r, _meta: metaByRow.get(r.row_id) || null }));

    const ranked = await runWorkerRanking(rowsWithMeta, q, exactOn, scopeForWorker);

    for (const r of ranked) merged.push({ key: makeKey(ac, r.row_id), ac, row_id: r.row_id, score: r.score });
  }

  rankedByRelevance = merged.sort((a, b) => (b.score !== a.score ? b.score - a.score : a.key.localeCompare(b.key)));
  await applyFiltersThenSortThenRender();
  setStatus(t("status_ready_results", { n: rankedView.length }));
}

// ---------- Chips ----------
function setActiveChip(scope) {
  searchScope = scope;
  chipVoter?.classList.toggle("active", scope === SCOPE.VOTER);
  chipRelative?.classList.toggle("active", scope === SCOPE.RELATIVE);
  chipAnywhere?.classList.toggle("active", scope === SCOPE.ANYWHERE);
  updateMoreFiltersEnabled();
}

function refreshChipLabels() {
  // keep existing DOM text via i18n data-i18n
}

// ---------- Events ----------
document.addEventListener("click", (ev) => {
  const inDistrict =
    districtPopoverLanding?.contains(ev.target) ||
    districtBtnLanding?.contains(ev.target) ||
    districtPopover?.contains(ev.target) ||
    districtBtn?.contains(ev.target);
  const inAc = acPopover?.contains(ev.target) || selectedAcBtn?.contains(ev.target);
  const inSort = sortPopover?.contains(ev.target) || sortBtn?.contains(ev.target);
  const inPage = pageSizePopover?.contains(ev.target) || pageSizeBtn?.contains(ev.target);
  const inFilters = filtersPopover?.contains(ev.target) || moreFiltersBtn?.contains(ev.target);

  if (!inDistrict) {
    closeDistrictPopover(districtPopoverLanding, districtBtnLanding);
    closeDistrictPopover(districtPopover, districtBtn);
  }
  if (!inAc) closeAcPopover();
  if (!inSort) closeSortPopover();
  if (!inPage) closePageSizePopover();
  if (!inFilters) closeFiltersPopover();
});

districtBtnLanding.onclick = () => {
  if (districtPopoverLanding.style.display === "block") closeDistrictPopover(districtPopoverLanding, districtBtnLanding);
  else {
    closeDistrictPopover(districtPopover, districtBtn);
    openDistrictPopover(districtPopoverLanding, districtBtnLanding);
  }
};

districtBtn.onclick = () => {
  if (districtPopover.style.display === "block") closeDistrictPopover(districtPopover, districtBtn);
  else {
    closeDistrictPopover(districtPopoverLanding, districtBtnLanding);
    openDistrictPopover(districtPopover, districtBtn);
  }
};

selectedAcBtn.onclick = () => {
  if (acPopover.style.display === "block") closeAcPopover();
  else openAcPopover();
};

sortBtn.onclick = () => {
  if (sortPopover.style.display === "block") closeSortPopover();
  else openSortPopover();
};

pageSizeBtn.onclick = () => {
  if (pageSizePopover.style.display === "block") closePageSizePopover();
  else openPageSizePopover();
};

searchBtnLanding.onclick = () => runSearch();
searchBtnResults.onclick = () => runSearch();

qLanding.addEventListener("keydown", (e) => {
  if (e.key === "Enter") runSearch();
});
qResults.addEventListener("keydown", (e) => {
  if (e.key === "Enter") runSearch();
});

chipVoter.onclick = () => setActiveChip(SCOPE.VOTER);
chipRelative.onclick = () => setActiveChip(SCOPE.RELATIVE);
chipAnywhere.onclick = () => setActiveChip(SCOPE.ANYWHERE);

prevBtn.onclick = async () => {
  page = Math.max(1, page - 1);
  await renderPage();
};
nextBtn.onclick = async () => {
  page = page + 1;
  await renderPage();
};

clearBtn.onclick = () => {
  rankedByRelevance = [];
  filteredBase = [];
  rankedView = [];
  displayCache.clear();
  scoreCache.clear();
  page = 1;
  resultsEl.innerHTML = "";
  pager.style.display = "none";
  pageInfo.textContent = "0";
  resultsCount.textContent = "0";
  showLanding();
  setStatus(t("status_select_district"));
};

iosHintCloseLanding?.addEventListener("click", () => (iosHintLanding.style.display = "none"));
iosHintCloseResults?.addEventListener("click", () => (iosHintResults.style.display = "none"));

// ---------- District popover rendering ----------
function renderDistrictPopover(popEl, btnEl) {
  if (!popEl) return;

  const list = (districtManifest?.districts || []).slice();
  const q = norm(districtQuery).toLowerCase();
  const filtered = q ? list.filter((d) => d.label.toLowerCase().includes(q)) : list;

  popEl.innerHTML = filtered
    .map((d) => `<button type="button" class="option" data-id="${d.id}">${escapeHtml(d.label)}</button>`)
    .join("");

  popEl.querySelectorAll("button.option").forEach((b) => {
    b.onclick = () => {
      setDistrictById(b.dataset.id);
      closeDistrictPopover(popEl, btnEl);
      closeDistrictPopover(districtPopoverLanding, districtBtnLanding);
      closeDistrictPopover(districtPopover, districtBtn);
    };
  });
}

// ---------- Boot ----------
setMeta(t("status_not_loaded"));
setStatus(t("status_select_district"));

setActiveChip(SCOPE.VOTER);
setIncludeTypingChecked(true);

setSearchEnabled(false);
showLanding();

updateDistrictUI();
updateSelectedAcText();

setSortMode(SORT.RELEVANCE);
renderSortPopover();

setupPageSizeDefaultIfNeeded();
renderPageSizePopover();

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
  setLanguage(loadSavedLanguageOrDefault());
  await loadDistrictManifest(STATE_CODE_DEFAULT);
  populateDistrictHiddenSelect();
  setStatus(t("status_select_district"));

  // show iOS hint only for iOS Safari
  if (isIOSSafari()) {
    if (iosHintLanding) iosHintLanding.style.display = "flex";
    if (iosHintResults) iosHintResults.style.display = "flex";
  } else {
    if (iosHintLanding) iosHintLanding.style.display = "none";
    if (iosHintResults) iosHintResults.style.display = "none";
  }

  // render district popovers on open
  const rerender = () => {
    renderDistrictPopover(districtPopoverLanding, districtBtnLanding);
    renderDistrictPopover(districtPopover, districtBtn);
  };
  rerender();

  // initial enable state
  syncSearchButtonState();
})();
