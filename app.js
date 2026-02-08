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

async function postJson(url, data, { timeoutMs = 25000 } = {}) {
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
    try { json = text ? JSON.parse(text) : null; } catch { json = null; }

    if (!resp.ok) {
      const msg = (json && (json.error || json.message)) || text || `HTTP ${resp.status}`;
      throw new Error(msg);
    }
    return json;
  } finally {
    clearTimeout(t);
  }
}

async function callFn(name, payload) {
  return await postJson(fnUrl(name), payload);
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
let rankedByRelevance = []; // full [{key, ac, row_id, score}]
let filteredBase = []; // after filters, in relevance order
let rankedView = []; // after sort, for paging
let page = 1;

let pageSize = PAGE_SIZE_DESKTOP_DEFAULT;

// Used to cancel district-preload if user switches district quickly
let districtPreloadToken = 0;

let ageMap = null; // Map(key -> ageNumber|null)
let displayCache = new Map(); // Map(key -> rowObject)

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

function renderTranslitSuggestions(
  popEl,
  suggestions,
  { onPick, activeIndex = -1 } = {}
) {
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
  let lastInterim = "";
  let suppressSuggestDuringSpeech = false;

  function now() {
    return Date.now();
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
    // Keep search button state behavior intact
    // (existing listeners still attached globally; we don't remove them)

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

      if (
        e.key === "Escape" ||
        e.key === "ArrowDown" ||
        e.key === "ArrowUp" ||
        e.key === "Enter"
      ) {
        e.preventDefault();
        e.stopPropagation();
        if (typeof e.stopImmediatePropagation === "function")
          e.stopImmediatePropagation();
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
    const SpeechRecognition =
      window.SpeechRecognition || window.webkitSpeechRecognition;
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
    lastInterim = "";
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
          let finalText = "";

          for (let i = ev.resultIndex; i < ev.results.length; i++) {
            const res = ev.results[i];
            const txt = res?.[0]?.transcript ?? "";
            if (res.isFinal) finalText += txt;
            else interim += txt;
          }

          interim = String(interim || "").trim();
          finalText = String(finalText || "").trim();

          if (interim && interim !== lastInterim) {
            lastInterim = interim;
            ignoreUntil = now() + 60;
            setInputValueNoRerender(inputEl, interim);
          }

          if (finalText) {
            handleSpeechFinal(finalText);
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

  if (
    !isResultsVisible() &&
    (!current.lastQuery || !String(current.lastQuery).trim())
  ) {
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

// Page-size popover
const pageSizeBtn = $("pageSizeBtn");
const pageSizeText = $("pageSizeText");
const pageSizePopover = $("pageSizePopover");

// status/meta (both landing + results)
const statusLanding = $("statusLanding");
const metaLanding = $("metaLanding");
const statusResults = $("statusResults");
const metaResults = $("metaResults");

function setStatus(msg) {
  if (statusLanding) statusLanding.textContent = msg ?? "";
  if (statusResults) statusResults.textContent = msg ?? "";
}

function setBar(_pct) {}

function setMeta(msg) {
  if (metaLanding) metaLanding.textContent = msg ?? "";
  if (metaResults) metaResults.textContent = msg ?? "";
}

function showLanding() {
  landingSection.style.display = "flex";
  resultsSection.style.display = "none";
}
function showResults() {
  landingSection.style.display = "none";
  resultsSection.style.display = "block";
}
function isResultsVisible() {
  return window.getComputedStyle(resultsSection).display !== "none";
}

function getActiveQueryInput() {
  return isResultsVisible() ? qResults : qLanding;
}

function setSearchEnabled(enabled) {
  qLanding.disabled = !enabled;
  qResults.disabled = !enabled;
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
  const canSearch =
    Boolean(districtACsAll.length) &&
    q.length > 0 &&
    !qLanding.disabled &&
    !qResults.disabled;
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
  if (m === "eq")
    return `${t("equal_to")} ${(filters.age.a ?? "").toString().trim()}`;
  if (m === "gt")
    return `${t("greater_than")} ${(filters.age.a ?? "").toString().trim()}`;
  if (m === "lt")
    return `${t("less_than")} ${(filters.age.a ?? "").toString().trim()}`;
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
  if (!raw) return { districts: FALLBACK_DISTRICT_MAP };

  if (Array.isArray(raw.districts)) {
    return {
      districts: raw.districts
        .map((d) => ({
          id:
            String(d.id ?? d.label ?? d.name ?? "").trim() ||
            String(d.name ?? d.label ?? "District"),
          label: String(d.label ?? d.name ?? d.id ?? "District"),
          acs: Array.isArray(d.acs)
            ? d.acs.map(Number).filter(Number.isFinite)
            : [],
        }))
        .filter((d) => d.acs.length > 0),
    };
  }

  if (typeof raw === "object") {
    const districts = [];
    for (const [k, v] of Object.entries(raw)) {
      if (!Array.isArray(v)) continue;
      const acs = v.map(Number).filter(Number.isFinite);
      if (!acs.length) continue;
      districts.push({ id: k, label: k, acs });
    }
    if (districts.length) return { districts };
  }

  return { districts: FALLBACK_DISTRICT_MAP };
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
    console.warn(
      "Using fallback district mapping because manifest load failed:",
      e
    );
    districtManifest = { districts: FALLBACK_DISTRICT_MAP };
  }
}

// Keep hidden select populated for compatibility (not used for UI)
function populateDistrictHiddenSelect() {
  if (!districtSelHidden) return;
  districtSelHidden.innerHTML =
    `<option value="">${escapeHtml(t("select_district"))}</option>` +
    (districtManifest?.districts || [])
      .map(
        (d) =>
          `<option value="${escapeHtml(d.id)}">${escapeHtml(
            d.label
          )}</option>`
      )
      .join("");
}

function updateDistrictUI() {
  const label = currentDistrictLabel || t("select_district");
  if (districtMirror) districtMirror.textContent = label;
  if (districtMirrorLanding) districtMirrorLanding.textContent = label;
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
async function preloadDistrictACs(acs, districtLabel) {
  if (!acs || !acs.length) return;

  const token = ++districtPreloadToken;

  setSearchEnabled(false);
  setDistrictLoading(true);

  try {
    setStatus(
      t("status_loading_district", { district: districtLabel, n: acs.length })
    );
    setMeta("");

    for (let i = 0; i < acs.length; i++) {
      if (token !== districtPreloadToken) return; // cancelled

      const ac = acs[i];
      setStatus(
        t("status_loading_district_ac", {
          district: districtLabel,
          ac,
          i: i + 1,
          n: acs.length,
        })
      );
      try {
        await loadAC(STATE_CODE_DEFAULT, ac);
      } catch (e) {
        console.warn("Preload: skipping AC due to load error:", ac, e);
      }
    }

    if (token !== districtPreloadToken) return;
    setStatus(
      t("status_ready_district_loaded", {
        district: districtLabel,
        n: acs.length,
      })
    );
  } finally {
    if (token === districtPreloadToken) {
      setDistrictLoading(false);
      setSearchEnabled(true);
      syncSearchButtonState();
    }
  }
}

function setDistrictById(id) {
  const d = (districtManifest?.districts || []).find((x) => x.id === id);
  if (!d) return;

  currentDistrictId = d.id;
  currentDistrictLabel = d.label;
  districtACsAll = d.acs.slice().map(Number).filter(Number.isFinite);
  selectedACs.clear(); // default = all

  if (districtSelHidden) districtSelHidden.value = d.id;

  updateDistrictUI();
  updateSelectedAcText();

  rankedByRelevance = [];
  filteredBase = [];
  rankedView = [];
  ageMap = null;
  displayCache.clear();
  clearFilters();
  closeFiltersPopover();
  closeDistrictPopovers();
  closeAcPopover();
  closeSortPopover();
  closePageSizePopover();

  preloadDistrictACs(districtACsAll.slice(), currentDistrictLabel);

  syncSearchButtonState();
}

// ---------- Gender domain discovery (per loaded AC) ----------
function normGenderValue(v) {
  const s = String(v ?? "").trim().toLowerCase();
  if (!s) return "other";
  if (
    s === "m" ||
    s.includes("male") ||
    s.includes("पुरुष") ||
    s.includes("पु") ||
    s.includes("man")
  )
    return "male";
  if (
    s === "f" ||
    s.includes("female") ||
    s.includes("महिला") ||
    s.includes("स्त्री") ||
    s.includes("woman")
  )
    return "female";
  if (s.includes("other") || s.includes("अन्य") || s === "o") return "other";
  return "other";
}

async function loadGenderDomain() {
  // No-op in Turso-backed version (gender filtering happens client-side by fetching Age/Gender for candidate ids).
  genderBuckets = { male: new Set(), female: new Set(), other: new Set() };
}


// ---------- Load AC (views swap to that AC) ----------
async function loadAC(stateCode, acNo) {
  // Turso-backed: no local parquet loading; we only validate connectivity and cache meta per AC.
  current.state = stateCode;
  current.ac = acNo;

  const districtSlug = slugifyDistrictId(currentDistrictId || "");
  if (!districtSlug) {
    current.loaded = false;
    throw new Error("District not selected");
  }

  const cacheKey = `${districtSlug}:${Number(acNo)}`;
  if (!loadAC._metaCache) loadAC._metaCache = new Map();
  const metaCache = loadAC._metaCache;

  if (metaCache.has(cacheKey)) {
    current.meta = metaCache.get(cacheKey);
    current.loaded = true;
    const voters = current.meta?.voters;
    setMeta(
      voters !== undefined
        ? `Loaded AC${String(acNo).padStart(2, "0")} • voters: ${voters}`
        : `Loaded AC${String(acNo).padStart(2, "0")}`
    );
    return;
  }

  // Lightweight "ping" + voters count (helps keep existing preload/status microinteraction)
  const meta = await callFn("ac_meta", {
    district: districtSlug,
    state: stateCode,
    ac: Number(acNo),
  });

  current.meta = meta || null;
  current.loaded = true;
  metaCache.set(cacheKey, current.meta);

  const voters = current.meta?.voters;
  setMeta(
    voters !== undefined
      ? `Loaded AC${String(acNo).padStart(2, "0")} • voters: ${voters}`
      : `Loaded AC${String(acNo).padStart(2, "0")}`
  );
}



/* --------------- REST OF YOUR FILE ---------------
   Everything below here is unchanged EXCEPT:
   - initWorker(): worker URL hardened using import.meta.url
   (This prevents odd path resolution issues on some hosts)
-------------------------------------------------- */

// ---------- Candidate generation ----------
async function queryIndexCandidates(viewName, keys) {
  if (!keys || !keys.length) return new Map();

  const districtSlug = slugifyDistrictId(currentDistrictId || "");
  if (!districtSlug) throw new Error("District not selected");

  const viewToTable = {
    idx_voter: "idx_voter_strict",
    idx_relative: "idx_relative_strict",
    idx_loose_voter: "idx_voter_loose",
    idx_loose_relative: "idx_relative_loose",
    idx_exact_voter: "idx_voter_exact",
    idx_exact_relative: "idx_relative_exact",
  };

  const table = viewToTable[viewName];
  if (!table) throw new Error(`Unknown index view: ${viewName}`);

  const resp = await callFn("candidates", {
    district: districtSlug,
    state: current.state || STATE_CODE_DEFAULT,
    ac: Number(current.ac),
    table,
    keys,
  });

  const m = new Map();
  for (const r of resp?.rows || []) {
    m.set(Number(r.row_id), {
      hit_count: Number(r.hit_count),
      and_hit: Boolean(r.and_hit),
    });
  }
  return m;
}


function buildKeysFromTokens

function buildKeysFromTokens(tokens, prefixLen) {
  const keys = tokens.map((t) => prefixN(t, prefixLen)).filter(Boolean);
  const joins = joinVariantsTokens(tokens);
  for (const j of joins) {
    const k = prefixN(j, prefixLen);
    if (k) keys.push(k);
  }
  return Array.from(new Set(keys));
}

async function getCandidatesForQuery(q, scope, exactOn) {
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

  let strictVoterMap = new Map(),
    strictRelMap = new Map();
  let exactVoterMap = new Map(),
    exactRelMap = new Map();
  let looseVoterMap = new Map(),
    looseRelMap = new Map();

  const jobs = [];
  const wantLoose = !exactOn;

  if (scope === SCOPE.VOTER) {
    if (strictKeys.length)
      jobs.push(
        queryIndexCandidates("idx_voter", strictKeys).then(
          (m) => (strictVoterMap = m)
        )
      );
    if (exactKeys.length)
      jobs.push(
        queryIndexCandidates("idx_exact_voter", exactKeys).then(
          (m) => (exactVoterMap = m)
        )
      );
    if (wantLoose && looseKeys.length)
      jobs.push(
        queryIndexCandidates("idx_loose_voter", looseKeys).then(
          (m) => (looseVoterMap = m)
        )
      );
  } else if (scope === SCOPE.RELATIVE) {
    if (strictKeys.length)
      jobs.push(
        queryIndexCandidates("idx_relative", strictKeys).then(
          (m) => (strictRelMap = m)
        )
      );
    if (exactKeys.length)
      jobs.push(
        queryIndexCandidates("idx_exact_relative", exactKeys).then(
          (m) => (exactRelMap = m)
        )
      );
    if (wantLoose && looseKeys.length)
      jobs.push(
        queryIndexCandidates("idx_loose_relative", looseKeys).then(
          (m) => (looseRelMap = m)
        )
      );
  } else {
    if (strictKeys.length) {
      jobs.push(
        queryIndexCandidates("idx_voter", strictKeys).then(
          (m) => (strictVoterMap = m)
        )
      );
      jobs.push(
        queryIndexCandidates("idx_relative", strictKeys).then(
          (m) => (strictRelMap = m)
        )
      );
    }
    if (exactKeys.length) {
      jobs.push(
        queryIndexCandidates("idx_exact_voter", exactKeys).then(
          (m) => (exactVoterMap = m)
        )
      );
      jobs.push(
        queryIndexCandidates("idx_exact_relative", exactKeys).then(
          (m) => (exactRelMap = m)
        )
      );
    }
    if (wantLoose && looseKeys.length) {
      jobs.push(
        queryIndexCandidates("idx_loose_voter", looseKeys).then(
          (m) => (looseVoterMap = m)
        )
      );
      jobs.push(
        queryIndexCandidates("idx_loose_relative", looseKeys).then(
          (m) => (looseRelMap = m)
        )
      );
    }
  }

  await Promise.all(jobs);

  const metaByRow = new Map();

  function upsert(row_id, patch) {
    const cur =
      metaByRow.get(row_id) || {
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
    metaByRow.set(row_id, { ...cur, ...patch });
  }

  for (const [rid, m] of strictVoterMap.entries())
    upsert(rid, { voter_hit_count: m.hit_count, voter_and_hit: m.and_hit });
  for (const [rid, m] of strictRelMap.entries())
    upsert(rid, {
      relative_hit_count: m.hit_count,
      relative_and_hit: m.and_hit,
    });
  for (const [rid, m] of exactVoterMap.entries())
    upsert(rid, {
      voter_exact_hit_count: m.hit_count,
      voter_exact_and_hit: m.and_hit,
    });
  for (const [rid, m] of exactRelMap.entries())
    upsert(rid, {
      relative_exact_hit_count: m.hit_count,
      relative_exact_and_hit: m.and_hit,
    });
  for (const [rid, m] of looseVoterMap.entries())
    upsert(rid, {
      voter_loose_hit_count: m.hit_count,
      voter_loose_and_hit: m.and_hit,
    });
  for (const [rid, m] of looseRelMap.entries())
    upsert(rid, {
      relative_loose_hit_count: m.hit_count,
      relative_loose_and_hit: m.and_hit,
    });

  const candidates = Array.from(metaByRow.keys());
  return { candidates, metaByRow, strictKeys, exactKeys, looseKeys };
}

// ---------- Fetch scoring rows (current loaded AC) ----------
async function fetchRowsByIds(rowIds) {
  if (!rowIds || !rowIds.length) return [];

  const districtSlug = slugifyDistrictId(currentDistrictId || "");
  if (!districtSlug) throw new Error("District not selected");

  const out = [];
  for (let i = 0; i < rowIds.length; i += FETCH_ID_CHUNK) {
    const chunk = rowIds.slice(i, i + FETCH_ID_CHUNK).map(Number);

    const resp = await callFn("rows", {
      district: districtSlug,
      state: current.state || STATE_CODE_DEFAULT,
      ac: Number(current.ac),
      kind: "score",
      row_ids: chunk,
    });

    for (const r of resp?.rows || []) {
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


// ---------- Worker

// ---------- Worker ----------
let worker;
let pendingResolve = null;
let pendingReject = null;

function initWorker() {
  if (worker) return;

  // Harden worker URL resolution (safe under subfolders / different base href)
  worker = new Worker(new URL("./worker.js", import.meta.url), {
    type: "module",
  });

  worker.onmessage = async (ev) => {
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
      if (pendingResolve) {
        const r = pendingResolve;
        pendingResolve = null;
        pendingReject = null;
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
        rej(new Error(msg.message));
      }
      return;
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

// ---------- PDF link ----------
function buildPdfUrl(row) {
  const state = formatCell(row["State Code"]);
  const ac = formatCell(row["AC No"]);
  const part = formatCell(row["Part No"]);
  if (!state || !ac || !part) return "";
  return `https://www.eci.gov.in/sir/f3/${state}/data/OLDSIRROLL/${state}/${ac}/${state}_${ac}_${part}.pdf`;
}

// ---------- Display fetch (current loaded AC) ----------
async function fetchDisplayRowsByIds(rowIds) {
  if (!rowIds || !rowIds.length) return [];

  const districtSlug = slugifyDistrictId(currentDistrictId || "");
  if (!districtSlug) throw new Error("District not selected");

  const resp = await callFn("rows", {
    district: districtSlug,
    state: current.state || STATE_CODE_DEFAULT,
    ac: Number(current.ac),
    kind: "display",
    row_ids: rowIds.map(Number),
  });

  const rows = resp?.rows || [];
  // Normalize row_id to number (keep the rest of keys verbatim)
  return rows.map((r) => {
    const out = { ...r };
    out.row_id = Number(out.row_id);
    return out;
  });
}


// ---------- Sorting

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
    await loadAC(STATE_CODE_DEFAULT, ac);

    for (let i = 0; i < rids.length; i += FETCH_ID_CHUNK) {
      const chunk = rids.slice(i, i + FETCH_ID_CHUNK).map(Number);

      const resp = await callFn("rows", {
        district: districtSlug,
        state: current.state || STATE_CODE_DEFAULT,
        ac: Number(ac),
        kind: "age",
        row_ids: chunk,
      });

      for (const r of resp?.rows || []) {
        const rid = Number(r.row_id);
        const k = makeKey(ac, rid);
        ageMap.set(k, parseAgeValue(r.Age ?? r.age));
        done++;
      }
    }
  }

  setStatus(t("status_age_sort_ready", { done, total }));
}


function setSortMode

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
    rankedView = filteredBase.slice();
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

    if (aMissing && bMissing) {
      if (b.score !== a.score) return b.score - a.score;
      return String(a.key).localeCompare(String(b.key));
    }
    if (aMissing) return 1;
    if (bMissing) return -1;

    if (aa !== bb) return (aa - bb) * dir;
    if (b.score !== a.score) return b.score - a.score;
    return String(a.key).localeCompare(String(b.key));
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

// Compute row-id set by Gender/Age for ONE AC (views already loaded)
async function computeRowIdSetByGenderAndAgeForAc(rowIdsInThisAc) {
  const hasGender = filters.gender !== "all";
  const hasAge = filters.age.mode !== "any";
  if (!hasGender && !hasAge) return null;
  if (!rowIdsInThisAc || !rowIdsInThisAc.length) return new Set();

  const districtSlug = slugifyDistrictId(currentDistrictId || "");
  if (!districtSlug) throw new Error("District not selected");

  // Fetch Age/Gender for these ids and filter client-side.
  const out = new Set();

  for (let i = 0; i < rowIdsInThisAc.length; i += FETCH_ID_CHUNK) {
    const chunk = rowIdsInThisAc.slice(i, i + FETCH_ID_CHUNK).map(Number);

    const resp = await callFn("rows", {
      district: districtSlug,
      state: current.state || STATE_CODE_DEFAULT,
      ac: Number(current.ac),
      kind: "gender_age",
      row_ids: chunk,
    });

    for (const r of resp?.rows || []) {
      const rid = Number(r.row_id);

      if (hasGender) {
        const g = normGenderValue(r.Gender ?? r.gender);
        if (filters.gender !== g) continue;
      }

      if (hasAge) {
        const age = parseAgeValue(r.Age ?? r.age);
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
          const lo = Math.min(a, b), hi = Math.max(a, b);
          if (age === null || age < lo || age > hi) continue;
        }
      }

      out.add(rid);
    }
  }

  return out;
}


async function computeRowIdSetByRelativeFilterForAc

async function computeRowIdSetByRelativeFilterForAc(exactOn) {
  const rel = norm(filters.relativeName || "");
  if (!rel) return null;
  const { candidates } = await getCandidatesForQuery(rel, SCOPE.RELATIVE, exactOn);
  return new Set(candidates.map(Number));
}

async function applyFiltersThenSortThenRender() {
  filteredBase = rankedByRelevance.slice();

  if (searchScope !== SCOPE.VOTER) {
    clearFilters();
  }

  if (searchScope === SCOPE.VOTER && rankedByRelevance.length) {
    const exactOn = exactOnFromIncludeTyping();

    const byAc = new Map();
    for (const x of rankedByRelevance) {
      if (!byAc.has(x.ac)) byAc.set(x.ac, []);
      byAc.get(x.ac).push(x.row_id);
    }

    const allowedKeys = new Set();

    setStatus(t("status_applying_filters"));

    let acIdx = 0;
    for (const [ac, rowIds] of byAc.entries()) {
      acIdx++;
      setStatus(t("status_applying_filters_ac", { ac, i: acIdx, n: byAc.size }));

      await loadAC(STATE_CODE_DEFAULT, ac);

      let relSet = null;
      if (norm(filters.relativeName || "")) {
        relSet = await computeRowIdSetByRelativeFilterForAc(exactOn);
      }

      const gaSet = await computeRowIdSetByGenderAndAgeForAc(rowIds);

      for (const rid of rowIds) {
        if (relSet && !relSet.has(rid)) continue;
        if (gaSet && !gaSet.has(rid)) continue;
        allowedKeys.add(makeKey(ac, rid));
      }
    }

    if (allowedKeys.size) {
      filteredBase = rankedByRelevance.filter((x) => allowedKeys.has(x.key));
    } else if (norm(filters.relativeName || "") || filters.gender !== "all" || filters.age.mode !== "any") {
      filteredBase = [];
    }
  }

  await applySort();

  page = 1;
  displayCache.clear();

  await renderPage();
  resultsCountEl.textContent = String(rankedView.length || 0);

  updateMoreFiltersEnabled();
  renderFiltersPopoverRoot();
  renderSortPopover();
  renderPageSizePopover();
}

// ---------- Search ----------
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
  displayCache.clear();
  page = 1;

  pagerEl.style.display = "none";
  $("results").innerHTML = "";

  setBar(0);

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

  const scopeForWorker = searchScope === SCOPE.ANYWHERE ? SCOPE.VOTER : searchScope;

  const acList = getActiveACs();
  if (!acList.length) {
    setStatus(t("status_no_acs_selected"));
    return;
  }

  showResults();
  resultsCountEl.textContent = "0";

  const merged = [];

  for (let i = 0; i < acList.length; i++) {
    const ac = acList[i];

    setStatus(t("status_stage0", { ac, i: i + 1, n: acList.length }));

    setDistrictLoading(true);
    try {
      await loadAC(STATE_CODE_DEFAULT, ac);
    } catch (e) {
      console.warn("Skipping AC due to load error:", ac, e);
      continue;
    } finally {
      setDistrictLoading(false);
    }

    setStatus(exactOn ? t("status_stage1_exact", { ac }) : t("status_stage1_loose", { ac }));

    const { candidates, metaByRow } = await getCandidatesForQuery(qStrict, searchScope, exactOn);
    if (!candidates.length) continue;

    setStatus(t("status_stage2", { n: candidates.length, ac }));

    const rows = await fetchRowsByIds(candidates);
    const rowsWithMeta = rows.map((r) => ({ ...r, _meta: metaByRow.get(r.row_id) || null }));

    setStatus(t("status_stage3", { n: rowsWithMeta.length, ac }));

    const ranked = await runWorkerRanking(rowsWithMeta, qStrict, exactOn, scopeForWorker);

    for (const r of ranked) {
      merged.push({ key: makeKey(ac, r.row_id), ac, row_id: r.row_id, score: r.score });
    }

    resultsCountEl.textContent = String(merged.length);
  }

  rankedByRelevance = merged.sort((a, b) => {
    if (b.score !== a.score) return b.score - a.score;
    if (a.ac !== b.ac) return a.ac - b.ac;
    return a.row_id - b.row_id;
  });

  await applyFiltersThenSortThenRender();
  setStatus(t("status_ready_results", { n: rankedView.length }));
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
    let idx = 0;
    for (const [ac, rowIds] of missingByAc.entries()) {
      idx++;
      setStatus(t("status_loading_page_rows", { page, ac, i: idx, n: missingByAc.size }));
      await loadAC(STATE_CODE_DEFAULT, ac);
      const rows = await fetchDisplayRowsByIds(rowIds);
      for (const r of rows) {
        const k = makeKey(ac, r.row_id);
        displayCache.set(k, r);
      }
    }
  }

  const scoreMap = new Map(slice.map((x) => [x.key, x.score]));
  const orderedRows = slice.map((x) => displayCache.get(x.key)).filter(Boolean);

  $("results").innerHTML = renderTable(orderedRows, scoreMap);

  pagerEl.style.display = total > 0 ? "flex" : "none";
  currentPageCount.textContent = t("page_x_of_y", { p: page, t: totalPages });
  pageInfo.textContent = t("showing_prefix", { from: total ? start + 1 : 0, to: end });
  resultsCountEl.textContent = String(total);

  if (total) setStatus(t("status_showing_range", { from: start + 1, to: end, total }));
  else setStatus(t("status_ready_results", { n: 0 }));
}

function renderTable(rows, scoreMap) {
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
          const score = scoreMap.get(k);

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
                const title =
                  h.key === "Voter Name" && typeof score === "number"
                    ? `title="score: ${score.toFixed(0)}"`
                    : "";
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

    if (reason === "scope" || reason === "exact") {
      if (hasQueryableState()) await runSearch();
      return;
    }

    if (reason === "filters" || reason === "sort") {
      if (rankedByRelevance.length) await applyFiltersThenSortThenRender();
      return;
    }

    if (reason === "district" || reason === "acs") {
      if (hasQueryableState()) await runSearch();
      return;
    }
  }, 90);
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

function openModal({ title, subtitle, fields, onDone }) {
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

  modalOverlay.style.display = "flex";
  modalOverlay.setAttribute("aria-hidden", "false");

  const first = fields.find((f) => f._el)?._el;
  if (first) setTimeout(() => first.focus(), 0);
}

function closeModal() {
  modalOverlay.style.display = "none";
  modalOverlay.setAttribute("aria-hidden", "true");
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
    fields: [{ inputType: "text", placeholder: t("modal_enter_name"), value: filters.relativeName || "" }],
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

  const q = (districtQuery || "").trim().toLowerCase();
  const list = (districtManifest?.districts || []).filter((d) => {
    if (!q) return true;
    return String(d.label || d.id || "").toLowerCase().includes(q);
  });

  for (const d of list) {
    const isSel = d.id === currentDistrictId;
    const row = popRow({
      left: d.label,
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
      left: `AC ${ac}`,
      right: "",
      chevron: false,
      selected: effectiveSelected,
      onClick: () => {
        if (isAllACsSelected()) selectedACs = new Set(districtACsAll);

        if (selectedACs.has(ac)) selectedACs.delete(ac);
        else selectedACs.add(ac);

        if (selectedACs.size === districtACsAll.length) selectedACs.clear();

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
