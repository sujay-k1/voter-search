// ---------- Worker ----------
let worker;
let pendingResolve = null;
let pendingReject = null;

function initWorker() {
  if (worker) return;

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

// ---------- Display fetch (current AC) ----------
async function fetchDisplayRowsByIds(rowIds) {
  const districtId = getCurrentDistrictSlug();
  const ac = Number(current.ac);
  if (!districtId || !Number.isFinite(ac) || !rowIds || !rowIds.length) return [];

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
  if (!keysToLoad || !keysToLoad.length) {
    ageMap = ageMap || new Map();
    return;
  }

  ageMap = ageMap || new Map();

  // Fill from scoreCache first
  for (const k of keysToLoad) {
    if (ageMap.has(k)) continue;
    const cached = scoreCache.get(k);
    if (cached) ageMap.set(k, cached.ageParsed ?? null);
  }

  const missingByAc = new Map();
  for (const k of keysToLoad) {
    if (ageMap.has(k)) continue;
    const [acStr, ridStr] = String(k).split(":");
    const ac = Number(acStr);
    const rid = Number(ridStr);
    if (!Number.isFinite(ac) || !Number.isFinite(rid)) continue;
    if (!missingByAc.has(ac)) missingByAc.set(ac, []);
    missingByAc.get(ac).push(rid);
  }

  if (!missingByAc.size) return;

  const districtId = getCurrentDistrictSlug();
  if (!districtId) return;

  let done =
    keysToLoad.length -
    Array.from(missingByAc.values()).reduce((a, b) => a + b.length, 0);
  const total = keysToLoad.length;

  setStatus(t("status_preparing_age_sort"));

  let idx = 0;
  for (const [ac, rids] of missingByAc.entries()) {
    idx++;
    await loadAC(STATE_CODE_DEFAULT, ac);

    for (let i = 0; i < rids.length; i += FETCH_ID_CHUNK) {
      const chunk = rids.slice(i, i + FETCH_ID_CHUNK);
      const json = await callFn("rows", {
        state: STATE_CODE_DEFAULT,
        districtId,
        ac,
        mode: "age",
        rowIds: chunk,
      });

      const rows = Array.isArray(json.rows) ? json.rows : [];
      for (const r of rows) {
        const rid = Number(r.row_id);
        const key = makeKey(ac, rid);
        const ageParsed = parseAgeValue(r.age ?? r.Age ?? r["Age"]);
        ageMap.set(key, ageParsed);

        if (scoreCache.has(key)) {
          const prev = scoreCache.get(key);
          scoreCache.set(key, {
            ...prev,
            ageParsed,
            ageRaw: r.age ?? r.Age ?? r["Age"],
          });
        } else {
          scoreCache.set(key, {
            ageRaw: r.age ?? r.Age ?? r["Age"] ?? null,
            ageParsed,
            genderRaw: null,
            genderBucket: "other",
          });
        }
        done++;
      }
    }

    setStatus(t("status_age_sort_ready", { done, total }));
  }
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
  return isResultsVisible()
    ? exactToggleResults.checked
    : exactToggleLanding.checked;
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
async function computeRowIdSetByGenderAndAgeForAc(rowIdsInThisAc) {
  const hasGender = filters.gender !== "all";
  const hasAge = filters.age.mode !== "any";
  if (!hasGender && !hasAge) return null;
  if (!rowIdsInThisAc || !rowIdsInThisAc.length) return new Set();

  const districtId = getCurrentDistrictSlug();
  const ac = Number(current.ac);
  if (!districtId || !Number.isFinite(ac)) return null;

  const need = [];
  for (const rid of rowIdsInThisAc) {
    const k = makeKey(ac, rid);
    if (!scoreCache.has(k)) need.push(rid);
  }

  for (let i = 0; i < need.length; i += FETCH_ID_CHUNK) {
    const chunk = need.slice(i, i + FETCH_ID_CHUNK);
    const json = await callFn("rows", {
      state: STATE_CODE_DEFAULT,
      districtId,
      ac,
      mode: "score",
      rowIds: chunk,
    });
    const rows = Array.isArray(json.rows) ? json.rows : [];
    for (const r of rows) cacheScoreRow(ac, r);
  }

  const allowed = new Set();

  const mode = filters.age.mode;
  const a = Number(filters.age.a);
  const b = Number(filters.age.b);

  for (const rid of rowIdsInThisAc) {
    const k = makeKey(ac, rid);
    const cached = scoreCache.get(k);

    if (hasGender) {
      const bucket = cached?.genderBucket || "other";
      if (filters.gender !== bucket) continue;
    }

    if (hasAge) {
      const age = cached?.ageParsed ?? null;
      if (age === null) continue;

      if (mode === "eq" && Number.isFinite(a) && age !== a) continue;
      if (mode === "gt" && Number.isFinite(a) && !(age > a)) continue;
      if (mode === "lt" && Number.isFinite(a) && !(age < a)) continue;

      if (mode === "range" && Number.isFinite(a) && Number.isFinite(b)) {
        const lo = Math.min(a, b);
        const hi = Math.max(a, b);
        if (!(age >= lo && age <= hi)) continue;
      }
    }

    allowed.add(Number(rid));
  }

  return allowed;
}

async function computeRowIdSetByRelativeFilterForAc(exactOn) {
  const rel = norm(filters.relativeName || "");
  if (!rel) return null;
  const { candidates } = await getCandidatesForQuery(rel, SCOPE.RELATIVE, exactOn);
  return new Set(candidates.map(Number));
}

/* -------------------------
   The remainder of your file
   (rendering, UI events, boot)
   stays the same EXCEPT:
   - clear scoreCache on new search
-------------------------- */

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
  scoreCache.clear();
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

// (Everything else below remains from your original file)
// -----------------------------------------------------
// IMPORTANT: keep your existing tokenize/norm/render/event/boot code.
// -----------------------------------------------------


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
