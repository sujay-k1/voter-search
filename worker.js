// worker.js (ES module)
//
// ✅ Keeps your existing behavior:
// 1) 1-word query matches ANY token position (and joined variants)
// 2) Independent vowels (अ/आ, इ/ई, ...) are in PHONETIC sets AND treated as consonant-side entities
// 3) Marks (ँंः़्) are counted as matra mismatches
// 4) Existing PREFIX FALLBACK for K=2/3 still exists and ranks below FULL
//
// ✅ NEW (fixes "ईसिडोर ति" / partial-last-word returning 0):
// 5) NEW LOW-RANK fallback family that allows:
//    - suffix-only additions (candidate word longer than query word)
//      * 1-word query: unlimited additions
//      * 2+ words: first word additions capped at 2 entities; later words unlimited
//    - "outside substitutions" (NOT in your visual/phonetic sets) with caps per query-word length:
//      * qLen=3 => 1
//      * qLen=4..8 => 2
//      * qLen>=9 => 3
//    - Standard substitutions from your sets are still allowed and do NOT count as outside substitutions
//
// Ranking order (best -> worst):
//   EXACT mode > TYPO_FULL > TYPO_PF > TYPO_ADD_OUTSIDE

/* =====================
   Character classes
===================== */
const INDEP_VOWELS = new Set(["अ","आ","इ","ई","उ","ऊ","ए","ऐ","ओ","औ","ऋ","ॠ","ऌ","ॡ"]);
const MATRAS = new Set(["ा","ि","ी","ु","ू","े","ै","ो","ौ","ृ","ॄ","ॢ","ॣ"]);
const MARKS_AS_MATRA = new Set(["ँ","ं","ः","़","्"]);

const TYPE = {
  EXACT: 0,
  PHONETIC: 1,
  VISUAL_P0: 2,
  VISUAL_P1: 3,
  VISUAL_P2: 4,
  OTHER: 9
};

// Caps from your rules (FULL word compare caps)
const MAX_CONS_MISMATCH_PER_WORD = 4;
const MAX_TOTAL_CONS_2WORD = 5;
const MAX_TOTAL_CONS_3PLUS = 7;

// Prefix fallback policy (older K=2/3 policy)
const PREFIX_FALLBACK_MAX_EXTRA_SUFFIX = 2;
const PREFIX_FALLBACK_GLOBAL_EXTRA_PER_WORD = 2;
const PREFIX_K2_MAX_SUBS = 1;
const PREFIX_K3_MAX_SUBS = 2;

// NEW additions policy (low rank)
const ADD_FALLBACK_FIRST_WORD_MAX_ADD_ENTITIES_IN_MULTI = 2;

/* =====================
   Sets (YOUR FINAL POLICY)
   phonetic > all visual (P0/P1/P2)
===================== */
const VISUAL_P0 = [
  ["ए","प"],
  ["क","फ"],
  ["ख","रव","थ","य","रा","स","श"],
  ["ग","रा","म"],
  ["घ","ध","छ"],
  ["ङ","ड","ह"],
  ["च","ज","ज्ञ","ञ"],
  ["झ","डा"],
  ["ट","ढ","द","ठ"],
  ["त","न"],
  ["प","ष","य","भ","म","न","प्न"],
  ["ब","व","ञ"],
  ["र","१"],
  ["श","रा","१।"],
  ["त्र","ञ"],
  ["त्त","त"],
  ["स्न","स"],
];

const VISUAL_P1 = [
  ["ण","ग"],
  ["ह","हा","घ","छ"],
  ["ड","ह","इ","झ"],
  ["प","ए"],
  ["स","रा","श"],
  ["र","ल"],
];

const VISUAL_P2 = [
  ["प","फ","च"],
];

const PHONETIC = [
  ["अ","आ"],
  ["इ","ई"],
  ["उ","ऊ"],
  ["ए","ऐ"],
  ["ओ","औ"],
  ["ऋ","ॠ"],
  ["ऌ","ॡ"],

  ["क","ख"],
  ["ग","घ","ह"],
  ["च","छ"],
  ["ज","झ"],
  ["ट","ठ"],
  ["ड","ढ","द","ध","त","थ"],
  ["ण","न"],
  ["प","फ"],
  ["ब","भ","व"],
  ["य","ज"],
  ["स","श","ष"],
  ["त्र","ट्र"],
  ["ज्ञ","ज्या"],
  ["र","ड़"],
];

/* =====================
   Substitution maps
===================== */
function makePairMap(groups, typeCode) {
  const m = new Map();
  for (const g of groups) {
    for (let i = 0; i < g.length; i++) {
      for (let j = 0; j < g.length; j++) {
        if (i === j) continue;
        m.set(`${g[i]}|${g[j]}`, typeCode);
      }
    }
  }
  return m;
}

const PHON_MAP = makePairMap(PHONETIC, TYPE.PHONETIC);
const VIS0_MAP = makePairMap(VISUAL_P0, TYPE.VISUAL_P0);
const VIS1_MAP = makePairMap(VISUAL_P1, TYPE.VISUAL_P1);
const VIS2_MAP = makePairMap(VISUAL_P2, TYPE.VISUAL_P2);

function substType(a, b) {
  if (a === b) return TYPE.EXACT;

  const p = PHON_MAP.get(`${a}|${b}`);
  if (p != null) return p;

  const v0 = VIS0_MAP.get(`${a}|${b}`);
  if (v0 != null) return v0;

  const v1 = VIS1_MAP.get(`${a}|${b}`);
  if (v1 != null) return v1;

  const v2 = VIS2_MAP.get(`${a}|${b}`);
  if (v2 != null) return v2;

  return TYPE.OTHER;
}

/* =====================
   Normalization / tokenization
===================== */
function normStrict(s) {
  if (s == null) return "";
  s = String(s).replace(/\u00A0/g, " ").trim();
  s = s.replace(/[.,;:|/\\()[\]{}<>"'`~!@#$%^&*_+=?-]/g, " ");
  s = s.replace(/\s+/g, " ").trim();
  return s;
}
function tokenizeStrict(s) {
  s = normStrict(s);
  if (!s) return [];
  return s.split(" ").filter(Boolean);
}
function stripMarks(s) {
  const x = normStrict(s);
  if (!x) return "";
  let out = "";
  for (const ch of x) {
    if (MARKS_AS_MATRA.has(ch)) continue;
    out += ch;
  }
  return out.replace(/\s+/g, " ").trim();
}
function countMarks(s) {
  const x = normStrict(s);
  if (!x) return 0;
  let c = 0;
  for (const ch of x) if (MARKS_AS_MATRA.has(ch)) c++;
  return c;
}

/* =====================
   Entity segmentation (multi-glyph units)
===================== */
function buildEntityList() {
  const set = new Set();
  const addAll = (arr) => { for (const g of arr) for (const it of g) set.add(it); };
  addAll(VISUAL_P0);
  addAll(VISUAL_P1);
  addAll(VISUAL_P2);
  addAll(PHONETIC);
  set.add("१");
  set.add("१।");
  return Array.from(set).filter(Boolean).sort((a, b) => b.length - a.length);
}
const ENTITY_LIST = buildEntityList();

function segmentEntities(word) {
  const w = word || "";
  const out = [];
  let i = 0;
  while (i < w.length) {
    let matched = null;
    for (const ent of ENTITY_LIST) {
      if (ent && w.startsWith(ent, i)) { matched = ent; break; }
    }
    if (matched) {
      out.push(matched);
      i += matched.length;
    } else {
      out.push(w[i]);
      i += 1;
    }
  }
  return out;
}

function isMatraLikeUnit(u) {
  return u && u.length === 1 && MATRAS.has(u);
}

/* =====================
   FULL word compare (no insert/delete)
===================== */
function compareWordFull(qWord, cWord, allowConsonantSubs) {
  const qRaw = normStrict(qWord);
  const cRaw = normStrict(cWord);
  if (!qRaw || !cRaw) return { ok: false };

  const marksDiff = Math.abs(countMarks(qRaw) - countMarks(cRaw));

  const q = stripMarks(qRaw).replace(/\s+/g, "");
  const c = stripMarks(cRaw).replace(/\s+/g, "");
  if (!q || !c) return { ok: false };

  const qEnt = segmentEntities(q);
  const cEnt = segmentEntities(c);

  if (qEnt.length !== cEnt.length) return { ok: false };

  let consonantMismatches = 0;
  let matraMismatches = marksDiff;
  let phoneticCount = 0;
  let visualP0Count = 0;
  let visualP1Count = 0;
  let visualP2Count = 0;

  for (let i = 0; i < qEnt.length; i++) {
    const a = qEnt[i];
    const b = cEnt[i];
    if (a === b) continue;

    const aMat = isMatraLikeUnit(a);
    const bMat = isMatraLikeUnit(b);
    if (aMat || bMat) { matraMismatches += 1; continue; }

    if (!allowConsonantSubs) return { ok: false };

    const t = substType(a, b);
    if (t === TYPE.PHONETIC) phoneticCount += 1;
    else if (t === TYPE.VISUAL_P0) visualP0Count += 1;
    else if (t === TYPE.VISUAL_P1) visualP1Count += 1;
    else if (t === TYPE.VISUAL_P2) visualP2Count += 1;
    else return { ok: false };

    consonantMismatches += 1;
  }

  if (consonantMismatches > MAX_CONS_MISMATCH_PER_WORD) return { ok: false };

  const totalVisual = visualP0Count + visualP1Count + visualP2Count;
  let typeBucket = 4;
  if (consonantMismatches === 0) typeBucket = 0;
  else if (phoneticCount > 0 && totalVisual === 0) typeBucket = 0;
  else if (phoneticCount === 0 && totalVisual > 0) {
    if (visualP0Count > 0 && (visualP1Count + visualP2Count) === 0) typeBucket = 1;
    else if (visualP1Count > 0 && (visualP0Count + visualP2Count) === 0) typeBucket = 2;
    else if (visualP2Count > 0 && (visualP0Count + visualP1Count) === 0) typeBucket = 3;
    else typeBucket = 4;
  } else {
    typeBucket = 4;
  }

  return {
    ok: true,
    consonantMismatches,
    matraMismatches,
    typeBucket,
    detail: { phoneticCount, visualP0Count, visualP1Count, visualP2Count }
  };
}

/* =====================
   Older PF (K=2/3) compare
===================== */
function compareWordPrefixFallback(qWord, cWord, allowConsonantSubs) {
  if (!allowConsonantSubs) return { ok: false };

  const qRaw = normStrict(qWord);
  const cRaw = normStrict(cWord);
  if (!qRaw || !cRaw) return { ok: false };

  const qMarks = countMarks(qRaw);
  const cMarks = countMarks(cRaw);
  const marksDiff = Math.abs(qMarks - cMarks);

  const q = stripMarks(qRaw).replace(/\s+/g, "");
  const c = stripMarks(cRaw).replace(/\s+/g, "");
  if (!q || !c) return { ok: false };

  const qEnt = segmentEntities(q);
  const cEnt = segmentEntities(c);

  const qLen = qEnt.length;
  const cLen = cEnt.length;

  if (!(qLen === 2 || qLen === 3)) return { ok: false };

  const K = qLen;
  const maxSubs = (K === 2) ? PREFIX_K2_MAX_SUBS : PREFIX_K3_MAX_SUBS;

  if (cLen < qLen) return { ok: false };
  const extraSuffix = cLen - qLen;
  if (extraSuffix > PREFIX_FALLBACK_MAX_EXTRA_SUFFIX) return { ok: false };

  let subs = 0;
  let matraMismatches = marksDiff;
  let phoneticCount = 0;
  let visualP0Count = 0;
  let visualP1Count = 0;
  let visualP2Count = 0;

  for (let i = 0; i < K; i++) {
    const a = qEnt[i];
    const b = cEnt[i];
    if (a === b) continue;

    const aMat = isMatraLikeUnit(a);
    const bMat = isMatraLikeUnit(b);
    if (aMat || bMat) { matraMismatches += 1; continue; }

    const t = substType(a, b);
    if (t === TYPE.OTHER) return { ok: false };

    subs += 1;
    if (subs > maxSubs) return { ok: false };

    if (t === TYPE.PHONETIC) phoneticCount += 1;
    else if (t === TYPE.VISUAL_P0) visualP0Count += 1;
    else if (t === TYPE.VISUAL_P1) visualP1Count += 1;
    else if (t === TYPE.VISUAL_P2) visualP2Count += 1;
  }

  const totalVisual = visualP0Count + visualP1Count + visualP2Count;
  let typeBucket = 4;
  if (subs === 0) typeBucket = 0;
  else if (phoneticCount > 0 && totalVisual === 0) typeBucket = 0;
  else if (phoneticCount === 0 && totalVisual > 0) {
    if (visualP0Count > 0 && (visualP1Count + visualP2Count) === 0) typeBucket = 1;
    else if (visualP1Count > 0 && (visualP0Count + visualP2Count) === 0) typeBucket = 2;
    else if (visualP2Count > 0 && (visualP0Count + visualP1Count) === 0) typeBucket = 3;
    else typeBucket = 4;
  } else {
    typeBucket = 4;
  }

  return {
    ok: true,
    K,
    subs,
    typeBucket,
    matraMismatches,
    extraSuffix,
    detail: { phoneticCount, visualP0Count, visualP1Count, visualP2Count }
  };
}

/* =====================
   NEW: Additions + outside substitution fallback (LOWEST rank)
===================== */
function outsideSubsCapByLen(qLen) {
  if (qLen <= 0) return 0;
  if (qLen === 3) return 1;
  if (qLen >= 4 && qLen <= 8) return 2;
  if (qLen >= 9) return 3;
  // qLen 1-2: allow 0 outside subs (keeps integrity)
  return 0;
}

// Compares query entities against candidate prefix entities.
// Allows:
// - standard substitutions from your sets (not counted as outside)
// - "outside substitutions" capped by qLen bucket
// - suffix additions (candidate can be longer)
function compareWordAddOutside(qWord, cWord, allowConsonantSubs, addCap /* null = unlimited */) {
  if (!allowConsonantSubs) return { ok: false };

  const qRaw = normStrict(qWord);
  const cRaw = normStrict(cWord);
  if (!qRaw || !cRaw) return { ok: false };

  const marksDiff = Math.abs(countMarks(qRaw) - countMarks(cRaw));

  const q = stripMarks(qRaw).replace(/\s+/g, "");
  const c = stripMarks(cRaw).replace(/\s+/g, "");
  if (!q || !c) return { ok: false };

  const qEnt = segmentEntities(q);
  const cEnt = segmentEntities(c);

  const qLen = qEnt.length;
  const cLen = cEnt.length;

  // candidate must be at least query length for prefix match
  if (cLen < qLen) return { ok: false };

  const additions = cLen - qLen;
  if (addCap != null && additions > addCap) return { ok: false };

  const maxOutside = outsideSubsCapByLen(qLen);

  let outsideSubs = 0;
  let consonantMismatches = 0; // includes both set-subs + outside-subs
  let matraMismatches = marksDiff;

  let phoneticCount = 0;
  let visualP0Count = 0;
  let visualP1Count = 0;
  let visualP2Count = 0;
  let outsideCount = 0;

  for (let i = 0; i < qLen; i++) {
    const a = qEnt[i];
    const b = cEnt[i];
    if (a === b) continue;

    const aMat = isMatraLikeUnit(a);
    const bMat = isMatraLikeUnit(b);
    if (aMat || bMat) { matraMismatches += 1; continue; }

    const t = substType(a, b);
    if (t === TYPE.OTHER) {
      // outside substitution
      outsideSubs += 1;
      outsideCount += 1;
      if (outsideSubs > maxOutside) return { ok: false };
      consonantMismatches += 1;
      continue;
    }

    // set substitution
    consonantMismatches += 1;
    if (t === TYPE.PHONETIC) phoneticCount += 1;
    else if (t === TYPE.VISUAL_P0) visualP0Count += 1;
    else if (t === TYPE.VISUAL_P1) visualP1Count += 1;
    else if (t === TYPE.VISUAL_P2) visualP2Count += 1;
  }

  // type bucket for the prefix area (ignores outside, which is tracked separately)
  const totalVisual = visualP0Count + visualP1Count + visualP2Count;
  let typeBucket = 4;
  if (consonantMismatches === 0) typeBucket = 0;
  else if (phoneticCount > 0 && totalVisual === 0) typeBucket = 0;
  else if (phoneticCount === 0 && totalVisual > 0) {
    if (visualP0Count > 0 && (visualP1Count + visualP2Count) === 0) typeBucket = 1;
    else if (visualP1Count > 0 && (visualP0Count + visualP2Count) === 0) typeBucket = 2;
    else if (visualP2Count > 0 && (visualP0Count + visualP1Count) === 0) typeBucket = 3;
    else typeBucket = 4;
  } else {
    typeBucket = 4;
  }

  return {
    ok: true,
    qLen,
    additions,
    outsideSubs,
    consonantMismatches,
    matraMismatches,
    typeBucket,
    detail: { phoneticCount, visualP0Count, visualP1Count, visualP2Count, outsideCount }
  };
}

/* =====================
   Joined targets for 1-word scoring
===================== */
function buildOneWordTargets(tokens) {
  const out = [];
  for (let i = 0; i < tokens.length; i++) out.push({ text: tokens[i], kind: "TOKEN", pos: i, span: 1 });
  for (let i = 0; i < tokens.length - 1; i++) out.push({ text: tokens[i] + tokens[i+1], kind: "JOIN2", pos: i, span: 2 });
  if (tokens.length >= 2) out.push({ text: tokens.join(""), kind: "FULLJOIN", pos: 0, span: tokens.length });
  return out;
}
function kindRank(kind) {
  if (kind === "TOKEN") return 0;
  if (kind === "JOIN2") return 1;
  if (kind === "FULLJOIN") return 2;
  return 9;
}

/* =====================
   Exact scenarios
===================== */
function exactScenarioKey(qToks, cToks) {
  if (qToks.length === 1) {
    const q = qToks[0];
    const targets = buildOneWordTargets(cToks);

    let best = null;
    for (const t of targets) {
      if (t.text === q) {
        const desc = {
          ok: true,
          scenarioId: 0,
          kindRank: kindRank(t.kind),
          pos: t.pos,
          span: t.span,
          suffixCount: 0,
          totalWords: cToks.length
        };
        if (!best) best = desc;
        else {
          const a = [desc.kindRank, desc.pos, desc.span];
          const b = [best.kindRank, best.pos, best.span];
          if (a[0] < b[0] || (a[0] === b[0] && a[1] < b[1]) || (a[0] === b[0] && a[1] === b[1] && a[2] < b[2])) {
            best = desc;
          }
        }
      }
    }
    if (best) return best;

    if (cToks.length >= 1 && cToks[0] === q) {
      return {
        ok: true,
        scenarioId: 1,
        kindRank: 0,
        pos: 0,
        span: 1,
        suffixCount: cToks.length - 1,
        totalWords: cToks.length
      };
    }

    return { ok: false };
  }

  if (cToks.length >= qToks.length) {
    let ok = true;
    for (let i = 0; i < qToks.length; i++) {
      if (qToks[i] !== cToks[i]) { ok = false; break; }
    }
    if (ok) {
      return { ok: true, scenarioId: 10, kindRank: 0, pos: 0, span: qToks.length, suffixCount: cToks.length - qToks.length, totalWords: cToks.length };
    }
  }

  return { ok: false };
}

/* =====================
   Typing buckets (your ladder)
===================== */
function typingBucketOrder(nWords, perWordConMism, totalConMism) {
  if (nWords === 1) {
    if (totalConMism <= 4) return 0;
    return null;
  }
  if (nWords === 2) {
    if (totalConMism > MAX_TOTAL_CONS_2WORD) return null;

    const w1 = perWordConMism[0], w2 = perWordConMism[1];

    if (w1 === 0 && w2 > 0 && w2 <= 2) return 0;
    if (w2 === 0 && w1 > 0 && w1 <= 2) return 1;
    if (w1 === 0 && w2 > 0 && w2 <= 4) return 2;
    if (w2 > 0 && w2 <= 4 && w1 > 0 && w1 <= 2) return 3;

    return 9;
  }

  if (totalConMism > MAX_TOTAL_CONS_3PLUS) return null;

  const w = perWordConMism;
  const last = nWords - 1;
  const mid = 1;
  const first = 0;

  const allZeroExcept = (idx) => w.every((x, i) => (i === idx ? x > 0 : x === 0));

  if (allZeroExcept(last) && w[last] <= 2) return 0;
  if (nWords >= 3 && allZeroExcept(mid) && w[mid] <= 2) return 1;
  if (allZeroExcept(first) && w[first] <= 2) return 2;
  if (allZeroExcept(last) && w[last] <= 4) return 3;

  if (nWords === 3 && w[first] === 0 && w[mid] > 0 && w[mid] <= 2 && w[last] > 0 && w[last] <= 4) return 4;
  if (nWords === 3 && w[last] === 0 && w[first] > 0 && w[first] <= 2 && w[mid] > 0 && w[mid] <= 2) return 5;
  if (nWords === 3 && w[first] <= 2 && w[mid] <= 2 && w[last] <= 4 && (w[first] + w[mid] + w[last] > 0)) return 6;

  return 9;
}

/* =====================
   Key comparison / utilities
===================== */
function cmpKey(a, b) {
  const n = Math.max(a.length, b.length);
  for (let i = 0; i < n; i++) {
    const av = a[i] ?? 0;
    const bv = b[i] ?? 0;
    if (av !== bv) return av < bv ? -1 : 1;
  }
  return 0;
}

function parseSerial(serial) {
  if (serial == null) return 0;
  const s = String(serial).trim();
  const m = s.match(/(\d+)/);
  if (!m) return 0;
  const n = Number(m[1]);
  return Number.isFinite(n) ? n : 0;
}

/* =====================
   Field evaluation (core)
===================== */
function evaluateAgainstField(qToks, candToks, serialNo, fieldName, exactOn) {
  // EXACT MODE always outranks typing
  const ex = exactScenarioKey(qToks, candToks);
  if (ex.ok) {
    const key = [0, ex.scenarioId, ex.kindRank, ex.pos, ex.suffixCount, ex.totalWords, serialNo];
    const explain = `mode=EXACT field=${fieldName} scenario=${ex.scenarioId} kindRank=${ex.kindRank} pos=${ex.pos} suffix=${ex.suffixCount} words=${ex.totalWords} serial=${serialNo}`;
    return { ok: true, key, explain, match_field: fieldName };
  }

  if (exactOn) return { ok: false }; // exact mode disallows consonant subs entirely

  // TYPING MODE
  if (candToks.length < qToks.length) return { ok: false };

  // 1-word query: match ANY token position + joins (FULL word compare)
  if (qToks.length === 1) {
    const q = qToks[0];
    const targets = buildOneWordTargets(candToks);

    let best = null;
    for (const t of targets) {
      const r = compareWordFull(q, t.text, true);
      if (!r.ok) continue;

      const totalCon = r.consonantMismatches;
      const totalMatra = r.matraMismatches;
      const bucket = typingBucketOrder(1, [totalCon], totalCon);
      if (bucket == null) continue;

      const key = [1, 0, bucket, totalCon, r.typeBucket, totalMatra, kindRank(t.kind), t.pos, serialNo];
      const explain = `mode=TYPO_FULL field=${fieldName} bucket=${bucket} con=${totalCon} typeB=${r.typeBucket} matra=${totalMatra} kind=${t.kind}@${t.pos} serial=${serialNo}`;

      const cand = { ok: true, key, explain, match_field: fieldName };
      if (!best || cmpKey(cand.key, best.key) < 0) best = cand;
    }

    // If FULL fails for 1-word, try the new ADD/OUTSIDE fallback against token/join variants
    if (!best) {
      let bestAO = null;
      for (const t of targets) {
        const rao = compareWordAddOutside(q, t.text, true, null /* unlimited adds */);
        if (!rao.ok) continue;

        // family=2 (lowest), then outsideSubs, then additions, then typeBucket, then matra, then kindRank/pos
        const key = [1, 2, rao.outsideSubs, rao.additions, rao.typeBucket, rao.matraMismatches, kindRank(t.kind), t.pos, serialNo];
        const explain = `mode=TYPO_AO field=${fieldName} outside=${rao.outsideSubs} add=${rao.additions} typeB=${rao.typeBucket} matra=${rao.matraMismatches} kind=${t.kind}@${t.pos} serial=${serialNo}`;

        const cand = { ok: true, key, explain, match_field: fieldName };
        if (!bestAO || cmpKey(cand.key, bestAO.key) < 0) bestAO = cand;
      }
      if (bestAO) return bestAO;
    }

    return best || { ok: false };
  }

  // 2+ word query: align to first qToks.length candidate tokens (prefix match is handled later)
  const aligned = candToks.slice(0, qToks.length);

  // 1) FULL word-to-word (your normal rules)
  const perWordFull = [];
  let totalConFull = 0;
  let totalMatraFull = 0;

  let fullOk = true;
  for (let i = 0; i < qToks.length; i++) {
    const r = compareWordFull(qToks[i], aligned[i], true);
    if (!r.ok) { fullOk = false; break; }
    perWordFull.push(r);
    totalConFull += r.consonantMismatches;
    totalMatraFull += r.matraMismatches;
  }

  if (fullOk) {
    const bucket = typingBucketOrder(qToks.length, perWordFull.map(x => x.consonantMismatches), totalConFull);
    if (bucket == null) return { ok: false };

    let severitySum = 0;
    for (const w of perWordFull) {
      severitySum += (w.consonantMismatches * 1_000_000) + (w.typeBucket * 10_000) + w.matraMismatches;
    }

    const suffixCount = candToks.length - qToks.length;
    const totalWords = candToks.length;

    const key = [1, 0, bucket, severitySum, suffixCount, totalWords, serialNo];
    const explain = `mode=TYPO_FULL field=${fieldName} bucket=${bucket} con=${totalConFull} matra=${totalMatraFull} sum=${severitySum} suffix=${suffixCount} words=${totalWords} serial=${serialNo}`;

    return { ok: true, key, explain, match_field: fieldName };
  }

  // 2) PF fallback (K=2/3 rule)
  const perWordPF = [];
  let globalExtra = 0;

  let pfOk = true;
  for (let i = 0; i < qToks.length; i++) {
    const rpf = compareWordPrefixFallback(qToks[i], aligned[i], true);
    if (!rpf.ok) { pfOk = false; break; }
    perWordPF.push(rpf);
    globalExtra += rpf.extraSuffix;
  }

  if (pfOk) {
    if (globalExtra > PREFIX_FALLBACK_GLOBAL_EXTRA_PER_WORD * qToks.length) return { ok: false };

    let subsSum = 0;
    let typeSum = 0;
    let matraSum = 0;
    let extraSum = 0;

    for (let i = 0; i < perWordPF.length; i++) {
      const w = perWordPF[i];
      const posW = 1.0 + (Math.max(0, (3 - i)) * 0.05);
      subsSum += w.subs * 1000 * posW;
      typeSum += w.typeBucket * 10 * posW;
      matraSum += w.matraMismatches * 1 * posW;
      extraSum += w.extraSuffix * 100 * posW;
    }

    const suffixCount = candToks.length - qToks.length;
    const totalWords = candToks.length;

    const key = [1, 1, subsSum, typeSum, matraSum, extraSum, suffixCount, totalWords, serialNo];
    const explain = `mode=TYPO_PF field=${fieldName} subsSum=${subsSum.toFixed(2)} typeSum=${typeSum.toFixed(2)} matraSum=${matraSum.toFixed(2)} extraSum=${extraSum.toFixed(2)} globalExtra=${globalExtra} serial=${serialNo}`;

    return { ok: true, key, explain, match_field: fieldName };
  }

  // 3) NEW lowest-rank fallback: additions + outside substitutions
  //    Apply word-by-word prefix compare.
  const perWordAO = [];
  let outsideTotal = 0;
  let addTotal = 0;
  let typeSum = 0;
  let matraSum = 0;

  for (let i = 0; i < qToks.length; i++) {
    const isFirst = (i === 0);
    const addCap = isFirst ? ADD_FALLBACK_FIRST_WORD_MAX_ADD_ENTITIES_IN_MULTI : null;

    const rao = compareWordAddOutside(qToks[i], aligned[i], true, addCap);
    if (!rao.ok) return { ok: false };

    perWordAO.push(rao);

    // penalize earlier words more (first word integrity)
    const posW = 1.0 + (Math.max(0, (3 - i)) * 0.10);
    outsideTotal += rao.outsideSubs * posW;
    addTotal += rao.additions * (isFirst ? 2.0 : 1.0); // first-word additions heavier
    typeSum += rao.typeBucket * posW;
    matraSum += rao.matraMismatches * posW;
  }

  const suffixCount = candToks.length - qToks.length;
  const totalWords = candToks.length;

  // family=2 => below FULL and PF
  // Order inside family: outsideSubs (worse), then additions (worse), then type, then matra, then suffixCount, then serial
  const key = [1, 2, outsideTotal, addTotal, typeSum, matraSum, suffixCount, totalWords, serialNo];

  const explain =
    `mode=TYPO_AO field=${fieldName} outsideTotal=${outsideTotal.toFixed(2)} addTotal=${addTotal.toFixed(2)} typeSum=${typeSum.toFixed(2)} matraSum=${matraSum.toFixed(2)} suffix=${suffixCount} words=${totalWords} serial=${serialNo}`;

  return { ok: true, key, explain, match_field: fieldName };
}

/* =====================
   Row evaluation + tie rules
===================== */
let scope = "voter";
let exactOn = false;
let total = 0;
let received = 0;
let buffer = [];
let qTokens = [];

function evaluateRow(row) {
  const serialNo = parseSerial(row.serial_no);

  const voterTokens = tokenizeStrict(row.voter_name_raw || "");
  const relTokens = tokenizeStrict(row.relative_name_raw || "");

  let best = null;

  const consider = (fieldName, candToks) => {
    if (!candToks.length) return;
    const res = evaluateAgainstField(qTokens, candToks, serialNo, fieldName, exactOn);
    if (!res.ok) return;

    if (!best) { best = res; return; }

    const c = cmpKey(res.key, best.key);
    if (c < 0) best = res;
    else if (c === 0) {
      if (best.match_field !== "voter" && fieldName === "voter") best = res;
    }
  };

  if (scope === "voter") consider("voter", voterTokens);
  else if (scope === "relative") consider("relative", relTokens);
  else { consider("voter", voterTokens); consider("relative", relTokens); }

  return best;
}

function postProgress(phase) {
  self.postMessage({
    type: "progress",
    phase,
    candidates: total,
    done: received,
    total
  });
}

/* =====================
   Worker messaging
===================== */
self.onmessage = (ev) => {
  const msg = ev.data;

  try {
    if (msg.type === "start") {
      scope = msg.scope || "voter";
      exactOn = !!msg.exactOn;

      const q = normStrict(msg.query || "");
      qTokens = tokenizeStrict(q);

      total = Number(msg.total || 0);
      received = 0;
      buffer = [];

      postProgress("ranking");
      return;
    }

    if (msg.type === "batch") {
      const rows = msg.rows || [];

      for (const row of rows) {
        const best = evaluateRow(row);
        if (best) {
          buffer.push({
            row_id: Number(row.row_id),
            key: best.key,
            explain: best.explain,
            match_field: best.match_field
          });
        }
      }

      received += rows.length;
      if (received % 4000 === 0 || received === total) postProgress("ranking");
      return;
    }

    if (msg.type === "finish") {
      postProgress("sorting");

      buffer.sort((a, b) => {
        const c = cmpKey(a.key, b.key);
        if (c !== 0) return c;

        // deterministic fallback:
        if (a.match_field !== b.match_field) {
          if (a.match_field === "voter") return -1;
          if (b.match_field === "voter") return 1;
        }
        return a.row_id - b.row_id;
      });

      const ranked = buffer.map((x, idx) => ({
        row_id: x.row_id,
        rank: idx,
        explain: x.explain
      }));

      self.postMessage({ type: "done", ranked });
      return;
    }
  } catch (e) {
    self.postMessage({ type: "error", message: e?.message || String(e) });
  }
};
