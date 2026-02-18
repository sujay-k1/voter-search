// worker.js (ES module)
//
// Message protocol (required by app.js):
//   - Receive: {type:'start', query, exactOn, scope, total}
//   - Receive: {type:'batch', rows:[...]}
//   - Receive: {type:'finish'}
//   - Send:    {type:'progress', done, total, phase:'scoring'} (optional)
//   - Send:    {type:'done', ranked:[{row_id, key:number[], explain, match_field}]}
//
// EXACT MODE (exactOn === true):
//   - match if query words are an ordered-subsequence of candidate words
//   - each matched word must have identical STRUCTURAL entity sequence
//   - only matra/marks can differ (matraDiff used for ranking)
//
// TYPING-MISTAKES MODE (exactOn === false):
//   - includes ALL exact-mode results with same ordering
//   - additionally allows:
//       * internal substitutions (within confusable sets; weighted by tier)
//       * outside substitutions (non-set) with caps + protected-prefix constraint
//       * empty substitutions (deletions) with separate quota (<= entities-2)
//       * suffix insertions only (extra entities at end of a word), ranked at the bottom
//   - For MULTIWORD QUERIES: protected-prefix rule applies ONLY to the FIRST word,
//     as per your latest instruction: “Apply to all multiword queries”.

/* =====================
   Confusable sets (as provided)
   phonetic > all visual (P0/P1/P2)
===================== */
const VISUAL_P0 = [
  ["ए", "प"],
  ["क", "फ"],
  ["ख", "रव", "थ", "य", "रा", "स", "श"],
  ["ग", "रा", "म"],
  ["घ", "ध", "छ"],
  ["ङ", "ड", "ह"],
  ["च", "ज", "ज्ञ", "ञ"],
  ["झ", "डा"],
  ["ट", "ढ", "द", "ठ"],
  ["त", "न"],
  ["प", "ष", "य", "भ", "म", "न", "प्न"],
  ["ब", "व", "ञ"],
  ["र", "१"],
  ["श", "रा", "१।"],
  ["त्र", "ञ"],
  ["त्त", "त"],
  ["स्न", "स"],
];

const VISUAL_P1 = [
  ["ण", "ग"],
  ["ह", "हा", "घ", "छ"],
  ["ड", "ह", "इ", "झ"],
  ["प", "ए"],
  ["स", "रा", "श"],
  ["र", "ल"],
];

const VISUAL_P2 = [
  ["प", "फ", "च"],
];

const PHONETIC = [
  ["अ", "आ"],
  ["इ", "ई"],
  ["उ", "ऊ"],
  ["ए", "ऐ"],
  ["ओ", "औ"],
  ["ऋ", "ॠ"],
  ["ऌ", "ॡ"],

  ["क", "ख"],
  ["ग", "घ", "ह"],
  ["च", "छ"],
  ["ज", "झ"],
  ["ट", "ठ"],
  ["ड", "ढ", "द", "ध", "त", "थ"],
  ["ण", "न"],
  ["प", "फ"],
  ["ब", "भ", "व"],
  ["य", "ज"],
  ["स", "श", "ष"],
  ["त्र", "ट्र"],
  ["ज्ञ", "ज्या"],
  ["र", "ड़"],
  ["ग्गा", "गा"],
  ["त्त", "त"],
  ["कई", "कै", "कय"],
  ["खई", "खै", "खय"],
  ["गई", "गै", "गय"],
  ["घई", "घै", "घय"],
  ["चई", "चै", "चय"],
  ["छई", "छै", "छय"],
  ["जई", "जै", "जय"],
  ["झई", "झै", "झय"],
  ["टई", "टै", "टय"],
  ["डई", "डै", "डय"],
  ["दई", "डै", "दय"],
  ["धई", "घै", "धय"],
  ["नई", "नै", "नय"],
  ["पई", "पै", "पय"],
  ["फई", "फै", "फय"],
  ["बई", "बै", "बय"],
  ["भई", "भै", "भय"],
  ["मई", "मै", "मय"],
  ["रई", "रै", "रय"],
  ["लई", "लै", "लय"],
  ["वई", "वै", "वय"],
  ["सई", "सै", "सय"],
  ["शई", "शै", "शय"],
  ["षई", "षै", "षय"],
];

/* =====================
   Marks (matra-diff)
   - ं /ँ /ः /़ count as matra difference
   - halant counts as matra difference UNLESS it's inside an entity token
===================== */
const MATRA_CHARS = new Set([
  "\u0901", // chandrabindu ँ
  "\u0902", // anusvara ं
  "\u0903", // visarga ः
  "\u093C", // nukta ़
  "\u094D", // halant ्

  // vowel signs
  "\u093E", "\u093F", "\u0940", "\u0941", "\u0942",
  "\u0943", "\u0944", "\u0947", "\u0948", "\u094B", "\u094C",
  "\u0962", "\u0963",
]);

function isMatraChar(ch) {
  return MATRA_CHARS.has(ch);
}

function removeMatras(s) {
  let out = "";
  for (let i = 0; i < s.length; i++) {
    const ch = s[i];
    if (!isMatraChar(ch)) out += ch;
  }
  return out;
}

function normName(s) {
  if (!s) return "";
  // keep Devanagari as-is; just normalize whitespace + remove common punctuation noise
  return String(s)
    .replace(/[\u00A0\u200B]/g, " ")
    .replace(/[.,;:!?'"(){}\[\]<>|\\/`~^*+=—–_-]/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

function splitWords(s) {
  const t = normName(s);
  if (!t) return [];
  return t.split(" ").filter(Boolean);
}

/* =====================
   Confusable map with tiers
   tier: 1 phonetic, 2 visual P0, 3 visual P1, 4 visual P2
===================== */
function buildTierMap() {
  const map = new Map(); // key: a -> Map(b -> tier)
  function addGroup(group, tier) {
    for (let i = 0; i < group.length; i++) {
      for (let j = 0; j < group.length; j++) {
        if (i === j) continue;
        const a = group[i];
        const b = group[j];
        if (!map.has(a)) map.set(a, new Map());
        const inner = map.get(a);
        const prev = inner.get(b);
        if (prev == null || tier < prev) inner.set(b, tier);
      }
    }
  }
  for (const g of PHONETIC) addGroup(g, 1);
  for (const g of VISUAL_P0) addGroup(g, 2);
  for (const g of VISUAL_P1) addGroup(g, 3);
  for (const g of VISUAL_P2) addGroup(g, 4);
  return map;
}

const CONFUSABLE = buildTierMap();

function confusableTier(a, b) {
  if (a === b) return 0;
  const m = CONFUSABLE.get(a);
  if (!m) return null;
  const t = m.get(b);
  return t == null ? null : t;
}

/* =====================
   Multi-entity dictionary (all tokens with length>1 from sets)
===================== */
function buildMultiEntityList() {
  const set = new Set();
  const pushAll = (groups) => {
    for (const g of groups) for (const tok of g) if (tok.length > 1) set.add(tok);
  };
  pushAll(VISUAL_P0);
  pushAll(VISUAL_P1);
  pushAll(VISUAL_P2);
  pushAll(PHONETIC);

  // Sort longest-first for greedy tokenization
  return Array.from(set).sort((a, b) => b.length - a.length);
}
const MULTI_ENTITIES = buildMultiEntityList();

/* =====================
   Tokenize a word into entity tokens
   - Greedy match multi-entities (including ones that contain matras/halant)
   - Then attach trailing matra marks to the chosen entity (so त्रा becomes one token)
===================== */
const WORD_CACHE = new Map(); // word -> parsed

function parseWord(word) {
  const w = normName(word);
  const cached = WORD_CACHE.get(w);
  if (cached) return cached;

  const tokens = [];
  let i = 0;
  while (i < w.length) {
    // skip leading spaces (shouldn't exist after normName, but safe)
    if (w[i] === " ") { i++; continue; }

    let matched = null;
    for (const ent of MULTI_ENTITIES) {
      if (w.startsWith(ent, i)) { matched = ent; break; }
    }

    let surface;
    let isMulti = false;
    if (matched) {
      surface = matched;
      isMulti = true;
      i += matched.length;
    } else {
      surface = w[i];
      isMulti = false;
      i += 1;
    }

    // Attach trailing marks (matras/diacritics/halant) to this token
    while (i < w.length && isMatraChar(w[i])) {
      surface += w[i];
      i++;
    }

    // If we accidentally got a pure-mark token, discard it
    const struct = removeMatras(surface);
    if (!struct) continue;

    // Extract marks for matraDiff
    let marks = [];
    for (let k = 0; k < surface.length; k++) {
      const ch = surface[k];
      if (isMatraChar(ch)) marks.push(ch);
    }

    // Decision: ignore halant in matraDiff if it’s part of an entity token
    // (i.e., if token was created by multi-entity match or contains halant inside)
    if (isMulti) {
      marks = marks.filter(ch => ch !== "\u094D");
    }

    // Sort marks for stable distance calc
    const marksKey = marks.slice().sort().join("");

    tokens.push({
      surface,
      struct,
      isMulti,
      marksKey,
    });
  }

  const parsed = {
    raw: w,
    tokens,
    structs: tokens.map(t => t.struct),
    len: tokens.length,
  };
  WORD_CACHE.set(w, parsed);
  return parsed;
}

/* =====================
   Small Levenshtein (marks strings are tiny)
===================== */
function lev(a, b) {
  if (a === b) return 0;
  const n = a.length, m = b.length;
  if (n === 0) return m;
  if (m === 0) return n;

  const dp = new Array(m + 1);
  for (let j = 0; j <= m; j++) dp[j] = j;

  for (let i = 1; i <= n; i++) {
    let prev = dp[0];
    dp[0] = i;
    for (let j = 1; j <= m; j++) {
      const tmp = dp[j];
      const cost = a[i - 1] === b[j - 1] ? 0 : 1;
      dp[j] = Math.min(
        dp[j] + 1,       // delete
        dp[j - 1] + 1,   // insert
        prev + cost      // replace
      );
      prev = tmp;
    }
  }
  return dp[m];
}

function matraDiffToken(qTok, cTok) {
  return lev(qTok.marksKey, cTok.marksKey);
}

/* =====================
   Word exact match (struct sequence must be identical)
===================== */
function wordExactMatch(qW, cW) {
  if (qW.len !== cW.len) return null;
  for (let i = 0; i < qW.len; i++) {
    if (qW.structs[i] !== cW.structs[i]) return null;
  }
  let md = 0;
  for (let i = 0; i < qW.len; i++) md += matraDiffToken(qW.tokens[i], cW.tokens[i]);
  return { matraDiff: md };
}

/* =====================
   Outside-substitution caps (per word entity length)
   - len<=2 => 0
   - 3..5  => 1
   - 6..9  => 2
   - 10+   => 3
===================== */
function outsideNonEmptyCapForEntities(entLen) {
  if (entLen <= 2) return 0;
  if (entLen <= 5) return 1;
  if (entLen <= 9) return 2;
  return 3;
}

// Empty substitution quota:
// “up to 2 less than total number of entities” => max deletions = entLen - 2
function emptyCapForEntities(entLen) {
  if (entLen <= 2) return 0;
  return Math.max(0, entLen - 2);
}

/* =====================
   Word fuzzy match via DP
   Operations allowed:
   - match (struct equal) -> matraDiff only
   - internal substitution (confusable sets) -> internalCount + tierScore
   - outside substitution (non-empty)        -> outsideNonEmpty++
   - empty substitution (deletion)           -> outsideEmpty++
   - suffix insertions only (extra cand tokens at end) -> insertionCount
   Constraints:
   - outside subs forbidden in protected prefix
   - outsideNonEmpty cap (as above)
   - outsideEmpty cap (entLen-2)
===================== */
function betterWordState(a, b) {
  if (b == null) return true;
  if (a == null) return false;

  // Primary ordering inside a word:
  // externalTotal, externalNonEmpty, internalCount, tierScore, matraDiff, outsidePosPenalty, insertionCount
  if (a.externalTotal !== b.externalTotal) return a.externalTotal < b.externalTotal;
  if (a.outsideNonEmpty !== b.outsideNonEmpty) return a.outsideNonEmpty < b.outsideNonEmpty;
  if (a.internalCount !== b.internalCount) return a.internalCount < b.internalCount;
  if (a.tierScore !== b.tierScore) return a.tierScore < b.tierScore;
  if (a.matraDiff !== b.matraDiff) return a.matraDiff < b.matraDiff;
  if (a.outsidePosPenalty !== b.outsidePosPenalty) return a.outsidePosPenalty < b.outsidePosPenalty;
  if (a.insertionCount !== b.insertionCount) return a.insertionCount < b.insertionCount;

  // tie
  return false;
}

function wordFuzzyMatch(qW, cW, opts) {
  const qLen = qW.len;
  const cLen = cW.len;

  const outsideCap = opts.allowOutside ? outsideNonEmptyCapForEntities(qLen) : 0;
  const emptyCap = opts.allowOutside ? emptyCapForEntities(qLen) : 0;
  const protectedPrefixLen = opts.protectedPrefixLen || 0; // applies only to word1 of multiword queries
  const maxInternal = 4; // as per spec

  // dp[i][j] = best state converting q[0..i) to c[0..j)
  const dp = Array.from({ length: qLen + 1 }, () => Array(cLen + 1).fill(null));

  dp[0][0] = {
    matraDiff: 0,
    internalCount: 0,
    tierScore: 0,
    outsideNonEmpty: 0,
    outsideEmpty: 0,
    outsidePosPenalty: 0,
    insertionCount: 0,
    externalTotal: 0,
    // for explain
    ops: [],
  };

  for (let i = 0; i <= qLen; i++) {
    for (let j = 0; j <= cLen; j++) {
      const cur = dp[i][j];
      if (!cur) continue;

      // If query fully consumed, we can only accept suffix insertions (remaining cand tokens)
      if (i === qLen) {
        const ins = cLen - j;
        const st = {
          ...cur,
          insertionCount: ins,
        };
        st.externalTotal = st.outsideNonEmpty + st.outsideEmpty;
        // choose best among dp[qLen][cLen] via a special accumulator later
        // We'll place it at dp[qLen][cLen] by “fast-forwarding”.
        if (ins >= 0) {
          const finalOps = cur.ops.slice();
          if (ins > 0) finalOps.push(`+ins:${ins}`);
          const fin = { ...st, ops: finalOps };
          const prev = dp[qLen][cLen];
          if (betterWordState(fin, prev)) dp[qLen][cLen] = fin;
        }
        continue;
      }

      // 1) Match/Substitute consuming one from each
      if (j < cLen) {
        const qTok = qW.tokens[i];
        const cTok = cW.tokens[j];

        // Match (struct equal)
        if (qTok.struct === cTok.struct) {
          const md = matraDiffToken(qTok, cTok);
          const next = {
            ...cur,
            matraDiff: cur.matraDiff + md,
            ops: cur.ops.concat(md ? [`=${i}:${md}`] : [`=${i}`]),
          };
          next.externalTotal = next.outsideNonEmpty + next.outsideEmpty;
          const prev = dp[i + 1][j + 1];
          if (betterWordState(next, prev)) dp[i + 1][j + 1] = next;
        } else {
          // Internal substitution if confusable
          const tier = confusableTier(qTok.surface, cTok.surface);
          if (tier != null) {
            if (cur.internalCount + 1 <= maxInternal) {
              const next = {
                ...cur,
                internalCount: cur.internalCount + 1,
                tierScore: cur.tierScore + tier,
                ops: cur.ops.concat([`~${i}:${tier}`]),
              };
              next.externalTotal = next.outsideNonEmpty + next.outsideEmpty;
              const prev = dp[i + 1][j + 1];
              if (betterWordState(next, prev)) dp[i + 1][j + 1] = next;
            }
          } else if (opts.allowOutside) {
            // Outside substitution (non-empty)
            // Constraints:
            // - qLen <=2 => outsideCap=0 already
            // - forbidden in protected prefix
            if (i >= protectedPrefixLen && cur.outsideNonEmpty < outsideCap) {
              const posPenalty = cur.outsidePosPenalty + ((qLen - 1 - i) * 10 + 1); // earlier is worse
              const next = {
                ...cur,
                outsideNonEmpty: cur.outsideNonEmpty + 1,
                outsidePosPenalty: posPenalty,
                ops: cur.ops.concat([`!${i}`]),
              };
              next.externalTotal = next.outsideNonEmpty + next.outsideEmpty;
              const prev = dp[i + 1][j + 1];
              if (betterWordState(next, prev)) dp[i + 1][j + 1] = next;
            }
          }
        }
      }

      // 2) Empty substitution (deletion): consume one query token, keep candidate
      if (opts.allowOutside) {
        if (i >= protectedPrefixLen && cur.outsideEmpty < emptyCap) {
          const posPenalty = cur.outsidePosPenalty + ((qLen - 1 - i) * 10 + 2);
          const next = {
            ...cur,
            outsideEmpty: cur.outsideEmpty + 1,
            outsidePosPenalty: posPenalty,
            ops: cur.ops.concat([`Ø${i}`]),
          };
          next.externalTotal = next.outsideNonEmpty + next.outsideEmpty;
          const prev = dp[i + 1][j];
          if (betterWordState(next, prev)) dp[i + 1][j] = next;
        }
      }
    }
  }

  const fin = dp[qLen][cLen];
  if (!fin) return null;

  // Global “insertions ranked at bottom”:
  // keep insertionCount but it will push match to modeGroup=3 later.
  return {
    matraDiff: fin.matraDiff,
    internalCount: fin.internalCount,
    tierScore: fin.tierScore,
    outsideNonEmpty: fin.outsideNonEmpty,
    outsideEmpty: fin.outsideEmpty,
    outsidePosPenalty: fin.outsidePosPenalty,
    insertionCount: fin.insertionCount,
    ops: fin.ops,
  };
}

/* =====================
   Affix (word insertion) ranking helper
   - prefers: none < suffix-only < prefix-only < middle-only < combos
===================== */
function affixKey(prefixWords, middleWords, suffixWords) {
  const p = prefixWords, m = middleWords, s = suffixWords;

  if (p === 0 && m === 0 && s === 0) return [0, 0, 0, 0];
  if (p === 0 && m === 0 && s > 0) return [1, Math.min(s, 3), s, 0]; // suffix-only
  if (p > 0 && m === 0 && s === 0) return [2, Math.min(p, 3), p, 0]; // prefix-only
  if (p === 0 && m > 0 && s === 0) return [3, Math.min(m, 4), m, 0]; // middle-only

  // combos (lowest priority within exact/internal/outside groups)
  return [4, p + m + s, p, m * 100 + s];
}

/* =====================
   Name-level alignment (ordered subsequence of words)
   Tries all alignments (names are short) and picks best by rankKey.
===================== */
function compareRankKey(a, b) {
  const A = a, B = b;
  const n = Math.max(A.length, B.length);
  for (let i = 0; i < n; i++) {
    const av = A[i] ?? 0;
    const bv = B[i] ?? 0;
    if (av !== bv) return av - bv;
  }
  return 0;
}

function penaltyFromMatraDiff(d) {
  // Threshold behavior used for multiword exact ordering:
  // small diffs (<=2) are “light”; >=3 are “heavy” and should drop below affix-exact matches.
  if (d <= 2) return d;
  return 100 + d;
}

function buildExplain(modeLabel, qWords, cWords, alignIdx, prefixWords, middleWords, suffixWords, perWord) {
  const pairs = alignIdx.map((ci, qi) => `${qWords[qi]}↔${cWords[ci]}`).join(" | ");
  const md = perWord.map(x => x.matraDiff ?? 0).join(",");
  const inS = perWord.map(x => x.internalCount ?? 0).join(",");
  const outS = perWord.map(x => (x.outsideNonEmpty ?? 0) + (x.outsideEmpty ?? 0)).join(",");
  const insS = perWord.map(x => x.insertionCount ?? 0).join(",");
  return `${modeLabel}; ${pairs}; affix(p=${prefixWords},m=${middleWords},s=${suffixWords}); matra=[${md}] internal=[${inS}] outside=[${outS}] ins=[${insS}]`;
}

function buildRankKeyOneWordExact(matraDiff, prefixWords, middleWords, suffixWords) {
  // Exact 1-word ranking closely follows the user list:
  // primary: matraDiff (0,1,2,3,4+), secondary: affix pattern (none, suffix, prefix, middle, combos)
  const mdGroup = matraDiff <= 3 ? matraDiff : 4;
  const ak = affixKey(prefixWords, middleWords, suffixWords);
  return [0, mdGroup, matraDiff, ...ak, prefixWords, middleWords, suffixWords];
}

function buildRankKeyOneWordInternal(internalCount, tierScore, matraDiff, prefixWords, middleWords, suffixWords) {
  const mdGroup = matraDiff <= 3 ? matraDiff : 4;
  const ak = affixKey(prefixWords, middleWords, suffixWords);
  return [1, internalCount, tierScore, mdGroup, matraDiff, ...ak, prefixWords, middleWords, suffixWords];
}

function buildRankKeyMultiwordExact(matraDiffs, prefixWords, middleWords, suffixWords) {
  const ak = affixKey(prefixWords, middleWords, suffixWords);

  // Word weights: word1 matters more than later words
  let matraPenalty = 0;
  for (let i = 0; i < matraDiffs.length; i++) {
    const w = (i === 0) ? 20 : 1;
    matraPenalty += w * penaltyFromMatraDiff(matraDiffs[i] || 0);
  }

  // Small affix penalty to break ties; big matraPenalty (>=100) can drop below affix-exact matches
  const affixPenalty = (prefixWords + middleWords + suffixWords) * 5 + ak[0] * 2;
  const overall = matraPenalty + affixPenalty;

  return [0, overall, ...ak, ...matraDiffs, prefixWords, middleWords, suffixWords];
}

function buildRankKeyMultiwordInternal(internalCounts, tierScores, matraDiffs, prefixWords, middleWords, suffixWords) {
  const ak = affixKey(prefixWords, middleWords, suffixWords);

  const subsVec = internalCounts.slice().reverse(); // last word first (favoured)
  const tierVec = tierScores.slice().reverse();

  let matraPenalty = 0;
  for (let i = 0; i < matraDiffs.length; i++) {
    const w = (i === 0) ? 20 : 1;
    matraPenalty += w * penaltyFromMatraDiff(matraDiffs[i] || 0);
  }
  const affixPenalty = (prefixWords + middleWords + suffixWords) * 5 + ak[0] * 2;
  const overall = matraPenalty + affixPenalty;

  const internalTotal = internalCounts.reduce((a, b) => a + b, 0);

  return [1, ...subsVec, internalTotal, ...tierVec, overall, ...ak, ...matraDiffs, prefixWords, middleWords, suffixWords];
}

function buildRankKeyMultiwordOutside(outsideCounts, internalCounts, outsidePosPenalties, tierScores, matraDiffs, prefixWords, middleWords, suffixWords) {
  const ak = affixKey(prefixWords, middleWords, suffixWords);

  const extVec = outsideCounts.slice().reverse(); // last word first
  const subsVec = internalCounts.slice().reverse();
  const posVec = outsidePosPenalties.slice().reverse();
  const tierVec = tierScores.slice().reverse();

  let matraPenalty = 0;
  for (let i = 0; i < matraDiffs.length; i++) {
    const w = (i === 0) ? 20 : 1;
    matraPenalty += w * penaltyFromMatraDiff(matraDiffs[i] || 0);
  }
  const affixPenalty = (prefixWords + middleWords + suffixWords) * 5 + ak[0] * 2;
  const overall = matraPenalty + affixPenalty;

  const extTotal = outsideCounts.reduce((a, b) => a + b, 0);
  const internalTotal = internalCounts.reduce((a, b) => a + b, 0);

  return [2, ...extVec, extTotal, ...subsVec, internalTotal, ...posVec, ...tierVec, overall, ...ak, ...matraDiffs, prefixWords, middleWords, suffixWords];
}

function buildRankKeyMultiwordInsertion(insertionCounts, outsideCounts, internalCounts, outsidePosPenalties, tierScores, matraDiffs, prefixWords, middleWords, suffixWords) {
  const ak = affixKey(prefixWords, middleWords, suffixWords);

  const insVec = insertionCounts.slice().reverse();
  const extVec = outsideCounts.slice().reverse();
  const subsVec = internalCounts.slice().reverse();
  const posVec = outsidePosPenalties.slice().reverse();
  const tierVec = tierScores.slice().reverse();

  let matraPenalty = 0;
  for (let i = 0; i < matraDiffs.length; i++) {
    const w = (i === 0) ? 20 : 1;
    matraPenalty += w * penaltyFromMatraDiff(matraDiffs[i] || 0);
  }
  const affixPenalty = (prefixWords + middleWords + suffixWords) * 5 + ak[0] * 2;
  const overall = matraPenalty + affixPenalty;

  const insTotal = insertionCounts.reduce((a, b) => a + b, 0);
  const extTotal = outsideCounts.reduce((a, b) => a + b, 0);
  const internalTotal = internalCounts.reduce((a, b) => a + b, 0);

  return [3, ...insVec, insTotal, ...extVec, extTotal, ...subsVec, internalTotal, ...posVec, ...tierVec, overall, ...ak, ...matraDiffs, prefixWords, middleWords, suffixWords];
}

function matchNameExact(qWordsParsed, candWordsParsed) {
  const Q = qWordsParsed.length;
  const C = candWordsParsed.length;
  if (Q === 0 || C === 0) return null;
  if (Q > C) return null;

  let best = null;

  // DFS alignments: pick indices i0 < i1 < ...
  function rec(qi, startCi, chosen) {
    if (qi === Q) {
      // compute affixes
      const first = chosen[0];
      const last = chosen[chosen.length - 1];
      const prefixWords = first;
      const suffixWords = (C - 1 - last);
      let middleWords = 0;
      for (let k = 0; k < chosen.length - 1; k++) {
        middleWords += (chosen[k + 1] - chosen[k] - 1);
      }

      const perWord = [];
      const md = [];
      for (let k = 0; k < Q; k++) {
        const m = wordExactMatch(qWordsParsed[k], candWordsParsed[chosen[k]]);
        if (!m) return;
        perWord.push({ matraDiff: m.matraDiff, internalCount: 0, outsideNonEmpty: 0, outsideEmpty: 0, insertionCount: 0, outsidePosPenalty: 0, tierScore: 0 });
        md.push(m.matraDiff);
      }

      let key;
      if (Q === 1) {
        key = buildRankKeyOneWordExact(md[0], prefixWords, middleWords, suffixWords);
      } else {
        key = buildRankKeyMultiwordExact(md, prefixWords, middleWords, suffixWords);
      }

      const explain = buildExplain("EXACT", qWordsParsed.map(w => w.raw), candWordsParsed.map(w => w.raw), chosen, prefixWords, middleWords, suffixWords, perWord);

      const cand = { key, explain, chosen, perWord };
      if (!best || compareRankKey(cand.key, best.key) < 0) best = cand;
      return;
    }

    for (let ci = startCi; ci < C; ci++) {
      // Quick structural filter: exact needs same struct sequence
      if (wordExactMatch(qWordsParsed[qi], candWordsParsed[ci])) {
        rec(qi + 1, ci + 1, chosen.concat(ci));
      }
    }
  }

  rec(0, 0, []);
  return best;
}

function matchNameFuzzy(qWordsParsed, candWordsParsed) {
  const Q = qWordsParsed.length;
  const C = candWordsParsed.length;
  if (Q === 0 || C === 0) return null;
  if (Q > C) return null;

  let best = null;

  // Extra rule: for 2-word queries, if outside on word2 > n/2, disallow outside on word1
  // We apply this by trying both options when Q===2.
  const enforceWord2OutsideGate = (Q === 2);

  function rec(qi, startCi, chosen, perWordStates, outsideUsedWord1Locked) {
    if (qi === Q) {
      const first = chosen[0];
      const last = chosen[chosen.length - 1];
      const prefixWords = first;
      const suffixWords = (C - 1 - last);
      let middleWords = 0;
      for (let k = 0; k < chosen.length - 1; k++) middleWords += (chosen[k + 1] - chosen[k] - 1);

      const md = perWordStates.map(s => s.matraDiff || 0);
      const internalCounts = perWordStates.map(s => s.internalCount || 0);
      const tierScores = perWordStates.map(s => s.tierScore || 0);
      const outsideCounts = perWordStates.map(s => (s.outsideNonEmpty || 0) + (s.outsideEmpty || 0));
      const outsidePosPenalties = perWordStates.map(s => s.outsidePosPenalty || 0);
      const insertionCounts = perWordStates.map(s => s.insertionCount || 0);

      const anyInsertion = insertionCounts.some(x => x > 0);
      const anyOutside = outsideCounts.some(x => x > 0);
      const anyInternal = internalCounts.some(x => x > 0);

      let key;
      let modeLabel;

      if (!anyOutside && !anyInternal && !anyInsertion) {
        // exact-like (matra-only) — keep it in EXACT ordering even in typing-mistakes mode
        modeLabel = "EXACT";
        if (Q === 1) key = buildRankKeyOneWordExact(md[0], prefixWords, middleWords, suffixWords);
        else key = buildRankKeyMultiwordExact(md, prefixWords, middleWords, suffixWords);
      } else if (anyInsertion) {
        modeLabel = "INSERTION";
        key = buildRankKeyMultiwordInsertion(insertionCounts, outsideCounts, internalCounts, outsidePosPenalties, tierScores, md, prefixWords, middleWords, suffixWords);
      } else if (anyOutside) {
        modeLabel = "OUTSIDE";
        key = buildRankKeyMultiwordOutside(outsideCounts, internalCounts, outsidePosPenalties, tierScores, md, prefixWords, middleWords, suffixWords);
      } else {
        modeLabel = "INTERNAL";
        if (Q === 1) {
          key = buildRankKeyOneWordInternal(internalCounts[0], tierScores[0], md[0], prefixWords, middleWords, suffixWords);
        } else {
          key = buildRankKeyMultiwordInternal(internalCounts, tierScores, md, prefixWords, middleWords, suffixWords);
        }
      }

      const explain = buildExplain(modeLabel, qWordsParsed.map(w => w.raw), candWordsParsed.map(w => w.raw), chosen, prefixWords, middleWords, suffixWords, perWordStates);

      const cand = { key, explain, chosen, perWordStates };
      if (!best || compareRankKey(cand.key, best.key) < 0) best = cand;
      return;
    }

    for (let ci = startCi; ci < C; ci++) {
      // Fuzzy word match per word
      const qW = qWordsParsed[qi];
      const cW = candWordsParsed[ci];

      const isMultiwordQuery = (Q >= 2);
      // Apply protected prefix rule ONLY to first word of multiword queries (your instruction)
      let protectedPrefixLen = 0;
      if (isMultiwordQuery && qi === 0) {
        protectedPrefixLen = qW.len <= 5 ? 2 : 3; // as per your multiword rule
      }

      // Outside allowed unless locked by the word2 rule
      let allowOutside = true;
      if (enforceWord2OutsideGate && qi === 0 && outsideUsedWord1Locked) allowOutside = false;

      const st = wordFuzzyMatch(qW, cW, {
        allowOutside,
        protectedPrefixLen,
      });
      if (!st) continue;

      // Apply “word2 outside gate”: if word2 outside > n/2 then lock outside on word1
      let nextLocked = outsideUsedWord1Locked;
      if (enforceWord2OutsideGate && qi === 1) {
        const outside2 = (st.outsideNonEmpty || 0) + (st.outsideEmpty || 0);
        const n = qW.len;
        if (outside2 > Math.floor(n / 2)) {
          nextLocked = true;
        }
      }

      rec(qi + 1, ci + 1, chosen.concat(ci), perWordStates.concat(st), nextLocked);
    }
  }

  // Try both paths for the word2 gate:
  // - start unlocked
  rec(0, 0, [], [], false);

  return best;
}

function scoreCandidateName(queryWordsParsed, candName, exactOn) {
  const cWords = splitWords(candName).map(parseWord);
  if (cWords.length === 0) return null;

  if (exactOn) {
    const ex = matchNameExact(queryWordsParsed, cWords);
    if (!ex) return null;
    return { ...ex, matchMode: "EXACT" };
  }

  // typing-mistakes mode: exact results included naturally because fuzzy DP can do pure matches;
  // but we still compute exact first as a fast path.
  const ex = matchNameExact(queryWordsParsed, cWords);
  if (ex) return { ...ex, matchMode: "EXACT" };

  const fu = matchNameFuzzy(queryWordsParsed, cWords);
  if (!fu) return null;
  return { ...fu, matchMode: "FUZZY" };
}

/* =====================
   Worker runtime
===================== */
let STATE = null;

function resetState() {
  STATE = {
    query: "",
    queryWords: [],
    exactOn: true,
    scope: "voter",
    total: 0,
    done: 0,
    hits: [],
  };
}

resetState();

self.onmessage = (ev) => {
  try {
    const msg = ev.data || {};
    const type = msg.type;

    if (type === "start") {
      resetState();
      STATE.query = normName(msg.query || "");
      STATE.exactOn = !!msg.exactOn;
      STATE.scope = msg.scope || "voter";
      STATE.total = msg.total || 0;
      STATE.done = 0;

      const qWords = splitWords(STATE.query).map(parseWord);
      STATE.queryWords = qWords;

      return;
    }

    if (type === "batch") {
      const rows = msg.rows || [];
      for (const row of rows) {
        const voter = row.voter_name_norm || row["Voter Name"] || "";
        const rel = row.relative_name_norm || row["Relative Name"] || "";

        let best = null;
        let bestField = null;

        if (STATE.scope === "voter" || STATE.scope === "anywhere") {
          const m = scoreCandidateName(STATE.queryWords, voter, STATE.exactOn);
          if (m) {
            best = m;
            bestField = "voter";
          }
        }
        if (STATE.scope === "relative" || STATE.scope === "anywhere") {
          const m = scoreCandidateName(STATE.queryWords, rel, STATE.exactOn);
          if (m && (!best || compareRankKey(m.key, best.key) < 0)) {
            best = m;
            bestField = "relative";
          }
        }

        if (best) {
          STATE.hits.push({
            row_id: row.row_id,
            key: best.key,
            explain: best.explain,
            match_field: bestField,
          });
        }
      }

      STATE.done += rows.length;
      // progress ping (cheap)
      if (STATE.total) {
        self.postMessage({ type: "progress", done: STATE.done, total: STATE.total, phase: "scoring" });
      }
      return;
    }

    if (type === "finish") {
      // sort by lexicographic rankKey
      STATE.hits.sort((a, b) => compareRankKey(a.key, b.key));

      self.postMessage({
        type: "done",
        ranked: STATE.hits,
      });
      return;
    }
  } catch (err) {
    const message = err && err.message ? err.message : String(err || "Worker error");
    try {
      self.postMessage({ type: "error", message });
    } catch {}
  }
};
