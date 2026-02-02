// worker.js (ES module)

let query = "";
let qTokens = [];
let qTokenCount = 0;

let scope = "voter";      // "voter" | "relative" | "anywhere"
let exactOn = false;

let total = 0;
let received = 0;
let buffer = []; // full ranked results: {row_id, score}

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

function trigrams(s) {
  s = (s || "").replace(/\s+/g, "");
  if (s.length < 3) return [];
  const out = [];
  for (let i = 0; i <= s.length - 3; i++) out.push(s.slice(i, i + 3));
  return out;
}

function jaccard(a, b) {
  if (a.length === 0 || b.length === 0) return 0;
  const A = new Set(a);
  const B = new Set(b);
  let inter = 0;
  for (const x of A) if (B.has(x)) inter++;
  const uni = A.size + B.size - inter;
  return uni === 0 ? 0 : inter / uni;
}

// ----- Exact tiers (reorder-aware) -----
// Returns { tier: number, windowLen: number|null }
// Higher tier is better.
// We keep it deterministic and cheap.
function exactTier(qNorm, qToks, textNorm) {
  if (!textNorm) return { tier: 0, windowLen: null };

  // Tier 5: full string equal
  if (textNorm === qNorm) return { tier: 5, windowLen: qToks.length };

  const tToks = tokenize(textNorm);
  if (!tToks.length || !qToks.length) return { tier: 0, windowLen: null };

  // helper: multiset compare by sorted tokens (ok for our use)
  const qs = [...qToks].slice().sort().join("\u0001");
  const tsExact = [...tToks].slice().sort().join("\u0001");

  // Tier 4: exact tokens (same multiset, same length), reorder allowed
  if (tToks.length === qToks.length && tsExact === qs) {
    return { tier: 4, windowLen: qToks.length };
  }

  const k = qToks.length;

  // Tier 3: contains contiguous subsequence in same order
  if (k <= tToks.length) {
    for (let i = 0; i <= tToks.length - k; i++) {
      let ok = true;
      for (let j = 0; j < k; j++) {
        if (tToks[i + j] !== qToks[j]) { ok = false; break; }
      }
      if (ok) return { tier: 3, windowLen: k };
    }
  }

  // Tier 2: contains contiguous window of length k whose token-multiset equals query (any order contiguous)
  if (k <= tToks.length) {
    const qSorted = [...qToks].slice().sort().join("\u0001");
    for (let i = 0; i <= tToks.length - k; i++) {
      const win = tToks.slice(i, i + k).slice().sort().join("\u0001");
      if (win === qSorted) return { tier: 2, windowLen: k };
    }
  }

  // Tier 1: contains all tokens anywhere (order irrelevant); prefer tighter window for tie-break
  // We'll compute min window length that covers all tokens (approx by positions).
  const positions = new Map(); // token -> list of indices
  for (let i = 0; i < tToks.length; i++) {
    const tok = tToks[i];
    if (!positions.has(tok)) positions.set(tok, []);
    positions.get(tok).push(i);
  }

  for (const qt of qToks) {
    if (!positions.has(qt)) return { tier: 0, windowLen: null };
  }

  // For window size: greedy min/max over first occurrences is an approximation but stable.
  // Good enough for ranking.
  let minPos = Infinity;
  let maxPos = -Infinity;
  for (const qt of qToks) {
    const p = positions.get(qt)[0];
    minPos = Math.min(minPos, p);
    maxPos = Math.max(maxPos, p);
  }
  const windowLen = (maxPos - minPos + 1);

  return { tier: 1, windowLen };
}

function baseFuzzyScore(qNorm, qToks, textNorm) {
  if (!textNorm) return 0;

  const hayTokens = tokenize(textNorm);

  // Token coverage
  let tokenHits = 0;
  for (const qt of qToks) {
    const hit = hayTokens.some(ht => ht.startsWith(qt) || ht.includes(qt));
    if (hit) tokenHits++;
  }
  const tokenCoverage = qToks.length ? tokenHits / qToks.length : 0;

  // Trigram similarity
  const triQ = trigrams(qNorm);
  const triH = trigrams(textNorm);
  const triSim = jaccard(triQ, triH);

  // Boosts
  let boost = 0;
  if (textNorm.startsWith(qNorm)) boost += 0.20;
  if (textNorm.includes(qNorm)) boost += 0.10;

  return (0.55 * triSim) + (0.35 * tokenCoverage) + boost;
}

function scoreAgainstField(row, fieldTextNorm) {
  const qNorm = query;
  const qToks = qTokens;

  // exact tier (optional)
  let tier = 0;
  let windowLen = null;
  if (exactOn) {
    const t = exactTier(qNorm, qToks, fieldTextNorm);
    tier = t.tier;
    windowLen = t.windowLen;
  }

  // fuzzy
  const fuzzy = baseFuzzyScore(qNorm, qToks, fieldTextNorm);

  // index metadata boosts
  const meta = row._meta || {};
  let hitCount = 0;
  let andHit = false;

  if (scope === "voter") {
    hitCount = Number(meta.voter_hit_count || 0);
    andHit = Boolean(meta.voter_and_hit);
  } else if (scope === "relative") {
    hitCount = Number(meta.relative_hit_count || 0);
    andHit = Boolean(meta.relative_and_hit);
  } else {
    // anywhere: choose best from either field for boosts
    const hv = Number(meta.voter_hit_count || 0);
    const hr = Number(meta.relative_hit_count || 0);
    hitCount = Math.max(hv, hr);
    andHit = Boolean(meta.voter_and_hit) || Boolean(meta.relative_and_hit);
  }

  const tokenFrac = qTokenCount ? (hitCount / qTokenCount) : 0;

  // tier boosts: big separation to get your desired ordering
  let tierBoost = 0;
  if (exactOn) {
    // tiers: 5 (exact string), 4 (exact tokens reordered), 3 (contiguous in-order),
    // 2 (contiguous any-order), 1 (all tokens anywhere)
    if (tier === 5) tierBoost = 1.20;
    else if (tier === 4) tierBoost = 1.05;
    else if (tier === 3) tierBoost = 0.85;
    else if (tier === 2) tierBoost = 0.70;
    else if (tier === 1) tierBoost = 0.45;

    // smaller window is better (bring जय प्रसाद राम above राम प्रसाद जय)
    if (tier === 1 && windowLen != null) {
      // windowLen >= tokenCount
      const tightness = (qTokenCount / windowLen); // 0..1
      tierBoost += 0.10 * tightness;
    }
  }

  // and-hit boost (matched all query prefixes in index)
  const andBoost = andHit ? 0.12 : 0;

  // tokenFrac boost (more query prefixes matched)
  const hitBoost = 0.18 * tokenFrac;

  const score = fuzzy + tierBoost + andBoost + hitBoost;

  return score;
}

function scoreRow(row) {
  const voter = norm(row.voter_name_raw || row.voter_name_norm || "");
  const rel   = norm(row.relative_name_raw || row.relative_name_norm || "");

  if (scope === "voter") {
    return scoreAgainstField(row, voter);
  }
  if (scope === "relative") {
    return scoreAgainstField(row, rel);
  }

  // anywhere: score both fields and keep best
  const sv = scoreAgainstField(row, voter);
  const sr = scoreAgainstField(row, rel);
  return Math.max(sv, sr);
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

self.onmessage = (ev) => {
  const msg = ev.data;

  try {
    if (msg.type === "start") {
      query = norm(msg.query || "");
      qTokens = tokenize(query);
      qTokenCount = Number(msg.tokenCount || qTokens.length || 0);

      scope = msg.scope || "voter";
      exactOn = Boolean(msg.exactOn);

      total = Number(msg.total || 0);
      received = 0;
      buffer = [];
      postProgress("scoring");
      return;
    }

    if (msg.type === "batch") {
      const rows = msg.rows || [];
      for (const row of rows) {
        const s = scoreRow(row);

        // Per agreement: exclude score=0 rows
        if (s > 0) {
          buffer.push({
            row_id: Number(row.row_id),
            score: s
          });
        }
      }
      received += rows.length;
      if (received % 4000 === 0 || received === total) postProgress("scoring");
      return;
    }

    if (msg.type === "finish") {
      postProgress("sorting");

      buffer.sort((a, b) => {
        if (b.score !== a.score) return b.score - a.score;
        return a.row_id - b.row_id;
      });

      self.postMessage({ type: "done", ranked: buffer });
      return;
    }
  } catch (e) {
    self.postMessage({ type: "error", message: e?.message || String(e) });
  }
};
