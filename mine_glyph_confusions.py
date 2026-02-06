import os
import re
import json
import math
import time
import hashlib
from pathlib import Path
from collections import defaultdict, Counter
from concurrent.futures import ProcessPoolExecutor, as_completed

import pandas as pd

# ---------------- CONFIG ----------------
STATE_CODE = "S27"

# Script will be run from ~/Desktop/voter-search/
# and expects ./data/S27/ac=XX/voters.parquet
DATA_ROOT = Path("./data")

# Columns to mine from (must exist in voters.parquet)
TEXT_COLS = ["voter_name_norm", "relative_name_norm"]

# Performance knobs
MAX_WORKERS = None
READ_ENGINE = "pyarrow"
CHUNK_ROWS = None

# Token filtering
MIN_TOKEN_LEN = 2
MAX_TOKEN_LEN = 24

# Mining knobs
MAX_DIST = 2
MAX_CHUNK_LEN = 3

# Caps
MAX_VARIANTS_PER_SKELETON = 40
MAX_PAIRS_PER_SKELETON = 400
MAX_SUGGESTIONS_TOTAL = 30000

# Reduce false positives: ignore super-common tokens
IGNORE_TOP_FREQ_RATIO = 0.0008
IGNORE_MIN_COUNT = 150

# Keep a few examples per (src,dst) for debugging
MAX_EXAMPLES_PER_PAIR = 3

# NEW: control whether matra-only confusions are removed from main output
DROP_MATRA_ONLY_FROM_MAIN = True
# --------------------------------------


DEVANAGARI_RE = re.compile(r"[\u0900-\u097F]+")
PUNCT_RE = re.compile(r"[.,;:|/\\()\[\]{}<>\"'`~!@#$%^&*_+=?-]")

# Matras / marks (for stripping in skeleton + matra-only detection)
MATRA_SET = set(list("ािीुूेैोौृॄॢॣ"))
MARK_SET = set(list("ँंः़्॒॑"))

ONLY_MATRA_OR_MARK = MATRA_SET | MARK_SET


def stable_hash(s: str) -> int:
    h = hashlib.blake2b(s.encode("utf-8"), digest_size=8).digest()
    return int.from_bytes(h, "big", signed=False)


def list_ac_dirs(state_dir: Path):
    if not state_dir.exists():
        return []
    out = []
    for p in state_dir.iterdir():
        if p.is_dir() and p.name.startswith("ac="):
            out.append(p)
    return sorted(out, key=lambda x: x.name)


def read_voters_min_cols(voters_path: Path, cols):
    return pd.read_parquet(voters_path, columns=cols, engine=READ_ENGINE)


def normalize_spaces_and_punct(s: str) -> str:
    if s is None:
        return ""
    s = str(s).replace("\u00A0", " ").strip()
    s = re.sub(r"\s+", " ", s)
    s = PUNCT_RE.sub(" ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s


def tokenize(s: str):
    s = normalize_spaces_and_punct(s)
    if not s:
        return []
    toks = []
    for t in s.split(" "):
        t = t.strip()
        if not t:
            continue
        if DEVANAGARI_RE.fullmatch(t):
            if MIN_TOKEN_LEN <= len(t) <= MAX_TOKEN_LEN:
                toks.append(t)
    return toks


def strip_matras_marks(s: str) -> str:
    if not s:
        return ""
    out = []
    for ch in s:
        if ch in MATRA_SET:
            continue
        if ch in MARK_SET:
            continue
        out.append(ch)
    return "".join(out)


def skeleton_key(token: str) -> str:
    """
    Blocking key:
    remove matras+marks, keep independent vowels (as you wanted)
    """
    sk = strip_matras_marks(token)
    return sk


def is_pure_matra_or_mark(s: str) -> bool:
    if not s:
        return True
    for ch in s:
        if ch not in ONLY_MATRA_OR_MARK:
            return False
    return True


def is_matra_only_confusion(src: str, dst: str) -> bool:
    """
    True if the *base skeleton* (matra/mark stripped) is identical and non-empty.
    This catches:
      - matra changes (मि vs मा)
      - matra re-ordering (कु vs ुक in OCR segmentation)
      - anusvara/halant noise (if only marks differ)
    """
    bs = strip_matras_marks(src)
    bd = strip_matras_marks(dst)
    if not bs or not bd:
        return False
    return bs == bd


def bounded_levenshtein(a: str, b: str, max_dist: int) -> int:
    if a == b:
        return 0
    la, lb = len(a), len(b)
    if abs(la - lb) > max_dist:
        return max_dist + 1
    if la > lb:
        a, b = b, a
        la, lb = lb, la

    prev = list(range(lb + 1))
    for i in range(1, la + 1):
        cur = [i] + [0] * lb
        j_start = max(1, i - max_dist)
        j_end = min(lb, i + max_dist)

        for j in range(1, j_start):
            cur[j] = max_dist + 1

        min_row = max_dist + 1
        ai = a[i - 1]

        for j in range(j_start, j_end + 1):
            bj = b[j - 1]
            cost = 0 if ai == bj else 1
            cur[j] = min(
                prev[j] + 1,
                cur[j - 1] + 1,
                prev[j - 1] + cost
            )
            if cur[j] < min_row:
                min_row = cur[j]

        for j in range(j_end + 1, lb + 1):
            cur[j] = max_dist + 1

        if min_row > max_dist:
            return max_dist + 1
        prev = cur

    return prev[lb]


def align_ops(a: str, b: str):
    la, lb = len(a), len(b)
    dp = [[0] * (lb + 1) for _ in range(la + 1)]
    bt = [[None] * (lb + 1) for _ in range(la + 1)]

    for i in range(1, la + 1):
        dp[i][0] = i
        bt[i][0] = "D"
    for j in range(1, lb + 1):
        dp[0][j] = j
        bt[0][j] = "I"

    for i in range(1, la + 1):
        ai = a[i - 1]
        for j in range(1, lb + 1):
            bj = b[j - 1]
            if ai == bj:
                dp[i][j] = dp[i - 1][j - 1]
                bt[i][j] = "M"
            else:
                del_c = dp[i - 1][j] + 1
                ins_c = dp[i][j - 1] + 1
                sub_c = dp[i - 1][j - 1] + 1
                m = min(del_c, ins_c, sub_c)
                dp[i][j] = m
                if m == sub_c:
                    bt[i][j] = "S"
                elif m == del_c:
                    bt[i][j] = "D"
                else:
                    bt[i][j] = "I"

    ops = []
    i, j = la, lb
    while i > 0 or j > 0:
        op = bt[i][j]
        if op == "M":
            ops.append(("M", a[i - 1], b[j - 1]))
            i -= 1
            j -= 1
        elif op == "S":
            ops.append(("S", a[i - 1], b[j - 1]))
            i -= 1
            j -= 1
        elif op == "D":
            ops.append(("D", a[i - 1], None))
            i -= 1
        elif op == "I":
            ops.append(("I", None, b[j - 1]))
            j -= 1
        else:
            if i > 0:
                ops.append(("D", a[i - 1], None))
                i -= 1
            elif j > 0:
                ops.append(("I", None, b[j - 1]))
                j -= 1

    ops.reverse()
    return ops, dp[la][lb]


def extract_chunks(a: str, b: str, max_chunk_len: int):
    ops, dist = align_ops(a, b)
    if dist == 0:
        return []

    chunks = []
    src_buf = []
    dst_buf = []

    def flush():
        nonlocal src_buf, dst_buf
        if not src_buf and not dst_buf:
            return
        src = "".join([c for c in src_buf if c is not None])
        dst = "".join([c for c in dst_buf if c is not None])

        if len(src) > max_chunk_len or len(dst) > max_chunk_len:
            src_buf, dst_buf = [], []
            return

        # Still drop pure-matra chunks
        if is_pure_matra_or_mark(src) and is_pure_matra_or_mark(dst):
            src_buf, dst_buf = [], []
            return

        if src == dst:
            src_buf, dst_buf = [], []
            return

        chunks.append((src, dst))
        src_buf, dst_buf = [], []

    for op, ca, cb in ops:
        if op == "M":
            flush()
        elif op == "S":
            src_buf.append(ca)
            dst_buf.append(cb)
        elif op == "D":
            src_buf.append(ca)
        elif op == "I":
            dst_buf.append(cb)

    flush()
    return chunks


def choose_workers():
    if MAX_WORKERS and MAX_WORKERS > 0:
        return MAX_WORKERS
    try:
        n = os.cpu_count() or 8
    except Exception:
        n = 8
    return max(2, min(12, n - 2))


def mine_one_ac(ac_dir: Path):
    t0 = time.time()
    voters_path = ac_dir / "voters.parquet"
    if not voters_path.exists():
        return {"ac": ac_dir.name, "ok": False, "error": f"missing {voters_path}"}

    df = read_voters_min_cols(voters_path, TEXT_COLS)

    freq = Counter()
    rows = df.to_dict(orient="records")
    for r in rows:
        for c in TEXT_COLS:
            for tok in tokenize(r.get(c, "")):
                freq[tok] += 1

    if not freq:
        return {
            "ac": ac_dir.name,
            "ok": True,
            "tokens": 0,
            "pairs_accepted": 0,
            "suggestions": [],
            "matra_only": [],
            "notes": "no devanagari tokens found",
            "built_at_epoch": int(time.time()),
            "seconds": round(time.time() - t0, 3),
        }

    # Stop tokens
    tokens_sorted = [t for t, _ in freq.most_common()]
    top_n = max(1, int(len(tokens_sorted) * IGNORE_TOP_FREQ_RATIO))
    stop = set()
    for t in tokens_sorted[:top_n]:
        if freq[t] >= IGNORE_MIN_COUNT:
            stop.add(t)

    vocab = [t for t in freq.keys() if t not in stop]

    # Group by skeleton
    sk_groups = defaultdict(list)
    for t in vocab:
        sk = skeleton_key(t)
        if len(sk) < 2:
            continue
        sk_groups[sk].append(t)

    # Accumulators
    conf_main = defaultdict(float)
    conf_matra = defaultdict(float)

    ex_main = defaultdict(list)
    ex_matra = defaultdict(list)

    compared_pairs = 0
    accepted_pairs = 0

    for sk in sorted(sk_groups.keys(), key=stable_hash):
        tokens = sk_groups[sk]
        if len(tokens) < 2:
            continue

        tokens = sorted(tokens, key=lambda x: (-freq[x], stable_hash(x)))[:MAX_VARIANTS_PER_SKELETON]

        local = 0
        n = len(tokens)
        for i in range(n):
            a = tokens[i]
            for j in range(i + 1, n):
                b = tokens[j]
                local += 1
                if local > MAX_PAIRS_PER_SKELETON:
                    break

                compared_pairs += 1
                if abs(len(a) - len(b)) > MAX_DIST:
                    continue

                d = bounded_levenshtein(a, b, MAX_DIST)
                if d == 0 or d > MAX_DIST:
                    continue

                chunks = extract_chunks(a, b, MAX_CHUNK_LEN)
                if not chunks:
                    continue

                w = math.sqrt(freq[a] * freq[b])
                accepted_pairs += 1

                for src, dst in chunks:
                    if is_matra_only_confusion(src, dst):
                        conf_matra[(src, dst)] += w
                        if len(ex_matra[(src, dst)]) < MAX_EXAMPLES_PER_PAIR:
                            ex_matra[(src, dst)].append((a, b))
                        # Optionally do NOT add to main
                        if DROP_MATRA_ONLY_FROM_MAIN:
                            continue

                    conf_main[(src, dst)] += w
                    if len(ex_main[(src, dst)]) < MAX_EXAMPLES_PER_PAIR:
                        ex_main[(src, dst)].append((a, b))

                if accepted_pairs >= MAX_SUGGESTIONS_TOTAL:
                    break

            if local > MAX_PAIRS_PER_SKELETON or accepted_pairs >= MAX_SUGGESTIONS_TOTAL:
                break

        if accepted_pairs >= MAX_SUGGESTIONS_TOTAL:
            break

    def pack(conf_map, ex_map):
        out = []
        for (src, dst), w in conf_map.items():
            if src == dst:
                continue
            item = {"src": src, "dst": dst, "weight": float(w)}
            ex = ex_map.get((src, dst))
            if ex:
                item["examples"] = ex
            out.append(item)
        out.sort(key=lambda x: x["weight"], reverse=True)
        return out

    suggestions_main = pack(conf_main, ex_main)
    suggestions_matra = pack(conf_matra, ex_matra)

    return {
        "ac": ac_dir.name,
        "ok": True,
        "tokens": len(freq),
        "vocab_used": len(vocab),
        "stop_tokens": len(stop),
        "skeleton_groups": len(sk_groups),
        "pairs_compared": compared_pairs,
        "pairs_accepted": accepted_pairs,
        "suggestions": suggestions_main[:5000],
        "matra_only": suggestions_matra[:5000],
        "built_at_epoch": int(time.time()),
        "seconds": round(time.time() - t0, 3),
        "config": {
            "min_token_len": MIN_TOKEN_LEN,
            "max_token_len": MAX_TOKEN_LEN,
            "max_dist": MAX_DIST,
            "max_chunk_len": MAX_CHUNK_LEN,
            "max_variants_per_skeleton": MAX_VARIANTS_PER_SKELETON,
            "max_pairs_per_skeleton": MAX_PAIRS_PER_SKELETON,
            "drop_matra_only_from_main": DROP_MATRA_ONLY_FROM_MAIN,
        },
    }


def merge_ac_outputs(ac_outputs, key_name: str):
    merged = defaultdict(float)
    ex_merged = defaultdict(list)

    for r in ac_outputs:
        if not r.get("ok"):
            continue
        for s in r.get(key_name, []):
            src = s.get("src", "")
            dst = s.get("dst", "")
            w = float(s.get("weight", 0.0))
            if src == dst:
                continue
            merged[(src, dst)] += w

            ex = s.get("examples") or []
            if ex and len(ex_merged[(src, dst)]) < MAX_EXAMPLES_PER_PAIR:
                for pair in ex:
                    if len(ex_merged[(src, dst)]) >= MAX_EXAMPLES_PER_PAIR:
                        break
                    ex_merged[(src, dst)].append(pair)

    merged_list = []
    for (src, dst), w in merged.items():
        item = {"src": src, "dst": dst, "weight": float(w)}
        ex = ex_merged.get((src, dst))
        if ex:
            item["examples"] = ex
        merged_list.append(item)

    merged_list.sort(key=lambda x: x["weight"], reverse=True)
    return merged_list


def main():
    state_dir = DATA_ROOT / STATE_CODE
    if not state_dir.exists():
        raise RuntimeError(f"State dir not found: {state_dir} (run from voter-search folder?)")

    ac_dirs = list_ac_dirs(state_dir)
    if not ac_dirs:
        raise RuntimeError(f"No AC dirs found under {state_dir} (expected ac=01, ac=02, ...)")

    workers = choose_workers()
    print(f"Found {len(ac_dirs)} AC folders under {state_dir}")
    print(f"Using {workers} workers")

    outputs = []
    t0 = time.time()

    with ProcessPoolExecutor(max_workers=workers) as ex:
        futs = {ex.submit(mine_one_ac, ac): ac for ac in ac_dirs}
        for fut in as_completed(futs):
            ac = futs[fut]
            try:
                res = fut.result()
            except Exception as e:
                res = {"ac": ac.name, "ok": False, "error": str(e)}
            outputs.append(res)

            if res.get("ok"):
                print(
                    f"✅ {res['ac']}: accepted_pairs={res.get('pairs_accepted')} "
                    f"tokens={res.get('tokens')} in {res.get('seconds')}s"
                )
            else:
                print(f"⚠️  {res['ac']}: {res.get('error')}")

            # Per-AC output (same filename)
            out_path = state_dir / ac.name / "glyph_mine.json"
            try:
                with open(out_path, "w", encoding="utf-8") as f:
                    json.dump(res, f, ensure_ascii=False, indent=2)
            except Exception as e:
                print(f"⚠️  Failed to write {out_path}: {e}")

    # Merge state-wide MAIN (same filename as before)
    merged_main = merge_ac_outputs(outputs, "suggestions")
    merged_path = state_dir / "glyph_confusions_mined.json"
    merged_obj = {
        "state_code": STATE_CODE,
        "built_at_epoch": int(time.time()),
        "ac_count": len(ac_dirs),
        "workers": workers,
        "merged_count": len(merged_main),
        "top": merged_main[:2000],
    }
    with open(merged_path, "w", encoding="utf-8") as f:
        json.dump(merged_obj, f, ensure_ascii=False, indent=2)

    # Merge state-wide MATRA-ONLY (new file, for inspection / optional use)
    merged_matra = merge_ac_outputs(outputs, "matra_only")
    merged_matra_path = state_dir / "glyph_confusions_mined_matra_only.json"
    merged_matra_obj = {
        "state_code": STATE_CODE,
        "built_at_epoch": int(time.time()),
        "ac_count": len(ac_dirs),
        "workers": workers,
        "merged_count": len(merged_matra),
        "top": merged_matra[:2000],
    }
    with open(merged_matra_path, "w", encoding="utf-8") as f:
        json.dump(merged_matra_obj, f, ensure_ascii=False, indent=2)

    print("\nDone.")
    print(f"Per-AC outputs written to: {state_dir}/ac=XX/glyph_mine.json")
    print(f"Merged MAIN written to: {merged_path}")
    print(f"Merged MATRA-ONLY written to: {merged_matra_path}")
    print(f"Total seconds: {round(time.time() - t0, 3)}")


if __name__ == "__main__":
    main()
