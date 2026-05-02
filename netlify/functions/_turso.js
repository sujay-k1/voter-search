/**
 * netlify/functions/_turso.js
 * Shared helpers for Turso (libSQL) access.
 *
 * IMPORTANT:
 * - All district DBs are in a single Turso account (token C).
 * - We DO NOT hard-restrict district slugs via a mapping anymore.
 *   If the DB exists, the functions should work.
 *
 * Required env vars in Netlify:
 *   TURSO_TOKEN_C  (or TURSO_TOKEN)
 *
 * Optional env vars:
 *   TURSO_USER        (default: sujay-k3)
 *   TURSO_URL_SUFFIX  (default: aws-ap-south-1.turso.io)
 *   TURSO_DB_PREFIX   (default: s27-)
 */

let _libsqlMod = null;
async function getLibsql() {
  if (_libsqlMod) return _libsqlMod;
  _libsqlMod = await import("@libsql/client");
  return _libsqlMod;
}

function slugifyDistrictId(id) {
  return String(id ?? "")
    .trim()
    .toLowerCase()
    .replace(/[_\s]+/g, "-")
    .replace(/[^a-z0-9-]/g, "-")
    .replace(/-+/g, "-")
    .replace(/^-+|-+$/g, "");
}

function resolveAccount(districtIdOrSlug) {
  const slug = slugifyDistrictId(districtIdOrSlug);
  if (!slug) throw new Error("Missing district");

  // Single account mode.
  const token = process.env.TURSO_TOKEN_C || process.env.TURSO_TOKEN;
  const user = process.env.TURSO_USER || "sujay-k3";

  return { slug, account: "C", user, token };
}

const CLIENT_CACHE = new Map();

async function getClient(districtIdOrSlug) {
  const { slug, account, user, token } = resolveAccount(districtIdOrSlug);
  if (!token) throw new Error(`Missing TURSO_TOKEN_${account} (or TURSO_TOKEN) in Netlify env vars`);

  const suffix = process.env.TURSO_URL_SUFFIX || "aws-ap-south-1.turso.io";
  const prefix = process.env.TURSO_DB_PREFIX || "s27-";

  // DB naming convention: <prefix><district-slug>-<user>.<suffix>
  // Example: libsql://s27-dumka-sujay-k3.aws-ap-south-1.turso.io
  const url = `libsql://${prefix}${slug}-${user}.${suffix}`;
  const cacheKey = `${account}:${slug}`;

  if (CLIENT_CACHE.has(cacheKey)) return CLIENT_CACHE.get(cacheKey);

  const { createClient } = await getLibsql();
  const client = createClient({ url, authToken: token });

  CLIENT_CACHE.set(cacheKey, client);
  return client;
}

function json(statusCode, obj) {
  return {
    statusCode,
    headers: {
      "content-type": "application/json; charset=utf-8",
      "access-control-allow-origin": "*",
      "access-control-allow-headers": "content-type",
      "access-control-allow-methods": "POST,OPTIONS",
    },
    body: JSON.stringify(obj ?? {}),
  };
}

const DEFAULT_VPS_DISTRICTS = [
  "dhanbad",
  "khunti",
  "ranchi",
  "hazaribagh",
  "bokaro",
  "palamu",
  "east-singhbhum",
  "gumla",
  "saraikela-kharswan",
  "west-singhbhum",
  "chatra",
  "deoghar",
  "dumka",
  "giridih",
  "godda",
  "jamtara",
  "kodarma",
  "pakur",
  "ramgarh",
  "sahebganj",
].join(",");

function parseDistrictSet(input, fallback = DEFAULT_VPS_DISTRICTS) {
  const raw = String(input ?? fallback);
  const out = new Set();
  for (const part of raw.split(",")) {
    const slug = slugifyDistrictId(part);
    if (slug) out.add(slug);
  }
  return out;
}

function shouldUseVpsDistrict(districtIdOrSlug) {
  const slug = slugifyDistrictId(districtIdOrSlug);
  if (!slug) return false;
  const districts = parseDistrictSet(process.env.VPS_API_DISTRICTS, DEFAULT_VPS_DISTRICTS);
  return districts.has(slug);
}

async function proxyJsonPost(path, body) {
  const base = process.env.VPS_API_BASE || "https://api.sujaykumar.net/";
  const timeoutMs = Math.max(1000, asInt(process.env.VPS_API_TIMEOUT_MS, 60000) || 60000);
  const url = new URL(String(path || "").replace(/^\/+/, ""), base).toString();

  const ctrl = new AbortController();
  const timer = setTimeout(() => ctrl.abort("timeout"), timeoutMs);

  try {
    const resp = await fetch(url, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify(body ?? {}),
      signal: ctrl.signal,
    });

    const text = await resp.text();
    let payload = null;
    try {
      payload = text ? JSON.parse(text) : {};
    } catch {
      payload = { ok: false, error: text || `HTTP ${resp.status}` };
    }

    return json(resp.status, payload);
  } catch (err) {
    const msg = String(err?.message || err || "VPS proxy failed");
    return json(502, { ok: false, error: `VPS proxy failed: ${msg}` });
  } finally {
    clearTimeout(timer);
  }
}

function ok(obj) {
  return json(200, { ok: true, ...(obj ?? {}) });
}

function badRequest(msg) {
  return json(400, { ok: false, error: String(msg || "Bad Request") });
}

function serverError(err) {
  const msg = err && err.message ? err.message : String(err || "Server Error");
  return json(500, { ok: false, error: msg });
}

async function readJsonBody(event) {
  try {
    const raw = event.body || "";
    return raw ? JSON.parse(raw) : {};
  } catch {
    return null;
  }
}

function asInt(x, fallback = null) {
  const n = Number(x);
  return Number.isFinite(n) ? n : fallback;
}

function asString(x, fallback = "") {
  return x === null || x === undefined ? fallback : String(x);
}

function toBufferMaybe(x) {
  if (!x) return null;

  // libsql sometimes returns Uint8Array/ArrayBuffer for BLOB.
  if (Buffer.isBuffer(x)) return x;
  if (x instanceof Uint8Array) return Buffer.from(x);
  if (x instanceof ArrayBuffer) return Buffer.from(new Uint8Array(x));

  // Defensive: if a driver ever returns base64 string for BLOB.
  if (typeof x === "string") {
    // Try base64 first. If it's not base64, Buffer will still create bytes,
    // but decodeRowIds will then fail gracefully (empty/garbage => no candidates).
    try {
      return Buffer.from(x, "base64");
    } catch {
      try {
        return Buffer.from(x);
      } catch {
        return null;
      }
    }
  }

  try {
    return Buffer.from(x);
  } catch {
    return null;
  }
}

// Heuristic decoder for row_ids BLOB.
// Supports: packed uint32 LE, packed uint64 LE, varint (LEB128), and varint-delta (cumsum).
function decodeRowIds(blob, nHint) {
  const buf = toBufferMaybe(blob);
  if (!buf || !buf.length) return [];

  const len = buf.length;
  const n = asInt(nHint, null);

  function decodeU32() {
    const out = [];
    for (let i = 0; i + 4 <= len; i += 4) out.push(buf.readUInt32LE(i));
    return out;
  }

  function decodeU64() {
    // Optimized for our data:
    // row_id values fit in uint32 (<= ~200k), but blobs are packed as uint64 LE.
    // Reading BigInt is significantly slower than reading UInt32.
    //
    // Safe behavior:
    // - Fast path: read low 32 bits (every 8 bytes) IF all high 32 bits are zero.
    // - Fallback: if any high word is non-zero, compute using number math when safe,
    //   otherwise use BigInt for that element.
    const out = [];
    for (let i = 0; i + 8 <= len; i += 8) {
      const lo = buf.readUInt32LE(i);
      const hi = buf.readUInt32LE(i + 4);

      if (hi === 0) {
        out.push(lo);
        continue;
      }

      // Rare fallback: keep correctness even if a DB ever stores larger ids.
      // Use number math when safe; otherwise BigInt.
      const asNum = hi * 4294967296 + lo;
      if (Number.isSafeInteger(asNum)) {
        out.push(asNum);
      } else {
        const v = buf.readBigUInt64LE(i);
        const num = Number(v);
        out.push(Number.isFinite(num) ? num : 0);
      }
    }
    return out;
  }

  // Fast paths when n is known.
  if (n && len === n * 4) return decodeU32();
  if (n && len === n * 8) return decodeU64();

  // Heuristic fixed-width.
  if (len % 4 === 0 && (!n || len / 4 === n)) return decodeU32();
  if (len % 8 === 0 && (!n || len / 8 === n)) return decodeU64();

  // Varint (unsigned LEB128)
  const raw = [];
  let i = 0;
  while (i < len) {
    let res = 0;
    let shift = 0;
    while (true) {
      if (i >= len) break;
      const b = buf[i++];
      res |= (b & 0x7f) << shift;
      if ((b & 0x80) === 0) break;
      shift += 7;
      if (shift > 35) break;
    }
    raw.push(res >>> 0);
  }

  const directOk = !n || raw.length === n;

  // Varint-delta
  const delta = [];
  let acc = 0;
  for (const d of raw) {
    acc += d;
    delta.push(acc >>> 0);
  }
  const deltaOk = !n || delta.length === n;

  if (directOk && !deltaOk) return raw;
  if (!directOk && deltaOk) return delta;

  // If both plausible, choose the one that looks like increasing ids.
  const maxDirect = raw.reduce((m, v) => (v > m ? v : m), 0);
  const maxDelta = delta.reduce((m, v) => (v > m ? v : m), 0);

  if (maxDirect < 5000 && maxDelta > 5000) return delta;

  return raw;
}

module.exports = {
  getClient,
  ok,
  badRequest,
  serverError,
  readJsonBody,
  asInt,
  asString,
  decodeRowIds,
  slugifyDistrictId,
  shouldUseVpsDistrict,
  proxyJsonPost,
};
