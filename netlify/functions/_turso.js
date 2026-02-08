/**
 * netlify/functions/_turso.js
 * Shared helpers for Turso (libSQL) access.
 *
 * Env vars required in Netlify:
 *   TURSO_TOKEN_A, TURSO_TOKEN_B, TURSO_TOKEN_C
 *
 * District -> account mapping is hard-coded (lossless to your stated distribution).
 */

const DISTRICTS_A = new Set([
  "chatra","hazaribagh","deoghar","jamtara","dumka","kodarma","pakur","ramgarh","giridih","sahebganj","godda"
]);

const DISTRICTS_B = new Set([
  "bokaro","khunti","dhanbad","ranchi","east-singhbhum","saraikela-kharswan","gumla","west-singhbhum"
]);

const DISTRICTS_C = new Set([
  "garhwa","lohardaga","simdega","latehar","palamu"
]);

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

  if (DISTRICTS_A.has(slug)) return { slug, account: "A", user: "sujay-k1", token: process.env.TURSO_TOKEN_A };
  if (DISTRICTS_B.has(slug)) return { slug, account: "B", user: "sujay-k2", token: process.env.TURSO_TOKEN_B };
  if (DISTRICTS_C.has(slug)) return { slug, account: "C", user: "sujay-k3", token: process.env.TURSO_TOKEN_C };

  throw new Error(`Unknown district slug: ${slug}`);
}

let _libsqlMod = null;
async function getLibsql() {
  if (_libsqlMod) return _libsqlMod;
  _libsqlMod = await import("@libsql/client");
  return _libsqlMod;
}

const CLIENT_CACHE = new Map();

async function getClient(districtIdOrSlug) {
  const { slug, account, user, token } = resolveAccount(districtIdOrSlug);
  if (!token) throw new Error(`Missing TURSO_TOKEN_${account} in Netlify env vars`);

  const url = `libsql://s27-${slug}-${user}.aws-ap-south-1.turso.io`;
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

function ok(obj) {
  return json(200, { ok: true, ...obj });
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
  if (Buffer.isBuffer(x)) return x;
  if (x instanceof Uint8Array) return Buffer.from(x);
  if (x instanceof ArrayBuffer) return Buffer.from(new Uint8Array(x));
  return Buffer.from(x);
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
    const out = [];
    for (let i = 0; i + 8 <= len; i += 8) {
      const v = buf.readBigUInt64LE(i);
      const num = Number(v);
      out.push(Number.isFinite(num) ? num : 0);
    }
    return out;
  }

  if (n && len === n * 4) return decodeU32();
  if (n && len === n * 8) return decodeU64();

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

  const delta = [];
  let acc = 0;
  for (const d of raw) {
    acc += d;
    delta.push(acc >>> 0);
  }
  const deltaOk = !n || delta.length === n;

  if (directOk && !deltaOk) return raw;
  if (!directOk && deltaOk) return delta;

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
};
