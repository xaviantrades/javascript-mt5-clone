/** MetaQuotes broker-server discovery, based on the source research report. */
import { createHash } from "node:crypto";

const HMAC_KEY = Buffer.from([
  61, 123, 21, 22, 214, 234, 187, 52, 217, 214, 99, 227, 98, 62, 27, 215,
  251, 220, 174, 244, 87, 59, 223, 53, 127, 168, 207, 11, 190, 173, 146, 127,
]);
const SEARCH_URL = "https://search.mtapi.io/Search?company={company}&mt5=true";
const SEARCHMQ_URL = "https://updates.metaquotes.net/public/mt5/network";
const USER_AGENT = "MetaTrader 5 Terminal/5.5830";
const REQUEST_TIMEOUT_MS = 12_000;

const md5 = (data) => createHash("md5").update(data).digest();

/**
 * Percent-encode like Python's urllib.parse.quote (keeps "/" and "_.-~" plus
 * alphanumerics; encodes everything else, including !'()*).
 * The encoded text is part of the signed request body, so it must match exactly.
 */
export function pyQuote(text) {
  return encodeURIComponent(text)
    .replace(/%2F/gi, "/")
    .replace(/[!'()*]/g, (c) => "%" + c.charCodeAt(0).toString(16).toUpperCase());
}

export function signature(body) {
  const first = md5(Buffer.from(body, "latin1"));
  return md5(Buffer.concat([first, HMAC_KEY])).toString("hex");
}

function makeCookie() {
  const now = BigInt(Math.floor(Date.now() / 1000));
  const ms = (process.hrtime.bigint() / 1_000_000n) & 0x1ffffffn;
  const value = ((now - 1420070400n) | (ms << 32n)) | 0x4200000000000000n;

  // MSVC-style LCG, matching the original implementation.
  let seed = Number(BigInt(Date.now()) * 1_000_000n & 0xffffffffn) >>> 0;
  const sample = Buffer.alloc(256);
  for (let i = 0; i < 256; i++) {
    seed = (Math.imul(seed, 214013) + 2531011) >>> 0;
    sample[i] = (seed >>> 16) & 0xff;
  }
  const digest = Buffer.from(md5(sample));
  let sum = 0;
  for (let i = 1; i < digest.length; i++) sum += digest[i];
  digest[0] = sum & 0xff;
  const tid = digest.subarray(0, 8).toString("hex").toUpperCase().slice(0, 17);
  return `_fz_uniq=${value};uniq=${value};age=${now - 86400n};tid=${tid}`;
}

async function searchMq(server) {
  const body = `company=${pyQuote(server)}&code=mt5`;
  const form = `${body}&signature=${signature(body)}&ver=2`;
  const response = await fetch(SEARCHMQ_URL, {
    method: "POST",
    body: form,
    headers: {
      "Content-Type": "application/x-www-form-urlencoded",
      Accept: "*/*",
      "User-Agent": USER_AGENT,
      Cookie: makeCookie(),
    },
    signal: AbortSignal.timeout(REQUEST_TIMEOUT_MS),
  });
  if (!response.ok) {
    throw new Error(`HTTP ${response.status} from MetaQuotes broker search`);
  }
  const raw = await response.text();
  const start = raw.indexOf("{");
  if (start < 0) {
    throw new Error("MetaQuotes broker search returned no JSON");
  }
  return JSON.parse(raw.slice(start)).result ?? [];
}

async function searchFallback(server) {
  const response = await fetch(SEARCH_URL.replace("{company}", pyQuote(server)), {
    signal: AbortSignal.timeout(REQUEST_TIMEOUT_MS),
  });
  if (!response.ok) {
    throw new Error(`HTTP ${response.status} from fallback broker search`);
  }
  return (await response.json()).result ?? [];
}

/**
 * Return all matching broker access endpoints as [host, port] pairs.
 * @param {string} server  exact broker server name, e.g. "Broker-MT5Live"
 * @returns {Promise<Array<[string, number]>>}
 */
export async function findServerEndpoints(server) {
  const errors = [];
  for (const searchFn of [searchMq, searchFallback]) {
    try {
      for (const company of await searchFn(server)) {
        for (const item of company.results ?? []) {
          if (String(item.name ?? "").toLowerCase() !== server.toLowerCase()) continue;
          const endpoints = new Map();
          for (const access of item.access ?? []) {
            const idx = String(access).lastIndexOf(":");
            const host = idx >= 0 ? access.slice(0, idx) : "";
            const portText = idx >= 0 ? access.slice(idx + 1) : String(access);
            if (host) {
              const port = parseInt(portText || "443", 10);
              endpoints.set(`${host}:${port}`, [host, port]);
            }
          }
          if (endpoints.size) return [...endpoints.values()];
        }
      }
    } catch (err) {
      errors.push(err.message);
    }
  }
  throw new Error(`Could not resolve MT5 server '${server}'. ` + errors.join(" | "));
}
