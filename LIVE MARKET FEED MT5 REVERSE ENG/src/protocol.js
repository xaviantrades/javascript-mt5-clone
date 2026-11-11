/** Only the MT5 WebSocket protocol pieces required for market data. */
import { createCipheriv, createDecipheriv, randomBytes } from "node:crypto";
import { inflateRawSync, inflateSync } from "node:zlib";

import { BrokerSymbol, Candle, Quote } from "./models.js";

export const STATIC_KEY = Buffer.from(
  "02de02a1a65cc794684fcbea1ecb0fd7" + "4ae657e43662c11eee885d2fd64f4964",
  "hex",
);
const ZERO_IV = Buffer.alloc(16);

export const TIMEFRAMES = {
  M1: 1, M2: 2, M3: 3, M4: 4, M5: 5,
  M6: 6, M10: 10, M12: 12, M15: 15, M20: 20, M30: 30,
  H1: 16385, H2: 16386, H3: 16387, H4: 16388,
  H6: 16390, H8: 16392, H12: 16396,
  D1: 16408, W1: 32769, MN1: 49153,
};

export const TIMEFRAME_SECONDS = {
  M1: 60, M2: 120, M3: 180, M4: 240, M5: 300,
  M6: 360, M10: 600, M12: 720, M15: 900, M20: 1200, M30: 1800,
  H1: 3600, H2: 7200, H3: 10800, H4: 14400,
  H6: 21600, H8: 28800, H12: 43200,
  D1: 86400, W1: 604800, MN1: 2592000,
};

export const SYMBOL_RECORD_SIZE = 526;
export const QUOTE_RECORD_SIZE = 50;
export const CANDLE_RECORD_SIZE = 48;

function cipherName(key) {
  if (![16, 24, 32].includes(key.length)) {
    throw new RangeError(`Invalid AES key length: ${key.length} bytes`);
  }
  return `aes-${key.length * 8}-cbc`;
}

/** AES-CBC with a zero IV and PKCS#7 padding (what the MT5 web terminal uses). */
export function aesEncrypt(key, plaintext) {
  const cipher = createCipheriv(cipherName(key), key, ZERO_IV);
  return Buffer.concat([cipher.update(plaintext), cipher.final()]);
}

export function aesDecrypt(key, ciphertext) {
  if (!ciphertext || ciphertext.length === 0 || ciphertext.length % 16 !== 0) {
    throw new Error("AES ciphertext must be a non-empty multiple of 16 bytes");
  }
  const decipher = createDecipheriv(cipherName(key), key, ZERO_IV);
  try {
    return Buffer.concat([decipher.update(ciphertext), decipher.final()]);
  } catch {
    throw new Error("Invalid AES padding");
  }
}

/** 2 random bytes + uint16 LE command id + payload. */
export function buildCommand(commandId, payload = Buffer.alloc(0)) {
  const header = Buffer.alloc(4);
  randomBytes(2).copy(header, 0);
  header.writeUInt16LE(commandId, 2);
  return Buffer.concat([header, payload]);
}

/** uint32 LE length + uint32 LE 1 + encrypted bytes. */
export function packWire(encrypted) {
  const header = Buffer.alloc(8);
  header.writeUInt32LE(encrypted.length, 0);
  header.writeUInt32LE(1, 4);
  return Buffer.concat([header, encrypted]);
}

export function parseResponse(data) {
  if (data.length < 5) {
    throw new Error("MT5 response is shorter than its 5-byte header");
  }
  return {
    tag: data.readUInt16LE(0),
    cmdId: data.readUInt16LE(2),
    resCode: data[4],
    resBody: data.subarray(5),
  };
}

export function decodeUtf16(raw) {
  return raw.toString("utf16le").split("\u0000", 1)[0];
}

/** @returns {Map<string, BrokerSymbol>} keyed by broker symbol name */
export function parseSymbols(body) {
  if (body.length < 8) {
    throw new Error("Symbol catalogue response is too short");
  }
  const compressed = body.subarray(4);
  let data;
  try {
    data = inflateSync(compressed);
  } catch {
    data = inflateRawSync(compressed);
  }
  if (data.length < 4) {
    throw new Error("Decompressed symbol catalogue is too short");
  }

  const count = data.readUInt32LE(0);
  let offset = 4;
  const symbols = new Map();
  for (let i = 0; i < count; i++) {
    if (offset + SYMBOL_RECORD_SIZE > data.length) {
      throw new Error("Truncated symbol catalogue");
    }
    const name = decodeUtf16(data.subarray(offset, offset + 64));
    const description = decodeUtf16(data.subarray(offset + 64, offset + 192));
    const digits = data.readUInt32LE(offset + 192);
    const symbolId = data.readUInt32LE(offset + 196);
    const path = decodeUtf16(data.subarray(offset + 200, offset + 456));
    if (name) {
      symbols.set(name, new BrokerSymbol(name, description, digits, symbolId, path));
    }
    offset += SYMBOL_RECORD_SIZE;
  }
  return symbols;
}

/**
 * Decode 50-byte quote records.
 * Layout (little-endian): u32 symbolId, i32 seconds, u32 fields, f64 bid, f64 ask,
 * f64 last, i64 tickVolume, u32 msDelta, u16 flags.
 * @param {Buffer} body
 * @param {Map<number, BrokerSymbol>} symbolsById
 */
export function parseQuotes(body, symbolsById) {
  const quotes = [];
  let offset = 0;
  while (offset + QUOTE_RECORD_SIZE <= body.length) {
    const symbolId = body.readUInt32LE(offset);
    const seconds = body.readInt32LE(offset + 4);
    const bidRaw = body.readDoubleLE(offset + 12);
    const askRaw = body.readDoubleLE(offset + 20);
    const lastRaw = body.readDoubleLE(offset + 28);
    const tickVolume = Number(body.readBigInt64LE(offset + 36));
    const msDelta = body.readUInt32LE(offset + 44);
    const flags = body.readUInt16LE(offset + 48);

    const symbol = symbolsById.get(symbolId);
    if (symbol !== undefined) {
      const scale = 10 ** symbol.digits;
      quotes.push(
        new Quote(
          symbol.name,
          symbolId,
          bidRaw / scale,
          askRaw / scale,
          lastRaw / scale,
          tickVolume,
          new Date(seconds * 1000 + msDelta),
          flags,
        ),
      );
    }
    offset += QUOTE_RECORD_SIZE;
  }
  return quotes;
}

/** 74-byte rates request: UTF-16LE symbol (64 B), u16 timeframe id, i32 start, i32 end. */
export function buildRatesPayload(symbol, timeframe, start, end) {
  if (!(timeframe in TIMEFRAMES)) {
    throw new Error(`Unsupported timeframe: ${timeframe}`);
  }
  const encoded = Buffer.from(symbol, "utf16le");
  if (encoded.length > 64) {
    throw new Error("Symbol name exceeds the 64-byte MT5 field");
  }
  const payload = Buffer.alloc(74);
  encoded.copy(payload, 0);
  payload.writeUInt16LE(TIMEFRAMES[timeframe], 64);
  payload.writeInt32LE(start, 66);
  payload.writeInt32LE(end, 70);
  return payload;
}

/** Decode 48-byte candle records: i32 time, 4 x f64 OHLC, i64 tickVolume, i32 spread. */
export function parseCandles(body, symbol, timeframe) {
  if (body.length % CANDLE_RECORD_SIZE !== 0) {
    throw new Error(
      `Rates response length ${body.length} is not divisible by ${CANDLE_RECORD_SIZE}`,
    );
  }
  const candles = [];
  for (let offset = 0; offset < body.length; offset += CANDLE_RECORD_SIZE) {
    candles.push(
      new Candle(
        symbol,
        timeframe,
        body.readInt32LE(offset),
        body.readDoubleLE(offset + 4),
        body.readDoubleLE(offset + 12),
        body.readDoubleLE(offset + 20),
        body.readDoubleLE(offset + 28),
        Number(body.readBigInt64LE(offset + 36)),
        body.readInt32LE(offset + 44),
      ),
    );
  }
  return candles;
}
