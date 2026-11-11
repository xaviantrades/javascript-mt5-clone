import assert from "node:assert/strict";
import { test } from "node:test";

import { BrokerSymbol } from "../src/models.js";
import {
  CANDLE_RECORD_SIZE, QUOTE_RECORD_SIZE, aesDecrypt, aesEncrypt, buildCommand,
  buildRatesPayload, packWire, parseCandles, parseQuotes, parseResponse,
} from "../src/protocol.js";

test("aes round trip", () => {
  const key = Buffer.from(Array.from({ length: 32 }, (_, i) => i));
  const payload = Buffer.from("market-data-protocol-test");
  assert.deepEqual(aesDecrypt(key, aesEncrypt(key, payload)), payload);
});

test("aes rejects bad ciphertext", () => {
  const key = Buffer.alloc(32, 7);
  assert.throws(() => aesDecrypt(key, Buffer.alloc(0)));
  assert.throws(() => aesDecrypt(key, Buffer.alloc(15)));
  assert.throws(() => aesDecrypt(key, Buffer.alloc(16, 1)), /Invalid AES padding/);
});

test("command header", () => {
  const command = buildCommand(11, Buffer.from("abc"));
  assert.equal(command.readUInt16LE(2), 11);
  assert.equal(command.subarray(4).toString(), "abc");
});

test("wire framing", () => {
  const framed = packWire(Buffer.alloc(32, 9));
  assert.equal(framed.readUInt32LE(0), 32);
  assert.equal(framed.readUInt32LE(4), 1);
  assert.equal(framed.length, 40);
});

test("response parser", () => {
  const raw = Buffer.concat([Buffer.from([11, 0, 11, 0, 0]), Buffer.from("rates")]);
  const parsed = parseResponse(raw);
  assert.equal(parsed.cmdId, 11);
  assert.equal(parsed.resCode, 0);
  assert.equal(parsed.resBody.toString(), "rates");
});

test("rates payload layout", () => {
  const payload = buildRatesPayload("EURUSDm", "M1", 100, 200);
  assert.equal(payload.length, 74);
  assert.equal(payload.readUInt16LE(64), 1);
  assert.equal(payload.readInt32LE(66), 100);
  assert.equal(payload.readInt32LE(70), 200);
  assert.equal(payload.subarray(0, 14).toString("utf16le"), "EURUSDm");
  assert.throws(() => buildRatesPayload("EURUSDm", "X9", 1, 2), /Unsupported timeframe/);
});

test("parse candle record", () => {
  const timestamp = 1_700_000_000;
  const body = Buffer.alloc(CANDLE_RECORD_SIZE);
  body.writeInt32LE(timestamp, 0);
  [1.1, 1.2, 1.0, 1.15].forEach((v, i) => body.writeDoubleLE(v, 4 + i * 8));
  body.writeBigInt64LE(123n, 36);
  body.writeInt32LE(8, 44);
  const candle = parseCandles(body, "EURUSDm", "M1")[0];
  assert.equal(candle.timestamp, timestamp);
  assert.equal(candle.close, 1.15);
  assert.equal(candle.tickVolume, 123);
  assert.equal(candle.spread, 8);
  assert.equal(candle.time.toISOString(), "2023-11-14T22:13:20.000Z");
});

test("parse quote record", () => {
  const symbol = new BrokerSymbol("EURUSDm", "Euro vs US Dollar", 5, 42);
  const body = Buffer.alloc(QUOTE_RECORD_SIZE);
  body.writeUInt32LE(42, 0);
  body.writeInt32LE(1_700_000_000, 4);
  body.writeDoubleLE(110000, 12);
  body.writeDoubleLE(110010, 20);
  body.writeDoubleLE(110005, 28);
  body.writeBigInt64LE(9n, 36);
  body.writeUInt32LE(250, 44);
  body.writeUInt16LE(3, 48);
  const quote = parseQuotes(body, new Map([[42, symbol]]))[0];
  assert.equal(quote.bid, 1.1);
  assert.equal(quote.ask, 1.1001);
  assert.equal(quote.timestamp.getTime(), 1_700_000_000_250);
  assert.equal(quote.flags, 3);
});
