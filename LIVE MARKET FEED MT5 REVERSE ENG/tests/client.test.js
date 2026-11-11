/**
 * End-to-end test: MarketDataClient against an in-process mock MT5 server that
 * speaks the same wire protocol (auth -> login -> symbols -> subscribe -> quotes/rates).
 * No broker, credentials or internet access needed.
 */
import assert from "node:assert/strict";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { test } from "node:test";
import { deflateSync } from "node:zlib";

import { WebSocketServer } from "ws";

import { MarketDataClient } from "../src/client.js";
import { STATIC_KEY, aesDecrypt, aesEncrypt, packWire } from "../src/protocol.js";
import { silentLogger } from "../src/util.js";

const SESSION_KEY = Buffer.alloc(32, 0x5a);
const SYMBOL_ID = 42;

function response(cmdId, resCode = 0, body = Buffer.alloc(0)) {
  const header = Buffer.alloc(5);
  header.writeUInt16LE(7, 0);
  header.writeUInt16LE(cmdId, 2);
  header[4] = resCode;
  return Buffer.concat([header, body]);
}

function symbolCatalogue() {
  const record = Buffer.alloc(526);
  Buffer.from("EURUSDm", "utf16le").copy(record, 0);
  Buffer.from("Euro vs US Dollar", "utf16le").copy(record, 64);
  record.writeUInt32LE(5, 192);
  record.writeUInt32LE(SYMBOL_ID, 196);
  Buffer.from("Forex\\EURUSDm", "utf16le").copy(record, 200);
  const count = Buffer.alloc(4);
  count.writeUInt32LE(1, 0);
  return Buffer.concat([Buffer.alloc(4), deflateSync(Buffer.concat([count, record]))]);
}

function quoteRecord() {
  const body = Buffer.alloc(50);
  body.writeUInt32LE(SYMBOL_ID, 0);
  body.writeInt32LE(1_700_000_000, 4);
  body.writeDoubleLE(110000, 12);
  body.writeDoubleLE(110010, 20);
  body.writeDoubleLE(110005, 28);
  body.writeBigInt64LE(9n, 36);
  body.writeUInt32LE(250, 44);
  return body;
}

function candlesFor(start, end) {
  const records = [];
  for (let ts = Math.ceil(start / 60) * 60; ts <= end && records.length < 5; ts += 60) {
    const rec = Buffer.alloc(48);
    rec.writeInt32LE(ts, 0);
    [1.1, 1.2, 1.0, 1.15].forEach((v, i) => rec.writeDoubleLE(v, 4 + i * 8));
    rec.writeBigInt64LE(100n, 36);
    rec.writeInt32LE(2, 44);
    records.push(rec);
  }
  return Buffer.concat(records);
}

function startMockServer(onLogin) {
  const state = { connections: 0, subscribed: [], heartbeats: 0, loginSeen: null };
  const wss = new WebSocketServer({ port: 0, path: "/terminal" });

  wss.on("connection", (ws) => {
    state.connections += 1;
    let authed = false;
    const send = (key, cmdId, resCode, body) =>
      ws.send(packWire(aesEncrypt(key, response(cmdId, resCode, body))));

    ws.on("message", (raw) => {
      const len = raw.readUInt32LE(0);
      const key = authed ? SESSION_KEY : STATIC_KEY;
      const plain = aesDecrypt(key, raw.subarray(8, 8 + len));
      const cmd = plain.readUInt16LE(2);
      const payload = plain.subarray(4);

      if (cmd === 0) {
        authed = true;
        send(STATIC_KEY, 0, 0, Buffer.concat([Buffer.alloc(66), SESSION_KEY]));
      } else if (cmd === 28) {
        state.loginSeen = {
          password: payload.subarray(4, 4 + 18).toString("utf16le"),
          serverLen: payload.readUInt32LE(476),
          server: payload.subarray(480, 480 + payload.readUInt32LE(476) * 2).toString("utf16le"),
          login: Number(payload.readBigUInt64LE(736)),
        };
        send(SESSION_KEY, 28, 0);
        onLogin?.(ws, state);
      } else if (cmd === 34) {
        send(SESSION_KEY, 34, 0, symbolCatalogue());
      } else if (cmd === 7) {
        const n = payload.readUInt32LE(0);
        state.subscribed = Array.from({ length: n }, (_, i) => payload.readUInt32LE(4 + i * 4));
        send(SESSION_KEY, 8, 0, quoteRecord());
      } else if (cmd === 11) {
        const start = payload.readInt32LE(66);
        const end = payload.readInt32LE(70);
        send(SESSION_KEY, 11, 0, candlesFor(start, end));
      } else if (cmd === 51) {
        state.heartbeats += 1;
      }
    });
  });

  return new Promise((resolve) => wss.on("listening", () => resolve({ wss, state })));
}

const until = async (predicate, timeoutMs, label) => {
  const deadline = Date.now() + timeoutMs;
  while (!predicate()) {
    if (Date.now() > deadline) assert.fail(`Timed out waiting for ${label}`);
    await new Promise((r) => setTimeout(r, 25));
  }
};

function makeClient(port, dir) {
  return new MarketDataClient({
    login: 12345678,
    password: "secret-pw",
    server: "Mock-Server",
    symbols: ["EURUSDm"],
    timeframes: ["M1"],
    database: join(dir, "feed.sqlite3"),
    initialBars: 10,
    backfillInterval: 2,
    secure: false,
    endpoints: [["127.0.0.1", port]],
    logger: silentLogger,
  });
}

test("streams quotes, backfills candles into SQLite, and sends a correct login", async () => {
  const dir = mkdtempSync(join(tmpdir(), "mt5-e2e-"));
  const { wss, state } = await startMockServer();
  const client = makeClient(wss.address().port, dir);
  const quotes = [];
  const candles = [];
  client.onQuote = (q) => quotes.push(q);
  client.onCandle = async (c) => candles.push(c);

  const running = client.runForever();
  try {
    await until(() => quotes.length > 0 && candles.length > 0, 8000, "quote and candle");

    assert.deepEqual(state.loginSeen, {
      password: "secret-pw", serverLen: 11, server: "Mock-Server", login: 12345678,
    });
    assert.deepEqual(state.subscribed, [SYMBOL_ID]);

    assert.equal(quotes[0].symbol, "EURUSDm");
    assert.equal(quotes[0].bid, 1.1);
    assert.equal(quotes[0].ask, 1.1001);
    assert.equal(client.quotes.get("EURUSDm").bid, 1.1);

    assert.equal(candles[0].symbol, "EURUSDm");
    assert.equal(candles[0].close, 1.15);
    const stored = client.getHistory("EURUSDm", "M1", { limit: 100 });
    assert.ok(stored.length > 0);
    assert.ok(stored.every((c, i) => i === 0 || c.timestamp > stored[i - 1].timestamp));

    await until(() => state.heartbeats >= 1, 6000, "heartbeat");
  } finally {
    await client.stop();
    await running;
    client.closeStore();
    wss.close();
    rmSync(dir, { recursive: true, force: true });
  }
});

test("reconnects after the server drops the connection", async () => {
  const dir = mkdtempSync(join(tmpdir(), "mt5-e2e-"));
  let dropped = false;
  const { wss, state } = await startMockServer((ws) => {
    if (!dropped) {
      dropped = true;
      setTimeout(() => ws.terminate(), 300);
    }
  });
  const client = makeClient(wss.address().port, dir);
  const quotes = [];
  client.onQuote = (q) => quotes.push(q);

  const running = client.runForever();
  try {
    await until(() => state.connections >= 2 && quotes.length >= 2, 12000, "reconnect");
    assert.ok(state.connections >= 2);
  } finally {
    await client.stop();
    await running;
    client.closeStore();
    wss.close();
    rmSync(dir, { recursive: true, force: true });
  }
});

test("rejects unknown timeframes and empty symbol lists", () => {
  assert.throws(
    () => new MarketDataClient({ login: 1, password: "x", server: "s", symbols: ["A"], timeframes: ["M7"], database: ":memory:" }),
    /Unsupported timeframes/,
  );
  assert.throws(
    () => new MarketDataClient({ login: 1, password: "x", server: "s", symbols: [], database: ":memory:" }),
    /At least one broker symbol/,
  );
});
