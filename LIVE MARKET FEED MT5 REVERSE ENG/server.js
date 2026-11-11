/** Bridge between the MT5 feed and chart.html. Run: node --env-file=.env server.js */
import { createServer } from "node:http";
import { readFile } from "node:fs/promises";
import { fileURLToPath } from "node:url";
import { WebSocketServer } from "ws";

import { MarketDataClient, TIMEFRAME_SECONDS } from "./src/index.js";

const need = (name) => {
  if (!process.env[name]) {
    console.error(`Missing environment variable ${name}. See .env.example.`);
    process.exit(1);
  }
  return process.env[name];
};
const list = (v, d) => (v ?? d).split(",").map((s) => s.trim()).filter(Boolean);
const PORT = Number(process.env.PORT ?? 3000);

const client = new MarketDataClient({
  login: Number(need("MT5_LOGIN")),
  password: need("MT5_PASSWORD"),
  server: need("MT5_SERVER"),
  symbols: list(process.env.MT5_SYMBOLS, "EURUSDm"),
  timeframes: list(process.env.MT5_TIMEFRAMES, "M1,M5,H1"),
  database: process.env.MT5_DATABASE ?? "market_data.sqlite3",
  initialBars: 2000,
  backfillInterval: 15,
});

const server = createServer(handle);
const wss = new WebSocketServer({ server, path: "/live" });
const broadcast = (msg) => {
  const text = JSON.stringify(msg);
  for (const ws of wss.clients) if (ws.readyState === 1) ws.send(text);
};
const connected = () => Boolean(client.connection) && client.symbols.size > 0;

client.onQuote = (q) =>
  broadcast({ t: "tick", s: q.symbol, bid: q.bid, ask: q.ask, time: q.timestamp.getTime() });
client.onCandle = (c) => broadcast({ t: "candle", s: c.symbol, tf: c.timeframe, c });
wss.on("connection", (ws) => ws.send(JSON.stringify({ t: "status", connected: connected() })));
const statusTimer = setInterval(() => broadcast({ t: "status", connected: connected() }), 3000);
statusTimer.unref();

const busy = new Set();

/** Walk backwards from the oldest stored candle, saving broker history until `bars` older candles are added. */
async function backfill(symbol, tf, bars) {
  const period = TIMEFRAME_SECONDS[tf];
  const row = client.store.db
    .prepare("SELECT MIN(timestamp) AS ts FROM candles WHERE symbol=? AND timeframe=?")
    .get(symbol, tf);
  let end = row?.ts != null ? Number(row.ts) : Math.floor(Date.now() / 1000);
  let added = 0;
  let empty = 0;
  while (added < bars && empty < 10 && end > 0) {
    const start = Math.max(0, end - client.pageBars * period);
    const rows = await client.getRates(symbol, tf, start, end);
    const fresh = rows.filter((c) => c.timestamp < end).length;
    if (rows.length) client.store.upsert(rows);
    added += fresh;
    empty = fresh ? 0 : empty + 1; // weekends/closures return nothing; stop after 10 empty pages
    end = start;
    broadcast({ t: "backfill", symbol, tf, added, oldest: end });
  }
  return { added, oldest: end };
}

const json = (res, code, body) => {
  res.writeHead(code, { "Content-Type": "application/json" });
  res.end(JSON.stringify(body));
};

async function handle(req, res) {
  const u = new URL(req.url, "http://localhost");
  const symbol = u.searchParams.get("symbol");
  const tf = u.searchParams.get("tf");
  const valid = client.requestedSymbols.includes(symbol) && client.timeframes.includes(tf);
  try {
    if (u.pathname === "/") {
      res.writeHead(200, { "Content-Type": "text/html; charset=utf-8" });
      return res.end(await readFile(fileURLToPath(new URL("./chart.html", import.meta.url))));
    }
    if (u.pathname === "/api/config") {
      const digits = {};
      for (const n of client.requestedSymbols) if (client.symbols.has(n)) digits[n] = client.symbols.get(n).digits;
      return json(res, 200, { symbols: client.requestedSymbols, timeframes: client.timeframes, digits });
    }
    if (u.pathname === "/api/history") {
      if (!valid) return json(res, 400, { error: "Unknown symbol or timeframe" });
      const limit = Math.min(5000, Math.max(1, Number(u.searchParams.get("limit") ?? 1500)));
      const end = u.searchParams.has("end") ? Number(u.searchParams.get("end")) : null;
      return json(res, 200, client.getHistory(symbol, tf, { limit, end }));
    }
    if (u.pathname === "/api/backfill" && req.method === "POST") {
      if (!valid) return json(res, 400, { error: "Unknown symbol or timeframe" });
      if (!connected()) return json(res, 503, { error: "Not connected to the broker yet" });
      const key = `${symbol}|${tf}`;
      if (busy.has(key)) return json(res, 409, { error: "Backfill already running for this chart" });
      busy.add(key);
      try {
        const bars = Math.min(200000, Math.max(100, Number(u.searchParams.get("bars") ?? 5000)));
        return json(res, 200, await backfill(symbol, tf, bars));
      } finally {
        busy.delete(key);
      }
    }
    json(res, 404, { error: "Not found" });
  } catch (err) {
    json(res, 500, { error: err.message });
  }
}

server.listen(PORT, "127.0.0.1", () => console.log(`Chart platform: http://localhost:${PORT}`));

for (const sig of ["SIGINT", "SIGTERM"]) process.on(sig, () => client.stop());
await client.runForever();
client.closeStore();
for (const ws of wss.clients) ws.terminate();
server.close();
server.closeAllConnections?.();
