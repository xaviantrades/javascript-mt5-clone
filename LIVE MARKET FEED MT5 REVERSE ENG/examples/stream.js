/** Example: print live quotes while candles are continuously stored. */
import { MarketDataClient } from "../src/index.js";

function requireEnv(name) {
  const value = process.env[name];
  if (!value) {
    console.error(`Missing environment variable ${name}. See README.md / .env.example.`);
    process.exit(1);
  }
  return value;
}

const client = new MarketDataClient({
  login: Number(requireEnv("MT5_LOGIN")),
  password: requireEnv("MT5_PASSWORD"),
  server: requireEnv("MT5_SERVER"),
  symbols: (process.env.MT5_SYMBOLS ?? "EURUSDm").split(",").map((s) => s.trim()),
  timeframes: (process.env.MT5_TIMEFRAMES ?? "M1,M5,H1").split(",").map((s) => s.trim()),
  database: process.env.MT5_DATABASE ?? "market_data.sqlite3",
  initialBars: 2000,
  backfillInterval: 15,
});

client.onQuote = (q) =>
  console.log(`TICK ${q.symbol} bid=${q.bid} ask=${q.ask} time=${q.timestamp.toISOString()}`);

client.onCandle = (c) =>
  console.log(
    `BAR ${c.symbol} ${c.timeframe} ${c.timestamp} ` +
      `O=${c.open} H=${c.high} L=${c.low} C=${c.close}`,
  );

// Ctrl+C: stop cleanly so the socket closes and the database is flushed.
let stopping = false;
for (const signal of ["SIGINT", "SIGTERM"]) {
  process.on(signal, () => {
    if (stopping) process.exit(1);
    stopping = true;
    console.log("\nStopping...");
    client.stop();
  });
}

try {
  await client.runForever();
} finally {
  client.closeStore();
}
