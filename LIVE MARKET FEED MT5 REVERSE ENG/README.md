# MT5 Market Data Client (JavaScript / Node.js)

A JavaScript port of the Python `mt5-market-data` client. It does only two jobs:

1. Keep an authenticated MT5 WebSocket session open and publish live quotes for the symbols you ask for.
2. Continuously backfill and refresh historical OHLC candles, storing them in SQLite.

It intentionally contains **no** order placement, trade execution, account balance, position, or pending-order API.

## Requirements

- **Node.js 22.13 or newer** (check with `node --version`). SQLite comes built into Node (`node:sqlite`), so the only npm dependency is `ws`.
- An MT5 account (use a **demo account** first): login number, password and the exact broker server name.

## Install

```bash
cd mt5-market-data-js
npm install
```

## Run the example

The example (`examples/stream.js`) prints live ticks and candles. Credentials come from environment variables, never from source files.

**Option A: `.env` file (easiest, works on every OS)**

```bash
cp .env.example .env      # Windows: copy .env.example .env
# edit .env with your login, password, server, symbols
node --env-file=.env examples/stream.js
```

**Option B: set variables in the shell**

PowerShell:
```powershell
$env:MT5_LOGIN="12345678"
$env:MT5_PASSWORD="your-password"
$env:MT5_SERVER="Broker-MT5Live"
$env:MT5_SYMBOLS="EURUSDm,GBPUSDm"
$env:MT5_TIMEFRAMES="M1,M5,H1"
npm start
```

Linux / macOS:
```bash
export MT5_LOGIN=12345678
export MT5_PASSWORD='your-password'
export MT5_SERVER='Broker-MT5Live'
export MT5_SYMBOLS='EURUSDm,GBPUSDm'
export MT5_TIMEFRAMES='M1,M5,H1'
npm start
```

| Variable | Required | Meaning |
|---|---|---|
| `MT5_LOGIN` | yes | MT5 account number |
| `MT5_PASSWORD` | yes | account password |
| `MT5_SERVER` | yes | exact broker server name, e.g. `Broker-MT5Live` |
| `MT5_SYMBOLS` | no (default `EURUSDm`) | comma-separated broker symbol names |
| `MT5_TIMEFRAMES` | no (default `M1,M5,H1`) | any of `M1 M2 M3 M4 M5 M6 M10 M12 M15 M20 M30 H1 H2 H3 H4 H6 H8 H12 D1 W1 MN1` |
| `MT5_DATABASE` | no (default `market_data.sqlite3`) | SQLite file path |

You should see lines like:

```
TICK EURUSDm bid=1.0842 ask=1.08432 time=2026-11-11T09:30:01.250Z
BAR EURUSDm M1 1762853400 O=1.0841 H=1.0843 L=1.0840 C=1.0842
```

Press **Ctrl+C** to stop. The socket is closed and the database is closed cleanly.

Symbol names are broker-specific. Use the exact name from your broker's symbol list (for example `EURUSDm`, not `EURUSD`).

## Use as a library

```js
import { MarketDataClient } from "./src/index.js";

const feed = new MarketDataClient({
  login: 12345678,
  password: process.env.MT5_PASSWORD,
  server: "Broker-MT5Live",
  symbols: ["EURUSDm", "GBPUSDm"],
  timeframes: ["M1", "M5", "H1"],
  database: "prices.sqlite3",
  initialBars: 2000,
  backfillInterval: 15,
});

feed.onQuote = (q) => console.log(q.symbol, q.bid, q.ask);
feed.onCandle = (c) => console.log(c.symbol, c.timeframe, c.timestamp, c.close);

process.on("SIGINT", () => feed.stop());

try {
  await feed.runForever();   // resolves after stop()
} finally {
  feed.closeStore();
}
```

Callbacks may be `async`. An exception inside a callback is logged and does not stop the feed.

Read locally cached candles (oldest first):

```js
const bars = feed.getHistory("EURUSDm", "M1", { limit: 500 });
// optional bounds, in UTC Unix seconds: { limit, start, end }
console.log(bars.at(-1).close, bars.at(-1).time);   // .time is a Date
```

### Constructor options

| Option | Default | Meaning |
|---|---|---|
| `login`, `password`, `server` | required | account credentials and exact server name |
| `symbols` | required | broker symbol names to subscribe to |
| `timeframes` | `["M1"]` | candle timeframes to keep synchronized |
| `database` | `"market_data.sqlite3"` | SQLite file |
| `initialBars` | `1000` | how many bars to fetch the first time a symbol/timeframe is seen |
| `backfillInterval` | `15` | seconds between history refresh passes (minimum 2) |
| `pageBars` | `1000` | maximum bars per history request (minimum 100) |
| `verifyTls` | `false` | see "TLS" below |
| `logger` | console | any object with `debug/info/warn/error`; pass `silentLogger` to mute |
| `endpoints`, `secure` | none, `true` | advanced: skip broker discovery and connect to `[[host, port]]`, optionally over plain `ws://` (used by the tests) |

### Other methods

- `await feed.getRates(symbol, timeframe, startUnix, endUnix)` requests a candle range directly from the broker.
- `feed.quotes` is a `Map` holding the latest quote per symbol.
- `await feed.stop()` makes `runForever()` finish.

## How it works

1. Resolve the broker server name to WebSocket endpoints using the MetaQuotes SearchMQ service (fallback: the Search endpoint).
2. Open `wss://HOST:PORT/terminal`.
3. Send command 0 encrypted with the protocol's static AES-256-CBC key and read the 32-byte session key from the reply.
4. Send command 28 with the 912-byte login payload.
5. Run a receive loop and a heartbeat loop (command 51 every 3 seconds).
6. Request the symbol catalogue (command 34), map symbol names to internal IDs, subscribe (command 7).
7. Decode pushed command 8 messages (50-byte quote records) and call `onQuote`.
8. Request candles with command 11 (74-byte request, 48-byte records) and upsert them into SQLite keyed by `(symbol, timeframe, timestamp)`.
9. Repeat history sync every `backfillInterval` seconds; each pass starts one candle before the last stored one so the current bar is refreshed.
10. If the socket or heartbeat fails, reconnect with exponential backoff (1 s up to 30 s), log in again, resubscribe and resume history from the last stored candle.

## Important behavior and limitations

- Live data is quote/tick data (bid, ask, last, tick volume, timestamp, flags). It is not a guaranteed lossless tick archive.
- History is broker-provided OHLC. Availability and maximum range are controlled by the broker; this client cannot create history the broker does not provide.
- **TLS:** broker discovery returns IP addresses while the certificates name a hostname, so `verifyTls` defaults to `false`, matching the original. That disables certificate identity checks and carries a man-in-the-middle risk, and the client logs a warning each time it connects this way. Set `verifyTls: true` wherever the endpoint certificate validates.
- This implements a reverse-engineered, undocumented protocol. Broker updates may change behavior. **Test with a demo account** before relying on it.
- Keep your password out of source control. `.env` is already in `.gitignore`.
- Node prints `ExperimentalWarning: SQLite is an experimental feature` on start. That is harmless. To hide it, run with `node --disable-warning=ExperimentalWarning examples/stream.js`.

## Tests

```bash
npm test
```

The suite covers packet layout, AES framing, quote/candle/symbol parsing, the rates request layout, SQLite de-duplication, and an end-to-end run of the real client against an in-process mock MT5 server (login payload, subscription, quotes, history backfill, heartbeat, and reconnect after a dropped connection). It does not replace a live test against a real broker.

## Differences from the Python version

- `snake_case` became `camelCase` (`on_quote` is `onQuote`, `run_forever` is `runForever`, `tick_volume` is `tickVolume`, and so on).
- `getHistory(symbol, timeframe, { limit, start, end })` takes an options object.
- The `Symbol` model is exported as `BrokerSymbol` so it does not clash with JavaScript's built-in `Symbol`.
- AES uses Node's built-in `crypto`, HTTP uses built-in `fetch`, and SQLite uses built-in `node:sqlite`, so `pycryptodome`, `requests` and `sqlite3` are no longer needed. Only `ws` is installed from npm.
- Quote `timestamp` is a JavaScript `Date`; candle `timestamp` is still UTC Unix seconds, with a `.time` getter returning a `Date`.
- The SQLite file format is the same, so a database created by the Python version can be reused.
