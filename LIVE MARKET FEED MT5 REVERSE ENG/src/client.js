/** Market-data-only MT5 client with reconnect and continuous candle synchronization. */
import { findServerEndpoints } from "./broker.js";
import { MT5Connection } from "./connection.js";
import {
  TIMEFRAME_SECONDS, buildRatesPayload, parseCandles, parseQuotes, parseSymbols,
} from "./protocol.js";
import { CandleStore } from "./storage.js";
import {
  AsyncEvent, ConnectionError, MT5TimeoutError, Mutex, PermissionError,
  consoleLogger, waitAny, yieldToEventLoop,
} from "./util.js";

/**
 * Connect to an MT5 web-terminal server and stream only market data.
 * No account, order, position, or trade methods are implemented.
 */
export class MarketDataClient {
  /**
   * @param {object} options
   * @param {number|string} options.login
   * @param {string} options.password
   * @param {string} options.server        exact broker server name
   * @param {string[]} options.symbols     exact broker symbol names
   * @param {string[]} [options.timeframes=["M1"]]
   * @param {string} [options.database="market_data.sqlite3"]
   * @param {number} [options.initialBars=1000]
   * @param {number} [options.backfillInterval=15]  seconds between history passes (min 2)
   * @param {number} [options.pageBars=1000]
   * @param {boolean} [options.verifyTls=false]
   * @param {object} [options.logger]  {debug, info, warn, error}; defaults to console
   * @param {Array<[string, number]>} [options.endpoints]  skip broker discovery and
   *   connect straight to these [host, port] pairs (advanced / testing)
   * @param {boolean} [options.secure=true]  false uses ws:// instead of wss:// (testing)
   */
  constructor({
    login,
    password,
    server,
    symbols,
    timeframes,
    database = "market_data.sqlite3",
    initialBars = 1000,
    backfillInterval = 15,
    pageBars = 1000,
    verifyTls = false,
    logger = consoleLogger,
    endpoints = null,
    secure = true,
  }) {
    if (!symbols || symbols.length === 0) {
      throw new Error("At least one broker symbol is required");
    }
    this.login = Number(login);
    this.password = password;
    this.server = server;
    this.requestedSymbols = [...new Set(symbols)];
    this.timeframes = [...new Set(timeframes && timeframes.length ? timeframes : ["M1"])];
    const unknown = this.timeframes.filter((tf) => !(tf in TIMEFRAME_SECONDS));
    if (unknown.length) {
      throw new Error(`Unsupported timeframes: ${JSON.stringify(unknown.sort())}`);
    }
    this.initialBars = Math.max(1, initialBars);
    this.backfillInterval = Math.max(2, backfillInterval);
    this.pageBars = Math.max(100, pageBars);
    this.verifyTls = verifyTls;
    this.log = logger;
    this.endpoints = endpoints;
    this.secure = secure;

    this.store = new CandleStore(database);
    this.connection = null;
    /** @type {Map<string, import("./models.js").BrokerSymbol>} */
    this.symbols = new Map();
    this.symbolsById = new Map();
    /** @type {Map<string, import("./models.js").Quote>} latest quote per symbol */
    this.quotes = new Map();

    /** Set to `(quote) => ...` (sync or async). */
    this.onQuote = null;
    /** Set to `(candle) => ...` (sync or async). */
    this.onCandle = null;

    this._pending = new Map();
    this._commandLocks = new Map();
    this._session = null;
    this._receiverTask = null;
    this._heartbeatTask = null;
    this._historyTask = null;
    this._connectionLost = new AsyncEvent();
    this._stop = new AsyncEvent();
  }

  async _callback(callback, value) {
    if (!callback) return;
    try {
      await callback(value);
    } catch (err) {
      this.log.error("Market-data callback failed", err);
    }
  }

  _commandLock(commandId) {
    let lock = this._commandLocks.get(commandId);
    if (!lock) {
      lock = new Mutex();
      this._commandLocks.set(commandId, lock);
    }
    return lock;
  }

  /** Send a command and wait for the response carrying the same command id. */
  _request(commandId, payload = Buffer.alloc(0), timeoutMs = 15_000) {
    return this._commandLock(commandId).run(async () => {
      let entry;
      const responsePromise = new Promise((resolve, reject) => {
        entry = { resolve, reject };
      });
      responsePromise.catch(() => {}); // avoid unhandled rejection if we time out first
      this._pending.set(commandId, entry);

      let timer;
      try {
        await this.connection.send(commandId, payload);
        const timeout = new Promise((_, reject) => {
          timer = setTimeout(
            () => reject(new MT5TimeoutError(`MT5 command ${commandId} timed out`)),
            timeoutMs,
          );
        });
        const response = await Promise.race([responsePromise, timeout]);
        if (response.resCode !== 0) {
          throw new Error(`MT5 command ${commandId} failed with code ${response.resCode}`);
        }
        return response;
      } finally {
        clearTimeout(timer);
        if (this._pending.get(commandId) === entry) this._pending.delete(commandId);
      }
    });
  }

  async _authenticate() {
    await this.connection.sendAuth();
    const response = await this.connection.receiveAuth();
    if (response.cmdId !== 0 || response.resCode !== 0) {
      throw new PermissionError("MT5 WebSocket session authentication failed");
    }
    if (response.resBody.length < 98) {
      throw new ConnectionError("Authentication response did not contain a session key");
    }
    this.connection.sessionKey = Buffer.from(response.resBody.subarray(66));
  }

  async _login() {
    const payload = Buffer.alloc(912);
    const password = Buffer.from(this.password, "utf16le");
    if (password.length > 64) {
      throw new Error("MT5 password exceeds the researched 64-byte field");
    }
    password.copy(payload, 4);
    const serverBytes = Buffer.from(this.server, "utf16le");
    if (serverBytes.length > 256) {
      throw new Error("Broker server name exceeds the researched 256-byte field");
    }
    payload.writeUInt32LE(serverBytes.length / 2, 476);
    serverBytes.copy(payload, 480);
    payload.writeBigUInt64LE(BigInt(this.login), 736);

    await this.connection.send(28, payload);
    const response = await this.connection.receive(15_000);
    if (response.cmdId !== 28 || response.resCode !== 0) {
      throw new PermissionError("MT5 account login failed");
    }
  }

  /** Resolve the broker, authenticate, load symbols, and subscribe. */
  async connect() {
    const endpoints = this.endpoints ?? (await findServerEndpoints(this.server));
    const failures = [];
    for (const [host, port] of endpoints) {
      const connection = new MT5Connection(host, port, this.verifyTls, {
        secure: this.secure,
      });
      const session = { closed: new AsyncEvent() };
      this._session = session;
      try {
        this.connection = connection;
        if (this.secure && !this.verifyTls) {
          this.log.warn(
            "TLS certificate verification is disabled (verifyTls=false); " +
              "the connection is open to man-in-the-middle attacks.",
          );
        }
        await connection.connect();
        await this._authenticate();
        await this._login();
        this._connectionLost.clear();
        this._receiverTask = this._receiveLoop(session, connection);
        this._heartbeatTask = this._heartbeatLoop(session, connection);

        const response = await this._request(34, Buffer.alloc(0), 25_000);
        this.symbols = parseSymbols(response.resBody);
        this.symbolsById = new Map([...this.symbols.values()].map((s) => [s.symbolId, s]));

        const missing = this.requestedSymbols.filter((name) => !this.symbols.has(name));
        if (missing.length) {
          throw new Error("Broker does not expose requested symbol(s): " + missing.join(", "));
        }

        const ids = this.requestedSymbols.map((name) => this.symbols.get(name).symbolId);
        const payload = Buffer.alloc(4 + ids.length * 4);
        payload.writeUInt32LE(ids.length, 0);
        ids.forEach((id, i) => payload.writeUInt32LE(id, 4 + i * 4));
        await connection.send(7, payload);

        this.log.info(
          `Connected to ${host}:${port}; subscribed to ${this.requestedSymbols.join(", ")}`,
        );
        return;
      } catch (err) {
        failures.push(`${host}:${port}: ${err.message}`);
        await this._disconnect();
      }
    }
    throw new ConnectionError("All broker endpoints failed: " + failures.join(" | "));
  }

  async _receiveLoop(session, connection) {
    try {
      while (!this._stop.isSet() && !session.closed.isSet()) {
        let message;
        try {
          message = await connection.receive(30_000);
        } catch (err) {
          if (err instanceof MT5TimeoutError) continue;
          throw err;
        }
        const commandId = message.cmdId;
        if (commandId === 8) {
          await this._handleQuotes(message.resBody);
        } else if (commandId === 17) {
          // Symbol specification updates are not needed for OHLC history.
          continue;
        } else if (this._pending.has(commandId)) {
          this._pending.get(commandId).resolve(message);
        }
      }
    } catch (err) {
      if (session.closed.isSet()) return; // expected: we are disconnecting
      this.log.error("MT5 receive loop stopped", err);
      this._connectionLost.set();
    }
  }

  async _handleQuotes(body) {
    for (const quote of parseQuotes(body, this.symbolsById)) {
      this.quotes.set(quote.symbol, quote);
      await this._callback(this.onQuote, quote);
    }
  }

  async _heartbeatLoop(session, connection) {
    try {
      while (!this._stop.isSet() && !session.closed.isSet()) {
        if (await session.closed.wait(3_000)) return;
        await connection.send(51);
      }
    } catch (err) {
      if (session.closed.isSet()) return;
      this.log.error("MT5 heartbeat failed", err);
      this._connectionLost.set();
    }
  }

  /**
   * Request a broker candle range. Times are UTC Unix seconds.
   * @returns {Promise<import("./models.js").Candle[]>}
   */
  async getRates(symbol, timeframe, start, end) {
    if (!this.symbols.has(symbol)) {
      throw new Error(`Unknown broker symbol: ${symbol}`);
    }
    const response = await this._request(
      11, buildRatesPayload(symbol, timeframe, start, end), 30_000,
    );
    return parseCandles(response.resBody, symbol, timeframe);
  }

  async _syncHistory(symbol, timeframe) {
    const period = TIMEFRAME_SECONDS[timeframe];
    const latest = this.store.latestTimestamp(symbol, timeframe);
    const now = Math.floor(Date.now() / 1000);
    let cursor =
      latest === null
        ? Math.max(0, now - this.initialBars * period)
        : Math.max(0, latest - period);

    // Request bounded ranges. Re-query one overlapping candle so the current
    // unfinished bar is refreshed and any just-completed bar is not skipped.
    while (cursor < now && !this._stop.isSet() && !this._connectionLost.isSet()) {
      const end = Math.min(now, cursor + this.pageBars * period);
      const candles = await this.getRates(symbol, timeframe, cursor, end);
      if (candles.length) {
        this.store.upsert(candles);
        for (const candle of candles) {
          await this._callback(this.onCandle, candle);
        }
      }
      cursor = end;
      await yieldToEventLoop();
      if (end >= now) break;
    }
  }

  async _historyLoop(session) {
    const stopped = () =>
      this._stop.isSet() || this._connectionLost.isSet() || session.closed.isSet();
    while (!stopped()) {
      const started = Date.now();
      for (const symbol of this.requestedSymbols) {
        for (const timeframe of this.timeframes) {
          if (stopped()) return;
          try {
            await this._syncHistory(symbol, timeframe);
          } catch (err) {
            if (session.closed.isSet()) return;
            this.log.error(`History sync failed for ${symbol} ${timeframe}`, err);
          }
        }
      }
      const elapsed = Date.now() - started;
      const pause = Math.max(1_000, this.backfillInterval * 1_000 - elapsed);
      if (await waitAny([session.closed, this._stop, this._connectionLost], pause)) return;
    }
  }

  /** Newest `limit` locally stored candles, oldest first. */
  getHistory(symbol, timeframe, { limit = 500, start = null, end = null } = {}) {
    return this.store.getCandles(symbol, timeframe, { limit, start, end });
  }

  async _disconnect() {
    if (this._session) this._session.closed.set();
    const tasks = [this._historyTask, this._heartbeatTask, this._receiverTask];
    this._historyTask = this._heartbeatTask = this._receiverTask = null;

    const connection = this.connection;
    this.connection = null;
    if (connection) {
      try {
        await connection.close();
      } catch {
        /* ignore */
      }
    }
    for (const entry of this._pending.values()) {
      entry.reject(new ConnectionError("Disconnected"));
    }
    this._pending.clear();
    await Promise.allSettled(tasks.filter(Boolean));
  }

  /** Keep the feed alive. Reconnects, resubscribes and resumes candle sync. */
  async runForever() {
    let delay = 1_000;
    try {
      while (!this._stop.isSet()) {
        try {
          await this.connect();
          delay = 1_000;
          this._historyTask = this._historyLoop(this._session);
          await waitAny([this._connectionLost, this._stop]);
        } catch (err) {
          this.log.error("MT5 connection attempt failed:", err.message);
        } finally {
          await this._disconnect();
        }
        if (!this._stop.isSet()) {
          this.log.warn(
            `Reconnecting to ${this.server} in ${(delay / 1000).toFixed(1)} seconds`,
          );
          await this._stop.wait(delay);
          delay = Math.min(delay * 2, 30_000);
        }
      }
    } finally {
      await this._disconnect();
    }
  }

  /** Ask runForever() to finish. Safe to call from a signal handler. */
  async stop() {
    this._stop.set();
    this._connectionLost.set();
    if (this.connection) await this.connection.close();
  }

  closeStore() {
    this.store.close();
  }
}
