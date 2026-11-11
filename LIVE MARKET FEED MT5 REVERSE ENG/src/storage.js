/** Small durable SQLite candle store; timestamps are UTC Unix seconds. */
import { DatabaseSync } from "node:sqlite";

import { Candle } from "./models.js";

export class CandleStore {
  constructor(path = "market_data.sqlite3") {
    this.path = path;
    this.db = new DatabaseSync(path);
    this.db.exec("PRAGMA journal_mode=WAL");
    this.db.exec(`
      CREATE TABLE IF NOT EXISTS candles (
        symbol TEXT NOT NULL,
        timeframe TEXT NOT NULL,
        timestamp INTEGER NOT NULL,
        open REAL NOT NULL,
        high REAL NOT NULL,
        low REAL NOT NULL,
        close REAL NOT NULL,
        tick_volume INTEGER NOT NULL,
        spread INTEGER NOT NULL,
        PRIMARY KEY (symbol, timeframe, timestamp)
      )
    `);
    this.db.exec(`
      CREATE INDEX IF NOT EXISTS idx_candles_lookup
      ON candles(symbol, timeframe, timestamp)
    `);

    this._latest = this.db.prepare(
      "SELECT MAX(timestamp) AS ts FROM candles WHERE symbol=? AND timeframe=?",
    );
    this._upsert = this.db.prepare(`
      INSERT INTO candles
      (symbol,timeframe,timestamp,open,high,low,close,tick_volume,spread)
      VALUES (?,?,?,?,?,?,?,?,?)
      ON CONFLICT(symbol,timeframe,timestamp) DO UPDATE SET
        open=excluded.open, high=excluded.high, low=excluded.low,
        close=excluded.close, tick_volume=excluded.tick_volume,
        spread=excluded.spread
    `);
  }

  /** @returns {number | null} newest stored candle timestamp, or null if none */
  latestTimestamp(symbol, timeframe) {
    const row = this._latest.get(symbol, timeframe);
    return row && row.ts !== null && row.ts !== undefined ? Number(row.ts) : null;
  }

  /** Insert or update candles in one transaction. Returns the number of rows written. */
  upsert(candles) {
    if (!candles.length) return 0;
    let written = 0;
    this.db.exec("BEGIN");
    try {
      for (const c of candles) {
        const info = this._upsert.run(
          c.symbol, c.timeframe, c.timestamp, c.open, c.high, c.low,
          c.close, c.tickVolume, c.spread,
        );
        written += Number(info.changes);
      }
      this.db.exec("COMMIT");
    } catch (err) {
      this.db.exec("ROLLBACK");
      throw err;
    }
    return written;
  }

  /** Newest `limit` candles in ascending time order, optionally bounded by start/end. */
  getCandles(symbol, timeframe, { limit = 500, start = null, end = null } = {}) {
    let sql = `
      SELECT timestamp,open,high,low,close,tick_volume,spread
      FROM candles WHERE symbol=? AND timeframe=?
    `;
    const params = [symbol, timeframe];
    if (start !== null) {
      sql += " AND timestamp>=?";
      params.push(start);
    }
    if (end !== null) {
      sql += " AND timestamp<=?";
      params.push(end);
    }
    sql += " ORDER BY timestamp DESC LIMIT ?";
    params.push(limit);

    const rows = this.db.prepare(sql).all(...params);
    rows.reverse();
    return rows.map(
      (r) =>
        new Candle(
          symbol, timeframe, Number(r.timestamp), r.open, r.high, r.low,
          r.close, Number(r.tick_volume), Number(r.spread),
        ),
    );
  }

  close() {
    this.db.close();
  }
}
