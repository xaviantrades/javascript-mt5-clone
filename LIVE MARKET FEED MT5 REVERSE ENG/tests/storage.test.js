import assert from "node:assert/strict";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { test } from "node:test";

import { Candle } from "../src/models.js";
import { CandleStore } from "../src/storage.js";

test("store upserts and orders", () => {
  const dir = mkdtempSync(join(tmpdir(), "mt5-"));
  const store = new CandleStore(join(dir, "history.sqlite3"));
  try {
    const first = new Candle("EURUSDm", "M1", 60, 1, 2, 0.5, 1.5, 10, 1);
    const updated = new Candle("EURUSDm", "M1", 60, 1, 2.1, 0.5, 2, 11, 1);
    const second = new Candle("EURUSDm", "M1", 120, 2, 3, 1, 2.5, 12, 1);
    store.upsert([first, updated, second]);

    const rows = store.getCandles("EURUSDm", "M1", { limit: 10 });
    assert.equal(rows.length, 2);
    assert.equal(rows[0].high, 2.1);
    assert.equal(rows[0].close, 2);
    assert.equal(store.latestTimestamp("EURUSDm", "M1"), 120);
    assert.equal(store.latestTimestamp("EURUSDm", "H1"), null);

    const bounded = store.getCandles("EURUSDm", "M1", { start: 100 });
    assert.deepEqual(bounded.map((c) => c.timestamp), [120]);
  } finally {
    store.close();
    rmSync(dir, { recursive: true, force: true });
  }
});
