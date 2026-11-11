/** Small async helpers that replace asyncio.Event / asyncio.Lock / wait_for. */

export class MT5TimeoutError extends Error {
  constructor(message = "Operation timed out") {
    super(message);
    this.name = "MT5TimeoutError";
  }
}

export class ConnectionError extends Error {
  constructor(message) {
    super(message);
    this.name = "ConnectionError";
  }
}

export class PermissionError extends Error {
  constructor(message) {
    super(message);
    this.name = "PermissionError";
  }
}

/** Resolves true once any of the events is set, or false after timeoutMs. */
export function waitAny(events, timeoutMs) {
  return new Promise((resolve) => {
    const unsubscribers = [];
    let timer = null;
    const finish = (value) => {
      if (timer !== null) clearTimeout(timer);
      for (const off of unsubscribers) off();
      resolve(value);
    };
    for (const event of events) {
      if (event.isSet()) {
        finish(true);
        return;
      }
      unsubscribers.push(event.onSet(() => finish(true)));
    }
    if (timeoutMs !== undefined) {
      timer = setTimeout(() => finish(false), timeoutMs);
    }
  });
}

export class AsyncEvent {
  #isSet = false;
  #listeners = new Set();

  isSet() {
    return this.#isSet;
  }

  set() {
    if (this.#isSet) return;
    this.#isSet = true;
    const listeners = [...this.#listeners];
    this.#listeners.clear();
    for (const listener of listeners) listener();
  }

  clear() {
    this.#isSet = false;
  }

  /** Register a one-shot listener; returns an unsubscribe function. */
  onSet(listener) {
    this.#listeners.add(listener);
    return () => this.#listeners.delete(listener);
  }

  /** Resolves true when set, or false if timeoutMs elapses first. */
  wait(timeoutMs) {
    return waitAny([this], timeoutMs);
  }
}

/** Serialises async work, like asyncio.Lock. */
export class Mutex {
  #tail = Promise.resolve();

  async run(fn) {
    const previous = this.#tail;
    let release;
    this.#tail = new Promise((resolve) => {
      release = resolve;
    });
    await previous;
    try {
      return await fn();
    } finally {
      release();
    }
  }
}

export const yieldToEventLoop = () => new Promise((resolve) => setImmediate(resolve));

const noop = () => {};
export const silentLogger = { debug: noop, info: noop, warn: noop, error: noop };

export const consoleLogger = {
  debug: (...args) => console.debug("[mt5_market_data]", ...args),
  info: (...args) => console.log("[mt5_market_data]", ...args),
  warn: (...args) => console.warn("[mt5_market_data]", ...args),
  error: (...args) => console.error("[mt5_market_data]", ...args),
};
