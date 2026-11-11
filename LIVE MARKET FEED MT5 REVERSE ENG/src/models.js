/** A tradable instrument from the broker's symbol catalogue. */
export class BrokerSymbol {
  constructor(name, description, digits, symbolId, path = "") {
    this.name = name;
    this.description = description;
    this.digits = digits;
    this.symbolId = symbolId;
    this.path = path;
  }
}

/** A live bid/ask update. `timestamp` is a Date (UTC instant). */
export class Quote {
  constructor(symbol, symbolId, bid, ask, last, tickVolume, timestamp, flags = 0) {
    this.symbol = symbol;
    this.symbolId = symbolId;
    this.bid = bid;
    this.ask = ask;
    this.last = last;
    this.tickVolume = tickVolume;
    this.timestamp = timestamp;
    this.flags = flags;
  }
}

/** An OHLC candle. `timestamp` is the candle open time in UTC Unix seconds. */
export class Candle {
  constructor(symbol, timeframe, timestamp, open, high, low, close, tickVolume, spread) {
    this.symbol = symbol;
    this.timeframe = timeframe;
    this.timestamp = timestamp;
    this.open = open;
    this.high = high;
    this.low = low;
    this.close = close;
    this.tickVolume = tickVolume;
    this.spread = spread;
  }

  /** Candle open time as a Date. */
  get time() {
    return new Date(this.timestamp * 1000);
  }
}
