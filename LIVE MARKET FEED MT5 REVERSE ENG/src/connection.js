/** Encrypted WebSocket transport for the MT5 web-terminal protocol. */
import WebSocket from "ws";

import {
  STATIC_KEY, aesDecrypt, aesEncrypt, buildCommand, packWire, parseResponse,
} from "./protocol.js";
import { ConnectionError, MT5TimeoutError, Mutex } from "./util.js";

export class MT5Connection {
  /**
   * @param {string} host
   * @param {number} port
   * @param {boolean} verifyTls  when false, TLS certificate checks are disabled
   *   (the broker endpoints are IP addresses whose certificates name a hostname)
   * @param {{secure?: boolean}} [options]  secure=false uses ws:// (tests only)
   */
  constructor(host, port = 443, verifyTls = false, { secure = true } = {}) {
    this.host = host;
    this.port = port;
    this.verifyTls = verifyTls;
    this.secure = secure;
    this.websocket = null;
    this.sessionKey = null;

    this._sendLock = new Mutex();
    this._queue = [];
    this._waiters = [];
    this._closedError = null;
  }

  async connect() {
    const scheme = this.secure ? "wss" : "ws";
    const origin = `${this.secure ? "https" : "http"}://${this.host}:${this.port}`;
    const url = `${scheme}://${this.host}:${this.port}/terminal`;

    const ws = new WebSocket(url, {
      rejectUnauthorized: this.verifyTls,
      origin,
      handshakeTimeout: 15_000,
      maxPayload: 8 * 1024 * 1024,
    });

    await new Promise((resolve, reject) => {
      const onOpen = () => {
        ws.off("error", onError);
        resolve();
      };
      const onError = (err) => {
        ws.off("open", onOpen);
        reject(err);
      };
      ws.once("open", onOpen);
      ws.once("error", onError);
    });

    this.websocket = ws;
    ws.on("message", (data, isBinary) => this._onMessage(data, isBinary));
    ws.on("error", (err) => this._fail(new ConnectionError(`WebSocket error: ${err.message}`)));
    ws.on("close", () => this._fail(new ConnectionError("MT5 WebSocket closed")));
  }

  _onMessage(data, isBinary) {
    const item = { data, isBinary };
    const waiter = this._waiters.shift();
    if (waiter) {
      clearTimeout(waiter.timer);
      waiter.resolve(item);
    } else {
      this._queue.push(item);
    }
  }

  _fail(error) {
    if (this._closedError === null) this._closedError = error;
    const waiters = this._waiters.splice(0);
    for (const waiter of waiters) {
      clearTimeout(waiter.timer);
      waiter.reject(this._closedError);
    }
  }

  _nextRaw(timeoutMs) {
    if (this._queue.length) return Promise.resolve(this._queue.shift());
    if (this._closedError) return Promise.reject(this._closedError);
    return new Promise((resolve, reject) => {
      const waiter = { resolve, reject, timer: null };
      waiter.timer = setTimeout(() => {
        const idx = this._waiters.indexOf(waiter);
        if (idx >= 0) this._waiters.splice(idx, 1);
        reject(new MT5TimeoutError(`No MT5 message within ${timeoutMs} ms`));
      }, timeoutMs);
      this._waiters.push(waiter);
    });
  }

  async _send(key, commandId, payload) {
    await this._sendLock.run(async () => {
      if (this.websocket === null || this.websocket.readyState !== WebSocket.OPEN) {
        throw new ConnectionError("MT5 WebSocket is not open");
      }
      const encrypted = aesEncrypt(key, buildCommand(commandId, payload));
      const frame = packWire(encrypted);
      await new Promise((resolve, reject) => {
        this.websocket.send(frame, { binary: true }, (err) => (err ? reject(err) : resolve()));
      });
    });
  }

  async sendAuth() {
    await this._send(STATIC_KEY, 0, Buffer.alloc(64));
  }

  async send(commandId, payload = Buffer.alloc(0)) {
    if (this.sessionKey === null) {
      throw new Error("Session key has not been negotiated");
    }
    await this._send(this.sessionKey, commandId, payload);
  }

  async _receive(key, timeoutMs) {
    const { data, isBinary } = await this._nextRaw(timeoutMs);
    if (!isBinary || data.length < 8) {
      throw new ConnectionError("Unexpected MT5 WebSocket frame");
    }
    const declaredLength = data.readUInt32LE(0);
    if (declaredLength > data.length - 8) {
      throw new ConnectionError("Truncated MT5 WebSocket frame");
    }
    return parseResponse(aesDecrypt(key, data.subarray(8, 8 + declaredLength)));
  }

  receiveAuth(timeoutMs = 12_000) {
    return this._receive(STATIC_KEY, timeoutMs);
  }

  receive(timeoutMs = 30_000) {
    if (this.sessionKey === null) {
      return Promise.reject(new Error("Session key has not been negotiated"));
    }
    return this._receive(this.sessionKey, timeoutMs);
  }

  async close() {
    const ws = this.websocket;
    this.websocket = null;
    this._fail(new ConnectionError("MT5 WebSocket closed"));
    if (ws === null || ws.readyState === WebSocket.CLOSED) return;

    await new Promise((resolve) => {
      const timer = setTimeout(() => {
        ws.terminate();
        resolve();
      }, 5_000);
      ws.once("close", () => {
        clearTimeout(timer);
        resolve();
      });
      try {
        ws.close();
      } catch {
        ws.terminate();
      }
    });
  }
}
