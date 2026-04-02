import dgram from "node:dgram";
import process from "node:process";

const utf8Decoder = new TextDecoder("utf-8", { fatal: true });

export const VERSION = 1;
export const KEEPALIVE_EVERY_MS = 5000;
export const SESSION_TIMEOUT_MS = 20000;
export const PUNCH_EVERY_MS = 500;
export const PUNCH_TIMEOUT_MS = 20000;
export const REGISTER_EVERY_MS = 10000;
export const MAX_DATA_CHUNK = 1024;
export const INTRO_EVERY_MS = 1000;
export const RETRANSMIT_EVERY_MS = 200;

export function logEvent(level, impl, role, event, message, fields = {}) {
  const parts = [
    `ts=${JSON.stringify(new Date().toISOString())}`,
    `level=${JSON.stringify(level)}`,
    `impl=${JSON.stringify(impl)}`,
    `role=${JSON.stringify(role)}`,
    `event=${JSON.stringify(event)}`,
    `msg=${JSON.stringify(message)}`
  ];
  for (const [key, value] of Object.entries(fields)) {
    if (value !== undefined) {
      parts.push(`${key}=${JSON.stringify(value)}`);
    }
  }
  process.stderr.write(`${parts.join(" ")}\n`);
}

export function createSocket(port, host = "0.0.0.0") {
  const socket = dgram.createSocket("udp4");
  return new Promise((resolve, reject) => {
    socket.once("error", reject);
    socket.bind(port, host, () => {
      socket.removeListener("error", reject);
      resolve(socket);
    });
  });
}

export function sendJson(socket, port, host, msg) {
  const payload = Buffer.from(JSON.stringify({ ...msg, v: VERSION, ts: Date.now() }));
  return new Promise((resolve, reject) => {
    socket.send(payload, port, host, (err) => (err ? reject(err) : resolve()));
  });
}

export function decodeJson(buffer) {
  return JSON.parse(utf8Decoder.decode(buffer));
}

export function hasVersion(msg) {
  return Number(msg?.v) === VERSION;
}

export function encodeData(buffer) {
  return Buffer.from(buffer).toString("base64");
}

export function decodeData(value) {
  if (!value) {
    return Buffer.alloc(0);
  }
  if (typeof value !== "string" || !/^(?:[A-Za-z0-9+/]{4})*(?:[A-Za-z0-9+/]{2}==|[A-Za-z0-9+/]{3}=)?$/.test(value)) {
    throw new Error("invalid base64");
  }
  const buffer = Buffer.from(value, "base64");
  if (buffer.toString("base64") !== value) {
    throw new Error("invalid base64");
  }
  return buffer;
}

export function parseHostPort(value) {
  const idx = value.lastIndexOf(":");
  if (idx < 0) {
    throw new Error(`invalid address: ${value}`);
  }
  return { host: value.slice(0, idx), port: Number(value.slice(idx + 1)) };
}

export function terminalInfo() {
  return {
    term: process.env.TERM || "xterm-256color",
    term_program: process.env.TERM_PROGRAM || "rshell-bun-client",
    terminal: process.env.TERMINAL || "rshell-bun-client",
    cols: process.stdout.columns || 0,
    rows: process.stdout.rows || 0
  };
}

export function positiveInt(value, fallback = 0) {
  const num = Number(value);
  return Number.isInteger(num) && num > 0 ? num : fallback;
}

export class ReliableSender {
  constructor(stream) {
    this.stream = stream;
    this.nextSeq = 0;
    this.pending = new Map();
  }

  push(session, token, chunk) {
    this.nextSeq += 1;
    const encoded = encodeData(chunk);
    this.pending.set(this.nextSeq, { data: encoded, sentAt: Date.now() });
    return { type: "data", session, token, stream: this.stream, seq: this.nextSeq, data: encoded };
  }

  ack(ack) {
    for (const seq of this.pending.keys()) {
      if (seq <= ack) {
        this.pending.delete(seq);
      }
    }
  }

  retransmit(session, token) {
    const now = Date.now();
    const messages = [];
    for (const [seq, chunk] of this.pending) {
      if (now - chunk.sentAt < RETRANSMIT_EVERY_MS) {
        continue;
      }
      chunk.sentAt = now;
      messages.push({ type: "data", session, token, stream: this.stream, seq, data: chunk.data });
    }
    return messages;
  }
}

export class ReliableReceiver {
  constructor() {
    this.expected = 1;
    this.buffer = new Map();
  }

  accept(seq, chunk) {
    if (!Number.isInteger(seq) || seq <= 0) {
      return { chunks: [], ack: this.expected - 1 };
    }
    if (seq < this.expected) {
      return { chunks: [], ack: this.expected - 1 };
    }
    if (!this.buffer.has(seq)) {
      this.buffer.set(seq, Buffer.from(chunk));
    }
    const chunks = [];
    while (this.buffer.has(this.expected)) {
      chunks.push(this.buffer.get(this.expected));
      this.buffer.delete(this.expected);
      this.expected += 1;
    }
    return { chunks, ack: this.expected - 1 };
  }
}
