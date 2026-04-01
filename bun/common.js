import dgram from "node:dgram";

export const VERSION = 1;
export const KEEPALIVE_EVERY_MS = 5000;
export const SESSION_TIMEOUT_MS = 20000;
export const PUNCH_EVERY_MS = 500;
export const PUNCH_TIMEOUT_MS = 15000;
export const REGISTER_EVERY_MS = 10000;

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
  return JSON.parse(buffer.toString("utf8"));
}

export function encodeData(buffer) {
  return Buffer.from(buffer).toString("base64");
}

export function decodeData(value) {
  return Buffer.from(value || "", "base64");
}

export function parseHostPort(value) {
  const idx = value.lastIndexOf(":");
  if (idx < 0) {
    throw new Error(`invalid address: ${value}`);
  }
  return { host: value.slice(0, idx), port: Number(value.slice(idx + 1)) };
}
