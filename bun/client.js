import { execFile } from "node:child_process";
import process from "node:process";
import { createSocket, decodeData, decodeJson, parseHostPort, PUNCH_EVERY_MS, PUNCH_TIMEOUT_MS, KEEPALIVE_EVERY_MS, SESSION_TIMEOUT_MS, sendJson, encodeData } from "./common.js";

const args = new Map(process.argv.slice(2).map((arg) => {
  const [k, v] = arg.split("=", 2);
  return [k.replace(/^--/, ""), v ?? ""];
}));

const rendezvous = parseHostPort(args.get("rendezvous") || "127.0.0.1:4000");
const service = args.get("service") || "demo-shell";
const listenPort = Number(args.get("port") || 0);
const socket = await createSocket(listenPort);

let peer = null;
let sessionId = null;
let token = null;
let active = false;
let lastSeen = Date.now();
let closed = false;

function close(reason = "close") {
  if (closed) {
    return;
  }
  closed = true;
  if (peer && sessionId && token) {
    sendJson(socket, peer.port, peer.host, { type: "close", session: sessionId, token, reason }).catch(() => {});
  }
  if (process.stdin.isTTY) {
    process.stdin.setRawMode(false);
  }
  process.exit(0);
}

function stty(args) {
  return new Promise((resolve, reject) => {
    const child = execFile("stty", args, { stdio: ["inherit", "pipe", "pipe"] }, (err, stdout) => {
      if (err) {
        reject(err);
        return;
      }
      resolve(stdout.toString());
    });
    child.stdin?.end();
  });
}

await sendJson(socket, rendezvous.port, rendezvous.host, { type: "connect_request", service, meta: { impl: "bun-client" } });

socket.on("message", async (data) => {
  let msg;
  try {
    msg = decodeJson(data);
  } catch {
    return;
  }
  if (msg.type === "error") {
    console.error(`${msg.code}: ${msg.message}`);
    process.exit(1);
  }
  if (msg.type === "connect_intro") {
    peer = parseHostPort(msg.peer_addr);
    sessionId = msg.session;
    token = msg.token;
    const deadline = Date.now() + PUNCH_TIMEOUT_MS;
    const timer = setInterval(() => {
      if (closed || active || Date.now() > deadline) {
        clearInterval(timer);
        return;
      }
      sendJson(socket, peer.port, peer.host, { type: "punch", session: sessionId, token }).catch(() => {});
      sendJson(socket, peer.port, peer.host, { type: "hello", session: sessionId, token, role: "client" }).catch(() => {});
    }, PUNCH_EVERY_MS);

    if (process.stdin.isTTY) {
      process.stdin.setRawMode(true);
    }
    process.stdin.resume();
    process.stdin.on("data", (chunk) => {
      if (peer && sessionId && token) {
        sendJson(socket, peer.port, peer.host, { type: "stdin", session: sessionId, token, data: encodeData(chunk) }).catch(() => {});
      }
    });
    try {
      const size = await stty(["size"]);
      const [rows, cols] = size.trim().split(/\s+/).map(Number);
      if (rows && cols) {
        await sendJson(socket, peer.port, peer.host, { type: "resize", session: sessionId, token, rows, cols }).catch(() => {});
      }
    } catch {
    }
    return;
  }
  if (msg.session !== sessionId || msg.token !== token) {
    return;
  }
  lastSeen = Date.now();
  switch (msg.type) {
    case "punch":
      await sendJson(socket, peer.port, peer.host, { type: "hello", session: sessionId, token, role: "client" }).catch(() => {});
      break;
    case "hello":
    case "hello_ack":
      active = true;
      await sendJson(socket, peer.port, peer.host, { type: "hello_ack", session: sessionId, token }).catch(() => {});
      break;
    case "stdout":
      process.stdout.write(decodeData(msg.data));
      break;
    case "keepalive":
      await sendJson(socket, peer.port, peer.host, { type: "keepalive", session: sessionId, token }).catch(() => {});
      break;
    case "close":
      close(msg.reason || "peer_close");
      break;
  }
});

setInterval(() => {
  if (closed || !peer || !sessionId || !token) {
    return;
  }
  if (Date.now() - lastSeen > SESSION_TIMEOUT_MS) {
    console.error("session timeout");
    close("timeout");
    return;
  }
  sendJson(socket, peer.port, peer.host, { type: "keepalive", session: sessionId, token }).catch(() => {});
}, KEEPALIVE_EVERY_MS);

process.on("SIGINT", () => close("signal"));
process.on("SIGTERM", () => close("signal"));
