import process from "node:process";
import { createSocket, decodeData, decodeJson, hasVersion, INTRO_EVERY_MS, logEvent, MAX_DATA_CHUNK, parseHostPort, PUNCH_EVERY_MS, PUNCH_TIMEOUT_MS, KEEPALIVE_EVERY_MS, RETRANSMIT_EVERY_MS, SESSION_TIMEOUT_MS, sendJson, positiveInt, ReliableReceiver, ReliableSender, terminalInfo } from "./common.js";

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
let stdinAttached = false;
let resizeAttached = false;
let resizeTimer = null;
let introDeadline = Date.now() + PUNCH_TIMEOUT_MS;
const stdinSender = new ReliableSender("stdin");
const ptyReceiver = new ReliableReceiver();

function requestResize() {
  if (resizeTimer !== null) {
    clearTimeout(resizeTimer);
  }
  resizeTimer = setTimeout(() => {
    resizeTimer = null;
    sendResize();
  }, 50);
}

function helloMessage() {
  return { type: "hello", session: sessionId, token, role: "client", ...terminalInfo() };
}

function sendChunkedStdin(chunk) {
  if (!peer || !sessionId || !token) {
    return;
  }
  for (let offset = 0; offset < chunk.length; offset += MAX_DATA_CHUNK) {
    sendJson(socket, peer.port, peer.host, stdinSender.push(sessionId, token, chunk.subarray(offset, offset + MAX_DATA_CHUNK))).catch(() => {});
  }
}

function sendResize() {
  if (!peer || !sessionId || !token) {
    return;
  }
  sendJson(socket, peer.port, peer.host, { type: "resize", session: sessionId, token, ...terminalInfo() }).catch(() => {});
}

function close(reason = "close", exitCode = 0, notify = true) {
  if (closed) {
    return;
  }
  closed = true;
  if (notify && peer && sessionId && token) {
    sendJson(socket, peer.port, peer.host, { type: "close", session: sessionId, token, reason }).catch(() => {});
  }
  if (stdinAttached) {
    process.stdin.removeAllListeners("data");
    process.stdin.removeAllListeners("end");
    process.stdin.removeAllListeners("close");
    stdinAttached = false;
  }
  if (resizeAttached) {
    process.stdout.off("resize", requestResize);
    resizeAttached = false;
  }
  if (resizeTimer !== null) {
    clearTimeout(resizeTimer);
    resizeTimer = null;
  }
  if (process.stdin.isTTY) {
    process.stdin.setRawMode(false);
  }
  if (reason === "timeout") {
    logEvent("error", "bun", "client", "session_timeout", "session timed out", { session: sessionId });
  }
  process.exit(exitCode);
}

await sendJson(socket, rendezvous.port, rendezvous.host, { type: "connect_request", service, meta: { impl: "bun-client" } });
const introTimer = setInterval(() => {
  if (peer || closed) {
    clearInterval(introTimer);
    return;
  }
  if (Date.now() > introDeadline) {
    clearInterval(introTimer);
    logEvent("error", "bun", "client", "intro_timeout", "timed out waiting for rendezvous intro", { service });
    process.exit(1);
  }
  sendJson(socket, rendezvous.port, rendezvous.host, { type: "connect_request", service, meta: { impl: "bun-client" } }).catch(() => {});
}, INTRO_EVERY_MS);

socket.on("message", async (data) => {
  let msg;
  try {
    msg = decodeJson(data);
  } catch {
    return;
  }
  if (!hasVersion(msg)) {
    return;
  }
  if (msg.type === "error") {
    logEvent("error", "bun", "client", "server_error", "rendezvous returned an error", { code: msg.code, detail: msg.message });
    process.exit(1);
  }
  if (msg.type === "connect_intro") {
    clearInterval(introTimer);
    peer = parseHostPort(msg.peer_addr);
    sessionId = msg.session;
    token = msg.token;
    introDeadline = Date.now() + positiveInt(msg.timeout_ms, PUNCH_TIMEOUT_MS);
    logEvent("info", "bun", "client", "session_intro", "received session intro", { service, session: sessionId, peer: `${peer.host}:${peer.port}` });
    const deadline = Date.now() + PUNCH_TIMEOUT_MS;
    const timer = setInterval(() => {
      if (closed || active || Date.now() > deadline) {
        clearInterval(timer);
        return;
      }
      sendJson(socket, peer.port, peer.host, { type: "punch", session: sessionId, token }).catch(() => {});
      sendJson(socket, peer.port, peer.host, helloMessage()).catch(() => {});
    }, PUNCH_EVERY_MS);

    if (process.stdin.isTTY) {
      process.stdin.setRawMode(true);
      process.stdout.on("resize", requestResize);
      resizeAttached = true;
    }
    process.stdin.resume();
    if (!stdinAttached) {
      process.stdin.on("data", sendChunkedStdin);
      process.stdin.once("end", () => close("stdin_eof"));
      process.stdin.once("close", () => close("stdin_eof"));
      stdinAttached = true;
    }
    requestResize();
    return;
  }
  if (msg.session !== sessionId || msg.token !== token) {
    return;
  }
  lastSeen = Date.now();
  switch (msg.type) {
    case "punch":
      await sendJson(socket, peer.port, peer.host, helloMessage()).catch(() => {});
      break;
    case "hello":
    case "hello_ack":
      active = true;
      await sendJson(socket, peer.port, peer.host, { type: "hello_ack", session: sessionId, token }).catch(() => {});
      break;
    case "data":
      if (msg.stream !== "pty") {
        break;
      }
      try {
        const { chunks, ack } = ptyReceiver.accept(Number(msg.seq || 0), decodeData(msg.data));
        for (const chunk of chunks) {
          process.stdout.write(chunk);
        }
        await sendJson(socket, peer.port, peer.host, { type: "ack", session: sessionId, token, stream: "pty", ack }).catch(() => {});
      } catch {
        logEvent("error", "bun", "client", "invalid_payload", "dropped invalid stdout payload", { session: sessionId });
      }
      break;
    case "ack":
      if (msg.stream === "stdin") {
        stdinSender.ack(Number(msg.ack || 0));
      }
      break;
    case "keepalive":
      break;
    case "close":
      close(msg.reason || "peer_close", 0, false);
      break;
  }
});

setInterval(() => {
  if (closed || !peer || !sessionId || !token) {
    return;
  }
  if (Date.now() - lastSeen > SESSION_TIMEOUT_MS) {
    close("timeout", 1);
    return;
  }
  sendJson(socket, peer.port, peer.host, { type: "keepalive", session: sessionId, token }).catch(() => {});
}, KEEPALIVE_EVERY_MS);

setInterval(() => {
  if (closed || !peer || !sessionId || !token) {
    return;
  }
  for (const msg of stdinSender.retransmit(sessionId, token)) {
    sendJson(socket, peer.port, peer.host, msg).catch(() => {});
  }
}, RETRANSMIT_EVERY_MS);

process.on("SIGINT", () => close("signal", 0));
process.on("SIGTERM", () => close("signal", 0));
function handleSuspend() {
  if (process.stdin.isTTY) {
    process.stdin.setRawMode(false);
  }
  process.off("SIGTSTP", handleSuspend);
  process.kill(process.pid, "SIGTSTP");
  process.on("SIGTSTP", handleSuspend);
}
process.on("SIGTSTP", handleSuspend);
process.on("SIGCONT", () => {
  if (process.stdin.isTTY && !closed) {
    process.stdin.setRawMode(true);
    requestResize();
  }
});
