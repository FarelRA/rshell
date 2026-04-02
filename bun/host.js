import process from "node:process";
import { createSocket, decodeData, decodeJson, hasVersion, logEvent, MAX_DATA_CHUNK, parseHostPort, PUNCH_EVERY_MS, PUNCH_TIMEOUT_MS, REGISTER_EVERY_MS, KEEPALIVE_EVERY_MS, RETRANSMIT_EVERY_MS, SESSION_TIMEOUT_MS, sendJson, positiveInt, ReliableReceiver, ReliableSender } from "./common.js";

let spawnPty = null;
try {
  ({ spawn: spawnPty } = await import("bun-pty"));
} catch {
}

process.title = "rshell-bun-host";

const args = new Map(process.argv.slice(2).map((arg) => {
  const [k, v] = arg.split("=", 2);
  return [k.replace(/^--/, ""), v ?? ""];
}));

const rendezvous = parseHostPort(args.get("rendezvous") || "127.0.0.1:4000");
const service = args.get("service") || "demo-shell";
const listenPort = Number(args.get("port") || 0);
const shell = args.get("shell") || process.env.SHELL || "/bin/sh";

const socket = await createSocket(listenPort);
const sessions = new Map();

async function register() {
  await sendJson(socket, rendezvous.port, rendezvous.host, {
    type: "register",
    service,
    meta: { impl: "bun-host" }
  });
}

function closeSession(session, reason = "close") {
  if (!session || session.closed) {
    return;
  }
  session.closed = true;
  clearInterval(session.punchTimer);
  session.outputSub?.dispose?.();
  session.exitSub?.dispose?.();
  sendJson(socket, session.peer.port, session.peer.host, { type: "close", session: session.id, token: session.token, reason }).catch(() => {});
  try {
    session.pty?.kill?.("SIGKILL");
  } catch {
  }
}

function sendStdout(session, chunk) {
  for (let offset = 0; offset < chunk.length; offset += MAX_DATA_CHUNK) {
    sendJson(socket, session.peer.port, session.peer.host, session.ptySender.push(session.id, session.token, chunk.subarray(offset, offset + MAX_DATA_CHUNK))).catch(() => {});
  }
}

function startShell(session) {
  if (session.active || session.closed) {
    return;
  }
  if (!spawnPty) {
    closeSession(session, "shell_start_failed");
    sessions.delete(session.id);
    return;
  }
  let pty;
  try {
    pty = spawnPty(shell, ["-i"], {
      name: session.term || "xterm-256color",
      cols: session.cols || 80,
      rows: session.rows || 24,
      cwd: process.cwd(),
      env: {
        ...process.env,
        TERM: session.term || "xterm-256color",
        TERM_PROGRAM: "rshell-bun-host",
        TERMINAL: "rshell-bun-host"
      }
    });
  } catch {
    closeSession(session, "shell_start_failed");
    sessions.delete(session.id);
    return;
  }
  session.outputSub = pty.onData((data) => {
    sendStdout(session, Buffer.from(data, "latin1"));
  });
  session.exitSub = pty.onExit(() => {
    closeSession(session, "shell_exit");
    sessions.delete(session.id);
  });
  session.pty = pty;
  session.active = true;
}

function applyResize(session) {
  if (!session.pty || !session.rows || !session.cols) {
    return;
  }
  try {
    session.pty.resize(session.cols, session.rows);
  } catch {
  }
}

function captureTerminal(session, msg) {
  if (msg.term) session.term = msg.term;
  session.cols = positiveInt(msg.cols, session.cols);
  session.rows = positiveInt(msg.rows, session.rows);
}

await register();
setInterval(() => register().catch((err) => logEvent("error", "bun", "host", "register_failed", "failed to refresh registration", { service, error: err.message })), REGISTER_EVERY_MS);
setInterval(() => {
  const now = Date.now();
  sendJson(socket, rendezvous.port, rendezvous.host, { type: "keepalive", service }).catch(() => {});
  for (const [id, session] of sessions) {
    if (session.closed || now - session.lastSeen > SESSION_TIMEOUT_MS) {
      closeSession(session, "timeout");
      sessions.delete(id);
      continue;
    }
    if (session.active) {
      sendJson(socket, session.peer.port, session.peer.host, { type: "keepalive", session: session.id, token: session.token }).catch(() => {});
    }
  }
}, KEEPALIVE_EVERY_MS);

setInterval(() => {
  for (const session of sessions.values()) {
    if (session.closed || !session.ptySender) {
      continue;
    }
    for (const msg of session.ptySender.retransmit(session.id, session.token)) {
      sendJson(socket, session.peer.port, session.peer.host, msg).catch(() => {});
    }
  }
}, RETRANSMIT_EVERY_MS);

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
  switch (msg.type) {
    case "register_ok":
      logEvent("info", "bun", "host", "registered", "registration confirmed", { service, public_addr: msg.public_addr });
      return;
    case "connect_intro": {
      const peer = parseHostPort(msg.peer_addr);
      const session = {
        id: msg.session,
        token: msg.token,
        peer,
        active: false,
        closed: false,
        lastSeen: Date.now(),
        term: "xterm-256color",
        cols: 80,
        rows: 24,
        stdinReceiver: new ReliableReceiver(),
        ptySender: new ReliableSender("pty"),
        pty: null,
        outputSub: null,
        exitSub: null
      };
      session.punchTimer = setInterval(() => {
        if (session.closed || session.active || Date.now() - session.lastSeen > PUNCH_TIMEOUT_MS) {
          clearInterval(session.punchTimer);
          return;
        }
        sendJson(socket, peer.port, peer.host, { type: "punch", session: session.id, token: session.token }).catch(() => {});
        sendJson(socket, peer.port, peer.host, { type: "hello", session: session.id, token: session.token, role: "host" }).catch(() => {});
      }, PUNCH_EVERY_MS);
      sessions.set(session.id, session);
      logEvent("info", "bun", "host", "session_intro", "received session intro", { service, session: session.id, peer: `${peer.host}:${peer.port}` });
      return;
    }
  }
  const session = sessions.get(msg.session);
  if (!session || session.token !== msg.token) {
    return;
  }
  session.lastSeen = Date.now();
  switch (msg.type) {
    case "punch":
      await sendJson(socket, session.peer.port, session.peer.host, { type: "hello", session: session.id, token: session.token, role: "host" }).catch(() => {});
      break;
    case "hello":
      captureTerminal(session, msg);
      startShell(session);
      await sendJson(socket, session.peer.port, session.peer.host, { type: "hello_ack", session: session.id, token: session.token }).catch(() => {});
      break;
    case "hello_ack":
      startShell(session);
      break;
    case "data":
      if (msg.stream !== "stdin") {
        break;
      }
      startShell(session);
      try {
        const { chunks, ack } = session.stdinReceiver.accept(Number(msg.seq || 0), decodeData(msg.data));
        for (const chunk of chunks) {
          session.pty?.write?.(chunk.toString("latin1"));
        }
        await sendJson(socket, session.peer.port, session.peer.host, { type: "ack", session: session.id, token: session.token, stream: "stdin", ack }).catch(() => {});
      } catch {
        logEvent("error", "bun", "host", "invalid_payload", "dropped invalid stdin payload", { session: session.id });
      }
      break;
    case "ack":
      if (msg.stream === "pty") {
        session.ptySender.ack(Number(msg.ack || 0));
      }
      break;
    case "keepalive":
      break;
    case "close":
      closeSession(session, msg.reason || "peer_close");
      sessions.delete(session.id);
      break;
    case "resize":
      captureTerminal(session, msg);
      applyResize(session);
      break;
  }
});
