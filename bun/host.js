import { spawn } from "node:child_process";
import { createSocket, decodeData, decodeJson, parseHostPort, PUNCH_EVERY_MS, PUNCH_TIMEOUT_MS, REGISTER_EVERY_MS, KEEPALIVE_EVERY_MS, SESSION_TIMEOUT_MS, sendJson, encodeData } from "./common.js";

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
  if (session.keepaliveTimer) {
    clearInterval(session.keepaliveTimer);
  }
  sendJson(socket, session.peer.port, session.peer.host, { type: "close", session: session.id, token: session.token, reason }).catch(() => {});
  session.child?.kill("SIGKILL");
}

function startShell(session) {
  if (session.active || session.closed) {
    return;
  }
  const child = spawn("script", ["-qfc", `${shell} -i`, "/dev/null"], { stdio: ["pipe", "pipe", "pipe"] });
  child.stderr.pipe(child.stdout);
  child.stdout.on("data", (chunk) => {
    sendJson(socket, session.peer.port, session.peer.host, {
      type: "stdout",
      session: session.id,
      token: session.token,
      data: encodeData(chunk)
    }).catch(() => {});
  });
  child.on("exit", () => closeSession(session, "shell_exit"));
  session.child = child;
  session.active = true;
}

await register();
setInterval(() => register().catch((err) => console.error("register error", err.message)), REGISTER_EVERY_MS);
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

socket.on("message", async (data, remote) => {
  let msg;
  try {
    msg = decodeJson(data);
  } catch {
    return;
  }
  switch (msg.type) {
    case "register_ok":
      console.error(`registered ${service} public=${msg.public_addr}`);
      return;
    case "connect_intro": {
      const peer = parseHostPort(msg.peer_addr);
      const session = {
        id: msg.session,
        token: msg.token,
        peer,
        active: false,
        closed: false,
        lastSeen: Date.now()
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
    case "hello_ack":
      startShell(session);
      await sendJson(socket, session.peer.port, session.peer.host, { type: "hello_ack", session: session.id, token: session.token }).catch(() => {});
      break;
    case "stdin":
      startShell(session);
      session.child?.stdin.write(decodeData(msg.data));
      break;
    case "keepalive":
      await sendJson(socket, session.peer.port, session.peer.host, { type: "keepalive", session: session.id, token: session.token }).catch(() => {});
      break;
    case "close":
      closeSession(session, msg.reason || "peer_close");
      sessions.delete(session.id);
      break;
    case "resize":
      break;
  }
});
