import base64
import binascii
import fcntl
import json
import socket
import struct
import sys
import termios
import time
import os

VERSION = 1
MAX_PACKET_SIZE = 64 * 1024
CHUNK_SIZE = 1024
INTRO_EVERY = 1.0
RETRANSMIT_EVERY = 0.2
KEEPALIVE_EVERY = 5
SESSION_TIMEOUT = 20
PUNCH_EVERY = 0.5
PUNCH_TIMEOUT = 20
REGISTER_EVERY = 10


def send_json(sock, addr, msg):
    payload = dict(msg)
    payload["v"] = VERSION
    payload["ts"] = int(time.time() * 1000)
    sock.sendto(json.dumps(payload).encode("utf-8"), addr)


def recv_json(data):
    try:
        payload = data.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise ValueError("invalid utf-8") from exc
    msg = json.loads(payload)
    if not isinstance(msg, dict):
        raise ValueError("json object required")
    return msg


def parse_host_port(value):
    host, port = value.rsplit(":", 1)
    return host, int(port)


def encode_data(data):
    return base64.b64encode(data).decode("ascii")


def decode_data(value):
    if not value:
        return b""
    try:
        return base64.b64decode(value.encode("ascii"), validate=True)
    except (binascii.Error, ValueError) as exc:
        raise ValueError("invalid base64") from exc


def has_version(msg):
    return isinstance(msg, dict) and msg.get("v") == VERSION


def terminal_info():
    cols, rows = get_terminal_size()
    return {
        "term": os.environ.get("TERM", "xterm-256color"),
        "term_program": os.environ.get("TERM_PROGRAM", "rshell-python-client"),
        "terminal": os.environ.get("TERMINAL", "rshell-python-client"),
        "cols": cols,
        "rows": rows,
    }


def positive_int(value, fallback=0):
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        return fallback
    return parsed if parsed > 0 else fallback


def log_event(level, impl, role, event, message, **fields):
    parts = [
        f"ts={json.dumps(time.strftime('%Y-%m-%dT%H:%M:%S', time.gmtime()) + f'.{int((time.time() % 1) * 1_000_000):06d}Z')}",
        f"level={json.dumps(level)}",
        f"impl={json.dumps(impl)}",
        f"role={json.dumps(role)}",
        f"event={json.dumps(event)}",
        f"msg={json.dumps(message)}",
    ]
    for key, value in fields.items():
        if value is not None:
            parts.append(f"{key}={json.dumps(value)}")
    print(" ".join(parts), file=sys.stderr, flush=True)


class ReliableSender:
    def __init__(self, stream):
        self.stream = stream
        self.next_seq = 0
        self.pending = {}

    def push(self, session_id, token, chunk):
        self.next_seq += 1
        encoded = encode_data(chunk)
        self.pending[self.next_seq] = {"data": encoded, "sent_at": time.time()}
        return {"type": "data", "session": session_id, "token": token, "stream": self.stream, "seq": self.next_seq, "data": encoded}

    def ack(self, ack):
        for seq in list(self.pending):
            if seq <= ack:
                self.pending.pop(seq, None)

    def retransmit(self, session_id, token):
        now = time.time()
        messages = []
        for seq, chunk in list(self.pending.items()):
            if now - chunk["sent_at"] < RETRANSMIT_EVERY:
                continue
            chunk["sent_at"] = now
            messages.append({"type": "data", "session": session_id, "token": token, "stream": self.stream, "seq": seq, "data": chunk["data"]})
        return messages


class ReliableReceiver:
    def __init__(self):
        self.expected = 1
        self.buffer = {}

    def accept(self, seq, chunk):
        if seq <= 0:
            return [], self.expected - 1
        if seq < self.expected:
            return [], self.expected - 1
        self.buffer.setdefault(seq, bytes(chunk))
        chunks = []
        while self.expected in self.buffer:
            chunks.append(self.buffer.pop(self.expected))
            self.expected += 1
        return chunks, self.expected - 1


def get_terminal_size():
    for fd in (1, 0):
        try:
            cols, rows = os.get_terminal_size(fd)
            return cols, rows
        except OSError:
            continue
    return 0, 0


def set_winsize(fd, rows, cols):
    if fd is None or rows <= 0 or cols <= 0:
        return
    fcntl.ioctl(fd, termios.TIOCSWINSZ, struct.pack("HHHH", rows, cols, 0, 0))
