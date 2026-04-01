import base64
import fcntl
import json
import socket
import struct
import termios
import time
import os

VERSION = 1
MAX_PACKET_SIZE = 64 * 1024
KEEPALIVE_EVERY = 5
SESSION_TIMEOUT = 20
PUNCH_EVERY = 0.5
PUNCH_TIMEOUT = 15
REGISTER_EVERY = 10


def send_json(sock, addr, msg):
    payload = dict(msg)
    payload["v"] = VERSION
    payload["ts"] = int(time.time() * 1000)
    sock.sendto(json.dumps(payload).encode("utf-8"), addr)


def recv_json(data):
    return json.loads(data.decode("utf-8"))


def parse_host_port(value):
    host, port = value.rsplit(":", 1)
    return host, int(port)


def encode_data(data):
    return base64.b64encode(data).decode("ascii")


def decode_data(value):
    return base64.b64decode(value.encode("ascii")) if value else b""


def terminal_info():
    cols, rows = get_terminal_size()
    return {
        "term": os.environ.get("TERM", "xterm-256color"),
        "term_program": os.environ.get("TERM_PROGRAM", "rshell-python-client"),
        "terminal": os.environ.get("TERMINAL", "rshell-python-client"),
        "cols": cols,
        "rows": rows,
    }


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
