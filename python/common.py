import base64
import json
import socket
import time

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
