#!/usr/bin/env python3
import argparse
import os
import select
import signal
import socket
import subprocess
import sys
import termios
import threading
import time
import tty

from common import KEEPALIVE_EVERY, MAX_PACKET_SIZE, PUNCH_EVERY, PUNCH_TIMEOUT, SESSION_TIMEOUT, decode_data, encode_data, parse_host_port, recv_json, send_json


def get_tty_size():
    try:
        out = subprocess.check_output(["stty", "size"], stdin=sys.stdin)
        rows, cols = out.decode().strip().split()
        return int(rows), int(cols)
    except Exception:
        return None, None


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--rendezvous", default="127.0.0.1:4000")
    parser.add_argument("--service", default="demo-shell")
    parser.add_argument("--listen", default="0.0.0.0:0")
    args = parser.parse_args()

    host, port = parse_host_port(args.listen)
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.bind((host, port))
    sock.settimeout(1.0)

    rdv = parse_host_port(args.rendezvous)
    send_json(sock, rdv, {"type": "connect_request", "service": args.service, "meta": {"impl": "python-client"}})

    peer = None
    session_id = None
    token = None
    active = False
    last_seen = time.time()
    closed = False

    old_attrs = None
    if sys.stdin.isatty():
        old_attrs = termios.tcgetattr(sys.stdin.fileno())

    def restore():
        if old_attrs is not None:
            termios.tcsetattr(sys.stdin.fileno(), termios.TCSADRAIN, old_attrs)

    def close(reason="close"):
        nonlocal closed
        if closed:
            return
        closed = True
        if peer and session_id and token:
            try:
                send_json(sock, peer, {"type": "close", "session": session_id, "token": token, "reason": reason})
            except OSError:
                pass
        restore()
        raise SystemExit(0)

    def stop(_sig, _frame):
        close("signal")

    signal.signal(signal.SIGINT, stop)
    signal.signal(signal.SIGTERM, stop)

    while peer is None:
        data, _ = sock.recvfrom(MAX_PACKET_SIZE)
        msg = recv_json(data)
        if msg.get("type") == "error":
            raise SystemExit(f"{msg.get('code')}: {msg.get('message')}")
        if msg.get("type") == "connect_intro":
            peer = parse_host_port(msg["peer_addr"])
            session_id = msg["session"]
            token = msg["token"]

    if old_attrs is not None:
        tty.setraw(sys.stdin.fileno())
    rows, cols = get_tty_size()
    if rows and cols:
        send_json(sock, peer, {"type": "resize", "session": session_id, "token": token, "rows": rows, "cols": cols})

    def punch_loop():
        deadline = time.time() + PUNCH_TIMEOUT
        while time.time() < deadline and not closed and not active:
            send_json(sock, peer, {"type": "punch", "session": session_id, "token": token})
            send_json(sock, peer, {"type": "hello", "session": session_id, "token": token, "role": "client"})
            time.sleep(PUNCH_EVERY)

    def keepalive_loop():
        nonlocal last_seen
        while not closed:
            if time.time() - last_seen > SESSION_TIMEOUT:
                print("session timeout", file=sys.stderr)
                close("timeout")
            if peer and session_id and token:
                send_json(sock, peer, {"type": "keepalive", "session": session_id, "token": token})
            time.sleep(KEEPALIVE_EVERY)

    threading.Thread(target=punch_loop, daemon=True).start()
    threading.Thread(target=keepalive_loop, daemon=True).start()

    while True:
        readable, _, _ = select.select([sock, sys.stdin], [], [], 0.1)
        if sys.stdin in readable:
            chunk = os.read(sys.stdin.fileno(), 4096)
            if not chunk:
                close("stdin_eof")
            send_json(sock, peer, {"type": "stdin", "session": session_id, "token": token, "data": encode_data(chunk)})
        if sock in readable:
            data, _ = sock.recvfrom(MAX_PACKET_SIZE)
            msg = recv_json(data)
            if msg.get("session") != session_id or msg.get("token") != token:
                continue
            last_seen = time.time()
            msg_type = msg.get("type")
            if msg_type == "punch":
                send_json(sock, peer, {"type": "hello", "session": session_id, "token": token, "role": "client"})
            elif msg_type in ("hello", "hello_ack"):
                active = True
                send_json(sock, peer, {"type": "hello_ack", "session": session_id, "token": token})
            elif msg_type == "stdout":
                os.write(sys.stdout.fileno(), decode_data(msg.get("data", "")))
            elif msg_type == "keepalive":
                send_json(sock, peer, {"type": "keepalive", "session": session_id, "token": token})
            elif msg_type == "close":
                close(msg.get("reason", "peer_close"))


if __name__ == "__main__":
    main()
