#!/usr/bin/env python3
import argparse
import os
import select
import signal
import socket
import sys
import termios
import threading
import time
import tty

from common import KEEPALIVE_EVERY, MAX_PACKET_SIZE, PUNCH_EVERY, PUNCH_TIMEOUT, SESSION_TIMEOUT, decode_data, encode_data, parse_host_port, recv_json, send_json, terminal_info


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
    close_reason = None
    stop_event = threading.Event()

    old_attrs = None
    raw_mode = False
    if sys.stdin.isatty():
        old_attrs = termios.tcgetattr(sys.stdin.fileno())

    def restore():
        if old_attrs is not None:
            termios.tcsetattr(sys.stdin.fileno(), termios.TCSADRAIN, old_attrs)

    def hello_message():
        payload = {"type": "hello", "session": session_id, "token": token, "role": "client"}
        payload.update(terminal_info())
        return payload

    def send_resize():
        if peer and session_id and token:
            payload = {"type": "resize", "session": session_id, "token": token}
            payload.update(terminal_info())
            send_json(sock, peer, payload)

    def begin_close(reason="close", notify=True):
        nonlocal closed, close_reason
        if closed:
            return
        closed = True
        close_reason = reason
        stop_event.set()
        if notify and peer and session_id and token:
            try:
                send_json(sock, peer, {"type": "close", "session": session_id, "token": token, "reason": reason})
            except OSError:
                pass

    def stop(sig, _frame):
        if sig == signal.SIGWINCH:
            try:
                send_resize()
            except OSError:
                pass
            return
        if sig == signal.SIGINT and raw_mode:
            return
        begin_close("signal")

    signal.signal(signal.SIGINT, stop)
    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGWINCH, stop)

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
        raw_mode = True
    send_resize()

    def punch_loop():
        deadline = time.time() + PUNCH_TIMEOUT
        while time.time() < deadline and not stop_event.is_set() and not active:
            send_json(sock, peer, {"type": "punch", "session": session_id, "token": token})
            send_json(sock, peer, hello_message())
            time.sleep(PUNCH_EVERY)

    def keepalive_loop():
        nonlocal last_seen
        while not stop_event.is_set():
            if time.time() - last_seen > SESSION_TIMEOUT:
                print("session timeout", file=sys.stderr)
                begin_close("timeout")
                return
            if peer and session_id and token:
                send_json(sock, peer, {"type": "keepalive", "session": session_id, "token": token})
            stop_event.wait(KEEPALIVE_EVERY)

    threading.Thread(target=punch_loop, daemon=True).start()
    threading.Thread(target=keepalive_loop, daemon=True).start()

    try:
        while not stop_event.is_set():
            watch = [sock]
            if not closed:
                watch.append(sys.stdin)
            readable, _, _ = select.select(watch, [], [], 0.1)
            if sys.stdin in readable:
                chunk = os.read(sys.stdin.fileno(), 4096)
                if not chunk:
                    begin_close("stdin_eof")
                    continue
                send_json(sock, peer, {"type": "stdin", "session": session_id, "token": token, "data": encode_data(chunk)})
            if sock in readable:
                data, _ = sock.recvfrom(MAX_PACKET_SIZE)
                msg = recv_json(data)
                if msg.get("session") != session_id or msg.get("token") != token:
                    continue
                last_seen = time.time()
                msg_type = msg.get("type")
                if msg_type == "punch":
                    send_json(sock, peer, hello_message())
                elif msg_type in ("hello", "hello_ack"):
                    active = True
                    send_json(sock, peer, {"type": "hello_ack", "session": session_id, "token": token})
                elif msg_type == "stdout":
                    os.write(sys.stdout.fileno(), decode_data(msg.get("data", "")))
                elif msg_type == "keepalive":
                    pass
                elif msg_type == "close":
                    begin_close(msg.get("reason", "peer_close"), notify=False)
    finally:
        restore()
        if close_reason == "timeout":
            raise SystemExit(1)


if __name__ == "__main__":
    main()
