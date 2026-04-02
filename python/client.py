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

from common import CHUNK_SIZE, INTRO_EVERY, KEEPALIVE_EVERY, MAX_PACKET_SIZE, PUNCH_EVERY, PUNCH_TIMEOUT, ReliableReceiver, ReliableSender, SESSION_TIMEOUT, decode_data, has_version, log_event, parse_host_port, recv_json, send_json, terminal_info


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--rendezvous", default="127.0.0.1:4000")
    parser.add_argument("--service", default="demo-shell")
    parser.add_argument("--listen", default="0.0.0.0:0")
    args = parser.parse_args()

    host, port = parse_host_port(args.listen)
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.bind((host, port))
    sock.setblocking(False)

    rdv = parse_host_port(args.rendezvous)
    def send_connect_request():
        send_json(sock, rdv, {"type": "connect_request", "service": args.service, "meta": {"impl": "python-client"}})

    send_connect_request()

    peer = None
    session_id = None
    token = None
    active = False
    last_seen = time.time()
    closed = False
    close_reason = None
    exit_code = 0
    stop_event = threading.Event()
    suspend_lock = threading.Lock()
    resize_timer = None
    stdin_sender = ReliableSender("stdin")
    pty_receiver = ReliableReceiver()

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

    def request_resize():
        nonlocal resize_timer
        with suspend_lock:
            if resize_timer is not None:
                resize_timer.cancel()
            resize_timer = threading.Timer(0.05, send_resize)
            resize_timer.daemon = True
            resize_timer.start()

    def begin_close(reason="close", notify=True):
        nonlocal closed, close_reason, exit_code
        if closed:
            return
        closed = True
        close_reason = reason
        if reason == "timeout":
            exit_code = 1
        stop_event.set()
        if notify and peer and session_id and token:
            try:
                send_json(sock, peer, {"type": "close", "session": session_id, "token": token, "reason": reason})
            except OSError:
                pass

    def stop(sig, _frame):
        nonlocal raw_mode
        if sig == signal.SIGWINCH:
            try:
                request_resize()
            except OSError:
                pass
            return
        if sig == signal.SIGTSTP:
            restore()
            raw_mode = False
            signal.signal(signal.SIGTSTP, signal.SIG_DFL)
            os.kill(os.getpid(), signal.SIGTSTP)
            signal.signal(signal.SIGTSTP, stop)
            return
        if sig == signal.SIGCONT:
            if old_attrs is not None and not closed:
                tty.setraw(sys.stdin.fileno())
                raw_mode = True
                request_resize()
            return
        if sig == signal.SIGINT and raw_mode:
            return
        begin_close("signal")

    signal.signal(signal.SIGINT, stop)
    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGWINCH, stop)
    signal.signal(signal.SIGTSTP, stop)
    signal.signal(signal.SIGCONT, stop)

    intro_deadline = time.time() + PUNCH_TIMEOUT
    while peer is None and not stop_event.is_set() and time.time() < intro_deadline:
        readable, _, _ = select.select([sock], [], [], INTRO_EVERY)
        if sock not in readable:
            try:
                send_connect_request()
            except OSError:
                pass
            continue
        try:
            data, _ = sock.recvfrom(MAX_PACKET_SIZE)
            msg = recv_json(data)
        except (BlockingIOError, ValueError):
            continue
        if not has_version(msg):
            continue
        if msg.get("type") == "error":
            log_event("error", "python", "client", "server_error", "rendezvous returned an error", code=msg.get("code"), detail=msg.get("message"))
            raise SystemExit(f"{msg.get('code')}: {msg.get('message')}")
        if msg.get("type") == "connect_intro":
            peer = parse_host_port(msg["peer_addr"])
            session_id = msg["session"]
            token = msg["token"]
            log_event("info", "python", "client", "session_intro", "received session intro", service=args.service, session=session_id, peer=f"{peer[0]}:{peer[1]}")

    if peer is None:
        restore()
        log_event("error", "python", "client", "intro_timeout", "timed out waiting for rendezvous intro", service=args.service)
        raise SystemExit(exit_code)

    if old_attrs is not None:
        tty.setraw(sys.stdin.fileno())
        raw_mode = True
    request_resize()

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
                log_event("error", "python", "client", "session_timeout", "session timed out", session=session_id)
                begin_close("timeout")
                return
            if peer and session_id and token:
                send_json(sock, peer, {"type": "keepalive", "session": session_id, "token": token})
            stop_event.wait(KEEPALIVE_EVERY)

    def retransmit_loop():
        while not stop_event.is_set():
            if peer and session_id and token:
                for msg in stdin_sender.retransmit(session_id, token):
                    send_json(sock, peer, msg)
            stop_event.wait(0.2)

    threading.Thread(target=punch_loop, daemon=True).start()
    threading.Thread(target=keepalive_loop, daemon=True).start()
    threading.Thread(target=retransmit_loop, daemon=True).start()

    try:
        while not stop_event.is_set():
            watch = [sock]
            if not closed:
                watch.append(sys.stdin)
            readable, _, _ = select.select(watch, [], [], 1.0)
            if sys.stdin in readable:
                chunk = os.read(sys.stdin.fileno(), CHUNK_SIZE)
                if not chunk:
                    begin_close("stdin_eof")
                    continue
                send_json(sock, peer, stdin_sender.push(session_id, token, chunk))
            if sock in readable:
                try:
                    data, _ = sock.recvfrom(MAX_PACKET_SIZE)
                    msg = recv_json(data)
                except (BlockingIOError, ValueError):
                    continue
                if not has_version(msg):
                    continue
                if msg.get("session") != session_id or msg.get("token") != token:
                    continue
                last_seen = time.time()
                msg_type = msg.get("type")
                if msg_type == "punch":
                    send_json(sock, peer, hello_message())
                elif msg_type in ("hello", "hello_ack"):
                    active = True
                    send_json(sock, peer, {"type": "hello_ack", "session": session_id, "token": token})
                elif msg_type == "data" and msg.get("stream") == "pty":
                    try:
                        chunks, ack = pty_receiver.accept(int(msg.get("seq") or 0), decode_data(msg.get("data", "")))
                        for delivered in chunks:
                            os.write(sys.stdout.fileno(), delivered)
                        send_json(sock, peer, {"type": "ack", "session": session_id, "token": token, "stream": "pty", "ack": ack})
                    except ValueError:
                        log_event("error", "python", "client", "invalid_payload", "dropped invalid stdout payload", session=session_id)
                        continue
                elif msg_type == "ack" and msg.get("stream") == "stdin":
                    stdin_sender.ack(int(msg.get("ack") or 0))
                elif msg_type == "keepalive":
                    pass
                elif msg_type == "close":
                    log_event("info", "python", "client", "session_closed", "session closed by peer", session=session_id, reason=msg.get("reason", "peer_close"))
                    begin_close(msg.get("reason", "peer_close"), notify=False)
    finally:
        restore()
        if exit_code != 0:
            raise SystemExit(exit_code)


if __name__ == "__main__":
    main()
