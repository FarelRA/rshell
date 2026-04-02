#!/usr/bin/env python3
import argparse
import ctypes
import os
import pty
import select
import signal
import socket
import sys
import threading
import time

from common import CHUNK_SIZE, KEEPALIVE_EVERY, MAX_PACKET_SIZE, PUNCH_EVERY, PUNCH_TIMEOUT, REGISTER_EVERY, ReliableReceiver, ReliableSender, SESSION_TIMEOUT, decode_data, encode_data, has_version, log_event, parse_host_port, positive_int, recv_json, send_json, set_winsize


class Session:
    def __init__(self, session_id, token, peer):
        self.id = session_id
        self.token = token
        self.peer = peer
        self.active = False
        self.closed = False
        self.last_seen = time.time()
        self.pty_fd = None
        self.child_pid = None
        self.term = "xterm-256color"
        self.cols = 80
        self.rows = 24
        self.stdin_rx = ReliableReceiver()
        self.pty_tx = ReliableSender("pty")


def main():
    try:
        ctypes.CDLL(None).prctl(15, b"rshell-python-h", 0, 0, 0)
    except Exception:
        pass
    parser = argparse.ArgumentParser()
    parser.add_argument("--rendezvous", default="127.0.0.1:4000")
    parser.add_argument("--service", default="demo-shell")
    parser.add_argument("--listen", default="0.0.0.0:0")
    parser.add_argument("--shell", default=os.environ.get("SHELL", "/bin/sh"))
    args = parser.parse_args()

    host, port = parse_host_port(args.listen)
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.bind((host, port))
    sock.setblocking(False)

    rdv = parse_host_port(args.rendezvous)
    sessions = {}
    lock = threading.Lock()
    running = True

    def register():
        send_json(sock, rdv, {"type": "register", "service": args.service, "meta": {"impl": "python-host"}})

    def remove_session(session_id):
        with lock:
            sessions.pop(session_id, None)

    def capture_terminal(sess, msg):
        sess.term = msg.get("term") or sess.term
        sess.cols = positive_int(msg.get("cols"), sess.cols)
        sess.rows = positive_int(msg.get("rows"), sess.rows)

    def close_session(sess, reason="close"):
        if sess.closed:
            return
        sess.closed = True
        try:
            send_json(sock, sess.peer, {"type": "close", "session": sess.id, "token": sess.token, "reason": reason})
        except OSError:
            pass
        if sess.pty_fd is not None:
            try:
                os.close(sess.pty_fd)
            except OSError:
                pass
            sess.pty_fd = None
        if sess.child_pid:
            try:
                os.kill(sess.child_pid, signal.SIGKILL)
            except OSError:
                pass
            sess.child_pid = None
        sess.active = False

    def start_shell(sess):
        if sess.active or sess.closed:
            return
        try:
            pid, fd = pty.fork()
        except OSError:
            log_event("error", "python", "host", "shell_start_failed", "failed to start shell", session=sess.id)
            close_session(sess, "shell_start_failed")
            return
        if pid == 0:
            os.environ["TERM"] = sess.term
            os.environ["TERM_PROGRAM"] = "rshell-python-host"
            os.environ["TERMINAL"] = "rshell-python-host"
            try:
                os.execvp(args.shell, ["rshell-python-host", "-i"])
            except OSError:
                os._exit(1)
        sess.child_pid = pid
        sess.pty_fd = fd
        set_winsize(fd, sess.rows, sess.cols)
        sess.active = True

        def pump_stdout():
            while not sess.closed:
                try:
                    chunk = os.read(fd, CHUNK_SIZE)
                except OSError:
                    break
                if chunk:
                    try:
                        send_json(sock, sess.peer, sess.pty_tx.push(sess.id, sess.token, chunk))
                    except OSError:
                        pass
                    continue
                break
            close_session(sess, "shell_exit")
            remove_session(sess.id)

        threading.Thread(target=pump_stdout, daemon=True).start()

        def reap_child():
            try:
                os.waitpid(pid, 0)
            except ChildProcessError:
                pass
            sess.child_pid = None
            close_session(sess, "shell_exit")
            remove_session(sess.id)

        threading.Thread(target=reap_child, daemon=True).start()

    def punch_loop(sess):
        deadline = time.time() + PUNCH_TIMEOUT
        while time.time() < deadline and not sess.closed and not sess.active:
            try:
                send_json(sock, sess.peer, {"type": "punch", "session": sess.id, "token": sess.token})
                send_json(sock, sess.peer, {"type": "hello", "session": sess.id, "token": sess.token, "role": "host"})
            except OSError:
                pass
            time.sleep(PUNCH_EVERY)

    def housekeeping():
        while running:
            now = time.time()
            try:
                send_json(sock, rdv, {"type": "keepalive", "service": args.service})
            except OSError:
                pass
            with lock:
                for key, sess in list(sessions.items()):
                    if sess.closed or now - sess.last_seen > SESSION_TIMEOUT:
                        close_session(sess, "timeout")
                        sessions.pop(key, None)
                        continue
                    if sess.active:
                        try:
                            send_json(sock, sess.peer, {"type": "keepalive", "session": sess.id, "token": sess.token})
                            for msg in sess.pty_tx.retransmit(sess.id, sess.token):
                                send_json(sock, sess.peer, msg)
                        except OSError:
                            pass
            time.sleep(KEEPALIVE_EVERY)

    def registration_loop():
        while running:
            try:
                register()
            except OSError:
                pass
            time.sleep(REGISTER_EVERY)

    threading.Thread(target=registration_loop, daemon=True).start()
    threading.Thread(target=housekeeping, daemon=True).start()
    register()

    def stop(_sig, _frame):
        nonlocal running
        running = False
        with lock:
            for sess in list(sessions.values()):
                close_session(sess, "signal")
        sys.exit(0)

    signal.signal(signal.SIGINT, stop)
    signal.signal(signal.SIGTERM, stop)

    while True:
        readable, _, _ = select.select([sock], [], [], 0.5)
        if sock not in readable:
            continue

        try:
            data, _addr = sock.recvfrom(MAX_PACKET_SIZE)
            msg = recv_json(data)
        except (BlockingIOError, ValueError):
            continue
        if not has_version(msg):
            continue

        msg_type = msg.get("type")
        if msg_type == "register_ok":
            log_event("info", "python", "host", "registered", "registration confirmed", service=args.service, public_addr=msg.get("public_addr"))
            continue
        if msg_type == "connect_intro":
            sess = Session(msg["session"], msg["token"], parse_host_port(msg["peer_addr"]))
            with lock:
                sessions[sess.id] = sess
            threading.Thread(target=punch_loop, args=(sess,), daemon=True).start()
            log_event("info", "python", "host", "session_intro", "received session intro", service=args.service, session=sess.id, peer=f"{sess.peer[0]}:{sess.peer[1]}")
            continue

        session_id = msg.get("session")
        if not session_id:
            continue
        with lock:
            sess = sessions.get(session_id)
        if not sess or sess.closed or sess.token != msg.get("token"):
            continue
        sess.last_seen = time.time()

        if msg_type == "punch":
            send_json(sock, sess.peer, {"type": "hello", "session": sess.id, "token": sess.token, "role": "host"})
        elif msg_type == "hello":
            capture_terminal(sess, msg)
            start_shell(sess)
            send_json(sock, sess.peer, {"type": "hello_ack", "session": sess.id, "token": sess.token})
        elif msg_type == "hello_ack":
            start_shell(sess)
        elif msg_type == "data" and msg.get("stream") == "stdin":
            start_shell(sess)
            if sess.pty_fd is not None:
                try:
                    chunks, ack = sess.stdin_rx.accept(positive_int(msg.get("seq"), 0), decode_data(msg.get("data", "")))
                    for delivered in chunks:
                        os.write(sess.pty_fd, delivered)
                    send_json(sock, sess.peer, {"type": "ack", "session": sess.id, "token": sess.token, "stream": "stdin", "ack": ack})
                except (OSError, ValueError):
                    log_event("error", "python", "host", "invalid_payload", "dropped invalid stdin payload", session=sess.id)
                    continue
        elif msg_type == "ack" and msg.get("stream") == "pty":
            sess.pty_tx.ack(positive_int(msg.get("ack"), 0))
        elif msg_type == "resize":
            capture_terminal(sess, msg)
            try:
                set_winsize(sess.pty_fd, sess.rows, sess.cols)
            except OSError:
                remove_session(sess.id)
        elif msg_type == "keepalive":
            pass
        elif msg_type == "close":
            with lock:
                close_session(sess, msg.get("reason", "peer_close"))
                sessions.pop(sess.id, None)


if __name__ == "__main__":
    main()
