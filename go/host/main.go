package main

import (
	"flag"
	"io"
	"net"
	"os"
	"os/exec"
	"sync"
	"time"
	"unsafe"

	"github.com/creack/pty"
	"golang.org/x/sys/unix"
	"rshell/go/common"
)

type session struct {
	id         string
	token      string
	peer       *net.UDPAddr
	shutdownCh chan struct{}
	punchUntil time.Time
	mu         sync.RWMutex
	active     bool
	lastSeen   time.Time
	cmd        *exec.Cmd
	stdin      io.WriteCloser
	stdinCh    chan []byte
	ptyFile    *os.File
	closed     bool
	term       string
	cols       uint16
	rows       uint16
	stdinRx    *common.ReliableReceiver
	ptyTx      *common.ReliableSender
	closeOnce  sync.Once
	onClose    func(string)
}

func main() {
	logger := common.NewLogger("go", "host")
	setProcessName("rshell-go-host")
	rendezvous := flag.String("rendezvous", "127.0.0.1:4000", "rendezvous host:port")
	service := flag.String("service", "demo-shell", "service name")
	listen := flag.String("listen", ":0", "local UDP listen address")
	shell := flag.String("shell", os.Getenv("SHELL"), "shell path")
	flag.Parse()

	if *shell == "" {
		*shell = "/bin/sh"
	}

	localAddr, err := net.ResolveUDPAddr("udp", *listen)
	if err != nil {
		logger.Error("startup_failed", "failed to resolve local address", "listen", *listen, "error", err.Error())
		os.Exit(1)
	}
	conn, err := net.ListenUDP("udp", localAddr)
	if err != nil {
		logger.Error("startup_failed", "failed to listen on udp socket", "listen", *listen, "error", err.Error())
		os.Exit(1)
	}
	defer conn.Close()

	rdvAddr, err := net.ResolveUDPAddr("udp", *rendezvous)
	if err != nil {
		logger.Error("startup_failed", "failed to resolve rendezvous address", "rendezvous", *rendezvous, "error", err.Error())
		os.Exit(1)
	}

	sessions := map[string]*session{}
	var mu sync.Mutex

	sendRegister := func() {
		err := common.SendJSON(conn, rdvAddr, common.Message{
			"type":    "register",
			"service": *service,
			"meta":    common.Message{"impl": "go-host"},
		})
		if err != nil {
			logger.Error("register_failed", "failed to refresh registration", "service", *service, "error", err.Error())
		}
	}

	sendRegister()
	go func() {
		ticker := time.NewTicker(common.RegisterEvery)
		defer ticker.Stop()
		for range ticker.C {
			sendRegister()
		}
	}()

	go func() {
		ticker := time.NewTicker(common.KeepaliveEvery)
		defer ticker.Stop()
		for range ticker.C {
			now := time.Now()
			mu.Lock()
			current := make([]*session, 0, len(sessions))
			for _, sess := range sessions {
				current = append(current, sess)
			}
			mu.Unlock()
			for _, sess := range current {
				active, closed, lastSeen := sess.state()
				if closed || now.Sub(lastSeen) > common.SessionTimeout {
					closeSession(conn, sess, "timeout")
					mu.Lock()
					delete(sessions, sess.id)
					mu.Unlock()
					continue
				}
				if active {
					_ = common.SendJSON(conn, sess.peer, common.Message{"type": "keepalive", "session": sess.id, "token": sess.token})
				}
			}
			_ = common.SendJSON(conn, rdvAddr, common.Message{"type": "keepalive", "service": *service})
		}
	}()

	buf := make([]byte, common.MaxPacketSize)
	for {
		n, addr, err := conn.ReadFromUDP(buf)
		if err != nil {
			logger.Error("read_failed", "failed to read udp packet", "error", err.Error())
			continue
		}

		msg, err := common.DecodeJSON(buf[:n])
		if err != nil {
			logger.Error("invalid_packet", "failed to decode udp packet", "peer", addr.String(), "error", err.Error())
			continue
		}
		if !common.ValidVersion(msg) {
			continue
		}

		switch common.String(msg, "type") {
		case "register_ok":
			logger.Info("registered", "registration confirmed", "service", *service, "public_addr", common.String(msg, "public_addr"))
		case "connect_intro":
			peerAddr, err := net.ResolveUDPAddr("udp", common.String(msg, "peer_addr"))
			if err != nil {
				logger.Error("invalid_packet", "received invalid peer address", "peer_addr", common.String(msg, "peer_addr"), "error", err.Error())
				continue
			}
			sid := common.String(msg, "session")
			sess := &session{id: sid, token: common.String(msg, "token"), peer: peerAddr, lastSeen: time.Now(), shutdownCh: make(chan struct{}), punchUntil: time.Now().Add(common.PunchTimeout), term: "xterm-256color", cols: 80, rows: 24, stdinRx: common.NewReliableReceiver(), ptyTx: common.NewReliableSender("pty")}
			sess.onClose = func(_ string) {
				mu.Lock()
				delete(sessions, sid)
				mu.Unlock()
			}
			mu.Lock()
			sessions[sess.id] = sess
			mu.Unlock()
			go punchLoop(conn, sess, "host")
			logger.Info("session_intro", "received session intro", "service", *service, "session", sess.id, "peer", peerAddr.String())
		case "punch", "hello", "hello_ack", "data", "ack", "keepalive", "close", "resize":
			sid := common.String(msg, "session")
			mu.Lock()
			sess := sessions[sid]
			mu.Unlock()
			if sess == nil || sess.token != common.String(msg, "token") {
				continue
			}
			sess.touch()
			switch common.String(msg, "type") {
			case "punch":
				_ = common.SendJSON(conn, sess.peer, common.Message{"type": "hello", "session": sess.id, "token": sess.token, "role": "host"})
			case "hello":
				captureTerminal(sess, msg)
				if !activateHostSession(conn, sess, *shell) {
					mu.Lock()
					delete(sessions, sid)
					mu.Unlock()
					continue
				}
				_ = common.SendJSON(conn, sess.peer, common.Message{"type": "hello_ack", "session": sess.id, "token": sess.token})
			case "hello_ack":
				if !activateHostSession(conn, sess, *shell) {
					mu.Lock()
					delete(sessions, sid)
					mu.Unlock()
				}
			case "data":
				if common.String(msg, "stream") != "stdin" {
					continue
				}
				if !activateHostSession(conn, sess, *shell) {
					mu.Lock()
					delete(sessions, sid)
					mu.Unlock()
					continue
				}
				if stdin := sess.stdinWriter(); stdin != nil {
					chunk, decodeErr := common.DecodeData(common.String(msg, "data"))
					if decodeErr != nil {
						logger.Error("invalid_payload", "dropped invalid stdin payload", "session", sid, "error", decodeErr.Error())
						continue
					}
					chunks, ack := sess.stdinRx.Accept(uint64(common.Int(msg, "seq")), chunk)
					for _, delivered := range chunks {
						sess.enqueueStdin(delivered)
					}
					_ = common.SendJSON(conn, sess.peer, common.Message{"type": "ack", "session": sess.id, "token": sess.token, "stream": "stdin", "ack": ack})
				}
			case "ack":
				if common.String(msg, "stream") == "pty" && sess.ptyTx != nil {
					sess.ptyTx.Ack(uint64(common.Int(msg, "ack")))
				}
			case "keepalive":
				// Receiving a keepalive is sufficient to refresh lastSeen.
			case "resize":
				captureTerminal(sess, msg)
				resizeSession(sess)
			case "close":
				closeSession(conn, sess, common.String(msg, "reason"))
			}
		case "error":
			logger.Error("server_error", "rendezvous returned an error", "code", common.String(msg, "code"), "detail", common.String(msg, "message"))
		}
	}
}

func punchLoop(conn *net.UDPConn, sess *session, role string) {
	ticker := time.NewTicker(common.PunchEvery)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			if !sess.shouldPunch(time.Now()) {
				return
			}
			_ = common.SendJSON(conn, sess.peer, common.Message{"type": "punch", "session": sess.id, "token": sess.token})
			_ = common.SendJSON(conn, sess.peer, common.Message{"type": "hello", "session": sess.id, "token": sess.token, "role": role})
		case <-sess.shutdownCh:
			return
		}
	}
}

func captureTerminal(sess *session, msg common.Message) {
	sess.mu.Lock()
	defer sess.mu.Unlock()
	if term := common.String(msg, "term"); term != "" {
		sess.term = term
	}
	if cols := common.Int(msg, "cols"); cols > 0 {
		sess.cols = uint16(cols)
	}
	if rows := common.Int(msg, "rows"); rows > 0 {
		sess.rows = uint16(rows)
	}
}

func activateHostSession(conn *net.UDPConn, sess *session, shell string) bool {
	term, cols, rows, active, closed := sess.startConfig()
	if active || closed {
		return true
	}
	cmd := exec.Command(shell, "-i")
	cmd.Env = append(os.Environ(),
		"TERM="+fallback(term, "xterm-256color"),
		"TERM_PROGRAM=rshell-go-host",
		"TERMINAL=rshell-go-host",
	)
	ptyFile, err := pty.StartWithSize(cmd, &pty.Winsize{Cols: atLeastOne(cols), Rows: atLeastOne(rows)})
	if err != nil {
		common.NewLogger("go", "host").Error("shell_start_failed", "failed to start shell", "session", sess.id, "error", err.Error())
		closeSession(conn, sess, "shell_start_failed")
		return false
	}
	stdinCh := make(chan []byte, 64)
	if !sess.markStarted(cmd, ptyFile) {
		_ = ptyFile.Close()
		_ = cmd.Process.Kill()
		return false
	}
	sess.setStdinChannel(stdinCh)

	go func() {
		for chunk := range stdinCh {
			if _, err := ptyFile.Write(chunk); err != nil {
				closeSession(conn, sess, "shell_exit")
				return
			}
		}
	}()

	go func() {
		ticker := time.NewTicker(common.RetransmitEvery)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				for _, msg := range sess.ptyTx.Retransmit(sess.id, sess.token) {
					_ = common.SendJSON(conn, sess.peer, msg)
				}
			case <-sess.shutdownCh:
				return
			}
		}
	}()

	go func() {
		buf := make([]byte, common.MaxDataChunk)
		for {
			n, err := ptyFile.Read(buf)
			if n > 0 && !sess.isClosed() {
				_ = common.SendJSON(conn, sess.peer, sess.ptyTx.Push(buf[:n], sess.id, sess.token))
			}
			if err != nil {
				if err != io.EOF && !sess.isClosed() {
					common.NewLogger("go", "host").Error("pty_read_failed", "failed to read pty output", "session", sess.id, "error", err.Error())
				}
				closeSession(conn, sess, "shell_exit")
				return
			}
		}
	}()

	go func() {
		if err := cmd.Wait(); err != nil && !sess.isClosed() {
			common.NewLogger("go", "host").Error("shell_wait_failed", "shell process exited with error", "session", sess.id, "error", err.Error())
		}
		closeSession(conn, sess, "shell_exit")
	}()
	return true
}

func resizeSession(sess *session) {
	if sess == nil {
		return
	}
	ptyFile, cols, rows := sess.resizeConfig()
	if ptyFile == nil || cols == 0 || rows == 0 {
		return
	}
	_ = pty.Setsize(ptyFile, &pty.Winsize{Cols: atLeastOne(cols), Rows: atLeastOne(rows)})
}

func closeSession(conn *net.UDPConn, sess *session, reason string) {
	if sess == nil {
		return
	}
	sess.closeOnce.Do(func() {
		sess.mu.Lock()
		sess.closed = true
		stdin := sess.stdin
		stdinCh := sess.stdinCh
		ptyFile := sess.ptyFile
		cmd := sess.cmd
		sess.stdin = nil
		sess.stdinCh = nil
		sess.ptyFile = nil
		sess.cmd = nil
		sess.active = false
		sess.mu.Unlock()
		close(sess.shutdownCh)
		_ = common.SendJSON(conn, sess.peer, common.Message{"type": "close", "session": sess.id, "token": sess.token, "reason": reason})
		if sess.onClose != nil {
			sess.onClose(reason)
		}
		if stdinCh != nil {
			close(stdinCh)
		}
		if stdin != nil {
			_ = stdin.Close()
		}
		if ptyFile != nil {
			_ = ptyFile.Close()
		}
		if cmd != nil && cmd.Process != nil {
			_ = cmd.Process.Kill()
		}
	})
}

func (s *session) state() (bool, bool, time.Time) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.active, s.closed, s.lastSeen
}

func (s *session) touch() {
	s.mu.Lock()
	s.lastSeen = time.Now()
	s.mu.Unlock()
}

func (s *session) shouldPunch(now time.Time) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return now.Before(s.punchUntil) && !s.closed && !s.active
}

func (s *session) startConfig() (string, uint16, uint16, bool, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.term, s.cols, s.rows, s.active, s.closed
}

func (s *session) markStarted(cmd *exec.Cmd, ptyFile *os.File) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed || s.active {
		return false
	}
	s.active = true
	s.cmd = cmd
	s.stdin = ptyFile
	s.ptyFile = ptyFile
	return true
}

func (s *session) stdinWriter() io.WriteCloser {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.stdin
}

func (s *session) setStdinChannel(stdinCh chan []byte) {
	s.mu.Lock()
	s.stdinCh = stdinCh
	s.mu.Unlock()
}

func (s *session) enqueueStdin(chunk []byte) {
	s.mu.RLock()
	ch := s.stdinCh
	closed := s.closed
	s.mu.RUnlock()
	if closed || ch == nil || len(chunk) == 0 {
		return
	}
	copyChunk := append([]byte(nil), chunk...)
	ch <- copyChunk
}

func (s *session) resizeConfig() (*os.File, uint16, uint16) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.ptyFile, s.cols, s.rows
}

func (s *session) isClosed() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.closed
}

func fallback(value, defaultValue string) string {
	if value != "" {
		return value
	}
	return defaultValue
}

func atLeastOne(value uint16) uint16 {
	if value == 0 {
		return 1
	}
	return value
}

func setProcessName(name string) {
	buf := make([]byte, 16)
	copy(buf, []byte(name))
	_ = unix.Prctl(unix.PR_SET_NAME, uintptr(unsafe.Pointer(&buf[0])), 0, 0, 0)
}
