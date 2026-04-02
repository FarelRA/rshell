package main

import (
	"flag"
	"net"
	"os"
	"os/signal"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"rshell/go/common"
)

func main() {
	logger := common.NewLogger("go", "client")
	rendezvous := flag.String("rendezvous", "127.0.0.1:4000", "rendezvous host:port")
	service := flag.String("service", "demo-shell", "service name")
	listen := flag.String("listen", ":0", "local UDP listen address")
	flag.Parse()

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

	sendConnectRequest := func() error {
		return common.SendJSON(conn, rdvAddr, common.Message{"type": "connect_request", "service": *service, "meta": common.Message{"impl": "go-client"}})
	}
	if err := sendConnectRequest(); err != nil {
		logger.Error("startup_failed", "failed to send connect request", "service", *service, "error", err.Error())
		os.Exit(1)
	}

	buf := make([]byte, common.MaxPacketSize)
	var sessionID, token string
	var peer *net.UDPAddr
	var active atomic.Bool
	var lastSeen atomic.Int64
	lastSeen.Store(time.Now().UnixNano())
	var exitCode atomic.Int32
	done := make(chan struct{})
	var doneOnce sync.Once
	var suspended atomic.Bool
	stdinSender := common.NewReliableSender("stdin")
	ptyReceiver := common.NewReliableReceiver()
	closeDone := func() { doneOnce.Do(func() { close(done) }) }
	requestClose := func(reason string, code int32, notify bool) {
		if code != 0 {
			exitCode.Store(code)
		}
		if notify && peer != nil && sessionID != "" && token != "" {
			_ = common.SendJSON(conn, peer, common.Message{"type": "close", "session": sessionID, "token": token, "reason": reason})
		}
		closeDone()
	}
	go func() {
		<-done
		_ = conn.SetReadDeadline(time.Now())
	}()

	introDeadline := time.Now().Add(common.PunchTimeout)
	for peer == nil && time.Now().Before(introDeadline) {
		_ = conn.SetReadDeadline(time.Now().Add(common.IntroEvery))
		n, _, err := conn.ReadFromUDP(buf)
		if err != nil {
			if ne, ok := err.(net.Error); ok && ne.Timeout() {
				_ = sendConnectRequest()
				continue
			}
			logger.Error("read_failed", "failed while waiting for intro", "error", err.Error())
			os.Exit(1)
		}
		msg, err := common.DecodeJSON(buf[:n])
		if err != nil {
			continue
		}
		if !common.ValidVersion(msg) {
			continue
		}
		switch common.String(msg, "type") {
		case "connect_intro":
			peer, err = net.ResolveUDPAddr("udp", common.String(msg, "peer_addr"))
			if err != nil {
				logger.Error("startup_failed", "failed to resolve peer address", "peer_addr", common.String(msg, "peer_addr"), "error", err.Error())
				os.Exit(1)
			}
			sessionID = common.String(msg, "session")
			token = common.String(msg, "token")
			logger.Info("session_intro", "received session intro", "service", *service, "session", sessionID, "peer", peer.String())
			go func() {
				ticker := time.NewTicker(common.PunchEvery)
				defer ticker.Stop()
				deadline := time.Now().Add(common.PunchTimeout)
				for time.Now().Before(deadline) && !active.Load() {
					<-ticker.C
					_ = common.SendJSON(conn, peer, common.Message{"type": "punch", "session": sessionID, "token": token})
					_ = common.SendJSON(conn, peer, helloMessage(sessionID, token))
				}
			}()
		case "error":
			logger.Error("server_error", "rendezvous returned an error", "code", common.String(msg, "code"), "detail", common.String(msg, "message"))
			os.Exit(1)
		}
	}
	_ = conn.SetReadDeadline(time.Time{})
	if peer == nil {
		logger.Error("intro_timeout", "timed out waiting for rendezvous intro", "service", *service)
		os.Exit(1)
	}

	var rawState *common.TerminalState
	restoreRaw := func() {
		_ = common.RestoreTerminal(rawState)
	}
	makeRaw := func() {
		if rawState == nil && common.IsTerminal() {
			state, rawErr := common.MakeRaw()
			if rawErr != nil {
				logger.Error("raw_mode_unavailable", "failed to enable raw mode", "error", rawErr.Error())
				return
			}
			rawState = state
		}
	}
	if common.IsTerminal() {
		makeRaw()
	}
	defer func() {
		restoreRaw()
		if code := exitCode.Load(); code != 0 {
			os.Exit(int(code))
		}
	}()
	resizeReq := make(chan struct{}, 1)
	requestResize := func() {
		select {
		case resizeReq <- struct{}{}:
		default:
		}
	}
	go func() {
		for range resizeReq {
			timer := time.NewTimer(50 * time.Millisecond)
			for {
				select {
				case <-resizeReq:
					if !timer.Stop() {
						<-timer.C
					}
					timer.Reset(50 * time.Millisecond)
				case <-timer.C:
					sendResize(conn, peer, sessionID, token)
					goto nextResize
				}
			}
		nextResize:
		}
	}()
	requestResize()
	stdoutCh := make(chan []byte, 64)
	go func() {
		for chunk := range stdoutCh {
			_, _ = os.Stdout.Write(chunk)
		}
	}()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM, syscall.SIGWINCH, syscall.SIGTSTP, syscall.SIGCONT)
	go func() {
		for sig := range sigCh {
			if sig == syscall.SIGWINCH {
				requestResize()
				continue
			}
			if sig == syscall.SIGTSTP {
				suspended.Store(true)
				restoreRaw()
				signal.Reset(syscall.SIGTSTP)
				_ = syscall.Kill(os.Getpid(), syscall.SIGTSTP)
				signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM, syscall.SIGWINCH, syscall.SIGTSTP, syscall.SIGCONT)
				continue
			}
			if sig == syscall.SIGCONT {
				if suspended.Load() {
					suspended.Store(false)
					makeRaw()
					requestResize()
				}
				continue
			}
			requestClose("signal", 0, true)
			return
		}
	}()

	go func() {
		in := make([]byte, common.MaxDataChunk)
		for {
			n, err := os.Stdin.Read(in)
			if n > 0 {
				_ = common.SendJSON(conn, peer, stdinSender.Push(in[:n], sessionID, token))
			}
			if err != nil {
				requestClose("stdin_eof", 0, true)
				return
			}
		}
	}()

	go func() {
		keepaliveTicker := time.NewTicker(common.KeepaliveEvery)
		retransmitTicker := time.NewTicker(common.RetransmitEvery)
		defer keepaliveTicker.Stop()
		defer retransmitTicker.Stop()
		for {
			select {
			case <-keepaliveTicker.C:
				if time.Since(time.Unix(0, lastSeen.Load())) > common.SessionTimeout {
					logger.Error("session_timeout", "session timed out", "session", sessionID)
					requestClose("timeout", 1, true)
					return
				}
				_ = common.SendJSON(conn, peer, common.Message{"type": "keepalive", "session": sessionID, "token": token})
			case <-retransmitTicker.C:
				for _, msg := range stdinSender.Retransmit(sessionID, token) {
					_ = common.SendJSON(conn, peer, msg)
				}
			case <-done:
				return
			}
		}
	}()

	for {
		n, _, err := conn.ReadFromUDP(buf)
		if err != nil {
			if ne, ok := err.(net.Error); ok && ne.Timeout() {
				select {
				case <-done:
					return
				default:
					continue
				}
			}
			logger.Error("read_failed", "failed to read peer packet", "error", err.Error())
			continue
		}
		msg, err := common.DecodeJSON(buf[:n])
		if err != nil {
			continue
		}
		if !common.ValidVersion(msg) {
			continue
		}
		if common.String(msg, "session") != sessionID || common.String(msg, "token") != token {
			continue
		}
		lastSeen.Store(time.Now().UnixNano())
		switch common.String(msg, "type") {
		case "punch":
			_ = common.SendJSON(conn, peer, helloMessage(sessionID, token))
		case "hello", "hello_ack":
			active.Store(true)
			_ = common.SendJSON(conn, peer, common.Message{"type": "hello_ack", "session": sessionID, "token": token})
		case "data":
			if common.String(msg, "stream") != "pty" {
				continue
			}
			chunk, decodeErr := common.DecodeData(common.String(msg, "data"))
			if decodeErr != nil {
				logger.Error("invalid_payload", "dropped invalid stdout payload", "session", sessionID, "error", decodeErr.Error())
				continue
			}
			chunks, ack := ptyReceiver.Accept(uint64(common.Int(msg, "seq")), chunk)
			for _, delivered := range chunks {
				stdoutCh <- delivered
			}
			_ = common.SendJSON(conn, peer, common.Message{"type": "ack", "session": sessionID, "token": token, "stream": "pty", "ack": ack})
		case "ack":
			if common.String(msg, "stream") == "stdin" {
				stdinSender.Ack(uint64(common.Int(msg, "ack")))
			}
		case "keepalive":
			// Receiving a keepalive is sufficient to refresh lastSeen.
		case "close":
			logger.Info("session_closed", "session closed by peer", "session", sessionID, "reason", common.String(msg, "reason"))
			requestClose(common.String(msg, "reason"), 0, false)
		}
	}
}

func helloMessage(sessionID, token string) common.Message {
	msg := common.Message{"type": "hello", "session": sessionID, "token": token, "role": "client"}
	for key, value := range common.TerminalInfo() {
		msg[key] = value
	}
	return msg
}

func sendResize(conn *net.UDPConn, peer *net.UDPAddr, sessionID, token string) {
	msg := common.Message{"type": "resize", "session": sessionID, "token": token}
	for key, value := range common.TerminalInfo() {
		msg[key] = value
	}
	_ = common.SendJSON(conn, peer, msg)
}
