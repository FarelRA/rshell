package main

import (
	"flag"
	"io"
	"log"
	"net"
	"os"
	"os/exec"
	"sync"
	"time"

	"rshell/go/common"
)

type session struct {
	id          string
	token       string
	peer        *net.UDPAddr
	active      bool
	lastSeen    time.Time
	cmd         *exec.Cmd
	stdin       io.WriteCloser
	stdout      io.ReadCloser
	closed      bool
	closeOnce   sync.Once
	shutdownCh  chan struct{}
	punchUntil  time.Time
	punchTicker *time.Ticker
}

func main() {
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
		log.Fatalf("resolve local addr: %v", err)
	}
	conn, err := net.ListenUDP("udp", localAddr)
	if err != nil {
		log.Fatalf("listen udp: %v", err)
	}
	defer conn.Close()

	rdvAddr, err := net.ResolveUDPAddr("udp", *rendezvous)
	if err != nil {
		log.Fatalf("resolve rendezvous: %v", err)
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
			log.Printf("register error: %v", err)
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
			mu.Lock()
			now := time.Now()
			for id, sess := range sessions {
				if sess.closed || now.Sub(sess.lastSeen) > common.SessionTimeout {
					closeSession(conn, sess, "timeout")
					delete(sessions, id)
					continue
				}
				if sess.active {
					_ = common.SendJSON(conn, sess.peer, common.Message{"type": "keepalive", "session": sess.id, "token": sess.token})
				}
			}
			mu.Unlock()
			_ = common.SendJSON(conn, rdvAddr, common.Message{"type": "keepalive", "service": *service})
		}
	}()

	buf := make([]byte, common.MaxPacketSize)
	for {
		n, addr, err := conn.ReadFromUDP(buf)
		if err != nil {
			log.Printf("read error: %v", err)
			continue
		}

		msg, err := common.DecodeJSON(buf[:n])
		if err != nil {
			log.Printf("decode error from %s: %v", addr, err)
			continue
		}

		typeName := common.String(msg, "type")
		switch typeName {
		case "register_ok":
			log.Printf("registered service=%s public_addr=%s", *service, common.String(msg, "public_addr"))
		case "connect_intro":
			peerAddr, err := net.ResolveUDPAddr("udp", common.String(msg, "peer_addr"))
			if err != nil {
				log.Printf("bad peer addr: %v", err)
				continue
			}
			sess := &session{id: common.String(msg, "session"), token: common.String(msg, "token"), peer: peerAddr, lastSeen: time.Now(), shutdownCh: make(chan struct{}), punchUntil: time.Now().Add(common.PunchTimeout)}
			mu.Lock()
			sessions[sess.id] = sess
			mu.Unlock()
			go punchLoop(conn, sess, "host")
			log.Printf("intro session=%s peer=%s", sess.id, peerAddr)
		case "punch", "hello", "hello_ack", "stdin", "keepalive", "close", "resize":
			sid := common.String(msg, "session")
			mu.Lock()
			sess := sessions[sid]
			mu.Unlock()
			if sess == nil || sess.token != common.String(msg, "token") {
				continue
			}
			sess.lastSeen = time.Now()
			switch typeName {
			case "punch":
				_ = common.SendJSON(conn, sess.peer, common.Message{"type": "hello", "session": sess.id, "token": sess.token, "role": "host"})
			case "hello":
				activateHostSession(conn, sess, *shell)
				_ = common.SendJSON(conn, sess.peer, common.Message{"type": "hello_ack", "session": sess.id, "token": sess.token})
			case "hello_ack":
				activateHostSession(conn, sess, *shell)
			case "stdin":
				activateHostSession(conn, sess, *shell)
				if sess.stdin != nil {
					_, _ = sess.stdin.Write(common.DecodeData(common.String(msg, "data")))
				}
			case "keepalive":
				_ = common.SendJSON(conn, sess.peer, common.Message{"type": "keepalive", "session": sess.id, "token": sess.token})
			case "resize":
				// Resize is intentionally best-effort in this version.
			case "close":
				mu.Lock()
				closeSession(conn, sess, common.String(msg, "reason"))
				delete(sessions, sid)
				mu.Unlock()
			}
		case "error":
			log.Printf("server error code=%s message=%s", common.String(msg, "code"), common.String(msg, "message"))
		}
	}
}

func punchLoop(conn *net.UDPConn, sess *session, role string) {
	ticker := time.NewTicker(common.PunchEvery)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			if time.Now().After(sess.punchUntil) || sess.closed || sess.active {
				return
			}
			_ = common.SendJSON(conn, sess.peer, common.Message{"type": "punch", "session": sess.id, "token": sess.token})
			_ = common.SendJSON(conn, sess.peer, common.Message{"type": "hello", "session": sess.id, "token": sess.token, "role": role})
		case <-sess.shutdownCh:
			return
		}
	}
}

func activateHostSession(conn *net.UDPConn, sess *session, shell string) {
	if sess.active || sess.closed {
		return
	}
	cmd := exec.Command("script", "-qfc", shell+" -i", "/dev/null")
	stdin, err := cmd.StdinPipe()
	if err != nil {
		log.Printf("stdin pipe error: %v", err)
		return
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		log.Printf("stdout pipe error: %v", err)
		return
	}
	cmd.Stderr = cmd.Stdout
	if err := cmd.Start(); err != nil {
		log.Printf("shell start error: %v", err)
		return
	}
	sess.active = true
	sess.cmd = cmd
	sess.stdin = stdin
	sess.stdout = stdout

	go func() {
		buf := make([]byte, 4096)
		for {
			n, err := stdout.Read(buf)
			if n > 0 && !sess.closed {
				_ = common.SendJSON(conn, sess.peer, common.Message{"type": "stdout", "session": sess.id, "token": sess.token, "data": common.EncodeData(buf[:n])})
			}
			if err != nil {
				if err != io.EOF {
					log.Printf("shell stdout error: %v", err)
				}
				closeSession(conn, sess, "shell_exit")
				return
			}
		}
	}()

	go func() {
		if err := cmd.Wait(); err != nil {
			log.Printf("shell wait error: %v", err)
		}
		closeSession(conn, sess, "shell_exit")
	}()
}

func closeSession(conn *net.UDPConn, sess *session, reason string) {
	if sess == nil {
		return
	}
	sess.closeOnce.Do(func() {
		sess.closed = true
		close(sess.shutdownCh)
		_ = common.SendJSON(conn, sess.peer, common.Message{"type": "close", "session": sess.id, "token": sess.token, "reason": reason})
		if sess.stdin != nil {
			_ = sess.stdin.Close()
		}
		if sess.stdout != nil {
			_ = sess.stdout.Close()
		}
		if sess.cmd != nil && sess.cmd.Process != nil {
			_ = sess.cmd.Process.Kill()
		}
	})
}
