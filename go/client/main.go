package main

import (
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"os/exec"
	"os/signal"
	"syscall"
	"time"

	"rshell/go/common"
)

func main() {
	rendezvous := flag.String("rendezvous", "127.0.0.1:4000", "rendezvous host:port")
	service := flag.String("service", "demo-shell", "service name")
	listen := flag.String("listen", ":0", "local UDP listen address")
	flag.Parse()

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

	if err := common.SendJSON(conn, rdvAddr, common.Message{"type": "connect_request", "service": *service, "meta": common.Message{"impl": "go-client"}}); err != nil {
		log.Fatalf("connect request: %v", err)
	}

	buf := make([]byte, common.MaxPacketSize)
	var sessionID, token string
	var peer *net.UDPAddr
	active := false
	lastSeen := time.Now()
	done := make(chan struct{})

	for !active {
		n, _, err := conn.ReadFromUDP(buf)
		if err != nil {
			log.Fatalf("read intro: %v", err)
		}
		msg, err := common.DecodeJSON(buf[:n])
		if err != nil {
			continue
		}
		switch common.String(msg, "type") {
		case "connect_intro":
			peer, err = net.ResolveUDPAddr("udp", common.String(msg, "peer_addr"))
			if err != nil {
				log.Fatalf("resolve peer addr: %v", err)
			}
			sessionID = common.String(msg, "session")
			token = common.String(msg, "token")
			go func() {
				ticker := time.NewTicker(common.PunchEvery)
				defer ticker.Stop()
				deadline := time.Now().Add(common.PunchTimeout)
				for time.Now().Before(deadline) && !active {
					<-ticker.C
					_ = common.SendJSON(conn, peer, common.Message{"type": "punch", "session": sessionID, "token": token})
					_ = common.SendJSON(conn, peer, common.Message{"type": "hello", "session": sessionID, "token": token, "role": "client"})
				}
			}()
		case "error":
			log.Fatalf("server error code=%s message=%s", common.String(msg, "code"), common.String(msg, "message"))
		}

		if peer != nil {
			break
		}
	}

	if peer == nil {
		log.Fatal("no peer introduction received")
	}

	restore, err := setRawMode()
	if err != nil {
		log.Printf("raw mode unavailable: %v", err)
		restore = func() {}
	}
	defer restore()
	sendResize(conn, peer, sessionID, token)

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigCh
		_ = common.SendJSON(conn, peer, common.Message{"type": "close", "session": sessionID, "token": token, "reason": "signal"})
		close(done)
	}()

	go func() {
		in := make([]byte, 4096)
		for {
			n, err := os.Stdin.Read(in)
			if n > 0 && peer != nil {
				_ = common.SendJSON(conn, peer, common.Message{"type": "stdin", "session": sessionID, "token": token, "data": common.EncodeData(in[:n])})
			}
			if err != nil {
				_ = common.SendJSON(conn, peer, common.Message{"type": "close", "session": sessionID, "token": token, "reason": "stdin_eof"})
				close(done)
				return
			}
		}
	}()

	go func() {
		ticker := time.NewTicker(common.KeepaliveEvery)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				if time.Since(lastSeen) > common.SessionTimeout {
					fmt.Fprintln(os.Stderr, "session timeout")
					close(done)
					return
				}
				_ = common.SendJSON(conn, peer, common.Message{"type": "keepalive", "session": sessionID, "token": token})
			case <-done:
				return
			}
		}
	}()

	for {
		conn.SetReadDeadline(time.Now().Add(1 * time.Second))
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
			log.Printf("read error: %v", err)
			continue
		}
		msg, err := common.DecodeJSON(buf[:n])
		if err != nil {
			continue
		}
		if common.String(msg, "session") != sessionID || common.String(msg, "token") != token {
			continue
		}
		lastSeen = time.Now()
		switch common.String(msg, "type") {
		case "punch":
			_ = common.SendJSON(conn, peer, common.Message{"type": "hello", "session": sessionID, "token": token, "role": "client"})
		case "hello", "hello_ack":
			active = true
			_ = common.SendJSON(conn, peer, common.Message{"type": "hello_ack", "session": sessionID, "token": token})
		case "stdout":
			_, _ = os.Stdout.Write(common.DecodeData(common.String(msg, "data")))
		case "keepalive":
			_ = common.SendJSON(conn, peer, common.Message{"type": "keepalive", "session": sessionID, "token": token})
		case "close":
			return
		}

		select {
		case <-done:
			return
		default:
		}
	}
}

func setRawMode() (func(), error) {
	original, err := exec.Command("stty", "-g").Output()
	if err != nil {
		return nil, err
	}
	cmd := exec.Command("stty", "raw", "-echo")
	cmd.Stdin = os.Stdin
	if err := cmd.Run(); err != nil {
		return nil, err
	}
	return func() {
		restore := exec.Command("stty", string(original))
		restore.Stdin = os.Stdin
		_ = restore.Run()
	}, nil
}

func sendResize(conn *net.UDPConn, peer *net.UDPAddr, sessionID, token string) {
	out, err := exec.Command("stty", "size").Output()
	if err != nil {
		return
	}
	var rows, cols int
	if _, err := fmt.Sscanf(string(out), "%d %d", &rows, &cols); err != nil {
		return
	}
	_ = common.SendJSON(conn, peer, common.Message{"type": "resize", "session": sessionID, "token": token, "rows": rows, "cols": cols})
}
