package main

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"flag"
	"log"
	"net"
	"sync"
	"time"
)

const (
	protocolVersion    = 1
	registrationTTL    = 30 * time.Second
	registrationTicker = 5 * time.Second
	sessionTTL         = 20 * time.Second
	maxPacketSize      = 64 * 1024
)

type message map[string]any

type registration struct {
	service string
	addr    *net.UDPAddr
	meta    map[string]any
	seenAt  time.Time
}

type session struct {
	id         string
	token      string
	service    string
	hostAddr   *net.UDPAddr
	clientAddr *net.UDPAddr
	hostMeta   map[string]any
	clientMeta map[string]any
	createdAt  time.Time
	lastSeen   time.Time
}

type serverState struct {
	mu            sync.Mutex
	registrations map[string]*registration
	sessions      map[string]*session
}

func main() {
	listen := flag.String("listen", ":4000", "UDP listen address")
	flag.Parse()

	addr, err := net.ResolveUDPAddr("udp", *listen)
	if err != nil {
		log.Fatalf("resolve listen addr: %v", err)
	}

	conn, err := net.ListenUDP("udp", addr)
	if err != nil {
		log.Fatalf("listen udp: %v", err)
	}
	defer conn.Close()

	state := &serverState{
		registrations: map[string]*registration{},
		sessions:      map[string]*session{},
	}

	go state.gcLoop()

	log.Printf("rendezvous listening on %s", conn.LocalAddr())
	buf := make([]byte, maxPacketSize)
	for {
		n, peer, err := conn.ReadFromUDP(buf)
		if err != nil {
			log.Printf("read error: %v", err)
			continue
		}

		var msg message
		if err := json.Unmarshal(buf[:n], &msg); err != nil {
			log.Printf("invalid json from %s: %v", peer, err)
			continue
		}

		state.handle(conn, peer, msg)
	}
}

func (s *serverState) handle(conn *net.UDPConn, peer *net.UDPAddr, msg message) {
	if intValue(msg["v"]) != protocolVersion {
		sendJSON(conn, peer, message{"v": protocolVersion, "type": "error", "code": "bad_version", "message": "unsupported protocol version"})
		return
	}

	switch stringValue(msg["type"]) {
	case "register":
		s.handleRegister(conn, peer, msg)
	case "connect_request":
		s.handleConnect(conn, peer, msg)
	case "keepalive":
		s.handleKeepalive(conn, peer, msg)
	default:
		sendJSON(conn, peer, message{"v": protocolVersion, "type": "error", "code": "bad_type", "message": "unknown message type"})
	}
}

func (s *serverState) handleRegister(conn *net.UDPConn, peer *net.UDPAddr, msg message) {
	service := stringValue(msg["service"])
	if service == "" {
		sendJSON(conn, peer, message{"v": protocolVersion, "type": "error", "code": "bad_request", "message": "missing service"})
		return
	}

	meta := mapValue(msg["meta"])

	s.mu.Lock()
	s.registrations[service] = &registration{service: service, addr: cloneAddr(peer), meta: meta, seenAt: time.Now()}
	s.mu.Unlock()

	sendJSON(conn, peer, message{
		"v":             protocolVersion,
		"type":          "register_ok",
		"service":       service,
		"public_addr":   peer.String(),
		"expires_in_ms": registrationTTL.Milliseconds(),
	})
	log.Printf("registered service=%s addr=%s", service, peer)
}

func (s *serverState) handleConnect(conn *net.UDPConn, peer *net.UDPAddr, msg message) {
	service := stringValue(msg["service"])
	if service == "" {
		sendJSON(conn, peer, message{"v": protocolVersion, "type": "error", "code": "bad_request", "message": "missing service"})
		return
	}

	clientMeta := mapValue(msg["meta"])

	s.mu.Lock()
	reg, ok := s.registrations[service]
	if !ok || time.Since(reg.seenAt) > registrationTTL {
		s.mu.Unlock()
		sendJSON(conn, peer, message{"v": protocolVersion, "type": "error", "code": "service_not_found", "message": "requested service is not registered"})
		return
	}

	sess := &session{
		id:         randomHex(16),
		token:      randomHex(24),
		service:    service,
		hostAddr:   cloneAddr(reg.addr),
		clientAddr: cloneAddr(peer),
		hostMeta:   reg.meta,
		clientMeta: clientMeta,
		createdAt:  time.Now(),
		lastSeen:   time.Now(),
	}
	s.sessions[sess.id] = sess
	s.mu.Unlock()

	hostIntro := message{
		"v":          protocolVersion,
		"type":       "connect_intro",
		"role":       "host",
		"service":    service,
		"session":    sess.id,
		"token":      sess.token,
		"peer_addr":  sess.clientAddr.String(),
		"peer_meta":  sess.clientMeta,
		"timeout_ms": 15000,
	}
	clientIntro := message{
		"v":          protocolVersion,
		"type":       "connect_intro",
		"role":       "client",
		"service":    service,
		"session":    sess.id,
		"token":      sess.token,
		"peer_addr":  sess.hostAddr.String(),
		"peer_meta":  sess.hostMeta,
		"timeout_ms": 15000,
	}

	sendJSON(conn, sess.hostAddr, hostIntro)
	sendJSON(conn, sess.clientAddr, clientIntro)
	log.Printf("introduced service=%s session=%s host=%s client=%s", service, sess.id, sess.hostAddr, sess.clientAddr)
}

func (s *serverState) handleKeepalive(conn *net.UDPConn, peer *net.UDPAddr, msg message) {
	service := stringValue(msg["service"])
	if service == "" {
		return
	}

	s.mu.Lock()
	if reg, ok := s.registrations[service]; ok && reg.addr.String() == peer.String() {
		reg.seenAt = time.Now()
	}
	s.mu.Unlock()

	sendJSON(conn, peer, message{"v": protocolVersion, "type": "keepalive_ack", "service": service})
}

func (s *serverState) gcLoop() {
	ticker := time.NewTicker(registrationTicker)
	defer ticker.Stop()

	for range ticker.C {
		now := time.Now()
		s.mu.Lock()
		for key, reg := range s.registrations {
			if now.Sub(reg.seenAt) > registrationTTL {
				delete(s.registrations, key)
			}
		}
		for key, sess := range s.sessions {
			if now.Sub(sess.lastSeen) > sessionTTL || now.Sub(sess.createdAt) > sessionTTL {
				delete(s.sessions, key)
			}
		}
		s.mu.Unlock()
	}
}

func sendJSON(conn *net.UDPConn, addr *net.UDPAddr, msg message) {
	msg["ts"] = time.Now().UnixMilli()
	buf, err := json.Marshal(msg)
	if err != nil {
		log.Printf("marshal error: %v", err)
		return
	}
	if _, err := conn.WriteToUDP(buf, addr); err != nil {
		log.Printf("write error to %s: %v", addr, err)
	}
}

func randomHex(size int) string {
	b := make([]byte, size)
	if _, err := rand.Read(b); err != nil {
		panic(err)
	}
	return hex.EncodeToString(b)
}

func stringValue(v any) string {
	s, _ := v.(string)
	return s
}

func intValue(v any) int {
	switch x := v.(type) {
	case float64:
		return int(x)
	case int:
		return x
	default:
		return 0
	}
}

func mapValue(v any) map[string]any {
	m, ok := v.(map[string]any)
	if !ok || m == nil {
		return map[string]any{}
	}
	return m
}

func cloneAddr(addr *net.UDPAddr) *net.UDPAddr {
	if addr == nil {
		return nil
	}
	ip := append([]byte(nil), addr.IP...)
	return &net.UDPAddr{IP: ip, Port: addr.Port, Zone: addr.Zone}
}
