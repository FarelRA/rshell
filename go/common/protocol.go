package common

import (
	"encoding/base64"
	"encoding/json"
	"log"
	"net"
	"os"
	"time"
)

const (
	Version          = 1
	MaxPacketSize    = 64 * 1024
	KeepaliveEvery   = 5 * time.Second
	SessionTimeout   = 20 * time.Second
	PunchEvery       = 500 * time.Millisecond
	PunchTimeout     = 15 * time.Second
	RegisterEvery    = 10 * time.Second
	RegisterDeadline = 30 * time.Second
)

type Message map[string]any

func SendJSON(conn *net.UDPConn, addr *net.UDPAddr, msg Message) error {
	msg["v"] = Version
	msg["ts"] = time.Now().UnixMilli()
	buf, err := json.Marshal(msg)
	if err != nil {
		return err
	}
	_, err = conn.WriteToUDP(buf, addr)
	return err
}

func DecodeJSON(buf []byte) (Message, error) {
	var msg Message
	err := json.Unmarshal(buf, &msg)
	return msg, err
}

func String(msg Message, key string) string {
	v, _ := msg[key].(string)
	return v
}

func Int(msg Message, key string) int {
	switch x := msg[key].(type) {
	case int:
		return x
	case float64:
		return int(x)
	default:
		return 0
	}
}

func Map(msg Message, key string) map[string]any {
	v, _ := msg[key].(map[string]any)
	if v == nil {
		return map[string]any{}
	}
	return v
}

func EncodeData(data []byte) string {
	return base64.StdEncoding.EncodeToString(data)
}

func DecodeData(s string) []byte {
	b, err := base64.StdEncoding.DecodeString(s)
	if err != nil {
		log.Printf("base64 decode error: %v", err)
		return nil
	}
	return b
}

func TerminalInfo() Message {
	cols, rows := 0, 0
	if size, err := GetTerminalSize(); err == nil {
		cols, rows = size[0], size[1]
	}
	return Message{
		"term":         envOrDefault("TERM", "xterm-256color"),
		"term_program": envOrDefault("TERM_PROGRAM", "rshell"),
		"terminal":     envOrDefault("TERMINAL", "rshell"),
		"cols":         cols,
		"rows":         rows,
	}
}

func envOrDefault(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}
