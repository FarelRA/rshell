package common

import (
	"encoding/json"
	"fmt"
	"log"
	"strings"
	"time"
)

type Logger struct {
	impl string
	role string
}

func NewLogger(impl, role string) *Logger {
	log.SetFlags(0)
	return &Logger{impl: impl, role: role}
}

func (l *Logger) Info(event, message string, fields ...any) {
	l.log("info", event, message, fields...)
}

func (l *Logger) Error(event, message string, fields ...any) {
	l.log("error", event, message, fields...)
}

func (l *Logger) log(level, event, message string, fields ...any) {
	parts := []string{
		"ts=" + quoteValue(time.Now().UTC().Format(time.RFC3339Nano)),
		"level=" + quoteValue(level),
		"impl=" + quoteValue(l.impl),
		"role=" + quoteValue(l.role),
		"event=" + quoteValue(event),
		"msg=" + quoteValue(message),
	}
	for i := 0; i+1 < len(fields); i += 2 {
		key, ok := fields[i].(string)
		if !ok || key == "" {
			continue
		}
		parts = append(parts, key+"="+quoteValue(fields[i+1]))
	}
	log.Print(strings.Join(parts, " "))
}

func quoteValue(value any) string {
	buf, err := json.Marshal(value)
	if err != nil {
		return fmt.Sprintf("%q", fmt.Sprint(value))
	}
	return string(buf)
}
