package common

import (
	"sync"
	"time"
)

const RetransmitEvery = 200 * time.Millisecond

type pendingChunk struct {
	data   string
	sentAt time.Time
}

type ReliableSender struct {
	mu      sync.Mutex
	stream  string
	nextSeq uint64
	pending map[uint64]pendingChunk
}

func NewReliableSender(stream string) *ReliableSender {
	return &ReliableSender{stream: stream, pending: map[uint64]pendingChunk{}}
}

func (s *ReliableSender) Push(data []byte, session, token string) Message {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.nextSeq++
	encoded := EncodeData(data)
	s.pending[s.nextSeq] = pendingChunk{data: encoded, sentAt: time.Now()}
	return Message{"type": "data", "session": session, "token": token, "stream": s.stream, "seq": s.nextSeq, "data": encoded}
}

func (s *ReliableSender) Ack(ack uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for seq := range s.pending {
		if seq <= ack {
			delete(s.pending, seq)
		}
	}
}

func (s *ReliableSender) Retransmit(session, token string) []Message {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := time.Now()
	messages := make([]Message, 0, len(s.pending))
	for seq, chunk := range s.pending {
		if now.Sub(chunk.sentAt) < RetransmitEvery {
			continue
		}
		chunk.sentAt = now
		s.pending[seq] = chunk
		messages = append(messages, Message{"type": "data", "session": session, "token": token, "stream": s.stream, "seq": seq, "data": chunk.data})
	}
	return messages
}

type ReliableReceiver struct {
	mu       sync.Mutex
	expected uint64
	buffer   map[uint64][]byte
}

func NewReliableReceiver() *ReliableReceiver {
	return &ReliableReceiver{expected: 1, buffer: map[uint64][]byte{}}
}

func (r *ReliableReceiver) Accept(seq uint64, data []byte) ([][]byte, uint64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if seq == 0 {
		return nil, r.expected - 1
	}
	if seq < r.expected {
		return nil, r.expected - 1
	}
	if _, ok := r.buffer[seq]; !ok {
		r.buffer[seq] = append([]byte(nil), data...)
	}
	chunks := make([][]byte, 0)
	for {
		chunk, ok := r.buffer[r.expected]
		if !ok {
			break
		}
		chunks = append(chunks, chunk)
		delete(r.buffer, r.expected)
		r.expected++
	}
	return chunks, r.expected - 1
}
