package mq

import (
	"errors"
	"sync"

	mqpb "streamer/proto"
)

// ErrCursorAheadOfTail is returned by a Store's Range when the requested start
// offset is at or past the current tail.
//
// It distinguishes "you are caught up, nothing new yet" (start == tail-1, a
// legitimate empty snapshot) from "the offset you committed belongs to a log
// incarnation that no longer exists". The latter happens on every real
// failover: the survivor's log can be shorter than the cursor a consumer
// committed against the lost leader, because the follower was partitioned, was
// behind, or had already trimmed past that point.
//
// Collapsing the two into an empty snapshot is a silent data-loss stall - the
// consumer reports healthy, retries nothing, and ingests nothing forever.
var ErrCursorAheadOfTail = errors.New("mq: consumer cursor is ahead of the log tail")

// LogEntry pairs a stored event with its absolute log offset. The offset is
// what consumers commit as their cursor and use as the shard key.
type LogEntry struct {
	Event  *mqpb.Event
	Offset int64
}

// Store is the durability boundary of the event log. The service logic
// (dedup, cursors, registry) sits on top of it and never mutates log state
// directly, so a durable implementation can be swapped in without touching the
// MessageQueue server. The next increment replaces MemStore with a
// write-ahead-log backed store (fsync before ack); the interface stays the same.
type Store interface {
	// Append stores e and returns its absolute log offset.
	Append(e *mqpb.Event) (int64, error)
	// Range returns a snapshot of the events with offset > start, in log
	// order, up to the current tail. A start below the retained head is
	// clamped to the head (retention may have trimmed the prefix).
	Range(start int64) ([]LogEntry, error)
	// Tail returns the absolute offset the next Append will produce.
	Tail() int64
	// Base returns the absolute offset of the first entry still retained, so the
	// log occupies [Base, Tail-1] and holds Tail-1-... -Base+1 entries. It is
	// part of Store rather than only DurableStore because the queue-depth metric
	// needs it on both backends: the durable-only accessor reports 0 for an
	// in-memory store, which would make the metric silently wrong in every test
	// and in the -data-dir="" deployment.
	Base() int64
	// Trim drops every event with offset <= floor. It is the retention
	// barrier; never drops below the current head.
	Trim(floor int64)
}

// MemStore is the in-memory Store used today. Offsets are global and
// monotonic: the first event ever appended is offset 0 and offsets are never
// reused, even after trimming (base advances instead).
type MemStore struct {
	mu   sync.Mutex
	base int64 // absolute offset of log[0]; 0 until the first Trim
	log  []*mqpb.Event
}

// NewMemStore returns an empty in-memory Store.
func NewMemStore() *MemStore { return &MemStore{} }

// Append stores e and returns its absolute log offset.
func (s *MemStore) Append(e *mqpb.Event) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	off := s.base + int64(len(s.log))
	s.log = append(s.log, e)
	return off, nil
}

// Range returns a snapshot of the retained events with offset > start.
func (s *MemStore) Range(start int64) ([]LogEntry, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	head := s.base // absolute offset of the first retained event
	first := start + 1
	if first < head {
		first = head
	}
	tail := s.base + int64(len(s.log))
	if start >= tail {
		// A fully caught-up consumer sits at tail-1, so `start >= tail` can
		// only mean the cursor names an offset this log incarnation never
		// had: it came from a different log (a follower promoted after the
		// old leader was lost, or a fresh volume after a cutover).
		//
		// Returning an empty snapshot here would be a silent stall - the
		// consumer would poll forever, log nothing, and ingest nothing while
		// every health check stayed green. Surface it as an error so the
		// consumer can re-anchor onto the live log instead.
		return nil, ErrCursorAheadOfTail
	}
	i := first - head // >= 0 after clamping
	if i > int64(len(s.log)) {
		i = int64(len(s.log))
	}
	out := make([]LogEntry, 0, len(s.log)-int(i))
	for j := i; j < int64(len(s.log)); j++ {
		out = append(out, LogEntry{Event: s.log[j], Offset: head + j})
	}
	return out, nil
}

// Base returns the absolute offset of the first retained event, which is 0
// until the first trim.
func (s *MemStore) Base() int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.base
}

// Tail returns the absolute offset of the next append.
func (s *MemStore) Tail() int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.base + int64(len(s.log))
}

// Trim drops retained events with offset <= floor.
func (s *MemStore) Trim(floor int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if floor < s.base {
		return
	}
	n := floor - s.base + 1
	if n > int64(len(s.log)) {
		n = int64(len(s.log))
	}
	// Copy into a fresh slice so trimmed events are actually released (the
	// point of retention) rather than pinned by the old backing array.
	kept := make([]*mqpb.Event, len(s.log)-int(n))
	copy(kept, s.log[n:])
	s.log = kept
	s.base += n
}
