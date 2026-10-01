package offsets

import "sync/atomic"

// Manager tracks the two state pointers described in the assignment:
//
//   - ReadPosition: the position (0-based data row index) of the row that has
//     just been read from the CSV file and handed off for processing.
//   - CommittedPosition: the highest position that has received an ack from
//     the message queue so far.
//
// Both are in-file indices (they reset to 0 each time the CSV iteration
// wraps), and are monotonic within a loop. Atomic increments make them safe to
// update from the read goroutine (ReadPosition) and the ack-handling
// goroutine (CommittedPosition) concurrently.
type Manager struct {
	read      atomic.Int64
	committed atomic.Int64
}

// New returns a Manager seeded at offset (both pointers start there), which is
// used for crash recovery via the MQ's GetOffset(consumer_id).
func New(offset int64) *Manager {
	m := &Manager{}
	m.read.Store(offset)
	m.committed.Store(offset)
	return m
}

// ResetTo restarts both pointers at the given offset. Used at startup/recovery
// and when the CSV file is reloaded due to a configMap rotation.
func (m *Manager) ResetTo(offset int64) {
	m.read.Store(offset)
	m.committed.Store(offset)
}

// AdvanceRead moves ReadPosition forward to pos if pos is ahead of it.
func (m *Manager) AdvanceRead(pos int64) {
	m.read.Store(m.max(m.read.Load(), pos))
}

// Commit advances CommittedPosition to pos (the offset carried by the latest
// ack). Only forward movement is honoured; out-of-order acks for earlier
// positions are ignored.
func (m *Manager) Commit(pos int64) {
	m.committed.Store(m.max(m.committed.Load(), pos))
}

// ReadPosition returns the current read pointer.
func (m *Manager) ReadPosition() int64 { return m.read.Load() }

// CommittedPosition returns the current acknowledged pointer.
func (m *Manager) CommittedPosition() int64 { return m.committed.Load() }

// Lag returns how far the read pointer is ahead of the committed pointer. A
// growing lag indicates the MQ is acking slower than events are produced.
func (m *Manager) Lag() int64 {
	return m.read.Load() - m.committed.Load()
}

func (m *Manager) max(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}
