package mq

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"sync"
	"syscall"

	mqpb "streamer/proto"
)

// DurableStore is the subset of the load-bearing seam a write-ahead-log store
// adds on top of Store. A Server backed by a DurableStore can safely
// acknowledge producers: every append/commit/trim is fsynced before the server
// replies, so acking implies "on disk", and a restart reproduces the log and
// every cursor exactly.
type DurableStore interface {
	Store
	// Commit durably records a consumer's offset so it survives a restart.
	// Monotonic: an offset at or below the stored one is ignored.
	Commit(group, consumer string, offset int64) error
	// Rewind durably moves a cursor backwards, to the retained head, when the
	// committed offset is unreachable because it came from a log incarnation
	// this node no longer has. Unlike Commit it is not monotonic.
	Rewind(group, consumer string, offset int64) error
	// Cursors returns every durable cursor: streamer-derived producers plus
	// committed consumers (collector group and others).
	Cursors() []CursorState
	// Events returns the retained event log (so a server can rebuild its dedup
	// window after a restart).
	Events() []LogEntry
	// Base returns the absolute offset of the retained head (the offset of
	// log[0]); a follower uses it to align its own log with the leader.
	Base() int64
	// ApplyCheckpoint converges the store to a leader checkpoint: cursor
	// offsets are merged (never regressed) and, when the leader's base moved,
	// the WAL is rewritten as a compact checkpoint + retained tail so restart
	// replay reproduces the same base. Followers are the only callers.
	//
	// A follower must DISCARD its own retained log when the leader's base
	// differs, not relabel it. Log entries carry implicit absolute offsets
	// (Base()+index), so moving the base without dropping the matching prefix
	// silently renumbers events the follower never received from this leader:
	// Tail() then reports base+len(log), overshooting the leader's real tail.
	// The follower asks to resume past everything the leader has, the leader
	// finds nothing to send, closes the stream, and with syncReplicas>=1 every
	// publish blocks forever. Keeping the divergent entries is never correct -
	// the leader's log is authoritative - so drop them and re-replay.
	ApplyCheckpoint(cp *mqpb.ReplicaCheckpoint) error
	// Close flushes and releases the log.
	Close() error
}

// CursorState is one durable consumer offset. Streamer-group cursors are the
// last acked csv_line_offset per pod; collector-group cursors are committed
// log offsets.
type CursorState struct {
	Group    string
	Consumer string
	Offset   int64
}

type cursorKey struct{ group, consumer string }

// walRecord is the JSON wire frame for one WAL entry. Writing every mutation as
// a length-prefixed JSON frame (rather than raw bytes) keeps the on-disk format
// self-describing and forward-tolerant.
type walRecord struct {
	Type     string         `json:"t"` // "x" checkpoint | "a" append | "c" commit | "t" trim
	Check    *walCheckpoint `json:"x,omitempty"`
	Event    *eventJSON     `json:"e,omitempty"`
	Group    string         `json:"g,omitempty"`
	Consumer string         `json:"cid,omitempty"`
	Offset   int64          `json:"o,omitempty"`
	Floor    int64          `json:"f,omitempty"`
}

// walCheckpoint seeds replay with the absolute base offset and every cursor, so
// a compactor can drop old records while preserving offsets.
type walCheckpoint struct {
	Base    int64         `json:"base"`
	Cursors []CursorState `json:"cursors"`
}

const (
	// compactThresholdBytes triggers a checkpoint rewrite of the WAL once the
	// file exceeds this, bounding restart time and disk usage.
	compactThresholdBytes = 8 << 20

	// walFrameHeaderLen is the fixed size of the length prefix of each frame.
	walFrameHeaderLen = 4
)

// WALStore is a Store backed by a write-ahead log on disk. Durability contract:
//
//   - Append: fsyncs the frame before updating memory and returning -> an acked
//     event is on disk.
//   - Commit/Trim: same discipline (fsync before the caller proceeds).
//
// Replay reproduces the exact log (same base and offsets) plus every cursor, so
// a restarted MQ looks like the crashed one minus any un-acked tail. The
// streamer-group cursor for a pod is derived during replay from the last
// appended event carrying Tags["pod_name"]==pod (its ack offset is that event's
// csv_line_offset), so producer acks need no extra records.
type WALStore struct {
	mu   sync.Mutex
	f    *os.File
	size int64

	hdrBuf [walFrameHeaderLen]byte // replay scratch

	base    int64         // absolute offset of log[0]
	log     []*mqpb.Event // retained events
	commits map[cursorKey]int64
	derived map[string]int64 // streamer group: pod_name -> last acked csv_line_offset
}

// NewWalStore opens (creating if needed) the WAL at path and replays it. The
// file is locked exclusively so a data-dir belongs to one MQ process at a time.
func NewWalStore(path string) (*WALStore, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, fmt.Errorf("mkdir %s: %w", filepath.Dir(path), err)
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0o644)
	if err != nil {
		return nil, fmt.Errorf("open wal: %w", err)
	}
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		f.Close()
		return nil, fmt.Errorf("wal %s is locked by another process: %w", path, err)
	}
	st, _ := f.Stat()
	s := &WALStore{
		f:       f,
		size:    st.Size(),
		commits: make(map[cursorKey]int64),
		derived: make(map[string]int64),
	}
	if err := s.replay(); err != nil {
		f.Close()
		return nil, err
	}
	log.Printf("wal open %s: base=%d retained=%d committed_cursors=%d size=%d",
		path, s.base, len(s.log), len(s.commits), s.size)
	return s, nil
}

// replay reads every complete frame and applies it in order. A torn trailing
// frame (crash mid-write) is cut off; everything before it is valid.
func (s *WALStore) replay() error {
	var goodEnd int64 // byte offset just past the last fully-consumed frame
	for {
		n, err := readFull(s.f, s.hdrBuf[:])
		if err != nil {
			if errors.Is(err, io.EOF) && n == 0 {
				return nil // clean end of file
			}
			log.Printf("wal replay: discarding partial tail frame (%v)", err)
			return s.truncateTo(goodEnd)
		}
		frameLen := binary.BigEndian.Uint32(s.hdrBuf[:])
		buf := make([]byte, frameLen)
		if _, err := readFull(s.f, buf); err != nil {
			log.Printf("wal replay: discarding partial tail frame (%v)", err)
			return s.truncateTo(goodEnd)
		}
		goodEnd += walFrameHeaderLen + int64(frameLen)
		var rec walRecord
		if err := json.Unmarshal(buf, &rec); err != nil {
			return fmt.Errorf("replay frame: %w", err)
		}
		switch rec.Type {
		case "x":
			s.base = rec.Check.Base
			s.commits = make(map[cursorKey]int64)
			for _, cs := range rec.Check.Cursors {
				s.applyCommitted(cs.Group, cs.Consumer, cs.Offset)
			}
		case "a":
			s.log = append(s.log, rec.Event.toEvent())
			if pod := rec.Event.Tags["pod_name"]; pod != "" {
				if off := rec.Event.CSVLineOffset; off > s.derived[pod] {
					s.derived[pod] = off
				}
			}
		case "c":
			s.applyCommitted(rec.Group, rec.Consumer, rec.Offset)
		case "r":
			// Explicit rewind (see WALStore.Rewind): assigned verbatim,
			// bypassing the monotonic rule that protects ordinary commits.
			s.commits[cursorKey{normGroup(rec.Group), rec.Consumer}] = rec.Offset
		case "t":
			s.trimLocked(rec.Floor)
		default:
			return fmt.Errorf("replay: unknown record type %q", rec.Type)
		}
	}
}

// truncateTo cuts the file down to byte limit (the end of the last complete
// frame), so a torn tail never replays.
func (s *WALStore) truncateTo(limit int64) error {
	if err := s.f.Truncate(limit); err != nil {
		return fmt.Errorf("truncate torn tail: %w", err)
	}
	if _, err := s.f.Seek(limit, 0); err != nil {
		return err
	}
	s.size = limit
	return nil
}

func (s *WALStore) applyCommitted(group, consumer string, offset int64) {
	key := cursorKey{normGroup(group), consumer}
	if offset <= s.commits[key] {
		return
	}
	s.commits[key] = offset
}

// Rewind durably moves a consumer cursor backwards, to the retained head, when
// the offset it had committed belongs to a log incarnation this node no longer
// has (the usual case: a follower promoted after its leader was lost, whose log
// is shorter than the cursor consumers had already committed).
//
// Commit cannot express this because it is deliberately monotonic - a stale or
// out-of-order commit must never rewind a cursor, or a duplicate publisher ack
// would replay the whole log. A rewind is not that: it is the server deciding,
// with the log in front of it, that the cursor is unreachable and must be
// re-anchored. It gets its own record type ("r") so replay applies it verbatim
// instead of running it through the monotonic rule.
func (s *WALStore) Rewind(group, consumer string, offset int64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	key := cursorKey{normGroup(group), consumer}
	if s.commits[key] <= offset {
		return nil // already at or behind the anchor: nothing to undo
	}
	if err := s.writeFrame(walRecord{Type: "r", Group: normGroup(group), Consumer: consumer, Offset: offset}); err != nil {
		return err
	}
	if err := s.f.Sync(); err != nil {
		return fmt.Errorf("wal sync: %w", err)
	}
	s.commits[key] = offset
	return nil
}

// Append durably stores e (fsync before returning) and returns its log offset.
func (s *WALStore) Append(e *mqpb.Event) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	off := s.base + int64(len(s.log))
	if err := s.writeFrame(walRecord{Type: "a", Event: toEventJSON(e)}); err != nil {
		return 0, err
	}
	if err := s.f.Sync(); err != nil {
		return 0, fmt.Errorf("wal sync: %w", err)
	}
	s.log = append(s.log, e)
	return off, nil
}

// Range returns a snapshot of the retained events with offset > start.
func (s *WALStore) Range(start int64) ([]LogEntry, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	first := start + 1
	if first < s.base {
		first = s.base
	}
	tail := s.base + int64(len(s.log))
	if start >= tail {
		// See MemStore.Range: a start at or past the tail means the cursor
		// names a log incarnation that no longer exists. An empty snapshot
		// here would stall the consumer silently and forever.
		return nil, ErrCursorAheadOfTail
	}
	i := first - s.base
	if i > int64(len(s.log)) {
		i = int64(len(s.log))
	}
	out := make([]LogEntry, 0, len(s.log)-int(i))
	for j := i; j < int64(len(s.log)); j++ {
		out = append(out, LogEntry{Event: s.log[j], Offset: s.base + j})
	}
	return out, nil
}

// Tail returns the absolute offset of the next append.
func (s *WALStore) Tail() int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.base + int64(len(s.log))
}

// Trim drops retained events with offset <= floor and persists the barrier.
func (s *WALStore) Trim(floor int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if floor < s.base {
		return
	}
	if err := s.writeFrame(walRecord{Type: "t", Floor: floor}); err != nil {
		log.Printf("wal trim persist: %v", err)
		return
	}
	if err := s.f.Sync(); err != nil {
		log.Printf("wal trim sync: %v", err)
	}
	s.trimLocked(floor)
	s.maybeCompactLocked()
}

// Commit durably records a consumer offset (fsync before returning). A no-op
// when the cursor has not advanced (nothing to persist).
func (s *WALStore) Commit(group, consumer string, offset int64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	key := cursorKey{normGroup(group), consumer}
	if s.commits[key] >= offset {
		return nil
	}
	if err := s.writeFrame(walRecord{Type: "c", Group: normGroup(group), Consumer: consumer, Offset: offset}); err != nil {
		return err
	}
	if err := s.f.Sync(); err != nil {
		return fmt.Errorf("wal sync: %w", err)
	}
	s.commits[key] = offset
	return nil
}

// Cursors returns streamer-derived cursors plus all committed cursors.
func (s *WALStore) Cursors() []CursorState {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]CursorState, 0, len(s.commits)+len(s.derived))
	for pod, off := range s.derived {
		out = append(out, CursorState{Group: streamerGroup, Consumer: pod, Offset: off})
	}
	for key, off := range s.commits {
		out = append(out, CursorState{Group: key.group, Consumer: key.consumer, Offset: off})
	}
	return out
}

// Events returns the retained log.
func (s *WALStore) Events() []LogEntry {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]LogEntry, len(s.log))
	for i, e := range s.log {
		out[i] = LogEntry{Event: e, Offset: s.base + int64(i)}
	}
	return out
}

// Base returns the absolute offset of the retained head (log[0] == s.base).
func (s *WALStore) Base() int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.base
}

// ApplyCheckpoint converges the store to a leader checkpoint. Cursors merge
// monotonically in memory (they are also durable on this log via its own "c"
// frames, and a compaction rewrite persists the merged set). When the leader's
// base moved, the file is rewritten to [checkpoint][retained tail] so a restart
// replays the exact base; a follower only ever applies offsets a leader has
// retained, so this keeps follower offsets in lockstep with the leader.
func (s *WALStore) ApplyCheckpoint(cp *mqpb.ReplicaCheckpoint) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, c := range cp.Cursors {
		s.applyCommitted(c.Group, c.Consumer, c.Offset)
	}
	if cp.Base == s.base {
		return nil // base unchanged; nothing durable to rewrite
	}
	// The leader's log is authoritative. Drop our retained entries instead of
	// renumbering them: entries are addressed as base+index, so keeping a log
	// written against a different base makes Tail() overshoot the leader and
	// wedges replication. The leader re-sends everything from the new base.
	s.log = nil
	s.base = cp.Base
	return s.rewriteLocked()
}

// Close flushes and releases the log (releasing the file lock on shutdown).
func (s *WALStore) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.f.Sync(); err != nil {
		return err
	}
	return s.f.Close()
}

// trimLocked advances s.base past every event with offset <= floor, releasing
// the trimmed prefix. Callers hold s.mu.
func (s *WALStore) trimLocked(floor int64) {
	if floor < s.base {
		return
	}
	n := floor - s.base + 1
	if n > int64(len(s.log)) {
		n = int64(len(s.log))
	}
	kept := make([]*mqpb.Event, len(s.log)-int(n))
	copy(kept, s.log[n:])
	s.log = kept
	s.base += n
}

// writeFrame serializes and appends one frame. Callers hold s.mu.
func (s *WALStore) writeFrame(rec walRecord) error {
	buf, err := json.Marshal(rec)
	if err != nil {
		return fmt.Errorf("marshal frame: %w", err)
	}
	var hdr [walFrameHeaderLen]byte
	binary.BigEndian.PutUint32(hdr[:], uint32(len(buf)))
	if _, err := s.f.Write(hdr[:]); err != nil {
		return fmt.Errorf("wal write header: %w", err)
	}
	if _, err := s.f.Write(buf); err != nil {
		return fmt.Errorf("wal write frame: %w", err)
	}
	s.size += walFrameHeaderLen + int64(len(buf))
	return nil
}

// maybeCompactLocked rewrites the WAL as a checkpoint + retained tail once the
// file grows past compactThresholdBytes, dropping per-mutation commit/trim
// records while preserving offsets and cursors. Callers hold s.mu.
func (s *WALStore) maybeCompactLocked() {
	if s.size <= compactThresholdBytes {
		return
	}
	if err := s.rewriteLocked(); err != nil {
		log.Printf("wal compact: %v", err)
	}
}

// rewriteLocked atomically replaces the WAL with a compaction frame
// (checkpoint: current base + every durable cursor) followed by the retained
// event tail. Replay of the rewritten file reproduces the same log and
// cursors. Callers hold s.mu.
func (s *WALStore) rewriteLocked() error {
	tf, err := os.CreateTemp(filepath.Dir(s.f.Name()), ".wal-compact-*")
	if err != nil {
		return err
	}
	write := func(rec walRecord) error {
		buf, merr := json.Marshal(rec)
		if merr != nil {
			return merr
		}
		var hdr [walFrameHeaderLen]byte
		binary.BigEndian.PutUint32(hdr[:], uint32(len(buf)))
		if _, werr := tf.Write(hdr[:]); werr != nil {
			return werr
		}
		_, werr := tf.Write(buf)
		return werr
	}
	cp := &walCheckpoint{Base: s.base}
	for pod, off := range s.derived {
		cp.Cursors = append(cp.Cursors, CursorState{Group: streamerGroup, Consumer: pod, Offset: off})
	}
	for key, off := range s.commits {
		cp.Cursors = append(cp.Cursors, CursorState{Group: key.group, Consumer: key.consumer, Offset: off})
	}
	if err := write(walRecord{Type: "x", Check: cp}); err != nil {
		tf.Close()
		return err
	}
	for _, e := range s.log {
		if err := write(walRecord{Type: "a", Event: toEventJSON(e)}); err != nil {
			tf.Close()
			return err
		}
	}
	if err := tf.Sync(); err != nil {
		tf.Close()
		return err
	}
	if err := tf.Close(); err != nil {
		return err
	}
	if err := os.Rename(tf.Name(), s.f.Name()); err != nil {
		return err
	}
	nf, err := os.OpenFile(s.f.Name(), os.O_CREATE|os.O_RDWR|os.O_APPEND, 0o644)
	if err != nil {
		return err
	}
	if err := syscall.Flock(int(nf.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		nf.Close()
		return err
	}
	s.f.Close()
	s.f = nf
	s.size = 0
	log.Printf("wal rewritten: checkpoint(base=%d) + %d retained appends", s.base, len(s.log))
	return nil
}

// toEventJSON is the snake_case wire form of an event (shared with server.go's
// logging helpers so the on-disk bytes match the human-readable event log).
func toEventJSON(e *mqpb.Event) *eventJSON {
	return &eventJSON{
		EventID:         e.EventId,
		SourceTimestamp: e.SourceTimestamp,
		MetricName:      e.MetricName,
		GPUIndex:        e.GpuIndex,
		DeviceName:      e.DeviceName,
		DeviceID:        e.DeviceId,
		ModelName:       e.ModelName,
		Hostname:        e.Hostname,
		Value:           e.Value,
		CSVLineOffset:   e.CsvLineOffset,
		LoopCount:       e.LoopCount,
		Tags:            e.Tags,
	}
}

// toEvent restores a wire-form event to its proto shape.
func (j *eventJSON) toEvent() *mqpb.Event {
	// Records written before the `timestamp` field was dropped have no
	// source_timestamp; fall back to the legacy key so they keep their time.
	srcTS := j.SourceTimestamp
	if srcTS == "" {
		srcTS = j.LegacyTimestamp
	}
	return &mqpb.Event{
		EventId:         j.EventID,
		SourceTimestamp: srcTS,
		MetricName:      j.MetricName,
		GpuIndex:        j.GPUIndex,
		DeviceName:      j.DeviceName,
		DeviceId:        j.DeviceID,
		ModelName:       j.ModelName,
		Hostname:        j.Hostname,
		Value:           j.Value,
		CsvLineOffset:   j.CSVLineOffset,
		LoopCount:       j.LoopCount,
		Tags:            j.Tags,
	}
}

// readFull reads len(buf) bytes from f, reporting how many were read. It
// returns io.EOF on a short read at end-of-file so callers can tell a clean
// boundary (n == len(buf) after reaching EOF) from a torn frame.
func readFull(f *os.File, buf []byte) (int, error) {
	total := 0
	for total < len(buf) {
		n, err := f.Read(buf[total:])
		total += n
		if err != nil {
			if err == io.EOF && total == len(buf) {
				return total, nil
			}
			return total, err
		}
	}
	return total, nil
}
