package mq

import (
	"context"
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	mqpb "streamer/proto"
)

func tmpWal(t *testing.T) string {
	t.Helper()
	return filepath.Join(t.TempDir(), "wal.log")
}

func pubEvent(id string, csv int64, pod string) *mqpb.Event {
	return &mqpb.Event{
		EventId:       id,
		CsvLineOffset: csv,
		Tags:          map[string]string{"pod_name": pod},
	}
}

func publishOne(t *testing.T, c mqpb.MessageQueueClient, ctx context.Context, e *mqpb.Event) (*mqpb.Ack, error) {
	t.Helper()
	stream, err := c.PublishEvents(ctx)
	if err != nil {
		t.Fatalf("open publish stream: %v", err)
	}
	if err := stream.Send(e); err != nil {
		t.Fatalf("publish send: %v", err)
	}
	return stream.Recv()
}

// TestWalReplayPreservesOffsetsAndCursors: after a stop, a fresh WALStore +
// Server on the same path reproduces identical log offsets, the streamer-derived
// cursor (from appended pod events), and committed cursors.
func TestWalReplayPreservesOffsetsAndCursors(t *testing.T) {
	path := tmpWal(t)

	s1, err := NewWalStore(path)
	if err != nil {
		t.Fatalf("open wal: %v", err)
	}
	srv1 := NewWithStore(time.Minute, s1)

	for i, e := range []*mqpb.Event{
		pubEvent("e_0", 0, "pod-0"),
		pubEvent("e_1", 1, "pod-0"),
		pubEvent("e_2", 2, "pod-0"),
		pubEvent("e_3", 0, "pod-1"),
	} {
		off, err := s1.Append(e)
		if err != nil || off != int64(i) {
			t.Fatalf("append %d: off=%d err=%v, want off=%d", i, off, err, i)
		}
	}
	if err := s1.Commit(collectorGroup, "collector-0", 2); err != nil {
		t.Fatalf("commit: %v", err)
	}
	if err := srv1.Close(); err != nil {
		t.Fatalf("srv close: %v", err)
	}

	s2, err := NewWalStore(path)
	if err != nil {
		t.Fatalf("reopen wal: %v", err)
	}
	defer s2.Close()
	if got := s2.Tail(); got != 4 {
		t.Fatalf("replayed Tail = %d, want 4", got)
	}

	srv2 := NewWithStore(time.Minute, s2)

	var pod0, pod1 int64
	for _, cs := range s2.Cursors() {
		switch cs.Consumer {
		case "pod-0":
			pod0 = cs.Offset
		case "pod-1":
			pod1 = cs.Offset
		case "collector-0":
			if cs.Group != collectorGroup || cs.Offset != 2 {
				t.Fatalf("collector cursor = %+v, want group=collector offset=2", cs)
			}
		}
	}
	if pod0 != 2 || pod1 != 0 {
		t.Fatalf("streamer cursors after replay: pod-0=%d pod-1=%d, want 2/0", pod0, pod1)
	}

	addr := startServer(t, srv2)
	c := dial(t, addr)
	ctx := context.Background()

	off, err := c.GetOffset(ctx, &mqpb.GetOffsetRequest{ConsumerId: "pod-0", Group: streamerGroup})
	if err != nil || !off.Exists || off.Offset != 2 {
		t.Fatalf("get streamer offset: resp=%v err=%v, want exists=true offset=2", off, err)
	}
	coff, err := c.GetOffset(ctx, &mqpb.GetOffsetRequest{ConsumerId: "collector-0", Group: collectorGroup})
	if err != nil || !coff.Exists || coff.Offset != 2 {
		t.Fatalf("get collector offset: resp=%v err=%v, want exists=true offset=2", coff, err)
	}
	entries := consumeAll(t, c, ctx, -1)
	wantOffsets := []int64{0, 1, 2, 3}
	if len(entries) != len(wantOffsets) {
		t.Fatalf("consumed %d events, want %d", len(entries), len(wantOffsets))
	}
	for i, w := range wantOffsets {
		if entries[i].Offset != w || entries[i].Event.EventId != "e_"+strconv.Itoa(i) {
			t.Fatalf("entry %d = offset=%d id=%s, want offset=%d id=e_%d", i, entries[i].Offset, entries[i].Event.EventId, w, i)
		}
	}
	ack, err := publishOne(t, c, ctx, pubEvent("e_4", 3, "pod-0"))
	if err != nil || ack.Offset != 3 {
		t.Fatalf("post-replay publish ack = %+v err=%v, want csv offset 3", ack, err)
	}
	// The replayed log continues seamlessly: the new event lands at log offset 4.
	ents := consumeAll(t, c, ctx, 3)
	if len(ents) != 1 || ents[0].Offset != 4 || ents[0].Event.EventId != "e_4" {
		t.Fatalf("post-replay consume = %+v, want one event e_4@offset 4", ents)
	}
}

// TestWalTornTailIsIgnored simulates a crash mid-write: a partial frame is left
// on disk after fully-acked frames. Replay must truncate it and reproduce the
// complete log exactly.
func TestWalTornTailIsIgnored(t *testing.T) {
	path := tmpWal(t)
	s, err := NewWalStore(path)
	if err != nil {
		t.Fatalf("open wal: %v", err)
	}
	for i := 0; i < 3; i++ {
		if _, err := s.Append(pubEvent("e_"+strconv.Itoa(i), int64(i), "pod-0")); err != nil {
			t.Fatalf("append %d: %v", i, err)
		}
	}
	var hdr [4]byte
	binary.BigEndian.PutUint32(hdr[:], 50)
	if _, err := s.f.Write(hdr[:]); err != nil {
		t.Fatalf("write torn header: %v", err)
	}
	if _, err := s.f.Write([]byte{1, 2, 3}); err != nil {
		t.Fatalf("write torn payload: %v", err)
	}
	if err := os.Remove(s.f.Name() + ".orig"); err == nil { // no-op guard
	}
	if err := s.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	fullLen := func() int64 { st, _ := os.Stat(path); return st.Size() }
	before := fullLen()

	s2, err := NewWalStore(path)
	if err != nil {
		t.Fatalf("reopen after torn tail: %v", err)
	}
	defer s2.Close()

	if got := fullLen(); got != before-7 {
		t.Fatalf("torn tail not truncated: file %d -> %d, want %d", before, got, before-7)
	}
	entries, err := s2.Range(-1)
	if err != nil || len(entries) != 3 {
		t.Fatalf("range after replay: n=%d err=%v, want 3", len(entries), err)
	}
	if got := s2.Tail(); got != 3 {
		t.Fatalf("replayed tail = %d, want 3 (torn frame dropped)", got)
	}
	for i, le := range entries {
		if le.Offset != int64(i) || le.Event.EventId != "e_"+strconv.Itoa(i) {
			t.Fatalf("entry %d = %+v, want e_%d@%d", i, le, i, i)
		}
	}
}

// TestWalTrimPersists: retention barriers survive a restart (base advances so
// replay produces identical offsets while the full prefix is dropped).
func TestWalTrimPersists(t *testing.T) {
	path := tmpWal(t)
	s, err := NewWalStore(path)
	if err != nil {
		t.Fatalf("open wal: %v", err)
	}
	for i := 0; i < 5; i++ {
		if _, err := s.Append(pubEvent("e_"+strconv.Itoa(i), int64(i), "pod-0")); err != nil {
			t.Fatalf("append %d: %v", i, err)
		}
	}
	s.Trim(2)
	if err := s.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	s2, err := NewWalStore(path)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer s2.Close()
	if got := s2.Tail(); got != 5 {
		t.Fatalf("replayed tail = %d, want 5 (trim barrier preserved)", got)
	}
	entries, err := s2.Range(-1)
	if err != nil {
		t.Fatalf("range: %v", err)
	}
	if len(entries) != 2 {
		t.Fatalf("retained after replay = %d, want 2", len(entries))
	}
	for i, le := range entries {
		if le.Offset != int64(i+3) || le.Event.CsvLineOffset != int64(i+3) {
			t.Fatalf("entry %d = offset=%d csv=%d, want offset&csv=%d (prefix trimmed, offsets preserved)", i, le.Offset, le.Event.CsvLineOffset, i+3)
		}
	}
}

// TestWalCompactRewritesCheckpoint: once past the threshold, a checkpoint +
// retained-tail rewrite preserves base, offsets, cursors across a restart.
func TestWalCompactRewritesCheckpoint(t *testing.T) {
	path := tmpWal(t)
	s, err := NewWalStore(path)
	if err != nil {
		t.Fatalf("open wal: %v", err)
	}
	for i := 0; i < 4; i++ {
		if _, err := s.Append(pubEvent("e_"+strconv.Itoa(i), int64(i), "pod-0")); err != nil {
			t.Fatalf("append %d: %v", i, err)
		}
	}
	if err := s.Commit(collectorGroup, "collector-0", 1); err != nil {
		t.Fatalf("commit: %v", err)
	}
	s.Trim(1)
	s.mu.Lock()
	s.size = compactThresholdBytes + 1
	s.mu.Unlock()
	s.maybeCompactLocked()
	if err := s.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	s2, err := NewWalStore(path)
	if err != nil {
		t.Fatalf("reopen after compact: %v", err)
	}
	defer s2.Close()
	entries, err := s2.Range(-1)
	if err != nil || len(entries) != 2 {
		t.Fatalf("range after compact: n=%d err=%v, want 2", len(entries), err)
	}
	for i, le := range entries {
		if le.Offset != int64(i+2) {
			t.Fatalf("entry %d offset=%d, want %d", i, le.Offset, i+2)
		}
	}
	var collector int64 = -1
	for _, cs := range s2.Cursors() {
		if cs.Group == collectorGroup && cs.Consumer == "collector-0" {
			collector = cs.Offset
		}
	}
	if collector != 1 {
		t.Fatalf("collector cursor after compact = %d, want 1", collector)
	}
}

// TestWalDoubleOpenLocked: a data-dir is owned by one process at a time.
func TestWalDoubleOpenLocked(t *testing.T) {
	path := tmpWal(t)
	s, err := NewWalStore(path)
	if err != nil {
		t.Fatalf("open wal: %v", err)
	}
	defer s.Close()
	if _, err := NewWalStore(path); err == nil || !strings.Contains(err.Error(), "locked") {
		t.Fatalf("second open err = %v, want flock failure", err)
	}
}

// TestWalServerCrashRecovery: end-to-end at the RPC level - the MQ is stopped
// after acked publishes and a fresh Server on the same log resumes identically.
func TestWalServerCrashRecovery(t *testing.T) {
	path := tmpWal(t)
	store1, err := NewWalStore(path)
	if err != nil {
		t.Fatalf("open wal: %v", err)
	}
	srv1 := NewWithStore(time.Minute, store1)
	addr := startServer(t, srv1)
	c := dial(t, addr)
	ctx := context.Background()

	stream, err := c.PublishEvents(ctx)
	if err != nil {
		t.Fatalf("open publish: %v", err)
	}
	for i := 0; i < 3; i++ {
		if err := stream.Send(pubEvent("e_crash_"+strconv.Itoa(i), int64(i), "pod-0")); err != nil {
			t.Fatalf("send: %v", err)
		}
		if _, err := stream.Recv(); err != nil {
			t.Fatalf("ack: %v", err)
		}
	}
	if _, err := c.CommitOffset(ctx, &mqpb.CommitOffsetRequest{ConsumerId: "collector-0", Group: collectorGroup, Offset: 2}); err != nil {
		t.Fatalf("commit: %v", err)
	}
	// Crash: the server dies; each acked append was already fsynced.
	if err := store1.Close(); err != nil {
		t.Fatalf("store close: %v", err)
	}

	store2, err := NewWalStore(path)
	if err != nil {
		t.Fatalf("reopen after crash: %v", err)
	}
	defer store2.Close()
	srv2 := NewWithStore(time.Minute, store2)
	addr2 := startServer(t, srv2)
	c2 := dial(t, addr2)

	ack, err := publishOne(t, c2, ctx, pubEvent("e_crash_3", 3, "pod-0"))
	if err != nil || ack.Offset != 3 {
		t.Fatalf("post-crash publish ack = %+v err=%v, want offset=3", ack, err)
	}
	entries := consumeAll(t, c2, ctx, -1)
	if len(entries) != 4 {
		t.Fatalf("consumed %d after restart, want 4 (3 replayed + 1 new)", len(entries))
	}
	for i, le := range entries {
		if le.Offset != int64(i) {
			t.Fatalf("entry %d offset=%d, want %d", i, le.Offset, i)
		}
	}
}

// TestApplyCheckpointDropsDivergentLog pins the follower-side half of the
// replication wedge.
//
// Entries are addressed as base+index, so ApplyCheckpoint moving the base
// while KEEPING a log written against the old base makes Tail() overshoot the
// leader's real tail. In production this made the follower request a resume
// offset ~290k events beyond the leader's log, so the leader had nothing to
// send, closed the stream, and with syncReplicas=1 every publish blocked
// forever. Applying a leader checkpoint must drop the divergent entries: the
// leader's log is authoritative.
func TestApplyCheckpointDropsDivergentLog(t *testing.T) {
	s := newWal(t)
	for i := int64(0); i < 5; i++ {
		if _, err := s.Append(&mqpb.Event{EventId: fmt.Sprintf("e_%d", i), CsvLineOffset: i + 1}); err != nil {
			t.Fatalf("append %d: %v", i, err)
		}
	}
	if got := s.Tail(); got != 5 {
		t.Fatalf("precondition: Tail() = %d, want 5", got)
	}

	// The leader reports a base far ahead of this follower's log. Mirrors
	// production: follower base 286780 with 287184 retained entries while the
	// leader had already trimmed to base 377340.
	leaderBase := int64(377340)
	if err := s.ApplyCheckpoint(&mqpb.ReplicaCheckpoint{Base: leaderBase}); err != nil {
		t.Fatalf("apply checkpoint: %v", err)
	}

	if got := s.Base(); got != leaderBase {
		t.Fatalf("Base() = %d, want leader base %d", got, leaderBase)
	}
	// The critical assertion: Tail() must not overshoot the leader's tail. The
	// divergent entries would have made this 377340+5.
	if got := s.Tail(); got != leaderBase {
		t.Fatalf("Tail() = %d, want %d: stale log kept, resume offset would overshoot the leader", got, leaderBase)
	}

	// The dropped events must not resurface: Range from the new base is empty
	// until the leader re-sends them.
	entries, err := s.Range(leaderBase - 1)
	if err != nil {
		t.Fatalf("range: %v", err)
	}
	if len(entries) != 0 {
		t.Fatalf("Range returned %d entries, want 0 (divergent log was kept)", len(entries))
	}

	// A re-sent event lands at the leader's base, proving the store is usable.
	off, err := s.Append(&mqpb.Event{EventId: "from_leader", CsvLineOffset: 99})
	if err != nil {
		t.Fatalf("append after checkpoint: %v", err)
	}
	if off != leaderBase {
		t.Fatalf("append offset = %d, want %d", off, leaderBase)
	}
}
