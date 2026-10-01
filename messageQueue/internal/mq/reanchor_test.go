package mq

import (
	"context"
	"errors"
	"io"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	mqpb "streamer/proto"
)

// drain reads a consume stream to EOF and returns the offsets it saw.
func drain(t *testing.T, c mqpb.MessageQueueClient, consumer string, start int64) ([]int64, error) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	stream, err := c.Consume(ctx, &mqpb.ConsumeRequest{ConsumerId: consumer, StartOffset: start})
	if err != nil {
		return nil, err
	}
	var offs []int64
	for {
		msg, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			return offs, nil
		}
		if err != nil {
			return offs, err
		}
		offs = append(offs, msg.Offset)
	}
}

// TestCaughtUpConsumerGetsAnEmptyStreamNotAnError pins the boundary between
// "you are up to date" and "your cursor is unreachable". A consumer that has
// consumed everything sits at tail-1 and must keep getting a clean empty
// snapshot, which is how it knows to poll again. If that case started erroring
// the collector would treat normal idleness as a fault.
func TestCaughtUpConsumerGetsAnEmptyStreamNotAnError(t *testing.T) {
	addr := startServer(t, New(time.Minute))
	client := dial(t, addr)
	publishN(t, client, "e", 5)

	tail := int64(5) // offsets 0..4
	offs, err := drain(t, client, "collector", tail-1)
	if err != nil {
		t.Fatalf("caught-up consumer must get a clean EOF, got %v", err)
	}
	if len(offs) != 0 {
		t.Fatalf("expected no events past the tail, got %v", offs)
	}
}

// TestCursorAheadOfTailIsOutOfRange is the regression test for the silent
// data-loss stall.
//
// A consumer's committed cursor can name an offset the current log incarnation
// never had: the leader it committed against was lost, and the promoted
// survivor's log is shorter (it was behind, partitioned, or had trimmed past
// that point). Returning an empty snapshot in that case is indistinguishable
// from "caught up", so the collector polls forever, ingests nothing, logs
// nothing, and every health check stays green. It must be an explicit
// OutOfRange so the client knows to re-anchor.
func TestCursorAheadOfTailIsOutOfRange(t *testing.T) {
	addr := startServer(t, New(time.Minute))
	client := dial(t, addr)
	publishN(t, client, "e", 5) // offsets 0..4

	_, err := drain(t, client, "collector", 48_115_077)
	if status.Code(err) != codes.OutOfRange {
		t.Fatalf("stale cursor must surface as OutOfRange, got %v (code %v)", err, status.Code(err))
	}
	if !strings.Contains(err.Error(), "48115077") {
		t.Fatalf("error should quote the offending offset so operators can see the gap: %v", err)
	}
}

// TestGetOffsetReanchorsUnreachableCursor covers the recovery half: the broker
// repairs the cursor onto the retained head and tells the consumer where to
// resume, so the consumer can make progress instead of stalling.
func TestGetOffsetReanchorsUnreachableCursor(t *testing.T) {
	addr := startServer(t, New(time.Minute))
	client := dial(t, addr)
	ctx := context.Background()
	publishN(t, client, "e", 5)

	// Commit a cursor that this log incarnation cannot satisfy.
	if _, err := client.CommitOffset(ctx, &mqpb.CommitOffsetRequest{ConsumerId: "collector", Offset: 48_115_077, Group: collectorGroup}); err != nil {
		t.Fatalf("commit: %v", err)
	}

	got, err := client.GetOffset(ctx, &mqpb.GetOffsetRequest{ConsumerId: "collector", Group: collectorGroup})
	if err != nil {
		t.Fatalf("GetOffset: %v", err)
	}
	if !got.Exists {
		t.Fatal("cursor exists, GetOffset must not report a fresh consumer")
	}
	if got.Offset != -1 {
		t.Fatalf("re-anchor point=%d, want -1 (just before the retained head)", got.Offset)
	}

	// And the repaired cursor must actually be consumable, not merely reported.
	offs, err := drain(t, client, "collector", got.Offset)
	if err != nil {
		t.Fatalf("consume from re-anchored cursor: %v", err)
	}
	if len(offs) != 5 {
		t.Fatalf("re-anchored read returned %d events, want 5", len(offs))
	}
}

// TestGetOffsetLeavesReachableCursorsAlone: the repair must not fire on a
// normal caught-up cursor, or every poll would rewind and replay the log.
func TestGetOffsetLeavesReachableCursorsAlone(t *testing.T) {
	addr := startServer(t, New(time.Minute))
	client := dial(t, addr)
	ctx := context.Background()
	publishN(t, client, "e", 5)

	for _, want := range []int64{0, 2, 4} {
		if _, err := client.CommitOffset(ctx, &mqpb.CommitOffsetRequest{ConsumerId: "collector", Offset: want, Group: collectorGroup}); err != nil {
			t.Fatalf("commit %d: %v", want, err)
		}
		got, err := client.GetOffset(ctx, &mqpb.GetOffsetRequest{ConsumerId: "collector", Group: collectorGroup})
		if err != nil {
			t.Fatalf("GetOffset: %v", err)
		}
		if got.Offset != want {
			t.Fatalf("reachable cursor %d was rewritten to %d", want, got.Offset)
		}
	}
}

// TestReanchoredCursorSurvivesRestart is what separates a real fix from an
// in-memory patch. The rewind must be durable, otherwise every restart repeats
// the stall and the pipeline never recovers without operator action.
func TestReanchoredCursorSurvivesRestart(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "wal.log")

	func() {
		st, err := NewWalStore(path)
		if err != nil {
			t.Fatalf("open wal: %v", err)
		}
		for i := 0; i < 5; i++ {
			if _, err := st.Append(&mqpb.Event{EventId: "e", CsvLineOffset: int64(i)}); err != nil {
				t.Fatalf("append: %v", err)
			}
		}
		if err := st.Commit(collectorGroup, "collector", 48_115_077); err != nil {
			t.Fatalf("commit: %v", err)
		}
		if err := st.Close(); err != nil {
			t.Fatalf("close: %v", err)
		}
	}()

	st, err := NewWalStore(path)
	if err != nil {
		t.Fatalf("reopen wal: %v", err)
	}
	t.Cleanup(func() { st.Close() })
	s := NewWithStore(time.Minute, st)
	if err := NewHA(s).Promote(0); err != nil {
		t.Fatalf("promote: %v", err)
	}
	ctx := context.Background()

	got, err := s.GetOffset(ctx, &mqpb.GetOffsetRequest{ConsumerId: "collector", Group: collectorGroup})
	if err != nil {
		t.Fatalf("GetOffset: %v", err)
	}
	if got.Offset != -1 {
		t.Fatalf("after restart GetOffset=%d, want -1", got.Offset)
	}

	// The durable value must be the rewound one, or the next restart repeats.
	if err := st.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	st2, err := NewWalStore(path)
	if err != nil {
		t.Fatalf("reopen wal 2: %v", err)
	}
	t.Cleanup(func() { st2.Close() })
	s2 := NewWithStore(time.Minute, st2)
	if err := NewHA(s2).Promote(0); err != nil {
		t.Fatalf("promote 2: %v", err)
	}
	got2, err := s2.GetOffset(ctx, &mqpb.GetOffsetRequest{ConsumerId: "collector", Group: collectorGroup})
	if err != nil {
		t.Fatalf("GetOffset 2: %v", err)
	}
	if got2.Offset != -1 {
		t.Fatalf("re-anchor did not survive restart: %d", got2.Offset)
	}
}

// TestRewindIsDistinctFromCommit documents why Commit could not be reused: it
// is monotonic by design, so a backwards move is silently dropped.
func TestRewindIsDistinctFromCommit(t *testing.T) {
	st, err := NewWalStore(filepath.Join(t.TempDir(), "wal.log"))
	if err != nil {
		t.Fatalf("open wal: %v", err)
	}
	t.Cleanup(func() { st.Close() })

	if err := st.Commit(collectorGroup, "collector", 100); err != nil {
		t.Fatalf("commit: %v", err)
	}
	if err := st.Commit(collectorGroup, "collector", 5); err != nil {
		t.Fatalf("commit backwards: %v", err)
	}
	for _, c := range st.Cursors() {
		if c.Consumer == "collector" && c.Offset != 100 {
			t.Fatalf("Commit must be monotonic: cursor went to %d", c.Offset)
		}
	}

	if err := st.Rewind(collectorGroup, "collector", -1); err != nil {
		t.Fatalf("rewind: %v", err)
	}
	found := false
	for _, c := range st.Cursors() {
		if c.Consumer == "collector" {
			found = true
			if c.Offset != -1 {
				t.Fatalf("Rewind did not move the cursor: %d", c.Offset)
			}
		}
	}
	if !found {
		t.Fatal("collector cursor vanished")
	}
}

// TestWALRangeReportsAheadOfTail applies the same contract to the durable
// store, which is what actually runs in production.
func TestWALRangeReportsAheadOfTail(t *testing.T) {
	st, err := NewWalStore(filepath.Join(t.TempDir(), "wal.log"))
	if err != nil {
		t.Fatalf("open wal: %v", err)
	}
	t.Cleanup(func() { st.Close() })
	for i := 0; i < 3; i++ {
		if _, err := st.Append(&mqpb.Event{EventId: "e", CsvLineOffset: int64(i)}); err != nil {
			t.Fatalf("append: %v", err)
		}
	}
	if _, err := st.Range(2); err != nil {
		t.Fatalf("caught-up read must succeed, got %v", err)
	}
	if _, err := st.Range(999); !errors.Is(err, ErrCursorAheadOfTail) {
		t.Fatalf("stale cursor must be ErrCursorAheadOfTail, got %v", err)
	}
}
