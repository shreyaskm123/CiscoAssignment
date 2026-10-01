package mq

import (
	"context"
	"testing"
	"time"

	mqpb "streamer/proto"
)

// These tests cover the HA contract at the Server level, independent of
// Kubernetes: promotion must not lose data, demotion must fence writes
// immediately, and a promoted follower must be able to keep acking. The
// election timing itself is covered in internal/election.

// publishN sends n events through a real publish stream and fails the test if
// any ack is missing.
func publishN(t *testing.T, c mqpb.MessageQueueClient, prefix string, n int) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	stream, err := c.PublishEvents(ctx)
	if err != nil {
		t.Fatalf("publish stream: %v", err)
	}
	for i := 0; i < n; i++ {
		if err := stream.Send(&mqpb.Event{
			EventId:       prefix + string(rune('a'+i%26)) + string(rune('0'+i/26)),
			CsvLineOffset: int64(i + 1),
			Tags:          map[string]string{"pod_name": "streamer-0"},
		}); err != nil {
			t.Fatalf("send: %v", err)
		}
		ack, err := stream.Recv()
		if err != nil {
			t.Fatalf("ack recv: %v", err)
		}
		if !ack.Success {
			t.Fatalf("ack not successful: %+v", ack)
		}
	}
	stream.CloseSend()
}

// TestFollowerRejectsWritesUntilPromoted is the safety property the whole
// design rests on: a node that has not won the lease must never ack a
// publisher, because a second writer would fork the log.
func TestFollowerRejectsWritesUntilPromoted(t *testing.T) {
	s := NewWithStore(time.Minute, newWal(t))
	ha := NewHA(s)
	ha.Demote()

	addr := startAll(t, s)
	c := dial(t, addr)
	ctx := context.Background()

	stream, err := c.PublishEvents(ctx)
	if err != nil {
		t.Fatalf("publish stream: %v", err)
	}
	if err := stream.Send(&mqpb.Event{EventId: "e1", CsvLineOffset: 1, Tags: map[string]string{"pod_name": "s0"}}); err != nil {
		t.Fatalf("send: %v", err)
	}
	if _, err := stream.Recv(); err == nil {
		t.Fatal("a follower must not ack a publish")
	}

	// CommitOffset is the other write path and must be fenced too: it drives
	// the retention barrier, and a follower accepting commits would trim the
	// log on cursors it cannot durably own.
	if _, err := c.CommitOffset(ctx, &mqpb.CommitOffsetRequest{Group: collectorGroup, ConsumerId: "c0", Offset: 5}); err == nil {
		t.Fatal("a follower must reject CommitOffset")
	}

	if err := ha.Promote(0); err != nil {
		t.Fatalf("promote: %v", err)
	}
	if _, err := c.CommitOffset(ctx, &mqpb.CommitOffsetRequest{Group: collectorGroup, ConsumerId: "c0", Offset: 5}); err != nil {
		t.Fatalf("after promotion CommitOffset must succeed: %v", err)
	}
}

// TestPromoteRefusesWithoutDurableStore guards the promotion path from silently
// accepting writes it cannot survive a restart with.
func TestPromoteRefusesWithoutDurableStore(t *testing.T) {
	s := New(time.Minute) // in-memory
	ha := NewHA(s)
	ha.Demote()
	if err := ha.Promote(0); err == nil {
		t.Fatal("promoting an in-memory node must fail: acks would not survive restart")
	}
	if s.IsLeader() {
		t.Fatal("a refused promotion must leave the node read-only")
	}
}

// TestPromotionKeepsCommittedOffsetsAndLog is the no-data-loss property: a
// client that committed offset N before a failover must resume at N+1 against
// the promoted node, with no gap and no replay. If promotion lost or rewound
// state, the collector would either skip events or re-read a large window.
func TestPromotionKeepsCommittedOffsetsAndLog(t *testing.T) {
	leaderSrv := NewWithStore(time.Minute, newWal(t))
	leaderAddr := startAll(t, leaderSrv)
	leaderClient := dial(t, leaderAddr)

	followerSrv := newFollower(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go RunFollower(ctx, leaderAddr, followerSrv)

	// The leader publishes and a collector commits partway in.
	publishN(t, leaderClient, "pre_", 3)
	fc := dial(t, startAll(t, leaderSrv))
	// Log offsets are 0-based, so committing 1 means "offsets 0 and 1 are
	// durably processed" and a resume must start at 2.
	if _, err := fc.CommitOffset(ctx, &mqpb.CommitOffsetRequest{Group: collectorGroup, ConsumerId: "collector-0", Offset: 1}); err != nil {
		t.Fatalf("commit: %v", err)
	}

	// Wait until the follower holds every frame, so the promotion below is
	// starting from a fully caught-up replica rather than a lucky snapshot.
	//
	// Both the log AND the cursor must have replicated. Waiting on the tail
	// alone is racy: the commit frame is enqueued after the appends, so a
	// follower can have all three events and still be missing the cursor.
	// Promoting in that window loses the collector's position.
	waitFor(t, "follower to replicate the pre-failover log", func() bool {
		return followerSrv.store.Tail() == leaderSrv.store.Tail() && followerSrv.store.Tail() == 3
	})
	waitFor(t, "follower to replicate the pre-failover cursor", func() bool {
		for _, c := range followerSrv.store.(DurableStore).Cursors() {
			if c.Group == collectorGroup && c.Consumer == "collector-0" && c.Offset == 1 {
				return true
			}
		}
		return false
	})

	// Failover: the leader dies, the follower is promoted in place. No restart,
	// no new store, no replay.
	ha := NewHA(followerSrv)
	if err := ha.Promote(0); err != nil {
		t.Fatalf("promote: %v", err)
	}
	fa := dial(t, startAll(t, followerSrv))

	got, err := fa.GetOffset(ctx, &mqpb.GetOffsetRequest{Group: collectorGroup, ConsumerId: "collector-0"})
	if err != nil {
		t.Fatalf("GetOffset: %v", err)
	}
	if !got.Exists || got.Offset != 1 {
		t.Fatalf("committed cursor lost across promotion: exists=%v offset=%d (want 1)", got.Exists, got.Offset)
	}

	// Consuming from the committed cursor must yield exactly the unprocessed
	// event - offset 3 - proving neither a gap nor a duplicate replay.
	stream, err := fa.Consume(ctx, &mqpb.ConsumeRequest{StartOffset: got.Offset, ConsumerId: "collector-0", Group: collectorGroup})
	if err != nil {
		t.Fatalf("consume: %v", err)
	}
	ev, err := stream.Recv()
	if err != nil {
		t.Fatalf("expected the event after the committed cursor: %v", err)
	}
	if ev.Offset != 2 {
		t.Fatalf("resumed at the wrong offset: got %d want 2 (log offsets are 0-based)", ev.Offset)
	}
	if _, err := stream.Recv(); err == nil {
		t.Fatal("consume replayed events the collector had already committed")
	}
}

// TestDemoteFencesWritesImmediately is the split-brain guard at the Server
// level: the moment leadership is withdrawn, writes must be refused even though
// the process is still perfectly healthy and serving reads.
func TestDemoteFencesWritesImmediately(t *testing.T) {
	s := NewWithStore(time.Minute, newWal(t))
	ha := NewHA(s)
	addr := startAll(t, s)
	c := dial(t, addr)
	ctx := context.Background()

	publishN(t, c, "pre_", 2)
	if !s.IsLeader() {
		t.Fatal("setup: expected leader")
	}

	ha.Demote()
	if s.IsLeader() {
		t.Fatal("Demote must clear leadership")
	}

	stream, err := c.PublishEvents(ctx)
	if err != nil {
		t.Fatalf("publish stream: %v", err)
	}
	if err := stream.Send(&mqpb.Event{EventId: "post-demote", CsvLineOffset: 99, Tags: map[string]string{"pod_name": "s0"}}); err != nil {
		t.Fatalf("send: %v", err)
	}
	if _, err := stream.Recv(); err == nil {
		t.Fatal("a demoted node must not ack a publish")
	}
	// Reads must keep working: a follower serves them from its own log.
	rstream, err := c.Consume(ctx, &mqpb.ConsumeRequest{StartOffset: -1, ConsumerId: "c", Group: collectorGroup})
	if err != nil {
		t.Fatalf("consume: %v", err)
	}
	if _, err := rstream.Recv(); err != nil {
		t.Fatalf("a follower must still serve reads: %v", err)
	}
}

// TestDemoteIsIdempotent: fencing runs on every election transition, including
// repeated losses and shutdown, so it must be safe to call any number of times.
func TestDemoteIsIdempotent(t *testing.T) {
	s := NewWithStore(time.Minute, newWal(t))
	ha := NewHA(s)
	ha.Demote()
	ha.Demote()
	if err := ha.Promote(0); err != nil {
		t.Fatalf("promote: %v", err)
	}
	ha.Demote()
	ha.Demote()
	if s.IsLeader() {
		t.Fatal("repeated Demote must leave the node read-only")
	}
	// Term counts promotions, so an operator can tell a survivor from a fresh node.
	if got := ha.Term(); got != 1 {
		t.Fatalf("Term()=%d, want 1", got)
	}
	if err := ha.Promote(0); err != nil {
		t.Fatalf("re-promote: %v", err)
	}
	if got := ha.Term(); got != 2 {
		t.Fatalf("Term() after re-promotion=%d, want 2", got)
	}
}

// TestPromotedFollowerStillAcksNewWrites is the availability half of the
// failover story: after promotion the survivor must actually serve the pipeline,
// not just report the right offsets.
func TestPromotedFollowerStillAcksNewWrites(t *testing.T) {
	leaderSrv := NewWithStore(time.Minute, newWal(t))
	leaderAddr := startAll(t, leaderSrv)
	lc := dial(t, leaderAddr)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	followerSrv := newFollower(t)
	go RunFollower(ctx, leaderAddr, followerSrv)

	publishN(t, lc, "pre_", 3)
	waitFor(t, "follower catch-up", func() bool { return followerSrv.store.Tail() == 3 })

	ha := NewHA(followerSrv)
	if err := ha.Promote(0); err != nil {
		t.Fatalf("promote: %v", err)
	}
	fc := dial(t, startAll(t, followerSrv))

	// Publishing continues against the promoted node, and the log grows from
	// where the old leader left off rather than restarting at offset 0.
	publishN(t, fc, "post_", 3)
	if got := followerSrv.store.Tail(); got != 6 {
		t.Fatalf("log tail after failover=%d, want 6 (no rewind, no gap)", got)
	}
}

// TestPromotionRebuildsDedupWindow: the follower's dedup window is not
// replicated, so promotion must rebuild it from its own durable log. Without
// this a streamer reconnecting after a failover would re-publish events already
// stored and they would be appended twice.
func TestPromotionRebuildsDedupWindow(t *testing.T) {
	leaderSrv := NewWithStore(time.Minute, newWal(t))
	leaderAddr := startAll(t, leaderSrv)
	lc := dial(t, leaderAddr)

	followerSrv := newFollower(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go RunFollower(ctx, leaderAddr, followerSrv)

	publishN(t, lc, "dup_", 3)
	waitFor(t, "follower catch-up", func() bool { return followerSrv.store.Tail() == 3 })

	ha := NewHA(followerSrv)
	if err := ha.Promote(0); err != nil {
		t.Fatalf("promote: %v", err)
	}
	fa := dial(t, startAll(t, followerSrv))

	// Re-send an event_id that is already in the log. It must be acked (the
	// producer makes progress) but not appended a second time.
	before := followerSrv.store.Tail()
	stream, err := fa.PublishEvents(ctx)
	if err != nil {
		t.Fatalf("publish stream: %v", err)
	}
	if err := stream.Send(&mqpb.Event{EventId: "dup_a0", CsvLineOffset: 1, Tags: map[string]string{"pod_name": "s0"}}); err != nil {
		t.Fatalf("send: %v", err)
	}
	ack, err := stream.Recv()
	if err != nil {
		t.Fatalf("ack: %v", err)
	}
	if !ack.Success {
		t.Fatal("a replayed event must still be acked so the producer progresses")
	}
	if ack.Error == "" {
		t.Fatal("ack should report the event as a duplicate")
	}
	if after := followerSrv.store.Tail(); after != before {
		t.Fatalf("duplicate was appended again: tail %d -> %d", before, after)
	}
}
