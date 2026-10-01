package mq

import (
	"context"
	"fmt"
	"io"
	"testing"
	"time"

	mqpb "streamer/proto"
)

// publish sends n events (unique ids) on one publish stream for the pod,
// sequentially, and expects an ack per event.
func publish(t *testing.T, c mqpb.MessageQueueClient, pod string, ids []string, offsets []int64) {
	t.Helper()
	ctx := context.Background()
	stream, err := c.PublishEvents(ctx)
	if err != nil {
		t.Fatalf("open publish stream: %v", err)
	}
	for i, id := range ids {
		if err := stream.Send(&mqpb.Event{EventId: id, CsvLineOffset: offsets[i], Tags: map[string]string{"pod_name": pod}}); err != nil {
			t.Fatalf("send %q: %v", id, err)
		}
		if ack, err := stream.Recv(); err != nil {
			t.Fatalf("recv ack %q: %v", id, err)
		} else if ack.Offset != offsets[i] {
			t.Fatalf("ack offset = %d, want %d", ack.Offset, offsets[i])
		}
	}
	stream.CloseSend()
}

// consumeAll reads a Consume stream until EOF and returns the delivered events.
func consumeAll(t *testing.T, c mqpb.MessageQueueClient, ctx context.Context, start int64) []*mqpb.ConsumedEvent {
	t.Helper()
	stream, err := c.Consume(ctx, &mqpb.ConsumeRequest{ConsumerId: "collector-0", StartOffset: start, Group: collectorGroup})
	if err != nil {
		t.Fatalf("open consume stream: %v", err)
	}
	var out []*mqpb.ConsumedEvent
	for {
		ev, err := stream.Recv()
		if err == io.EOF {
			return out
		}
		if err != nil {
			t.Fatalf("consume recv: %v", err)
		}
		out = append(out, ev)
	}
}

func offsetsOf(evs []*mqpb.ConsumedEvent) []int64 {
	out := make([]int64, 0, len(evs))
	for _, e := range evs {
		out = append(out, e.Offset)
	}
	return out
}

func eqOffsets(got, want []int64) bool {
	if len(got) != len(want) {
		return false
	}
	for i := range got {
		if got[i] != want[i] {
			return false
		}
	}
	return true
}

func TestConsumeFromHeadPublishesAll(t *testing.T) {
	addr := startServer(t, New(time.Minute))
	c := dial(t, addr)
	ctx := context.Background()

	publish(t, c, "pod-0", []string{"e1", "e2", "e3"}, []int64{0, 1, 2})

	evs := consumeAll(t, c, ctx, -1)
	if !eqOffsets(offsetsOf(evs), []int64{0, 1, 2}) {
		t.Fatalf("consume from head = %v, want [0 1 2]", offsetsOf(evs))
	}
	for i, e := range evs {
		if e.Event.EventId != []string{"e1", "e2", "e3"}[i] {
			t.Fatalf("event %d mismatch: got %s", i, e.Event.EventId)
		}
	}
}

func TestConsumeFromCursorMidLog(t *testing.T) {
	addr := startServer(t, New(time.Minute))
	c := dial(t, addr)
	ctx := context.Background()

	publish(t, c, "pod-0", []string{"e1", "e2", "e3", "e4", "e5"}, []int64{0, 1, 2, 3, 4})

	evs := consumeAll(t, c, ctx, 2)
	if !eqOffsets(offsetsOf(evs), []int64{3, 4}) {
		t.Fatalf("consume from cursor 2 = %v, want [3 4]", offsetsOf(evs))
	}
}

func TestConsumeEmptyTailImmediateEOF(t *testing.T) {
	addr := startServer(t, New(time.Minute))
	c := dial(t, addr)
	ctx := context.Background()

	publish(t, c, "pod-0", []string{"e1", "e2", "e3", "e4", "e5"}, []int64{0, 1, 2, 3, 4})

	if evs := consumeAll(t, c, ctx, 4); len(evs) != 0 {
		t.Fatalf("consume past tail = %v, want empty", offsetsOf(evs))
	}
}

func TestGlobalOffsetContinuityAcrossPublishers(t *testing.T) {
	addr := startServer(t, New(time.Minute))
	c := dial(t, addr)
	ctx := context.Background()

	publish(t, c, "pod-0", []string{"a1", "a2"}, []int64{0, 1})
	publish(t, c, "pod-1", []string{"b1", "b2"}, []int64{0, 1})

	evs := consumeAll(t, c, ctx, -1)
	if !eqOffsets(offsetsOf(evs), []int64{0, 1, 2, 3}) {
		t.Fatalf("offsets across two publishers = %v, want contiguous [0 1 2 3]", offsetsOf(evs))
	}
}

func TestCommitOffsetAndGetOffsetMonotonic(t *testing.T) {
	addr := startServer(t, New(time.Minute))
	c := dial(t, addr)
	ctx := context.Background()

	// The offsets committed below have to exist: a collector cursor is an
	// offset the consumer actually processed, so a cursor past the log tail
	// describes an unreachable log incarnation and GetOffset re-anchors it
	// (see ErrCursorAheadOfTail). Publish enough to make 0..6 reachable.
	publish(t, c, "pod-0", []string{"e1", "e2", "e3", "e4", "e5", "e6", "e7"}, []int64{0, 1, 2, 3, 4, 5, 6})

	if _, err := c.CommitOffset(ctx, &mqpb.CommitOffsetRequest{ConsumerId: "collector-0", Offset: 4, Group: collectorGroup}); err != nil {
		t.Fatalf("commit 4: %v", err)
	}
	if _, err := c.CommitOffset(ctx, &mqpb.CommitOffsetRequest{ConsumerId: "collector-0", Offset: 2, Group: collectorGroup}); err != nil {
		t.Fatalf("stale commit: %v", err)
	}
	resp, err := c.GetOffset(ctx, &mqpb.GetOffsetRequest{ConsumerId: "collector-0", Group: collectorGroup})
	if err != nil || !resp.Exists || resp.Offset != 4 {
		t.Fatalf("get offset = %+v err=%v, want exists=true offset=4 (monotonic)", resp, err)
	}
	if _, err := c.CommitOffset(ctx, &mqpb.CommitOffsetRequest{ConsumerId: "collector-0", Offset: 6, Group: collectorGroup}); err != nil {
		t.Fatalf("commit 6: %v", err)
	}
	resp, _ = c.GetOffset(ctx, &mqpb.GetOffsetRequest{ConsumerId: "collector-0", Group: collectorGroup})
	if !resp.Exists || resp.Offset != 6 {
		t.Fatalf("get offset after forward commit = %+v, want 6", resp)
	}
}

func TestRetentionTrimsCommittedTail(t *testing.T) {
	addr := startServer(t, New(time.Minute))
	c := dial(t, addr)
	ctx := context.Background()

	publish(t, c, "pod-0", []string{"e1", "e2", "e3", "e4", "e5"}, []int64{0, 1, 2, 3, 4})
	if _, err := c.JoinPartition(ctx, &mqpb.JoinPartitionRequest{ConsumerId: "collector-0", Group: collectorGroup, TtlSeconds: 60}); err != nil {
		t.Fatalf("join: %v", err)
	}
	if _, err := c.CommitOffset(ctx, &mqpb.CommitOffsetRequest{ConsumerId: "collector-0", Offset: 4, Group: collectorGroup}); err != nil {
		t.Fatalf("commit: %v", err)
	}
	publish(t, c, "pod-0", []string{"e6"}, []int64{5})

	// Events 0..4 are durably handled (committed) and trimmed; head = offset 5.
	evs := consumeAll(t, c, ctx, -1)
	if !eqOffsets(offsetsOf(evs), []int64{5}) {
		t.Fatalf("consume after trim = %v, want [5]", offsetsOf(evs))
	}
}

func TestRetentionHonorsSlowestCommittedCollector(t *testing.T) {
	addr := startServer(t, New(time.Minute))
	c := dial(t, addr)
	ctx := context.Background()

	publish(t, c, "pod-0", []string{"e0", "e1", "e2", "e3", "e4"}, []int64{0, 1, 2, 3, 4})
	for _, id := range []string{"c0", "c1"} {
		if _, err := c.JoinPartition(ctx, &mqpb.JoinPartitionRequest{ConsumerId: id, Group: collectorGroup, TtlSeconds: 60}); err != nil {
			t.Fatalf("join %s: %v", id, err)
		}
	}
	if _, err := c.CommitOffset(ctx, &mqpb.CommitOffsetRequest{ConsumerId: "c0", Offset: 4, Group: collectorGroup}); err != nil {
		t.Fatalf("commit c0: %v", err)
	}
	if _, err := c.CommitOffset(ctx, &mqpb.CommitOffsetRequest{ConsumerId: "c1", Offset: 1, Group: collectorGroup}); err != nil {
		t.Fatalf("commit c1: %v", err)
	}
	publish(t, c, "pod-0", []string{"e5", "e6", "e7"}, []int64{5, 6, 7})

	// floor = min(4,1) = 1 -> retained head = offset 2.
	evs := consumeAll(t, c, ctx, -1)
	if !eqOffsets(offsetsOf(evs), []int64{2, 3, 4, 5, 6, 7}) {
		t.Fatalf("consume with slowest c1 = %v, want [2 3 4 5 6 7]", offsetsOf(evs))
	}

	// c1 catches up -> floor = min(4,6) = 4 -> head = 5.
	if _, err := c.CommitOffset(ctx, &mqpb.CommitOffsetRequest{ConsumerId: "c1", Offset: 6, Group: collectorGroup}); err != nil {
		t.Fatalf("commit c1 catch-up: %v", err)
	}
	evs = consumeAll(t, c, ctx, -1)
	if !eqOffsets(offsetsOf(evs), []int64{5, 6, 7}) {
		t.Fatalf("consume after c1 catches up = %v, want [5 6 7]", offsetsOf(evs))
	}
}

func TestRetentionStopsForUncommittedCollector(t *testing.T) {
	addr := startServer(t, New(time.Minute))
	c := dial(t, addr)
	ctx := context.Background()

	publish(t, c, "pod-0", []string{"e0", "e1", "e2", "e3", "e4"}, []int64{0, 1, 2, 3, 4})
	if _, err := c.JoinPartition(ctx, &mqpb.JoinPartitionRequest{ConsumerId: "c0", Group: collectorGroup, TtlSeconds: 60}); err != nil {
		t.Fatalf("join c0: %v", err)
	}
	if _, err := c.JoinPartition(ctx, &mqpb.JoinPartitionRequest{ConsumerId: "fresh", Group: collectorGroup, TtlSeconds: 60}); err != nil {
		t.Fatalf("join fresh: %v", err)
	}
	if _, err := c.CommitOffset(ctx, &mqpb.CommitOffsetRequest{ConsumerId: "c0", Offset: 4, Group: collectorGroup}); err != nil {
		t.Fatalf("commit c0: %v", err)
	}
	publish(t, c, "pod-0", []string{"e5", "e6", "e7"}, []int64{5, 6, 7})

	// "fresh" has joined but never committed: the retention barrier must not
	// move, or it would lose data the fresh collector still needs from the head.
	evs := consumeAll(t, c, ctx, -1)
	if !eqOffsets(offsetsOf(evs), []int64{0, 1, 2, 3, 4, 5, 6, 7}) {
		t.Fatalf("consume with uncommitted collector = %v, want everything [0..7]", offsetsOf(evs))
	}

	// Once fresh commits (to 2), floor = min(4,2) = 2 -> head = 3.
	if _, err := c.CommitOffset(ctx, &mqpb.CommitOffsetRequest{ConsumerId: "fresh", Offset: 2, Group: collectorGroup}); err != nil {
		t.Fatalf("commit fresh: %v", err)
	}
	evs = consumeAll(t, c, ctx, -1)
	if !eqOffsets(offsetsOf(evs), []int64{3, 4, 5, 6, 7}) {
		t.Fatalf("consume after fresh commits = %v, want [3 4 5 6 7]", offsetsOf(evs))
	}
}

func TestRetentionNoCollectorsKeepsLog(t *testing.T) {
	addr := startServer(t, New(time.Minute))
	c := dial(t, addr)
	ctx := context.Background()

	publish(t, c, "pod-0", []string{"e0", "e1", "e2"}, []int64{0, 1, 2})
	// A streamer-group commit must not move the (collector-group) barrier.
	if _, err := c.CommitOffset(ctx, &mqpb.CommitOffsetRequest{ConsumerId: "pod-0", Offset: 100, Group: streamerGroup}); err != nil {
		t.Fatalf("commit under streamer group: %v", err)
	}
	evs := consumeAll(t, c, ctx, -1)
	if !eqOffsets(offsetsOf(evs), []int64{0, 1, 2}) {
		t.Fatalf("log trimmed without collector commits = %v, want [0 1 2]", offsetsOf(evs))
	}
}

func TestGroupNamespacedRegistryAssignments(t *testing.T) {
	addr := startServer(t, New(time.Minute))
	c := dial(t, addr)
	ctx := context.Background()

	join := func(group, id string) (int32, int32) {
		t.Helper()
		resp, err := c.JoinPartition(ctx, &mqpb.JoinPartitionRequest{ConsumerId: id, Group: group, TtlSeconds: 60})
		if err != nil {
			t.Fatalf("join %s/%s: %v", group, id, err)
		}
		return resp.Index, resp.Total
	}

	if i, tot := join(streamerGroup, "s1"); i != 0 || tot != 1 {
		t.Fatalf("s1 = %d/%d, want 0/1", i, tot)
	}
	if i, tot := join(streamerGroup, "s2"); i != 1 || tot != 2 {
		t.Fatalf("s2 = %d/%d, want 1/2", i, tot)
	}
	if i, tot := join(collectorGroup, "x"); i != 0 || tot != 1 {
		t.Fatalf("x = %d/%d, want 0/1", i, tot)
	}
	if i, tot := join(collectorGroup, "y"); i != 1 || tot != 2 {
		t.Fatalf("y = %d/%d, want 1/2", i, tot)
	}
	if i, tot := join(collectorGroup, "z"); i != 2 || tot != 3 {
		t.Fatalf("z = %d/%d, want 2/3", i, tot)
	}
	// streamer "s2" slot is unaffected by the collector joins.
	if i, tot := join(streamerGroup, "s2"); i != 1 || tot != 2 {
		t.Fatalf("s2 after collector joins = %d/%d, want 1/2", i, tot)
	}
}

func TestCursorAndAckIsolationBetweenGroups(t *testing.T) {
	addr := startServer(t, New(time.Minute))
	c := dial(t, addr)
	ctx := context.Background()

	// "shared" publishes e1 with CSV line 7, so the streamer-group cursor for
	// this pod is 7 (a CSV source position, a different numbering space from
	// the log). The filler events go to another pod and carry no CSV offset, so
	// they lengthen the log without disturbing the streamer cursor being
	// asserted here.
	publish(t, c, "shared", []string{"e1"}, []int64{7})
	filler := make([]string, 43)
	lines := make([]int64, 43)
	for i := range filler {
		filler[i] = fmt.Sprintf("f%d", i)
	}
	publish(t, c, "filler", filler, lines)
	if _, err := c.CommitOffset(ctx, &mqpb.CommitOffsetRequest{ConsumerId: "shared", Offset: 42, Group: collectorGroup}); err != nil {
		t.Fatalf("commit: %v", err)
	}

	// The collector-group commit is invisible in the streamer group...
	got, err := c.GetOffset(ctx, &mqpb.GetOffsetRequest{ConsumerId: "shared", Group: streamerGroup})
	if err != nil || !got.Exists || got.Offset != 7 {
		t.Fatalf("streamer-group ack = %+v err=%v, want exists=true offset=7", got, err)
	}
	_, err = c.GetOffset(ctx, &mqpb.GetOffsetRequest{ConsumerId: "shared", Group: collectorGroup})
	if err != nil {
		t.Fatalf("collector-group get offset: %v", err)
	}
	got, _ = c.GetOffset(ctx, &mqpb.GetOffsetRequest{ConsumerId: "shared", Group: collectorGroup})
	if !got.Exists || got.Offset != 42 {
		t.Fatalf("collector-group cursor = %+v, want offset=42", got)
	}
}
