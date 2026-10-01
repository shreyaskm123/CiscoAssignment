package mq

import (
	"context"
	"errors"
	"io"
	"net"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	mqpb "streamer/proto"
)

func startAll(t *testing.T, s *Server) string {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	srv := grpc.NewServer()
	mqpb.RegisterMessageQueueServer(srv, s)
	mqpb.RegisterReplicationServer(srv, s)
	go srv.Serve(lis)
	t.Cleanup(srv.Stop)
	return lis.Addr().String()
}

func newWal(t *testing.T) *WALStore {
	t.Helper()
	s, err := NewWalStore(filepath.Join(t.TempDir(), "wal.log"))
	if err != nil {
		t.Fatalf("open wal: %v", err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}

func newFollower(t *testing.T) *Server {
	t.Helper()
	s := NewWithStore(time.Minute, newWal(t))
	if err := s.RequireDurableStore("follower"); err != nil {
		t.Fatalf("%v", err)
	}
	s.role.Store(int32(RoleFollower))
	return s
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timeout waiting for %s", what)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func TestLeaderFollowerReplication(t *testing.T) {
	leader := NewWithStore(time.Minute, newWal(t))
	leaderAddr := startAll(t, leader)
	leaderClient := dial(t, leaderAddr)

	follower := newFollower(t)
	followerAddr := startAll(t, follower)
	followerClient := dial(t, followerAddr)

	ctx, cancel := context.WithCancel(context.Background())
	go RunFollower(ctx, leaderAddr, follower)

	ev := func(id string, off int64) *mqpb.Event {
		return &mqpb.Event{EventId: id, CsvLineOffset: off, Tags: map[string]string{"pod_name": "pod-0"}}
	}
	stream, err := leaderClient.PublishEvents(ctx)
	if err != nil {
		t.Fatalf("publish stream: %v", err)
	}
	for _, e := range []*mqpb.Event{ev("e_1", 10), ev("e_2", 11), ev("e_3", 12)} {
		if err := stream.Send(e); err != nil {
			t.Fatalf("send: %v", err)
		}
		if ack, err := stream.Recv(); err != nil || !ack.Success {
			t.Fatalf("ack err=%v ack=%+v", err, ack)
		}
	}
	stream.CloseSend()

	// The follower converges to the same tail (events + offsets must align).
	waitFor(t, "follower tail == leader tail", func() bool {
		return follower.store.Tail() == leader.store.Tail() && follower.store.Tail() == 3
	})

	// Retention requires an active collector (trimLocked only acts when one has
	// joined). A leader commit to a cursor of 1 trims offsets 0,1 away; the trim
	// frame must propagate so both logs share the base 2.
	if _, err := leaderClient.JoinPartition(ctx, &mqpb.JoinPartitionRequest{Group: collectorGroup, ConsumerId: "collector-1", TtlSeconds: 300}); err != nil {
		t.Fatalf("join collector: %v", err)
	}
	if _, err := leaderClient.CommitOffset(ctx, &mqpb.CommitOffsetRequest{
		Group: collectorGroup, ConsumerId: "collector-1", Offset: 1,
	}); err != nil {
		t.Fatalf("commit: %v", err)
	}
	waitFor(t, "follower collector cursor", func() bool {
		off, err := followerClient.GetOffset(ctx, &mqpb.GetOffsetRequest{Group: collectorGroup, ConsumerId: "collector-1"})
		return err == nil && off.Exists && off.Offset == 1
	})
	waitFor(t, "follower base == leader base (trim propagated)", func() bool {
		ls, ok := leader.store.(DurableStore)
		fs, ok2 := follower.store.(DurableStore)
		return ok && ok2 && fs.Base() == ls.Base() && ls.Base() == 2
	})

	// Streamer cursors replicate too (their source is the appended events).
	waitFor(t, "follower streamer cursor", func() bool {
		off, err := followerClient.GetOffset(ctx, &mqpb.GetOffsetRequest{Group: streamerGroup, ConsumerId: "pod-0"})
		return err == nil && off.Exists && off.Offset == 12
	})

	// Failover: stop the follower's replication, then promote the same store as
	// a fresh leader (its WAL has everything the leader acked). The promoted
	// node must serve the surviving event and every cursor without a restart.
	cancel()
	waitFor(t, "follower replication stopped", func() bool {
		// Wait for the follower store to be quiet before promoting it.
		m := follower.store.Tail()
		time.Sleep(50 * time.Millisecond)
		return follower.store.Tail() == m
	})
	promoted := NewWithStore(time.Minute, follower.store)
	promotedClient := dial(t, startAll(t, promoted))
	pctx := context.Background()
	off, err := promotedClient.GetOffset(pctx, &mqpb.GetOffsetRequest{Group: streamerGroup, ConsumerId: "pod-0"})
	if err != nil || !off.Exists || off.Offset != 12 {
		t.Fatalf("promoted GetOffset = %+v err=%v, want exists offset 12", off, err)
	}
	off, err = promotedClient.GetOffset(pctx, &mqpb.GetOffsetRequest{Group: collectorGroup, ConsumerId: "collector-1"})
	if err != nil || !off.Exists || off.Offset != 1 {
		t.Fatalf("promoted collector cursor = %+v err=%v, want 1", off, err)
	}
	got := collectAll(t, promotedClient, pctx, -1)
	if len(got) != 1 || got[0].Event.EventId != "e_3" || got[0].Offset != 2 {
		t.Fatalf("promoted log = %+v, want [e_3@2]", got)
	}
}

func TestFollowerRejectsWrites(t *testing.T) {
	follower := newFollower(t)
	client := dial(t, startAll(t, follower))
	ctx := context.Background()

	stream, err := client.PublishEvents(ctx)
	if err != nil {
		t.Fatalf("publish stream: %v", err)
	}
	if err := stream.Send(&mqpb.Event{EventId: "e_1", CsvLineOffset: 0, Tags: map[string]string{"pod_name": "pod-0"}}); err != nil {
		t.Fatalf("send: %v", err)
	}
	_, err = stream.Recv()
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("publish on follower: err=%v, want FailedPrecondition", err)
	}

	// Reads still work locally.
	if _, err := client.GetOffset(ctx, &mqpb.GetOffsetRequest{ConsumerId: "pod-0", Group: streamerGroup}); err != nil {
		t.Fatalf("follower GetOffset should serve reads: %v", err)
	}

	_, err = client.CommitOffset(ctx, &mqpb.CommitOffsetRequest{Group: collectorGroup, ConsumerId: "c", Offset: 1})
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("commit on follower: err=%v, want FailedPrecondition", err)
	}
}

func TestSyncReplicationGatesAck(t *testing.T) {
	// A leader configured for sync-replication with no followers connected (yet).
	leader := NewWithStore(time.Minute, newWal(t))
	leader.EnableSyncReplication(1)
	leaderAddr := startAll(t, leader)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Publish must NOT be acked until a follower is connected and applies it.
	ackCh := make(chan *mqpb.Ack, 1)
	errCh := make(chan error, 1)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		c := dial(t, leaderAddr)
		stream, err := c.PublishEvents(ctx)
		if err != nil {
			errCh <- err
			return
		}
		if err := stream.Send(&mqpb.Event{EventId: "sync_1", CsvLineOffset: 5, Tags: map[string]string{"pod_name": "pod-0"}}); err != nil {
			errCh <- err
			return
		}
		ack, err := stream.Recv()
		if err != nil {
			errCh <- err
			return
		}
		ackCh <- ack
	}()
	defer wg.Wait()

	select {
	case ack := <-ackCh:
		t.Fatalf("acked before any follower connected: %+v", ack)
	case err := <-errCh:
		t.Fatalf("publish failed before follower connected: %v", err)
	case <-time.After(300 * time.Millisecond):
		// Good: sync replication is withholding the ack.
	}

	// Connecting a follower releases the gate once it applies the event.
	go RunFollower(ctx, leaderAddr, newFollower(t))
	select {
	case ack := <-ackCh:
		if !ack.Success || ack.Offset != 5 {
			t.Fatalf("ack after follower = %+v, want success offset=5", ack)
		}
	case err := <-errCh:
		t.Fatalf("publish failed: %v", err)
	case <-time.After(10 * time.Second):
		t.Fatalf("ack never released after follower connected")
	}
}

func collectAll(t *testing.T, c mqpb.MessageQueueClient, ctx context.Context, start int64) []*mqpb.ConsumedEvent {
	t.Helper()
	stream, err := c.Consume(ctx, &mqpb.ConsumeRequest{StartOffset: start, ConsumerId: "probe", Group: "probe"})
	if err != nil {
		t.Fatalf("consume: %v", err)
	}
	var out []*mqpb.ConsumedEvent
	for {
		ce, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatalf("consume recv: %v", err)
		}
		out = append(out, ce)
	}
	return out
}

// TestFollowerConvergesAfterLeaderBaseMoved reproduces the production wedge:
// retention advanced the leader's base, the follower's checkpoint converged
// its base, and the two logs stopped agreeing on what offset "tail" means.
//
// Before the fix, ApplyCheckpoint moved base but kept the follower's own
// retained entries. Because entries are addressed as base+index, Tail() then
// reported an offset far beyond the leader's real tail, the follower asked to
// resume past everything the leader had, the leader had nothing to send and
// closed the stream, and with syncReplicates>=1 every publish blocked forever.
// This test asserts the follower converges instead.
func TestFollowerConvergesAfterLeaderBaseMoved(t *testing.T) {
	leader := NewWithStore(time.Minute, newWal(t))
	leaderAddr := startAll(t, leader)
	leaderClient := dial(t, leaderAddr)

	follower := newFollower(t)
	followerAddr := startAll(t, follower)
	followerClient := dial(t, followerAddr)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go RunFollower(ctx, leaderAddr, follower)

	ev := func(id string, off int64) *mqpb.Event {
		return &mqpb.Event{EventId: id, CsvLineOffset: off, Tags: map[string]string{"pod_name": "pod-0"}}
	}
	stream, err := leaderClient.PublishEvents(ctx)
	if err != nil {
		t.Fatalf("publish stream: %v", err)
	}
	for _, e := range []*mqpb.Event{ev("e_1", 10), ev("e_2", 11), ev("e_3", 12)} {
		if err := stream.Send(e); err != nil {
			t.Fatalf("send: %v", err)
		}
		if ack, err := stream.Recv(); err != nil || !ack.Success {
			t.Fatalf("ack err=%v ack=%+v", err, ack)
		}
	}
	stream.CloseSend()
	waitFor(t, "follower tail == leader tail", func() bool {
		return follower.store.Tail() == leader.store.Tail()
	})

	// Retention requires an active collector. Advancing the durable cursor
	// moves the leader's base, which is the condition that desynchronised the
	// two logs in production.
	if _, err := leaderClient.JoinPartition(ctx, &mqpb.JoinPartitionRequest{Group: collectorGroup, ConsumerId: "collector-1", TtlSeconds: 300}); err != nil {
		t.Fatalf("join collector: %v", err)
	}
	for off := int64(1); off <= 2; off++ {
		if _, err := leaderClient.CommitOffset(ctx, &mqpb.CommitOffsetRequest{
			Group: collectorGroup, ConsumerId: "collector-1", Offset: off,
		}); err != nil {
			t.Fatalf("commit %d: %v", off, err)
		}
	}
	waitFor(t, "leader base advanced", func() bool { return leader.store.Tail() >= 3 })

	// The follower must converge on the leader's base and tail. If it kept its
	// own entries, Tail() overshoots and this never settles.
	waitFor(t, "follower converged on leader base+tail", func() bool {
		return follower.store.Tail() == leader.store.Tail()
	})

	// And replication must still work afterwards: publish more and confirm the
	// follower receives it, which is exactly what stalled in production.
	stream2, err := leaderClient.PublishEvents(ctx)
	if err != nil {
		t.Fatalf("publish stream 2: %v", err)
	}
	for _, e := range []*mqpb.Event{ev("e_4", 13), ev("e_5", 14)} {
		if err := stream2.Send(e); err != nil {
			t.Fatalf("send after convergence: %v", err)
		}
		if ack, err := stream2.Recv(); err != nil || !ack.Success {
			t.Fatalf("ack after convergence err=%v ack=%+v", err, ack)
		}
	}
	stream2.CloseSend()
	tail := leader.store.Tail()
	waitFor(t, "follower tail after post-convergence publish", func() bool {
		return follower.store.Tail() == tail
	})

	// The collector cursor must survive the base move.
	off, err := followerClient.GetOffset(ctx, &mqpb.GetOffsetRequest{Group: collectorGroup, ConsumerId: "collector-1"})
	if err != nil || !off.Exists || off.Offset != 2 {
		t.Fatalf("follower collector cursor err=%v off=%+v, want offset 2", err, off)
	}
}

// TestLeaderReseedsFollowerResumePastTail pins the leader-side half of the
// contract: a follower that asks to resume at or past the leader's tail cannot
// be served. The leader must re-seed it from the retained head instead of
// closing the stream, otherwise the follower reconnects forever and every
// publish blocks.
func TestLeaderReseedsFollowerResumePastTail(t *testing.T) {
	leader := NewWithStore(time.Minute, newWal(t))
	leaderAddr := startAll(t, leader)

	for _, e := range []*mqpb.Event{
		{EventId: "e_1", CsvLineOffset: 1, Tags: map[string]string{"pod_name": "pod-0"}},
		{EventId: "e_2", CsvLineOffset: 2, Tags: map[string]string{"pod_name": "pod-0"}},
	} {
		if _, err := leader.store.Append(e); err != nil {
			t.Fatalf("append: %v", err)
		}
	}
	tail := leader.store.Tail()

	// Dial the replication service directly and claim to be far ahead.
	conn, err := grpc.NewClient(leaderAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()
	client := mqpb.NewReplicationClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	stream, err := client.Sync(ctx)
	if err != nil {
		t.Fatalf("sync: %v", err)
	}
	// resume far beyond the tail: a stale/divergent follower incarnation.
	if err := stream.Send(&mqpb.ReplicaAck{ResumeLogOffset: tail + 10_000}); err != nil {
		t.Fatalf("hello: %v", err)
	}

	// The leader must answer with a checkpoint rather than hanging up.
	fr, err := stream.Recv()
	if err != nil {
		t.Fatalf("expected checkpoint instead of a closed stream, got: %v", err)
	}
	if fr.Type != "x" {
		t.Fatalf("first frame type = %q, want checkpoint", fr.Type)
	}

	// Then it must re-seed the retained events instead of sending nothing.
	got := map[string]bool{}
	for {
		fr, err = stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			break
		}
		if fr.Type == "a" && fr.Event != nil {
			got[fr.Event.EventId] = true
		}
	}
	if !got["e_1"] || !got["e_2"] {
		t.Fatalf("leader did not re-seed retained events, got %v", got)
	}
}
