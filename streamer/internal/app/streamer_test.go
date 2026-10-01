package app

import (
	"context"
	"errors"
	"io"
	"net"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"

	"streamer/internal/config"
	"streamer/internal/csvsrc"
	"streamer/internal/mqclient"
	mqpb "streamer/proto"
)

// fakeMQ is an in-memory MessageQueue server used to test the streamer
// end-to-end: it stores events, acks each one with the event's offset, and
// supports GetOffset + the partition registry (JoinPartition/LeavePartition).
type fakeMQ struct {
	mqpb.UnimplementedMessageQueueServer

	mu        sync.Mutex
	consumers map[string]int64
	events    []*mqpb.Event

	reMu    sync.Mutex
	replica map[string]struct{} // live registry consumers
}

func newFakeMQ() *fakeMQ {
	return &fakeMQ{
		consumers: map[string]int64{},
		replica:   map[string]struct{}{},
	}
}

func (f *fakeMQ) GetOffset(_ context.Context, req *mqpb.GetOffsetRequest) (*mqpb.GetOffsetResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if off, ok := f.consumers[req.ConsumerId]; ok {
		return &mqpb.GetOffsetResponse{Offset: off, Exists: true}, nil
	}
	return &mqpb.GetOffsetResponse{Exists: false}, nil
}

// JoinPartition mimics the real MQ registry: index = rank in the sorted set.
func (f *fakeMQ) JoinPartition(_ context.Context, req *mqpb.JoinPartitionRequest) (*mqpb.JoinPartitionResponse, error) {
	f.reMu.Lock()
	defer f.reMu.Unlock()
	f.replica[req.ConsumerId] = struct{}{}
	ids := make([]string, 0, len(f.replica))
	for id := range f.replica {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	idx := 0
	for i, id := range ids {
		if id == req.ConsumerId {
			idx = i
			break
		}
	}
	return &mqpb.JoinPartitionResponse{Index: int32(idx), Total: int32(len(ids))}, nil
}

func (f *fakeMQ) LeavePartition(_ context.Context, req *mqpb.LeavePartitionRequest) (*mqpb.LeavePartitionResponse, error) {
	f.reMu.Lock()
	defer f.reMu.Unlock()
	delete(f.replica, req.ConsumerId)
	return &mqpb.LeavePartitionResponse{}, nil
}

func (f *fakeMQ) joinConsumer(id string) {
	f.reMu.Lock()
	defer f.reMu.Unlock()
	f.replica[id] = struct{}{}
}

func (f *fakeMQ) activeCount() int {
	f.reMu.Lock()
	defer f.reMu.Unlock()
	return len(f.replica)
}

func (f *fakeMQ) PublishEvents(stream grpc.BidiStreamingServer[mqpb.Event, mqpb.Ack]) error {
	for {
		e, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return err
		}
		f.mu.Lock()
		f.events = append(f.events, e)
		f.mu.Unlock()
		if err := stream.Send(&mqpb.Ack{EventId: e.EventId, Offset: e.CsvLineOffset, Success: true}); err != nil {
			return err
		}
	}
}

func (f *fakeMQ) received() []*mqpb.Event {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]*mqpb.Event, len(f.events))
	copy(out, f.events)
	return out
}

const testCSV = "../csvsrc/testdata/metrics.csv"

func startFakeMQ(t *testing.T, mq *fakeMQ) string {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	srv := grpc.NewServer()
	mqpb.RegisterMessageQueueServer(srv, mq)
	go srv.Serve(lis)
	t.Cleanup(srv.Stop)
	return lis.Addr().String()
}

func testConfig(addr string) *config.Config {
	return &config.Config{
		PodName:                "telemetry-streamer-test",
		Cluster:                "test-cluster",
		CSVPath:                filepath.Clean(testCSV),
		MQAddr:                 addr,
		ConsumerID:             "test-consumer",
		ReloadCheckInterval:    60,
		BatchSize:              1,
		FlushIntervalMs:        1,
		RowDelayMs:             10,
		RegistryTTLSec:         60,
		RegistryPollIntervalMs: 50,
	}
}

func TestStreamerPublishesOwnedRows(t *testing.T) {
	mq := newFakeMQ()
	addr := startFakeMQ(t, mq)
	cfg := testConfig(addr)

	reader, err := csvsrc.Open(cfg.CSVPath)
	if err != nil {
		t.Fatalf("open csv: %v", err)
	}
	client, err := mqclient.Dial(context.Background(), addr)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer client.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 600*time.Millisecond)
	defer cancel()

	s, err := New(ctx, cfg, reader, client)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	_ = s.Run(ctx) // returns ctx.Err() on timeout

	events := mq.received()
	if len(events) < 3 {
		t.Fatalf("received %d events, want at least 3 (row 3 has non-numeric value and is skipped)", len(events))
	}

	// Row 0 mapping: id format, timestamp, metric fields, tags, value.
	e0 := events[0]
	if e0.EventId != "evt_telemetry-streamer-test_L0_O0" {
		t.Errorf("event_id = %s", e0.EventId)
	}
	if e0.MetricName != "DCGM_FI_DEV_GPU_UTIL" {
		t.Errorf("metric_name = %s", e0.MetricName)
	}
	if e0.GpuIndex != 0 || e0.DeviceName != "nvidia0" || e0.DeviceId == "" || e0.ModelName == "" {
		t.Errorf("gpu fields mismatched: %+v", e0)
	}
	if e0.Hostname != "mtv5-dgx1" {
		t.Errorf("hostname = %s", e0.Hostname)
	}
	if e0.Value != 85.5 {
		t.Errorf("value = %v, want 85.5", e0.Value)
	}
	if e0.CsvLineOffset != 0 || e0.LoopCount != 0 {
		t.Errorf("offset/loop = %d/%d, want 0/0", e0.CsvLineOffset, e0.LoopCount)
	}
	if e0.Tags["pod_name"] != "telemetry-streamer-test" || e0.Tags["cluster"] != "test-cluster" {
		t.Errorf("tags = %v", e0.Tags)
	}
	if e0.SourceTimestamp == "" {
		t.Error("source_timestamp empty")
	}
	if e0.SourceTimestamp == "2025-07-18T20:42:34Z" {
		t.Errorf("the CSV timestamp leaked into the event: source_ts=%q", e0.SourceTimestamp)
	}

	// Sequential offsets, monotonically increasing within the loop.
	for i := 1; i < 3; i++ {
		if events[i].CsvLineOffset != int64(i) {
			t.Errorf("event %d offset = %d, want %d", i, events[i].CsvLineOffset, i)
		}
	}

	// None of the received events may be the malformed row (offset 3).
	for _, e := range events {
		if e.CsvLineOffset == 3 {
			t.Errorf("malformed row (offset 3) was published: %+v", e)
		}
	}
}

func TestStreamerResumesFromGetOffset(t *testing.T) {
	mq := newFakeMQ()
	mq.consumers["test-consumer"] = 1 // simulate a prior ack at offset 1
	addr := startFakeMQ(t, mq)
	cfg := testConfig(addr)

	reader, err := csvsrc.Open(cfg.CSVPath)
	if err != nil {
		t.Fatalf("open csv: %v", err)
	}
	client, err := mqclient.Dial(context.Background(), addr)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer client.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 600*time.Millisecond)
	defer cancel()

	s, err := New(ctx, cfg, reader, client)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if s.offs.CommittedPosition() != 1 {
		t.Errorf("recovered committed = %d, want 1", s.offs.CommittedPosition())
	}
	_ = s.Run(ctx)

	events := mq.received()
	if len(events) == 0 {
		t.Fatal("no events received after recovery")
	}
	// First event after recovery must be offset 1, not 0.
	if events[0].CsvLineOffset != 1 {
		t.Errorf("first recovered event offset = %d, want 1 (resume from keep the read pointer)", events[0].CsvLineOffset)
	}
	if events[0].EventId != "evt_telemetry-streamer-test_L0_O1" {
		t.Errorf("first recovered event_id = %s", events[0].EventId)
	}
}

func TestPartitionedPublishingProducesNoDuplicates(t *testing.T) {
	mq := newFakeMQ()
	addr := startFakeMQ(t, mq)

	// Pre-register all three consumers so every replica's JoinPartition sees
	// the full fleet from the start (rank = stable index out of 3), matching
	// the real registry semantics. Consumer names sort lexicographically:
	// consumer-0 → 0/3, consumer-1 → 1/3, consumer-2 → 2/3.
	// Replicas stream concurrently so the registry stays at 3 for the whole
	// run (a replica deregisters only when its own run exits).
	const replicas = 3
	for idx := 0; idx < replicas; idx++ {
		mq.joinConsumer("consumer-" + string(rune('0'+idx)))
	}

	var wg sync.WaitGroup
	for idx := 0; idx < replicas; idx++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			cfg := testConfig(addr)
			cfg.ConsumerID = "consumer-" + string(rune('0'+idx))

			reader, err := csvsrc.Open(cfg.CSVPath)
			if err != nil {
				t.Errorf("open csv: %v", err)
				return
			}
			client, err := mqclient.Dial(context.Background(), addr)
			if err != nil {
				t.Errorf("dial: %v", err)
				return
			}
			defer client.Close()

			ctx, cancel := context.WithTimeout(context.Background(), 400*time.Millisecond)
			s, err := New(ctx, cfg, reader, client)
			if err != nil {
				t.Errorf("New pod %d: %v", idx, err)
				cancel()
				return
			}
			_ = s.Run(ctx)
			cancel()
		}(idx)
	}
	wg.Wait()

	// Every published event must have a unique event_id — no duplicate rows
	// processed across the three replica partitions.
	ids := map[string]bool{}
	for _, e := range mq.received() {
		if ids[e.EventId] {
			t.Errorf("duplicate event_id published: %s", e.EventId)
		}
		ids[e.EventId] = true
	}
}

// TestStreamerRegistryDrivenRepartition verifies Option A end-to-end:
//  1. the streamer learns its (index, total) from the MQ partition registry at
//     startup (not from env),
//  2. when a second consumer joins (scale-up) the watcher heartbeat detects it
//     and the scheduler repartitions to the new (index, total),
//  3. after that repartition it only publishes rows owned by its new index,
//  4. on graceful shutdown it deregisters and the registry shrinks back.
func TestStreamerRegistryDrivenRepartition(t *testing.T) {
	mq := newFakeMQ()
	addr := startFakeMQ(t, mq)

	cfg := testConfig(addr)
	cfg.RegistryTTLSec = 60
	cfg.RegistryPollIntervalMs = 50
	cfg.ConsumerID = "zzz-consumer" // sorts after "aaa-consumer"

	reader, err := csvsrc.Open(cfg.CSVPath)
	if err != nil {
		t.Fatalf("open csv: %v", err)
	}
	client, err := mqclient.Dial(context.Background(), addr)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer client.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	s, err := New(ctx, cfg, reader, client)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if idx, total := s.Partition(); idx != 0 || total != 1 {
		t.Fatalf("initial partition = %d/%d, want 0/1 (only consumer)", idx, total)
	}

	done := make(chan error, 1)
	go func() { done <- s.Run(ctx) }()

	waitFor(t, 2*time.Second, "streamer produces on initial layout", func() bool {
		return s.Produced() > 0
	})

	// Scale up: another consumer joins → "aaa-consumer" < "zzz-consumer",
	// so this streamer must move to index 1 of 2.
	mq.joinConsumer("aaa-consumer")
	waitFor(t, 2*time.Second, "repartition to 1/2", func() bool {
		idx, total := s.Partition()
		return idx == 1 && total == 2
	})

	// After repartition it only owns odd offsets in the file (index 1 % 2).
	// Look at events that arrive *after* the repartition (events before it were
	// produced under the old 0/1 layout where every offset was owned).
	baseline := len(mq.received())
	waitFor(t, 2*time.Second, "only odd-offset events published after repartition", func() bool {
		if len(mq.received()) <= baseline {
			return false // wait for at least one post-repartition event
		}
		for _, e := range mq.received()[baseline:] {
			if e.CsvLineOffset%2 == 0 {
				return false
			}
		}
		return true
	})

	// Graceful shutdown: drain completes and the streamer deregisters.
	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("streamer did not stop after cancel")
	}
	waitFor(t, time.Second, "registry shrinks after leave", func() bool {
		return mq.activeCount() == 1 // "aaa-consumer" still registered
	})
}

func waitFor(t *testing.T, timeout time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for: %s", what)
}

// TestMapEventStampsCurrentTimeOnly verifies the CSV's timestamp column is
// never propagated: the event's only timestamp is the streamer's current UTC
// time.
func TestMapEventStampsCurrentTimeOnly(t *testing.T) {
	cfg := testConfig("127.0.0.1:1") // MQ addr unused for the mapping
	row := csvsrc.Row{
		Raw:     []string{"2025-07-18T20:42:34.000000000Z", "DCGM_FI_DEV_GPU_UTIL", "0", "nvidia0", "uuid-1", "m", "h", "c", "p", "ns", "85.5", ""},
		LineNum: 7,
		Loop:    2,
	}
	before := time.Now().UTC().Add(-time.Second)
	ev := mapEvent(cfg, row)
	after := time.Now().UTC().Add(time.Second)

	got, err := time.Parse(time.RFC3339Nano, ev.SourceTimestamp)
	if err != nil {
		t.Fatalf("parse source_timestamp %q: %v", ev.SourceTimestamp, err)
	}
	if got.Before(before) || got.After(after) {
		t.Errorf("source_timestamp = %v, want the current time (between %v and %v)", got, before, after)
	}
	if strings.Contains(ev.SourceTimestamp, "2025-07-18") {
		t.Errorf("CSV timestamp leaked: source_ts=%q", ev.SourceTimestamp)
	}
	if ev.EventId != "evt_telemetry-streamer-test_L2_O7" {
		t.Errorf("event_id = %s, want deterministic id evt_telemetry-streamer-test_L2_O7", ev.EventId)
	}
}

// TestMapEventIgnoresMalformedCSVTimestamp: the CSV column is never parsed, so
// a garbage value there can neither fail the row nor reach the event.
func TestMapEventIgnoresMalformedCSVTimestamp(t *testing.T) {
	cfg := testConfig("127.0.0.1:1")
	row := csvsrc.Row{
		Raw:     []string{"not-a-time", "DCGM_FI_DEV_GPU_UTIL", "1", "nvidia1", "uuid-2", "m", "h", "c", "p", "ns", "1.0", ""},
		LineNum: 1,
	}
	ev := mapEvent(cfg, row)
	if _, err := time.Parse(time.RFC3339Nano, ev.SourceTimestamp); err != nil {
		t.Errorf("source_timestamp %q is not RFC3339: %v", ev.SourceTimestamp, err)
	}
}
