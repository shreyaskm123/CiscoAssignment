package app

import (
	"context"
	"errors"
	"testing"
	"time"

	"collector/internal/config"
	"collector/internal/partition"
	"collector/internal/sink"

	mqpb "streamer/proto"
)

// The readiness probe is only useful if it goes NotReady for the right reasons.
// These tests pin the rules Status() applies, because a probe that is always
// green is what this work replaced: a `command: ["/bin/true"]` probe reported
// 1/1 Running while the pipeline was ingesting at a fraction of its rate for
// hours.

func probeConfig(t *testing.T) *config.Config {
	t.Helper()
	return &config.Config{
		ConsumerID:    "collector-probe",
		BatchSize:     5,
		FlushInterval: 50 * time.Millisecond,
		InsertRetries: 3,
		InsertBackoff: 5 * time.Millisecond,
		RegistryTTL:   time.Minute,
		RegistryPoll:  25 * time.Millisecond,
		ConsumePoll:   20 * time.Millisecond,
		ShutdownGrace: time.Second,
		DeadLetterDir: t.TempDir(),
		ReadyMaxStall: 200 * time.Millisecond,
	}
}

// A fresh collector has no shard assignment, so it must not claim to be ready:
// until the registry watcher reports a stable (index, total) it does not know
// which offsets are its responsibility.
func TestStatusNotReadyWithoutShardAssignment(t *testing.T) {
	c := New(probeConfig(t), newFakeMQ(), &fakeSink{})
	got := c.Status()
	if got.Ready {
		t.Fatal("a collector with no shard assignment must not be ready")
	}
	if got.Reason == "" {
		t.Error("a NotReady result must say why")
	}
	if got.Fields["shard"] != "0/0" {
		t.Errorf("shard = %q, want 0/0", got.Fields["shard"])
	}
}

// Once assigned, with no faults, it is ready.
func TestStatusReadyWhenAssignedAndHealthy(t *testing.T) {
	c := New(probeConfig(t), newFakeMQ(), &fakeSink{})
	c.shard.Store(stateOf(0, 1))
	c.lastProgress.Store(time.Now().UnixNano())
	got := c.Status()
	if !got.Ready {
		t.Fatalf("want ready, got NotReady: %s", got.Reason)
	}
	for _, k := range []string{"consumer", "shard", "committed_offset", "cursor_lag", "events_written_total", "dead_letter_rows_total"} {
		if _, ok := got.Fields[k]; !ok {
			t.Errorf("readiness output is missing the %q field", k)
		}
	}
}

// The stall case, and the reason this work exists: the collector is blocked in
// Recv with the MQ not acking, so nothing errors and no counter moves. Only a
// clock notices.
func TestStatusNotReadyWhenStalled(t *testing.T) {
	c := New(probeConfig(t), newFakeMQ(), &fakeSink{})
	c.shard.Store(stateOf(0, 1))
	c.lastProgress.Store(time.Now().Add(-time.Second).UnixNano()) // as if 1s ago
	got := c.Status()
	if got.Ready {
		t.Fatal("a collector that has made no progress must not be ready")
	}
	if got.Fields["last_progress_age_seconds"] == "" {
		t.Error("the stall reason should be quantified in the fields")
	}
}

// Progress within the window keeps it ready, so a busy-but-healthy pod never
// flaps.
func TestStatusStaysReadyWhileProgressing(t *testing.T) {
	c := New(probeConfig(t), newFakeMQ(), &fakeSink{})
	c.shard.Store(stateOf(0, 1))
	for i := 0; i < 3; i++ {
		c.lastProgress.Store(time.Now().UnixNano())
		if got := c.Status(); !got.Ready {
			t.Fatalf("iteration %d: want ready, got NotReady: %s", i, got.Reason)
		}
		time.Sleep(40 * time.Millisecond)
	}
}

// A failing write is a fault that clears itself when ClickHouse recovers, so it
// gates readiness while it lasts and not afterwards.
func TestStatusNotReadyWhileWritesFail(t *testing.T) {
	cfg := probeConfig(t)
	c := New(cfg, newFakeMQ(), &errSink{err: errors.New("clickhouse down")})
	c.shard.Store(stateOf(0, 1))
	c.lastProgress.Store(time.Now().UnixNano())
	c.written.Store(0)
	c.writeFailures.Store(1)
	c.writeFailSince.Store(time.Now().Add(-2 * time.Second).UnixNano())
	c.deadLettered.Store(10)

	got := c.Status()
	if got.Ready {
		t.Fatal("a collector that cannot store what it reads must not be ready")
	}
	if got.Fields["write_failures_total"] != "1" {
		t.Errorf("write_failures_total = %q, want 1", got.Fields["write_failures_total"])
	}
	if got.Fields["dead_letter_rows_total"] != "10" {
		t.Errorf("dead_letter_rows_total = %q, want 10", got.Fields["dead_letter_rows_total"])
	}

	// Recovery: one good write clears the streak and readiness returns.
	c.writeFailSince.Store(0)
	c.lastProgress.Store(time.Now().UnixNano())
	if got := c.Status(); !got.Ready {
		t.Errorf("want ready after writes recovered, got NotReady: %s", got.Reason)
	}
}

// Dead-lettered events are reported but must NOT gate readiness: the count only
// grows, so gating on it would hold a pod NotReady forever after ClickHouse
// recovered and would hang the next rolling update.
func TestDeadLetterCountDoesNotGateReadiness(t *testing.T) {
	c := New(probeConfig(t), newFakeMQ(), &fakeSink{})
	c.shard.Store(stateOf(0, 1))
	c.lastProgress.Store(time.Now().UnixNano())
	c.deadLettered.Store(1_000_000)
	if got := c.Status(); !got.Ready {
		t.Errorf("a large historical dead-letter count must not gate readiness, got NotReady: %s", got.Reason)
	}
}

// With the stall check disabled, an idle collector is simply ready: some
// deployments pause their publishers on purpose.
func TestStallCheckCanBeDisabled(t *testing.T) {
	cfg := probeConfig(t)
	cfg.ReadyMaxStall = 0
	c := New(cfg, newFakeMQ(), &fakeSink{})
	c.shard.Store(stateOf(0, 1))
	c.lastProgress.Store(time.Now().Add(-time.Hour).UnixNano())
	if got := c.Status(); !got.Ready {
		t.Errorf("with READY_MAX_STALL_SECONDS=0 an idle collector should be ready, got: %s", got.Reason)
	}
}

// Counters advance on the real paths, so the numbers on /readyz mean something.
func TestCountersAdvanceOnConsumeAndWrite(t *testing.T) {
	mq := newFakeMQ()
	for i := int64(0); i < 6; i++ {
		mq.seed(&mqpb.Event{EventId: "evt_" + string(rune('a'+i)), CsvLineOffset: i})
	}
	cfg := probeConfig(t)
	cfg.BatchSize = 3
	c := New(cfg, mq, &fakeSink{})

	ctx, cancel := contextWithTimeout(t, 3*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- c.Run(ctx) }()
	waitFor(t, func() bool { return mq.cursor("collector-probe") == 5 }, 3*time.Second)
	cancel()
	<-done

	if c.written.Load() == 0 {
		t.Error("events_written_total did not advance after a successful write")
	}
	if c.lastProgress.Load() == 0 {
		t.Error("lastProgress was never set")
	}
	if c.deadLettered.Load() != 0 {
		t.Errorf("dead_lettered = %d, want 0 when nothing failed", c.deadLettered.Load())
	}
}

func TestDeadLetterCounterAdvances(t *testing.T) {
	cfg := probeConfig(t)
	c := New(cfg, newFakeMQ(), &errSink{err: errors.New("down")})
	batch := []sink.Event{{Event: &mqpb.Event{EventId: "evt_1"}, Offset: 1}, {Event: &mqpb.Event{EventId: "evt_2"}, Offset: 2}}
	if err := c.deadLetter(batch, errors.New("down")); err != nil {
		t.Fatalf("deadLetter: %v", err)
	}
	if got := c.deadLettered.Load(); got != 2 {
		t.Errorf("dead_lettered = %d, want 2", got)
	}
	if c.deadLettered.Load() != 2 {
		t.Error("the dead-letter counter must only count events actually written")
	}
}

// stateOf is partition.State without making the test import it at every use.
func stateOf(index, total int) partition.State { return partition.State{Index: index, Total: total} }

// contextWithTimeout keeps the test bodies short.
func contextWithTimeout(t *testing.T, d time.Duration) (ctx context.Context, cancel context.CancelFunc) {
	t.Helper()
	return context.WithTimeout(context.Background(), d)
}

// errSink fails every write, standing in for a ClickHouse that is refusing
// inserts (which is what the dead-letter path exists for).
type errSink struct{ err error }

func (s *errSink) Write(_ context.Context, _ []sink.Event) error { return s.err }
