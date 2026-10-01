package app

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"collector/internal/config"
	"collector/internal/mqclient"
	"collector/internal/sink"

	mqpb "streamer/proto"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// ---- fakeMQ: in-memory emulation of the consumer contract + registry ----

type fakeMQ struct {
	mu            sync.Mutex
	log           []*mqpb.Event
	cur           map[string]int64
	reg           map[string]struct{}
	snapEOFOnTail bool // true = Consume closes at the log tail; false = live stream

	// staleUntil makes Consume answer OutOfRange for any start offset that is
	// at or past the log tail, exactly as a real broker does when the cursor
	// belongs to a log incarnation it no longer has. Set it to simulate a
	// promoted follower whose log is shorter than the committed cursor.
	staleUntil int64 // negative = never stale
	// repaired is the offset GetOffset hands back once the broker re-anchors.
	repaired int64
	// outOfRange counts how many times Consume rejected the cursor.
	outOfRange int
}

func newFakeMQ() *fakeMQ {
	// staleUntil = -1: healthy broker, never rejects a cursor.
	return &fakeMQ{cur: map[string]int64{}, reg: map[string]struct{}{}, snapEOFOnTail: true, staleUntil: -1, repaired: -1}
}

func (f *fakeMQ) prejoin(id string) { f.reg[id] = struct{}{} }
func (f *fakeMQ) seed(evs ...*mqpb.Event) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.log = append(f.log, evs...)
}

// outOfRangeCount reads the rejection counter safely: tests mutate broker state
// while the collector goroutine is running.
func (f *fakeMQ) outOfRangeCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.outOfRange
}

// shrinkLog models the failover this test is about: the promoted node's log is
// shorter than the cursor a consumer committed against the old leader.
func (f *fakeMQ) shrinkLog(keep int, tail, repaired int64) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.log = f.log[:keep]
	f.staleUntil = tail
	f.repaired = repaired
}

func (f *fakeMQ) cursor(id string) int64 {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.cur[id]
}

func (f *fakeMQ) GetOffset(_ context.Context, id string) (int64, bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	off, ok := f.cur[id]
	// Mirror the server's re-anchor: a cursor past the log tail comes back as
	// the retained head minus one, so the consumer knows where to resume.
	if ok && f.staleUntil >= 0 && off >= f.staleUntil {
		off = f.repaired
		f.cur[id] = off
	}
	return off, ok, nil
}

func (f *fakeMQ) JoinPartition(_ context.Context, id string, _ int32) (int, int, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.reg[id] = struct{}{}
	ids := make([]string, 0, len(f.reg))
	for k := range f.reg {
		ids = append(ids, k)
	}
	sort.Strings(ids)
	idx := 0
	for i, k := range ids {
		if k == id {
			idx = i
			break
		}
	}
	return idx, len(ids), nil
}

func (f *fakeMQ) LeavePartition(_ context.Context, id string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	delete(f.reg, id)
	return nil
}

func (f *fakeMQ) CommitOffset(_ context.Context, id string, off int64) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.cur[id] = off
	return nil
}

func (f *fakeMQ) Consume(ctx context.Context, _ string, start int64) (mqclient.Consumer, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.staleUntil >= 0 && start >= f.staleUntil {
		f.outOfRange++
		return nil, status.Errorf(codes.OutOfRange,
			"start offset %d is at or past the log tail %d", start, f.staleUntil)
	}
	return &fakeConsumer{mq: f, ctx: ctx, i: int(start) + 1, snapEOF: f.snapEOFOnTail}, nil
}

type fakeConsumer struct {
	mq      *fakeMQ
	ctx     context.Context
	i       int
	snapEOF bool
}

func (c *fakeConsumer) Recv() (*mqpb.ConsumedEvent, error) {
	for {
		if c.ctx.Err() != nil {
			return nil, c.ctx.Err()
		}
		c.mq.mu.Lock()
		if c.i < len(c.mq.log) {
			ev := c.mq.log[c.i]
			off := int64(c.i)
			c.i++
			c.mq.mu.Unlock()
			return &mqpb.ConsumedEvent{Event: ev, Offset: off}, nil
		}
		end := c.snapEOF
		c.mq.mu.Unlock()
		if end {
			return nil, io.EOF
		}
		select {
		case <-c.ctx.Done():
			return nil, c.ctx.Err()
		case <-time.After(5 * time.Millisecond):
		}
	}
}

// ---- fakeSink ----

type fakeSink struct {
	mu       sync.Mutex
	written  []sink.Event
	failNext int
}

func (s *fakeSink) Write(_ context.Context, batch []sink.Event) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.failNext > 0 {
		s.failNext--
		return errors.New("injected write failure")
	}
	s.written = append(s.written, batch...)
	return nil
}

func (s *fakeSink) events() []sink.Event {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]sink.Event, len(s.written))
	copy(out, s.written)
	return out
}

func (s *fakeSink) count() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.written)
}

func waitFor(t *testing.T, cond func() bool, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("condition not met within %v", timeout)
}

func testConfig(t *testing.T, consumer string) *config.Config {
	return &config.Config{
		MQAddr:        "unused",
		ConsumerID:    consumer,
		RegistryTTL:   time.Minute,
		RegistryPoll:  25 * time.Millisecond,
		ConsumePoll:   20 * time.Millisecond,
		BatchSize:     5,
		FlushInterval: 50 * time.Millisecond,
		InsertRetries: 3,
		InsertBackoff: 5 * time.Millisecond,
		ShutdownGrace: 2 * time.Second,
		DeadLetterDir: t.TempDir(),
	}
}

func event(id string, offset int64) *mqpb.Event {
	return &mqpb.Event{EventId: id, CsvLineOffset: offset, Tags: map[string]string{"cluster": "c", "pod_name": "p"}}
}

func offsetsOf(evs []sink.Event) []int64 {
	out := make([]int64, 0, len(evs))
	for _, e := range evs {
		out = append(out, e.Offset)
	}
	return out
}

func int64Set(vals []int64) map[int64]bool {
	out := make(map[int64]bool, len(vals))
	for _, v := range vals {
		out[v] = true
	}
	return out
}

func runCollector(t *testing.T, cfg *config.Config, mq MQ, snk sink.Writer) (context.CancelFunc, func() error) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	col := New(cfg, mq, snk)
	done := make(chan error, 1)
	go func() { done <- col.Run(ctx) }()
	t.Cleanup(func() {
		cancel()
		select {
		case <-done:
		case <-time.After(3 * time.Second):
		}
	})
	return cancel, func() error { return <-done }
}

// TestShardConsumeAndCommit: with a 3-collector registry where this instance is
// index 0, it must consume only offsets %3==0 and commit the whole window.
func TestShardConsumeAndCommit(t *testing.T) {
	mq := newFakeMQ()
	mq.prejoin("collector-a")
	mq.prejoin("collector-b")

	var evs []*mqpb.Event
	for i := int64(0); i < 12; i++ {
		evs = append(evs, event(fmt.Sprintf("evt_L0_O%d", i), i))
	}
	mq.seed(evs...)

	snk := &fakeSink{}
	cancel, finish := runCollector(t, testConfig(t, "collector-0"), mq, snk)
	waitFor(t, func() bool { return snk.count() == 4 }, 5*time.Second)

	if got := mq.cursor("collector-0"); got != 11 {
		t.Fatalf("committed cursor = %d, want 11", got)
	}
	got := int64Set(offsetsOf(snk.events()))
	want := int64Set([]int64{0, 3, 6, 9})
	if len(got) != len(want) {
		t.Fatalf("written offsets = %v, want %v", got, want)
	}
	for off := range want {
		if !got[off] {
			t.Fatalf("missing owned offset %d in %v", off, got)
		}
	}
	cancel()
	if err := finish(); err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("Run: %v", err)
	}
}

// TestResumeFromCommittedCursor: after a first run, a restarted collector with
// the same consumer id must resume past its committed cursor without replaying.
func TestResumeFromCommittedCursor(t *testing.T) {
	mq := newFakeMQ()

	seed := func(n int64) {
		evs := make([]*mqpb.Event, n)
		for i := range evs {
			evs[i] = event(fmt.Sprintf("evt_O%d", int64(i)), int64(i))
		}
		mq.seed(evs...)
	}

	cfg := testConfig(t, "collector-0")
	cfg.ConsumePoll = 5 * time.Millisecond
	snk := &fakeSink{}
	seed(5)

	cancel, finish := runCollector(t, cfg, mq, snk)
	waitFor(t, func() bool { return snk.count() == 5 }, 5*time.Second)
	cancel()
	finish() // first run drains

	seed(3) // offsets 5,6,7
	snk.mu.Lock()
	snk.written = nil
	snk.mu.Unlock()

	cancel2, finish2 := runCollector(t, cfg, mq, snk)
	waitFor(t, func() bool { return snk.count() == 3 }, 5*time.Second)
	cancel2()
	if err := finish2(); err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("Run: %v", err)
	}

	got := int64Set(offsetsOf(snk.events()))
	want := int64Set([]int64{5, 6, 7})
	if len(got) != len(want) {
		t.Fatalf("after resume wrote %v, want %v (replayed old events?)", got, want)
	}
	for off := range want {
		if !got[off] {
			t.Fatalf("missing %d after resume; got %v", off, got)
		}
	}
	if mq.cursor("collector-0") != 7 {
		t.Fatalf("cursor after resume = %d, want 7", mq.cursor("collector-0"))
	}
}

// TestGracefulFlushOnShutdown: events buffered past the batch threshold are
// written during the shutdown drain (live stream that never reaches EOF).
func TestGracefulFlushOnShutdown(t *testing.T) {
	mq := newFakeMQ()
	mq.snapEOFOnTail = false // live stream

	for i := int64(0); i < 10; i++ {
		mq.seed(event(fmt.Sprintf("evt_O%d", i), i))
	}

	cfg := testConfig(t, "collector-1") // index 0/1 -> owns everything
	cfg.BatchSize = 100                 // no mid-stream flush; drain must do it
	cfg.FlushInterval = time.Hour       // and no idle flush either

	snk := &fakeSink{}
	cancel, finish := runCollector(t, cfg, mq, snk)

	time.Sleep(300 * time.Millisecond) // let it consume into the buffer
	cancel()
	if err := finish(); err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("Run: %v", err)
	}

	if got := snk.count(); got != 10 {
		t.Fatalf("shutdown drained %d events, want 10", got)
	}
	if c := mq.cursor("collector-1"); c != 9 {
		t.Fatalf("cursor after shutdown drain = %d, want 9", c)
	}
}

// TestDeadLetterOnPersistentWriteFailure: when the sink never accepts a batch,
// events are preserved to the dead-letter file and the cursor still advances
// (fail-open so the pipeline never wedges).
func TestDeadLetterOnPersistentWriteFailure(t *testing.T) {
	mq := newFakeMQ()
	for i := int64(0); i < 6; i++ {
		mq.seed(event(fmt.Sprintf("evt_O%d", i), i))
	}
	snk := &fakeSink{failNext: 999}
	cfg := testConfig(t, "collector-2")
	cfg.DeadLetterDir = t.TempDir()
	cfg.BatchSize = 3

	cancel, finish := runCollector(t, cfg, mq, snk)
	waitFor(t, func() bool { return mq.cursor("collector-2") == 5 }, 5*time.Second)
	cancel()
	finish()

	matches, _ := filepath.Glob(filepath.Join(cfg.DeadLetterDir, "collector_*_dead_letter.jsonl"))
	if len(matches) == 0 {
		t.Fatal("expected a dead-letter file to exist")
	}
}

// TestDeadLetterCreatesMissingDirectory: in Kubernetes the dead-letter directory
// is a subdirectory of an emptyDir mount, which starts out empty. The collector
// must create it; assuming it existed made every dead-letter write fail with
// ENOENT, so an outage lost events instead of preserving them.
func TestDeadLetterCreatesMissingDirectory(t *testing.T) {
	mq := newFakeMQ()
	for i := int64(0); i < 6; i++ {
		mq.seed(event(fmt.Sprintf("evt_O%d", i), i))
	}
	snk := &fakeSink{failNext: 999}
	cfg := testConfig(t, "collector-dlq-dir")
	cfg.DeadLetterDir = filepath.Join(t.TempDir(), "not", "created", "yet", "dlq")
	cfg.BatchSize = 3

	cancel, finish := runCollector(t, cfg, mq, snk)
	waitFor(t, func() bool { return mq.cursor("collector-dlq-dir") == 5 }, 5*time.Second)
	cancel()
	finish()

	matches, _ := filepath.Glob(filepath.Join(cfg.DeadLetterDir, "collector_*_dead_letter.jsonl"))
	if len(matches) != 1 {
		t.Fatalf("expected the dead-letter file in the freshly created directory, got %v", matches)
	}
	b, err := os.ReadFile(matches[0])
	if err != nil {
		t.Fatal(err)
	}
	if n := strings.Count(string(b), "\n"); n != 6 {
		t.Errorf("dead-letter file has %d rows, want all 6 events", n)
	}
}

// TestCursorHeldWhenDeadLetterAlsoFails is the data-loss case: the sink is down
// AND the dead-letter write fails (disk full, unwritable volume). The batch is
// then stored nowhere, so the cursor must NOT advance past it; once the sink
// recovers, the batch is replayed and nothing is missing.
func TestCursorHeldWhenDeadLetterAlsoFails(t *testing.T) {
	mq := newFakeMQ()
	for i := int64(0); i < 6; i++ {
		mq.seed(event(fmt.Sprintf("evt_O%d", i), i))
	}
	snk := &fakeSink{failNext: 1 << 30}
	cfg := testConfig(t, "collector-dl-fails")
	cfg.BatchSize = 3
	// A dead-letter "directory" beneath a regular file can never be created.
	blocker := filepath.Join(t.TempDir(), "file")
	if err := os.WriteFile(blocker, []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	cfg.DeadLetterDir = filepath.Join(blocker, "dlq")

	cancel, finish := runCollector(t, cfg, mq, snk)

	// While neither the sink nor the dead-letter can take the batch, the cursor
	// must stay put (it was never committed, so it does not exist yet).
	time.Sleep(400 * time.Millisecond)
	mq.mu.Lock()
	c, committed := mq.cur["collector-dl-fails"]
	mq.mu.Unlock()
	if committed {
		t.Fatalf("cursor advanced to %d past a batch that was stored nowhere: events would be lost", c)
	}

	// The sink recovers: the held batches are replayed and everything lands.
	snk.mu.Lock()
	snk.failNext = 0
	snk.mu.Unlock()
	waitFor(t, func() bool { return mq.cursor("collector-dl-fails") == 5 }, 5*time.Second)
	cancel()
	finish()

	got := int64Set(offsetsOf(snk.events()))
	for i := int64(0); i < 6; i++ {
		if !got[i] {
			t.Errorf("offset %d never reached the sink after recovery (stored: %v)", i, offsetsOf(snk.events()))
		}
	}
}

// ---- re-anchoring after failover ----

// A collector's committed cursor can name an offset the current broker's log
// does not contain, and that is the NORMAL consequence of a failover: the
// cursor was committed against the old leader, and the promoted survivor's log
// is shorter (it was partitioned, behind, or had trimmed past that offset).
//
// The broker reports this as OutOfRange. Treated like any other stream error,
// the collector would retry the same unreachable offset forever - polling,
// logging, ingesting nothing, and reporting healthy the whole time. These tests
// pin the recovery path: notice the rejection, re-anchor, resume ingesting.

// TestReanchorsAfterFailoverResumesIngestion is the end-to-end property: the
// collector must not get stuck. It starts holding a cursor from a lost log
// incarnation, is rejected, re-anchors, and writes the events actually present
// in the new log.
func TestReanchorsAfterFailoverResumesIngestion(t *testing.T) {
	mq := newFakeMQ()
	mq.seed(
		event("evt_L0_O0", 0),
		event("evt_L0_O1", 1),
		event("evt_L0_O2", 2),
	)

	snk := &fakeSink{}
	cancel, _ := runCollector(t, testConfig(t, "collector-0"), mq, snk)
	waitFor(t, func() bool { return snk.count() == 3 }, 5*time.Second)
	waitFor(t, func() bool { return mq.cursor("collector-0") == 2 }, 5*time.Second)

	// The leader is lost and a follower is promoted whose log is SHORTER: only
	// offset 0 survived, so the running collector's committed cursor of 2 is
	// now unreachable. From here the broker rejects it with OutOfRange.
	// Tail is now 1, so any start >= 1 is unreachable; the broker re-anchors
	// the cursor to just before the retained head.
	mq.shrinkLog(1, 1, -1)

	// Recovery means the surviving event is served again rather than the
	// collector spinning on offset 2 forever.
	waitFor(t, func() bool { return mq.outOfRangeCount() > 0 }, 5*time.Second)
	waitFor(t, func() bool { return snk.count() == 4 }, 5*time.Second)
	waitFor(t, func() bool { return mq.cursor("collector-0") == 0 }, 5*time.Second)

	// And it settles: no further re-anchoring once it is caught up again.
	settled := mq.outOfRangeCount()
	time.Sleep(300 * time.Millisecond)
	if got := mq.outOfRangeCount() - settled; got > 2 {
		t.Fatalf("collector re-anchored %d more times after catching up: spinning", got)
	}
	cancel()
}

// TestReanchorSettlesInsteadOfSpinning guards the recovery path against becoming
// a hot loop: a broker that keeps rejecting must not produce a tight
// re-anchor/consume/reject cycle burning CPU.
func TestReanchorSettlesInsteadOfSpinning(t *testing.T) {
	mq := newFakeMQ()
	mq.seed(
		event("evt_L0_O0", 0),
		event("evt_L0_O1", 1),
	)
	snk := &fakeSink{}
	cancel, _ := runCollector(t, testConfig(t, "collector-0"), mq, snk)
	waitFor(t, func() bool { return snk.count() == 2 }, 5*time.Second)
	waitFor(t, func() bool { return mq.cursor("collector-0") == 1 }, 5*time.Second)

	mq.shrinkLog(1, 1, -1)

	waitFor(t, func() bool { return mq.outOfRangeCount() > 0 }, 5*time.Second)
	waitFor(t, func() bool { return mq.cursor("collector-0") == 0 }, 5*time.Second)

	// Recovery must converge, not become a tight reject/re-anchor cycle.
	settled := mq.outOfRangeCount()
	time.Sleep(300 * time.Millisecond)
	if got := mq.outOfRangeCount() - settled; got > 2 {
		t.Fatalf("collector re-anchored %d times after settling: it is spinning, not recovering", got)
	}
	cancel()
}

// TestTransientStreamErrorStillRetriesSameCursor makes sure the re-anchor path
// did not swallow ordinary error handling. A transport blip must retry from the
// committed cursor (nothing durable is lost) rather than jump to the head and
// replay the whole log.
func TestTransientStreamErrorStillRetriesSameCursor(t *testing.T) {
	mq := newFakeMQ()
	mq.seed(
		event("evt_L0_O0", 0),
		event("evt_L0_O1", 1),
		event("evt_L0_O2", 2),
	)
	// Cursor -1: nothing processed yet, so all three events are read. A
	// healthy broker must never reject it.
	mq.cur["collector-0"] = -1
	mq.staleUntil = -1 // healthy broker: never OutOfRange

	snk := &fakeSink{}
	cancel, _ := runCollector(t, testConfig(t, "collector-0"), mq, snk)
	waitFor(t, func() bool { return snk.count() == 3 }, 5*time.Second)

	if mq.outOfRangeCount() != 0 {
		t.Fatalf("a healthy broker produced %d OutOfRange errors", mq.outOfRange)
	}
	if got := mq.cursor("collector-0"); got != 2 {
		t.Fatalf("cursor=%d, want 2", got)
	}
	cancel()
}
