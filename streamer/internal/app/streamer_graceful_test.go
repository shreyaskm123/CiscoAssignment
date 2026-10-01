package app

import (
	"context"
	"testing"
	"time"

	"streamer/internal/csvsrc"
	"streamer/internal/mqclient"
)

// TestGracefulShutdownFlushesBufferedEvents proves that cancelling the run
// context delivers every event that was already accepted into the publish
// buffer (graceful drain), with none dropped.
//
// Setup: a single replica publishes all rows; BATCH_SIZE is large and the
// flush interval is huge so events accumulate in the buffer instead of being
// flushed during the run. When the context is cancelled, the drain must push
// the whole backlog to the MQ before Run returns.
func TestGracefulShutdownFlushesBufferedEvents(t *testing.T) {
	mq := newFakeMQ()
	addr := startFakeMQ(t, mq)
	cfg := testConfig(addr)
	cfg.BatchSize = 1000
	cfg.FlushIntervalMs = 60000 // never flushed by the interval within the test
	cfg.RowDelayMs = 0
	cfg.ShutdownGraceMs = 5000

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
	done := make(chan error, 1)
	go func() { done <- s.Run(ctx) }()

	// Let the streamer produce events continuously (looping over the small CSV)
	// so that a big backlog accumulates in the buffer.
	time.Sleep(300 * time.Millisecond)
	if s.Produced() == 0 {
		t.Fatal("streamer produced no events before shutdown")
	}

	cancel() // SIGTERM equivalent → graceful drain

	select {
	case err := <-done:
		if err == nil {
			t.Fatal("Run should return a non-nil error on cancel")
		}
	case <-time.After(10 * time.Second):
		t.Fatal("Run did not return after cancel (drain hung)")
	}

	// Compare against the final accepted count (produced can tick once more
	// between the pre-cancel check and the drain closing the input).
	produced := s.Produced()
	delivered := int64(len(mq.received()))
	if delivered != produced {
		t.Errorf("data loss on graceful shutdown: produced=%d delivered=%d", produced, delivered)
	}
}
