// Package app orchestrates the collector: it waits for a stable registry
// assignment, consumes the MQ log shard it owns (offset % total == index),
// buffers events, durably writes batches to ClickHouse, and only then advances
// its committed cursor (crash recovery) — so delivery is at-least-once and the
// storage layer collapses any residual duplicates.
package app

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"collector/internal/config"
	"collector/internal/health"
	"collector/internal/mqclient"
	"collector/internal/partition"
	"collector/internal/sink"

	mqpb "streamer/proto"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// MQ is the message-queue surface the collector depends on (implemented by
// mqclient.Client and by the in-memory fake used in tests).
type MQ interface {
	GetOffset(ctx context.Context, consumerID string) (int64, bool, error)
	JoinPartition(ctx context.Context, consumerID string, ttlSeconds int32) (index, total int, err error)
	LeavePartition(ctx context.Context, consumerID string) error
	Consume(ctx context.Context, consumerID string, start int64) (mqclient.Consumer, error)
	CommitOffset(ctx context.Context, consumerID string, offset int64) error
}

// Collector consumes the owned shard of the MQ log and writes it to a sink.
type Collector struct {
	cfg *config.Config
	mq  MQ
	wr  sink.Writer

	watcher *partition.Watcher
	shard   atomic.Value // partition.State

	lastRead  atomic.Int64 // highest log offset observed on the stream
	committed atomic.Int64 // highest log offset durably written + committed

	buf       []sink.Event
	lastFlush time.Time
	dlOnce    sync.Once
	dlPath    string

	// Counters behind Status(), which is what the readiness probe reads.
	//
	// They are cumulative and monotonic on purpose, and none of them may gate
	// readiness: a counter that only grows (events written, events
	// dead-lettered) would keep a pod NotReady forever after its cause passed,
	// and a rolling update would hang waiting for Ready. Readiness is gated only
	// on conditions that clear themselves: a shard assignment, an in-flight
	// write failure, and progress stopping.
	lastProgress   atomic.Int64 // unix nanos of the last sign of life
	written        atomic.Int64 // events successfully written
	skipped        atomic.Int64 // read but not owned by this shard
	writeFailures  atomic.Int64 // batches that failed every attempt
	writeFailSince atomic.Int64 // unix nanos the current failure streak began
	deadLettered   atomic.Int64 // events preserved to the dead-letter file
}

// New wires a Collector without starting it. onReconfigure installs the shard
// assignment produced by the watcher.
func New(cfg *config.Config, mq MQ, wr sink.Writer) *Collector {
	c := &Collector{
		cfg:    cfg,
		mq:     mq,
		wr:     wr,
		buf:    make([]sink.Event, 0, cfg.BatchSize),
		dlPath: filepath.Join(cfg.DeadLetterDir, fmt.Sprintf("collector_%s_dead_letter.jsonl", cfg.ConsumerID)),
	}
	// The real shard assignment arrives from the registry watcher during Run;
	// until it is applied the collector is not Ready and does not consume, so
	// this zero state is never used for an actual ownership check.
	c.shard.Store(partition.State{})
	// Start the stall clock here: from now on "no progress" is measured against
	// this, and a zero timestamp would read as "stalled since the epoch".
	c.lastProgress.Store(time.Now().UnixNano())
	return c
}

// Run blocks until ctx is cancelled, then drains and returns nil.
func (c *Collector) Run(ctx context.Context) error {
	c.lastFlush = time.Now()
	c.watcher = partition.NewWatcher(
		c.mq,
		c.cfg.ConsumerID,
		c.cfg.RegistryTTL,
		c.cfg.RegistryPoll,
		func(s partition.State) {
			c.shard.Store(s)
			log.Printf("repartitioned: total=%d index=%d", s.Total, s.Index)
		},
	)
	c.watcher.Start(ctx)

	select {
	case <-c.watcher.Ready():
	case <-ctx.Done():
		return ctx.Err()
	}

	start, exists, err := c.mq.GetOffset(ctx, c.cfg.ConsumerID)
	if err != nil {
		return fmt.Errorf("get offset: %w", err)
	}
	if !exists {
		start = -1 // fresh consumer: nothing committed, start at the log head
	}
	c.committed.Store(start)
	c.lastRead.Store(start)

	poll := time.NewTicker(c.cfg.ConsumePoll)
	defer poll.Stop()

	for ctx.Err() == nil {
		err := c.consumeStream(ctx, c.committed.Load())
		if ctx.Err() != nil {
			break // graceful cancellation: shutdown() drains what's buffered
		}
		switch {
		case err == nil:
			// Log tail reached; poll again shortly (handled below).
		case status.Code(err) == codes.OutOfRange:
			// The broker rejected our cursor: it was committed against a log
			// incarnation the current leader no longer has (normal after a
			// failover, when the survivor's log is shorter than the cursor).
			// Re-anchor from the broker's view rather than retrying an offset
			// that can never resolve, which would stall us silently forever.
			log.Printf("cursor out of range (%v); re-anchoring from broker", err)
			c.disposeBuffer()
			if aerr := c.reanchor(ctx); aerr != nil {
				log.Printf("re-anchor failed: %v", aerr)
				if !sleepCtx(ctx, c.cfg.InsertBackoff) {
					return c.shutdown()
				}
			}
		default:
			// Transient stream error: nothing durable lost — replay from the
			// committed cursor (buffered-but-unflushed events are re-read).
			log.Printf("consume stream: %v", err)
			c.disposeBuffer()
			if !sleepCtx(ctx, c.cfg.InsertBackoff) {
				return c.shutdown()
			}
		}
		// Log tail reached; poll again shortly.
		select {
		case <-ctx.Done():
		case <-poll.C:
		}
	}

	return c.shutdown()
}

// reanchor resets the read position to whatever the broker reports for us. The
// broker repairs an unreachable cursor onto its retained head and says so, so
// this cannot move us backwards past data we still owe the sink - it only ever
// re-reads, and the sink deduplicates by event_id.
func (c *Collector) reanchor(ctx context.Context) error {
	start, exists, err := c.mq.GetOffset(ctx, c.cfg.ConsumerID)
	if err != nil {
		return err
	}
	if !exists {
		start = -1
	}
	prev := c.committed.Swap(start)
	c.lastRead.Store(start)
	log.Printf("re-anchored: cursor %d -> %d (exists=%t)", prev, start, exists)
	return nil
}

// sleepCtx waits for d and reports whether it completed (false means ctx was
// cancelled, so the caller must stop rather than poll again).
func sleepCtx(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}

// consumeStream reads one window of the log (from start to the current tail),
// applying the shard filter and flushing when the batch fills up.
func (c *Collector) consumeStream(ctx context.Context, start int64) error {
	stream, err := c.mq.Consume(ctx, c.cfg.ConsumerID, start)
	if err != nil {
		return err
	}
	for {
		msg, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			return c.maybeFlush(ctx)
		}
		if err != nil {
			return err
		}
		c.lastRead.Store(msg.Offset)
		c.lastProgress.Store(time.Now().UnixNano())
		if !c.owns(msg.Offset) {
			c.skipped.Add(1)
			continue
		}
		c.buf = append(c.buf, sink.Event{Event: msg.Event, Offset: msg.Offset})
		if c.flushReady() {
			if err := c.flush(ctx); err != nil {
				return err
			}
		}
	}
}

// owns reports whether the current shard assignment owns the offset.
func (c *Collector) owns(offset int64) bool {
	return c.shard.Load().(partition.State).Owns(offset)
}

// flushReady triggers on batch-size or elapsed-since-last-flush.
func (c *Collector) flushReady() bool {
	if len(c.buf) == 0 {
		return false
	}
	if len(c.buf) >= c.cfg.BatchSize {
		return true
	}
	return time.Since(c.lastFlush) >= c.cfg.FlushInterval
}

func (c *Collector) maybeFlush(ctx context.Context) error {
	if len(c.buf) > 0 {
		return c.flush(ctx)
	}
	return nil
}

// flush writes the buffered batch to the sink (with retries) and, once the
// write is durable, commits the highest observed offset. If the sink keeps
// failing, the batch is dead-lettered and the cursor still advances (fail-open
// policy: the pipeline never wedges).
//
// The one thing flush will NOT do is advance the cursor past a batch that was
// neither stored nor dead-lettered. Fail-open exists so a down sink cannot wedge
// the pipeline, not so events can vanish: if the dead-letter write fails too
// (disk full, unwritable volume), the batch is nowhere, so the cursor is held and
// an error is returned. The caller then re-reads from the committed cursor, which
// replays the batch once something can accept it; the sink absorbs the replay.
func (c *Collector) flush(ctx context.Context) error {
	if len(c.buf) == 0 {
		return nil
	}
	batch := c.buf
	c.buf = c.buf[:0]
	c.lastFlush = time.Now()

	now := time.Now()
	var writeErr error
	for attempt := 0; attempt < c.cfg.InsertRetries; attempt++ {
		writeErr = c.wr.Write(ctx, batch)
		if writeErr == nil {
			break
		}
		log.Printf("write to ClickHouse failed (attempt %d/%d): %v", attempt+1, c.cfg.InsertRetries, writeErr)
		if attempt < c.cfg.InsertRetries-1 && ctx.Err() == nil {
			time.Sleep(c.cfg.InsertBackoff)
		}
	}
	if writeErr == nil {
		c.written.Add(int64(len(batch)))
		// Clears the failure streak, so readiness recovers on its own once
		// ClickHouse accepts writes again.
		c.writeFailSince.Store(0)
		c.lastProgress.Store(time.Now().UnixNano())
	}
	if writeErr != nil {
		c.writeFailures.Add(1)
		if c.writeFailSince.Load() == 0 {
			c.writeFailSince.Store(now.UnixNano())
		}
		if dlErr := c.deadLetter(batch, writeErr); dlErr != nil {
			log.Printf("batch of %d events (offsets %d..%d) is neither stored nor dead-lettered; "+
				"holding the cursor at %d so it is replayed: store error: %v; dead-letter error: %v",
				len(batch), batch[0].Offset, batch[len(batch)-1].Offset, c.committed.Load(), writeErr, dlErr)
			return fmt.Errorf("batch not persisted anywhere: %w", dlErr)
		}
	}

	// Cursor advances to the highest offset read so far: everything <= lastRead
	// is handled (owned events were written or dead-lettered; skipped events are
	// no-ops). This is the at-least-once contract.
	if err := c.mq.CommitOffset(context.Background(), c.cfg.ConsumerID, c.lastRead.Load()); err != nil && ctx.Err() == nil {
		log.Printf("commit offset: %v", err)
	} else {
		c.lastProgress.Store(time.Now().UnixNano())
	}
	c.committed.Store(c.lastRead.Load())
	return nil
}

// disposeBuffer clears any buffered (unflushed) events. It is safe because the
// committed cursor never advances past them: a crash or stream error replays
// them from the committed position, and the storage layer absorbs the replay.
func (c *Collector) disposeBuffer() {
	c.buf = c.buf[:0]
}

// shutdown drains whatever is buffered, leaves the registry, and closes out.
func (c *Collector) shutdown() error {
	dctx, cancel := context.WithTimeout(context.Background(), c.cfg.ShutdownGrace)
	defer cancel()
	if len(c.buf) > 0 {
		if err := c.flush(dctx); err != nil {
			// Not committed, so the next start replays it from the cursor.
			log.Printf("shutdown flush: %v", err)
		}
	}
	leaveCtx, leaveCancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer leaveCancel()
	if err := c.mq.LeavePartition(leaveCtx, c.cfg.ConsumerID); err != nil {
		log.Printf("leave partition: %v", err)
	}
	log.Printf("collector stopped: consumer=%s committedOffset=%d", c.cfg.ConsumerID, c.committed.Load())
	return nil
}

// Status is the collector's self-assessment, served on /readyz.
//
// Readiness is false only for conditions that clear themselves, so a pod
// recovers without a restart as soon as its cause passes:
//
//   - no shard assignment yet: the registry watcher has not reported a stable
//     (index, total), so this replica does not yet know which offsets are its
//     responsibility and must not be treated as a working consumer;
//   - a write failure in flight: the last batch failed every attempt, so events
//     are being taken from the log and are not reaching ClickHouse;
//   - no progress: nothing has been read, written or committed for
//     ReadyMaxStall. This is the one that catches a stall with no error at all -
//     a message queue that stops acking leaves the collector blocked in Recv,
//     reporting nothing, which is exactly how a pipeline once ran for hours at a
//     fraction of its rate while every pod looked healthy.
//
// Dead-lettered events are reported but deliberately do NOT gate readiness:
// that count only grows, so gating on it would hold a pod NotReady forever after
// ClickHouse recovered and would wedge the next rolling update.
func (c *Collector) Status() health.Check {
	shard := c.shard.Load().(partition.State)
	now := time.Now()
	fields := map[string]string{
		"consumer":               c.cfg.ConsumerID,
		"shard":                  fmt.Sprintf("%d/%d", shard.Index, shard.Total),
		"committed_offset":       strconv.FormatInt(c.committed.Load(), 10),
		"last_read_offset":       strconv.FormatInt(c.lastRead.Load(), 10),
		"cursor_lag":             strconv.FormatInt(c.lastRead.Load()-c.committed.Load(), 10),
		"events_written_total":   strconv.FormatInt(c.written.Load(), 10),
		"events_skipped_total":   strconv.FormatInt(c.skipped.Load(), 10),
		"write_failures_total":   strconv.FormatInt(c.writeFailures.Load(), 10),
		"dead_letter_rows_total": strconv.FormatInt(c.deadLettered.Load(), 10),
	}
	if since := c.writeFailSince.Load(); since != 0 {
		fields["failing_write_age_seconds"] = strconv.FormatInt(int64(now.Sub(time.Unix(0, since)).Seconds()), 10)
	}
	if last := c.lastProgress.Load(); last != 0 {
		fields["last_progress_age_seconds"] = strconv.FormatInt(int64(now.Sub(time.Unix(0, last)).Seconds()), 10)
	}

	switch {
	case shard.Total <= 0:
		return health.Check{Reason: "no shard assignment from the MQ registry yet", Fields: fields}
	case c.writeFailSince.Load() != 0:
		age := int64(now.Sub(time.Unix(0, c.writeFailSince.Load())).Seconds())
		return health.Check{
			Reason: fmt.Sprintf("writing to the sink has been failing for %ds (events are being read but not stored)", age),
			Fields: fields,
		}
	}
	if max := c.cfg.ReadyMaxStall; max > 0 {
		if last := c.lastProgress.Load(); last != 0 && now.Sub(time.Unix(0, last)) > max {
			return health.Check{
				Reason: fmt.Sprintf("no progress for %ds (waiting on the message queue or the sink)",
					int64(now.Sub(time.Unix(0, last)).Seconds())),
				Fields: fields,
			}
		}
	}
	return health.Check{Ready: true, Fields: fields}
}

// deadLetter writes the batch to a JSONL side channel so data is never silently
// dropped even when ClickHouse is hard-down. It returns an error if the batch
// could not be fully and durably written, in which case the caller must not
// treat the batch as handled.
func (c *Collector) deadLetter(batch []sink.Event, cause error) error {
	c.dlOnce.Do(func() {
		log.Printf("dead-lettering failed batches to %s (first cause: %v)", c.dlPath, cause)
	})
	// The directory is created here, not assumed: on Kubernetes it is a
	// subdirectory of an emptyDir mount, which starts out empty. Assuming it
	// existed made every dead-letter write fail with ENOENT, so a ClickHouse
	// outage silently dropped events instead of preserving them.
	if err := os.MkdirAll(filepath.Dir(c.dlPath), 0o755); err != nil {
		return fmt.Errorf("dead-letter dir: %w", err)
	}
	f, err := os.OpenFile(c.dlPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		return fmt.Errorf("dead-letter open: %w", err)
	}
	enc := json.NewEncoder(f)
	saved := 0
	for _, ev := range batch {
		if err := enc.Encode(deadLetterRow{Event: ev.Event, Offset: ev.Offset}); err != nil {
			f.Close()
			return fmt.Errorf("dead-letter write: %w", err)
		}
		saved++
	}
	// Sync: the whole point is to survive the failure that put us here.
	if err := f.Sync(); err != nil {
		f.Close()
		return fmt.Errorf("dead-letter sync: %w", err)
	}
	if err := f.Close(); err != nil {
		return fmt.Errorf("dead-letter close: %w", err)
	}
	c.deadLettered.Add(int64(saved))
	return nil
}

type deadLetterRow struct {
	Event  *mqpb.Event `json:"event"`
	Offset int64       `json:"offset"`
}
