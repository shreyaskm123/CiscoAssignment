package app

import (
	"context"
	"fmt"
	"log"
	"strconv"
	"sync/atomic"
	"time"

	"streamer/internal/config"
	"streamer/internal/csvsrc"
	"streamer/internal/event"
	"streamer/internal/health"
	"streamer/internal/mqclient"
	"streamer/internal/offsets"
	"streamer/internal/scheduler"

	mqpb "streamer/proto"
)

// Streamer ties the components together:
//
//	reader  CSV iteration with loop/reset semantics
//	offs    readPosition / committedPosition pointers
//	sched   deterministic partition (currentLineNum % replicas == index)
//	pub     gRPC publish stream with ack-driven offset commit
//	batch   buffers owned events and flushes at a bound
//	watcher picks up scale up/down and configMap rotations at runtime
type Streamer struct {
	cfg    *config.Config
	reader *csvsrc.Reader
	offs   *offsets.Manager
	sched  *scheduler.Scheduler
	client *mqclient.Client
	pub    *mqclient.Stream
	batch  *batcher
	watch  *configWatcher

	// lastAck is the unix-nano time of the most recent acknowledged publish:
	// the strongest end-to-end signal this replica has, because an ack means the
	// message queue made the event durable. Zero means it has never happened.
	lastAck       atomic.Int64
	lastCommitted atomic.Int64 // committed offset that lastAck was recorded for
	startedAt     int64        // unix nanos of construction, for the stall clock
}

// New wires the components and performs crash recovery via GetOffset before
// returning. Acks advance committedPosition. The replica joins the MQ's
// partition registry to learn its (index, total) for the deterministic
// workload distribution.
func New(ctx context.Context, cfg *config.Config, reader *csvsrc.Reader, client *mqclient.Client) (*Streamer, error) {
	// Crash recovery: ask the MQ where this consumer last got an ack and
	// resume the read pointer from there.
	start, exists, err := client.GetOffset(ctx, cfg.ConsumerID)
	if err != nil {
		return nil, err
	}
	if exists {
		log.Printf("recovered: consumer %s resumes at offset %d", cfg.ConsumerID, start)
		reader.SeekTo(start)
	}
	offs := offsets.New(start)

	// Partition assignment comes exclusively from the registry: the index is
	// the replica's lexicographic rank among the live consumers in the MQ
	// partition registry (see registry.Join), never from an env var.
	schedIdx, schedTotal, err := client.JoinPartition(ctx, cfg.ConsumerID, int32(cfg.RegistryTTLSec))
	if err != nil {
		return nil, fmt.Errorf("registry join: %w", err)
	}
	log.Printf("registry: consumer %s joined as index %d/%d", cfg.ConsumerID, schedIdx, schedTotal)
	sched := scheduler.New(schedIdx, schedTotal)

	// The Streamer exists before the publish stream so the ack callback can
	// record progress on it. The callback only runs once publishing starts, by
	// which time s is fully built.
	s := &Streamer{
		cfg:       cfg,
		startedAt: time.Now().UnixNano(),
		reader:    reader,
		offs:      offs,
		sched:     sched,
		client:    client,
	}

	pub, err := client.NewStream(ctx, func(ack *mqpb.Ack) {
		if ack.Success {
			offs.Commit(ack.Offset)
			s.recordAck(ack.Offset)
		} else {
			log.Printf("mq ack failure for %s: %s", ack.EventId, ack.Error)
		}
	})
	if err != nil {
		return nil, err
	}
	s.pub = pub
	s.batch = newBatcher(cfg.BatchSize, time.Duration(cfg.FlushIntervalMs)*time.Millisecond, pub)
	s.watch = newConfigWatcher(cfg, client, sched, func(i, r int) {
		sched.Reconfigure(i, r)
		log.Printf("repartitioned: POD_INDEX=%d TOTAL_REPLICAS=%d", i, r)
	})
	return s, nil
}

// Run executes the streaming loop until ctx is cancelled. readPosition
// advances for every row read; only rows owned by this replica are published.
func (s *Streamer) Run(ctx context.Context) error {
	defer s.pub.Close()

	s.watch.start(ctx)
	s.batch.start(ctx)
	go s.watchCSV(ctx)

	// Do not publish until the partition assignment from the registry has
	// stabilized (two consecutive identical polls). Skipping this produces a
	// fleet-wide inconsistent first loop when replicas boot in one wave.
	select {
	case <-s.watch.Ready():
	case <-ctx.Done():
		return ctx.Err()
	}

	delay := time.Duration(s.cfg.RowDelayMs) * time.Millisecond
	grace := time.Duration(s.cfg.ShutdownGraceMs) * time.Millisecond

	for {
		select {
		case <-ctx.Done():
			log.Printf("shutdown signal received; draining %d buffered/in-flight events (grace %s)",
				s.batch.pendingLen(), grace)
			s.batch.drain(grace)
			// Deregister only after the drain so events are still acked;
			// survivors then rebalance immediately instead of waiting out
			// the heartbeat TTL.
			if err := s.leavePartition(); err != nil {
				log.Printf("registry leave: %v", err)
			}
			log.Printf("streamer stopped: readPosition=%d committedPosition=%d lag=%d produced=%d",
				s.offs.ReadPosition(), s.offs.CommittedPosition(), s.offs.Lag(), s.batch.produced.Load())
			return ctx.Err()
		default:
		}

		row, err := s.reader.Next()
		if err != nil {
			// no data rows yet (configMap not mounted): retry shortly
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(500 * time.Millisecond):
			}
			continue
		}

		s.offs.AdvanceRead(row.LineNum)

		// Deterministic workload distribution:
		// currentLineNum % total == index → this replica processes the row.
		if !s.sched.Owns(row.LineNum) {
			continue
		}

		if !validValue(row) {
			log.Printf("skip row offset=%d: non-numeric value %q", row.LineNum, row.Raw[csvsrc.ColValue])
			if delay > 0 {
				time.Sleep(delay)
			}
			continue
		}

		s.batch.add(mapEvent(s.cfg, row))

		if delay > 0 {
			time.Sleep(delay)
		}
	}
}

// recordAck notes that the message queue acknowledged an event, which is the
// only proof this replica has that its output was made durable somewhere. It is
// the signal that stalls when sync replication wedges: publishes keep being
// accepted, no error is raised, and no ack ever comes back.
func (s *Streamer) recordAck(offset int64) {
	s.lastAck.Store(time.Now().UnixNano())
	s.lastCommitted.Store(offset)
}

// Status is the streamer's self-assessment, served on /readyz.
//
// Readiness is false only for conditions that clear themselves, so the pod
// recovers on its own once its cause passes:
//
//   - no partition assignment yet: without an (index, total) this replica does
//     not know which CSV rows are its responsibility;
//   - a publish failing right now: the last Send to the MQ failed;
//   - no acknowledged publish for ReadyMaxStall: the stall case. The streamer
//     blocks in Send while the queue withholds acks, so nothing errors and the
//     pod would otherwise look healthy while producing nothing.
//
// The unacked backlog is reported but does not gate readiness: its natural size
// depends on the batch size and the round-trip time, so a fixed threshold would
// either flap on a slow link or miss a real stall. It is the number to look at
// when the stall check fires.
func (s *Streamer) Status() health.Check {
	now := time.Now()
	index, total := s.sched.PodIndex(), s.sched.TotalReplicas()
	sent, acked := s.pub.Sent(), s.pub.Acked()
	fields := map[string]string{
		"consumer":           s.cfg.ConsumerID,
		"shard":              fmt.Sprintf("%d/%d", index, total),
		"read_position":      strconv.FormatInt(s.offs.ReadPosition(), 10),
		"committed_position": strconv.FormatInt(s.offs.CommittedPosition(), 10),
		"lag":                strconv.FormatInt(s.offs.Lag(), 10),
		"produced_total":     strconv.FormatInt(s.batch.produced.Load(), 10),
		"sent_total":         strconv.FormatInt(sent, 10),
		"acked_total":        strconv.FormatInt(acked, 10),
		"unacked_in_flight":  strconv.FormatInt(sent-acked, 10),
	}
	if last := s.lastAck.Load(); last != 0 {
		fields["last_ack_age_seconds"] = strconv.FormatInt(int64(now.Sub(time.Unix(0, last)).Seconds()), 10)
	}
	if last := s.batch.lastSendFail.Load(); last != 0 {
		fields["last_send_failure_age_seconds"] = strconv.FormatInt(int64(now.Sub(time.Unix(0, last)).Seconds()), 10)
	}

	switch {
	case total <= 0:
		return health.Check{Reason: "no partition assignment from the MQ registry yet", Fields: fields}
	case s.batch.lastSendFail.Load() != 0:
		age := int64(now.Sub(time.Unix(0, s.batch.lastSendFail.Load())).Seconds())
		return health.Check{Reason: fmt.Sprintf("publishing to the message queue has been failing for %ds", age), Fields: fields}
	}
	if max := s.cfg.ReadyMaxStall; max > 0 {
		// Measured from the last ack when there has been one, otherwise from
		// construction: a replica that has never published anything must not
		// read as stalled since the epoch.
		since := s.lastAck.Load()
		if since == 0 {
			since = s.startedAt
		}
		if since != 0 && now.Sub(time.Unix(0, since)) > max {
			// Stated as observation, not diagnosis: "sent" counts writes to
			// the stream and "unacked" the ones the queue has not confirmed, and
			// which of those is non-zero depends on whether the publish is
			// blocked outright or accepted and left unconfirmed. The fields carry
			// the numbers; the operator supplies the interpretation.
			return health.Check{
				Reason: fmt.Sprintf("no acknowledged publish for %ds (%d sent, %d awaiting acknowledgement)",
					int64(now.Sub(time.Unix(0, since)).Seconds()), sent, sent-acked),
				Fields: fields,
			}
		}
	}
	return health.Check{Ready: true, Fields: fields}
}

// Produced returns how many events were accepted into the publish buffer.
// Used by tests (and observability) to prove no events are lost on shutdown.
func (s *Streamer) Produced() int64 {
	return s.batch.produced.Load()
}

// Partition returns the replica's currently configured (index, total). Used by
// tests and observability to confirm registry-driven (re)partitioning.
func (s *Streamer) Partition() (index, total int) {
	return s.sched.PodIndex(), s.sched.TotalReplicas()
}

// leavePartition deregisters from the MQ registry on graceful shutdown,
// bounding the RPC so it can never hang the exit.
func (s *Streamer) leavePartition() error {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	return s.client.LeavePartition(ctx, s.cfg.ConsumerID)
}

// mapEvent converts a CSV row to the wire event.
//
// The CSV's own timestamp column is deliberately never read: the file is
// replayed in a loop, so its fixed historical time would repeat on every pass
// and say nothing about when the event actually happened. SourceTimestamp is
// the only timestamp on the event: the streamer's current UTC time, taken once
// per event, and the same value reaches the MQ, the collector and ClickHouse
// (source_ts). Time-window queries therefore select by when the streamer
// produced the event.
// The event id remains deterministic (evt_<pod>_L<loop>_O<offset>).
func mapEvent(cfg *config.Config, row csvsrc.Row) *mqpb.Event {
	now := time.Now().UTC().Format(time.RFC3339Nano)
	return &mqpb.Event{
		EventId:         event.ID(cfg.PodName, row.Loop, row.LineNum),
		SourceTimestamp: now,
		MetricName:      row.Raw[csvsrc.ColMetricName],
		GpuIndex:        parseInt(row.Raw[csvsrc.ColGPUID]),
		DeviceName:      row.Raw[csvsrc.ColDevice],
		DeviceId:        row.Raw[csvsrc.ColUUID],
		ModelName:       row.Raw[csvsrc.ColModelName],
		Hostname:        row.Raw[csvsrc.ColHostname],
		Value:           parseFloat(row.Raw[csvsrc.ColValue]),
		CsvLineOffset:   row.LineNum,
		LoopCount:       row.Loop,
		Tags: map[string]string{
			"cluster":  cfg.Cluster,
			"pod_name": cfg.PodName,
		},
	}
}

// validValue ensures the row's value parses as a float so malformed rows never
// reach the MQ.
func validValue(row csvsrc.Row) bool {
	_, err := strconv.ParseFloat(row.Raw[csvsrc.ColValue], 64)
	return err == nil
}

// watchCSV periodically stats the mounted CSV so a configMap rotation is picked
// up without restarting the pod. On rotation the reader reloads and both
// pointers restart at 0 (fresh dataset).
func (s *Streamer) watchCSV(ctx context.Context) {
	t := time.NewTicker(time.Duration(s.cfg.ReloadCheckInterval) * time.Second)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			reloaded, err := s.reader.ReloadCheck()
			if err != nil {
				log.Printf("csv reload check: %v", err)
				continue
			}
			if reloaded {
				log.Printf("csv changed; restarting iteration from offset 0")
				s.reader.SeekTo(0)
				s.offs.ResetTo(0)
			}
		}
	}
}

func parseInt(s string) int32 {
	if n, err := strconv.ParseInt(s, 10, 32); err == nil {
		return int32(n)
	}
	return 0
}

func parseFloat(s string) float64 {
	if f, err := strconv.ParseFloat(s, 64); err == nil {
		return f
	}
	return 0
}
