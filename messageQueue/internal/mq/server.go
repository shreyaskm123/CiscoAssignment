// Package mq implements the MessageQueue gRPC service: it receives a stream of
// Events, acknowledges each one (carrying the consumer's committed offset), and
// answers GetOffset/JoinPartition/LeavePartition plus the consumer read path
// (Consume/CommitOffset). It is the real counterpart to the streamer proto
// contract (streamer/proto/mq.proto).
//
// State layout:
//
//   - The event log lives behind the Store seam (in-memory today). Every new
//     event is appended with a global monotonic log offset; the log is the
//     source the consumers read from.
//   - Fleet state is namespaced by "group" (see JoinPartition.group): each
//     group ("streamer", "collector", ...) has its own partition registry and
//     its own per-consumer offsets. Publisher acks always land in the streamer
//     group; collector cursors/retention live in the collector group.
package mq

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"math"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"messagequeue/internal/registry"

	mqpb "streamer/proto"
)

// dedupCap bounds the event_id dedup set (FIFO eviction). Offsets are still
// committed per consumer regardless of dedup, so only re-sent duplicates within
// this window are suppressed.
const dedupCap = 100_000

// waitReplicationTimeout bounds how long a publisher waits for sync-replicas
// followers to apply an event before the ack is withheld (the producer then
// re-sends). Generous so a briefly reconnecting follower fails safe.
const waitReplicationTimeout = 5 * time.Second

// Fleet namespaces shared between the MQ and its clients. Publisher acks are
// always recorded under the streamer group; collector cursor/retention state
// under the collector group. An empty group on a request falls back to
// defaultGroup (keeps callers that don't care about scoping working).
const (
	streamerGroup  = "streamer"
	collectorGroup = "collector"
	defaultGroup   = "default"
)

func normGroup(g string) string {
	if g == "" {
		return defaultGroup
	}
	return g
}

// groupState is the per-group fleet state: an independent partition registry
// and the group's per-consumer offsets (acked csv lines for streamers, log
// cursors for collectors).
type groupState struct {
	reg       *registry.Registry
	consumers map[string]int64 // consumer_id -> offset
}

// Server is the concrete MessageQueue implementation.
type Server struct {
	mqpb.UnimplementedMessageQueueServer
	mqpb.UnimplementedReplicationServer

	store Store // event log (durability seam; WAL-backed store comes later)
	ttl   time.Duration

	mu        sync.Mutex
	groups    map[string]*groupState // group name -> fleet state
	seen      map[string]struct{}    // event_id -> present (dedup window)
	seenOrder []string               // FIFO insertion order for seen eviction

	// --- replication (leader) ---
	// gen is the leader's mutation sequence number: every replicated mutation
	// (append/commit/trim) bumps it, and acks-still-pending publishers wait
	// until syncWrites followers have applied up to it. Sync writes are empty
	// (0) unless -sync-replicas is set.
	gen        int64
	syncWrites int
	nextFid    int64
	followers  map[string]*follower // live Sync streams (id -> state)

	// role is this node's leader/follower position, driven by the election
	// elector through the HA controller (see leader.go). It is an atomic rather
	// than a bool under mu because it is read on every publish and commit - the
	// hot path - and because fencing must take effect immediately when the lease
	// is lost, without waiting behind an in-flight handler holding s.mu.
	//
	// RoleFollower is the zero value on purpose: a node that has not won the
	// lease is read-only, so an unelected process cannot ack a publisher.
	role atomic.Int32 // Role
}

// IsLeader reports whether this node currently accepts writes. A write is
// permitted only while this is true, i.e. only while this node holds the
// election lease.
func (s *Server) IsLeader() bool { return Role(s.role.Load()) == RoleLeader }

// logf is the package logger seam, indirected so tests can capture output and
// so failover events can be surfaced distinctly from routine debug logging.
var logf = log.Printf

// storeBase returns the retained head offset, or 0 for a non-durable store.
func (s *Server) storeBase() int64 {
	if ds, ok := s.store.(DurableStore); ok {
		return ds.Base()
	}
	return 0
}

// setSyncWrites adjusts the leader's sync-replication quorum under the lock.
// Promote/Demote use it because the setting is only meaningful on a leader and
// must not leak across role changes.
func (s *Server) setSyncWrites(n int) {
	s.mu.Lock()
	s.syncWrites = n
	s.mu.Unlock()
}

// New returns a Server with the given default heartbeat TTL for registries,
// backed by an in-memory Store (no durability).
func New(ttl time.Duration) *Server {
	return NewWithStore(ttl, NewMemStore())
}

// NewWithStore returns a Server backed by the given Store, seeded (when
// durable) from its replayed cursors and retained events so a restarted MQ
// resumes exactly where the crashed one left off.
func NewWithStore(ttl time.Duration, store Store) *Server {
	s := &Server{
		store:  store,
		ttl:    ttl,
		groups: make(map[string]*groupState),
		seen:   make(map[string]struct{}),
	}
	// A single node with no election running has no lease to hold, so it is the
	// leader by default. The HA deployment overrides this immediately: the
	// constructor default is leader, and a node in an election pair must be
	// demoted to follower until it actually wins the lease. main.go does that
	// before the gRPC server starts serving, so no RPC is ever reachable in the
	// wrong role.
	s.role.Store(int32(RoleLeader))
	s.seedLocked()
	return s
}

// EnableSyncReplication marks the server as a leader that only acks a
// publisher once syncWrites connected followers have applied the mutation (a
// cked event is therefore on a quorum and survives a leader crash). Must be
// called before the server serves RPCs.
func (s *Server) EnableSyncReplication(syncWrites int) {
	s.setSyncWrites(syncWrites)
}

// RequireDurableStore fails when the server has no durable store, i.e. when
// acking a publisher could not survive a restart. Both replication and failover
// depend on this: a node that cannot persist must neither replicate nor be
// promoted, because in both cases an acked event would be lost silently.
func (s *Server) RequireDurableStore(what string) error {
	if _, ok := s.store.(DurableStore); !ok {
		return fmt.Errorf("%s requires a durable store (-data-dir)", what)
	}
	return nil
}

// seedLocked folds durable cursors and the retained tail into the in-memory
// group offsets and dedup window.
func (s *Server) seedLocked() {
	ds, ok := s.store.(DurableStore)
	if !ok {
		return
	}
	for _, cs := range ds.Cursors() {
		g := s.groupLocked(cs.Group)
		if off, ok := g.consumers[cs.Consumer]; !ok || cs.Offset >= off {
			g.consumers[cs.Consumer] = cs.Offset
		}
	}
	for _, le := range ds.Events() {
		// Derive streamer-group cursors from the retained events themselves
		// (mirrors WAL replay). Store persistence rebuilds these on a restart;
		// a live promotion (e.g. a follower that never replayed) needs the same
		// view without one.
		if pod := le.Event.Tags["pod_name"]; pod != "" {
			g := s.groupLocked(streamerGroup)
			if le.Event.CsvLineOffset > g.consumers[pod] {
				g.consumers[pod] = le.Event.CsvLineOffset
			}
		}
		if _, dup := s.seen[le.Event.EventId]; dup {
			continue
		}
		s.seen[le.Event.EventId] = struct{}{}
		s.seenOrder = append(s.seenOrder, le.Event.EventId)
		if len(s.seenOrder) > dedupCap {
			oldest := s.seenOrder[0]
			s.seenOrder = s.seenOrder[1:]
			delete(s.seen, oldest)
		}
	}
}

// Close flushes and releases durable state (no-op for an in-memory store).
func (s *Server) Close() error {
	if ds, ok := s.store.(DurableStore); ok {
		return ds.Close()
	}
	return nil
}

// groupLocked returns (creating if needed) the group's state. Callers must
// hold s.mu.
func (s *Server) groupLocked(name string) *groupState {
	g, ok := s.groups[name]
	if !ok {
		g = &groupState{reg: registry.New(s.ttl), consumers: make(map[string]int64)}
		s.groups[name] = g
	}
	return g
}

// eventJSON is the canonical snake_case JSON representation of an event, kept
// identical to the streamer's stub so the same format is logged everywhere.
type eventJSON struct {
	EventID         string `json:"event_id"`
	SourceTimestamp string `json:"source_timestamp"`
	// LegacyTimestamp is the pre-removal `timestamp` key. It is decode-only
	// (omitempty, never set on write): WAL records written before the field
	// was dropped still carry the event time under this key, and it is read as
	// the fallback so those events keep their time across an upgrade.
	LegacyTimestamp string            `json:"timestamp,omitempty"`
	MetricName      string            `json:"metric_name"`
	GPUIndex        int32             `json:"gpu_index"`
	DeviceName      string            `json:"device_name"`
	DeviceID        string            `json:"device_id"`
	ModelName       string            `json:"model_name"`
	Hostname        string            `json:"hostname"`
	Value           float64           `json:"value"`
	CSVLineOffset   int64             `json:"csv_line_offset"`
	LoopCount       int64             `json:"loop_count"`
	Tags            map[string]string `json:"tags"`
}

// PublishEvents receives a stream of events, acknowledges each one, and commits
// the per-consumer offset under the streamer group. A repeat of an already-seen
// event_id is acknowledged (so the producer progresses) but not counted as a
// new delivery (and not appended to the log again). When sync replication is
// on, an ack is only sent after the configured number of followers have applied
// the event, so an acked event survives a leader crash.
func (s *Server) PublishEvents(stream grpc.BidiStreamingServer[mqpb.Event, mqpb.Ack]) error {
	// Fence writes on role, not on a static flag: a node that loses the lease
	// must stop acking immediately, before a rival can be elected, or both nodes
	// would accept publishes and the log would fork.
	if !s.IsLeader() {
		return status.Error(codes.FailedPrecondition, FenceError("").Error())
	}
	for {
		e, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return err
		}

		cid := e.Tags["pod_name"]
		offset, duplicate, gen, err := s.record(e, cid)
		if err != nil {
			// Not durable: do not ack, so the producer re-sends on reconnect.
			return err
		}
		if err := s.waitReplicated(stream.Context(), gen); err != nil {
			// Sync replication could not be satisfied for this event: do not
			// ack, so the producer re-sends and the event is retried.
			return err
		}

		log.Printf("RECEIVED event_id=%s consumer=%s offset=%d -> %s",
			e.EventId, cid, offset, jsonEvent(e))

		if err := stream.Send(&mqpb.Ack{
			EventId: e.EventId,
			Offset:  offset,
			Success: true,
			Error:   dedupNote(duplicate),
		}); err != nil {
			return err
		}
	}
}

// record stores the event in the log (unless a duplicate of an already-stored
// event) and returns the streamer-group offset for the consumer. The event_id
// enters the dedup set only after the append succeeded, so a failed append
// cannot silently shadow a later retry. Connected followers are notified of the
// append under the same lock (latest first, so a slow follower only ever
// re-syncs from its watermark). It returns the mutation's replication gen (0
// when nothing was replicated: a duplicate or no connected followers).
func (s *Server) record(e *mqpb.Event, cid string) (int64, bool, int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	g := s.groupLocked(streamerGroup)
	if _, dup := s.seen[e.EventId]; dup {
		return g.consumers[cid], true, 0, nil
	}

	off, err := s.store.Append(e)
	if err != nil {
		return 0, false, 0, err
	}
	s.seen[e.EventId] = struct{}{}
	s.seenOrder = append(s.seenOrder, e.EventId)
	if len(s.seenOrder) > dedupCap {
		oldest := s.seenOrder[0]
		s.seenOrder = s.seenOrder[1:]
		delete(s.seen, oldest)
	}

	g.consumers[cid] = e.CsvLineOffset
	log.Printf("LOG append event_id=%s log_offset=%d", e.EventId, off)
	// Every new append gets a replication gen, even with no followers
	// connected yet: with -sync-replicas, waitReplicated then withholds the
	// ack until followers exist and have applied it (a reconnecting follower
	// catches the event up via its checkpoint replay).
	gen := s.nextGenLocked()
	s.enqueueLocked(gen, "a", e, "", "", off, 0)
	return e.CsvLineOffset, false, gen, nil
}

// waitReplicated blocks until at least syncWrites connected followers have
// applied the mutation with gen (or no followers can be gathered in time),
// so the caller only acks a producer once the event is replicated. Returns
// immediately when sync replication is off or gen is 0 (nothing to wait for).
func (s *Server) waitReplicated(ctx context.Context, gen int64) error {
	if s.syncWrites <= 0 || gen == 0 {
		return nil
	}
	deadline := time.Now().Add(waitReplicationTimeout)
	for {
		s.mu.Lock()
		applied, connected := int64(0), int64(0)
		for _, f := range s.followers {
			connected++
			if f.synced.Load() && f.appliedGen.Load() >= gen {
				applied++
			}
		}
		s.mu.Unlock()
		if applied >= int64(s.syncWrites) {
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("sync replication: waited %s for %d/%d followers to apply gen %d (connected=%d)",
				waitReplicationTimeout, applied, s.syncWrites, gen, connected)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(2 * time.Millisecond):
		}
	}
}

func dedupNote(duplicate bool) string {
	if duplicate {
		return "duplicate; already stored"
	}
	return ""
}

// GetOffset returns the last acknowledged offset for a consumer within its
// group (exists=false when nothing is known yet, i.e. a fresh consumer that
// must start at -1).
//
// A stored cursor that is at or past the log tail cannot be honoured: it was
// committed against a log incarnation that no longer exists (the leader that
// held it was lost, and the promoted survivor's log is shorter). Serving it
// verbatim would hand the consumer an offset it can never reach, so instead
// this re-anchors onto the retained head and reports it. Re-anchoring re-reads
// events the consumer may already have ingested, which is safe because
// downstream deduplicates by event_id, whereas the alternative - hanging on an
// unreachable cursor - loses every future event.
func (s *Server) GetOffset(_ context.Context, req *mqpb.GetOffsetRequest) (*mqpb.GetOffsetResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	group := normGroup(req.Group)
	g, ok := s.groups[group]
	if !ok {
		return &mqpb.GetOffsetResponse{Exists: false}, nil
	}
	off, ok := g.consumers[req.ConsumerId]
	if !ok {
		return &mqpb.GetOffsetResponse{Exists: false}, nil
	}
	if group != collectorGroup {
		// Only the collector group holds log cursors. A streamer-group cursor
		// is the producer's CSV source line, which is an unrelated numbering
		// space: comparing it to the log tail would spuriously rewind healthy
		// publishers, and they would re-send CSV lines the log already has.
		return &mqpb.GetOffsetResponse{Offset: off, Exists: true}, nil
	}
	tail := s.store.Tail()
	if off >= tail {
		head := s.storeBase()
		anchor := head - 1 // the offset just before the first retained event
		logf("CURSOR RE-ANCHOR group=%s consumer=%s committed=%d tail=%d head=%d: "+
			"committed cursor belongs to a log incarnation this node no longer has; "+
			"restarting from the retained head", group, req.ConsumerId, off, tail, head)
		g.consumers[req.ConsumerId] = anchor
		if ds, d := s.store.(DurableStore); d {
			// Rewind, not Commit: Commit is monotonic and would ignore a
			// backwards move, leaving the durable cursor unreachable across a
			// restart and re-triggering this rewind forever.
			if err := ds.Rewind(group, req.ConsumerId, anchor); err != nil {
				// The in-memory cursor is already repaired, so this consumer
				// makes progress now; the durable value converges on the next
				// restart.
				logf("CURSOR RE-ANCHOR group=%s consumer=%s durable rewind failed: %v",
					group, req.ConsumerId, err)
			}
		}
		return &mqpb.GetOffsetResponse{Offset: anchor, Exists: true}, nil
	}
	return &mqpb.GetOffsetResponse{Offset: off, Exists: true}, nil
}

// JoinPartition registers/heartbeats a consumer with its group's registry and
// returns the assigned (index, total) among that group's live consumers.
func (s *Server) JoinPartition(_ context.Context, req *mqpb.JoinPartitionRequest) (*mqpb.JoinPartitionResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	group := normGroup(req.Group)
	g := s.groupLocked(group)
	idx, total := g.reg.Join(req.ConsumerId, time.Duration(req.TtlSeconds)*time.Second)
	log.Printf("registry join group=%s consumer=%s index=%d/%d", group, req.ConsumerId, idx, total)
	return &mqpb.JoinPartitionResponse{Index: int32(idx), Total: int32(total)}, nil
}

// LeavePartition removes a consumer from its group's registry immediately.
func (s *Server) LeavePartition(_ context.Context, req *mqpb.LeavePartitionRequest) (*mqpb.LeavePartitionResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	group := normGroup(req.Group)
	g := s.groupLocked(group)
	g.reg.Leave(req.ConsumerId)
	log.Printf("registry leave group=%s consumer=%s (active=%d)", group, req.ConsumerId, g.reg.Count())
	return &mqpb.LeavePartitionResponse{}, nil
}

// Consume streams every event with offset > start_offset, in log order, up to
// the current tail, then closes the stream. It snapshots the log at call time:
// events appended while streaming are delivered on the consumer's next Consume.
//
// A start offset at or past the tail means the consumer committed against a log
// incarnation this node no longer has, which happens on every failover where
// the survivor's log is shorter than the cursor. That is reported as
// OutOfRange rather than an empty snapshot: an empty snapshot looks identical to
// "caught up", and the consumer would then poll forever, ingesting nothing and
// reporting healthy. OutOfRange tells it to re-read GetOffset, which re-anchors
// onto the retained head.
func (s *Server) Consume(req *mqpb.ConsumeRequest, stream grpc.ServerStreamingServer[mqpb.ConsumedEvent]) error {
	entries, err := s.store.Range(req.StartOffset)
	if err != nil {
		if errors.Is(err, ErrCursorAheadOfTail) {
			logf("CONSUME OUT-OF-RANGE start=%d tail=%d head=%d: committed cursor is not in "+
				"this log incarnation; consumer must re-anchor via GetOffset",
				req.StartOffset, s.store.Tail(), s.storeBase())
			return status.Errorf(codes.OutOfRange,
				"start offset %d is at or past the log tail %d: this cursor was committed "+
					"against a log incarnation this node no longer has; re-read GetOffset",
				req.StartOffset, s.store.Tail())
		}
		return err
	}
	for _, le := range entries {
		if err := stream.Send(&mqpb.ConsumedEvent{Event: le.Event, Offset: le.Offset}); err != nil {
			return err
		}
	}
	return nil
}

// CommitOffset records a consumer's processed position in its group (monotonic:
// a stale/out-of-order commit never regresses the cursor). The cursor is made
// durable first and only then applied to memory, so a crash after the RPC
// reply cannot lose it. Cursor movement in the collector group drives the log
// retention barrier.
func (s *Server) CommitOffset(_ context.Context, req *mqpb.CommitOffsetRequest) (*mqpb.CommitOffsetResponse, error) {
	if !s.IsLeader() {
		return nil, status.Error(codes.FailedPrecondition, FenceError("").Error())
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	group := normGroup(req.Group)
	g := s.groupLocked(group)
	if req.Offset <= g.consumers[req.ConsumerId] {
		return &mqpb.CommitOffsetResponse{}, nil
	}
	if ds, ok := s.store.(DurableStore); ok {
		if err := ds.Commit(group, req.ConsumerId, req.Offset); err != nil {
			return nil, err
		}
	}
	g.consumers[req.ConsumerId] = req.Offset
	s.enqueueLocked(s.nextGenLocked(), "c", nil, group, req.ConsumerId, req.Offset, 0)
	s.trimLocked()
	return &mqpb.CommitOffsetResponse{}, nil
}

// trimLocked advances the retention barrier: once every collector-group
// consumer has durably handled an offset, events at or below it are dropped.
// A collector that has joined but never committed blocks trimming entirely
// (it still needs everything from the head). No collectors -> no trimming.
// Callers must hold s.mu.
func (s *Server) trimLocked() {
	g, ok := s.groups[collectorGroup]
	if !ok || len(g.reg.Active()) == 0 {
		return
	}
	for _, id := range g.reg.Active() {
		if _, ok := g.consumers[id]; !ok {
			return // active collector with no cursor yet: retain everything
		}
	}
	floor := int64(math.MaxInt64)
	for _, c := range g.consumers {
		if c < floor {
			floor = c
		}
	}
	if floor == math.MaxInt64 {
		return
	}
	before := s.store.Tail()
	s.store.Trim(floor)
	s.enqueueLocked(s.nextGenLocked(), "t", nil, "", "", 0, floor)
	log.Printf("retention trim log_offset<=%d (log %d->%d)", floor, before, s.store.Tail())
}

// ActiveConsumers returns every live consumer across all groups (sorted); it
// is exposed for observability/tests.
func (s *Server) ActiveConsumers() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	var all []string
	for _, g := range s.groups {
		all = append(all, g.reg.Active()...)
	}
	sort.Strings(all)
	return all
}

func jsonEvent(e *mqpb.Event) string {
	out := &eventJSON{
		EventID:         e.EventId,
		SourceTimestamp: e.SourceTimestamp,
		MetricName:      e.MetricName,
		GPUIndex:        e.GpuIndex,
		DeviceName:      e.DeviceName,
		DeviceID:        e.DeviceId,
		ModelName:       e.ModelName,
		Hostname:        e.Hostname,
		Value:           e.Value,
		CSVLineOffset:   e.CsvLineOffset,
		LoopCount:       e.LoopCount,
		Tags:            e.Tags,
	}
	b, _ := json.Marshal(out)
	return string(b)
}
