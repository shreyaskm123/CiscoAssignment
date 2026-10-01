// Leader-follower replication (see streamer/proto/mq.proto: Replication).
//
// The leader replicates every log mutation to connected followers and only
// acks a publisher once syncWrites followers have applied it (-> an acked
// event is on a quorum and survives a leader crash). What is NOT replicated:
//
//   - the per-event dedup window (advisory; empty on a promoted node, and the
//     ClickHouse ReplacingMergeTree absorbs the resulting duplicates), and
//   - the partition registry (ephemeral per-node state; it self-heals because
//     streamers/collectors re-join within their heartbeat TTL).
//
// What IS replicated is everything the log needs to fail over losslessly:
// appends, commit cursors and retention trims, plus an initial checkpoint
// (base + every cursor) so a connecting follower converges fast even if it was
// briefly offline.
//
// A follower keeps its own durable WAL (it must run with -data-dir) and serves
// reads locally from it, but rejects the mutating RPCs.
package mq

import (
	"context"
	"fmt"
	"log"
	"sync/atomic"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"messagequeue/internal/auth"
	mqpb "streamer/proto"
)

// followerBuf bounds the queued live frames for one follower before its sender
// drops the connection (the follower then reconnects and re-syncs from its own
// watermark, so no frame is ever lost - only delayed).
const followerBuf = 5000

// frameMsg is one pending replication frame queued for a follower.
type frameMsg struct {
	gen      int64
	ty       string // "a" | "c" | "t"
	event    *mqpb.Event
	group    string
	consumer string
	offset   int64
	floor    int64
}

// follower is the leader-side state for one connected Sync stream.
type follower struct {
	id         string
	ch         chan frameMsg
	lastSent   int64        // highest log offset already sent (dup guard)
	appliedGen atomic.Int64 // highest gen the follower acked
	synced     atomic.Bool  // caught up past the initial replay (live phase)
	saturated  atomic.Bool  // its queue overflowed -> reconnect/resync
}

// nextGenLocked returns the next replication gen. Callers must hold s.mu.
func (s *Server) nextGenLocked() int64 {
	s.gen++
	return s.gen
}

// enqueueLocked queues a replication frame (gen already assigned) to every
// connected follower. Callers must hold s.mu (it touches s.followers). A slow
// follower is flagged saturated instead of blocking the hot path; its sender
// drops the connection and the follower re-syncs from its own watermark on
// reconnect, so no frame is lost - only delayed.
func (s *Server) enqueueLocked(gen int64, ty string, e *mqpb.Event, group, consumer string, offset, floor int64) {
	if len(s.followers) == 0 {
		return
	}
	msg := frameMsg{gen: gen, ty: ty, event: e, group: group, consumer: consumer, offset: offset, floor: floor}
	for _, f := range s.followers {
		select {
		case f.ch <- msg:
		default:
			f.saturated.Store(true)
		}
	}
}

func (s *Server) dropFollower(f *follower) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if cur, ok := s.followers[f.id]; ok && cur == f {
		delete(s.followers, f.id)
		close(f.ch)
		log.Printf("replication follower %s disconnected", f.id)
	}
}

// Sync is the leader side of the Replication service (implemented only when
// this node runs as a leader; a follower still advertises the service so it
// can be promoted in place by restarting without -leader).
//
// The stream protocol is: the follower sends one hello (ReplicaAck with its
// current log offset) and the leader answers with a checkpoint ("x") plus a
// replay of the retained events the follower lacks, then the live mutation
// stream. The follower acks every applied frame with its gen so the leader can
// wait for syncWrites followers before acking publishers.
func (s *Server) Sync(stream mqpb.Replication_SyncServer) error {
	hello, err := stream.Recv()
	if err != nil {
		return err
	}

	s.mu.Lock()
	s.nextFid++
	f := &follower{id: fmt.Sprintf("f%d", s.nextFid), ch: make(chan frameMsg, followerBuf), lastSent: hello.ResumeLogOffset}
	if s.followers == nil {
		s.followers = make(map[string]*follower)
	}
	s.followers[f.id] = f
	s.gen++ // reserve one gen shared by the whole catch-up snapshot
	cpGen := s.gen
	s.mu.Unlock()
	log.Printf("replication follower %s connected (resume after log offset %d)", f.id, hello.ResumeLogOffset)

	// Checkpoint: base + every durable cursor.
	check := &mqpb.ReplicaCheckpoint{}
	if ds, ok := s.store.(DurableStore); ok {
		check.Base = ds.Base()
		for _, c := range ds.Cursors() {
			check.Cursors = append(check.Cursors, &mqpb.CursorStatus{Group: c.Group, Consumer: c.Consumer, Offset: c.Offset})
		}
	}
	if err := stream.Send(&mqpb.ReplicaFrame{Gen: cpGen, Type: "x", Check: check}); err != nil {
		s.dropFollower(f)
		return err
	}

	// Replay: retained events the follower does not have yet (Range returns
	// offsets > ResumeLogOffset, clamped to the retained head).
	//
	// A resume offset at or past our tail cannot be satisfied: the follower is
	// claiming to hold a log incarnation we no longer serve. Range reports that
	// as ErrCursorAheadOfTail, and returning it would close the stream and trap
	// the follower in a reconnect loop. Re-seed from our retained head instead,
	// which is also the only outcome that converges: the follower's offsets
	// beyond this point belong to a log the leader does not have.
	resume := hello.ResumeLogOffset
	if tail := s.store.Tail(); resume >= tail {
		// Re-seed from the retained head. Only a durable store can report its
		// base; without one, fall back to replaying the whole log.
		base := int64(0)
		if ds, ok := s.store.(DurableStore); ok {
			base = ds.Base()
			log.Printf("follower %s resume offset %d is past leader tail %d; re-seeding from base %d", f.id, resume, tail, base)
			resume = base - 1
		} else {
			log.Printf("follower %s resume offset %d is past leader tail %d; re-seeding from log head", f.id, resume, tail)
			resume = -1
		}
	}
	entries, err := s.store.Range(resume)
	if err != nil {
		s.dropFollower(f)
		return err
	}
	for _, le := range entries {
		if err := stream.Send(&mqpb.ReplicaFrame{Gen: cpGen, Type: "a", Event: le.Event}); err != nil {
			s.dropFollower(f)
			return err
		}
		if le.Offset > f.lastSent {
			f.lastSent = le.Offset
		}
	}
	f.synced.Store(true)
	log.Printf("replication follower %s caught up through log offset %d (live phase)", f.id, f.lastSent)

	// Fold the follower's applied-gen watermark in as acks arrive.
	go func() {
		for {
			ack, err := stream.Recv()
			if err != nil {
				return
			}
			if ack.Gen > f.appliedGen.Load() {
				f.appliedGen.Store(ack.Gen)
			}
		}
	}()

	// Live phase: this single writer preserves leader ordering
	// (append/commit/trim frames share the stream, so a follower applies them
	// in the exact order the leader did).
	for {
		if f.saturated.Load() {
			log.Printf("replication follower %s slow (queue overflow); dropping for resync", f.id)
			s.dropFollower(f)
			return nil
		}
		select {
		case msg, ok := <-f.ch:
			if !ok {
				s.dropFollower(f)
				return nil
			}
			if msg.ty == "a" && msg.offset <= f.lastSent {
				continue // duplicate of a frame already replayed
			}
			if err := stream.Send(frameFromMsg(msg)); err != nil {
				s.dropFollower(f)
				return err
			}
			if msg.ty == "a" && msg.offset > f.lastSent {
				f.lastSent = msg.offset
			}
		case <-stream.Context().Done():
			s.dropFollower(f)
			return nil
		}
	}
}

func frameFromMsg(m frameMsg) *mqpb.ReplicaFrame {
	fr := &mqpb.ReplicaFrame{Gen: m.gen, Type: m.ty}
	switch m.ty {
	case "a":
		fr.Event = m.event
	case "c":
		fr.Group, fr.Consumer, fr.Offset = m.group, m.consumer, m.offset
	case "t":
		fr.Floor = m.floor
	}
	return fr
}

// applyReplicatedFrame applies one frame from the leader durably (via the
// store) and folds it into the in-memory cursors so this node can serve reads
// (and, after promotion, writers) without a restart.
func (s *Server) applyReplicatedFrame(fr *mqpb.ReplicaFrame) error {
	switch fr.Type {
	case "x":
		if ds, ok := s.store.(DurableStore); ok {
			if err := ds.ApplyCheckpoint(fr.Check); err != nil {
				return err
			}
		}
		s.mu.Lock()
		for _, c := range fr.Check.Cursors {
			g := s.groupLocked(c.Group)
			if off, ok := g.consumers[c.Consumer]; !ok || c.Offset > off {
				g.consumers[c.Consumer] = c.Offset
			}
		}
		s.mu.Unlock()
	case "a":
		if _, err := s.store.Append(fr.Event); err != nil {
			return err
		}
		if pod := fr.Event.Tags["pod_name"]; pod != "" {
			s.mu.Lock()
			g := s.groupLocked(streamerGroup)
			if fr.Event.CsvLineOffset > g.consumers[pod] {
				g.consumers[pod] = fr.Event.CsvLineOffset
			}
			s.mu.Unlock()
		}
	case "c":
		if ds, ok := s.store.(DurableStore); ok {
			if err := ds.Commit(fr.Group, fr.Consumer, fr.Offset); err != nil {
				return err
			}
		}
		s.mu.Lock()
		g := s.groupLocked(normGroup(fr.Group))
		if fr.Offset > g.consumers[fr.Consumer] {
			g.consumers[fr.Consumer] = fr.Offset
		}
		s.mu.Unlock()
	case "t":
		s.store.Trim(fr.Floor)
	}
	return nil
}

// FollowerOption configures the follower's outbound connection to the leader.
type FollowerOption func(*followerConfig)

type followerConfig struct {
	token     string
	tokenFile string
}

// WithFollowerToken makes the follower present token on its replication stream.
// Replication is a registered gRPC service like any other, so a follower that
// does not authenticate is rejected once auth is enabled.
func WithFollowerToken(token string) FollowerOption {
	return func(c *followerConfig) { c.token = token }
}

// WithFollowerTokenFile re-reads the follower's token from path on every RPC, so
// rotation does not require restarting the pod.
func WithFollowerTokenFile(path string) FollowerOption {
	return func(c *followerConfig) { c.tokenFile = path }
}

// followerConfigFrom materialises the option set once per connection attempt.
func followerConfigFrom(opts []FollowerOption) followerConfig {
	var cfg followerConfig
	for _, o := range opts {
		o(&cfg)
	}
	return cfg
}

// RunFollower drives the follower side of replication until ctx is cancelled.
// It is retained for single-node-style deployments and tests; the HA deployment
// uses followerLoop (leader.go) instead, which re-resolves the leader address
// on every retry so a failover needs no reconfiguration.
func RunFollower(ctx context.Context, leaderAddr string, s *Server, opts ...FollowerOption) error {
	cfg := followerConfigFrom(opts)
	backoff := 500 * time.Millisecond
	for {
		err := s.followOnce(ctx, leaderAddr, cfg)
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if err != nil {
			logf("replication sync to leader %s interrupted (%v); retrying in %s", leaderAddr, err, backoff)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(backoff):
		}
		if backoff < 5*time.Second {
			backoff *= 2
		}
	}
}

// FollowOnce runs exactly one Sync stream to leaderAddr and returns when it
// breaks. It is exported-within-package so the HA follower loop can drive
// reconnection itself and decide, between attempts, whether this node should
// still be following at all.
func (s *Server) FollowOnce(ctx context.Context, leaderAddr string, opts ...FollowerOption) error {
	return s.followOnce(ctx, leaderAddr, followerConfigFrom(opts))
}

func (s *Server) followOnce(ctx context.Context, leaderAddr string, cfg followerConfig) error {
	dialOpts := []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())}
	if cfg.token != "" || cfg.tokenFile != "" {
		dialOpts = append(dialOpts, grpc.WithPerRPCCredentials(auth.TokenCredentials{
			Token:     cfg.token,
			TokenFile: cfg.tokenFile,
		}))
	}
	conn, err := grpc.NewClient(leaderAddr, dialOpts...)
	if err != nil {
		return err
	}
	defer conn.Close()
	client := mqpb.NewReplicationClient(conn)
	stream, err := client.Sync(ctx)
	if err != nil {
		return err
	}
	resume := s.store.Tail() - 1
	if err := stream.Send(&mqpb.ReplicaAck{ResumeLogOffset: resume}); err != nil {
		return err
	}
	logf("follower syncing to leader %s (resume after log offset %d)", leaderAddr, resume)
	for {
		fr, err := stream.Recv()
		if err != nil {
			return err
		}
		if err := s.applyReplicatedFrame(fr); err != nil {
			return err
		}
		if err := stream.Send(&mqpb.ReplicaAck{Gen: fr.Gen}); err != nil {
			return err
		}
	}
}
