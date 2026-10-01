package mq

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"
)

// Role is the node's position in the leader/follower pair.
type Role int32

const (
	// RoleFollower serves reads from its local log and rejects writes. A
	// follower is the safe default: a node that has not proven it holds the
	// lease must never ack a publisher.
	RoleFollower Role = iota
	// RoleLeader accepts writes and replicates them to followers.
	RoleLeader
)

func (r Role) String() string {
	if r == RoleLeader {
		return "leader"
	}
	return "follower"
}

// term is the leadership generation. It increments on every promotion so an
// operator can see a failover happened, and so replicated state from a
// previous term is distinguishable from the current one.
type termState struct {
	role atomic.Int32
	gen  atomic.Int64
}

// HA is the HA controller for one mq.Server.
//
// It owns the node's role and is driven by the election elector: the elector
// decides *when* this node may lead, and the HA controller performs the actual
// transition on the Server (fencing writes, or opening them after a promotion).
//
// The two responsibilities are separated deliberately. The elector knows about
// leases and time; it knows nothing about the event log. The Server knows about
// the log; it knows nothing about leases. The only contract between them is
// that Promote/Demote are called on election transitions, and that writes are
// permitted only while role == RoleLeader.
//
// Why role lives on the Server and not only in the elector: a fenced old leader
// may be partitioned from the API server while still perfectly able to serve
// clients. If "am I allowed to write" were answered by asking the elector, that
// node would either keep answering yes (split brain) or block on an API call in
// the publish hot path. Instead the role is a local atomic that the elector's
// OnStoppedLeading callback clears, so fencing is immediate and local.
type HA struct {
	srv   *Server
	state *termState
}

// NewHA returns a controller in RoleFollower. The safe default matters: a
// node only becomes a writer after it has won the lease.
func NewHA(s *Server) *HA {
	return &HA{srv: s, state: &termState{}}
}

// Promote makes this node the leader in place, without a restart.
//
// This is the failover path, and it has to be cheap and lossless. The node is a
// follower that has been applying the leader's log, so at this moment its log,
// cursors and dedup window are already current: promotion is a permission flip,
// not a data copy. That is why this can happen inside a running process and why
// clients keep their established offsets across a failover.
func (l *HA) Promote(syncWrites int) error {
	if _, ok := l.srv.store.(DurableStore); !ok {
		// Promoting an in-memory node would ack writes that vanish on restart,
		// which is strictly worse than refusing to promote.
		return fmt.Errorf("refusing to promote without a durable store: an acked event must survive restart")
	}

	// The follower may have missed the final frames of the old leader's term
	// (it learns of the loss only when its Sync stream drops). Re-seed from the
	// durable state so the dedup window and cursors are exactly what a restart
	// would produce, then reconcile with anything the store holds.
	l.srv.mu.Lock()
	l.srv.seen = make(map[string]struct{}, len(l.srv.seen))
	l.srv.seenOrder = nil
	l.srv.seedLocked()
	l.srv.mu.Unlock()

	l.srv.role.Store(int32(RoleLeader))
	l.srv.EnableSyncReplication(syncWrites)
	gen := l.state.gen.Add(1)
	l.srv.logRole(RoleLeader, gen)
	return nil
}

// Demote fences this node back to read-only. It is called when the lease is
// lost or the process steps down, and it must be safe to call repeatedly and at
// any time, including while RPCs are in flight.
//
// Ordering matters: the role is cleared *before* anything else, so a
// concurrent PublishEvents either already passed the check (and is completing
// against the log we still hold) or will be rejected. There is no window in
// which a new write is accepted under a lost lease.
func (l *HA) Demote() {
	l.srv.role.Store(int32(RoleFollower))
	// Stop gating acks on followers: this node is not the leader, so the
	// sync-replication setting is meaningless here and must not leak into a
	// later promotion of a different node.
	l.srv.setSyncWrites(0)
	l.srv.logRole(RoleFollower, l.state.gen.Load())
}

// Role reports the current role.
func (l *HA) Role() Role { return Role(l.srv.role.Load()) }

// Term returns how many times this node has been promoted. Zero means it has
// never led, which is how an operator tells a fresh node from a survivor of a
// real failover.
func (l *HA) Term() int64 { return l.state.gen.Load() }

// IsLeader reports whether writes are currently permitted.
func (l *HA) IsLeader() bool { return l.Role() == RoleLeader }

// FenceError is returned to a client that tried to write to a node that is not
// (or is no longer) the leader. It is deliberately a distinct code from a
// generic error so a client can tell "retry somewhere else" from "the MQ is
// broken", and it names the current holder so an operator can see where writes
// are actually going.
func FenceError(holder string) error {
	if holder == "" {
		return fmt.Errorf("node is not the leader (no lease holder)")
	}
	return fmt.Errorf("node is not the leader; writes are served by %q", holder)
}

// followerLoop runs the follower side of replication as a restartable unit, so
// a node can replicate from a leader, promote, and later demote and replicate
// again - all without a restart. That matters during a flapping failover: a pod
// that was briefly the leader and then lost the lease must resume following
// rather than sit idle believing it is still in charge.
type followerLoop struct {
	srv     *Server
	leader  LeaderResolver
	opts    []FollowerOption
	leader_ atomic.Pointer[string]
}

// LeaderResolver returns the current leader address to replicate from. It is a
// function so the address can be re-resolved on every reconnect instead of being
// captured once at startup.
type LeaderResolver func(ctx context.Context) (string, error)

// NewFollowerLoop returns a loop that follows whatever leader LeaderResolver
// reports at the time of each connection attempt.
func NewFollowerLoop(s *Server, resolve LeaderResolver, opts ...FollowerOption) *followerLoop {
	return &followerLoop{srv: s, leader: resolve, opts: opts}
}

// Run follows the current leader until ctx is cancelled, re-resolving the
// address after every interruption. Re-resolution is what makes failover
// transparent: once the lease moves, the next reconnect targets the new leader
// without any configuration change.
func (f *followerLoop) Run(ctx context.Context) error {
	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		// A node that has been promoted must not keep pulling from a leader:
		// it would apply frames it already applied itself and, worse, could
		// rewind its own log. Check before each connect.
		if f.srv.role.Load() == int32(RoleLeader) {
			return nil
		}
		addr, err := f.leader(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			if !sleepCtx(ctx, retryPause) {
				return ctx.Err()
			}
			continue
		}
		held := f.leader_.Swap(&addr)
		if held == nil || *held != addr {
			f.srv.logFollowing(addr)
		}
		// followOnce blocks for the life of one Sync stream and returns when it
		// breaks; the outer loop re-resolves and reconnects.
		err = f.srv.FollowOnce(ctx, addr, f.opts...)
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if err != nil {
			f.srv.logFollowInterrupted(addr, err)
		}
		if !sleepCtx(ctx, retryPause) {
			return ctx.Err()
		}
	}
}

// retryPause is the pause between replication reconnect attempts. It matches
// the previous fixed backoff so failover timing is unchanged: a node that
// re-resolves every retryPause converges on the new leader within about one
// pause of the lease moving.
const retryPause = 500 * time.Millisecond

// sleepCtx waits d or returns false if ctx ends first.
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

// logRole emits one prominent line per role change. Failover is an event an
// operator must be able to find in logs without turning on debug logging.
func (s *Server) logRole(r Role, gen int64) {
	if r == RoleLeader {
		logf("PROMOTED to leader (term %d): accepting publishes; log base=%d tail=%d",
			gen, s.storeBase(), s.store.Tail())
		return
	}
	logf("DEMOTED to follower (term %d): writes fenced; reads still served from local log", gen)
}

// logFollowing reports the leader address a follower is now replicating from.
func (s *Server) logFollowing(addr string) {
	logf("replicating from leader %s", addr)
}

// logFollowInterrupted reports a dropped replication stream. Like the publish
// path, replication failures are expected during a failover and should be
// visible but not alarming.
func (s *Server) logFollowInterrupted(addr string, err error) {
	logf("replication from %s interrupted (%v); will re-resolve and retry", addr, err)
}
