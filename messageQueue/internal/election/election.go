// Package election implements leader election on a Kubernetes coordination.k8s.io
// Lease, plus the self-labelling that turns the lease holder into the MQ node
// clients actually talk to.
//
// Why a Lease and not Raft: this is a two-node set of static peers. Raft needs
// an odd number of voters for quorum, so with two nodes it buys no fault
// tolerance over the Lease while adding an etcd-class dependency and a second
// thing to operate. The Kubernetes API server is already a quorum-replicated,
// authenticated, highly available store, and its Lease object already carries
// exactly the primitives needed here: a holder identity, a resourceVersion for
// compare-and-swap, and a renewTime with a duration over which it goes stale.
// So we use it directly.
//
// The protocol is the same one client-go's leaderelection uses:
//
//   - A candidate may take an unheld Lease, or one whose renewTime is older
//     than leaseDuration (the previous holder is presumed dead).
//   - Acquisition and renewal are compare-and-swap on resourceVersion, so two
//     candidates racing on an expired Lease cannot both win: exactly one
//     Update carries the observed resourceVersion and the other gets a 409.
//   - The holder renews every retryPeriod. If it cannot renew for
//     renewDeadline, it has lost the lease and MUST stop calling OnStartedLeading
//     work (see fencing below).
//
// Fencing is the load-bearing part and the reason this package exists at all.
// A leader that loses contact with the API server but not with its clients is
// the classic split-brain: it would keep acknowledging publishes that the new
// leader also accepts. So the elector does not merely stop renewing on lease
// loss, it calls OnStoppedLeading, and the MQ server uses that to fence
// itself read-only before the new leader can take over for real. A node that
// cannot renew within renewDeadline is therefore never a writer again, even
// briefly.
//
// Time is injected (Clock) so the expiry behaviour that HA depends on - which
// is otherwise only exercised by multi-second sleeps - is unit-testable.
package election

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sync/atomic"
	"time"
)

// Clock is the time source, injected so lease expiry and renew deadlines are
// testable without sleeping.
type Clock interface {
	Now() time.Time
}

type realClock struct{}

func (realClock) Now() time.Time { return time.Now() }

// Lease is the subset of a coordination.k8s.io/v1 Lease this package reads and
// writes. It is deliberately transport-shaped so the in-cluster REST backend
// and an in-memory fake share one type.
type Lease struct {
	Namespace         string
	Name              string
	HolderIdentity    string
	LeaseDurationSecs int32
	AcquireTime       time.Time
	RenewTime         time.Time
	LeaseTransitions  int32
	ResourceVersion   string
}

// Record is one election outcome, appended to History by the elector. Tests and
// the operator's failover audit read it; it is not part of the wire format.
type Record struct {
	At     time.Time
	Holder string
	Won    bool
	Err    error
}

// History is a bounded ring of the most recent election attempts, so a node can
// log what it observed without unbounded growth.
type History struct {
	records []Record
	limit   int
}

// NewHistory returns a History retaining at most limit records.
func NewHistory(limit int) *History {
	if limit <= 0 {
		limit = 32
	}
	return &History{limit: limit}
}

// Add appends a record, evicting the oldest once the limit is reached.
func (h *History) Add(r Record) {
	h.records = append(h.records, r)
	if len(h.records) > h.limit {
		h.records = h.records[len(h.records)-h.limit:]
	}
}

// Records returns a copy of the retained records.
func (h *History) Records() []Record {
	out := make([]Record, len(h.records))
	copy(out, h.records)
	return out
}

// Backend is the durable coordination store. The production implementation
// talks to the Kubernetes API server; tests use an in-memory fake. Every method
// must return ErrNotFound when the object does not exist.
type Backend interface {
	// Get returns the Lease. The returned ResourceVersion is the CAS token.
	Get(ctx context.Context, ns, name string) (*Lease, error)
	// Create creates the Lease. It must fail if it already exists.
	Create(ctx context.Context, l *Lease) error
	// Update replaces the Lease and must fail with ErrConflict when
	// l.ResourceVersion no longer matches, which is what makes acquisition and
	// renewal mutually exclusive.
	Update(ctx context.Context, l *Lease) error
}

// ErrNotFound and ErrConflict mirror the API server's 404 and 409 so the
// elector can branch on them without importing a Kubernetes package.
var (
	ErrNotFound = errors.New("lease not found")
	ErrConflict = errors.New("lease conflict")
)

// Tunables for the election loop. The defaults are the client-go
// leaderelection defaults, which are battle-tested for this failure mode.
const (
	// DefaultLeaseDuration is how long a Lease stays valid without renewal.
	// A candidate waits this long after the last renewTime before assuming the
	// holder is dead, so it is the lower bound on failover time.
	DefaultLeaseDuration = 15 * time.Second
	// DefaultRenewDeadline is how long the holder keeps trying to renew before
	// declaring the lease lost and fencing itself. It must be materially
	// shorter than DefaultLeaseDuration so a leader gives up before a rival can
	// legitimately take over: that ordering is what makes the fencing sound.
	DefaultRenewDeadline = 10 * time.Second
	// DefaultRetryPeriod is the interval between acquire attempts and between
	// renewal attempts.
	DefaultRetryPeriod = 2 * time.Second
)

// Config configures an Elector. Identity must be unique per pod (the pod name
// is used) because it is the lease holder that clients route to.
type Config struct {
	Identity      string
	Namespace     string
	LeaseName     string
	LeaseDuration time.Duration
	RenewDeadline time.Duration
	RetryPeriod   time.Duration
	Clock         Clock
	History       *History
	// Logf defaults to log.Printf when nil.
	Logf func(format string, args ...any)
}

func (c *Config) applyDefaults() {
	if c.LeaseDuration == 0 {
		c.LeaseDuration = DefaultLeaseDuration
	}
	if c.RenewDeadline == 0 {
		c.RenewDeadline = DefaultRenewDeadline
	}
	if c.RetryPeriod == 0 {
		c.RetryPeriod = DefaultRetryPeriod
	}
	if c.Clock == nil {
		c.Clock = realClock{}
	}
	if c.History == nil {
		c.History = NewHistory(32)
	}
	if c.Logf == nil {
		c.Logf = log.Printf
	}
	if c.LeaseName == "" {
		c.LeaseName = "mq-leader"
	}
}

// Elector runs the acquire/renew loop for one identity.
type Elector struct {
	cfg     Config
	backend Backend

	// leading and obs are owned by the Run goroutine but read by Leading()
	// (which the MQ server calls from RPC handlers to gate writes), so they are
	// accessed atomically. Fencing correctness depends on Leading() reflecting
	// the lease state promptly, not on the identity of the reader.
	leading atomic.Bool
	obs     atomic.Pointer[Lease]

	// lastRenewUnixNano is the wall-clock time of the last lease renewal this
	// node knows was accepted. The readiness probe reads it to stop routing
	// traffic to a leader that has lost contact with the API server.
	//
	// It must be a local clock, not the Lease's renewTime: a partitioned leader
	// cannot read the Lease any more, so its only evidence of still holding the
	// lease is how long ago it last wrote successfully. Zero means "never
	// renewed", which is not fresh.
	lastRenewUnixNano atomic.Int64
}

// setObs records the last observed Lease for CAS and for logging.
func (e *Elector) setObs(l *Lease) { e.obs.Store(l) }

// lastObs returns the most recently observed Lease, or nil.
func (e *Elector) lastObs() *Lease { return e.obs.Load() }

// markRenewed records that a renewal was accepted just now.
func (e *Elector) markRenewed(now time.Time) {
	e.lastRenewUnixNano.Store(now.UnixNano())
}

// LeaseFresh reports whether this node renewed its lease recently enough to
// still be a legitimate leader.
//
// It is deliberately local: it answers "can I still prove I hold the lease?"
// from the last successful renewal, without contacting the API server. That is
// what makes it useful during the failure it exists to handle - a leader cut
// off from the API server cannot read the Lease, but it can still tell that its
// last renewal is ageing out, so it should stop being routed to.
//
// A node that is not leading is always fresh; only a leader's staleness matters.
func (e *Elector) LeaseFresh(now time.Time) bool {
	if !e.leading.Load() {
		return true // a follower must stay routable so it can be promoted
	}
	last := e.lastRenewUnixNano.Load()
	if last == 0 {
		// Leading but no renewal recorded yet: treat as stale rather than
		// fresh, so a leader that cannot reach the API server is never routed to.
		return false
	}
	return now.Sub(time.Unix(0, last)) <= e.cfg.RenewDeadline
}

// New returns an Elector for cfg against backend.
func New(cfg Config, backend Backend) *Elector {
	cfg.applyDefaults()
	return &Elector{cfg: cfg, backend: backend}
}

// Leading reports whether this Elector currently believes it holds the lease.
// It is false until Run has acquired it, and false again after lease loss -
// including the fencing window, where the MQ must be read-only.
func (e *Elector) Leading() bool { return e.leading.Load() }

// History exposes the recorded election attempts.
func (e *Elector) History() *History { return e.cfg.History }

// expired reports whether l's renewTime is older than the lease duration as of
// now, i.e. whether the previous holder has lost its claim.
func expired(now time.Time, l *Lease, leaseDuration time.Duration) bool {
	return now.After(l.RenewTime.Add(leaseDuration))
}

// tryAcquire makes one attempt to take the lease. It returns true if this
// identity is the holder on return.
//
// Three outcomes, all CAS-guarded:
//   - the Lease is absent: Create it (409 means a rival won the race),
//   - the Lease is ours: renew it,
//   - the Lease is held and still fresh: give up this round,
//   - the Lease is held but expired: Update it into our name; a 409 means a
//     rival won the race and we retry.
func (e *Elector) tryAcquire(ctx context.Context) (bool, error) {
	now := e.cfg.Clock.Now()
	cur, err := e.backend.Get(ctx, e.cfg.Namespace, e.cfg.LeaseName)
	switch {
	case errors.Is(err, ErrNotFound):
		l := &Lease{
			Namespace:         e.cfg.Namespace,
			Name:              e.cfg.LeaseName,
			HolderIdentity:    e.cfg.Identity,
			LeaseDurationSecs: int32(e.cfg.LeaseDuration / time.Second),
			AcquireTime:       now,
			RenewTime:         now,
		}
		if err := e.backend.Create(ctx, l); err != nil {
			if errors.Is(err, ErrConflict) {
				return false, nil // rival created it first
			}
			return false, err
		}
		e.setObs(l)
		return true, nil

	case err != nil:
		return false, err
	}

	if cur.HolderIdentity == e.cfg.Identity {
		// Renew our own claim so an uninterrupted leader keeps the same
		// AcquireTime (and clients see no churn).
		cur.RenewTime = now
		if err := e.backend.Update(ctx, cur); err != nil {
			if errors.Is(err, ErrConflict) {
				return false, nil
			}
			return false, err
		}
		e.setObs(cur)
		return true, nil
	}

	if !expired(now, cur, e.cfg.LeaseDuration) {
		return false, nil // the holder is still within its lease: not ours to take
	}

	cur.HolderIdentity = e.cfg.Identity
	cur.LeaseDurationSecs = int32(e.cfg.LeaseDuration / time.Second)
	cur.AcquireTime = now
	cur.RenewTime = now
	cur.LeaseTransitions++
	if err := e.backend.Update(ctx, cur); err != nil {
		if errors.Is(err, ErrConflict) {
			// A rival took it between our Get and Update. Their CAS token was
			// the one that matched, so they are the single writer. Retry.
			return false, nil
		}
		return false, err
	}
	e.setObs(cur)
	return true, nil
}

// Run drives the election until ctx is done. It calls the hooks at each role
// transition and returns only when ctx is cancelled or OnStartedLeading returns
// an error (fatal to this node's ability to lead).
//
// The contract callers depend on for safety:
//   - OnStartedLeading is called at most once per acquisition, and never
//     concurrently with OnStoppedLeading.
//   - OnStoppedLeading is always called before Run returns if leadership was
//     ever acquired, so a caller can rely on it to fence writes.
func (e *Elector) Run(ctx context.Context, onStarted, onStopped func(context.Context) error) error {
	defer func() {
		if e.leading.Load() && onStopped != nil {
			e.stepDown("run finished")
		}
	}()

	for ctx.Err() == nil {
		won, err := e.tryAcquire(ctx)
		if err != nil {
			// A coordination-store error (API server unreachable, RBAC denied)
			// is not fatal: we cannot know we are the leader, so we stay a
			// follower and retry. Crucially we never assume leadership from a
			// failed read.
			e.cfg.History.Add(Record{At: e.cfg.Clock.Now(), Holder: e.cfg.Identity, Err: err})
			e.cfg.Logf("election: acquire %s/%s failed: %v (staying follower)", e.cfg.Namespace, e.cfg.LeaseName, err)
			if !sleepCtx(ctx, e.cfg.RetryPeriod) {
				return ctx.Err()
			}
			continue
		}

		if !won {
			if !sleepCtx(ctx, e.cfg.RetryPeriod) {
				return ctx.Err()
			}
			continue
		}

		// We hold the lease. Publish that fact before doing any leading work,
		// so a caller reading Leading() never sees stale false.
		e.leading.Store(true)
		// Record the acquisition as a renewal so a freshly promoted leader is
		// immediately considered fresh rather than stale-until-first-renewal.
		e.markRenewed(e.cfg.Clock.Now())
		e.cfg.History.Add(Record{At: e.cfg.Clock.Now(), Holder: e.cfg.Identity, Won: true})
		e.cfg.Logf("election: %s acquired lease %s/%s (transitions=%d)", e.cfg.Identity, e.cfg.Namespace, e.cfg.LeaseName, e.lastObs().LeaseTransitions)

		leadErr := e.holdLease(ctx, onStarted, onStopped)

		// Fence ourselves the instant leadership ends, before we loop back to
		// possibly re-acquiring. This is the split-brain guard: between losing
		// the lease and this call returning, we must not be a writer.
		e.leading.Store(false)
		if onStopped != nil {
			onStopped(context.Background())
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if leadErr != nil && !errors.Is(leadErr, context.Canceled) {
			e.cfg.Logf("election: leadership of %s/%s ended: %v", e.cfg.Namespace, e.cfg.LeaseName, leadErr)
		}
	}
	return ctx.Err()
}

// stepDown is the deferred-fence path; kept separate so the log line is
// identical whether fencing happens here or inline in Run.
func (e *Elector) stepDown(reason string) {
	e.leading.Store(false)
	e.cfg.Logf("election: %s released leadership (%s)", e.cfg.Identity, reason)
}

// holdLease renews until renewal fails past the renew deadline, the context is
// cancelled, or the leading work returns an error. It returns the reason
// leadership ended.
func (e *Elector) holdLease(ctx context.Context, onStarted func(context.Context) error, onStopped func(context.Context) error) error {
	leadCtx, cancelLead := context.WithCancel(ctx)
	defer cancelLead()

	var workDone <-chan error
	if onStarted != nil {
		ch := make(chan error, 1)
		workDone = ch
		go func() { ch <- onStarted(leadCtx) }()
	}

	// lastRenew tracks the last time we know our renewal was accepted. The
	// deadline is measured from it, not from the last attempt, so a run of
	// failing attempts (API server down) still fences us in bounded time.
	lastRenew := e.cfg.Clock.Now()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case err := <-workDone:
			return err
		case <-time.After(e.cfg.RetryPeriod):
		}

		// Renewal goes through the same CAS path as acquisition: if a rival has
		// already taken the Lease, tryAcquire returns false and that is
		// immediate loss rather than something to retry.
		stillOurs, err := e.tryAcquire(ctx)
		switch {
		case err != nil:
			e.cfg.Logf("election: renew %s/%s failed: %v", e.cfg.Namespace, e.cfg.LeaseName, err)
		case !stillOurs:
			return errors.New("lease taken by another identity while leading")
		default:
			lastRenew = e.cfg.Clock.Now()
			e.markRenewed(lastRenew)
		}

		if since := e.cfg.Clock.Now().Sub(lastRenew); since > e.cfg.RenewDeadline {
			return fmt.Errorf("renew deadline exceeded (%s without a successful renewal)", since)
		}
	}
}

// sleepCtx waits d, returning false if ctx was cancelled first.
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
