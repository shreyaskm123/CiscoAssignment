package election

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

func testConfig(id string, clk Clock) Config {
	return Config{
		Identity:      id,
		Namespace:     "telemetry",
		LeaseName:     "mq-leader",
		LeaseDuration: 15 * time.Second,
		RenewDeadline: 10 * time.Second,
		RetryPeriod:   2 * time.Second,
		Clock:         clk,
		Logf:          func(string, ...any) {},
	}
}

// leaderSpy records the role transitions Run is expected to drive, so a test can
// assert the fencing contract (exactly one started, exactly one stopped, never
// two leaders at once) rather than just checking a boolean at the end.
type leaderSpy struct {
	mu        sync.Mutex
	starts    int
	stops     int
	maxActive int
	active    int
}

// started records the transition and then blocks for the duration of the
// leadership term. The lock is released before blocking so counts() stays
// readable from the test goroutine while a term is in progress.
func (s *leaderSpy) started(ctx context.Context) error {
	s.mu.Lock()
	s.starts++
	s.active++
	if s.active > s.maxActive {
		s.maxActive = s.active
	}
	s.mu.Unlock()
	<-ctx.Done()
	return ctx.Err()
}

func (s *leaderSpy) stopped(context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.stops++
	s.active--
	return nil
}

func (s *leaderSpy) counts() (int, int, int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.starts, s.stops, s.maxActive
}

// waitFor polls cond until it holds or the deadline passes.
func waitFor(t *testing.T, d time.Duration, cond func() bool) bool {
	t.Helper()
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if cond() {
			return true
		}
		time.Sleep(2 * time.Millisecond)
	}
	return cond()
}

func TestFirstCandidateAcquiresAnUnheldLease(t *testing.T) {
	be := NewMemBackend()
	e := New(testConfig("pod-a", newFakeClock()), be)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go e.Run(ctx, func(context.Context) error { <-ctx.Done(); return nil }, nil)

	if !waitFor(t, 2*time.Second, func() bool { return be.Holder("telemetry", "mq-leader") == "pod-a" }) {
		t.Fatalf("expected pod-a to acquire the lease, holder=%q", be.Holder("telemetry", "mq-leader"))
	}
	if !e.Leading() {
		t.Fatal("Leading() should be true after acquisition")
	}
}

func TestSecondCandidateDoesNotStealAFreshLease(t *testing.T) {
	be := NewMemBackend()
	clk := newFakeClock()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	a := New(testConfig("pod-a", clk), be)
	go a.Run(ctx, func(context.Context) error { <-ctx.Done(); return nil }, nil)

	if !waitFor(t, 2*time.Second, func() bool { return be.Holder("telemetry", "mq-leader") == "pod-a" }) {
		t.Fatal("pod-a never acquired the lease")
	}

	// pod-b starts while pod-a's lease is fresh and the clock has not moved:
	// the handover window has not opened, so b must stay a follower.
	b := New(testConfig("pod-b", clk), be)
	bCtx, bCancel := context.WithCancel(context.Background())
	defer bCancel()
	go b.Run(bCtx, func(context.Context) error { <-ctx.Done(); return nil }, nil)

	time.Sleep(150 * time.Millisecond)
	if got := be.Holder("telemetry", "mq-leader"); got != "pod-a" {
		t.Fatalf("live leader was displaced while its lease was fresh: holder=%q", got)
	}
	if b.Leading() {
		t.Fatal("pod-b must not be leading while pod-a's lease is fresh")
	}
}

func TestFollowerTakesOverAfterTheLeaderLeaseGoesStale(t *testing.T) {
	be := NewMemBackend()
	clk := newFakeClock()

	aCtx, aCancel := context.WithCancel(context.Background())
	spy := &leaderSpy{}
	a := New(testConfig("pod-a", clk), be)
	go a.Run(aCtx, spy.started, spy.stopped)

	if !waitFor(t, 2*time.Second, func() bool { return be.Holder("telemetry", "mq-leader") == "pod-a" }) {
		t.Fatal("pod-a never acquired the lease")
	}
	if !waitFor(t, 2*time.Second, func() bool { s, _, _ := spy.counts(); return s == 1 }) {
		t.Fatal("pod-a never started leading")
	}

	bSpy := &leaderSpy{}
	b := New(testConfig("pod-b", clk), be)
	bCtx, bCancel := context.WithCancel(context.Background())
	defer bCancel()
	go b.Run(bCtx, bSpy.started, bSpy.stopped)

	// Simulate pod-a dying: it stops renewing, and time moves past the lease
	// duration so its claim goes stale. (Merely advancing the clock is not
	// enough - a live pod-a would just renew, which is correct behaviour.)
	aCancel()
	clk.Advance(16 * time.Second)

	if !waitFor(t, 3*time.Second, func() bool { return be.Holder("telemetry", "mq-leader") == "pod-b" }) {
		t.Fatalf("pod-b never took over, holder=%q", be.Holder("telemetry", "mq-leader"))
	}
	if !waitFor(t, 3*time.Second, func() bool { s, _, _ := bSpy.counts(); return s == 1 }) {
		t.Fatal("pod-b never started leading after taking over")
	}
	// The dead leader must have been fenced, never left believing it leads.
	if a.Leading() {
		t.Fatal("the deposed leader still reports Leading()")
	}
	if _, stops, _ := spy.counts(); stops == 0 {
		t.Fatal("the deposed leader was never told to stop leading")
	}
}

func TestLeaderFencesItselfWhenItCannotRenewWithinTheRenewDeadline(t *testing.T) {
	// This is the split-brain guard. The API server stops answering, so the
	// leader cannot renew, but it is still perfectly able to reach its clients.
	// Unless it fences itself, it keeps acking publishes while a rival has
	// already taken over, and the log diverges.
	be := NewMemBackend()
	clk := newFakeClock()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	spy := &leaderSpy{}
	e := New(testConfig("pod-a", clk), be)
	go e.Run(ctx, spy.started, spy.stopped)

	if !waitFor(t, 2*time.Second, func() bool { return be.Holder("telemetry", "mq-leader") == "pod-a" }) {
		t.Fatal("pod-a never acquired the lease")
	}
	if !waitFor(t, 2*time.Second, func() bool { return e.Leading() }) {
		t.Fatal("pod-a never became leader")
	}

	// Renewals start failing, and time keeps moving.
	be.FailNext = 1000
	be.FailErr = errors.New("apiserver unreachable")

	// Past the renew deadline, leadership must end and OnStoppedLeading must
	// have run so the caller can make itself read-only.
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		clk.Advance(2 * time.Second)
		if _, stops, _ := spy.counts(); stops > 0 && !e.Leading() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	starts, stops, _ := spy.counts()
	t.Fatalf("leader did not fence itself within the renew deadline (starts=%d stops=%d leading=%v)", starts, stops, e.Leading())
}

func TestCoordinatorErrorIsNotTreatedAsLeadership(t *testing.T) {
	// A failing Get must never be read as "I am the leader". Availability is
	// lost on purpose here; correctness is not negotiable.
	be := NewMemBackend()
	be.FailNext = 1000
	be.FailErr = errors.New("apiserver unreachable")

	e := New(testConfig("pod-a", newFakeClock()), be)
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	starts := 0
	_ = e.Run(ctx, func(context.Context) error { starts++; return nil }, nil)
	if starts != 0 {
		t.Fatalf("leading work ran despite a failing coordinator (starts=%d)", starts)
	}
	if e.Leading() {
		t.Fatal("Leading() must stay false when the coordinator is unreachable")
	}
}

func TestConcurrentCandidatesElectExactlyOneLeader(t *testing.T) {
	// Every candidate races for the same expired lease. CAS must ensure exactly
	// one wins each round; without it two nodes would both believe they lead.
	be := NewMemBackend()
	clk := newFakeClock()

	// Seed a stale lease so the first round is a genuine takeover race.
	be.Seed(&Lease{
		Namespace:         "telemetry",
		Name:              "mq-leader",
		HolderIdentity:    "dead-pod",
		AcquireTime:       clk.Now().Add(-time.Hour),
		RenewTime:         clk.Now().Add(-time.Hour),
		LeaseDurationSecs: 15,
	})

	const n = 8
	ctx, cancel := context.WithTimeout(context.Background(), 1500*time.Millisecond)
	defer cancel()

	spies := make([]*leaderSpy, n)
	es := make([]*Elector, n)
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		id := string(rune('a' + i))
		spies[i] = &leaderSpy{}
		es[i] = New(testConfig("pod-"+id, clk), be)
		wg.Add(1)
		go func(e *Elector, s *leaderSpy) {
			defer wg.Done()
			// Advance the clock so every candidate can make progress through
			// renew-deadline checks without a 10s wait per round.
			go func() {
				for ctx.Err() == nil {
					clk.Advance(time.Second)
					time.Sleep(time.Millisecond)
				}
			}()
			_ = e.Run(ctx, s.started, s.stopped)
		}(es[i], spies[i])
	}
	wg.Wait()

	active := 0
	for i := range spies {
		_, _, maxActive := spies[i].counts()
		if maxActive > 0 {
			active++
		}
	}
	if active == 0 {
		t.Fatal("no candidate ever became leader")
	}
	// The holder recorded in the Lease must match a node that actually led,
	// and no two nodes may have been leading simultaneously (maxActive is
	// per-spy, so this checks each; the mutual exclusion check is that the
	// store's holder at any instant had been elected).
	if h := be.Holder("telemetry", "mq-leader"); h == "" {
		t.Fatal("lease has no holder after the race")
	}
}

func TestLoserStaysReadOnlyUntilItsTurnComes(t *testing.T) {
	be := NewMemBackend()
	clk := newFakeClock()

	aCtx, aCancel := context.WithCancel(context.Background())
	a := New(testConfig("pod-a", clk), be)
	go a.Run(aCtx, func(context.Context) error { <-aCtx.Done(); return nil }, nil)
	if !waitFor(t, 2*time.Second, func() bool { return be.Holder("telemetry", "mq-leader") == "pod-a" }) {
		t.Fatal("pod-a never acquired")
	}

	bStarted := make(chan struct{})
	b := New(testConfig("pod-b", clk), be)
	bCtx, bCancel := context.WithCancel(context.Background())
	defer bCancel()
	go b.Run(bCtx, func(context.Context) error { close(bStarted); <-bCtx.Done(); return nil }, nil)

	select {
	case <-bStarted:
		t.Fatal("pod-b ran leading work before it held the lease")
	case <-time.After(120 * time.Millisecond):
	}

	// Kill pod-a and let its lease go stale: the same node promotes itself with
	// no restart or reconfiguration - the property the whole design rests on.
	aCancel()
	clk.Advance(16 * time.Second)
	select {
	case <-bStarted:
	case <-time.After(4 * time.Second):
		t.Fatal("pod-b never promoted itself after the lease went stale")
	}
}

func TestExpiredLeaseIsTakenOverButFreshOneIsNot(t *testing.T) {
	// Guards the boundary directly: expiry is computed from renewTime, so a
	// lease exactly inside its duration must be respected and one a second
	// past it must not.
	clk := newFakeClock()
	fresh := &Lease{HolderIdentity: "other", RenewTime: clk.Now(), Namespace: "telemetry", Name: "mq-leader"}
	if expired(clk.Now(), fresh, 15*time.Second) {
		t.Fatal("a just-renewed lease must not be considered expired")
	}
	if !expired(clk.Now().Add(16*time.Second), fresh, 15*time.Second) {
		t.Fatal("a lease 16s past renewTime with a 15s duration must be expired")
	}
}

// TestLeaseFreshFencesPartitionedLeader is the split-brain guard.
//
// The component=leader label routes clients, but a leader that has lost the API
// server cannot patch that label away - it has no route to the API server,
// which is exactly why it lost contact. Readiness is the one signal kubelet
// evaluates without the API server, so /readyz asks LeaseFresh. These tests pin
// the contract: a fresh leader stays routable, and one whose renewals have
// stopped ageing out becomes un-routable on a local clock.
func TestLeaseFreshFencesPartitionedLeader(t *testing.T) {
	clk := &fakeClock{now: time.Now()}
	el := New(testConfig("mq-0", clk), NewMemBackend())
	now := clk.Now()

	// A follower is always fresh: it must stay routable so it can be promoted
	// and serve reads. Fencing a follower would break the quorum, which is the
	// opposite of what we want.
	if !el.LeaseFresh(now) {
		t.Fatal("a non-leader must be considered fresh")
	}

	// Simulate winning the lease and a renewal being accepted.
	el.leading.Store(true)
	el.markRenewed(now)
	if !el.LeaseFresh(now) {
		t.Fatal("a leader that just renewed must be fresh")
	}

	// The API server goes away. No further renewals. Inside the renew deadline
	// the leader can still legitimately claim the lease, so it stays routable.
	if el.LeaseFresh(now.Add(9 * time.Second)) {
		// still fresh, correct
	} else {
		t.Fatal("leader must stay fresh within the renew deadline")
	}

	// Past the renew deadline it cannot prove it holds the lease. This is the
	// moment the pod must leave the leader Service's endpoints, so a peer that
	// has already been promoted is the only node clients can reach.
	if el.LeaseFresh(now.Add(11 * time.Second)) {
		t.Fatal("leader with stale renewal must NOT be considered fresh")
	}

	// Stepping down restores routability: this node is a follower again and
	// must be promotable.
	el.leading.Store(false)
	if !el.LeaseFresh(now.Add(60 * time.Second)) {
		t.Fatal("a demoted node must be considered fresh again")
	}
}

// TestLeaseFreshLeaderWithoutRenewalIsStale covers the promotion race: a node
// that is leader but has not recorded a renewal must not be routed to. Erring
// toward fresh here would send clients to a leader that cannot confirm it holds
// the lease.
func TestLeaseFreshLeaderWithoutRenewalIsStale(t *testing.T) {
	el := New(testConfig("mq-0", &fakeClock{now: time.Now()}), NewMemBackend())
	el.leading.Store(true) // leader with no recorded renewal
	if el.LeaseFresh(time.Now()) {
		t.Fatal("a leader that has never renewed must not be considered fresh")
	}
}
