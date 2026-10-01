package partition

import (
	"context"
	"log"
	"sync"
	"time"
)

// Registry is the minimal MQ surface the watcher needs (implemented by the
// collector's MQ dependency in production).
type Registry interface {
	JoinPartition(ctx context.Context, consumerID string, ttlSeconds int32) (index, total int, err error)
}

// Watcher keeps the collector's shard State in sync with the fleet: it
// heartbeats the MQ partition registry on every poll and repartitions when the
// returned assignment changes (scale up/down, pod death).
//
// Startup stability gate: replicas of the same scale wave register within
// milliseconds of each other, so a single join can observe a partial set. The
// assignment is only applied once two consecutive polls agree, mirroring the
// streamer's gate, before the collector starts consuming.
type Watcher struct {
	reg           Registry
	consumerID    string
	ttl           time.Duration
	pollInterval  time.Duration
	onReconfigure func(State)

	mu      sync.Mutex
	applied State

	prev      *State
	ready     chan struct{}
	readyOnce sync.Once
}

func NewWatcher(
	reg Registry,
	consumerID string,
	ttl time.Duration,
	pollInterval time.Duration,
	onReconfigure func(State),
) *Watcher {
	w := &Watcher{
		reg:           reg,
		consumerID:    consumerID,
		ttl:           ttl,
		pollInterval:  pollInterval,
		onReconfigure: onReconfigure,
		ready:         make(chan struct{}),
	}
	return w
}

// Ready is closed once the first stable assignment has been applied.
func (w *Watcher) Ready() <-chan struct{} { return w.ready }

// Start launches the polling loop.
func (w *Watcher) Start(ctx context.Context) {
	go w.loop(ctx)
}

// Applied returns the currently applied assignment.
func (w *Watcher) Applied() State {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.applied
}

func (w *Watcher) loop(ctx context.Context) {
	t := time.NewTicker(w.pollInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			next, ok := w.poll(ctx)
			if !ok {
				continue // keep last-known layout; retry next tick
			}
			if w.isReady() {
				w.apply(next) // post-boot membership change: repartition now
				continue
			}
			if w.prev != nil && *w.prev == next {
				w.apply(next)
				w.readyOnce.Do(func() { close(w.ready) })
				continue
			}
			cur := next
			w.prev = &cur
		}
	}
}

func (w *Watcher) apply(s State) {
	w.mu.Lock()
	changed := s != w.applied
	w.applied = s
	w.mu.Unlock()
	if changed {
		w.onReconfigure(s)
	}
}

func (w *Watcher) isReady() bool {
	select {
	case <-w.ready:
		return true
	default:
		return false
	}
}

func (w *Watcher) poll(ctx context.Context) (State, bool) {
	idx, total, err := w.reg.JoinPartition(ctx, w.consumerID, int32(w.ttl.Seconds()))
	if err != nil {
		log.Printf("registry heartbeat: %v", err)
		return State{}, false
	}
	return State{Index: idx, Total: total}, true
}
