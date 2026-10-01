package app

import (
	"context"
	"log"
	"sync"
	"time"

	"streamer/internal/config"
	"streamer/internal/mqclient"
	"streamer/internal/scheduler"
)

// configWatcher keeps the scheduler's (index, total) in sync with the fleet.
// It heartbeats the MQ's partition registry on every tick and repartitions
// when the returned assignment changes (scale up/down, pod death).
// Startup stability gate: replicas of the same scale wave join the registry
// within milliseconds of each other, so a single Join can observe a partial
// set (e.g. 0/2 while a sibling is still registering). To avoid streaming
// under a transiently wrong layout, Run waits on Ready before producing: the
// assignment is only applied once the registry returns the same (index,total)
// on two consecutive polls. After that, membership changes repartition
// immediately (dedup absorbs the small handover).
type configWatcher struct {
	cfg    *config.Config
	client *mqclient.Client
	sched  *scheduler.Scheduler

	onReconfigure func(podIndex, totalReplicas int)
	applied       layout
	prev          *layout

	ready     chan struct{}
	readyOnce sync.Once
}

func newConfigWatcher(cfg *config.Config, client *mqclient.Client, sched *scheduler.Scheduler, onReconfigure func(int, int)) *configWatcher {
	w := &configWatcher{
		cfg:           cfg,
		client:        client,
		sched:         sched,
		onReconfigure: onReconfigure,
		applied:       layout{sched.PodIndex(), sched.TotalReplicas()},
		ready:         make(chan struct{}),
	}
	return w
}

// Ready is closed once the first stable assignment has been applied. Run waits
// on it before starting to publish.
func (w *configWatcher) Ready() <-chan struct{} { return w.ready }

func (w *configWatcher) start(ctx context.Context) {
	go w.loop(ctx)
}

func (w *configWatcher) loop(ctx context.Context) {
	interval := time.Duration(w.cfg.RegistryPollIntervalMs) * time.Millisecond
	if interval <= 0 {
		interval = 5 * time.Second
	}
	t := time.NewTicker(interval)
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
			// Pre-ready stabilization: wait for two identical consecutive polls.
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

func (w *configWatcher) apply(l layout) {
	if l == w.applied {
		return
	}
	w.applied = l
	w.onReconfigure(l.index, l.total)
}

func (w *configWatcher) isReady() bool {
	select {
	case <-w.ready:
		return true
	default:
		return false
	}
}

type layout struct{ index, total int }

func (w *configWatcher) poll(ctx context.Context) (layout, bool) {
	idx, total, err := w.client.JoinPartition(ctx, w.cfg.ConsumerID, int32(w.cfg.RegistryTTLSec))
	if err != nil {
		log.Printf("registry heartbeat: %v", err)
		return layout{}, false
	}
	return layout{index: idx, total: total}, true
}
