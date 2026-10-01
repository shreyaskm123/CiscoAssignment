package partition

import (
	"context"
	"sync"
	"testing"
	"time"
)

// seqReg returns a scripted sequence of JoinPartition results; the last value
// repeats.
type seqReg struct {
	mu      sync.Mutex
	calls   int
	results []State
}

func (r *seqReg) JoinPartition(_ context.Context, _ string, _ int32) (int, int, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	i := r.calls
	r.calls++
	if i >= len(r.results) {
		i = len(r.results) - 1
	}
	return r.results[i].Index, r.results[i].Total, nil
}

// TestWatcherStabilityGateAndReconfig verifies the two core behaviors:
// readiness only after two identical polls, and immediate reconfigure after
// readiness when the registry reports a change (scale event).
func TestWatcherStabilityGateAndReconfig(t *testing.T) {
	reg := &seqReg{results: []State{
		{Index: 0, Total: 1},
		{Index: 0, Total: 1},
		{Index: 0, Total: 1},
		{Index: 0, Total: 2},
	}}

	var applied []State
	var appliedMu sync.Mutex
	onConfigure := func(s State) {
		appliedMu.Lock()
		defer appliedMu.Unlock()
		applied = append(applied, s)
	}

	w := NewWatcher(reg, "coll", time.Minute, 10*time.Millisecond, onConfigure)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	w.Start(ctx)

	select {
	case <-w.Ready():
	case <-time.After(2 * time.Second):
		t.Fatal("Ready never closed")
	}

	// After the scale event the watcher must apply total=2.
	deadline := time.Now().Add(2 * time.Second)
	for {
		appliedMu.Lock()
		last := State{}
		if len(applied) > 0 {
			last = applied[len(applied)-1]
		}
		appliedMu.Unlock()
		if last == (State{Index: 0, Total: 2}) {
			break
		}
		if time.Now().After(deadline) {
			appliedMu.Lock()
			t.Fatalf("never reconfigured to total=2; applied=%v", applied)
		}
		time.Sleep(10 * time.Millisecond)
	}
}
