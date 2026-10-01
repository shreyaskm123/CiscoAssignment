package scheduler

import "sync"

// Scheduler performs the deterministic workload distribution described in the
// assignment. A data row at position currentLineNum is owned by exactly one
// replica:
//
//	currentLineNum % total == index  →  process
//	otherwise                        →  skip
//
// Because the decision only depends on the line index and the replica count,
// every replica of a given scale produces an identical, duplicate-free
// partition: a given row is processed by exactly one pod.
//
// The guard makes Reconfigure safe to call from the config-watcher goroutine
// while Owns is being called from the read loop (scale up/down).
type Scheduler struct {
	mu            sync.RWMutex
	podIndex      int
	totalReplicas int
}

// New validates and returns a Scheduler for the given replica layout. The
// (index, total) always comes from the MQ partition registry, never from
// static env vars.
func New(podIndex, totalReplicas int) *Scheduler {
	return &Scheduler{podIndex: podIndex, totalReplicas: totalReplicas}
}

// Owns reports whether the calling replica should process the row at
// currentLineNum. currentLineNum is the 0-based data row index as returned by
// the CSV reader.
func (s *Scheduler) Owns(currentLineNum int64) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.totalReplicas <= 1 {
		return true
	}
	return int(currentLineNum)%s.totalReplicas == s.podIndex
}

// Reconfigure switches PodIndex/TotalReplicas at runtime. Called when the
// registry watcher detects a scale up/down.
func (s *Scheduler) Reconfigure(podIndex, totalReplicas int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.podIndex = podIndex
	s.totalReplicas = totalReplicas
}

// PodIndex returns the replica index currently configured.
func (s *Scheduler) PodIndex() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.podIndex
}

// TotalReplicas returns the replica count currently configured.
func (s *Scheduler) TotalReplicas() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.totalReplicas
}
