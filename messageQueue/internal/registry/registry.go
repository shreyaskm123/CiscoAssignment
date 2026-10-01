// Package registry implements the partition registry used to assign unique
// replica indices to a stateless Deployment fleet.
//
// The registry keeps a set of live consumers keyed by consumer_id, each with a
// heartbeat TTL. A consumer calls Join to register (and to heartbeat); entries
// older than their TTL are swept so dead pods self-heal out of the assignment.
// The assigned index is the consumer's rank in the lexicographically sorted set
// of live consumers, and total is the size of that set — so every live
// consumer always holds a unique index in [0, total).
package registry

import (
	"sort"
	"sync"
	"time"
)

// Registry is safe for concurrent use. Its state is deliberately small and
// lives entirely in memory (the streamer is stateless; the fleet layout is the
// only coordination point and it fits in a map).
type Registry struct {
	mu    sync.Mutex
	ttl   time.Duration
	alive map[string]entry
}

type entry struct {
	last time.Time
	ttl  time.Duration
}

// New returns a Registry. ttl is the default heartbeat TTL used when a caller
// does not supply one (a request with ttl>0 overrides it per consumer).
func New(ttl time.Duration) *Registry {
	return &Registry{ttl: ttl, alive: make(map[string]entry)}
}

// Join marks consumer id as alive and returns its assigned (index, total).
// Repeated calls heartbeat the consumer. A consumer whose TTL expires is
// dropped by subsequent visits (and by the next sweep in Join/Active), which
// recomputes the assignment without outside coordination.
func (r *Registry) Join(id string, ttl time.Duration) (index, total int) {
	if ttl <= 0 {
		ttl = r.ttl
	}
	now := time.Now()
	r.mu.Lock()
	defer r.mu.Unlock()
	r.sweep(now)
	r.alive[id] = entry{last: now, ttl: ttl}
	return r.assign(id)
}

// Leave removes id immediately. Used on graceful shutdown so surviving
// replicas rebalance without waiting out the TTL.
func (r *Registry) Leave(id string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.alive, id)
}

// Active returns the live consumer ids in sorted order (same order used for
// index assignment). Mostly for observability and tests.
func (r *Registry) Active() []string {
	now := time.Now()
	r.mu.Lock()
	defer r.mu.Unlock()
	r.sweep(now)
	return sortedKeys(r.alive)
}

// Count returns the number of live consumers after sweeping expired entries.
func (r *Registry) Count() int {
	now := time.Now()
	r.mu.Lock()
	defer r.mu.Unlock()
	r.sweep(now)
	return len(r.alive)
}

// sweep drops consumers whose last heartbeat is older than their TTL.
func (r *Registry) sweep(now time.Time) {
	for id, e := range r.alive {
		if e.last.Add(e.ttl).Before(now) {
			delete(r.alive, id)
		}
	}
}

// assign computes index = rank of id in the sorted live set; it must be called
// with r.mu held.
func (r *Registry) assign(id string) (index, total int) {
	ids := sortedKeys(r.alive)
	for i, k := range ids {
		if k == id {
			return i, len(ids)
		}
	}
	// id was just inserted above; this branch is unreachable in practice.
	return 0, len(ids)
}

func sortedKeys(m map[string]entry) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}
