package election

import (
	"context"
	"sync"
	"time"
)

// MemBackend is an in-memory Backend for local runs and tests. It enforces the
// same compare-and-swap contract as the API server: Update fails with
// ErrConflict when the caller's ResourceVersion is not the current one, and
// Create fails with ErrConflict when the object already exists. Without that
// enforcement the elector's mutual-exclusion tests would prove nothing, since
// the races are exactly what CAS is there to resolve.
type MemBackend struct {
	mu     sync.Mutex
	leases map[string]*Lease
	rv     int
	// FailNext, when > 0, makes the next N operations return this error. Used
	// to simulate an unreachable API server and assert that a leader fences
	// itself within the renew deadline.
	FailNext int
	FailErr  error
}

// NewMemBackend returns an empty in-memory Backend.
func NewMemBackend() *MemBackend {
	return &MemBackend{leases: make(map[string]*Lease)}
}

func memKey(ns, name string) string { return ns + "/" + name }

// Get returns the Lease or ErrNotFound.
func (m *MemBackend) Get(_ context.Context, ns, name string) (*Lease, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.FailNext > 0 {
		m.FailNext--
		return nil, m.FailErr
	}
	l, ok := m.leases[memKey(ns, name)]
	if !ok {
		return nil, ErrNotFound
	}
	cp := *l
	return &cp, nil
}

// Create inserts the Lease, failing with ErrConflict if it exists.
func (m *MemBackend) Create(_ context.Context, l *Lease) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.FailNext > 0 {
		m.FailNext--
		return m.FailErr
	}
	k := memKey(l.Namespace, l.Name)
	if _, ok := m.leases[k]; ok {
		return ErrConflict
	}
	m.rv++
	cp := *l
	cp.ResourceVersion = itoa(m.rv)
	m.leases[k] = &cp
	return nil
}

// Update replaces the Lease only if ResourceVersion matches the stored one.
func (m *MemBackend) Update(_ context.Context, l *Lease) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.FailNext > 0 {
		m.FailNext--
		return m.FailErr
	}
	k := memKey(l.Namespace, l.Name)
	cur, ok := m.leases[k]
	if !ok {
		return ErrNotFound
	}
	if l.ResourceVersion != cur.ResourceVersion {
		return ErrConflict
	}
	m.rv++
	cp := *l
	cp.ResourceVersion = itoa(m.rv)
	m.leases[k] = &cp
	return nil
}

// Seed installs a Lease directly, bypassing CAS. It is for setting up a
// specific starting state (e.g. a stale lease held by a dead identity).
func (m *MemBackend) Seed(l *Lease) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.rv++
	cp := *l
	cp.ResourceVersion = itoa(m.rv)
	m.leases[memKey(l.Namespace, l.Name)] = &cp
}

// Holder returns the current holder identity, or "" when absent.
func (m *MemBackend) Holder(ns, name string) string {
	m.mu.Lock()
	defer m.mu.Unlock()
	if l, ok := m.leases[memKey(ns, name)]; ok {
		return l.HolderIdentity
	}
	return ""
}

func itoa(i int) string {
	if i == 0 {
		return "0"
	}
	var b []byte
	for i > 0 {
		b = append([]byte{byte('0' + i%10)}, b...)
		i /= 10
	}
	return string(b)
}

// fakeClock is a manually advanced Clock for tests.
type fakeClock struct {
	mu  sync.Mutex
	now time.Time
}

func newFakeClock() *fakeClock {
	return &fakeClock{now: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)}
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *fakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
}
