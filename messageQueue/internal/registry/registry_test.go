package registry

import (
	"testing"
	"time"
)

func TestJoinAssignsSortedRanks(t *testing.T) {
	r := New(10 * time.Second)

	idx, total := r.Join("b", 0)
	if total != 1 || idx != 0 {
		t.Fatalf("single join: got (index=%d,total=%d), want (0,1)", idx, total)
	}

	idx, total = r.Join("a", 0)
	if total != 2 || idx != 0 {
		t.Fatalf("join a: got (index=%d,total=%d), want (0,2)", idx, total)
	}

	idx, total = r.Join("c", 0)
	if total != 3 || idx != 2 {
		t.Fatalf("join c: got (index=%d,total=%d), want (2,3)", idx, total)
	}

	// b sits between a and c.
	idx, total = r.Join("b", 0)
	if total != 3 || idx != 1 {
		t.Fatalf("heartbeat b: got (index=%d,total=%d), want (1,3)", idx, total)
	}
}

func TestRanksAreUnique(t *testing.T) {
	r := New(10 * time.Second)
	ids := []string{"p0", "p5", "p2", "p9", "p1", "p4", "p7", "p3", "p8", "p6"}
	for i, id := range ids {
		_, total := r.Join(id, 0)
		if total != i+1 {
			t.Fatalf("join %s (%dth): total=%d, want %d", id, i+1, total, i+1)
		}
	}

	// With the set stable, the current assignment must hand each consumer a
	// unique, contiguous index in [0, N).
	got := map[string]int{}
	for _, id := range ids {
		idx, total := r.Join(id, 0) // heartbeat
		if total != len(ids) {
			t.Fatalf("heartbeat %s: total=%d, want %d", id, total, len(ids))
		}
		got[id] = idx
	}
	for _, idx := range got {
		if idx < 0 || idx >= len(ids) {
			t.Fatalf("index %d out of range [0,%d)", idx, len(ids))
		}
	}
	uniq := map[int]bool{}
	for _, idx := range got {
		if uniq[idx] {
			t.Fatalf("duplicate index %d in stable assignment %v", idx, got)
		}
		uniq[idx] = true
	}
	if len(uniq) != len(ids) {
		t.Fatalf("expected %d unique indices, got %d (%v)", len(ids), len(uniq), got)
	}
}

func TestTTLExpiryShrinksAndRearranges(t *testing.T) {
	r := New(40 * time.Millisecond)

	r.Join("a", 0)
	r.Join("b", 0)
	if got := r.Count(); got != 2 {
		t.Fatalf("count before expiry = %d, want 2", got)
	}

	// Let a and b expire, then let c join: c must be the only live consumer.
	time.Sleep(50 * time.Millisecond)
	idx, total := r.Join("c", time.Second)
	if total != 1 || idx != 0 {
		t.Fatalf("after expiry: got (index=%d,total=%d), want (0,1)", idx, total)
	}
}

func TestLeaveRemovesImmediately(t *testing.T) {
	r := New(time.Minute)
	r.Join("a", 0)
	r.Join("b", 0)
	r.Join("c", 0)

	r.Leave("b")
	idx, total := r.Join("b", 0)
	if total != 3 { // b re-registered; a, c never left
		t.Fatalf("total after leave+rejoin = %d, want 3", total)
	}
	if idx != 1 {
		t.Fatalf("b index after leave+rejoin = %d, want 1 (a<b<c)", idx)
	}
	if got := r.Count(); got != 3 {
		t.Fatalf("count after leave+rejoin = %d, want 3", got)
	}
}

func TestHeartbeatKeepsAlive(t *testing.T) {
	r := New(30 * time.Millisecond)
	r.Join("a", 0)
	for i := 0; i < 5; i++ {
		time.Sleep(15 * time.Millisecond)
		if _, total := r.Join("a", 0); total != 1 {
			t.Fatalf("heartbeat lost consumer: total=%d, want 1", total)
		}
	}
}
