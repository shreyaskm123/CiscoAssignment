package scheduler

import "testing"

func TestOwnsSingleReplicaProcessesEverything(t *testing.T) {
	s := New(0, 1)
	for n := int64(0); n < 100; n++ {
		if !s.Owns(n) {
			t.Fatalf("single replica should own row %d", n)
		}
	}
}

func TestOwnsDistributesDeterministically(t *testing.T) {
	replicas := 3
	indexes := []int{0, 1, 2}

	for farIdx, pod := range indexes {
		s := New(pod, replicas)
		for n := int64(0); n < 30; n++ {
			want := int(n)%replicas == pod
			if got := s.Owns(n); got != want {
				t.Errorf("pod %d owns(%d) = %v, want %v (expected %d%%3==%d)", pod, n, got, want, n, pod)
				break
			}
		}
		if farIdx == 0 {
			continue
		}
	}
}

// TestNoDuplicatesAcrossReplicas verifies the partition is complete and
// duplicate-free: for any line range, exactly one replica owns each line.
func TestNoDuplicatesAcrossReplicas(t *testing.T) {
	const (
		replicas = 3
		lines    = 60
	)
	seen := make([]int, lines)
	for pod := 0; pod < replicas; pod++ {
		s := New(pod, replicas)
		for n := int64(0); n < lines; n++ {
			if s.Owns(n) {
				seen[n]++
			}
		}
	}
	for n := 0; n < lines; n++ {
		if seen[n] != 1 {
			t.Errorf("line %d owned by %d replicas, want exactly 1 (no dupes, no gaps)", n, seen[n])
		}
	}
}

func TestReconfigureChangesPartition(t *testing.T) {
	s := New(0, 1)
	if !s.Owns(5) {
		t.Fatal("expected to own everything before reconfigure")
	}
	// Scale up: 3 replicas, still index 0.
	s.Reconfigure(0, 3)
	if s.Owns(5) {
		t.Error("should not own line 5 with podIndex 0 and 3 replicas")
	}
	if !s.Owns(9) {
		t.Error("should own line 9 with podIndex 0 and 3 replicas")
	}
	if s.PodIndex() != 0 || s.TotalReplicas() != 3 {
		t.Errorf("reconfigured state = %d/%d, want 0/3", s.PodIndex(), s.TotalReplicas())
	}
}
