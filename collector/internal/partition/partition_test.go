package partition

import "testing"

func TestStateOwns(t *testing.T) {
	cases := []struct {
		st     State
		offset int64
		want   bool
	}{
		{State{0, 3}, 0, true},
		{State{0, 3}, 3, true},
		{State{1, 3}, 1, true},
		{State{2, 3}, 2, true},
		{State{0, 3}, 1, false},
		{State{0, 3}, 2, false},
		{State{0, 1}, 9999, true}, // single replica owns everything
		{State{0, 3}, -1, false},  // negative offsets are never owned
		{State{-1, 3}, 0, false},  // unassigned index owns nothing
		{State{0, 0}, 0, false},   // zero total owns nothing
	}
	for _, c := range cases {
		if got := c.st.Owns(c.offset); got != c.want {
			t.Errorf("Owns(%+v, %d) = %v, want %v", c.st, c.offset, got, c.want)
		}
	}
}

// TestOwnershipDisjoint guarantees the "no two instances read the same batch"
// invariant: for any replica count, every offset is owned by exactly one index.
func TestOwnershipDisjoint(t *testing.T) {
	for total := 1; total <= 6; total++ {
		owners := make(map[int64][]int)
		for idx := 0; idx < total; idx++ {
			for off := int64(0); off < int64(total*7); off++ {
				if (State{idx, total}).Owns(off) {
					owners[off] = append(owners[off], idx)
				}
			}
		}
		for off := int64(0); off < int64(total*7); off++ {
			if len(owners[off]) != 1 {
				t.Fatalf("total=%d offset=%d owned by %d replicas (want exactly 1)", total, off, len(owners[off]))
			}
		}
	}
}
