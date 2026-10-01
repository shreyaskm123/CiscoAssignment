package offsets

import "testing"

func TestNewStartsAtOffset(t *testing.T) {
	m := New(40)
	if m.ReadPosition() != 40 || m.CommittedPosition() != 40 {
		t.Errorf("New(40): read=%d committed=%d, want 40/40", m.ReadPosition(), m.CommittedPosition())
	}
}

func TestAdvanceReadIsMonotonicAndForwardOnly(t *testing.T) {
	m := New(0)
	m.AdvanceRead(10)
	if m.ReadPosition() != 10 {
		t.Errorf("read = %d, want 10", m.ReadPosition())
	}
	// Backwards advance must be ignored.
	m.AdvanceRead(5)
	if m.ReadPosition() != 10 {
		t.Errorf("read = %d after backwards advance, want 10", m.ReadPosition())
	}
}

func TestCommitIsMonotonic(t *testing.T) {
	m := New(0)
	m.Commit(7)
	if m.CommittedPosition() != 7 {
		t.Errorf("committed = %d, want 7", m.CommittedPosition())
	}
	// Stale ack (arrived out of order) must not move the committed pointer back.
	m.Commit(3)
	if m.CommittedPosition() != 7 {
		t.Errorf("committed = %d after stale ack, want 7", m.CommittedPosition())
	}
	m.Commit(9)
	if m.CommittedPosition() != 9 {
		t.Errorf("committed = %d, want 9", m.CommittedPosition())
	}
}

func TestLag(t *testing.T) {
	m := New(0)
	m.AdvanceRead(12)
	m.Commit(12)
	if m.Lag() != 0 {
		t.Errorf("lag = %d, want 0", m.Lag())
	}
	m.AdvanceRead(15)
	if m.Lag() != 3 {
		t.Errorf("lag = %d, want 3", m.Lag())
	}
}

func TestResetTo(t *testing.T) {
	m := New(0)
	m.AdvanceRead(20)
	m.Commit(15)
	m.ResetTo(2)
	if m.ReadPosition() != 2 || m.CommittedPosition() != 2 {
		t.Errorf("after ResetTo(2): read=%d committed=%d, want 2/2", m.ReadPosition(), m.CommittedPosition())
	}
}
