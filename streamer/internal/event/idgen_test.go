package event

import "testing"

func TestID(t *testing.T) {
	got := ID("telemetry-streamer-1", 0, 105)
	want := "evt_telemetry-streamer-1_L0_O105"
	if got != want {
		t.Errorf("ID(%q,0,105) = %q, want %q", "telemetry-streamer-1", got, want)
	}
}

func TestIDIsDeterministic(t *testing.T) {
	a := ID("streamer-a", 3, 42)
	b := ID("streamer-a", 3, 42)
	if a != b {
		t.Errorf("same inputs produced different ids: %q vs %q", a, b)
	}
}

func TestIDVariesAcrossDimensions(t *testing.T) {
	same := ID("p", 1, 2)
	otherPod := ID("q", 1, 2)
	otherLoop := ID("p", 2, 2)
	otherOffset := ID("p", 1, 3)
	ids := map[string]string{same: "base", otherPod: "pod", otherLoop: "loop", otherOffset: "offset"}
	if len(ids) != 4 {
		t.Errorf("expected 4 distinct ids across pod/loop/offset, got %d: %v", len(ids), ids)
	}
}
