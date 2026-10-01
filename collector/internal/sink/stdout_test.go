package sink

import (
	"bytes"
	"context"
	"strings"
	"testing"

	mqpb "streamer/proto"
)

func TestStdoutSinkPrintsReceiptLines(t *testing.T) {
	var buf bytes.Buffer
	s := newStdoutSink(&buf)

	evs := []Event{
		{Offset: 3, Event: &mqpb.Event{
			EventId: "evt_a", CsvLineOffset: 42, LoopCount: 1, MetricName: "DCGM_FI_DEV_GPU_UTIL",
			GpuIndex: 2, Value: 99.5, Tags: map[string]string{"pod_name": "telemetry-streamer-0"},
		}},
		{Offset: 4, Event: &mqpb.Event{EventId: "evt_b", CsvLineOffset: 43, Tags: map[string]string{"pod_name": "telemetry-streamer-1"}}},
	}

	if err := s.Write(context.Background(), evs); err != nil {
		t.Fatalf("write: %v", err)
	}
	if s.Received() != 2 {
		t.Fatalf("received = %d, want 2", s.Received())
	}

	lines := strings.Split(strings.TrimSpace(buf.String()), "\n")
	if len(lines) != 2 {
		t.Fatalf("got %d lines, want 2:\n%s", len(lines), buf.String())
	}
	for _, want := range []string{"log_offset=3", "csv_line_offset=42", "loop_count=1", "value=99.5", "pod_name=\"telemetry-streamer-0\""} {
		if !strings.Contains(lines[0], want) {
			t.Fatalf("line 1 missing %q: %s", want, lines[0])
		}
	}
}
