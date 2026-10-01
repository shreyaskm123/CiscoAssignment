package sink

import (
	"context"
	"fmt"
	"io"
	"os"
	"sync/atomic"
)

// StdoutSink is a demo/testing sink: it prints one parseable receipt line per
// event and counts them. It always succeeds, so every buffered batch is
// committed and the cursor advances — this is how an end-to-end run proves
// "everything the MQ delivered reached a consumer". It is NOT a production
// writer; select it with SINK=stdout only for lab/demo runs (ClickHouseSink is
// the default and the production path).
type StdoutSink struct {
	w        io.Writer
	received atomic.Int64 // events accepted by Write
}

// NewStdout returns a StdoutSink that writes receipt lines to standard output.
func NewStdout() *StdoutSink { return &StdoutSink{w: os.Stdout} }

func newStdoutSink(w io.Writer) *StdoutSink { return &StdoutSink{w: w} }

// Received returns how many events have been accepted.
func (s *StdoutSink) Received() int64 { return s.received.Load() }

// Write prints one EVENT line per batch entry and returns nil.
func (s *StdoutSink) Write(_ context.Context, batch []Event) error {
	for _, ev := range batch {
		fmt.Fprintf(s.w, "EVENT log_offset=%d csv_line_offset=%d loop_count=%d event_id=%q metric_name=%q gpu_index=%d value=%g pod_name=%q source_ts=%q\n",
			ev.Offset, ev.Event.CsvLineOffset, ev.Event.LoopCount, ev.Event.EventId,
			ev.Event.MetricName, ev.Event.GpuIndex, ev.Event.Value, ev.Event.Tags["pod_name"], ev.Event.SourceTimestamp)
		s.received.Add(1)
	}
	return nil
}
