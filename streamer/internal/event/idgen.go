package event

import "fmt"

// ID generates the deterministic, globally-unique event id for a row:
//
//	evt_<pod_name>_L<loop_count>_O<csv_line_offset>
//
// The id is a pure function of (pod_name, loop_count, csv_line_offset), all of
// which are fully determined by the replica and its position in the file. The
// same row therefore always yields the same id, which is what makes the MQ-side
// de-duplication idempotent: a re-sent event carries the same event_id and the
// MQ can recognise it as already received.
func ID(podName string, loopCount, csvLineOffset int64) string {
	return fmt.Sprintf("evt_%s_L%d_O%d", podName, loopCount, csvLineOffset)
}
