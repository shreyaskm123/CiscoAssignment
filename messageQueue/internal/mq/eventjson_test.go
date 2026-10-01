package mq

import (
	"encoding/json"
	"strings"
	"testing"

	mqpb "streamer/proto"
)

// The WAL stores events as JSON. source_timestamp is the event's only
// timestamp, so it must survive an encode/decode round trip: if it were
// dropped, a replay after an MQ restart would hand the collector an event with
// no time and it would be stamped with the wrong one.
func TestEventJSONRoundTripKeepsSourceTimestamp(t *testing.T) {
	in := &mqpb.Event{
		EventId:         "evt_pod_L1_O2",
		SourceTimestamp: "2026-10-01T18:44:44.882790639Z",
		MetricName:      "DCGM_FI_DEV_GPU_UTIL",
		Value:           42.5,
	}
	b, err := json.Marshal(toEventJSON(in))
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(b), `"timestamp"`) {
		t.Errorf("encoded event still carries a legacy timestamp key: %s", b)
	}
	var j eventJSON
	if err := json.Unmarshal(b, &j); err != nil {
		t.Fatal(err)
	}
	if got := j.toEvent().SourceTimestamp; got != in.SourceTimestamp {
		t.Errorf("source_timestamp after round trip = %q, want %q", got, in.SourceTimestamp)
	}
}

// WAL records written before the `timestamp` field was dropped have only the
// legacy key. They must keep their time rather than come back empty.
func TestEventJSONDecodesLegacyTimestampKey(t *testing.T) {
	legacy := `{"event_id":"evt_old","timestamp":"2026-10-01T10:00:00.5Z","metric_name":"m"}`
	var j eventJSON
	if err := json.Unmarshal([]byte(legacy), &j); err != nil {
		t.Fatal(err)
	}
	if got := j.toEvent().SourceTimestamp; got != "2026-10-01T10:00:00.5Z" {
		t.Errorf("legacy record decoded to source_timestamp %q, want its old timestamp", got)
	}

	// When both keys are present, source_timestamp wins.
	both := `{"event_id":"evt","timestamp":"2020-01-01T00:00:00Z","source_timestamp":"2026-10-01T10:00:00Z"}`
	j = eventJSON{}
	if err := json.Unmarshal([]byte(both), &j); err != nil {
		t.Fatal(err)
	}
	if got := j.toEvent().SourceTimestamp; got != "2026-10-01T10:00:00Z" {
		t.Errorf("source_timestamp = %q, want it to win over the legacy key", got)
	}
}
