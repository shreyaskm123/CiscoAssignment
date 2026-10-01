package app

import (
	"context"
	"strings"
	"testing"
	"time"

	"streamer/internal/csvsrc"
	"streamer/internal/mqclient"
)

// Readiness is the only thing standing between a stalled message queue and a
// pipeline that looks healthy while producing nothing, so the rules Status()
// applies are pinned here rather than trusted.

func newStatusStreamer(t *testing.T) *Streamer {
	t.Helper()
	mq := newFakeMQ()
	addr := startFakeMQ(t, mq)
	cfg := testConfig(addr)
	cfg.ReadyMaxStall = 200 * time.Millisecond

	reader, err := csvsrc.Open(cfg.CSVPath)
	if err != nil {
		t.Fatalf("open csv: %v", err)
	}
	client, err := mqclient.Dial(context.Background(), addr)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() { _ = client.Close() })

	s, err := New(context.Background(), cfg, reader, client)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return s
}

// A replica that has lost its assignment (or has not been given one yet) does
// not know which CSV rows are its responsibility, so it must not claim ready.
func TestStreamerNotReadyWithoutShardAssignment(t *testing.T) {
	s := newStatusStreamer(t)
	s.recordAck(1)
	s.sched.Reconfigure(0, 0)
	got := s.Status()
	if got.Ready {
		t.Fatal("a streamer with no partition assignment must not be ready")
	}
	if got.Reason == "" {
		t.Error("a NotReady result must say why")
	}
}

func TestStreamerReadyWhenAssignedAndAcking(t *testing.T) {
	s := newStatusStreamer(t)
	s.recordAck(1)
	got := s.Status()
	if !got.Ready {
		t.Fatalf("want ready, got NotReady: %s", got.Reason)
	}
	for _, k := range []string{"consumer", "shard", "read_position", "committed_position", "sent_total", "acked_total", "unacked_in_flight"} {
		if _, ok := got.Fields[k]; !ok {
			t.Errorf("readiness output is missing the %q field", k)
		}
	}
}

// The stall this work exists to catch: the queue accepted the publishes and
// never acknowledged them, so no error is raised and no counter moves. Only the
// clock notices.
func TestStreamerNotReadyWhenNoAckArrives(t *testing.T) {
	s := newStatusStreamer(t)
	s.recordAck(1)
	s.lastAck.Store(time.Now().Add(-time.Second).UnixNano()) // last ack a second ago
	got := s.Status()
	if got.Ready {
		t.Fatal("a streamer with no acknowledged publish must not be ready")
	}
	// Assert the substance, not the exact phrasing: the message must say that
	// nothing has been acknowledged and must quantify it.
	if !strings.Contains(got.Reason, "no acknowledged publish") {
		t.Errorf("the reason must state that nothing has been acknowledged, got %q", got.Reason)
	}
	if !strings.Contains(got.Reason, "awaiting acknowledgement") {
		t.Errorf("the reason must quantify the unacknowledged backlog, got %q", got.Reason)
	}
	if got.Fields["last_ack_age_seconds"] == "" {
		t.Error("the ack age should be quantified in the fields")
	}
}

// The stall clock has to start somewhere before the first ack, or a replica that
// has never published would read as stalled since the epoch forever.
func TestStreamerStallClockRunsFromConstructionBeforeAnyAck(t *testing.T) {
	s := newStatusStreamer(t)
	s.startedAt = time.Now().Add(-time.Second).UnixNano()
	if got := s.Status(); got.Ready {
		t.Error("a replica that has never had a publish acked must not read as ready")
	}
}

func TestStreamerStallCheckCanBeDisabled(t *testing.T) {
	s := newStatusStreamer(t)
	s.cfg.ReadyMaxStall = 0
	s.startedAt = time.Now().Add(-time.Hour).UnixNano()
	if got := s.Status(); !got.Ready {
		t.Errorf("with READY_MAX_STALL_SECONDS=0 an idle streamer should be ready, got: %s", got.Reason)
	}
}

func TestStreamerNotReadyWhilePublishFails(t *testing.T) {
	s := newStatusStreamer(t)
	s.recordAck(1)
	s.batch.lastSendFail.Store(time.Now().Add(-time.Second).UnixNano())
	got := s.Status()
	if got.Ready {
		t.Fatal("a streamer that cannot publish must not be ready")
	}
	if got.Fields["last_send_failure_age_seconds"] == "" {
		t.Error("the send-failure age should be in the fields")
	}
}

// Recovery must not need a restart: a fresh ack clears the stall.
func TestStreamerRecoversWithoutRestart(t *testing.T) {
	s := newStatusStreamer(t)
	s.recordAck(1)
	s.lastAck.Store(time.Now().Add(-time.Second).UnixNano())
	if s.Status().Ready {
		t.Fatal("precondition: want NotReady while stalled")
	}
	s.recordAck(2)
	if got := s.Status(); !got.Ready {
		t.Errorf("a new ack must restore readiness without a restart, got: %s", got.Reason)
	}
}
