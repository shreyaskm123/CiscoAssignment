package mq

import (
	"testing"
	"time"

	mqpb "streamer/proto"
)

// The queue depth is read from memory rather than from the process log, because
// the log rotates within seconds: a head read from it lagged the live committed
// cursor by hundreds of offsets and the difference came out negative. These tests
// pin the arithmetic the /metrics output depends on.
//
// Two behaviours of the server matter when writing them. A server built without
// election is the leader from the start, so CommitOffset is accepted. And the
// retention barrier only advances for a consumer that has JOINED the registry —
// a cursor on its own is not enough (see trimLocked).

func newStatsServer(t *testing.T) *Server {
	t.Helper()
	return NewWithStore(time.Minute, NewMemStore())
}

func commit(t *testing.T, s *Server, group, consumer string, offset int64) {
	t.Helper()
	if _, err := s.CommitOffset(t.Context(), &mqpb.CommitOffsetRequest{
		ConsumerId: consumer, Offset: offset, Group: group,
	}); err != nil {
		t.Fatalf("commit %s/%s=%d: %v", group, consumer, offset, err)
	}
}

func TestStatsOnEmptyLog(t *testing.T) {
	st := newStatsServer(t).Stats()
	if st.Head != -1 {
		t.Errorf("head = %d, want -1 for an empty log", st.Head)
	}
	if st.Base != 0 {
		t.Errorf("base = %d, want 0", st.Base)
	}
	if st.Retained() != 0 {
		t.Errorf("retained = %d, want 0", st.Retained())
	}
}

// Depth is the headline number: entries sitting in the log that no consumer has
// committed, and nothing else.
func TestStatsRetainedCountsUncommittedEntries(t *testing.T) {
	s := newStatsServer(t)
	for i := 0; i < 5; i++ {
		if _, err := s.store.Append(&mqpb.Event{EventId: string(rune('a' + i))}); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := s.JoinPartition(t.Context(), &mqpb.JoinPartitionRequest{ConsumerId: "collector-0", Group: collectorGroup, TtlSeconds: 60}); err != nil {
		t.Fatal(err)
	}
	if st := s.Stats(); st.Head != 4 || st.Retained() != 5 {
		t.Fatalf("after 5 appends: head=%d retained=%d, want head=4 retained=5", st.Head, st.Retained())
	}

	// The consumer commits through offset 2, so three are handled and two remain.
	commit(t, s, collectorGroup, "collector-0", 2)
	// Trim(floor=2) drops offsets 0,1,2, so base becomes 3: the first entry
	// still retained.
	st := s.Stats()
	if st.Base != 3 {
		t.Errorf("base = %d, want 3 (0,1,2 committed and trimmed)", st.Base)
	}
	if st.Retained() != 2 {
		t.Errorf("retained = %d, want 2 (offsets 3 and 4)", st.Retained())
	}
	if got, ok := st.Lag(ConsumerCursor{Group: collectorGroup, Consumer: "collector-0", Committed: 2, Kind: CursorLogOffset}); !ok || got != 2 {
		t.Errorf("collector lag = %d ok=%v, want 2/true", got, ok)
	}
}

// A committed cursor with no registered consumer must NOT move the barrier: a
// collector that has joined but never committed still needs the whole log. This
// is why depth can only be read as "retained", not derived from cursors alone.
func TestStatsCursorWithoutRegisteredConsumerDoesNotTrim(t *testing.T) {
	s := newStatsServer(t)
	for i := 0; i < 4; i++ {
		if _, err := s.store.Append(&mqpb.Event{EventId: string(rune('a' + i))}); err != nil {
			t.Fatal(err)
		}
	}
	commit(t, s, collectorGroup, "never-joined", 3) // no JoinPartition
	if st := s.Stats(); st.Base != 0 || st.Retained() != 4 {
		t.Errorf("base=%d retained=%d, want 0/4: an unregistered consumer must block trimming", st.Base, st.Retained())
	}
}

// Two groups hold independent cursors, which is why no single "unacked" number
// is exported.
func TestStatsReportsEveryGroupCursor(t *testing.T) {
	s := newStatsServer(t)
	for i := 0; i < 4; i++ {
		if _, err := s.store.Append(&mqpb.Event{EventId: string(rune('a' + i))}); err != nil {
			t.Fatal(err)
		}
	}
	commit(t, s, collectorGroup, "c1", 3)
	commit(t, s, "streamer", "p1", 1)

	st := s.Stats()
	if len(st.Consumers) != 2 {
		t.Fatalf("consumers = %+v, want 2 cursors", st.Consumers)
	}
	// Sorted by group then consumer, so a scrape is byte-stable.
	if st.Consumers[0].Group != collectorGroup || st.Consumers[1].Group != "streamer" {
		t.Errorf("cursors not sorted by group: %+v", st.Consumers)
	}
	if got, ok := st.Lag(st.Consumers[0]); !ok || got != 0 {
		t.Errorf("collector lag = %d ok=%v, want 0/true (caught up)", got, ok)
	}
	// The streamer group commits a CSV row, not a log offset. Reporting
	// head-cursor here would claim a lag of millions, so no lag is exported.
	if st.Consumers[1].Kind != CursorExternal {
		t.Errorf("streamer cursor kind = %q, want %q", st.Consumers[1].Kind, CursorExternal)
	}
	if got, ok := st.Lag(st.Consumers[1]); ok {
		t.Errorf("streamer lag = %d, want it withheld", got)
	}
}

func TestStatsLagNeverNegative(t *testing.T) {
	s := newStatsServer(t)
	if _, err := s.store.Append(&mqpb.Event{EventId: "a"}); err != nil {
		t.Fatal(err)
	}
	st := s.Stats()
	// A cursor ahead of the head happens after a failover onto a longer log.
	if got, ok := st.Lag(ConsumerCursor{Committed: st.Head + 50, Kind: CursorLogOffset}); !ok || got != 0 {
		t.Errorf("lag with a cursor ahead of the head = %d ok=%v, want 0/true", got, ok)
	}
}

func TestStatsRoleFollowsServer(t *testing.T) {
	st := newStatsServer(t).Stats()
	// A server built without election is the leader from the start; main.go
	// demotes it before serving when election is on.
	if st.Role != "leader" || !st.IsLeader {
		t.Errorf("role = %q, want leader", st.Role)
	}
}

// A nil server must not panic: the probe seam is nil in any mode that has not
// built one yet.
func TestStatsNilServerSafe(t *testing.T) {
	var s *Server
	st := s.Stats()
	if st.Head != -1 || st.Retained() != 0 {
		t.Errorf("nil server stats = %+v, want an empty snapshot", st)
	}
}
