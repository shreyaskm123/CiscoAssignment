package mq

import "sort"

// Queue observability.
//
// The log's head, its retention floor and every consumer's committed cursor are
// already in memory, so answering "how deep is the queue" costs nothing and
// needs no new bookkeeping. That matters because the alternative is reading
// them out of the process log, which is not a measurement: the log rotates
// within seconds at a few hundred events per second, so a head read from it
// lagged the live committed cursor by hundreds of offsets and the difference
// came out negative.
//
// Depth is deliberately not reported as a single "unacked" number. There is no
// such thing here: two consumer groups (streamer and collector) hold independent
// cursors, and the streamer group's cursor is a position in the CSV rather than
// an offset in the log, so it cannot be subtracted from the log head at all.
// What is meaningful is the retained window (what the queue is holding) plus
// each consumer's own lag, and both are exported.

// Cursor kinds. Which one a group uses decides whether its committed value can
// be compared against the log head.
const (
	// CursorLogOffset means Committed is an offset in this queue's log, so a lag
	// against the head is meaningful. The collector group consumes the log and
	// commits offsets it has processed.
	CursorLogOffset = "log-offset"
	// CursorExternal means Committed is a position in something outside the log,
	// such as the streamer's row in the CSV. Subtracting it from the head would
	// produce a huge meaningless number: with a head near 10.9M and a CSV row of
	// 221 it would claim a lag of 10.9M.
	CursorExternal = "external-position"
)

// ConsumerCursor is one consumer group's committed position.
type ConsumerCursor struct {
	Group     string
	Consumer  string
	Committed int64
	// Kind is CursorLogOffset or CursorExternal. It travels with the cursor
	// because it is the consumer's contract, not a property of the metric: the
	// same head is meaningful to subtract from one group and nonsense for another.
	Kind string
}

// IsLogOffset reports whether Committed indexes the queue log, and therefore
// whether a lag against the head means anything.
func (c ConsumerCursor) IsLogOffset() bool { return c.Kind == CursorLogOffset }

// Stats is a point-in-time snapshot of the queue. Every field is read under the
// relevant lock, so it is a consistent-enough view for an operator or a metric
// scrape without serialising the data path.
type Stats struct {
	// Role is "leader" or "follower".
	Role string
	// IsLeader mirrors Role for callers that want a boolean.
	IsLeader bool
	// Head is the last log offset assigned to an event, or -1 when the log is
	// empty. It is the offset an event would need to exceed to be appended.
	Head int64
	// Base is the offset of the first entry still retained in the log: every
	// offset strictly below it has been committed by a consumer and trimmed. It
	// is the low end of [Base, Head], which is what makes Retained() a simple
	// Head-Base+1. Calling it a "floor" invites the opposite reading, so it is
	// named for what it is.
	Base int64
	// Consumers is every committed cursor, sorted for a stable metric output.
	Consumers []ConsumerCursor
}

// Retained is the number of entries sitting in the log that no consumer has
// committed yet. That is the queue depth: entries at or below Base are gone,
// entries above Head do not exist.
func (s Stats) Retained() int64 {
	if s.Head < s.Base {
		return 0
	}
	return s.Head - s.Base + 1
}

// Lag is how far this consumer's committed cursor is behind the log head. It is
// only meaningful for a log-offset cursor; the second return value is false for
// an external cursor, and callers must not export a lag in that case.
func (s Stats) Lag(c ConsumerCursor) (int64, bool) {
	if !c.IsLogOffset() {
		return 0, false
	}
	if s.Head < c.Committed {
		// The cursor is ahead of the head: the log was trimmed past it (or this
		// node just adopted a longer log). Report 0 rather than a negative lag.
		return 0, true
	}
	return s.Head - c.Committed, true
}

// StatsProvider is the seam the /metrics handler uses, so the health package
// does not have to depend on a live *Server.
type StatsProvider interface {
	Stats() Stats
}

// cursorKind classifies a group's committed value. The collector group consumes
// the log and commits log offsets; the streamer group commits a row in the CSV,
// which is an external position. Any group added later that consumes the log
// needs to be classified here, otherwise its lag is withheld rather than
// exported wrong.
func cursorKind(group string) string {
	if group == collectorGroup {
		return CursorLogOffset
	}
	return CursorExternal
}

// Stats snapshots the queue. The store calls take their own locks, and the
// group map is read under the server lock, so this is safe to call from a
// metrics scrape while publishes and commits are in flight.
func (s *Server) Stats() Stats {
	st := Stats{Head: -1}
	if s == nil {
		return st
	}
	if s.store != nil {
		// Tail() is the offset the NEXT append will produce, so the last offset
		// actually assigned is one less. An empty log yields Tail()==base==0,
		// which would otherwise report a head of -1 twice over.
		if tail := s.store.Tail(); tail > 0 {
			st.Head = tail - 1
		}
	}
	// Read through the Store interface, not storeBase(): the latter is 0 unless
	// the store is durable, which would report a permanently empty backlog.
	st.Base = s.store.Base()
	if s.IsLeader() {
		st.Role, st.IsLeader = "leader", true
	} else {
		st.Role = "follower"
	}

	s.mu.Lock()
	for name, g := range s.groups {
		for consumer, offset := range g.consumers {
			st.Consumers = append(st.Consumers, ConsumerCursor{
				Group: name, Consumer: consumer, Committed: offset, Kind: cursorKind(name),
			})
		}
	}
	s.mu.Unlock()
	// Sorted so a scrape produces byte-identical output run to run; metric
	// consumers and diffs both benefit.
	sort.Slice(st.Consumers, func(i, j int) bool {
		if st.Consumers[i].Group != st.Consumers[j].Group {
			return st.Consumers[i].Group < st.Consumers[j].Group
		}
		return st.Consumers[i].Consumer < st.Consumers[j].Consumer
	})
	return st
}
