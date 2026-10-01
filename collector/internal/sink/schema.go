package sink

import (
	"context"
	"fmt"
	"log"
	"net"
	"regexp"
	"strconv"
	"strings"
	"time"
)

// invalidIdentifierError is raised when db/table don't pass schema.ValidIdentifier.
type invalidIdentifierError struct{ db, table string }

func (e *invalidIdentifierError) Error() string {
	return fmt.Sprintf("invalid ClickHouse identifiers db=%q table=%q", e.db, e.table)
}

func netJoinHostPort(host string, port int) string {
	return net.JoinHostPort(host, strconv.Itoa(port))
}

// eventIDRE is the charset the streamer generates (evt_<pod>_L<n>_O<n>, pod
// names are DNS-style); used to safely inline ids into an IN clause.
var eventIDRE = regexp.MustCompile(`^[a-zA-Z0-9_.:+-]{1,256}$`)

// parseTimestamp converts the event's RFC3339 timestamp into a time.Time. On
// malformed input it falls back to now (a single bad row must not wedge a
// batch).
func parseTimestamp(s string, now time.Time) time.Time {
	for _, layout := range []string{time.RFC3339Nano, time.RFC3339} {
		if t, err := time.Parse(layout, s); err == nil {
			return t
		}
	}
	return now
}

// filterExisting drops batch rows whose event_id is already present in the
// table (the optional DEDUP_PRE_CHECK path). On any query problem it fails
// open: the batch is written anyway so processing never stalls — the
// ReplacingMergeTree remains the safety net.
func (s *ClickHouseSink) filterExisting(ctx context.Context, batch []Event) []Event {
	ids := make([]string, 0, len(batch))
	for _, ev := range batch {
		if eventIDRE.MatchString(ev.Event.EventId) {
			ids = append(ids, ev.Event.EventId)
		}
	}
	if len(ids) == 0 {
		return batch
	}

	var present []string
	rows, err := s.conn.Query(ctx, "SELECT event_id FROM "+s.table+" WHERE event_id IN ("+quoteIn(ids)+")")
	if err != nil {
		log.Printf("dedup pre-check query failed; writing batch anyway: %v", err)
		return batch
	}
	defer rows.Close()
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			log.Printf("dedup pre-check scan failed; writing batch anyway: %v", err)
			return batch
		}
		present = append(present, id)
	}
	if err := rows.Err(); err != nil {
		log.Printf("dedup pre-check error; writing batch anyway: %v", err)
		return batch
	}

	set := make(map[string]struct{}, len(present))
	for _, id := range present {
		set[id] = struct{}{}
	}
	out := batch[:0]
	for _, ev := range batch {
		if _, dup := set[ev.Event.EventId]; !dup {
			out = append(out, ev)
		}
	}
	return out
}

// quoteIn builds a quoted, safely-escaped IN-list from ids already validated
// against eventIDRE (the charset excludes quotes, doubling is a belt-and-
// suspenders guard).
func quoteIn(ids []string) string {
	quoted := make([]string, 0, len(ids))
	for _, id := range ids {
		quoted = append(quoted, "'"+strings.ReplaceAll(id, "'", "''")+"'")
	}
	return strings.Join(quoted, ",")
}
