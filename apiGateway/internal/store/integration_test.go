//go:build integration

// Real-ClickHouse test for the three query APIs. Skipped by default; run with:
//
//	CLICKHOUSE_HOST=localhost go test -tags integration ./internal/store/ -run TestStore
//
// It creates the table through collector/schema.TableDDL — the same DDL the
// collector applies — so any sort-key change is exercised here too.
package store

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"

	"collector/schema"
)

func TestStoreQueryAPIs(t *testing.T) {
	host := os.Getenv("CLICKHOUSE_HOST")
	if host == "" {
		t.Skip("set CLICKHOUSE_HOST (and CLICKHOUSE_PORT/USER/PASSWORD) to run")
	}
	ctx := context.Background()
	port := envDefault(os.Getenv("CLICKHOUSE_PORT"), "9000")
	user := envDefault(os.Getenv("CLICKHOUSE_USER"), "default")

	const db = "telemetry_it"
	tableName := fmt.Sprintf("events_api_%d", time.Now().UnixNano())
	conn, err := clickhouse.Open(&clickhouse.Options{
		Addr: []string{host + ":" + port},
		Auth: clickhouse.Auth{Username: user, Password: os.Getenv("CLICKHOUSE_PASSWORD")},
	})
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer func() { _ = conn.Exec(ctx, "DROP TABLE IF EXISTS "+db+"."+tableName); conn.Close() }()
	if err := conn.Exec(ctx, "CREATE DATABASE IF NOT EXISTS "+db); err != nil {
		t.Fatalf("create db: %v", err)
	}
	if err := conn.Exec(ctx, schema.TableDDL(db, tableName)); err != nil {
		t.Fatalf("apply schema: %v", err)
	}

	insert := func(eventID, sourceTS string, metric string, index uint16, device, deviceID, model, host string, val float64) {
		b, err := conn.PrepareBatch(ctx, "INSERT INTO "+db+"."+tableName)
		if err != nil {
			t.Fatalf("prepare: %v", err)
		}
		_ = b.Append(
			eventID,           // event_id
			time.Now().UTC(),  // received_at (version)
			parseTS(sourceTS), // source_ts (the event time)
			metric,            // metric_name
			index,             // gpu_index
			device,            // device_name
			deviceID,          // device_id
			model,             // model_name
			host,              // hostname
			val,               // value
			uint64(0),         // csv_line_offset
			uint64(1),         // loop_count
			"c1",              // cluster
			"p1",              // pod_name
			int64(0),          // mq_offset
		)
		if err := b.Send(); err != nil {
			t.Fatalf("insert %s: %v", eventID, err)
		}
	}

	// Two GPUs, two timestamps; evt_A_t1 is delivered twice (at-least-once replay).
	insert("evt_A_t1", "2026-09-22T12:00:00.000000Z", "DCGM_FI_DEV_GPU_UTIL", 0, "nvidia0", "GPU-A", "H100", "h1", 10)
	insert("evt_A_t1", "2026-09-22T12:00:00.000000Z", "DCGM_FI_DEV_GPU_UTIL", 0, "nvidia0", "GPU-A", "H100", "h1", 10) // replay
	insert("evt_A_t2", "2026-09-22T12:01:00.000000Z", "DCGM_FI_DEV_MEM_TEMP", 0, "nvidia0", "GPU-A", "H100", "h1", 55)
	insert("evt_B_t2", "2026-09-22T12:01:00.000000Z", "DCGM_FI_DEV_GPU_UTIL", 4, "nvidia4", "GPU-B", "A100", "h2", 99)

	s, err := Open(ctx, host, mustAtoi(port), user, os.Getenv("CLICKHOUSE_PASSWORD"), db, tableName)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer s.Close()

	// Ask for everything the fixture has, so the assertions below stay about
	// query correctness rather than pagination (which has its own tests).
	gpus, gpuTotal, gpusMore, err := s.ListGPUs(ctx, Page{Limit: 1000})
	if err != nil {
		t.Fatalf("ListGPUs: %v", err)
	}
	if gpuTotal != 2 {
		t.Errorf("ListGPUs total = %d want 2", gpuTotal)
	}
	if gpusMore {
		t.Error("ListGPUs hasMore = true on a 2-GPU fixture read in one page")
	}
	if len(gpus) != 2 {
		t.Fatalf("ListGPUs = %d want 2 (%+v)", len(gpus), gpus)
	}
	if gpus[0].ID != "GPU-A" || gpus[0].Model != "H100" || gpus[0].Index != 0 {
		t.Fatalf("GPU-A metadata wrong: %+v", gpus[0])
	}

	evs, evTotal, evMore, err := s.Telemetry(ctx, "GPU-A", nil, nil, Page{Limit: 1000})
	if err != nil {
		t.Fatalf("Telemetry: %v", err)
	}
	if len(evs) != 2 {
		t.Fatalf("Telemetry(GPU-A) = %d want 2 (replay must collapse via FINAL)", len(evs))
	}
	if evs[0].EventID != "evt_A_t1" || evs[1].EventID != "evt_A_t2" {
		t.Fatalf("collapse/order wrong: %+v", evs)
	}
	if !fmtLess(evs[0].SourceTS, evs[1].SourceTS) {
		t.Fatalf("not time-ordered: %q then %q", evs[0].SourceTS, evs[1].SourceTS)
	}
	// The window count must come from the same scan as the rows, not a second
	// query that could disagree with it.
	if evTotal != int64(len(evs)) {
		t.Errorf("total = %d, want %d - the count must match the rows returned",
			evTotal, len(evs))
	}
	if evMore {
		t.Error("hasMore = true, but the whole fixture fit in one page of 1000")
	}

	// Pagination: page 1 of 1 row, then an offset past the end still reports
	// the true total so a caller can recover from over-paging.
	one, oneTotal, oneMore, err := s.Telemetry(ctx, "GPU-A", nil, nil, Page{Limit: 1})
	if err != nil {
		t.Fatalf("Telemetry page 1: %v", err)
	}
	if len(one) != 1 || one[0].EventID != "evt_A_t1" || oneTotal != 2 {
		t.Errorf("page(limit=1) = %+v total=%d, want evt_A_t1 and total 2", one, oneTotal)
	}
	if !oneMore {
		t.Error("hasMore = false on page 1 of 2, so a client would stop one row early")
	}
	// Cursor paging is what the next link uses, so it is the path that has to be
	// exact against a real database. Walk the fixture with a cursor and require
	// every row exactly once.
	cur := Page{Limit: 1}
	seen := []string{}
	more := true
	for more && len(seen) < 10 {
		rows, _, m, err := s.Telemetry(ctx, "GPU-A", nil, nil, cur)
		if err != nil {
			t.Fatalf("cursor walk: %v", err)
		}
		for _, r := range rows {
			seen = append(seen, r.EventID)
		}
		more = m
		if more && len(rows) > 0 {
			last := rows[len(rows)-1]
			cur = Page{Limit: 1, After: EncodeCursor(last.SourceTS, last.EventID)}
		}
	}
	if more {
		t.Fatal("cursor walk did not terminate")
	}
	if strings.Join(seen, ",") != "evt_A_t1,evt_A_t2" {
		t.Errorf("cursor walk saw %v, want exactly evt_A_t1,evt_A_t2", seen)
	}

	two, _, twoMore, err := s.Telemetry(ctx, "GPU-A", nil, nil, Page{Limit: 1, Offset: 1})
	if err != nil {
		t.Fatalf("Telemetry page 2: %v", err)
	}
	if len(two) != 1 || two[0].EventID != "evt_A_t2" {
		t.Errorf("page(offset=1) = %+v, want evt_A_t2 - pages must not overlap", two)
	}
	if twoMore {
		t.Error("hasMore = true on the final page; next would never be null")
	}
	none, noneTotal, noneMore, err := s.Telemetry(ctx, "GPU-A", nil, nil, Page{Limit: 1, Offset: 99})
	if err != nil {
		t.Fatalf("Telemetry past the end: %v", err)
	}
	if len(none) != 0 || noneTotal != 2 {
		t.Errorf("offset past end = %d rows total=%d, want 0 rows and total 2", len(none), noneTotal)
	}
	if noneMore {
		t.Error("hasMore = true past the end of the data")
	}

	t1 := time.Date(2026, 9, 22, 12, 0, 0, 0, time.UTC)
	t2 := time.Date(2026, 9, 22, 12, 1, 0, 0, time.UTC)

	// Inclusive single-instant window: only evt_A_t1.
	in, _, _, err := s.Telemetry(ctx, "GPU-A", &t1, &t1, Page{Limit: 1000})
	if err != nil {
		t.Fatalf("Telemetry window: %v", err)
	}
	if len(in) != 1 || in[0].EventID != "evt_A_t1" {
		t.Fatalf("window [t1,t1] = %+v want just evt_A_t1", in)
	}

	// Full span window: both GPU-A events, ascending.
	both, _, _, err := s.Telemetry(ctx, "GPU-A", &t1, &t2, Page{Limit: 1000})
	if err != nil {
		t.Fatalf("Telemetry full window: %v", err)
	}
	if len(both) != 2 || both[0].EventID != "evt_A_t1" || both[1].EventID != "evt_A_t2" {
		t.Fatalf("window [t1,t2] = %+v", both)
	}

	// Empty window must return zero rows (not an error).
	t0 := time.Date(2026, 9, 22, 10, 0, 0, 0, time.UTC)
	none, _, _, err = s.Telemetry(ctx, "GPU-A", &t0, &t0, Page{Limit: 1000})
	if err != nil {
		t.Fatalf("Telemetry empty: %v", err)
	}
	if len(none) != 0 {
		t.Fatalf("window [t0,t0] returned %d rows", len(none))
	}
}

// Descending paging inverts the resume comparison to "<" and adds DESC to the
// ORDER BY. The unit tests cover the logic against a fake, but only a real
// ClickHouse proves the tuple comparison
//
//	(source_ts, event_id) < (parseDateTime64BestEffort(...), ...)
//
// parses and behaves as a tuple, and that the surplus-row trick still reports
// hasMore correctly when the sort runs the other way.
func TestStoreDescendingTelemetry(t *testing.T) {
	host := os.Getenv("CLICKHOUSE_HOST")
	if host == "" {
		t.Skip("set CLICKHOUSE_HOST (and CLICKHOUSE_PORT/USER/PASSWORD) to run")
	}
	ctx := context.Background()
	port := envDefault(os.Getenv("CLICKHOUSE_PORT"), "9000")
	user := envDefault(os.Getenv("CLICKHOUSE_USER"), "default")

	const db = "telemetry_it"
	tableName := fmt.Sprintf("events_desc_%d", time.Now().UnixNano())
	conn, err := clickhouse.Open(&clickhouse.Options{
		Addr: []string{host + ":" + port},
		Auth: clickhouse.Auth{Username: user, Password: os.Getenv("CLICKHOUSE_PASSWORD")},
	})
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer func() { _ = conn.Exec(ctx, "DROP TABLE IF EXISTS "+db+"."+tableName); conn.Close() }()
	if err := conn.Exec(ctx, "CREATE DATABASE IF NOT EXISTS "+db); err != nil {
		t.Fatalf("create db: %v", err)
	}
	if err := conn.Exec(ctx, schema.TableDDL(db, tableName)); err != nil {
		t.Fatalf("apply schema: %v", err)
	}

	// Six rows, two per timestamp, so the event_id tiebreaker in the resume key
	// is exercised in the descending direction too.
	for i := 0; i < 3; i++ {
		for _, suffix := range []string{"a", "b"} {
			id := fmt.Sprintf("e%d%s", i, suffix)
			b, err := conn.PrepareBatch(ctx, "INSERT INTO "+db+"."+tableName)
			if err != nil {
				t.Fatalf("prepare: %v", err)
			}
			_ = b.Append(
				id, time.Now().UTC(),
				parseTS("2026-09-22T12:0"+fmt.Sprint(i)+":00.000000Z"),
				"DCGM_FI_DEV_GPU_UTIL", 0, "nvidia0", "GPU-D", "H100", "h1", float64(i),
				uint64(0), uint64(1), "c1", "p1", int64(0),
			)
			if err := b.Send(); err != nil {
				t.Fatalf("insert %s: %v", id, err)
			}
		}
	}

	s, err := Open(ctx, host, mustAtoi(port), user, os.Getenv("CLICKHOUSE_PASSWORD"), db, tableName)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer s.Close()

	// Page size 2 forces four pages, so the resume predicate runs for real.
	var (
		seen  []string
		pages int
		page  = Page{Limit: 2, Desc: true}
	)
	for {
		evs, total, more, err := s.Telemetry(ctx, "GPU-D", nil, nil, page)
		if err != nil {
			t.Fatalf("Telemetry page %d: %v", pages+1, err)
		}
		if total != 6 {
			t.Errorf("page %d total = %d, want 6 on every page", pages+1, total)
		}
		for _, e := range evs {
			seen = append(seen, e.EventID)
		}
		pages++
		if !more {
			break
		}
		if pages > 10 {
			t.Fatal("descending walk never terminated")
		}
		last := evs[len(evs)-1]
		page = Page{Limit: 2, Desc: true, After: EncodeTelemetryCursor(last.SourceTS, last.EventID, true)}
	}

	// Newest first, and the tiebreaker reversed within a timestamp.
	want := []string{"e2b", "e2a", "e1b", "e1a", "e0b", "e0a"}
	if len(seen) != len(want) {
		t.Fatalf("descending walk returned %d rows, want %d: %v", len(seen), len(want), seen)
	}
	for i := range want {
		if seen[i] != want[i] {
			t.Fatalf("descending order wrong at %d: got %v, want %v", i, seen, want)
		}
	}

	// A cursor from an ascending walk must be refused, not silently walked
	// backwards: the comparison operator differs, so honouring it would return
	// rows the client has already read.
	asc, _, _, err := s.Telemetry(ctx, "GPU-D", nil, nil, Page{Limit: 2})
	if err != nil {
		t.Fatalf("ascending page: %v", err)
	}
	ascCursor := EncodeTelemetryCursor(asc[len(asc)-1].SourceTS, asc[len(asc)-1].EventID, false)
	if _, _, _, err := s.Telemetry(ctx, "GPU-D", nil, nil, Page{Limit: 2, Desc: true, After: ascCursor}); !errors.Is(err, ErrInvalidCursor) {
		t.Errorf("ascending cursor on a descending page: err = %v, want ErrInvalidCursor", err)
	}

	// Same for the GPU list.
	if gpus, _, _, err := s.ListGPUs(ctx, Page{Limit: 10, Desc: true}); err != nil {
		t.Fatalf("ListGPUs desc: %v", err)
	} else if len(gpus) != 1 || gpus[0].ID != "GPU-D" {
		t.Errorf("ListGPUs desc = %+v, want the single GPU-D", gpus)
	}
}

func TestStoreWindowUsesSourceNotIngest(t *testing.T) {
	host := os.Getenv("CLICKHOUSE_HOST")
	if host == "" {
		t.Skip("set CLICKHOUSE_HOST (and CLICKHOUSE_PORT/USER/PASSWORD) to run")
	}
	ctx := context.Background()
	port := envDefault(os.Getenv("CLICKHOUSE_PORT"), "9000")
	user := envDefault(os.Getenv("CLICKHOUSE_USER"), "default")

	const db = "telemetry_it"
	tableName := fmt.Sprintf("events_late_%d", time.Now().UnixNano())
	conn, err := clickhouse.Open(&clickhouse.Options{
		Addr: []string{host + ":" + port},
		Auth: clickhouse.Auth{Username: user, Password: os.Getenv("CLICKHOUSE_PASSWORD")},
	})
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer func() { _ = conn.Exec(ctx, "DROP TABLE IF EXISTS "+db+"."+tableName); conn.Close() }()
	if err := conn.Exec(ctx, "CREATE DATABASE IF NOT EXISTS "+db); err != nil {
		t.Fatalf("create db: %v", err)
	}
	if err := conn.Exec(ctx, schema.TableDDL(db, tableName)); err != nil {
		t.Fatalf("apply schema: %v", err)
	}

	// One late-arriving event: it happened at measuredAt (its source_ts), but the
	// collector only wrote it hours later at ingestedAt (received_at).
	const (
		measuredAt = "2026-09-22T12:00:00.000000Z"
		ingestedAt = "2026-09-22T18:00:00.000000Z"
	)
	b, err := conn.PrepareBatch(ctx, "INSERT INTO "+db+"."+tableName+
		" (event_id, received_at, source_ts, metric_name, gpu_index, device_name, device_id,"+
		" model_name, hostname, value, csv_line_offset, loop_count, cluster, pod_name, mq_offset)")
	if err != nil {
		t.Fatalf("prepare: %v", err)
	}
	_ = b.Append(
		"evt_late", parseTS(ingestedAt), // event_id, received_at = write time (outside the queried window)
		parseTS(measuredAt), // source_ts = event time (inside the window)
		"DCGM_FI_DEV_GPU_UTIL", uint16(0), "nvidia0", "GPU-LATE",
		"H100", "h1", 42.0, uint64(0), uint64(1), "c1", "p1", int64(0),
	)
	if err := b.Send(); err != nil {
		t.Fatalf("insert late event: %v", err)
	}

	s, err := Open(ctx, host, mustAtoi(port), user, os.Getenv("CLICKHOUSE_PASSWORD"), db, tableName)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer s.Close()

	measured := parseTS(measuredAt)
	ingested := parseTS(ingestedAt)

	// The window that contains the event time (source_ts) must return the event,
	// even though the collector wrote it (received_at) hours outside the window.
	in, _, _, err := s.Telemetry(ctx, "GPU-LATE", &measured, &measured, Page{Limit: 1000})
	if err != nil {
		t.Fatalf("Telemetry measurement window: %v", err)
	}
	if len(in) != 1 || in[0].EventID != "evt_late" {
		t.Fatalf("measurement window [%s] = %+v, want evt_late", measuredAt, in)
	}
	if !parseTS(in[0].SourceTS).Equal(measured) {
		t.Errorf("SourceTS = %q, want %s", in[0].SourceTS, measuredAt)
	}

	// A window matching only the WRITE time must find nothing: the filter is on
	// source_ts, so a received_at window cannot smuggle the row back in.
	out, _, _, err := s.Telemetry(ctx, "GPU-LATE", &ingested, &ingested, Page{Limit: 1000})
	if err != nil {
		t.Fatalf("Telemetry ingest window: %v", err)
	}
	if len(out) != 0 {
		t.Fatalf("ingest window [%s] returned %d rows, want 0 (filter must be on source_ts)", ingestedAt, len(out))
	}
}

func parseTS(s string) time.Time {
	t, _ := time.Parse(time.RFC3339Nano, s)
	return t
}

func fmtLess(a, b string) bool {
	ta, ea := time.Parse(time.RFC3339Nano, a)
	tb, eb := time.Parse(time.RFC3339Nano, b)
	return ea == nil && eb == nil && ta.Before(tb)
}

func envDefault(v, def string) string {
	if v != "" {
		return v
	}
	return def
}

func mustAtoi(s string) int {
	n := 0
	for _, c := range s {
		if c < '0' || c > '9' {
			continue
		}
		n = n*10 + int(c-'0')
	}
	return n
}
