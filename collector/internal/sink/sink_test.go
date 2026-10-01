package sink

import (
	"context"
	"strings"
	"testing"
	"time"

	"collector/schema"
)

func TestTableDDL(t *testing.T) {
	ddl := schema.TableDDL("telemetry", "events")
	for _, want := range []string{
		"CREATE TABLE IF NOT EXISTS telemetry.events",
		"ENGINE = ReplacingMergeTree(received_at)",
		"ORDER BY (device_id, source_ts, event_id)",
		"PARTITION BY toYYYYMMDD(source_ts)",
		"event_id String",
		"mq_offset Int64",
	} {
		if !strings.Contains(ddl, want) {
			t.Errorf("DDL missing %q:\n%s", want, ddl)
		}
	}
}

// The event time lives in one column, source_ts. A second `ts` column (the old
// layout) must not come back in the DDL.
func TestTableDDLHasNoLegacyTSColumn(t *testing.T) {
	ddl := schema.TableDDL("telemetry", "events")
	for _, line := range strings.Split(ddl, "\n") {
		if strings.HasPrefix(strings.TrimSpace(line), "ts ") {
			t.Errorf("DDL still defines a legacy ts column: %q", line)
		}
	}
	if !strings.Contains(ddl, "source_ts DateTime64(9)") {
		t.Errorf("DDL missing source_ts:\n%s", ddl)
	}
}

func TestLegacyTimestampColumnCountSQL(t *testing.T) {
	q := schema.LegacyTimestampColumnCount("telemetry", "events")
	for _, want := range []string{"system.columns", "database = 'telemetry'", "table = 'events'", "name = 'ts'"} {
		if !strings.Contains(q, want) {
			t.Errorf("legacy check SQL missing %q: %s", want, q)
		}
	}
}

// The replicated DDL must keep exactly the same columns, partitioning and sort
// key as the single-server one (the API's queries depend on them), and differ
// only in the engine and ON CLUSTER.
func TestReplicatedTableDDL(t *testing.T) {
	ddl := schema.ReplicatedTableDDL("telemetry", "events", "telemetry")
	for _, want := range []string{
		"CREATE TABLE IF NOT EXISTS telemetry.events ON CLUSTER telemetry",
		"ENGINE = ReplicatedReplacingMergeTree('/clickhouse/tables/{shard}/telemetry/events', '{replica}', received_at)",
		"PARTITION BY toYYYYMMDD(source_ts)",
		"ORDER BY (device_id, source_ts, event_id)",
		"source_ts DateTime64(9)",
	} {
		if !strings.Contains(ddl, want) {
			t.Errorf("replicated DDL missing %q:\n%s", want, ddl)
		}
	}
	// Same body as the single-server table.
	single := schema.TableDDL("telemetry", "events")
	cols := func(d string) string { return d[strings.Index(d, "(\n")+2 : strings.Index(d, "\n) ENGINE")] }
	if cols(single) != cols(ddl) {
		t.Errorf("replicated and single-server column lists differ:\n%s\nvs\n%s", cols(single), cols(ddl))
	}
	if strings.Contains(ddl, "\n  ts ") {
		t.Errorf("replicated DDL still defines a ts column:\n%s", ddl)
	}
}

func TestCreateDatabaseOnCluster(t *testing.T) {
	if got, want := schema.CreateDatabaseOnCluster("telemetry", "telemetry"), "CREATE DATABASE IF NOT EXISTS telemetry ON CLUSTER telemetry"; got != want {
		t.Errorf("got %q want %q", got, want)
	}
}

func TestOpenClickHouseRejectsBadCluster(t *testing.T) {
	if _, err := OpenClickHouse(context.Background(), "localhost", 9000, "default", "", "telemetry", "events", "c; DROP", false); err == nil {
		t.Error("expected an error for an unsafe cluster name")
	}
}

func TestParseTimestamp(t *testing.T) {
	now := time.Now().UTC()
	cases := []struct {
		in      string
		wantUTC bool // whether we expect parsing to succeed
	}{
		{"2026-09-22T04:43:32.406273Z", true},
		{"2026-09-22T04:43:32Z", true},
		{"garbage", false},
		{"", false},
	}
	for _, c := range cases {
		got := parseTimestamp(c.in, now)
		if c.wantUTC && got.Equal(now) {
			t.Errorf("parseTimestamp(%q) fell back to now, want parsed ts", c.in)
		}
		if !c.wantUTC && !got.Equal(now) {
			t.Errorf("parseTimestamp(%q) = %v, want fallback now", c.in, got)
		}
	}
}

func TestQuoteIn(t *testing.T) {
	got := quoteIn([]string{"evt_a_L1_O2", "evt_b'x", "ok"})
	for _, want := range []string{"'evt_a_L1_O2'", "'evt_b''x'", "'ok'"} {
		if !strings.Contains(got, want) {
			t.Errorf("quoteIn missing %s: %s", want, got)
		}
	}
}

func TestQuoteInEmpty(t *testing.T) {
	if got := quoteIn(nil); got != "" {
		t.Errorf("quoteIn(nil) = %q, want empty", got)
	}
}

func TestOpenClickHouseRejectsBadIdentifiers(t *testing.T) {
	// No network is touched: identifier validation must fail first.
	if _, err := OpenClickHouse(context.Background(), "localhost", 9000, "default", "", "telemetry; DROP", "events", "", false); err == nil {
		t.Fatal("expected error for unsafe db identifier")
	}
	if _, err := OpenClickHouse(context.Background(), "localhost", 9000, "default", "", "telemetry", "events; DROP", "", false); err == nil {
		t.Fatal("expected error for unsafe table identifier")
	}
}
