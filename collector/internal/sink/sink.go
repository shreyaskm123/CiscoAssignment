// Package sink writes events to ClickHouse.
//
// Deduplication strategy (per the agreed design): ClickHouse is append-only and
// has no row-level unique constraint, so duplicates from at-least-once replay
// are collapsed at the storage layer with ReplacingMergeTree. The sort key
// (device_id, source_ts, event_id) keeps event_id as the unique dedup dimension (each
// logical event is exactly one key group) while ordering rows by GPU and time so
// the query API can serve GPU + time-window reads with prefix scans. The
// version column is received_at, so a redelivered event replaces the old row.
// An optional pre-insert check (DEDUP_PRE_CHECK) filters batches against
// already-present event_ids to avoid even writing the common duplicates; it is
// an optimization only — the merge tree stays the authoritative guard.
package sink

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"

	"collector/schema"
	mqpb "streamer/proto"
)

// Event is an event plus its MQ log offset, ready for a writer.
type Event struct {
	Event  *mqpb.Event
	Offset int64
}

// Writer is the storage-facing side of the sink. The collector owns batching
// (size + interval); Write persists one already-full batch.
type Writer interface {
	Write(ctx context.Context, batch []Event) error
}

// ClickHouseSink writes batches to a ReplacingMergeTree table.
type ClickHouseSink struct {
	conn       driver.Conn
	table      string
	dedupCheck bool
}

// OpenClickHouse connects, creates the (idempotent) schema, and returns a sink.
//
// With a non-empty cluster the schema is created ON CLUSTER with the replicated
// engine, so every replica gets the database and table. With an empty cluster
// it is a plain single-server table.
func OpenClickHouse(ctx context.Context, host string, port int, user, password, db, table, cluster string, dedupCheck bool) (*ClickHouseSink, error) {
	if !schema.ValidIdentifier(db) || !schema.ValidIdentifier(table) {
		return nil, &invalidIdentifierError{db: db, table: table}
	}
	if cluster != "" && !schema.ValidIdentifier(cluster) {
		return nil, fmt.Errorf("invalid CLICKHOUSE_CLUSTER %q: must be identifier-safe", cluster)
	}
	conn, err := clickhouse.Open(&clickhouse.Options{
		Addr:        []string{netJoinHostPort(host, port)},
		Auth:        clickhouse.Auth{Username: user, Password: password},
		DialTimeout: 10 * time.Second,
		Compression: &clickhouse.Compression{Method: clickhouse.CompressionLZ4},
	})
	if err != nil {
		return nil, err
	}
	if err := conn.Ping(ctx); err != nil {
		return nil, err
	}
	// The collector owns the schema: make the database, then the (idempotent)
	// table, so a fresh ClickHouse works with no manual DDL step.
	if cluster == "" {
		if err := conn.Exec(ctx, schema.CreateDatabase(db)); err != nil {
			return nil, err
		}
		if err := conn.Exec(ctx, schema.TableDDL(db, table)); err != nil {
			return nil, err
		}
	} else {
		// ON CLUSTER DDL is queued in Keeper and run by every replica. If a
		// replica is down, waiting for it would block the collector's start
		// (and with it all ingestion) for the full DDL timeout, even though the
		// surviving replica is perfectly able to take writes. So wait a bounded
		// time, do not fail on the replicas that did not answer (they run the
		// statement when they come back), and verify the table below on the
		// replica we are actually connected to.
		ddl := clickhouse.Context(ctx, clickhouse.WithSettings(clickhouse.Settings{
			"distributed_ddl_task_timeout": 60,
			"distributed_ddl_output_mode":  "never_throw",
		}))
		if err := conn.Exec(ddl, schema.CreateDatabaseOnCluster(db, cluster)); err != nil {
			return nil, fmt.Errorf("create database on cluster %s: %w", cluster, err)
		}
		if err := conn.Exec(ddl, schema.ReplicatedTableDDL(db, table, cluster)); err != nil {
			return nil, fmt.Errorf("create table on cluster %s: %w", cluster, err)
		}
		var engine string
		if err := conn.QueryRow(ctx, schema.TableEngine(db, table)).Scan(&engine); err != nil {
			return nil, fmt.Errorf("table %s is not present on the replica we are connected to after ON CLUSTER DDL "+
				"(is a replica or Keeper unavailable?): %w", schema.Qualified(db, table), err)
		}
		if !strings.HasPrefix(engine, "Replicated") {
			return nil, fmt.Errorf("table %s exists with engine %s, not a Replicated engine, so it would not replicate; "+
				"DROP TABLE %s ON CLUSTER %s SYNC and restart the collector so it recreates it",
				schema.Qualified(db, table), engine, schema.Qualified(db, table), cluster)
		}
	}
	var legacy uint64
	if err := conn.QueryRow(ctx, schema.LegacyTimestampColumnCount(db, table)).Scan(&legacy); err != nil {
		return nil, err
	}
	if legacy > 0 {
		return nil, fmt.Errorf("table %s has the legacy `ts` column (old layout sorted and partitioned by ts); "+
			"DROP TABLE %s and restart the collector so it recreates the table keyed on source_ts",
			schema.Qualified(db, table), schema.Qualified(db, table))
	}
	return &ClickHouseSink{conn: conn, table: schema.Qualified(db, table), dedupCheck: dedupCheck}, nil
}

func (s *ClickHouseSink) Close() error { return s.conn.Close() }

// Write inserts the batch as a single prepared batch. Each event's received_at
// is the version column, so ReplacingMergeTree keeps the latest delivery.
func (s *ClickHouseSink) Write(ctx context.Context, batch []Event) error {
	if len(batch) == 0 {
		return nil
	}
	if s.dedupCheck {
		batch = s.filterExisting(ctx, batch)
		if len(batch) == 0 {
			return nil
		}
	}

	b, err := s.conn.PrepareBatch(ctx, "INSERT INTO "+s.table+" (event_id, received_at, source_ts, metric_name, gpu_index, device_name, device_id, model_name, hostname, value, csv_line_offset, loop_count, cluster, pod_name, mq_offset)")
	if err != nil {
		return err
	}
	now := time.Now().UTC()
	for _, ev := range batch {
		sourceTS := parseTimestamp(ev.Event.SourceTimestamp, now)
		if err := b.Append(
			ev.Event.EventId,
			now,
			sourceTS,
			ev.Event.MetricName,
			int16(ev.Event.GpuIndex),
			ev.Event.DeviceName,
			ev.Event.DeviceId,
			ev.Event.ModelName,
			ev.Event.Hostname,
			ev.Event.Value,
			uint64(ev.Event.CsvLineOffset),
			uint64(ev.Event.LoopCount),
			ev.Event.Tags["cluster"],
			ev.Event.Tags["pod_name"],
			ev.Offset,
		); err != nil {
			return err
		}
	}
	return b.Send()
}
