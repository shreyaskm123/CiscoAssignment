// Package schema defines the single source of truth for the ClickHouse table
// that stores telemetry events. It is shared by the collector (which creates
// the table at startup) and the query API (which read-only serves it).
//
// The sort key is chosen to serve the three read APIs with prefix scans:
//
//	GET /api/v1/gpus                         -> SELECT ... GROUP BY device_id,...
//	GET /api/v1/gpus/{id}/telemetry          -> WHERE device_id = ? ORDER BY source_ts
//	GET /api/v1/gpus/{id}/telemetry?start&end -> WHERE device_id=? AND source_ts BETWEEN ..
//
// ORDER BY (device_id, source_ts, event_id) makes each of those a compact prefix
// range scan (ClickHouse storage is already sorted by source_ts within a GPU, so
// ORDER BY source_ts is satisfied by the index), and the day-level PARTITION BY
// clips time-window queries to the relevant partitions only.
package schema

import (
	"fmt"
	"regexp"
)

var identRE = regexp.MustCompile(`^[a-zA-Z_][a-zA-Z0-9_]*$`)

// ValidIdentifier reports whether db/table names are safe to inline in DDL.
func ValidIdentifier(s string) bool { return identRE.MatchString(s) }

// Qualified returns the db.table name, safe to reference once identity was
// validated with ValidIdentifier.
func Qualified(db, table string) string { return db + "." + table }

// CreateDatabase is the idempotent DDL that makes the events database exist so
// the collector can always self-provision on a fresh server.
func CreateDatabase(db string) string {
	return "CREATE DATABASE IF NOT EXISTS " + db
}

// TableDDL is the idempotent DDL for the events table.
//
// source_ts is the only event timestamp: the streamer's current UTC time when it
// produced the event (the CSV's own timestamp is never propagated). It is set
// once by the streamer and carried unchanged through the MQ, so a redelivered
// event arrives with the same value. The time-window API filters on it.
//
// Engine choice: ReplacingMergeTree(received_at) collapses rows that share the
// ORDER BY key, keeping the row with the greatest version (received_at). The
// key is (device_id, source_ts, event_id); because event_id is globally unique per
// logical event, a group holds exactly the duplicate deliveries of one event,
// so idempotency is preserved (SELECT ... FINAL, or after background merges)
// while the same key also gives the API its efficient access path.
func TableDDL(db, table string) string {
	return fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
%s
) ENGINE = ReplacingMergeTree(received_at)
%s`, Qualified(db, table), tableColumns, tableKeys)
}

// ReplicatedTableDDL is TableDDL for a replicated cluster: the same columns,
// partitioning and sort key, but ReplicatedReplacingMergeTree, created ON
// CLUSTER so every replica gets the table from one statement.
//
// {shard} and {replica} are ClickHouse macros, expanded on each replica from
// its own config (the chart sets them from the pod name), so the one statement
// yields a distinct replica path per node and the same shard path for all of
// them. The Keeper path includes the database and table so several tables can
// share a cluster.
//
// Replication happens between the replicas, not through the writer: an insert
// goes to ONE replica and the others fetch the part. The version column is
// still received_at, and a redelivered event still carries the same
// (device_id, source_ts, event_id) key, so it collapses on every replica.
func ReplicatedTableDDL(db, table, cluster string) string {
	return fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s ON CLUSTER %s (
%s
) ENGINE = ReplicatedReplacingMergeTree('/clickhouse/tables/{shard}/%s/%s', '{replica}', received_at)
%s`, Qualified(db, table), cluster, tableColumns, db, table, tableKeys)
}

// CreateDatabaseOnCluster creates the database on every replica. The replicated
// table can only be created where its database exists.
func CreateDatabaseOnCluster(db, cluster string) string {
	return "CREATE DATABASE IF NOT EXISTS " + db + " ON CLUSTER " + cluster
}

// TableEngine returns SQL selecting the engine of an existing table (no row if
// the table does not exist on the server that answers).
func TableEngine(db, table string) string {
	return fmt.Sprintf("SELECT engine FROM system.tables WHERE database = '%s' AND name = '%s'", db, table)
}

const tableColumns = `  event_id String,
  received_at DateTime64(9),
  source_ts DateTime64(9),
  metric_name String,
  gpu_index UInt16,
  device_name String,
  device_id String,
  model_name String,
  hostname String,
  value Float64,
  csv_line_offset UInt64,
  loop_count UInt64,
  cluster String,
  pod_name String,
  mq_offset Int64`

const tableKeys = `PARTITION BY toYYYYMMDD(source_ts)
ORDER BY (device_id, source_ts, event_id)`

// LegacyTimestampColumnCount returns SQL that counts a `ts` column on an
// existing table. Earlier versions kept a second timestamp, `ts`, and sorted and
// partitioned by it. CREATE TABLE IF NOT EXISTS leaves such a table untouched,
// and the sort and partition keys cannot be altered in place, so the collector
// refuses to start against it instead of writing rows with a wrong key. The
// remedy is to drop the table and let the collector recreate it.
func LegacyTimestampColumnCount(db, table string) string {
	return fmt.Sprintf("SELECT count() FROM system.columns WHERE database = '%s' AND table = '%s' AND name = 'ts'", db, table)
}
