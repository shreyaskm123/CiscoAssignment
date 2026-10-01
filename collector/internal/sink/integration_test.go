//go:build integration

// Integration test against a real ClickHouse. Skipped by default; run with:
//
//	CLICKHOUSE_HOST=localhost go test -tags integration ./internal/sink/ -run TestClickHouse
//
// (see README for spinning up a local ClickHouse with docker)
package sink

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"

	mqpb "streamer/proto"
)

func TestClickHouseIdempotentWrites(t *testing.T) {
	host := os.Getenv("CLICKHOUSE_HOST")
	if host == "" {
		t.Skip("set CLICKHOUSE_HOST (and CLICKHOUSE_PORT/USER/PASSWORD) to run")
	}
	ctx := context.Background()

	port := orDefault(os.Getenv("CLICKHOUSE_PORT"), "9000")
	user := orDefault(os.Getenv("CLICKHOUSE_USER"), "default")
	const db = "telemetry_it"
	table := fmt.Sprintf("events_it_%d", time.Now().UnixNano())

	conn, err := clickhouse.Open(&clickhouse.Options{
		Addr: []string{host + ":" + port},
		Auth: clickhouse.Auth{
			Username: user,
			Password: os.Getenv("CLICKHOUSE_PASSWORD"),
		},
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer func() {
		_ = conn.Exec(ctx, "DROP TABLE IF EXISTS "+db+"."+table)
		conn.Close()
	}()

	// The schema is bound to the <db>.<table> pair; point at a scratch db.
	if err := conn.Exec(ctx, "CREATE DATABASE IF NOT EXISTS "+db); err != nil {
		t.Fatalf("create db: %v", err)
	}

	s, err := OpenClickHouse(ctx, host, mustAtoi(port), user, "", db, table, "", false)
	if err != nil {
		t.Fatalf("open sink: %v", err)
	}
	defer s.Close()

	ev := &mqpb.Event{
		EventId: "evt_k8s-pod-0_L123_O456", SourceTimestamp: "2026-09-22T12:00:00.000000Z",
		MetricName: "DCGM_FI_DEV_GPU_UTIL", GpuIndex: 0, DeviceName: "nvidia0",
		DeviceId: "GPU-x", ModelName: "H100", Hostname: "h", Value: 42,
		CsvLineOffset: 456, LoopCount: 123,
		Tags: map[string]string{"cluster": "c", "pod_name": "k8s-pod-0"},
	}

	// Two deliveries of the same event (at-least-once replay) -> two writes.
	for i := 0; i < 2; i++ {
		if err := s.Write(ctx, []Event{{Event: ev, Offset: 10}}); err != nil {
			t.Fatalf("write: %v", err)
		}
	}

	qualified := db + "." + table
	var raw, final uint64
	if err := conn.QueryRow(ctx, "SELECT count() FROM "+qualified).Scan(&raw); err != nil {
		t.Fatalf("raw count: %v", err)
	}
	if err := conn.QueryRow(ctx, "SELECT count() FROM "+qualified+" FINAL").Scan(&final); err != nil {
		t.Fatalf("final count: %v", err)
	}
	if final != 1 {
		t.Fatalf("FINAL select returned %d rows for one event_id, want 1 (replay must collapse to one)", final)
	}
	t.Logf("raw rows=%d, FINAL rows=%d (idempotent collapse verified)", raw, final)
}

func orDefault(v, def string) string {
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
