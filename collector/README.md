# collector

The consumer side of the telemetry pipeline. Every `collector` replica owns a
shard of the MQ event log (`offset % total == index`, where `(index,total)`
comes from the MQ partition registry — the same machinery the streamer fleet
uses), reads its shard, batches, and writes durably to **ClickHouse**. Cursor
advancement is ack-driven: it commits only after the batch is written, so a
crash replays (and ClickHouse dedups the replay).

## Data flow

```
MQ event log ──(Consume from committed cursor)──► shard filter ──► batch buffer
                                                                    │ size / interval
                                                                    ▼
                                             ClickHouse INSERT (ReplacingMergeTree)
                                                                    │ ok
                                                                    ▼
                                            CommitOffset(cursor)  ── crash recovers here
```

> **Caveat — fail-open is not lossless.** If the ClickHouse insert keeps
> failing, the batch is written to a JSONL dead-letter file on ephemeral pod disk
> and the cursor advances anyway, so those rows never reach `telemetry.events`
> (see `docs/failure-modes.md` G1). The cursor is held only if the dead-letter
> write itself fails.

## Idempotency (three layers)

1. MQ `event_id` dedup — producer side.
2. Collector cursor — `GetOffset`/`CommitOffset`, advanced **after** the
   ClickHouse write is durable. That ordering makes delivery at-least-once: a
   crash between the two replays the batch, and layer 3 collapses the replay.
   It is **not** exactly-once — see the fail-open caveat below.
3. ClickHouse `ReplacingMergeTree(received_at)`, sort key `(device_id, source_ts,
   event_id)` — `event_id` is globally unique, so each key group is exactly the
   duplicate deliveries of one event, and `SELECT ... FINAL` (or background
   merges) collapse them to one row. The same key also sorts storage by GPU and
   time, so the query API (`/api/v1/gpus`, `/api/v1/gpus/{id}/telemetry`) reads
   with prefix range scans.

An optional `DEDUP_PRE_CHECK` filters each batch against already-present
`event_id`s before inserting (reduces physical duplicate writes; the merge tree
is retained as the authoritative guard).

## Run

```sh
# 1. message queue with the consumer contract (Consume/CommitOffset) on :50051
# 2. ClickHouse (quick start, docker)
docker run -d --name ch -p 8123:8123 -p 9000:9000 \
  clickhouse/clickhouse-server
# 3. the collector
MQ_ADDR=localhost:50051 CONSUMER_ID=collector-0 \
CLICKHOUSE_HOST=localhost go run ./cmd/collector
```

Every replica needs a distinct `CONSUMER_ID`; the registry assigns unique
indices automatically, and scale up/down is detected via heartbeat + TTL.

## Environment variables

| Variable | Default | Meaning |
|---|---|---|
| `MQ_TOKEN_FILE` | `""` | Bearer-token file for MQ auth, re-read per RPC |
| `MQ_TOKEN` | `""` | Inline token, used only when `MQ_TOKEN_FILE` is unset |
| `MQ_ADDR` | `localhost:50051` | gRPC endpoint of the message queue |
| `CONSUMER_ID` | `collector-0` | Identity for shard rank + committed cursor. **The chart sets `collector`** — a fixed value, so every replica joins under one id (G10b) |
| `REGISTRY_TTL_S` | `15` | Heartbeat TTL in the registry |
| `REGISTRY_POLL_INTERVAL_MS` | `5000` | Heartbeat cadence / rebalance detection |
| `CONSUME_POLL_INTERVAL_MS` | `500` | Wait between log-tail polls |
| `BATCH_SIZE` | `5000` | Rows per ClickHouse insert |
| `FLUSH_INTERVAL_MS` | `500` | Max buffering before a flush |
| `INSERT_RETRIES` | `3` | Attempts per failed batch (1 initial + 2 retries), then dead-letter |
| `INSERT_BACKOFF_MS` | `500` | Retry backoff |
| `SHUTDOWN_GRACE_MS` | `5000` | Drain bound on shutdown |
| `DEDUP_PRE_CHECK` | `false` | Optional batch `event_id` filter before INSERT |
| `DEAD_LETTER_DIR` | `$TMPDIR` | JSONL side channel for undeliverable batches |
| `CLICKHOUSE_HOST` / `_PORT` / `_USER` / `_PASSWORD` | `localhost`/`9000`/`default`/`` | ClickHouse connection |
| `CLICKHOUSE_DB` / `CLICKHOUSE_TABLE` | `telemetry` / `events` | Sink table (`ReplacingMergeTree`) |
| `SINK` | `clickhouse` | `clickhouse` (production) or `stdout` (demo/lab: prints one `EVENT` receipt line per delivery to stdout and commits, so the whole pipeline can be run — and verified — without ClickHouse) |

## No-ClickHouse lab run (3-component verification)

With `SINK=stdout` the collector prints a parseable receipt per event, e.g.:

```
EVENT log_offset=0 csv_line_offset=0 loop_count=0 event_id="evt_telemetry-streamer-0_L0_O0" metric_name="DCGM_FI_DEV_GPU_UTIL" gpu_index=0 value=0 pod_name="telemetry-streamer-0" source_ts="..."
```

Run the real message queue, one streamer over the metrics CSV, and the collector
with `SINK=stdout`; the full CSV then provably reaches a consumer — every
`csv_line_offset` appears, no deliveries are lost, and the MQ `LOG append`
count equals the collector's `EVENT` count (sent == received). This is exactly
the path the E2E harness under the project exercises; ClickHouse writes remain
integration-tested (`CLICKHOUSE_HOST=localhost go test -tags integration`).

## Persistence guarantees

- **Drain on shutdown:** exit 0; buffered events are flushed before the cursor
  is committed and the registry entry is released (`LeavePartition`).
- **Crash (kill -9):** cursor stays behind the unflushed buffer; restart resumes
  from `GetOffset` and replays — ClickHouse collapses the re-sent rows.
- **ClickHouse hard-down:** a batch is retried, then written to the dead-letter
  JSONL file and the cursor advances (fail-open; the pipeline never wedges). If that
  dead-letter write fails as well, the cursor is held and the batch is replayed
  instead of being dropped.

## Tests

```sh
go test ./... -race                      # unit tests (fakeMQ / fake sink)
CLICKHOUSE_HOST=localhost \
  go test -tags integration ./internal/sink/ -run TestClickHouse   # real CH
```