# collector

Consumer fleet (sink side): reads the MQ log shard owned by this replica and writes it to
ClickHouse. Fail-open by design — it never wedges the pipeline.

## Deploy
- Kind: Deployment, 1 replica today (chart `collector`).
- Group `collector` (independent registry + cursors from the streamer group).
- Writes CH native :9000 via Service `telemetry-clickhouse`.

## Configuration (env)

| Env | Default | Meaning |
|---|---|---|
| `MQ_ADDR` | `localhost:50051` | MQ gRPC address |
| `MQ_TOKEN_FILE` | `""` | bearer-token file for MQ auth, re-read on every RPC |
| `MQ_TOKEN` | `""` | inline token, used only when `MQ_TOKEN_FILE` is unset |
| `CLICKHOUSE_CLUSTER` | `""` | create the schema `ON CLUSTER <name>` with `ReplicatedReplacingMergeTree`; the chart sets `telemetry` |
| `CONSUMER_ID` | `collector-0` | Cursor identity (identifier-regex validated). **The chart sets `collector`** — a fixed value, so all replicas join under one id (G10b) |
| `REGISTRY_TTL_S` | `15` | heartbeat TTL |
| `REGISTRY_POLL_INTERVAL_MS` | `5000` | join/heartbeat cadence |
| `CONSUME_POLL_INTERVAL_MS` | `500` | poll after the log tail is reached. **The chart sets `200`**, so deployed pods use 200ms |
| `BATCH_SIZE` | `5000` | events per insert batch |
| `FLUSH_INTERVAL_MS` | `500` | max time a partial batch waits before flush. **The chart sets `300`** |
| `INSERT_RETRIES` | `3` | CH insert **attempts** per batch before dead-letter (1 initial + 2 retries) |
| `INSERT_BACKOFF_MS` | `500` | delay between insert retries (flat today → G2) |
| `SHUTDOWN_GRACE_MS` | `5000` | drain bound on shutdown |
| `DEDUP_PRE_CHECK` | `false` | pre-insert event_id existence check (optimization) |
| `HEALTH_ADDR` | `:8082` | listen address for `/healthz` and `/readyz` (empty disables) |
| `READY_MAX_STALL_SECONDS` | `300` | no read/write/commit for this long ⇒ NotReady; `0` disables |
| `DEAD_LETTER_DIR` | `os.TempDir()` | fail-open JSONL side-channel. **The chart sets `/var/lib/collector/dlq`** on an `emptyDir`, so dead letters do not survive a pod restart (G1) |
| `SINK` | `clickhouse` | `clickhouse` \| `stdout` (stdout = lab receipts) |
| `CLICKHOUSE_HOST` | `localhost` | CH host |
| `CLICKHOUSE_PORT` | `9000` | CH native port |
| `CLICKHOUSE_USER` | `default` | CH user |
| `CLICKHOUSE_PASSWORD` | — | CH password (empty = no auth) |
| `CLICKHOUSE_DB` | `telemetry` | Database |
| `CLICKHOUSE_TABLE` | `events` | Table (auto-created, idempotent DDL) |

## Probes

Both the collector and the streamer serve two endpoints for the kubelet:

| Endpoint | Probe | Answers |
|---|---|---|
| `GET /healthz` | liveness | is the process still serving? Deliberately dependency-free |
| `GET /readyz` | readiness | can it do its job right now, and if not, why |

`/readyz` returns the whole assessment as JSON, including counters, so the
numbers needed to explain a `NotReady` are available from the probe itself:

```bash
kubectl -n telemetry port-forward deploy/telemetry-collector 8082:8082
curl -s localhost:8082/readyz | python3 -m json.tool
```

```json
{"ready": true,
 "fields": {"consumer": "collector", "shard": "0/1", "cursor_lag": "0",
            "committed_offset": "74213", "last_read_offset": "74213",
  "events_written_total": "33589", "events_skipped_total": "0",
  "write_failures_total": "0",
            "dead_letter_rows_total": "0", "last_progress_age_seconds": "0"}}
```

### Why liveness does not check dependencies

If `/healthz` failed when ClickHouse or the MQ was unavailable, every replica
would restart at once and a dependency outage would become a crash loop. So
liveness reports only that the process is up, and everything dependency-shaped is
on `/readyz`.

### What makes a pod NotReady

Only conditions that clear themselves, so a pod recovers without a restart as
soon as the cause passes:

- **no shard assignment** from the MQ registry: the replica does not yet know
  which rows or offsets are its responsibility;
- **a failure in flight**: the last batch could not be written, or the last
  publish failed;
- **no progress for `READY_MAX_STALL_SECONDS`** (default 300, `0` disables).

The stall check is the important one. A message queue that stops acknowledging
raises no error anywhere: the streamer blocks in `Send`, the collector blocks in
`Recv`, and the pipeline goes quiet while every pod still reports `1/1 Running`.
That is not hypothetical — this stack once ran at 0.2% of its normal rate for
about nine hours with nothing to show for it. Only a clock notices.

Dead-lettered events are reported but deliberately do **not** gate readiness: that
count only grows, so gating on it would hold a pod `NotReady` forever after
ClickHouse recovered and would hang the next rolling update.

### Measured behaviour

With `READY_MAX_STALL_SECONDS=30` and the MQ scaled to zero: the collector went
`0/1` at t+60s, the streamers at t+90s, each reporting `no progress for Ns` and
`no acknowledged publish for Ns`. With the MQ restored they returned to `1/1` by
t+50-80s **with 0 restarts** — the recovery a restart would have achieved, without
the disruption.

### What this does not do

It detects a stall; it does not fix one. A stalled MQ follower will be reported,
not repaired — that still needs the leader-side lag work in
`docs/failure-modes.md` (G6b). It also cannot see a pipeline that is *slow* rather
than stopped: if events keep flowing, just at a fraction of the expected rate,
every check stays green. Catching that needs a rate or lag alert.

## Behavior / internals
- **Startup**: dials MQ, opens CH (single ping + DDL — no retry today → **G3**), waits on the
  configMap watcher, then reads its committed cursor (`GetOffset`; fresh consumer → `-1` = head).
- **Consume loop**: re-opens `Consume` from the committed cursor on every stream end/error;
  applies its shard filter (`offset % total == index`); flushes on size or interval.
- **Flush**: `INSERT_RETRIES` attempts with `INSERT_BACKOFF_MS`, then dead-letter to
  `DEAD_LETTER_DIR/collector_<consumerID>_dead_letter.jsonl` and **advances the cursor** (fail-open).
  The directory is created on demand and the file is fsynced. If the dead-letter write
  itself fails (disk full, unwritable volume) the batch is stored nowhere, so the
  cursor is **held**, not advanced: the collector re-reads from the committed cursor
  and replays the batch once the sink or the dead-letter can take it.
  The clickhouse-go driver auto-reconnects on the next call, so brief CH restarts self-heal.
- **Shutdown**: final flush runs on a **fresh** `context.WithTimeout(background, ShutdownGrace)` —
  the last buffered batch is written, not dropped.
- **Dedup strategy**: storage-side voltage — `ReplacingMergeTree` versioned by `received_at`
  collapses any redelivered `event_id`; optional pre-insert check avoids writing common duplicates.
- **Schema**: the events table has one event timestamp, `source_ts` (the streamer's current UTC
  time; the CSV timestamp is never propagated), plus `received_at` (write time, the dedup version).
  The table is keyed `(device_id, source_ts, event_id)` and partitioned by day on `source_ts`.
  Startup runs the idempotent table DDL, then refuses to start if it finds a table from the old
  layout (one that still has a `ts` column): sort and partition keys cannot be altered in place,
  so the fix is `DROP TABLE telemetry.events` and a collector restart. Inserts use an explicit
  column list.

## Safety properties
- At-least-once end-to-end; cursor only advances after a durable write (or a dead-letter, G1 caveat).
- Never silently drops: an unwritable CH results in dead-letter + cursor advance (not a stall) — **unless the dead-letter write itself fails**, in which case the cursor is held and the batch is replayed, never dropped, but if the
  dead-letter write fails too, the cursor is held and the batch is replayed.
- Concurrency: single goroutine reader + the registry heartbeat; no shared mutable state races.

## Known gaps
- **G1 (critical)**: dead-letter files are on ephemeral pod disk; pod restart during a CH outage
  loses them (cursor already advanced). Fix = PVC + startup replay (idempotent).
- **G2**: flat 3×500ms insert retries; want exponential backoff before giving up.
- **G3**: no boot retry — crash-loops until CH/table exists (add initContainer or backoff).
- **G4**: CH password from `CLICKHOUSE_PASSWORD` plaintext env; auth currently disabled (secret-ify).
- **G11**: scaling to >1 replica is supported by design (registry rank = shard) but no HPA yet.

## How to check it's healthy
```sql
SELECT count(), max(mq_offset), uniqExact(event_id) FROM telemetry.events FINAL;
-- collector group drives retention: MQ logs show "retention trim …" only when it's committing.
```