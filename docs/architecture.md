# Architecture

## Components

| Component | Role | Deploy |
|---|---|---|
| `streamer` | Reads the configMap-mounted metrics CSV, assigns each row to a replica, publishes events over gRPC to the MQ leader. Stateless fleet (3 replicas). | Deployment |
| `messagequeue` | Custom durable log + replicated broker: WAL, follower replication, dedup, per-consumer cursors, retention trim. 2 replicas; one is the elected leader at any time. | Single StatefulSet of role-agnostic replicas |
| `collector` | Consumes the log shard owned by this replica, batches events, writes to ClickHouse. 1 replica today (shard 0). | Deployment |
| `clickhouse` | Storage: `ReplicatedReplacingMergeTree` table `events`. 1 shard, 2 replicas, each with a full copy. | StatefulSet + PVC per replica |
| `clickhouse-keeper` | Raft coordination service the replicas need to replicate (3 members, tolerates 1 failure). | StatefulSet + PVC per member |
| `apigateway` | Read API over ClickHouse. | Deployment |

## Data flow

1. **Streamer** reads CSV rows, filters to its deterministic shard
   (`lineNum % total == index`, index = registry rank), batches, and publishes via a
   bidi gRPC stream to `telemetry-messagequeue-leader:50051`.
2. **MessageQueue** appends each event to the WAL (fsync), replicates it to the
   follower, and only acks once the follower has applied it (`-sync-replicas 1`).
   Duplicate `event_id`s are deduped against a `seen` map rebuilt from the WAL on restart.
   The window is bounded at 100k ids with FIFO eviction, so it only suppresses re-sends
   inside that window; `ReplacingMergeTree` is the backstop beyond it.
3. **Collector** opens a `Consume` stream from its committed cursor, filters to its
   shard (`offset % total == index`), and batch-inserts into ClickHouse.
   With the shipped chart `CONSUMER_ID` is a fixed value, so `total` is always 1 and every
   collector replica currently owns the whole log (see `failure-modes.md` G10b).
   The cursor is committed only after a durable write or a fail-open dead-letter; if
   neither succeeds the cursor is held and the batch is replayed.
4. **ClickHouse** dedups redeliveries with `ReplacingMergeTree`, versioned by `received_at`. The collector writes to one replica (through the `telemetry-clickhouse` Service) and the other replica fetches the data part from it.
5. **apiGateway** serves `GET /api/v1/gpus` and `GET /api/v1/gpus/{id}/telemetry`
   (time-windowed by measurement time `source_ts` via `start_time`/`end_time`).

## Message queue design

- **WAL log**: ordered, append-only log; cleanups via a retention barrier.
- **Sync replication**: the leader withholds a publish ack until `syncWrites` connected
  followers have applied the entry, for at most 5s (`waitReplicationTimeout`). If that
  expires the leader **fails the publish RPC** rather than waiting indefinitely, and the
  producer reconnects and re-sends; offsets do not advance while a follower is missing.
  Caveat: a duplicate-`event_id` ack returns replication gen 0 and skips the wait
  entirely, so such an event may exist only on the leader (see `failure-modes.md` G10).
- **Dedup**: `seen` map keyed by `event_id`; a re-published event is acked with the
  publisher's current cursor (its last acked `csv_line_offset`) and is **not** appended
  again (idempotent producers).
- **Registry + cursors**: every *group* (`streamer`, `collector`) has an independent
  in-memory registry (heartbeat TTL + sweep) and per-consumer commit cursors. Shard
  index = lexicographic rank in the live set.
- **Retention trim**: advances only when the collector group is non-empty and every
  live collector has committed; trims below the minimum cursor. No collectors → no trim.

## Data model

- Table `telemetry.events`, `ReplicatedReplacingMergeTree('/clickhouse/tables/{shard}/telemetry/events', '{replica}', received_at)`, created `ON CLUSTER telemetry` by the collector.
- `ORDER BY (device_id, source_ts, event_id)` — `event_id` is the unique dedup dimension.
- `PARTITION BY toYYYYMMDD(source_ts)`.
- Columns: `event_id, received_at, source_ts, metric_name, gpu_index, device_name,
  device_id, model_name, hostname, value, csv_line_offset, loop_count, cluster, pod_name,
  mq_offset` (the DDL and every INSERT put `received_at` second, right after `event_id`).
- **Time columns** (both DateTime64(9)):
  - `source_ts` — the only event timestamp: the streamer's **current UTC time** when it produced the
    event. The CSV's own timestamp column is never read or propagated (the file is replayed in a
    loop, so its fixed historical time would repeat on every pass). It is stamped once by the
    streamer (per-pod UTC clock is safe because each CSV line is owned by exactly one replica) and
    carried unchanged through the MQ, so a redelivered event arrives with the same value and
    collapses on the dedup key. It is also the sort key, the partition key and the time axis the
    API filters on.
  - `received_at` — when the collector wrote the row. It is the dedup version column and the way
    to measure pipeline lag (`received_at - source_ts`). It is not returned by the API.

## Guarantees

- **At-least-once delivery** from the streamer into the log on the normal path (a failed
  publish re-sends); duplicates collapsed by MQ dedup + ReplacingMergeTree. Exception: on
  shutdown with an unreachable MQ the drain gives up and drops the remaining buffered
  events (`batcher.go`, "drop ... during drain").
- **Verified invariant** on a wiped universe with no dead-lettered batches:
  `uniqExact(event_id) == count(DISTINCT mq_offset)` and
  `count(DISTINCT mq_offset) == max-min+1` (min = 0) after `OPTIMIZE FINAL`. It does
  **not** hold after a fail-open dead-letter — those offsets never reach ClickHouse — and
  `min = 0` only holds for a table built from the very first offset.
- **Acked events are durable** on both leader and follower while replication is healthy.
- **Fail-open collector**: on persistent ClickHouse failure the collector dead-letters
  the batch and advances the cursor; it never wedges (see `failure-modes.md` for the
  durability caveat of this path).

## Failure model highlights

- **Auto leader election**: every MQ replica runs the same binary and contends for the
  Kubernetes Lease `mq-leader`; the holder promotes itself and labels itself
  `app.kubernetes.io/component=leader`, and a replica that loses the lease fences its
  writes and clears that label within `-election-renew-deadline`. Promotion and demotion
  need no operator step.
- ClickHouse is replicated (2 replicas + 3 Keeper members): a pod loss does not interrupt ingestion or reads. Replication is asynchronous, so an acknowledged write sits on one replica for a moment before the other fetches it (see `operations.md`, "ClickHouse high availability").
- Crash matrices A–G all pass with zero loss/dupes (see `failure-modes.md`).