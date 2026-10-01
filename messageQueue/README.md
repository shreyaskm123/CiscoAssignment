# messagequeue

Reference implementation of the message queue (MQ) for the telemetry pipeline:
the single endpoint every `telemetry-streamer` replica publishes to, the owner
of the **partition registry**, and — since the collector shipped — the source
of the consumer read path (`Consume`/`CommitOffset`) that the collector fleet
pulls from.

## Services (gRPC, `streamer/proto/mq.proto`)

| RPC | Purpose |
|---|---|
| `PublishEvents` | bidi streaming: client sends events, server ack-streams the committed per-consumer offset; replies carry acked/pending. **Dedup**: an `event_id` already seen is acked with its stored offset instead of being appended to the log again. |
| `GetOffset` | last known offset for `consumer_id` **in its group** (crash recovery / resume). |
| `JoinPartition` | register/heartbeat with the group's partition registry. Returned `index = rank` among that group's current live set, `total = len(live)`. Doubles as the heartbeat; consumers re-register every `REGISTRY_POLL_INTERVAL_MS`. |
| `LeavePartition` | deregister on graceful shutdown (after the drain) so survivors rebalance immediately. |
| `Consume` | server-streams every event with `offset > start_offset` in log order up to the current tail, then closes; the consumer re-invokes from its committed cursor (500ms poll) to follow new events. |
| `CommitOffset` | records a consumer's processed cursor (monotonic). Must be called after the collector's durable write, so a crash replays from the cursor. Drives the log retention barrier. |
| `Replication.Sync` | (HA only) bidi: followers send a resume offset + ack each applied frame; the leader streams the checkpoint and the mutation log. |

## Event log

- The log is an append-only sequence of events behind a small `Store` seam
  (`internal/mq/store.go`): `Append` assigns a **global, monotonic log offset**,
  `Range` serves a snapshot for a `Consume` window, `Trim` drops events at or
  below the retention barrier. Two implementations exist:
  - `MemStore` (in-memory) — no durability; the default when `-data-dir` is
    unset.
  - `WALStore` (`internal/mq/walstore.go`) — the write-ahead log. Every
    `Append`, `Trim` and committed cursor is **fsynced before the server
    replies**, so "acked" implies "on disk". On boot it replays the log exactly
    (same base + offsets), rebuilds the streamer/accepted cursors and the dedup
    window, truncates any torn tail frame left by a crash mid-write, and
    rewrites the file to a checkpoint + retained tail past that threshold.
- **Leader-follower replication** (see next section): a follower keeps a live
  copy of the log on its own WAL and, with `-sync-replicas N`, an acked event is
  replicated to N followers before the publisher hears back — so it survives a
  leader crash. Neither feature requires changes to the streamer/collector,
  which only see the gRPC contract.
- **Retention:** events at or below the lowest committed collector-group cursor
  are dropped (the trim barrier is durable in the WAL). A collector that has
  joined but never committed forces full retention (it still needs everything
  from the head); no collectors -> no trimming.
- Log offsets are distinct from the streamer's `csv_line_offset`; the collector
  uses log offsets as its shard key (`offset % total == index`) and its cursor.

## Groups

State is namespaced by `group` so independent fleets don't share one shard
assignment: the streamer fleet registers/reads in group `streamer`, the
collector fleet in `collector` (own registry indices *and* own cursor boxes).
Publisher acks always land in the `streamer` group; collector cursors and the
retention barrier live in the `collector` group. An empty group on a request
falls back to `default`.

## Run

```sh
go build -o /tmp/mq-bin ./cmd/messagequeue

# in-memory (no crash recovery)
/tmp/mq-bin -addr :50051 -ttl-seconds 15

# durable: write-ahead log on disk, replayed on restart (crash recovery)
/tmp/mq-bin -addr :50051 -data-dir /var/lib/messagequeue/wal.log

# high availability, explicit roles (manual; what the chart replaced)
/tmp/mq-bin -addr :50051 -data-dir /var/lib/mq/leader -sync-replicas 1 &
/tmp/mq-bin -addr :50052 -data-dir /var/lib/mq/f1 -leader localhost:50051 &

# high availability, automatic: both replicas are identical and elect a leader
# through a Kubernetes Lease. This is what the Helm chart runs.
POD_NAME=node-0 POD_NAMESPACE=telemetry \
  /tmp/mq-bin -addr :50051 -data-dir /var/lib/mq/node-0 \
    -election -election-lease mq-leader -sync-replicas 1 &
POD_NAME=node-1 POD_NAMESPACE=telemetry \
  /tmp/mq-bin -addr :50051 -data-dir /var/lib/mq/node-1 \
    -election -election-lease mq-leader -sync-replicas 1 &
```

`-election` and `-leader` are mutually exclusive: the election decides the role,
`-leader` pins it. A replica that loses the lease fences its writes immediately
and clears its `component=leader` label, so a partitioned old leader is removed
from the leader Service by kubelet on a local clock rather than by the API server
it can no longer reach.

`-data-dir` is the WAL path. The file is flock-exclusive (a data-dir is owned
by one process at a time); a second process on the same path fails at boot. No
flag means the in-memory `MemStore`, preserving the pre-WAL behaviour.

## High availability (leader-follower)

The `Replication` service (`streamer/proto/mq.proto`) streams every log
mutation — appends, commit cursors, retention trims — from the leader to each
follower, plus a checkpoint (absolute base + every cursor) whenever a follower
(re)connects so it converges fast even after an outage. Frames carry a
monotonic `gen`; the follower acks each one after applying it durably to its own
WAL.

- The leader only acks a publisher once `-sync-replicas N` followers have
  applied that event's `gen`. With `N >= 1`, **an acked event is on a quorum**:
  it survives a leader crash. Publish throughput is then bounded by the
  follower's fsync (expected for strong durability).
- What is NOT replicated: the per-event dedup window (advisory; a promoted node
  starts empty and the sink's `ReplacingMergeTree` absorbs the re-sends) and the
  partition registry (ephemeral per node; clients re-heartbeat within their TTL
  after promotion).
- A follower is **read-only**: locally it serves `Consume`/`GetOffset`/
  `JoinPartition`/`LeavePartition`, but rejects `PublishEvents`/`CommitOffset`
  with `FailedPrecondition`. It must run with its own `-data-dir`.
- **Failover:** on a leader crash, stop the follower and start it again without
  `-leader` (same `-data-dir`). Its WAL has everything the leader acked; boot
  replay restores the log and every cursor (streamer recovery offsets and
  collector committed cursors), so `GetOffset` resumes producers/consumers
  exactly where they were and no acked event is lost. Point producers at the new
  leader. (In-flight, un-acked events are simply re-published by the producer;
  dedup + the sink absorb any overlap.)
- Slow followers are disconnected and re-synced from their own watermark — the
  protocol is idempotent, so no frame is lost, only delayed.

Crash-recovery guarantees, once `-data-dir` is set:

- Every acked `PublishEvents` survives a `kill -9` (the event is fsynced before
  the ack leaves the server).
- A restarted MQ resumes **exactly**: the same log offsets continue (never
  reused), producers pick up via their replayed `GetOffset`, consumers re-read
  from their committed cursors, and the dedup window is rebuilt — a streamer
  re-send is acked as a duplicate, never re-appended.
- At-least-once delivery: an event consumed but not yet committed when the MQ
  crashes is re-delivered on restart (the collector re-invokes `Consume` from
  its cursor). Collapse duplicates at the sink (ClickHouse `ReplacingMergeTree`).

## Health and metrics

`-health-addr` (default `:8081`, empty disables) serves three endpoints:

| Endpoint | Auth | Purpose |
|---|---|---|
| `/healthz` | none | plain liveness — deliberately dependency-free, because a probe that reflected a dependency outage would restart every pod at once |
| `/readyz` | none | readiness; role- and lease-aware. A **follower is always ready** so it can be promoted; a leader whose lease renewal has gone stale returns `503` and drops out of the leader Service |
| `/metrics` | none | Prometheus text: `mq_role`, `mq_log_head`, `mq_log_base`, `mq_retained_entries`, `mq_consumer_committed_offset`, `mq_consumer_lag`, `mq_process_resident_memory_bytes`, `mq_go_memstats_heap_inuse_bytes`, `mq_go_goroutines`, `mq_wal_bytes` |

`/metrics` needs no authentication and exposes queue internals, so it is not
published by default. Enable it with `messagequeue.metrics.enabled=true`, which
creates one NodePort Service per replica; see `docs/operations.md`
("MQ metrics") for the host-side bridge used on a local kind cluster.

## Tests

```sh
go test ./... -race     # publish/ack, dedup, consume windows, retention
                        # barrier, monotonic commits, group namespacing,
                        # WAL replay/torn-tail/trim/compact + server-level
                        # crash-recovery resume, leader-follower replication
                        # (sync-gated acks, read-only followers, failover)
```