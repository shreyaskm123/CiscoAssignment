# telemetry-streamer

Streams the DCGM GPU metrics CSV (mounted from a Kubernetes configMap) to the
message queue as real-time events: deterministic workload distribution across
replicas, graceful no-data-loss shutdown, and idempotent producer semantics.

## How it works

```
                                   metrics.csv (configMap mount)
                                          │
                                          ▼
                              ┌─────────────────────┐
                              │   csvsrc.Reader     │  currentLineNum (0-based data row)
                              │  loops on EOF       │  resets to 0, increments Loop
                              └─────────┬───────────┘
                                        │ Row{Raw, LineNum, Loop}
                                        ▼
                              ┌─────────────────────┐
                              │  scheduler          │  currentLineNum % total == index
                              │  (deterministic)    │  → owned by this replica, else skip
                              └─────────┬───────────┘
                                        │ owned row
                                        ▼
                              ┌─────────────────────┐
                              │  event / idgen       │  evt_<pod>_L<loop>_O<offset>  + source_timestamp=now
                              └─────────┬───────────┘
                                        ▼
                              ┌─────────────────────┐
                              │  mqclient            │  PublishEvents (bidi stream)
                              │  batcher             │  ack → offsets.Commit
                              └─────────────────────┘
```

Key behaviours from the assignment and how they map:

| Requirement | Where |
|---|---|
| CSV in configMap, mounted per pod | `k8s/configmap.yaml`, mounted at `/mnt/data/metrics.csv` |
| `currentLineNum` in-memory, resets to 0 on wrap | `internal/csvsrc/reader.go` (`Next`, `Loop`, `SeekTo`) |
| Deterministic distribution `lineNum % N == index` | `internal/scheduler` |
| Scale up/down re-`partition` automatically | `internal/app/configwatch.go` (registry heartbeat detects membership change → `sched.Reconfigure`) |
| Timestamp stamped with *now* per event | `mapEvent` in `internal/app/streamer.go` |
| `readPosition` / `committedPosition` | `internal/offsets` (atomic, ack-driven commit) |
| Idempotent event id | `internal/event/idgen.go` → `evt_<pod>_L<loop>_O<offset>` |
| Crash recovery via `getOffset(consumer_id)` | `mqclient.GetOffset` called in `app.New`; resumes the reader |
| `(index,total)` from registry membership | `internal/mqclient/mqclient.go` `JoinPartition` → rank in sorted live set (the only source of the partition) |

## Environment variables

| Variable | Default | Meaning |
|---|---|---|
| `POD_NAME` | `telemetry-streamer-0` | Pod identity (via downward API) |
| `CLUSTER` | `ai-prod-01` | Value of the `tags.cluster` label |
| `CONSUMER_ID` | `<POD_NAME>` | Identity passed to `GetOffset` for recovery |
| `CSV_PATH` | `/mnt/data/metrics.csv` | Mounted CSV location |
| `HEALTH_ADDR` | `:8082` | Listen address for `/healthz` and `/readyz` (empty disables) |
| `READY_MAX_STALL_SECONDS` | `300` | No acknowledged publish for this long ⇒ NotReady; `0` disables |
| `MQ_TOKEN_FILE` | `""` | Bearer-token file for MQ auth, re-read per RPC so rotation needs no reconnect |
| `MQ_TOKEN` | `""` | Inline token, used only when `MQ_TOKEN_FILE` is unset |
| `MQ_ADDR` | `localhost:50051` | gRPC endpoint of the message queue |
| `ROW_DELAY_MS` | `5` | Pacing of the read loop (0 = as fast as possible) |
| `BATCH_SIZE` | `100` | Events per publish-stream flush |
| `FLUSH_INTERVAL_MS` | `200` | Max buffering before flush |
| `SHUTDOWN_GRACE_MS` | `5000` | Max time to drain buffered/in-flight events on shutdown |
| `RELOAD_CHECK_INTERVAL_S` | `5` | How often the CSV is stat'ed for rotation |
| `REGISTRY_TTL_S` | `15` | Heartbeat TTL requested from the registry (dead replicas expire) |
| `REGISTRY_POLL_INTERVAL_MS` | `5000` | Registry join/heartbeat cadence = how fast scale up/down is detected |

## Multi-replica partitioning (Option A: MQ partition registry)

For a stateless Deployment fleet, an index cannot come from the pod name (k8s
Deployment names are random). There is no env-based fallback: each replica
registers with the MQ's partition registry (`JoinPartition`) and receives a
unique index = its rank among the current live consumers:

- runs on every `REGISTRY_POLL_INTERVAL_MS` (also serves as the heartbeat);
- dead replicas expire after `REGISTRY_TTL_S` and the fleet rebalances;
- a **startup stability gate** (Ready) prevents streaming under a transient
  partial view when a scale wave boots simultaneously;
- graceful shutdown deregisters (`LeavePartition`) *after* the drain so the
  survivors rebalance immediately.

The registry lives in the `messageQueue` component (`messagequeue/cmd/messagequeue`),
which also implements `PublishEvents` (ack/offset, dedup on `event_id`) and
`GetOffset`.

## Graceful shutdown

On SIGTERM/SIGINT the streamer drains before exiting:

1. stops accepting new events (`batcher.closeInput`)
2. flushes every event already buffered to the stream and calls `CloseSend`
3. waits for the MQ's **acks** to confirm delivery (or `SHUTDOWN_GRACE_MS`)
4. logs final `readPosition` / `committedPosition` / `produced`

Verified by `TestGracefulShutdownFlushesBufferedEvents`: cancelling the run
context delivers exactly the number of events produced — none dropped. If the
MQ is unreachable at shutdown, `SendUntil` (2s) plus `SHUTDOWN_GRACE_MS` bound
the wait so the process still terminates.

## Local run

```sh
# 1. start the message queue (see ../messageQueue) on :50051
# 2. run a streamer; every replica just needs a distinct POD_NAME —
#    the registry picks a unique index automatically
CSV_PATH=~/Downloads/dcgm_metrics_20250718_134233.csv \
POD_NAME=telemetry-streamer-0 \
MQ_ADDR=localhost:50051 go run ./cmd/streamer
```

## Tests

```sh
go test ./... -race
```

## Kubernetes

Prefer the Helm chart — it is what CI and the runbook use:

```sh
make cluster && make deploy      # or ./scripts/quickstart.sh
```

The standalone manifests under `k8s/` are a secondary path, kept in step with the
chart by hand. They used to point at an MQ Service that the chart never creates
and probe with `exec: ["/bin/true"]`, so following them yielded a Deployment
that could not reach the MQ and could not fail its probe. Both are fixed, but
the chart is the supported route:

```sh
kubectl create namespace telemetry
kubectl apply -f k8s/configmap.yaml
kubectl apply -f k8s/deployment.yaml
kubectl apply -f k8s/hpa.yaml
```

No configMap sync is needed on scale up/down: the HPA changes the Deployment's
replica count and every pod learns the new total from the MQ partition registry
on its next heartbeat (`REGISTRY_POLL_INTERVAL_MS`), then repartitions.

## Notes / assumptions

- Offsets (`csv_line_offset`) are 0-based **data-row** indices (header excluded)
  and reset to 0 every loop; the MQ persists the last-acked offset per consumer.
  Idempotency (dedup on `event_id`) absorbs re-sends after a crash/restart.
- gRPC uses insecure credentials (private cluster); swap in TLS creds for
  production.
- A configMap rotation is detected by mtime and restarts iteration at offset 0.