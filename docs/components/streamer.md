# streamer

Publisher fleet: reads the DCGM-style metrics CSV (configMap-mounted), assigns each row to a
replica, and publishes events to the message queue. Stateless but cursor-resumed.

## Deploy
- Kind: Deployment, 3 replicas (chart `streamer`).
- CSV mounted from a configMap at `/mnt/data/metrics.csv` (default).
- Consumes via Service `telemetry-messagequeue-leader:50051`.

## Configuration (env)

| Env | Default | Meaning |
|---|---|---|
| `POD_NAME` | `telemetry-streamer-0` | Identity = registry consumer id (per-pod) |
| `CLUSTER` | `ai-prod-01` | Cluster tag stamped on every event |
| `HEALTH_ADDR` | `:8082` | listen address for `/healthz` and `/readyz` (empty disables) |
| `READY_MAX_STALL_SECONDS` | `300` | no acknowledged publish for this long ⇒ NotReady; `0` disables |
| `CSV_PATH` | `/mnt/data/metrics.csv` | configMap-mounted CSV |
| `MQ_ADDR` | `localhost:50051` | MQ gRPC address |
| `MQ_TOKEN_FILE` | `""` | bearer-token file for MQ auth, re-read on every RPC so a rotated Secret takes effect without reconnecting |
| `MQ_TOKEN` | `""` | inline token, used only when `MQ_TOKEN_FILE` is unset | `localhost:50051` | MQ gRPC address |
| `CONSUMER_ID` | = `POD_NAME` | Cursor identity (recovery is per pod) |
| `RELOAD_CHECK_INTERVAL_S` | `5` | CSV stat interval (configMap rotations) |
| `BATCH_SIZE` | `100` | buffered events per publish flush |
| `FLUSH_INTERVAL_MS` | `200` | max time a buffered batch waits before send |
| `ROW_DELAY_MS` | `5` | read-loop pacing (0 = as fast as possible) |
| `SHUTDOWN_GRACE_MS` | `5000` | drain bound on shutdown |
| `REGISTRY_TTL_S` | `15` | heartbeat TTL requested on registry join |
| `REGISTRY_POLL_INTERVAL_MS` | `5000` | join/heartbeat cadence (= scale-up detection) |

## Probes

Both the collector and the streamer serve two endpoints for the kubelet:

| Endpoint | Probe | Answers |
|---|---|---|
| `GET /healthz` | liveness | is the process still serving? Deliberately dependency-free |
| `GET /readyz` | readiness | can it do its job right now, and if not, why |

`/readyz` returns the whole assessment as JSON, including counters, so the
numbers needed to explain a `NotReady` are available from the probe itself:

```bash
kubectl -n telemetry port-forward deploy/telemetry-streamer 8082:8082
curl -s localhost:8082/readyz | python3 -m json.tool
```

```json
{"ready": true,
 "fields": {"consumer": "telemetry-streamer-0", "shard": "0/3",
            "read_position": "399", "committed_position": "399", "lag": "0",
            "produced_total": "2417643", "sent_total": "2417531",
            "acked_total": "986807", "unacked_in_flight": "1430724",
            "last_ack_age_seconds": "253"}}
```

`unacked_in_flight` is the field to look at when the stall check fires: it is the
count of events sent to the MQ that have not been acked back. `last_ack_age_seconds`
and `last_send_failure_age_seconds` appear only once there has been an ack or a send
failure respectively.

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
- **a publish failure in flight**: the last `Send` to the MQ failed (the stream
  never writes batches — it has no write-failure counter; that is the collector);
- **no acknowledged publish for `READY_MAX_STALL_SECONDS`** (default 300, `0`
  disables), reported as "no acknowledged publish for Ns".

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
- **Deterministic sharding**: replica `index` handles `currentLineNum % total == index`, where `currentLineNum` is the 0-based **data-row** index (CSV header excluded), where
  `(index, total)` comes from the MQ registry (rank in sorted live set, group = `streamer`).
  Scale up/down re-ranks indices and causes safe replay (MQ dedup absorbs re-publishes).
- **Timestamp**: `mapEvent` never reads the CSV's timestamp column. It takes the streamer's
  **local UTC time** (`time.Now().UTC()`) once per event and sets `source_timestamp` to that value
  (the event has no other timestamp), so the same value is passed through the MQ and collector and
  stored in ClickHouse as `source_ts`. Because each CSV line is owned by a single
  replica, each event's timestamp comes from exactly one pod — no cross-pod writer conflict. A
  malformed CSV timestamp cannot affect a row, since the column is ignored.
- **Cursor resume** (`internal/offsets`): each pod resumes from its committed CSV line offset
  after a restart; CSV open is retried (1s) when the configMap isn't mounted yet.
- **Publish**: events → batch goroutine → `PublishEvents` bidi stream; `readAcks` tracks acks.
  On a dead stream, `ensure()` recreates it and the failed event is re-sent (never silently dropped).
- **Graceful shutdown**: batcher `closeInput()` then `drain(grace)`; the final drain uses a fresh
  2s deadline (`SendUntil`) and **waits for acks** before exiting — buffered/in-flight events are
  not lost on SIGTERM.
- **Validation**: non-numeric `value` rows are skipped and logged, not published.

## Retry / backoff
- Publish reconnect sleeps a fixed `500ms` (sendUntil) — **flat, infinite, no jitter** (roadmap G2:
  replace with exponential backoff, cap ~30s).
- CSV open retry is a fixed 1s loop until the file appears or the pod is stopped.

## Safety properties
- No event is silently dropped: on stream failure the event stays buffered and is re-sent;
  dedup on the MQ maps re-publishes back to the original offset.
- Shutdown is secondarily bounded (grace) so a downed MQ can't hang the process forever

## Observability / troubleshooting
- Stdout logs: publish errors, restart summaries
  (`readPosition/committedPosition/lag/produced`), registry joins.
- Healthy log line to expect after MQ restarts: `registry join group=streamer consumer=telemetry-streamer-… index=N/3`.