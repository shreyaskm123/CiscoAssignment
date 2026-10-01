# messageQueue

Custom durable, replicated event log + gRPC broker. The heart of the pipeline's
data-safety story (WAL durability, sync replication, dedup, cursors, retention).

## Deploy
- One StatefulSet: `telemetry-messagequeue` (default 2 replicas). A Kubernetes Lease `mq-leader` elects exactly one leader; the rest are replicas.
- Services: headless `telemetry-messagequeue` plus a leader-only Service that tracks the `app.kubernetes.io/component=leader` label.
- Data dir on a PVC per ordinal (`data-telemetry-messagequeue-0`, `-1`); `-data-dir` empty = in-memory (no crash recovery).

## Authentication

Tokens arrive as environment variables, never as flags (flags are visible in `ps` and
`/proc` to everything in the container):

| Env var | Default | Meaning |
|---|---|---|
| `MQ_AUTH_TOKENS_FILE` | `""` | file of client bearer tokens; re-read on every RPC |
| `MQ_AUTH_TOKENS` | `""` | inline token list, used only when the file is unset |
| `MQ_AUTH_TOKEN_FILE` | `""` | token this node presents when it pulls replication from a leader |
| `MQ_AUTH_TOKEN` | `""` | inline fallback for the above |

The server logs a WARNING at startup when auth is disabled.

## Command-line flags

| Flag | Default | Meaning |
|---|---|---|
| `-addr` | `:50051` | gRPC listen address |
| `-health-addr` | `:8081` | HTTP listener for `/healthz`, `/readyz`, `/metrics`; empty disables |
| `-ttl-seconds` | `15` | default heartbeat TTL for the partition registry |
| `-data-dir` | `""` | path to the write-ahead-log **file** (parent dirs are created on demand; the file is `flock`-exclusive). Empty = in-memory `MemStore`, no crash recovery |
| `-leader` | `""` | if set, run as a **follower** replicating from this leader addr (requires `-data-dir`). Mutually exclusive with `-election` |
| `-sync-replicas` | `0` | leader: withhold a publish ack until N followers have applied the event (the chart uses `1`). Applied at promotion time under `-election`; ignored on a follower |

**Leader election** (what the chart actually runs — see `helm-charts/telemetry/charts/messagequeue/values.yaml`):

| Flag | Default | Meaning |
|---|---|---|
| `-election` | `false` | contend for the Lease and promote/demote automatically |
| `-election-namespace` | `""` | defaults to the pod's own namespace |
| `-election-lease` | `mq-leader` | name of the `coordination.k8s.io` Lease |
| `-election-identity` | `""` | defaults to `$POD_NAME` |
| `-election-lease-duration` | `15s` | how long a lease stays valid without renewal; the lower bound on failover time |
| `-election-renew-deadline` | `10s` | how long a leader keeps retrying renewal before it fences itself |
| `-election-retry-period` | `2s` | interval between acquire attempts and between renewals |
| `-leader-service` | `telemetry-messagequeue-leader` | Service the elected leader patches its `component=leader` label into |
| `-pod-name-env` | `POD_NAME` | env var holding this pod's name |
| `-pod-namespace-env` | `POD_NAMESPACE` | env var holding this pod's namespace |

## RPC surface (`streamer/proto/mq.proto`)

```
service MessageQueue {
  rpc PublishEvents(stream Event) returns (stream Ack);   // publisher write + ack stream
  rpc GetOffset(GetOffsetRequest) returns (GetOffsetResponse);      // committed cursor
  rpc JoinPartition(JoinPartitionRequest) returns (JoinPartitionResponse);
  rpc LeavePartition(LeavePartitionRequest) returns (LeavePartitionResponse);
  rpc Consume(ConsumeRequest) returns (stream ConsumedEvent);       // caller-paced read
  rpc CommitOffset(CommitOffsetRequest) returns (CommitOffsetResponse);
}
service Replication { rpc Sync(stream ReplicaAck) returns (stream ReplicaFrame); } // leader<->follower
```

## Role behavior

- **Leader** (`no -leader`): accepts publishes; appends to WAL (fsync); replicates to follower;
  with `-sync-replicas 1` it withholds the ack until the follower applied the event.
  If no follower is connected, `waitReplicated` times out → publish RPC errors → producer retries.
- **Follower** (`-leader …`): replays from the leader (resume after its log watermark), applies
  appends + checkpoints + cursor commits to its **own WAL**, acks frames. Read-only for commits
  (`CommitOffset` → `FailedPrecondition`). Its WAL is fully replayable — this is what makes
  promotion to leader possible without re-seeding.
- **Dedup**: `seen` map keyed by `event_id`, rebuilt from the WAL on restart; a re-published
  event returns its original offset instead of a second append.
- **Registry + cursors**: per-group (`streamer`, `collector`) in-memory registry
  (heartbeat TTL + sweep); shard `(index, total)` = rank in the lexicographically sorted live
  set; per-consumer committed cursors persisted to the WAL (durable before applied).
- **Retention trim**: only advances once the collector group is non-empty and every live
  collector has a committed cursor; trims at/below the minimum cursor. **No collectors → no trim.**

## Frame/event internals (replication)
`ReplicaFrame` types: `a` append, `c` cursor commit, `t` trim, `x` checkpoint (the stream hello is a `ReplicaAck{resume_log_offset}`, not a frame). The follower
applies those on top of its base, and `ApplyCheckpoint` keeps it consistent across reconnects.

## Concurrency
- Single `sync.Mutex` on the server; registry fully `Lock()`-held (no `RLock` misuse);
  replicator uses `atomic.Int64/atomic.Bool` (`appliedGen`, `synced`, `saturated`).
  `saturated` signals a follower queue overflow → reconnect + resync from WAL.
- Run `go test -race ./...` in `messageQueue/` to certify (roadmap G8).

## Data-safety notes
- An event is acked only after fsync on the leader (+ follower when sync-replicated) —
  acked events survive leader restart on the same WAL/PVC (proven in crash Cycles C/E/G).
- **Single-copy window (G10):** a duplicate `event_id` ack returns replication gen 0, and
  `waitReplicated` returns immediately for gen 0 — so such acks skip replication entirely,
  regardless of whether a follower is connected. The acked event may then exist only on the
  leader until that follower catches up. Not hit by any tested cycle; choose to document or
  close it.
- The log is **not** a topic/queue: no SUBSCRIBE, no NACK, no per-message redelivery —
  consumers replay from cursors and storage dedups.

## Operations
- **Failover**: automatic. A `mq-leader` Lease elects the leader and each pod
  promotes or demotes itself; see `operations.md` for timing and for how to
  remove a leader on purpose.
- **Stale-leader fence**: a leader that cannot renew its Lease answers 503 on
  `/readyz`, which drops it from the leader Service's endpoints without needing
  the API server it lost. `/healthz` stays 200 so the node is not restarted.
- **Probe coupling**: `startupProbe`, `livenessProbe` and `readinessProbe` are all `httpGet`
  on the named port `health` (`healthPort: 8081`), **not** `tcpSocket` on the `grpc` port.
  Changing `-addr` therefore does not require touching `containerPort grpc`, which tracks
  `.Values.port` independently, so the stated kubelet-kills-the-pod failure cannot occur.
- **Stalled rollout**: after a resource-only `helm upgrade`, the StatefulSet may not roll its pod;
  force it with `kubectl delete pod telemetry-messagequeue-0 -n telemetry --wait=false`.
- **Memory**: leader was OOMKilled at 256Mi; deployed at cpu 1 / mem 1Gi.
- Logs are noisy (per-event `RECEIVED`/`LOG append` lines); use `grep -vE "RECEIVED|LOG append"`.