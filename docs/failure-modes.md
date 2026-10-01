# Failure modes, crash tests, and known gaps

## Crash-resilience matrix (verified on kind)

All cycles were run against a wiped universe (fresh MQ WALs, `telemetry.events` truncated to 0)
with the pipeline actively flowing. After each cycle's recovery, the audit (see `operations.md`)
must PASS: contiguous offsets from 0 (no holes = no loss) and `uniqExact(event_id)` equal to
`count(DISTINCT mq_offset)` (perfect 1:1 = no dupes).

| Cycle | What was crashed | Result |
|---|---|---|
| base | none (fresh universe) | PASS (17,720 events) |
| A | collector pod | PASS |
| B | one streamer pod | PASS |
| C | MQ leader pod (same-WAL restart) | PASS |
| D | MQ follower pod | PASS |
| E | **both** MQ nodes scaled to 0 together | PASS |
| F | collector + MQ leader simultaneously | PASS |
| G | full freeze (3 streamers + collector + both MQ, cold re-boot) | PASS |

Healthy pipeline continued to grow after every cycle with audit still green.

## Why each crash is safe (design reasons)

- **Caller-paced consumption**: no persistent SUBSCRIBE; the collector (re)opens
  `Consume` from its committed cursor on every poll/error, and the streamer recreates its
  publish stream on any failure. Broker restarts never "silence" a consumer.
- **WAL durability**: acked events are on the leader WAL (fsync) and the follower
  (replicated copy) while healthy; `seen` dedup is rebuilt from the WAL on restart, so
  re-sent tail events return their original offset.
- **Cursored replay + storage dedup**: any redelivery replays from the committed cursor and
  collapses in `ReplacingMergeTree`, so at-least-once never becomes duplicates.
- **Retention safety**: trim is gated on a live collector group with committed cursors;
  with no collectors the log is retained.

## Known gaps and roadmap

Priority-ordered work items (from the full code/design review). No item is a known
silent-loss bug in tested paths; they are durability/security/ops hardening.

| ID | Gap | Why it matters | Fix |
|---|---|---|---|
| G1 | Dead-letter is non-durable (ephemeral pod disk) + no replay | Long CH outage after the retry budget → batches lost if the pod restarts | Mount a PVC at `DEAD_LETTER_DIR`; replay leftover DLQ files into CH at startup before consuming (idempotent via `event_id`) |
| G2 | Flat retries (no exponential backoff) | Streamer hammers MQ every 500ms forever; collector 3× fixed 500ms — self-inflicted traffic during outages | Exponential backoff + jitter, capped (0.5s→…→30s), reset on recovery |
| G3 | Crash-looping at boot until CH/MQ ready | Collector/api `log.Fatalf` if ClickHouse or the table is missing; streamer if MQ is down | Startup retry-with-backoff in `main.go`s + initContainers that wait for CH/MQ |
| G4 | No externalized secrets; CH auth disabled | Passwordless `default` user; no K8s Secrets | Secret-backed `users.d`/`users.xml` + env via `secretKeyRef`; enable auth |
| G5 | ~~No dedicated health endpoint~~ **fixed** | probes now hit `GET /healthz` (`store.Ping`, no table scan) instead of the heavy `/api/v1/gpus` list query | done — `/healthz` + probes repointed |
| G6 | ~~Observability: logs only~~ **MQ fixed**, collector/streamer not | The collector and streamer serve real `/healthz` and `/readyz`, so a stalled replica reports `NotReady` instead of `1/1 Running` (they used to probe with `exec: ["/bin/true"]`). The MQ now serves `/metrics` on its health port (depth, retained window, per-consumer cursor and lag, RSS/heap/goroutines, WAL size), exposed per replica on a node port — see *MQ metrics* in the runbook. The collector and streamer still export nothing | Export `produced/acked/sent` and cursor lag for the collector and streamer | `/metrics` for collector and streamer |
| G6b | No stall detection or repair in the MQ | A stalled follower leaves the leader withholding sync-replication acks, so publishers block silently: the pipeline once ran at 0.2% of its rate for ~9 hours. The new readiness probes now *report* that, but nothing repairs it | Per-follower last-applied tracking on the leader; a bounded `-sync-replicas-timeout`; a staleness-driven follower liveness probe, or opt-in eviction from the sync set |
| G7 | No NetworkPolicies / TLS | Any pod can reach CH/MQ; all gRPC + HTTP plaintext | NetworkPolicies per service; TLS on gRPC and CH |
| ~~G8~~ **partly fixed** | `make test` runs `go test -race` across all four modules and is clean; `make check` is the CI gate. Still open: both `internal/mqclient` packages have **0% statement coverage**, so `-race` cannot see the streamer `sendUntil`/`readAcks` resend path or the collector client. Passes on executed paths only | Tests for the gRPC client layer (fake bidi stream, forced `Send` failure); wire `make check` into CI |
| G9 | MQ StatefulSet rolling-update quirk unverified on clean cluster | One observed stall on `helm upgrade` of resources | Reproduce on a fresh cluster; fix chart or document |
| G10 | Sync-replication single-copy window | While the follower is down, acked events can exist only on the leader; if leader+PV lost before follower catches up, that window is lost | Choose: document as accepted, or require replication on duplicate-ack path |
| G10b | **All collector replicas share one consumer id** | `CONSUMER_ID` is a fixed value, so every replica joins the `collector` group under the same id. The registry is a map keyed by that id, so N replicas collapse to one entry and the MQ reports `total=1 index=0` — each replica is told it owns the whole log. Observed at 10 replicas: no parallelism, and each event written up to 10 times (harmless for reads, which use `FINAL`, but ~10x the ClickHouse write load). Unlike the streamer, whose cursors are pod-scoped, the collector is not | Derive `CONSUMER_ID` from `metadata.name` via the downward API so the registry hands out real partitions |
| G11 | No collector HPA | Scaling exists by design but is manual | CPU-based HPA (safe: shard=registry rank, replay dedup) |
| G12 | ~~Single-node CH~~ **fixed** (HA); backups still open | ClickHouse runs as 2 replicas + 3 Keeper with PDBs and soft anti-affinity; there is still no backup/snapshot job, and on a one-node cluster a node loss takes every replica | CH backup job; run on >= 2 nodes |
| ~~G13~~ **done** | Leader election ships: `messageQueue/internal/election` (Lease + CAS), `-election`, `ha.Promote/Demote`, chart `leaderElection.enabled: true`. Verified live — killing the leader promoted the survivor in <10s with zero offset loss | — (entry was stale: election is implemented and tested) |

## Tested boundaries / caveats

- ClickHouse itself is **deliberately outside** the crash matrix: the collector's retry budget
  is small (3 × 500ms) before fail-open dead-letter, and a sustained CH outage would send
  batches to the (currently ephemeral) DLQ instead of CH.
- All crash cycles used clean pod restarts **on unchanged PVCs/WALs** — they prove restart
  continuity, not resilience to persistent volume loss.