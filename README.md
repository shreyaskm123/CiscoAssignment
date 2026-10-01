# telemetry

A DCGM-style GPU metrics pipeline, end to end:

```
streamer ──▶ messagequeue ──▶ collector ──▶ ClickHouse ──▶ apigateway
 (CSV)        (leader +         (sharded,      (time-series)   (authenticated
               sync-replicated   dedup)                        read API)
               follower)
```

Four Go services (streamer, message queue, collector, API gateway) behind a
ClickHouse database, packaged as a Helm umbrella chart and deployable to a local
[kind](https://kind.sigs.k8s.io/) cluster in a single command. No cloud
account, no registry, no cost.

## Quickstart

```bash
./scripts/quickstart.sh --tag v1.0.0
```

Builds the images, creates the cluster, deploys with Helm, and runs 43–47
end-to-end assertions (~7 min with a warm Docker cache). The count varies only
with which optional WAL log-line checks still find their line in the pod's log
buffer; the last full run passed 46.

### Call the API

There are four routes. `/healthz` is open. Every `/api/v1/*` route needs a
Bearer token. The long-lived token written by quickstart **works on the data
routes directly** — you do not have to call `/api/v1/token` first.

```bash
TOKEN=$(cat .auth/api-token.txt)
kubectl -n telemetry port-forward svc/telemetry-apigateway 8080:8080 &

curl -s localhost:8080/healthz
# {"status":"ok"}

# list GPUs (paginated; `count` is the page, `total` is all of them)
curl -s localhost:8080/api/v1/gpus -H "Authorization: Bearer $TOKEN" \
  | python3 -m json.tool

# telemetry for one GPU — use the `id` field (GPU-...), not the numeric index
GPU_ID=$(curl -s localhost:8080/api/v1/gpus -H "Authorization: Bearer $TOKEN" \
  | python3 -c "import json,sys; print(json.load(sys.stdin)['gpus'][0]['id'])")
curl -s "localhost:8080/api/v1/gpus/$GPU_ID/telemetry" \
  -H "Authorization: Bearer $TOKEN" | python3 -m json.tool | head -40

# no token → 401
curl -s -o /dev/null -w '%{http_code}\n' localhost:8080/api/v1/gpus
```

| Method | Path | Auth |
|---|---|---|
| `GET` | `/healthz` | none |
| `POST` | `/api/v1/token` | static token from `.auth/api-token.txt` |
| `GET` | `/api/v1/gpus` | static token **or** the access token from `/api/v1/token` |
| `GET` | `/api/v1/gpus/{id}/telemetry` | same |

There is no `GET /api/v1/gpus/{id}` (404 by design). `{id}` is the `GPU-…`
string from the list.

### API spec

[`apiGateway/openapi.json`](apiGateway/openapi.json) is the machine-readable
description of those routes: import it into Postman, or feed it to a client
generator. It is **generated from the code**, not maintained by hand, so it
cannot drift from what the server serves:

```bash
cd apiGateway && go generate ./...   # rewrite openapi.json after changing the API
```

Routes come from the table in `internal/httpapi/routes.go` (the same table
`NewServer` registers) and the schemas from reflection over the Go response
types. A test fails if the committed file is stale, so changing a route or a
field without regenerating breaks the build.

Optional 15-minute JWT (a JWT cannot mint another JWT):

```bash
JWT=$(curl -s -X POST localhost:8080/api/v1/token \
  -H "Authorization: Bearer $TOKEN" \
  | python3 -c "import json,sys; print(json.load(sys.stdin)['access_token'])")
```

### Scale

Defaults: 3 streamers, 2 MQ replicas, 1 collector. Scale with kubectl:

```bash
kubectl -n telemetry scale deploy/telemetry-streamer --replicas=6
kubectl -n telemetry scale deploy/telemetry-collector --replicas=2
kubectl -n telemetry scale statefulset/telemetry-messagequeue --replicas=3
```

Streamer replicas partition the CSV (`currentLineNum % N == index`, data-row
indices with the header excluded) and only pace owned rows, so more streamers
raise ingest until the MQ (`sync-replicas=1`) or the collector saturates.
**Extra MQ replicas add HA. Extra collector replicas do not shard** — `CONSUMER_ID`
is a fixed value, so the registry hands every replica index 0 / total 1 and each
writes the whole log (N× the write load, no parallelism; see `failure-modes.md` G10b).
Stored events converge to duplicate-free; check it with a `FINAL` read or after
`OPTIMIZE FINAL`, since a bare `count()` includes not-yet-merged duplicates:
`SELECT uniqExact(event_id) = count() FROM telemetry.events FINAL`.

**Requirements:** Docker Desktop, kind, kubectl, helm ≥ 3.8, python3. ~8GB RAM
free.

Full walkthrough with expected output at every step:
**[docs/quickstart.md](docs/quickstart.md)**.

## What it does

Each component is a separate deployable, and the interesting parts are in the
protocol between them:

- **The message queue is a custom replicated log.** Every replica runs an
  identical binary; one wins a Kubernetes Lease and becomes leader. With
  `sync-replicas=1` a publish is acknowledged only after a follower has applied
  it, so an acknowledged event is already on the survivor's disk before the
  leader can die.
- **The stale-leader fence.** A leader that loses the API server cannot patch
  away its own `component=leader` label — that needs the API server it lost. It
  stays in the leader Service's endpoints accepting writes a survivor is also
  accepting. `/readyz` closes this locally: kubelet drops a NotReady pod from
  endpoints without needing the API server, so a leader whose lease renewal goes
  stale returns 503 and leaves rotation. `/healthz` deliberately ignores the
  lease, so that node is *not* restarted — restarting it would destroy the
  quorum.
- **At-least-once into the queue, effectively-once out of ClickHouse.** Fan-out
  at the MQ plus dedup at the collector and `ReplacingMergeTree` collapse residual
  duplicates, verified across a real failover by asserting offsets stay contiguous
  with no gaps and no duplicates in the post-failover window. It is **not**
  exactly-once: on a sustained ClickHouse outage the collector's fail-open path
  writes the batch to a JSONL dead-letter file on ephemeral pod disk and advances
  the cursor, so those rows never reach ClickHouse (`failure-modes.md` G1).
- **Network policies are enforced**, and they are not decorative: they are what
  makes the "all followers" and "long-lived connections still work after you
  applied a policy" failure modes real.

## Repository layout

| Path | What it is |
|---|---|
| `streamer/` | Publishes the metrics CSV as a stream |
| `messageQueue/` | The replicated log, leader election, stale-leader fence |
| `collector/` | Sharded consumption, dedup, ClickHouse writes |
| `apiGateway/` | Authenticated read API (`openapi.json` is generated from the code) |
| `helm-charts/telemetry/` | Umbrella chart with 5 subcharts + NetworkPolicies |
| `scripts/` | Everything below, each runnable on its own |
| `docs/` | Architecture, operations, per-component notes |

## Scripts

Run in this order if you are doing it by hand; `quickstart.sh` does all of it.

| Script | Purpose |
|---|---|
| `quickstart.sh` | All of the below, then verify |
| `kind-up.sh` | Create cluster + namespace, detect the API server IP, generate `dist/values-local.yaml`. `--down` tears down |
| `build-images.sh` | Build the four images. `IMG_TAG=... ` to tag |
| `load-images.sh` | `kind load` the images into the node |
| `bootstrap-auth-secrets.sh` | Create the Secrets. `--force` to rotate |
| `verify-clickhouse-ha.sh` | Check ClickHouse HA: pod counts, Keeper quorum, replica health, data identical on every replica, round trip. `--failover` also kills a replica and the Keeper leader under live ingest |
| `verify-deployment.sh` | 43–47 checks, varying only with which optional WAL log-line checks still find their line in the pod's log buffer. The failover path contributes exactly one check whether it is a scale-down promotion or a cold restart of the whole set. `--cleanup` for a clean slate |
| `mq-metrics-port.sh` | Publish the MQ `/metrics` endpoints on the host for browser/Postman use. `start` / `status` / `stop` |
| `save-images.sh` | Export images to a tarball for another machine |

## Deploying

Always two values files:

```bash
helm upgrade --install telemetry helm-charts/telemetry -n telemetry \
  -f helm-charts/telemetry/values-prod.yaml \
  -f dist/values-local.yaml \
  --wait --timeout 10m
```

- `values-prod.yaml` — machine-*independent*: auth Secrets, NetworkPolicies.
- `dist/values-local.yaml` — machine-*specific*: API server IP, image tags. Written
  by `kind-up.sh`.

Deploying with only the first file leaves **every MQ replica a follower
forever**, with all pods reporting `Running`. This is the most common failure on
a fresh machine.

## Observability

The MQ serves Prometheus text metrics on its health port, next to `/healthz`
and `/readyz`. They answer the two questions that logs cannot: **how deep is
the queue** and **what is the MQ using**.

```bash
./scripts/mq-metrics-port.sh start     # publish the endpoints
./scripts/mq-metrics-port.sh status    # one line per replica
./scripts/mq-metrics-port.sh stop      # tear down
```

Then open these in a browser or Postman — refresh to poll, nothing is cached:

```
http://localhost:30920/metrics    # replica 0
http://localhost:30921/metrics    # replica 1
```

```
replica-0  leader   depth=0         rss=34.8MiB heap=14.6MiB wal=0.8MiB
replica-1  follower depth=0         rss=16.3MiB heap=2.0MiB  wal=0.8MiB
```

The forwards run in the background and survive closing the terminal, so `start`
is needed only once — or after a `kubectl`/cluster restart. On a real cluster
skip the bridge and scrape the per-replica NodePort Service directly.

| Metric | Meaning |
|---|---|
| `mq_role` | `1` on the leader, `0` on a follower |
| `mq_retained_entries` | **queue depth** — published but not yet processed |
| `mq_log_head` / `mq_log_base` | last offset assigned / first offset still retained |
| `mq_consumer_lag` | how far a `kind="log-offset"` consumer trails the head |
| `mq_process_resident_memory_bytes` | RSS — what the 1Gi container limit applies to |
| `mq_go_memstats_heap_inuse_bytes` | live heap; grows with depth |
| `mq_go_goroutines` | goroutine count; a leak canary |
| `mq_wal_bytes` | on-disk write-ahead log — what a crash would replay |

Two things that are easy to misread:

- **Depth is "unacked by at least one active consumer", not by everyone.** The
  trim barrier is the *minimum* committed offset across the `collector` group,
  so with one collector depth and its lag are equal; with several, depth is the
  slowest one's backlog.
- **`mq_process_virtual_memory_bytes` is ~1.3GB and means nothing.** Go reserves
  that address space; it is untouched, not usage. Alert on RSS.

Cursors labelled `kind="external-position"` (the `streamer` group) carry a CSV
row number, not a log offset, so no lag is published for them — subtracting a
CSV row from the log head would report a lag in the millions.

Full metric table, the Helm values that expose it, and troubleshooting:
[operations.md](docs/operations.md#mq-metrics-queue-depth-and-memory).

## Tests

Everything is driven from the `Makefile` at the repo root. `make help` lists
every target.

```bash
make test           # unit tests for all four modules, with -race
make coverage       # statement coverage per module and combined
make coverage-html  # the same, as browsable HTML in dist/coverage/
make check          # lint (fmt+vet+tidy) + helm-lint + openapi-check + test (the CI gate)
```

Each component is its own Go module, so `go test ./...` cannot cross between
them; the Makefile loops for you and — unlike a bare `for` loop in a shell —
**fails fast**. A bare loop exits with the status of its *last* iteration, so a
failure in the first module would be reported as success.

Current coverage (run `make coverage` to reproduce):

| Module | Coverage | Statements |
|---|---|---|
| streamer | 51.9% | 286/551 |
| collector | 47.2% | 210/445 |
| messageQueue | 68.9% | 987/1432 |
| apiGateway | 57.4% | 342/596 |
| **combined** | **60.4%** | 1825/3024 |

The apiGateway tests include the OpenAPI checks: the committed `openapi.json`
must match what the code generates, and every documented operation must be
served by the real mux. To regenerate or verify it:

```bash
make openapi         # regenerate apiGateway/openapi.json from the code
make openapi-check   # fail if the committed file is stale
```

## Documentation

| Document | Covers |
|---|---|
| [quickstart.md](docs/quickstart.md) | From empty machine to working API |
| [architecture.md](docs/architecture.md) | Components, data flow, MQ design, data model, guarantees |
| [operations.md](docs/operations.md) | Health checks, failover, scaling, troubleshooting |
| [failure-modes.md](docs/failure-modes.md) | Crash-test results and known gaps |
| [components/](docs/components/) | Per-component detail |

## AI assistance

The whole stack was developed with AI assistance, and **[AI.md](AI.md)** is the
required write-up: the prompts used at each stage (repo bootstrap, code, unit
tests, build environment), what the AI did versus what a human decided, and — the
part that matters most — **where a prompt fell short and what manual work followed**.

Two things it is careful about:

- **Provenance is marked on every prompt.** ✅ means it was actually issued;
  🔁 means it was reconstructed from the code it produced, with the artefact cited,
  because the transcript predating the recorded window was summarised rather than
  verbatim. No invented prompt is presented as a real one.
- **The failures are documented in more detail than the successes.** 22 factual
  errors in the documentation, a bug report that described a different codebase, a
  `curl` check that passed while the browser genuinely failed, and several
  confidently-wrong answers that were only caught because the question was
  falsifiable.

```bash
make check        # reproduce the verification claims in AI.md
```

## License

This project is licensed under the [MIT License](LICENSE).
