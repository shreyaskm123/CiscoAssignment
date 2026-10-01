# Quickstart — kind cluster to working API

Everything you need to run this stack locally from an empty machine, in order,
with the output you should expect at each step.

- **Time:** **~6–10 min measured**, from an empty machine to 45/45 checks passing.
  Breakdown of a real 5m25s run (warm Docker cache): cluster ~2m, build ~20s,
  image load ~30s, secrets ~2s, helm install ~2m, verification ~2m45s. A cold
  Docker cache adds several minutes for base-image pulls.
- **Cost:** nothing. Everything runs in Docker on your laptop.

If any step fails, stop there and fix it. Each step depends on the previous one,
and a later step will fail in a confusing way if an earlier one silently did the
wrong thing.

---

## 0. Prerequisites

| Tool | Version | Install |
|---|---|---|
| Docker Desktop | any recent | <https://www.docker.com/products/docker-desktop/> |
| kind | any recent | `brew install kind` |
| kubectl | any recent | `brew install kubectl` |
| helm | **≥ 3.8** | `brew install helm` |
| python3 | 3.9+ | ships with macOS / `brew install python` |

**Disk:** the stack needs ~10 GB free on the volume backing Docker's data root —
not the host disk, which on a laptop is the VM and can be far smaller.
`quickstart.sh` checks this before it creates anything and tells you how to
reclaim space if it is short. Check it on its own with `make preflight`; override
with `MIN_FREE_GB=<n>` or skip with `SKIP_DISK_CHECK=1`. See
[operations.md](operations.md#disk-preflight).

Confirm:

```bash
docker info >/dev/null && echo "docker ok"
kind version && kubectl version --client && helm version
```

Resources to free up: the stack runs 8 pods (3 streamer, 2 MQ, 1 collector,
1 ClickHouse, 1 API) and ClickHouse reserves 5Gi. **8GB RAM free is the
practical minimum.**

---

## The whole thing in one command

If you just want it running:

```bash
git clone <your-fork-url> telemetry && cd telemetry
./scripts/quickstart.sh --tag v1.0.0
```

That script performs steps 1–7 below and then verifies the result, ending in
`passed: 45   failed: 0`. It builds the images, so expect Docker to be busy for
a minute or two.

The rest of this document explains what it does, and how to run the steps
individually.

---

## 1. Create the kind cluster

```bash
./scripts/kind-up.sh --tag v1.0.0
```

> Pass the **same tag** you use in steps 2 and 3. This step writes it into
> `dist/values-local.yaml`, and if it does not match the tag you actually built,
> every pod fails with `ErrImageNeverPull`.

Expected:

```
==> reusing existing cluster 'telemetry'      # or "creating kind cluster ..."
==> generating dist/values-local.yaml
wrote .../dist/values-local.yaml (apiServer cidr 172.18.0.2/32, image tag v1.0.0)
==> ensuring namespace 'telemetry'
namespace 'telemetry' ready
```

This does three things beyond `kind create cluster`:

1. Creates the `telemetry` namespace.
2. **Detects the API server's real address** and writes it to
   `dist/values-local.yaml`. This is the single most important machine-specific
   value. On kind the apiserver listens on the control-plane node's bridge IP at
   port 6443, and kube-proxy DNATs the `10.96.0.1:443` service IP onto it.
   kindnet evaluates NetworkPolicy egress *after* that DNAT, so the MQ replicas
   see port 6443 and get dropped unless a rule allows that exact address. Get it
   wrong and **every MQ replica silently stays a follower forever while all of
   them report Running.**
3. Writes your image tag (`--tag`, default `latest`) and `pullPolicy: Never`
   into the same file.

Inspect what it generated:

```bash
cat dist/values-local.yaml
```

> **Never deploy with `values-prod.yaml` alone.** It used to hardcode
> `172.18.0.2/32`, which worked on exactly one laptop. Always pass both files.

---

## 2. Build the images

```bash
IMG_TAG=v1.0.0 ./scripts/build-images.sh
```

Expected:

```
==> building telemetry-streamer:v1.0.0 from ./streamer
==> building telemetry-collector:v1.0.0 from ./collector
==> building telemetry-mq:v1.0.0 from ./messageQueue
==> building telemetry-api:v1.0.0 from ./apiGateway
done. images:
  telemetry-streamer:v1.0.0
  telemetry-collector:v1.0.0
  telemetry-mq:v1.0.0
  telemetry-api:v1.0.0
```

To rebuild one component: `IMG_TAG=v1.0.0 ./scripts/build-images.sh collector`.

> **Use an explicit tag, not `latest`.** A kind node caches images. With
> `pullPolicy: IfNotPresent` a rebuilt `latest` leaves the *old* binary running,
> `helm upgrade` restarts the pod, and the rollout reports success while
> nothing changed. An explicit tag makes that impossible.

---

## 3. Load the images into the node

```bash
./scripts/load-images.sh v1.0.0
```

Expected:

```
==> loading telemetry-streamer telemetry-collector telemetry-mq telemetry-api:v1.0.0 into cluster 'telemetry'
  telemetry-streamer:v1.0.0 ... ok
  telemetry-collector:v1.0.0 ... ok
  telemetry-mq:v1.0.0 ... ok
  telemetry-api:v1.0.0 ... ok
all images present on the 'telemetry' node
```

Confirm on the node itself:

```bash
docker exec telemetry-control-plane crictl images | grep telemetry-
```

ClickHouse is *not* loaded here — that image comes from Docker Hub and the node
pulls it normally.

---

## 4. Create the auth Secrets

```bash
./scripts/bootstrap-auth-secrets.sh
```

Expected:

```
api tokens: created/rotated (1 client identity: shreyas)
mq tokens: created/rotated (streamer, collector, mq, follower)
clickhouse users: created/rotated (telemetry_writer, telemetry_reader)
token signing key: created/rotated
```

Nothing is printed to stdout. Values are written to `./.auth/` (git-ignored,
mode 600) and mirrored to `/tmp`:

| File | Used by |
|---|---|
| `.auth/api-token.txt` | your API calls in step 8 |
| `.auth/mq-streamer.txt`, `mq-collector.txt`, `mq-self.txt`, `mq-follower.txt` | the MQ replicas and clients |
| `.auth/ch-writer-pass.txt`, `ch-reader-pass.txt` | ClickHouse |
| `.auth/token-signing-key.txt` | signs short-lived API access tokens |

Re-running is safe: existing Secrets are kept. Use `--force` to rotate, and
note that rotating the signing key invalidates every access token already issued.

> Four MQ identities exist, and the chart mounts a specific Secret key for each.
> Omitting one makes the MQ pod's volume unbindable, so it never leaves
> `ContainerCreating`. `mq` is the replica's own peer-replication identity and is
> the one most easily forgotten.

---

## 5. Deploy with Helm

```bash
helm upgrade --install telemetry ./helm-charts/telemetry \
  -n telemetry \
  -f ./helm-charts/telemetry/values-prod.yaml \
  -f ./dist/values-local.yaml \
  --wait --timeout 10m
```

Expected: `Release "telemetry" has been installed. Waiting for deployment ...`
then `STATUS: deployed`.

Two files, both required:

| File | Provides |
|---|---|
| `values-prod.yaml` | auth Secrets and NetworkPolicies — the machine-*independent* settings |
| `dist/values-local.yaml` | API server IP, image tags, pull policy — the machine-*specific* settings |

`--wait` matters: it blocks until every Deployment and StatefulSet is ready, so
a failure here is a real failure rather than something you discover two steps
later.

---

## 6. Check everything is up

```bash
kubectl -n telemetry get pods
```

Expected — 12 pods, all `1/1 Running`:

```
NAME                                           READY   STATUS    RESTARTS   AGE
telemetry-apigateway-...                       1/1     Running   0          2m
telemetry-clickhouse-0                         1/1     Running   0          2m
telemetry-clickhouse-1                         1/1     Running   0          2m
telemetry-clickhouse-keeper-...                1/1     Running   0          2m   (x3)
telemetry-collector-...                        1/1     Running   0          2m
telemetry-messagequeue-0                       1/1     Running   0          2m
telemetry-messagequeue-1                       1/1     Running   0          2m
telemetry-streamer-...                         1/1     Running   0          2m   (x3)
```

Then confirm **exactly one** MQ pod leads:

```bash
kubectl -n telemetry get lease mq-leader \
  -o jsonpath='{.spec.holderIdentity}{"\t"}{.spec.leaseTransitions}{"\n"}'
kubectl -n telemetry get pods -l app.kubernetes.io/name=messagequeue \
  -o custom-columns=NAME:.metadata.name,ROLE:.metadata.labels.app\\.kubernetes\\.io/component
```

Expected — one leader, the other a replica, and a **transition count of 0**,
because whichever pod starts first simply wins the lease:

```
telemetry-messagequeue-0	0
NAME                       ROLE
telemetry-messagequeue-0   leader
telemetry-messagequeue-1   replica
```

Either pod may win; only the counts are predictable.

### Expect a few restarts on a first install

The collector creates the `telemetry` database, and both the collector and the
API gateway fail fast when it does not exist yet. On a cold install they lose
that race, exit, and get restarted — typically `RESTARTS` of 3–5 on
`apigateway` and `collector`, while everything else stays at `0`.

**This is expected and self-healing.** The counts stop climbing within a minute
and both settle at `1/1 Running`. To tell the settled state from a crash loop:

```bash
kubectl -n telemetry get pods    # compare the restart counts 30 seconds apart
```

A continuing climb is *not* expected — see Troubleshooting.

> **All followers means the API-server CIDR in step 1 is wrong.** Every replica
> looks healthy but nobody can win the lease. Re-run `./scripts/kind-up.sh --tag v1.0.0`
> and redeploy. This is the single most common failure on a fresh machine, and
> nothing else in the system reports it.

---

## 7. Verify all components

```bash
IMG_TAG=v1.0.0 ./scripts/verify-deployment.sh --skip-deploy
```

This is a real end-to-end check, not a smoke test. It runs 43–47 depending on
which optional WAL log-line checks still find their line in the pod's log buffer.
The failover path itself contributes exactly one check either way — whether the
survivor is promoted after a scale-down, or the whole set is scaled to zero and
re-elected from a cold start. The last full run passed 46:

| Section | Checks | Mutates cluster? |
|---|---|---|
| 1. baseline | the release is configured as intended (all 4 image tags + the API-server egress rule), and all 5 components have every pod Ready | no |
| 2. election | exactly one leader, label matches Lease holder, Service has exactly one endpoint, Lease is renewing | no |
| 3. WAL | leader **and** follower WALs are being written (mtime advancing) and non-empty on disk | no |
| 4. ingestion | row count is growing in ClickHouse; no duplicate `event_id` in the last 200k rows | no |
| 5. API | `/healthz` 200, unauthenticated request rejected 401, token exchange, authenticated data routes 200 | no |
| 6. failover | removes the leader, a survivor is promoted, ingestion resumes, **offsets stay contiguous with no gaps and no duplicates** | yes |
| 7. scale | scales 2→1→3→2, one leader throughout, every pod Ready, helm re-syncs cleanly | yes |

Sections 1–5 run concurrently (they only observe). 6–7 run serially after them
because they scale the StatefulSet and delete the Lease.

Expected ending:

```
== summary ==
   passed: 46   failed: 0

   all checks passed
```

(`passed` is 45 or 46 depending on the failover path; `failed: 0` is what
matters.)

It takes ~2m45s: ~21s for the concurrent observation phase, then ~65s for
failover and ~65s for scale, both of which wait on real leader elections. To see
the sections run in order instead, add `--serial`.

> **Pass both values files when you run this yourself.** It deploys again as part
> of its run, and a single-file invocation reverts the images to an unbuilt
> `latest` and deletes the egress CIDR:
>
> ```bash
> IMG_TAG=v1.0.0 \
> VALUES="./helm-charts/telemetry/values-prod.yaml ./dist/values-local.yaml" \
>   ./scripts/verify-deployment.sh --skip-deploy
> ```

---

## 8. Call the API

Every `/api/v1/*` route needs `Authorization: Bearer <token>`. `/healthz` does
not. The long-lived token in `.auth/api-token.txt` **is already valid on the
data routes** — exchanging it at `POST /api/v1/token` is optional (it gives a
15-minute JWT). You can skip 8a and go straight to 8b with `$TOKEN`.

Start a port-forward (leave it running):

```bash
kubectl -n telemetry port-forward svc/telemetry-apigateway 8080:8080
```

### 8a. (Optional) Exchange for a 15-minute JWT

Skip this if you just want to hit the API: use `TOKEN=$(cat .auth/api-token.txt)`
as the Bearer value on `/api/v1/gpus` and `/telemetry`. Come here only if you
want a short-lived token.

In another terminal:

```bash
TOKEN=$(cat .auth/api-token.txt)

curl -s -X POST localhost:8080/api/v1/token \
  -H "Authorization: Bearer $TOKEN" | python3 -m json.tool
```

```json
{
  "access_token": "eyJhbGciOiJIUzI1NiIs...",
  "token_type": "Bearer",
  "expires_in": 900
}
```

The static token is long-lived and should stay out of shell history and Postman
workspaces. This is why the exchange endpoint exists.

### 8b. Call a data endpoint

`$TOKEN` from `.auth/api-token.txt` is enough. `$JWT` from 8a also works.

```bash
TOKEN=$(cat .auth/api-token.txt)

curl -s localhost:8080/api/v1/gpus -H "Authorization: Bearer $TOKEN" \
  | python3 -m json.tool
```

> These examples use `python3`, which is already required by the scripts. Swap
> in `jq` if you prefer it — it is not a prerequisite.

```json
{
  "count": 10,
  "gpus": [
    {
      "id": "GPU-016f5163-a200-f674-2110-97949df1c49a",
      "device": "nvidia1",
      "index": 1,
      "hostname": "mtv5-dgx1-hgpu-020",
      "model": "NVIDIA H100 80GB HBM3"
    },
    ...
  ],
  "limit": 10,
  "offset": 0,
  "order": "asc",
  "total": 247,
  "next": "http://localhost:8080/api/v1/gpus?cursor=R1BVLTBiY2NhOGQ0..."
}
```

> **The list is paginated.** `count` is how many GPUs are in *this page*, not the
> total — the fixture has ~247, so `count` is `10` by default and `total` is the
> real number. Follow `next` to get the following page:
>
> ```bash
> curl -s "$NEXT_PAGE" -H "Authorization: Bearer $TOKEN" | python3 -m json.tool
> ```
>
> Same shape on the telemetry endpoint: `count` is the page size (default 10,
> capped by your `limit`) and `total` is all matching events.

The full route list is short — there are exactly four endpoints:

| Method | Path | Auth |
|---|---|---|
| `GET` | `/healthz` | none |
| `POST` | `/api/v1/token` | static token |
| `GET` | `/api/v1/gpus` | access token |
| `GET` | `/api/v1/gpus/{id}/telemetry` | access token |

There is no `GET /api/v1/gpus/{id}` — that returns 404 by design.

`{id}` is the GPU **id** from the list above (a `GPU-…` string), not the numeric
`index`. Fetch a live id and query it:

```bash
GPU_ID=$(curl -s localhost:8080/api/v1/gpus -H "Authorization: Bearer $TOKEN" \
      | python3 -c "import json,sys; print(json.load(sys.stdin)['gpus'][0]['id'])")

curl -s "localhost:8080/api/v1/gpus/$GPU_ID/telemetry" \
  -H "Authorization: Bearer $TOKEN" | python3 -m json.tool
```

> Use `GPU_ID`, not `GID`: **`GID` is zsh's group-id parameter** and assigning to
> it fails on macOS's default shell. `jq -r '.gpus[0].id'` is equivalent if you
> have `jq` installed — it is convenient but not required.

```json
{
  "count": 10,
  "events": [
    {
      "event_id": "evt_telemetry-streamer-76797f6ff4-brjkc_L160_O40",
      "source_ts": "2026-10-01T12:29:58.263085431Z",
      "metric_name": "DCGM_FI_DEV_GPU_UTIL",
      "gpu_index": 1,
      "device_id": "GPU-016f5163-a200-f674-2110-97949df1c49a",
      "value": 0,
      "cluster": "ai-prod-01",
      "pod_name": "telemetry-streamer-76797f6ff4-brjkc",
      "csv_line_offset": 40,
      "loop_count": 160,
      "mq_offset": 64348
    },
    ...
  ],
  "gpu_id": "GPU-016f5163-a200-f674-2110-97949df1c49a",
  "limit": 10,
  "offset": 0,
  "order": "asc",
  "total": 639,
  "next": "http://localhost:8080/api/v1/gpus/GPU-016f.../telemetry?cursor=..."
}
```

Narrow it to a time window, and page it:

```bash
curl -s "localhost:8080/api/v1/gpus/$GPU_ID/telemetry?start_time=$(date -u -v-10M +%Y-%m-%dT%H:%M:%SZ)&end_time=$(date -u +%Y-%m-%dT%H:%M:%SZ)&limit=5" \
  -H "Authorization: Bearer $TOKEN" | python3 -m json.tool
```

> `source_ts` is the streamer's UTC time when it produced the event. The CSV's own
> timestamp is never used, and there is no separate `ts` field.

### 8c. Prove auth is enforced

```bash
curl -s -o /dev/null -w '%{http_code}\n' localhost:8080/api/v1/gpus
# 401
```

---

## 8d. Scale streamer, MQ, collector

Defaults are 3 streamers, 2 MQ replicas, 1 collector.

```bash
kubectl -n telemetry scale deploy/telemetry-streamer --replicas=6
kubectl -n telemetry scale deploy/telemetry-collector --replicas=2
kubectl -n telemetry scale statefulset/telemetry-messagequeue --replicas=3
```

Wait until Ready, then check stored events stay duplicate-free:

```bash
kubectl -n telemetry get pods
kubectl -n telemetry exec telemetry-clickhouse-0 -- \
  clickhouse-client -q "SELECT count(), uniqExact(event_id) FROM telemetry.events"
```

`count()` and `uniqExact(event_id)` must stay equal. Streamer replicas only
sleep on rows they own, so extra streamers raise ingest until the MQ or
collector saturates. Extra collectors and MQ replicas add HA, not a second
copy of the data.

Scale back the same way (`--replicas=3`, `--replicas=1`, `--replicas=2`).

---

## 9. Watch the queue depth and memory

The MQ publishes Prometheus metrics on its health port. Publishing them to the
host takes one command, after which you can watch the queue in a browser:

```bash
./scripts/mq-metrics-port.sh start
```

```
http://localhost:30920/metrics    # replica 0
http://localhost:30921/metrics    # replica 1
```

Open either URL in a browser (or Postman) and refresh to poll:

```
mq_role{node="telemetry-messagequeue-0"} 1
mq_log_head{node="telemetry-messagequeue-0"} 14426
mq_log_base{node="telemetry-messagequeue-0"} 14427
mq_retained_entries{node="telemetry-messagequeue-0"} 0
mq_consumer_lag{node="...messagequeue-0",group="collector",...} 0
mq_process_resident_memory_bytes{node="telemetry-messagequeue-0"} 14958592
mq_go_memstats_heap_inuse_bytes{node="telemetry-messagequeue-0"} 3244032
mq_go_goroutines{node="telemetry-messagequeue-0"} 18
mq_wal_bytes{node="telemetry-messagequeue-0"} 6645345
```

Or a one-line summary per replica:

```bash
./scripts/mq-metrics-port.sh status
```

```
replica-0  leader   depth=0         rss=34.8MiB heap=14.6MiB wal=0.8MiB
replica-1  follower depth=0         rss=16.3MiB heap=2.0MiB  wal=0.8MiB
```

Read it like this:

- **`depth`** is `mq_retained_entries`: how many messages are published but not
  yet processed. While the pipeline runs it hovers near 0; stop the consumer
  (`kubectl -n telemetry scale deploy/telemetry-collector --replicas=0`) and it
  climbs, which is the fastest way to see the queue back up.
- **`mq_role` `1` is the leader.** Clients only ever connect to that pod; the
  follower keeps a full copy of the log for failover, so it legitimately reports
  a depth too.
- **`rss`** is what the 1Gi memory limit applies to. Ignore
  `mq_process_virtual_memory_bytes` — Go reserves ~1.3GB of address space and
  never touches most of it.
- **`depth` is not "unacked by everyone"** but by *at least one* active
  consumer. The barrier is the minimum committed offset in the `collector`
  group, so one collector makes depth and its lag identical by definition.

The forwards run in the background and survive closing this terminal, so you
only start them once. Tear them down when you are done:

```bash
./scripts/mq-metrics-port.sh stop
```

Full metric table and the Helm values that expose the endpoints:
[operations.md](operations.md#mq-metrics-queue-depth-and-memory).

---

## 10. Watch it work

```bash
# data flowing into ClickHouse
kubectl -n telemetry exec telemetry-clickhouse-0 -- \
  clickhouse-client -q "SELECT count() FROM telemetry.events"

# watch it climb
watch -n5 'kubectl -n telemetry exec telemetry-clickhouse-0 -- \
  clickhouse-client -q "SELECT count() FROM telemetry.events"'

# leader election in action
kubectl -n telemetry logs -l app.kubernetes.io/name=messagequeue -f | grep -E 'PROMOTED|DEMOTED'

# what the API gateway is doing
kubectl -n telemetry logs -l app.kubernetes.io/name=apigateway -f
```

---

## Teardown

```bash
./scripts/kind-up.sh --down        # deletes the cluster
./scripts/quickstart.sh --recreate # deletes it and builds a fresh one
```

`--down` leaves Docker volumes and `./dist` alone. `./.auth` persists too, so
credentials survive a cluster rebuild.

---

## Troubleshooting

**All MQ replicas are followers.** The API-server CIDR is wrong. Re-run
`scripts/kind-up.sh` and redeploy with both values files. Confirm the detected
address is really the control-plane's:

```bash
docker inspect telemetry-control-plane \
  --format '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}'
cat dist/values-local.yaml
```

**`ErrImageNeverPull`.** The tag in `dist/values-local.yaml` isn't on the node.
Rebuild and reload with the *same* tag: `IMG_TAG=v1.0.0 ./scripts/build-images.sh`
then `./scripts/load-images.sh v1.0.0`.

**MQ pods stuck in `ContainerCreating`.** A Secret key is missing. The MQ mounts
`telemetry-mq-tokens` keys `tokens` and `mq`. Re-run
`./scripts/bootstrap-auth-secrets.sh --force --only mq`, then restart the pods.

**`Error` exit from the API gateway on a fresh install.** It fail-fasts when
ClickHouse isn't accepting connections yet, and kubelet restarts it. One restart
right after install is normal; a restart loop is not.

**`helm --wait` times out.** Check what is actually not ready before assuming a
chart bug:

```bash
kubectl -n telemetry get pods
kubectl -n telemetry describe pod <not-ready-pod> | tail -30
```

**`v1 Endpoints is deprecated`** warning on `kubectl get endpoints`. Cosmetic —
the chart uses the stable v1 Endpoints API, which Kubernetes still serves.

**Port 8080 already in use.** Pick another local port:
`kubectl -n telemetry port-forward svc/telemetry-apigateway 9090:8080`.

---

## What each component does

`streamer → messagequeue → collector → ClickHouse → apiGateway`

| Component | Replicas | Role |
|---|---|---|
| streamer | 3 | Shards a bundled DCGM-style metrics CSV, publishes to the MQ |
| messagequeue | 2 | Leader elected via a Kubernetes Lease; `sync-replicas=1` so an acked event is on the survivor's disk before the leader can die |
| collector | 1 | Consumes the log in shards, deduplicates, writes to ClickHouse |
| clickhouse | 1 | Time-series storage |
| apigateway | 1 | Authenticated read API over ClickHouse |

See `docs/architecture.md` for the design and `docs/operations.md` for day-two
operations.