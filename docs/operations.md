# Operations runbook

## Deploy (local kind cluster)

```bash
./scripts/quickstart.sh --tag v1.0.0
```

That is the supported path. Do not `helm install` with only `values.yaml` or only
`values-prod.yaml`: both miss the generated image tags and the kind API-server
CIDR. See `docs/quickstart.md`.

### Disk preflight

Both `quickstart.sh` and `kind-up.sh` check free space **before** creating
anything, because running out of disk does not fail cleanly: `kind create` or an
image pull dies part-way through with an error that points nowhere near the cause.

Check it on its own with:

```bash
make preflight
```

The volume measured is the one **backing Docker's data root**, which on a laptop
is the VM's disk and *not* the host's. That distinction matters — on this machine
the host has 332 GB free while the Docker volume has 14 GB of 39 GB, so a
host-level `df` check would have passed while the build still died:

```
  disk    : ok 14 GiB free of 39 GiB on the Docker volume (/var/lib/docker)
```

Thresholds: **warn** below 12 GiB free, **fail** below 8 GiB (the stack needs
roughly 2 GB of images, 3 GB for the kind nodes, and the chart requests 5Gi x2 +
1Gi x3 of ClickHouse PVCs that ingest fills up).

On failure it prints the remediation rather than just failing:

```
  docker system df              # what is using it
  docker system prune -a        # remove unused images
  rm -rf dist                   # generated values and coverage output
```

Override for a genuinely small machine, or skip entirely:

```bash
MIN_FREE_GB=4 ./scripts/quickstart.sh
SKIP_DISK_CHECK=1 ./scripts/quickstart.sh
```

### Always change the image tag when you rebuild

A kind node that has already seen `telemetry-api:latest` **keeps serving the
cached image**. `helm upgrade` restarts the pod, the pod starts the old binary,
and the rollout reports success — so you end up debugging code that is not
running. This is not hypothetical; it cost an hour on this stack.

```bash
# build with a tag that changes every time, and load exactly that
TAG="rev$(date +%s)"
IMG_TAG="$TAG" ./scripts/build-images.sh apiGateway
kind load docker-image "telemetry-api:$TAG" --name telemetry

# then write the tag into dist/values-local.yaml and deploy BOTH files
./scripts/kind-up.sh --tag "$TAG"
helm upgrade telemetry ./helm-charts/telemetry -n telemetry \
  -f helm-charts/telemetry/values-prod.yaml \
  -f dist/values-local.yaml
```

`dist/values-local.yaml` pins every image tag with `pullPolicy: Never`: no
registry exists in kind, so a missing tag should fail loudly rather than
silently pull something else. Verify what actually started:

```bash
kubectl get pod -n telemetry -l app.kubernetes.io/name=apigateway \
  -o jsonpath='{.items[0].spec.containers[0].image}{"\n"}'
```

## Deploy with authentication (recommended)

The default install is unauthenticated (development mode). The authenticated
install wires per-service bearer tokens and scoped ClickHouse users that live
in Kubernetes Secrets, never in values files.

```bash
# 1. create the Secrets (writes credentials to ./.auth/, mode 600)
./scripts/bootstrap-auth-secrets.sh

# 2. install/upgrade with the authenticated overlay
helm upgrade --install telemetry ./helm-charts/telemetry -n telemetry \
  -f helm-charts/telemetry/values-prod.yaml
```

The bootstrap also creates `telemetry-api-token-key`, the HMAC key behind the
short-lived access tokens described below. If you already ran the script before
that existed, create just the key with:

```bash
./scripts/bootstrap-auth-secrets.sh --only tokenkey
```

## Token expiry and rotation

There are two kinds of API credential, and they behave differently.

| Credential | Lifetime | Held by | Purpose |
|---|---|---|---|
| Static service token | **never expires** | streamer, collector, messagequeue, ClickHouse clients | in-cluster service-to-service auth |
| Static API token | **never expires** | you | the long-lived secret; authenticates the token exchange and still works on every API route |
| Signed access token | **15m** (`apigateway.auth.tokenTTL`) | humans, Postman, scripts | what you actually send to `/api/v1/*` |

The in-cluster tokens are intentionally static: they are held by pods that are
already replaced by a Deployment rollout, and a TTL there would only create a
renewal loop with no security gain. The one credential a human is expected to
carry around is a different problem, so only that one expires.

### Getting an access token

Exchange the static token for a short-lived signed one. The response is
`Bearer` / `access_token` / `expires_in` / `expires_at` / `identity`:

```bash
STATIC=$(cat ./.auth/api-token.txt)

ACCESS=$(curl -s -X POST \
  -H "Authorization: Bearer $STATIC" \
  http://localhost:8080/api/v1/token | python3 -c 'import json,sys; print(json.load(sys.stdin)["access_token"])')

curl -s -H "Authorization: Bearer $ACCESS" http://localhost:8080/api/v1/gpus
```

Refresh by running the exchange again; the previous access token keeps working
until it expires on its own, so there is no in-flight breakage. Verification
allows 30s of clock skew, so a token is actually rejected at roughly
`tokenTTL + 30s`, not `tokenTTL`.

**Only the static token can exchange.** Presenting an *access* token to
`POST /api/v1/token` returns `401`:

```json
{"error":"the token exchange requires the long-lived API token, not an access token"}
```

This is deliberate. The access token is accepted on every other `/api/v1/*`
route, so if it could also renew itself a leaked 15-minute token would refresh
indefinitely and the expiry would be decorative. Renewal is the one operation
that requires the long-lived secret — which is exactly what makes a leak
detectable: someone holding only an access token has a 15-minute window and no
way to extend it.

The static token still authenticates `/api/v1/*` directly, which is deliberate
back-compatibility. The benefit of the exchange is that the long-lived secret no
longer has to be pasted into a Postman workspace, a shared shell, or a screen
share — it is used once, in one place, and only the 15-minute token travels
afterwards. If you want the short-lived token to be the *only* thing that works
from outside, remove the static entry from `telemetry-api-tokens` and keep a
copy in `.auth/`; the exchange will then be the only way in.

### What the access token is, and what it is not

It is a stateless HMAC-SHA256 token of the form
`base64url(payload).base64url(signature)` with `sub`, `iat`, `exp` and `jti`
claims. It is not a JWT and no JWT library is involved. There is no server-side
session and no denylist, which is the direct consequence of being stateless:

- **You cannot revoke one access token.** It stays valid until `exp`.
- **Rotating the signing key revokes all of them at once.** This is the blunt
  instrument, and it is the right one for a leak.
- **Rotating the static API token does not revoke outstanding access tokens**
  either, because the signed tokens were never derived from it. It only stops
  new ones being minted.

### Rotating credentials

Rotate one component at a time (or omit `--only` to rotate everything):

```bash
./scripts/bootstrap-auth-secrets.sh --force --only api        # API token
./scripts/bootstrap-auth-secrets.sh --force --only mq         # MQ tokens
./scripts/bootstrap-auth-secrets.sh --force --only clickhouse # ClickHouse users
./scripts/bootstrap-auth-secrets.sh --force --only interserver # ClickHouse replica credential
./scripts/bootstrap-auth-secrets.sh --force --only tokenkey   # signing key
./scripts/bootstrap-auth-secrets.sh --force                   # all five
```

Running it without `--force` never rotates; it only re-exports the current
credentials into `./.auth/` so the documented `cat .auth/api-token.txt` keeps
working. Those files are mode 600 and git-ignored.

How fast a rotation takes effect differs per component, and the reason is the
mount type, not the application code:

| Component | Mount | Propagation |
|---|---|---|
| apiGateway | whole directory | live in ~20s, **no restart** |
| messagequeue / clients | whole directory | live in ~20s, **no restart** |
| apiGateway signing key | env var (`secretKeyRef`) | **requires pod restart** |
| clickhouse | `subPath: users.xml` | **requires pod restart** |

The app re-reads its token file on every request / every RPC, so once the
kubelet has synced the directory the new token is in force immediately. A
`subPath` mount is a different story: Kubernetes never updates it in place, so
the file on disk keeps the old value no matter how often it is re-read.

The signing key is an env var, not a mounted file, precisely because rotating it
must invalidate every outstanding access token — a kubelet file sync would
race with pods that had already cached the old key, and a stale key would
quietly keep accepting tokens. Restart instead:

```bash
./scripts/bootstrap-auth-secrets.sh --force --only tokenkey
kubectl rollout restart deploy/telemetry-apigateway -n telemetry
```

ClickHouse rotation needs both halves, in this order:

```bash
./scripts/bootstrap-auth-secrets.sh --force --only clickhouse
kubectl rollout restart statefulset/telemetry-clickhouse -n telemetry
kubectl rollout restart deploy/telemetry-collector deploy/telemetry-apigateway -n telemetry
```

Restarting only ClickHouse leaves the clients holding the old password (Secret
env vars are bound at container start) and they fail auth; restarting only the
clients leaves ClickHouse serving the old hashes. Also note a client that starts
while ClickHouse is still coming up will crash-loop on
`dial tcp ...:9000: i/o timeout` until ClickHouse is listening — wait for
ClickHouse to be Ready first, and expect the apiGateway to take a few restarts.

### What authentication covers

| Component | Mechanism | Scope |
|---|---|---|
| apiGateway | `Authorization: Bearer <token>` | `/api/v1/*`; `/healthz` stays open for probes |
| apiGateway | `POST /api/v1/token` | exchanges a static token for a signed one; only registered when a signing key is configured |
| messagequeue | gRPC metadata bearer token | unary + stream RPCs, and follower replication |
| clickhouse | native protocol user + password | `telemetry_writer` (write), `telemetry_reader` (read), `default` locked to loopback |

## Network isolation

`networkPolicy.enabled` (default `true`) renders a default-deny policy plus an
explicit allow list. This **is enforced** by kind's kindnet CNI (verified, not
assumed). ClickHouse 8123/9009 are deliberately not opened.

| Policy | Direction | Allows |
|---|---|---|
| `default-deny-all` | in + out | nothing |
| `allow-dns-egress` | out | kube-dns UDP/TCP 53 |
| `mq-leader-ingress` | in | 50051 from streamer, collector, follower |
| `mq-follower-ingress` | in | 50052 from streamer, collector, leader |
| `clickhouse-ingress` | in | 9000 from collector, apigateway |
| `apigateway-ingress` | in | 8080 from anywhere (auth is in-process) |
| `mq-client-egress` | out | streamer/collector/messagequeue → 50051/50052 |
| `clickhouse-client-egress` | out | collector/apigateway → 9000 |

A connection needs permission in **both** directions: the destination's ingress
rule *and* the client's egress rule. The egress half is easy to forget, and the
resulting failure is deceptive — connections established before the policy was
applied keep working, so writes look healthy while every new dial times out
(`dial tcp 10.96.11.45:9000: i/o timeout`). After changing these policies,
restart the workloads so existing connections are re-established:

```bash
kubectl rollout restart statefulset/telemetry-clickhouse -n telemetry
kubectl rollout restart deploy/telemetry-collector deploy/telemetry-apigateway deploy/telemetry-streamer -n telemetry
```

Quick check that isolation is really on (an unlabelled pod must be blocked):

```bash
kubectl run nettest -n telemetry --image=busybox:1.36 --restart=Never --rm -i --quiet -- \
  sh -c 'nc -z -w 4 telemetry-clickhouse 9000 && echo REACHABLE || echo BLOCKED'
```

## Health checks

```bash
kubectl get pods -n telemetry
# expect 8/8 Running: apiGateway 1, clickhouse 1, collector 1, mq leader+follower, streamer x3

# port-forwards for local access
kubectl port-forward -n telemetry svc/telemetry-apigateway 8080:8080
kubectl port-forward -n telemetry svc/telemetry-clickhouse 8123:8123   # raw CH HTTP SQL
# if something local already owns 8123, use any free port and adjust the URLs below:
#   kubectl port-forward -n telemetry svc/telemetry-clickhouse 18123:8123

CHPW=$(cat ./.auth/ch-reader-pass.txt)
curl -s http://localhost:8080/healthz
curl -s -H "Authorization: Bearer $(cat ./.auth/api-token.txt)" http://localhost:8080/api/v1/gpus
curl -s -H "Authorization: Bearer $(cat ./.auth/api-token.txt)" \
  "http://localhost:8080/api/v1/gpus/<GPU-ID>/telemetry"
curl -s -u "telemetry_reader:$CHPW" -X POST http://localhost:8123/ \
  --data-binary 'SELECT count(), uniqExact(event_id) FROM telemetry.events FINAL'
```

`/api/v1/*` needs a token; `/healthz` does not. Those curls show the static
token for brevity — in real use, exchange it for a 15-minute access token first
(see above).

Send SQL to ClickHouse's HTTP interface as a **raw POST body**, not as a
`query=` form field:

```bash
# correct - the body IS the query
curl -s -u "telemetry_reader:$CHPW" -X POST http://localhost:8123/ \
  --data-binary 'SELECT count() FROM telemetry.events'

# wrong - ClickHouse does not form-decode the body, so it parses the literal
# text "query=SELECT+count()" and fails with Code 62
curl -s -X POST http://localhost:8123/ --data-urlencode 'query=SELECT count()'
```

(`+` meaning "space" only works in a URL query string, i.e. the `GET
...?query=SELECT+1` form, not in a POST body.)

## Calling the API from Postman

Prerequisites: the port-forward on 8080 above must be running, and
`./scripts/bootstrap-auth-secrets.sh` must have been run.

1. **Exchange the static token for a short-lived access token.** This is the
   one step that stops a long-lived secret living in your Postman workspace:

   ```bash
   STATIC=$(cat ./.auth/api-token.txt)
   curl -s -X POST -H "Authorization: Bearer $STATIC" \
     http://localhost:8080/api/v1/token
   # {"identity":"shreyas","token_type":"Bearer","access_token":"...","expires_in":900,"expires_at":"..."}
   ```

   Copy the `access_token` value — **not** the static token — for step 2. If you
   would rather not paste anything into Postman, skip straight to using the
   static token; it still works, it just never expires.

2. **Collection-level auth** — in Postman create/open a collection, then
   *Authorization → Type: Bearer Token → Token: `<access token>`*. Every request
   in the collection inherits it. Do **not** add an `Authorization` header by
   hand; let Postman manage it. When it stops working, run step 1 again rather
   than reaching for the static token.

3. **Health check (no token required)** — this is the one open endpoint:

   ```bash
   curl -s http://localhost:8080/healthz
   ```

   In Postman: `GET http://localhost:8080/healthz` → expect `200` and
   `{"status":"ok"}` with an empty `error` field.

4. **List GPUs** — `GET http://localhost:8080/api/v1/gpus`
   → `200` with `{"count":N,"total":T,"limit":10,"offset":0,"order":"asc","next":...,"gpus":[...]}`.
   Copy any `id` for the next step.

5. **Telemetry for one GPU, first page** —
   `GET http://localhost:8080/api/v1/gpus/<GPU-ID>/telemetry` (no query params)
   → `200`. Without a window the whole history matches, but only the first page
   comes back (see *Pagination* below): `count` is 10 and `total` is the full
   number of matching events. `next` is non-null unless everything fit on one
   page.

   Add `order=desc` to get the newest samples first, which is usually what you
   want when poking at a live GPU:

   ```bash
   curl -s -H "Authorization: Bearer $(cat ./.auth/api-token.txt)" \
     "http://localhost:8080/api/v1/gpus/<GPU-ID>/telemetry?order=desc&limit=5"
   ```

6. **Follow the `next` link** — this is the important part: `next` is an
   absolute URL that already carries the right `cursor` *and* preserves any
   filters you passed, so a client just follows it verbatim:

   ```bash
   curl -s -H "Authorization: Bearer $(cat ./.auth/api-token.txt)" "$NEXT"
   ```

   In Postman, paste the `next` value into the request URL. Do not hand-edit the
   `cursor` or compute your own `offset`: following `next` is what guarantees you
   see every row exactly once (see *Pagination* for why this data makes `offset`
   unsafe). `next` is `null` on the final page, and it carries `order` forward
   too, so a `desc` walk stays newest-first all the way down.

7. **Telemetry over a time window** — the parameters are
   `start_time` and `end_time` (**not** `from`/`to`), and they are compared
   against `source_ts`, the streamer's UTC time when it produced the event.
   Both bounds are inclusive. Use times around now, not the CSV's date:

   ```text
   GET http://localhost:8080/api/v1/gpus/<GPU-ID>/telemetry?start_time=<10 min ago>&end_time=<now>
   ```

   → `200`, with only the events produced inside that window (so `total` is
   smaller than the unwindowed call once the stack has been running longer than
   the window; rows from before this change carry the old July 2025 time and are
   excluded). The response also echoes back the normalised
   `start_time`/`end_time` it actually used, and the `next` link carries the
   window forward so page 2 cannot silently fall outside it.

8. **Telemetry with no data in the window** — this is the case that proves
   filtering is really happening rather than being ignored:

   ```text
   GET http://localhost:8080/api/v1/gpus/<GPU-ID>/telemetry?start_time=2020-01-01T00:00:00Z&end_time=2020-01-02T00:00:00Z
   ```

   → `200` with `count: 0` and `total: 0`. An empty window is *not* an error.

9. **Negative tests** (in Postman, temporarily clear the collection token or
   send a bad value):

   | Request | Expect |
   |---|---|
   | `GET /api/v1/gpus` with no token | `401` `{"error":"unauthorized"}` |
   | `GET /api/v1/gpus` with a wrong token | `401` `{"error":"unauthorized"}` |
   | `GET /api/v1/gpus` with an expired access token | `401` `{"error":"unauthorized"}` |
   | `GET /api/v1/gpus` with one character of an access token changed | `401` |
   | `GET /api/v1/gpus` with an access token minus its signature | `401` |
   | `POST /api/v1/token` with no token | `401` `{"error":"unauthorized"}` |
   | `POST /api/v1/token` with an **access** token | `401` (cannot self-renew) |
   | `start_time` later than `end_time` | `400` `start_time must not be after end_time` |
   | unparseable `start_time` | `400` `invalid start_time` |
   | unparseable `end_time` | `400` `invalid end_time` |
   | malformed GPU id (e.g. `..%2Fetc`) | `400` `invalid GPU id` |
   | well-formed but unknown GPU id | `200` with `count: 0` (not a 404) |
   | wrong parameter name, e.g. `?from=...` | `200`, window silently ignored |
   | `?limit=0` or `?limit=-1` | `400` `limit must be a positive integer` |
   | `?limit=abc` or `?limit=1.5` | `400` `limit must be an integer` |
   | `?limit=1001` (over the cap) | `400` `limit must not exceed 1000` |
   | `?offset=-1` or `?offset=abc` | `400` `offset must be a non-negative integer` |
   | `?limit=` or `?limit=%2010` (blank) | `200`, falls back to the default 10 |
   | `?order=dsc` or `?order=down` | `400` `order must be asc or desc` |
   | `?order=DESC` or `?order=%20desc%20` | `200`, normalised to `desc` |
   | ascending `cursor` reused with `order=desc` | `400` `invalid cursor` |

   The last two rows are the ones to watch: unknown GPU ids are *not* a 404, and
   a misspelled parameter is silently ignored rather than rejected. If you want
   strictness there, it is a small change in `handleTelemetry`. A *blank* limit is
   treated as "not supplied" on purpose, so a client that templates
   `?limit={{pageSize}}` and renders an empty string gets the default page
   instead of a `400` it cannot interpret.

## Pagination

`GET /api/v1/gpus` and `GET /api/v1/gpus/{id}/telemetry` both paginate.

| Field | Meaning |
|---|---|
| `count` | rows in *this* response |
| `total` | rows matching the filter, counted per request (may drift on a live table) |
| `limit` | page size actually used |
| `offset` | echo of the requested `offset` (see *Paging by cursor* below) |
| `order` | sort direction actually used: `asc` or `desc` |
| `next` | absolute URL for the next page, or `null` on the last page |

| Parameter | Default | Rules |
|---|---|---|
| `limit` | `10` | integer, `1`-`1000` |
| `offset` | `0` | integer, `>= 0`; random access, see below |
| `order` | `asc` | `asc` or `desc`, case-insensitive, whitespace trimmed |
| `cursor` | none | opaque resume key taken from a previous `next` link |

Example response (abbreviated):

```json
{
  "count": 10,
  "total": 6509,
  "limit": 10,
  "offset": 0,
  "order": "asc",
  "next": "http://localhost:8080/api/v1/gpus/GPU-abc/telemetry?cursor=ZXZlbnRfaWQ&limit=10",
  "events": [ ... ]
}
```

### Ordering

`order=desc` returns the **newest rows first**, which is what you usually want
when exploring a live GPU:

```bash
curl -s -H "Authorization: Bearer $(cat ./.auth/api-token.txt)" \
  "http://localhost:8080/api/v1/gpus/<GPU-ID>/telemetry?order=desc&limit=5"
```

- Telemetry sorts by `(source_ts, event_id)`; the GPU list sorts by `device_id`.
  `event_id` is only a tiebreaker for rows sharing a `source_ts`, and it makes
  the order total so paging can never skip or repeat a row.
- The default stays `asc` (oldest first) so existing callers are unaffected.
- A value that is neither `asc` nor `desc` is a `400`, not a silent fallback. A
  typo like `order=dsc` must not quietly hand back the oldest rows while you
  believe you asked for the newest - and nothing in the response would tell you.
- A blank `order=` is treated as "not supplied", like a blank `limit`.

`order` is part of the sort *key*, not a cosmetic flag, which has one
consequence worth knowing: **a cursor is only valid for the direction it was
issued in.** Replaying an ascending cursor against `order=desc` returns
`400 invalid cursor` instead of the wrong page - the resume comparison inverts
(`>` becomes `<`), so honouring it would return rows you have already read and a
client summing the pages would compute a wrong total with no error anywhere.
`next` carries `order` forward automatically, so following the links is always
safe.

### Paging by cursor

`next` resumes from a **cursor** (a cursor/keyset) rather than an `offset`, and
this matters on this data specifically. The events table is written to
continuously, and a batch of samples often shares a single measurement
timestamp, so rows arrive *behind* the position a client has already read. With
`OFFSET`, every such insert shifts all later rows forward and the boundary row is
handed out **twice** - a client that follows offsets double-counts a metric while
never seeing an error. Resuming on "greater than the last key I saw" cannot skip
or repeat a row, no matter what lands in between.

Rules worth knowing:

- **Follow `next`; do not compute offsets yourself.** It is the only way to
  guarantee complete, non-overlapping coverage.
- **There is no `prev`.** Paging is forward-only.
- `next` is an absolute URL built from the request, honouring `X-Forwarded-Proto`
  and `X-Forwarded-Host` when present, so it stays correct behind an ingress.
  It preserves your filters (`start_time`, `end_time`, `limit`, `order`) and drops
  any `offset`, because a resume key and an offset are different mechanisms.
- `next` is `null` exactly on the final page - not one request later. The API
  asks the database for one row more than you asked for and keeps it secret, so
  it never makes you fetch a trailing empty page.
- Treat `cursor` as opaque: it encodes the last row's sort key, and it is *not*
  signed, so the server cannot tell a cursor it issued from arbitrary well-formed
  base64 of the right shape.
  - Wrong shape or unparseable (`400 invalid cursor`): truncated, hand-edited, or
    carried over from the other endpoint. Fix the client.
  - Right shape but meaningless: an empty page (`count: 0`) with the true
    `total` and `next: null`. Restart the walk without the cursor - the non-zero
    `total` is how you tell "I am past the end" from "there is no data".
- **Data that arrives mid-walk is not stitched into that walk.** A row that lands
  behind your cursor belongs to a later walk. That is deliberate: the alternative
  is going backwards, which is what causes double-counting. A fresh walk sees it.
- `offset` still works for random access (`?offset=500` jumps to a position), but
  it is *not* safe for walking a table that is being written to, and it is never
  what `next` uses. If you pass both, `cursor` wins.
- Ordering is `source_ts, event_id` for telemetry and `device_id` for GPUs -
  both total orders, so a row cannot move between pages. Rows are still read with
  `FINAL`, so an at-least-once replay never shifts the page boundaries.
- `total` is counted over the whole filtered set in the same scan as the rows, so
  it can never disagree with the page you are holding. It is recomputed per
  request, so on a table that is still being written to it can *drift upward*
  during a long walk (you may see 8221 on page 1 and 8223 on the last). Treat
  it as "about this many rows right now", not a fixed snapshot size; the number
  of rows you actually read is the authoritative one. Use a bounded
  `start_time`/`end_time` if you need a stable count.

   credentials, not the writer ones:

   ```bash
   cat ./.auth/ch-reader-pass.txt
   ```

   ```text
   POST http://localhost:8123/?database=telemetry
   Authorization: Basic base64(telemetry_reader:<paste>)
   Body -> raw -> SELECT count() FROM events
   ```

   Expect a `200` with a plain-text number. This is exactly the same request
   the apiGateway makes internally, which makes it a good way to confirm the
   API is returning data rather than failing silently.

   Using the *writer* password here is the simplest way to confirm least
   privilege is real: a write attempt returns
   `Code: 497 ... Not enough privileges`.

## Loss / dedup audit

After any resilience test (or ad hoc), verify no holes and perfect 1:1 event→offset:

```sql
SELECT
  count(DISTINCT mq_offset)            AS offsets,
  max(mq_offset) - min(mq_offset) + 1  AS expect_range,
  min(mq_offset)                       AS min,
  count(DISTINCT mq_offset) - uniqExact(event_id) AS dupes,
  count()                              AS rows
FROM telemetry.events FINAL;
-- PASS criteria: offsets == expect_range, min == 0, dupes == 0
SELECT count() AS rows, uniqExact(event_id) AS distinct_events FROM telemetry.events FINAL; -- rows == distinct_events
```

`FINAL` is required because `ReplacingMergeTree` merges in the background.

## MQ metrics (queue depth and memory)

The MQ serves Prometheus text metrics on its health port alongside `/healthz`
and `/readyz`. That endpoint is unauthenticated and exposes queue internals
(consumer ids, offsets, depth), which is why it is not published by default.

### Publishing it to the host

The chart creates one NodePort Service per replica
(`<release>-messagequeue-metrics-0` and `-1`), so each pod has its own stable
URL and leader and follower are never interleaved into one trend. Enable with:

```bash
helm upgrade telemetry helm-charts/telemetry -n telemetry \
  -f dist/values-local.yaml \
  --set messagequeue.metrics.enabled=true --set messagequeue.metrics.nodePortBase=30910
```

On a local kind cluster the node ports still need bridging to the host:

```bash
./scripts/mq-metrics-port.sh start    # forwards, then prints the URLs
./scripts/mq-metrics-port.sh status   # one line per replica
./scripts/mq-metrics-port.sh stop
```

Then open, in a browser or Postman:

```
http://localhost:30920/metrics    # replica 0
http://localhost:30921/metrics    # replica 1
```

Refresh to poll; nothing is cached. The forwards run in the background and
survive closing the terminal.

The script uses `kubectl port-forward` rather than publishing a Docker port on
purpose. Docker here runs inside a Colima VM whose port forwarder goes stale: it
keeps accepting connections for a container that no longer exists and answers
every one with "Empty reply from server", and it does not recover on its own.
Observed holding a port for minutes after the container was gone, and failing a
freshly published port too. A port-forward rides the API server instead, which is
already reachable, and answered on the first attempt every time.

On a real cluster skip the bridge entirely and scrape the NodePort (or
ClusterIP) Service directly. `scripts/kind-up.sh` writes `extraPortMappings` for
these ports, but that only applies at cluster creation — it will not reach an
existing cluster.

### Reading the numbers

| metric | meaning |
|---|---|
| `mq_role` | `1` on the leader, `0` on a follower |
| `mq_log_head` | last offset assigned to an event; `-1` when the log is empty |
| `mq_log_base` | offset of the first entry **still retained**; everything below it is committed and trimmed |
| `mq_retained_entries` | **queue depth** — published but not yet fully processed (`head - base + 1`) |
| `mq_consumer_committed_offset` | one series per consumer, labelled by `kind` |
| `mq_consumer_lag` | how far a `kind="log-offset"` consumer trails the head |
| `mq_process_resident_memory_bytes` | RSS — this is what the 1Gi container limit applies to |
| `mq_process_virtual_memory_bytes` | reserved address space (~1.3GB); **ignore it**, it is untouched, not usage |
| `mq_go_memstats_heap_inuse_bytes` | live heap; the number that grows with depth |
| `mq_go_memstats_heap_alloc_bytes` | allocated, including uncollected garbage |
| `mq_go_memstats_sys_bytes` | obtained from the OS, including what Go holds but is not using |
| `mq_go_goroutines` | goroutine count; a leak canary |
| `mq_wal_bytes` | on-disk write-ahead log — what a crash would replay |
| `mq_wal_stat_failed` | `1` if the WAL could not be stat'd, so a missing log is not read as "0 bytes" |

Two things that are easy to misread:

- **`mq_retained_entries` is "unacked by at least one active consumer", not by
  everyone.** The trim barrier is the *minimum* committed offset across the
  active consumers of the `collector` group, so an entry survives until every
  one has committed past it. With a single collector, depth and that
  collector's lag are equal by definition. With several, depth is the slowest
  one's backlog.
- **Cursors labelled `kind="external-position"` have no lag.** The `streamer`
  group commits a row in the source CSV, not a log offset, so subtracting it
  from the head would report a lag in the millions. Lag is withheld for those
  series on purpose.

A follower legitimately reports its own depth: it holds a full replicated copy
of the log, which is what makes failover lossless. Clients never read from it —
they connect to the `<release>-messagequeue-leader` Service, which selects only
the pod labelled `component=leader`.

Quick check without the bridge:

```bash
kubectl -n telemetry exec telemetry-messagequeue-0 -- \
  wget -qO- http://127.0.0.1:8081/metrics | grep -E '^mq_(role|retained_entries)'
```

## Data volume and retention

**The events table has no TTL.** It is `PARTITION BY toYYYYMMDD(ts)` with no
retention clause, so it grows without bound for as long as the streamer runs.
Nothing in the chart or the collector schema prunes it, and the only TTLs in the
repo are unrelated (`REGISTRY_TTL_S` for the service registry, `API_TOKEN_TTL`
for access tokens).

Measured on the local kind cluster with the stock fan-out:

| | |
|---|---|
| Ingest rate | ~670,000 rows/hour (~16M rows/day) |
| On disk | ~1.9 KB/row |
| Disk growth | ~30 GB/day |
| Partitions | one per day, keyed on `source_ts` (event time) |

That fills the default kind node long before anyone notices. Check the runway
with:

```bash
kubectl -n telemetry exec telemetry-clickhouse-0 -- df -h /var/lib/clickhouse
```

Because the table is partitioned by ingest day, adding a TTL is cheap and
ClickHouse drops whole parts rather than deleting rows one at a time:

```sql
-- example only - pick a retention window you actually want
ALTER TABLE telemetry.events MODIFY TTL ts + INTERVAL 7 DAY;
```

Two things to know before you do:

- A TTL **deletes data**. There is no undo, and the apiGateway's `total` counts
  will drop to match. Decide the window deliberately rather than reaching for the
  shortest one that makes the warning go away.
- The TTL has to live in `collector/schema.TableDDL`, not just be applied by
  hand, or the next collector start against a fresh table will recreate it
  without retention.

Non-destructive cleanups, if you want disk back without losing rows:

```sql
-- merge parts; safe, keeps every row, speeds up queries
OPTIMIZE TABLE telemetry.events FINAL;
```

Duplicate replays are **not** a source of growth: `ReplacingMergeTree` collapses
them, and `count()` equals the `FINAL` count. If those two ever diverge, a
replay is stuck unmerged and `OPTIMIZE ... FINAL` is the fix.

### Telling event time from ingestion time

The table has two timestamps:

| Column | Meaning |
|---|---|
| `source_ts` | the **streamer's UTC time** when it produced the event; what the API's `start_time`/`end_time` filter on, and the table's sort and partition key. The CSV's own timestamp is never used |
| `received_at` | when the **collector wrote the row** (not returned by the API); `received_at - source_ts` is the pipeline latency |

```sql
-- how long ago the newest event was produced
SELECT dateDiff('second', max(source_ts), now64(9)) FROM telemetry.events;

-- how long ago the collector last wrote anything
SELECT dateDiff('second', max(received_at), now64(9)) FROM telemetry.events;
```

One consequence worth knowing: because the fixture is a fixed file, the
streamers finish their replay and then idle, so the row count stops growing and
the right-hand query above starts climbing. That is the end of the data, not a
stall. To tell them apart, check whether the count is still moving
(`SELECT count()` twice, a few seconds apart) rather than looking at a clock.

## ClickHouse high availability

ClickHouse runs as **2 replicas of one shard** (`telemetry-clickhouse-0/1`), each
holding a full copy of the data on its own PVC, coordinated by a **3-member
ClickHouse Keeper** ensemble (`telemetry-clickhouse-keeper-0/1/2`). Keeper is the
Raft store that replication needs: without a quorum every replicated table goes
read-only, which is why it is 3 members and not 1.

```
collector --INSERT--> Service telemetry-clickhouse --> one replica
                                   replica 0 <--- fetch parts (9009) ---> replica 1
                                       |                                    |
                                       +------ Keeper (2181), 3 members ----+
apigateway --SELECT--> Service telemetry-clickhouse --> any Ready replica
```

### Checking it

```bash
./scripts/verify-clickhouse-ha.sh              # no disruption
./scripts/verify-clickhouse-ha.sh --failover   # kills a replica, then the Keeper leader
```

It checks that all ClickHouse and Keeper pods are Ready and the counts match the
configuration, that Keeper has one leader and synced followers, that every replica
is writable and sees all its peers, that **both replicas hold identical events**
(count and content hash over a closed time window), that a row written on one
replica appears on the other, and that rows are still arriving. The failover mode
repeats the convergence check after the pods recover.

By hand:

```bash
kubectl -n telemetry get pods -l 'app.kubernetes.io/name in (clickhouse,clickhouse-keeper)'
kubectl -n telemetry exec telemetry-clickhouse-0 -- clickhouse-client -q \
  "SELECT replica_name, is_readonly, total_replicas, active_replicas, queue_size, absolute_delay
   FROM system.replicas WHERE table='events'"
# each replica should list total_replicas = active_replicas = 2, is_readonly = 0
```

`telemetry_reader` and `telemetry_writer` cannot read `system.replicas`; run these
as the in-container admin user (`kubectl exec ... clickhouse-client`, no password,
loopback only).

### What happens on a failure

| Failure | Effect |
|---|---|
| One ClickHouse pod dies | Its readiness probe fails, the Service stops routing to it, and the collector and API keep working on the other replica. When it returns it re-syncs from its peer and rejoins the Service. |
| One Keeper member dies | Quorum (2 of 3) holds. If it was the leader, a new one is elected within seconds. Replicas stay writable. |
| Two Keeper members die | No quorum: replicated tables go **read-only** (`is_readonly=1`) and inserts fail until a quorum returns. Reads keep working: the replicas stay in the Service and the API returns 200 (tested by scaling Keeper to 1). The collector retries and does not advance its MQ offset, so nothing is dropped; ingestion resumes by itself when Keeper is back. Keep the `minAvailable` PDBs in place during drains. |
| The node dies (kind: there is one worker) | Everything on it is lost until it returns. HA across nodes needs a cluster with at least 2 worker nodes. |

The collector commits its MQ offset only after the insert succeeds, so a write that
fails during a failover is retried, not dropped.

### Things to know

- **Replication is asynchronous.** An insert is acknowledged once it is on one
  replica; the other fetches it within moments. If that replica's disk were lost in
  that window, the unfetched rows would be lost. `insert_quorum=2` closes the
  window, but then writes fail whenever one of two replicas is down, which defeats
  HA, so it is not enabled.
- **Storage doubles.** Each replica holds a full copy, so the disk growth figures
  in "Data volume and retention" apply per replica. On kind both replicas share one
  node disk (the Docker VM's), which fills quickly. A full disk makes ClickHouse
  inserts fail (`Cannot reserve ... not enough space`).
- **Docker leftovers can fill that disk.** Every `docker build` leaves an untagged
  multi-stage builder image of about 1 GB. After many rebuilds, run
  `docker image prune -f` (dangling images only) and check
  `docker exec telemetry-worker df -h /var`.
- **The first install is noisy.** The collector and API start before Keeper has a
  quorum (the API also waits for the table the collector creates), so they restart
  a few times and settle on their own.
- **Adding a replica** (`clickhouse.replicas: 3`): upgrade with Helm, then restart
  the collector so its `ON CLUSTER` DDL creates the table on the new member; it then
  fetches the existing data from its peers.
- **Rotating the replica credential:** `bootstrap-auth-secrets.sh --force --only
  interserver`, then `kubectl rollout restart sts/telemetry-clickhouse`.
- **Upgrading an existing single-node install** needs the old non-replicated table
  dropped first (`DROP TABLE telemetry.events`) with the collector scaled to 0; the
  collector refuses to start against a table that is not a `Replicated` engine.
  Data in the old table is not carried over by that procedure.
- **Network:** port 9009 (replica fetches) and 2181/9234 (Keeper) are opened by
  NetworkPolicy only between the ClickHouse and Keeper pods. Keeper has no
  authentication of its own; that policy is its access control.

## MQ failover (automatic; leader election)

Leader election is on by default. There is nothing to promote by hand: a single
`coordination.k8s.io/v1` Lease named `mq-leader` decides who leads, and each pod
promotes or demotes itself from its own renewal loop.

Check the current state:

```bash
kubectl -n telemetry get lease mq-leader \
  -o jsonpath='{.spec.holderIdentity}{"\t"}{.spec.leaseTransitions}{"\n"}'
kubectl -n telemetry get pods -l app.kubernetes.io/name=messagequeue \
  -o custom-columns=NAME:.metadata.name,ROLE:.metadata.labels.app\\.kubernetes\\.io/component
```

The `component=leader` label and the Lease holder must agree. If they disagree,
the cluster is mid-election; re-read after a few seconds rather than acting.

### Failover timing

With the default `leaseDuration=15s`, `renewDeadline=10s`, `retryPeriod=2s`, a
leader that loses the API server stops renewing and fences itself within about
10s. A follower wins the Lease shortly after and promotes. Budget roughly
15–20s of ingest pause.

### Removing a leader on purpose

Deleting the pod is **not** a failover test. The StatefulSet recreates it under
the same identity and it re-acquires the Lease before a peer can win — observed
returning in ~11s with `leaseTransitions` unchanged. A StatefulSet also only
ever removes the **highest** ordinal, so scaling down removes a follower
whenever the leader sits on ordinal 0.

To actually remove the leader, first let the two pods re-race the Lease so the
holder is on the top ordinal, then scale below it:

```bash
kubectl -n telemetry delete lease mq-leader
# wait until the holder is the higher ordinal, then:
ORD=$(kubectl -n telemetry get lease mq-leader -o jsonpath='{.spec.holderIdentity}')
kubectl -n telemetry scale statefulset telemetry-messagequeue \
  --replicas="${ORD##*-}"
```

If the leader is on ordinal 0 there is no way to remove it while keeping a
survivor, because no lower replica exists. Scale to 0 and back to test recovery
instead — that proves WAL recovery and offset contiguity but is not a promotion
by a surviving peer. `scripts/verify-deployment.sh` reports which path it took.

### The stale-leader fence

`component=leader` cannot be the only routing signal. A partitioned leader has no
route to the API server, which is exactly why it lost contact, so it cannot
patch its own label away and stays in the leader Service's endpoints while
accepting writes a survivor is also accepting.

`/readyz` closes that gap locally: kubelet drops a NotReady pod from endpoints
without needing the API server. A leader answers 503 once its lease renewal goes
stale. `/healthz` deliberately ignores the lease, so a fenced leader is removed
from routing but **not** restarted — restarting it would discard its quorum
state.

If you see `503 leader but lease renewal is stale` on `/readyz`, that node has
lost the API server. Check its connectivity before assuming a broker fault.

## Verifying a deployment

`scripts/verify-deployment.sh` exercises the whole stack and exits non-zero on
any failed check:

```bash
IMG_TAG=<mq-tag> COLLECTOR_TAG=<collector-tag> scripts/verify-deployment.sh
scripts/verify-deployment.sh --skip-deploy          # verify what is already running
scripts/verify-deployment.sh --cleanup              # helm uninstall + delete MQ PVCs first
scripts/verify-deployment.sh --serial               # force sections 1-5 to run in order
```

Sections 1–5 (baseline, election, WAL, ingestion, API) only observe, so they run
concurrently; sections 6–7 (failover, scale) mutate the cluster and run serially
after them. Use `--serial` when you want output in strict order.

Pass `IMG_TAG` and `COLLECTOR_TAG` separately. Setting only `IMG_TAG` applies it
to the collector too, which asks the collector to pull the MQ tag and fails every
later `helm --wait` with an `ImagePullBackOff` that looks unrelated.

`--cleanup` uninstalls the release and deletes the MQ PVCs, so the next deploy
starts from an empty WAL. ClickHouse data is not touched.

**Known quirk**: a StatefulSet that received a resource-only upgrade can sit with
`updateRevision != currentRevision` but never roll its pod. Workaround if a rollout stalls:
`kubectl delete pod telemetry-messagequeue-0 -n telemetry --wait=false` (controller recreates
from the current template). Re-verify on a clean cluster rather than assuming a chart bug.

## Troubleshooting

- **MQ leader OOMKilled**: bump memory in `helm-charts/telemetry/charts/messagequeue/values.yaml`
  (limits cpu 1 / mem 1Gi) and re-`helm upgrade`; force the pod to recreate if rollout stalls
  (see quirk above).
- **ClickHouse OOM on big dedup queries**: raised to 2Gi in
  `helm-charts/telemetry/charts/clickhouse/values.yaml` (was 921.60MiB cap breach during
  `uniqExact` while merges ran).
- **apiGateway won't start / table missing**: the API fails fast if `telemetry.events` doesn't
  exist yet; wait for the collector to create it before port-forwarding, or add the
  initContainer wait (see `failure-modes.md` G3).
- **CH users.xml**: the `default` fragment must be a complete, valid user (password + networks +
  profile + quota). Do not use `<password remove='1'/>` alone — ClickHouse dies with Code 36.
- **Changing MQ gRPC port breaks liveness**: the probe targets the named port `grpc`; whenever you
  change `-addr`, update `ports[0].containerPort`/port name too.
- **Race detector hygiene**: run `go test -race ./...` in `streamer/`, `collector/`,
  `messageQueue/`, `apiGateway/` after changes.

## Clean-reset / full-wipe procedure

Used to establish a deterministic "fresh universe" before resilience runs:

1. `kubectl scale --replicas=0 deploy telemetry-streamer` and `telemetry-collector`;
   scale the MQ StatefulSet to 0.
2. Delete the MQ PVCs to reset WALs:
   `kubectl delete pvc -l app.kubernetes.io/name=messagequeue -n telemetry`.
3. Truncate the CH events table. **Use `ON CLUSTER`** — the table is
   `ReplicatedReplacingMergeTree`, so a plain `TRUNCATE` empties only the local
   replica and leaves the two replicas diverged (one empty, one full):

   ```sql
   TRUNCATE TABLE IF EXISTS telemetry.events ON CLUSTER 'telemetry'
   ```

   Run it on either replica; `ON CLUSTER` reaches the other. Verify both:
   `SELECT count() FROM telemetry.events` on `clickhouse-0` and `clickhouse-1`.
4. Scale everything back up (streamer 3, collector 1, MQ 2). `scripts/verify-deployment.sh --cleanup`
   does steps 1–2 for the MQ and reinstalls the release, leaving the ClickHouse data alone.