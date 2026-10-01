# apiGateway

Read-only REST API over the `telemetry.events` ClickHouse table. Lists the GPUs
that have telemetry and returns their measurements by time window. It is the
only component that talks to ClickHouse with a **read-only** account.

- Detailed behaviour, env vars and caveats: [docs/components/apiGateway.md](../docs/components/apiGateway.md)
- End-to-end usage with curl/Postman: [docs/operations.md](../docs/operations.md)

## Routes

| Route | Auth | Purpose |
|---|---|---|
| `GET /healthz` | none | Liveness/readiness. Pings ClickHouse directly (`store.Ping`, 2s bound) — no table access, so it stays fast however large `events` grows. This is what the Kubernetes probes target |
| `POST /api/v1/token` | static token | Exchange a long-lived token for a short-lived JWT. Registered only when `API_TOKEN_SIGNING_KEY` is set; absent (404) otherwise |
| `GET /api/v1/gpus` | bearer | One page of GPUs with telemetry |
| `GET /api/v1/gpus/{id}/telemetry` | bearer | Measurements for one GPU, ordered by `(source_ts, event_id)` |

Every `/api/v1` route requires `Authorization: Bearer <token>`. Only `/healthz`
is deliberately open, so kubelet can probe it without a credential.

## Build and test

```sh
go build ./cmd/api
go test ./... -race
```

## Run

```sh
CH_USER=telemetry_reader CH_PASSWORD=... \
CLICKHOUSE_HOST=clickhouse CLICKHOUSE_PORT=9000 \
API_TOKENS_FILE=/etc/telemetry/auth/tokens \
API_TOKEN_SIGNING_KEY="$(cat /etc/telemetry/auth/token-signing-key)" \
API_TOKEN_TTL=15m \
go run ./cmd/api
```

Tokens arrive as environment variables, never as flags — flags are visible in
`ps` and `/proc` to everything in the container. `API_TOKENS_FILE` is re-read on
every request, so a rotated Secret takes effect without a restart. The signing key
is injected from a Secret by the chart (`secretKeyRef`), and a key that is set
but malformed is a startup error rather than being silently ignored.

## OpenAPI

The spec is generated from the code, not hand-written:

```sh
make openapi         # write openapi.json
make openapi-check   # fail if the committed file is stale
```

A test (`internal/openapi`) fails if `openapi.json` no longer matches what the
code generates, so the spec cannot silently drift.

## Internals worth knowing

- **Every read uses `FINAL`.** The table is `ReplicatedReplacingMergeTree`, so a
  bare read would return un-merged duplicate rows left by at-least-once
  delivery. The two GPU-list queries omit `FINAL` safely because they
  `GROUP BY device_id`, which collapses duplicates before counting.
- **Paging is by resume key on `(source_ts, event_id)`, not `OFFSET`.** The
  table is written continuously and every row in a batch can share one
  `source_ts`, so a row ingested between two page requests would land *before*
  the current position and push everything along — an `OFFSET` walk would re-read
  that boundary row and double-count it. Resuming on "greater than the last key
  I saw" cannot skip or repeat a row regardless of what arrives meanwhile.
- **GPU id is validated by regex before it reaches SQL**, and every value is
  bound as a ClickHouse parameter (`{gpu:String}`) — no string interpolation
  into SQL.
- `Open(...)` fails fast if the table does not exist, so the API will not start
  before the collector has created `telemetry.events`.