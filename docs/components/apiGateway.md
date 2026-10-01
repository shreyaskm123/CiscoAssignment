# apiGateway

Read-only HTTP API over ClickHouse. No writes, no MQ dependency.

## OpenAPI spec

`apiGateway/openapi.json` (OpenAPI 3.1) describes every route. It is generated
from the code, never hand-edited:

| Part of the document | Where it comes from |
|---|---|
| Paths, methods, `operationId`, auth | the route table in `internal/httpapi/routes.go`, which `NewServer` also registers |
| Request/response schemas | reflection over the Go types the handlers return (`GPUListResponse`, `TelemetryResponse`, `TokenResponse`, `HealthResponse`, `ErrorResponse`, and `store.GPU` / `store.Telemetry`) |
| Parameter bounds | `httpapi.DefaultLimit` / `httpapi.MaxLimit`, exported for exactly this reason |
| Prose (summaries, error meanings) | hand-written in `internal/openapi/openapi.go` |

```bash
cd apiGateway
go generate ./...                    # rewrite openapi.json
go run ./cmd/genopenapi -check       # fail if the committed file is stale
go test ./internal/openapi/          # the staleness and consistency gates
```

What the tests enforce, so the file cannot quietly become fiction:

- the committed JSON is byte-identical to freshly generated output, and
  generation is deterministic (no diff churn);
- every registered route is documented, and nothing is documented that is not
  registered;
- each operation has a 200 with a schema, a 500, and a 401 iff it is behind the
  auth wrapper; `/healthz` carries an explicit empty `security` list rather than
  inheriting the document-level bearer requirement;
- path parameters exist in the path template and are required;
- the schemas carry the real field names and types (`source_ts` is
  `date-time`, `next` is nullable, `start_time`/`end_time` are optional);
- every `$ref` resolves and no component is unreferenced.

The route table is the single source of truth for both serving and
documentation, so a new endpoint appears in the spec in the same commit that adds
it.

## Deploy
- Kind: Deployment, 1 replica (chart `apigateway`).
- Listens on `:8080` (`HTTP_ADDR`).
- Service: `telemetry-apigateway` (ClusterIP :8080).

## Configuration (env)

| Env | Default | Meaning |
|---|---|---|
| `HTTP_ADDR` | `:8080` | HTTP listen address |
| `API_TOKENS_FILE` | `""` | file of long-lived bearer tokens; every `/api/v1` route requires one |
| `API_TOKENS` | `""` | inline token list, used only when `API_TOKENS_FILE` is unset |
| `API_TOKEN_SIGNING_KEY` | `""` | enables `POST /api/v1/token`; the route is absent when unset |
| `API_TOKEN_TTL` | `15m` | lifetime of an issued access token | `:8080` | HTTP listen address |
| `CLICKHOUSE_HOST` | `localhost` | ClickHouse host |
| `CLICKHOUSE_PORT` | `9000` | ClickHouse native port |
| `CLICKHOUSE_USER` | `default` | CH user |
| `CLICKHOUSE_PASSWORD` | — | CH password (empty = no auth) |
| `CLICKHOUSE_DB` | `telemetry` | Database |
| `CLICKHOUSE_TABLE` | `events` | Table |

## Endpoints

### `GET /healthz`
Liveness/readiness. Pings ClickHouse directly (`store.Ping`, 2s bound) — no table access, so it
stays fast no matter how large `events` grows. `200 {"status":"ok"}` / `500 {"error":"ping failed"}`.
This is what the Kubernetes probes target (not the heavy list endpoint).
```json
{ "status": "ok" }
```

### `POST /api/v1/token`
Exchange a long-lived static token for a short-lived access token. **Registered only when
`API_TOKEN_SIGNING_KEY` is set**; otherwise the route is absent and returns `404`. Only a
static token may be exchanged — an already-signed JWT cannot mint another.
```json
{ "access_token": "…", "token_type": "Bearer", "expires_in": 900,
  "expires_at": "2026-09-30T12:15:00Z", "identity": "reader" }
```

### `GET /api/v1/gpus`
List every GPU that has telemetry. Paged.
```json
{ "count": 10, "total": 247, "limit": 10, "offset": 0, "order": "asc", "next": "http://…?cursor=…",
  "gpus": [ { "id": "GPU-…", "device": "...", "index": 0, "hostname": "...", "model": "..." } ] }
```
Query: `limit` (1..1000, default 10), `offset`, `cursor`, `order` (`asc`|`desc`). `next` is an
absolute URL, or `null` on the last page.

### `GET /api/v1/gpus/{id}/telemetry`
Telemetry for one GPU, ordered by `(source_ts, event_id)`.
Query: `limit` (1..1000, default 10), `offset`, `cursor`, `order` (`asc` default | `desc`),
plus the optional window `start_time` / `end_time`. Times are RFC3339 / RFC3339Nano, the window
is **inclusive on both ends**, and it filters on `source_ts` (the streamer's current UTC time when
it produced the event; the CSV timestamp is never used). There is no separate `ts` field.
`order=desc` reverses the sort **and** the cursor-resume direction.
```json
{ "gpu_id": "GPU-…", "count": 10, "total": 41, "limit": 10, "offset": 0, "order": "asc", "next": null,
  "events": [ { "event_id":"…","source_ts":"2026-09-30T12:00:00.123Z","metric_name":"…","gpu_index":0,"device_name":"…","device_id":"…","model_name":"…","hostname":"…","value":1.5,"csv_line_offset":3,"loop_count":0,"cluster":"ai-prod-01","pod_name":"telemetry-streamer-…","mq_offset":42 } ] }
```
`start_time` / `end_time` are echoed back only when supplied. Errors (`{"error":"…"}`):
`400` invalid GPU id, invalid or reversed time window, malformed `limit`/`offset`/`order`, or an
invalid `cursor`; `401` missing/unknown/expired bearer token; `500` query failure.

## Internals
- Store = thin `clickhouse-go/v2` client; every read uses `FINAL` (collapses un-merged
  `ReplacingMergeTree` duplicates) and orders `ORDER BY source_ts, event_id`.
- GPU id is validated against `gpuIDRE` before it reaches SQL (parameterized `{gpu:String}` — no injection).
- `Open(ctx, …)` fails fast if the table doesn't exist — the API will not start before the
  collector has created `telemetry.events`. (Roadmap G3: add initContainer/backoff.)

## Caveats for users
- **Window semantics**: filters are on `source_ts`, the streamer's UTC time when the event was produced
  (the CSV timestamp is never propagated). It is also the table's sort and partition key, so
  these filters use the primary index. `received_at` (collector write time) is stored but not
  returned.
- Per-call `events` length is bounded by `limit` (default 10, max 1000), not by ClickHouse's
  max result size — it is never the full GPU count. Follow `next` to page.

## Known gaps
- ClickHouse auth is opt-in (G4). The API itself is authenticated: every `/api/v1` route
  requires `Authorization: Bearer <token>`, and only `/healthz` is deliberately open.