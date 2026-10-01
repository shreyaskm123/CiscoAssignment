# Telemetry platform — documentation

Map of the docs folder.

| Document | What it covers |
|---|---|
| [../AI.md](../AI.md) | **Required write-up**: prompts used per stage, what AI did vs a human, and where prompts fell short |
| `quickstart.md` | **Start here.** From an empty machine to a working API, with expected output at each step |
| `architecture.md` | Components, data flow, message queue design, data model, dedup, guarantees |
| `operations.md` | Health checks, failover procedure, scaling, troubleshooting, audit queries |
| `failure-modes.md` | Crash-test results, failure-mode analysis, known gaps and roadmap |
| `components/apiGateway.md` | API: endpoints, response shapes, env config, caveats |
| `components/messageQueue.md` | MQ: roles, flags, RPC surface, replication, dedup, retention, failover |
| `components/streamer.md` | Streamer: env config, sharding, cursors, publish/drain semantics |
| `components/collector.md` | Collector: env config, consume/flush loop, dead-letter, dedup strategy |

## System in one paragraph

The platform ingests a DCGM-style metrics CSV, publishes it as a stream of events through
a custom replicated message queue, consumes the log in sharded fashion, and lands the
data in ClickHouse for historical queries via a read API:

`streamer (CSV) -> messagequeue (leader + sync-replicated follower) -> collector (sharded) -> ClickHouse -> apiGateway`

All components run as a single kind cluster deployed from the `helm-charts/telemetry`
umbrella chart (subcharts: `streamer`, `collector`, `messagequeue`, `clickhouse`, `apigateway`).