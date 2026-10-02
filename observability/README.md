# Runtime observability

`runtime python` and `runtime api` write JSON operational logs to stderr. Stdout
`ready:` markers and Python response metadata remain available. Export is off
unless `OTEL_EXPORTER_OTLP_ENDPOINT` is set. No app metrics listener is needed.

```sh
docker compose -f observability/compose.yaml up -d
OTEL_EXPORTER_OTLP_ENDPOINT=http://127.0.0.1:4317 bin/nats-sandbox-runtime runtime api
docker compose -f observability/compose.yaml logs -f otel-collector
```

Collector receives all three signals on plaintext OTLP/gRPC port 4317. Debug output
shows logs/traces without storing searchable history. Prometheus scrapes internal
port 9464 and serves http://127.0.0.1:9090. Only loopback 4317/9090 are published.
Metric history uses a named volume. Stop with
`docker compose -f observability/compose.yaml down`; the volume remains.
`make observability-smoke` validates configs, exercises synthetic service traffic,
checks receipt/recovery, and stops this optional stack. Run it when the stack is
not already in use.

| Environment variable | Default / accepted values |
| --- | --- |
| `LOG_LEVEL` | `info`; debug/info/warn/error |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | absent disables export; `http://host:port` only, no credentials/path/query |
| `OTEL_EXPORTER_OTLP_PROTOCOL` | `grpc` only |
| `OTEL_EXPORTER_OTLP_INSECURE` | `true` only |
| `OTEL_SERVICE_NAME` | resource attribute `service.name`, otherwise `nats-sandbox-runtime` |
| `OTEL_RESOURCE_ATTRIBUTES` | standard SDK comma-separated resource attributes |
| `OTEL_TRACES_SAMPLER` | `parentbased_traceidratio` only |
| `OTEL_TRACES_SAMPLER_ARG` | `1.0`; finite ratio 0-1 |
| `OTEL_METRIC_EXPORT_INTERVAL` | `15000`; positive milliseconds |

Only these settings are supported. Per-signal endpoint/transport/sampler overrides
are not supported. Version defaults to `0.0.1`; each process generates its own
`service.instance.id` after resource merging. Every signal shares that identity,
which survives Collector/NATS reconnects. Prometheus `job`/`instance` identify
service/process; other resource attributes are not promoted to every metric.
`honor_labels` preserves those identities during scrape.

The pinned Prometheus exporter adds `_ratio` to the dimensionless worker gauge;
its values remain integer worker counts. Duration histograms use `_seconds`, and
counters use `_total`.

Example PromQL, checked with the pinned exporter:

```promql
sum by (instance, endpoint, result) (sandbox_nats_requests_total)
sum by (instance, route, result) (sandbox_http_requests_total)
histogram_quantile(0.95, sum by (le, instance) (rate(sandbox_runtime_run_duration_seconds_bucket[5m])))
sandbox_runtime_workers_ratio{state="idle"}
sandbox_runtime_reconnects_total
```

W3C `traceparent`/`tracestate` is extracted on NATS/HTTP and injected into API micro
INFO requests. Preparation, execution, and artifact upload are child spans. Root
sampling follows the configured ratio; parent sampled flags win. Logs/metrics are
not sampled. Static assets, SDK/client helpers, guest code, and demo/local commands
are excluded. SSE emits connection events/counts only. Bodies, code/output, tokens,
and object contents are not exported. Errors use bounded classifications; URLs
omit credentials.

Startup does not wait for Collector readiness. Queues cap logs/spans at 2048 each;
batches cap at 512 and schedule every 5 seconds. The pinned log SDK exports directly
from its queue, with no separate export buffer. Overflow follows SDK behavior
(oldest logs, new spans). String attributes cap at 1024 characters. Export cycles
include retry within 5 seconds (initial backoff 1 second, max 2 seconds). Native
gRPC reconnect recovers future exports without provider recreation. Dropped records
are not replayed; cumulative metrics survive outages in memory. Export warnings
bypass OTEL, limited to one per 30 seconds with suppression counts. No durable spool.

SIGINT/SIGTERM stops the runtime, then shuts down all providers concurrently under
a fresh shared 5-second deadline. HTTP shutdown allows 5 seconds, then closes
remaining streams. Existing NATS shutdown does not wait for active VM runs; those
traces may remain incomplete. Telemetry shutdown errors preserve the runner result.
NATS keeps unlimited reconnects/250 ms waits; Python registration retries every
second. API initial connection failure and terminal-registration behavior remain
unchanged.

A runtime on the Compose network uses `http://otel-collector:4317`; a host runtime
uses `http://127.0.0.1:4317`. Swarm's optional OTEL env wiring requires an external
Collector address reachable from its container. Container loopback refers to that
container. Operators configure production Collector forwarding to existing log/
trace backends. This change does not deploy Swarm.

Required checks: `tmp/go/bin/go test ./...`,
`tmp/go/bin/go test -race ./internal/... ./pkg/...`, `make build`,
`make observability-smoke`. Synthetic VM tests use embedded JetStream to verify
execution instrumentation/uploads, not Firecracker/KVM. Real VM E2E remains opt-in
with `NATS_RUNTIME_E2E=1` and an isolated `NATS_RUNTIME_URL`.
