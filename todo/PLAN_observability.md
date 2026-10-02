# PLAN: Structured logging, OTLP/gRPC export, and Prometheus

## Status

Awaiting final validation

## Goal

Add JSON operational logs and correlated OTEL logs/metrics/traces to `runtime python` and `runtime api`, covering NATS run/control, HTTP API, VM execution, workers, reconnects, and cleanup. Export through insecure OTLP/gRPC to a Collector; Prometheus scrapes that Collector.

Confirmed exclusions: SDK/client, guest Python, frontend, static assets, demo, and local-tool instrumentation. SSE has connection telemetry only. Decision 3 excludes persistent log/trace backends and live deployment; the reference stack demonstrates receipt and metric queries. Dashboards and alerts remain outside the requested scope.

## Success Criteria

- Runtime operational logs are JSON with consistent fields and severity.
- Enabled signals reach an OTLP/gRPC receiver without TLS; service instances remain distinguishable across NATS reconnects.
- Prometheus scrapes the Collector and can query request counts, errors, latency, and worker availability; the app does not need a Prometheus listener.
- Trace context reaches request handling and execution; logs identify the active trace where available.
- Exporter outages do not block sandbox requests; shutdown flushes within a bounded deadline.
- Existing request/response payloads, Python stdout/stderr headers, CLI helpers, and SSE behavior remain compatible.

## Baseline (before implementation)

- Go 1.26.2 is installed locally and includes `slog.NewMultiHandler`; no OTEL/Prometheus dependencies exist. Command runners handle interrupt only.
- `request_logging.go` contains text-log NATS/HTTP wrappers, including NATS response-error/missing-response tracking and HTTP SSE flushing.
- `runtime_python.go` supervises registration/reconnect and cleanup; execution/config helpers reset context to background. `runtime_api.go` combines HTTP/frontend with NATS and has no registration supervisor.
- Locked worker snapshots expose desired count and idle/busy states; busy workers survive shrink until release. API overview performs an outbound micro INFO request with a 500 ms timeout. SDK/client calls carry no trace headers.
- Swarm stack contains NATS and KVM runtime only. Tests cover control/reconnect/worker/SSE/output behavior; readiness tests use stdout `ready:`. VM E2E is gated by `NATS_RUNTIME_E2E=1`.

## Decision Queue

10. Final validation (open): confirm the implementation and archive this plan, or identify revisions for a new implementation cycle.

## Confirmed Decisions

Each decision was explicitly confirmed with option `1`.

- Decision 1: All signals use insecure OTLP/gRPC to Collector; Prometheus scrapes Collector. No app Prometheus endpoint.
- Decision 2: Runtime commands receive logs/metrics/traces within the goal's scope. SSE has connection events/counts only; exclude SDK/client, guest, frontend/static, demo/local tooling.
- Decision 3: Optional Collector/Prometheus Compose reference; logs/traces use debug output. Swarm runtime receives external-endpoint env wiring only. No persistent log/trace backend or live deployment.
- Decision 4: JSON `slog` on stderr by default, env configuration only, endpoint opt-in. Preserve stdout readiness and Python/helper output; no log-format switch or telemetry CLI flags.
- Decision 5: W3C context and the span/instrument contracts below; parent-based root sampling defaults to 100%, configurable. Metrics export/scrape default to 15 s; duration buckets span 1 ms-60 s; bounded labels.
- Decision 6: Best-effort nonblocking export, invalid local setup fails early, queues/batches/timeouts bounded below. Interrupt/SIGTERM preserves NATS stop semantics and adds a shared 5 s telemetry shutdown budget; unfinished runs may have incomplete traces.
- Decision 7: Go/race/build, plaintext OTLP checks, and Collector/Prometheus smoke required. Real-VM E2E optional and reported separately; rollout defaults to export disabled.
- Decision 8: Explicit native Collector/gRPC automatic recovery; preserve existing NATS reconnect/Python supervisor. No new API startup retry or terminal registration supervisor; providers/identity stay stable.
- Decision 9: Create goal `Implement todo/PLAN_observability.md` and implement the accepted plan. Confirmed with option `1`.

## Implementation Tracker

- [x] Add observability configuration, logger, SDK providers, and shutdown ownership.
- [x] Instrument runtime HTTP/NATS boundaries, execution, lifecycle, worker state, and SSE connection telemetry within Decision 2 scope.
- [x] Add optional local Collector/Prometheus Compose configuration, Swarm runtime OTEL env wiring, and deployment documentation.
- [x] Add focused behavioral and export integration tests.
- [x] Run verification, document compatibility and rollout, and request final validation.

## Implementation Plan

All implementation choices below follow confirmed Decisions 1-8.

### Package and lifecycle

- Add `internal/observability/` with `config.go` (supported env/validation), `observability.go` (resources/providers/init/shutdown), and `logging.go` (JSON output plus OTEL bridge). Keep request wrappers in `internal/app/request_logging.go`.
- Use standard-library `log/slog`, the official `otelslog` bridge, and OTLP gRPC log/metric/trace exporters. Use SDK batch processors and a periodic metric reader; pin compatible module versions during implementation.
- Go 1.26.2 is locally verified and includes `slog.NewMultiHandler`; use it for JSON/OTEL fanout. Add only a small decorator for shared level filtering and context trace IDs; preserve attributes/groups/context and JSON output when export fails. No custom fanout or extra logging dependency.
- Runtime command owns one provider set, initialized before service startup and closed after runner success/error. Inject concrete dependencies, release partial setup, make cleanup idempotent, handle interrupt/SIGTERM, and reuse providers across NATS reconnects. Excluded commands require no SDK setup.
- All signals share service name/version and a generated unique process-lifetime instance ID. Use SDK resource env support; `OTEL_SERVICE_NAME` overrides a name from resource attributes, otherwise use that attribute or the default. Default version is the existing runtime service version; generate instance ID after env merge so reconnects cannot change it or env accidentally merge processes.

### Logging and configuration

- JSON fields: `time`, `level`, `msg`, `event`, `service.name`, `service.instance.id`; request fields include transport, endpoint/route, result, status/code, and numeric `duration_ms`. Include `trace_id`/`span_id` from active context; execution logs may include `run_id`, `thread_id`, and `worker_id`.
- Convert runtime operational `log.Printf` calls to structured events. Retain existing stdout `ready:` markers; add a structured readiness event. Python output and helper command transcripts retain their existing channels/formats. Shared wrappers must preserve behavior for excluded demo callers without initializing/exporting telemetry.
- Confirmed config: `LOG_LEVEL=info` (allowed: debug/info/warn/error); absent `OTEL_EXPORTER_OTLP_ENDPOINT` disables all export while JSON service logs continue. One configured endpoint enables logs/metrics/traces. Example for a runtime joined to the reference Compose network: `OTEL_EXPORTER_OTLP_ENDPOINT=http://otel-collector:4317`, `OTEL_EXPORTER_OTLP_PROTOCOL=grpc`, `OTEL_EXPORTER_OTLP_INSECURE=true`, `OTEL_SERVICE_NAME=nats-sandbox-runtime`.
- Supported variables: the endpoint, protocol (default/only `grpc`), insecure (default/only `true`), service name (default `nats-sandbox-runtime`), `OTEL_RESOURCE_ATTRIBUTES`, and `LOG_LEVEL`. Reject invalid values, TLS endpoint schemes, and conflicting supported transport settings; do not add telemetry CLI flags or a log-format switch. Decision 5 confirms `OTEL_TRACES_SAMPLER=parentbased_traceidratio`, `OTEL_TRACES_SAMPLER_ARG=1.0` (finite ratio 0-1), and `OTEL_METRIC_EXPORT_INTERVAL=15000` (positive milliseconds). Document supported variables explicitly and avoid undocumented per-signal endpoint overrides from exporter SDK defaults.
- Export metadata rather than request bodies, Python source/output, object contents, tokens, or credential-bearing URLs. Use redacted NATS URLs and bounded classified errors.

### Instrumentation and contracts

- Extend existing NATS wrapper without losing response-error/missing-response classification. Extract only `traceparent`/`tracestate` with the standard W3C propagator. Runtime handler callbacks accept explicit context plus `micro.Request`; thread that context through run/control/config/object/VM operations, updating internal adapters/tests. Root incoming NATS trace context on a background context to preserve current execution cancellation semantics; inbound tracing must not create client deadline propagation or new cancellation behavior.
- HTTP middleware preserves `http.Flusher`, first status, implicit 200, streaming, and request context. Instrument HTTP API routes, excluding static assets. Use registered route patterns rather than raw dynamic paths; unmatched API paths receive a fixed label. SSE records connection open/close events and connection counts without per-message spans, connection-length spans, or request-latency samples.
- API outbound NATS calls create client spans and inject active trace headers using `RequestMsgWithContext`, retaining the overview's existing 500 ms timeout. Built-in micro INFO handlers are library-owned; do not claim a consumer child span for those handlers. Server support remains compatible with clients sending no headers. SDK/client instrumentation and public SDK interface changes are excluded by Decision 2.
- Confirmed spans: HTTP server named by normalized API route; NATS consumer named by registered endpoint; child internal spans `runtime.prepare`, `runtime.execute`, and `runtime.upload`; API NATS client span `nats.request micro.info`. Use the inbound remote parent for a single incoming request. HTTP spans mark 5xx/unexpected handler failure as errors; NATS spans mark micro/reply/missing-response failures. Execution spans reflect execution independently of response delivery. End spans once, including failures.
- Confirmed root sampler is `ParentBased(TraceIDRatioBased(ratio))`, default ratio 1.0; an existing parent's sampled flag wins. Trace sampling does not reduce logs or metrics. Duration histograms use seconds with explicit buckets `[0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60]`. Metric periodic export and reference Prometheus scrape default to 15 seconds; export interval may be overridden by the supported env variable.
- Confirmed instruments below use cumulative counters, observable gauges, and explicit-bucket histograms. Test translated Prometheus counter `_total` and histogram `_seconds` names with the pinned exporter.

| Instrument | Type/unit | Attributes and recording boundary |
| --- | --- | --- |
| `sandbox.nats.requests` | Counter/1 | `endpoint`, `result`, `code`; once after each registered handler completes. |
| `sandbox.nats.request.duration` | Histogram/s | Same attributes; handler start through response attempt. |
| `sandbox.http.requests` | Counter/1 | `route`, `method`, `status_code`, `result`; once after each non-SSE API request completes. |
| `sandbox.http.request.duration` | Histogram/s | Same attributes; non-SSE API handler duration. |
| `sandbox.runtime.runs` | Counter/1 | `result`; once when run work finishes after worker acquisition and JSON decode; malformed/busy rejections count only in NATS requests. |
| `sandbox.runtime.run.duration` | Histogram/s | `result`; preparation through execution/upload completion or failure, excluding response delivery. |
| `sandbox.runtime.workers` | Observable gauge/1 | `state=desired,total,idle,busy`; one locked snapshot; total equals idle + busy, may exceed desired during shrink. |
| `sandbox.runtime.reconnects` | Counter/1 | No attributes; each successful NATS reconnect callback, excluding initial connection. |
| `sandbox.runtime.cleanup.deleted` | Counter/1 | No attributes; confirmed successful deletions, including deletions before a later cleanup failure. |
| `sandbox.http.sse.connections` | Counter/1 | `event=open,close`; once at each worker stream connection boundary. |

- Metric `result` is success/error. NATS `endpoint` is one of the registered run/control subjects; `code` is none/400/500/503/other/reply_error/no_response. Route values are fixed API patterns or `unmatched`; methods outside GET/HEAD/POST/PUT/PATCH/DELETE/OPTIONS map to `OTHER`; status codes outside 100-599 map to 0. IDs, raw paths/keys/subjects, source/output, and error messages are never metric labels.
- Register one worker gauge callback per active runtime registration; unregister on close so replaced pools cannot double-report. Add runtime-only NATS reconnect callbacks while preserving existing auth/retry/closed callbacks. Cleanup metrics increment only for actual deletions, not concurrent already-missing objects; preserve cleanup's existing returned/logged count semantics if different. No retiring state, worker IDs, or object names as labels.

### Collector and Prometheus

- Confirmed flow: app -> insecure OTLP/gRPC -> Collector; Prometheus -> Collector HTTP `/metrics`. Reference ports: `4317` for OTLP and `9464` for scrape; Prometheus UI/API uses `9090`. Do not add an app `/metrics` route or Prometheus listener.
- Add `observability/otel-collector.yaml`, `observability/prometheus.yaml`, and `observability/compose.yaml`. Compose runs observability services only; a separately started host/container runtime selects its reachable endpoint. Add optional OTEL env wiring to the existing Swarm runtime, defaulting to export disabled, and document how to supply an externally reachable Collector address.
- Collector uses an OTLP gRPC receiver, memory limiter/batch processors, and Prometheus metrics exporter. Log/trace pipelines use the debug exporter for receipt verification; no Loki/Tempo/Grafana services or persistent log/trace storage. Production operators can configure their Collector to forward those signals to existing backends.
- Reference networking: host runtime sends to `http://127.0.0.1:4317`; a runtime joined to the Compose network sends to `http://otel-collector:4317`; Prometheus scrapes `otel-collector:9464` on that network. A Swarm runtime requires a Collector address reachable from its container; host loopback is not the same destination.
- Document local startup, endpoints, log/trace receipt via Collector logs, sample Prometheus queries, and shutdown. Prometheus uses a named data volume for metric history; debug output provides inspection rather than indexed log/trace history.
- Preserve per-process metric identity when multiple runtimes share the Collector. Do not indiscriminately promote resource attributes into labels.
- Pin images; keep plaintext receiver/scrape ports on the chosen internal network, with loopback mappings only if local smoke checks require them. Existing runtime startup must not depend on Collector readiness.

## Edge Cases and Failure Behavior

- No Collector config: local JSON logs and existing service behavior continue; no implicit localhost connection attempts.
- Invalid local config/provider construction fails before serving and releases partial setup. Valid startup never waits for Collector readiness; receiver outages leave runtime available.
- Native SDK queues cap logs/spans at 2048 each; max batch 512, scheduling interval 5 s, log SDK exports directly from its queue with no separate export buffer, log/span string attribute limit 1024 characters. Keep trace blocking enqueue disabled; use pinned SDK overflow/drop behavior, no durable spool.
- Exporter/processor/metric-reader cycles time out after 5 s including retry; transient backoff starts at 1 s, caps at 2 s and the cycle deadline. Permanent/exhausted batches drop. Native gRPC reconnect uses a 1-second initial backoff capped at 2 seconds and permits future export without provider/process restart; dropped records are not replayed, while cumulative metric aggregates persist for later collection during process lifetime.
- Preserve NATS unlimited reconnect/250 ms wait, Python registration retry/1 s, cancellation, auth, and closed callbacks. API initial-startup failure and terminal-registration behavior remain unchanged.
- SDK export diagnostics use a separate local JSON handler that bypasses OTEL, emits at most one warning per 30 seconds with suppressed-count context, and does not retry logging through the failing bridge. Runtime JSON output remains available during receiver outages; disk durability is not promised.
- Cleanup/reconnect failures produce structured events; metric/span failures must not change NATS responses or HTTP status.
- Missing/malformed trace headers produce a new trace; do not add unbounded baggage or raw header values to telemetry.
- Run rejection, response-send failure, timeout, cancellation, and stream disconnect preserve current outcomes. HTTP status remains authoritative for transport metrics; run/VM spans represent execution failures separately.
- Confirmed shutdown: preserve existing NATS service-stop/connection-close semantics and HTTP shutdown's 5-second bound; on HTTP shutdown expiry force-close remaining connections so SSE handlers terminate. Stop runtime registration/cleanup and remove gauge callbacks, record final local lifecycle events, then shut down providers concurrently using one fresh shared 5-second deadline context. Telemetry shutdown errors produce a local warning and do not replace the runner's exit result; invalid startup still fails normally.
- Provider shutdown flushes records already completed/enqueued. Existing NATS close does not await active VM runs; this change does not add graceful run draining or client cancellation propagation. Unfinished runs may have incomplete traces when the process exits. Do not claim full execution completion or lossless export from shutdown.
- Shutdown and reconnect cleanup must be race-safe/idempotent; remove old pool callbacks and close all constructed providers even when one provider returns an error.

## Tests and Verification

- Behavioral tests: JSON fields/types/levels/groups/trace IDs/output routing and payload/token exclusions; endpoint opt-in; invalid config/sampler/interval and resource precedence; excluded caller compatibility. HTTP/NATS cases cover success, micro/reply/missing-response failures, status, dynamic routes, missing/malformed parents, static exclusion, and SSE flush/cancel with connection-only telemetry.
- Recording exporters/manual reader: verify parent/child stages, sampler ratio/parent behavior, metric boundaries/labels/buckets, worker resize/acquire/release and callback replacement. Use existing embedded JetStream test server for artifact operations and a small internal function seam defaulting to `RunLocalPythonExec` for prepare/execute/upload success/failure without KVM. Existing `fakeRuntimeObjectStore` covers cleanup List/Delete only; do not assume it implements full JetStream ObjectStore or add a public executor interface.
- Plaintext OTLP integration: verify all signals/resources, completed-record flush, unavailable startup, slow/transient/permanent export, nonblocking overflow, bounded diagnostics, and total provider shutdown <=5 s. Restart receiver on the same address; new data must resume with stable identity/counters. Separately restart embedded NATS, verify reconnect counters/requests/Python retry, and preserve API startup failure. Test service stop/SIGTERM separately from telemetry flush; do not assert VM draining.
- Add `observability/test.sh` and `make observability-smoke`: validate pinned Collector config and `promtool`, start the reference Compose stack, then run `OBSERVABILITY_SMOKE=1 tmp/go/bin/go test ./internal/app -run TestObservabilityCollectorPrometheusSmoke -count=1`. Test service traffic/log/span receipt, translated counter/histogram/gauge queries via Prometheus, distinct identities for two emitters, and recovery after Collector restart. Use loopback ports/config from Compose; a cleanup trap stops only reference services and preserves its metric volume. Require no KVM; use existing shell/Go/Docker tools.
- Required: `tmp/go/bin/go test ./...`, `tmp/go/bin/go test -race ./internal/... ./pkg/...`, `make build`, and `make observability-smoke`. Verification outcomes and the workspace scan limitation are recorded in Completion Notes. Full source-package verification uses an isolated copy to exclude existing generated assets/toolchain sources.
- Optional real-VM proof requires isolated live JetStream, assets, KVM, and `NATS_RUNTIME_E2E=1`: `NATS_RUNTIME_E2E=1 NATS_RUNTIME_URL=<test-url> tmp/go/bin/go test ./internal/app -run 'TestRuntime(PythonService|PythonControlSettings|API)EndToEnd' -count=1`. Report separately; missing prerequisites mean VM execution remains unverified, not failure of observability acceptance.
- Rollout/docs: endpoint empty by default in Swarm, optional Compose, supported env/contracts/queries/recovery/drop behavior and proof limits in README. Unset endpoint to stop export while retaining JSON; restore prior image to roll back text-log behavior. No live deployment; explicit final validation is required before plan archival.

## Review Coverage

Decisions 1-8 reviewed: topology, scope, backend deliverables, logs/config compatibility, telemetry interfaces, failures/shutdown, verification/rollout, and automatic recovery scope.

## Assumptions

- Runtime commands are the target; excluded instrumentation boundaries are confirmed by Decision 2.
- Prometheus stores metrics; Collector debug output demonstrates log/trace receipt without a persistent log/trace backend, confirmed by Decision 3.
- No production endpoint, backend credentials, or live-deployment authorization was supplied.

## Critique Pass

None. Decisions 1-8 cover material scope/interfaces, failure/lifecycle, recovery, tests, compatibility, and rollout. Confirmed concrete smoke command and corrected ObjectStore test coverage; no product edits or verification results are implied by plan readiness.

## Sources

- [Prometheus OTLP guide](https://prometheus.io/docs/guides/opentelemetry/): native OTLP ingestion uses HTTP; recommended gRPC path therefore uses a Collector bridge.
- [Collector Prometheus exporter](https://github.com/open-telemetry/opentelemetry-collector-contrib/tree/main/exporter/prometheusexporter): exposes received metrics for HTTP scraping.
- [Go exporters](https://opentelemetry.io/docs/languages/go/exporters/), [otelslog](https://pkg.go.dev/go.opentelemetry.io/contrib/bridges/otelslog), and [OTLP/gRPC logs exporter](https://pkg.go.dev/go.opentelemetry.io/otel/exporters/otlp/otlplog/otlploggrpc): official SDK/bridge/export interfaces; verify pinned-version options when implementing.
- [Go trace SDK](https://pkg.go.dev/go.opentelemetry.io/otel/sdk/trace) and [Go log SDK](https://pkg.go.dev/go.opentelemetry.io/otel/sdk/log): bounded batch queue/timeout options; do not enable blocking trace enqueue. Exporter timeouts take precedence over retry limits.

## Completion Notes

Cycle 1: Implementation complete after Decision 9; awaiting Decision 10 final validation. The plan remains under `todo/` until explicit user confirmation permits archival to `todo/done/PLAN_observability.md`.

- Implemented JSON `slog` plus official OTEL providers/exporters; runtime Cobra runners own initialization, SIGINT/SIGTERM, and concurrent bounded shutdown. Context reaches NATS control/run, VM preparation/execution/upload, HTTP API, and outbound INFO. Reconnect/cleanup/worker/SSE telemetry follows the accepted contracts. Export remains opt-in.
- Added optional pinned Collector 0.161.0 / Prometheus 3.15.0 Compose stack, external-endpoint Swarm env wiring, `observability/test.sh`, Make target, and `observability/README.md`. No live Swarm deployment. Reference services were stopped after checks; the named metric volume remains.
- Pinned OTEL Go 1.46.0, log SDK/exporter 0.22.0, and otelslog 0.20.1. The pinned log SDK's separate export-buffer option is now a no-op, so the native direct-queue exporter provides the tighter bound. Prometheus translates the unit-1 worker gauge to `sandbox_runtime_workers_ratio`; values remain integer worker counts. Queries/docs match the verified translation.
- `make build`: passed. `tmp/go/bin/go test -race ./internal/... ./pkg/...`: passed. Tests cover plaintext receipt, resource identity, parent sampling/casing, context propagation, transport/execution error separation, preparation/execution/upload failure spans, bounded labels/buckets, SSE/forced shutdown, invalid config, SIGTERM, queue saturation, transient/permanent failures, future export recovery, NATS reconnect, worker shrink/unregister, and actual partial cleanup deletion counts.
- `make observability-smoke`: passed (28.178 s). Pinned config validation and promtool passed; real synthetic NATS/HTTP/VM-seam traffic from two OS processes produced logs/traces and Prometheus count/error/histogram/gauge/SSE queries. Collector restart recovered future logs and cumulative metrics with stable process identities.
- Direct workspace `tmp/go/bin/go test ./...` and `go mod tidy` encounter pre-existing generated sources under `tmp/go/test` and extracted `firecracker-assets`, including invalid relative imports, intentionally invalid Go test fixtures, and inaccessible filesystem directories. `tmp/go/bin/go -C /tmp/nats-observability-check test ./...` passes for an identical copy of the source packages (`cmd`, `internal`, `pkg`) and module files, excluding those generated trees. Module tidy also passed in that isolated source copy.
- Required race checks exposed existing readiness-buffer races in reconnect tests; test buffers now synchronize concurrent writes/reads. No runtime behavior change was needed for that test-harness issue.
- Real Firecracker/KVM E2E remains unrun and unverified. Synthetic execution/upload proof does not establish hardware/VM behavior. Request/response/Python metadata and readiness compatibility tests pass. No commit or production deployment was performed.
