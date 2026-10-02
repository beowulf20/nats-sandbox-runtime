#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
compose=(docker compose -f observability/compose.yaml)
trap '"${compose[@]}" down' EXIT
"${compose[@]}" run --rm --no-deps otel-collector validate --config=/etc/otelcol-contrib/config.yaml
"${compose[@]}" run --rm --no-deps --entrypoint /bin/promtool prometheus check config /etc/prometheus/prometheus.yml
"${compose[@]}" up -d
OBSERVABILITY_SMOKE=1 "${GO:-tmp/go/bin/go}" test ./internal/app -run '^TestObservabilityCollectorPrometheusSmoke$' -count=1 -timeout=180s
