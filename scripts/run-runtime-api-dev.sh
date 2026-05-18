set -eu

exec ./tmp/air/nats-sandbox-runtime runtime api \
  --listen "${RUNTIME_API_LISTEN:-127.0.0.1:8080}" \
  --bucket "${RUNTIME_API_BUCKET:-python-runtime-workspaces}"
