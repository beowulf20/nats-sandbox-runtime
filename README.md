# NATS Service Registration Test

Small Go service for testing how NATS handles multiple registrations of the same service when one instance disappears.

Each configured instance uses its own NATS connection and registers the same service:

- NATS URL: `nats://localhost:4222`
- Service name: `timestamp`
- Endpoint subject: `time.now`
- Response: `{"timestamp":"<RFC3339Nano UTC timestamp>"}`

## Run

Start NATS locally on `localhost:4222`, then run:

```bash
tmp/go/bin/go run ./cmd/nats-service-tests --instances 3
```

Short flag:

```bash
tmp/go/bin/go run ./cmd/nats-service-tests -i 3
```

## Request Timestamp

With the service running, request the timestamp endpoint:

```bash
nats request time.now ''
```

Short form:

```bash
nats req time.now ''
```

Example response:

```json
{"timestamp":"2026-05-06T16:34:56.000000789Z"}
```

Inspect service registrations:

```bash
nats micro ls
nats micro info timestamp
```

Stop the process with Ctrl-C.

## Local Firecracker Python

Download a repo-local Firecracker binary first:

```bash
make firecracker
```

Start a local Firecracker microVM with Python attached to the serial console:

```bash
tmp/go/bin/go run ./cmd/nats-service-tests local python
```

The first run creates a reusable snapshot cache in `firecracker-assets/python-snapshot`; later runs restore it for faster startup. Use `--snapshot-dir` to choose another cache location.
Before each run, `tmp/workspace` is copied into a VM data drive and mounted read-write at `/workspace` before user code starts. Exec scripts run as an unprivileged guest user after the guest root filesystem has been remounted read-only, so scripts can write workspace files but cannot change system files, remount devices, or configure swap. Files written under `/workspace` are copied back to `tmp/workspace` after a single exec run. Use `--workspace-dir` to choose another directory.
VM resources default to `128 MiB` RAM, a `16 MiB` workspace filesystem, no swap, and `1` vCPU. Use `--memory-mib` to choose any positive memory size, `--workspace-mib` to set the writable `/workspace` filesystem size, and `--swap-mib` to attach a dedicated `swap.raw` image as guest swap. CPU is capped at `1` vCPU unless you raise `--max-vcpus`; requests above the CPU cap are rejected before the VM starts.

Run an inline Python command instead of the REPL:

```bash
tmp/go/bin/go run ./cmd/nats-service-tests local python --exec 'print("hello from vm")'
```

Run a Python script file instead:

```bash
tmp/go/bin/go run ./cmd/nats-service-tests local python --exec-file tmp/test-file.py
```

Hide Firecracker process logs:

```bash
tmp/go/bin/go run ./cmd/nats-service-tests local python --hide-firecracker-log --exec 'print("hello from vm")'
```

Benchmark snapshot restore to Python exec:

```bash
tmp/go/bin/go run ./cmd/nats-service-tests local python --exec 'print("bench")' --runs 30
```

Use `--parallel-runs` to run at most that many benchmark VMs at once:

```bash
tmp/go/bin/go run ./cmd/nats-service-tests local python --exec 'print("bench")' --runs 30 --parallel-runs 4
```

Benchmark output includes restore-to-exec latency, configured guest RAM/swap, host Firecracker process max RSS, average CPU percent over each run, and CPU time. Use `--exec-timeout` for benchmark scripts that take longer than the default `5s`. Parallel benchmark runs restore the same snapshot while using per-run copies of the workspace and swap images.

The command uses `bin/firecracker`, `firecracker-assets/vmlinux.bin`, and `firecracker-assets/rootfs.ext4` by default. The guest logs `vm startup_ms=<ms>` when Python starts and `vm runtime_ms=<ms>` after Python exits while creating the snapshot. Use `--firecracker` to point at a different Firecracker binary.

## NATS Python Runtime

Run a NATS microservice that executes Python in a Firecracker VM and stores workspace artifacts in JetStream Object Store:

```bash
tmp/go/bin/go run ./cmd/nats-service-tests runtime python --bucket python-runtime-workspaces
```

The service listens on `python.run`. Request JSON can include inline code, Object Store-backed input files, and per-run resource overrides:

```bash
nats req python.run '{
  "code": "from pathlib import Path\nPath(\"charts/status_counts.png\").write_text(\"ok\")\nprint(\"done\")",
  "inputs": [
    {"object": "datasets/iot-device-timestamps.json", "path": "iot-device-timestamps.json"}
  ],
  "workspace_mib": 32,
  "thread_id": "conversation-42",
  "memory_mib": 128,
  "exec_timeout": "10s"
}'
```

The JSON response includes run metrics and uploaded artifact object keys. Stdout and stderr are returned as base64 response metadata headers:

- `Nats-Service-Tests-Python-Stdout-B64`
- `Nats-Service-Tests-Python-Stderr-B64`

The corresponding `*-Truncated` headers indicate whether the metadata was capped by `--truncate-log-mib`. Artifacts are uploaded under `runs/<run_id>/artifacts/<workspace-path>` in the configured bucket. Runtime workspace ext4 images persist for one hour only when `thread_id` is supplied; runs without `thread_id` use an ephemeral workspace image that is removed after the run.

Runtime execution is handled by a worker pool. At startup, `--workers` sets the initial desired worker count, and each worker owns an isolated snapshot directory under `<snapshot-dir>/workers/<worker-id>`. Requests are assigned to the first idle worker. The desired worker count can be changed while the service is running:

```bash
nats req python.control.workers.set '{"count":3}'
nats req python.control.workers.list '{}'
```

Increasing the count creates workers immediately. Decreasing it removes idle workers first and lets busy excess workers finish their current run before disappearing. Runtime defaults still apply to each worker, and per-run `python.run` resource fields have final precedence.

## Local Runtime API Console

Run the Python runtime and a local Horizon UI console from one process:

```bash
tmp/go/bin/go run ./cmd/nats-service-tests runtime api --bucket python-runtime-workspaces
```

The API listens on `127.0.0.1:8080` by default and serves `web/build`. Build the frontend first:

```bash
cd web
npm install
npm run build
```

Open `http://127.0.0.1:8080` for the console. The lateral navigation includes:

- `Overview`: NATS connection and runtime service status
- `Workers`: current worker pool status and desired worker count
- `Snapshots`: VM snapshot file status and per-worker reset
- `Workspaces`: one-hour persistent workspace ext4 image status by thread ID
- `Settings`: discoverable runtime defaults that affect future Python runs

For local development with reloadable server and UI processes, run:

```bash
make dev
```

The dev target runs Air for the Go runtime API on `127.0.0.1:8080` and the React dev server on `127.0.0.1:3000`. The UI dev server proxies `/api/*` requests to the runtime API. Override ports as needed:

```bash
make dev RUNTIME_API_LISTEN=127.0.0.1:8081 WEB_PORT=3001
```

The local JSON endpoints are:

- `GET /api/overview`
- `GET /api/workers`
- `GET /api/workers/events` for Server-Sent Events with live worker snapshots
- `PUT /api/workers` with `{"count": <integer>}`
- `GET /api/snapshots`
- `DELETE /api/snapshots/workers/{worker_id}` to reset VM snapshot files for an idle worker
- `GET /api/workspaces`
- `DELETE /api/workspaces/{key}` to reset an idle thread workspace ext4 image
- `GET /api/settings` for discoverable effective runtime settings
- `GET /api/settings/{key}`
- `PUT /api/settings/{key}` with `{"value": <json>}`
- `DELETE /api/settings/{key}` to reset a known setting to the startup default

Known settings are:

- `runtime.default_memory_mib`
- `runtime.default_swap_mib`
- `runtime.default_workspace_mib`
- `runtime.default_exec_timeout`
