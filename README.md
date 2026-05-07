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
  "memory_mib": 128,
  "exec_timeout": "10s"
}'
```

The JSON response includes run metrics and uploaded artifact object keys. Stdout and stderr are returned as base64 response metadata headers:

- `Nats-Service-Tests-Python-Stdout-B64`
- `Nats-Service-Tests-Python-Stderr-B64`

The corresponding `*-Truncated` headers indicate whether the metadata was capped by `--truncate-log-mib`. Artifacts are uploaded under `runs/<run_id>/artifacts/<workspace-path>` in the configured bucket.
