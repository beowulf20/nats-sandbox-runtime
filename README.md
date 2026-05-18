# NATS Sandbox Runtime

This repo packages a NATS microservice that runs Python code inside a Firecracker microVM. It is meant for agent and automation workloads that need a disposable sandbox, a stable NATS request interface, and workspace artifacts stored outside the VM in JetStream Object Store.

The main deployment target is:

```text
NATS request -> python.run -> worker pool -> Firecracker Python VM -> JetStream Object Store
```

The runtime can also serve the local Horizon web console on the same process with `runtime api`.

Python on Firecracker is the first supported runtime. The service shape is intentionally runtime-oriented rather than Python-only: the NATS request/control plane, worker pool, workspace artifact flow, and web console can be adapted for other sandbox runtimes later as new execution backends are needed.

## What It Provides

- `python.run`: executes inline Python or a Python object fetched from Object Store.
- Isolated Firecracker workers: each worker owns a VM snapshot cache and runs one request at a time.
- JetStream Object Store workspaces: input objects are copied into `/workspace`, and output files are uploaded as artifacts.
- Optional thread persistence: `thread_id` keeps a workspace image warm for one hour.
- Runtime controls: NATS subjects and HTTP API endpoints can resize workers and tune defaults for future runs.
- Base64 stdout/stderr metadata headers so the JSON response stays focused on status, metrics, and artifact references.

## Docker Image

The Dockerfile builds three things into one runtime image:

- the Go service binary;
- the built `web/` console;
- the Firecracker binary plus the checked-in kernel/rootfs assets needed by the Python guest.

Build it:

```bash
docker build -t nats-sandbox-runtime:local .
```

The image defaults to `runtime api`, listens on `0.0.0.0:8080`, and expects a NATS server named `nats` with JetStream enabled.

```bash
docker run --rm \
  --device=/dev/kvm \
  --privileged \
  -p 8080:8080 \
  -e NATS_URL=nats://host.docker.internal:4222 \
  -e NATS_BUCKET=python-runtime-workspaces \
  nats-sandbox-runtime:local
```

Firecracker needs host KVM access. `--privileged` is the broad, simple development option. For production, replace it with a tighter runtime policy that still exposes `/dev/kvm` and permits Firecracker KVM ioctls.

To run only the NATS microservice without the HTTP console:

```bash
docker run --rm \
  --device=/dev/kvm \
  --privileged \
  -e NATS_RUNTIME_MODE=python \
  -e NATS_URL=nats://host.docker.internal:4222 \
  nats-sandbox-runtime:local
```

You can also bypass the entrypoint defaults and pass the CLI directly:

```bash
docker run --rm --device=/dev/kvm --privileged nats-sandbox-runtime:local \
  runtime python \
  --url nats://host.docker.internal:4222 \
  --bucket python-runtime-workspaces \
  --workers 2
```

## Runtime Configuration

The container entrypoint maps environment variables to CLI flags. Extra arguments passed to `docker run` are appended last, so explicit flags can override the environment.

| Environment variable | Default | CLI flag | Purpose |
| --- | --- | --- | --- |
| `NATS_RUNTIME_MODE` | `api` | `runtime api` / `runtime python` | Starts the HTTP console plus runtime, or only the NATS runtime. |
| `RUNTIME_API_LISTEN` | `0.0.0.0:8080` | `--listen` | HTTP listen address for `runtime api`. |
| `RUNTIME_API_WEB_DIR` | `/opt/nats-sandbox-runtime/web/build` | `--web-dir` | Built frontend directory served by `runtime api`. |
| `NATS_URL` | `nats://nats:4222` | `--url` | NATS server URL. JetStream must be enabled. |
| `NATS_BUCKET` | `python-runtime-workspaces` | `--bucket` | Object Store bucket for input files and run artifacts. |
| `NATS_RUNTIME_WORKERS` | `1` | `--workers` | Initial worker count. Each worker can run one request at a time. |
| `NATS_RUNTIME_KERNEL` | packaged kernel | `--kernel` | Firecracker guest kernel path. |
| `NATS_RUNTIME_ROOTFS` | packaged rootfs | `--rootfs` | Firecracker guest root filesystem path. |
| `NATS_RUNTIME_FIRECRACKER` | `/usr/local/bin/firecracker` | `--firecracker` | Firecracker binary path. |
| `NATS_RUNTIME_MEMORY_MIB` | `128` | `--memory-mib` | Default guest memory per run. |
| `NATS_RUNTIME_SWAP_MIB` | `0` | `--swap-mib` | Default dedicated guest swap size. |
| `NATS_RUNTIME_WORKSPACE_MIB` | `16` | `--workspace-mib` | Default writable `/workspace` ext4 size. |
| `NATS_RUNTIME_VCPUS` | `1` | `--vcpus` | Guest vCPU count. |
| `NATS_RUNTIME_MAX_VCPUS` | `1` | `--max-vcpus` | Hard cap for requested vCPUs. |
| `NATS_RUNTIME_EXEC_TIMEOUT` | `5s` | `--exec-timeout` | Default Python execution timeout. |
| `NATS_RUNTIME_TRUNCATE_LOG_MIB` | `1` | `--truncate-log-mib` | Max stdout/stderr MiB returned in metadata headers. `0` disables truncation. |

Per-request fields such as `memory_mib`, `swap_mib`, `workspace_mib`, and `exec_timeout` override the startup defaults for that run.

## Scaling Warning

NATS load-balances requests across service instances that subscribe to the same `python.run` subject. The runtime does not currently share live workspace images between service instances.

That means `thread_id` workspace persistence is local to the service instance that handled the earlier request. If a later request with the same `thread_id` lands on a different service instance, that instance cannot reuse or exchange the first instance's workspace image. Run a single runtime service instance when thread workspace continuity matters, or treat multiple instances as independent sandbox pools until workspace exchange is implemented.

## NATS Request Examples

Run a simple inline script:

```bash
nats req python.run '{
  "code": "print(\"hello from firecracker\")"
}'
```

Create an artifact:

```bash
nats req python.run '{
  "run_id": "demo-artifact-1",
  "code": "from pathlib import Path\nPath(\"reports/result.txt\").write_text(\"ok\\n\")\nprint(\"done\")",
  "workspace_mib": 32,
  "exec_timeout": "10s"
}'
```

Use an Object Store input and keep a short-lived thread workspace:

```bash
nats object put --name datasets/input.txt python-runtime-workspaces ./input.txt

nats req python.run '{
  "thread_id": "conversation-42",
  "inputs": [
    {"object": "datasets/input.txt", "path": "input.txt"}
  ],
  "code": "from pathlib import Path\ntext = Path(\"input.txt\").read_text()\nPath(\"summary.txt\").write_text(text.upper())"
}'
```

Use code stored in Object Store:

```bash
nats object put --name code/job.py python-runtime-workspaces ./job.py

nats req python.run '{
  "code_object": "code/job.py",
  "memory_mib": 256,
  "exec_timeout": "30s"
}'
```

The JSON response includes the run ID, status, VM restore-to-exec latency, resource sizes, worker ID, and uploaded artifact keys:

```json
{
  "run_id": "demo-artifact-1",
  "status": "ok",
  "restore_exec_ms": 42,
  "guest_ram_mib": 128,
  "guest_swap_mib": 0,
  "workspace_mib": 32,
  "artifact_bucket": "python-runtime-workspaces",
  "artifacts": [
    {
      "path": "reports/result.txt",
      "object": "runs/demo-artifact-1/artifacts/reports/result.txt",
      "size": 3
    }
  ],
  "worker_id": "worker-1",
  "stdout_header": "Nats-Sandbox-Runtime-Python-Stdout-B64",
  "stderr_header": "Nats-Sandbox-Runtime-Python-Stderr-B64",
  "stdout_truncated": false,
  "stderr_truncated": false
}
```

Stdout and stderr are returned as base64 NATS headers:

- `Nats-Sandbox-Runtime-Python-Stdout-B64`
- `Nats-Sandbox-Runtime-Python-Stderr-B64`

## Go SDK V0

The `pkg/pyruntime` SDK wraps the byte-only V0 flow for Go callers. It uploads input bytes to the runtime Object Store bucket, calls `python.run`, downloads every returned workspace artifact into memory, and best-effort deletes only the temporary SDK input objects.

```go
package main

import (
	"context"
	"fmt"
	"log"

	"nats-sandbox-runtime/pkg/pyruntime"
)

func main() {
	ctx := context.Background()
	client, err := pyruntime.New(ctx, pyruntime.Config{
		URL:    "nats://localhost:4222",
		Bucket: "python-runtime-workspaces",
	})
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	result, err := client.Run(ctx, pyruntime.Request{
		ThreadID: "conversation-42",
		Code:     "from pathlib import Path\nPath('summary.txt').write_text(Path('input.txt').read_text().upper())",
		Files: map[string][]byte{
			"input.txt": []byte("hello\n"),
		},
	})
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println(result.Stdout)
	fmt.Printf("summary: %s\n", result.Files["summary.txt"])
}
```

SDK V0 requires `ThreadID` because persistence is the intended workspace model, but the same scaling caveat applies: workspace continuity is only guaranteed when later requests land on the same runtime service instance. Run a single runtime service instance when thread continuity matters, or treat multiple instances as independent sandbox pools until workspace exchange is implemented.

## Control Plane

Resize the worker pool while the service is running:

```bash
nats req python.control.workers.set '{"count":3}'
nats req python.control.workers.list '{}'
```

Runtime defaults can also be inspected and changed over NATS:

```bash
nats req python.control.settings.list '{}'
nats req python.control.settings.set '{"key":"runtime.default_memory_mib","value":256}'
nats req python.control.settings.get '{"key":"runtime.default_memory_mib"}'
nats req python.control.settings.delete '{"key":"runtime.default_memory_mib"}'
```

Known setting keys:

- `runtime.default_memory_mib`
- `runtime.default_swap_mib`
- `runtime.default_workspace_mib`
- `runtime.default_exec_timeout`

When `runtime api` is enabled, the same controls are exposed through the local HTTP API and the web console:

- `GET /api/overview`
- `GET /api/workers`
- `GET /api/workers/events`
- `PUT /api/workers`
- `GET /api/snapshots`
- `DELETE /api/snapshots/workers/{worker_id}`
- `GET /api/workspaces`
- `DELETE /api/workspaces/{key}`
- `GET /api/settings`
- `GET /api/settings/{key}`
- `PUT /api/settings/{key}`
- `DELETE /api/settings/{key}`

## Isolation Model

The service process runs on the host or in the container and talks to NATS. User Python does not run in that process. Each request is copied into a Firecracker guest and executed from `/workspace` as an unprivileged guest user.

Before user code runs, the guest root filesystem is remounted read-only. The writable area is a per-run or per-thread ext4 workspace image. After the run, files under `/workspace` are copied back out and uploaded to Object Store under:

```text
runs/<run_id>/artifacts/<workspace-path>
```

This design keeps the API simple: callers exchange JSON, Object Store keys, and NATS headers, while the runtime handles VM restore, workspace hydration, artifact upload, and cleanup.

## Local Development

Build the Go binary:

```bash
make build
```

Run the runtime API directly:

```bash
tmp/go/bin/go run ./cmd/nats-sandbox-runtime runtime api --bucket python-runtime-workspaces
```

Build the frontend first when serving the console without Docker:

```bash
cd web
npm install
npm run build
```

For reloadable Go and React development:

```bash
make dev
```

The dev target runs the Go runtime API on `127.0.0.1:8080` and the React dev server on `127.0.0.1:3000`.

## Local Firecracker Python Helper

For one-off local VM checks without NATS:

```bash
tmp/go/bin/go run ./cmd/nats-sandbox-runtime local python --exec 'print("hello from vm")'
```

Useful flags include:

- `--snapshot-dir`
- `--workspace-dir`
- `--memory-mib`
- `--swap-mib`
- `--workspace-mib`
- `--exec-timeout`
- `--runs`
- `--parallel-runs`
