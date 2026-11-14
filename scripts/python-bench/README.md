# Python guest benchmark

This benchmark runs a separate local NATS broker and two real Firecracker/KVM
workers, then removes its containers and network. It never reads deployment
credentials or service configuration. It requires Linux Docker with `/dev/kvm`,
a Unix Docker socket and a runtime image with the packaged guest assets.

From the repository root:

```bash
mkdir -p tmp/python-bench
CGO_ENABLED=0 go build -o tmp/python-bench/runtime ./cmd/nats-sandbox-runtime
go build -o tmp/python-bench/client ./scripts/python-bench
python3 scripts/python-bench/benchmark.py \
  --image nats-sandbox-runtime:local \
  --runtime tmp/python-bench/runtime --client tmp/python-bench/client \
  --workload cpu --output tmp/python-bench/baseline-cpu-1
```

Each invocation starts fresh containers and snapshot caches, runs two cold small
warmup requests without file inputs, and measures five batches of two concurrent requests on distinct
persistent threads. Defaults are 1 GiB runtime-container RAM plus 256 MiB host
swap allowance, 128 MiB guest RAM, 256 MiB guest swap, 64 MiB workspaces, and a
separate 512 MiB broker with no swap allowance. These limits apply only to the
benchmark containers. `--memory-mib 256` repeats a constrained scenario; use
identical limits for each compared image.

Each measured request uploads 65 inputs of 1 KiB, except `io`, which uploads four
inputs of 4 MiB. Batch latency includes those transfers; guest compute and compile
timers isolate the interpreter work.

Available workloads:

| Workload | Checks and measurements |
| --- | --- |
| `cpu` | Fixed integer-loop result, Python execution and repeated source-compilation time |
| `plot` | Matplotlib import and PNG creation, caller-selected Agg backend |
| `io` | Four 4 MiB inputs and an 8 MiB output per request, exact SHA-256 checks |
| `small` | 65 inputs of 1 KiB and small returned artifacts |
| `blas`, `blas1` | NumPy matrix multiplication, default pools versus explicit one-thread environment |
| `audit` | Exact interpreter/build/package versions, effective import-path bytecode validity and active NumPy OpenBLAS thread count |
| `backend` | Caller-selected SVG backend and workspace-local module imports |

All requests check read-only root, writable workspace, UID 65534, Python
assertions, thread identity and file exclusions. Input content changes on each
batch and each lane to detect stale workspace results. The `backend` check also
uses distinct module names to avoid timestamp-based bytecode ambiguity.

Outputs include client/request latency, throughput, allocation totals and peak
RSS; guest import/compute/compile time, executing-Python RSS/high-water mark and
VmSwap, plus VM-wide swap/fault counters;
50 ms Docker resource samples; runtime cgroup peaks, swap and OOM counters; VM-wide
memory context; and private runtime/broker logs. Peaks include cold preparation.
Snapshot sizes, allocated blocks and cache identities are recorded before
successful-run snapshots are removed to bound disk use. `--keep-snapshots` retains
them; failed-run snapshots are always retained for diagnosis.
Client allocation totals include fixture construction and validation, not just
SDK transfers. Failed assertions or requests fail the run and retain evidence.

For a comparison, run at least five fresh invocations per image/workload,
counterbalance image order, keep dependency versions and resources fixed, and
record image IDs and interpreter build flags. Wait for compiler builds to finish.
Host page caches are not flushed. Compare trial medians and paired variability,
and distinguish local KVM results from deployment-host acceptance. Files under
`tmp/`, including raw guest snapshots and benchmark reports, must stay out of commits.
