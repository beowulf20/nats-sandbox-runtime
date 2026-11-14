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
warmup requests with only the fixture script, and measures five batches of two concurrent requests on distinct
persistent threads. Defaults are 1 GiB runtime-container RAM plus 256 MiB host
swap allowance, 128 MiB guest RAM, 256 MiB guest swap, 64 MiB workspaces, and a
separate 512 MiB broker with no swap allowance. These limits apply only to the
benchmark containers. `--memory-mib 256` repeats a constrained scenario; use
identical limits for each compared image.

Each measured request uploads 64 data inputs of 1 KiB plus the fixture script,
except `io`, which uploads four data inputs of 4 MiB plus the script. Batch latency includes those transfers; guest compute and compile
timers isolate the interpreter work.

The fixture is checksum-verified and executed from `artifacts/benchmark.py`,
keeping the inline serial command short. This avoids conflating long-console-input
corruption with renderer or snapshot performance. The script is excluded from
returned artifacts with other request inputs.

Available workloads:

| Workload | Checks and measurements |
| --- | --- |
| `cpu` | Fixed integer-loop result, Python execution and repeated source-compilation time |
| `plot` | Matplotlib import and PNG creation, caller-selected Agg backend |
| `io` | Four 4 MiB inputs and an 8 MiB output per request, exact SHA-256 checks |
| `small` | 64 data inputs of 1 KiB plus the fixture script and small returned artifacts |
| `blas`, `blas1` | NumPy matrix multiplication, default pools versus explicit one-thread environment |
| `audit` | Exact interpreter/build/package versions, effective import-path bytecode validity and active NumPy OpenBLAS thread count |
| `backend` | Caller-selected SVG backend and workspace-local module imports |
| `state` | Fresh Python/NumPy/kernel random values, deterministic explicit seeds and no retained user globals/figures/rcParams |

Use `--import-profile numpy` or `--import-profile plot-agg` for a prewarmed
comparison, keeping generic execution as the baseline. `--charts 4` generates
four plots in one execution to measure import/restore amortization.
`--png-compression 1` trades larger lossless PNG files for faster encoding; the
default is 6. Plot results record hashes of decoded RGBA pixels so warm imports,
batching and compression can be compared without relying on PNG byte identity.

For an isolated Agg/mplcairo comparison, build the optional derivative and use
that same image for both renderers:

```bash
docker build -f Dockerfile.guest-mplcairo \
  --build-arg RUNTIME_IMAGE=nats-sandbox-runtime:local \
  -t nats-sandbox-mplcairo:local .
python3 scripts/python-bench/benchmark.py \
  --image nats-sandbox-mplcairo:local \
  --runtime tmp/python-bench/runtime --client tmp/python-bench/client \
  --workload plot --renderer cairo --import-profile plot-cairo \
  --plot-types line scatter dense --output tmp/python-bench/cairo-warm-1
```

Compare `--renderer agg` with omitted profile and `--import-profile plot-agg`,
and `--renderer cairo` with omitted profile and `--import-profile plot-cairo`.
The image adds pinned mplcairo and distro Cairo/pycairo without upgrading existing
Matplotlib/NumPy packages. Multiple plot types run as separate measured phases
in one fresh runtime, sharing its pristine profile cache. Rotate renderer/profile
and plot-type order between fresh trials. Phase files contain separate end-to-end
latency, figure setup, `Figure.draw` time and PNG/save remainder (including backend
conversion, encoding and file I/O). Cgroup peaks remain cumulative for the trial;
50 ms samples carry phase labels. No threads or resource limits are changed.

Fixtures use deterministic line (2,000 points), scatter (5,000 points) and dense
line (100,000 points) data with fixed labels and 640 by 480 pixel dimensions.
The client saves the first PNG per phase under `images/` and checks its dimensions.
Compare input data hashes and inspect images across renderers; exact pixel hashes
are useful within a renderer, while Agg/Cairo differences require visual checks.

Use `--plot-styles default fast` to pair ordinary and `plt.style.use("fast")`
requests within the same fresh runtime. Rotate style order between trials.
Only the explicit `fast` requests change simplification/chunking; no service
default is changed. The fixture asserts default path settings before each request
applies its style, detecting state leaked from an earlier fast request. It records
the actual threshold/chunk size, style setup time, input checksum time, package
metadata time and data preparation time. Fast style may change fine line detail
and marker rendering (observed in Cairo scatter output);
compare data hashes and inspect the separately saved PNGs before choosing it.
Timed plotting uses already-imported module version attributes rather than
importing `importlib.metadata` for package lookups on every request. The separate
`audit` workload retains complete distribution metadata outside plot comparisons.

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
