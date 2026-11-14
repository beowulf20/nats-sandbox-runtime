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

The pristine snapshot compiles the trusted execution wrapper once. Each restore
sends request arguments and fresh entropy for warm import profiles; user code
still compiles after workspace setup and privilege dropping. A snapshot version
change rebuilds older caches. Cold warmup includes this preparation, so compare
it separately from measured warm requests.
Warm import profiles also collect trusted warmup garbage before snapshotting;
user executions retain normal garbage collection and thresholds.

`processing_ms` starts after the benchmark file loader, source compilation and
initial assertions. It also ends before metrics serialization and writing.
Subtracting it from server duration includes this benchmark work as well as
runtime lifecycle costs; use phase measurements to distinguish them.
Request results include `run_id` to correlate with internal `runtime.run.completed`
logs without exposing timing probes in service responses.

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

## Optional Linux 6.11 / uringio comparison

`--kernel /absolute/path/to/vmlinux` bind-mounts an experimental guest kernel
inside only the benchmark container. The host kernel, packaged image kernel and
deployment configuration stay unchanged. The runner records the kernel SHA-256;
each invocation gets fresh snapshots. Boot and restore the kernel successfully
before comparing performance. On the tested AMD KVM host, upstream Linux 6.11.11
needed `CONFIG_PVH=n`, `CONFIG_VIRTIO_MMIO_CMDLINE_DEVICES=y`, `CONFIG_PCI=y` and
`CONFIG_X86_MPPARSE=y` on top of Firecracker v1.14.0's x86-64 6.1 guest config.
Snapshot restore additionally needed `noxsaves`: without it, the guest panicked in
FPU state restoration during `blockdev` execution. This is a local compatibility
finding, not a universal requirement or an upgrade to the deployment kernel.

To build that optional kernel, place the [6.11.11 source tarball](https://www.kernel.org/pub/linux/kernel/v6.x/linux-6.11.11.tar.xz)
and [Firecracker v1.14.0 baseline config](https://raw.githubusercontent.com/firecracker-microvm/firecracker/v1.14.0/resources/guest_configs/microvm-kernel-ci-x86_64-6.1.config)
under `tmp/kernel-context/`, naming the config `firecracker-6.1.config`. The recipe
checks both SHA-256 hashes before use. It builds a separate artifact, leaving the
image and deployment kernel intact:

```bash
docker build -f scripts/python-bench/Dockerfile.kernel-6.11 \
  --target artifacts --output type=local,dest=tmp/linux-6.11.11 \
  tmp/kernel-context
```

The optional `Dockerfile.guest-uringio` adds only a locally supplied, hash-checked
wheel to an existing CPython 3.12 x86-64 rootfs. Prepare a build context under
`tmp/` containing `wheels/uringio-0.4.4-cp312-cp312-manylinux2014_x86_64.manylinux_2_17_x86_64.manylinux_2_28_x86_64.whl`
and a `wheels/SHA256SUMS` file with this exact line:

```text
571ef4565ed08bfc6a098d42e70b44366a2b7cc61dda8b39b9ab4a9257e9e105  uringio-0.4.4-cp312-cp312-manylinux2014_x86_64.manylinux_2_17_x86_64.manylinux_2_28_x86_64.whl
```

Download that wheel from the [PyPI 0.4.4 release](https://pypi.org/project/uringio/0.4.4/#files),
then build and compare with the same derivative image for every arm:

```bash
docker build -f Dockerfile.guest-uringio \
  --build-arg RUNTIME_IMAGE=nats-sandbox-runtime:local \
  -t nats-sandbox-uringio:local tmp/uringio-context
python3 scripts/python-bench/benchmark.py \
  --image nats-sandbox-uringio:local \
  --runtime tmp/python-bench/runtime --client tmp/python-bench/client \
  --kernel tmp/linux-6.11.11/vmlinux-6.11.11.bin --io-backend uringio \
  --import-profile plot-agg --workload plot \
  --output tmp/python-bench/uringio-plot-1
```

Compare the packaged kernel with `--io-backend sync`, then the experimental
kernel with `sync`, `threads` and `uringio`. Run five fresh trials per workload
and rotate variant order. `threads` uses a four-thread executor; `uringio` uses
at most four concurrent file reads. All backends stream the same 64 KiB chunks,
handle short writes, verify exact checksums and preserve manifest order. Reads
are bounded by the known input file size. Read-only `uringio.open_file` calls
explicitly pass `mode=0`, as required by `openat2` without creation flags.

Plots encode to `BytesIO` for every backend, then use the selected file writer.
Compare these variants with each other; this changed fixture is not an exact
absolute-latency comparison with earlier direct-to-file plot measurements.
Setup includes backend imports and event-loop creation after snapshot restore;
the `plot-agg` snapshot warms Matplotlib/NumPy, not asyncio/uringio. Closing the
loop is timed too. The service's Go filesystem/NATS transfers are unaffected.

Results include setup, checksum-read, output-write and close times, kernel
release, guest-wide swap usage and descriptor retention after loop shutdown.
`io_fd_delta` is a diagnostic, not a passing cleanup assertion: stock uringio
0.4.4 retained two signal-pipe descriptors per loop in the local probe, growing
by ten across five create/close cycles even after GC. These experimental
measurements do not qualify the library for adoption. A separate first-error
probe also retained one to four regular-file descriptors after cancelling
concurrent reads and closing the loop. The candidate's cancellation path needs
ownership/cleanup work before reuse; successful-path timings do not validate it.
EOF, missing-file errors and cancellation of a pending FIFO read were observed,
but the final descriptor-cleanup assertion failed and that failure is retained.
Each service job still
destroys its separate VM/process after responding. Unit checks for the stdlib
backends require zero retained descriptors:

```bash
python3 -m unittest discover -s scripts/python-bench
```
