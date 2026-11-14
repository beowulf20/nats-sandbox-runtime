package app

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"path/filepath"
	"strconv"

	"nats-sandbox-runtime/internal/pythonprofile"
)

const localPythonImportProfileVersion = "v2"

func localPythonRestoreSwap(ctx context.Context, cfg LocalPythonConfig, paths localPythonSnapshotFiles, workDir string) (string, error) {
	if cfg.ImportProfile == "" || cfg.SwapMiB == 0 {
		return paths.SwapPath, nil
	}
	path := filepath.Join(workDir, "swap.raw")
	return path, copyLocalPythonImageFile(ctx, paths.SwapPath, path)
}

// Only trusted, immutable image modules enter these snapshots. The workspace
// stays unmounted, and no user code or request configuration has run yet.
func localPythonWarmImports(profile string) string {
	if profile == "" {
		return ""
	}
	input := "import random, fcntl, struct\nimport numpy as __nats_numpy\nimport numpy.random\n"
	if profile == pythonprofile.PlotAgg {
		input += "subprocess.run(['/usr/bin/mount', '-t', 'tmpfs', '-o', 'size=16m,mode=1777', 'tmpfs', '/tmp'], check=True)\n" +
			"os.makedirs('/tmp/nats-plot-cache', mode=0o755, exist_ok=True)\n" +
			"os.environ['MPLCONFIGDIR'] = '/tmp/nats-plot-cache'\n" +
			"os.environ['MPLBACKEND'] = 'Agg'\n" +
			"import matplotlib.pyplot as __nats_pyplot\n" +
			"__nats_figure = __nats_pyplot.figure(figsize=(1, 1))\n" +
			"__nats_figure.text(0.1, 0.1, '0123456789')\n" +
			"__nats_figure.canvas.draw()\n" +
			"__nats_pyplot.close('all')\ndel __nats_figure\n" +
			"subprocess.run(['/usr/bin/chown', '-R', '65534:65534', '/tmp/nats-plot-cache'], check=True)\n"
	}
	return input
}

// Guest kernel entropy and preloaded PRNGs must not repeat after a restore.
// Host entropy is mixed and credited only inside this private guest, while root
// privileges are still available. Explicit user seeds continue to work normally.
func localPythonWarmRestore(profile string) string {
	if profile == "" {
		return ""
	}
	seed := make([]byte, 32)
	if _, err := rand.Read(seed); err != nil {
		panic("host entropy unavailable")
	}
	return "__nats_seed = bytes.fromhex(" + strconv.Quote(hex.EncodeToString(seed)) + ")\n" +
		"with open('/dev/random', 'wb', buffering=0) as __nats_rng:\n" +
		"    fcntl.ioctl(__nats_rng, 0x40085203, struct.pack('ii', 256, len(__nats_seed)) + __nats_seed)\n" +
		"    fcntl.ioctl(__nats_rng, 0x5207)\n" +
		"random.seed(__nats_seed)\n__nats_numpy.random.seed(list(__nats_seed))\ndel __nats_seed\n"
}
