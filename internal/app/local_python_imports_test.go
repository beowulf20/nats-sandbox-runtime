package app

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestImportProfileCacheIsolation(t *testing.T) {
	cfg := defaultLocalPythonConfig()
	cfg.SnapshotDir = t.TempDir()
	cfg.WorkspaceImagePath = filepath.Join(t.TempDir(), "thread.ext4")
	seen := map[string]bool{}
	for _, profile := range []string{"", "numpy", "plot-agg", "plot-cairo"} {
		cfg.ImportProfile = profile
		if err := validateLocalPythonConfig(cfg); err != nil {
			t.Fatal(err)
		}
		paths := localPythonSnapshotPathsForConfig(cfg)
		if seen[paths.MemoryPath] {
			t.Fatalf("shared snapshot: %s", paths.MemoryPath)
		}
		seen[paths.MemoryPath] = true
		if paths.WorkspacePath != cfg.WorkspaceImagePath {
			t.Fatal("lost thread workspace")
		}
		// Identity must reject a different profile even with identical assets.
		other := cfg
		other.ImportProfile = "numpy"
		if profile == "numpy" {
			other.ImportProfile = "plot-agg"
		}
		if localPythonSnapshotVersion(cfg) == localPythonSnapshotVersion(other) {
			t.Fatal("profile missing from identity")
		}
	}
	cfg.ImportProfile = "../escape"
	if validateLocalPythonConfig(cfg) == nil {
		t.Fatal("accepted unsafe profile")
	}
}

func TestWarmRestoreUsesFreshHostEntropyAndGenericStaysUnchanged(t *testing.T) {
	if localPythonWarmRestore("") != "" || localPythonWarmImports("") != "" {
		t.Fatal("generic imports changed")
	}
	a, b := localPythonWarmRestore("numpy"), localPythonWarmRestore("numpy")
	if a == b {
		t.Fatal("repeated host entropy")
	}
	input := localPythonExecInputForProfile("pass", "job.py", true, "numpy")
	reseed, drop := strings.Index(input, "fcntl.ioctl"), strings.Index(input, "os.setuid")
	if reseed < 0 || drop < reseed {
		t.Fatal("kernel reseed requires root")
	}
}

func TestWarmSwapWritesCannotChangeBaseline(t *testing.T) {
	base := filepath.Join(t.TempDir(), "swap.raw")
	if err := os.WriteFile(base, []byte("baseline swapped pages"), 0600); err != nil {
		t.Fatal(err)
	}
	for _, profile := range []string{"numpy", "plot-agg", "plot-cairo"} {
		cfg := defaultLocalPythonConfig()
		cfg.ImportProfile = profile
		cfg.SwapMiB = 1
		path, err := localPythonRestoreSwap(t.Context(), cfg, localPythonSnapshotFiles{SwapPath: base}, t.TempDir())
		if err != nil {
			t.Fatal(err)
		}
		if path == base {
			t.Fatal("restored request writes baseline")
		}
		if err := os.WriteFile(path, []byte("request private pages"), 0600); err != nil {
			t.Fatal(err)
		}
		got, err := os.ReadFile(base)
		if err != nil || string(got) != "baseline swapped pages" {
			t.Fatalf("changed baseline: %q %v", got, err)
		}
	}
}
