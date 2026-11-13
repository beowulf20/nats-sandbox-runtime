package transfer

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
)

func TestProcessLimitAndJoin(t *testing.T) {
	var active, peak atomic.Int64
	started := make(chan struct{}, 4)
	release := make(chan struct{})
	var wg sync.WaitGroup
	for range 3 {
		wg.Go(func() {
			err := Parallel(t.Context(), 12, func(ctx context.Context, _ int) error {
				n := active.Add(1)
				defer active.Add(-1)
				for old := peak.Load(); n > old && !peak.CompareAndSwap(old, n); old = peak.Load() {
				}
				select {
				case started <- struct{}{}:
				default:
				}
				<-release
				return nil
			})
			if err != nil {
				t.Error(err)
			}
		})
	}
	for range 4 {
		<-started
	}
	close(release)
	wg.Wait()
	if peak.Load() != 4 || active.Load() != 0 {
		t.Fatalf("peak=%d active=%d", peak.Load(), active.Load())
	}

	cause := errors.New("first failure")
	err := Parallel(t.Context(), 100, func(ctx context.Context, i int) error {
		active.Add(1)
		defer active.Add(-1)
		if i == 0 {
			return cause
		}
		<-ctx.Done()
		return ctx.Err()
	})
	if !errors.Is(err, cause) || active.Load() != 0 {
		t.Fatalf("err=%v active=%d", err, active.Load())
	}
	err = Parallel(t.Context(), 8, func(context.Context, int) error { panic("private transfer failure") })
	if err == nil || len(slots) != 0 {
		t.Fatalf("panic leaked transfer slots: %v", err)
	}
}

func TestPrefixes(t *testing.T) {
	for _, bad := range []string{"", "/artifacts", "../secret", "a/../b", ".", "a//b", `a\b`, "a\x00b", "C:/tmp"} {
		if _, err := Prefixes([]string{bad}); err == nil {
			t.Errorf("accepted %q", bad)
		}
	}
	prefixes, err := Prefixes([]string{"artifacts/", "nested/input"})
	if err != nil {
		t.Fatal(err)
	}
	for p, want := range map[string]bool{"artifacts": true, "artifacts/in.json": true, "artifacts-other/a": false, "nested/input/a": true, "nested/inputs/a": false} {
		if Excluded(p, prefixes) != want {
			t.Errorf("excluded %q", p)
		}
	}
}
