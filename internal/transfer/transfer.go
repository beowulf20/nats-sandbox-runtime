// Package transfer bounds Object Store I/O across all callers in this process.
package transfer

import (
	"context"
	"fmt"
	"path"
	"strings"
	"sync"
)

var slots = make(chan struct{}, 4)

// Parallel cancels pending work on the first failure and joins all started work.
func Parallel(ctx context.Context, count int, fn func(context.Context, int) error) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	var wg sync.WaitGroup
	var once sync.Once
	var first error
	var mu sync.Mutex
	next := 0
	for range min(count, 4) {
		wg.Go(func() {
			for {
				mu.Lock()
				i := next
				next++
				mu.Unlock()
				if i >= count || ctx.Err() != nil {
					return
				}
				select {
				case slots <- struct{}{}:
				case <-ctx.Done():
					return
				}
				err := ctx.Err()
				if err == nil {
					err = func() (err error) {
						defer func() {
							if p := recover(); p != nil {
								err = fmt.Errorf("transfer panic: %v", p)
							}
						}()
						return fn(ctx, i)
					}()
				}
				<-slots
				if err != nil {
					once.Do(func() { first = err; cancel() })
					return
				}
			}
		})
	}
	wg.Wait()
	if first != nil {
		return first
	}
	return ctx.Err()
}

func Prefixes(prefixes []string) ([]string, error) {
	result := make([]string, 0, len(prefixes))
	for _, prefix := range prefixes {
		p := strings.TrimSuffix(prefix, "/")
		if p == "" || p == "." || strings.ContainsAny(p, "\\\x00:") || path.IsAbs(p) || path.Clean(p) != p {
			return nil, fmt.Errorf("invalid artifact exclusion prefix %q", prefix)
		}
		for _, part := range strings.Split(p, "/") {
			if part == ".." {
				return nil, fmt.Errorf("invalid artifact exclusion prefix %q", prefix)
			}
		}
		result = append(result, p)
	}
	return result, nil
}

func Excluded(name string, prefixes []string) bool {
	for _, p := range prefixes {
		if name == p || strings.HasPrefix(name, p+"/") {
			return true
		}
	}
	return false
}
