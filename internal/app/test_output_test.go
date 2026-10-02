package app

import (
	"bytes"
	"sync"
)

// Readiness is written by a runner goroutine and polled by the test goroutine.
type lockedTestBuffer struct {
	mu     sync.Mutex
	buffer bytes.Buffer
}

func (b *lockedTestBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buffer.Write(p)
}
func (b *lockedTestBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buffer.String()
}
