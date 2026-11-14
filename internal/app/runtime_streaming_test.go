package app

import (
	"bytes"
	"context"
	"errors"
	"io"
	"nats-sandbox-runtime/internal/transfer"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/nats-io/nats.go/jetstream"
)

func TestRuntimeInputStreamingReplacesShorterFilesAndConfinesPaths(t *testing.T) {
	registration, _ := concurrencyRuntime(t, 1, nil)
	store := registration.runtime.store
	if _, err := store.PutBytes(t.Context(), "input", []byte("new")); err != nil {
		t.Fatal(err)
	}
	workspace := t.TempDir()
	target := filepath.Join(workspace, "file")
	if err := os.WriteFile(target, bytes.Repeat([]byte("old"), 100), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := fetchRuntimeObject(t.Context(), store, workspace, PythonRunObjectMapping{Path: "file", Object: "input"}, 1024); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(target)
	if err != nil || string(data) != "new" {
		t.Fatalf("data=%q err=%v, want exact replacement", data, err)
	}
	outside := t.TempDir()
	if err := os.Symlink(outside, filepath.Join(workspace, "escape")); err != nil {
		t.Fatal(err)
	}
	if err := fetchRuntimeObject(t.Context(), store, workspace, PythonRunObjectMapping{Path: "escape/file", Object: "input"}, 1024); err == nil {
		t.Fatal("input followed a symlink outside its workspace")
	}
	if _, err := os.Stat(filepath.Join(outside, "file")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("outside workspace modified: %v", err)
	}
	if err := fetchRuntimeObject(t.Context(), store, workspace, PythonRunObjectMapping{Path: "file", Object: "input"}, 2); err == nil {
		t.Fatal("input exceeding workspace size accepted")
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if err := fetchRuntimeObject(ctx, store, workspace, PythonRunObjectMapping{Path: "file", Object: "input"}, 1024); err == nil {
		t.Fatal("cancelled fetch succeeded")
	}
	data, _ = os.ReadFile(target)
	if string(data) != "new" {
		t.Fatal("failed fetch replaced a valid input")
	}
}

type inputStream struct {
	io.Reader
	size   uint64
	closed bool
}

func (s *inputStream) Close() error { s.closed = true; return nil }
func (s *inputStream) Error() error { return nil }
func (s *inputStream) Info() (*jetstream.ObjectInfo, error) {
	return &jetstream.ObjectInfo{Size: s.size}, nil
}

type inputStreamStore struct {
	jetstream.ObjectStore
	stream *inputStream
}

func (s inputStreamStore) Get(context.Context, string, ...jetstream.GetObjectOpt) (jetstream.ObjectResult, error) {
	return s.stream, nil
}

type inputReadFailure struct{}

func (inputReadFailure) Read([]byte) (int, error) { return 0, io.ErrUnexpectedEOF }

type inputReadPanic struct{}

func (inputReadPanic) Read([]byte) (int, error) { panic("input reader panic") }

func TestRuntimeInputStreamingClosesFileAfterPanic(t *testing.T) {
	workspace := t.TempDir()
	stream := &inputStream{Reader: inputReadPanic{}, size: 1}
	err := transfer.Parallel(t.Context(), 1, func(ctx context.Context, _ int) error {
		return fetchRuntimeObject(ctx, inputStreamStore{stream: stream}, workspace, PythonRunObjectMapping{Path: "input", Object: "input"}, 10)
	})
	if err == nil || !stream.closed {
		t.Fatalf("err=%v stream closed=%t", err, stream.closed)
	}
	entries, _ := os.ReadDir(workspace)
	if len(entries) != 0 {
		t.Fatal("panicked transfer left a temporary file")
	}
	fds, err := os.ReadDir("/proc/self/fd")
	if err != nil {
		t.Skip("open-file verification requires procfs")
	}
	for _, fd := range fds {
		target, _ := os.Readlink(filepath.Join("/proc/self/fd", fd.Name()))
		if strings.HasPrefix(target, workspace+string(os.PathSeparator)) {
			t.Fatalf("transfer leaked open descriptor: %s", target)
		}
	}
}

func TestRuntimeInputStreamingRejectsIncompleteOrOversizedBodiesAtomically(t *testing.T) {
	for _, failRead := range []bool{false, true} {
		t.Run(map[bool]string{false: "understated_size", true: "read_failure"}[failRead], func(t *testing.T) {
			workspace := t.TempDir()
			target := filepath.Join(workspace, "file")
			if err := os.WriteFile(target, []byte("retained"), 0o600); err != nil {
				t.Fatal(err)
			}
			var reader io.Reader = bytes.NewReader(bytes.Repeat([]byte("x"), 100))
			if failRead {
				reader = io.MultiReader(bytes.NewReader([]byte("partial")), inputReadFailure{})
			}
			stream := &inputStream{Reader: reader, size: 1}
			err := fetchRuntimeObject(t.Context(), inputStreamStore{stream: stream}, workspace, PythonRunObjectMapping{Path: "file", Object: "input"}, 10)
			if err == nil || !stream.closed {
				t.Fatalf("err=%v closed=%t", err, stream.closed)
			}
			data, _ := os.ReadFile(target)
			if string(data) != "retained" {
				t.Fatal("failed stream replaced valid input")
			}
			entries, _ := os.ReadDir(workspace)
			if len(entries) != 1 {
				t.Fatal("failed stream left a temporary input")
			}
		})
	}
}
