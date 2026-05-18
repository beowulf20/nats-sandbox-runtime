package pyruntime

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/nats-io/nats.go/micro"
)

func TestRunValidatesRequiredFieldsAndInputPaths(t *testing.T) {
	client := newTestClient(Config{})

	for _, test := range []struct {
		name string
		req  Request
	}{
		{name: "missing thread id", req: Request{Code: "print(1)"}},
		{name: "missing code", req: Request{ThreadID: "thread-a"}},
		{name: "blank code", req: Request{ThreadID: "thread-a", Code: " \n\t "}},
		{name: "empty path", req: Request{ThreadID: "thread-a", Code: "print(1)", Files: map[string][]byte{"": []byte("x")}}},
		{name: "parent escape", req: Request{ThreadID: "thread-a", Code: "print(1)", Files: map[string][]byte{"../secret": []byte("x")}}},
		{name: "absolute path", req: Request{ThreadID: "thread-a", Code: "print(1)", Files: map[string][]byte{"/secret": []byte("x")}}},
		{name: "dot path", req: Request{ThreadID: "thread-a", Code: "print(1)", Files: map[string][]byte{".": []byte("x")}}},
		{name: "backslash path", req: Request{ThreadID: "thread-a", Code: "print(1)", Files: map[string][]byte{`data\input.txt`: []byte("x")}}},
	} {
		t.Run(test.name, func(t *testing.T) {
			_, err := client.Run(t.Context(), test.req)
			if err == nil {
				t.Fatal("Run returned nil, want validation error")
			}
		})
	}
}

func TestRunEnforcesInputSizeLimits(t *testing.T) {
	t.Run("per file", func(t *testing.T) {
		client := newTestClient(Config{MaxInputFileBytes: 2})
		_, err := client.Run(t.Context(), Request{
			ThreadID: "thread-a",
			Code:     "print(1)",
			Files:    map[string][]byte{"input.txt": []byte("abc")},
		})
		var limitErr SizeLimitError
		if !errors.As(err, &limitErr) {
			t.Fatalf("Run error = %v, want SizeLimitError", err)
		}
		if limitErr.Kind != "input_file" || limitErr.Path != "input.txt" || limitErr.Size != 3 || limitErr.Limit != 2 {
			t.Fatalf("SizeLimitError = %#v, want input_file input.txt 3/2", limitErr)
		}
	})

	t.Run("total", func(t *testing.T) {
		client := newTestClient(Config{MaxInputFileBytes: 10, MaxInputTotalBytes: 4})
		_, err := client.Run(t.Context(), Request{
			ThreadID: "thread-a",
			Code:     "print(1)",
			Files: map[string][]byte{
				"a.txt": []byte("abc"),
				"b.txt": []byte("de"),
			},
		})
		var limitErr SizeLimitError
		if !errors.As(err, &limitErr) {
			t.Fatalf("Run error = %v, want SizeLimitError", err)
		}
		if limitErr.Kind != "input_total" || limitErr.Size != 5 || limitErr.Limit != 4 {
			t.Fatalf("SizeLimitError = %#v, want input_total 5/4", limitErr)
		}
	})
}

func TestRunUploadsInputsAndSendsRuntimeWireRequest(t *testing.T) {
	store := newFakeStore()
	requester := &fakeRequester{
		handle: func(subject string, data []byte) *nats.Msg {
			var req pythonRunRequest
			if err := json.Unmarshal(data, &req); err != nil {
				t.Fatalf("unmarshal request returned error: %v", err)
			}
			if subject != runSubject {
				t.Fatalf("subject = %q, want %q", subject, runSubject)
			}
			if req.RunID == "" {
				t.Fatal("run_id is empty")
			}
			if req.ThreadID != "thread-a" || req.Code != "print('hi')" {
				t.Fatalf("request = %#v, want thread/code", req)
			}
			wantInputs := []pythonRunObjectMapping{
				{Object: "sdk-inputs/" + req.RunID + "/a.txt", Path: "a.txt"},
				{Object: "sdk-inputs/" + req.RunID + "/nested/b.txt", Path: "nested/b.txt"},
			}
			if !reflect.DeepEqual(req.Inputs, wantInputs) {
				t.Fatalf("inputs = %#v, want %#v", req.Inputs, wantInputs)
			}
			return runtimeResponse(req.RunID, nil)
		},
	}
	client := &Client{req: requester, store: store, cfg: withDefaults(Config{})}

	result, err := client.Run(t.Context(), Request{
		ThreadID: "thread-a",
		Code:     "print('hi')",
		Files: map[string][]byte{
			"nested/b.txt": []byte("b"),
			"a.txt":        []byte("a"),
		},
	})
	if err != nil {
		t.Fatalf("Run returned error: %v", err)
	}
	if result.RunID == "" {
		t.Fatal("result run id is empty")
	}
	if len(store.puts) != 2 {
		t.Fatalf("puts = %#v, want two uploads", store.puts)
	}
	if !strings.HasSuffix(store.puts[0], "/a.txt") || !strings.HasSuffix(store.puts[1], "/nested/b.txt") {
		t.Fatalf("puts = %#v, want deterministic sorted input uploads", store.puts)
	}
	if !reflect.DeepEqual(store.deleted, store.puts) {
		t.Fatalf("deleted = %#v, want cleanup of uploaded objects %#v", store.deleted, store.puts)
	}
}

func TestRunDecodesLogsAndDownloadsArtifacts(t *testing.T) {
	store := newFakeStore()
	store.objects["runs/run-a/artifacts/input.txt"] = []byte("original input")
	store.objects["runs/run-a/artifacts/out.txt"] = []byte("output")
	requester := &fakeRequester{
		response: runtimeResponse("run-a", []pythonRunArtifact{
			{Path: "input.txt", Object: "runs/run-a/artifacts/input.txt", Size: uint64(len("original input"))},
			{Path: "out.txt", Object: "runs/run-a/artifacts/out.txt", Size: uint64(len("output"))},
		}),
	}
	requester.response.Header.Set(stdoutHeader, base64.StdEncoding.EncodeToString([]byte("hello\n")))
	requester.response.Header.Set(stderrHeader, base64.StdEncoding.EncodeToString([]byte("warn\n")))
	client := &Client{req: requester, store: store, cfg: withDefaults(Config{})}

	result, err := client.Run(t.Context(), Request{ThreadID: "thread-a", Code: "print(1)"})
	if err != nil {
		t.Fatalf("Run returned error: %v", err)
	}
	if result.RunID != "run-a" || result.Stdout != "hello\n" || result.Stderr != "warn\n" {
		t.Fatalf("result logs = %#v, want decoded headers", result)
	}
	wantFiles := map[string][]byte{
		"input.txt": []byte("original input"),
		"out.txt":   []byte("output"),
	}
	if !reflect.DeepEqual(result.Files, wantFiles) {
		t.Fatalf("files = %#v, want all workspace artifacts %#v", result.Files, wantFiles)
	}
}

func TestRunReturnsMicroErrorsAndCleansUploadedInputs(t *testing.T) {
	store := newFakeStore()
	msg := &nats.Msg{Header: nats.Header{}}
	msg.Header.Set(micro.ErrorCodeHeader, "500")
	msg.Header.Set(micro.ErrorHeader, "boom")
	client := &Client{
		req:   &fakeRequester{response: msg},
		store: store,
		cfg:   withDefaults(Config{}),
	}

	_, err := client.Run(t.Context(), Request{
		ThreadID: "thread-a",
		Code:     "print(1)",
		Files:    map[string][]byte{"input.txt": []byte("payload")},
	})
	if err == nil || !strings.Contains(err.Error(), "python runtime error 500: boom") {
		t.Fatalf("Run error = %v, want runtime error", err)
	}
	if len(store.puts) != 1 || len(store.deleted) != 1 || store.deleted[0] != store.puts[0] {
		t.Fatalf("puts/deleted = %#v/%#v, want cleanup after runtime error", store.puts, store.deleted)
	}
}

func TestRunEnforcesOutputSizeLimitsBeforeDownloading(t *testing.T) {
	t.Run("per file", func(t *testing.T) {
		store := newFakeStore()
		client := &Client{
			req: &fakeRequester{response: runtimeResponse("run-a", []pythonRunArtifact{
				{Path: "big.txt", Object: "runs/run-a/artifacts/big.txt", Size: 3},
			})},
			store: store,
			cfg:   withDefaults(Config{MaxOutputFileBytes: 2}),
		}

		_, err := client.Run(t.Context(), Request{ThreadID: "thread-a", Code: "print(1)"})
		var limitErr SizeLimitError
		if !errors.As(err, &limitErr) {
			t.Fatalf("Run error = %v, want SizeLimitError", err)
		}
		if limitErr.Kind != "output_file" || limitErr.Path != "big.txt" || limitErr.Size != 3 || limitErr.Limit != 2 {
			t.Fatalf("SizeLimitError = %#v, want output_file big.txt 3/2", limitErr)
		}
		if len(store.gets) != 0 {
			t.Fatalf("gets = %#v, want no downloads after advertised output limit", store.gets)
		}
	})

	t.Run("total", func(t *testing.T) {
		store := newFakeStore()
		client := &Client{
			req: &fakeRequester{response: runtimeResponse("run-a", []pythonRunArtifact{
				{Path: "a.txt", Object: "runs/run-a/artifacts/a.txt", Size: 2},
				{Path: "b.txt", Object: "runs/run-a/artifacts/b.txt", Size: 3},
			})},
			store: store,
			cfg:   withDefaults(Config{MaxOutputFileBytes: 10, MaxOutputTotalBytes: 4}),
		}

		_, err := client.Run(t.Context(), Request{ThreadID: "thread-a", Code: "print(1)"})
		var limitErr SizeLimitError
		if !errors.As(err, &limitErr) {
			t.Fatalf("Run error = %v, want SizeLimitError", err)
		}
		if limitErr.Kind != "output_total" || limitErr.Size != 5 || limitErr.Limit != 4 {
			t.Fatalf("SizeLimitError = %#v, want output_total 5/4", limitErr)
		}
		if len(store.gets) != 0 {
			t.Fatalf("gets = %#v, want no downloads after advertised output limit", store.gets)
		}
	})
}

func TestRunEnforcesOutputLimitOnDownloadedBytes(t *testing.T) {
	store := newFakeStore()
	store.objects["runs/run-a/artifacts/big.txt"] = []byte("abcd")
	client := &Client{
		req: &fakeRequester{response: runtimeResponse("run-a", []pythonRunArtifact{
			{Path: "big.txt", Object: "runs/run-a/artifacts/big.txt", Size: 2},
		})},
		store: store,
		cfg:   withDefaults(Config{MaxOutputFileBytes: 3}),
	}

	_, err := client.Run(t.Context(), Request{ThreadID: "thread-a", Code: "print(1)"})
	var limitErr SizeLimitError
	if !errors.As(err, &limitErr) {
		t.Fatalf("Run error = %v, want SizeLimitError", err)
	}
	if limitErr.Kind != "output_file" || limitErr.Size != 4 || limitErr.Limit != 3 {
		t.Fatalf("SizeLimitError = %#v, want output_file downloaded size 4/3", limitErr)
	}
}

type fakeRequester struct {
	response *nats.Msg
	handle   func(subject string, data []byte) *nats.Msg

	subject string
	data    []byte
}

func (r *fakeRequester) RequestWithContext(_ context.Context, subject string, data []byte) (*nats.Msg, error) {
	r.subject = subject
	r.data = append([]byte(nil), data...)
	if r.handle != nil {
		return r.handle(subject, data), nil
	}
	return r.response, nil
}

type fakeStore struct {
	objects map[string][]byte
	puts    []string
	gets    []string
	deleted []string
}

func newFakeStore() *fakeStore {
	return &fakeStore{objects: make(map[string][]byte)}
}

func (s *fakeStore) PutBytes(_ context.Context, name string, data []byte) (*jetstream.ObjectInfo, error) {
	s.puts = append(s.puts, name)
	s.objects[name] = append([]byte(nil), data...)
	return nil, nil
}

func (s *fakeStore) GetBytes(_ context.Context, name string, _ ...jetstream.GetObjectOpt) ([]byte, error) {
	s.gets = append(s.gets, name)
	data, ok := s.objects[name]
	if !ok {
		return nil, fmt.Errorf("object %q not found", name)
	}
	return append([]byte(nil), data...), nil
}

func (s *fakeStore) Delete(_ context.Context, name string) error {
	s.deleted = append(s.deleted, name)
	delete(s.objects, name)
	return nil
}

func newTestClient(cfg Config) *Client {
	return &Client{
		req:   &fakeRequester{response: runtimeResponse("run-a", nil)},
		store: newFakeStore(),
		cfg:   withDefaults(cfg),
	}
}

func runtimeResponse(runID string, artifacts []pythonRunArtifact) *nats.Msg {
	data, err := json.Marshal(pythonRunResponse{RunID: runID, Artifacts: artifacts})
	if err != nil {
		panic(err)
	}
	return &nats.Msg{Header: nats.Header{}, Data: data}
}
