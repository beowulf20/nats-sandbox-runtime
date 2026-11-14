package pyruntime

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"reflect"
	"sort"
	"strings"
	"sync"
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

func TestImportProfilesAreOptInAndValidateBeforeUpload(t *testing.T) {
	for _, profile := range []string{"", ImportProfileNumPy, ImportProfilePlotAgg, ImportProfilePlotCairo} {
		client := newTestClient(Config{})
		client.req = &fakeRequester{handle: func(_ string, data []byte) *nats.Msg {
			var wire map[string]json.RawMessage
			if err := json.Unmarshal(data, &wire); err != nil {
				t.Fatal(err)
			}
			_, present := wire["import_profile"]
			if present != (profile != "") {
				t.Fatalf("profile presence=%t", present)
			}
			var req pythonRunRequest
			if err := json.Unmarshal(data, &req); err != nil {
				t.Fatal(err)
			}
			if req.ImportProfile != profile {
				t.Fatalf("profile=%q", req.ImportProfile)
			}
			return runtimeResponse(req.RunID, nil)
		}}
		if _, err := client.Run(t.Context(), Request{ThreadID: "thread", Code: "pass", ImportProfile: profile}); err != nil {
			t.Fatal(err)
		}
	}
	client := newTestClient(Config{})
	_, err := client.Run(t.Context(), Request{ThreadID: "thread", Code: "pass", ImportProfile: "../escape", Files: map[string][]byte{"a": {1}}})
	if err == nil || len(client.store.(*fakeStore).puts) != 0 {
		t.Fatal("invalid profile uploaded inputs")
	}
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
			if !reflect.DeepEqual(req.ArtifactExcludePrefixes, []string{"artifacts/"}) {
				t.Fatalf("exclusions=%v", req.ArtifactExcludePrefixes)
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
		ThreadID:                "thread-a",
		Code:                    "print('hi')",
		ArtifactExcludePrefixes: []string{"artifacts/"},
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
	sort.Strings(store.puts)
	if !strings.HasSuffix(store.puts[0], "/a.txt") || !strings.HasSuffix(store.puts[1], "/nested/b.txt") {
		t.Fatalf("puts = %#v, want both input uploads", store.puts)
	}
}

func TestExclusionValidationAndConcurrentActualTotal(t *testing.T) {
	c := newTestClient(Config{})
	_, err := c.Run(t.Context(), Request{ThreadID: "thread", Code: "fixture", ArtifactExcludePrefixes: []string{"../escape"}, Files: map[string][]byte{"a": {1}}})
	if err == nil || len(c.store.(*fakeStore).puts) != 0 {
		t.Fatalf("invalid exclusion uploaded inputs: err=%v", err)
	}
	store := newFakeStore()
	store.objects["a"], store.objects["b"] = []byte("abc"), []byte("def")
	files, err := downloadArtifacts(t.Context(), store, []pythonRunArtifact{{Path: "a", Object: "a", Size: 1}, {Path: "b", Object: "b", Size: 1}}, withDefaults(Config{MaxOutputFileBytes: 4, MaxOutputTotalBytes: 5}))
	var limit SizeLimitError
	if !errors.As(err, &limit) || limit.Kind != "output_total" || limit.Size != 6 || files != nil {
		t.Fatalf("files=%v err=%v", files, err)
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

func TestRunReturnsMicroErrorsWithoutDeletingUploadedInputs(t *testing.T) {
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
	if len(store.puts) != 1 {
		t.Fatalf("puts = %#v, want one uploaded input", store.puts)
	}
	if _, found := store.objects[store.puts[0]]; !found {
		t.Fatalf("uploaded input %q was deleted by SDK client", store.puts[0])
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

func TestRunExplainsObjectStoreConsumerDeletePermissionError(t *testing.T) {
	store := newFakeStore()
	store.getErr = errors.New(`nats: permissions violation: Permissions Violation for Publish to "$JS.API.CONSUMER.DELETE.OBJ_python-runtime-workspaces.bHINEdZ2"`)
	client := &Client{
		req: &fakeRequester{response: runtimeResponse("run-a", []pythonRunArtifact{
			{Path: "out.txt", Object: "runs/run-a/artifacts/out.txt", Size: 1},
		})},
		store: store,
		cfg:   withDefaults(Config{}),
	}

	_, err := client.Run(t.Context(), Request{ThreadID: "thread-a", Code: "print(1)"})
	if err == nil {
		t.Fatal("Run returned nil, want permission error")
	}
	if !strings.Contains(err.Error(), `$JS.API.CONSUMER.DELETE.OBJ_python-runtime-workspaces.>`) {
		t.Fatalf("Run error = %v, want consumer delete permission hint", err)
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
	mu        sync.Mutex
	objects   map[string][]byte
	puts      []string
	gets      []string
	getErr    error
	sizeHints map[string]uint64
	streams   []*fakeObjectResult
}

func newFakeStore() *fakeStore {
	return &fakeStore{objects: make(map[string][]byte)}
}

func (s *fakeStore) PutBytes(_ context.Context, name string, data []byte) (*jetstream.ObjectInfo, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.puts = append(s.puts, name)
	s.objects[name] = append([]byte(nil), data...)
	return nil, nil
}

func (s *fakeStore) Get(_ context.Context, name string, _ ...jetstream.GetObjectOpt) (jetstream.ObjectResult, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.gets = append(s.gets, name)
	if s.getErr != nil {
		return nil, s.getErr
	}
	data, ok := s.objects[name]
	if !ok {
		return nil, fmt.Errorf("object %q not found", name)
	}
	size := uint64(len(data))
	if hint, ok := s.sizeHints[name]; ok {
		size = hint
	}
	stream := &fakeObjectResult{Reader: bytes.NewReader(data), size: size}
	s.streams = append(s.streams, stream)
	return stream, nil
}

type fakeObjectResult struct {
	io.Reader
	size   uint64
	closed bool
}

func (r *fakeObjectResult) Info() (*jetstream.ObjectInfo, error) {
	return &jetstream.ObjectInfo{Size: r.size}, nil
}
func (r *fakeObjectResult) Error() error { return nil }
func (r *fakeObjectResult) Close() error { r.closed = true; return nil }

func TestDownloadsEnforceActualBytesWithUnderstatedObjectMetadata(t *testing.T) {
	store := newFakeStore()
	store.objects["a"], store.objects["b"] = []byte("abc"), []byte("def")
	store.sizeHints = map[string]uint64{"a": 1, "b": 1}
	files, err := downloadArtifacts(t.Context(), store, []pythonRunArtifact{{Path: "a", Object: "a", Size: 1}, {Path: "b", Object: "b", Size: 1}}, withDefaults(Config{MaxOutputFileBytes: 4, MaxOutputTotalBytes: 5}))
	var limit SizeLimitError
	if files != nil || !errors.As(err, &limit) || limit.Kind != "output_total" || limit.Size != 6 {
		t.Fatalf("files=%v err=%v, want actual total rejection", files, err)
	}
	for _, stream := range store.streams {
		if !stream.closed {
			t.Fatal("failed transfer left an object stream open")
		}
	}
}

func TestDownloadChecksRealObjectMetadataBeforeAllocating(t *testing.T) {
	store := newFakeStore()
	store.objects["a"] = []byte("abc")
	files, err := downloadArtifacts(t.Context(), store, []pythonRunArtifact{{Path: "a", Object: "a", Size: 1}}, withDefaults(Config{MaxOutputFileBytes: 2}))
	var limit SizeLimitError
	if files != nil || !errors.As(err, &limit) || limit.Kind != "output_file" || !store.streams[0].closed {
		t.Fatalf("files=%v err=%v, want closed stream and actual file limit", files, err)
	}
}

type failingObjectStore struct {
	*fakeStore
	stream *fakeObjectResult
}

func (s failingObjectStore) Get(context.Context, string, ...jetstream.GetObjectOpt) (jetstream.ObjectResult, error) {
	return s.stream, nil
}

type failureReader struct{ err error }

func (r failureReader) Read([]byte) (int, error) { return 0, r.err }

func TestDownloadRequiresSuccessfulStreamEOF(t *testing.T) {
	checksumErr := errors.New("object digest mismatch")
	stream := &fakeObjectResult{Reader: io.MultiReader(strings.NewReader("data"), failureReader{checksumErr}), size: 4}
	store := failingObjectStore{newFakeStore(), stream}
	files, err := downloadArtifacts(t.Context(), store, []pythonRunArtifact{{Path: "a", Object: "a", Size: 4}}, withDefaults(Config{}))
	if files != nil || !errors.Is(err, checksumErr) || !stream.closed {
		t.Fatalf("files=%v err=%v closed=%t", files, err, stream.closed)
	}
}

func TestDownloadPreservesAnEmptyFile(t *testing.T) {
	store := newFakeStore()
	store.objects["empty"] = []byte{}
	files, err := downloadArtifacts(t.Context(), store, []pythonRunArtifact{{Path: "empty", Object: "empty", Size: 0}}, withDefaults(Config{}))
	if err != nil {
		t.Fatal(err)
	}
	data, ok := files["empty"]
	if !ok || data == nil || len(data) != 0 {
		t.Fatalf("empty file lost byte-slice semantics: present=%t data=%#v", ok, data)
	}
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
