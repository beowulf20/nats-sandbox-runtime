package app

import (
	"bufio"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/micro"
)

const (
	defaultRuntimeREPLTimeout = 30 * time.Second
	runtimeREPLPrompt         = "py> "
)

type RuntimeREPLConfig struct {
	URL          string
	Token        string
	Timeout      time.Duration
	MemoryMiB    int64
	WorkspaceMiB int64
	ExecTimeout  string
}

type runtimeREPLRequester interface {
	RequestWithContext(ctx context.Context, subject string, data []byte) (*nats.Msg, error)
}

type runtimeREPLRunRequest struct {
	Code         string `json:"code"`
	MemoryMiB    int64  `json:"memory_mib,omitempty"`
	WorkspaceMiB int64  `json:"workspace_mib,omitempty"`
	ExecTimeout  string `json:"exec_timeout,omitempty"`
}

type runtimeREPLInput struct {
	line string
	err  error
	eof  bool
}

func defaultRuntimeREPLConfig() RuntimeREPLConfig {
	return RuntimeREPLConfig{
		URL:     envString("NATS_URL", LocalNATSURL),
		Timeout: defaultRuntimeREPLTimeout,
	}
}

func validateRuntimeREPLConfig(cfg RuntimeREPLConfig) error {
	if cfg.URL == "" {
		return fmt.Errorf("url must not be empty")
	}
	if cfg.Timeout <= 0 {
		return fmt.Errorf("timeout must be greater than 0")
	}
	if cfg.MemoryMiB < 0 {
		return fmt.Errorf("memory-mib must be at least 0")
	}
	if cfg.WorkspaceMiB < 0 {
		return fmt.Errorf("workspace-mib must be at least 0")
	}
	if cfg.ExecTimeout != "" {
		parsed, err := time.ParseDuration(cfg.ExecTimeout)
		if err != nil {
			return fmt.Errorf("invalid exec-timeout %q: %w", cfg.ExecTimeout, err)
		}
		if parsed <= 0 {
			return fmt.Errorf("exec-timeout must be greater than 0")
		}
	}
	return nil
}

func RunRuntimeREPL(ctx context.Context, cfg RuntimeREPLConfig, stdin io.Reader, stdout io.Writer, stderr io.Writer) error {
	if err := validateRuntimeREPLConfig(cfg); err != nil {
		return err
	}

	opts := append(natsConnectOptions("nats-sandbox-runtime-test-repl", cfg.Token), nats.Timeout(cfg.Timeout))
	nc, err := nats.Connect(cfg.URL, opts...)
	if err != nil {
		return fmt.Errorf("connect nats: %w", err)
	}
	defer nc.Close()

	return runRuntimeREPLWithRequester(ctx, cfg, nc, stdin, stdout, stderr)
}

func runRuntimeREPLWithRequester(ctx context.Context, cfg RuntimeREPLConfig, requester runtimeREPLRequester, stdin io.Reader, stdout io.Writer, stderr io.Writer) error {
	if err := validateRuntimeREPLConfig(cfg); err != nil {
		return err
	}
	if requester == nil {
		return fmt.Errorf("runtime repl requester is not configured")
	}

	inputs := readRuntimeREPLInput(ctx, stdin)
	for {
		if _, err := fmt.Fprint(stdout, runtimeREPLPrompt); err != nil {
			return fmt.Errorf("write prompt: %w", err)
		}

		var input runtimeREPLInput
		select {
		case <-ctx.Done():
			return ctx.Err()
		case next, ok := <-inputs:
			if !ok {
				return nil
			}
			input = next
		}
		if input.err != nil {
			return fmt.Errorf("read stdin: %w", input.err)
		}
		if input.eof {
			return nil
		}

		line := input.line
		switch line {
		case "":
			continue
		case "exit", "quit", ".exit":
			return nil
		}

		if err := requestRuntimeREPLLine(ctx, cfg, requester, line, stdout, stderr); err != nil {
			return err
		}
	}
}

func readRuntimeREPLInput(ctx context.Context, stdin io.Reader) <-chan runtimeREPLInput {
	inputs := make(chan runtimeREPLInput)
	go func() {
		defer close(inputs)
		scanner := bufio.NewScanner(stdin)
		for scanner.Scan() {
			input := runtimeREPLInput{line: scanner.Text()}
			select {
			case inputs <- input:
			case <-ctx.Done():
				return
			}
		}
		input := runtimeREPLInput{eof: true, err: scanner.Err()}
		select {
		case inputs <- input:
		case <-ctx.Done():
		}
	}()
	return inputs
}

func requestRuntimeREPLLine(ctx context.Context, cfg RuntimeREPLConfig, requester runtimeREPLRequester, line string, stdout io.Writer, stderr io.Writer) error {
	req := runtimeREPLRunRequest{
		Code:         line,
		MemoryMiB:    cfg.MemoryMiB,
		WorkspaceMiB: cfg.WorkspaceMiB,
		ExecTimeout:  cfg.ExecTimeout,
	}
	data, err := json.Marshal(req)
	if err != nil {
		return fmt.Errorf("marshal python run request: %w", err)
	}

	requestCtx, cancel := context.WithTimeout(ctx, cfg.Timeout)
	defer cancel()

	msg, err := requester.RequestWithContext(requestCtx, runtimePythonEndpointSubject, data)
	if err != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		return fmt.Errorf("request %s: %w", runtimePythonEndpointSubject, err)
	}

	if err := writeRuntimeREPLLogs(stdout, stderr, msg.Header); err != nil {
		return err
	}

	if description := msg.Header.Get(micro.ErrorHeader); description != "" {
		code := msg.Header.Get(micro.ErrorCodeHeader)
		if code == "" {
			code = "unknown"
		}
		fmt.Fprintf(stderr, "runtime_error: code=%s description=%s", code, description)
		if len(msg.Data) > 0 {
			fmt.Fprintf(stderr, " data=%s", strings.TrimSpace(string(msg.Data)))
		}
		fmt.Fprintln(stderr)
	}
	return nil
}

func writeRuntimeREPLLogs(stdout io.Writer, stderr io.Writer, headers nats.Header) error {
	if err := writeRuntimeREPLLog(stdout, headers, "Nats-Sandbox-Runtime-Python-Stdout-B64"); err != nil {
		return err
	}
	if err := writeRuntimeREPLLog(stderr, headers, "Nats-Sandbox-Runtime-Python-Stderr-B64"); err != nil {
		return err
	}
	return nil
}

func writeRuntimeREPLLog(out io.Writer, headers nats.Header, headerName string) error {
	encoded := headers.Get(headerName)
	if encoded == "" {
		return nil
	}
	data, err := base64.StdEncoding.DecodeString(encoded)
	if err != nil {
		return fmt.Errorf("decode %s: %w", headerName, err)
	}
	if len(data) == 0 {
		return nil
	}
	if _, err := out.Write(data); err != nil {
		return fmt.Errorf("write %s: %w", headerName, err)
	}
	return nil
}
