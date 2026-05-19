package app

import (
	"context"
	"encoding/base64"
	"fmt"
	"io"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/nats-io/nats.go/micro"
)

type NATSDeploymentTestConfig struct {
	URL     string
	Token   string
	Bucket  string
	Subject string
	Payload string
	Timeout time.Duration
}

func defaultNATSDeploymentTestConfig() NATSDeploymentTestConfig {
	return NATSDeploymentTestConfig{
		URL:     envString("NATS_URL", LocalNATSURL),
		Bucket:  envString("NATS_BUCKET", defaultRuntimeBucket),
		Timeout: 5 * time.Second,
	}
}

func validateNATSDeploymentTestConfig(cfg NATSDeploymentTestConfig) error {
	if cfg.URL == "" {
		return fmt.Errorf("url must not be empty")
	}
	if cfg.Timeout <= 0 {
		return fmt.Errorf("timeout must be greater than 0")
	}
	if cfg.Subject == "" && cfg.Payload != "" {
		return fmt.Errorf("subject must not be empty when payload is set")
	}
	return nil
}

func RunNATSDeploymentTest(ctx context.Context, cfg NATSDeploymentTestConfig, out io.Writer) error {
	if err := validateNATSDeploymentTestConfig(cfg); err != nil {
		return err
	}

	opts := append(natsConnectOptions("nats-sandbox-runtime-test", cfg.Token), nats.Timeout(cfg.Timeout))
	nc, err := nats.Connect(cfg.URL, opts...)
	if err != nil {
		return fmt.Errorf("connect nats: %w", err)
	}
	defer nc.Close()

	jetStream, _ := nc.ConnectedServerJetStream()
	fmt.Fprintf(out, "connected: url=%s server=%s version=%s jetstream=%t\n", nc.ConnectedUrlRedacted(), nc.ConnectedServerName(), nc.ConnectedServerVersion(), jetStream)

	if cfg.Bucket != "" {
		if !jetStream {
			return fmt.Errorf("connected NATS server does not report JetStream support")
		}
		if err := verifyNATSDeploymentBucket(ctx, nc, cfg, out); err != nil {
			return err
		}
	}

	if cfg.Subject != "" {
		if err := requestNATSDeploymentSubject(ctx, nc, cfg, out); err != nil {
			return err
		}
	}

	fmt.Fprintln(out, "ok")
	return nil
}

func verifyNATSDeploymentBucket(ctx context.Context, nc *nats.Conn, cfg NATSDeploymentTestConfig, out io.Writer) error {
	js, err := jetstream.New(nc)
	if err != nil {
		return fmt.Errorf("create jetstream context: %w", err)
	}
	requestCtx, cancel := context.WithTimeout(ctx, cfg.Timeout)
	defer cancel()

	store, err := js.ObjectStore(requestCtx, cfg.Bucket)
	if err != nil {
		return fmt.Errorf("open object store %q: %w", cfg.Bucket, err)
	}
	status, err := store.Status(requestCtx)
	if err != nil {
		return fmt.Errorf("read object store %q status: %w", cfg.Bucket, err)
	}
	fmt.Fprintf(out, "object_store: bucket=%s storage=%s replicas=%d size_bytes=%d\n", status.Bucket(), status.Storage(), status.Replicas(), status.Size())
	return nil
}

func requestNATSDeploymentSubject(ctx context.Context, nc *nats.Conn, cfg NATSDeploymentTestConfig, out io.Writer) error {
	requestCtx, cancel := context.WithTimeout(ctx, cfg.Timeout)
	defer cancel()

	start := time.Now()
	msg, err := nc.RequestWithContext(requestCtx, cfg.Subject, []byte(cfg.Payload))
	if err != nil {
		return fmt.Errorf("request %s: %w", cfg.Subject, err)
	}
	elapsed := time.Since(start)

	if description := msg.Header.Get(micro.ErrorHeader); description != "" {
		writeNATSDeploymentRuntimeLogHeaders(out, msg.Header)
		code := msg.Header.Get(micro.ErrorCodeHeader)
		return fmt.Errorf("request %s returned NATS error code=%s description=%s data=%s", cfg.Subject, code, description, string(msg.Data))
	}
	fmt.Fprintf(out, "request: subject=%s result=success bytes=%d duration=%s\n", cfg.Subject, len(msg.Data), elapsed)
	writeNATSDeploymentRuntimeLogHeaders(out, msg.Header)
	if len(msg.Data) > 0 {
		fmt.Fprintf(out, "response: %s\n", msg.Data)
	}
	return nil
}

func writeNATSDeploymentRuntimeLogHeaders(out io.Writer, headers nats.Header) {
	for _, logHeader := range []struct {
		name  string
		label string
	}{
		{name: "Nats-Sandbox-Runtime-Python-Stdout-B64", label: "python_stdout"},
		{name: "Nats-Sandbox-Runtime-Python-Stderr-B64", label: "python_stderr"},
	} {
		encoded := headers.Get(logHeader.name)
		if encoded == "" {
			continue
		}
		data, err := base64.StdEncoding.DecodeString(encoded)
		if err != nil {
			fmt.Fprintf(out, "%s: invalid base64 in header %s: %v\n", logHeader.label, logHeader.name, err)
			continue
		}
		if len(data) > 0 {
			fmt.Fprintf(out, "%s:\n%s\n", logHeader.label, data)
		}
		if headers.Get(logHeader.name+"-Truncated") == "true" {
			fmt.Fprintf(out, "%s_truncated: true\n", logHeader.label)
		}
	}
}
