package app

import (
	"bytes"
	"context"
	"encoding/json"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
)

func TestRunReconnectsAfterEmbeddedServerRestart(t *testing.T) {
	srv := startEmbeddedNATSServer(t, 0)
	port := srv.Addr().(*net.TCPAddr).Port
	url := srv.ClientURL()

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	var out bytes.Buffer
	errCh := make(chan error, 1)
	go func() {
		errCh <- Run(ctx, Config{URL: url, Instances: 1}, &out)
	}()
	t.Cleanup(func() {
		cancel()
		select {
		case err := <-errCh:
			if err != nil && err != context.Canceled {
				t.Logf("runtime returned error: %v", err)
			}
		case <-time.After(3 * time.Second):
			t.Log("runtime did not stop within cleanup timeout")
		}
	})
	waitForString(t, &out, "ready:", 3*time.Second)

	nc := connectEmbeddedNATSClient(t, url)
	defer nc.Close()
	requireTimestampResponse(t, nc)

	srv.Shutdown()
	srv.WaitForShutdown()

	srv = startEmbeddedNATSServer(t, port)
	t.Cleanup(func() {
		srv.Shutdown()
		srv.WaitForShutdown()
	})

	waitForNATSReconnect(t, nc)
	requireTimestampResponse(t, nc)
}

func startEmbeddedNATSServer(t *testing.T, port int) *server.Server {
	t.Helper()
	srv, err := server.NewServer(&server.Options{
		Host:   "127.0.0.1",
		Port:   port,
		NoSigs: true,
		NoLog:  true,
	})
	if err != nil {
		t.Fatalf("create embedded nats server returned error: %v", err)
	}
	srv.Start()
	if !srv.ReadyForConnections(5 * time.Second) {
		srv.Shutdown()
		t.Fatal("embedded nats server did not become ready")
	}
	return srv
}

func connectEmbeddedNATSClient(t *testing.T, url string) *nats.Conn {
	t.Helper()
	nc, err := nats.Connect(url,
		nats.Name("runtime-reconnect-test-client"),
		nats.MaxReconnects(-1),
		nats.ReconnectWait(250*time.Millisecond),
	)
	if err != nil {
		t.Fatalf("connect embedded nats client returned error: %v", err)
	}
	return nc
}

func waitForNATSReconnect(t *testing.T, nc *nats.Conn) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if nc.IsConnected() {
			return
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatalf("nats client status = %s, want connected after server restart", nc.Status())
}

func requireTimestampResponse(t *testing.T, nc *nats.Conn) {
	t.Helper()
	var lastErr error
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		msg, err := nc.Request("time.now", nil, 250*time.Millisecond)
		if err == nil {
			var payload map[string]string
			if err := json.Unmarshal(msg.Data, &payload); err != nil {
				t.Fatalf("timestamp response is not JSON: %v: %s", err, msg.Data)
			}
			if strings.TrimSpace(payload["timestamp"]) == "" {
				t.Fatalf("timestamp response = %s, want timestamp field", msg.Data)
			}
			return
		}
		lastErr = err
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatalf("request time.now returned error after retrying: %v", lastErr)
}
