# NATS Service Registrations Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a Go CLI that registers multiple NATS service instances exposing one timestamp endpoint.

**Architecture:** A Cobra command parses `--instances` and passes a config to an app runner. The runner creates one NATS connection per service instance and registers the same `timestamp` service with endpoint `time.now`. The timestamp package owns response JSON formatting.

**Tech Stack:** Go, `github.com/spf13/cobra`, `github.com/nats-io/nats.go`, `github.com/nats-io/nats.go/micro`.

---

### Task 1: Timestamp Payload

**Files:**
- Create: `internal/timestamp/timestamp_test.go`
- Create: `internal/timestamp/timestamp.go`

- [x] Write a failing test for RFC3339Nano UTC JSON payload.
- [x] Run `tmp/go/bin/go test ./internal/timestamp` and verify `Payload` is missing.
- [x] Implement `Payload(time.Time) ([]byte, error)`.
- [x] Run `tmp/go/bin/go test ./internal/timestamp` and verify pass.

### Task 2: CLI Config

**Files:**
- Create: `internal/app/root_test.go`
- Create: `internal/app/root.go`

- [x] Write failing tests for default one instance, `--instances 3`, and rejecting zero.
- [x] Run `tmp/go/bin/go test ./internal/app` and verify config symbols are missing.
- [x] Implement `Config`, `LocalNATSURL`, and `NewRootCommand`.
- [x] Run `tmp/go/bin/go test ./internal/app` and verify pass.

### Task 3: NATS Service Runner

**Files:**
- Create: `internal/app/service.go`
- Create: `cmd/nats-sandbox-runtime/main.go`
- Create: `README.md`

- [x] Implement `Run(context.Context, Config, io.Writer) error`.
- [x] Connect each instance to `nats://localhost:4222`.
- [x] Register service `timestamp` version `0.0.1`.
- [x] Register endpoint `time.now`.
- [x] Wait for Ctrl-C in `main`.
- [x] Document run and request commands.

### Task 4: Verification

**Files:**
- Check all created Go files.

- [x] Run `tmp/go/bin/gofmt -w ...`.
- [x] Run `tmp/go/bin/go test ./internal/app ./internal/timestamp`.
- [x] Run `tmp/go/bin/go test ./cmd/nats-sandbox-runtime ./internal/...`.
- [x] Run `tmp/go/bin/go build -buildvcs=false -o /tmp/nats-sandbox-runtime-check ./cmd/nats-sandbox-runtime`.
