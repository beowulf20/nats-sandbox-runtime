package app

import (
	"context"
	"time"

	"github.com/nats-io/nats.go/micro"
	"go.opentelemetry.io/otel/trace"
)

type runtimeExecutionStats struct {
	Completed               int64 `json:"completed"`
	Errors                  int64 `json:"errors"`
	RejectedBusy            int64 `json:"rejected_busy"`
	InFlight                int64 `json:"in_flight"`
	ProcessingTimeNS        int64 `json:"processing_time_ns"`
	AverageProcessingTimeNS int64 `json:"average_processing_time_ns"`
}

func (s *runtimePythonService) executionStats(endpoint *micro.Endpoint) any {
	if endpoint.Name != "run" {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	stats := s.stats
	if stats.Completed > 0 {
		stats.AverageProcessingTimeNS = stats.ProcessingTimeNS / stats.Completed
	}
	return map[string]any{"execution": stats}
}

func (s *runtimePythonService) dispatchRun(req micro.Request) {
	worker, ok := s.admit()
	telemetry := runtimeTelemetry(s.cfg)
	if !ok {
		observeMicroRequest(telemetry, runtimePythonEndpointSubject, func(_ context.Context, req micro.Request) {
			_ = req.Error("503", "python runtime is busy", nil)
		})(req)
		return
	}
	go func() {
		defer s.jobs.Done()
		observeMicroRequest(telemetry, runtimePythonEndpointSubject, func(ctx context.Context, req micro.Request) {
			base := s.jobContext
			if base == nil {
				base = context.Background()
			}
			ctx = trace.ContextWithSpan(base, trace.SpanFromContext(ctx))
			s.handleAccepted(ctx, req, worker)
		})(req)
	}()
}

// drain closes admission under the same mutex as WaitGroup.Add, then allows
// normal replies before cancelling. Reconnection must also join remaining jobs.
func (s *runtimePythonService) drain(grace, cleanup time.Duration) {
	s.mu.Lock()
	s.stopping = true
	s.mu.Unlock()
	done := make(chan struct{})
	go func() { s.jobs.Wait(); close(done) }()
	timer := time.NewTimer(grace)
	defer timer.Stop()
	select {
	case <-done:
		return
	case <-timer.C:
	}
	if s.cancelJobs != nil {
		s.cancelJobs()
	}
	timer.Reset(cleanup)
	select {
	case <-done:
	case <-timer.C:
	}
}
