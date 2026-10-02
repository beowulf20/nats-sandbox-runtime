package observability

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"time"

	"go.opentelemetry.io/contrib/bridges/otelslog"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlplog/otlploggrpc"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetricgrpc"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/metric"
	metricnoop "go.opentelemetry.io/otel/metric/noop"
	"go.opentelemetry.io/otel/propagation"
	sdklog "go.opentelemetry.io/otel/sdk/log"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
	tracenoop "go.opentelemetry.io/otel/trace/noop"
	"google.golang.org/grpc"
	"google.golang.org/grpc/backoff"
)

const ExportTimeout = 5 * time.Second

var DurationBuckets = []float64{.001, .005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10, 30, 60}
var Propagator = propagation.TraceContext{}

type Runtime struct {
	Logger                                                                       *slog.Logger
	Tracer                                                                       trace.Tracer
	Meter                                                                        metric.Meter
	InstanceID                                                                   string
	NATSRequests, HTTPRequests, Runs, Reconnects, CleanupDeleted, SSEConnections metric.Int64Counter
	NATSDuration, HTTPDuration, RunDuration                                      metric.Float64Histogram
	Workers                                                                      metric.Int64ObservableGauge
	traces                                                                       *sdktrace.TracerProvider
	metrics                                                                      *sdkmetric.MeterProvider
	logs                                                                         *sdklog.LoggerProvider
	diagnostics                                                                  *diagnostics
	closeOnce                                                                    sync.Once
	closeErr                                                                     error
}

func New(ctx context.Context, c Config, out io.Writer) (r *Runtime, err error) {
	r = &Runtime{Tracer: tracenoop.NewTracerProvider().Tracer("runtime"), Meter: metricnoop.NewMeterProvider().Meter("runtime"), InstanceID: rand.Text()}
	res, err := resource.New(ctx, resource.WithAttributes(attribute.String("service.name", "nats-sandbox-runtime"), attribute.String("service.version", "0.0.1")), resource.WithFromEnv())
	if err != nil {
		return nil, fmt.Errorf("telemetry resource: %w", err)
	}
	res, err = resource.Merge(res, resource.NewSchemaless(attribute.String("service.instance.id", r.InstanceID)))
	if err != nil {
		return nil, err
	}
	name, _ := res.Set().Value("service.name")
	local := slog.New(slog.NewJSONHandler(out, &slog.HandlerOptions{Level: c.Level})).With("service.name", name.AsString(), "service.instance.id", r.InstanceID)
	r.Logger = slog.New(contextHandler{local.Handler(), c.Level})
	r.diagnostics = &diagnostics{logger: local}
	if c.Endpoint != "" {
		dial := grpc.WithConnectParams(grpc.ConnectParams{Backoff: backoff.Config{BaseDelay: time.Second, Multiplier: 1.6, Jitter: .2, MaxDelay: 2 * time.Second}, MinConnectTimeout: ExportTimeout})
		otel.SetErrorHandler(r.diagnostics)
		defer func() {
			if err != nil {
				shutdownCtx, cancel := context.WithTimeout(context.Background(), ExportTimeout)
				defer cancel()
				_ = r.Shutdown(shutdownCtx)
			}
		}()
		te, e := otlptracegrpc.New(ctx, otlptracegrpc.WithEndpoint(c.Endpoint), otlptracegrpc.WithInsecure(), otlptracegrpc.WithDialOption(dial), otlptracegrpc.WithHeaders(map[string]string{}), otlptracegrpc.WithTimeout(ExportTimeout), otlptracegrpc.WithRetry(otlptracegrpc.RetryConfig{Enabled: true, InitialInterval: time.Second, MaxInterval: 2 * time.Second, MaxElapsedTime: ExportTimeout}))
		if e != nil {
			return r, e
		}
		r.traces = sdktrace.NewTracerProvider(sdktrace.WithResource(res), sdktrace.WithSampler(sdktrace.ParentBased(sdktrace.TraceIDRatioBased(c.SampleRatio))), sdktrace.WithSpanLimits(sdktrace.SpanLimits{AttributeValueLengthLimit: 1024, AttributeCountLimit: 128, EventCountLimit: 128, LinkCountLimit: 128, AttributePerEventCountLimit: 128, AttributePerLinkCountLimit: 128}), sdktrace.WithBatcher(te, sdktrace.WithMaxQueueSize(2048), sdktrace.WithMaxExportBatchSize(512), sdktrace.WithBatchTimeout(5*time.Second), sdktrace.WithExportTimeout(ExportTimeout)))
		me, e := otlpmetricgrpc.New(ctx, otlpmetricgrpc.WithEndpoint(c.Endpoint), otlpmetricgrpc.WithInsecure(), otlpmetricgrpc.WithDialOption(dial), otlpmetricgrpc.WithHeaders(map[string]string{}), otlpmetricgrpc.WithTimeout(ExportTimeout), otlpmetricgrpc.WithRetry(otlpmetricgrpc.RetryConfig{Enabled: true, InitialInterval: time.Second, MaxInterval: 2 * time.Second, MaxElapsedTime: ExportTimeout}), otlpmetricgrpc.WithTemporalitySelector(sdkmetric.DefaultTemporalitySelector))
		if e != nil {
			return r, e
		}
		r.metrics = sdkmetric.NewMeterProvider(sdkmetric.WithResource(res), sdkmetric.WithReader(sdkmetric.NewPeriodicReader(me, sdkmetric.WithInterval(c.MetricInterval), sdkmetric.WithTimeout(ExportTimeout))))
		le, e := otlploggrpc.New(ctx, otlploggrpc.WithEndpoint(c.Endpoint), otlploggrpc.WithInsecure(), otlploggrpc.WithDialOption(dial), otlploggrpc.WithHeaders(map[string]string{}), otlploggrpc.WithTimeout(ExportTimeout), otlploggrpc.WithRetry(otlploggrpc.RetryConfig{Enabled: true, InitialInterval: time.Second, MaxInterval: 2 * time.Second, MaxElapsedTime: ExportTimeout}))
		if e != nil {
			return r, e
		}
		r.logs = sdklog.NewLoggerProvider(sdklog.WithResource(res), sdklog.WithAttributeValueLengthLimit(1024), sdklog.WithProcessor(sdklog.NewBatchProcessor(le, sdklog.WithMaxQueueSize(2048), sdklog.WithExportMaxBatchSize(512), sdklog.WithExportInterval(5*time.Second), sdklog.WithExportTimeout(ExportTimeout))))
		r.Tracer = r.traces.Tracer("nats-sandbox-runtime")
		r.Meter = r.metrics.Meter("nats-sandbox-runtime")
		bridge := otelslog.NewHandler("nats-sandbox-runtime", otelslog.WithLoggerProvider(r.logs)).WithAttrs([]slog.Attr{slog.String("service.name", name.AsString()), slog.String("service.instance.id", r.InstanceID)})
		r.Logger = slog.New(contextHandler{slog.NewMultiHandler(local.Handler(), bridge), c.Level})
	}
	err = r.initInstruments()
	return r, err
}

func (r *Runtime) initInstruments() error {
	var err error
	for _, item := range []struct {
		name   string
		target *metric.Int64Counter
	}{
		{"sandbox.nats.requests", &r.NATSRequests}, {"sandbox.http.requests", &r.HTTPRequests}, {"sandbox.runtime.runs", &r.Runs}, {"sandbox.runtime.reconnects", &r.Reconnects}, {"sandbox.runtime.cleanup.deleted", &r.CleanupDeleted}, {"sandbox.http.sse.connections", &r.SSEConnections},
	} {
		*item.target, err = r.Meter.Int64Counter(item.name, metric.WithUnit("1"))
		if err != nil {
			return err
		}
	}
	for _, item := range []struct {
		name   string
		target *metric.Float64Histogram
	}{
		{"sandbox.nats.request.duration", &r.NATSDuration}, {"sandbox.http.request.duration", &r.HTTPDuration}, {"sandbox.runtime.run.duration", &r.RunDuration},
	} {
		*item.target, err = r.Meter.Float64Histogram(item.name, metric.WithUnit("s"), metric.WithExplicitBucketBoundaries(DurationBuckets...))
		if err != nil {
			return err
		}
	}
	r.Workers, err = r.Meter.Int64ObservableGauge("sandbox.runtime.workers", metric.WithUnit("1"))
	return err
}

// Each provider gets the same fresh caller deadline, concurrently.
func (r *Runtime) Shutdown(ctx context.Context) error {
	r.closeOnce.Do(func() {
		var wg sync.WaitGroup
		var mu sync.Mutex
		for _, stop := range []func(context.Context) error{
			func(ctx context.Context) error {
				if r.traces != nil {
					return r.traces.Shutdown(ctx)
				}
				return nil
			},
			func(ctx context.Context) error {
				if r.metrics != nil {
					return r.metrics.Shutdown(ctx)
				}
				return nil
			},
			func(ctx context.Context) error {
				if r.logs != nil {
					return r.logs.Shutdown(ctx)
				}
				return nil
			},
		} {
			wg.Go(func() {
				if err := stop(ctx); err != nil {
					mu.Lock()
					r.closeErr = errors.Join(r.closeErr, err)
					mu.Unlock()
				}
			})
		}
		wg.Wait()
		if r.closeErr != nil {
			r.diagnostics.Handle(r.closeErr)
		}
	})
	return r.closeErr
}
