package app

import (
	"context"
	"log"
	"log/slog"
	"nats-sandbox-runtime/internal/observability"
	"net/http"
	"strings"
	"time"

	"github.com/nats-io/nats.go/micro"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

func logMicroRequest(endpoint string, handler micro.HandlerFunc) micro.HandlerFunc {
	return func(req micro.Request) {
		start := time.Now()
		loggedReq := &microRequestLogger{Request: req}
		handler(loggedReq)
		elapsed := time.Since(start)

		if loggedReq.err != nil {
			log.Printf("request endpoint=%s subject=%s result=error error=%q duration=%s", endpoint, req.Subject(), loggedReq.err.Error(), elapsed)
			return
		}
		if loggedReq.errorCode != "" {
			log.Printf("request endpoint=%s subject=%s result=error code=%s duration=%s", endpoint, req.Subject(), loggedReq.errorCode, elapsed)
			return
		}
		if !loggedReq.completed {
			log.Printf("request endpoint=%s subject=%s result=error error=%q duration=%s", endpoint, req.Subject(), "no response sent", elapsed)
			return
		}
		log.Printf("request endpoint=%s subject=%s result=success duration=%s", endpoint, req.Subject(), elapsed)
	}
}

type microRequestLogger struct {
	micro.Request

	completed bool
	errorCode string
	err       error
}

func (r *microRequestLogger) Respond(data []byte, opts ...micro.RespondOpt) error {
	err := r.Request.Respond(data, opts...)
	r.completed = true
	r.err = err
	return err
}

func (r *microRequestLogger) RespondJSON(value any, opts ...micro.RespondOpt) error {
	err := r.Request.RespondJSON(value, opts...)
	r.completed = true
	r.err = err
	return err
}

func (r *microRequestLogger) Error(code, description string, data []byte, opts ...micro.RespondOpt) error {
	err := r.Request.Error(code, description, data, opts...)
	r.completed = true
	r.errorCode = code
	r.err = err
	return err
}

type httpResponseLogger struct {
	http.ResponseWriter

	status int
}

func (w *httpResponseLogger) WriteHeader(status int) {
	if w.status != 0 {
		return
	}
	w.status = status
	w.ResponseWriter.WriteHeader(status)
}

func (w *httpResponseLogger) Write(data []byte) (int, error) {
	if w.status == 0 {
		w.status = http.StatusOK
	}
	return w.ResponseWriter.Write(data)
}

func (w *httpResponseLogger) Flush() {
	if w.status == 0 {
		w.status = http.StatusOK
	}
	flusher, ok := w.ResponseWriter.(http.Flusher)
	if ok {
		flusher.Flush()
	}
}

func (w *httpResponseLogger) Unwrap() http.ResponseWriter { return w.ResponseWriter }

// NATS preserves header spelling; W3C names must work with either casing.
type natsTraceCarrier map[string][]string

func (c natsTraceCarrier) Get(key string) string {
	for k, v := range c {
		if strings.EqualFold(k, key) && len(v) > 0 {
			return v[0]
		}
	}
	return ""
}
func (c natsTraceCarrier) Set(key, value string) { c[key] = []string{value} }
func (c natsTraceCarrier) Keys() []string {
	keys := make([]string, 0, len(c))
	for key := range c {
		keys = append(keys, key)
	}
	return keys
}

func observeMicroRequest(t *observability.Runtime, endpoint string, handler func(context.Context, micro.Request)) micro.HandlerFunc {
	return func(req micro.Request) {
		start := time.Now()
		ctx := observability.Propagator.Extract(context.Background(), natsTraceCarrier(req.Headers()))
		ctx, span := t.Tracer.Start(ctx, "nats.consume "+endpoint, trace.WithSpanKind(trace.SpanKindConsumer), trace.WithAttributes(attribute.String("messaging.system", "nats"), attribute.String("messaging.destination.name", endpoint)))
		logged := &microRequestLogger{Request: req}
		defer func() {
			panicValue := recover()
			result, code := "success", "none"
			switch {
			case panicValue != nil:
				result, code = "error", "other"
			case logged.err != nil:
				result, code = "error", "reply_error"
			case logged.errorCode != "":
				result = "error"
				code = logged.errorCode
				if code != "400" && code != "500" && code != "503" {
					code = "other"
				}
			case !logged.completed:
				result, code = "error", "no_response"
			}
			elapsed := time.Since(start)
			attrs := metric.WithAttributes(attribute.String("endpoint", endpoint), attribute.String("result", result), attribute.String("code", code))
			t.NATSRequests.Add(ctx, 1, attrs)
			t.NATSDuration.Record(ctx, elapsed.Seconds(), attrs)
			level := slog.LevelInfo
			if result == "error" {
				level = slog.LevelError
				span.SetStatus(codes.Error, code)
			}
			t.Logger.Log(ctx, level, "NATS request completed", "event", "request.completed", "transport", "nats", "endpoint", endpoint, "result", result, "code", code, "duration_ms", float64(elapsed)/float64(time.Millisecond))
			span.End()
			if panicValue != nil {
				panic(panicValue)
			}
		}()
		handler(ctx, logged)
	}
}

func observeHTTPRequest(t *observability.Runtime, mux *http.ServeMux) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.HasPrefix(r.URL.Path, "/api/") && r.URL.Path != "/api" {
			mux.ServeHTTP(w, r)
			return
		}
		_, route := mux.Handler(r)
		if route == "/" || route == "" {
			route = "unmatched"
		}
		if route == "/api/workers/events" {
			mux.ServeHTTP(w, r)
			return
		}
		ctx := observability.Propagator.Extract(r.Context(), propagation.HeaderCarrier(r.Header))
		ctx, span := t.Tracer.Start(ctx, "http "+route, trace.WithSpanKind(trace.SpanKindServer), trace.WithAttributes(attribute.String("http.route", route)))
		start := time.Now()
		writer := &httpResponseLogger{ResponseWriter: w}
		defer func() {
			panicValue := recover()
			status := writer.status
			if status == 0 {
				status = 200
				if panicValue != nil {
					status = 500
				}
			}
			if status < 100 || status > 599 {
				status = 0
			}
			result := "success"
			if status >= 400 || panicValue != nil {
				result = "error"
			}
			if status >= 500 || panicValue != nil {
				span.SetStatus(codes.Error, "handler_error")
			}
			method := r.Method
			switch method {
			case "GET", "HEAD", "POST", "PUT", "PATCH", "DELETE", "OPTIONS":
			default:
				method = "OTHER"
			}
			elapsed := time.Since(start)
			attrs := metric.WithAttributes(attribute.String("route", route), attribute.String("method", method), attribute.Int("status_code", status), attribute.String("result", result))
			t.HTTPRequests.Add(ctx, 1, attrs)
			t.HTTPDuration.Record(ctx, elapsed.Seconds(), attrs)
			level := slog.LevelInfo
			if result == "error" {
				level = slog.LevelError
			}
			t.Logger.Log(ctx, level, "HTTP request completed", "event", "request.completed", "transport", "http", "route", route, "method", method, "status", status, "result", result, "duration_ms", float64(elapsed)/float64(time.Millisecond))
			span.SetAttributes(attribute.Int("http.response.status_code", status))
			span.End()
			if panicValue != nil {
				panic(panicValue)
			}
		}()
		mux.ServeHTTP(writer, r.WithContext(ctx))
	})
}
