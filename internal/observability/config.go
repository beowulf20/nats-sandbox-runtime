package observability

import (
	"fmt"
	"log/slog"
	"math"
	"net"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"
)

type Config struct {
	Endpoint       string
	Level          slog.Level
	SampleRatio    float64
	MetricInterval time.Duration
}

func ConfigFromEnv() (Config, error) {
	c := Config{Level: slog.LevelInfo, SampleRatio: 1, MetricInterval: 15 * time.Second}
	if v := os.Getenv("LOG_LEVEL"); v != "" {
		switch strings.ToLower(v) {
		case "debug":
			c.Level = slog.LevelDebug
		case "info":
		case "warn":
			c.Level = slog.LevelWarn
		case "error":
			c.Level = slog.LevelError
		default:
			return c, fmt.Errorf("LOG_LEVEL must be debug, info, warn, or error")
		}
	}
	if v := os.Getenv("OTEL_EXPORTER_OTLP_PROTOCOL"); v != "" && v != "grpc" {
		return c, fmt.Errorf("OTEL_EXPORTER_OTLP_PROTOCOL must be grpc")
	}
	if v := os.Getenv("OTEL_EXPORTER_OTLP_INSECURE"); v != "" && v != "true" {
		return c, fmt.Errorf("OTEL_EXPORTER_OTLP_INSECURE must be true")
	}
	if v := os.Getenv("OTEL_TRACES_SAMPLER"); v != "" && v != "parentbased_traceidratio" {
		return c, fmt.Errorf("OTEL_TRACES_SAMPLER must be parentbased_traceidratio")
	}
	if v := os.Getenv("OTEL_TRACES_SAMPLER_ARG"); v != "" {
		r, err := strconv.ParseFloat(v, 64)
		if err != nil || math.IsNaN(r) || math.IsInf(r, 0) || r < 0 || r > 1 {
			return c, fmt.Errorf("OTEL_TRACES_SAMPLER_ARG must be finite and between 0 and 1")
		}
		c.SampleRatio = r
	}
	if v := os.Getenv("OTEL_METRIC_EXPORT_INTERVAL"); v != "" {
		n, err := strconv.ParseInt(v, 10, 64)
		if err != nil || n <= 0 || n > math.MaxInt64/int64(time.Millisecond) {
			return c, fmt.Errorf("OTEL_METRIC_EXPORT_INTERVAL must be positive milliseconds")
		}
		c.MetricInterval = time.Duration(n) * time.Millisecond
	}
	c.Endpoint = os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT")
	if c.Endpoint != "" {
		u, err := url.Parse(c.Endpoint)
		if err != nil || u.Scheme != "http" || u.User != nil || u.Host == "" || (u.Path != "" && u.Path != "/") || u.RawQuery != "" || u.Fragment != "" {
			return c, fmt.Errorf("OTEL_EXPORTER_OTLP_ENDPOINT must be an http://host:port URL without credentials, path, query, or fragment")
		}
		_, port, err := net.SplitHostPort(u.Host)
		n, portErr := strconv.Atoi(port)
		if err != nil || portErr != nil || n < 1 || n > 65535 {
			return c, fmt.Errorf("OTEL_EXPORTER_OTLP_ENDPOINT requires a valid explicit port")
		}
		c.Endpoint = u.Host
	}
	return c, nil
}
