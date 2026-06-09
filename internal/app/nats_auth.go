package app

import (
	"os"
	"strings"
	"time"

	"github.com/nats-io/nats.go"
)

func natsConnectOptions(name, token string) []nats.Option {
	opts := []nats.Option{
		nats.Name(name),
		nats.MaxReconnects(-1),
		nats.ReconnectWait(250 * time.Millisecond),
	}
	if resolved := natsConnectToken(token); resolved != "" {
		opts = append(opts, nats.Token(resolved))
	}
	return opts
}

func natsConnectToken(token string) string {
	if token != "" {
		return token
	}
	if value := os.Getenv("NATS_TOKEN"); value != "" {
		return value
	}
	if path := os.Getenv("NATS_TOKEN_FILE"); path != "" {
		data, err := os.ReadFile(path)
		if err == nil {
			return strings.TrimSpace(string(data))
		}
	}
	return ""
}
