package app

import (
	"os"

	"github.com/nats-io/nats.go"
)

func natsConnectOptions(name, token string) []nats.Option {
	opts := []nats.Option{nats.Name(name)}
	if resolved := natsConnectToken(token); resolved != "" {
		opts = append(opts, nats.Token(resolved))
	}
	return opts
}

func natsConnectToken(token string) string {
	if token != "" {
		return token
	}
	return os.Getenv("NATS_TOKEN")
}
