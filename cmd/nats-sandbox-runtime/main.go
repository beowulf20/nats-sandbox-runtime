package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/signal"

	"nats-sandbox-runtime/internal/app"
)

func main() {
	cmd := app.NewRootCommandWithRuntimeAPIAndTools(func(cfg app.Config) error {
		ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
		defer stop()

		return app.Run(ctx, cfg, os.Stdout)
	}, func(ctx context.Context, cfg app.LocalPythonConfig) error {
		ctx, stop := signal.NotifyContext(ctx, os.Interrupt)
		defer stop()

		return app.RunLocalPython(ctx, cfg, os.Stdin, os.Stdout, os.Stderr)
	}, func(ctx context.Context, cfg app.RuntimePythonConfig) error {
		ctx, stop := signal.NotifyContext(ctx, os.Interrupt)
		defer stop()

		return app.RunRuntimePython(ctx, cfg, os.Stdout)
	}, func(ctx context.Context, cfg app.RuntimeAPIConfig) error {
		ctx, stop := signal.NotifyContext(ctx, os.Interrupt)
		defer stop()

		return app.RunRuntimeAPI(ctx, cfg, os.Stdout)
	}, func(ctx context.Context, cfg app.NATSDeploymentTestConfig) error {
		ctx, stop := signal.NotifyContext(ctx, os.Interrupt)
		defer stop()

		return app.RunNATSDeploymentTest(ctx, cfg, os.Stdout)
	}, func(ctx context.Context, cfg app.RuntimeREPLConfig) error {
		ctx, stop := signal.NotifyContext(ctx, os.Interrupt)
		defer stop()

		if err := app.RunRuntimeREPL(ctx, cfg, os.Stdin, os.Stdout, os.Stderr); err != nil {
			if errors.Is(err, context.Canceled) {
				return nil
			}
			return err
		}
		return nil
	})

	if err := cmd.Execute(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
