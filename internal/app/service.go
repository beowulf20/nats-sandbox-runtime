package app

import (
	"context"
	"fmt"
	"io"
	"time"

	"nats-sandbox-runtime/internal/timestamp"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/micro"
)

const (
	serviceName        = "timestamp"
	serviceVersion     = "0.0.1"
	serviceDescription = "Returns current timestamp"
)

type serviceInstance struct {
	conn    *nats.Conn
	service micro.Service
}

func Run(ctx context.Context, cfg Config, out io.Writer) error {
	if cfg.Instances < 1 {
		return fmt.Errorf("instances must be at least 1")
	}

	instances := make([]serviceInstance, 0, cfg.Instances)
	for i := 1; i <= cfg.Instances; i++ {
		instance, err := registerInstance(cfg.URL, i)
		if err != nil {
			stopInstances(instances)
			return err
		}
		instances = append(instances, instance)

		info := instance.service.Info()
		fmt.Fprintf(out, "registered instance %d: service=%s id=%s endpoint=time.now url=%s\n", i, info.Name, info.ID, cfg.URL)
	}

	fmt.Fprintf(out, "ready: %d service instance(s) registered; request subject time.now\n", len(instances))

	<-ctx.Done()

	stopInstances(instances)
	return nil
}

func registerInstance(url string, index int) (serviceInstance, error) {
	nc, err := nats.Connect(url, nats.Name(fmt.Sprintf("timestamp-service-%d", index)))
	if err != nil {
		return serviceInstance{}, fmt.Errorf("connect instance %d: %w", index, err)
	}

	srv, err := micro.AddService(nc, micro.Config{
		Name:        serviceName,
		Version:     serviceVersion,
		Description: serviceDescription,
	})
	if err != nil {
		nc.Close()
		return serviceInstance{}, fmt.Errorf("add service instance %d: %w", index, err)
	}

	group := srv.AddGroup("time")
	if err := group.AddEndpoint("now", micro.HandlerFunc(handleTimestamp)); err != nil {
		_ = srv.Stop()
		nc.Close()
		return serviceInstance{}, fmt.Errorf("add endpoint instance %d: %w", index, err)
	}

	return serviceInstance{conn: nc, service: srv}, nil
}

func handleTimestamp(req micro.Request) {
	payload, err := timestamp.Payload(time.Now())
	if err != nil {
		_ = req.Error("500", "timestamp payload failed", nil)
		return
	}
	_ = req.Respond(payload)
}

func stopInstances(instances []serviceInstance) {
	for i := len(instances) - 1; i >= 0; i-- {
		if instances[i].service != nil {
			_ = instances[i].service.Stop()
		}
		if instances[i].conn != nil {
			instances[i].conn.Close()
		}
	}
}
