# NATS Service Registrations Design

Goal: create a simple Go command that registers multiple copies of the same NATS service against `nats://localhost:4222`.

Design:

- Use Cobra for one CLI flag: `--instances` / `-i`.
- Default to one instance and reject values below one.
- Keep the NATS URL fixed at `nats://localhost:4222`.
- Create one NATS connection per service instance so each registration has independent connection state.
- Register the same service name, version, and endpoint on each instance.
- Add one endpoint at subject `time.now`.
- Return the current timestamp as JSON using RFC3339Nano in UTC.
- Keep the process alive until Ctrl-C, then stop services and close NATS connections.

Testing:

- Unit-test timestamp payload formatting.
- Unit-test Cobra default config, `--instances`, and invalid instance count.
- Build the command package.
