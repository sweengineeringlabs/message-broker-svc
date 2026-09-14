# message-broker-svc-nats-spi

`NatsMessageBroker`/`NatsTaskQueue`: NATS-backed implementations of
`message-broker-pattern`'s `MessageBroker` and `TaskQueue` traits, via `async-nats`
(`TaskQueue` uses JetStream for competing-consumer semantics).

See [Architecture](../../../../../docs/3-design/architecture.md) for backend
details and this repo's own history.
