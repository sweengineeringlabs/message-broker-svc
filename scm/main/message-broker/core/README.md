# message-broker-svc-core

`InMemoryMessageBroker`/`InMemoryTaskQueue`: the technology-free reference
implementation of `message-broker-pattern`'s `MessageBroker`/`TaskQueue`
traits, backed only by `tokio::sync::broadcast`/`tokio::sync::mpsc`. Not an
`spi` — it wraps no external technology.

See [Architecture](../../../../docs/3-design/architecture.md) for the full
explanation.
