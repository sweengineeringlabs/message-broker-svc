# message-broker-svc-inmemory-spi

`InMemoryMessageBroker`/`InMemoryTaskQueue`: in-process implementations of
`message-broker-pattern`'s `MessageBroker` (via `tokio::sync::broadcast`) and
`TaskQueue` (via `tokio::sync::mpsc`) traits. No external service dependency.

See [Architecture](../../../../../docs/3-design/architecture.md) for how this
backend was restored and why it exists alongside the no-op reference.
