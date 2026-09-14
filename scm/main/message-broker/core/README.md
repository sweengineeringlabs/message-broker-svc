# message-broker-svc-core

`InMemoryMessageBroker`: the technology-free reference implementation of
`message-broker-pattern`'s `MessageBroker` trait, backed only by
`tokio::sync::broadcast`. Not an `spi` — it wraps no external technology.
(The in-memory `TaskQueue` implementation that used to live here moved to
[`task-queue-svc`](https://github.com/sweengineeringlabs/task-queue-svc), SRP.)

See [Architecture](../../../../docs/3-design/architecture.md) for the full
explanation.
