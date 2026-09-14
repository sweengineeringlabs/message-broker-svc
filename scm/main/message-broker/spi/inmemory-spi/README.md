# message-broker-svc-inmemory-spi

`InMemoryMessageBroker`/`InMemoryTaskQueue`: in-process implementations of
`message-broker-pattern`'s `MessageBroker` (via `tokio::sync::broadcast`) and
`TaskQueue` (via `tokio::sync::mpsc`) traits. No external service dependency.

Ported 1:1 from `edge-runtime`'s `runtime-message-broker-core`, restoring the gap
`edge-message-broker`'s original extraction left behind (see
`message-broker-pattern`'s own architecture doc, "Restoring the real in-memory
backend") — the `MessageBroker` half first, `TaskQueue` following once it became clear
leaving it behind in `edge-runtime` was an incomplete migration, not a scope decision.
