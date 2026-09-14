# message-broker-svc-nats-spi

`NatsMessageBroker`/`NatsTaskQueue`: NATS-backed implementations of
`message-broker-pattern`'s `MessageBroker` and `TaskQueue` traits, via `async-nats`
(`TaskQueue` uses JetStream for competing-consumer semantics).

Extracted from `edge-runtime`'s `runtime-message-broker-nats-spi` per
[edge-message-broker#6](https://github.com/sweengineeringlabs/edge-message-broker/issues/6).
`MessageBroker` was ported first; `TaskQueue` followed once it became clear leaving it
behind in `edge-runtime` was an incomplete migration, not a scope decision — see
`message-broker-pattern`'s own architecture doc for the full reasoning.
