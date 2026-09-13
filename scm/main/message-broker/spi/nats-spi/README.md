# message-broker-pattern-nats-spi

`NatsMessageBroker`: NATS-backed implementation of `message-broker-pattern-contract`'s
`MessageBroker` trait, via `async-nats`.

Extracted from `edge-runtime`'s `runtime-message-broker-nats-spi` (`MessageBroker`-only —
its `TaskQueue` implementation was not ported) per
[edge-message-broker#6](https://github.com/sweengineeringlabs/edge-message-broker/issues/6).
