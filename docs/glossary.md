# Glossary

Alphabetized list of terms used in `message-broker-svc`.

---

**InMemoryConfig** - `message-broker-svc-core`'s config type. No fields — this backend takes no runtime parameters.

**InMemoryMessageBroker** - In-process `MessageBroker` backed by `tokio::sync::broadcast`. Real, full-fanout pub/sub, distinct from `NoopMessageBroker`.

**InMemoryTaskQueue** - In-process `TaskQueue` backed by `tokio::sync::mpsc`.

**KafkaConfig** - `message-broker-svc-kafka-spi`'s config type (`url`, `group_id`).

**KafkaMessageBroker** - `MessageBroker` backed by `rdkafka`. Each `subscribe()` call derives its own unique consumer-group ID so multiple subscribers fan out rather than compete for partitions.

**KafkaTaskQueue** - `TaskQueue` backed by `rdkafka`, using the caller-supplied `group_id` directly — competing consumption is what a task queue wants.

**MessageBrokerFactory** - Construction facade in `message-broker-svc-saf`: `noop`/`in_memory`/`nats`/`kafka`/`postgres`, all returning `Box<dyn MessageBroker>`. No shared "which backend" type, no config-driven runtime dispatch.

**NatsConfig** - `message-broker-svc-nats-spi`'s config type (`url`).

**NatsMessageBroker** - `MessageBroker` backed by `async-nats`.

**NatsTaskQueue** - `TaskQueue` backed by `async-nats` JetStream, using competing-consumer groups.

**NoopMessageBroker** - Reference no-op `MessageBroker`: publishing discards the message, subscribing yields an empty stream. `pub(crate)`, reachable only via `MessageBrokerFactory::noop()`.

**PostgresConfig** - `message-broker-svc-postgres-spi`'s config type (`url`, `queue_name`).

**PostgresMessageBroker** - `MessageBroker` backed by `sqlx` + the `pgmq` Postgres extension. Delivery is queue semantics (one consumer per message), not broadcast — the one backend here that doesn't fan out. Has no `TaskQueue` counterpart.

**TaskQueueFactory** - Construction facade in `message-broker-svc-saf`, mirroring `MessageBrokerFactory`: `in_memory`/`nats`/`kafka`, all returning `Box<dyn TaskQueue>`. No `postgres` constructor and no no-op reference — neither exists for `TaskQueue` in this domain.

**validate_config** - A default method on `message-broker-pattern`'s own `Validator` trait: validates any `Self: Validator` before a backend attempts I/O. Every backend's config type calls `self.validate_config()` once in its own constructor instead of hand-rolling its own check. Previously lived here as `message-broker-svc-spi-shared`'s `ValidatorExt`, folded directly into `Validator` itself — see `message-broker-pattern`'s own glossary/architecture doc.

**validator_response** - `Validator`'s second default method: wraps a `Validator`-implementing config in the `ValidatorResponse` every `MessageBroker::validator()` implementation returns, byte-for-byte identical across all four backends before this method existed.

[← Docs index](README.md)
