# Glossary

Alphabetized list of terms used in `message-broker-svc`.

---

**AnyMessageBroker** - Zero-cost enum in `message-broker-svc-saf`, one variant per backend (`Noop`/`InMemory`/`Nats`/`Kafka`/`Postgres`), implementing `MessageBroker` by matching on `self` and delegating. The uniform return type every `MessageBrokerFactory` constructor returns — replaces `Box<dyn MessageBroker>`, which stopped compiling once `MessageBroker` lost object safety (its methods return `impl Future`, not a boxed one). See `docs/3-design/architecture.md`'s "Why `AnyMessageBroker`, not `Box<dyn MessageBroker>`".

**InMemoryConfig** - `message-broker-svc-core`'s config type. No fields — this backend takes no runtime parameters.

**InMemoryMessageBroker** - In-process `MessageBroker` backed by `tokio::sync::broadcast`. Real, full-fanout pub/sub, distinct from `NoopMessageBroker`.

**KafkaConfig** - `message-broker-svc-kafka-spi`'s config type (`url`, `group_id`).

**KafkaMessageBroker** - `MessageBroker` backed by `rdkafka`. Each `subscribe()` call derives its own unique consumer-group ID so multiple subscribers fan out rather than compete for partitions.

**MessageBrokerFactory** - Construction facade in `message-broker-svc-saf`: `noop`/`in_memory`/`nats`/`kafka`/`postgres`, all returning `AnyMessageBroker`. No shared "which backend" type, no config-driven runtime dispatch.

**NatsConfig** - `message-broker-svc-nats-spi`'s config type (`url`).

**NatsMessageBroker** - `MessageBroker` backed by `async-nats`.

**NoopMessageBroker** - Reference no-op `MessageBroker`: publishing discards the message, subscribing yields an empty stream. `pub` (required as `AnyMessageBroker`'s own variant payload), but its containing module is never `pub`, so it stays unreachable to construct from outside `message-broker-svc-saf` — only `MessageBrokerFactory::noop()` returns one.

**PostgresConfig** - `message-broker-svc-postgres-spi`'s config type (`url`, `queue_name`).

**PostgresMessageBroker** - `MessageBroker` backed by `sqlx` + the `pgmq` Postgres extension. Delivery is queue semantics (one consumer per message), not broadcast — the one backend here that doesn't fan out.

**InMemoryTaskQueue**, **NatsTaskQueue**, **KafkaTaskQueue**, **TaskQueueFactory** - moved to
[`task-queue-svc`](https://github.com/sweengineeringlabs/task-queue-svc) (SRP —
see `docs/3-design/architecture.md`'s "Why `TaskQueue` implementations moved
out (SRP)"). See that repo's own glossary.

**validate_config** - A default method on `message-broker-pattern`'s own `Validator` trait: validates any `Self: Validator` before a backend attempts I/O. Every backend's config type calls `self.validate_config()` once in its own constructor instead of hand-rolling its own check. Previously lived here as `message-broker-svc-spi-shared`'s `ValidatorExt`, folded directly into `Validator` itself — see `message-broker-pattern`'s own glossary/architecture doc.

**validator_response** - `Validator`'s second default method: wraps a `Validator`-implementing config in the `ValidatorResponse` every `MessageBroker::validator()` implementation returns, byte-for-byte identical across all four backends before this method existed.

[← Docs index](README.md)
