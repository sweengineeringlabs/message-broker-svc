# message-broker-svc

> **TLDR:** Concrete in-memory/NATS/Kafka/Postgres `MessageBroker` implementations on
> top of
> [`message-broker-pattern`](https://github.com/sweengineeringlabs/message-broker-pattern)'s
> contract. See [Architecture](docs/3-design/architecture.md) for the full design.

Extracted from `edge-runtime`'s in-tree pilot
(`scm/main/message-broker/{spi,saf}`, commits `ec34244`/`16ec508`) per
[edge-message-broker#6](https://github.com/sweengineeringlabs/edge-message-broker/issues/6),
mirroring this org's `<name>-pattern`/`<name>-svc` split (see
[`wasm-capability-svc`](https://github.com/sweengineeringlabs/wasm-capability-svc)).

## Quick Start

```rust
use message_broker_svc_saf::MessageBrokerFactory;

let broker = MessageBrokerFactory::nats("nats://localhost:4222").await?;
```

## Crates

| Crate | What it is |
|-------|------------|
| [`message-broker-svc-inmemory-spi`](scm/main/message-broker/spi/inmemory-spi) | `InMemoryMessageBroker` — `tokio::sync::broadcast`-backed |
| [`message-broker-svc-nats-spi`](scm/main/message-broker/spi/nats-spi) | `NatsMessageBroker` — `async-nats`-backed |
| [`message-broker-svc-kafka-spi`](scm/main/message-broker/spi/kafka-spi) | `KafkaMessageBroker` — `rdkafka`-backed |
| [`message-broker-svc-postgres-spi`](scm/main/message-broker/spi/postgres-spi) | `PostgresMessageBroker` — `pgmq`-backed |
| [`message-broker-svc-saf`](scm/main/message-broker/saf) | `MessageBrokerFactory` — the construction/dispatch facade consumers depend on |

No `BackendKind` enum, no shared config struct, no `from_config` dispatch — each `spi`
crate owns its own config type (`InMemoryConfig`/`NatsConfig`/`KafkaConfig`/`PostgresConfig`),
and `MessageBrokerFactory` exposes five independent, directly-typed constructors
(`noop`/`in_memory`/`nats`/`kafka`/`postgres`), not a runtime-selectable registry. See
[Architecture](docs/3-design/architecture.md) for why.

**Scope note:** this repo covers only `message-broker-pattern`'s `MessageBroker` trait.
`edge-runtime`'s own pilot also implemented a richer `TaskQueue` contract
(`runtime-message-broker-contract`, a superset) and an `ApplicationConfig`/`BrokerProvider`
composition layer (including the `BackendKind`-driven `from_config` dispatch mechanism
itself) — neither was ported here; they remain `edge-runtime`-specific concerns until a
similar extraction is scoped for them separately. The real,
`tokio::sync::broadcast`-backed in-memory backend *is* part of this repo, as
`message-broker-svc-inmemory-spi` — initially left out alongside the above on a mistaken
belief that nothing depended on it, restored once checking `edge-runtime`'s actual source
showed it does (see [Architecture](docs/3-design/architecture.md)).
`MessageBrokerFactory::noop()` remains this repo's own no-op reference implementation,
distinct from the real in-memory backend.

## Documentation

| Document | Description |
|----------|--------------|
| [Docs index](docs/README.md) | Full documentation index |
| [Architecture](docs/3-design/architecture.md) | Component diagram, dispatch table, scope boundary |
| [ADR-001](docs/3-design/adr/ADR-001-extract-from-edge-runtime-pilot.md) | Why this repo extracts from edge-runtime's pilot |
| [Developer Guide](docs/4-development/developer_guide.md) | Repo layout, feature flags, live-infra tests |

## License

MIT OR Apache-2.0
