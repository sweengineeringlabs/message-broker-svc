# message-broker-svc

> **TLDR:** Concrete in-memory/NATS/Kafka/Postgres `MessageBroker` implementations, plus
> in-memory/NATS/Kafka `TaskQueue` implementations, on top of
> [`message-broker-pattern`](https://github.com/sweengineeringlabs/message-broker-pattern)'s
> contract. See [Architecture](docs/3-design/architecture.md) for the full design.

Extracted from `edge-runtime`'s in-tree pilot
(`scm/main/message-broker/{spi,saf}`, commits `ec34244`/`16ec508`) per
[edge-message-broker#6](https://github.com/sweengineeringlabs/edge-message-broker/issues/6),
mirroring this org's `<name>-pattern`/`<name>-svc` split (see
[`wasm-capability-svc`](https://github.com/sweengineeringlabs/wasm-capability-svc)).

## Quick Start

Requires the `inmemory` feature (`cargo add message-broker-svc-saf --features inmemory`):

```rust
use message_broker_svc_saf::MessageBrokerFactory;

// No external service required -- real in-process pub/sub, not a stub.
let broker = MessageBrokerFactory::in_memory();
```

## Crates

| Crate | What it is |
|-------|------------|
| [`message-broker-svc-inmemory-spi`](scm/main/message-broker/spi/inmemory-spi) | `InMemoryMessageBroker` (`tokio::sync::broadcast`) + `InMemoryTaskQueue` (`tokio::sync::mpsc`) |
| [`message-broker-svc-nats-spi`](scm/main/message-broker/spi/nats-spi) | `NatsMessageBroker` + `NatsTaskQueue` — `async-nats`-backed |
| [`message-broker-svc-kafka-spi`](scm/main/message-broker/spi/kafka-spi) | `KafkaMessageBroker` + `KafkaTaskQueue` — `rdkafka`-backed |
| [`message-broker-svc-postgres-spi`](scm/main/message-broker/spi/postgres-spi) | `PostgresMessageBroker` — `pgmq`-backed; no `TaskQueue` (`edge-runtime`'s original pilot never had one for Postgres) |
| [`message-broker-svc-saf`](scm/main/message-broker/saf) | `MessageBrokerFactory` + `TaskQueueFactory` — the construction facades consumers depend on |

No `BackendKind` enum, no shared config struct, no `from_config` dispatch — each `spi`
crate owns its own config type (`InMemoryConfig`/`NatsConfig`/`KafkaConfig`/`PostgresConfig`),
`MessageBrokerFactory` exposes five independent, directly-typed constructors
(`noop`/`in_memory`/`nats`/`kafka`/`postgres`), and `TaskQueueFactory` mirrors it with
three more (`in_memory`/`nats`/`kafka`, no `postgres`, no no-op) — none of it a
runtime-selectable registry. See [Architecture](docs/3-design/architecture.md) for why.

**Scope note:** this repo covers `message-broker-pattern`'s `MessageBroker` *and*
`TaskQueue` traits — both now live in that one pattern crate. `TaskQueue` was initially
left out here (and left behind in `edge-runtime`'s own local copy of the contract) on
the belief that was a deliberate scope boundary; it wasn't — a consumer is supposed to
get this domain's whole primitive set from `message-broker-pattern` plus this repo, not
half of it redefined downstream. `ApplicationConfig`/`BrokerProvider` (an
`edge-runtime`-specific composition layer, including the `BackendKind`-driven
`from_config` dispatch mechanism) remains the one thing genuinely out of scope — it's
`edge-runtime`-specific by nature, not a migration gap. See
[Architecture](docs/3-design/architecture.md) for the full reasoning, including why the
in-memory backend is real (not `noop`'s stand-in) and why backend selection is five
independent constructors rather than a config-driven dispatch.

## Documentation

| Document | Description |
|----------|--------------|
| [Docs index](docs/README.md) | Full documentation index |
| [Architecture](docs/3-design/architecture.md) | Component diagram, dispatch table, scope boundary |
| [ADR-001](docs/3-design/adr/ADR-001-extract-from-edge-runtime-pilot.md) | Why this repo extracts from edge-runtime's pilot |
| [Developer Guide](docs/4-development/developer_guide.md) | Repo layout, feature flags, live-infra tests |

## License

MIT OR Apache-2.0
