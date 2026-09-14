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
| [`message-broker-svc-core`](scm/main/message-broker/core) | The technology-free reference implementation: in-memory `MessageBroker` + `TaskQueue` (no external dependency, so not an "spi") |
| [`message-broker-svc-spi-shared`](scm/main/message-broker/spi/shared) | Implementation code shared by more than one `*-spi` backend (`ValidatorExt`) |
| [`message-broker-svc-nats-spi`](scm/main/message-broker/spi/nats-spi) | NATS `MessageBroker` + `TaskQueue` |
| [`message-broker-svc-kafka-spi`](scm/main/message-broker/spi/kafka-spi) | Kafka `MessageBroker` + `TaskQueue` |
| [`message-broker-svc-postgres-spi`](scm/main/message-broker/spi/postgres-spi) | Postgres `MessageBroker` (no `TaskQueue`) |
| [`message-broker-svc-saf`](scm/main/message-broker/saf) | `MessageBrokerFactory` + `TaskQueueFactory` — construction facades consumers depend on |

See [Architecture](docs/3-design/architecture.md) for how backend selection works and
what's deliberately out of scope.

## Documentation

| Document | Description |
|----------|--------------|
| [Docs index](docs/README.md) | Full documentation index |
| [Architecture](docs/3-design/architecture.md) | Component diagram, dispatch table, scope boundary |
| [ADR-001](docs/3-design/adr/ADR-001-extract-from-edge-runtime-pilot.md) | Why this repo extracts from edge-runtime's pilot |
| [Developer Guide](docs/4-development/developer_guide.md) | Repo layout, feature flags, live-infra tests |

## License

MIT OR Apache-2.0
