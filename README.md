# message-broker-svc

Concrete message-broker service: NATS/Kafka/Postgres `MessageBroker` implementations on
top of [`message-broker-pattern`](https://github.com/sweengineeringlabs/message-broker-pattern)'s
contract.

Extracted from `edge-runtime`'s in-tree pilot
(`scm/main/message-broker/{spi,saf}`, commits `ec34244`/`16ec508`) per
[edge-message-broker#6](https://github.com/sweengineeringlabs/edge-message-broker/issues/6),
mirroring this org's `<name>-pattern`/`<name>-svc` split (see
[`wasm-capability-svc`](https://github.com/sweengineeringlabs/wasm-capability-svc)).

Four crates:

| Crate | What it is |
|-------|------------|
| [`message-broker-pattern-nats-spi`](scm/main/message-broker/spi/nats) | `NatsMessageBroker` — `async-nats`-backed |
| [`message-broker-pattern-kafka-spi`](scm/main/message-broker/spi/kafka) | `KafkaMessageBroker` — `rdkafka`-backed |
| [`message-broker-pattern-postgres-spi`](scm/main/message-broker/spi/postgres) | `PostgresMessageBroker` — `pgmq`-backed |
| [`message-broker-svc-saf`](scm/main/message-broker/saf) | `MessageBrokerFactory` — the construction/dispatch facade consumers depend on |

**Scope note:** this repo covers only `message-broker-pattern-contract`'s `MessageBroker`
trait. `edge-runtime`'s own pilot also implemented a richer `TaskQueue` contract
(`runtime-message-broker-contract`, a superset) and an `ApplicationConfig`/`BrokerProvider`
composition layer — neither was ported here; they remain `edge-runtime`-specific concerns
until a similar extraction is scoped for them separately. The in-memory backend is not
part of this repo either — see `message-broker-pattern-saf::BrokerSvc::noop_broker` for
the reference no-op broker.

## License

MIT OR Apache-2.0
