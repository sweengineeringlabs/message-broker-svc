# message-broker-svc Developer Guide

## Repo Structure

```
message-broker-svc/
├── README.md
├── scm/
│   ├── Cargo.toml          # workspace: [main/message-broker/{spi/nats,spi/kafka,spi/postgres,saf}]
│   ├── docs/
│   │   ├── 3-design/architecture.md
│   │   ├── 3-design/adr/ADR-001-extract-from-edge-runtime-pilot.md
│   │   └── 4-development/developer_guide.md     # this file
│   └── main/message-broker/
│       ├── spi/
│       │   ├── nats/        # message-broker-pattern-nats-spi
│       │   ├── kafka/       # message-broker-pattern-kafka-spi
│       │   └── postgres/    # message-broker-pattern-postgres-spi
│       └── saf/              # message-broker-svc-saf -- MessageBrokerFactory
```

## Branching and Releases

- `dev` is the default branch; all work lands there first.
- `main` gets fast-forwarded to `dev` after a shipped change, not on every commit.
- Pre-1.0 SemVer: a breaking change bumps the minor version.
- Not yet tagged or published to crates.io. Depends on
  [`message-broker-pattern`](https://github.com/sweengineeringlabs/message-broker-pattern)
  via `git`, `branch = "dev"`, for the same reason — pin both to a tag once
  `message-broker-pattern` cuts one.

## Working on Any Crate

All four crates are members of `scm/Cargo.toml`, so from `scm/`:

```
cargo test --workspace --all-targets
cargo clippy --workspace --all-targets -- -D warnings
cargo fmt --check
```

Each `spi` crate's real feature is off by default in `message-broker-svc-saf` — enable
what you're working on:

```
cargo test --manifest-path main/message-broker/saf/Cargo.toml --features nats
cargo test --manifest-path main/message-broker/saf/Cargo.toml --features kafka
cargo test --manifest-path main/message-broker/saf/Cargo.toml --features postgres
cargo test --manifest-path main/message-broker/saf/Cargo.toml --features nats,kafka,postgres
```

`kafka`'s `rdkafka` dependency builds `librdkafka` from source via `cmake` on first
build (`cmake-build` Cargo feature) — the first `cargo build`/`test` touching that crate
takes noticeably longer (~2 minutes) than a pure-Rust crate; subsequent builds are
incremental like any other crate.

## Live-Infra Tests

Every `spi` crate's tests run without any live backend — connection/config error paths
only. Round-trip tests against a real broker are `#[ignore]`d by default:

```
# Kafka
KAFKA_BROKERS=localhost:9092 cargo test --manifest-path main/message-broker/saf/Cargo.toml \
  --features kafka -- --include-ignored --test kafka_message_broker_int_test

# Postgres (CREATE EXTENSION pgmq; must already be applied on the target database)
POSTGRES_DSN=postgres://postgres:postgres@localhost:5433/edge_test \
  cargo test --manifest-path main/message-broker/saf/Cargo.toml \
  --features postgres -- --include-ignored --test postgres_message_broker_int_test
```

## No Direct spi Import Outside saf

Enforced, not a style preference: `NatsMessageBroker`/`KafkaMessageBroker`/
`PostgresMessageBroker` are `pub` within their own `spi` crates (required for
`message-broker-svc-saf` to construct them across the crate boundary), but `saf`'s own
`lib.rs` re-exports only `MessageBrokerFactory` — a consumer depending on
`message-broker-svc-saf` alone cannot name a concrete backend type without also
depending directly on that `spi` crate itself.

## The path + version Dependency Rule

`message-broker-svc-saf` depends on its sibling `spi` crates with **both** `path` and
`version` set — same reasoning as `message-broker-pattern`'s own developer guide (a
`path`-only dependency makes the crate unpublishable). The `message-broker-pattern-*`
crates are pinned via `git`/`branch` for now, not `path` + `version`, since they live in
a separate repo with no tag cut yet.

## Scope

See `architecture.md`'s Scope boundary section before adding anything here that isn't a
`MessageBroker` implementation — `TaskQueue`, `ApplicationConfig`/`BrokerProvider`, and a
real in-memory backend were all deliberately left out of this extraction.

## See Also

- [Architecture](../3-design/architecture.md)
- [ADR-001](../3-design/adr/ADR-001-extract-from-edge-runtime-pilot.md)
