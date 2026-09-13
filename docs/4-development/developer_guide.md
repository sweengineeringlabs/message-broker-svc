# message-broker-svc Developer Guide

## Repo Structure

```
message-broker-svc/
├── README.md
├── docs/
│   ├── README.md                                 # docs section index
│   ├── 0-ideation/papers/README.md
│   ├── 3-design/README.md, architecture.md
│   ├── 3-design/adr/ADR-001-extract-from-edge-runtime-pilot.md
│   └── 4-development/README.md, developer_guide.md   # this file
└── scm/
    ├── Cargo.toml          # workspace: [core, spi/*, saf]
    └── main/message-broker/
        ├── core/              # message-broker-svc-core -- validate_config<C: Validator>
        ├── spi/
        │   ├── nats-spi/        # message-broker-svc-nats-spi
        │   ├── kafka-spi/       # message-broker-svc-kafka-spi
        │   └── postgres-spi/    # message-broker-svc-postgres-spi
        └── saf/              # message-broker-svc-saf -- MessageBrokerFactory
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

All five crates are members of `scm/Cargo.toml`, so from `scm/`:

```
cargo test --workspace --all-targets
cargo clippy --workspace --all-targets -- -D warnings
cargo fmt --check
```

`MessageBrokerFactory::noop()` is always available, no feature required. Each real
backend's feature is off by default in `message-broker-svc-saf` — enable what you're
working on:

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

## Config: Each spi Crate Owns Its Own, None Shared

There is no `BackendKind` enum, no shared `MessageBrokerConfig` struct, and no
`from_config` dispatch anywhere in this repo — see `architecture.md`'s own section on
this and ADR-001's amendment for the full reasoning. Each `spi` crate defines its own
local config type (`NatsConfig`/`KafkaConfig`/`PostgresConfig`), implementing both
`configbuilder::OptionalSection` and `message-broker-pattern::Validator` independently.
Adding a new backend means adding a new `spi` crate with its own config type and a new
`MessageBrokerFactory` constructor — never touching a shared enum or struct, because
there isn't one.

## message-broker-svc-core: the One Shared, Generic Implementation

Every `spi` crate's constructor calls `message_broker_svc_core::validate_config(&config)`
once, before doing any I/O — one generic function
(`validate_config<C: Validator>(config: &C) -> Result<(), BrokerError>`), reused
identically by all three backends instead of each hand-rolling its own check. Mirrors
`ledger`'s own split: `LedgerPayload` (trait) lives in `ledger-base-port`; the generic
implementation that exploits it (`RedbLogStore<P: LedgerPayload>`, etc.) lives in
`ledger`'s own `adapter/replication` crate, never in the port crate. Adding a fifth
backend means implementing `Validator` on its own config type and calling this same
function — not inventing a new validation approach. See `architecture.md`'s "Why `core`
exists" section for the full reasoning.

## The path + version Dependency Rule

`message-broker-svc-saf` depends on its sibling `spi` crates with **both** `path` and
`version` set — same reasoning as `message-broker-pattern`'s own developer guide (a
`path`-only dependency makes the crate unpublishable). The `message-broker-pattern`
dependency is pinned via `git`/`branch` for now, not `path` + `version`, since it lives
in a separate repo with no tag cut yet.

## Scope

See `architecture.md`'s Scope boundary section before adding anything here that isn't a
`MessageBroker` implementation — `TaskQueue`, `ApplicationConfig`/`BrokerProvider`, and
the real, `tokio::sync::broadcast`-backed in-memory backend were all deliberately left
out of this extraction. `NoopMessageBroker` (in `-saf`) is this repo's own no-op
reference implementation, not that real in-memory backend.

## See Also

- [Architecture](../3-design/architecture.md)
- [ADR-001](../3-design/adr/ADR-001-extract-from-edge-runtime-pilot.md)
