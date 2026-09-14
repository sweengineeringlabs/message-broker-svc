# message-broker-svc Developer Guide

**Audience**: Developers, contributors.

## Repo Structure

```
message-broker-svc/
├── README.md
├── docs/
│   ├── README.md                                 # docs section index
│   ├── glossary.md                                # domain terminology
│   ├── 0-ideation/papers/README.md
│   ├── 3-design/README.md, architecture.md
│   ├── 3-design/compliance/compliance_checklist.md
│   ├── 3-design/adr/README.md, ADR-001-extract-from-edge-runtime-pilot.md
│   └── 4-development/README.md, developer_guide.md   # this file
└── scm/
    ├── Cargo.toml          # workspace: [core, spi/*, saf]
    └── main/message-broker/
        ├── core/              # message-broker-svc-core -- InMemoryMessageBroker + InMemoryTaskQueue (technology-free, not an spi)
        ├── spi/
        │   ├── nats-spi/        # message-broker-svc-nats-spi -- *MessageBroker + *TaskQueue
        │   ├── kafka-spi/       # message-broker-svc-kafka-spi -- *MessageBroker + *TaskQueue
        │   └── postgres-spi/    # message-broker-svc-postgres-spi -- *MessageBroker only
        └── saf/              # message-broker-svc-saf -- MessageBrokerFactory, TaskQueueFactory
```

No `spi/shared` crate — `Validator::validate_config`/`validator_response` live directly
on `message-broker-pattern`'s own `Validator` trait as default methods; see
architecture.md's "Why `validate_config`/`validator_response` live on `Validator`
itself, not a `spi/shared` crate".

## Branching and Releases

- `dev` is the default branch; all work lands there first.
- `main` gets fast-forwarded to `dev` after a shipped change, not on every commit.
- Pre-1.0 SemVer: a breaking change bumps the minor version.
- Five crates in-repo: `message-broker-svc-core` (v0.2.1 — the in-memory reference
  implementation, not an "spi"; see architecture.md), `-nats-spi` (v0.1.4),
  `-kafka-spi` (v0.1.4), `-postgres-spi` (v0.1.3), `-saf` (v0.2.2). There is no
  `message-broker-svc-spi-shared` — it existed briefly (v0.1.0) between
  [message-broker-svc#2](https://github.com/sweengineeringlabs/message-broker-svc/issues/2)
  and [message-broker-svc#3](https://github.com/sweengineeringlabs/message-broker-svc/issues/3),
  then was deleted once `validate_config`/`validator_response` moved directly onto
  `message-broker-pattern`'s own `Validator` trait — do not depend on it, it is
  orphaned on crates.io (no corresponding source in this repo). Pre-1.0 SemVer: a
  breaking change bumps the minor version; internal dependency-swap-only changes (no
  public API change) bump the patch. Each tagged in this repo's own git history to
  match (`core/v0.2.1`, `nats-spi/v0.1.4`, etc., matching `wasm-capability-pattern`'s
  own per-crate tag convention). Depends on
  [`message-broker-pattern`](https://crates.io/crates/message-broker-pattern) by
  version (`"0.1.3"` — needed for `Validator::validate_config`/`validator_response` —
  see "The path + version Dependency Rule" below), not `git`.

## Working on Any Crate

All five crates are members of `scm/Cargo.toml`, so from `scm/`:

```
cargo test --workspace --all-targets
cargo clippy --workspace --all-targets -- -D warnings
cargo fmt --check
```

`MessageBrokerFactory::noop()` is always available, no feature required (`TaskQueueFactory`
has no no-op equivalent). Each real backend's feature is off by default in
`message-broker-svc-saf` — enable what you're working on. Each feature gates both
`MessageBrokerFactory`'s and (where one exists) `TaskQueueFactory`'s constructor for
that backend together — there is no way to enable one without the other:

```
cargo test --manifest-path main/message-broker/saf/Cargo.toml --features inmemory
cargo test --manifest-path main/message-broker/saf/Cargo.toml --features nats
cargo test --manifest-path main/message-broker/saf/Cargo.toml --features kafka
cargo test --manifest-path main/message-broker/saf/Cargo.toml --features postgres
cargo test --manifest-path main/message-broker/saf/Cargo.toml --features inmemory,nats,kafka,postgres
```

`inmemory` needs no external service or live-infra opt-in — unlike the other three
backends, its tests exercise real publish/subscribe (and enqueue/dequeue) delivery
directly.

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

Enforced, not a style preference: `InMemoryMessageBroker`/`InMemoryTaskQueue`/
`NatsMessageBroker`/`NatsTaskQueue`/`KafkaMessageBroker`/`KafkaTaskQueue`/
`PostgresMessageBroker` are `pub` within their own `spi` crates (required for
`message-broker-svc-saf` to construct them across the crate boundary), but `saf`'s own
`lib.rs` re-exports only `MessageBrokerFactory`/`TaskQueueFactory` — a consumer
depending on `message-broker-svc-saf` alone cannot name a concrete backend type
without also depending directly on that `spi` crate itself.

## Config: Each spi Crate Owns Its Own, None Shared

There is no `BackendKind` enum, no shared `MessageBrokerConfig` struct, and no
`from_config` dispatch anywhere in this repo — see `architecture.md`'s own section on
this and ADR-001's amendment for the full reasoning. Each backend defines its own
local config type (`InMemoryConfig` in `core`, `NatsConfig`/`KafkaConfig`/`PostgresConfig`
each in their own `spi` crate), implementing both `configbuilder::OptionalSection` and
`message-broker-pattern::Validator` independently.
Adding a new backend means adding a new `spi` crate with its own config type and a new
`MessageBrokerFactory` constructor — never touching a shared enum or struct, because
there isn't one.

## Validator::validate_config/validator_response: the One Shared, Generic Implementation

Every backend's constructor calls `config.validate_config()` — from
`message_broker_pattern::Validator` itself, the same trait every config type already
implements. No `spi/shared` crate, no extension trait: `validate_config`/
`validator_response` are default methods declared directly on `Validator` in
`message-broker-pattern`, reused identically by all four backends instead of each
hand-rolling its own check. This lived here first, as `message-broker-svc-spi-shared`'s
`ValidatorExt` extension trait; moved into `message-broker-pattern` itself once checked
against that crate's own precedent for what "zero implementation" actually rules out
(implementing a primary trait for a concrete type, not declaring a default method on a
trait). See `architecture.md`'s "Why `validate_config`/`validator_response` live on
`Validator` itself, not a `spi/shared` crate" section for the full reasoning. Adding a
fifth backend means implementing `Validator` on its own config type —
`validate_config`/`validator_response` come for free through the trait, not a new
function or extension trait to reach for.

## The path + version Dependency Rule

`message-broker-svc-saf` depends on its sibling `spi` crates with **both** `path` and
`version` set — same reasoning as `message-broker-pattern`'s own developer guide (a
`path`-only dependency makes the crate unpublishable). The `message-broker-pattern`
dependency is now pinned the same way, by plain version requirement — it was
`git`/`branch`-pinned only until that repo cut its first published tag. Every crate's
declared floor was bumped to `"0.1.3"` alongside the `Validator::validate_config`/
`validator_response` move (needed — that method set doesn't exist before 0.1.3), so the
floor is uniform across every crate in this repo as of that change.

## Scope

See `architecture.md`'s Scope boundary section for the current, up-to-date picture.
Two things were each initially left out of this extraction on the mistaken belief that
nothing depended on them, and both were later corrected once that belief was checked
against `edge-runtime`'s actual source: the real, `tokio::sync::broadcast`-backed
in-memory `MessageBroker` (now `message-broker-svc-core`; see architecture.md's
"Restoring the real in-memory backend") and `TaskQueue` itself, for every backend that
has one (now `message-broker-svc-core`'s `InMemoryTaskQueue` plus
`message-broker-svc-{nats,kafka}-spi`'s `*TaskQueue` types and `TaskQueueFactory`; see
architecture.md's "Scope boundary" `Update (TaskQueue)` note).
`ApplicationConfig`/`BrokerProvider` (the `BackendKind`-driven `from_config` dispatch
mechanism itself) remains the one thing genuinely, permanently out of scope — it's
`edge-runtime`-specific composition, not a migration gap. `NoopMessageBroker` (in
`-saf`) remains this repo's own no-op reference implementation, distinct from the real
in-memory backend; there is no no-op `TaskQueue` equivalent.

## See Also

- [Architecture](../3-design/architecture.md)
- [ADR-001](../3-design/adr/ADR-001-extract-from-edge-runtime-pilot.md)
