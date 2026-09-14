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
        ├── core/              # message-broker-svc-core -- InMemoryMessageBroker (technology-free, not an spi)
        ├── spi/
        │   ├── nats-spi/        # message-broker-svc-nats-spi -- *MessageBroker
        │   ├── kafka-spi/       # message-broker-svc-kafka-spi -- *MessageBroker
        │   └── postgres-spi/    # message-broker-svc-postgres-spi -- *MessageBroker
        └── saf/              # message-broker-svc-saf -- MessageBrokerFactory
```

No `spi/shared` crate — `Validator::validate_config`/`validator_response` live directly
on `message-broker-pattern`'s own `Validator` trait as default methods; see
architecture.md's "Why `validate_config`/`validator_response` live on `Validator`
itself, not a `spi/shared` crate". No `TaskQueue` anywhere in this repo either — moved
to [`task-queue-svc`](https://github.com/sweengineeringlabs/task-queue-svc) (SRP); see
architecture.md's "Why `TaskQueue` implementations moved out (SRP)".

## Branching and Releases

- `dev` is the default branch; all work lands there first.
- `main` gets fast-forwarded to `dev` after a shipped change, not on every commit.
- Pre-1.0 SemVer: a breaking change bumps the minor version.
- Five crates in-repo: `message-broker-svc-core` (v0.3.0 — the in-memory reference
  implementation, not an "spi"; see architecture.md), `-nats-spi` (v0.2.0),
  `-kafka-spi` (v0.2.0), `-postgres-spi` (v0.1.4), `-saf` (v0.3.0). There is no
  `message-broker-svc-spi-shared` (deleted, see previous entries in this history)
  and no `TaskQueue` anywhere in this repo (moved to `task-queue-svc`, SRP — see
  [message-broker-pattern](https://github.com/sweengineeringlabs/message-broker-pattern)'s
  own ADR-002 and this repo's architecture.md). Pre-1.0 SemVer: a
  breaking change bumps the minor version; internal dependency-swap-only changes (no
  public API change) bump the patch — losing a `TaskQueue` export is a breaking
  change, hence the minor bumps above. Each tagged in this repo's own git history to
  match (`core/v0.3.0`, `nats-spi/v0.2.0`, etc., matching `wasm-capability-pattern`'s
  own per-crate tag convention). Depends on
  [`message-broker-pattern`](https://github.com/sweengineeringlabs/message-broker-pattern)
  by `git`+`tag` (`v0.2.0`) — that repo's own crates.io publish is stale (v0.1.2,
  predates `Validator::validate_config`/`validator_response` and the `TaskQueue`
  removal); switch back to a version requirement once it publishes v0.2.0.

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

Enforced, not a style preference: `InMemoryMessageBroker`/`NatsMessageBroker`/
`KafkaMessageBroker`/`PostgresMessageBroker` are `pub` within their own `spi` crates
(required for `message-broker-svc-saf` to construct them across the crate boundary),
but `saf`'s own `lib.rs` re-exports only `MessageBrokerFactory` — a consumer depending
on `message-broker-svc-saf` alone cannot name a concrete backend type without also
depending directly on that `spi` crate itself. (`InMemoryTaskQueue`/`NatsTaskQueue`/
`KafkaTaskQueue` used to be on this list — moved to `task-queue-svc`, SRP.)

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
`path`-only dependency makes the crate unpublishable); this rule is unchanged and
applies only to intra-repo, same-workspace dependencies.

The `message-broker-pattern` dependency itself is different: every crate in this repo
now depends on it by `git`+`tag` (`tag = "v0.2.0"`), not by version requirement. This is
this org's own standing convention for a cross-repo dependency whose target hasn't
published the needed version to crates.io yet — `message-broker-pattern`'s own
crates.io publish is stale (v0.1.2, predates both the `Validator::validate_config`/
`validator_response` move and the `TaskQueue` removal). Switch each of these back to a
plain version requirement once `message-broker-pattern` publishes v0.2.0 to crates.io.

## Scope

See `architecture.md`'s Scope boundary section for the current, up-to-date picture.
One thing was initially left out of this extraction on the mistaken belief that
nothing depended on it, and was later corrected once that belief was checked
against `edge-runtime`'s actual source: the real, `tokio::sync::broadcast`-backed
in-memory `MessageBroker` (now `message-broker-svc-core`; see architecture.md's
"Restoring the real in-memory backend"). `ApplicationConfig`/`BrokerProvider` (the
`BackendKind`-driven `from_config` dispatch mechanism itself) remains the one thing
genuinely, permanently out of scope — it's `edge-runtime`-specific composition, not a
migration gap. `NoopMessageBroker` (in `-saf`) remains this repo's own no-op reference
implementation, distinct from the real in-memory backend.

**Amendment (SRP):** `TaskQueue` itself, for every backend that had one, was also
briefly in scope for this repo (`message-broker-svc-core`'s `InMemoryTaskQueue` plus
`message-broker-svc-{nats,kafka}-spi`'s `*TaskQueue` types and `TaskQueueFactory`) but
has since moved out entirely, to its own repo pair
([`task-queue-pattern`](https://github.com/sweengineeringlabs/task-queue-pattern)/
[`task-queue-svc`](https://github.com/sweengineeringlabs/task-queue-svc)) — `MessageBroker`
(fan-out/broadcast) and `TaskQueue` (competing-consumer) are different responsibilities
that happened to share an origin repo, not one responsibility; see architecture.md's
"Why `TaskQueue` implementations moved out (SRP)". `TaskQueue` is no longer in scope
for this repo at all, in any form.

## See Also

- [Architecture](../3-design/architecture.md)
- [ADR-001](../3-design/adr/ADR-001-extract-from-edge-runtime-pilot.md)
