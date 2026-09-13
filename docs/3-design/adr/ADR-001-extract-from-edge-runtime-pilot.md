# ADR-001: Extract `spi`/`saf` from `edge-runtime`'s in-tree pilot, `MessageBroker`-only

**Status**: Accepted
**Date**: 2026-09-13
**Deciders**: Amu Hlongwane

## Context

`edge-message-broker#6` decided to split that repo's single-crate contract into a
`message-broker-pattern` repo (generic contract + reference core, mirroring
`wasm-capability-pattern`) and this repo, `message-broker-svc` (concrete NATS/Kafka/Postgres
backends, mirroring `wasm-capability-svc`).

`edge-runtime#96` (its own EPIC to migrate to a domain-first `{contract,core,spi,saf}`
layout) had already piloted exactly this shape for message-broker *in-tree*, at
`scm/main/message-broker/{contract,core,spi/{kafka,nats,postgres},saf}` (commits
`ec34244`, `16ec508`) — six crates: `runtime-message-broker-{contract,core,saf}` and
`runtime-message-broker-{kafka,nats,postgres}-spi`. That pilot's own `contract` crate
depends on `edge-message-broker` (`swe-edge-message-broker`, git tag `v0.3.8`) as a
superset, adding `TaskQueue`/`Task`/`TaskHandle` on top of this org's `MessageBroker`
contract, and was never tagged/published — `ec34244`'s own commit body says so
explicitly.

## Decision

Extract the `MessageBroker`-only portion of that pilot into this repo, rather than
rebuild from scratch:

- `runtime-message-broker-{nats,kafka,postgres}-spi` → this repo's
  `message-broker-svc-{nats,kafka,postgres}-spi`, each renamed to depend on
  `message-broker-pattern-contract`/`-core` (the plain `MessageBroker` contract) instead
  of `runtime-message-broker-contract` (the `TaskQueue`-superset one).
- `runtime-message-broker-saf` → this repo's `message-broker-svc-saf`, trimmed to
  `MessageBrokerFactory`'s `MessageBroker` dispatch only.

Each provider crate's source split cleanly along an existing seam: `kafka_message_broker.rs`/
`nats_message_broker.rs` (`MessageBroker` impl) were already separate files from
`kafka_task_queue.rs`/`nats_task_queue.rs` (`TaskQueue` impl), with no shared state between
them (only `kafka`'s `constants.rs` had one `TaskQueue`-only constant mixed in, dropped
during extraction). Postgres never had a `TaskQueue` implementation to split out.

**Not ported, and deliberately so** (see `architecture.md`'s Scope boundary section for
the reasoning): `TaskQueue`, `ApplicationConfig`/`BrokerProvider`, and the real
`tokio::sync::broadcast`-backed in-memory broker (`runtime-message-broker-core`).

## Consequences

- `edge-runtime#96`'s own message-broker pilot scope needs updating to point at this
  repo's published crates instead of keeping a second, divergent in-tree copy — tracked
  via `edge-message-broker#6`'s E3, not resolved by this ADR.
- Interim dependency on `message-broker-pattern-contract`/`-core` is via git (`branch =
  "dev"`), not a crates.io version or git tag — neither has been cut yet. Follow-up: pin
  to a tag once `message-broker-pattern` cuts one, and again once/if these crates publish
  to crates.io.
- A consumer that needs `TaskQueue` semantics still depends on `edge-runtime`'s own
  crates for that; this repo does not attempt to serve that need.

## Related patterns

- **Strangler extraction** — lifting a bounded slice of a larger, in-tree system into its
  own repo without disturbing what's left behind, rather than a big-bang rewrite.

## Amendment: 2026-09-13 -- remove BackendKind/MessageBrokerConfig, no from_config, Noop moves here

The initial extraction carried over the original pilot's `BackendKind`
(`InMemory`/`Nats`/`Kafka`/`Postgres`) and `MessageBrokerConfig` (a single struct with
every backend's fields unioned together: `backend`, `url`, `group_id`, `queue_name`),
and a `MessageBrokerFactory::from_config` that matched on `BackendKind` to dispatch
across whichever backends were feature-compiled in. Two defects, caught in review before
either shipped to a tag:

1. **`BackendKind`'s variants name specific technologies directly** — the same defect
   `wasm-capability-pattern`'s own `CapabilityProtocol` enum had before its own ADR-001
   amendment deleted it outright. No contract or shared type should ever enumerate
   technology names; `runtime-svc-registry`'s `RuntimePhase` enum is the legitimate
   alternative shape (`Configured`/`Starting`/`Running`/...) — abstract lifecycle
   *states*, never implementation names.
2. **`from_config` matching on a "which backend" value to construct one of several
   compiled-in implementations *is* a registry** — precisely the job
   `runtime-svc-registry` (built on `svc-registry-pattern`'s `Named`/`Registry<T>`)
   already exists to do generically. This repo represents exactly one implementation at
   a time per build; `from_config` made it look like a registry of interchangeable named
   backends instead.

**Decision**:
- Deleted `BackendKind`, `MessageBrokerConfig`, and `MessageBrokerFactory::from_config`
  outright — not genericized into a `String`-keyed type + `HashMap` options bag. That
  alternative was considered and rejected on zero-cost-abstraction grounds: the set of
  backends is closed and known by whoever builds this repo (unlike `Registry<T: Named>`'s
  genuinely open-ended `T`), so a real `enum` (compiler-checked exhaustiveness, no
  runtime string match) is strictly better than a generic stand-in this domain will
  never need the flexibility of.
- Each `spi` crate now defines its **own** local config type (`NatsConfig`, `KafkaConfig`,
  `PostgresConfig`), independently implementing both `configbuilder::OptionalSection`
  (real TOML-section loading) and `message-broker-pattern`'s own `Validator` — see
  `architecture.md`'s own section on this, and `edge-llm`'s `provider/contract` doc
  comment for the precedent (a contract type never implements a foreign trait like
  `OptionalSection` itself; a downstream adapter crate defines its own local type
  instead). No shared base type, no crate-spanning config concept anywhere in this repo.
- `MessageBrokerFactory` keeps `nats`/`kafka`/`postgres` exactly as they were (direct,
  independently-typed, feature-gated constructors — these were never the problem) and
  gains a fourth, `noop`, alongside them.
- `NoopMessageBroker`/`NoopValidator` moved here, into `-saf`, from
  `message-broker-pattern` (where an earlier, incorrect version of this same amendment
  had them living as the pattern's "own" reference implementation, mirroring
  `wasm-capability-pattern`'s `core`). Corrected: `Noop` is still a concrete
  implementation of `MessageBroker`/`Validator` — the pattern has zero implementations
  of its own traits, no exceptions, including trivial zero-dependency ones. `Noop`
  stands on equal footing with `nats`/`kafka`/`postgres` as one more implementation
  choice, not a pattern-owned special case.

**Consequences**:
- `message-broker-pattern`'s own dependency on this repo's design is unchanged — it
  still only supplies `MessageBroker`/`Validator`/`Message`/etc., now provably never
  referenced by any backend-selection concept anywhere.
- Every `spi` crate's `Cargo.toml` gained `configbuilder` + `serde` as direct
  dependencies (for its own `OptionalSection`/`Deserialize` impls) and lost nothing —
  they never depended on a shared config crate to begin with once
  `message-broker-pattern-core` was removed from existence.
- Test coverage for config loading/validation moved from one shared
  `message_broker_config_int_test.rs` (in `-saf`, testing the deleted shared struct) to
  three per-crate files (`nats_config_int_test.rs`/`kafka_config_int_test.rs`/
  `postgres_config_int_test.rs`, one per `spi` crate), each testing only its own
  backend's fields.

Verified: `cargo build/test --workspace` clean (all three feature flags together, 62
real assertions + 5 correctly `#[ignore]`d live-infra tests), `cargo fmt --check` and
`cargo clippy --workspace --all-targets --features nats,kafka,postgres -- -D warnings`
both clean, `grep`-confirmed zero remaining references to `BackendKind`/
`MessageBrokerConfig`/`from_config` anywhere in the repo.

[← Docs index](../../README.md)
