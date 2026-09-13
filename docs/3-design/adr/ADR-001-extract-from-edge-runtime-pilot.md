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

## Amendment: 2026-09-13 -- add `message-broker-svc-core`, wire real construction-time validation

The previous amendment gave each `spi` crate its own `Validator`-implementing config
type but never wired that trait to anywhere that mattered: `NatsMessageBroker::connect`
had its own hand-rolled `url.trim().is_empty()` check instead of using `NatsConfig`'s own
`Validator` impl, and `KafkaMessageBroker::new`/`PostgresMessageBroker::connect`
validated nothing at all before connecting. `Validator` was implemented on all three
config types but only ever exercised through `configbuilder`'s TOML-loading path and
unit tests.

Checked this against `ledger`'s own crate split, at the user's direction: `LedgerPayload`
(the trait) lives in `ledger-base-port`, a pure port crate — but the generic, reusable
*implementation* that exploits it (`RedbLogStore<P: LedgerPayload>`, `GrpcNetwork<P>`,
the whole raft replication stack) lives in `ledger`'s own `adapter/replication` crate,
never inside `ledger-base-port` itself. `a2a-ledger`'s `TaskEvent` and `a2ac-ledger`'s
own payload type each bring their own shape and get that shared machinery for free.

**Decision**: added `message-broker-svc-core`, a new workspace member depending only on
`message-broker-pattern`, providing one generic function:
`validate_config<C: Validator>(config: &C) -> Result<(), BrokerError>`. Every `spi`
crate now depends on it and calls it once in its own constructor
(`NatsMessageBroker::connect`/`KafkaMessageBroker::new`/`PostgresMessageBroker::connect`),
replacing NATS's duplicate ad hoc check and adding real validation to Kafka/Postgres for
the first time. Nothing was removed: `MessageBroker::validator()`,
`message-broker-pattern`'s `Validator` trait, and `MessageBrokerFactory::validate<V>`
are all unchanged.

This mirrors `ledger`'s split exactly: `message-broker-pattern` stays `ledger-base-port`'s
equivalent — trait signatures only, zero implementation, zero generic algorithms, no
exceptions. `message-broker-svc-core` is the `adapter/replication` equivalent — the one
place a pattern trait bound gets a real, reusable implementation, so every `spi` crate
brings its own config shape and reuses the same validation behavior instead of each
inventing its own.

`application_type = "lib"` (not `"adapter"`): this crate implements no trait itself, it
calls one generically — `arch audit`'s `adapter_implements_port` rule confirmed
`"adapter"` was the wrong classification. Accepted, pre-existing-category `arch audit`
exceptions on this crate, matching the same categories already documented above:
`package_name_no_sea_suffix`/`boundary_no_antipattern_names` (the `-core` suffix, an
org-wide naming convention this `arch` version's rule predates), `examples_dir_lib`
(info-severity), `security_dependency_audit_configured` (root `deny.toml` exists, not
visible when auditing a nested crate path in isolation). `workspace_members_prefer_glob`
also fires on `scm/Cargo.toml`'s explicit `core`/`saf` entries alongside the `spi/*`
glob — confirmed structurally forced: `cargo metadata` errors if `main/message-broker/*`
is used instead, since `spi/` itself (the glob's parent) has no `Cargo.toml`; explicit
listing is the rule's own documented fallback for exactly this case.

Verified: `cargo test --workspace --all-targets --features nats,kafka,postgres` clean
(new tests: `message-broker-svc-core`'s own 2, plus one new blank-field regression test
per backend in `-saf`'s existing integration test files). `cargo fmt --check` and
`cargo clippy --workspace --all-targets --features nats,kafka,postgres -- -D warnings`
both clean. `arch audit .` from `scm/`: 505 passed, 7 failed across 5 members — all 7
matching an already-documented accepted category, none new defects.

## Amendment: 2026-09-13 -- comprehensive method-by-method audit, two more findings

At the user's request, a genuinely comprehensive pass: every method on every
`MessageBroker` implementor, checked against "is this forced to be concrete (a real
wire-protocol detail), or is it duplicated logic that only touches
`message-broker-pattern`'s own vocabulary?" -- with the added constraint that nothing
gets removed, only maximized toward the contract.

**Finding 1 -- `validator()` was duplicated, not just `validate_config`.** Every
implementor's `validator()` body -- `NatsMessageBroker`, `KafkaMessageBroker`,
`PostgresMessageBroker`, `NoopMessageBroker` -- was the identical one-liner
(`Arc::clone(&self.config) as Arc<dyn Validator>`, wrapped in `ValidatorResponse`).
Same shape as the earlier `validate_config` finding, just missed the first time.
Added `message-broker-svc-core::validator_response<C: Validator>(config: &Arc<C>) ->
ValidatorResponse`; every implementor's `validator()` is now one line calling it.
`publish`/`subscribe`/`health_check` were checked the same way and are genuinely
forced to be concrete -- each talks to a different wire protocol with no shared
algorithm to extract.

**Finding 2 -- the four `MessageBrokerFactory` constructors returned two different
types.** `noop()` returned `Box<dyn MessageBroker>`; `nats`/`kafka`/`postgres` returned
`impl MessageBroker`. `impl Trait` in return position is a distinct anonymous type per
function -- a caller picking a backend at runtime (`if cfg.backend == "kafka" { ... }
else { MessageBrokerFactory::noop() }`) could not unify three of these four
constructors into one variable without boxing them itself first. The whole point of a
factory returning `impl MessageBroker`/`Box<dyn MessageBroker>` is that the caller
never has to think about which concrete backend it got -- three call sites quietly
broke that. Fixed: all four now return `Box<dyn MessageBroker>` (the three fallible
ones as `Result<Box<dyn MessageBroker>, BrokerError>`). A real regression test
(`test_kafka_and_noop_constructors_return_the_same_boxed_broker_type`) puts both in one
`Vec<Box<dyn MessageBroker>>` -- before this fix, that literal would have failed to
*compile*, not just to pass.

**Consequences**: `message-broker-svc-saf` gained a dependency on
`message-broker-svc-core` (previously only the three `spi` crates depended on it) for
`NoopMessageBroker::validator()` to reuse `validator_response` too -- every
`MessageBroker` implementor in this repo, including the reference one, now goes
through the same shared helper, no exception.

Verified: `cargo test --workspace --all-targets --features nats,kafka,postgres` clean
(4 new tests: `core`'s own 2 for `validator_response`, plus the new type-unification
regression test in `-saf`). `cargo fmt --check` and `cargo clippy --workspace
--all-targets --features nats,kafka,postgres -- -D warnings` both clean. `arch audit .`
from `scm/`: 505 passed, 7 failed across 5 members -- same accepted category as before,
none new.

## Amendment: 2026-09-13 -- remove MessageBrokerFactory::create_config_builder and ::validate

Two more methods on `MessageBrokerFactory`, checked against the same question as the
previous amendment ("is this forced to be concrete, or duplicated/redundant logic?"),
turned out to be neither forced nor reused -- each was `-saf` inventing its own surface
with nothing behind it beyond what the interface it wrapped already exposed directly:

- **`validate<V: Validator>(v: &V) -> Result<(), ValidationError>`** -- its entire body
  was `v.validate(ValidationRequest)`, a pure passthrough. `Validator::validate` is
  already `pub` on the trait; any caller holding a `V: Validator` already had
  `.validate(ValidationRequest)` directly, with zero difference in behavior. Grep across
  this repo found exactly one caller of `MessageBrokerFactory::validate` -- its own test,
  using an ad hoc test double, never a real `NatsConfig`/`KafkaConfig`/`PostgresConfig`.
- **`create_config_builder() -> configbuilder::ConfigBuilderImpl`** -- returned
  `configbuilder::ConfigLoaderFactory::create_config_builder()` pre-seeded with this
  crate's own `CARGO_PKG_NAME`/`CARGO_PKG_VERSION`. Grep found exactly one caller in the
  whole repo -- its own test, which never depended on *which* name/version was seeded,
  only that the resulting builder worked. No `spi` crate's `OptionalSection` loading
  path went through it either -- each builds its own loader directly via
  `configbuilder::ConfigLoaderFactory::create_loader_for_dir(...)`, bypassing `-saf`
  entirely.

Both are the same "declare and abandon" shape the earlier `MessageBroker::validator()`
finding was: implemented, tested in isolation, never chained into a real flow. Removed
outright rather than kept as decoration -- a `-svc` repo must never carry its own ad hoc
methods that only restate a capability the interface (`Validator::validate`,
`configbuilder::ConfigLoaderFactory::create_config_builder()`) already exposes directly;
"`-svc` must not define new primitives, it must use what `-pattern`/the interface
already provides" applies as much to redundant convenience wrappers as it does to new
types.

**Consequences**: `message-broker-svc-saf`'s `Cargo.toml` drops its direct `configbuilder`
dependency entirely -- nothing in `-saf`'s own source references it any more (only the
three `spi` crates and `message-broker-svc-core` still depend on the pattern's
`Validator`/foreign `OptionalSection` machinery). `application_config_builder_int_test.rs`
deleted outright (tested only the removed method); the two `validate` tests in
`message_broker_factory_int_test.rs` removed the same way.

A caller that still wants a `-saf`-identity-seeded config builder gets it by calling
`configbuilder::ConfigLoaderFactory::create_config_builder()` directly and seeding it
with their own `env!("CARGO_PKG_NAME")`/`env!("CARGO_PKG_VERSION")` -- not `-saf`'s,
since nothing in this repo ever needed that specific identity. This is the one narrow,
acknowledged behavioral difference from the removed wrapper; it was never exercised by
any real code path in this repo (confirmed) or by any other repo on disk depending on
this crate (also confirmed -- this crate is not yet published, so no external consumer
exists to be affected either).

Verified: `cargo test --workspace --all-targets --features nats,kafka,postgres` clean.
`cargo fmt --check` and `cargo clippy --workspace --all-targets --features
nats,kafka,postgres -- -D warnings` both clean.

[← Docs index](../../README.md)
