# Architecture Compliance Checklist

**Audience**: Architects, contributors, reviewers.

Derived from [architecture.md](../architecture.md). Every rule here is enforceable —
re-run the listed command after any change and expect the stated result.

## 1. No shared config/backend-selector type

| # | Rule | Verify |
|---|------|--------|
| 1 | No crate-spanning "which backend" type anywhere — no enum, no shared config struct, no `from_config` dispatch | `grep -rn "BackendKind\|MessageBrokerConfig\|fn from_config" main/message-broker/*/src/**/*.rs main/message-broker/saf/src/*.rs` returns nothing |
| 2 | Each `spi` crate owns its own local config type, none shared | `grep -rln "^pub struct .*Config" main/message-broker/spi/*/src/*.rs` shows exactly one config struct per `spi` crate, no shared type imported across crates |

## 2. `core` is the technology-free reference implementation, not an "spi"

| # | Rule | Verify |
|---|------|--------|
| 3 | `message-broker-svc-core` names no backend technology and has no external-technology dependency | `grep -nE "^\s*(pub )?(struct\|enum\|fn) \w*(Kafka\|Nats\|Postgres\|Redis)" main/message-broker/core/src/*.rs` returns nothing; `main/message-broker/core/Cargo.toml`'s `[dependencies]` lists only `message-broker-pattern`, `configbuilder`, `serde`, `futures`, `tokio` (no `rdkafka`/`async-nats`/`sqlx`) |
| 4 | No crate under `spi/` is named for a backend that wraps nothing external (there is no "in-memory spi") | `ls main/message-broker/spi/` lists only `nats-spi`, `kafka-spi`, `postgres-spi` — no `inmemory-spi`, no `shared` |

## 3. No hand-rolled validate_config/validator_response — they live on Validator itself

| # | Rule | Verify |
|---|------|--------|
| 5 | No `spi`/`core`/`saf` crate hand-rolls, re-declares, or wraps its own version of `validate_config`/`validator_response` | `grep -rln "fn validate_config\|fn validator_response" main/message-broker/spi/nats-spi/src/*.rs main/message-broker/spi/kafka-spi/src/*.rs main/message-broker/spi/postgres-spi/src/*.rs main/message-broker/core/src/*.rs main/message-broker/saf/src/*.rs` returns nothing (every backend calls `message-broker-pattern`'s `Validator::validate_config`/`validator_response` default methods directly) |
| 6 | There is no `spi/shared` crate — it was deleted, not deprecated, once its content moved to `message-broker-pattern` | `ls main/message-broker/spi/` returns no `shared` entry; `grep -rln "spi-shared\|spi_shared" --include="*.toml" --include="*.rs" main/message-broker/` returns nothing |

## 4. Uniform constructor return types

| # | Rule | Verify |
|---|------|--------|
| 7 | Every `MessageBrokerFactory` constructor returns `Box<dyn MessageBroker>` — never a mix of `impl MessageBroker` and `Box<dyn MessageBroker>` | `grep -n "pub fn \(noop\|in_memory\|nats\|kafka\|postgres\)" main/message-broker/saf/src/broker_factory.rs` — every signature's return type is `Box<dyn MessageBroker>` (or `Result<Box<dyn MessageBroker>, BrokerError>`) |
| 8 | Regression test exists proving constructors from different backends unify into one `Vec` | `cargo test -p message-broker-svc-saf --features inmemory,kafka test_kafka_and_noop_constructors_return_the_same_boxed_broker_type` passes |

## 5. No concrete backend type leaks through `saf`

| # | Rule | Verify |
|---|------|--------|
| 9 | `saf`'s own `lib.rs` never re-exports a `spi`/`core` crate's concrete type (`NatsMessageBroker`, `InMemoryMessageBroker`, etc.), nor its own `pub(crate)` no-op reference | `grep -n "^pub use" main/message-broker/saf/src/lib.rs` shows only `MessageBrokerFactory` — `NoopMessageBroker`/`NoopValidator` stay `pub(crate)`, reachable only via `MessageBrokerFactory::noop()`'s returned trait object |
| 10 | No `TaskQueue`-related type lives in this repo anymore (moved to `task-queue-svc`, SRP) | `grep -rln "TaskQueue" main/message-broker/*/src/**/*.rs main/message-broker/saf/src/*.rs` returns nothing outside doc-comment prose pointing to `task-queue-svc` |

## 6. Scope boundary

| # | Rule | Verify |
|---|------|--------|
| 11 | This repo has zero dependency on `edge-runtime` | `grep -rn "edge-runtime\|edge_runtime" main/message-broker/*/Cargo.toml main/message-broker/saf/Cargo.toml` returns nothing under `[dependencies]` (historical/doc-comment mentions of `edge-runtime` as prior art are expected and not violations) |
| 12 | `ApplicationConfig`/`BrokerProvider` (the `edge-runtime`-specific composition layer) is never ported here | `grep -rn "ApplicationConfig\|BrokerProvider" main/message-broker/*/src/**/*.rs main/message-broker/saf/src/*.rs` returns nothing |

## 7. Lint gates

| # | Rule | Verify |
|---|------|--------|
| 13 | `#![deny(unsafe_code)]` enforced across every crate | `cargo build --workspace` fails on any `unsafe` block |
| 14 | `#![warn(missing_docs)]` enforced across every crate | `cargo doc --workspace --no-deps` warns on any undocumented public item |
| 15 | `cargo clippy --workspace --all-targets --features inmemory,nats,kafka,postgres -- -D warnings` clean | Run before every commit |
| 16 | `cargo fmt --check` clean across every crate | Run before every commit |

[← 3-design index](../README.md)
