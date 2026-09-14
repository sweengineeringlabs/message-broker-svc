# Architecture Compliance Checklist

**Audience**: Architects, contributors, reviewers.

Derived from [architecture.md](../architecture.md). Every rule here is enforceable —
re-run the listed command after any change and expect the stated result.

## 1. No shared config/backend-selector type

| # | Rule | Verify |
|---|------|--------|
| 1 | No crate-spanning "which backend" type anywhere — no enum, no shared config struct, no `from_config` dispatch | `grep -rn "BackendKind\|MessageBrokerConfig\|fn from_config" main/message-broker/*/src/**/*.rs main/message-broker/saf/src/*.rs` returns nothing |
| 2 | Each `spi` crate owns its own local config type, none shared | `grep -rln "^pub struct .*Config" main/message-broker/spi/*/src/*.rs` shows exactly one config struct per `spi` crate, no shared type imported across crates |

## 2. `core` holds only genuinely shared, generic logic

| # | Rule | Verify |
|---|------|--------|
| 3 | `message-broker-svc-core` exposes only `validate_config<C: Validator>`/`validator_response<C: Validator>` — no backend-specific logic | `grep -n "^pub fn" main/message-broker/core/src/*.rs` shows exactly these two functions |
| 4 | No `spi` crate hand-rolls its own version of `validate_config`/`validator_response` | `grep -rln "fn validate_config\|fn validator_response" main/message-broker/spi/*/src/*.rs` returns nothing (all call into `core` instead) |

## 3. Uniform constructor return types

| # | Rule | Verify |
|---|------|--------|
| 5 | Every `MessageBrokerFactory` constructor returns `Box<dyn MessageBroker>` — never a mix of `impl MessageBroker` and `Box<dyn MessageBroker>` | `grep -n "pub fn \(noop\|in_memory\|nats\|kafka\|postgres\)" main/message-broker/saf/src/broker_factory.rs` — every signature's return type is `Box<dyn MessageBroker>` (or `Result<Box<dyn MessageBroker>, BrokerError>`) |
| 6 | Every `TaskQueueFactory` constructor returns `Box<dyn TaskQueue>` — never a mix | `grep -n "pub fn \(in_memory\|nats\|kafka\)" main/message-broker/saf/src/task_queue_factory.rs` — every signature's return type is `Box<dyn TaskQueue>` (or `Result<Box<dyn TaskQueue>, QueueError>`) |
| 7 | Regression test exists proving constructors from different backends unify into one `Vec` | `cargo test -p message-broker-svc-saf --features inmemory,kafka test_kafka_and_noop_constructors_return_the_same_boxed_broker_type` and `cargo test -p message-broker-svc-saf --features inmemory,kafka test_kafka_and_in_memory_constructors_return_the_same_boxed_queue_type` both pass |

## 4. No concrete backend type leaks through `saf`

| # | Rule | Verify |
|---|------|--------|
| 8 | `saf`'s own `lib.rs` never re-exports a `spi` crate's concrete type (`NatsMessageBroker`, `KafkaTaskQueue`, etc.), nor its own `pub(crate)` no-op reference | `grep -n "^pub use" main/message-broker/saf/src/lib.rs` shows only `MessageBrokerFactory`/`TaskQueueFactory` — `NoopMessageBroker`/`NoopValidator` stay `pub(crate)`, reachable only via `MessageBrokerFactory::noop()`'s returned trait object |

## 5. Scope boundary

| # | Rule | Verify |
|---|------|--------|
| 9 | This repo has zero dependency on `edge-runtime` | `grep -rn "edge-runtime\|edge_runtime" main/message-broker/*/Cargo.toml main/message-broker/saf/Cargo.toml` returns nothing under `[dependencies]` (historical/doc-comment mentions of `edge-runtime` as prior art are expected and not violations) |
| 10 | `ApplicationConfig`/`BrokerProvider` (the `edge-runtime`-specific composition layer) is never ported here | `grep -rn "ApplicationConfig\|BrokerProvider" main/message-broker/*/src/**/*.rs main/message-broker/saf/src/*.rs` returns nothing |

## 6. Lint gates

| # | Rule | Verify |
|---|------|--------|
| 11 | `#![deny(unsafe_code)]` enforced across every crate | `cargo build --workspace` fails on any `unsafe` block |
| 12 | `#![warn(missing_docs)]` enforced across every crate | `cargo doc --workspace --no-deps` warns on any undocumented public item |
| 13 | `cargo clippy --workspace --all-targets --features inmemory,nats,kafka,postgres -- -D warnings` clean | Run before every commit |
| 14 | `cargo fmt --check` clean across every crate | Run before every commit |

[← 3-design index](../README.md)
