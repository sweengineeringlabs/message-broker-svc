//! `message_broker_svc_core` — the technology-free reference implementation
//! of `message-broker-pattern`'s `MessageBroker` trait.
//!
//! [`InMemoryMessageBroker`] is backed by `tokio::sync::broadcast` only --
//! no external service, no network connection, zero external-technology
//! dependency. Per this org's own convention (`runtime-resource-limit-core`,
//! and `edge-runtime`'s original `runtime-message-broker-core`), `core`
//! names exactly this shape: the pure, in-process reference impl a domain's
//! traits get for free. There is no such thing as an "in-memory SPI" -- SPI
//! names a crate that wraps one *external* technology (NATS, Kafka,
//! Postgres), and in-memory wraps nothing external, so it belongs here, not
//! under `spi/`.
//!
//! `InMemoryTaskQueue` moved to `task-queue-svc-core` — `TaskQueue`
//! (competing-consumer) and `MessageBroker` (fan-out/broadcast) are
//! separate domains, split for SRP; see
//! `message-broker-pattern`'s own ADR-002 and
//! `../template-engine`'s `pattern_svc_workflow.md`.

#![cfg_attr(test, allow(clippy::unwrap_used, clippy::expect_used))]

mod inmemory_config;
mod inmemory_message_broker;

pub use inmemory_config::InMemoryConfig;
pub use inmemory_message_broker::InMemoryMessageBroker;
