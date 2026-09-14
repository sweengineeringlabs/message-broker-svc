//! `message_broker_svc_nats_spi` — NATS implementation of
//! `message-broker-pattern`'s `MessageBroker` trait.
//!
//! Consumed only by `message-broker-svc-saf`. `NatsTaskQueue` moved to
//! `task-queue-svc-nats-spi` (SRP — `TaskQueue`/`MessageBroker` are
//! separate domains).

#![cfg_attr(test, allow(clippy::unwrap_used, clippy::expect_used))]

mod nats_config;
mod nats_message_broker;

pub use nats_config::NatsConfig;
pub use nats_message_broker::NatsMessageBroker;
