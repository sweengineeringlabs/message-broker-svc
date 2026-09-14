//! `message_broker_svc_nats_spi` — NATS implementation of
//! `message-broker-pattern`'s `MessageBroker` and `TaskQueue` traits.
//!
//! Consumed only by `message-broker-svc-saf`.

#![cfg_attr(test, allow(clippy::unwrap_used, clippy::expect_used))]

mod constants;
mod nats_config;
mod nats_message_broker;
mod nats_task_queue;

pub use nats_config::NatsConfig;
pub use nats_message_broker::NatsMessageBroker;
pub use nats_task_queue::NatsTaskQueue;
