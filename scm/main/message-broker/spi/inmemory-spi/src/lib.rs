//! `message_broker_svc_inmemory_spi` — in-process implementation of
//! `message-broker-pattern`'s `MessageBroker` trait (backed by
//! `tokio::sync::broadcast`) and `TaskQueue` trait (backed by
//! `tokio::sync::mpsc`).
//!
//! Consumed only by `message-broker-svc-saf`.

#![cfg_attr(test, allow(clippy::unwrap_used, clippy::expect_used))]

mod inmemory_config;
mod inmemory_message_broker;
mod inmemory_task_queue;

pub use inmemory_config::InMemoryConfig;
pub use inmemory_message_broker::InMemoryMessageBroker;
pub use inmemory_task_queue::InMemoryTaskQueue;
