//! `message_broker_svc_inmemory_spi` — in-process implementation of
//! `message-broker-pattern`'s `MessageBroker` trait, backed by
//! `tokio::sync::broadcast`.
//!
//! Consumed only by `message-broker-svc-saf`.

#![cfg_attr(test, allow(clippy::unwrap_used, clippy::expect_used))]

mod inmemory_config;
mod inmemory_message_broker;

pub use inmemory_config::InMemoryConfig;
pub use inmemory_message_broker::InMemoryMessageBroker;
