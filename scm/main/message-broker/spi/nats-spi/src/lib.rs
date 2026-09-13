//! `message_broker_pattern_nats_spi` — NATS implementation of
//! `message-broker-pattern-contract`'s `MessageBroker` trait.
//!
//! Consumed only by `message-broker-svc-saf`.

#![cfg_attr(test, allow(clippy::unwrap_used, clippy::expect_used))]

mod nats_message_broker;

pub use nats_message_broker::NatsMessageBroker;
