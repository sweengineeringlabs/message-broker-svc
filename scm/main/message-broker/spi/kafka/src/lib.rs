//! `message_broker_pattern_kafka_spi` — Apache Kafka implementation of
//! `message-broker-pattern-contract`'s `MessageBroker` trait.
//!
//! Consumed only by `message-broker-svc-saf`.

#![cfg_attr(test, allow(clippy::unwrap_used, clippy::expect_used))]

mod constants;
mod kafka_message_broker;

pub use kafka_message_broker::KafkaMessageBroker;
