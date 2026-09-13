//! `message_broker_svc_kafka_spi` — Apache Kafka implementation of
//! `message-broker-pattern`'s `MessageBroker` trait.
//!
//! Consumed only by `message-broker-svc-saf`.

#![cfg_attr(test, allow(clippy::unwrap_used, clippy::expect_used))]

mod constants;
mod kafka_config;
mod kafka_message_broker;

pub use kafka_config::KafkaConfig;
pub use kafka_message_broker::KafkaMessageBroker;
