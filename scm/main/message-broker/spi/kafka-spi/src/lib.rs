//! `message_broker_svc_kafka_spi` — Apache Kafka implementation of
//! `message-broker-pattern`'s `MessageBroker` and `TaskQueue` traits.
//!
//! Consumed only by `message-broker-svc-saf`.

#![cfg_attr(test, allow(clippy::unwrap_used, clippy::expect_used))]

mod constants;
mod kafka_config;
mod kafka_message_broker;
mod kafka_task_queue;
mod logging_consumer_context;

pub use kafka_config::KafkaConfig;
pub use kafka_message_broker::KafkaMessageBroker;
pub use kafka_task_queue::KafkaTaskQueue;
