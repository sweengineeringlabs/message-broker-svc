//! `message_broker_pattern_postgres_spi` — Postgres (`pgmq`) implementation of
//! `message-broker-pattern-contract`'s `MessageBroker` trait.
//!
//! Consumed only by `message-broker-svc-saf`.

#![cfg_attr(test, allow(clippy::unwrap_used, clippy::expect_used))]

mod postgres_message_broker;

pub use postgres_message_broker::PostgresMessageBroker;
