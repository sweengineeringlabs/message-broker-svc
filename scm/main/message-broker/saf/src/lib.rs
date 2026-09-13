//! `message_broker_svc_saf` — message broker construction/dispatch factory.
//!
//! The `MessageBroker` contract (trait + value types) lives in
//! `message-broker-pattern-contract`; the config vocabulary
//! (`MessageBrokerConfig`) lives in `message-broker-pattern-core`; this crate
//! owns the construction/dispatch factory that selects among the
//! `message-broker-pattern-{nats,kafka,postgres}-spi` crates' backends.
//!
//! # Security note
//!
//! `NatsMessageBroker`/`KafkaMessageBroker`/`PostgresMessageBroker` are `pub`
//! within their own `message-broker-pattern-*-spi` crates (required for this
//! crate to construct them across the crate boundary), but this crate's own
//! `lib.rs` re-exports only [`MessageBrokerFactory`] — a consumer depending
//! on `message-broker-svc-saf` alone cannot name `KafkaMessageBroker` etc.
//! without also depending directly on the `spi` crate itself. Compile-time
//! visibility is the authoritative check here; no runtime assertion is
//! needed.

mod broker_factory;

pub use broker_factory::MessageBrokerFactory;
