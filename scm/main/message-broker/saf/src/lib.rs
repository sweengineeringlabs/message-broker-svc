//! `message_broker_svc_saf` — message broker construction/dispatch factory.
//!
//! The `MessageBroker` contract (trait + value types) lives in
//! `message-broker-pattern`; this crate owns the reference no-op
//! implementation (introduces no primitive beyond what the contract already
//! declares, so it lives here rather than in the contract itself — see
//! `message-broker-pattern`'s own ADR-001 amendment) and the
//! construction/dispatch factory that selects among it and the
//! `message-broker-svc-{nats,kafka,postgres}-spi` crates' backends.
//!
//! # Security note
//!
//! `NatsMessageBroker`/`KafkaMessageBroker`/`PostgresMessageBroker` are `pub`
//! within their own `message-broker-svc-*-spi` crates (required for this
//! crate to construct them across the crate boundary), but this crate's own
//! `lib.rs` re-exports only [`MessageBrokerFactory`] — a consumer depending
//! on `message-broker-svc-saf` alone cannot name `KafkaMessageBroker` etc.
//! without also depending directly on the `spi` crate itself. Compile-time
//! visibility is the authoritative check here; no runtime assertion is
//! needed.

mod broker_factory;
mod noop_message_broker;
mod noop_validator;

pub use broker_factory::MessageBrokerFactory;
