//! `message_broker_svc_saf` — message broker construction/dispatch facade.
//!
//! The `MessageBroker` contract (trait + value types) lives in
//! `message-broker-pattern`; this crate owns the reference no-op
//! `MessageBroker` implementation (introduces no primitive beyond what the
//! contract already declares, so it lives here rather than in the contract
//! itself — see `message-broker-pattern`'s own ADR-001 amendment) and the
//! construction factory that selects among it, `message-broker-svc-core`'s
//! in-memory backend (the technology-free reference implementation — not an
//! "spi", since it wraps no external technology), and the
//! `message-broker-svc-{nats,kafka,postgres}-spi` crates' backends.
//!
//! `TaskQueueFactory` moved to `task-queue-svc-saf` — `TaskQueue`
//! (competing-consumer) and `MessageBroker` (fan-out/broadcast) are
//! separate domains, split for SRP; see `message-broker-pattern`'s own
//! ADR-002.
//!
//! # Security note
//!
//! `NatsMessageBroker`/`KafkaMessageBroker`/`PostgresMessageBroker` are
//! `pub` within their own `message-broker-svc-*-spi` crates, and
//! `InMemoryMessageBroker` is `pub` within `message-broker-svc-core`
//! (required for this crate to construct them across the crate boundary),
//! but this crate's own `lib.rs` re-exports only
//! [`MessageBrokerFactory`] — a consumer depending on
//! `message-broker-svc-saf` alone cannot name `KafkaMessageBroker` etc.
//! without also depending directly on the `core`/`spi` crate itself.
//! Compile-time visibility is the authoritative check here; no runtime
//! assertion is needed.

mod broker_factory;
mod noop_message_broker;
mod noop_validator;

pub use broker_factory::MessageBrokerFactory;
