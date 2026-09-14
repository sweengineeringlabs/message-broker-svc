//! `message_broker_svc_saf` — message broker/task queue construction/dispatch
//! factory.
//!
//! The `MessageBroker`/`TaskQueue` contracts (traits + value types) live in
//! `message-broker-pattern`; this crate owns the reference no-op
//! `MessageBroker` implementation (introduces no primitive beyond what the
//! contract already declares, so it lives here rather than in the contract
//! itself — see `message-broker-pattern`'s own ADR-001 amendment) and the
//! construction factories that select among it, `message-broker-svc-core`'s
//! in-memory backend (the technology-free reference implementation — not an
//! "spi", since it wraps no external technology), and the
//! `message-broker-svc-{nats,kafka,postgres}-spi` crates' backends.
//! [`TaskQueueFactory`] has no `postgres` constructor and no no-op reference
//! — neither exists for `TaskQueue` in this domain.
//!
//! # Security note
//!
//! `NatsMessageBroker`/`KafkaMessageBroker`/`PostgresMessageBroker`/
//! `NatsTaskQueue`/`KafkaTaskQueue` are `pub` within their own
//! `message-broker-svc-*-spi` crates, and `InMemoryMessageBroker`/
//! `InMemoryTaskQueue` are `pub` within `message-broker-svc-core` (required
//! for this crate to construct them across the crate boundary), but this
//! crate's own `lib.rs` re-exports only
//! [`MessageBrokerFactory`]/[`TaskQueueFactory`] — a consumer depending on
//! `message-broker-svc-saf` alone cannot name `KafkaMessageBroker` etc.
//! without also depending directly on the `core`/`spi` crate itself.
//! Compile-time visibility is the authoritative check here; no runtime
//! assertion is needed.

mod broker_factory;
mod noop_message_broker;
mod noop_validator;
mod task_queue_factory;

pub use broker_factory::MessageBrokerFactory;
pub use task_queue_factory::TaskQueueFactory;
