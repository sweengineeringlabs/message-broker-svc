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
//! # Zero-cost, uniform return type
//!
//! `MessageBroker` is not object-safe (its methods return `impl Future`,
//! not a boxed one — see `message-broker-pattern`'s own architecture doc).
//! [`MessageBrokerFactory`]'s five constructors still need one uniform
//! return type — a real, config-driven runtime backend-selection need this
//! repo has always had (unlike single-backend `-svc` repos, which just
//! return `impl MessageBroker` directly). [`AnyMessageBroker`] is that type:
//! a plain enum, one variant per backend, implementing `MessageBroker` by
//! matching on `self` and delegating. Zero-cost — no heap allocation, no
//! vtable, a `match` compiles to a jump table — see
//! `docs/3-design/architecture.md`'s "Why `AnyMessageBroker`, not
//! `Box<dyn MessageBroker>`" section.
//!
//! # Security note
//!
//! `NatsMessageBroker`/`KafkaMessageBroker`/`PostgresMessageBroker` are
//! `pub` within their own `message-broker-svc-*-spi` crates, and
//! `InMemoryMessageBroker` is `pub` within `message-broker-svc-core`
//! (required for this crate to construct them across the crate boundary,
//! and to name them as `AnyMessageBroker`'s own variant payloads). A
//! consumer depending on `message-broker-svc-saf` alone still never needs
//! to add a direct dependency on any `core`/`spi` crate to construct or use
//! a broker through [`MessageBrokerFactory`]/[`MessageBroker`] — every
//! constructor and every trait method works without naming a concrete
//! backend type. Compile-time visibility is the authoritative check for
//! construction; no runtime assertion is needed.

mod any_message_broker;
mod broker_factory;
mod noop_message_broker;
mod noop_validator;

pub use any_message_broker::AnyMessageBroker;
pub use broker_factory::MessageBrokerFactory;
