//! [`AnyMessageBroker`] — the uniform, zero-cost return type every
//! [`crate::MessageBrokerFactory`] constructor returns.

use message_broker_pattern::{
    BrokerError, HealthCheckRequest, MessageBroker, PublishRequest, SubscribeRequest,
    SubscribeResponse, ValidatorRequest, ValidatorResponse,
};

use crate::noop_message_broker::NoopMessageBroker;

/// One concrete type covering every backend this repo ships, selected by
/// which [`crate::MessageBrokerFactory`] constructor was called.
///
/// Zero-cost: no heap allocation, no vtable. Matching on `self` compiles to
/// a jump table, and each variant's own `async` code becomes one branch of
/// a single generated state machine per trait method — not a boxed future.
/// This is what makes a uniform, runtime-selectable broker type possible
/// again after `MessageBroker` lost object safety (`Box<dyn MessageBroker>`
/// no longer exists) — see `docs/3-design/architecture.md`'s "Why
/// `AnyMessageBroker`, not `Box<dyn MessageBroker>`".
pub enum AnyMessageBroker {
    /// The no-op reference broker.
    Noop(NoopMessageBroker),
    /// The in-process, `tokio::sync::broadcast`-backed broker.
    #[cfg(feature = "inmemory")]
    InMemory(message_broker_svc_core::InMemoryMessageBroker),
    /// The NATS-backed broker.
    #[cfg(feature = "nats")]
    Nats(message_broker_svc_nats_spi::NatsMessageBroker),
    /// The Kafka-backed broker.
    #[cfg(feature = "kafka")]
    Kafka(message_broker_svc_kafka_spi::KafkaMessageBroker),
    /// The Postgres/`pgmq`-backed broker.
    #[cfg(feature = "postgres")]
    Postgres(message_broker_svc_postgres_spi::PostgresMessageBroker),
}

impl MessageBroker for AnyMessageBroker {
    async fn publish(&self, request: PublishRequest) -> Result<(), BrokerError> {
        match self {
            Self::Noop(b) => b.publish(request).await,
            #[cfg(feature = "inmemory")]
            Self::InMemory(b) => b.publish(request).await,
            #[cfg(feature = "nats")]
            Self::Nats(b) => b.publish(request).await,
            #[cfg(feature = "kafka")]
            Self::Kafka(b) => b.publish(request).await,
            #[cfg(feature = "postgres")]
            Self::Postgres(b) => b.publish(request).await,
        }
    }

    async fn subscribe(&self, request: SubscribeRequest) -> Result<SubscribeResponse, BrokerError> {
        match self {
            Self::Noop(b) => b.subscribe(request).await,
            #[cfg(feature = "inmemory")]
            Self::InMemory(b) => b.subscribe(request).await,
            #[cfg(feature = "nats")]
            Self::Nats(b) => b.subscribe(request).await,
            #[cfg(feature = "kafka")]
            Self::Kafka(b) => b.subscribe(request).await,
            #[cfg(feature = "postgres")]
            Self::Postgres(b) => b.subscribe(request).await,
        }
    }

    async fn health_check(&self, request: HealthCheckRequest) -> Result<(), BrokerError> {
        match self {
            Self::Noop(b) => b.health_check(request).await,
            #[cfg(feature = "inmemory")]
            Self::InMemory(b) => b.health_check(request).await,
            #[cfg(feature = "nats")]
            Self::Nats(b) => b.health_check(request).await,
            #[cfg(feature = "kafka")]
            Self::Kafka(b) => b.health_check(request).await,
            #[cfg(feature = "postgres")]
            Self::Postgres(b) => b.health_check(request).await,
        }
    }

    fn validator(&self, request: ValidatorRequest) -> Result<ValidatorResponse, BrokerError> {
        match self {
            Self::Noop(b) => b.validator(request),
            #[cfg(feature = "inmemory")]
            Self::InMemory(b) => b.validator(request),
            #[cfg(feature = "nats")]
            Self::Nats(b) => b.validator(request),
            #[cfg(feature = "kafka")]
            Self::Kafka(b) => b.validator(request),
            #[cfg(feature = "postgres")]
            Self::Postgres(b) => b.validator(request),
        }
    }
}
