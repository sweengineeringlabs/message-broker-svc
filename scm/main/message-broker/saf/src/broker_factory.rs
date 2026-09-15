//! [`MessageBrokerFactory`] — public message broker construction surface.
//!
//! All factory methods are associated functions on this zero-size type.
//! Consumers never construct `MessageBrokerFactory` directly — they call
//! associated functions like [`MessageBrokerFactory::nats`]. Each method is
//! independent and directly typed: no shared "which backend" type drives
//! *which constructor to call* -- there is no `BackendKind`/`from_config`
//! dispatch here, and a caller who needs to pick a backend from a
//! runtime-loaded name is building a registry, not the "-svc represents
//! exactly one implementation" shape this factory offers, see
//! `runtime-svc-registry` for that. All five constructors DO return one
//! common type, [`crate::AnyMessageBroker`] -- see that type's own doc
//! comment and `docs/3-design/architecture.md`'s "Why `AnyMessageBroker`,
//! not `Box<dyn MessageBroker>`" for why a uniform return type and a
//! `BackendKind`-style dispatch enum are different things, not the same
//! anti-pattern twice.

#[cfg(feature = "inmemory")]
use message_broker_svc_core::InMemoryMessageBroker;
#[cfg(feature = "kafka")]
use message_broker_svc_kafka_spi::KafkaMessageBroker;
#[cfg(feature = "nats")]
use message_broker_svc_nats_spi::NatsMessageBroker;
#[cfg(feature = "postgres")]
use message_broker_svc_postgres_spi::PostgresMessageBroker;

#[cfg(any(feature = "kafka", feature = "nats", feature = "postgres"))]
use message_broker_pattern::BrokerError;

use crate::noop_message_broker::NoopMessageBroker;
use crate::AnyMessageBroker;

/// Zero-size factory type for constructing message broker instances.
pub struct MessageBrokerFactory;

impl MessageBrokerFactory {
    /// Construct the no-op reference broker.
    ///
    /// Publishing discards the message and subscribing yields an empty stream.
    /// Intended for tests and as a safe default; production deployments use
    /// [`MessageBrokerFactory::in_memory`]/[`MessageBrokerFactory::nats`]/
    /// [`MessageBrokerFactory::kafka`]/[`MessageBrokerFactory::postgres`] instead.
    pub fn noop() -> AnyMessageBroker {
        AnyMessageBroker::Noop(NoopMessageBroker)
    }

    /// Construct a real, in-process pub/sub broker backed by
    /// [`tokio::sync::broadcast`].
    ///
    /// Unlike [`MessageBrokerFactory::noop`], published messages are actually
    /// delivered to every active subscriber of the same topic, in the same
    /// process. Topics are created lazily on first subscription. No external
    /// service or network connection is required, so this constructor never
    /// fails.
    ///
    /// Requires the `inmemory` feature.
    #[cfg(feature = "inmemory")]
    pub fn in_memory() -> AnyMessageBroker {
        AnyMessageBroker::InMemory(InMemoryMessageBroker::new())
    }

    /// Connect to a Kafka cluster and return a broker handle.
    ///
    /// # Errors
    ///
    /// Returns [`BrokerError::Connection`] if the rdkafka client configuration is
    /// rejected (e.g. invalid broker address format).
    ///
    /// Requires the `kafka` feature.
    #[cfg(feature = "kafka")]
    pub fn kafka(brokers: &str, group_id: &str) -> Result<AnyMessageBroker, BrokerError> {
        Ok(AnyMessageBroker::Kafka(KafkaMessageBroker::new(
            brokers, group_id,
        )?))
    }

    /// Connect to a NATS server and return a broker handle.
    ///
    /// # Errors
    ///
    /// Returns [`BrokerError::Connection`] if the NATS server is unreachable.
    ///
    /// Requires the `nats` feature.
    #[cfg(feature = "nats")]
    pub async fn nats(url: &str) -> Result<AnyMessageBroker, BrokerError> {
        Ok(AnyMessageBroker::Nats(
            NatsMessageBroker::connect(url).await?,
        ))
    }

    /// Connect to Postgres and return a `pgmq`-backed broker handle.
    ///
    /// `queue_name` is created on the target database if it does not already
    /// exist. Each `topic` argument passed to the returned broker's
    /// `publish`/`subscribe` methods addresses its own `pgmq` queue,
    /// independent of `queue_name`. Unlike the `nats`/`kafka` backends,
    /// delivery is queue semantics: a message goes to exactly one
    /// `subscribe` caller, not to every active subscriber.
    ///
    /// # Errors
    ///
    /// Returns [`BrokerError::Connection`] if the DSN is malformed, Postgres is
    /// unreachable, or the `pgmq` extension is not installed.
    ///
    /// Requires the `postgres` feature.
    #[cfg(feature = "postgres")]
    pub async fn postgres(dsn: &str, queue_name: &str) -> Result<AnyMessageBroker, BrokerError> {
        Ok(AnyMessageBroker::Postgres(
            PostgresMessageBroker::connect(dsn, queue_name).await?,
        ))
    }
}
