//! [`MessageBrokerFactory`] — public message broker construction surface.
//!
//! All factory methods are associated functions on this zero-size type.
//! Consumers never construct `MessageBrokerFactory` directly — they call
//! associated functions like [`MessageBrokerFactory::nats`]. Each method is
//! independent and directly typed: no shared "which backend" type ties them
//! together, and no config-driven runtime dispatch across them exists here.
//! A caller who needs to pick a backend from a runtime-loaded name is
//! building a registry, not the "-svc represents exactly one implementation"
//! shape this factory offers -- see `runtime-svc-registry` for that.

#[cfg(feature = "kafka")]
use message_broker_svc_kafka_spi::KafkaMessageBroker;
#[cfg(feature = "nats")]
use message_broker_svc_nats_spi::NatsMessageBroker;
#[cfg(feature = "postgres")]
use message_broker_svc_postgres_spi::PostgresMessageBroker;

use configbuilder::ConfigBuilder;
use message_broker_pattern::{
    BrokerError, MessageBroker, ValidationError, ValidationRequest, Validator,
};

use crate::noop_message_broker::NoopMessageBroker;

/// Zero-size factory type for constructing message broker instances.
pub struct MessageBrokerFactory;

impl MessageBrokerFactory {
    /// Return a [`ConfigBuilderImpl`](configbuilder::ConfigBuilderImpl) pre-seeded with this crate's package name and version.
    pub fn create_config_builder() -> configbuilder::ConfigBuilderImpl {
        configbuilder::ConfigLoaderFactory::create_config_builder()
            .with_name(env!("CARGO_PKG_NAME"))
            .with_version(env!("CARGO_PKG_VERSION"))
    }

    /// Construct the no-op reference broker.
    ///
    /// Publishing discards the message and subscribing yields an empty stream.
    /// Intended for tests and as a safe default; production deployments use
    /// [`MessageBrokerFactory::nats`]/[`MessageBrokerFactory::kafka`]/
    /// [`MessageBrokerFactory::postgres`] instead.
    pub fn noop() -> Box<dyn MessageBroker> {
        Box::new(NoopMessageBroker)
    }

    /// Validate a value that implements [`Validator`].
    pub fn validate<V: Validator>(v: &V) -> Result<(), ValidationError> {
        v.validate(ValidationRequest)
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
    pub fn kafka(brokers: &str, group_id: &str) -> Result<Box<dyn MessageBroker>, BrokerError> {
        Ok(Box::new(KafkaMessageBroker::new(brokers, group_id)?))
    }

    /// Connect to a NATS server and return a broker handle.
    ///
    /// # Errors
    ///
    /// Returns [`BrokerError::Connection`] if the NATS server is unreachable.
    ///
    /// Requires the `nats` feature.
    #[cfg(feature = "nats")]
    pub async fn nats(url: &str) -> Result<Box<dyn MessageBroker>, BrokerError> {
        Ok(Box::new(NatsMessageBroker::connect(url).await?))
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
    pub async fn postgres(
        dsn: &str,
        queue_name: &str,
    ) -> Result<Box<dyn MessageBroker>, BrokerError> {
        Ok(Box::new(
            PostgresMessageBroker::connect(dsn, queue_name).await?,
        ))
    }
}
