//! [`MessageBrokerFactory`] — public message broker construction/dispatch surface.
//!
//! All factory methods are associated functions on this zero-size type.
//! Consumers never construct `MessageBrokerFactory` directly — they call
//! associated functions like [`MessageBrokerFactory::nats`]. Implementation
//! types are returned directly (or as `Box<dyn MessageBroker>` from
//! [`from_config`](MessageBrokerFactory::from_config)) — consumers receive
//! concrete types from the factory methods below and may use them as
//! `impl Trait` at call sites.

#[cfg(feature = "kafka")]
use message_broker_svc_kafka_spi::KafkaMessageBroker;
#[cfg(feature = "nats")]
use message_broker_svc_nats_spi::NatsMessageBroker;
#[cfg(feature = "postgres")]
use message_broker_svc_postgres_spi::PostgresMessageBroker;

use configbuilder::ConfigBuilder;
use message_broker_pattern_contract::{BackendKind, BrokerError, MessageBroker};
use message_broker_pattern_core::MessageBrokerConfig;

/// Zero-size factory type for constructing message broker instances.
pub struct MessageBrokerFactory;

impl MessageBrokerFactory {
    /// Return a [`configbuilder::ConfigBuilderImpl`] pre-seeded with this crate's package name and version.
    pub fn create_config_builder() -> configbuilder::ConfigBuilderImpl {
        configbuilder::ConfigLoaderFactory::create_config_builder()
            .with_name(env!("CARGO_PKG_NAME"))
            .with_version(env!("CARGO_PKG_VERSION"))
    }

    /// Construct and wire a broker from a loaded [`MessageBrokerConfig`].
    ///
    /// The backend is selected by [`MessageBrokerConfig::backend`]:
    /// - [`BackendKind::InMemory`] is not constructible by this factory — use
    ///   `message-broker-pattern-saf::BrokerSvc::noop_broker` for a reference
    ///   in-process broker instead.
    /// - [`BackendKind::Nats`] connects to the configured `url`
    ///   (requires the `nats` feature).
    /// - [`BackendKind::Kafka`] initialises a Kafka client for the configured `url`
    ///   (bootstrap brokers) and `group_id` (requires the `kafka` feature).
    /// - [`BackendKind::Postgres`] connects to the configured `url` (Postgres DSN)
    ///   and ensures `queue_name` exists (requires the `postgres` feature).
    ///
    /// # Errors
    ///
    /// - [`BrokerError::Unavailable`] if the requested backend's Cargo feature
    ///   is not compiled in, or `BackendKind::InMemory` was selected.
    /// - [`BrokerError::Connection`] if a connection cannot be established, or
    ///   if a required config field is missing.
    pub async fn from_config(
        config: &MessageBrokerConfig,
    ) -> Result<Box<dyn MessageBroker>, BrokerError> {
        match config.backend {
            BackendKind::InMemory => Err(BrokerError::Unavailable(
                "in_memory backend is not constructed by message-broker-svc-saf; use \
                 message-broker-pattern-saf::BrokerSvc::noop_broker instead"
                    .to_owned(),
            )),
            BackendKind::Nats => {
                #[cfg(feature = "nats")]
                {
                    let url = config.url.as_deref().ok_or_else(|| {
                        BrokerError::Connection(
                            "nats backend requires a `url` but none was configured".to_owned(),
                        )
                    })?;
                    NatsMessageBroker::connect(url)
                        .await
                        .map(|b| Box::new(b) as Box<dyn MessageBroker>)
                }
                #[cfg(not(feature = "nats"))]
                {
                    Err(BrokerError::Unavailable(
                        "nats backend requires the `nats` feature".to_owned(),
                    ))
                }
            }
            BackendKind::Kafka => {
                #[cfg(feature = "kafka")]
                {
                    let url = config.url.as_deref().ok_or_else(|| {
                        BrokerError::Connection(
                            "kafka backend requires a `url` (bootstrap brokers) but none was configured"
                                .to_owned(),
                        )
                    })?;
                    let group_id = config.group_id.as_deref().ok_or_else(|| {
                        BrokerError::Connection(
                            "kafka backend requires a `group_id` but none was configured"
                                .to_owned(),
                        )
                    })?;
                    KafkaMessageBroker::new(url, group_id)
                        .map(|b| Box::new(b) as Box<dyn MessageBroker>)
                }
                #[cfg(not(feature = "kafka"))]
                {
                    Err(BrokerError::Unavailable(
                        "kafka backend requires the `kafka` feature".to_owned(),
                    ))
                }
            }
            BackendKind::Postgres => {
                #[cfg(feature = "postgres")]
                {
                    let url = config.url.as_deref().ok_or_else(|| {
                        BrokerError::Connection(
                            "postgres backend requires a `url` but none was configured".to_owned(),
                        )
                    })?;
                    let queue_name = config.queue_name.as_deref().ok_or_else(|| {
                        BrokerError::Connection(
                            "postgres backend requires a `queue_name` but none was configured"
                                .to_owned(),
                        )
                    })?;
                    PostgresMessageBroker::connect(url, queue_name)
                        .await
                        .map(|b| Box::new(b) as Box<dyn MessageBroker>)
                }
                #[cfg(not(feature = "postgres"))]
                {
                    Err(BrokerError::Unavailable(
                        "postgres backend requires the `postgres` feature".to_owned(),
                    ))
                }
            }
        }
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
    pub fn kafka(brokers: &str, group_id: &str) -> Result<impl MessageBroker, BrokerError> {
        KafkaMessageBroker::new(brokers, group_id)
    }

    /// Connect to a NATS server and return a broker handle.
    ///
    /// # Errors
    ///
    /// Returns [`BrokerError::Connection`] if the NATS server is unreachable.
    ///
    /// Requires the `nats` feature.
    #[cfg(feature = "nats")]
    pub async fn nats(url: &str) -> Result<impl MessageBroker, BrokerError> {
        NatsMessageBroker::connect(url).await
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
    pub async fn postgres(dsn: &str, queue_name: &str) -> Result<impl MessageBroker, BrokerError> {
        PostgresMessageBroker::connect(dsn, queue_name).await
    }
}
