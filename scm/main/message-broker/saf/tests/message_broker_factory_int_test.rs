//! Integration tests for [`MessageBrokerFactory`].
#![allow(clippy::unwrap_used, clippy::expect_used)]

use configbuilder::BuilderFinalizer;
use message_broker_svc_saf::MessageBrokerFactory;

/// @covers: MessageBrokerFactory::create_config_builder
#[test]
fn test_message_broker_factory_create_config_builder_is_pre_seeded() {
    let builder = MessageBrokerFactory::create_config_builder();
    let loader = builder.build_loader();
    assert!(loader.is_ok(), "builder must construct a valid loader");
}

/// @covers: MessageBrokerFactory::from_config — in_memory is not this factory's job.
#[tokio::test]
async fn test_from_config_in_memory_returns_unavailable() {
    use message_broker_pattern_contract::{BackendKind, BrokerError};
    use message_broker_pattern_core::MessageBrokerConfig;

    let config = MessageBrokerConfig {
        backend: BackendKind::InMemory,
        url: None,
        group_id: None,
        queue_name: None,
    };
    let result = MessageBrokerFactory::from_config(&config).await;
    assert!(
        matches!(result, Err(BrokerError::Unavailable(_))),
        "in_memory must be rejected — it belongs to message-broker-pattern-saf::BrokerSvc"
    );
}
