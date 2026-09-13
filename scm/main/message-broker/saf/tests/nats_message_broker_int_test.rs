//! Integration tests for the NATS message broker API marker.

/// @covers: MessageBrokerFactory::nats
#[cfg(feature = "nats")]
#[tokio::test]
async fn test_nats_message_broker_connect_fails_for_unreachable_host() {
    use message_broker_pattern_contract::BrokerError;
    use message_broker_svc_saf::MessageBrokerFactory;
    let result = MessageBrokerFactory::nats("nats://127.0.0.1:4229").await;
    assert!(matches!(result, Err(BrokerError::Connection(_))));
}

/// @covers: MessageBrokerFactory::from_config — without the nats feature, Unavailable.
#[cfg(not(feature = "nats"))]
#[tokio::test]
async fn test_from_config_nats_without_feature_returns_unavailable() {
    use message_broker_pattern_contract::{BackendKind, BrokerError};
    use message_broker_pattern_core::MessageBrokerConfig;
    use message_broker_svc_saf::MessageBrokerFactory;

    let cfg = MessageBrokerConfig {
        backend: BackendKind::Nats,
        url: Some("nats://127.0.0.1:4229".into()),
        group_id: None,
        queue_name: None,
    };
    let result = MessageBrokerFactory::from_config(&cfg).await;
    assert!(
        matches!(result, Err(BrokerError::Unavailable(_))),
        "expected Unavailable when the nats feature is not compiled in"
    );
}
