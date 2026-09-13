//! Integration tests for the NATS message broker factory method.

#![allow(clippy::expect_used)]

/// @covers: MessageBrokerFactory::nats
#[cfg(feature = "nats")]
#[tokio::test]
async fn test_nats_message_broker_connect_fails_for_unreachable_host() {
    use message_broker_pattern::BrokerError;
    use message_broker_svc_saf::MessageBrokerFactory;
    let result = MessageBrokerFactory::nats("nats://127.0.0.1:4229").await;
    assert!(matches!(result, Err(BrokerError::Connection(_))));
}

/// @covers: MessageBrokerFactory::nats — rejects a blank `url` via
/// `NatsConfig`'s own `Validator` impl (through `message-broker-svc-core`'s
/// `validate_config`), before ever attempting a network connection.
#[cfg(feature = "nats")]
#[tokio::test]
async fn test_nats_message_broker_connect_fails_fast_for_blank_url() {
    use message_broker_pattern::BrokerError;
    use message_broker_svc_saf::MessageBrokerFactory;

    let result = tokio::time::timeout(
        std::time::Duration::from_secs(1),
        MessageBrokerFactory::nats("   "),
    )
    .await
    .expect("validation must reject a blank url long before any connect timeout");
    assert!(matches!(result, Err(BrokerError::Connection(_))));
}
