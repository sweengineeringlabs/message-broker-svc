//! Integration tests for the NATS message broker factory method.

/// @covers: MessageBrokerFactory::nats
#[cfg(feature = "nats")]
#[tokio::test]
async fn test_nats_message_broker_connect_fails_for_unreachable_host() {
    use message_broker_pattern::BrokerError;
    use message_broker_svc_saf::MessageBrokerFactory;
    let result = MessageBrokerFactory::nats("nats://127.0.0.1:4229").await;
    assert!(matches!(result, Err(BrokerError::Connection(_))));
}
