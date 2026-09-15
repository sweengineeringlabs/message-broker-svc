//! Integration tests for [`MessageBrokerFactory::in_memory`].
//!
//! Unlike the NATS/Kafka/Postgres constructors, this backend needs no live
//! external service, so its full real behavior (not just error paths) is
//! exercised here.

#![allow(clippy::unwrap_used, clippy::expect_used)]

#[cfg(feature = "inmemory")]
mod inmemory_feature {
    use std::sync::Arc;

    use futures::StreamExt as _;
    use message_broker_pattern::{
        HealthCheckRequest, Message, MessageBroker, PublishRequest, SubscribeRequest,
    };
    use message_broker_svc_saf::MessageBrokerFactory;

    /// @covers: MessageBrokerFactory::in_memory
    #[tokio::test]
    async fn test_in_memory_health_check_returns_ok() {
        let broker = MessageBrokerFactory::in_memory();
        assert!(broker.health_check(HealthCheckRequest).await.is_ok());
    }

    /// @covers: MessageBrokerFactory::in_memory — publish/subscribe actually
    /// deliver, unlike `noop`'s inert publish/subscribe.
    #[tokio::test]
    async fn test_in_memory_publish_then_subscribe_delivers_message() {
        let broker = MessageBrokerFactory::in_memory();
        let mut response = broker
            .subscribe(SubscribeRequest {
                topic: "svc-test".to_string(),
            })
            .await
            .expect("subscribe succeeds");

        broker
            .publish(PublishRequest {
                topic: "svc-test".to_string(),
                message: Arc::new(Message::new(b"ping".as_ref())),
            })
            .await
            .expect("publish succeeds");

        let received = response
            .stream
            .next()
            .await
            .expect("a message was delivered")
            .expect("delivery did not error");
        assert_eq!(received.payload, b"ping");
    }

    /// @covers: MessageBrokerFactory::in_memory — rejects an empty topic.
    #[tokio::test]
    async fn test_in_memory_publish_rejects_empty_topic() {
        use message_broker_pattern::BrokerError;

        let broker = MessageBrokerFactory::in_memory();
        let result = broker
            .publish(PublishRequest {
                topic: String::new(),
                message: Arc::new(Message::new(b"x".as_ref())),
            })
            .await;
        assert!(matches!(result, Err(BrokerError::Publish { .. })));
    }
}
