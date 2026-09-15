//! Integration tests for [`MessageBrokerFactory`]'s no-op broker.

#![allow(clippy::unwrap_used, clippy::expect_used)]

use message_broker_pattern::{HealthCheckRequest, MessageBroker};
use message_broker_svc_saf::MessageBrokerFactory;

/// @covers: noop
#[tokio::test]
async fn test_noop_health_check_returns_ok() {
    assert!(MessageBrokerFactory::noop()
        .health_check(HealthCheckRequest)
        .await
        .is_ok());
}

/// @covers: noop
#[tokio::test]
async fn test_noop_publish_then_subscribe_is_inert() {
    use futures::StreamExt as _;
    use message_broker_pattern::{Message, PublishRequest, SubscribeRequest};

    let broker = MessageBrokerFactory::noop();
    broker
        .publish(PublishRequest {
            topic: "svc-test".to_string(),
            message: std::sync::Arc::new(Message::new(b"ping".as_ref())),
        })
        .await
        .unwrap();
    let mut response = broker
        .subscribe(SubscribeRequest {
            topic: "svc-test".to_string(),
        })
        .await
        .unwrap();
    assert!(
        response.stream.next().await.is_none(),
        "noop broker delivers nothing"
    );
}

/// @covers: noop, kafka — both return [`AnyMessageBroker`], so a caller can
/// unify brokers picked from different constructors into one `Vec` (or one
/// `if`/`else` branch) without manually boxing any of them itself, and
/// without paying for a `Box<dyn MessageBroker>` (which no longer exists —
/// `MessageBroker` isn't object-safe once its methods return `impl Future`).
/// Before `kafka` returned this same enum, this test would fail to
/// *compile*, not just fail to pass: `impl MessageBroker` is a distinct
/// anonymous type per call site, not assignable alongside `noop()`'s
/// `AnyMessageBroker` in one `Vec`.
#[cfg(feature = "kafka")]
#[test]
fn test_kafka_and_noop_constructors_return_the_same_broker_type() {
    use message_broker_svc_saf::AnyMessageBroker;

    let brokers: Vec<AnyMessageBroker> = vec![
        MessageBrokerFactory::noop(),
        MessageBrokerFactory::kafka("127.0.0.1:9999", "test-group")
            .expect("kafka client construction succeeds before first IO"),
    ];
    assert_eq!(brokers.len(), 2);
}
