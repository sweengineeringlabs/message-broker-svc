//! Integration tests for the Kafka message broker.
//!
//! These tests run against a synthetic unreachable broker to verify error paths.
//! Tests that require a live Kafka cluster are skipped when none is available.

#![allow(clippy::unwrap_used, clippy::expect_used)]

/// @covers: MessageBrokerFactory::kafka — construction succeeds even for an unreachable host.
#[cfg(feature = "kafka")]
#[tokio::test]
async fn test_kafka_message_broker_factory_constructs_without_network() {
    use message_broker_svc_saf::MessageBrokerFactory;
    // rdkafka connects lazily — construction must not panic or error.
    let result = MessageBrokerFactory::kafka("127.0.0.1:9999", "test-group");
    assert!(
        result.is_ok(),
        "Kafka broker factory must succeed before the first IO attempt"
    );
}

/// @covers: MessageBrokerFactory::kafka — rejects a blank `group_id` via
/// `KafkaConfig`'s own `Validator` impl (through `message-broker-svc-spi-shared`'s
/// `ValidatorExt::validate_config`), before ever building an rdkafka client.
#[cfg(feature = "kafka")]
#[test]
fn test_kafka_message_broker_factory_rejects_blank_group_id() {
    use message_broker_pattern::BrokerError;
    use message_broker_svc_saf::MessageBrokerFactory;

    let result = MessageBrokerFactory::kafka("127.0.0.1:9999", "   ");
    assert!(matches!(result, Err(BrokerError::Connection(_))));
}

/// @covers: MessageBrokerFactory::kafka — health_check fails for an unreachable broker.
#[cfg(feature = "kafka")]
#[tokio::test]
async fn test_kafka_message_broker_health_check_fails_for_unreachable_broker() {
    use message_broker_pattern::{BrokerError, HealthCheckRequest};
    use message_broker_svc_saf::MessageBrokerFactory;

    let broker =
        MessageBrokerFactory::kafka("127.0.0.1:9999", "test-group").expect("client builds");
    let result = broker.health_check(HealthCheckRequest).await;
    assert!(
        matches!(result, Err(BrokerError::Connection(_))),
        "health_check must return Connection error for unreachable broker"
    );
}

/// @covers: MessageBrokerFactory::kafka — publish fails for an unreachable broker.
#[cfg(feature = "kafka")]
#[tokio::test]
async fn test_kafka_message_broker_publish_fails_for_unreachable_broker() {
    use std::sync::Arc;

    use message_broker_pattern::{BrokerError, Message, PublishRequest};
    use message_broker_svc_saf::MessageBrokerFactory;

    let broker =
        MessageBrokerFactory::kafka("127.0.0.1:9999", "test-group").expect("client builds");
    let result = broker
        .publish(PublishRequest {
            topic: "topic".to_string(),
            message: Arc::new(Message::new(b"payload".as_ref())),
        })
        .await;
    assert!(
        matches!(result, Err(BrokerError::Publish { .. })),
        "publish must return Publish error for unreachable broker"
    );
}

// ── Live-broker tests ────────────────────────────────────────────────────────
//
// Run with a real Kafka cluster:
//
//   KAFKA_BROKERS=localhost:9092 cargo test --features kafka -- \
//     --include-ignored --test kafka_message_broker_int_test
//
// All tests below are ignored unless explicitly included so normal CI (without
// a Kafka sidecar) still passes green.

/// Returns the broker address from KAFKA_BROKERS, or panics with a clear message.
#[cfg(feature = "kafka")]
fn require_kafka_brokers() -> String {
    std::env::var("KAFKA_BROKERS")
        .expect("KAFKA_BROKERS env var must be set to run live-broker tests (e.g. localhost:9092)")
}

/// @covers: publish + subscribe — happy-path roundtrip with a live broker.
///
/// Publishes one message and verifies the subscriber stream yields it with the
/// same payload. Also exercises the bounded channel created by `subscribe`.
#[cfg(feature = "kafka")]
#[tokio::test]
#[ignore = "requires-kafka"]
async fn test_publish_subscribe_roundtrip_with_live_broker() {
    use std::sync::Arc;

    use futures::StreamExt as _;
    use message_broker_pattern::{Message, PublishRequest, SubscribeRequest};
    use message_broker_svc_saf::MessageBrokerFactory;

    let brokers = require_kafka_brokers();
    let topic = "swe-edge-test-pub-sub-roundtrip";
    let payload = b"live-broker-payload";

    let broker = MessageBrokerFactory::kafka(&brokers, "swe-edge-test-group-sub")
        .expect("broker construction must succeed with a live broker");

    // Subscribe before publishing so the consumer is assigned the partition first.
    let mut stream = broker
        .subscribe(SubscribeRequest {
            topic: topic.to_string(),
        })
        .await
        .expect("subscribe must succeed with a live broker")
        .stream;

    // Pause to let the consumer group rebalance complete.
    tokio::time::sleep(std::time::Duration::from_secs(2)).await;

    broker
        .publish(PublishRequest {
            topic: topic.to_string(),
            message: Arc::new(Message::new(payload.as_ref())),
        })
        .await
        .expect("publish must succeed with a live broker");

    let received = tokio::time::timeout(std::time::Duration::from_secs(10), stream.next())
        .await
        .expect("message must arrive within 10 s")
        .expect("stream must not end before yielding a message")
        .expect("stream item must not be an error");

    assert_eq!(
        received.payload.as_slice(),
        payload,
        "received payload must match the published payload"
    );
}

/// @covers: publish + subscribe — headers survive a publish/consume round trip.
#[cfg(feature = "kafka")]
#[tokio::test]
#[ignore = "requires-kafka"]
async fn test_publish_subscribe_headers_survive_roundtrip_with_live_broker() {
    use std::collections::HashMap;
    use std::sync::Arc;

    use futures::StreamExt as _;
    use message_broker_pattern::{Message, PublishRequest, SubscribeRequest};
    use message_broker_svc_saf::MessageBrokerFactory;

    let brokers = require_kafka_brokers();
    let topic = "swe-edge-test-headers-roundtrip";
    let payload = b"headers-roundtrip-payload";
    let mut headers = HashMap::new();
    headers.insert("correlation-id".to_string(), "live-broker-42".to_string());
    headers.insert("content-type".to_string(), "application/json".to_string());

    let broker = MessageBrokerFactory::kafka(&brokers, "swe-edge-test-group-headers")
        .expect("broker construction must succeed with a live broker");

    let mut stream = broker
        .subscribe(SubscribeRequest {
            topic: topic.to_string(),
        })
        .await
        .expect("subscribe must succeed with a live broker")
        .stream;

    tokio::time::sleep(std::time::Duration::from_secs(2)).await;

    broker
        .publish(PublishRequest {
            topic: topic.to_string(),
            message: Arc::new(Message::with_headers(payload.as_ref(), headers.clone())),
        })
        .await
        .expect("publish must succeed with a live broker");

    let received = tokio::time::timeout(std::time::Duration::from_secs(10), stream.next())
        .await
        .expect("message must arrive within 10 s")
        .expect("stream must not end before yielding a message")
        .expect("stream item must not be an error");

    assert_eq!(received.payload.as_slice(), payload);
    assert_eq!(
        received.headers, headers,
        "received headers must match the published headers exactly"
    );
}

/// @covers: subscribe — bounded channel construction does not panic or deadlock.
#[cfg(feature = "kafka")]
#[tokio::test]
async fn test_subscribe_returns_stream_without_panicking() {
    use message_broker_pattern::SubscribeRequest;
    use message_broker_svc_saf::MessageBrokerFactory;

    let broker = MessageBrokerFactory::kafka("127.0.0.1:9999", "test-group-cap")
        .expect("construction must succeed before first IO");

    let result = broker
        .subscribe(SubscribeRequest {
            topic: "test-topic-cap".to_string(),
        })
        .await;
    assert!(
        result.is_ok(),
        "subscribe must succeed (channel construction, not network)"
    );
}
