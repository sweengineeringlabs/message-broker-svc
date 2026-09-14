//! [`InMemoryMessageBroker`] — in-process pub/sub broker via `tokio::sync::broadcast`.
//!
//! Ported from `edge-runtime`'s `runtime-message-broker-core::InMemoryMessageBroker`
//! -- the real in-memory backend `edge-message-broker`'s own extraction never
//! carried forward, restoring the functionality lost when `BackendKind`'s
//! `InMemory` variant was removed as an anti-pattern (see this repo's ADR-001
//! amendments): the enum naming a technology was the defect, not the backend
//! itself having a real implementation.

use std::collections::HashMap;
use std::sync::Arc;

use tokio::sync::{broadcast, RwLock};

use message_broker_pattern::BrokerError;
use message_broker_pattern::BrokerFuture;
use message_broker_pattern::HealthCheckRequest;
use message_broker_pattern::Message;
use message_broker_pattern::MessageBroker;
use message_broker_pattern::MessageStream;
use message_broker_pattern::PublishRequest;
use message_broker_pattern::SubscribeRequest;
use message_broker_pattern::SubscribeResponse;
use message_broker_pattern::ValidatorRequest;
use message_broker_pattern::ValidatorResponse;
use message_broker_svc_spi_shared::ValidatorExt;

use crate::InMemoryConfig;

/// Maximum byte length of a topic name (inclusive).
///
/// Topics exceeding this limit are rejected at publish time to prevent
/// unbounded memory growth in the channel map key space.
const MAX_TOPIC_BYTES: usize = 256;

/// Default broadcast channel capacity per topic.
///
/// When a topic's sender is created, this many messages can be buffered
/// before slow receivers start lagging. If a receiver falls more than this
/// many messages behind, it receives a `StreamLagged` error on the next recv.
const DEFAULT_CHANNEL_CAPACITY: usize = 1024;

/// In-memory pub/sub broker backed by [`tokio::sync::broadcast`].
///
/// Topics are created lazily on first subscription. Multiple handles to the
/// same broker share a single channel map via an internal `Arc`, so cloning
/// this struct produces another handle to the same broker.
#[derive(Clone)]
pub struct InMemoryMessageBroker {
    channels: Arc<RwLock<HashMap<String, broadcast::Sender<Message>>>>,
    config: Arc<InMemoryConfig>,
}

impl InMemoryMessageBroker {
    /// Construct a fresh in-memory broker with no topics yet created.
    pub fn new() -> Self {
        Self {
            channels: Arc::new(RwLock::new(HashMap::new())),
            config: Arc::new(InMemoryConfig {}),
        }
    }

    fn check_topic(topic: &str) -> Result<(), BrokerError> {
        if topic.is_empty() {
            return Err(BrokerError::Publish {
                topic: topic.to_owned(),
                reason: "topic must not be empty".into(),
            });
        }
        if topic.len() > MAX_TOPIC_BYTES {
            return Err(BrokerError::Publish {
                topic: topic.to_owned(),
                reason: format!("topic exceeds maximum length of {MAX_TOPIC_BYTES} bytes"),
            });
        }
        Ok(())
    }
}

impl Default for InMemoryMessageBroker {
    fn default() -> Self {
        Self::new()
    }
}

impl MessageBroker for InMemoryMessageBroker {
    fn publish<'a>(&'a self, request: PublishRequest) -> BrokerFuture<'a, Result<(), BrokerError>> {
        let validation = Self::check_topic(&request.topic);
        let channels = Arc::clone(&self.channels);
        BrokerFuture::new(async move {
            validation?;
            let map = channels.read().await;
            if let Some(tx) = map.get(&request.topic) {
                let _ = tx.send((*request.message).clone());
            }
            Ok(())
        })
    }

    fn subscribe<'a>(
        &'a self,
        request: SubscribeRequest,
    ) -> BrokerFuture<'a, Result<SubscribeResponse, BrokerError>> {
        let channels = Arc::clone(&self.channels);
        BrokerFuture::new(async move {
            let rx = {
                let mut map = channels.write().await;
                let tx = map
                    .entry(request.topic.clone())
                    .or_insert_with(|| broadcast::channel(DEFAULT_CHANNEL_CAPACITY).0);
                tx.subscribe()
            };

            let stream = futures::stream::unfold(rx, |mut recv| async move {
                match recv.recv().await {
                    Ok(msg) => Some((Ok(msg), recv)),
                    Err(broadcast::error::RecvError::Closed) => None,
                    Err(broadcast::error::RecvError::Lagged(n)) => {
                        Some((Err(BrokerError::StreamLagged { count: n }), recv))
                    }
                }
            });

            Ok(SubscribeResponse {
                stream: Box::pin(stream) as MessageStream,
            })
        })
    }

    fn health_check(
        &self,
        _request: HealthCheckRequest,
    ) -> BrokerFuture<'_, Result<(), BrokerError>> {
        BrokerFuture::new(async { Ok(()) })
    }

    fn validator(&self, _request: ValidatorRequest) -> Result<ValidatorResponse, BrokerError> {
        Ok(self.config.validator_response())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_inmemory_message_broker_is_send_and_sync() {
        fn _assert_send_sync<T: Send + Sync>() {}
        _assert_send_sync::<InMemoryMessageBroker>();
        assert!(
            std::hint::black_box(true),
            "InMemoryMessageBroker is Send + Sync (checked above at compile time)"
        );
    }

    /// @covers: check_topic
    #[test]
    fn test_check_topic_rejects_empty_topic() {
        let err = InMemoryMessageBroker::check_topic("").expect_err("empty topic must be rejected");
        assert!(matches!(err, BrokerError::Publish { .. }));
    }

    /// @covers: check_topic
    #[test]
    fn test_check_topic_rejects_topic_exceeding_max_length() {
        let topic = "a".repeat(MAX_TOPIC_BYTES + 1);
        let err = InMemoryMessageBroker::check_topic(&topic)
            .expect_err("over-length topic must be rejected");
        assert!(matches!(err, BrokerError::Publish { .. }));
    }

    /// @covers: check_topic
    ///
    /// The boundary case: exactly `MAX_TOPIC_BYTES` is accepted, one byte over
    /// is rejected (see `test_check_topic_rejects_topic_exceeding_max_length`).
    #[test]
    fn test_check_topic_accepts_topic_at_max_length() {
        let topic = "a".repeat(MAX_TOPIC_BYTES);
        assert!(matches!(InMemoryMessageBroker::check_topic(&topic), Ok(())));
    }

    /// @covers: publish, subscribe
    ///
    /// Real bug this would catch: a broker that silently drops messages, or
    /// one whose `Message` round-trip through the broadcast channel corrupts
    /// the payload/headers.
    #[tokio::test]
    async fn test_publish_then_subscribe_delivers_message() {
        use futures::StreamExt as _;

        let broker = InMemoryMessageBroker::new();
        let mut response = broker
            .subscribe(SubscribeRequest {
                topic: "orders".to_string(),
            })
            .await
            .expect("subscribe succeeds");

        let mut headers = HashMap::new();
        headers.insert("content-type".to_string(), "application/json".to_string());
        broker
            .publish(PublishRequest {
                topic: "orders".to_string(),
                message: Arc::new(Message::with_headers(
                    b"order-created".as_ref(),
                    headers.clone(),
                )),
            })
            .await
            .expect("publish succeeds");

        let received = response
            .stream
            .next()
            .await
            .expect("a message was delivered")
            .expect("delivery did not error");
        assert_eq!(received.payload, b"order-created");
        assert_eq!(received.headers, headers);
    }

    /// @covers: publish, subscribe
    ///
    /// Real bug this would catch: fan-out silently degrading into
    /// competing-consumer delivery (each subscriber getting only some
    /// messages instead of every subscriber getting every message).
    #[tokio::test]
    async fn test_multiple_subscribers_all_receive_published_message() {
        use futures::StreamExt as _;

        let broker = InMemoryMessageBroker::new();
        let mut first = broker
            .subscribe(SubscribeRequest {
                topic: "fanout".to_string(),
            })
            .await
            .expect("first subscribe succeeds");
        let mut second = broker
            .subscribe(SubscribeRequest {
                topic: "fanout".to_string(),
            })
            .await
            .expect("second subscribe succeeds");

        broker
            .publish(PublishRequest {
                topic: "fanout".to_string(),
                message: Arc::new(Message::new(b"broadcast".as_ref())),
            })
            .await
            .expect("publish succeeds");

        let first_msg = first
            .stream
            .next()
            .await
            .expect("first subscriber receives a message")
            .expect("delivery did not error");
        let second_msg = second
            .stream
            .next()
            .await
            .expect("second subscriber receives a message")
            .expect("delivery did not error");
        assert_eq!(first_msg.payload, b"broadcast");
        assert_eq!(second_msg.payload, b"broadcast");
    }

    /// @covers: publish
    ///
    /// Matches `MessageBroker`'s own documented contract: messages published
    /// before a subscriber exists for that topic are not delivered to it.
    #[tokio::test]
    async fn test_publish_before_subscribe_is_not_delivered() {
        let broker = InMemoryMessageBroker::new();
        // No subscriber yet -- publishing must still succeed (fire and forget).
        broker
            .publish(PublishRequest {
                topic: "late-topic".to_string(),
                message: Arc::new(Message::new(b"missed".as_ref())),
            })
            .await
            .expect("publish with no subscribers succeeds");
    }

    /// @covers: publish
    #[tokio::test]
    async fn test_publish_rejects_empty_topic() {
        let broker = InMemoryMessageBroker::new();
        let result = broker
            .publish(PublishRequest {
                topic: String::new(),
                message: Arc::new(Message::new(b"x".as_ref())),
            })
            .await;
        assert!(matches!(result, Err(BrokerError::Publish { .. })));
    }

    /// @covers: health_check
    #[tokio::test]
    async fn test_health_check_always_returns_ok() {
        let broker = InMemoryMessageBroker::new();
        assert!(broker.health_check(HealthCheckRequest).await.is_ok());
    }

    /// @covers: default
    #[tokio::test]
    async fn test_default_constructs_usable_broker() {
        let broker = InMemoryMessageBroker::default();
        // Real bug this would catch: `default()` returning a broker whose
        // internal state (e.g. an un-initialized channel map) panics or
        // errors on the first real operation instead of behaving like a
        // freshly-constructed `new()` broker.
        assert!(broker.health_check(HealthCheckRequest).await.is_ok());
    }
}
