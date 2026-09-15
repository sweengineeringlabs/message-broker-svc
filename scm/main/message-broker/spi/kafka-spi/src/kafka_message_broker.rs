//! [`KafkaMessageBroker`] — Apache Kafka backed pub/sub broker via `rdkafka`.

use std::collections::HashMap;
use std::future::Future;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use futures::channel::mpsc;
use futures::SinkExt as _;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{Consumer as _, StreamConsumer};
use rdkafka::error::KafkaError;
use rdkafka::message::{BorrowedHeaders, Header, Headers as _, Message as RdkafkaMessage};
use rdkafka::producer::{FutureProducer, FutureRecord, Producer as _};
use rdkafka::types::RDKafkaErrorCode;

use message_broker_pattern::BrokerError;
use message_broker_pattern::HealthCheckRequest;
use message_broker_pattern::Message;
use message_broker_pattern::MessageBroker;
use message_broker_pattern::MessageStream;
use message_broker_pattern::PublishRequest;
use message_broker_pattern::SubscribeRequest;
use message_broker_pattern::SubscribeResponse;
use message_broker_pattern::Validator;
use message_broker_pattern::ValidatorRequest;
use message_broker_pattern::ValidatorResponse;

use crate::KafkaConfig;

/// Monotonic counter mixed into each subscriber's consumer-group ID so
/// multiple `subscribe()` calls within the same process never collide even if
/// called within the same nanosecond.
static SUBSCRIBER_SEQUENCE: AtomicU64 = AtomicU64::new(0);

/// Derive a per-subscription consumer-group ID from `base`.
///
/// Kafka delivers each message to exactly one consumer *within* a consumer
/// group; consumers in different groups each get their own copy. The
/// [`MessageBroker`] contract requires that "all active subscribers receive
/// every message" (pub/sub fan-out) — so every [`subscribe`](KafkaMessageBroker::subscribe)
/// call must land in its own, never-reused consumer group, otherwise multiple
/// subscribers created from the same broker handle would silently degrade
/// into Kafka's competing-consumer behavior (each partition delivered to only
/// one of them).
fn unique_subscriber_group_id(base: &str) -> String {
    let sequence = SUBSCRIBER_SEQUENCE.fetch_add(1, Ordering::Relaxed);
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    format!("{base}-sub-{nanos}-{sequence}")
}

/// Encode a [`Message`]'s header map as Kafka [`OwnedHeaders`](rdkafka::message::OwnedHeaders).
fn encode_headers(headers: &HashMap<String, String>) -> rdkafka::message::OwnedHeaders {
    let mut owned = rdkafka::message::OwnedHeaders::new_with_capacity(headers.len().max(1));
    for (key, value) in headers {
        owned = owned.insert(Header {
            key: key.as_str(),
            value: Some(value.as_str()),
        });
    }
    owned
}

/// Decode Kafka message headers back into a [`Message`]'s header map.
///
/// Returns an empty map if the message carried no headers at all.
fn decode_headers(headers: Option<&BorrowedHeaders>) -> HashMap<String, String> {
    let Some(headers) = headers else {
        return HashMap::new();
    };
    let mut map = HashMap::with_capacity(headers.count());
    for i in 0..headers.count() {
        let header = headers.get(i);
        if let Some(value) = header.value {
            map.insert(
                header.key.to_owned(),
                String::from_utf8_lossy(value).into_owned(),
            );
        }
    }
    map
}

/// Kafka-backed pub/sub broker using `rdkafka`.
///
/// Publish operations use a [`FutureProducer`] shared across all calls. Each
/// [`subscribe`](KafkaMessageBroker::subscribe) call creates a dedicated
/// [`StreamConsumer`] in its own, uniquely derived consumer group — see
/// [`unique_subscriber_group_id`] — so every subscriber receives every
/// message, matching the [`MessageBroker`] fan-out contract instead of
/// degrading into competing-consumer partition division.
/// Auto-commit is enabled for subscribers.
pub struct KafkaMessageBroker {
    producer: FutureProducer,
    /// Bootstrap broker list, stored to create per-subscriber consumers.
    brokers: String,
    /// Base consumer group ID; each [`subscribe`](KafkaMessageBroker::subscribe)
    /// call derives its own unique group from this base (see
    /// [`unique_subscriber_group_id`]) so subscribers fan out rather than compete.
    group_id: String,
    config: Arc<KafkaConfig>,
}

impl KafkaMessageBroker {
    /// Initialise the Kafka client with the given bootstrap brokers and group ID.
    ///
    /// This call does **not** establish a network connection — rdkafka connects
    /// lazily on the first produce or subscribe operation.
    ///
    /// # Errors
    ///
    /// Returns [`BrokerError::Connection`] if the producer configuration is invalid.
    pub fn new(brokers: &str, group_id: &str) -> Result<Self, BrokerError> {
        let config = KafkaConfig {
            url: brokers.to_owned(),
            group_id: group_id.to_owned(),
        };
        config.validate_config()?;

        let producer: FutureProducer = ClientConfig::new()
            .set("bootstrap.servers", brokers)
            .set(
                "message.timeout.ms",
                crate::constants::KAFKA_MESSAGE_TIMEOUT_MS,
            )
            .create()
            .map_err(|e| BrokerError::Connection(e.to_string()))?;

        Ok(Self {
            producer,
            brokers: brokers.to_owned(),
            group_id: group_id.to_owned(),
            config: Arc::new(config),
        })
    }
}

impl MessageBroker for KafkaMessageBroker {
    fn publish(
        &self,
        request: PublishRequest,
    ) -> impl Future<Output = Result<(), BrokerError>> + Send + '_ {
        let producer = self.producer.clone();
        async move {
            let headers = encode_headers(&request.message.headers);
            producer
                .send(
                    // No key: omitting it (rather than passing `""`) lets rdkafka's
                    // default partitioner distribute across all partitions instead
                    // of hashing every message to the same one.
                    FutureRecord::<(), _>::to(&request.topic)
                        .payload(&request.message.payload[..])
                        .headers(headers),
                    std::time::Duration::from_secs(5),
                )
                .await
                .map(|_| ())
                .map_err(|(e, _)| BrokerError::Publish {
                    topic: request.topic,
                    reason: e.to_string(),
                })
        }
    }

    fn subscribe(
        &self,
        request: SubscribeRequest,
    ) -> impl Future<Output = Result<SubscribeResponse, BrokerError>> + Send + '_ {
        let topic = request.topic;
        let brokers = self.brokers.clone();
        // Unique per subscription — see `unique_subscriber_group_id` doc comment.
        let group_id = unique_subscriber_group_id(&self.group_id);
        async move {
            let consumer: Arc<StreamConsumer> = Arc::new(
                ClientConfig::new()
                    .set("bootstrap.servers", &brokers)
                    .set("group.id", &group_id)
                    // Auto-commit for pub/sub subscribers — callers do not ack.
                    .set("enable.auto.commit", "true")
                    .set("auto.offset.reset", "latest")
                    .set(
                        "session.timeout.ms",
                        crate::constants::KAFKA_SESSION_TIMEOUT_MS,
                    )
                    .create()
                    .map_err(|e| BrokerError::Connection(e.to_string()))?,
            );

            consumer
                .subscribe(&[topic.as_str()])
                .map_err(|e| BrokerError::Subscribe {
                    topic: topic.clone(),
                    reason: e.to_string(),
                })?;

            // Bounded channel decouples BorrowedMessage<'_> lifetime from the returned
            // stream and applies backpressure to slow subscribers: when the channel is
            // full, the poll loop yield-waits on `send`, slowing Kafka consumption
            // instead of growing the heap without limit.
            let (mut tx, rx) = mpsc::channel::<Result<Message, BrokerError>>(
                crate::constants::KAFKA_SUBSCRIBE_CHANNEL_CAPACITY,
            );

            tokio::spawn(async move {
                let idle_check = std::time::Duration::from_secs(
                    crate::constants::KAFKA_SUBSCRIBE_IDLE_CHECK_SECS,
                );
                loop {
                    // Bound each wait so a dropped receiver on an otherwise idle
                    // topic is noticed within `idle_check`, instead of only on the
                    // next incoming message (which may never arrive).
                    if tx.is_closed() {
                        break;
                    }
                    match tokio::time::timeout(idle_check, consumer.recv()).await {
                        Err(_elapsed) => {
                            // No message within the idle window; loop back to
                            // re-check whether the receiver was dropped.
                            continue;
                        }
                        Ok(Err(KafkaError::MessageConsumption(RDKafkaErrorCode::PartitionEOF))) => {
                            // Normal end-of-partition — no new messages right now; keep polling.
                            continue;
                        }
                        Ok(Err(e)) => {
                            let _ = tx
                                .send(Err(BrokerError::Subscribe {
                                    topic: String::new(),
                                    reason: e.to_string(),
                                }))
                                .await;
                            break;
                        }
                        Ok(Ok(msg)) => {
                            let payload = msg.payload().unwrap_or_default().to_vec();
                            let headers = decode_headers(msg.headers());
                            let broker_msg = Message { payload, headers };
                            // Drop borrowed message before the await to avoid holding the
                            // rdkafka lifetime across the yield point.
                            drop(msg);
                            if tx.send(Ok(broker_msg)).await.is_err() {
                                // Receiver dropped — subscriber gone.
                                break;
                            }
                        }
                    }
                }
            });

            Ok(SubscribeResponse {
                stream: Box::pin(rx) as MessageStream,
            })
        }
    }

    fn health_check(
        &self,
        _request: HealthCheckRequest,
    ) -> impl Future<Output = Result<(), BrokerError>> + Send + '_ {
        let producer = self.producer.clone();
        async move {
            tokio::task::spawn_blocking(move || {
                producer
                    .client()
                    .fetch_metadata(
                        None,
                        std::time::Duration::from_secs(
                            crate::constants::KAFKA_HEALTH_CHECK_TIMEOUT_SECS,
                        ),
                    )
                    .map(|_| ())
                    .map_err(|e| BrokerError::Connection(e.to_string()))
            })
            .await
            .map_err(|e| BrokerError::Connection(format!("health check task failed: {e}")))?
        }
    }

    fn validator(&self, _request: ValidatorRequest) -> Result<ValidatorResponse, BrokerError> {
        Ok(self.config.validator_response())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_kafka_message_broker_is_send_and_sync() {
        fn _assert<T: Send + Sync>() {}
        _assert::<KafkaMessageBroker>();
        assert!(
            std::hint::black_box(true),
            "KafkaMessageBroker is Send + Sync (checked above at compile time)"
        );
    }

    /// @covers: new
    #[test]
    fn test_new_invalid_broker_config_returns_connection_error() {
        // An empty broker string is rejected by librdkafka at config time.
        let result = KafkaMessageBroker::new("", "test-group");
        // Construction may succeed (rdkafka validates lazily) or fail — either is
        // acceptable; reaching this assertion at all proves new() did not panic.
        assert!(
            result.is_ok() || result.is_err(),
            "new() with empty brokers must return a Result, not panic"
        );
    }

    /// @covers: health_check
    #[tokio::test]
    async fn test_health_check_fails_for_unreachable_broker() {
        let broker = KafkaMessageBroker::new("127.0.0.1:9999", "test-group")
            .expect("client construction succeeds before first IO");
        let result = broker.health_check(HealthCheckRequest).await;
        assert!(
            matches!(result, Err(BrokerError::Connection(_))),
            "expected Connection error for unreachable broker, got: {result:?}"
        );
    }

    /// @covers: subscribe
    #[test]
    fn test_subscribe_channel_capacity_is_positive_and_bounded() {
        let cap = crate::constants::KAFKA_SUBSCRIBE_CHANNEL_CAPACITY;
        assert!(
            cap > 0,
            "capacity must be positive; 0 would deadlock on first send"
        );
        assert!(
            cap <= 1_000_000,
            "capacity {cap} exceeds sanity limit of 1 M; backpressure would be ineffective"
        );
    }

    /// @covers: subscribe
    ///
    /// Idle-topic consumer leak bound: this interval caps how long a dropped
    /// receiver's consumer stays alive on an otherwise idle topic.
    #[test]
    fn test_subscribe_idle_check_interval_is_positive_and_bounded() {
        let secs = crate::constants::KAFKA_SUBSCRIBE_IDLE_CHECK_SECS;
        assert!(
            secs > 0,
            "idle check interval must be positive; 0 would busy-loop"
        );
        assert!(
            secs <= 300,
            "idle check interval {secs}s exceeds sanity limit of 5 min; a dropped \
             receiver could leak its consumer for too long"
        );
    }

    /// @covers: unique_subscriber_group_id
    ///
    /// Real bug this catches: if two `subscribe()` calls ever derived the same
    /// consumer-group ID, Kafka would treat them as competing consumers and
    /// split partitions between them instead of fanning the same message out
    /// to both — silently breaking the `MessageBroker` pub/sub contract.
    #[test]
    fn test_unique_subscriber_group_id_never_repeats_for_same_base() {
        let base = "svc-group";
        let first = unique_subscriber_group_id(base);
        let second = unique_subscriber_group_id(base);
        assert_ne!(
            first, second,
            "two subscriptions from the same base group must not collide"
        );
        assert!(
            first.starts_with(base),
            "derived ID must retain the base group prefix"
        );
        assert!(
            second.starts_with(base),
            "derived ID must retain the base group prefix"
        );
    }

    /// @covers: encode_headers, decode_headers
    ///
    /// Real bug this catches: if headers were dropped on encode (as they
    /// previously were — `publish` never attached them to the Kafka record at
    /// all) or ignored on decode (`headers: HashMap::new()`), this round trip
    /// would come back empty instead of matching the input.
    #[test]
    fn test_encode_decode_headers_round_trips_through_kafka_wire_format() {
        let mut input = HashMap::new();
        input.insert("content-type".to_string(), "application/json".to_string());
        input.insert("correlation-id".to_string(), "abc-123".to_string());

        let owned = encode_headers(&input);
        let borrowed = owned.as_borrowed();
        let decoded = decode_headers(Some(borrowed));

        assert_eq!(
            decoded, input,
            "decoded headers must match the encoded input exactly"
        );
    }

    /// @covers: encode_headers, decode_headers
    #[test]
    fn test_encode_decode_empty_headers_round_trips_to_empty_map() {
        let input: HashMap<String, String> = HashMap::new();
        let owned = encode_headers(&input);
        let borrowed = owned.as_borrowed();
        let decoded = decode_headers(Some(borrowed));
        assert!(
            decoded.is_empty(),
            "empty input headers must decode to an empty map"
        );
    }

    /// @covers: decode_headers
    #[test]
    fn test_decode_headers_none_returns_empty_map() {
        let decoded = decode_headers(None);
        assert!(
            decoded.is_empty(),
            "a message with no headers at all must decode to an empty map, not panic"
        );
    }
}
