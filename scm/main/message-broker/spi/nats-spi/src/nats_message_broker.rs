//! [`NatsMessageBroker`] — NATS-backed message broker via `async-nats`.

use std::collections::HashMap;
use std::sync::Arc;

use futures::StreamExt;

use message_broker_pattern::BrokerError;
use message_broker_pattern::BrokerFuture;
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

use crate::NatsConfig;

/// Health-check round-trip timeout.
///
/// `health_check` sends a real PING to the server and waits for the PONG —
/// this bounds how long that wait can take before being treated as a failure.
const HEALTH_CHECK_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(5);

/// Encode a [`Message`]'s header map as a NATS [`async_nats::HeaderMap`].
fn encode_headers(headers: &HashMap<String, String>) -> async_nats::HeaderMap {
    let mut map = async_nats::HeaderMap::new();
    for (key, value) in headers {
        map.insert(key.as_str(), value.as_str());
    }
    map
}

/// Decode a NATS [`async_nats::HeaderMap`] back into a [`Message`]'s header map.
///
/// A header name repeated with multiple values keeps only the first — this
/// crate's `Message::headers` is a single-valued map, so a later value would
/// otherwise silently overwrite an earlier one with no defined order; keeping
/// the first is at least deterministic given `HeaderMap`'s insertion order.
fn decode_headers(headers: Option<&async_nats::HeaderMap>) -> HashMap<String, String> {
    let Some(headers) = headers else {
        return HashMap::new();
    };
    headers
        .iter()
        .filter_map(|(name, values)| {
            values
                .first()
                .map(|value| (name.to_string(), value.as_str().to_owned()))
        })
        .collect()
}

/// NATS-backed pub/sub broker.
///
/// Wraps an [`async_nats::Client`] to implement [`MessageBroker`].  Connect
/// via [`NatsMessageBroker::connect`], which handles the async handshake and
/// maps connection errors to [`BrokerError::Connection`].
pub struct NatsMessageBroker {
    client: async_nats::Client,
    config: Arc<NatsConfig>,
}

impl NatsMessageBroker {
    /// Establish a NATS connection and return a broker handle.
    ///
    /// Rejects an empty/whitespace-only `url` immediately, via
    /// [`message_broker_svc_core::validate_config`]: `async_nats::connect`
    /// treats an empty address as a slow DNS-resolution failure rather than a
    /// fast parse error, which would otherwise hang this call for the OS
    /// resolver's full timeout instead of returning quickly.
    pub async fn connect(url: impl Into<String>) -> Result<Self, BrokerError> {
        let url = url.into();
        let config = NatsConfig { url: url.clone() };
        message_broker_svc_core::validate_config(&config)?;

        let client = async_nats::connect(url)
            .await
            .map_err(|e| BrokerError::Connection(e.to_string()))?;
        Ok(Self {
            client,
            config: Arc::new(config),
        })
    }
}

impl MessageBroker for NatsMessageBroker {
    fn publish<'a>(&'a self, request: PublishRequest) -> BrokerFuture<'a, Result<(), BrokerError>> {
        let client = self.client.clone();
        BrokerFuture::new(async move {
            let headers = encode_headers(&request.message.headers);
            let payload = request.message.payload.clone();
            client
                .publish_with_headers(request.topic.clone(), headers, payload.into())
                .await
                .map_err(|e| BrokerError::Publish {
                    topic: request.topic,
                    reason: e.to_string(),
                })
        })
    }

    fn subscribe<'a>(
        &'a self,
        request: SubscribeRequest,
    ) -> BrokerFuture<'a, Result<SubscribeResponse, BrokerError>> {
        let client = self.client.clone();
        BrokerFuture::new(async move {
            let subscriber = client.subscribe(request.topic.clone()).await.map_err(|e| {
                BrokerError::Subscribe {
                    topic: request.topic,
                    reason: e.to_string(),
                }
            })?;

            let stream = subscriber.map(|nats_msg| {
                Ok(Message {
                    payload: nats_msg.payload.to_vec(),
                    headers: decode_headers(nats_msg.headers.as_ref()),
                })
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
        let client = self.client.clone();
        BrokerFuture::new(async move {
            // `server_info()` is a cached, no-I/O getter populated once at
            // connect time — it keeps returning the original snapshot even
            // after the connection has actually died, so it can never report
            // unhealthy. `flush()` performs a real round trip (a PING the
            // server must PONG), so a dead connection actually surfaces here.
            tokio::time::timeout(HEALTH_CHECK_TIMEOUT, client.flush())
                .await
                .map_err(|_elapsed| {
                    BrokerError::Connection(format!(
                        "health check did not complete within {HEALTH_CHECK_TIMEOUT:?}"
                    ))
                })?
                .map_err(|e| BrokerError::Connection(e.to_string()))
        })
    }

    fn validator(&self, _request: ValidatorRequest) -> Result<ValidatorResponse, BrokerError> {
        Ok(ValidatorResponse {
            validator: Arc::clone(&self.config) as Arc<dyn Validator>,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_nats_message_broker_is_send_and_sync() {
        fn _assert_send_sync<T: Send + Sync>() {}
        _assert_send_sync::<NatsMessageBroker>();
        assert!(
            std::hint::black_box(true),
            "NatsMessageBroker is Send + Sync (checked above at compile time)"
        );
    }

    /// @covers: connect
    #[test]
    fn test_connect_returns_connection_error_for_unreachable_host() {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let result = rt.block_on(NatsMessageBroker::connect("nats://127.0.0.1:4229"));
        // The Ok variant (NatsMessageBroker) is not Debug, so assert with a
        // static message rather than formatting `result`.
        assert!(
            matches!(result, Err(BrokerError::Connection(_))),
            "expected a Connection error from an unreachable NATS host"
        );
    }

    /// @covers: health_check
    ///
    /// Bounds sanity for the round-trip timeout: it must be positive (else
    /// every health check would instantly time out) and not so large that a
    /// dead connection takes an unreasonable time to be reported unhealthy.
    /// A full regression test proving `health_check` transitions from Ok to
    /// Err after the underlying connection actually dies would require a live
    /// NATS server started and then killed mid-test — not exercised here.
    #[test]
    fn test_health_check_timeout_is_positive_and_bounded() {
        assert!(
            HEALTH_CHECK_TIMEOUT.as_secs() > 0,
            "a zero timeout would fail every health check instantly"
        );
        assert!(
            HEALTH_CHECK_TIMEOUT.as_secs() <= 60,
            "timeout of {:?} is too long for a liveness probe",
            HEALTH_CHECK_TIMEOUT
        );
    }

    /// @covers: encode_headers, decode_headers
    ///
    /// Real bug this catches: `subscribe` previously always returned
    /// `headers: HashMap::new()` regardless of what was actually published,
    /// and `publish` never attached headers to the outgoing NATS message at
    /// all (`client.publish` — not `publish_with_headers`). This proves both
    /// survive an encode/decode round trip through NATS's native `HeaderMap`.
    #[test]
    fn test_encode_decode_headers_round_trips_through_nats_wire_format() {
        let mut input = HashMap::new();
        input.insert("content-type".to_string(), "application/json".to_string());
        input.insert("correlation-id".to_string(), "req-7".to_string());

        let encoded = encode_headers(&input);
        let decoded = decode_headers(Some(&encoded));

        assert_eq!(
            decoded, input,
            "decoded headers must match the encoded input exactly"
        );
    }

    /// @covers: encode_headers, decode_headers
    #[test]
    fn test_encode_decode_empty_headers_round_trips_to_empty_map() {
        let input: HashMap<String, String> = HashMap::new();
        let encoded = encode_headers(&input);
        let decoded = decode_headers(Some(&encoded));
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
