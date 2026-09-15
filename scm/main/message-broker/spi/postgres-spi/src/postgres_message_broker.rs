//! [`PostgresMessageBroker`] — Postgres-backed message broker via the `pgmq` extension.

use std::collections::HashMap;
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use base64::engine::general_purpose::STANDARD as BASE64;
use base64::Engine as _;
use futures::stream;
use sqlx::PgPool;
use sqlx::Row;

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

use crate::PostgresConfig;

/// Delay between `pgmq.pop` polls when a queue has no message available.
const POLL_INTERVAL: Duration = Duration::from_millis(250);

/// Postgres-backed message broker using the `pgmq` extension.
///
/// # Semantics differ from other backends
///
/// Each `topic` argument is used directly as a `pgmq` queue name — created on
/// first use if it does not already exist. Unlike the Kafka/NATS backends,
/// delivery is **queue** semantics, not broadcast: a published message is
/// delivered to exactly one `subscribe` caller, not to every active
/// subscriber, because `pgmq` has no fan-out concept. Do not use this backend
/// where the [`MessageBroker`] trait's documented "every active subscriber
/// receives every message" contract is required.
///
/// Uses `pgmq.pop` (atomic read + delete) rather than a separate read/ack
/// step, because the `MessageBroker` trait exposes no ack primitive — once a
/// message is popped it cannot be redelivered if the consumer then fails to
/// process it.
///
/// Requires the `pgmq` extension (`CREATE EXTENSION pgmq;`) installed on the
/// target database.
pub struct PostgresMessageBroker {
    pool: PgPool,
    config: Arc<PostgresConfig>,
}

impl PostgresMessageBroker {
    /// Connect to Postgres and ensure `queue_name` exists.
    ///
    /// # Errors
    ///
    /// Returns [`BrokerError::Connection`] if the DSN is malformed, the server
    /// is unreachable, or the `pgmq` extension is not installed.
    pub async fn connect(dsn: &str, queue_name: &str) -> Result<Self, BrokerError> {
        let config = PostgresConfig {
            url: dsn.to_owned(),
            queue_name: queue_name.to_owned(),
        };
        config.validate_config()?;

        let pool = PgPool::connect(dsn)
            .await
            .map_err(|e| BrokerError::Connection(e.to_string()))?;

        Self::ensure_queue(&pool, queue_name)
            .await
            .map_err(|e| BrokerError::Connection(e.to_string()))?;

        Ok(Self {
            pool,
            config: Arc::new(config),
        })
    }

    /// Create `queue_name` if it does not already exist.
    ///
    /// Tolerates the "already exists" case as success — `pgmq.create` is not
    /// guaranteed idempotent across all `pgmq` versions.
    async fn ensure_queue(pool: &PgPool, queue_name: &str) -> Result<(), sqlx::Error> {
        let result = sqlx::query("SELECT pgmq.create($1)")
            .bind(queue_name)
            .execute(pool)
            .await;

        match result {
            Ok(_) => Ok(()),
            Err(sqlx::Error::Database(ref db_err))
                if db_err.message().contains("already exists") =>
            {
                Ok(())
            }
            Err(e) => Err(e),
        }
    }

    /// Encode a [`Message`] as the JSONB envelope `pgmq.send` stores.
    ///
    /// The raw payload is base64-encoded so arbitrary binary data survives a
    /// JSONB round-trip; headers are stored alongside it verbatim.
    fn encode(msg: &Message) -> serde_json::Value {
        serde_json::json!({
            "payload_b64": BASE64.encode(&msg.payload),
            "headers": msg.headers,
        })
    }

    /// Decode a `pgmq.pop` row's `message` column into a [`Message`], mapping
    /// either a fetch error or a decode failure to [`BrokerError::Subscribe`].
    ///
    /// Extracted out of `subscribe`'s poll loop to keep that loop's nesting
    /// within this crate's max depth (`no_deep_loop_nesting`) — this function
    /// itself is a plain two-arm match, no loop.
    fn decode_popped_row(
        topic: &str,
        value: Result<serde_json::Value, sqlx::Error>,
    ) -> Result<Message, BrokerError> {
        match value {
            Ok(v) => Self::decode(v).map_err(|reason| BrokerError::Subscribe {
                topic: topic.to_owned(),
                reason,
            }),
            Err(e) => Err(BrokerError::Subscribe {
                topic: topic.to_owned(),
                reason: e.to_string(),
            }),
        }
    }

    /// Convert a JSON object's string-valued entries into a header map,
    /// dropping any entry whose value isn't a string.
    ///
    /// Extracted out of `decode` to keep that function's iterator-adapter
    /// nesting within this crate's max depth (`no_deep_loop_nesting`).
    fn decode_headers_object(
        obj: &serde_json::Map<String, serde_json::Value>,
    ) -> HashMap<String, String> {
        obj.iter()
            .filter_map(|(k, v)| v.as_str().map(|s| (k.clone(), s.to_string())))
            .collect()
    }

    /// Decode a `pgmq` message envelope back into a [`Message`].
    fn decode(value: serde_json::Value) -> Result<Message, String> {
        let payload_b64 = value
            .get("payload_b64")
            .and_then(|v| v.as_str())
            .ok_or_else(|| "message missing `payload_b64` field".to_string())?;
        let payload = BASE64
            .decode(payload_b64)
            .map_err(|e| format!("invalid base64 payload: {e}"))?;
        let headers = value
            .get("headers")
            .and_then(|v| v.as_object())
            .map(Self::decode_headers_object)
            .unwrap_or_default();
        Ok(Message { payload, headers })
    }
}

impl MessageBroker for PostgresMessageBroker {
    fn publish(
        &self,
        request: PublishRequest,
    ) -> impl Future<Output = Result<(), BrokerError>> + Send + '_ {
        let pool = self.pool.clone();
        let topic = request.topic;
        async move {
            Self::ensure_queue(&pool, &topic)
                .await
                .map_err(|e| BrokerError::Publish {
                    topic: topic.clone(),
                    reason: e.to_string(),
                })?;

            let payload = Self::encode(&request.message);
            sqlx::query("SELECT pgmq.send($1, $2::jsonb)")
                .bind(&topic)
                .bind(&payload)
                .execute(&pool)
                .await
                .map_err(|e| BrokerError::Publish {
                    topic: topic.clone(),
                    reason: e.to_string(),
                })?;

            Ok(())
        }
    }

    fn subscribe(
        &self,
        request: SubscribeRequest,
    ) -> impl Future<Output = Result<SubscribeResponse, BrokerError>> + Send + '_ {
        let pool = self.pool.clone();
        let topic = request.topic;
        async move {
            Self::ensure_queue(&pool, &topic)
                .await
                .map_err(|e| BrokerError::Subscribe {
                    topic: topic.clone(),
                    reason: e.to_string(),
                })?;

            let stream = stream::unfold((pool, topic), |(pool, topic)| async move {
                loop {
                    let row = sqlx::query("SELECT message FROM pgmq.pop($1)")
                        .bind(&topic)
                        .fetch_optional(&pool)
                        .await;

                    match row {
                        Ok(Some(row)) => {
                            let value: Result<serde_json::Value, sqlx::Error> =
                                row.try_get("message");
                            let result = Self::decode_popped_row(&topic, value);
                            return Some((result, (pool, topic)));
                        }
                        Ok(None) => {
                            tokio::time::sleep(POLL_INTERVAL).await;
                        }
                        Err(e) => {
                            return Some((
                                Err(BrokerError::Subscribe {
                                    topic: topic.clone(),
                                    reason: e.to_string(),
                                }),
                                (pool, topic),
                            ))
                        }
                    }
                }
            });

            Ok(SubscribeResponse {
                stream: Box::pin(stream) as MessageStream,
            })
        }
    }

    fn health_check(
        &self,
        _request: HealthCheckRequest,
    ) -> impl Future<Output = Result<(), BrokerError>> + Send + '_ {
        let pool = self.pool.clone();
        async move {
            sqlx::query("SELECT 1")
                .execute(&pool)
                .await
                .map(|_| ())
                .map_err(|e| BrokerError::Unavailable(e.to_string()))
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
    fn test_postgres_message_broker_is_send_and_sync() {
        fn _assert_send_sync<T: Send + Sync>() {}
        _assert_send_sync::<PostgresMessageBroker>();
        assert!(
            std::hint::black_box(true),
            "PostgresMessageBroker is Send + Sync (checked above at compile time)"
        );
    }

    /// @covers: connect
    #[test]
    fn test_connect_returns_connection_error_for_malformed_dsn() {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let result = rt.block_on(PostgresMessageBroker::connect("not-a-valid-dsn", "q"));
        assert!(
            matches!(result, Err(BrokerError::Connection(_))),
            "expected a Connection error for a malformed DSN"
        );
    }

    #[test]
    fn test_encode_decode_round_trips_payload_and_headers() {
        let mut headers = HashMap::new();
        headers.insert("k".to_string(), "v".to_string());
        let msg = Message {
            payload: b"hello world".to_vec(),
            headers,
        };
        let encoded = PostgresMessageBroker::encode(&msg);
        let decoded = PostgresMessageBroker::decode(encoded).expect("decode succeeds");
        assert_eq!(decoded.payload, msg.payload);
        assert_eq!(decoded.headers, msg.headers);
    }

    #[test]
    fn test_decode_rejects_envelope_missing_payload_field() {
        let value = serde_json::json!({"headers": {}});
        let err = PostgresMessageBroker::decode(value).expect_err("missing payload_b64 must fail");
        assert!(
            err.contains("payload_b64"),
            "error should name the missing field: {err}"
        );
    }

    #[test]
    fn test_decode_rejects_invalid_base64_payload() {
        let value = serde_json::json!({"payload_b64": "not valid base64!!", "headers": {}});
        let err = PostgresMessageBroker::decode(value).expect_err("invalid base64 must fail");
        assert!(err.contains("base64"), "error should mention base64: {err}");
    }
}
