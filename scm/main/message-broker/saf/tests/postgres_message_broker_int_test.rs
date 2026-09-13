//! Feature-gated coverage of the `postgres` broker backend's error paths, via
//! `MessageBrokerFactory::postgres`/`from_config`.
//!
//! No live Postgres/`pgmq` instance is available in CI or local dev by default, so
//! most tests here exercise what is verifiable without one: connection/config error
//! paths. The `pgmq`-backed round-trip tests (send/pop against a real queue) at the
//! bottom of this file require a live Postgres with `CREATE EXTENSION pgmq;`
//! applied — they are `#[ignore]`d by default and run explicitly with
//! `POSTGRES_DSN=... cargo test --features postgres -- --include-ignored`.
//!
//! Direct-dep coverage for `sqlx` itself lives in
//! `message-broker-pattern-postgres-spi`'s own `tests/sqlx_int_test.rs`.

#![allow(clippy::unwrap_used, clippy::expect_used)]

#[cfg(feature = "postgres")]
mod postgres_feature {
    use message_broker_pattern_contract::BrokerError;
    use message_broker_pattern_core::MessageBrokerConfig;
    use message_broker_svc_saf::MessageBrokerFactory;

    /// @covers: MessageBrokerFactory::postgres
    /// Connecting to an unreachable Postgres host must fail with a Connection
    /// error, not panic or hang — no live server required (connection refused
    /// on an unused local port is immediate).
    #[test]
    fn test_postgres_connect_returns_connection_error_for_unreachable_host() {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let result = rt.block_on(MessageBrokerFactory::postgres(
            "postgres://user:pass@127.0.0.1:1/app",
            "edge_events",
        ));
        assert!(
            matches!(result, Err(BrokerError::Connection(_))),
            "expected a Connection error for an unreachable Postgres host"
        );
    }

    /// @covers: MessageBrokerFactory::from_config
    /// `from_config` with `BackendKind::Postgres` and no `url` must fail fast
    /// with a Connection error rather than attempting to connect with an empty DSN.
    #[test]
    fn test_from_config_postgres_without_url_returns_connection_error() {
        use message_broker_pattern_contract::BackendKind;
        let config = MessageBrokerConfig {
            backend: BackendKind::Postgres,
            url: None,
            group_id: None,
            queue_name: Some("edge_events".into()),
        };
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let result = rt.block_on(MessageBrokerFactory::from_config(&config));
        assert!(
            matches!(result, Err(BrokerError::Connection(_))),
            "expected a Connection error when postgres backend has no url"
        );
    }

    /// @covers: MessageBrokerFactory::from_config
    /// `from_config` with `BackendKind::Postgres` and no `queue_name` must fail
    /// fast with a Connection error.
    #[test]
    fn test_from_config_postgres_without_queue_name_returns_connection_error() {
        use message_broker_pattern_contract::BackendKind;
        let config = MessageBrokerConfig {
            backend: BackendKind::Postgres,
            url: Some("postgres://user:pass@127.0.0.1:1/app".into()),
            group_id: None,
            queue_name: None,
        };
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let result = rt.block_on(MessageBrokerFactory::from_config(&config));
        assert!(
            matches!(result, Err(BrokerError::Connection(_))),
            "expected a Connection error when postgres backend has no queue_name"
        );
    }

    // ── Live-pgmq tests ──────────────────────────────────────────────────────
    //
    // Run with a real Postgres + `pgmq` instance:
    //
    //   POSTGRES_DSN=postgres://postgres:postgres@localhost:5433/edge_test \
    //     cargo test --features postgres -- \
    //     --include-ignored --test postgres_message_broker_int_test
    //
    // All tests below are ignored unless explicitly included so normal CI
    // (without a Postgres+pgmq sidecar) still passes green.

    /// Returns the DSN from POSTGRES_DSN, or panics with a clear message.
    fn require_postgres_dsn() -> String {
        std::env::var("POSTGRES_DSN").expect(
            "POSTGRES_DSN env var must be set to run live-pgmq tests \
             (e.g. postgres://postgres:postgres@localhost:5433/edge_test)",
        )
    }

    /// @covers: publish + subscribe — happy-path roundtrip against a live
    /// `pgmq` queue.
    #[tokio::test]
    #[ignore = "requires-postgres"]
    async fn test_publish_subscribe_roundtrip_with_live_pgmq() {
        use std::sync::Arc;

        use futures::StreamExt as _;
        use message_broker_pattern_contract::{
            Message, MessageBroker as _, PublishRequest, SubscribeRequest,
        };

        let dsn = require_postgres_dsn();
        let queue = "swe_edge_test_pub_sub_roundtrip";
        let payload = b"live-pgmq-payload";

        let broker = MessageBrokerFactory::postgres(&dsn, queue)
            .await
            .expect("broker construction must succeed against a live Postgres+pgmq instance");

        let mut stream = broker
            .subscribe(SubscribeRequest {
                topic: queue.to_string(),
            })
            .await
            .expect("subscribe must succeed against a live pgmq queue")
            .stream;

        broker
            .publish(PublishRequest {
                topic: queue.to_string(),
                message: Arc::new(Message::new(payload.as_ref())),
            })
            .await
            .expect("publish must succeed against a live pgmq queue");

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

    /// @covers: publish + subscribe — headers survive the JSONB round-trip.
    #[tokio::test]
    #[ignore = "requires-postgres"]
    async fn test_publish_subscribe_roundtrip_preserves_headers_with_live_pgmq() {
        use std::sync::Arc;

        use futures::StreamExt as _;
        use message_broker_pattern_contract::{
            Message, MessageBroker as _, PublishRequest, SubscribeRequest,
        };

        let dsn = require_postgres_dsn();
        let queue = "swe_edge_test_pub_sub_headers";

        let broker = MessageBrokerFactory::postgres(&dsn, queue)
            .await
            .expect("broker construction must succeed against a live Postgres+pgmq instance");

        let mut stream = broker
            .subscribe(SubscribeRequest {
                topic: queue.to_string(),
            })
            .await
            .expect("subscribe must succeed against a live pgmq queue")
            .stream;

        let msg = Message::with_headers(
            b"payload-with-headers".as_ref(),
            [("trace-id".to_string(), "abc123".to_string())].into(),
        );

        broker
            .publish(PublishRequest {
                topic: queue.to_string(),
                message: Arc::new(msg),
            })
            .await
            .expect("publish must succeed against a live pgmq queue");

        let received = tokio::time::timeout(std::time::Duration::from_secs(10), stream.next())
            .await
            .expect("message must arrive within 10 s")
            .expect("stream must not end before yielding a message")
            .expect("stream item must not be an error");

        assert_eq!(
            received.headers.get("trace-id").map(String::as_str),
            Some("abc123"),
            "headers must survive the pgmq JSONB round-trip"
        );
    }

    /// @covers: publish + subscribe — durability across a process restart.
    #[tokio::test]
    #[ignore = "requires-postgres"]
    async fn test_message_survives_broker_process_restart_with_live_pgmq() {
        use std::sync::Arc;

        use futures::StreamExt as _;
        use message_broker_pattern_contract::{
            Message, MessageBroker as _, PublishRequest, SubscribeRequest,
        };

        let dsn = require_postgres_dsn();
        let queue = "swe_edge_test_survives_restart";
        let payload = b"survives-restart-payload";

        {
            let producer = MessageBrokerFactory::postgres(&dsn, queue)
                .await
                .expect("broker construction must succeed against a live Postgres+pgmq instance");

            producer
                .publish(PublishRequest {
                    topic: queue.to_string(),
                    message: Arc::new(Message::new(payload.as_ref())),
                })
                .await
                .expect("publish must succeed against a live pgmq queue");

            // Dropped here — closes this instance's connection pool, standing
            // in for the producing process exiting before anyone consumes.
        }

        let consumer = MessageBrokerFactory::postgres(&dsn, queue)
            .await
            .expect("broker construction must succeed against a live Postgres+pgmq instance");

        let mut stream = consumer
            .subscribe(SubscribeRequest {
                topic: queue.to_string(),
            })
            .await
            .expect("subscribe must succeed against a live pgmq queue")
            .stream;

        let received = tokio::time::timeout(std::time::Duration::from_secs(10), stream.next())
            .await
            .expect("message must arrive within 10 s")
            .expect("stream must not end before yielding a message")
            .expect("stream item must not be an error");

        assert_eq!(
            received.payload.as_slice(),
            payload,
            "message sent by a now-dropped broker instance must still be readable by a fresh one"
        );
    }
}

#[cfg(not(feature = "postgres"))]
mod postgres_feature_disabled {
    use message_broker_pattern_contract::{BackendKind, BrokerError};
    use message_broker_pattern_core::MessageBrokerConfig;
    use message_broker_svc_saf::MessageBrokerFactory;

    /// @covers: MessageBrokerFactory::from_config
    /// Without the `postgres` feature compiled in, selecting the Postgres
    /// backend must fail with Unavailable, not panic or silently no-op.
    #[test]
    fn test_from_config_postgres_without_feature_returns_unavailable() {
        let config = MessageBrokerConfig {
            backend: BackendKind::Postgres,
            url: Some("postgres://user:pass@127.0.0.1:1/app".into()),
            group_id: None,
            queue_name: Some("edge_events".into()),
        };
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let result = rt.block_on(MessageBrokerFactory::from_config(&config));
        assert!(
            matches!(result, Err(BrokerError::Unavailable(_))),
            "expected Unavailable when the postgres feature is not compiled in"
        );
    }
}
