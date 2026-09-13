//! Integration tests for [`MessageBrokerFactory`]'s no-op broker and `validate`.

#![allow(clippy::unwrap_used, clippy::expect_used)]

use message_broker_pattern::HealthCheckRequest;
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

/// @covers: validate — delegates to the value's own Validator::validate
#[test]
fn test_validate_ok_for_valid_type_happy() {
    use message_broker_pattern::{ValidationError, ValidationRequest, Validator};

    struct Valid;
    impl Validator for Valid {
        fn validate(&self, _request: ValidationRequest) -> Result<(), ValidationError> {
            Ok(())
        }
    }
    assert_eq!(MessageBrokerFactory::validate(&Valid), Ok(()));
}

/// @covers: validate — returns err for an invalid type
#[test]
fn test_validate_err_for_invalid_type_error() {
    use message_broker_pattern::{ValidationError, ValidationRequest, Validator};

    struct Invalid;
    impl Validator for Invalid {
        fn validate(&self, _request: ValidationRequest) -> Result<(), ValidationError> {
            Err(ValidationError {
                violations: vec!["bad state".to_string()],
            })
        }
    }
    let result = MessageBrokerFactory::validate(&Invalid);
    assert_eq!(
        result,
        Err(ValidationError {
            violations: vec!["bad state".to_string()],
        })
    );
}
