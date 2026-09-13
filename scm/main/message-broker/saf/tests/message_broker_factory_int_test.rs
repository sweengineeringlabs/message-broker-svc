//! Integration tests for [`MessageBrokerFactory`].
#![allow(clippy::unwrap_used, clippy::expect_used)]

use configbuilder::{BuilderFinalizer, FeatureStateOps, OptionalSection};
use message_broker_pattern_core::MessageBrokerConfig;
use message_broker_svc_saf::MessageBrokerFactory;

/// @covers: MessageBrokerFactory::create_config_builder — the built loader is
/// genuinely usable, not just non-erroring: loading an absent
/// `[message_broker]` section through it resolves to Disabled rather than
/// panicking or silently enabling.
#[test]
fn test_message_broker_factory_create_config_builder_is_pre_seeded() {
    let loader = MessageBrokerFactory::create_config_builder()
        .build_loader()
        .expect("builder must construct a valid loader");
    let state = MessageBrokerConfig::load_optional(&loader)
        .expect("an absent section must resolve to Disabled, not an error");
    assert!(
        state.is_disabled(),
        "a builder with no configured directories must resolve every section to Disabled"
    );
}

/// @covers: MessageBrokerFactory::from_config — in_memory is not this factory's job.
#[tokio::test]
async fn test_from_config_in_memory_returns_unavailable() {
    use message_broker_pattern_contract::{BackendKind, BrokerError};
    use message_broker_pattern_core::MessageBrokerConfig;

    let config = MessageBrokerConfig {
        backend: BackendKind::InMemory,
        url: None,
        group_id: None,
        queue_name: None,
    };
    let result = MessageBrokerFactory::from_config(&config).await;
    assert!(
        matches!(result, Err(BrokerError::Unavailable(_))),
        "in_memory must be rejected — it belongs to message-broker-pattern-saf::BrokerSvc"
    );
}
