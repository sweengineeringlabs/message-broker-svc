//! Integration tests for [`MessageBrokerFactory::create_config_builder`].

#![allow(clippy::unwrap_used, clippy::expect_used)]

use configbuilder::BuilderFinalizer;
use message_broker_svc_saf::MessageBrokerFactory;

/// @covers: create_config_builder — each call returns an independently usable
/// builder, not a shared/cached one that only works the first time.
#[test]
fn test_create_config_builder_each_call_returns_independently_usable_builder_edge() {
    let first = MessageBrokerFactory::create_config_builder()
        .build_loader()
        .is_ok();
    let second = MessageBrokerFactory::create_config_builder()
        .build_loader()
        .is_ok();
    assert_eq!(
        first, second,
        "two independently constructed builders must behave identically"
    );
    assert!(first, "both builders must produce a usable loader");
}
