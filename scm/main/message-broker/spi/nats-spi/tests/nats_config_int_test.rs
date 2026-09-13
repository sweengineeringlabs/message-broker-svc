//! Integration tests for [`NatsConfig`] as an `OptionalSection`.
//!
//! Exercises presence-based enabling, the `enabled = false` disable toggle,
//! `deny_unknown_fields` strictness, and cross-field validation.

#![allow(clippy::unwrap_used, clippy::expect_used)]

use configbuilder::{ConfigError, ConfigLoaderFactory, FeatureStateOps, OptionalSection};
use message_broker_svc_nats_spi::NatsConfig;
use tempfile::TempDir;

/// Write `content` to `application.toml` in a fresh temp dir and return a loader
/// rooted at that dir, along with the dir guard (kept alive by the caller).
fn loader_with(content: &str) -> (TempDir, configbuilder::SectionLoaderImpl) {
    let dir = TempDir::new().expect("create temp dir");
    std::fs::write(dir.path().join("application.toml"), content).expect("write application.toml");
    let loader = ConfigLoaderFactory::create_loader_for_dir(dir.path());
    (dir, loader)
}

/// @covers: section_name
#[test]
fn test_section_name_is_message_broker() {
    assert_eq!(NatsConfig::section_name(), "message_broker");
}

/// @covers: metadata
#[test]
fn test_metadata_describes_feature_and_owner() {
    let meta = NatsConfig::metadata();
    assert!(!meta.description.is_empty());
    assert_eq!(meta.owner, "platform-team");
    assert_eq!(meta.deprecated_since, None);
}

/// @covers: load_optional — an absent section resolves to Disabled, not an error.
#[test]
fn test_load_absent_section_returns_disabled() {
    let (_dir, loader) = loader_with("[unrelated]\nkey = \"value\"");
    let state = NatsConfig::load_optional(&loader).expect("absent section is not an error");
    assert!(state.is_disabled());
}

/// @covers: load_optional — presence of the section enables it; fields parse.
#[test]
fn test_load_with_url_returns_enabled() {
    let (_dir, loader) = loader_with("[message_broker]\nurl = \"nats://nats.internal:4222\"");
    let state = NatsConfig::load_optional(&loader).expect("valid section loads");
    let cfg = state.into_option().expect("section present => Enabled");
    assert_eq!(cfg.url, "nats://nats.internal:4222");
}

/// @covers: enabled = false — disables a section that is otherwise present.
#[test]
fn test_enabled_false_disables_present_section() {
    let (_dir, loader) = loader_with("[message_broker]\nurl = \"nats://x:4222\"\nenabled = false");
    let state = NatsConfig::load_optional(&loader).expect("enabled=false is not an error");
    assert!(state.is_disabled());
}

/// @covers: deny_unknown_fields — `enabled = true` is rejected as an unknown field.
#[test]
fn test_enabled_true_is_rejected_by_deny_unknown_fields() {
    let (_dir, loader) = loader_with("[message_broker]\nurl = \"nats://x:4222\"\nenabled = true");
    let err = NatsConfig::load_optional(&loader)
        .expect_err("enabled = true must be rejected by deny_unknown_fields");
    assert!(matches!(err, ConfigError::Parse(_)), "got {err:?}");
}

/// @covers: deny_unknown_fields — an arbitrary unknown key is rejected.
#[test]
fn test_unknown_field_is_rejected() {
    let (_dir, loader) = loader_with("[message_broker]\nurl = \"nats://x:4222\"\nbogus = 1");
    let err = NatsConfig::load_optional(&loader).expect_err("unknown field must be rejected");
    assert!(matches!(err, ConfigError::Parse(_)), "got {err:?}");
}

/// @covers: load_optional — a required field entirely missing fails to parse
/// (not a validation error — `url` is a plain `String`, not `Option<String>`).
#[test]
fn test_without_url_returns_parse_error() {
    let (_dir, loader) = loader_with("[message_broker]\n");
    let err = NatsConfig::load_optional(&loader).expect_err("missing url must fail to parse");
    assert!(matches!(err, ConfigError::Parse(_)), "got {err:?}");
}

/// @covers: validate_enabled — a blank url is rejected (not just None).
#[test]
fn test_with_blank_url_returns_validation_error() {
    let (_dir, loader) = loader_with("[message_broker]\nurl = \"   \"");
    let err = NatsConfig::load_optional(&loader).expect_err("blank url must fail validation");
    assert!(matches!(err, ConfigError::Validation { .. }), "got {err:?}");
}
