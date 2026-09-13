//! Integration tests for [`KafkaConfig`] as an `OptionalSection`.

#![allow(clippy::unwrap_used, clippy::expect_used)]

use configbuilder::{ConfigError, ConfigLoaderFactory, FeatureStateOps, OptionalSection};
use message_broker_svc_kafka_spi::KafkaConfig;
use tempfile::TempDir;

fn loader_with(content: &str) -> (TempDir, configbuilder::SectionLoaderImpl) {
    let dir = TempDir::new().expect("create temp dir");
    std::fs::write(dir.path().join("application.toml"), content).expect("write application.toml");
    let loader = ConfigLoaderFactory::create_loader_for_dir(dir.path());
    (dir, loader)
}

/// @covers: section_name
#[test]
fn test_section_name_is_message_broker() {
    assert_eq!(KafkaConfig::section_name(), "message_broker");
}

/// @covers: load_optional — an absent section resolves to Disabled, not an error.
#[test]
fn test_load_absent_section_returns_disabled() {
    let (_dir, loader) = loader_with("[unrelated]\nkey = \"value\"");
    let state = KafkaConfig::load_optional(&loader).expect("absent section is not an error");
    assert!(state.is_disabled());
}

/// @covers: load_optional — url + group_id enables and parses.
#[test]
fn test_load_with_url_and_group_id_returns_enabled() {
    let (_dir, loader) =
        loader_with("[message_broker]\nurl = \"broker:9092\"\ngroup_id = \"workers\"");
    let state = KafkaConfig::load_optional(&loader).expect("valid section loads");
    let cfg = state.into_option().expect("section present => Enabled");
    assert_eq!(cfg.url, "broker:9092");
    assert_eq!(cfg.group_id, "workers");
}

/// @covers: deny_unknown_fields — an arbitrary unknown key is rejected.
#[test]
fn test_unknown_field_is_rejected() {
    let (_dir, loader) =
        loader_with("[message_broker]\nurl = \"broker:9092\"\ngroup_id = \"w\"\nbogus = 1");
    let err = KafkaConfig::load_optional(&loader).expect_err("unknown field must be rejected");
    assert!(matches!(err, ConfigError::Parse(_)), "got {err:?}");
}

/// @covers: load_optional — a required field entirely missing fails to parse.
#[test]
fn test_without_url_returns_parse_error() {
    let (_dir, loader) = loader_with("[message_broker]\ngroup_id = \"workers\"");
    let err = KafkaConfig::load_optional(&loader).expect_err("missing url must fail to parse");
    assert!(matches!(err, ConfigError::Parse(_)), "got {err:?}");
}

/// @covers: load_optional — a required field entirely missing fails to parse.
#[test]
fn test_without_group_id_returns_parse_error() {
    let (_dir, loader) = loader_with("[message_broker]\nurl = \"broker:9092\"");
    let err = KafkaConfig::load_optional(&loader).expect_err("missing group_id must fail to parse");
    assert!(matches!(err, ConfigError::Parse(_)), "got {err:?}");
}

/// @covers: validate_enabled — a blank url (present but empty) is rejected.
#[test]
fn test_with_blank_url_returns_validation_error() {
    let (_dir, loader) = loader_with("[message_broker]\nurl = \"   \"\ngroup_id = \"workers\"");
    let err = KafkaConfig::load_optional(&loader).expect_err("blank url must fail validation");
    assert!(matches!(err, ConfigError::Validation { .. }), "got {err:?}");
}
