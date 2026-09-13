//! Integration tests for [`PostgresConfig`] as an `OptionalSection`.

#![allow(clippy::unwrap_used, clippy::expect_used)]

use configbuilder::{ConfigError, ConfigLoaderFactory, FeatureStateOps, OptionalSection};
use message_broker_svc_postgres_spi::PostgresConfig;
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
    assert_eq!(PostgresConfig::section_name(), "message_broker");
}

/// @covers: load_optional — an absent section resolves to Disabled, not an error.
#[test]
fn test_load_absent_section_returns_disabled() {
    let (_dir, loader) = loader_with("[unrelated]\nkey = \"value\"");
    let state = PostgresConfig::load_optional(&loader).expect("absent section is not an error");
    assert!(state.is_disabled());
}

/// @covers: load_optional — url + queue_name enables and parses.
#[test]
fn test_load_with_url_and_queue_name_returns_enabled() {
    let (_dir, loader) = loader_with(
        "[message_broker]\nurl = \"postgres://localhost/app\"\nqueue_name = \"edge_events\"",
    );
    let state = PostgresConfig::load_optional(&loader).expect("valid section loads");
    let cfg = state.into_option().expect("section present => Enabled");
    assert_eq!(cfg.url, "postgres://localhost/app");
    assert_eq!(cfg.queue_name, "edge_events");
}

/// @covers: deny_unknown_fields — an arbitrary unknown key is rejected.
#[test]
fn test_unknown_field_is_rejected() {
    let (_dir, loader) = loader_with(
        "[message_broker]\nurl = \"postgres://localhost/app\"\nqueue_name = \"q\"\nbogus = 1",
    );
    let err = PostgresConfig::load_optional(&loader).expect_err("unknown field must be rejected");
    assert!(matches!(err, ConfigError::Parse(_)), "got {err:?}");
}

/// @covers: load_optional — a required field entirely missing fails to parse.
#[test]
fn test_without_url_returns_parse_error() {
    let (_dir, loader) = loader_with("[message_broker]\nqueue_name = \"edge_events\"");
    let err = PostgresConfig::load_optional(&loader).expect_err("missing url must fail to parse");
    assert!(matches!(err, ConfigError::Parse(_)), "got {err:?}");
}

/// @covers: load_optional — a required field entirely missing fails to parse.
#[test]
fn test_without_queue_name_returns_parse_error() {
    let (_dir, loader) = loader_with("[message_broker]\nurl = \"postgres://localhost/app\"");
    let err =
        PostgresConfig::load_optional(&loader).expect_err("missing queue_name must fail to parse");
    assert!(matches!(err, ConfigError::Parse(_)), "got {err:?}");
}

/// @covers: validate_enabled — a blank url (present but empty) is rejected.
#[test]
fn test_with_blank_url_returns_validation_error() {
    let (_dir, loader) = loader_with("[message_broker]\nurl = \"   \"\nqueue_name = \"q\"");
    let err = PostgresConfig::load_optional(&loader).expect_err("blank url must fail validation");
    assert!(matches!(err, ConfigError::Validation { .. }), "got {err:?}");
}
