//! Integration tests for [`InMemoryConfig`] as an `OptionalSection`.
//!
//! Exercises presence-based enabling, the `enabled = false` disable toggle,
//! and `deny_unknown_fields` strictness -- there is no cross-field validation
//! to exercise since this config carries no fields.

#![allow(clippy::unwrap_used, clippy::expect_used)]

use configbuilder::{ConfigError, ConfigLoaderFactory, FeatureStateOps, OptionalSection};
use message_broker_svc_inmemory_spi::InMemoryConfig;
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
    assert_eq!(InMemoryConfig::section_name(), "message_broker");
}

/// @covers: metadata
#[test]
fn test_metadata_describes_feature_and_owner() {
    let meta = InMemoryConfig::metadata();
    assert!(!meta.description.is_empty());
    assert_eq!(meta.owner, "platform-team");
    assert_eq!(meta.deprecated_since, None);
}

/// @covers: load_optional — an absent section resolves to Disabled, not an error.
#[test]
fn test_load_absent_section_returns_disabled() {
    let (_dir, loader) = loader_with("[unrelated]\nkey = \"value\"");
    let state = InMemoryConfig::load_optional(&loader).expect("absent section is not an error");
    assert!(state.is_disabled());
}

/// @covers: load_optional — presence of an empty section enables it, since
/// this config carries no required fields.
#[test]
fn test_load_present_empty_section_returns_enabled() {
    let (_dir, loader) = loader_with("[message_broker]\n");
    let state = InMemoryConfig::load_optional(&loader).expect("empty section loads");
    assert!(state.into_option().is_some(), "section present => Enabled");
}

/// @covers: enabled = false — disables a section that is otherwise present.
#[test]
fn test_enabled_false_disables_present_section() {
    let (_dir, loader) = loader_with("[message_broker]\nenabled = false");
    let state = InMemoryConfig::load_optional(&loader).expect("enabled=false is not an error");
    assert!(state.is_disabled());
}

/// @covers: deny_unknown_fields — `enabled = true` is rejected as an unknown field.
#[test]
fn test_enabled_true_is_rejected_by_deny_unknown_fields() {
    let (_dir, loader) = loader_with("[message_broker]\nenabled = true");
    let err = InMemoryConfig::load_optional(&loader)
        .expect_err("enabled = true must be rejected by deny_unknown_fields");
    assert!(matches!(err, ConfigError::Parse(_)), "got {err:?}");
}

/// @covers: deny_unknown_fields — an arbitrary unknown key is rejected, since
/// this backend defines no fields at all.
#[test]
fn test_unknown_field_is_rejected() {
    let (_dir, loader) = loader_with("[message_broker]\nbogus = 1");
    let err = InMemoryConfig::load_optional(&loader).expect_err("unknown field must be rejected");
    assert!(matches!(err, ConfigError::Parse(_)), "got {err:?}");
}
