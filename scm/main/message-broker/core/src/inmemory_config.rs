//! [`InMemoryConfig`] — this crate's own local config type.
//!
//! Per this org's own precedent (`edge-llm`'s `provider/contract`): a contract
//! type never implements a foreign trait like `configbuilder::OptionalSection`
//! itself. A downstream adapter crate that needs TOML-section loading defines
//! its own local type instead. This is that type: owned by this crate, not
//! `message-broker-pattern`, implementing both `configbuilder::OptionalSection`
//! and `message-broker-pattern`'s own [`Validator`].
//!
//! Unlike `NatsConfig`/`KafkaConfig`/`PostgresConfig`, this backend takes no
//! runtime parameters at all -- `InMemoryConfig` has zero fields, so
//! `validate`/`validate_enabled` are trivially always `Ok`. It still exists
//! (rather than being skipped) for the same reason every other backend's
//! config exists: `MessageBroker::validator()` requires a return value, and
//! declaring `[message_broker]` as present-but-empty still has real, testable
//! meaning -- `deny_unknown_fields` still rejects any field this backend
//! doesn't define, and presence/absence still drives `OptionalSection`
//! enabling, exactly like the other three.

use configbuilder::{ConfigError, FeatureMetadata, OptionalSection};
use message_broker_pattern::{ValidationError, ValidationRequest, Validator};

/// In-memory backend configuration: the `[message_broker]` TOML section shape
/// for an in-process, no-external-dependency deployment. Carries no fields --
/// this backend takes no runtime parameters.
///
/// # Examples
///
/// ```toml
/// [message_broker]
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Default, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InMemoryConfig {}

impl Validator for InMemoryConfig {
    fn validate(&self, _request: ValidationRequest) -> Result<(), ValidationError> {
        Ok(())
    }
}

impl OptionalSection for InMemoryConfig {
    // @allow: no_stub_fn_bodies — returns the canonical section key, not a stub
    fn section_name() -> &'static str {
        "message_broker"
    }

    fn validate_enabled(&self) -> Result<(), ConfigError> {
        Ok(())
    }

    fn metadata() -> FeatureMetadata {
        FeatureMetadata {
            description: "In-memory (tokio::sync::broadcast)-backed message broker",
            owner: "platform-team",
            deprecated_since: None,
        }
    }
}
