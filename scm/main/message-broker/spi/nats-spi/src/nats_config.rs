//! [`NatsConfig`] — this crate's own local config type.
//!
//! Per this org's own precedent (`edge-llm`'s `provider/contract`): a contract
//! type never implements a foreign trait like `configbuilder::OptionalSection`
//! itself — that would be real implementation code, and the orphan rule means
//! no other crate could write it either, so the capability would be
//! unimplementable anywhere. A downstream adapter crate that needs
//! TOML-section loading defines its own local type instead. This is that type:
//! owned by this crate, not `message-broker-pattern`, implementing both
//! `configbuilder::OptionalSection` (real TOML-section loading) and
//! `message-broker-pattern`'s own [`Validator`] (the trait
//! [`crate::NatsMessageBroker::validator`] returns a handle to) — mapped back
//! to the contract, not a freestanding capability invented on the side.

use configbuilder::{ConfigError, FeatureMetadata, OptionalSection};
use message_broker_pattern::{ValidationError, ValidationRequest, Validator};

/// NATS backend configuration: the `[message_broker]` TOML section shape for
/// a NATS-backed deployment.
#[derive(Debug, Clone, PartialEq, Eq, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NatsConfig {
    /// NATS server URL (e.g. `"nats://host:4222"`). Required.
    pub url: String,
}

impl NatsConfig {
    fn validate_fields(&self) -> Result<(), String> {
        if self.url.trim().is_empty() {
            return Err(
                "nats backend requires a non-empty `url` (e.g. url = \"nats://host:4222\")"
                    .to_string(),
            );
        }
        Ok(())
    }
}

impl Validator for NatsConfig {
    fn validate(&self, _request: ValidationRequest) -> Result<(), ValidationError> {
        self.validate_fields().map_err(|reason| ValidationError {
            violations: vec![reason],
        })
    }
}

impl OptionalSection for NatsConfig {
    // @allow: no_stub_fn_bodies — returns the canonical section key, not a stub
    fn section_name() -> &'static str {
        "message_broker"
    }

    fn validate_enabled(&self) -> Result<(), ConfigError> {
        self.validate_fields()
            .map_err(|reason| ConfigError::Validation {
                section: Self::section_name().to_string(),
                reason,
            })
    }

    fn metadata() -> FeatureMetadata {
        FeatureMetadata {
            description: "NATS-backed message broker",
            owner: "platform-team",
            deprecated_since: None,
        }
    }
}
