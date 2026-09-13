//! [`KafkaConfig`] — this crate's own local config type.
//!
//! Per this org's own precedent (`edge-llm`'s `provider/contract`): a contract
//! type never implements a foreign trait like `configbuilder::OptionalSection`
//! itself. A downstream adapter crate that needs TOML-section loading defines
//! its own local type instead. This is that type: owned by this crate, not
//! `message-broker-pattern`, implementing both `configbuilder::OptionalSection`
//! and `message-broker-pattern`'s own [`Validator`].

use configbuilder::{ConfigError, FeatureMetadata, OptionalSection};
use message_broker_pattern::{ValidationError, ValidationRequest, Validator};

/// Kafka backend configuration: the `[message_broker]` TOML section shape for
/// a Kafka-backed deployment.
///
/// # Examples
///
/// ```toml
/// [message_broker]
/// url      = "kafka-broker-1:9092,kafka-broker-2:9092"
/// group_id = "my-service"
/// ```
#[derive(Debug, Clone, PartialEq, Eq, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct KafkaConfig {
    /// Comma-separated bootstrap brokers (e.g. `"broker1:9092,broker2:9092"`). Required.
    pub url: String,
    /// Consumer group identifier. Required.
    pub group_id: String,
}

impl KafkaConfig {
    fn validate_fields(&self) -> Result<(), String> {
        if self.url.trim().is_empty() {
            return Err("kafka backend requires a non-empty `url` \
                 (bootstrap brokers, e.g. url = \"broker1:9092,broker2:9092\")"
                .to_string());
        }
        if self.group_id.trim().is_empty() {
            return Err("kafka backend requires a non-empty `group_id`".to_string());
        }
        Ok(())
    }
}

impl Validator for KafkaConfig {
    fn validate(&self, _request: ValidationRequest) -> Result<(), ValidationError> {
        self.validate_fields().map_err(|reason| ValidationError {
            violations: vec![reason],
        })
    }
}

impl OptionalSection for KafkaConfig {
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
            description: "Kafka-backed message broker",
            owner: "platform-team",
            deprecated_since: None,
        }
    }
}
