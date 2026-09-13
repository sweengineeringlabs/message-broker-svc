//! [`PostgresConfig`] — this crate's own local config type.
//!
//! Per this org's own precedent (`edge-llm`'s `provider/contract`): a contract
//! type never implements a foreign trait like `configbuilder::OptionalSection`
//! itself. A downstream adapter crate that needs TOML-section loading defines
//! its own local type instead. This is that type: owned by this crate, not
//! `message-broker-pattern`, implementing both `configbuilder::OptionalSection`
//! and `message-broker-pattern`'s own [`Validator`].

use configbuilder::{ConfigError, FeatureMetadata, OptionalSection};
use message_broker_pattern::{ValidationError, ValidationRequest, Validator};

/// Postgres backend configuration: the `[message_broker]` TOML section shape
/// for a `pgmq`-backed deployment.
///
/// # Examples
///
/// ```toml
/// [message_broker]
/// url        = "postgres://user:pass@localhost/app"
/// queue_name = "edge_events"
/// ```
#[derive(Debug, Clone, PartialEq, Eq, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PostgresConfig {
    /// Postgres DSN (e.g. `"postgres://user:pass@host/db"`). Required.
    pub url: String,
    /// `pgmq` queue name. Required.
    pub queue_name: String,
}

impl PostgresConfig {
    fn validate_fields(&self) -> Result<(), String> {
        if self.url.trim().is_empty() {
            return Err("postgres backend requires a non-empty `url` \
                 (Postgres DSN, e.g. url = \"postgres://user:pass@host/db\")"
                .to_string());
        }
        if self.queue_name.trim().is_empty() {
            return Err("postgres backend requires a non-empty `queue_name`".to_string());
        }
        Ok(())
    }
}

impl Validator for PostgresConfig {
    fn validate(&self, _request: ValidationRequest) -> Result<(), ValidationError> {
        self.validate_fields().map_err(|reason| ValidationError {
            violations: vec![reason],
        })
    }
}

impl OptionalSection for PostgresConfig {
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
            description: "Postgres (pgmq)-backed message broker",
            owner: "platform-team",
            deprecated_since: None,
        }
    }
}
