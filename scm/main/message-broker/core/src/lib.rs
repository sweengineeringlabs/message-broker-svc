//! `message_broker_svc_core` — generic implementation code shared by every
//! `*-spi` backend.
//!
//! Exploits `message-broker-pattern`'s own trait bounds directly (`Validator`),
//! the same way `ledger`'s own `adapter/replication` crate is generic over
//! `LedgerPayload` without living inside `ledger-base-port`: the pattern
//! declares the trait, this crate is the one place that trait bound gets a
//! real, reusable implementation, and every `*-spi` crate depends on this one
//! instead of duplicating the same logic three times. `message-broker-pattern`
//! itself stays exactly `traits`/`vo`/`dto`/`error`/`types` — zero
//! implementation, no exceptions; this crate is where "forced to implement"
//! becomes real.

mod config_validation;
mod validator_handle;

pub use config_validation::validate_config;
pub use validator_handle::validator_response;
