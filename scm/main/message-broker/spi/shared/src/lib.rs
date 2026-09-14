//! `message_broker_svc_spi_shared` — implementation code shared by more than
//! one `*-spi` backend.
//!
//! Contract-level `{vo,error,dto,entity}` types belong exclusively to
//! `message-broker-pattern`, never here. This crate holds only the one thing
//! the `nats`/`kafka`/`postgres` backends (and `-saf`'s own `NoopMessageBroker`)
//! genuinely share: turning any `message-broker-pattern::Validator` impl into
//! a `BrokerError` check and into a type-erased [`ValidatorResponse`] handle.
//!
//! Rather than a free-standing helper struct consumer crates call into, this
//! is an extension trait over `Validator` itself, blanket-implemented for
//! every type that implements it -- program to the interface, not to the
//! consumer.

use std::sync::Arc;

use message_broker_pattern::{BrokerError, Validator, ValidatorResponse};

/// Extends every [`Validator`] impl with the config-check and
/// handle-construction behavior every `*-spi` backend needs.
pub trait ValidatorExt: Validator {
    /// Run this config's own [`Validator::validate`], mapping any violation
    /// into a [`BrokerError::Connection`] so callers can `?` it directly at
    /// connect time.
    fn validate_config(&self) -> Result<(), BrokerError> {
        self.validate(message_broker_pattern::ValidationRequest)
            .map_err(|e| BrokerError::Connection(e.to_string()))
    }

    /// Wrap this shared config handle as a type-erased [`ValidatorResponse`],
    /// for `MessageBroker::validator()` to return.
    fn validator_response(self: &Arc<Self>) -> ValidatorResponse
    where
        Self: Sized + 'static,
    {
        ValidatorResponse {
            validator: Arc::clone(self) as Arc<dyn Validator>,
        }
    }
}

impl<T: Validator> ValidatorExt for T {}
