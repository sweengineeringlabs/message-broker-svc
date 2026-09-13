//! [`NoopValidator`] — always-valid reference `Validator`, for backends (like
//! [`crate::noop_message_broker::NoopMessageBroker`]) with no config of their
//! own to validate.

use message_broker_pattern::{ValidationError, ValidationRequest, Validator};

/// Always-valid reference [`Validator`].
pub(crate) struct NoopValidator;

impl Validator for NoopValidator {
    fn validate(&self, _request: ValidationRequest) -> Result<(), ValidationError> {
        Ok(())
    }
}
