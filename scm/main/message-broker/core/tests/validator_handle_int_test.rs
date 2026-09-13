//! Integration tests for [`message_broker_svc_core::validator_response`].

use std::sync::Arc;

use message_broker_pattern::{ValidationError, ValidationRequest, Validator};
use message_broker_svc_core::validator_response;

struct AlwaysValid;

impl Validator for AlwaysValid {
    fn validate(&self, _request: ValidationRequest) -> Result<(), ValidationError> {
        Ok(())
    }
}

struct AlwaysInvalid;

impl Validator for AlwaysInvalid {
    fn validate(&self, _request: ValidationRequest) -> Result<(), ValidationError> {
        Err(ValidationError {
            violations: vec!["always invalid".to_owned()],
        })
    }
}

/// @covers: validator_response — the returned handle actually delegates to
/// the wrapped config's own `validate`, not a stub that always says ok.
#[test]
fn test_validator_response_delegates_validate_to_wrapped_config_happy() {
    let response = validator_response(&Arc::new(AlwaysValid));
    assert!(matches!(
        response.validator.validate(ValidationRequest),
        Ok(())
    ));
}

/// @covers: validator_response — an invalid wrapped config's violations
/// still surface through the type-erased handle.
#[test]
fn test_validator_response_delegates_validate_to_wrapped_config_error() {
    let response = validator_response(&Arc::new(AlwaysInvalid));
    let result = response.validator.validate(ValidationRequest);
    match result {
        Err(ValidationError { violations }) => {
            assert_eq!(violations, vec!["always invalid".to_owned()]);
        }
        Ok(()) => panic!("expected the wrapped config's own validation failure to surface"),
    }
}
