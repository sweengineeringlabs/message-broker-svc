//! Integration tests for [`message_broker_svc_spi_shared::ValidatorExt`].

use std::sync::Arc;

use message_broker_pattern::{BrokerError, ValidationError, ValidationRequest, Validator};
use message_broker_svc_spi_shared::ValidatorExt;

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

/// @covers: validate_config — a valid config passes through untouched.
#[test]
fn test_validate_config_ok_for_valid_config_happy() {
    assert!(matches!(AlwaysValid.validate_config(), Ok(())));
}

/// @covers: validate_config — an invalid config's violation text surfaces in
/// the returned error, not silently dropped.
#[test]
fn test_validate_config_err_for_invalid_config_error() {
    let result = AlwaysInvalid.validate_config();
    match result {
        Err(BrokerError::Connection(reason)) => {
            assert!(reason.contains("always invalid"), "reason was: {reason}");
        }
        other => panic!("expected BrokerError::Connection, got {other:?}"),
    }
}

/// @covers: validator_response — the returned handle actually delegates to
/// the wrapped config's own `validate`, not a stub that always says ok.
#[test]
fn test_validator_response_delegates_validate_to_wrapped_config_happy() {
    let response = Arc::new(AlwaysValid).validator_response();
    assert!(matches!(
        response.validator.validate(ValidationRequest),
        Ok(())
    ));
}

/// @covers: validator_response — an invalid wrapped config's violations
/// still surface through the type-erased handle.
#[test]
fn test_validator_response_delegates_validate_to_wrapped_config_error() {
    let response = Arc::new(AlwaysInvalid).validator_response();
    let result = response.validator.validate(ValidationRequest);
    match result {
        Err(ValidationError { violations }) => {
            assert_eq!(violations, vec!["always invalid".to_owned()]);
        }
        Ok(()) => panic!("expected the wrapped config's own validation failure to surface"),
    }
}
