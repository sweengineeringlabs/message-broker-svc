//! Integration tests for [`message_broker_svc_core::validate_config`].

use message_broker_pattern::{BrokerError, ValidationError, ValidationRequest, Validator};
use message_broker_svc_core::validate_config;

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
    assert!(matches!(validate_config(&AlwaysValid), Ok(())));
}

/// @covers: validate_config — an invalid config's violation text surfaces in
/// the returned error, not silently dropped.
#[test]
fn test_validate_config_err_for_invalid_config_error() {
    let result = validate_config(&AlwaysInvalid);
    match result {
        Err(BrokerError::Connection(reason)) => {
            assert!(reason.contains("always invalid"), "reason was: {reason}");
        }
        other => panic!("expected BrokerError::Connection, got {other:?}"),
    }
}
