//! [`validate_config`] — the one shared "validate, then map to a construction
//! error" step every backend's constructor needs.

use message_broker_pattern::{BrokerError, ValidationRequest, Validator};

/// Validate `config` and map any violation into a [`BrokerError::Connection`].
///
/// Every backend's constructor (`NatsMessageBroker::connect`,
/// `KafkaMessageBroker::new`, `PostgresMessageBroker::connect`) calls this
/// once, right after building its own config type and before doing any I/O —
/// generic over anything implementing `message-broker-pattern`'s own
/// [`Validator`], so adding a new backend later means implementing `Validator`
/// on its own config type and calling this, not hand-rolling another check.
pub fn validate_config<C: Validator>(config: &C) -> Result<(), BrokerError> {
    config
        .validate(ValidationRequest)
        .map_err(|e| BrokerError::Connection(e.to_string()))
}
