//! [`validator_response`] — the one shared "hand back a type-erased validator
//! handle" step every backend's `MessageBroker::validator` needs.

use std::sync::Arc;

use message_broker_pattern::{Validator, ValidatorResponse};

/// Build a [`ValidatorResponse`] wrapping `config` as a type-erased
/// [`Arc<dyn Validator>`].
///
/// Every backend's `validator()` (`NatsMessageBroker`, `KafkaMessageBroker`,
/// `PostgresMessageBroker`, `NoopMessageBroker`) had the identical one-line
/// body before this existed — generic over anything implementing
/// `message-broker-pattern`'s own [`Validator`], so adding a new backend
/// later means calling this instead of retyping the same cast.
pub fn validator_response<C: Validator + 'static>(config: &Arc<C>) -> ValidatorResponse {
    ValidatorResponse {
        validator: Arc::clone(config) as Arc<dyn Validator>,
    }
}
