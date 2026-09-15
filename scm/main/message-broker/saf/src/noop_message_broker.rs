//! [`NoopMessageBroker`] — the reference no-op broker.
//!
//! Publishing discards the message; subscribing yields an immediately-empty
//! stream. Implements `MessageBroker` using only what
//! `message-broker-pattern` itself declares — no external technology. Lives
//! here, in `-saf`, rather than in the contract: it is still a real
//! implementation, and the contract has zero implementations of its own
//! traits, none excepted (see `message-broker-pattern`'s ADR-001 amendment).

use std::sync::Arc;

use message_broker_pattern::{
    BrokerError, HealthCheckRequest, MessageBroker, MessageStream, PublishRequest,
    SubscribeRequest, SubscribeResponse, Validator, ValidatorRequest, ValidatorResponse,
};

use crate::noop_validator::NoopValidator;

/// Freshly constructed each call: [`NoopMessageBroker`] is a zero-sized unit
/// struct with no config field to hold and clone, unlike the real backends
/// (`Arc<their-own-Config>`) — see `message-broker-pattern`'s
/// [`Validator::validator_response`], the same default trait method every
/// backend uses.
fn noop_validator_handle() -> Arc<NoopValidator> {
    Arc::new(NoopValidator)
}

/// No-op [`MessageBroker`]: `publish` succeeds without delivery, `subscribe`
/// returns an empty stream, `health_check` always reports healthy.
///
/// `pub`, not `pub(crate)`: it's a variant payload of the public
/// [`crate::AnyMessageBroker`] enum, so it must be at least as visible as
/// that enum itself. Still unreachable to construct from outside this
/// crate -- there is no public constructor, only
/// [`crate::MessageBrokerFactory::noop`], which returns it already wrapped.
pub struct NoopMessageBroker;

impl MessageBroker for NoopMessageBroker {
    async fn publish(&self, _request: PublishRequest) -> Result<(), BrokerError> {
        Ok(())
    }

    async fn subscribe(
        &self,
        _request: SubscribeRequest,
    ) -> Result<SubscribeResponse, BrokerError> {
        let stream: MessageStream = Box::pin(futures::stream::empty());
        Ok(SubscribeResponse { stream })
    }

    async fn health_check(&self, _request: HealthCheckRequest) -> Result<(), BrokerError> {
        Ok(())
    }

    fn validator(&self, _request: ValidatorRequest) -> Result<ValidatorResponse, BrokerError> {
        Ok(noop_validator_handle().validator_response())
    }
}
