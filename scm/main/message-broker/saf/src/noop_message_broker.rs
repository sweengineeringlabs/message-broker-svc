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
    BrokerError, BrokerFuture, HealthCheckRequest, MessageBroker, MessageStream, PublishRequest,
    SubscribeRequest, SubscribeResponse, ValidatorRequest, ValidatorResponse,
};
use message_broker_svc_spi_shared::ValidatorExt;

use crate::noop_validator::NoopValidator;

/// Freshly constructed each call: [`NoopMessageBroker`] is a zero-sized unit
/// struct with no config field to hold and clone, unlike the real backends
/// (`Arc<their-own-Config>`) — see `message-broker-svc-spi-shared`'s
/// [`ValidatorExt::validator_response`], the same shared extension trait
/// every backend uses.
fn noop_validator_handle() -> Arc<NoopValidator> {
    Arc::new(NoopValidator)
}

/// No-op [`MessageBroker`]: `publish` succeeds without delivery, `subscribe`
/// returns an empty stream, `health_check` always reports healthy.
pub(crate) struct NoopMessageBroker;

impl MessageBroker for NoopMessageBroker {
    fn publish<'a>(
        &'a self,
        _request: PublishRequest,
    ) -> BrokerFuture<'a, Result<(), BrokerError>> {
        BrokerFuture::new(async { Ok(()) })
    }

    fn subscribe<'a>(
        &'a self,
        _request: SubscribeRequest,
    ) -> BrokerFuture<'a, Result<SubscribeResponse, BrokerError>> {
        BrokerFuture::new(async {
            let stream: MessageStream = Box::pin(futures::stream::empty());
            Ok(SubscribeResponse { stream })
        })
    }

    fn health_check(
        &self,
        _request: HealthCheckRequest,
    ) -> BrokerFuture<'_, Result<(), BrokerError>> {
        BrokerFuture::new(async { Ok(()) })
    }

    fn validator(&self, _request: ValidatorRequest) -> Result<ValidatorResponse, BrokerError> {
        Ok(noop_validator_handle().validator_response())
    }
}
