# message-broker-svc-saf

`MessageBrokerFactory` and `TaskQueueFactory`: construction facades for every
backend this repo ships, selected by Cargo feature (`inmemory`/`nats`/`kafka`/
`postgres`; `TaskQueueFactory` has no `postgres` or no-op constructor). See
[Architecture](../../../../docs/3-design/architecture.md) for how backend
selection works and this repo's own history.
