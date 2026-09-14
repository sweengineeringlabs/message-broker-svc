# message-broker-svc-postgres-spi

`PostgresMessageBroker`: `pgmq`-backed implementation of
`message-broker-pattern`'s `MessageBroker` trait, via `sqlx`. No `TaskQueue`
implementation — this backend never had one.

Requires the `pgmq` extension (`CREATE EXTENSION pgmq;`) installed on the target
database. Delivery is **queue** semantics, not broadcast.

See [Architecture](../../../../../docs/3-design/architecture.md) for the full
explanation.
