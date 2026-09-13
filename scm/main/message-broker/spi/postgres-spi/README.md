# message-broker-svc-postgres-spi

`PostgresMessageBroker`: `pgmq`-backed implementation of
`message-broker-pattern-contract`'s `MessageBroker` trait, via `sqlx`.

Requires the `pgmq` extension (`CREATE EXTENSION pgmq;`) installed on the target
database. Delivery is **queue** semantics, not broadcast — see the doc comment on
`PostgresMessageBroker` for the full explanation.

Extracted from `edge-runtime`'s `runtime-message-broker-postgres-spi` per
[edge-message-broker#6](https://github.com/sweengineeringlabs/edge-message-broker/issues/6).
