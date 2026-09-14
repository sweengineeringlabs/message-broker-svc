# message-broker-svc-spi-shared

`ValidatorExt`: an extension trait over `message-broker-pattern`'s `Validator`,
giving every backend config type `validate_config()`/`validator_response()`
for free. Shared by the `kafka`/`nats`/`postgres` `*-spi` crates and `-saf`'s
own `NoopMessageBroker` — the one thing more than one `spi` genuinely shares.
No `{vo,error,dto,entity}` contract types live here; those stay in
`message-broker-pattern` exclusively.

See [Architecture](../../../../../docs/3-design/architecture.md) for the full
explanation.
