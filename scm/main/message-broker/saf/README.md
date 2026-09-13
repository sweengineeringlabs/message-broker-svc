# message-broker-svc-saf

`MessageBrokerFactory`: the construction/dispatch facade selecting among the
`nats`/`kafka`/`postgres` `MessageBroker` backends, by Cargo feature or via
`from_config`/`MessageBrokerConfig`.

Extracted from `edge-runtime`'s `runtime-message-broker-saf` (trimmed to
`MessageBroker` dispatch only — the `in_memory`/`tokio-rt` path, `ApplicationConfig`,
and `BrokerProvider` were not ported; see this repo's top-level README's Scope note)
per [edge-message-broker#6](https://github.com/sweengineeringlabs/edge-message-broker/issues/6).
