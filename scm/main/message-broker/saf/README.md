# message-broker-svc-saf

`MessageBrokerFactory`: construction facade for the no-op reference broker and
the `inmemory`/`nats`/`kafka`/`postgres` `MessageBroker` backends, selected by
Cargo feature — no runtime config-driven dispatch (`ApplicationConfig`/
`BrokerProvider`/`from_config` were never ported; they're `edge-runtime`-specific
composition concerns, not part of this domain's contract).

`TaskQueueFactory`: the equivalent construction facade for the
`inmemory`/`nats`/`kafka` `TaskQueue` backends. No `postgres` constructor and no
no-op reference — neither exists for `TaskQueue` in this domain.

Extracted from `edge-runtime`'s `runtime-message-broker-saf` per
[edge-message-broker#6](https://github.com/sweengineeringlabs/edge-message-broker/issues/6).
`MessageBrokerFactory` was ported first, `inmemory` initially missing and later
restored; `TaskQueueFactory` followed once it became clear leaving `TaskQueue`
out of this repo entirely was an incomplete migration, not a scope decision —
see this repo's own architecture doc.
