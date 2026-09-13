# ADR-001: Extract `spi`/`saf` from `edge-runtime`'s in-tree pilot, `MessageBroker`-only

**Status**: Accepted
**Date**: 2026-09-13
**Deciders**: Amu Hlongwane

## Context

`edge-message-broker#6` decided to split that repo's single-crate contract into a
`message-broker-pattern` repo (generic contract + reference core, mirroring
`wasm-capability-pattern`) and this repo, `message-broker-svc` (concrete NATS/Kafka/Postgres
backends, mirroring `wasm-capability-svc`).

`edge-runtime#96` (its own EPIC to migrate to a domain-first `{contract,core,spi,saf}`
layout) had already piloted exactly this shape for message-broker *in-tree*, at
`scm/main/message-broker/{contract,core,spi/{kafka,nats,postgres},saf}` (commits
`ec34244`, `16ec508`) — six crates: `runtime-message-broker-{contract,core,saf}` and
`runtime-message-broker-{kafka,nats,postgres}-spi`. That pilot's own `contract` crate
depends on `edge-message-broker` (`swe-edge-message-broker`, git tag `v0.3.8`) as a
superset, adding `TaskQueue`/`Task`/`TaskHandle` on top of this org's `MessageBroker`
contract, and was never tagged/published — `ec34244`'s own commit body says so
explicitly.

## Decision

Extract the `MessageBroker`-only portion of that pilot into this repo, rather than
rebuild from scratch:

- `runtime-message-broker-{nats,kafka,postgres}-spi` → this repo's
  `message-broker-svc-{nats,kafka,postgres}-spi`, each renamed to depend on
  `message-broker-pattern-contract`/`-core` (the plain `MessageBroker` contract) instead
  of `runtime-message-broker-contract` (the `TaskQueue`-superset one).
- `runtime-message-broker-saf` → this repo's `message-broker-svc-saf`, trimmed to
  `MessageBrokerFactory`'s `MessageBroker` dispatch only.

Each provider crate's source split cleanly along an existing seam: `kafka_message_broker.rs`/
`nats_message_broker.rs` (`MessageBroker` impl) were already separate files from
`kafka_task_queue.rs`/`nats_task_queue.rs` (`TaskQueue` impl), with no shared state between
them (only `kafka`'s `constants.rs` had one `TaskQueue`-only constant mixed in, dropped
during extraction). Postgres never had a `TaskQueue` implementation to split out.

**Not ported, and deliberately so** (see `architecture.md`'s Scope boundary section for
the reasoning): `TaskQueue`, `ApplicationConfig`/`BrokerProvider`, and the real
`tokio::sync::broadcast`-backed in-memory broker (`runtime-message-broker-core`).

## Consequences

- `edge-runtime#96`'s own message-broker pilot scope needs updating to point at this
  repo's published crates instead of keeping a second, divergent in-tree copy — tracked
  via `edge-message-broker#6`'s E3, not resolved by this ADR.
- Interim dependency on `message-broker-pattern-contract`/`-core` is via git (`branch =
  "dev"`), not a crates.io version or git tag — neither has been cut yet. Follow-up: pin
  to a tag once `message-broker-pattern` cuts one, and again once/if these crates publish
  to crates.io.
- A consumer that needs `TaskQueue` semantics still depends on `edge-runtime`'s own
  crates for that; this repo does not attempt to serve that need.

## Related patterns

- **Strangler extraction** — lifting a bounded slice of a larger, in-tree system into its
  own repo without disturbing what's left behind, rather than a big-bang rewrite.

[← Docs index](../../README.md)
