# Documentation Index

**Audience**: All — architects, contributors, and consumers evaluating this crate family.

Docs for [`message-broker-svc`](https://github.com/sweengineeringlabs/message-broker-svc),
the concrete in-memory/NATS/Kafka/Postgres `MessageBroker` implementations, the
in-memory/NATS/Kafka `TaskQueue` implementations, and their construction facades
(`MessageBrokerFactory`/`TaskQueueFactory`).

| Section | What's there |
|---------|--------------|
| [0-ideation/papers](0-ideation/papers/README.md) | Prior-art papers, currently empty |
| [3-design](3-design/README.md) | Architecture, compliance checklist, and ADR: why this repo extracts from `edge-runtime`'s pilot, why `TaskQueue` lives here too, the one thing genuinely out of scope |
| [4-development](4-development/README.md) | Developer guide: repo structure, feature flags, live-infra tests |
| [glossary.md](glossary.md) | Domain terminology |

New to this repo? Start with [3-design/architecture.md](3-design/architecture.md).
