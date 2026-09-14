# message-broker-svc-saf

`MessageBrokerFactory`: construction facade for every `MessageBroker` backend
this repo ships, selected by Cargo feature (`inmemory`/`nats`/`kafka`/
`postgres`). See [Architecture](../../../../docs/3-design/architecture.md) for
how backend selection works and this repo's own history. (`TaskQueueFactory`
moved to [`task-queue-svc`](https://github.com/sweengineeringlabs/task-queue-svc), SRP.)
