# message-broker-svc-kafka-spi

`KafkaMessageBroker`: Apache Kafka-backed implementation of
`message-broker-pattern`'s `MessageBroker` trait, via `rdkafka`. (The
`KafkaTaskQueue` implementation that used to live here moved to
[`task-queue-svc`](https://github.com/sweengineeringlabs/task-queue-svc), SRP.)

See [Architecture](../../../../../docs/3-design/architecture.md) for backend
details and this repo's own history.
