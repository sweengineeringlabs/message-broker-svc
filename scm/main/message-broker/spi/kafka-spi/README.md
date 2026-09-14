# message-broker-svc-kafka-spi

`KafkaMessageBroker`/`KafkaTaskQueue`: Apache Kafka-backed implementations of
`message-broker-pattern`'s `MessageBroker` and `TaskQueue` traits, via `rdkafka`.

Extracted from `edge-runtime`'s `runtime-message-broker-kafka-spi` per
[edge-message-broker#6](https://github.com/sweengineeringlabs/edge-message-broker/issues/6).
`MessageBroker` was ported first; `TaskQueue` (and `LoggingConsumerContext`, which only
`KafkaTaskQueue` needs) followed once it became clear leaving `TaskQueue` behind in
`edge-runtime` was an incomplete migration, not a scope decision — see
`message-broker-pattern`'s own architecture doc for the full reasoning.
