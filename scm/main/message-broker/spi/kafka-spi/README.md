# message-broker-svc-kafka-spi

`KafkaMessageBroker`: Apache Kafka-backed implementation of
`message-broker-pattern-contract`'s `MessageBroker` trait, via `rdkafka`.

Extracted from `edge-runtime`'s `runtime-message-broker-kafka-spi` (`MessageBroker`-only —
its `TaskQueue` implementation and `LoggingConsumerContext` were not ported, since neither
is referenced by the `MessageBroker` impl) per
[edge-message-broker#6](https://github.com/sweengineeringlabs/edge-message-broker/issues/6).
