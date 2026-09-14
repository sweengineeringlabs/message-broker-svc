//! Kafka-specific tuning constants — this crate's own concern.

/// Kafka producer `message.timeout.ms` — maximum time to wait for delivery acknowledgement.
pub(crate) const KAFKA_MESSAGE_TIMEOUT_MS: &str = "5000";

/// Kafka consumer `session.timeout.ms` — broker considers consumer dead after this interval.
pub(crate) const KAFKA_SESSION_TIMEOUT_MS: &str = "6000";

/// Kafka health-check metadata fetch timeout in seconds.
pub(crate) const KAFKA_HEALTH_CHECK_TIMEOUT_SECS: u64 = 5;

/// Kafka subscribe channel capacity — bounds the in-memory buffer between the
/// rdkafka poll loop and the returned `MessageStream`.
///
/// When the channel is full the poll loop yield-waits on `send`, slowing Kafka
/// consumption and applying natural backpressure to slow subscribers.
pub(crate) const KAFKA_SUBSCRIBE_CHANNEL_CAPACITY: usize = 1024;

/// Idle-topic check interval for the broker subscriber poll loop (seconds).
///
/// A subscriber poll loop blocks on the underlying client's receive call, which
/// can wait indefinitely on an idle topic. Bounding each wait to this interval
/// lets the loop periodically check whether its output channel's receiver was
/// dropped, instead of only noticing on the next incoming message — bounding how
/// long an abandoned subscription can keep its consumer alive.
pub(crate) const KAFKA_SUBSCRIBE_IDLE_CHECK_SECS: u64 = 5;

/// Kafka dequeue poll timeout in milliseconds.
///
/// `dequeue()` waits at most this long for a message before returning `None`.
/// Sized to keep queue workers responsive without spinning.
pub(crate) const KAFKA_DEQUEUE_POLL_TIMEOUT_MS: u64 = 100;
