# Changelog

## [message-broker-svc-postgres-spi 0.2.1] - 2026-10-28

### Security

- Replaced SQLx with the Postgres-native `tokio-postgres` and `deadpool-postgres`
  client stack. This removes SQLx's optional MySQL and vulnerable RSA packages
  from downstream lockfiles.
- Kept TLS certificate verification enabled with Rustls, `ring`, and Mozilla
  WebPKI roots.

### Verification

- CI passed Rust 1.99.1 formatting, strict Clippy across all workspace targets,
  Postgres SPI tests, live pgmq integration tests, and cargo audit.
- Published as `message-broker-svc-postgres-spi` 0.2.1 and tagged
  `postgres-spi/v0.2.1` on main.

## [message-broker-svc-core 0.4.1] - 2026-10-28

### Changed

- Declared the crates.io `message-broker-pattern` 0.3.0 version requirement
  alongside the pinned Git source used by this workspace.

## [message-broker-svc-nats-spi 0.3.1] - 2026-10-28

### Changed

- Declared the crates.io `message-broker-pattern` 0.3.0 version requirement
  alongside the pinned Git source used by this workspace.

## [message-broker-svc-kafka-spi 0.3.1] - 2026-10-28

### Changed

- Declared the crates.io `message-broker-pattern` 0.3.0 version requirement
  alongside the pinned Git source used by this workspace.

## [message-broker-svc-saf 0.4.1] - 2026-10-28

### Changed

- Align the facade's pattern, core, NATS, Kafka, and Postgres dependencies with
  the current stable release family.
