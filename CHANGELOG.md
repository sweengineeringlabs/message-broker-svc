# Changelog

## [message-broker-svc-postgres-spi 0.2.1] - 2026-10-28

### Security

- Replaced SQLx with the Postgres-native `tokio-postgres` and `deadpool-postgres`
  client stack. This removes SQLx's optional MySQL and vulnerable RSA packages
  from downstream lockfiles.
- Kept TLS certificate verification enabled with Rustls, `ring`, and Mozilla
  WebPKI roots.

### Verification

- Workspace all-target check, Postgres SPI tests, strict Clippy, and consumer
  workspace audit pass locally.
- Live Postgres/pgmq tests and release publication remain pending.
