//! Direct-dep integration test for sqlx (arch rule 95 — dep must have test coverage).
//!
//! This test exercises `sqlx` unconditionally (no feature gate) to satisfy the
//! structural audit requirement that every dependency used in src/ has integration
//! test coverage. `postgres_message_broker.rs` uses it to connect to Postgres and
//! run `pgmq` queries.

#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::str::FromStr;

use sqlx::postgres::PgConnectOptions;

/// @covers: sqlx
/// Verifies sqlx's Postgres DSN parser accepts a well-formed connection string
/// without requiring a running server (`sqlx` connects lazily).
#[test]
fn test_sqlx_pg_connect_options_parses_valid_dsn() {
    let opts = PgConnectOptions::from_str("postgres://user:pass@127.0.0.1:5432/app")
        .expect("well-formed DSN must parse without a live connection");
    assert_eq!(opts.get_host(), "127.0.0.1");
    assert_eq!(opts.get_port(), 5432);
}

/// @covers: sqlx
/// Verifies sqlx's Postgres DSN parser rejects a malformed connection string.
#[test]
fn test_sqlx_pg_connect_options_rejects_malformed_dsn() {
    let result = PgConnectOptions::from_str("not-a-valid-dsn");
    assert!(result.is_err(), "malformed DSN must fail to parse");
}
