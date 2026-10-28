//! Integration coverage for PostgreSQL connection option parsing without a live database.

#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::str::FromStr;

use tokio_postgres::Config as PostgresConnectOptions;

/// @covers: PostgresConnectOptions
/// Verifies the Postgres DSN parser accepts a well-formed connection string.
#[test]
fn test_postgres_connect_options_parses_valid_dsn() {
    let opts = PostgresConnectOptions::from_str("postgres://user:pass@127.0.0.1:5432/app")
        .expect("well-formed DSN must parse without a live connection");
    assert_eq!(
        opts.get_hosts(),
        &[tokio_postgres::config::Host::Tcp("127.0.0.1".to_string())]
    );
    assert_eq!(opts.get_ports(), &[5432]);
}

/// @covers: PostgresConnectOptions
/// Verifies the Postgres DSN parser rejects a malformed connection string.
#[test]
fn test_postgres_connect_options_rejects_malformed_dsn() {
    let result = PostgresConnectOptions::from_str("not-a-valid-dsn");
    assert!(result.is_err(), "malformed DSN must fail to parse");
}
