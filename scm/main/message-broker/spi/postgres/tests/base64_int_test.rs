//! Direct-dep integration test for base64 (arch rule 95 — dep must have test coverage).
//!
//! This test exercises `base64` unconditionally (no feature gate) to satisfy the
//! structural audit requirement that every dependency used in src/ has integration
//! test coverage. `postgres_message_broker.rs` uses it to encode/decode message
//! payloads as JSONB-safe strings.

#![allow(clippy::expect_used)]

use base64::engine::general_purpose::STANDARD as BASE64;
use base64::Engine as _;

/// @covers: base64
/// Verifies a round-trip through the exact encoder/decoder configuration
/// `PostgresMessageBroker` uses (standard alphabet, with padding).
#[test]
fn test_base64_standard_engine_round_trips_arbitrary_bytes() {
    let original = b"\x00\x01\xffhello world\xfe";
    let encoded = BASE64.encode(original);
    let decoded = BASE64.decode(&encoded).expect("valid base64 must decode");
    assert_eq!(decoded, original);
}

/// @covers: base64
/// Verifies malformed base64 is rejected rather than silently truncated.
#[test]
fn test_base64_standard_engine_rejects_invalid_input() {
    let result = BASE64.decode("not valid base64!!");
    assert!(result.is_err(), "invalid base64 must fail to decode");
}
