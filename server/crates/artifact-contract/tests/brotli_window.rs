use artifact_contract::{
    brotli_covering_window_bits, BROTLI_MAX_WINDOW_BITS, BROTLI_MIN_WINDOW_BITS,
};
use serde_json::Value;

const WINDOWS: &str = include_str!("../../../../spec/fixtures/brotli-large-window/windows.json");

/// The client's decoder refuses a stream whose window is larger than the one
/// it computes from the decoded length, so both sides read one table.
#[test]
fn the_covering_window_is_the_table_the_client_reads_too() {
    let table: Value = serde_json::from_str(WINDOWS).unwrap();
    let rows = table["rows"].as_array().unwrap();
    assert!(rows.len() >= 12);
    for row in rows {
        let decoded_bytes = row["decodedBytes"].as_u64().unwrap();
        let window_bits = u8::try_from(row["windowBits"].as_u64().unwrap()).unwrap();
        assert_eq!(
            brotli_covering_window_bits(decoded_bytes),
            Some(window_bits),
            "{decoded_bytes}"
        );
    }
}

#[test]
fn no_window_covers_more_than_the_largest() {
    assert_eq!(brotli_covering_window_bits(0), Some(BROTLI_MIN_WINDOW_BITS));
    assert_eq!(
        brotli_covering_window_bits(1 << 30),
        Some(BROTLI_MAX_WINDOW_BITS)
    );
    assert_eq!(brotli_covering_window_bits((1 << 30) + 1), None);
}
