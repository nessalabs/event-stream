use event_stream::domain::{
    decode_cursor, encode_cursor, Cursor, CursorTokenError, EventId, IncarnationId, NewEvent,
    Payload, Record, SchemaId, SchemaRef, StreamId, StreamKey, CURSOR_VERSION,
    MAX_CURSOR_TOKEN_BYTES,
};
use std::sync::Arc;

fn fixture() -> Cursor {
    Cursor::new(
        StreamKey {
            id: StreamId::new("orders").unwrap(),
            incarnation: IncarnationId([0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15]),
        },
        u64::MAX,
    )
}

#[test]
fn golden_token_is_stable_and_canonical() {
    const GOLDEN: &str = "esc_010000066f7264657273000102030405060708090a0b0c0d0e0fffffffffffffffff";
    let cursor = fixture();
    assert_eq!(encode_cursor(&cursor).unwrap(), GOLDEN);
    assert_eq!(decode_cursor(GOLDEN).unwrap(), cursor);
    assert_eq!(
        decode_cursor(GOLDEN).unwrap().encode_token().unwrap(),
        GOLDEN
    );
}

#[test]
fn full_u64_range_and_maximum_identifier_round_trip() {
    let id = "x".repeat(256);
    for offset in [0, 1, i64::MAX as u64, i64::MAX as u64 + 1, u64::MAX] {
        let cursor = Cursor::new(
            StreamKey {
                id: StreamId::new(&id).unwrap(),
                incarnation: IncarnationId([0xff; 16]),
            },
            offset,
        );
        let token = cursor.encode_token().unwrap();
        assert_eq!(token.len(), MAX_CURSOR_TOKEN_BYTES);
        assert_eq!(Cursor::decode_token(&token).unwrap(), cursor);
    }
}

#[test]
fn versions_flags_lengths_hex_and_utf8_are_rejected() {
    let golden = fixture().encode_token().unwrap();
    let mut value = golden.clone();
    value.replace_range(4..6, "02");
    assert_eq!(
        decode_cursor(&value),
        Err(CursorTokenError::UnsupportedVersion(2))
    );
    value = golden.clone();
    value.replace_range(6..8, "01");
    assert_eq!(
        decode_cursor(&value),
        Err(CursorTokenError::UnsupportedFlags(1))
    );
    value = golden.clone();
    value.replace_range(8..12, "0101");
    assert_eq!(decode_cursor(&value), Err(CursorTokenError::InvalidLength));
    value = golden.clone();
    value.replace_range(12..14, "ff");
    assert_eq!(
        decode_cursor(&value),
        Err(CursorTokenError::InvalidStreamId)
    );
    value = golden.to_uppercase();
    assert_eq!(decode_cursor(&value), Err(CursorTokenError::InvalidPrefix));
    value = golden.clone();
    value.replace_range(12..14, "GG");
    assert_eq!(
        decode_cursor(&value),
        Err(CursorTokenError::InvalidEncoding)
    );
    assert_eq!(
        decode_cursor(&(golden.clone() + "00")),
        Err(CursorTokenError::InvalidLength)
    );
    assert_eq!(decode_cursor("esc_"), Err(CursorTokenError::InvalidLength));
}

#[test]
fn unsupported_cursor_version_cannot_be_encoded() {
    let mut cursor = fixture();
    cursor.version = CURSOR_VERSION + 1;
    assert_eq!(
        cursor.encode_token(),
        Err(CursorTokenError::UnsupportedVersion(2))
    );
}

#[test]
fn malformed_input_fuzz_is_bounded_and_never_panics() {
    let golden = fixture().encode_token().unwrap();
    let mut seed = 0x9e37_79b9_7f4a_7c15_u64;
    for length in 0..=MAX_CURSOR_TOKEN_BYTES + 32 {
        let mut bytes = vec![0_u8; length];
        for byte in &mut bytes {
            seed ^= seed << 13;
            seed ^= seed >> 7;
            seed ^= seed << 17;
            *byte = (seed & 0x7f) as u8;
        }
        if let Ok(text) = std::str::from_utf8(&bytes) {
            let _ = decode_cursor(text);
        }
    }
    for index in 0..golden.len() {
        let mut bytes = golden.as_bytes().to_vec();
        bytes[index] = b'z';
        let _ = decode_cursor(std::str::from_utf8(&bytes).unwrap());
    }
}

#[test]
fn cursor_transport_does_not_interpret_unknown_event_envelopes() {
    let cursor = fixture();
    let record = Arc::new(Record {
        cursor: decode_cursor(&cursor.encode_token().unwrap()).unwrap(),
        event: NewEvent {
            id: EventId::new("opaque-1").unwrap(),
            schema: SchemaRef {
                id: SchemaId::new("future.vendor.envelope").unwrap(),
                version: u32::MAX,
            },
            payload: Payload::copy_from_slice(&[0, 255, 17, 99]),
        },
    });
    assert_eq!(record.cursor, cursor);
    assert_eq!(record.event.schema.version, u32::MAX);
    assert_eq!(record.event.payload.as_bytes(), &[0, 255, 17, 99]);
}
