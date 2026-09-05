use super::{Cursor, IncarnationId, StreamId, StreamKey, CURSOR_VERSION, MAX_IDENTIFIER_BYTES};

const TOKEN_PREFIX: &str = "esc_";
const FIXED_PAYLOAD_BYTES: usize = 1 + 1 + 2 + 16 + 8;
pub const MAX_CURSOR_TOKEN_BYTES: usize =
    TOKEN_PREFIX.len() + 2 * (FIXED_PAYLOAD_BYTES + MAX_IDENTIFIER_BYTES);
pub const MIN_CURSOR_TOKEN_BYTES: usize = TOKEN_PREFIX.len() + 2 * (FIXED_PAYLOAD_BYTES + 1);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CursorTokenError {
    InvalidLength,
    InvalidPrefix,
    InvalidEncoding,
    UnsupportedVersion(u8),
    UnsupportedFlags(u8),
    InvalidStreamId,
}

impl std::fmt::Display for CursorTokenError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self:?}")
    }
}
impl std::error::Error for CursorTokenError {}

impl Cursor {
    /// Encode a cursor as a canonical lowercase ASCII token.
    pub fn encode_token(&self) -> std::result::Result<String, CursorTokenError> {
        if self.version != CURSOR_VERSION {
            return Err(CursorTokenError::UnsupportedVersion(self.version));
        }
        let stream = self.stream.id.as_str().as_bytes();
        let stream_len =
            u16::try_from(stream.len()).map_err(|_| CursorTokenError::InvalidStreamId)?;
        let mut token =
            String::with_capacity(TOKEN_PREFIX.len() + 2 * (FIXED_PAYLOAD_BYTES + stream.len()));
        token.push_str(TOKEN_PREFIX);
        push_hex(&mut token, CURSOR_VERSION);
        push_hex(&mut token, 0); // Version 1 has no optional or required flags.
        for byte in stream_len.to_be_bytes() {
            push_hex(&mut token, byte);
        }
        for &byte in stream {
            push_hex(&mut token, byte);
        }
        for byte in self.stream.incarnation.0 {
            push_hex(&mut token, byte);
        }
        for byte in self.offset.to_be_bytes() {
            push_hex(&mut token, byte);
        }
        Ok(token)
    }

    /// Decode a canonical cursor token. Declared lengths are checked before the
    /// only variable-size allocation, the final stream ID, is limited to 256 bytes.
    pub fn decode_token(token: &str) -> std::result::Result<Self, CursorTokenError> {
        let bytes = token.as_bytes();
        if bytes.len() < MIN_CURSOR_TOKEN_BYTES
            || bytes.len() > MAX_CURSOR_TOKEN_BYTES
            || (bytes.len() - TOKEN_PREFIX.len()) % 2 != 0
        {
            return Err(CursorTokenError::InvalidLength);
        }
        if !bytes.starts_with(TOKEN_PREFIX.as_bytes()) {
            return Err(CursorTokenError::InvalidPrefix);
        }
        let version = read_hex_byte(bytes, 0)?;
        if version != CURSOR_VERSION {
            return Err(CursorTokenError::UnsupportedVersion(version));
        }
        let flags = read_hex_byte(bytes, 1)?;
        if flags != 0 {
            return Err(CursorTokenError::UnsupportedFlags(flags));
        }
        let stream_len =
            u16::from_be_bytes([read_hex_byte(bytes, 2)?, read_hex_byte(bytes, 3)?]) as usize;
        let payload_bytes = FIXED_PAYLOAD_BYTES
            .checked_add(stream_len)
            .ok_or(CursorTokenError::InvalidLength)?;
        if bytes.len() != TOKEN_PREFIX.len() + 2 * payload_bytes {
            return Err(CursorTokenError::InvalidLength);
        }
        if stream_len == 0 || stream_len > MAX_IDENTIFIER_BYTES {
            return Err(CursorTokenError::InvalidStreamId);
        }

        let mut stream_bytes = [0_u8; MAX_IDENTIFIER_BYTES];
        for (index, byte) in stream_bytes[..stream_len].iter_mut().enumerate() {
            *byte = read_hex_byte(bytes, 4 + index)?;
        }
        let stream_text = std::str::from_utf8(&stream_bytes[..stream_len])
            .map_err(|_| CursorTokenError::InvalidStreamId)?;
        let id = StreamId::new(stream_text).map_err(|_| CursorTokenError::InvalidStreamId)?;
        let mut incarnation = [0_u8; 16];
        let incarnation_start = 4 + stream_len;
        for (index, byte) in incarnation.iter_mut().enumerate() {
            *byte = read_hex_byte(bytes, incarnation_start + index)?;
        }
        let mut offset = [0_u8; 8];
        let offset_start = incarnation_start + incarnation.len();
        for (index, byte) in offset.iter_mut().enumerate() {
            *byte = read_hex_byte(bytes, offset_start + index)?;
        }
        Ok(Self {
            version,
            stream: StreamKey {
                id,
                incarnation: IncarnationId(incarnation),
            },
            offset: u64::from_be_bytes(offset),
        })
    }
}

pub fn encode_cursor(cursor: &Cursor) -> std::result::Result<String, CursorTokenError> {
    cursor.encode_token()
}
pub fn decode_cursor(token: &str) -> std::result::Result<Cursor, CursorTokenError> {
    Cursor::decode_token(token)
}

fn push_hex(output: &mut String, byte: u8) {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    output.push(HEX[(byte >> 4) as usize] as char);
    output.push(HEX[(byte & 0x0f) as usize] as char);
}

fn read_hex_byte(token: &[u8], payload_index: usize) -> std::result::Result<u8, CursorTokenError> {
    let index = TOKEN_PREFIX.len()
        + payload_index
            .checked_mul(2)
            .ok_or(CursorTokenError::InvalidLength)?;
    let high = hex_nibble(*token.get(index).ok_or(CursorTokenError::InvalidLength)?)?;
    let low = hex_nibble(
        *token
            .get(index + 1)
            .ok_or(CursorTokenError::InvalidLength)?,
    )?;
    Ok((high << 4) | low)
}

fn hex_nibble(byte: u8) -> std::result::Result<u8, CursorTokenError> {
    match byte {
        b'0'..=b'9' => Ok(byte - b'0'),
        b'a'..=b'f' => Ok(byte - b'a' + 10),
        _ => Err(CursorTokenError::InvalidEncoding),
    }
}
