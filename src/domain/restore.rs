use super::{StreamKey, ValidationError, MAX_IDENTIFIER_BYTES};

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct RestoreOperationId(Box<str>);

impl RestoreOperationId {
    pub fn new(value: impl AsRef<str>) -> Result<Self, ValidationError> {
        let value = value.as_ref();
        if value.is_empty() || value.len() > MAX_IDENTIFIER_BYTES {
            return Err(ValidationError::InvalidIdentifier);
        }
        Ok(Self(value.into()))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct BackupIdentity(pub [u8; 32]);

impl BackupIdentity {
    pub fn from_hex(value: &str) -> Result<Self, ValidationError> {
        if value.len() != 64 {
            return Err(ValidationError::InvalidIdentifier);
        }
        let mut bytes = [0u8; 32];
        for (index, pair) in value.as_bytes().chunks_exact(2).enumerate() {
            let high = hex_digit(pair[0]).ok_or(ValidationError::InvalidIdentifier)?;
            let low = hex_digit(pair[1]).ok_or(ValidationError::InvalidIdentifier)?;
            bytes[index] = (high << 4) | low;
        }
        Ok(Self(bytes))
    }

    pub fn to_hex(self) -> String {
        const HEX: &[u8; 16] = b"0123456789abcdef";
        let mut encoded = String::with_capacity(64);
        for byte in self.0 {
            encoded.push(char::from(HEX[usize::from(byte >> 4)]));
            encoded.push(char::from(HEX[usize::from(byte & 0x0f)]));
        }
        encoded
    }
}

fn hex_digit(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        _ => None,
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct IncarnationMapping {
    pub old: StreamKey,
    pub new: StreamKey,
}

impl IncarnationMapping {
    pub fn accounted_bytes(&self) -> Option<usize> {
        self.old
            .id
            .as_str()
            .len()
            .checked_add(self.new.id.as_str().len())?
            .checked_add(128)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn backup_identity_hex_is_canonical_and_bounded() {
        let identity = BackupIdentity([0xab; 32]);
        let encoded = identity.to_hex();
        assert_eq!(encoded, "ab".repeat(32));
        assert_eq!(BackupIdentity::from_hex(&encoded).unwrap(), identity);
        assert!(BackupIdentity::from_hex(&"AB".repeat(32)).is_err());
        assert!(BackupIdentity::from_hex("00").is_err());
    }

    #[test]
    fn restore_operation_id_uses_identifier_byte_limit() {
        assert!(RestoreOperationId::new("").is_err());
        assert!(RestoreOperationId::new("x".repeat(MAX_IDENTIFIER_BYTES)).is_ok());
        assert!(RestoreOperationId::new("x".repeat(MAX_IDENTIFIER_BYTES + 1)).is_err());
    }
}
