use super::{StreamKey, ValidationError, MAX_IDENTIFIER_BYTES};

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct LifecycleOperationId(Box<str>);

impl LifecycleOperationId {
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

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LifecycleAction {
    Delete,
    Reset,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LifecycleRequest {
    pub operation_id: LifecycleOperationId,
    pub expected: StreamKey,
    pub action: LifecycleAction,
}

impl LifecycleRequest {
    pub fn receipt_charge(&self) -> Option<usize> {
        self.operation_id
            .as_str()
            .len()
            .checked_add(self.expected.id.as_str().len())?
            .checked_add(256)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LifecycleReceipt {
    pub request: LifecycleRequest,
    pub replacement: Option<StreamKey>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum StreamAvailability {
    Active(StreamKey),
    Unavailable(StreamKey),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CleanupLimits {
    pub max_records: usize,
    pub max_bytes: usize,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CleanupProgress {
    pub stream: Option<StreamKey>,
    pub removed_records: usize,
    pub removed_bytes: usize,
    pub remaining: bool,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lifecycle_operation_ids_use_the_shared_utf8_byte_limit() {
        assert!(LifecycleOperationId::new("").is_err());
        assert!(LifecycleOperationId::new("x".repeat(MAX_IDENTIFIER_BYTES)).is_ok());
        assert!(LifecycleOperationId::new("x".repeat(MAX_IDENTIFIER_BYTES + 1)).is_err());
        assert!(LifecycleOperationId::new("🦀".repeat(MAX_IDENTIFIER_BYTES / 4)).is_ok());
        assert!(
            LifecycleOperationId::new(format!("{}x", "🦀".repeat(MAX_IDENTIFIER_BYTES / 4)))
                .is_err()
        );
    }
}
