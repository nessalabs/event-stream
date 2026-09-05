use super::{Cursor, EventId, NewEvent, StreamKey, ValidationError, MAX_IDENTIFIER_BYTES};

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct RetentionOperationId(Box<str>);

impl RetentionOperationId {
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
pub struct RetryGeneration(u64);

impl RetryGeneration {
    pub const LEGACY: Self = Self(0);
    pub const FIRST: Self = Self(1);

    pub const fn new(value: u64) -> Self {
        Self(value)
    }

    pub const fn get(self) -> u64 {
        self.0
    }

    pub fn checked_next(self) -> Option<Self> {
        self.0.checked_add(1).map(Self)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GeneratedEvent {
    pub generation: RetryGeneration,
    pub event: NewEvent,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RetryPolicyState {
    Lifetime,
    Generational {
        oldest_accepted: RetryGeneration,
        current: RetryGeneration,
    },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EnableRetryPolicy {
    pub operation_id: RetentionOperationId,
    pub stream: StreamKey,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AdvanceRetryGeneration {
    pub operation_id: RetentionOperationId,
    pub stream: StreamKey,
    pub expected_current: RetryGeneration,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ExpireRetryGenerations {
    pub operation_id: RetentionOperationId,
    pub stream: StreamKey,
    pub expected_oldest: RetryGeneration,
    pub retain_from: RetryGeneration,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AdvanceRetentionFloor {
    pub operation_id: RetentionOperationId,
    pub stream: StreamKey,
    pub expected_floor: Cursor,
    pub new_floor: Cursor,
}

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct GeneratedEventIdentity {
    pub stream: StreamKey,
    pub generation: RetryGeneration,
    pub event_id: EventId,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn operation_id_and_generation_boundaries_are_explicit() {
        assert!(RetentionOperationId::new("").is_err());
        assert!(RetentionOperationId::new("x".repeat(MAX_IDENTIFIER_BYTES)).is_ok());
        assert!(RetentionOperationId::new("x".repeat(MAX_IDENTIFIER_BYTES + 1)).is_err());
        assert_eq!(RetryGeneration::LEGACY.get(), 0);
        assert_eq!(
            RetryGeneration::FIRST.checked_next(),
            Some(RetryGeneration::new(2))
        );
        assert_eq!(RetryGeneration::new(u64::MAX).checked_next(), None);
    }
}
