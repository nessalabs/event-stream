use super::{Record, StreamKey, ValidationError, MAX_IDENTIFIER_BYTES};
use std::sync::Arc;

macro_rules! replica_identifier {
    ($name:ident) => {
        #[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
        pub struct $name(Box<str>);

        impl $name {
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
    };
}

replica_identifier!(ReplicaId);
replica_identifier!(ReplicationOperationId);

#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct OriginId(pub [u8; 16]);

#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct DestinationEpoch(pub [u8; 16]);

#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct BatchId(pub [u8; 16]);

#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct BootstrapId(pub [u8; 16]);

#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct ReplicaReadLeaseId(pub [u8; 16]);

#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct DurableTimestampMillis(pub u64);

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct OriginStream {
    pub origin: OriginId,
    pub stream: StreamKey,
}

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct ReplicaPosition {
    pub stream: OriginStream,
    /// The committed prefix ends at this offset. Zero means no replicated records.
    pub offset: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReplicaBatch {
    pub id: BatchId,
    pub destination_epoch: DestinationEpoch,
    pub after: ReplicaPosition,
    pub records: Vec<Arc<Record>>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReplicaReceipt {
    pub batch: BatchId,
    pub destination_epoch: DestinationEpoch,
    pub committed_through: ReplicaPosition,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReplicaPage {
    pub after: ReplicaPosition,
    pub records: Vec<Arc<Record>>,
    pub next: ReplicaPosition,
    pub complete: bool,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn replica_identifiers_are_bounded() {
        assert!(ReplicaId::new("").is_err());
        assert!(ReplicaId::new("r".repeat(MAX_IDENTIFIER_BYTES)).is_ok());
        assert!(ReplicaId::new("r".repeat(MAX_IDENTIFIER_BYTES + 1)).is_err());
        assert!(ReplicationOperationId::new("attach-1").is_ok());
    }
}
