use super::{Cursor, Payload, SchemaRef};

/// Logical descriptor envelope charged by snapshot stores. Allocator overhead is measured
/// separately and is not represented by this value.
pub const SNAPSHOT_DESCRIPTOR_ENVELOPE_BYTES: usize = 192;
/// Logical row/index envelope charged once for each distinct stored chunk.
pub const SNAPSHOT_CHUNK_ENVELOPE_BYTES: usize = 64;

#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct SnapshotId(pub [u8; 16]);

impl SnapshotId {
    pub const fn from_bytes(bytes: [u8; 16]) -> Self {
        Self(bytes)
    }

    pub const fn as_bytes(&self) -> &[u8; 16] {
        &self.0
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct SnapshotDigest(pub [u8; 32]);

impl SnapshotDigest {
    pub const fn from_bytes(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }

    pub const fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SnapshotDescriptor {
    pub id: SnapshotId,
    pub covered: Cursor,
    pub schema: SchemaRef,
    pub content_bytes: u64,
    pub digest: SnapshotDigest,
}

impl SnapshotDescriptor {
    pub fn accounted_bytes(&self) -> Option<usize> {
        self.covered
            .stream
            .id
            .as_str()
            .len()
            .checked_add(self.schema.id.as_str().len())?
            .checked_add(SNAPSHOT_DESCRIPTOR_ENVELOPE_BYTES)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SnapshotUploadState {
    Uploading,
    Verifying,
    Verified,
    Published,
    Aborted,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SnapshotUploadProgress {
    pub descriptor: SnapshotDescriptor,
    pub accepted_bytes: u64,
    pub verified_bytes: u64,
    pub state: SnapshotUploadState,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SnapshotChunk {
    pub offset: u64,
    pub bytes: Payload,
}

impl SnapshotChunk {
    pub fn accounted_bytes(&self) -> Option<usize> {
        self.bytes.len().checked_add(SNAPSHOT_CHUNK_ENVELOPE_BYTES)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SnapshotContinuation {
    pub covered: Cursor,
    pub id: SnapshotId,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct RecoveryLeaseId(pub [u8; 16]);

impl RecoveryLeaseId {
    pub const fn from_bytes(bytes: [u8; 16]) -> Self {
        Self(bytes)
    }

    pub const fn as_bytes(&self) -> &[u8; 16] {
        &self.0
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RecoveryPlan {
    pub lease: RecoveryLeaseId,
    pub snapshot: SnapshotDescriptor,
    pub through: Cursor,
}
