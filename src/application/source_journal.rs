use super::{RetentionError, RetentionStore};
use crate::domain::*;
use async_trait::async_trait;
use std::time::Duration;

pub type JournalResult<T> = std::result::Result<T, JournalError>;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SourceProgress {
    pub binding: SourceBinding,
    pub captured_end: u64,
    pub capture_receipt_floor: u64,
    pub checkpoint_offset: u64,
    pub next_item_index: u64,
    pub committed_output: Option<Cursor>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BeginSourceReceipt {
    pub request: BeginSource,
    pub progress: SourceProgress,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CaptureReceipt {
    pub start: SourcePosition,
    pub end: u64,
    /// SHA-256 content identity retained after captured bytes are cleaned up.
    pub digest: CaptureDigest,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CheckpointReceipt {
    pub checkpoint: ParserCheckpoint,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AdvanceCaptureReceiptFloorReceipt {
    pub request: AdvanceCaptureReceiptFloor,
    pub progress: SourceProgress,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RawPageLimits {
    pub max_segments: usize,
    pub max_bytes: usize,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RawPage {
    pub start: SourcePosition,
    pub bytes: Payload,
    pub next_offset: u64,
    pub complete: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct JournalCleanupLimits {
    pub max_segment_rows: usize,
    pub max_marker_rows: usize,
    pub max_receipt_rows: usize,
    pub max_bytes: usize,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct JournalCleanupProgress {
    pub removed_segment_rows: usize,
    pub removed_marker_rows: usize,
    pub removed_receipt_rows: usize,
    pub removed_bytes: usize,
    pub remaining: bool,
}

#[derive(Clone, Debug)]
pub struct SourceJournalStoreConfig {
    pub storage: JournalStorageConfig,
    pub receipts: JournalReceiptConfig,
    pub cleanup: JournalCleanupLimits,
}

#[derive(Clone, Debug)]
pub struct JournalStorageConfig {
    pub max_sources: usize,
    pub max_segments: usize,
    pub max_captured_bytes: u64,
    pub max_segment_bytes: usize,
    pub max_output_markers: usize,
    pub max_marker_bytes: u64,
    pub max_checkpoints: usize,
    pub max_checkpoint_state_bytes: usize,
    pub max_staging_bytes: u64,
}

#[derive(Clone, Debug)]
pub struct JournalReceiptConfig {
    pub max_source_receipts: usize,
    pub max_receipt_bytes: usize,
}

#[derive(Clone, Debug)]
pub struct JournalAdmissionConfig {
    pub max_concurrent: usize,
    pub max_waiters: usize,
    pub max_waiter_bytes: usize,
    pub max_in_flight_bytes: usize,
    pub admission_timeout: Duration,
}

impl Default for JournalAdmissionConfig {
    fn default() -> Self {
        Self {
            max_concurrent: 4,
            max_waiters: 32,
            max_waiter_bytes: 4 * 1024 * 1024,
            max_in_flight_bytes: 4 * 1024 * 1024,
            admission_timeout: Duration::from_secs(5),
        }
    }
}

impl JournalAdmissionConfig {
    pub fn validate(&self) -> JournalResult<()> {
        if self.max_concurrent == 0
            || self.max_waiters == 0
            || self.max_waiter_bytes == 0
            || self.max_in_flight_bytes == 0
            || self.max_waiter_bytes > u32::MAX as usize
            || self.max_in_flight_bytes > u32::MAX as usize
            || self.admission_timeout.is_zero()
        {
            return Err(JournalError::InvalidConfig(
                "journal admission limits must be finite".into(),
            ));
        }
        Ok(())
    }
}

impl Default for SourceJournalStoreConfig {
    fn default() -> Self {
        Self {
            storage: JournalStorageConfig {
                max_sources: 1024,
                max_segments: 16_384,
                max_captured_bytes: 64 * 1024 * 1024,
                max_segment_bytes: 64 * 1024,
                max_output_markers: 100_000,
                max_marker_bytes: 32 * 1024 * 1024,
                max_checkpoints: 1024,
                max_checkpoint_state_bytes: 64 * 1024,
                max_staging_bytes: 4 * 1024 * 1024,
            },
            receipts: JournalReceiptConfig {
                max_source_receipts: 16_384,
                max_receipt_bytes: 4 * 1024 * 1024,
            },
            cleanup: JournalCleanupLimits {
                max_segment_rows: 256,
                max_marker_rows: 256,
                max_receipt_rows: 256,
                max_bytes: 2 * 1024 * 1024,
            },
        }
    }
}

impl SourceJournalStoreConfig {
    pub fn validate(&self) -> JournalResult<()> {
        let storage = &self.storage;
        let largest_cleanup_row = storage
            .max_segment_bytes
            .checked_add(128 + MAX_IDENTIFIER_BYTES)
            .and_then(|segment| {
                let marker = 192usize.checked_add(3 * MAX_IDENTIFIER_BYTES)?;
                let receipt = 160usize.checked_add(2 * MAX_IDENTIFIER_BYTES)?;
                Some(segment.max(marker).max(receipt))
            });
        if storage.max_sources == 0
            || storage.max_segments == 0
            || storage.max_captured_bytes == 0
            || storage.max_segment_bytes == 0
            || storage.max_output_markers == 0
            || storage.max_marker_bytes == 0
            || storage.max_checkpoints == 0
            || storage.max_checkpoint_state_bytes == 0
            || storage.max_staging_bytes == 0
            || self.receipts.max_source_receipts == 0
            || self.receipts.max_receipt_bytes == 0
            || self.cleanup.max_segment_rows == 0
            || self.cleanup.max_marker_rows == 0
            || self.cleanup.max_receipt_rows == 0
            || self.cleanup.max_bytes == 0
            || largest_cleanup_row.is_none_or(|charge| self.cleanup.max_bytes < charge)
        {
            return Err(JournalError::InvalidConfig(
                "source journal limits must be finite and cleanup must fit every accepted row"
                    .into(),
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum JournalError {
    InvalidConfig(String),
    InvalidInput(String),
    NotFound {
        source: SourceKey,
    },
    StaleSource {
        current: Option<Box<SourceKey>>,
    },
    SourceBindingConflict {
        source: SourceKey,
    },
    OperationConflict {
        operation_id: JournalOperationId,
    },
    CaptureConflict {
        start: SourcePosition,
    },
    StaleCaptureReceiptFloor {
        current: u64,
    },
    CaptureReceiptExpired {
        floor: u64,
    },
    MissingCapturedHistory {
        available_from: u64,
    },
    OutputConflict {
        source: SourceKey,
        item_index: u64,
    },
    CheckpointConflict {
        source: SourceKey,
    },
    SourceSealed {
        end: u64,
    },
    SealUnknown(Box<SealSource>),
    FinishUnknown(Box<FinishSource>),
    RetryGenerationExpired,
    CapacityExceeded,
    Overloaded,
    AdmissionTimeout,
    Closed,
    CorruptStorage(String),
    StorageFailure(String),
    BeginUnknown(Box<BeginSource>),
    CaptureUnknown(Box<RawSegment>),
    AdvanceCaptureReceiptFloorUnknown(Box<AdvanceCaptureReceiptFloor>),
    OutputUnknown {
        output_stream: Box<StreamKey>,
        output: Box<JournaledOutput>,
    },
    CheckpointUnknown(Box<ParserCheckpoint>),
}

impl std::fmt::Display for JournalError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::CaptureUnknown(segment) => write!(
                formatter,
                "capture result unknown for {} at byte {} ({} bytes)",
                segment.start.source.id.as_str(),
                segment.start.offset,
                segment.bytes.len()
            ),
            Self::OutputUnknown { output, .. } => write!(
                formatter,
                "output result unknown for {} item {}",
                output.source.id.as_str(),
                output.position.item_index
            ),
            Self::CheckpointUnknown(checkpoint) => write!(
                formatter,
                "checkpoint result unknown for {} at byte {}",
                checkpoint.source.source.id.as_str(),
                checkpoint.source.offset
            ),
            other => write!(formatter, "{other:?}"),
        }
    }
}

impl std::error::Error for JournalError {}

impl From<RetentionError> for JournalError {
    fn from(error: RetentionError) -> Self {
        match error {
            RetentionError::Closed => Self::Closed,
            RetentionError::CapacityExceeded => Self::CapacityExceeded,
            RetentionError::RetryGenerationExpired { .. }
            | RetentionError::LegacyRetryNotFound { .. } => Self::RetryGenerationExpired,
            RetentionError::CorruptStorage(detail) => Self::CorruptStorage(detail),
            other => Self::StorageFailure(other.to_string()),
        }
    }
}

#[async_trait]
pub trait SourceJournalStore: RetentionStore {
    async fn begin_source(&self, request: BeginSource) -> JournalResult<BeginSourceReceipt>;
    async fn capture_segment(&self, segment: RawSegment) -> JournalResult<CaptureReceipt>;
    async fn advance_capture_receipt_floor(
        &self,
        request: AdvanceCaptureReceiptFloor,
    ) -> JournalResult<AdvanceCaptureReceiptFloorReceipt>;
    async fn append_captured(
        &self,
        output_stream: &StreamKey,
        output: JournaledOutput,
    ) -> JournalResult<AppendReceipt>;
    async fn publish_parser_checkpoint(
        &self,
        checkpoint: ParserCheckpoint,
    ) -> JournalResult<CheckpointReceipt>;
    async fn source_status(&self, source: &SourceKey) -> JournalResult<SourceProgress>;
    async fn read_captured(
        &self,
        source: &SourceKey,
        offset: u64,
        limits: RawPageLimits,
    ) -> JournalResult<RawPage>;
    async fn latest_checkpoint(
        &self,
        source: &SourceKey,
    ) -> JournalResult<Option<ParserCheckpoint>>;
    async fn cleanup_captured(
        &self,
        limits: JournalCleanupLimits,
    ) -> JournalResult<JournalCleanupProgress>;
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SealSourceReceipt {
    pub request: SealSource,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FinishSourceReceipt {
    pub request: FinishSource,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SourceFinalizationStatus {
    pub sealed_end: Option<u64>,
    pub parser_finished: bool,
}

/// Durable EOF capability. See the source-journal end-of-input contract.
/// Implementing capture alone does not imply this capability.
#[async_trait]
pub trait SourceFinalizationStore: SourceJournalStore {
    async fn seal_source(&self, request: SealSource) -> JournalResult<SealSourceReceipt>;
    async fn finish_source(&self, request: FinishSource) -> JournalResult<FinishSourceReceipt>;
    async fn source_finalization(
        &self,
        source: &SourceKey,
    ) -> JournalResult<SourceFinalizationStatus>;
}
