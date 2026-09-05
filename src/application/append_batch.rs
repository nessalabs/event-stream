//! Bounded requests for sharing storage work. This is not an atomic multi-stream API.
use super::types::{Error, Result};
use crate::domain::{AppendReceipt, NewEvent, StreamKey, CURSOR_VERSION};

/// Hard ceiling for one store call. Runtime admission remains a separate budget.
pub const MAX_APPEND_BATCH_RECORDS: usize = 64;

#[derive(Debug)]
pub struct AppendRequest {
    pub stream: StreamKey,
    pub event: NewEvent,
}

#[derive(Clone, Copy, Debug)]
pub struct AppendBatchLimits {
    pub max_records: usize,
    /// Logical event envelopes, stream names and allocated request-vector slots.
    /// This is not an allocator-exact bound on adapter transactions or receipts.
    pub max_bytes: usize,
}

/// Validated, immutable input order. Keep this request until all outcomes resolve.
#[derive(Debug)]
pub struct AppendBatch {
    items: Vec<AppendRequest>,
    accounted_bytes: usize,
}

impl AppendBatch {
    /// Reject before storage I/O. The caller owns input allocation before this call.
    /// Excess vector capacity is charged too; validation never clones payloads.
    pub fn new(items: Vec<AppendRequest>, limits: AppendBatchLimits) -> Result<Self> {
        if limits.max_records == 0
            || limits.max_records > MAX_APPEND_BATCH_RECORDS
            || limits.max_bytes == 0
        {
            return Err(Error::InvalidConfig("invalid append batch limits".into()));
        }
        if items.is_empty() {
            return Err(Error::InvalidConfig(
                "append batch must not be empty".into(),
            ));
        }
        if items.len() > limits.max_records {
            return Err(Error::CapacityExceeded);
        }
        let accounted_bytes = items
            .iter()
            .fold(
                items
                    .capacity()
                    .checked_mul(std::mem::size_of::<AppendRequest>()),
                |total, item| {
                    total
                        .and_then(|n| n.checked_add(item.event.accounted_bytes()))
                        .and_then(|n| n.checked_add(item.stream.id.as_str().len()))
                },
            )
            .ok_or(Error::CapacityExceeded)?;
        if accounted_bytes > limits.max_bytes {
            return Err(Error::CapacityExceeded);
        }
        Ok(Self {
            items,
            accounted_bytes,
        })
    }

    pub fn items(&self) -> &[AppendRequest] {
        &self.items
    }

    pub fn accounted_bytes(&self) -> usize {
        self.accounted_bytes
    }

    /// Call before publishing any adapter outcomes. Wrong count or identity means
    /// the adapter contract is broken; callers must retain original unresolved IDs.
    /// This verifies shape and identity, not that storage actually committed.
    pub fn validate_results(&self, results: &[Result<AppendReceipt>]) -> Result<()> {
        let invalid = || Error::StoreCorrupt("append batch outcomes do not match requests".into());
        if results.len() != self.items.len() {
            return Err(invalid());
        }
        for (item, result) in self.items.iter().zip(results) {
            match result {
                Ok(receipt) => {
                    let record = &receipt.record;
                    if record.cursor.version != CURSOR_VERSION
                        || record.cursor.offset == 0
                        || record.cursor.stream != item.stream
                        || record.event != item.event
                    {
                        return Err(invalid());
                    }
                }
                Err(
                    Error::CommitUnknown { event_id } | Error::IdempotencyConflict { event_id },
                ) if *event_id != item.event.id => return Err(invalid()),
                _ => {}
            }
        }
        Ok(())
    }
}
