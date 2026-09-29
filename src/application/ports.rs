use super::append_batch::AppendBatch;
use super::types::*;
use crate::domain::*;
use async_trait::async_trait;
use std::{sync::Arc, time::Duration};

/// An exclusively owned store. Implementations retain ownership until all accepted
/// I/O has stopped, including when a caller drops an operation future.
#[async_trait]
pub trait EventStore: Send + Sync + Sized + 'static {
    type Options: Send + 'static;
    async fn open(options: Self::Options) -> Result<Self>;
    fn capabilities(&self) -> StoreCapabilities;
    async fn create_if_absent(&self, id: &StreamId) -> Result<StreamKey>;
    /// Find an active stream without creating one. A retired name is an error,
    /// not absence, so callers cannot silently reuse a deleted identity.
    async fn find_stream(&self, id: &StreamId) -> Result<Option<StreamKey>>;
    async fn append_atomic(&self, stream: &StreamKey, event: NewEvent) -> Result<AppendReceipt>;
    /// Process inputs in order and return one outcome per input, including errors.
    /// A conflict must not discard another item's success. This fallback commits
    /// separately; optimized adapters may share a transaction but must only return
    /// successful receipts after commit. No cross-stream atomicity is promised.
    /// Callers must retain the batch and drive this future through cancellation
    /// of individual callers, then validate_results before publishing outcomes.
    async fn append_batch(&self, batch: &AppendBatch) -> Vec<Result<AppendReceipt>> {
        let mut results = Vec::with_capacity(batch.items().len());
        for item in batch.items() {
            results.push(self.append_atomic(&item.stream, item.event.clone()).await);
        }
        results
    }
    async fn lookup_event(&self, stream: &StreamKey, id: &EventId) -> Result<Option<Arc<Record>>>;
    async fn bounds(&self, stream: &StreamKey) -> Result<Bounds>;
    /// Return a contiguous prefix of (after, through], respecting both limits.
    /// Empty below through, internal gaps, or mismatched identities are corruption.
    async fn read_range(
        &self,
        stream: &StreamKey,
        after: u64,
        through: u64,
        limits: PageLimits,
    ) -> Result<Page>;
    async fn close(&self) -> Result<()>;
}

/// Optional lifecycle and retired-history maintenance boundary.
#[async_trait]
pub trait LifecycleStore: EventStore {
    async fn change_lifecycle(&self, request: LifecycleRequest) -> Result<LifecycleReceipt>;
    async fn cleanup_retired(&self, limits: CleanupLimits) -> Result<CleanupProgress>;
}

/// Typed boundary consumed by ingestion and external callers. Runtime implements
/// this trait; scenarios call the same implementation as applications.
#[async_trait]
pub trait EventSink: Send + Sync {
    async fn append(&self, stream: &StreamKey, event: NewEvent) -> Result<AppendReceipt>;
}

#[async_trait]
pub trait EventReader: Send + Sync {
    async fn create_stream(&self, id: &StreamId) -> Result<StreamKey>;
    /// Find an active stream without changing the store.
    async fn find_stream(&self, id: &StreamId) -> Result<Option<StreamKey>>;
    async fn bounds(&self, stream: &StreamKey) -> Result<Bounds>;
    async fn read_after(
        &self,
        after: &Cursor,
        limits: PageLimits,
        through: Option<&Cursor>,
    ) -> Result<Page>;
}

#[async_trait]
pub trait EventSubscription: Send {
    /// At most one terminal error; then None. Cancellation must preserve cursor state.
    async fn next(&mut self) -> Option<Result<Arc<Record>>>;
    fn last_delivered(&self) -> &Cursor;
}

#[async_trait]
pub trait EventRuntime: EventSink + EventReader {
    type Subscription: EventSubscription;
    async fn try_append(&self, stream: &StreamKey, event: NewEvent) -> Result<AppendReceipt>;
    async fn subscribe(
        &self,
        stream: &StreamKey,
        options: SubscriptionOptions,
    ) -> Result<Self::Subscription>;
    async fn shutdown(&self, deadline: Duration) -> Result<ShutdownReport>;
}
