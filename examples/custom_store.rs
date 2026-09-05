use async_trait::async_trait;
use event_stream::infrastructure::{MemoryStore, MemoryStoreOptions};
use event_stream::*;
use std::{sync::Arc, time::Duration};

/// A small custom adapter showing that the application runtime depends on the port.
struct MyStore(MemoryStore);

#[async_trait]
impl EventStore for MyStore {
    type Options = MemoryStoreOptions;
    async fn open(options: Self::Options) -> Result<Self> {
        Ok(Self(MemoryStore::open(options).await?))
    }
    fn capabilities(&self) -> StoreCapabilities {
        self.0.capabilities()
    }
    async fn create_if_absent(&self, id: &StreamId) -> Result<StreamKey> {
        self.0.create_if_absent(id).await
    }
    async fn append_atomic(&self, stream: &StreamKey, event: NewEvent) -> Result<AppendReceipt> {
        self.0.append_atomic(stream, event).await
    }
    async fn lookup_event(&self, stream: &StreamKey, id: &EventId) -> Result<Option<Arc<Record>>> {
        self.0.lookup_event(stream, id).await
    }
    async fn bounds(&self, stream: &StreamKey) -> Result<Bounds> {
        self.0.bounds(stream).await
    }
    async fn read_range(
        &self,
        stream: &StreamKey,
        after: u64,
        through: u64,
        limits: PageLimits,
    ) -> Result<Page> {
        self.0.read_range(stream, after, through, limits).await
    }
    async fn close(&self) -> Result<()> {
        self.0.close().await
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let runtime =
        Runtime::<MyStore>::open(MemoryStoreOptions::default(), RuntimeConfig::default()).await?;
    let stream = runtime.create_stream(&StreamId::new("custom")?).await?;
    let receipt = runtime
        .append(
            &stream,
            NewEvent {
                id: EventId::new("event-1")?,
                schema: SchemaRef {
                    id: SchemaId::new("example.bytes")?,
                    version: 1,
                },
                payload: Payload::copy_from_slice(b"hello"),
            },
        )
        .await?;
    println!("committed offset {}", receipt.record.cursor.offset);
    runtime.shutdown(Duration::from_secs(1)).await?;
    Ok(())
}
