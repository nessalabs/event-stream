//! Shared SQLite scenario for an interrupted snapshot upload.
//!
//! The fixture uses only the production snapshot store API. It closes the
//! database with two partial uploads, reopens it, resumes one upload, and
//! aborts and incrementally cleans the other.

use event_stream::{
    infrastructure::{SqliteOptions, SqliteStore},
    Cursor, EventStore, PageLimits, Payload, SchemaId, SchemaRef, SnapshotChunk,
    SnapshotCleanupLimits, SnapshotDescriptor, SnapshotDigest, SnapshotError, SnapshotId,
    SnapshotStore, SnapshotUploadState, StreamId, VerificationLimits,
};
use sha2::{Digest, Sha256};
use std::{path::Path, time::Duration};

const CHUNK_BYTES: usize = 4;
const CLEANUP_MAX_STEPS: usize = 8;

#[derive(Debug)]
pub struct Evidence {
    pub staged_bytes_after_reopen: u64,
    pub published_count_while_interrupted: usize,
    pub exact_retry_accepted_bytes: u64,
    pub resumed_snapshot_bytes: Vec<u8>,
    pub baseline_snapshot_bytes: Vec<u8>,
    pub cleanup_steps: usize,
    pub cleanup_removed_chunks: usize,
    pub cleanup_removed_snapshots: usize,
    pub uploads_remaining: usize,
}

fn descriptor(id: u8, stream: &event_stream::StreamKey, bytes: &[u8]) -> SnapshotDescriptor {
    SnapshotDescriptor {
        id: SnapshotId::from_bytes([id; 16]),
        covered: Cursor::new(stream.clone(), 0),
        schema: SchemaRef {
            id: SchemaId::new("snapshot-interruption.state").unwrap(),
            version: 1,
        },
        content_bytes: bytes.len() as u64,
        digest: SnapshotDigest::from_bytes(Sha256::digest(bytes).into()),
    }
}

async fn upload_all(store: &SqliteStore, snapshot: &SnapshotDescriptor, bytes: &[u8]) {
    store.begin_snapshot(snapshot.clone()).await.unwrap();
    for (index, chunk) in bytes.chunks(CHUNK_BYTES).enumerate() {
        store
            .put_snapshot_chunk(
                snapshot.id,
                SnapshotChunk {
                    offset: (index * CHUNK_BYTES) as u64,
                    bytes: Payload::copy_from_slice(chunk),
                },
            )
            .await
            .unwrap();
    }
    let progress = store
        .verify_snapshot_step(
            snapshot.id,
            VerificationLimits {
                max_chunks: 8,
                max_bytes: 4096,
            },
        )
        .await
        .unwrap();
    assert_eq!(progress.state, SnapshotUploadState::Verified);
    assert_eq!(
        store.publish_snapshot(snapshot.id).await.unwrap(),
        *snapshot
    );
}

async fn read_snapshot(store: &SqliteStore, id: SnapshotId) -> Vec<u8> {
    let plan = store
        .acquire_recovery(id, Duration::from_secs(10))
        .await
        .unwrap();
    let mut bytes = Vec::new();
    let mut offset = 0;
    loop {
        let page = store
            .read_snapshot_chunk(plan.lease, offset, CHUNK_BYTES)
            .await
            .unwrap();
        bytes.extend_from_slice(page.bytes.as_bytes());
        offset = page.next_offset;
        if page.complete {
            break;
        }
    }
    store.release_recovery(plan.lease).await.unwrap();
    bytes
}

pub async fn run(directory: &Path) -> Evidence {
    std::fs::create_dir(directory).unwrap();
    let database = directory.join("snapshot-interruption.sqlite3");
    let store = SqliteStore::open(SqliteOptions::new(&database))
        .await
        .unwrap();
    let stream = store
        .create_if_absent(&StreamId::new("snapshot-interruption").unwrap())
        .await
        .unwrap();

    let baseline_bytes = b"published-baseline";
    let baseline = descriptor(21, &stream, baseline_bytes);
    upload_all(&store, &baseline, baseline_bytes).await;

    let resumed_bytes = b"resume-after-reopen";
    let resumed = descriptor(22, &stream, resumed_bytes);
    store.begin_snapshot(resumed.clone()).await.unwrap();
    let first_resumed_chunk = SnapshotChunk {
        offset: 0,
        bytes: Payload::copy_from_slice(&resumed_bytes[..CHUNK_BYTES]),
    };
    store
        .put_snapshot_chunk(resumed.id, first_resumed_chunk.clone())
        .await
        .unwrap();

    let abandoned_bytes = b"abort-after-reopen";
    let abandoned = descriptor(23, &stream, abandoned_bytes);
    store.begin_snapshot(abandoned.clone()).await.unwrap();
    for (index, chunk) in abandoned_bytes[..CHUNK_BYTES * 2]
        .chunks(CHUNK_BYTES)
        .enumerate()
    {
        store
            .put_snapshot_chunk(
                abandoned.id,
                SnapshotChunk {
                    offset: (index * CHUNK_BYTES) as u64,
                    bytes: Payload::copy_from_slice(chunk),
                },
            )
            .await
            .unwrap();
    }
    store.close().await.unwrap();

    let reopened = SqliteStore::open(SqliteOptions::new(&database))
        .await
        .unwrap();
    let resumed_progress = reopened.snapshot_status(resumed.id).await.unwrap();
    let abandoned_progress = reopened.snapshot_status(abandoned.id).await.unwrap();
    assert_eq!(resumed_progress.state, SnapshotUploadState::Uploading);
    assert_eq!(resumed_progress.accepted_bytes, CHUNK_BYTES as u64);
    assert_eq!(abandoned_progress.state, SnapshotUploadState::Uploading);
    assert_eq!(abandoned_progress.accepted_bytes, (CHUNK_BYTES * 2) as u64);

    let interrupted_page = reopened
        .list_snapshots(
            &stream,
            None,
            PageLimits {
                max_records: 8,
                max_bytes: 4096,
            },
        )
        .await
        .unwrap();
    assert!(interrupted_page.complete);
    assert_eq!(interrupted_page.entries, vec![baseline.clone()]);
    assert_eq!(
        reopened
            .acquire_recovery(resumed.id, Duration::from_secs(10))
            .await,
        Err(SnapshotError::NotFound { id: resumed.id })
    );
    assert_eq!(read_snapshot(&reopened, baseline.id).await, baseline_bytes);

    // Retrying the exact persisted chunk must not append or charge it twice.
    let exact_retry = reopened
        .put_snapshot_chunk(resumed.id, first_resumed_chunk)
        .await
        .unwrap();
    assert_eq!(exact_retry.accepted_bytes, CHUNK_BYTES as u64);
    for (index, chunk) in resumed_bytes[CHUNK_BYTES..].chunks(CHUNK_BYTES).enumerate() {
        reopened
            .put_snapshot_chunk(
                resumed.id,
                SnapshotChunk {
                    offset: (CHUNK_BYTES + index * CHUNK_BYTES) as u64,
                    bytes: Payload::copy_from_slice(chunk),
                },
            )
            .await
            .unwrap();
    }
    let verified = reopened
        .verify_snapshot_step(
            resumed.id,
            VerificationLimits {
                max_chunks: 8,
                max_bytes: 4096,
            },
        )
        .await
        .unwrap();
    assert_eq!(verified.state, SnapshotUploadState::Verified);
    reopened.publish_snapshot(resumed.id).await.unwrap();

    reopened.abort_snapshot(abandoned.id).await.unwrap();
    let mut cleanup_steps = 0;
    let mut cleanup_removed_chunks = 0;
    let mut cleanup_removed_snapshots = 0;
    loop {
        assert!(
            cleanup_steps < CLEANUP_MAX_STEPS,
            "snapshot cleanup did not converge"
        );
        let progress = reopened
            .cleanup_snapshot_staging(SnapshotCleanupLimits {
                max_rows: 1,
                max_bytes: 128 * 1024,
            })
            .await
            .unwrap();
        cleanup_steps += 1;
        cleanup_removed_chunks += progress.removed_chunks;
        cleanup_removed_snapshots += progress.removed_snapshots;
        assert!(progress.removed_chunks + progress.removed_snapshots <= 1);
        if !progress.remaining {
            break;
        }
    }

    let uploads = reopened
        .list_snapshot_uploads(
            None,
            PageLimits {
                max_records: 8,
                max_bytes: 4096,
            },
        )
        .await
        .unwrap();
    assert!(uploads.complete);
    assert!(uploads.entries.is_empty());
    let final_page = reopened
        .list_snapshots(
            &stream,
            None,
            PageLimits {
                max_records: 8,
                max_bytes: 4096,
            },
        )
        .await
        .unwrap();
    assert_eq!(final_page.entries.len(), 2);
    assert!(final_page.entries.contains(&baseline));
    assert!(final_page.entries.contains(&resumed));
    let resumed_snapshot_bytes = read_snapshot(&reopened, resumed.id).await;
    let baseline_snapshot_bytes = read_snapshot(&reopened, baseline.id).await;
    assert_eq!(resumed_snapshot_bytes, resumed_bytes);
    assert_eq!(baseline_snapshot_bytes, baseline_bytes);
    reopened.close().await.unwrap();

    Evidence {
        staged_bytes_after_reopen: resumed_progress.accepted_bytes
            + abandoned_progress.accepted_bytes,
        published_count_while_interrupted: interrupted_page.entries.len(),
        exact_retry_accepted_bytes: exact_retry.accepted_bytes,
        resumed_snapshot_bytes,
        baseline_snapshot_bytes,
        cleanup_steps,
        cleanup_removed_chunks,
        cleanup_removed_snapshots,
        uploads_remaining: uploads.entries.len(),
    }
}
