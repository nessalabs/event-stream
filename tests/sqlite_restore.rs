#![cfg(feature = "sqlite")]

use event_stream::{
    infrastructure::{
        SqliteFailureInjection, SqliteOptions, SqliteRestoreBackend, SqliteRestoreFailureInjection,
        SqliteRestoreManager, SqliteStore,
    },
    CleanupLimits, Error, EventId, EventStore, LifecycleAction, LifecycleOperationId,
    LifecycleRequest, LifecycleStore, NewEvent, PageLimits, Payload, RestoreConfig, RestoreError,
    RestoreOperationId, RestoreReceipt, RestoreRequest, SchemaId, SchemaRef, StreamId,
};
use rusqlite::Connection;
use std::path::PathBuf;
#[cfg(feature = "test-support")]
use std::{
    process::{Child, Command, Stdio},
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::{Duration, Instant},
};

#[cfg(feature = "test-support")]
use event_stream::infrastructure::{SqliteRestoreObserver, SqliteRestoreStage};

#[cfg(feature = "replication")]
#[tokio::test]
async fn controlled_restore_rotates_destination_and_preserves_published_bootstrap() {
    use event_stream::{
        BatchId, BootstrapId, Cursor, DestinationEpoch, IncarnationId, OriginId, OriginStream,
        PublishReplicaBootstrap, Record, ReplicaBatch, ReplicaBatchDestinationStore,
        ReplicaBootstrap, ReplicaBootstrapBatch, ReplicaBootstrapChunk,
        ReplicaBootstrapVerificationLimits, ReplicaDestinationStore, ReplicaId, ReplicaPosition,
        ReplicationError, ReplicationOperationId, SnapshotChunk, SnapshotDescriptor,
        SnapshotDigest, SnapshotId, StreamKey, VerifyReplicaBootstrap,
    };
    use sha2::{Digest, Sha256};
    use std::sync::Arc;

    let root = temp_root("replication-destination-roundtrip");
    let source = root.join("backup.sqlite3");
    let store = SqliteStore::open(SqliteOptions::new(&source))
        .await
        .unwrap();
    let old_epoch = ReplicaBatchDestinationStore::destination_epoch(&store)
        .await
        .unwrap();
    let stream = OriginStream {
        origin: OriginId([41; 16]),
        stream: StreamKey {
            id: StreamId::new("remote-stream").unwrap(),
            incarnation: IncarnationId([42; 16]),
        },
    };
    let content = b"restored opaque replica snapshot";
    let bootstrap = ReplicaBootstrap {
        operation_id: ReplicationOperationId::new("restore-destination-begin").unwrap(),
        id: BootstrapId([43; 16]),
        replica: ReplicaId::new("restored-replica").unwrap(),
        destination_epoch: old_epoch,
        stream: stream.clone(),
        snapshot: SnapshotDescriptor {
            id: SnapshotId([44; 16]),
            covered: Cursor::new(stream.stream.clone(), 1),
            schema: SchemaRef {
                id: SchemaId::new("remote-state").unwrap(),
                version: 1,
            },
            content_bytes: content.len() as u64,
            digest: SnapshotDigest(Sha256::digest(content).into()),
        },
        through: ReplicaPosition {
            stream: stream.clone(),
            offset: 2,
        },
    };
    store
        .begin_replica_bootstrap(bootstrap.clone())
        .await
        .unwrap();
    store
        .put_replica_bootstrap_chunk(ReplicaBootstrapChunk {
            id: bootstrap.id,
            chunk: SnapshotChunk {
                offset: 0,
                bytes: Payload::copy_from_slice(content),
            },
        })
        .await
        .unwrap();
    store
        .put_replica_bootstrap_batch(ReplicaBootstrapBatch {
            id: bootstrap.id,
            batch: ReplicaBatch {
                id: BatchId([45; 16]),
                destination_epoch: old_epoch,
                after: ReplicaPosition {
                    stream: stream.clone(),
                    offset: 1,
                },
                records: vec![Arc::new(Record {
                    cursor: Cursor::new(stream.stream.clone(), 2),
                    event: event("restored-suffix", b"suffix"),
                })],
            },
        })
        .await
        .unwrap();
    assert!(
        store
            .verify_replica_bootstrap_step(VerifyReplicaBootstrap {
                id: bootstrap.id,
                limits: ReplicaBootstrapVerificationLimits {
                    max_chunks: 1,
                    max_records: 1,
                    max_bytes: 4096,
                },
            })
            .await
            .unwrap()
            .complete
    );
    store
        .publish_replica_bootstrap(PublishReplicaBootstrap {
            operation_id: ReplicationOperationId::new("restore-destination-publish").unwrap(),
            id: bootstrap.id,
            destination_epoch: old_epoch,
        })
        .await
        .unwrap();
    store.close().await.unwrap();

    let manager = SqliteRestoreManager::new(
        SqliteRestoreBackend::new(&root).unwrap(),
        RestoreConfig::default(),
    )
    .unwrap();
    let request = RestoreRequest {
        operation_id: RestoreOperationId::new("restore-replication-destination").unwrap(),
        backup_identity: manager.inspect_backup(source.clone()).await.unwrap(),
        source,
        destination: PathBuf::from("restored.sqlite3"),
    };
    let receipt = manager.restore(request).await.unwrap();
    let restored = SqliteStore::open(SqliteOptions::new(&receipt.destination))
        .await
        .unwrap();
    let new_epoch = ReplicaBatchDestinationStore::destination_epoch(&restored)
        .await
        .unwrap();
    assert_ne!(new_epoch, old_epoch);
    assert_eq!(
        restored
            .published_replica_bootstrap(&stream)
            .await
            .unwrap()
            .unwrap()
            .request,
        bootstrap
    );
    let lease = restored
        .acquire_replica_bootstrap_read(&stream, Duration::from_secs(30))
        .await
        .unwrap();
    assert_eq!(
        restored
            .read_replica_bootstrap_bytes(lease.lease, 0, 4096)
            .await
            .unwrap()
            .bytes
            .as_bytes(),
        content
    );
    let page = restored
        .read_replica_after(
            &ReplicaPosition {
                stream: stream.clone(),
                offset: 1,
            },
            event_stream::ReplicaBatchLimits {
                max_records: 1,
                max_bytes: 4096,
            },
        )
        .await
        .unwrap();
    assert_eq!(page.records[0].event.payload.as_bytes(), b"suffix");
    assert!(matches!(
        restored
            .begin_replica_bootstrap(ReplicaBootstrap {
                operation_id: ReplicationOperationId::new("old-epoch-retry").unwrap(),
                id: BootstrapId([46; 16]),
                destination_epoch: DestinationEpoch(old_epoch.0),
                ..bootstrap.clone()
            })
            .await,
        Err(ReplicationError::DestinationReplaced { current }) if current == new_epoch
    ));
    restored.close().await.unwrap();
    std::fs::remove_dir_all(root).unwrap();
}

#[cfg(feature = "test-support")]
#[derive(Debug, Default)]
struct CommitObserver {
    record_pages: AtomicUsize,
}

#[cfg(feature = "snapshots")]
#[tokio::test]
async fn controlled_restore_preserves_snapshot_bytes_and_remaps_stream_identity() {
    use event_stream::{
        Cursor, SnapshotChunk, SnapshotDescriptor, SnapshotDigest, SnapshotId, SnapshotStore,
        VerificationLimits,
    };
    use sha2::{Digest, Sha256};

    let root = temp_root("snapshot-roundtrip");
    let source = root.join("backup.sqlite3");
    let store = SqliteStore::open(SqliteOptions::new(&source))
        .await
        .unwrap();
    let stream = store
        .create_if_absent(&StreamId::new("snapshot-stream").unwrap())
        .await
        .unwrap();
    store
        .append_atomic(&stream, event("snapshot-event", b"suffix"))
        .await
        .unwrap();
    let content = b"opaque application state";
    let descriptor = SnapshotDescriptor {
        id: SnapshotId::from_bytes([71; 16]),
        covered: Cursor::new(stream.clone(), 1),
        schema: SchemaRef {
            id: SchemaId::new("snapshot.state").unwrap(),
            version: 4,
        },
        content_bytes: content.len() as u64,
        digest: SnapshotDigest::from_bytes(Sha256::digest(content).into()),
    };
    store.begin_snapshot(descriptor.clone()).await.unwrap();
    store
        .put_snapshot_chunk(
            descriptor.id,
            SnapshotChunk {
                offset: 0,
                bytes: Payload::copy_from_slice(content),
            },
        )
        .await
        .unwrap();
    store
        .verify_snapshot_step(
            descriptor.id,
            VerificationLimits {
                max_chunks: 4,
                max_bytes: 4096,
            },
        )
        .await
        .unwrap();
    store.publish_snapshot(descriptor.id).await.unwrap();
    store.close().await.unwrap();

    let manager = SqliteRestoreManager::new(
        SqliteRestoreBackend::new(&root).unwrap(),
        RestoreConfig::default(),
    )
    .unwrap();
    let request = RestoreRequest {
        operation_id: RestoreOperationId::new("restore-snapshot").unwrap(),
        backup_identity: manager.inspect_backup(source.clone()).await.unwrap(),
        source,
        destination: PathBuf::from("restored.sqlite3"),
    };
    let receipt = manager.restore(request).await.unwrap();
    let mapping = manager
        .read_mapping(
            receipt.clone(),
            None,
            PageLimits {
                max_records: 8,
                max_bytes: 4096,
            },
        )
        .await
        .unwrap()
        .entries
        .into_iter()
        .find(|entry| entry.old == stream)
        .unwrap();
    assert_ne!(mapping.old.incarnation, mapping.new.incarnation);
    let restored = SqliteStore::open(SqliteOptions::new(&receipt.destination))
        .await
        .unwrap();
    let restored_descriptor = restored
        .snapshot_status(descriptor.id)
        .await
        .unwrap()
        .descriptor;
    assert_eq!(restored_descriptor.covered.stream, mapping.new);
    assert_eq!(restored_descriptor.covered.offset, 1);
    let plan = restored
        .acquire_recovery(descriptor.id, Duration::from_secs(30))
        .await
        .unwrap();
    assert_eq!(
        restored
            .read_snapshot_chunk(plan.lease, 0, 4096)
            .await
            .unwrap()
            .bytes
            .as_bytes(),
        content
    );
    restored.close().await.unwrap();
    std::fs::remove_dir_all(root).unwrap();
}

#[cfg(feature = "retention")]
#[tokio::test]
async fn controlled_restore_preserves_generated_retry_identity_and_merged_history() {
    use event_stream::{
        AdvanceRetryGeneration, EnableRetryPolicy, GeneratedEvent, RetentionOperationId,
        RetentionStore, RetryGeneration,
    };

    let root = temp_root("retention-roundtrip");
    let source = root.join("backup.sqlite3");
    let store = SqliteStore::open(SqliteOptions::new(&source))
        .await
        .unwrap();
    let stream = store
        .create_if_absent(&StreamId::new("retention-stream").unwrap())
        .await
        .unwrap();
    store
        .append_atomic(&stream, event("same", b"legacy"))
        .await
        .unwrap();
    store
        .enable_retry_policy(EnableRetryPolicy {
            operation_id: RetentionOperationId::new("restore-enable").unwrap(),
            stream: stream.clone(),
        })
        .await
        .unwrap();
    store
        .append_generated(
            &stream,
            GeneratedEvent {
                generation: RetryGeneration::FIRST,
                event: event("same", b"first"),
            },
        )
        .await
        .unwrap();
    store
        .advance_retry_generation(AdvanceRetryGeneration {
            operation_id: RetentionOperationId::new("restore-advance").unwrap(),
            stream: stream.clone(),
            expected_current: RetryGeneration::FIRST,
        })
        .await
        .unwrap();
    store
        .append_generated(
            &stream,
            GeneratedEvent {
                generation: RetryGeneration::new(2),
                event: event("same", b"second"),
            },
        )
        .await
        .unwrap();
    store.close().await.unwrap();

    let manager = SqliteRestoreManager::new(
        SqliteRestoreBackend::new(&root).unwrap(),
        RestoreConfig::default(),
    )
    .unwrap();
    let request = RestoreRequest {
        operation_id: RestoreOperationId::new("restore-retention").unwrap(),
        backup_identity: manager.inspect_backup(source.clone()).await.unwrap(),
        source,
        destination: PathBuf::from("restored.sqlite3"),
    };
    let receipt = manager.restore(request).await.unwrap();
    let mapping = manager
        .read_mapping(
            receipt.clone(),
            None,
            PageLimits {
                max_records: 8,
                max_bytes: 4096,
            },
        )
        .await
        .unwrap()
        .entries
        .into_iter()
        .find(|entry| entry.old == stream)
        .unwrap();
    let restored = SqliteStore::open(SqliteOptions::new(&receipt.destination))
        .await
        .unwrap();
    let page = restored
        .read_range(
            &mapping.new,
            0,
            3,
            PageLimits {
                max_records: 3,
                max_bytes: 4096,
            },
        )
        .await
        .unwrap();
    assert_eq!(
        page.records
            .iter()
            .map(|record| record.event.payload.as_bytes())
            .collect::<Vec<_>>(),
        vec![
            b"legacy".as_slice(),
            b"first".as_slice(),
            b"second".as_slice()
        ]
    );
    assert_eq!(
        restored
            .lookup_generated(
                &mapping.new,
                RetryGeneration::FIRST,
                &EventId::new("same").unwrap()
            )
            .await
            .unwrap()
            .unwrap()
            .event
            .payload
            .as_bytes(),
        b"first"
    );
    restored.close().await.unwrap();
    std::fs::remove_dir_all(root).unwrap();
}

#[cfg(feature = "source-journal")]
#[tokio::test]
async fn controlled_restore_preserves_captured_prefix_and_uncheckpointed_output() {
    use event_stream::{
        AdvanceCaptureReceiptFloor, BeginSource, DecodedPosition, EnableRetryPolicy,
        JournalCleanupLimits, JournalOperationId, JournaledOutput, ParserId, ParserRef,
        RawPageLimits, RawSegment, RetentionOperationId, RetentionStore, SourceBinding, SourceId,
        SourceIncarnation, SourceJournalStore, SourceKey, SourcePosition,
    };

    let root = temp_root("journal-roundtrip");
    let source_path = root.join("backup.sqlite3");
    let store = SqliteStore::open(SqliteOptions::new(&source_path))
        .await
        .unwrap();
    let output = store
        .create_if_absent(&StreamId::new("journal-output").unwrap())
        .await
        .unwrap();
    store
        .enable_retry_policy(EnableRetryPolicy {
            operation_id: RetentionOperationId::new("journal-output-enable").unwrap(),
            stream: output.clone(),
        })
        .await
        .unwrap();
    let parser = ParserRef {
        id: ParserId::new("bytes").unwrap(),
        version: 4,
    };
    let raw_only = SourceKey {
        id: SourceId::new("a-raw-only").unwrap(),
        incarnation: SourceIncarnation([31; 16]),
    };
    let output_pending = SourceKey {
        id: SourceId::new("b-output-pending").unwrap(),
        incarnation: SourceIncarnation([32; 16]),
    };
    for (index, source) in [&raw_only, &output_pending].into_iter().enumerate() {
        store
            .begin_source(BeginSource {
                operation_id: JournalOperationId::new(format!("journal-begin-{index}")).unwrap(),
                binding: SourceBinding {
                    source: source.clone(),
                    parser: parser.clone(),
                    output_stream: output.clone(),
                },
            })
            .await
            .unwrap();
        store
            .capture_segment(RawSegment {
                start: SourcePosition {
                    source: source.clone(),
                    offset: 0,
                },
                bytes: Payload::copy_from_slice(b"abc"),
            })
            .await
            .unwrap();
    }
    store
        .advance_capture_receipt_floor(AdvanceCaptureReceiptFloor {
            operation_id: JournalOperationId::new("raw-only-floor").unwrap(),
            source: raw_only.clone(),
            expected_floor: 0,
            new_floor: 3,
        })
        .await
        .unwrap();
    let cleanup = store
        .cleanup_captured(JournalCleanupLimits {
            max_segment_rows: 1,
            max_marker_rows: 1,
            max_receipt_rows: 1,
            max_bytes: 2 * 1024 * 1024,
        })
        .await
        .unwrap();
    assert_eq!(cleanup.removed_receipt_rows, 1);
    assert_eq!(cleanup.removed_segment_rows, 0);
    let pending = JournaledOutput {
        source: output_pending.clone(),
        position: DecodedPosition {
            source_byte: 0,
            item_index: 0,
        },
        event: event("journal-output-0", b"mapped output"),
    };
    store
        .append_captured(&output, pending.clone())
        .await
        .unwrap();
    store.close().await.unwrap();

    let manager = SqliteRestoreManager::new(
        SqliteRestoreBackend::new(&root).unwrap(),
        RestoreConfig::default(),
    )
    .unwrap();
    let request = RestoreRequest {
        operation_id: RestoreOperationId::new("restore-journal").unwrap(),
        backup_identity: manager.inspect_backup(source_path.clone()).await.unwrap(),
        source: source_path,
        destination: PathBuf::from("restored.sqlite3"),
    };
    let receipt = manager.restore(request).await.unwrap();
    let output_mapping = manager
        .read_mapping(
            receipt.clone(),
            None,
            PageLimits {
                max_records: 16,
                max_bytes: 4096,
            },
        )
        .await
        .unwrap()
        .entries
        .into_iter()
        .find(|entry| entry.old == output)
        .unwrap();
    let restored = SqliteStore::open(SqliteOptions::new(&receipt.destination))
        .await
        .unwrap();
    assert_eq!(
        restored
            .source_status(&raw_only)
            .await
            .unwrap()
            .binding
            .output_stream,
        output_mapping.new
    );
    assert_eq!(
        restored
            .read_captured(
                &raw_only,
                0,
                RawPageLimits {
                    max_segments: 1,
                    max_bytes: 3,
                },
            )
            .await
            .unwrap()
            .bytes
            .as_bytes(),
        b"abc"
    );
    assert!(matches!(
        restored
            .capture_segment(RawSegment {
                start: SourcePosition {
                    source: raw_only.clone(),
                    offset: 0,
                },
                bytes: Payload::copy_from_slice(b"abc"),
            })
            .await,
        Err(event_stream::JournalError::CaptureReceiptExpired { floor: 3 })
    ));
    let retried = restored
        .append_captured(&output_mapping.new, pending)
        .await
        .unwrap();
    assert_eq!(retried.kind, event_stream::AppendKind::Deduplicated);
    assert_eq!(retried.record.event.payload.as_bytes(), b"mapped output");
    restored.close().await.unwrap();
    std::fs::remove_dir_all(root).unwrap();
}

#[cfg(feature = "source-journal")]
#[tokio::test]
async fn journal_restore_rejects_forged_zero_charge_with_matching_counters() {
    use event_stream::{
        BeginSource, JournalOperationId, ParserId, ParserRef, RawSegment, SourceBinding, SourceId,
        SourceIncarnation, SourceJournalStore, SourceKey, SourcePosition,
    };

    let root = temp_root("journal-forged-charge");
    let source_path = root.join("backup.sqlite3");
    let store = SqliteStore::open(SqliteOptions::new(&source_path))
        .await
        .unwrap();
    let output = store
        .create_if_absent(&StreamId::new("journal-output").unwrap())
        .await
        .unwrap();
    let source = SourceKey {
        id: SourceId::new(format!("source-{}", "x".repeat(192))).unwrap(),
        incarnation: SourceIncarnation([33; 16]),
    };
    store
        .begin_source(BeginSource {
            operation_id: JournalOperationId::new("journal-forged-begin").unwrap(),
            binding: SourceBinding {
                source: source.clone(),
                parser: ParserRef {
                    id: ParserId::new("bytes").unwrap(),
                    version: 1,
                },
                output_stream: output,
            },
        })
        .await
        .unwrap();
    store
        .capture_segment(RawSegment {
            start: SourcePosition { source, offset: 0 },
            bytes: Payload::copy_from_slice(b"abc"),
        })
        .await
        .unwrap();
    store.close().await.unwrap();

    let raw = Connection::open(&source_path).unwrap();
    let charge: i64 = raw
        .query_row("SELECT charge FROM journal_capture_receipts", [], |row| {
            row.get(0)
        })
        .unwrap();
    raw.execute("UPDATE journal_capture_receipts SET charge=0", [])
        .unwrap();
    raw.execute(
        "UPDATE journal_metadata SET receipt_bytes=receipt_bytes-?1 WHERE singleton=1",
        [charge],
    )
    .unwrap();
    drop(raw);

    let manager = SqliteRestoreManager::new(
        SqliteRestoreBackend::new(&root).unwrap(),
        RestoreConfig::default(),
    )
    .unwrap();
    let request = RestoreRequest {
        operation_id: RestoreOperationId::new("restore-forged-journal").unwrap(),
        backup_identity: manager.inspect_backup(source_path.clone()).await.unwrap(),
        source: source_path,
        destination: PathBuf::from("restored.sqlite3"),
    };
    assert!(matches!(
        manager.restore(request).await,
        Err(RestoreError::CorruptBackup(_))
    ));
    assert!(!root.join("restored.sqlite3").exists());
    std::fs::remove_dir_all(root).unwrap();
}

#[cfg(feature = "retention")]
#[tokio::test]
async fn retention_restore_rejects_oversized_receipt_and_honors_page_byte_limit() {
    use event_stream::{EnableRetryPolicy, RetentionOperationId, RetentionStore};

    async fn source_with_receipt(
        root: &std::path::Path,
        name: &str,
        stream_id: &str,
        operation_id: &str,
    ) -> PathBuf {
        let source = root.join(name);
        let store = SqliteStore::open(SqliteOptions::new(&source))
            .await
            .unwrap();
        let stream = store
            .create_if_absent(&StreamId::new(stream_id).unwrap())
            .await
            .unwrap();
        store
            .enable_retry_policy(EnableRetryPolicy {
                operation_id: RetentionOperationId::new(operation_id).unwrap(),
                stream,
            })
            .await
            .unwrap();
        store.close().await.unwrap();
        source
    }

    let root = temp_root("retention-malformed");
    let source = source_with_receipt(
        &root,
        "oversized.sqlite3",
        "bounded-retention",
        "bounded-enable",
    )
    .await;
    let huge = "x".repeat(1024 * 1024);
    let conn = Connection::open(&source).unwrap();
    conn.pragma_update(None, "ignore_check_constraints", true)
        .unwrap();
    conn.execute(
        "UPDATE retention_receipts SET operation_id=?1,charge=?2",
        rusqlite::params![huge, 1024 * 1024 + "bounded-retention".len() + 256],
    )
    .unwrap();
    conn.execute(
        "UPDATE retention_metadata SET receipt_bytes=?1",
        [1024 * 1024 + "bounded-retention".len() + 256],
    )
    .unwrap();
    drop(conn);
    let manager = SqliteRestoreManager::new(
        SqliteRestoreBackend::new(&root).unwrap(),
        RestoreConfig::default(),
    )
    .unwrap();
    let request = RestoreRequest {
        operation_id: RestoreOperationId::new("oversized-receipt").unwrap(),
        backup_identity: manager.inspect_backup(source.clone()).await.unwrap(),
        source,
        destination: PathBuf::from("oversized-restored.sqlite3"),
    };
    assert!(matches!(
        manager.restore(request).await,
        Err(RestoreError::CorruptBackup(_))
    ));

    let source = source_with_receipt(
        &root,
        "low-page.sqlite3",
        "bounded-retention",
        "bounded-enable",
    )
    .await;
    let mut config = RestoreConfig::default();
    config.page.max_bytes = 640;
    let manager =
        SqliteRestoreManager::new(SqliteRestoreBackend::new(&root).unwrap(), config).unwrap();
    let request = RestoreRequest {
        operation_id: RestoreOperationId::new("low-page-retention").unwrap(),
        backup_identity: manager.inspect_backup(source.clone()).await.unwrap(),
        source,
        destination: PathBuf::from("low-page-restored.sqlite3"),
    };
    manager.restore(request).await.unwrap();

    let large_id = "s".repeat(256);
    let large_operation = "o".repeat(256);
    let source = source_with_receipt(
        &root,
        "large-valid-receipt.sqlite3",
        &large_id,
        &large_operation,
    )
    .await;
    let manager = SqliteRestoreManager::new(
        SqliteRestoreBackend::new(&root).unwrap(),
        RestoreConfig {
            page: event_stream::PageLimits {
                max_records: RestoreConfig::default().page.max_records,
                max_bytes: 640,
            },
            ..RestoreConfig::default()
        },
    )
    .unwrap();
    let request = RestoreRequest {
        operation_id: RestoreOperationId::new("large-valid-receipt").unwrap(),
        backup_identity: manager.inspect_backup(source.clone()).await.unwrap(),
        source,
        destination: PathBuf::from("large-valid-receipt-restored.sqlite3"),
    };
    assert!(matches!(
        manager.restore(request).await,
        Err(RestoreError::CapacityExceeded)
    ));
    std::fs::remove_dir_all(root).unwrap();
}

#[cfg(feature = "retention")]
#[tokio::test]
async fn retention_restore_keyset_pages_many_pending_cleanup_ranges() {
    use event_stream::{
        AdvanceRetentionFloor, EnableRetryPolicy, RetentionOperationId, RetentionStore,
    };

    let root = temp_root("retention-many-pending");
    let source = root.join("backup.sqlite3");
    let store = SqliteStore::open(SqliteOptions::new(&source))
        .await
        .unwrap();
    for index in 0..65 {
        let stream = store
            .create_if_absent(&StreamId::new(format!("pending-{index:03}")).unwrap())
            .await
            .unwrap();
        store
            .append_atomic(&stream, event("legacy", &[index as u8]))
            .await
            .unwrap();
        store
            .enable_retry_policy(EnableRetryPolicy {
                operation_id: RetentionOperationId::new(format!("enable-{index:03}")).unwrap(),
                stream: stream.clone(),
            })
            .await
            .unwrap();
        store
            .advance_retention_floor(AdvanceRetentionFloor {
                operation_id: RetentionOperationId::new(format!("floor-{index:03}")).unwrap(),
                stream: stream.clone(),
                expected_floor: event_stream::Cursor::new(stream.clone(), 0),
                new_floor: event_stream::Cursor::new(stream, 1),
            })
            .await
            .unwrap();
    }
    store.close().await.unwrap();
    let mut config = RestoreConfig::default();
    config.page.max_records = 3;
    let manager =
        SqliteRestoreManager::new(SqliteRestoreBackend::new(&root).unwrap(), config).unwrap();
    let request = RestoreRequest {
        operation_id: RestoreOperationId::new("restore-many-pending").unwrap(),
        backup_identity: manager.inspect_backup(source.clone()).await.unwrap(),
        source,
        destination: PathBuf::from("restored.sqlite3"),
    };
    let receipt = manager.restore(request).await.unwrap();
    let conn = Connection::open(&receipt.destination).unwrap();
    assert_eq!(
        conn.query_row("SELECT count(*) FROM retention_cleanup", [], |row| row
            .get::<_, i64>(0))
            .unwrap(),
        65
    );
    assert_eq!(
        conn.query_row(
            "SELECT pending_count FROM retention_metadata WHERE singleton=1",
            [],
            |row| row.get::<_, i64>(0)
        )
        .unwrap(),
        65
    );
    drop(conn);
    std::fs::remove_dir_all(root).unwrap();
}

#[cfg(feature = "retention")]
#[tokio::test]
async fn retention_restore_accepts_expired_prefix_between_bounded_cleanup_turns() {
    use event_stream::{
        AdvanceRetentionFloor, EnableRetryPolicy, ExpireRetryGenerations, RetentionCleanupLimits,
        RetentionOperationId, RetentionStore, RetryGeneration,
    };

    let root = temp_root("retention-partial-cleanup");
    let source = root.join("backup.sqlite3");
    let retained = event("legacy", &[7; 512]);
    let mut options = SqliteOptions::new(&source);
    options.max_record_bytes = retained.accounted_bytes();
    let store = SqliteStore::open(options).await.unwrap();
    let stream = store
        .create_if_absent(&StreamId::new("partial-cleanup").unwrap())
        .await
        .unwrap();
    store
        .append_atomic(&stream, retained.clone())
        .await
        .unwrap();
    store
        .enable_retry_policy(EnableRetryPolicy {
            operation_id: RetentionOperationId::new("partial-enable").unwrap(),
            stream: stream.clone(),
        })
        .await
        .unwrap();
    store
        .advance_retention_floor(AdvanceRetentionFloor {
            operation_id: RetentionOperationId::new("partial-floor").unwrap(),
            stream: stream.clone(),
            expected_floor: event_stream::Cursor::new(stream.clone(), 0),
            new_floor: event_stream::Cursor::new(stream.clone(), 1),
        })
        .await
        .unwrap();
    store
        .expire_retry_generations(ExpireRetryGenerations {
            operation_id: RetentionOperationId::new("partial-expire").unwrap(),
            stream: stream.clone(),
            expected_oldest: RetryGeneration::LEGACY,
            retain_from: RetryGeneration::FIRST,
        })
        .await
        .unwrap();
    let first_turn = store
        .cleanup_retention(RetentionCleanupLimits {
            max_event_rows: 1,
            max_retry_rows: 1,
            max_bytes: retained.accounted_bytes() + 384,
        })
        .await
        .unwrap();
    assert_eq!(first_turn.removed_retry_rows, 1);
    assert_eq!(first_turn.removed_event_rows, 0);
    assert!(first_turn.remaining);
    store.close().await.unwrap();

    let manager = SqliteRestoreManager::new(
        SqliteRestoreBackend::new(&root).unwrap(),
        RestoreConfig::default(),
    )
    .unwrap();
    let request = RestoreRequest {
        operation_id: RestoreOperationId::new("restore-partial-cleanup").unwrap(),
        backup_identity: manager.inspect_backup(source.clone()).await.unwrap(),
        source,
        destination: PathBuf::from("restored.sqlite3"),
    };
    let receipt = manager.restore(request).await.unwrap();
    let mapping = manager
        .read_mapping(
            receipt.clone(),
            None,
            PageLimits {
                max_records: 8,
                max_bytes: 4096,
            },
        )
        .await
        .unwrap()
        .entries
        .into_iter()
        .find(|entry| entry.old == stream)
        .unwrap();
    let restored = SqliteStore::open(SqliteOptions::new(&receipt.destination))
        .await
        .unwrap();
    let second_turn = restored
        .cleanup_retention(RetentionCleanupLimits {
            max_event_rows: 1,
            max_retry_rows: 1,
            max_bytes: 2 * 1024 * 1024,
        })
        .await
        .unwrap();
    assert_eq!(second_turn.removed_retry_rows, 0);
    assert_eq!(second_turn.removed_event_rows, 1);
    assert!(!second_turn.remaining);
    assert!(restored
        .read_range(
            &mapping.new,
            1,
            1,
            PageLimits {
                max_records: 1,
                max_bytes: 4096,
            },
        )
        .await
        .unwrap()
        .records
        .is_empty());
    restored.close().await.unwrap();
    std::fs::remove_dir_all(root).unwrap();
}

#[cfg(feature = "snapshots")]
#[tokio::test]
async fn controlled_restore_rejects_snapshot_payload_that_does_not_match_digest() {
    use event_stream::{
        Cursor, SnapshotChunk, SnapshotDescriptor, SnapshotDigest, SnapshotId, SnapshotStore,
        VerificationLimits,
    };
    use sha2::{Digest, Sha256};

    let root = temp_root("snapshot-corrupt-content");
    let source = root.join("backup.sqlite3");
    let store = SqliteStore::open(SqliteOptions::new(&source))
        .await
        .unwrap();
    let stream = store
        .create_if_absent(&StreamId::new("snapshot-corrupt-content").unwrap())
        .await
        .unwrap();
    let descriptor = SnapshotDescriptor {
        id: SnapshotId::from_bytes([72; 16]),
        covered: Cursor::new(stream, 0),
        schema: SchemaRef {
            id: SchemaId::new("snapshot.state").unwrap(),
            version: 1,
        },
        content_bytes: 4,
        digest: SnapshotDigest::from_bytes(Sha256::digest(b"good").into()),
    };
    store.begin_snapshot(descriptor.clone()).await.unwrap();
    store
        .put_snapshot_chunk(
            descriptor.id,
            SnapshotChunk {
                offset: 0,
                bytes: Payload::copy_from_slice(b"good"),
            },
        )
        .await
        .unwrap();
    store
        .verify_snapshot_step(
            descriptor.id,
            VerificationLimits {
                max_chunks: 1,
                max_bytes: 4,
            },
        )
        .await
        .unwrap();
    store.publish_snapshot(descriptor.id).await.unwrap();
    store.close().await.unwrap();
    Connection::open(&source)
        .unwrap()
        .execute(
            "UPDATE snapshot_chunks SET bytes=?1 WHERE snapshot_id=?2",
            rusqlite::params![b"evil".as_slice(), descriptor.id.as_bytes().as_slice()],
        )
        .unwrap();

    let manager = SqliteRestoreManager::new(
        SqliteRestoreBackend::new(&root).unwrap(),
        RestoreConfig::default(),
    )
    .unwrap();
    let request = RestoreRequest {
        operation_id: RestoreOperationId::new("restore-corrupt-snapshot").unwrap(),
        backup_identity: manager.inspect_backup(source.clone()).await.unwrap(),
        source,
        destination: PathBuf::from("restored.sqlite3"),
    };
    assert!(matches!(
        manager.restore(request).await,
        Err(RestoreError::CorruptBackup(_))
    ));
    std::fs::remove_dir_all(root).unwrap();
}

#[cfg(feature = "snapshots")]
#[tokio::test]
async fn controlled_restore_accepts_partial_and_complete_aborted_cleanup_states() {
    use event_stream::{
        Cursor, SnapshotChunk, SnapshotCleanupLimits, SnapshotDescriptor, SnapshotDigest,
        SnapshotId, SnapshotStore, SnapshotUploadState,
    };
    use sha2::{Digest, Sha256};

    let root = temp_root("snapshot-aborted-cleanup");
    let source = root.join("backup.sqlite3");
    let store = SqliteStore::open(SqliteOptions::new(&source))
        .await
        .unwrap();
    let stream = store
        .create_if_absent(&StreamId::new("snapshot-aborted-cleanup").unwrap())
        .await
        .unwrap();
    let descriptor = SnapshotDescriptor {
        id: SnapshotId::from_bytes([73; 16]),
        covered: Cursor::new(stream, 0),
        schema: SchemaRef {
            id: SchemaId::new("snapshot.state").unwrap(),
            version: 1,
        },
        content_bytes: 2,
        digest: SnapshotDigest::from_bytes(Sha256::digest(b"ab").into()),
    };
    store.begin_snapshot(descriptor.clone()).await.unwrap();
    for (offset, byte) in [(0, b"a".as_slice()), (1, b"b".as_slice())] {
        store
            .put_snapshot_chunk(
                descriptor.id,
                SnapshotChunk {
                    offset,
                    bytes: Payload::copy_from_slice(byte),
                },
            )
            .await
            .unwrap();
    }
    store.abort_snapshot(descriptor.id).await.unwrap();
    let partial = store
        .cleanup_snapshot_staging(SnapshotCleanupLimits {
            max_rows: 1,
            max_bytes: 2 * 1024 * 1024,
        })
        .await
        .unwrap();
    assert_eq!(partial.removed_chunks, 1);
    assert_eq!(partial.removed_snapshots, 0);
    store.close().await.unwrap();

    for (operation, destination) in [
        ("restore-partial-abort", "partial.sqlite3"),
        ("restore-empty-pending-abort", "empty-pending.sqlite3"),
        ("restore-cleaned-abort", "cleaned.sqlite3"),
    ] {
        let manager = SqliteRestoreManager::new(
            SqliteRestoreBackend::new(&root).unwrap(),
            RestoreConfig::default(),
        )
        .unwrap();
        let request = RestoreRequest {
            operation_id: RestoreOperationId::new(operation).unwrap(),
            backup_identity: manager.inspect_backup(source.clone()).await.unwrap(),
            source: source.clone(),
            destination: PathBuf::from(destination),
        };
        let receipt = manager.restore(request).await.unwrap();
        let restored = SqliteStore::open(SqliteOptions::new(&receipt.destination))
            .await
            .unwrap();
        assert_eq!(
            restored.snapshot_status(descriptor.id).await.unwrap().state,
            SnapshotUploadState::Aborted
        );
        if operation == "restore-partial-abort" {
            assert_eq!(
                restored
                    .put_snapshot_chunk(
                        descriptor.id,
                        SnapshotChunk {
                            offset: 1,
                            bytes: Payload::copy_from_slice(b"b"),
                        },
                    )
                    .await
                    .unwrap()
                    .state,
                SnapshotUploadState::Aborted
            );
            assert!(matches!(
                restored
                    .put_snapshot_chunk(
                        descriptor.id,
                        SnapshotChunk {
                            offset: 1,
                            bytes: Payload::copy_from_slice(b"x"),
                        },
                    )
                    .await,
                Err(event_stream::SnapshotError::OperationConflict { .. })
            ));
            let cleanup = restored
                .cleanup_snapshot_staging(SnapshotCleanupLimits {
                    max_rows: 2,
                    max_bytes: 2 * 1024 * 1024,
                })
                .await
                .unwrap();
            assert_eq!(cleanup.removed_chunks, 1);
            assert_eq!(cleanup.removed_snapshots, 1);
        } else if operation == "restore-empty-pending-abort" {
            let uploads = restored
                .list_snapshot_uploads(
                    None,
                    PageLimits {
                        max_records: 8,
                        max_bytes: 4096,
                    },
                )
                .await
                .unwrap();
            assert_eq!(uploads.entries.len(), 1);
            let cleanup = restored
                .cleanup_snapshot_staging(SnapshotCleanupLimits {
                    max_rows: 1,
                    max_bytes: 2 * 1024 * 1024,
                })
                .await
                .unwrap();
            assert_eq!(cleanup.removed_chunks, 0);
            assert_eq!(cleanup.removed_snapshots, 1);
        } else {
            let uploads = restored
                .list_snapshot_uploads(
                    None,
                    PageLimits {
                        max_records: 8,
                        max_bytes: 4096,
                    },
                )
                .await
                .unwrap();
            assert!(uploads.entries.is_empty());
            assert!(uploads.complete);
        }
        restored.close().await.unwrap();
        if operation == "restore-partial-abort" {
            let source_store = SqliteStore::open(SqliteOptions::new(&source))
                .await
                .unwrap();
            let final_cleanup = source_store
                .cleanup_snapshot_staging(SnapshotCleanupLimits {
                    max_rows: 1,
                    max_bytes: 2 * 1024 * 1024,
                })
                .await
                .unwrap();
            assert_eq!(final_cleanup.removed_chunks, 1);
            assert_eq!(final_cleanup.removed_snapshots, 0);
            source_store.close().await.unwrap();
        } else if operation == "restore-empty-pending-abort" {
            let source_store = SqliteStore::open(SqliteOptions::new(&source))
                .await
                .unwrap();
            let final_cleanup = source_store
                .cleanup_snapshot_staging(SnapshotCleanupLimits {
                    max_rows: 1,
                    max_bytes: 2 * 1024 * 1024,
                })
                .await
                .unwrap();
            assert_eq!(final_cleanup.removed_chunks, 0);
            assert_eq!(final_cleanup.removed_snapshots, 1);
            source_store.close().await.unwrap();
        }
    }
    std::fs::remove_dir_all(root).unwrap();
}

#[cfg(not(feature = "snapshots"))]
#[tokio::test]
async fn snapshot_bearing_backup_is_explicitly_rejected_when_support_is_disabled() {
    let root = temp_root("snapshot-feature-off");
    let source = root.join("backup.sqlite3");
    let store = SqliteStore::open(SqliteOptions::new(&source))
        .await
        .unwrap();
    store.close().await.unwrap();
    Connection::open(&source)
        .unwrap()
        .execute_batch(
            "CREATE TABLE snapshot_metadata(singleton INTEGER);
         CREATE TABLE snapshots(snapshot_id BLOB);
         CREATE TABLE snapshot_chunks(snapshot_id BLOB);",
        )
        .unwrap();
    let manager = SqliteRestoreManager::new(
        SqliteRestoreBackend::new(&root).unwrap(),
        RestoreConfig::default(),
    )
    .unwrap();
    let request = RestoreRequest {
        operation_id: RestoreOperationId::new("restore-snapshot-feature-off").unwrap(),
        backup_identity: manager.inspect_backup(source.clone()).await.unwrap(),
        source,
        destination: PathBuf::from("restored.sqlite3"),
    };
    assert_eq!(
        manager.restore(request).await.unwrap_err(),
        RestoreError::UnsupportedBackup(2)
    );
    std::fs::remove_dir_all(root).unwrap();
}

#[cfg(feature = "replication")]
#[tokio::test]
async fn controlled_restore_rotates_origin_and_detaches_imported_replica() {
    use event_stream::{
        AttachReplica, DestinationEpoch, OriginStream, ReplicaId, ReplicaStart,
        ReplicationOperationId, ReplicationOriginStore,
    };
    use std::time::Duration;

    let root = temp_root("replication-restore-boundary");
    let source = root.join("backup.sqlite3");
    let store = SqliteStore::open(SqliteOptions::new(&source))
        .await
        .unwrap();
    let stream = store
        .create_if_absent(&StreamId::new("replication-restore").unwrap())
        .await
        .unwrap();
    let origin = OriginStream {
        origin: store.origin_identity().await.unwrap(),
        stream: stream.clone(),
    };
    store
        .attach_replica(AttachReplica {
            operation_id: ReplicationOperationId::new("replication-restore-attach").unwrap(),
            replica: ReplicaId::new("replication-restore-replica").unwrap(),
            stream: origin.clone(),
            destination_epoch: DestinationEpoch([81; 16]),
            max_backlog_bytes: 4096,
            max_backlog_age: Duration::from_secs(60),
            start: ReplicaStart::FromBeginning,
        })
        .await
        .unwrap();
    store.close().await.unwrap();
    let manager = SqliteRestoreManager::new(
        SqliteRestoreBackend::new(&root).unwrap(),
        RestoreConfig::default(),
    )
    .unwrap();
    let request = RestoreRequest {
        operation_id: RestoreOperationId::new("restore-replication-boundary").unwrap(),
        backup_identity: manager.inspect_backup(source.clone()).await.unwrap(),
        source,
        destination: PathBuf::from("restored.sqlite3"),
    };
    let receipt = manager.restore(request).await.unwrap();
    let mapping = manager
        .read_mapping(
            receipt.clone(),
            None,
            PageLimits {
                max_records: 8,
                max_bytes: 4096,
            },
        )
        .await
        .unwrap()
        .entries
        .into_iter()
        .find(|entry| entry.old == stream)
        .unwrap();
    let restored = SqliteStore::open(SqliteOptions::new(&receipt.destination))
        .await
        .unwrap();
    let restored_origin = restored.origin_identity().await.unwrap();
    assert_ne!(restored_origin, origin.origin);
    let transformed = OriginStream {
        origin: restored_origin,
        stream: mapping.new,
    };
    let status = restored
        .replica_status(
            &ReplicaId::new("replication-restore-replica").unwrap(),
            &transformed,
        )
        .await
        .unwrap();
    assert_eq!(
        status.mode,
        event_stream::ReplicaMode::DetachedNeedsBootstrap
    );
    assert_eq!(status.backlog_records, 0);
    restored.close().await.unwrap();
    std::fs::remove_dir_all(root).unwrap();
}

#[cfg(feature = "snapshots")]
#[tokio::test]
async fn restore_keeps_published_snapshot_after_its_retired_lifetime_was_cleaned() {
    use event_stream::{
        Cursor, SnapshotDescriptor, SnapshotDigest, SnapshotError, SnapshotId, SnapshotStore,
        VerificationLimits,
    };
    use sha2::{Digest, Sha256};

    let root = temp_root("snapshot-cleaned-lifetime");
    let source = root.join("backup.sqlite3");
    let store = SqliteStore::open(SqliteOptions::new(&source))
        .await
        .unwrap();
    let stream = store
        .create_if_absent(&StreamId::new("snapshot-cleaned-lifetime").unwrap())
        .await
        .unwrap();
    store
        .append_atomic(&stream, event("one", b"removed"))
        .await
        .unwrap();
    let descriptor = SnapshotDescriptor {
        id: SnapshotId::from_bytes([74; 16]),
        covered: Cursor::new(stream.clone(), 1),
        schema: SchemaRef {
            id: SchemaId::new("snapshot.state").unwrap(),
            version: 1,
        },
        content_bytes: 0,
        digest: SnapshotDigest::from_bytes(Sha256::digest([]).into()),
    };
    store.begin_snapshot(descriptor.clone()).await.unwrap();
    store
        .verify_snapshot_step(
            descriptor.id,
            VerificationLimits {
                max_chunks: 1,
                max_bytes: 1,
            },
        )
        .await
        .unwrap();
    store.publish_snapshot(descriptor.id).await.unwrap();
    store
        .change_lifecycle(LifecycleRequest {
            operation_id: LifecycleOperationId::new("retire-snapshot-stream").unwrap(),
            expected: stream,
            action: LifecycleAction::Reset,
        })
        .await
        .unwrap();
    store
        .cleanup_retired(CleanupLimits {
            max_records: 8,
            max_bytes: 2 * 1024 * 1024,
        })
        .await
        .unwrap();
    store.close().await.unwrap();

    let manager = SqliteRestoreManager::new(
        SqliteRestoreBackend::new(&root).unwrap(),
        RestoreConfig::default(),
    )
    .unwrap();
    let request = RestoreRequest {
        operation_id: RestoreOperationId::new("restore-cleaned-lifetime-snapshot").unwrap(),
        backup_identity: manager.inspect_backup(source.clone()).await.unwrap(),
        source,
        destination: PathBuf::from("restored.sqlite3"),
    };
    let receipt = manager.restore(request).await.unwrap();
    let restored = SqliteStore::open(SqliteOptions::new(&receipt.destination))
        .await
        .unwrap();
    assert_eq!(
        restored
            .snapshot_status(descriptor.id)
            .await
            .unwrap()
            .descriptor
            .id,
        descriptor.id
    );
    assert!(matches!(
        restored
            .acquire_recovery(descriptor.id, Duration::from_secs(10))
            .await,
        Err(SnapshotError::MissingHistory { .. })
    ));
    restored.close().await.unwrap();
    std::fs::remove_dir_all(root).unwrap();
}

#[cfg(feature = "test-support")]
impl SqliteRestoreObserver for CommitObserver {
    fn observe(&self, stage: SqliteRestoreStage) {
        if stage == SqliteRestoreStage::RecordPageCommitted {
            self.record_pages.fetch_add(1, Ordering::Relaxed);
        }
    }
}

#[cfg(feature = "test-support")]
struct ChildGuard(Option<Child>);

#[cfg(feature = "test-support")]
impl ChildGuard {
    fn kill_and_wait(mut self) -> std::process::ExitStatus {
        let mut child = self.0.take().unwrap();
        child.kill().unwrap();
        child.wait().unwrap()
    }
}

#[cfg(feature = "test-support")]
impl Drop for ChildGuard {
    fn drop(&mut self) {
        if let Some(child) = &mut self.0 {
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}

fn temp_root(label: &str) -> PathBuf {
    let root = std::env::temp_dir().join(format!(
        "event-stream-restore-{label}-{}",
        uuid::Uuid::new_v4()
    ));
    std::fs::create_dir(&root).unwrap();
    root.canonicalize().unwrap()
}

async fn simple_restore_fixture(
    label: &str,
    failure: Option<SqliteRestoreFailureInjection>,
) -> (PathBuf, PathBuf, RestoreRequest, SqliteRestoreManager) {
    let root = temp_root(label);
    let source = root.join("backup.sqlite3");
    let store = SqliteStore::open(SqliteOptions::new(&source))
        .await
        .unwrap();
    let stream = store
        .create_if_absent(&StreamId::new("live").unwrap())
        .await
        .unwrap();
    store
        .append_atomic(&stream, event("one", b"restore publication state"))
        .await
        .unwrap();
    store.close().await.unwrap();

    let backend = match failure {
        Some(failure) => SqliteRestoreBackend::new(&root)
            .unwrap()
            .with_failure_injection(failure),
        None => SqliteRestoreBackend::new(&root).unwrap(),
    };
    let manager = SqliteRestoreManager::new(backend, RestoreConfig::default()).unwrap();
    let identity = manager.inspect_backup(source.clone()).await.unwrap();
    let request = RestoreRequest {
        operation_id: RestoreOperationId::new(format!("restore-{label}")).unwrap(),
        backup_identity: identity,
        source: source.clone(),
        destination: PathBuf::from("restored.sqlite3"),
    };
    (root, source, request, manager)
}

fn staging_file(root: &std::path::Path) -> PathBuf {
    let mut matches = std::fs::read_dir(root)
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .filter(|path| {
            path.file_name()
                .and_then(|name| name.to_str())
                .is_some_and(|name| {
                    name.starts_with(".event-stream-restore-") && name.ends_with(".sqlite3")
                })
        });
    let staging = matches.next().expect("restore staging file");
    assert!(matches.next().is_none(), "one deterministic staging file");
    staging
}

fn owner_directory(root: &std::path::Path) -> PathBuf {
    let mut matches = std::fs::read_dir(root)
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .filter(|path| {
            path.file_name()
                .and_then(|name| name.to_str())
                .is_some_and(|name| {
                    name.starts_with(".event-stream-restore-") && name.ends_with(".owner")
                })
        });
    let owner = matches.next().expect("restore owner directory");
    assert!(matches.next().is_none(), "one operation owner directory");
    owner
}

fn downgrade_to_legacy_format_two(path: &std::path::Path) {
    let conn = Connection::open(path).unwrap();
    conn.execute_batch(
        "BEGIN IMMEDIATE;
         CREATE TABLE event_stream_metadata_legacy(
           singleton INTEGER PRIMARY KEY CHECK(singleton=1),
           format_version INTEGER NOT NULL,
           lifecycle_receipt_count INTEGER NOT NULL DEFAULT 0 CHECK(lifecycle_receipt_count>=0),
           lifecycle_receipt_bytes INTEGER NOT NULL DEFAULT 0 CHECK(lifecycle_receipt_bytes>=0),
           retired_lifetime_count INTEGER NOT NULL DEFAULT 0 CHECK(retired_lifetime_count>=0),
           retired_metadata_bytes INTEGER NOT NULL DEFAULT 0 CHECK(retired_metadata_bytes>=0)
         );
         INSERT INTO event_stream_metadata_legacy
           SELECT singleton,format_version,lifecycle_receipt_count,lifecycle_receipt_bytes,
                  retired_lifetime_count,retired_metadata_bytes
           FROM event_stream_metadata;
         DROP TABLE event_stream_metadata;
         ALTER TABLE event_stream_metadata_legacy RENAME TO event_stream_metadata;
         DROP TABLE restore_metadata;
         DROP TABLE restore_mappings;
         COMMIT;",
    )
    .unwrap();
}

fn event(id: &str, payload: &[u8]) -> NewEvent {
    NewEvent {
        id: EventId::new(id).unwrap(),
        schema: SchemaRef {
            id: SchemaId::new("restore.test").unwrap(),
            version: 7,
        },
        payload: Payload::copy_from_slice(payload),
    }
}

#[cfg(feature = "test-support")]
#[tokio::test]
async fn record_pages_batch_across_streams_and_skip_empty_lifetimes() {
    let root = temp_root("cross-stream-record-pages");
    let source = root.join("backup.sqlite3");
    let store = SqliteStore::open(SqliteOptions::new(&source))
        .await
        .unwrap();
    for index in 0..5 {
        let stream = store
            .create_if_absent(&StreamId::new(format!("stream-{index}")).unwrap())
            .await
            .unwrap();
        store
            .append_atomic(
                &stream,
                event(&format!("event-{index}"), &[index as u8; 16]),
            )
            .await
            .unwrap();
    }
    store
        .create_if_absent(&StreamId::new("empty-stream").unwrap())
        .await
        .unwrap();
    store.close().await.unwrap();

    let observer = Arc::new(CommitObserver::default());
    let backend = SqliteRestoreBackend::new(&root)
        .unwrap()
        .with_observer(observer.clone());
    let config = RestoreConfig {
        page: PageLimits {
            max_records: 2,
            max_bytes: 4096,
        },
        ..RestoreConfig::default()
    };
    let manager = SqliteRestoreManager::new(backend, config).unwrap();
    let identity = manager.inspect_backup(source.clone()).await.unwrap();
    let receipt = manager
        .restore(RestoreRequest {
            operation_id: RestoreOperationId::new("cross-stream-pages").unwrap(),
            backup_identity: identity,
            source,
            destination: PathBuf::from("restored.sqlite3"),
        })
        .await
        .unwrap();
    assert_eq!(
        observer.record_pages.load(Ordering::Relaxed),
        3,
        "five one-record streams use ceil(5 / 2) record transactions"
    );

    let restored = SqliteStore::open(SqliteOptions::new(&receipt.destination))
        .await
        .unwrap();
    for index in 0..5 {
        let stream = restored
            .create_if_absent(&StreamId::new(format!("stream-{index}")).unwrap())
            .await
            .unwrap();
        let page = restored
            .read_range(
                &stream,
                0,
                1,
                PageLimits {
                    max_records: 1,
                    max_bytes: 4096,
                },
            )
            .await
            .unwrap();
        assert_eq!(page.records.len(), 1);
        assert_eq!(page.records[0].event.id.as_str(), format!("event-{index}"));
        assert_eq!(page.records[0].event.payload.as_bytes(), &[index as u8; 16]);
    }
    let empty = restored
        .create_if_absent(&StreamId::new("empty-stream").unwrap())
        .await
        .unwrap();
    assert_eq!(restored.bounds(&empty).await.unwrap().tail.offset, 0);
    restored.close().await.unwrap();
    std::fs::remove_dir_all(root).unwrap();
}

#[cfg(feature = "test-support")]
#[tokio::test]
async fn record_page_byte_limit_flushes_across_streams() {
    let root = temp_root("record-byte-pages");
    let source = root.join("backup.sqlite3");
    let store = SqliteStore::open(SqliteOptions::new(&source))
        .await
        .unwrap();
    for index in 0..3 {
        let stream = store
            .create_if_absent(&StreamId::new(format!("byte-stream-{index}")).unwrap())
            .await
            .unwrap();
        store
            .append_atomic(
                &stream,
                event(&format!("byte-event-{index}"), &[index as u8; 500]),
            )
            .await
            .unwrap();
    }
    store.close().await.unwrap();

    let observer = Arc::new(CommitObserver::default());
    let backend = SqliteRestoreBackend::new(&root)
        .unwrap()
        .with_observer(observer.clone());
    let config = RestoreConfig {
        page: PageLimits {
            max_records: 16,
            max_bytes: 700,
        },
        ..RestoreConfig::default()
    };
    let manager = SqliteRestoreManager::new(backend, config).unwrap();
    let identity = manager.inspect_backup(source.clone()).await.unwrap();
    manager
        .restore(RestoreRequest {
            operation_id: RestoreOperationId::new("byte-pages").unwrap(),
            backup_identity: identity,
            source,
            destination: PathBuf::from("restored.sqlite3"),
        })
        .await
        .unwrap();
    assert_eq!(
        observer.record_pages.load(Ordering::Relaxed),
        3,
        "the 700-byte page budget admits one charged record per transaction"
    );
    std::fs::remove_dir_all(root).unwrap();
}

#[tokio::test]
async fn staging_quota_failure_never_publishes_the_destination() {
    let root = temp_root("staging-quota");
    let source = root.join("backup.sqlite3");
    let store = SqliteStore::open(SqliteOptions::new(&source))
        .await
        .unwrap();
    let stream = store
        .create_if_absent(&StreamId::new("quota-stream").unwrap())
        .await
        .unwrap();
    for index in 0..128 {
        store
            .append_atomic(
                &stream,
                event(&format!("quota-event-{index}"), &[index as u8; 1024]),
            )
            .await
            .unwrap();
    }
    store.close().await.unwrap();

    let config = RestoreConfig {
        max_staging_bytes: 128 * 1024,
        page: PageLimits {
            max_records: 16,
            max_bytes: 32 * 1024,
        },
        ..RestoreConfig::default()
    };
    let manager =
        SqliteRestoreManager::new(SqliteRestoreBackend::new(&root).unwrap(), config).unwrap();
    let identity = manager.inspect_backup(source.clone()).await.unwrap();
    let request = RestoreRequest {
        operation_id: RestoreOperationId::new("staging-quota").unwrap(),
        backup_identity: identity,
        source,
        destination: PathBuf::from("restored.sqlite3"),
    };
    let result = manager.restore(request).await;
    assert!(
        matches!(&result, Err(RestoreError::CapacityExceeded)),
        "unexpected staging quota result: {result:?}"
    );
    assert!(!root.join("restored.sqlite3").exists());
    let staging = staging_file(&root);
    let incomplete: i64 = Connection::open(staging)
        .unwrap()
        .query_row(
            "SELECT restore_incomplete FROM event_stream_metadata WHERE singleton=1",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(incomplete, 1);
    std::fs::remove_dir_all(root).unwrap();
}

#[tokio::test]
async fn restore_rejects_a_missing_record_before_the_declared_tail() {
    let (root, source, request, manager) = simple_restore_fixture("missing-tail", None).await;
    let connection = Connection::open(&source).unwrap();
    connection.execute("DELETE FROM event_records", []).unwrap();
    drop(connection);
    let identity = manager.inspect_backup(source.clone()).await.unwrap();
    assert!(matches!(
        manager
            .restore(RestoreRequest {
                backup_identity: identity,
                ..request
            })
            .await,
        Err(RestoreError::CorruptBackup(_))
    ));
    std::fs::remove_dir_all(root).unwrap();
}

#[tokio::test]
async fn restore_rejects_schema_versions_outside_the_domain_range() {
    for (label, version) in [
        ("negative-version", -1_i64),
        ("large-version", 4_294_967_296),
    ] {
        let (root, source, request, manager) = simple_restore_fixture(label, None).await;
        let connection = Connection::open(&source).unwrap();
        connection
            .execute("UPDATE event_records SET schema_version=?1", [version])
            .unwrap();
        drop(connection);
        let identity = manager.inspect_backup(source.clone()).await.unwrap();
        assert!(matches!(
            manager
                .restore(RestoreRequest {
                    backup_identity: identity,
                    ..request
                })
                .await,
            Err(RestoreError::CorruptBackup(_))
        ));
        assert!(!root.join("restored.sqlite3").exists());
        std::fs::remove_dir_all(root).unwrap();
    }
}

#[tokio::test]
async fn logical_restore_rewrites_full_identity_union_and_survives_restart() {
    let root = temp_root("roundtrip");
    let source = root.join("backup.sqlite3");
    let store = SqliteStore::open(SqliteOptions::new(&source))
        .await
        .unwrap();

    let retired_a = store
        .create_if_absent(&StreamId::new("retired-name").unwrap())
        .await
        .unwrap();
    store
        .append_atomic(&retired_a, event("retired-record", b"cleaned"))
        .await
        .unwrap();
    let reset_request = LifecycleRequest {
        operation_id: LifecycleOperationId::new("source-reset").unwrap(),
        expected: retired_a.clone(),
        action: LifecycleAction::Reset,
    };
    let retired_b = store
        .change_lifecycle(reset_request)
        .await
        .unwrap()
        .replacement
        .unwrap();
    store
        .change_lifecycle(LifecycleRequest {
            operation_id: LifecycleOperationId::new("source-delete").unwrap(),
            expected: retired_b.clone(),
            action: LifecycleAction::Delete,
        })
        .await
        .unwrap();
    loop {
        let progress = store
            .cleanup_retired(CleanupLimits {
                max_records: 8,
                max_bytes: 2 * 1024 * 1024,
            })
            .await
            .unwrap();
        if !progress.remaining {
            break;
        }
    }

    let live = store
        .create_if_absent(&StreamId::new("live-name").unwrap())
        .await
        .unwrap();
    store
        .append_atomic(&live, event("live-1", b"exact restored bytes"))
        .await
        .unwrap();
    store.close().await.unwrap();

    let config = RestoreConfig {
        max_source_bytes: 32 * 1024 * 1024,
        max_staging_bytes: 32 * 1024 * 1024,
        page: PageLimits {
            max_records: 2,
            max_bytes: 2 * 1024 * 1024,
        },
        ..RestoreConfig::default()
    };
    let manager =
        SqliteRestoreManager::new(SqliteRestoreBackend::new(&root).unwrap(), config).unwrap();
    let identity = manager.inspect_backup(source.clone()).await.unwrap();
    let request = RestoreRequest {
        operation_id: RestoreOperationId::new("restore-roundtrip").unwrap(),
        backup_identity: identity,
        source: source.clone(),
        destination: PathBuf::from("restored.sqlite3"),
    };
    let receipt = manager.restore(request.clone()).await.unwrap();
    assert_eq!(receipt.mapping_count, 3);
    assert_eq!(manager.restore(request.clone()).await.unwrap(), receipt);

    let first_page = manager
        .read_mapping(
            receipt.clone(),
            None,
            PageLimits {
                max_records: 2,
                max_bytes: 4096,
            },
        )
        .await
        .unwrap();
    assert_eq!(first_page.entries.len(), 2);
    assert!(!first_page.complete);
    let second_page = manager
        .read_mapping(
            receipt.clone(),
            first_page.next_after.clone(),
            PageLimits {
                max_records: 2,
                max_bytes: 4096,
            },
        )
        .await
        .unwrap();
    assert_eq!(second_page.entries.len(), 1);
    assert!(second_page.complete);
    let mappings: Vec<_> = first_page
        .entries
        .into_iter()
        .chain(second_page.entries)
        .collect();
    let mapped_live = mappings
        .iter()
        .find(|entry| entry.old == live)
        .unwrap()
        .new
        .clone();
    let mapped_a = mappings
        .iter()
        .find(|entry| entry.old == retired_a)
        .unwrap()
        .new
        .clone();
    let mapped_b = mappings
        .iter()
        .find(|entry| entry.old == retired_b)
        .unwrap()
        .new
        .clone();
    assert_ne!(mapped_live.incarnation, live.incarnation);

    let restored = SqliteStore::open(SqliteOptions::new(&receipt.destination))
        .await
        .unwrap();
    assert!(matches!(
        restored.bounds(&live).await,
        Err(Error::StaleIncarnation { .. })
    ));
    let page = restored
        .read_range(
            &mapped_live,
            0,
            1,
            PageLimits {
                max_records: 1,
                max_bytes: 4096,
            },
        )
        .await
        .unwrap();
    assert_eq!(
        page.records[0].event.payload.as_bytes(),
        b"exact restored bytes"
    );
    assert!(matches!(
        restored
            .create_if_absent(&StreamId::new("retired-name").unwrap())
            .await,
        Err(Error::StreamUnavailable { last }) if *last == mapped_b
    ));
    let restored_reset = restored
        .change_lifecycle(LifecycleRequest {
            operation_id: LifecycleOperationId::new("source-reset").unwrap(),
            expected: mapped_a,
            action: LifecycleAction::Reset,
        })
        .await
        .unwrap();
    assert_eq!(restored_reset.replacement, Some(mapped_b));
    restored.close().await.unwrap();

    let conflicting = RestoreRequest {
        operation_id: RestoreOperationId::new("other-operation").unwrap(),
        ..request
    };
    assert!(matches!(
        manager.restore(conflicting).await,
        Err(RestoreError::DestinationExists(_))
    ));
    std::fs::remove_dir_all(root).unwrap();
}

#[tokio::test]
async fn completed_unpublished_restore_retries_without_source() {
    let (root, source, request, manager) = simple_restore_fixture(
        "completed-unpublished",
        Some(SqliteRestoreFailureInjection::AfterStagingCompletion),
    )
    .await;

    assert!(matches!(
        manager.restore(request.clone()).await,
        Err(RestoreError::StorageFailure(_))
    ));
    let staging = staging_file(&root);
    assert!(SqliteStore::open(SqliteOptions::new(&staging))
        .await
        .is_err());
    let relocated = root.join("relocated-backup.sqlite3");
    std::fs::rename(&source, &relocated).unwrap();

    let mut relocated_request = request.clone();
    relocated_request.source = relocated;
    let receipt = manager.restore(relocated_request).await.unwrap();
    assert_eq!(receipt.destination, root.join("restored.sqlite3"));
    assert!(!staging.exists());
    assert_eq!(manager.restore(request).await.unwrap(), receipt);
    let restored = SqliteStore::open(SqliteOptions::new(&receipt.destination))
        .await
        .unwrap();
    restored.close().await.unwrap();
    std::fs::remove_dir_all(root).unwrap();
}

#[tokio::test]
async fn corrupt_completed_mapping_count_is_rejected_before_publication() {
    let (root, _source, request, manager) = simple_restore_fixture(
        "completed-count-corrupt",
        Some(SqliteRestoreFailureInjection::AfterStagingCompletion),
    )
    .await;
    assert!(manager.restore(request.clone()).await.is_err());
    let staging = staging_file(&root);
    let connection = Connection::open(&staging).unwrap();
    connection
        .execute("UPDATE restore_metadata SET mapping_count=0", [])
        .unwrap();
    drop(connection);

    assert!(matches!(
        manager.restore(request.clone()).await,
        Err(RestoreError::CorruptBackup(_))
    ));
    assert!(!root.join("restored.sqlite3").exists());
    let state: i64 = Connection::open(&staging)
        .unwrap()
        .query_row(
            "SELECT restore_incomplete FROM event_stream_metadata WHERE singleton=1",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(state, 2);
    manager.cleanup_staging(request).await.unwrap();
    assert!(!root.join("restored.sqlite3").exists());
    assert!(!staging.exists());
    assert!(std::fs::read_dir(&root).unwrap().all(|entry| {
        !entry
            .unwrap()
            .file_name()
            .to_string_lossy()
            .ends_with(".owner")
    }));
    std::fs::remove_dir_all(root).unwrap();
}

#[tokio::test]
async fn linked_restore_retries_each_publication_boundary_without_source() {
    for (label, failure) in [
        (
            "linked-state-two",
            SqliteRestoreFailureInjection::AfterDestinationLink,
        ),
        (
            "linked-state-zero",
            SqliteRestoreFailureInjection::AfterPublishedMarker,
        ),
    ] {
        let (root, source, request, manager) = simple_restore_fixture(label, Some(failure)).await;
        assert!(matches!(
            manager.restore(request.clone()).await,
            Err(RestoreError::PublicationUnknown { .. })
        ));
        let destination = root.join("restored.sqlite3");
        let staging = staging_file(&root);
        assert!(destination.exists());
        assert!(SqliteStore::open(SqliteOptions::new(&destination))
            .await
            .is_err());
        if failure == SqliteRestoreFailureInjection::AfterDestinationLink {
            let unpublished = RestoreReceipt {
                operation_id: request.operation_id.clone(),
                backup_identity: request.backup_identity,
                destination: destination.clone(),
                mapping_count: 1,
            };
            assert!(manager
                .read_mapping(
                    unpublished,
                    None,
                    PageLimits {
                        max_records: 1,
                        max_bytes: 4096,
                    },
                )
                .await
                .is_err());
            let state: i64 = Connection::open(&destination)
                .unwrap()
                .query_row(
                    "SELECT restore_incomplete FROM event_stream_metadata WHERE singleton=1",
                    [],
                    |row| row.get(0),
                )
                .unwrap();
            assert_eq!(state, 2, "mapping reads must not recover publication");
        }
        std::fs::remove_file(source).unwrap();

        if failure == SqliteRestoreFailureInjection::AfterPublishedMarker {
            std::fs::remove_file(&staging).unwrap();
            assert!(owner_directory(&root).exists());
        }

        let receipt = manager.restore(request.clone()).await.unwrap();
        assert_eq!(receipt.destination, destination);
        assert!(!staging.exists());
        assert!(std::fs::read_dir(&root).unwrap().all(|entry| {
            !entry
                .unwrap()
                .file_name()
                .to_string_lossy()
                .ends_with(".owner")
        }));
        assert_eq!(manager.restore(request).await.unwrap(), receipt);
        let restored = SqliteStore::open(SqliteOptions::new(&receipt.destination))
            .await
            .unwrap();
        restored.close().await.unwrap();
        std::fs::remove_dir_all(root).unwrap();
    }
}

#[tokio::test]
async fn cleanup_removes_owned_completed_staging_without_source() {
    let (root, source, request, manager) = simple_restore_fixture(
        "cleanup-completed",
        Some(SqliteRestoreFailureInjection::AfterStagingCompletion),
    )
    .await;
    assert!(manager.restore(request.clone()).await.is_err());
    let staging = staging_file(&root);
    std::fs::remove_file(source).unwrap();

    manager.cleanup_staging(request.clone()).await.unwrap();
    assert!(!staging.exists());
    assert!(manager.cleanup_staging(request).await.is_ok());
    std::fs::remove_dir_all(root).unwrap();
}

#[cfg(feature = "test-support")]
#[tokio::test]
async fn cleanup_reports_exact_unlink_and_directory_sync_failures() {
    let (root, _source, request, creator) = simple_restore_fixture(
        "cleanup-faults-unlink",
        Some(SqliteRestoreFailureInjection::AfterStagingCompletion),
    )
    .await;
    assert!(creator.restore(request.clone()).await.is_err());
    let staging = staging_file(&root);
    let unlink = SqliteRestoreManager::new(
        SqliteRestoreBackend::new(&root)
            .unwrap()
            .with_failure_injection(SqliteRestoreFailureInjection::DuringCleanupUnlink),
        RestoreConfig::default(),
    )
    .unwrap();
    assert!(matches!(
        unlink.cleanup_staging(request.clone()).await,
        Err(RestoreError::StagingCleanupFailed { paths }) if paths == vec![staging.clone()]
    ));
    assert!(staging.exists());
    SqliteRestoreManager::new(
        SqliteRestoreBackend::new(&root).unwrap(),
        RestoreConfig::default(),
    )
    .unwrap()
    .cleanup_staging(request)
    .await
    .unwrap();
    std::fs::remove_dir_all(root).unwrap();

    let (root, _source, request, creator) = simple_restore_fixture(
        "cleanup-faults-sync",
        Some(SqliteRestoreFailureInjection::AfterStagingCompletion),
    )
    .await;
    assert!(creator.restore(request.clone()).await.is_err());
    let sync = SqliteRestoreManager::new(
        SqliteRestoreBackend::new(&root)
            .unwrap()
            .with_failure_injection(SqliteRestoreFailureInjection::DuringCleanupDirectorySync),
        RestoreConfig::default(),
    )
    .unwrap();
    assert!(matches!(
        sync.cleanup_staging(request.clone()).await,
        Err(RestoreError::StagingCleanupFailed { paths }) if paths == vec![root.clone()]
    ));
    assert!(sync.cleanup_staging(request).await.is_ok());
    std::fs::remove_dir_all(root).unwrap();
}

#[cfg(feature = "test-support")]
#[tokio::test]
async fn operation_reservation_rejects_conflicts_and_preserves_unknown_entries() {
    let (root, _source, request, manager) = simple_restore_fixture(
        "reservation-conflict",
        Some(SqliteRestoreFailureInjection::AfterOwnerReservation),
    )
    .await;
    assert!(manager.restore(request.clone()).await.is_err());
    let owner = owner_directory(&root);
    let unexpected = owner.join("unexpected");
    std::fs::write(&unexpected, b"external").unwrap();
    assert!(matches!(
        manager.restore(request.clone()).await,
        Err(RestoreError::RequestConflict { .. })
    ));
    assert_eq!(std::fs::read(&unexpected).unwrap(), b"external");
    std::fs::remove_file(unexpected).unwrap();

    let second = root.join("different.sqlite3");
    let store = SqliteStore::open(SqliteOptions::new(&second))
        .await
        .unwrap();
    let key = store
        .create_if_absent(&StreamId::new("different").unwrap())
        .await
        .unwrap();
    store
        .append_atomic(&key, event("different", b"different bytes"))
        .await
        .unwrap();
    store.close().await.unwrap();
    let second_identity = manager.inspect_backup(second.clone()).await.unwrap();
    let conflict = RestoreRequest {
        backup_identity: second_identity,
        source: second,
        ..request.clone()
    };
    assert!(matches!(
        manager.restore(conflict).await,
        Err(RestoreError::RequestConflict { .. })
    ));
    manager.cleanup_staging(request).await.unwrap();
    std::fs::remove_dir_all(root).unwrap();
}

#[cfg(feature = "test-support")]
#[tokio::test]
async fn unbound_empty_staging_is_preserved_and_not_adopted() {
    let (root, _source, request, manager) = simple_restore_fixture(
        "unbound-empty-staging",
        Some(SqliteRestoreFailureInjection::AfterStagingFileReservation),
    )
    .await;
    assert!(manager.restore(request.clone()).await.is_err());
    let staging = staging_file(&root);
    let owner = owner_directory(&root);
    let binding = std::fs::read_dir(&owner)
        .unwrap()
        .next()
        .unwrap()
        .unwrap()
        .path();
    std::fs::remove_dir(binding).unwrap();
    assert_eq!(std::fs::metadata(&staging).unwrap().len(), 0);

    assert!(matches!(
        manager.restore(request.clone()).await,
        Err(RestoreError::RequestConflict { .. })
    ));
    assert_eq!(std::fs::metadata(&staging).unwrap().len(), 0);
    assert!(std::fs::read_dir(&owner).unwrap().next().is_none());

    std::fs::remove_file(staging).unwrap();
    std::fs::remove_dir(owner).unwrap();
    std::fs::remove_dir_all(root).unwrap();
}

#[cfg(all(feature = "test-support", unix))]
#[tokio::test]
async fn symlinked_operation_owner_is_rejected_and_preserved() {
    use std::os::unix::fs::symlink;

    let (root, _source, request, manager) = simple_restore_fixture(
        "reservation-symlink",
        Some(SqliteRestoreFailureInjection::AfterOwnerReservation),
    )
    .await;
    assert!(manager.restore(request.clone()).await.is_err());
    let owner = owner_directory(&root);
    std::fs::remove_dir_all(&owner).unwrap();
    let outside = root.join("outside-owner");
    std::fs::create_dir(&outside).unwrap();
    symlink(&outside, &owner).unwrap();
    std::fs::remove_dir(&outside).unwrap();
    assert!(matches!(
        manager.restore(request).await,
        Err(RestoreError::StagingCleanupFailed { .. })
    ));
    assert!(std::fs::symlink_metadata(owner)
        .unwrap()
        .file_type()
        .is_symlink());
    std::fs::remove_dir_all(root).unwrap();
}

#[cfg(all(feature = "test-support", unix))]
#[tokio::test]
async fn dangling_symlinked_request_binding_is_rejected_and_preserved() {
    use std::os::unix::fs::symlink;

    let (root, _source, request, manager) = simple_restore_fixture(
        "reservation-binding-symlink",
        Some(SqliteRestoreFailureInjection::AfterOwnerReservation),
    )
    .await;
    assert!(manager.restore(request.clone()).await.is_err());
    let owner = owner_directory(&root);
    let binding = std::fs::read_dir(&owner)
        .unwrap()
        .next()
        .unwrap()
        .unwrap()
        .path();
    std::fs::remove_dir(&binding).unwrap();
    symlink(root.join("missing-request-binding"), &binding).unwrap();

    assert!(matches!(
        manager.restore(request.clone()).await,
        Err(RestoreError::StagingCleanupFailed { .. })
    ));
    assert!(std::fs::symlink_metadata(&binding)
        .unwrap()
        .file_type()
        .is_symlink());
    assert!(matches!(
        manager.cleanup_staging(request).await,
        Err(RestoreError::StagingCleanupFailed { .. })
    ));
    assert!(std::fs::symlink_metadata(binding)
        .unwrap()
        .file_type()
        .is_symlink());
    std::fs::remove_dir_all(root).unwrap();
}

#[cfg(all(feature = "test-support", unix))]
#[tokio::test]
async fn staging_symlink_and_hardlink_are_rejected_before_sqlite_open() {
    use std::os::unix::fs::{symlink, MetadataExt};

    for hard_link in [false, true] {
        let label = if hard_link {
            "staging-hardlink"
        } else {
            "staging-symlink"
        };
        let (root, source, request, manager) = simple_restore_fixture(
            label,
            Some(SqliteRestoreFailureInjection::AfterStagingFileReservation),
        )
        .await;
        assert!(manager.restore(request.clone()).await.is_err());
        let staging = staging_file(&root);
        std::fs::remove_file(&staging).unwrap();
        let external = root.join(format!("external-empty-{label}"));
        std::fs::File::create(&external).unwrap();
        if hard_link {
            std::fs::hard_link(&external, &staging).unwrap();
        } else {
            symlink(&external, &staging).unwrap();
        }

        assert!(matches!(
            manager.restore(request.clone()).await,
            Err(RestoreError::StagingCleanupFailed { paths }) if paths == vec![staging.clone()]
        ));
        let metadata = std::fs::symlink_metadata(&staging).unwrap();
        if hard_link {
            assert_eq!(metadata.nlink(), 2);
        } else {
            assert!(metadata.file_type().is_symlink());
        }
        assert_eq!(std::fs::metadata(&external).unwrap().len(), 0);
        let count: i64 =
            Connection::open_with_flags(&source, rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY)
                .unwrap()
                .query_row("SELECT COUNT(*) FROM event_records", [], |row| row.get(0))
                .unwrap();
        assert_eq!(count, 1);
        assert!(matches!(
            manager.cleanup_staging(request).await,
            Err(RestoreError::StagingCleanupFailed { paths }) if paths == vec![staging.clone()]
        ));
        assert!(std::fs::symlink_metadata(&staging).is_ok());
        std::fs::remove_dir_all(root).unwrap();
    }
}

#[tokio::test]
async fn operation_id_cannot_escape_or_lengthen_staging_name() {
    let (root, _source, mut request, manager) = simple_restore_fixture("hashed-name", None).await;
    request.operation_id =
        RestoreOperationId::new("../nested/../../operation-with-path-separators").unwrap();
    let receipt = manager.restore(request).await.unwrap();
    assert_eq!(receipt.destination, root.join("restored.sqlite3"));
    assert!(!root.join("nested").exists());
    std::fs::remove_dir_all(root).unwrap();
}

#[tokio::test]
async fn legacy_format_two_is_inspected_read_only_restored_and_extended_on_normal_open() {
    let root = temp_root("legacy-v2");
    let source = root.join("legacy.sqlite3");
    let store = SqliteStore::open(SqliteOptions::new(&source))
        .await
        .unwrap();
    let live = store
        .create_if_absent(&StreamId::new("legacy-live").unwrap())
        .await
        .unwrap();
    store
        .append_atomic(&live, event("legacy-event", b"legacy payload"))
        .await
        .unwrap();
    let deleted = store
        .create_if_absent(&StreamId::new("legacy-deleted").unwrap())
        .await
        .unwrap();
    let lifecycle = LifecycleRequest {
        operation_id: LifecycleOperationId::new("legacy-delete").unwrap(),
        expected: deleted,
        action: LifecycleAction::Delete,
    };
    let lifecycle_receipt = store.change_lifecycle(lifecycle.clone()).await.unwrap();
    store.close().await.unwrap();
    downgrade_to_legacy_format_two(&source);

    let manager = SqliteRestoreManager::new(
        SqliteRestoreBackend::new(&root).unwrap(),
        RestoreConfig::default(),
    )
    .unwrap();
    let identity = manager.inspect_backup(source.clone()).await.unwrap();
    let connection = Connection::open(&source).unwrap();
    let restore_tables: u32 = connection
        .query_row(
            "SELECT count(*) FROM sqlite_schema WHERE type='table' AND name LIKE 'restore_%'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(
        restore_tables, 0,
        "read-only inspection must not extend schema"
    );
    drop(connection);

    let restored = manager
        .restore(RestoreRequest {
            operation_id: RestoreOperationId::new("restore-legacy-v2").unwrap(),
            backup_identity: identity,
            source: source.clone(),
            destination: PathBuf::from("restored-legacy.sqlite3"),
        })
        .await
        .unwrap();
    assert_eq!(restored.mapping_count, 2);

    let mut failed_extension = SqliteOptions::new(&source);
    failed_extension.failure_injection = Some(SqliteFailureInjection::BeforeMigrationCommit);
    assert!(matches!(
        SqliteStore::open(failed_extension).await,
        Err(Error::StoreWriteFailed(_))
    ));
    let connection = Connection::open(&source).unwrap();
    let partial_extension: (u32, u32) = connection
        .query_row(
            "SELECT
               (SELECT count(*) FROM sqlite_schema WHERE type='table' AND name LIKE 'restore_%'),
               (SELECT count(*) FROM pragma_table_info('event_stream_metadata') WHERE name='restore_incomplete')",
            [],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap();
    assert_eq!(partial_extension, (0, 0));
    drop(connection);

    let source_store = SqliteStore::open(SqliteOptions::new(&source))
        .await
        .unwrap();
    assert_eq!(source_store.bounds(&live).await.unwrap().tail.offset, 1);
    assert_eq!(
        source_store.change_lifecycle(lifecycle).await.unwrap(),
        lifecycle_receipt
    );
    source_store.close().await.unwrap();
    let connection = Connection::open(&source).unwrap();
    let extended: (u32, u32) = connection
        .query_row(
            "SELECT
               (SELECT count(*) FROM sqlite_schema WHERE type='table' AND name LIKE 'restore_%'),
               (SELECT count(*) FROM pragma_table_info('event_stream_metadata') WHERE name='restore_incomplete')",
            [],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap();
    assert_eq!(extended, (2, 1));
    drop(connection);
    std::fs::remove_dir_all(root).unwrap();
}

#[tokio::test]
async fn restore_rejects_records_outside_declared_lifetime_extent() {
    let (root, source, request, manager) = simple_restore_fixture("record-extent", None).await;
    let connection = Connection::open(&source).unwrap();
    let stream_key: i64 = connection
        .query_row(
            "SELECT stream_key FROM event_streams WHERE public_id='live'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    connection
        .execute(
            "INSERT INTO event_records(stream_key,offset,event_id,schema_id,schema_version,payload)
             VALUES(?1,?2,'outside','restore.test',7,X'01')",
            rusqlite::params![stream_key, 0_u64.to_be_bytes().as_slice()],
        )
        .unwrap();
    drop(connection);
    let identity = manager.inspect_backup(source.clone()).await.unwrap();
    let corrupt_request = RestoreRequest {
        backup_identity: identity,
        ..request
    };
    assert!(matches!(
        manager.restore(corrupt_request).await,
        Err(RestoreError::CorruptBackup(_))
    ));
    std::fs::remove_dir_all(root).unwrap();
}

#[tokio::test]
async fn mapping_reader_guards_malformed_large_columns_before_materialization() {
    let (root, _source, request, manager) = simple_restore_fixture("mapping-guard", None).await;
    let receipt = manager.restore(request).await.unwrap();
    let connection = Connection::open(&receipt.destination).unwrap();
    connection
        .execute_batch("PRAGMA ignore_check_constraints=ON;")
        .unwrap();
    connection
        .execute(
            "UPDATE restore_mappings SET new_incarnation=zeroblob(2097152)",
            [],
        )
        .unwrap();
    drop(connection);
    assert!(matches!(
        manager
            .read_mapping(
                receipt,
                None,
                PageLimits {
                    max_records: 1,
                    max_bytes: 4096,
                },
            )
            .await,
        Err(RestoreError::CorruptBackup(_))
    ));
    std::fs::remove_dir_all(root).unwrap();
}

#[tokio::test]
async fn published_receipt_rejects_malformed_types_counts_and_accounting() {
    for (label, mutation) in [
        (
            "receipt-count-type",
            "UPDATE restore_metadata SET mapping_count='not-an-integer'",
        ),
        (
            "receipt-count-mismatch",
            "UPDATE restore_metadata SET mapping_count=0",
        ),
        (
            "receipt-state-type",
            "UPDATE event_stream_metadata SET restore_incomplete='not-an-integer'",
        ),
        (
            "receipt-counter-mismatch",
            "UPDATE event_stream_metadata SET lifecycle_receipt_count=1",
        ),
    ] {
        let (root, _source, request, manager) = simple_restore_fixture(label, None).await;
        let receipt = manager.restore(request.clone()).await.unwrap();
        let connection = Connection::open(&receipt.destination).unwrap();
        connection
            .execute_batch("PRAGMA ignore_check_constraints=ON;")
            .unwrap();
        connection.execute(mutation, []).unwrap();
        drop(connection);

        assert!(matches!(
            manager.restore(request).await,
            Err(RestoreError::CorruptBackup(_))
        ));
        std::fs::remove_dir_all(root).unwrap();
    }
}

#[tokio::test]
async fn backup_inspection_rejects_oversized_replacement_before_import() {
    let (root, source, _request, manager) =
        simple_restore_fixture("receipt-replacement", None).await;
    let connection = Connection::open(&source).unwrap();
    connection
        .execute_batch("PRAGMA ignore_check_constraints=ON;")
        .unwrap();
    let incarnation: Vec<u8> = connection
        .query_row(
            "SELECT incarnation FROM event_streams WHERE public_id='live'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    connection
        .execute(
            "INSERT INTO lifecycle_receipts(
               operation_id,action,expected_public_id,expected_incarnation,
               replacement_incarnation,charge
             ) VALUES('oversized-replacement',1,'live',?1,zeroblob(2097152),281)",
            rusqlite::params![incarnation],
        )
        .unwrap();
    connection
        .execute(
            "UPDATE event_stream_metadata
             SET lifecycle_receipt_count=1,lifecycle_receipt_bytes=281",
            [],
        )
        .unwrap();
    drop(connection);

    assert!(matches!(
        manager.inspect_backup(source).await,
        Err(RestoreError::CorruptBackup(_))
    ));
    std::fs::remove_dir_all(root).unwrap();
}

#[tokio::test]
async fn mapping_reader_rejects_an_incarnation_that_was_not_refreshed() {
    let (root, _source, request, manager) = simple_restore_fixture("mapping-freshness", None).await;
    let receipt = manager.restore(request).await.unwrap();
    let connection = Connection::open(&receipt.destination).unwrap();
    connection
        .execute(
            "UPDATE restore_mappings SET new_incarnation=old_incarnation",
            [],
        )
        .unwrap();
    drop(connection);

    assert!(matches!(
        manager
            .read_mapping(
                receipt,
                None,
                PageLimits {
                    max_records: 1,
                    max_bytes: 4096,
                },
            )
            .await,
        Err(RestoreError::CorruptBackup(_))
    ));
    std::fs::remove_dir_all(root).unwrap();
}

#[cfg(feature = "test-support")]
#[tokio::test]
async fn restore_process_kill_child() {
    let Ok(root) = std::env::var("EVENT_STREAM_RESTORE_KILL_ROOT") else {
        return;
    };
    let failure = match std::env::var("EVENT_STREAM_RESTORE_KILL_PHASE")
        .unwrap()
        .as_str()
    {
        "owner" => SqliteRestoreFailureInjection::AfterOwnerReservation,
        "reserved" => SqliteRestoreFailureInjection::AfterStagingFileReservation,
        "completed" => SqliteRestoreFailureInjection::AfterStagingCompletion,
        "linked" => SqliteRestoreFailureInjection::AfterDestinationLink,
        "published" => SqliteRestoreFailureInjection::AfterPublishedMarker,
        other => panic!("unknown restore kill phase {other}"),
    };
    let root = PathBuf::from(root);
    let pause_marker = PathBuf::from(std::env::var("EVENT_STREAM_RESTORE_PAUSE_MARKER").unwrap());
    let source = root.join("backup.sqlite3");
    let backend = SqliteRestoreBackend::new(&root)
        .unwrap()
        .with_pause_injection(failure, pause_marker);
    let manager = SqliteRestoreManager::new(backend, RestoreConfig::default()).unwrap();
    let identity = manager.inspect_backup(source.clone()).await.unwrap();
    let request = RestoreRequest {
        operation_id: RestoreOperationId::new("restore-process-kill").unwrap(),
        backup_identity: identity,
        source,
        destination: PathBuf::from("restored.sqlite3"),
    };
    let _ = manager.restore(request).await;
    panic!("pause injection unexpectedly returned");
}

#[cfg(feature = "test-support")]
#[tokio::test]
async fn abrupt_process_kill_recovers_bootstrap_and_publication_boundaries() {
    for phase in ["owner", "reserved", "completed", "linked", "published"] {
        let (root, source, request, manager) = simple_restore_fixture("process-kill", None).await;
        drop(manager);
        let pause_marker = root.join(format!("pause-{phase}"));
        let child = Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "restore_process_kill_child",
                "--nocapture",
                "--test-threads=1",
            ])
            .env("EVENT_STREAM_RESTORE_KILL_ROOT", &root)
            .env("EVENT_STREAM_RESTORE_KILL_PHASE", phase)
            .env("EVENT_STREAM_RESTORE_PAUSE_MARKER", &pause_marker)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::inherit())
            .spawn()
            .unwrap();
        let child = ChildGuard(Some(child));
        let deadline = Instant::now() + Duration::from_secs(10);
        while !pause_marker.exists() && Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(5));
        }
        assert!(pause_marker.exists(), "child reached {phase} boundary");
        assert!(!child.kill_and_wait().success());
        std::fs::remove_file(pause_marker).unwrap();
        if phase != "owner" && phase != "reserved" {
            std::fs::remove_file(&source).unwrap();
        } else if phase == "reserved" {
            let staging = staging_file(&root);
            assert!(SqliteStore::open(SqliteOptions::new(staging))
                .await
                .is_err());
        }

        let manager = SqliteRestoreManager::new(
            SqliteRestoreBackend::new(&root).unwrap(),
            RestoreConfig::default(),
        )
        .unwrap();
        let receipt = manager.restore(request.clone()).await.unwrap();
        assert_eq!(manager.restore(request).await.unwrap(), receipt);
        let mapping = manager
            .read_mapping(
                receipt.clone(),
                None,
                PageLimits {
                    max_records: 1,
                    max_bytes: 4096,
                },
            )
            .await
            .unwrap()
            .entries
            .pop()
            .unwrap();
        assert_ne!(mapping.old.incarnation, mapping.new.incarnation);
        for _ in 0..2 {
            let restored = SqliteStore::open(SqliteOptions::new(&receipt.destination))
                .await
                .unwrap();
            let page = restored
                .read_range(
                    &mapping.new,
                    0,
                    1,
                    PageLimits {
                        max_records: 1,
                        max_bytes: 4096,
                    },
                )
                .await
                .unwrap();
            assert_eq!(page.records.len(), 1);
            assert_eq!(page.records[0].cursor.offset, 1);
            assert_eq!(page.records[0].event.id.as_str(), "one");
            assert_eq!(page.records[0].event.schema.id.as_str(), "restore.test");
            assert_eq!(page.records[0].event.schema.version, 7);
            assert_eq!(
                page.records[0].event.payload.as_bytes(),
                b"restore publication state"
            );
            restored.close().await.unwrap();
        }
        std::fs::remove_dir_all(root).unwrap();
    }
}
