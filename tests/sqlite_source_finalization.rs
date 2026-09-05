#![cfg(all(feature = "sqlite", feature = "source-journal"))]

use event_stream::infrastructure::{
    SqliteFailureInjection, SqliteOptions, SqliteRestoreBackend, SqliteRestoreManager, SqliteStore,
};
use event_stream::*;
use std::path::{Path, PathBuf};

fn binding(stream: StreamKey) -> SourceBinding {
    SourceBinding {
        source: SourceKey {
            id: SourceId::new("finite-input").unwrap(),
            incarnation: SourceIncarnation([17; 16]),
        },
        parser: ParserRef {
            id: ParserId::new("fixture-parser").unwrap(),
            version: 1,
        },
        output_stream: stream,
    }
}

async fn open_source(
    path: &Path,
    injection: Option<SqliteFailureInjection>,
) -> (SqliteStore, SourceBinding) {
    let mut options = SqliteOptions::new(path);
    options.failure_injection = injection;
    let store = SqliteStore::open(options).await.unwrap();
    let stream = store
        .create_if_absent(&StreamId::new("final-output").unwrap())
        .await
        .unwrap();
    store
        .enable_retry_policy(EnableRetryPolicy {
            operation_id: RetentionOperationId::new("enable-final-output").unwrap(),
            stream: stream.clone(),
        })
        .await
        .unwrap();
    let binding = binding(stream);
    store
        .begin_source(BeginSource {
            operation_id: JournalOperationId::new("begin-finite-input").unwrap(),
            binding: binding.clone(),
        })
        .await
        .unwrap();
    (store, binding)
}

fn segment(binding: &SourceBinding) -> RawSegment {
    RawSegment {
        start: SourcePosition {
            source: binding.source.clone(),
            offset: 0,
        },
        bytes: Payload::copy_from_slice(b"abc"),
    }
}

fn output(binding: &SourceBinding) -> JournaledOutput {
    JournaledOutput {
        source: binding.source.clone(),
        position: DecodedPosition {
            source_byte: 0,
            item_index: 0,
        },
        event: NewEvent {
            id: EventId::new("final-item").unwrap(),
            schema: SchemaRef {
                id: SchemaId::new("bytes").unwrap(),
                version: 1,
            },
            payload: Payload::copy_from_slice(b"item"),
        },
    }
}

#[tokio::test]
async fn sqlite_seal_finish_reopen_and_restore_preserve_exact_state() {
    let root = std::env::temp_dir().join(format!("sqlite-source-eof-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir(&root).unwrap();
    let path = root.join("source.sqlite3");
    let (store, binding) = open_source(&path, None).await;
    assert_eq!(
        store.source_finalization(&binding.source).await.unwrap(),
        SourceFinalizationStatus {
            sealed_end: None,
            parser_finished: false,
        }
    );
    let unsealed_checkpoint = ParserCheckpoint {
        source: SourcePosition {
            source: binding.source.clone(),
            offset: 0,
        },
        parser: binding.parser.clone(),
        state: Payload::copy_from_slice(b"unsealed"),
        next_item_index: 0,
        output_stream: binding.output_stream.clone(),
        committed_output: None,
    };
    assert!(matches!(
        store
            .finish_source(FinishSource {
                checkpoint: unsealed_checkpoint,
            })
            .await,
        Err(JournalError::CheckpointConflict { .. })
    ));
    let captured = store.capture_segment(segment(&binding)).await.unwrap();
    let seal = SealSource {
        end: SourcePosition {
            source: binding.source.clone(),
            offset: 3,
        },
    };
    assert!(matches!(
        store
            .seal_source(SealSource {
                end: SourcePosition {
                    source: binding.source.clone(),
                    offset: 2,
                },
            })
            .await,
        Err(JournalError::InvalidInput(_))
    ));
    let sealed = store.seal_source(seal.clone()).await.unwrap();
    assert_eq!(store.seal_source(seal.clone()).await.unwrap(), sealed);
    assert_eq!(
        store.capture_segment(segment(&binding)).await.unwrap(),
        captured
    );
    assert!(matches!(
        store
            .capture_segment(RawSegment {
                start: seal.end.clone(),
                bytes: Payload::copy_from_slice(b"late"),
            })
            .await,
        Err(JournalError::SourceSealed { end: 3 })
    ));

    let appended = store
        .append_captured(&binding.output_stream, output(&binding))
        .await
        .unwrap();
    let checkpoint = ParserCheckpoint {
        source: seal.end.clone(),
        parser: binding.parser.clone(),
        state: Payload::copy_from_slice(b"finished-state"),
        next_item_index: 1,
        output_stream: binding.output_stream.clone(),
        committed_output: Some(appended.record.cursor.clone()),
    };
    store
        .publish_parser_checkpoint(checkpoint.clone())
        .await
        .unwrap();
    let finish = FinishSource {
        checkpoint: checkpoint.clone(),
    };
    let finished = store.finish_source(finish.clone()).await.unwrap();
    assert_eq!(store.finish_source(finish).await.unwrap(), finished);
    assert_eq!(
        store
            .append_captured(&binding.output_stream, output(&binding))
            .await
            .unwrap()
            .record,
        appended.record
    );
    let mut changed = checkpoint.clone();
    changed.state = Payload::copy_from_slice(b"changed");
    assert!(matches!(
        store.publish_parser_checkpoint(changed).await,
        Err(JournalError::CheckpointConflict { .. })
    ));
    let mut late = output(&binding);
    late.position.item_index = 1;
    late.event.id = EventId::new("late-item").unwrap();
    assert!(matches!(
        store.append_captured(&binding.output_stream, late).await,
        Err(JournalError::SourceSealed { end: 3 })
    ));
    EventStore::close(&store).await.unwrap();

    let reopened = SqliteStore::open(SqliteOptions::new(&path)).await.unwrap();
    assert_eq!(
        reopened.source_finalization(&binding.source).await.unwrap(),
        SourceFinalizationStatus {
            sealed_end: Some(3),
            parser_finished: true,
        }
    );
    assert_eq!(
        reopened.latest_checkpoint(&binding.source).await.unwrap(),
        Some(checkpoint.clone())
    );
    EventStore::close(&reopened).await.unwrap();

    let manager = SqliteRestoreManager::new(
        SqliteRestoreBackend::new(&root).unwrap(),
        RestoreConfig::default(),
    )
    .unwrap();
    let receipt = manager
        .restore(RestoreRequest {
            operation_id: RestoreOperationId::new("restore-finished-source").unwrap(),
            backup_identity: manager.inspect_backup(path.clone()).await.unwrap(),
            source: path,
            destination: PathBuf::from("restored.sqlite3"),
        })
        .await
        .unwrap();
    let restored = SqliteStore::open(SqliteOptions::new(&receipt.destination))
        .await
        .unwrap();
    assert_eq!(
        restored.source_finalization(&binding.source).await.unwrap(),
        SourceFinalizationStatus {
            sealed_end: Some(3),
            parser_finished: true,
        }
    );
    let restored_checkpoint = restored
        .latest_checkpoint(&binding.source)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(restored_checkpoint.source, checkpoint.source);
    assert_eq!(restored_checkpoint.state, checkpoint.state);
    assert_ne!(
        restored_checkpoint.output_stream.incarnation,
        checkpoint.output_stream.incarnation
    );
    EventStore::close(&restored).await.unwrap();
    std::fs::remove_dir_all(root).unwrap();
}

#[tokio::test]
async fn sqlite_finalization_lost_acknowledgements_reconcile_exactly() {
    for (label, injection) in [
        (
            "seal",
            SqliteFailureInjection::AfterJournalSealCommitAcknowledgementLost,
        ),
        (
            "finish",
            SqliteFailureInjection::AfterJournalFinishCommitAcknowledgementLost,
        ),
    ] {
        let root = std::env::temp_dir().join(format!(
            "sqlite-source-eof-{label}-{}",
            uuid::Uuid::new_v4()
        ));
        std::fs::create_dir(&root).unwrap();
        let path = root.join("source.sqlite3");
        let (store, binding) = open_source(&path, Some(injection)).await;
        store.capture_segment(segment(&binding)).await.unwrap();
        let seal = SealSource {
            end: SourcePosition {
                source: binding.source.clone(),
                offset: 3,
            },
        };
        if label == "seal" {
            assert_eq!(
                store.seal_source(seal.clone()).await.unwrap_err(),
                JournalError::SealUnknown(Box::new(seal.clone()))
            );
        } else {
            store.seal_source(seal.clone()).await.unwrap();
        }
        assert_eq!(store.seal_source(seal.clone()).await.unwrap().request, seal);
        let checkpoint = ParserCheckpoint {
            source: seal.end,
            parser: binding.parser.clone(),
            state: Payload::copy_from_slice(b"done"),
            next_item_index: 0,
            output_stream: binding.output_stream,
            committed_output: None,
        };
        store
            .publish_parser_checkpoint(checkpoint.clone())
            .await
            .unwrap();
        let finish = FinishSource { checkpoint };
        if label == "finish" {
            assert_eq!(
                store.finish_source(finish.clone()).await.unwrap_err(),
                JournalError::FinishUnknown(Box::new(finish.clone()))
            );
        }
        assert_eq!(
            store.finish_source(finish.clone()).await.unwrap().request,
            finish
        );
        EventStore::close(&store).await.unwrap();
        std::fs::remove_dir_all(root).unwrap();
    }
}

#[tokio::test]
async fn sqlite_finalization_precommit_failures_roll_back_and_retry() {
    for (label, injection) in [
        ("seal", SqliteFailureInjection::BeforeJournalSealCommit),
        ("finish", SqliteFailureInjection::BeforeJournalFinishCommit),
    ] {
        let root = std::env::temp_dir().join(format!(
            "sqlite-source-eof-before-{label}-{}",
            uuid::Uuid::new_v4()
        ));
        std::fs::create_dir(&root).unwrap();
        let path = root.join("source.sqlite3");
        let (store, binding) = open_source(&path, Some(injection)).await;
        store.capture_segment(segment(&binding)).await.unwrap();
        let seal = SealSource {
            end: SourcePosition {
                source: binding.source.clone(),
                offset: 3,
            },
        };
        if label == "seal" {
            assert!(matches!(
                store.seal_source(seal.clone()).await,
                Err(JournalError::StorageFailure(_))
            ));
            assert_eq!(
                store.source_finalization(&binding.source).await.unwrap(),
                SourceFinalizationStatus {
                    sealed_end: None,
                    parser_finished: false,
                }
            );
        }
        store.seal_source(seal.clone()).await.unwrap();
        let checkpoint = ParserCheckpoint {
            source: seal.end,
            parser: binding.parser.clone(),
            state: Payload::copy_from_slice(b"done"),
            next_item_index: 0,
            output_stream: binding.output_stream,
            committed_output: None,
        };
        store
            .publish_parser_checkpoint(checkpoint.clone())
            .await
            .unwrap();
        let finish = FinishSource { checkpoint };
        if label == "finish" {
            assert!(matches!(
                store.finish_source(finish.clone()).await,
                Err(JournalError::StorageFailure(_))
            ));
            assert!(
                !store
                    .source_finalization(&binding.source)
                    .await
                    .unwrap()
                    .parser_finished
            );
        }
        store.finish_source(finish).await.unwrap();
        assert!(
            store
                .source_finalization(&binding.source)
                .await
                .unwrap()
                .parser_finished
        );
        EventStore::close(&store).await.unwrap();
        std::fs::remove_dir_all(root).unwrap();
    }
}

#[tokio::test]
async fn sqlite_legacy_journal_schema_restores_read_only_and_migrates_on_open() {
    let root =
        std::env::temp_dir().join(format!("sqlite-source-eof-legacy-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir(&root).unwrap();
    let path = root.join("legacy.sqlite3");
    let (store, binding) = open_source(&path, None).await;
    store.capture_segment(segment(&binding)).await.unwrap();
    EventStore::close(&store).await.unwrap();

    rusqlite::Connection::open(&path)
        .unwrap()
        .execute_batch(
            "ALTER TABLE journal_sources DROP COLUMN parser_finished;
             ALTER TABLE journal_sources DROP COLUMN sealed_end;",
        )
        .unwrap();

    let manager = SqliteRestoreManager::new(
        SqliteRestoreBackend::new(&root).unwrap(),
        RestoreConfig::default(),
    )
    .unwrap();
    let receipt = manager
        .restore(RestoreRequest {
            operation_id: RestoreOperationId::new("restore-legacy-source").unwrap(),
            backup_identity: manager.inspect_backup(path.clone()).await.unwrap(),
            source: path.clone(),
            destination: PathBuf::from("restored-legacy.sqlite3"),
        })
        .await
        .unwrap();
    let restored = SqliteStore::open(SqliteOptions::new(&receipt.destination))
        .await
        .unwrap();
    assert_eq!(
        restored.source_finalization(&binding.source).await.unwrap(),
        SourceFinalizationStatus {
            sealed_end: None,
            parser_finished: false,
        }
    );
    assert_eq!(
        restored
            .read_captured(
                &binding.source,
                0,
                RawPageLimits {
                    max_segments: 1,
                    max_bytes: 3,
                },
            )
            .await
            .unwrap()
            .bytes,
        Payload::copy_from_slice(b"abc")
    );
    EventStore::close(&restored).await.unwrap();

    let migrated = SqliteStore::open(SqliteOptions::new(&path)).await.unwrap();
    assert_eq!(
        migrated.source_finalization(&binding.source).await.unwrap(),
        SourceFinalizationStatus {
            sealed_end: None,
            parser_finished: false,
        }
    );
    EventStore::close(&migrated).await.unwrap();
    let columns: i64 = rusqlite::Connection::open(&path)
        .unwrap()
        .query_row(
            "SELECT count(*) FROM pragma_table_info('journal_sources')
             WHERE name IN('sealed_end','parser_finished')",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(columns, 2);
    std::fs::remove_dir_all(root).unwrap();
}

#[tokio::test]
async fn sqlite_restore_rejects_inconsistent_source_finalization_metadata() {
    let root = std::env::temp_dir().join(format!(
        "sqlite-source-eof-corrupt-{}",
        uuid::Uuid::new_v4()
    ));
    std::fs::create_dir(&root).unwrap();
    let path = root.join("corrupt.sqlite3");
    let (store, binding) = open_source(&path, None).await;
    store.capture_segment(segment(&binding)).await.unwrap();
    EventStore::close(&store).await.unwrap();
    rusqlite::Connection::open(&path)
        .unwrap()
        .execute(
            "UPDATE journal_sources SET sealed_end=X'0000000000000002'",
            [],
        )
        .unwrap();

    let manager = SqliteRestoreManager::new(
        SqliteRestoreBackend::new(&root).unwrap(),
        RestoreConfig::default(),
    )
    .unwrap();
    let backup_identity = manager.inspect_backup(path.clone()).await.unwrap();
    assert!(matches!(
        manager
            .restore(RestoreRequest {
                operation_id: RestoreOperationId::new("restore-corrupt-finalization").unwrap(),
                backup_identity,
                source: path,
                destination: PathBuf::from("must-not-publish.sqlite3"),
            })
            .await,
        Err(RestoreError::CorruptBackup(_))
    ));
    std::fs::remove_dir_all(root).unwrap();
}
