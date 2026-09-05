#![cfg(all(feature = "sqlite", feature = "test-support"))]
use event_stream::infrastructure::{
    install_sqlite_vfs_recorder, sqlite_vfs_snapshot, SqliteOptions, SqliteStore, SqliteVfsSnapshot,
};
use event_stream::*;

fn syncs(s: SqliteVfsSnapshot) -> u64 {
    [
        s.source,
        s.target,
        s.source_journal,
        s.target_journal,
        s.temporary,
        s.other,
    ]
    .iter()
    .map(|c| c.sync_calls)
    .sum()
}
fn event(i: usize) -> NewEvent {
    NewEvent {
        id: EventId::new(format!("event-{i}")).unwrap(),
        schema: SchemaRef {
            id: SchemaId::new("sync.bytes").unwrap(),
            version: 1,
        },
        payload: Payload::copy_from_slice(b"durable"),
    }
}

// Separate test executable: recorder state has no concurrently running test users.
#[tokio::test]
async fn grouped_appends_share_real_sqlite_sync_work() {
    install_sqlite_vfs_recorder().unwrap();
    let dir =
        std::env::temp_dir().join(format!("event-stream-group-sync-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir(&dir).unwrap();
    let path = dir.join("events.db");
    let store = SqliteStore::open(SqliteOptions::new(&path)).await.unwrap();
    let control = store
        .create_if_absent(&StreamId::new("control").unwrap())
        .await
        .unwrap();
    let grouped = store
        .create_if_absent(&StreamId::new("grouped").unwrap())
        .await
        .unwrap();
    let before = syncs(sqlite_vfs_snapshot());
    for i in 0..8 {
        store.append_atomic(&control, event(i)).await.unwrap();
    }
    let single_syncs = syncs(sqlite_vfs_snapshot()) - before;
    let batch = AppendBatch::new(
        (0..8)
            .map(|i| AppendRequest {
                stream: grouped.clone(),
                event: event(i),
            })
            .collect(),
        AppendBatchLimits {
            max_records: 8,
            max_bytes: 8192,
        },
    )
    .unwrap();
    let before = syncs(sqlite_vfs_snapshot());
    let outcomes = store.append_batch(&batch).await;
    let grouped_syncs = syncs(sqlite_vfs_snapshot()) - before;
    batch.validate_results(&outcomes).unwrap();
    assert!(outcomes.iter().all(Result::is_ok));
    assert!(grouped_syncs > 0, "durable group must perform sync work");
    assert!(
        grouped_syncs < single_syncs,
        "group={grouped_syncs}, single={single_syncs}"
    );
    println!("eight inserts: individual sync calls={single_syncs}, grouped sync calls={grouped_syncs}; no latency benchmark");
    store.close().await.unwrap();
    let reopened = SqliteStore::open(SqliteOptions::new(&path)).await.unwrap();
    for stream in [&control, &grouped] {
        let page = reopened
            .read_range(
                stream,
                0,
                8,
                PageLimits {
                    max_records: 8,
                    max_bytes: 8192,
                },
            )
            .await
            .unwrap();
        assert_eq!(page.records.len(), 8);
        for (i, record) in page.records.iter().enumerate() {
            assert_eq!(record.event, event(i));
            assert_eq!(record.cursor.offset, i as u64 + 1);
        }
    }
    reopened.close().await.unwrap();
    std::fs::remove_dir_all(dir).unwrap();
}
