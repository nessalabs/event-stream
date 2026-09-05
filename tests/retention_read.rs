#![cfg(all(feature = "sqlite", feature = "retention"))]

#[path = "../verification/fixtures/retention_read.rs"]
mod retention_read;

#[tokio::test]
async fn sqlite_retention_read_has_only_complete_page_or_explicit_history_error() {
    let directory = std::env::temp_dir().join(format!(
        "event-stream-retention-read-{}",
        uuid::Uuid::new_v4()
    ));
    let evidence = tokio::time::timeout(
        std::time::Duration::from_secs(15),
        retention_read::run(&directory),
    )
    .await
    .expect("controlled retention/read schedule deadlocked");

    assert!(evidence.pending_read_was_history_unavailable);
    assert_eq!(evidence.pending_read_floor, 3);
    assert_eq!(evidence.pending_read_tail, 3);
    assert_eq!(evidence.completed_offsets, vec![1, 2, 3]);
    assert!(evidence.completed_page_complete);
    assert_eq!(evidence.completed_page_through, 3);
    assert_eq!(
        evidence.retained_payloads_after_cleanup,
        vec![
            b"read-after-sqlite:1".to_vec(),
            b"read-after-sqlite:2".to_vec(),
            b"read-after-sqlite:3".to_vec(),
        ]
    );
    assert_eq!(evidence.removed_event_rows, 6);
    assert!(!evidence.cleanup_remaining);

    std::fs::remove_dir_all(directory).unwrap();
}
