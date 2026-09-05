#![cfg(all(feature = "sqlite", feature = "snapshots"))]

#[path = "../verification/fixtures/snapshot_interruption.rs"]
mod fixture;

#[tokio::test]
async fn interrupted_snapshot_staging_is_hidden_resumable_and_bounded_after_reopen() {
    let directory =
        std::env::temp_dir().join(format!("snapshot-interruption-{}", uuid::Uuid::new_v4()));

    let evidence = fixture::run(&directory).await;

    assert_eq!(evidence.staged_bytes_after_reopen, 12);
    assert_eq!(evidence.published_count_while_interrupted, 1);
    assert_eq!(evidence.exact_retry_accepted_bytes, 4);
    assert_eq!(evidence.resumed_snapshot_bytes, b"resume-after-reopen");
    assert_eq!(evidence.baseline_snapshot_bytes, b"published-baseline");
    assert_eq!(evidence.cleanup_steps, 3);
    assert_eq!(evidence.cleanup_removed_chunks, 2);
    assert_eq!(evidence.cleanup_removed_snapshots, 1);
    assert_eq!(evidence.uploads_remaining, 0);

    std::fs::remove_dir_all(directory).unwrap();
}
