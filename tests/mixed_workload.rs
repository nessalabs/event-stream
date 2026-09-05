#![cfg(all(
    feature = "sqlite",
    feature = "source-journal",
    feature = "replication"
))]

#[path = "../verification/fixtures/mixed_workload.rs"]
mod mixed;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn scheduled_foreground_and_replay_survive_mixed_recovery_and_reply_loss() {
    for enabled in [false, true] {
        let directory =
            std::env::temp_dir().join(format!("mixed-workload-{}", uuid::Uuid::new_v4()));
        let result = mixed::run(
            &directory,
            mixed::MixedConfig {
                mixed: enabled,
                offers: 128,
                interval_us: 1250,
                snapshot_bytes: 64 * 1024,
            },
        )
        .await;
        assert_eq!(
            result.offered,
            result.accepted + result.runtime_rejected + result.generator_rejected
        );
        assert_eq!(result.receipt_ns.len(), result.accepted);
        assert_eq!(
            result.lateness_ns.len(),
            result.accepted + result.runtime_rejected
        );
        assert!(result.peak_tasks <= 64 && result.peak_runtime_queue <= 1024);
        assert_eq!(result.replay_pages, 64);
        if enabled {
            assert_eq!(result.removed_records, 32);
            assert_eq!(result.replica_tail, 33);
            assert!(result.maintenance_ns > 0 && result.overlap_receipts > 0);
        } else {
            assert_eq!(result.maintenance_ns, 0);
        }
        std::fs::remove_dir_all(directory).unwrap();
    }
}
