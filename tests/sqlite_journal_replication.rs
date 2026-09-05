#![cfg(all(
    feature = "sqlite",
    feature = "source-journal",
    feature = "replication"
))]
use event_stream::infrastructure::{SqliteOptions, SqliteStore};
use event_stream::ingestion::*;
use event_stream::*;
use std::time::Duration;
#[path = "../verification/fixtures/journal_replication.rs"]
mod fixture;
use fixture::{finish_parsing, prepare_input};
struct ChildProcess(std::process::Child);
impl Drop for ChildProcess {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}
async fn recover_input(database: &std::path::Path, stage: &str) {
    let ready = database.with_extension("ready");
    let mut child = ChildProcess(
        std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "journal_recovery_to_replica_survives_both_sqlite_restarts",
                "--nocapture",
            ])
            .env("EVENT_STREAM_JOURNAL_KILL_CHILD", database)
            .env("EVENT_STREAM_JOURNAL_KILL_STAGE", stage)
            .env("EVENT_STREAM_JOURNAL_KILL_READY", &ready)
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::inherit())
            .spawn()
            .unwrap(),
    );
    let deadline = std::time::Instant::now() + Duration::from_secs(20);
    while !ready.exists() {
        assert!(
            child.0.try_wait().unwrap().is_none(),
            "child exited before reaching commit gap"
        );
        assert!(
            std::time::Instant::now() < deadline,
            "child did not reach commit gap"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    child.0.kill().unwrap();
    let exit = child.0.wait().unwrap();
    assert!(!exit.success());
    #[cfg(unix)]
    {
        use std::os::unix::process::ExitStatusExt;
        assert_eq!(exit.signal(), Some(9));
    }
    let runtime =
        Runtime::<SqliteStore>::open(SqliteOptions::new(database), RuntimeConfig::default())
            .await
            .unwrap();
    let output = runtime
        .create_stream(&StreamId::new("origin").unwrap())
        .await
        .unwrap();
    let binding = SourceBinding {
        source: SourceKey {
            id: SourceId::new("raw-input").unwrap(),
            incarnation: SourceIncarnation([72; 16]),
        },
        parser: fixture::decoder().parser(),
        output_stream: output,
    };
    let before = runtime
        .read_after(
            &Cursor::new(binding.output_stream.clone(), 0),
            PageLimits {
                max_records: 4,
                max_bytes: RuntimeConfig::default().reads.page.max_bytes,
            },
            None,
        )
        .await
        .unwrap();
    assert_eq!(
        before.records.len(),
        match stage {
            "capture" => 0,
            "output" => 1,
            "checkpoint" => 2,
            _ => unreachable!(),
        },
        "wrong durable state at {stage}"
    );
    assert_eq!(
        runtime
            .latest_checkpoint(&binding.source)
            .await
            .unwrap()
            .is_some(),
        stage == "checkpoint"
    );
    finish_parsing(&runtime, &binding).await;
    runtime.shutdown(Duration::from_secs(2)).await.unwrap();
}
#[tokio::test]
async fn journal_recovery_to_replica_survives_both_sqlite_restarts() {
    if let Some(database) = std::env::var_os("EVENT_STREAM_JOURNAL_KILL_CHILD") {
        let (_runtime, _binding) = prepare_input(
            std::path::Path::new(&database),
            &std::env::var("EVENT_STREAM_JOURNAL_KILL_STAGE").unwrap(),
        )
        .await;
        // Signal only after the selected durable boundary has been reached.
        std::fs::write(
            std::env::var_os("EVENT_STREAM_JOURNAL_KILL_READY").unwrap(),
            b"committed",
        )
        .unwrap();
        std::future::pending::<()>().await;
        unreachable!();
    }

    for stage in ["capture", "output", "checkpoint"] {
        run_mixed(stage).await;
    }
}
async fn run_mixed(stage: &str) {
    let directory = std::env::temp_dir().join(format!("sqlite-bootstrap-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir(&directory).unwrap();
    recover_input(&directory.join("origin.db"), stage).await;
    let evidence = fixture::verify_recovered_history(&directory).await;
    assert_eq!(evidence.snapshot_bytes, b"state-through-a");
    assert_eq!(evidence.suffix_offset, 2);
    assert_eq!(evidence.removed_records, 2);
    assert_eq!(evidence.caught_up_offset, 3);
    assert_eq!(evidence.backlog_records, 0);
    assert_eq!(
        evidence.finalization,
        SourceFinalizationStatus {
            sealed_end: Some(4),
            parser_finished: true
        }
    );
    std::fs::remove_dir_all(directory).unwrap();
}
