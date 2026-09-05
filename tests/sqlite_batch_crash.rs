#![cfg(feature = "sqlite")]
use event_stream::infrastructure::{SqliteFailureInjection, SqliteOptions, SqliteStore};
use event_stream::*;
use std::{
    path::PathBuf,
    process::{Child, Command, Stdio},
    time::Duration,
};

struct Fixture {
    child: Option<Child>,
    dir: PathBuf,
}
impl Fixture {
    fn new() -> Self {
        let dir =
            std::env::temp_dir().join(format!("event-stream-group-crash-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir(&dir).unwrap();
        Self { child: None, dir }
    }
    fn path(&self) -> PathBuf {
        self.dir.join("events.db")
    }
    fn kill(&mut self) {
        let mut child = self.child.take().unwrap();
        child.kill().unwrap();
        assert!(!child.wait().unwrap().success());
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        if let Some(child) = &mut self.child {
            let _ = child.kill();
            let _ = child.wait();
        }
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}
fn event(id: &str, bytes: &[u8]) -> NewEvent {
    NewEvent {
        id: EventId::new(id).unwrap(),
        schema: SchemaRef {
            id: SchemaId::new("crash.bytes").unwrap(),
            version: 1,
        },
        payload: Payload::copy_from_slice(bytes),
    }
}
fn batch(stream: &StreamKey) -> AppendBatch {
    AppendBatch::new(
        [
            ("new-a", b"first".as_slice()),
            ("new-a", b"first".as_slice()),
            ("new-b", b"second".as_slice()),
        ]
        .into_iter()
        .map(|(id, bytes)| AppendRequest {
            stream: stream.clone(),
            event: event(id, bytes),
        })
        .collect(),
        AppendBatchLimits {
            max_records: 3,
            max_bytes: 8192,
        },
    )
    .unwrap()
}

// Entry point for an explicitly spawned child of the parent test below.
#[test]
fn grouped_crash_child() {
    let Some(path) = std::env::var_os("EVENT_STREAM_GROUP_CRASH_PATH") else {
        return;
    };
    let path = PathBuf::from(path);
    let phase = std::env::var("EVENT_STREAM_GROUP_CRASH_PHASE").unwrap();
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(async {
            let mut options = SqliteOptions::new(&path);
            options.failure_injection = Some(match phase.as_str() {
                "uncommitted" => SqliteFailureInjection::PauseBeforeCommit(Duration::from_secs(60)),
                "committed" => SqliteFailureInjection::PauseAfterCommitAcknowledgementLost(
                    Duration::from_secs(60),
                ),
                _ => panic!("unknown crash phase"),
            });
            let store = SqliteStore::open(options).await.unwrap();
            let stream = store
                .create_if_absent(&StreamId::new("group").unwrap())
                .await
                .unwrap();
            let outcomes = store.append_batch(&batch(&stream)).await;
            // The parent requires this to remain absent: kill precedes any response.
            std::fs::write(path.with_extension("reply"), format!("{outcomes:?}")).unwrap();
            store.close().await.unwrap();
        });
}

#[tokio::test]
async fn process_kill_on_both_sides_of_group_commit_preserves_exact_history() {
    for phase in ["uncommitted", "committed"] {
        let mut fixture = Fixture::new();
        let path = fixture.path();
        let initial = SqliteStore::open(SqliteOptions::new(&path)).await.unwrap();
        let stream = initial
            .create_if_absent(&StreamId::new("group").unwrap())
            .await
            .unwrap();
        let baseline = initial
            .append_atomic(&stream, event("old", b"baseline"))
            .await
            .unwrap();
        initial.close().await.unwrap();
        fixture.child = Some(
            Command::new(std::env::current_exe().unwrap())
                .args(["--exact", "grouped_crash_child", "--test-threads=1"])
                .env("EVENT_STREAM_GROUP_CRASH_PATH", &path)
                .env("EVENT_STREAM_GROUP_CRASH_PHASE", phase)
                .stdin(Stdio::null())
                .stdout(Stdio::null())
                .stderr(Stdio::inherit())
                .spawn()
                .unwrap(),
        );
        // This observer bypasses adapter ownership only to inspect committed state.
        // It does not mutate the database or share the child's connection.
        let observer = rusqlite::Connection::open_with_flags(
            &path,
            rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
        )
        .unwrap();
        observer.busy_timeout(Duration::ZERO).unwrap();
        tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                assert!(
                    fixture
                        .child
                        .as_mut()
                        .unwrap()
                        .try_wait()
                        .unwrap()
                        .is_none(),
                    "child exited before {phase} observation"
                );
                assert!(
                    !path.with_extension("reply").exists(),
                    "child returned before kill"
                );
                let count = observer
                    .query_row("SELECT count(*) FROM event_records", [], |r| {
                        r.get::<_, i64>(0)
                    })
                    .ok();
                let journal_written = std::fs::metadata(path.with_extension("db-journal"))
                    .is_ok_and(|m| m.len() > 512);
                if (phase == "committed" && count == Some(3))
                    || (phase == "uncommitted" && count == Some(1) && journal_written)
                {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(2)).await;
            }
        })
        .await
        .expect("child must reach observed transaction phase");
        drop(observer);
        fixture.kill();
        assert!(!path.with_extension("reply").exists());
        let reopened = SqliteStore::open(SqliteOptions::new(&path)).await.unwrap();
        let expected_tail = if phase == "committed" { 3 } else { 1 };
        assert_eq!(
            reopened.bounds(&stream).await.unwrap().tail.offset,
            expected_tail
        );
        assert_eq!(
            reopened
                .lookup_event(&stream, &EventId::new("old").unwrap())
                .await
                .unwrap()
                .unwrap(),
            baseline.record
        );
        for (id, bytes, offset) in [
            ("new-a", b"first".as_slice(), 2),
            ("new-b", b"second".as_slice(), 3),
        ] {
            let found = reopened
                .lookup_event(&stream, &EventId::new(id).unwrap())
                .await
                .unwrap();
            if phase == "committed" {
                let record = found.unwrap();
                assert_eq!(record.event, event(id, bytes));
                assert_eq!(record.cursor.offset, offset);
            } else {
                assert!(found.is_none());
            }
        }
        let retry = batch(&stream);
        let outcomes = reopened.append_batch(&retry).await;
        retry.validate_results(&outcomes).unwrap();
        for (i, outcome) in outcomes.iter().enumerate() {
            let receipt = outcome.as_ref().unwrap();
            assert_eq!(
                receipt.kind,
                if phase == "committed" || i == 1 {
                    AppendKind::Deduplicated
                } else {
                    AppendKind::Inserted
                }
            );
            assert_eq!(receipt.record.cursor.offset, if i == 2 { 3 } else { 2 });
        }
        assert_eq!(reopened.bounds(&stream).await.unwrap().tail.offset, 3);
        reopened.close().await.unwrap();
    }
}
