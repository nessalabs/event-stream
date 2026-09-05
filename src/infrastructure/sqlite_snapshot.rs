use super::sqlite::{
    read_page_exact, Command, SqliteFailureInjection, SqliteOptions, SqliteStore, StoredRange,
};
use crate::{application::*, domain::*};
use async_trait::async_trait;
use rusqlite::{params, Connection, OptionalExtension, TransactionBehavior};
use sha2::{Digest, Sha256};
use std::{collections::HashMap, time::Duration};
use tokio::sync::oneshot;

const UPLOADING: i64 = 0;
const VERIFYING: i64 = 1;
const VERIFIED: i64 = 2;
const PUBLISHED: i64 = 3;
const ABORTED: i64 = 4;

pub(super) enum SnapshotCommand {
    Begin(
        SnapshotDescriptor,
        oneshot::Sender<SnapshotResult<SnapshotUploadProgress>>,
    ),
    Put(
        SnapshotId,
        SnapshotChunk,
        oneshot::Sender<SnapshotResult<SnapshotUploadProgress>>,
    ),
    Status(
        SnapshotId,
        oneshot::Sender<SnapshotResult<SnapshotUploadProgress>>,
    ),
    Verify(
        SnapshotId,
        VerificationLimits,
        oneshot::Sender<SnapshotResult<SnapshotUploadProgress>>,
    ),
    Publish(
        SnapshotDescriptor,
        oneshot::Sender<SnapshotResult<SnapshotDescriptor>>,
    ),
    Abort(
        SnapshotId,
        oneshot::Sender<SnapshotResult<SnapshotAbortReceipt>>,
    ),
    Cleanup(
        SnapshotCleanupLimits,
        oneshot::Sender<SnapshotResult<SnapshotCleanupProgress>>,
    ),
    ListUploads(
        Option<SnapshotId>,
        PageLimits,
        oneshot::Sender<SnapshotResult<SnapshotUploadPage>>,
    ),
    List(
        StreamKey,
        Option<SnapshotContinuation>,
        PageLimits,
        oneshot::Sender<SnapshotResult<SnapshotPage>>,
    ),
    Acquire(
        SnapshotId,
        Duration,
        oneshot::Sender<SnapshotResult<RecoveryPlan>>,
    ),
    ReadBytes(
        RecoveryLeaseId,
        u64,
        usize,
        oneshot::Sender<SnapshotResult<SnapshotBytePage>>,
    ),
    ReadRecovery(
        RecoveryLeaseId,
        u64,
        PageLimits,
        oneshot::Sender<SnapshotResult<Page>>,
    ),
    Release(
        RecoveryLeaseId,
        oneshot::Sender<SnapshotResult<RecoveryRelease>>,
    ),
}

struct Verification {
    next: u64,
    hasher: Sha256,
}

#[derive(Clone)]
struct Lease {
    snapshot: SnapshotDescriptor,
    through: u64,
    expires: MonotonicTick,
}

pub(super) struct SqliteSnapshotState {
    verifications: HashMap<SnapshotId, Verification>,
    leases: HashMap<RecoveryLeaseId, Lease>,
}

impl SqliteSnapshotState {
    pub(super) fn new() -> Self {
        Self {
            verifications: HashMap::new(),
            leases: HashMap::new(),
        }
    }

    pub(super) fn maximum_cleanup_offset(&mut self, stream: &StreamKey, now: MonotonicTick) -> u64 {
        self.leases.retain(|_, lease| lease.expires.0 > now.0);
        self.leases
            .values()
            .filter(|lease| &lease.snapshot.covered.stream == stream)
            .map(|lease| lease.snapshot.covered.offset)
            .min()
            .unwrap_or(u64::MAX)
    }

    #[cfg(feature = "replication")]
    pub(super) fn acquire_for_replication(
        &mut self,
        conn: &Connection,
        id: SnapshotId,
        options: &SqliteOptions,
    ) -> SnapshotResult<(RecoveryPlan, MonotonicTick)> {
        let plan = acquire(
            conn,
            id,
            options.snapshots.recovery.max_lifetime,
            options,
            self,
        )?;
        let expires = self
            .leases
            .get(&plan.lease)
            .map(|lease| lease.expires)
            .ok_or_else(|| SnapshotError::CorruptStorage("new recovery lease is missing".into()))?;
        Ok((plan, expires))
    }

    #[cfg(feature = "replication")]
    pub(super) fn replication_lease_is_live(
        &self,
        id: RecoveryLeaseId,
        now: MonotonicTick,
    ) -> bool {
        self.leases
            .get(&id)
            .is_some_and(|lease| lease.expires.0 > now.0)
    }

    #[cfg(feature = "replication")]
    pub(super) fn release_for_replication(&mut self, id: RecoveryLeaseId) {
        self.leases.remove(&id);
    }

    #[cfg(feature = "replication")]
    pub(super) fn restore_replication_lease(
        &mut self,
        conn: &Connection,
        id: RecoveryLeaseId,
        snapshot: &SnapshotDescriptor,
        through: u64,
        expires: MonotonicTick,
        options: &SqliteOptions,
    ) -> SnapshotResult<()> {
        let now = options.snapshot_clock.now();
        if expires.0 <= now.0 {
            return Err(SnapshotError::ExpiredProtection { lease: id });
        }
        if self.leases.get(&id).is_some_and(|lease| {
            &lease.snapshot == snapshot && lease.through == through && lease.expires == expires
        }) {
            return Ok(());
        }
        self.leases.retain(|_, lease| lease.expires.0 > now.0);
        if self.leases.len() >= options.snapshots.recovery.max_leases {
            return Err(SnapshotError::CapacityExceeded);
        }
        let persisted = descriptor(conn, snapshot.id, None)?
            .filter(|progress| progress.state == SnapshotUploadState::Published)
            .ok_or(SnapshotError::NotFound { id: snapshot.id })?;
        if persisted.descriptor != *snapshot
            || exact_stream_bounds_optional(conn, &snapshot.covered.stream)?
                .is_none_or(|(_, floor, tail)| snapshot.covered.offset < floor || through > tail)
        {
            return Err(SnapshotError::CorruptStorage(
                "persisted replication recovery lease no longer names available history".into(),
            ));
        }
        self.leases.insert(
            id,
            Lease {
                snapshot: snapshot.clone(),
                through,
                expires,
            },
        );
        Ok(())
    }
}

pub(super) fn initialize_snapshot_schema(conn: &Connection) -> crate::application::Result<()> {
    let existing: u32 = conn
        .query_row(
            "SELECT count(*) FROM sqlite_schema WHERE type='table' AND name IN ('snapshot_metadata','snapshots','snapshot_chunks')",
            [],
            |row| row.get(0),
        )
        .map_err(|error| crate::application::Error::StoreCorrupt(format!("inspect snapshot schema: {error}")))?;
    if existing != 0 && existing != 3 {
        return Err(crate::application::Error::StoreCorrupt(
            "snapshot schema is only partially present".into(),
        ));
    }
    conn.execute_batch(
        "BEGIN IMMEDIATE;
         CREATE TABLE IF NOT EXISTS snapshot_metadata(
           singleton INTEGER PRIMARY KEY CHECK(singleton=1),
           staged_count INTEGER NOT NULL CHECK(staged_count>=0),
           staged_bytes INTEGER NOT NULL CHECK(staged_bytes>=0),
           published_count INTEGER NOT NULL CHECK(published_count>=0),
           published_bytes INTEGER NOT NULL CHECK(published_bytes>=0),
           chunk_count INTEGER NOT NULL CHECK(chunk_count>=0),
           chunk_metadata_bytes INTEGER NOT NULL CHECK(chunk_metadata_bytes>=0),
           descriptor_metadata_bytes INTEGER NOT NULL CHECK(descriptor_metadata_bytes>=0),
           receipt_count INTEGER NOT NULL CHECK(receipt_count>=0),
           receipt_bytes INTEGER NOT NULL CHECK(receipt_bytes>=0));
         CREATE TABLE IF NOT EXISTS snapshots(
           snapshot_id BLOB PRIMARY KEY CHECK(typeof(snapshot_id)='blob' AND length(snapshot_id)=16),
           public_id TEXT NOT NULL,
           incarnation BLOB NOT NULL CHECK(typeof(incarnation)='blob' AND length(incarnation)=16),
           covered BLOB NOT NULL CHECK(typeof(covered)='blob' AND length(covered)=8),
           schema_id TEXT NOT NULL,
           schema_version INTEGER NOT NULL,
           content_bytes BLOB NOT NULL CHECK(typeof(content_bytes)='blob' AND length(content_bytes)=8),
           digest BLOB NOT NULL CHECK(typeof(digest)='blob' AND length(digest)=32),
           state INTEGER NOT NULL CHECK(state BETWEEN 0 AND 4),
           accepted_bytes BLOB NOT NULL CHECK(typeof(accepted_bytes)='blob' AND length(accepted_bytes)=8),
           verified_bytes BLOB NOT NULL CHECK(typeof(verified_bytes)='blob' AND length(verified_bytes)=8),
           checksum_failed INTEGER NOT NULL DEFAULT 0 CHECK(checksum_failed IN (0,1)),
           cleaned INTEGER NOT NULL DEFAULT 0 CHECK(cleaned IN (0,1)),
           descriptor_charge INTEGER NOT NULL CHECK(descriptor_charge>=0),
           receipt_reserved INTEGER NOT NULL DEFAULT 0 CHECK(receipt_reserved IN (0,1)));
         CREATE TABLE IF NOT EXISTS snapshot_chunks(
           snapshot_id BLOB NOT NULL,
           offset BLOB NOT NULL CHECK(typeof(offset)='blob' AND length(offset)=8),
           bytes BLOB NOT NULL,
           PRIMARY KEY(snapshot_id,offset),
           FOREIGN KEY(snapshot_id) REFERENCES snapshots(snapshot_id));
         CREATE INDEX IF NOT EXISTS snapshots_published_idx
           ON snapshots(public_id,incarnation,state,covered,snapshot_id);
         CREATE INDEX IF NOT EXISTS snapshots_cleanup_idx ON snapshots(state,cleaned,snapshot_id);
         INSERT OR IGNORE INTO snapshot_metadata VALUES(1,0,0,0,0,0,0,0,0,0);
         UPDATE snapshots SET state=0,verified_bytes=zeroblob(8) WHERE state=1;
         COMMIT;",
    )
    .map_err(|error| {
        if error.sqlite_error_code() == Some(rusqlite::ErrorCode::DiskFull) {
            crate::application::Error::CapacityExceeded
        } else {
            crate::application::Error::StoreWriteFailed(format!(
                "initialize snapshot schema: {error}"
            ))
        }
    })?;
    validate_snapshot_counters(conn)
        .map_err(|error| crate::application::Error::StoreCorrupt(error.to_string()))
}

pub(crate) fn validate_snapshot_counters(conn: &Connection) -> SnapshotResult<()> {
    let stored: (i64,i64,i64,i64,i64,i64,i64,i64,i64) = conn.query_row(
        "SELECT staged_count,staged_bytes,published_count,published_bytes,chunk_count,chunk_metadata_bytes,descriptor_metadata_bytes,receipt_count,receipt_bytes FROM snapshot_metadata WHERE singleton=1",
        [], |r| Ok((r.get(0)?,r.get(1)?,r.get(2)?,r.get(3)?,r.get(4)?,r.get(5)?,r.get(6)?,r.get(7)?,r.get(8)?)))
        .map_err(|e| corrupt("read snapshot counters",e))?;
    if [
        stored.0, stored.1, stored.2, stored.3, stored.4, stored.5, stored.6, stored.7, stored.8,
    ]
    .iter()
    .any(|v| *v < 0)
    {
        return Err(SnapshotError::CorruptStorage(
            "snapshot counter is negative".into(),
        ));
    }
    let mut actual = [0_u64; 9];
    let mut statement = conn.prepare(
        "SELECT CASE WHEN typeof(snapshot_id)='blob' AND length(snapshot_id)=16 THEN snapshot_id END,
                CASE WHEN typeof(public_id)='text' AND octet_length(public_id) BETWEEN 1 AND 256 THEN octet_length(public_id) END,
                CASE WHEN typeof(schema_id)='text' AND octet_length(schema_id) BETWEEN 1 AND 256 THEN octet_length(schema_id) END,
                state, cleaned,
                CASE WHEN typeof(content_bytes)='blob' AND length(content_bytes)=8 THEN content_bytes END,
                CASE WHEN typeof(descriptor_charge)='integer' AND descriptor_charge>=0 THEN descriptor_charge END,
                CASE WHEN typeof(receipt_reserved)='integer' AND receipt_reserved IN (0,1) THEN receipt_reserved END,
                CASE WHEN typeof(accepted_bytes)='blob' AND length(accepted_bytes)=8 THEN accepted_bytes END,
                CASE WHEN typeof(verified_bytes)='blob' AND length(verified_bytes)=8 THEN verified_bytes END,
                CASE WHEN typeof(checksum_failed)='integer' AND checksum_failed IN (0,1) THEN checksum_failed END
         FROM snapshots",
    ).map_err(|e| corrupt("prepare snapshot counter audit", e))?;
    let rows = statement
        .query_map([], |row| {
            Ok((
                row.get::<_, Option<Vec<u8>>>(0)?,
                row.get::<_, Option<i64>>(1)?,
                row.get::<_, Option<i64>>(2)?,
                row.get::<_, i64>(3)?,
                row.get::<_, i64>(4)?,
                row.get::<_, Option<Vec<u8>>>(5)?,
                row.get::<_, Option<i64>>(6)?,
                row.get::<_, Option<i64>>(7)?,
                row.get::<_, Option<Vec<u8>>>(8)?,
                row.get::<_, Option<Vec<u8>>>(9)?,
                row.get::<_, Option<i64>>(10)?,
            ))
        })
        .map_err(|e| corrupt("query snapshot counter audit", e))?;
    for row in rows {
        let (
            id,
            public_len,
            schema_len,
            state,
            cleaned,
            content,
            descriptor_charge,
            receipt_reserved,
            accepted,
            verified,
            checksum_failed,
        ) = row.map_err(|e| corrupt("read snapshot counter audit", e))?;
        if id.is_none() {
            return Err(SnapshotError::CorruptStorage(
                "snapshot identifier is malformed".into(),
            ));
        }
        if !(UPLOADING..=ABORTED).contains(&state) || !(0..=1).contains(&cleaned) {
            return Err(SnapshotError::CorruptStorage(
                "snapshot state is malformed".into(),
            ));
        }
        let content = decode_u64(&content.ok_or_else(|| {
            SnapshotError::CorruptStorage("snapshot content length is malformed".into())
        })?)?;
        let descriptor_charge = u64::try_from(descriptor_charge.ok_or_else(|| {
            SnapshotError::CorruptStorage("snapshot descriptor charge is malformed".into())
        })?)
        .map_err(|_| {
            SnapshotError::CorruptStorage("snapshot descriptor charge is malformed".into())
        })?;
        let expected_charge = public_len
            .and_then(|public| schema_len.and_then(|schema| public.checked_add(schema)))
            .and_then(|names| names.checked_add(SNAPSHOT_DESCRIPTOR_ENVELOPE_BYTES as i64))
            .ok_or_else(|| {
                SnapshotError::CorruptStorage("snapshot descriptor charge is malformed".into())
            })?;
        if descriptor_charge != expected_charge as u64 {
            return Err(SnapshotError::CorruptStorage(
                "snapshot descriptor charge disagrees with its fields".into(),
            ));
        }
        let receipt_reserved = receipt_reserved.ok_or_else(|| {
            SnapshotError::CorruptStorage("snapshot receipt state is malformed".into())
        })?;
        let accepted = decode_u64(&accepted.ok_or_else(|| {
            SnapshotError::CorruptStorage("snapshot accepted byte count is malformed".into())
        })?)?;
        let verified = decode_u64(&verified.ok_or_else(|| {
            SnapshotError::CorruptStorage("snapshot verified byte count is malformed".into())
        })?)?;
        let checksum_failed = checksum_failed.ok_or_else(|| {
            SnapshotError::CorruptStorage("snapshot checksum state is malformed".into())
        })?;
        if accepted > content
            || verified > accepted
            || matches!(state, VERIFIED | PUBLISHED)
                && (accepted != content
                    || verified != content
                    || checksum_failed != 0
                    || cleaned != 0)
            || state == UPLOADING && verified != 0
            || state == ABORTED && receipt_reserved != 1
            || cleaned == 1 && (state != ABORTED || accepted != 0 || verified != 0)
        {
            return Err(SnapshotError::CorruptStorage(
                "snapshot progress metadata is inconsistent".into(),
            ));
        }
        if state != PUBLISHED && cleaned == 0 {
            actual[0] = actual[0].checked_add(1).ok_or_else(counter_overflow)?;
            actual[1] = actual[1]
                .checked_add(content)
                .ok_or_else(counter_overflow)?;
        }
        if state == PUBLISHED {
            actual[2] = actual[2].checked_add(1).ok_or_else(counter_overflow)?;
            actual[3] = actual[3]
                .checked_add(content)
                .ok_or_else(counter_overflow)?;
        }
        actual[6] = actual[6]
            .checked_add(descriptor_charge)
            .ok_or_else(counter_overflow)?;
        if receipt_reserved == 1 {
            actual[7] = actual[7].checked_add(1).ok_or_else(counter_overflow)?;
            actual[8] = actual[8]
                .checked_add(descriptor_charge)
                .ok_or_else(counter_overflow)?;
        }
    }
    let chunk_count: i64 = conn
        .query_row("SELECT count(*) FROM snapshot_chunks", [], |row| row.get(0))
        .map_err(|e| corrupt("count snapshot chunks", e))?;
    actual[4] = u64::try_from(chunk_count).map_err(|_| counter_overflow())?;
    actual[5] = actual[4]
        .checked_mul(SNAPSHOT_CHUNK_ENVELOPE_BYTES as u64)
        .ok_or_else(counter_overflow)?;
    let stored = [
        stored.0, stored.1, stored.2, stored.3, stored.4, stored.5, stored.6, stored.7, stored.8,
    ]
    .map(|value| value as u64);
    if stored != actual {
        return Err(SnapshotError::CorruptStorage(
            "snapshot counters disagree with rows".into(),
        ));
    }
    Ok(())
}

fn counter_overflow() -> SnapshotError {
    SnapshotError::CorruptStorage("snapshot counter audit overflowed".into())
}

fn consume_snapshot_failure(
    failure: &mut Option<SqliteFailureInjection>,
    expected: SqliteFailureInjection,
) -> bool {
    if failure.as_ref() == Some(&expected) {
        failure.take();
        true
    } else {
        false
    }
}

fn reject_before_snapshot_commit(
    failure: &mut Option<SqliteFailureInjection>,
) -> SnapshotResult<()> {
    if consume_snapshot_failure(failure, SqliteFailureInjection::BeforeSnapshotCommit) {
        Err(SnapshotError::StorageFailure(
            "injected failure before snapshot commit".into(),
        ))
    } else {
        Ok(())
    }
}

fn lose_snapshot_acknowledgement(failure: &mut Option<SqliteFailureInjection>) -> bool {
    if consume_snapshot_failure(
        failure,
        SqliteFailureInjection::AfterSnapshotCommitAcknowledgementLost,
    ) {
        return true;
    }
    let Some(SqliteFailureInjection::PauseAfterSnapshotCommitAcknowledgementLost(duration)) =
        failure.as_ref()
    else {
        return false;
    };
    let duration = *duration;
    failure.take();
    std::thread::sleep(duration);
    true
}

pub(super) fn handle_snapshot_command(
    conn: &mut Connection,
    command: SnapshotCommand,
    options: &SqliteOptions,
    state: &mut SqliteSnapshotState,
    failure: &mut Option<SqliteFailureInjection>,
) {
    match command {
        SnapshotCommand::Begin(v, tx) => {
            let _ = tx.send(begin(conn, v, options, failure));
        }
        SnapshotCommand::Put(id, v, tx) => {
            let _ = tx.send(put(conn, id, v, options, failure));
        }
        SnapshotCommand::Status(id, tx) => {
            let _ = tx.send(status(conn, id, state));
        }
        SnapshotCommand::Verify(id, l, tx) => {
            let _ = tx.send(verify(conn, id, l, options, state));
        }
        SnapshotCommand::Publish(descriptor, tx) => {
            let _ = tx.send(publish(conn, descriptor.id, options, state, failure));
        }
        SnapshotCommand::Abort(id, tx) => {
            let _ = tx.send(abort(conn, id, options, state, failure));
        }
        SnapshotCommand::Cleanup(l, tx) => {
            let _ = tx.send(cleanup(conn, l, options));
        }
        SnapshotCommand::ListUploads(a, l, tx) => {
            let _ = tx.send(list_uploads(conn, a, l, options, state));
        }
        SnapshotCommand::List(s, a, l, tx) => {
            let _ = tx.send(list(conn, &s, a, l, options));
        }
        SnapshotCommand::Acquire(id, l, tx) => {
            let _ = tx.send(acquire(conn, id, l, options, state));
        }
        SnapshotCommand::ReadBytes(id, o, m, tx) => {
            let _ = tx.send(read_bytes(conn, id, o, m, options, state));
        }
        SnapshotCommand::ReadRecovery(id, a, l, tx) => {
            let _ = tx.send(read_recovery(conn, id, a, l, options, state));
        }
        SnapshotCommand::Release(id, tx) => {
            let _ = tx.send(Ok(if state.leases.remove(&id).is_some() {
                RecoveryRelease::Released
            } else {
                RecoveryRelease::AlreadyReleased
            }));
        }
    }
}

pub(super) fn fail_snapshot_command(command: SnapshotCommand) {
    let e = || SnapshotError::StorageFailure("SQLite connection cannot be safely reused".into());
    match command {
        SnapshotCommand::Begin(descriptor, tx) => {
            let _ = tx.send(Err(SnapshotError::BeginUnknown {
                descriptor: Box::new(descriptor),
            }));
        }
        SnapshotCommand::Put(id, chunk, tx) => {
            let _ = tx.send(Err(SnapshotError::ChunkUnknown {
                id,
                chunk: Box::new(chunk),
            }));
        }
        SnapshotCommand::Status(_, tx) => {
            let _ = tx.send(Err(e()));
        }
        SnapshotCommand::Verify(_, _, tx) => {
            let _ = tx.send(Err(e()));
        }
        SnapshotCommand::Publish(descriptor, tx) => {
            let _ = tx.send(Err(SnapshotError::PublicationUnknown {
                descriptor: Box::new(descriptor),
            }));
        }
        SnapshotCommand::Abort(id, tx) => {
            let _ = tx.send(Err(SnapshotError::AbortUnknown { id }));
        }
        SnapshotCommand::Cleanup(_, tx) => {
            let _ = tx.send(Err(e()));
        }
        SnapshotCommand::ListUploads(_, _, tx) => {
            let _ = tx.send(Err(e()));
        }
        SnapshotCommand::List(_, _, _, tx) => {
            let _ = tx.send(Err(e()));
        }
        SnapshotCommand::Acquire(_, _, tx) => {
            let _ = tx.send(Err(e()));
        }
        SnapshotCommand::ReadBytes(_, _, _, tx) => {
            let _ = tx.send(Err(e()));
        }
        SnapshotCommand::ReadRecovery(_, _, _, tx) => {
            let _ = tx.send(Err(e()));
        }
        SnapshotCommand::Release(_, tx) => {
            let _ = tx.send(Err(e()));
        }
    }
}

fn begin(
    conn: &mut Connection,
    d: SnapshotDescriptor,
    o: &SqliteOptions,
    failure: &mut Option<SqliteFailureInjection>,
) -> SnapshotResult<SnapshotUploadProgress> {
    let unknown = d.clone();
    if d.covered.version != CURSOR_VERSION {
        return Err(SnapshotError::InvalidInput(
            "snapshot cursor version is unsupported".into(),
        ));
    }
    let charge = d.accounted_bytes().ok_or(SnapshotError::CapacityExceeded)?;
    if let Some(existing) = descriptor(conn, d.id, None)? {
        return if existing.descriptor == d {
            Ok(existing)
        } else {
            Err(SnapshotError::OperationConflict { id: d.id })
        };
    }
    let bounds = stream_bounds(conn, &d.covered.stream)?;
    if d.covered.offset < bounds.0 {
        return Err(SnapshotError::MissingHistory {
            floor: Box::new(Cursor::new(d.covered.stream.clone(), bounds.0)),
        });
    }
    if d.covered.offset > bounds.1 {
        return Err(SnapshotError::CursorAhead {
            tail: Box::new(Cursor::new(d.covered.stream.clone(), bounds.1)),
        });
    }
    let c = read_counters(conn)?;
    let s = &o.snapshots.storage;
    if c.staged_count >= s.max_staging_snapshots as u64
        || c.staged_count >= i64::MAX as u64
        || d.content_bytes > s.max_staging_bytes
        || c.staged_bytes
            .checked_add(d.content_bytes)
            .is_none_or(|v| v > s.max_staging_bytes || v > i64::MAX as u64)
        || c.descriptor_bytes
            .checked_add(charge as u64)
            .is_none_or(|v| v > s.max_descriptor_metadata_bytes as u64)
    {
        return Err(SnapshotError::CapacityExceeded);
    }
    let tx = conn
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .map_err(|e| write("begin snapshot", e))?;
    tx.execute(
        "INSERT INTO snapshots VALUES(?1,?2,?3,?4,?5,?6,?7,?8,0,?9,?9,0,0,?10,0)",
        params![
            d.id.0.as_slice(),
            d.covered.stream.id.as_str(),
            d.covered.stream.incarnation.0.as_slice(),
            be(d.covered.offset).as_slice(),
            d.schema.id.as_str(),
            d.schema.version,
            be(d.content_bytes).as_slice(),
            d.digest.0.as_slice(),
            be(0).as_slice(),
            charge as i64
        ],
    )
    .map_err(|e| write("insert snapshot", e))?;
    tx.execute("UPDATE snapshot_metadata SET staged_count=staged_count+1,staged_bytes=staged_bytes+?1,descriptor_metadata_bytes=descriptor_metadata_bytes+?2 WHERE singleton=1",params![i64u(d.content_bytes)?,charge as i64]).map_err(|e|write("reserve snapshot",e))?;
    reject_before_snapshot_commit(failure)?;
    tx.commit().map_err(|error| {
        if error.sqlite_error_code() == Some(rusqlite::ErrorCode::DiskFull) {
            SnapshotError::CapacityExceeded
        } else {
            SnapshotError::BeginUnknown {
                descriptor: Box::new(unknown.clone()),
            }
        }
    })?;
    if lose_snapshot_acknowledgement(failure) {
        return Err(SnapshotError::BeginUnknown {
            descriptor: Box::new(unknown),
        });
    }
    Ok(progress(d, 0, 0, UPLOADING))
}

fn put(
    conn: &mut Connection,
    id: SnapshotId,
    chunk: SnapshotChunk,
    o: &SqliteOptions,
    failure: &mut Option<SqliteFailureInjection>,
) -> SnapshotResult<SnapshotUploadProgress> {
    let unknown = chunk.clone();
    if chunk.bytes.is_empty() || chunk.bytes.len() > o.snapshots.storage.max_chunk_bytes {
        return Err(SnapshotError::InvalidInput(
            "snapshot chunk size is outside configured bounds".into(),
        ));
    }
    let current = descriptor(conn, id, None)?.ok_or(SnapshotError::NotFound { id })?;
    if let Some(existing)=conn.query_row("SELECT CASE WHEN typeof(bytes)='blob' AND octet_length(bytes)<=?3 THEN bytes END FROM snapshot_chunks WHERE snapshot_id=?1 AND offset=?2",params![id.0.as_slice(),be(chunk.offset).as_slice(),o.snapshots.storage.max_chunk_bytes as i64],|r|r.get::<_,Option<Vec<u8>>>(0)).optional().map_err(|e|corrupt("read snapshot chunk retry",e))? {
        let existing=existing.ok_or_else(||SnapshotError::CorruptStorage("stored snapshot chunk is invalid".into()))?;
        return if existing==chunk.bytes.as_bytes(){Ok(current)}else{Err(SnapshotError::OperationConflict{id})};
    }
    if current.state != SnapshotUploadState::Uploading {
        return Err(SnapshotError::IncompleteUpload { id });
    }
    if chunk.offset != current.accepted_bytes {
        return Err(SnapshotError::InvalidInput(
            "snapshot chunks must be contiguous".into(),
        ));
    }
    let next = chunk
        .offset
        .checked_add(chunk.bytes.len() as u64)
        .ok_or(SnapshotError::CapacityExceeded)?;
    if next > current.descriptor.content_bytes {
        return Err(SnapshotError::InvalidInput(
            "snapshot chunk exceeds declared content length".into(),
        ));
    }
    let c = read_counters(conn)?;
    let s = &o.snapshots.storage;
    if c.chunk_count >= s.max_chunks as u64
        || c.chunk_count >= i64::MAX as u64
        || c.chunk_metadata_bytes
            .checked_add(SNAPSHOT_CHUNK_ENVELOPE_BYTES as u64)
            .is_none_or(|v| v > s.max_chunk_metadata_bytes as u64 || v > i64::MAX as u64)
    {
        return Err(SnapshotError::CapacityExceeded);
    }
    let tx = conn
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .map_err(|e| write("begin chunk", e))?;
    tx.execute(
        "INSERT INTO snapshot_chunks VALUES(?1,?2,?3)",
        params![
            id.0.as_slice(),
            be(chunk.offset).as_slice(),
            chunk.bytes.as_bytes()
        ],
    )
    .map_err(|e| write("insert chunk", e))?;
    tx.execute(
        "UPDATE snapshots SET accepted_bytes=?1 WHERE snapshot_id=?2",
        params![be(next).as_slice(), id.0.as_slice()],
    )
    .map_err(|e| write("advance snapshot", e))?;
    tx.execute("UPDATE snapshot_metadata SET chunk_count=chunk_count+1,chunk_metadata_bytes=chunk_metadata_bytes+64 WHERE singleton=1",[]).map_err(|e|write("charge chunk",e))?;
    reject_before_snapshot_commit(failure)?;
    tx.commit().map_err(|error| {
        if error.sqlite_error_code() == Some(rusqlite::ErrorCode::DiskFull) {
            SnapshotError::CapacityExceeded
        } else {
            SnapshotError::ChunkUnknown {
                id,
                chunk: Box::new(unknown.clone()),
            }
        }
    })?;
    if lose_snapshot_acknowledgement(failure) {
        return Err(SnapshotError::ChunkUnknown {
            id,
            chunk: Box::new(unknown),
        });
    }
    status(conn, id, &SqliteSnapshotState::new())
}

fn status(
    conn: &Connection,
    id: SnapshotId,
    state: &SqliteSnapshotState,
) -> SnapshotResult<SnapshotUploadProgress> {
    descriptor(conn, id, state.verifications.get(&id).map(|v| v.next))?
        .ok_or(SnapshotError::NotFound { id })
}

fn verify(
    conn: &mut Connection,
    id: SnapshotId,
    l: VerificationLimits,
    o: &SqliteOptions,
    state: &mut SqliteSnapshotState,
) -> SnapshotResult<SnapshotUploadProgress> {
    let v = &o.snapshots.verification;
    if l.max_chunks == 0
        || l.max_bytes == 0
        || l.max_chunks > v.max_chunks_per_step
        || l.max_bytes > v.max_bytes_per_step
    {
        return Err(SnapshotError::InvalidInput(
            "verification step exceeds configured limits".into(),
        ));
    }
    let current = descriptor(conn, id, state.verifications.get(&id).map(|x| x.next))?
        .ok_or(SnapshotError::NotFound { id })?;
    if checksum_failed(conn, id)? {
        return Err(SnapshotError::ChecksumMismatch { id });
    }
    if matches!(
        current.state,
        SnapshotUploadState::Verified | SnapshotUploadState::Published
    ) {
        return Ok(current);
    }
    if current.state == SnapshotUploadState::Aborted {
        return Err(SnapshotError::IncompleteUpload { id });
    }
    if current.accepted_bytes != current.descriptor.content_bytes {
        return Err(SnapshotError::IncompleteUpload { id });
    }
    if !state.verifications.contains_key(&id) {
        if state.verifications.len() >= v.max_active {
            return Err(SnapshotError::CapacityExceeded);
        }
        state.verifications.insert(
            id,
            Verification {
                next: 0,
                hasher: Sha256::new(),
            },
        );
        conn.execute(
            "UPDATE snapshots SET state=1,verified_bytes=?1 WHERE snapshot_id=?2",
            params![be(0).as_slice(), id.0.as_slice()],
        )
        .map_err(|e| write("start verification", e))?;
    }
    let mut chunks = 0;
    let mut bytes = 0;
    while chunks < l.max_chunks && bytes < l.max_bytes {
        let next = state.verifications[&id].next;
        if next == current.descriptor.content_bytes {
            break;
        }
        let (start, data) = chunk_at(conn, id, next, o.snapshots.storage.max_chunk_bytes)?;
        let inside = (next - start) as usize;
        if inside >= data.len() {
            return Err(SnapshotError::CorruptStorage(
                "snapshot has a byte gap".into(),
            ));
        }
        let take = (data.len() - inside).min(l.max_bytes - bytes);
        let verification = state.verifications.get_mut(&id).unwrap();
        verification.hasher.update(&data[inside..inside + take]);
        verification.next = verification
            .next
            .checked_add(take as u64)
            .ok_or(SnapshotError::CapacityExceeded)?;
        bytes += take;
        chunks += 1;
    }
    let next = state.verifications[&id].next;
    if next == current.descriptor.content_bytes {
        let verification = state.verifications.remove(&id).unwrap();
        let actual: [u8; 32] = verification.hasher.finalize().into();
        if actual != current.descriptor.digest.0 {
            conn.execute(
                "UPDATE snapshots SET checksum_failed=1,verified_bytes=?1 WHERE snapshot_id=?2",
                params![be(0).as_slice(), id.0.as_slice()],
            )
            .map_err(|e| write("record checksum mismatch", e))?;
            return Err(SnapshotError::ChecksumMismatch { id });
        }
        conn.execute(
            "UPDATE snapshots SET state=2,verified_bytes=content_bytes WHERE snapshot_id=?1",
            [id.0.as_slice()],
        )
        .map_err(|e| write("finish verification", e))?;
    }
    status(conn, id, state)
}

fn publish(
    conn: &mut Connection,
    id: SnapshotId,
    o: &SqliteOptions,
    state: &mut SqliteSnapshotState,
    failure: &mut Option<SqliteFailureInjection>,
) -> SnapshotResult<SnapshotDescriptor> {
    let p = status(conn, id, state)?;
    if p.state == SnapshotUploadState::Published {
        return Ok(p.descriptor);
    }
    if p.state != SnapshotUploadState::Verified {
        return Err(SnapshotError::IncompleteUpload { id });
    }
    let bounds = exact_stream_bounds_optional(conn, &p.descriptor.covered.stream)?
        .map(|(_, floor, tail)| (floor, tail))
        .ok_or_else(|| SnapshotError::MissingHistory {
            floor: Box::new(p.descriptor.covered.clone()),
        })?;
    if p.descriptor.covered.offset < bounds.0 {
        return Err(SnapshotError::MissingHistory {
            floor: Box::new(Cursor::new(p.descriptor.covered.stream.clone(), bounds.0)),
        });
    }
    if p.descriptor.covered.offset > bounds.1 {
        return Err(SnapshotError::CursorAhead {
            tail: Box::new(Cursor::new(p.descriptor.covered.stream.clone(), bounds.1)),
        });
    }
    let c = read_counters(conn)?;
    let s = &o.snapshots.storage;
    if c.published_count >= s.max_snapshots as u64
        || c.published_count >= i64::MAX as u64
        || c.published_bytes
            .checked_add(p.descriptor.content_bytes)
            .is_none_or(|v| v > s.max_published_bytes || v > i64::MAX as u64)
    {
        return Err(SnapshotError::CapacityExceeded);
    }
    let tx = conn
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .map_err(|e| write("begin publication", e))?;
    tx.execute(
        "UPDATE snapshots SET state=3 WHERE snapshot_id=?1 AND state=2",
        [id.0.as_slice()],
    )
    .map_err(|e| write("publish snapshot", e))?;
    tx.execute("UPDATE snapshot_metadata SET staged_count=staged_count-1,staged_bytes=staged_bytes-?1,published_count=published_count+1,published_bytes=published_bytes+?1 WHERE singleton=1",[i64u(p.descriptor.content_bytes)?]).map_err(|e|write("move snapshot quota",e))?;
    let unknown = p.descriptor.clone();
    reject_before_snapshot_commit(failure)?;
    tx.commit().map_err(|error| {
        if error.sqlite_error_code() == Some(rusqlite::ErrorCode::DiskFull) {
            SnapshotError::CapacityExceeded
        } else {
            SnapshotError::PublicationUnknown {
                descriptor: Box::new(unknown.clone()),
            }
        }
    })?;
    if lose_snapshot_acknowledgement(failure) {
        return Err(SnapshotError::PublicationUnknown {
            descriptor: Box::new(unknown),
        });
    }
    Ok(p.descriptor)
}

fn abort(
    conn: &mut Connection,
    id: SnapshotId,
    o: &SqliteOptions,
    state: &mut SqliteSnapshotState,
    failure: &mut Option<SqliteFailureInjection>,
) -> SnapshotResult<SnapshotAbortReceipt> {
    let p = status(conn, id, state)?;
    if p.state == SnapshotUploadState::Aborted {
        return Ok(SnapshotAbortReceipt {
            id,
            already_aborted: true,
        });
    }
    if p.state == SnapshotUploadState::Published {
        return Err(SnapshotError::OperationConflict { id });
    }
    let charge = p
        .descriptor
        .accounted_bytes()
        .ok_or(SnapshotError::CapacityExceeded)?;
    let c = read_counters(conn)?;
    let s = &o.snapshots.storage;
    if c.receipt_count >= s.max_receipts as u64
        || c.receipt_count >= i64::MAX as u64
        || c.receipt_bytes
            .checked_add(charge as u64)
            .is_none_or(|v| v > s.max_receipt_bytes as u64 || v > i64::MAX as u64)
    {
        return Err(SnapshotError::CapacityExceeded);
    }
    let tx = conn
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .map_err(|e| write("begin abort", e))?;
    tx.execute(
        "UPDATE snapshots SET state=4,receipt_reserved=1 WHERE snapshot_id=?1",
        [id.0.as_slice()],
    )
    .map_err(|e| write("abort snapshot", e))?;
    tx.execute("UPDATE snapshot_metadata SET receipt_count=receipt_count+1,receipt_bytes=receipt_bytes+?1 WHERE singleton=1",[charge as i64]).map_err(|e|write("reserve abort receipt",e))?;
    reject_before_snapshot_commit(failure)?;
    tx.commit().map_err(|error| {
        if error.sqlite_error_code() == Some(rusqlite::ErrorCode::DiskFull) {
            SnapshotError::CapacityExceeded
        } else {
            SnapshotError::AbortUnknown { id }
        }
    })?;
    if lose_snapshot_acknowledgement(failure) {
        return Err(SnapshotError::AbortUnknown { id });
    }
    state.verifications.remove(&id);
    Ok(SnapshotAbortReceipt {
        id,
        already_aborted: false,
    })
}

fn cleanup(
    conn: &mut Connection,
    l: SnapshotCleanupLimits,
    o: &SqliteOptions,
) -> SnapshotResult<SnapshotCleanupProgress> {
    if l.max_rows == 0
        || l.max_bytes == 0
        || l.max_rows > o.snapshots.cleanup.max_rows
        || l.max_bytes > o.snapshots.cleanup.max_bytes
    {
        return Err(SnapshotError::InvalidInput(
            "snapshot cleanup exceeds configured limits".into(),
        ));
    }
    let tx = conn
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .map_err(|error| write("begin snapshot cleanup", error))?;
    let id=tx.query_row("SELECT CASE WHEN typeof(snapshot_id)='blob' AND length(snapshot_id)=16 THEN snapshot_id END FROM snapshots WHERE state=4 AND cleaned=0 ORDER BY snapshot_id LIMIT 1",[],|r|r.get::<_,Option<Vec<u8>>>(0)).optional().map_err(|e|corrupt("select aborted snapshot",e))?;
    let Some(id) = id else {
        tx.rollback()
            .map_err(|error| write("finish empty snapshot cleanup", error))?;
        return Ok(SnapshotCleanupProgress {
            removed_snapshots: 0,
            removed_chunks: 0,
            removed_bytes: 0,
            remaining: false,
        });
    };
    let id = SnapshotId(arr16(&id.ok_or_else(|| {
        SnapshotError::CorruptStorage("snapshot identifier is malformed".into())
    })?)?);
    let mut removed_chunks = 0;
    let mut removed_bytes: usize = 0;
    while removed_chunks < l.max_rows {
        let row=tx.query_row("SELECT CASE WHEN typeof(offset)='blob' AND length(offset)=8 THEN offset END,CASE WHEN typeof(bytes)='blob' THEN octet_length(bytes) END FROM snapshot_chunks WHERE snapshot_id=?1 ORDER BY offset LIMIT 1",[id.0.as_slice()],|r|Ok((r.get::<_,Option<Vec<u8>>>(0)?,r.get::<_,Option<i64>>(1)?))).optional().map_err(|e|corrupt("select cleanup chunk",e))?;
        let Some((offset, len)) = row else { break };
        let offset = offset.ok_or_else(|| {
            SnapshotError::CorruptStorage("snapshot chunk offset is malformed".into())
        })?;
        let len = len.ok_or_else(|| {
            SnapshotError::CorruptStorage("snapshot chunk bytes are malformed".into())
        })?;
        let charge = usize::try_from(len)
            .ok()
            .and_then(|v| v.checked_add(SNAPSHOT_CHUNK_ENVELOPE_BYTES))
            .ok_or_else(|| {
                SnapshotError::CorruptStorage("snapshot chunk charge is invalid".into())
            })?;
        if removed_bytes
            .checked_add(charge)
            .is_none_or(|v| v > l.max_bytes)
        {
            if removed_chunks == 0 {
                return Err(SnapshotError::CapacityExceeded);
            }
            break;
        }
        tx.execute(
            "DELETE FROM snapshot_chunks WHERE snapshot_id=?1 AND offset=?2",
            params![id.0.as_slice(), offset],
        )
        .map_err(|e| write("delete snapshot chunk", e))?;
        tx.execute("UPDATE snapshot_metadata SET chunk_count=chunk_count-1,chunk_metadata_bytes=chunk_metadata_bytes-64 WHERE singleton=1 AND chunk_count>=1 AND chunk_metadata_bytes>=64",[]).map_err(|e|write("release chunk charge",e))?;
        removed_chunks += 1;
        removed_bytes += charge;
    }
    let remaining_chunks: bool = tx
        .query_row(
            "SELECT EXISTS(SELECT 1 FROM snapshot_chunks WHERE snapshot_id=?1)",
            [id.0.as_slice()],
            |r| r.get(0),
        )
        .map_err(|e| corrupt("check cleanup chunks", e))?;
    let mut removed_snapshots = 0;
    if !remaining_chunks && removed_chunks < l.max_rows {
        let content = descriptor(&tx, id, None)?
            .ok_or(SnapshotError::NotFound { id })?
            .descriptor
            .content_bytes;
        tx.execute("UPDATE snapshots SET cleaned=1,accepted_bytes=?1,verified_bytes=?1 WHERE snapshot_id=?2",params![be(0).as_slice(),id.0.as_slice()]).map_err(|e|write("finish cleanup",e))?;
        let changed = tx.execute("UPDATE snapshot_metadata SET staged_count=staged_count-1,staged_bytes=staged_bytes-?1 WHERE singleton=1 AND staged_count>=1 AND staged_bytes>=?1",[i64u(content)?]).map_err(|e|write("release staging quota",e))?;
        if changed != 1 {
            return Err(SnapshotError::CorruptStorage(
                "snapshot staging counters underflow".into(),
            ));
        }
        removed_snapshots = 1;
    }
    let remaining: bool = tx
        .query_row(
            "SELECT EXISTS(SELECT 1 FROM snapshots WHERE state=4 AND cleaned=0)",
            [],
            |r| r.get(0),
        )
        .map_err(|e| corrupt("check cleanup work", e))?;
    let progress = SnapshotCleanupProgress {
        removed_snapshots,
        removed_chunks,
        removed_bytes,
        remaining,
    };
    tx.commit()
        .map_err(|error| write("commit snapshot cleanup", error))?;
    Ok(progress)
}

fn list_uploads(
    conn: &Connection,
    after: Option<SnapshotId>,
    l: PageLimits,
    o: &SqliteOptions,
    state: &SqliteSnapshotState,
) -> SnapshotResult<SnapshotUploadPage> {
    let sql_limit = validate_page(l, o.snapshots.storage.max_list_page)?;
    let mut stmt=conn.prepare("SELECT CASE WHEN typeof(snapshot_id)='blob' AND length(snapshot_id)=16 THEN snapshot_id END FROM snapshots WHERE state<>3 AND cleaned=0 AND (?1 IS NULL OR snapshot_id>?1) ORDER BY snapshot_id LIMIT ?2").map_err(|e|corrupt("prepare upload list",e))?;
    let after = after.map(|v| v.0);
    let rows = stmt
        .query_map(
            params![after.as_ref().map(|v| v.as_slice()), sql_limit],
            |r| r.get::<_, Option<Vec<u8>>>(0),
        )
        .map_err(|e| corrupt("query upload list", e))?;
    let mut entries = Vec::new();
    let mut bytes: usize = 0;
    let mut more = false;
    for row in rows {
        let raw = row
            .map_err(|e| corrupt("read upload id", e))?
            .ok_or_else(|| {
                SnapshotError::CorruptStorage("snapshot identifier is malformed".into())
            })?;
        let id = SnapshotId(arr16(&raw)?);
        let p = status(conn, id, state)?;
        let charge = p
            .descriptor
            .accounted_bytes()
            .and_then(|charge| charge.checked_add(64))
            .ok_or(SnapshotError::CapacityExceeded)?;
        if entries.len() == l.max_records
            || bytes.checked_add(charge).is_none_or(|v| v > l.max_bytes)
        {
            if entries.is_empty() {
                return Err(SnapshotError::CapacityExceeded);
            }
            more = true;
            break;
        }
        bytes += charge;
        entries.push(p);
    }
    let next_after = if more {
        entries.last().map(|v| v.descriptor.id)
    } else {
        None
    };
    Ok(SnapshotUploadPage {
        entries,
        next_after,
        complete: !more,
    })
}

fn list(
    conn: &Connection,
    stream: &StreamKey,
    after: Option<SnapshotContinuation>,
    l: PageLimits,
    o: &SqliteOptions,
) -> SnapshotResult<SnapshotPage> {
    let sql_limit = validate_page(l, o.snapshots.storage.max_list_page)?;
    stream_bounds(conn, stream)?;
    if after.as_ref().is_some_and(|a| a.covered.stream != *stream) {
        return Err(SnapshotError::InvalidInput(
            "snapshot continuation stream differs".into(),
        ));
    }
    let after = after.map(|a| (be(a.covered.offset), a.id.0));
    let mut stmt=conn.prepare("SELECT CASE WHEN typeof(snapshot_id)='blob' AND length(snapshot_id)=16 THEN snapshot_id END FROM snapshots WHERE public_id=?1 AND incarnation=?2 AND state=3 AND (?3 IS NULL OR covered>?3 OR (covered=?3 AND snapshot_id>?4)) ORDER BY covered,snapshot_id LIMIT ?5").map_err(|e|corrupt("prepare snapshot list",e))?;
    let rows = stmt
        .query_map(
            params![
                stream.id.as_str(),
                stream.incarnation.0.as_slice(),
                after.as_ref().map(|v| v.0.as_slice()),
                after.as_ref().map(|v| v.1.as_slice()),
                sql_limit
            ],
            |r| r.get::<_, Option<Vec<u8>>>(0),
        )
        .map_err(|e| corrupt("query snapshot list", e))?;
    let mut entries = Vec::new();
    let mut bytes: usize = 0;
    let mut more = false;
    for row in rows {
        let raw = row
            .map_err(|e| corrupt("read snapshot id", e))?
            .ok_or_else(|| {
                SnapshotError::CorruptStorage("snapshot identifier is malformed".into())
            })?;
        let id = SnapshotId(arr16(&raw)?);
        let d = descriptor(conn, id, None)?
            .ok_or(SnapshotError::NotFound { id })?
            .descriptor;
        let charge = d.accounted_bytes().ok_or(SnapshotError::CapacityExceeded)?;
        if entries.len() == l.max_records
            || bytes.checked_add(charge).is_none_or(|v| v > l.max_bytes)
        {
            if entries.is_empty() {
                return Err(SnapshotError::CapacityExceeded);
            }
            more = true;
            break;
        }
        bytes += charge;
        entries.push(d);
    }
    let next_after = if more {
        entries.last().map(|d| SnapshotContinuation {
            covered: d.covered.clone(),
            id: d.id,
        })
    } else {
        None
    };
    Ok(SnapshotPage {
        entries,
        next_after,
        complete: !more,
    })
}

fn acquire(
    conn: &Connection,
    id: SnapshotId,
    lifetime: Duration,
    o: &SqliteOptions,
    state: &mut SqliteSnapshotState,
) -> SnapshotResult<RecoveryPlan> {
    if lifetime.is_zero() || lifetime > o.snapshots.recovery.max_lifetime {
        return Err(SnapshotError::InvalidInput(
            "recovery lifetime exceeds configured limit".into(),
        ));
    }
    let now = o.snapshot_clock.now();
    let expires = now
        .checked_add(lifetime)
        .ok_or_else(|| SnapshotError::InvalidInput("recovery lifetime overflows clock".into()))?;
    state.leases.retain(|_, v| v.expires.0 > now.0);
    if state.leases.len() >= o.snapshots.recovery.max_leases {
        return Err(SnapshotError::CapacityExceeded);
    }
    let p = descriptor(conn, id, None)?
        .filter(|p| p.state == SnapshotUploadState::Published)
        .ok_or(SnapshotError::NotFound { id })?;
    let bounds = exact_stream_bounds_optional(conn, &p.descriptor.covered.stream)?
        .map(|(_, floor, tail)| (floor, tail))
        .ok_or_else(|| SnapshotError::MissingHistory {
            floor: Box::new(p.descriptor.covered.clone()),
        })?;
    if p.descriptor.covered.offset < bounds.0 {
        return Err(SnapshotError::MissingHistory {
            floor: Box::new(Cursor::new(p.descriptor.covered.stream.clone(), bounds.0)),
        });
    }
    let lease = loop {
        let x = RecoveryLeaseId(*uuid::Uuid::new_v4().as_bytes());
        if !state.leases.contains_key(&x) {
            break x;
        }
    };
    state.leases.insert(
        lease,
        Lease {
            snapshot: p.descriptor.clone(),
            through: bounds.1,
            expires,
        },
    );
    Ok(RecoveryPlan {
        lease,
        snapshot: p.descriptor.clone(),
        through: Cursor::new(p.descriptor.covered.stream, bounds.1),
    })
}

fn lease<'a>(
    id: RecoveryLeaseId,
    o: &SqliteOptions,
    state: &'a SqliteSnapshotState,
) -> SnapshotResult<&'a Lease> {
    state
        .leases
        .get(&id)
        .filter(|v| v.expires.0 > o.snapshot_clock.now().0)
        .ok_or(SnapshotError::ExpiredProtection { lease: id })
}
fn read_bytes(
    conn: &Connection,
    id: RecoveryLeaseId,
    offset: u64,
    max: usize,
    o: &SqliteOptions,
    state: &SqliteSnapshotState,
) -> SnapshotResult<SnapshotBytePage> {
    if max == 0 || max > o.snapshots.recovery.max_chunk_bytes {
        return Err(SnapshotError::InvalidInput(
            "snapshot read exceeds configured byte limit".into(),
        ));
    }
    let l = lease(id, o, state)?;
    let total = l.snapshot.content_bytes;
    if offset > total {
        return Err(SnapshotError::InvalidInput(
            "snapshot byte offset is beyond content".into(),
        ));
    }
    if offset == total {
        return Ok(SnapshotBytePage {
            snapshot: l.snapshot.id,
            offset,
            bytes: Payload::copy_from_slice(&[]),
            next_offset: offset,
            complete: true,
        });
    }
    let mut out = Vec::with_capacity(max);
    let mut next = offset;
    while out.len() < max && next < total {
        let (start, data) = chunk_at(
            conn,
            l.snapshot.id,
            next,
            o.snapshots.storage.max_chunk_bytes,
        )?;
        let inside = (next - start) as usize;
        if inside >= data.len() {
            return Err(SnapshotError::CorruptStorage(
                "snapshot has a byte gap".into(),
            ));
        }
        let take = (data.len() - inside).min(max - out.len());
        out.extend_from_slice(&data[inside..inside + take]);
        next += take as u64;
    }
    Ok(SnapshotBytePage {
        snapshot: l.snapshot.id,
        offset,
        bytes: Payload::copy_from_slice(&out),
        next_offset: next,
        complete: next == total,
    })
}
fn read_recovery(
    conn: &Connection,
    id: RecoveryLeaseId,
    after: u64,
    l: PageLimits,
    o: &SqliteOptions,
    state: &SqliteSnapshotState,
) -> SnapshotResult<Page> {
    validate_page(l, o.snapshots.recovery.max_page)?;
    let lease = lease(id, o, state)?;
    if after < lease.snapshot.covered.offset || after > lease.through {
        return Err(SnapshotError::InvalidInput(
            "recovery cursor is outside protected range".into(),
        ));
    }
    let (stream_key, floor, tail) = exact_stream_bounds(conn, &lease.snapshot.covered.stream)?;
    if tail < lease.through {
        return Err(SnapshotError::CorruptStorage(
            "protected recovery tail is no longer present".into(),
        ));
    }
    read_page_exact(
        conn,
        &lease.snapshot.covered.stream,
        StoredRange {
            key: stream_key,
            floor,
            tail,
        },
        after,
        lease.through,
        l,
        o.max_record_bytes,
    )
    .map_err(map_store)
}

#[derive(Clone, Copy)]
struct Counters {
    staged_count: u64,
    staged_bytes: u64,
    published_count: u64,
    published_bytes: u64,
    chunk_count: u64,
    chunk_metadata_bytes: u64,
    descriptor_bytes: u64,
    receipt_count: u64,
    receipt_bytes: u64,
}
fn read_counters(conn: &Connection) -> SnapshotResult<Counters> {
    let v:(i64,i64,i64,i64,i64,i64,i64,i64,i64)=conn.query_row("SELECT staged_count,staged_bytes,published_count,published_bytes,chunk_count,chunk_metadata_bytes,descriptor_metadata_bytes,receipt_count,receipt_bytes FROM snapshot_metadata WHERE singleton=1",[],|r|Ok((r.get(0)?,r.get(1)?,r.get(2)?,r.get(3)?,r.get(4)?,r.get(5)?,r.get(6)?,r.get(7)?,r.get(8)?))).map_err(|e|corrupt("read snapshot counters",e))?;
    let a = [v.0, v.1, v.2, v.3, v.4, v.5, v.6, v.7, v.8];
    if a.iter().any(|x| *x < 0) {
        return Err(SnapshotError::CorruptStorage(
            "snapshot counter is negative".into(),
        ));
    }
    Ok(Counters {
        staged_count: a[0] as u64,
        staged_bytes: a[1] as u64,
        published_count: a[2] as u64,
        published_bytes: a[3] as u64,
        chunk_count: a[4] as u64,
        chunk_metadata_bytes: a[5] as u64,
        descriptor_bytes: a[6] as u64,
        receipt_count: a[7] as u64,
        receipt_bytes: a[8] as u64,
    })
}

fn descriptor(
    conn: &Connection,
    id: SnapshotId,
    live_verified: Option<u64>,
) -> SnapshotResult<Option<SnapshotUploadProgress>> {
    conn.query_row("SELECT CASE WHEN typeof(public_id)='text' AND octet_length(public_id) BETWEEN 1 AND 256 THEN public_id END,CASE WHEN typeof(incarnation)='blob' AND length(incarnation)=16 THEN incarnation END,CASE WHEN typeof(covered)='blob' AND length(covered)=8 THEN covered END,CASE WHEN typeof(schema_id)='text' AND octet_length(schema_id) BETWEEN 1 AND 256 THEN schema_id END,CASE WHEN typeof(schema_version)='integer' THEN schema_version END,CASE WHEN typeof(content_bytes)='blob' AND length(content_bytes)=8 THEN content_bytes END,CASE WHEN typeof(digest)='blob' AND length(digest)=32 THEN digest END,CASE WHEN typeof(state)='integer' THEN state END,CASE WHEN typeof(accepted_bytes)='blob' AND length(accepted_bytes)=8 THEN accepted_bytes END,CASE WHEN typeof(verified_bytes)='blob' AND length(verified_bytes)=8 THEN verified_bytes END,CASE WHEN typeof(checksum_failed)='integer' AND checksum_failed IN (0,1) THEN checksum_failed END,CASE WHEN typeof(cleaned)='integer' AND cleaned IN (0,1) THEN cleaned END,CASE WHEN typeof(descriptor_charge)='integer' AND descriptor_charge>=0 THEN descriptor_charge END,CASE WHEN typeof(receipt_reserved)='integer' AND receipt_reserved IN (0,1) THEN receipt_reserved END FROM snapshots WHERE snapshot_id=?1",[id.0.as_slice()],|r|decode_progress(id,r,live_verified)).optional().map_err(|e|corrupt("read snapshot descriptor",e))?.transpose()
}
fn decode_progress(
    id: SnapshotId,
    r: &rusqlite::Row<'_>,
    live: Option<u64>,
) -> rusqlite::Result<SnapshotResult<SnapshotUploadProgress>> {
    let invalid = || SnapshotError::CorruptStorage("snapshot descriptor is malformed".into());
    let public: Option<String> = r.get(0)?;
    let inc: Option<Vec<u8>> = r.get(1)?;
    let covered: Option<Vec<u8>> = r.get(2)?;
    let schema: Option<String> = r.get(3)?;
    let version: Option<u32> = r.get(4)?;
    let content: Option<Vec<u8>> = r.get(5)?;
    let digest: Option<Vec<u8>> = r.get(6)?;
    let state: Option<i64> = r.get(7)?;
    let accepted: Option<Vec<u8>> = r.get(8)?;
    let verified: Option<Vec<u8>> = r.get(9)?;
    let checksum_failed: Option<i64> = r.get(10)?;
    let cleaned: Option<i64> = r.get(11)?;
    let descriptor_charge: Option<i64> = r.get(12)?;
    let receipt_reserved: Option<i64> = r.get(13)?;
    Ok((|| {
        let stream = StreamKey {
            id: StreamId::new(public.ok_or_else(invalid)?).map_err(|_| invalid())?,
            incarnation: IncarnationId(arr16(&inc.ok_or_else(invalid)?)?),
        };
        let descriptor = SnapshotDescriptor {
            id,
            covered: Cursor::new(stream, decode_u64(&covered.ok_or_else(invalid)?)?),
            schema: SchemaRef {
                id: SchemaId::new(schema.ok_or_else(invalid)?).map_err(|_| invalid())?,
                version: version.ok_or_else(invalid)?,
            },
            content_bytes: decode_u64(&content.ok_or_else(invalid)?)?,
            digest: SnapshotDigest(arr32(&digest.ok_or_else(invalid)?)?),
        };
        let state = match state.ok_or_else(invalid)? {
            UPLOADING => SnapshotUploadState::Uploading,
            VERIFYING => SnapshotUploadState::Verifying,
            VERIFIED => SnapshotUploadState::Verified,
            PUBLISHED => SnapshotUploadState::Published,
            ABORTED => SnapshotUploadState::Aborted,
            _ => return Err(invalid()),
        };
        let accepted_bytes = decode_u64(&accepted.ok_or_else(invalid)?)?;
        let verified_bytes = live.unwrap_or(decode_u64(&verified.ok_or_else(invalid)?)?);
        let checksum_failed = checksum_failed.ok_or_else(invalid)?;
        let cleaned = cleaned.ok_or_else(invalid)?;
        let receipt_reserved = receipt_reserved.ok_or_else(invalid)?;
        let expected_charge = descriptor.accounted_bytes().ok_or_else(invalid)?;
        if descriptor_charge.ok_or_else(invalid)? != expected_charge as i64
            || accepted_bytes > descriptor.content_bytes
            || verified_bytes > accepted_bytes
            || matches!(
                state,
                SnapshotUploadState::Verified | SnapshotUploadState::Published
            ) && (accepted_bytes != descriptor.content_bytes
                || verified_bytes != descriptor.content_bytes
                || checksum_failed != 0
                || cleaned != 0)
            || state == SnapshotUploadState::Uploading && verified_bytes != 0
            || state == SnapshotUploadState::Aborted && receipt_reserved != 1
            || cleaned == 1
                && (state != SnapshotUploadState::Aborted
                    || accepted_bytes != 0
                    || verified_bytes != 0)
        {
            return Err(invalid());
        }
        Ok(SnapshotUploadProgress {
            descriptor,
            accepted_bytes,
            verified_bytes,
            state,
        })
    })())
}
fn checksum_failed(conn: &Connection, id: SnapshotId) -> SnapshotResult<bool> {
    conn.query_row(
        "SELECT checksum_failed FROM snapshots WHERE snapshot_id=?1",
        [id.0.as_slice()],
        |r| r.get(0),
    )
    .map_err(|e| corrupt("read checksum state", e))
}
fn chunk_at(
    conn: &Connection,
    id: SnapshotId,
    offset: u64,
    max: usize,
) -> SnapshotResult<(u64, Vec<u8>)> {
    let row=conn.query_row("SELECT CASE WHEN typeof(offset)='blob' AND length(offset)=8 THEN offset END,CASE WHEN typeof(bytes)='blob' AND octet_length(bytes)<=?3 THEN bytes END FROM snapshot_chunks WHERE snapshot_id=?1 AND offset<=?2 ORDER BY offset DESC LIMIT 1",params![id.0.as_slice(),be(offset).as_slice(),max as i64],|r|Ok((r.get::<_,Option<Vec<u8>>>(0)?,r.get::<_,Option<Vec<u8>>>(1)?))).optional().map_err(|e|corrupt("seek snapshot chunk",e))?.ok_or_else(||SnapshotError::CorruptStorage("snapshot has a byte gap".into()))?;
    Ok((
        decode_u64(
            &row.0
                .ok_or_else(|| SnapshotError::CorruptStorage("chunk offset is malformed".into()))?,
        )?,
        row.1
            .ok_or_else(|| SnapshotError::CorruptStorage("snapshot chunk is malformed".into()))?,
    ))
}
fn stream_bounds(conn: &Connection, key: &StreamKey) -> SnapshotResult<(u64, u64)> {
    let row=conn.query_row("SELECT CASE WHEN typeof(s.incarnation)='blob' AND length(s.incarnation)=16 THEN s.incarnation END,CASE WHEN typeof(s.floor)='blob' AND length(s.floor)=8 THEN s.floor END,CASE WHEN typeof(s.tail)='blob' AND length(s.tail)=8 THEN s.tail END,CASE WHEN typeof(n.latest_incarnation)='blob' AND length(n.latest_incarnation)=16 THEN n.latest_incarnation END,n.active_stream_key,s.stream_key FROM event_stream_names n LEFT JOIN event_streams s ON s.public_id=n.public_id AND s.incarnation=?2 WHERE n.public_id=?1",params![key.id.as_str(),key.incarnation.0.as_slice()],|r|Ok((r.get::<_,Option<Vec<u8>>>(0)?,r.get::<_,Option<Vec<u8>>>(1)?,r.get::<_,Option<Vec<u8>>>(2)?,r.get::<_,Option<Vec<u8>>>(3)?,r.get::<_,Option<i64>>(4)?,r.get::<_,Option<i64>>(5)?))).optional().map_err(|e|corrupt("read snapshot stream",e))?;
    let Some((inc, floor, tail, latest, active, keyrow)) = row else {
        return Err(SnapshotError::StaleIncarnation { current: None });
    };
    if inc.is_none() || active != keyrow {
        return Err(SnapshotError::StaleIncarnation {
            current: Some(Box::new(StreamKey {
                id: key.id.clone(),
                incarnation: IncarnationId(arr16(&latest.ok_or_else(|| {
                    SnapshotError::CorruptStorage("latest stream incarnation is malformed".into())
                })?)?),
            })),
        });
    }
    Ok((
        decode_u64(
            &floor
                .ok_or_else(|| SnapshotError::CorruptStorage("stream floor is malformed".into()))?,
        )?,
        decode_u64(
            &tail
                .ok_or_else(|| SnapshotError::CorruptStorage("stream tail is malformed".into()))?,
        )?,
    ))
}

fn exact_stream_bounds(conn: &Connection, key: &StreamKey) -> SnapshotResult<(i64, u64, u64)> {
    exact_stream_bounds_optional(conn, key)?
        .ok_or_else(|| SnapshotError::CorruptStorage("protected recovery stream is missing".into()))
}

fn exact_stream_bounds_optional(
    conn: &Connection,
    key: &StreamKey,
) -> SnapshotResult<Option<(i64, u64, u64)>> {
    let row = conn
        .query_row(
            "SELECT stream_key,
                CASE WHEN typeof(floor)='blob' AND length(floor)=8 THEN floor END,
                CASE WHEN typeof(tail)='blob' AND length(tail)=8 THEN tail END
         FROM event_streams WHERE public_id=?1 AND incarnation=?2",
            params![key.id.as_str(), key.incarnation.0.as_slice()],
            |row| {
                Ok((
                    row.get::<_, i64>(0)?,
                    row.get::<_, Option<Vec<u8>>>(1)?,
                    row.get::<_, Option<Vec<u8>>>(2)?,
                ))
            },
        )
        .optional()
        .map_err(|error| corrupt("read protected stream bounds", error))?;
    let Some(row) = row else {
        return Ok(None);
    };
    let floor = decode_u64(&row.1.ok_or_else(|| {
        SnapshotError::CorruptStorage("protected stream floor is malformed".into())
    })?)?;
    let tail = decode_u64(&row.2.ok_or_else(|| {
        SnapshotError::CorruptStorage("protected stream tail is malformed".into())
    })?)?;
    Ok(Some((row.0, floor, tail)))
}
fn validate_page(v: PageLimits, max: PageLimits) -> SnapshotResult<i64> {
    if v.max_records == 0
        || v.max_bytes == 0
        || v.max_records > max.max_records
        || v.max_bytes > max.max_bytes
    {
        return Err(SnapshotError::InvalidInput(
            "snapshot page exceeds configured limits".into(),
        ));
    }
    let query_records = v.max_records.checked_add(1).ok_or_else(|| {
        SnapshotError::InvalidInput("snapshot page record limit overflows".into())
    })?;
    i64::try_from(query_records).map_err(|_| {
        SnapshotError::InvalidInput("snapshot page record limit exceeds SQLite range".into())
    })
}
fn progress(
    descriptor: SnapshotDescriptor,
    accepted: u64,
    verified: u64,
    state: i64,
) -> SnapshotUploadProgress {
    SnapshotUploadProgress {
        descriptor,
        accepted_bytes: accepted,
        verified_bytes: verified,
        state: match state {
            0 => SnapshotUploadState::Uploading,
            1 => SnapshotUploadState::Verifying,
            2 => SnapshotUploadState::Verified,
            3 => SnapshotUploadState::Published,
            _ => SnapshotUploadState::Aborted,
        },
    }
}
fn be(v: u64) -> [u8; 8] {
    v.to_be_bytes()
}
fn decode_u64(v: &[u8]) -> SnapshotResult<u64> {
    Ok(u64::from_be_bytes(v.try_into().map_err(|_| {
        SnapshotError::CorruptStorage("u64 storage is malformed".into())
    })?))
}
fn arr16(v: &[u8]) -> SnapshotResult<[u8; 16]> {
    v.try_into()
        .map_err(|_| SnapshotError::CorruptStorage("16-byte identity is malformed".into()))
}
fn arr32(v: &[u8]) -> SnapshotResult<[u8; 32]> {
    v.try_into()
        .map_err(|_| SnapshotError::CorruptStorage("digest is malformed".into()))
}
fn i64u(v: u64) -> SnapshotResult<i64> {
    i64::try_from(v).map_err(|_| SnapshotError::CapacityExceeded)
}
fn write(a: &str, e: rusqlite::Error) -> SnapshotError {
    if e.sqlite_error_code() == Some(rusqlite::ErrorCode::DiskFull) {
        SnapshotError::CapacityExceeded
    } else {
        SnapshotError::StorageFailure(format!("{a}: {e}"))
    }
}
fn corrupt(a: &str, e: rusqlite::Error) -> SnapshotError {
    SnapshotError::CorruptStorage(format!("{a}: {e}"))
}
fn map_store(e: crate::application::Error) -> SnapshotError {
    match e {
        crate::application::Error::CapacityExceeded => SnapshotError::CapacityExceeded,
        crate::application::Error::CursorAhead { tail } => SnapshotError::CursorAhead {
            tail: Box::new(tail),
        },
        crate::application::Error::HistoryUnavailable { bounds } => SnapshotError::MissingHistory {
            floor: Box::new(bounds.floor),
        },
        crate::application::Error::StoreCorrupt(v) => SnapshotError::CorruptStorage(v),
        v => SnapshotError::StorageFailure(v.to_string()),
    }
}

async fn receive<T>(rx: oneshot::Receiver<SnapshotResult<T>>) -> SnapshotResult<T> {
    rx.await.unwrap_or_else(|_| {
        Err(SnapshotError::StorageFailure(
            "SQLite snapshot worker stopped".into(),
        ))
    })
}

#[async_trait]
impl SnapshotStore for SqliteStore {
    async fn begin_snapshot(
        &self,
        v: SnapshotDescriptor,
    ) -> SnapshotResult<SnapshotUploadProgress> {
        let unknown = v.clone();
        let (tx, rx) = oneshot::channel();
        self.submit(Command::Snapshot(SnapshotCommand::Begin(v, tx)))
            .map_err(map_store)?;
        rx.await.unwrap_or_else(|_| {
            Err(SnapshotError::BeginUnknown {
                descriptor: Box::new(unknown),
            })
        })
    }
    async fn put_snapshot_chunk(
        &self,
        id: SnapshotId,
        v: SnapshotChunk,
    ) -> SnapshotResult<SnapshotUploadProgress> {
        if v.bytes.len() > self.snapshot_config.storage.max_chunk_bytes {
            return Err(SnapshotError::InvalidInput(
                "snapshot chunk size is outside configured bounds".into(),
            ));
        }
        let unknown = v.clone();
        let (tx, rx) = oneshot::channel();
        self.submit(Command::Snapshot(SnapshotCommand::Put(id, v, tx)))
            .map_err(map_store)?;
        rx.await.unwrap_or_else(|_| {
            Err(SnapshotError::ChunkUnknown {
                id,
                chunk: Box::new(unknown),
            })
        })
    }
    async fn snapshot_status(&self, id: SnapshotId) -> SnapshotResult<SnapshotUploadProgress> {
        let (tx, rx) = oneshot::channel();
        self.submit(Command::Snapshot(SnapshotCommand::Status(id, tx)))
            .map_err(map_store)?;
        receive(rx).await
    }
    async fn verify_snapshot_step(
        &self,
        id: SnapshotId,
        l: VerificationLimits,
    ) -> SnapshotResult<SnapshotUploadProgress> {
        let (tx, rx) = oneshot::channel();
        self.submit(Command::Snapshot(SnapshotCommand::Verify(id, l, tx)))
            .map_err(map_store)?;
        receive(rx).await
    }
    async fn publish_snapshot(&self, id: SnapshotId) -> SnapshotResult<SnapshotDescriptor> {
        let descriptor = self.snapshot_status(id).await?.descriptor;
        let (tx, rx) = oneshot::channel();
        self.submit(Command::Snapshot(SnapshotCommand::Publish(
            descriptor.clone(),
            tx,
        )))
        .map_err(map_store)?;
        rx.await.unwrap_or_else(|_| {
            Err(SnapshotError::PublicationUnknown {
                descriptor: Box::new(descriptor),
            })
        })
    }
    async fn abort_snapshot(&self, id: SnapshotId) -> SnapshotResult<SnapshotAbortReceipt> {
        let (tx, rx) = oneshot::channel();
        self.submit(Command::Snapshot(SnapshotCommand::Abort(id, tx)))
            .map_err(map_store)?;
        rx.await.unwrap_or(Err(SnapshotError::AbortUnknown { id }))
    }
    async fn cleanup_snapshot_staging(
        &self,
        l: SnapshotCleanupLimits,
    ) -> SnapshotResult<SnapshotCleanupProgress> {
        let (tx, rx) = oneshot::channel();
        self.submit(Command::Snapshot(SnapshotCommand::Cleanup(l, tx)))
            .map_err(map_store)?;
        receive(rx).await
    }
    async fn list_snapshot_uploads(
        &self,
        a: Option<SnapshotId>,
        l: PageLimits,
    ) -> SnapshotResult<SnapshotUploadPage> {
        let (tx, rx) = oneshot::channel();
        self.submit(Command::Snapshot(SnapshotCommand::ListUploads(a, l, tx)))
            .map_err(map_store)?;
        receive(rx).await
    }
    async fn list_snapshots(
        &self,
        s: &StreamKey,
        a: Option<SnapshotContinuation>,
        l: PageLimits,
    ) -> SnapshotResult<SnapshotPage> {
        let (tx, rx) = oneshot::channel();
        self.submit(Command::Snapshot(SnapshotCommand::List(
            s.clone(),
            a,
            l,
            tx,
        )))
        .map_err(map_store)?;
        receive(rx).await
    }
    async fn acquire_recovery(&self, id: SnapshotId, l: Duration) -> SnapshotResult<RecoveryPlan> {
        let (tx, rx) = oneshot::channel();
        self.submit(Command::Snapshot(SnapshotCommand::Acquire(id, l, tx)))
            .map_err(map_store)?;
        receive(rx).await
    }
    async fn read_snapshot_chunk(
        &self,
        id: RecoveryLeaseId,
        o: u64,
        m: usize,
    ) -> SnapshotResult<SnapshotBytePage> {
        let (tx, rx) = oneshot::channel();
        self.submit(Command::Snapshot(SnapshotCommand::ReadBytes(id, o, m, tx)))
            .map_err(map_store)?;
        receive(rx).await
    }
    async fn read_recovery_page(
        &self,
        id: RecoveryLeaseId,
        a: u64,
        l: PageLimits,
    ) -> SnapshotResult<Page> {
        let (tx, rx) = oneshot::channel();
        self.submit(Command::Snapshot(SnapshotCommand::ReadRecovery(
            id, a, l, tx,
        )))
        .map_err(map_store)?;
        receive(rx).await
    }
    async fn release_recovery(&self, id: RecoveryLeaseId) -> SnapshotResult<RecoveryRelease> {
        let (tx, rx) = oneshot::channel();
        self.submit(Command::Snapshot(SnapshotCommand::Release(id, tx)))
            .map_err(map_store)?;
        receive(rx).await
    }
}
