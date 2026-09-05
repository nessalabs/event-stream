use super::sqlite::{be, decode_offset, stream_row, Command, SqliteOptions, SqliteStore};
use crate::{application::*, domain::*};
use async_trait::async_trait;
use rusqlite::{params, Connection, OptionalExtension, TransactionBehavior};
use sha2::{Digest, Sha256};
use std::{
    collections::{BTreeMap, HashMap},
    sync::Arc,
    time::Duration,
};
use tokio::sync::oneshot;

const RECORD_OVERHEAD: usize = 256;
const RECEIPT_OVERHEAD: usize = 192;
type ReplicaStatusRow = (
    Vec<u8>,
    Vec<u8>,
    Vec<u8>,
    i64,
    Vec<u8>,
    Option<Vec<u8>>,
    i64,
    Option<Vec<u8>>,
);
type SavedAttachRow = (
    i64,
    String,
    Vec<u8>,
    String,
    Vec<u8>,
    Vec<u8>,
    Vec<u8>,
    i64,
    Vec<u8>,
    Vec<u8>,
    i64,
    Vec<u8>,
    Option<Vec<u8>>,
    i64,
    Option<Vec<u8>>,
);

struct BootstrapVerification {
    next_snapshot: u64,
    next_record: u64,
    hasher: Sha256,
}

struct ReplicaReadLease {
    bootstrap: BootstrapId,
    published: PublishedReplicaBootstrap,
    expires_at: DurableTimestampMillis,
}

pub(super) struct SqliteReplicationState {
    verifications: HashMap<BootstrapId, BootstrapVerification>,
    readers: HashMap<ReplicaReadLeaseId, ReplicaReadLease>,
    reader_expiries: BTreeMap<(DurableTimestampMillis, ReplicaReadLeaseId), ()>,
    reader_counts: HashMap<BootstrapId, usize>,
    origin_leases: HashMap<BootstrapId, RecoveryLeaseId>,
}

impl SqliteReplicationState {
    pub(super) fn new() -> Self {
        Self {
            verifications: HashMap::new(),
            readers: HashMap::new(),
            reader_expiries: BTreeMap::new(),
            reader_counts: HashMap::new(),
            origin_leases: HashMap::new(),
        }
    }
}

pub(super) enum ReplicationCommand {
    Origin(oneshot::Sender<ReplicationResult<OriginId>>),
    Attach(
        AttachReplica,
        oneshot::Sender<ReplicationResult<AttachReplicaReceipt>>,
    ),
    Status(
        ReplicaId,
        OriginStream,
        oneshot::Sender<ReplicationResult<ReplicaStatus>>,
    ),
    Prepare(
        PrepareReplicaBatch,
        oneshot::Sender<ReplicationResult<PrepareReplicaBatchReceipt>>,
    ),
    Acknowledge(
        AcknowledgeReplicaBatch,
        oneshot::Sender<ReplicationResult<AcknowledgeReplicaBatchReceipt>>,
    ),
    Detach(
        DetachReplica,
        oneshot::Sender<ReplicationResult<DetachReplicaReceipt>>,
    ),
    DestinationEpoch(oneshot::Sender<ReplicationResult<DestinationEpoch>>),
    Commit(
        ReplicaBatch,
        oneshot::Sender<ReplicationResult<ReplicaReceipt>>,
    ),
    Read(
        ReplicaPosition,
        ReplicaBatchLimits,
        oneshot::Sender<ReplicationResult<ReplicaPage>>,
    ),
    Floor(
        AdvanceReplicaReceiptFloor,
        oneshot::Sender<ReplicationResult<AdvanceReplicaReceiptFloorReceipt>>,
    ),
    BeginOriginBootstrap(
        BeginOriginBootstrap,
        oneshot::Sender<ReplicationResult<BeginOriginBootstrapReceipt>>,
    ),
    AcknowledgeOriginBootstrap(
        AcknowledgeOriginBootstrap,
        oneshot::Sender<ReplicationResult<AcknowledgeOriginBootstrapReceipt>>,
    ),
    CleanupOrigin(
        ReplicaCleanupLimits,
        oneshot::Sender<ReplicationResult<ReplicaCleanupProgress>>,
    ),
    BeginDestinationBootstrap(
        ReplicaBootstrap,
        oneshot::Sender<ReplicationResult<BeginReplicaBootstrapReceipt>>,
    ),
    PutBootstrapChunk(
        ReplicaBootstrapChunk,
        oneshot::Sender<ReplicationResult<ReplicaBootstrapChunkReceipt>>,
    ),
    PutBootstrapBatch(
        ReplicaBootstrapBatch,
        oneshot::Sender<ReplicationResult<ReplicaBootstrapBatchReceipt>>,
    ),
    VerifyBootstrap(
        VerifyReplicaBootstrap,
        oneshot::Sender<ReplicationResult<ReplicaBootstrapVerificationProgress>>,
    ),
    PublishBootstrap(
        PublishReplicaBootstrap,
        oneshot::Sender<ReplicationResult<ReplicaBootstrapReceipt>>,
    ),
    PublishedBootstrap(
        OriginStream,
        oneshot::Sender<ReplicationResult<Option<PublishedReplicaBootstrap>>>,
    ),
    AcquireBootstrapRead(
        OriginStream,
        Duration,
        oneshot::Sender<ReplicationResult<ReplicaBootstrapReadPlan>>,
    ),
    ReadBootstrapBytes(
        ReplicaReadLeaseId,
        u64,
        usize,
        oneshot::Sender<ReplicationResult<ReplicaBootstrapBytePage>>,
    ),
    ReleaseBootstrapRead(
        ReplicaReadLeaseId,
        oneshot::Sender<ReplicationResult<ReplicaReadRelease>>,
    ),
    AbortBootstrap(
        AbortReplicaBootstrap,
        oneshot::Sender<ReplicationResult<AbortReplicaBootstrapReceipt>>,
    ),
    CleanupDestination(
        ReplicaCleanupLimits,
        oneshot::Sender<ReplicationResult<ReplicaCleanupProgress>>,
    ),
}

fn storage(action: &str, error: rusqlite::Error) -> ReplicationError {
    if error.sqlite_error_code() == Some(rusqlite::ErrorCode::DiskFull) {
        ReplicationError::CapacityExceeded
    } else {
        ReplicationError::StorageFailure(format!("{action}: {error}"))
    }
}

fn corrupt(action: &str, error: rusqlite::Error) -> ReplicationError {
    ReplicationError::CorruptStorage(format!("{action}: {error}"))
}

fn decode(value: &[u8], label: &str) -> ReplicationResult<u64> {
    decode_offset(value)
        .map_err(|error| ReplicationError::CorruptStorage(format!("invalid {label}: {error}")))
}

pub(super) fn initialize_replication_schema(
    conn: &Connection,
    options: &SqliteOptions,
) -> crate::application::Result<()> {
    let destination_accounting_existed: bool = conn
        .query_row(
            "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type='table' AND name='replication_destination_accounting')",
            [],
            |row| row.get(0),
        )
        .map_err(|error| crate::application::Error::StoreCorrupt(format!("inspect destination accounting schema: {error}")))?;
    conn.execute_batch(
        "BEGIN IMMEDIATE;
         CREATE TABLE IF NOT EXISTS replication_metadata(
           singleton INTEGER PRIMARY KEY CHECK(singleton=1),origin_id BLOB NOT NULL CHECK(length(origin_id)=16),
           destination_epoch BLOB NOT NULL CHECK(length(destination_epoch)=16),last_clock BLOB);
         CREATE TABLE IF NOT EXISTS replication_origin_replicas(
           replica_id TEXT NOT NULL,origin_id BLOB NOT NULL CHECK(length(origin_id)=16),public_id TEXT NOT NULL,
           incarnation BLOB NOT NULL CHECK(length(incarnation)=16),destination_epoch BLOB NOT NULL CHECK(length(destination_epoch)=16),
           acknowledged BLOB NOT NULL CHECK(length(acknowledged)=8),max_backlog_bytes BLOB NOT NULL CHECK(length(max_backlog_bytes)=8),
           max_backlog_age_ms BLOB NOT NULL CHECK(length(max_backlog_age_ms)=8),
           backlog_records INTEGER NOT NULL CHECK(backlog_records>=0),backlog_bytes BLOB NOT NULL CHECK(length(backlog_bytes)=8),oldest_backlog_at BLOB,
           mode INTEGER NOT NULL CHECK(mode IN(0,1,2)),pending_batch BLOB,
           PRIMARY KEY(replica_id,origin_id,public_id,incarnation));
         CREATE TABLE IF NOT EXISTS replication_origin_record_times(
           stream_key INTEGER NOT NULL,offset BLOB NOT NULL CHECK(length(offset)=8),committed_at BLOB NOT NULL CHECK(length(committed_at)=8),
           PRIMARY KEY(stream_key,offset));
         CREATE TABLE IF NOT EXISTS replication_origin_operations(
           operation_id TEXT PRIMARY KEY,kind INTEGER NOT NULL CHECK(kind IN(0,1,2,3)),replica_id TEXT NOT NULL,
           origin_id BLOB NOT NULL CHECK(length(origin_id)=16),public_id TEXT NOT NULL,incarnation BLOB NOT NULL CHECK(length(incarnation)=16),
           batch_id BLOB,expected_after BLOB,result_through BLOB,destination_epoch BLOB NOT NULL CHECK(length(destination_epoch)=16),
           max_backlog_bytes BLOB,max_backlog_age_ms BLOB,start_mode INTEGER,
           limit_records INTEGER,limit_bytes BLOB,
           status_ack BLOB,status_backlog_records INTEGER,status_backlog_bytes BLOB,
           result_batch_records INTEGER,result_batch_bytes BLOB,
           status_oldest BLOB,status_mode INTEGER,status_pending BLOB);
         CREATE INDEX IF NOT EXISTS replication_origin_replicas_stream_idx
           ON replication_origin_replicas(origin_id,public_id,incarnation,mode);
         CREATE TABLE IF NOT EXISTS replication_destination_streams(
           origin_id BLOB NOT NULL CHECK(length(origin_id)=16),public_id TEXT NOT NULL,incarnation BLOB NOT NULL CHECK(length(incarnation)=16),
           floor BLOB NOT NULL CHECK(length(floor)=8),tail BLOB NOT NULL CHECK(length(tail)=8),
           PRIMARY KEY(origin_id,public_id,incarnation));
         CREATE TABLE IF NOT EXISTS replication_destination_records(
           origin_id BLOB NOT NULL,public_id TEXT NOT NULL,incarnation BLOB NOT NULL,offset BLOB NOT NULL CHECK(length(offset)=8),
           event_id TEXT NOT NULL,schema_id TEXT NOT NULL,schema_version INTEGER NOT NULL,payload BLOB NOT NULL,
           PRIMARY KEY(origin_id,public_id,incarnation,offset));
         CREATE TABLE IF NOT EXISTS replication_destination_receipts(
           batch_id BLOB PRIMARY KEY CHECK(length(batch_id)=16),destination_epoch BLOB NOT NULL CHECK(length(destination_epoch)=16),
           origin_id BLOB NOT NULL,public_id TEXT NOT NULL,incarnation BLOB NOT NULL,after_offset BLOB NOT NULL CHECK(length(after_offset)=8),
           through_offset BLOB NOT NULL CHECK(length(through_offset)=8));
         CREATE TABLE IF NOT EXISTS replication_destination_floor_receipts(
           operation_id TEXT PRIMARY KEY,origin_id BLOB NOT NULL,public_id TEXT NOT NULL,incarnation BLOB NOT NULL,
           destination_epoch BLOB NOT NULL,expected_floor BLOB NOT NULL,new_floor BLOB NOT NULL);
         CREATE TABLE IF NOT EXISTS replication_origin_bootstraps(
           bootstrap_id BLOB PRIMARY KEY CHECK(length(bootstrap_id)=16),replica_id TEXT NOT NULL,
           origin_id BLOB NOT NULL CHECK(length(origin_id)=16),public_id TEXT NOT NULL,incarnation BLOB NOT NULL CHECK(length(incarnation)=16),
           destination_operation_id TEXT NOT NULL,destination_epoch BLOB NOT NULL CHECK(length(destination_epoch)=16),
           snapshot_id BLOB NOT NULL CHECK(length(snapshot_id)=16),covered BLOB NOT NULL CHECK(length(covered)=8),
           schema_id TEXT NOT NULL,schema_version INTEGER NOT NULL,content_bytes BLOB NOT NULL CHECK(length(content_bytes)=8),
           digest BLOB NOT NULL CHECK(length(digest)=32),captured_tail BLOB NOT NULL CHECK(length(captured_tail)=8),
           suffix_records INTEGER NOT NULL CHECK(suffix_records>=0),suffix_bytes BLOB NOT NULL CHECK(length(suffix_bytes)=8),
           recovery_lease BLOB NOT NULL CHECK(length(recovery_lease)=16),protection_expires BLOB NOT NULL CHECK(length(protection_expires)=8),
           UNIQUE(replica_id,origin_id,public_id,incarnation));
         CREATE TABLE IF NOT EXISTS replication_origin_bootstrap_receipts(
           operation_id TEXT PRIMARY KEY,kind INTEGER NOT NULL CHECK(kind IN(0,1)),bootstrap_id BLOB NOT NULL CHECK(length(bootstrap_id)=16),
           destination_operation_id TEXT NOT NULL,replica_id TEXT NOT NULL,origin_id BLOB NOT NULL CHECK(length(origin_id)=16),
           public_id TEXT NOT NULL,incarnation BLOB NOT NULL CHECK(length(incarnation)=16),destination_epoch BLOB NOT NULL CHECK(length(destination_epoch)=16),
           snapshot_id BLOB NOT NULL CHECK(length(snapshot_id)=16),covered BLOB NOT NULL CHECK(length(covered)=8),schema_id TEXT NOT NULL,
           schema_version INTEGER NOT NULL,content_bytes BLOB NOT NULL CHECK(length(content_bytes)=8),digest BLOB NOT NULL CHECK(length(digest)=32),
           captured_tail BLOB NOT NULL CHECK(length(captured_tail)=8),status_ack BLOB NOT NULL CHECK(length(status_ack)=8),
           status_backlog_records INTEGER NOT NULL CHECK(status_backlog_records>=0),status_backlog_bytes BLOB NOT NULL CHECK(length(status_backlog_bytes)=8),
           status_oldest BLOB,status_mode INTEGER NOT NULL CHECK(status_mode IN(0,1,2)),protection_expires BLOB,completed INTEGER NOT NULL DEFAULT 0 CHECK(completed IN(0,1)));
         CREATE TABLE IF NOT EXISTS replication_destination_bootstraps(
           bootstrap_id BLOB PRIMARY KEY CHECK(length(bootstrap_id)=16),operation_id TEXT NOT NULL,replica_id TEXT NOT NULL,
           destination_epoch BLOB NOT NULL CHECK(length(destination_epoch)=16),origin_id BLOB NOT NULL CHECK(length(origin_id)=16),
           public_id TEXT NOT NULL,incarnation BLOB NOT NULL CHECK(length(incarnation)=16),snapshot_id BLOB NOT NULL CHECK(length(snapshot_id)=16),
           covered BLOB NOT NULL CHECK(length(covered)=8),schema_id TEXT NOT NULL,schema_version INTEGER NOT NULL,
           content_bytes BLOB NOT NULL CHECK(length(content_bytes)=8),digest BLOB NOT NULL CHECK(length(digest)=32),
           through_offset BLOB NOT NULL CHECK(length(through_offset)=8),state INTEGER NOT NULL CHECK(state IN(0,1,2,3)),
           accepted_bytes BLOB NOT NULL CHECK(length(accepted_bytes)=8),accepted_records INTEGER NOT NULL CHECK(accepted_records>=0),
           accepted_record_bytes BLOB NOT NULL CHECK(length(accepted_record_bytes)=8),publish_operation_id TEXT,abort_operation_id TEXT);
         CREATE TABLE IF NOT EXISTS replication_destination_bootstrap_chunks(
           bootstrap_id BLOB NOT NULL,offset BLOB NOT NULL CHECK(length(offset)=8),bytes BLOB NOT NULL,
           PRIMARY KEY(bootstrap_id,offset),FOREIGN KEY(bootstrap_id) REFERENCES replication_destination_bootstraps(bootstrap_id));
         CREATE TABLE IF NOT EXISTS replication_destination_bootstrap_records(
           bootstrap_id BLOB NOT NULL,offset BLOB NOT NULL CHECK(length(offset)=8),event_id TEXT NOT NULL,
           schema_id TEXT NOT NULL,schema_version INTEGER NOT NULL,payload BLOB NOT NULL,
           PRIMARY KEY(bootstrap_id,offset),FOREIGN KEY(bootstrap_id) REFERENCES replication_destination_bootstraps(bootstrap_id));
         CREATE TABLE IF NOT EXISTS replication_destination_bootstrap_batch_receipts(
           bootstrap_id BLOB NOT NULL,batch_id BLOB NOT NULL CHECK(length(batch_id)=16),after_offset BLOB NOT NULL CHECK(length(after_offset)=8),
           through_offset BLOB NOT NULL CHECK(length(through_offset)=8),PRIMARY KEY(bootstrap_id,batch_id),
           FOREIGN KEY(bootstrap_id) REFERENCES replication_destination_bootstraps(bootstrap_id));
         CREATE TABLE IF NOT EXISTS replication_destination_published_bootstraps(
           origin_id BLOB NOT NULL,public_id TEXT NOT NULL,incarnation BLOB NOT NULL,bootstrap_id BLOB NOT NULL UNIQUE,
           PRIMARY KEY(origin_id,public_id,incarnation),FOREIGN KEY(bootstrap_id) REFERENCES replication_destination_bootstraps(bootstrap_id));
         CREATE INDEX IF NOT EXISTS replication_destination_records_offset_idx
           ON replication_destination_records(origin_id,public_id,incarnation,offset);
         COMMIT;",
    ).map_err(|error| crate::application::Error::StoreWriteFailed(format!("initialize replication schema: {error}")))?;
    let has_completed: bool = conn
        .query_row(
            "SELECT EXISTS(SELECT 1 FROM pragma_table_info('replication_origin_bootstrap_receipts') WHERE name='completed')",
            [],
            |row| row.get(0),
        )
        .map_err(|error| {
            crate::application::Error::StoreCorrupt(format!(
                "inspect origin bootstrap receipt schema: {error}"
            ))
        })?;
    if !has_completed {
        conn.execute_batch(
            "BEGIN IMMEDIATE;
             ALTER TABLE replication_origin_bootstrap_receipts
               ADD COLUMN completed INTEGER NOT NULL DEFAULT 0 CHECK(completed IN(0,1));
             COMMIT;",
        )
        .map_err(|error| {
            crate::application::Error::StoreWriteFailed(format!(
                "extend origin bootstrap receipt schema: {error}"
            ))
        })?;
    }
    let exists: bool = conn
        .query_row(
            "SELECT EXISTS(SELECT 1 FROM replication_metadata WHERE singleton=1)",
            [],
            |row| row.get(0),
        )
        .map_err(|error| {
            crate::application::Error::StoreCorrupt(format!(
                "inspect replication metadata: {error}"
            ))
        })?;
    if !exists {
        let occupied: bool = conn
            .query_row(
                "SELECT EXISTS(SELECT 1 FROM replication_origin_replicas UNION ALL SELECT 1 FROM replication_origin_record_times UNION ALL SELECT 1 FROM replication_origin_operations UNION ALL SELECT 1 FROM replication_destination_streams UNION ALL SELECT 1 FROM replication_destination_records UNION ALL SELECT 1 FROM replication_destination_receipts UNION ALL SELECT 1 FROM replication_destination_floor_receipts)",
                [],
                |row| row.get(0),
            )
            .map_err(|error| crate::application::Error::StoreCorrupt(format!("inspect replication state: {error}")))?;
        if occupied {
            return Err(crate::application::Error::StoreCorrupt(
                "replication identity metadata is missing from a populated store".into(),
            ));
        }
        conn.execute(
            "INSERT INTO replication_metadata VALUES(1,?1,?2,NULL)",
            params![
                uuid::Uuid::new_v4().as_bytes(),
                uuid::Uuid::new_v4().as_bytes()
            ],
        )
        .map_err(|error| {
            crate::application::Error::StoreWriteFailed(format!(
                "initialize replication identity: {error}"
            ))
        })?;
    }
    initialize_destination_accounting(conn, destination_accounting_existed)?;
    validate_replication_state(conn, options)
}

fn batch_receipt_charge_from_parts(
    public_id: &str,
    after: u64,
    through: u64,
) -> crate::application::Result<u64> {
    let records = through.checked_sub(after).ok_or_else(|| {
        crate::application::Error::StoreCorrupt(
            "replica receipt ends before its starting position".into(),
        )
    })?;
    records
        .checked_mul(std::mem::size_of::<Arc<Record>>() as u64)
        .and_then(|bytes| bytes.checked_add((public_id.len() as u64).saturating_mul(2)))
        .and_then(|bytes| bytes.checked_add(RECEIPT_OVERHEAD as u64))
        .ok_or(crate::application::Error::CapacityExceeded)
}

fn floor_receipt_charge(operation_id: &str, public_id: &str) -> crate::application::Result<u64> {
    u64::try_from(operation_id.len())
        .ok()
        .and_then(|bytes| bytes.checked_add(public_id.len() as u64))
        .and_then(|bytes| bytes.checked_add(RECEIPT_OVERHEAD as u64))
        .ok_or(crate::application::Error::CapacityExceeded)
}

struct MeasuredDestinationAccounting {
    streams: u64,
    history_records: u64,
    history_bytes: u64,
    receipts: u64,
    receipt_bytes: u64,
    staging_chunks: u64,
    staging_chunk_bytes: u64,
    staging_records: u64,
    staging_record_bytes: u64,
}

fn measured_destination_accounting(
    conn: &Connection,
) -> crate::application::Result<MeasuredDestinationAccounting> {
    let (streams, records, history_bytes): (i64, i64, i64) = conn
        .query_row(
            "SELECT (SELECT count(*) FROM replication_destination_streams),
                    (SELECT count(*) FROM replication_destination_records),
                    (SELECT coalesce(sum(octet_length(event_id)+octet_length(schema_id)+octet_length(payload)+384),0) FROM replication_destination_records)",
            [],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .map_err(|error| crate::application::Error::StoreCorrupt(format!("measure destination history: {error}")))?;
    let mut receipt_count = 0u64;
    let mut receipt_bytes = 0u64;
    {
        let mut statement = conn
            .prepare("SELECT public_id,after_offset,through_offset FROM replication_destination_receipts")
            .map_err(|error| crate::application::Error::StoreCorrupt(format!("prepare destination receipt audit: {error}")))?;
        let mut rows = statement.query([]).map_err(|error| {
            crate::application::Error::StoreCorrupt(format!(
                "query destination receipt audit: {error}"
            ))
        })?;
        while let Some(row) = rows.next().map_err(|error| {
            crate::application::Error::StoreCorrupt(format!(
                "step destination receipt audit: {error}"
            ))
        })? {
            let public_id: String = row.get(0).map_err(|error| {
                crate::application::Error::StoreCorrupt(format!(
                    "decode destination receipt name: {error}"
                ))
            })?;
            let after: Vec<u8> = row.get(1).map_err(|error| {
                crate::application::Error::StoreCorrupt(format!(
                    "decode destination receipt start: {error}"
                ))
            })?;
            let through: Vec<u8> = row.get(2).map_err(|error| {
                crate::application::Error::StoreCorrupt(format!(
                    "decode destination receipt end: {error}"
                ))
            })?;
            receipt_count = receipt_count
                .checked_add(1)
                .ok_or(crate::application::Error::CapacityExceeded)?;
            receipt_bytes = receipt_bytes
                .checked_add(batch_receipt_charge_from_parts(
                    &public_id,
                    decode_offset(&after)?,
                    decode_offset(&through)?,
                )?)
                .ok_or(crate::application::Error::CapacityExceeded)?;
        }
    }
    {
        let mut statement = conn
            .prepare("SELECT operation_id,public_id FROM replication_destination_floor_receipts")
            .map_err(|error| {
                crate::application::Error::StoreCorrupt(format!(
                    "prepare destination floor receipt audit: {error}"
                ))
            })?;
        let mut rows = statement.query([]).map_err(|error| {
            crate::application::Error::StoreCorrupt(format!(
                "query destination floor receipt audit: {error}"
            ))
        })?;
        while let Some(row) = rows.next().map_err(|error| {
            crate::application::Error::StoreCorrupt(format!(
                "step destination floor receipt audit: {error}"
            ))
        })? {
            let operation_id: String = row.get(0).map_err(|error| {
                crate::application::Error::StoreCorrupt(format!(
                    "decode destination floor operation: {error}"
                ))
            })?;
            let public_id: String = row.get(1).map_err(|error| {
                crate::application::Error::StoreCorrupt(format!(
                    "decode destination floor name: {error}"
                ))
            })?;
            receipt_count = receipt_count
                .checked_add(1)
                .ok_or(crate::application::Error::CapacityExceeded)?;
            receipt_bytes = receipt_bytes
                .checked_add(floor_receipt_charge(&operation_id, &public_id)?)
                .ok_or(crate::application::Error::CapacityExceeded)?;
        }
    }
    let (chunk_count, chunk_bytes): (i64, i64) = conn
        .query_row(
            "SELECT count(*),coalesce(sum(octet_length(bytes)),0)
             FROM replication_destination_bootstrap_chunks",
            [],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .map_err(|error| {
            crate::application::Error::StoreCorrupt(format!(
                "measure destination bootstrap chunks: {error}"
            ))
        })?;
    let (staging_record_count, staging_record_bytes): (i64, i64) = conn
        .query_row(
            "SELECT count(*),coalesce(sum(octet_length(event_id)+octet_length(schema_id)+octet_length(payload)+384),0)
             FROM replication_destination_bootstrap_records",
            [],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .map_err(|error| {
            crate::application::Error::StoreCorrupt(format!(
                "measure destination bootstrap records: {error}"
            ))
        })?;
    Ok(MeasuredDestinationAccounting {
        streams: u64::try_from(streams).map_err(|_| {
            crate::application::Error::StoreCorrupt("invalid destination stream count".into())
        })?,
        history_records: u64::try_from(records).map_err(|_| {
            crate::application::Error::StoreCorrupt("invalid destination record count".into())
        })?,
        history_bytes: u64::try_from(history_bytes).map_err(|_| {
            crate::application::Error::StoreCorrupt("invalid destination history bytes".into())
        })?,
        receipts: receipt_count,
        receipt_bytes,
        staging_chunks: u64::try_from(chunk_count).map_err(|_| {
            crate::application::Error::StoreCorrupt(
                "invalid destination bootstrap chunk count".into(),
            )
        })?,
        staging_chunk_bytes: u64::try_from(chunk_bytes).map_err(|_| {
            crate::application::Error::StoreCorrupt(
                "invalid destination bootstrap chunk bytes".into(),
            )
        })?,
        staging_records: u64::try_from(staging_record_count).map_err(|_| {
            crate::application::Error::StoreCorrupt(
                "invalid destination bootstrap record count".into(),
            )
        })?,
        staging_record_bytes: u64::try_from(staging_record_bytes).map_err(|_| {
            crate::application::Error::StoreCorrupt(
                "invalid destination bootstrap record bytes".into(),
            )
        })?,
    })
}

fn initialize_destination_accounting(
    conn: &Connection,
    accounting_existed: bool,
) -> crate::application::Result<()> {
    if !accounting_existed {
        conn.execute_batch(
            "BEGIN IMMEDIATE;
             CREATE TABLE replication_destination_accounting(
               singleton INTEGER PRIMARY KEY CHECK(singleton=1),stream_count INTEGER NOT NULL CHECK(stream_count>=0),
               history_records INTEGER NOT NULL CHECK(history_records>=0),history_bytes BLOB NOT NULL CHECK(length(history_bytes)=8),
               receipt_count INTEGER NOT NULL CHECK(receipt_count>=0),receipt_bytes BLOB NOT NULL CHECK(length(receipt_bytes)=8),
               staging_chunk_count INTEGER NOT NULL CHECK(staging_chunk_count>=0),
               staging_chunk_bytes BLOB NOT NULL CHECK(length(staging_chunk_bytes)=8),
               staging_record_count INTEGER NOT NULL CHECK(staging_record_count>=0),
               staging_record_bytes BLOB NOT NULL CHECK(length(staging_record_bytes)=8));",
        )
        .map_err(|error| {
            crate::application::Error::StoreWriteFailed(format!(
                "begin destination accounting migration: {error}"
            ))
        })?;
        let measured = match measured_destination_accounting(conn) {
            Ok(measured) => measured,
            Err(error) => {
                let _ = conn.execute_batch("ROLLBACK");
                return Err(error);
            }
        };
        let inserted = conn.execute(
            "INSERT INTO replication_destination_accounting VALUES(1,?1,?2,?3,?4,?5,?6,?7,?8,?9)",
            params![
                i64::try_from(measured.streams)
                    .map_err(|_| crate::application::Error::CapacityExceeded)?,
                i64::try_from(measured.history_records)
                    .map_err(|_| crate::application::Error::CapacityExceeded)?,
                be(measured.history_bytes).as_slice(),
                i64::try_from(measured.receipts)
                    .map_err(|_| crate::application::Error::CapacityExceeded)?,
                be(measured.receipt_bytes).as_slice(),
                i64::try_from(measured.staging_chunks)
                    .map_err(|_| crate::application::Error::CapacityExceeded)?,
                be(measured.staging_chunk_bytes).as_slice(),
                i64::try_from(measured.staging_records)
                    .map_err(|_| crate::application::Error::CapacityExceeded)?,
                be(measured.staging_record_bytes).as_slice()
            ],
        );
        if let Err(error) = inserted {
            let _ = conn.execute_batch("ROLLBACK");
            return Err(crate::application::Error::StoreWriteFailed(format!(
                "initialize destination accounting: {error}"
            )));
        }
        return conn.execute_batch("COMMIT").map_err(|error| {
            crate::application::Error::StoreWriteFailed(format!(
                "commit destination accounting migration: {error}"
            ))
        });
    }
    type LegacyAccountingRow = (i64, i64, Vec<u8>, i64, Vec<u8>);
    let legacy: Option<LegacyAccountingRow> = conn
        .query_row(
            "SELECT stream_count,history_records,history_bytes,receipt_count,receipt_bytes FROM replication_destination_accounting WHERE singleton=1",
            [],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?, row.get(4)?)),
        )
        .optional()
        .map_err(|error| crate::application::Error::StoreCorrupt(format!("read destination accounting: {error}")))?;
    let measured = measured_destination_accounting(conn)?;
    if let Some((streams, records, bytes, receipts, receipt_bytes)) = legacy {
        let valid = u64::try_from(streams).ok() == Some(measured.streams)
            && u64::try_from(records).ok() == Some(measured.history_records)
            && decode_offset(&bytes).ok() == Some(measured.history_bytes)
            && u64::try_from(receipts).ok() == Some(measured.receipts)
            && decode_offset(&receipt_bytes).ok() == Some(measured.receipt_bytes);
        if !valid {
            return Err(crate::application::Error::StoreCorrupt(
                "destination replication counters do not match stored rows".into(),
            ));
        }
    } else {
        return Err(crate::application::Error::StoreCorrupt(
            "destination replication accounting metadata is missing".into(),
        ));
    }
    let chunk_columns: i64 = conn
        .query_row(
            "SELECT count(*) FROM pragma_table_info('replication_destination_accounting')
             WHERE name IN('staging_chunk_count','staging_chunk_bytes')",
            [],
            |row| row.get(0),
        )
        .map_err(|error| {
            crate::application::Error::StoreCorrupt(format!(
                "inspect destination chunk accounting schema: {error}"
            ))
        })?;
    match chunk_columns {
        0 => {
            conn.execute_batch(
                "BEGIN IMMEDIATE;
                 ALTER TABLE replication_destination_accounting
                   ADD COLUMN staging_chunk_count INTEGER NOT NULL DEFAULT 0 CHECK(staging_chunk_count>=0);
                 ALTER TABLE replication_destination_accounting
                   ADD COLUMN staging_chunk_bytes BLOB NOT NULL DEFAULT X'0000000000000000' CHECK(length(staging_chunk_bytes)=8);",
            )
            .map_err(|error| {
                crate::application::Error::StoreWriteFailed(format!(
                    "begin destination chunk accounting migration: {error}"
                ))
            })?;
            let updated = conn.execute(
                "UPDATE replication_destination_accounting
                 SET staging_chunk_count=?1,staging_chunk_bytes=?2 WHERE singleton=1",
                params![
                    i64::try_from(measured.staging_chunks)
                        .map_err(|_| crate::application::Error::CapacityExceeded)?,
                    be(measured.staging_chunk_bytes).as_slice()
                ],
            );
            if let Err(error) = updated {
                let _ = conn.execute_batch("ROLLBACK");
                return Err(crate::application::Error::StoreWriteFailed(format!(
                    "initialize destination chunk accounting: {error}"
                )));
            }
            conn.execute_batch("COMMIT").map_err(|error| {
                crate::application::Error::StoreWriteFailed(format!(
                    "commit destination chunk accounting migration: {error}"
                ))
            })?;
        }
        2 => {
            let (count, bytes): (i64, Vec<u8>) = conn
                .query_row(
                    "SELECT staging_chunk_count,staging_chunk_bytes
                     FROM replication_destination_accounting WHERE singleton=1",
                    [],
                    |row| Ok((row.get(0)?, row.get(1)?)),
                )
                .map_err(|error| {
                    crate::application::Error::StoreCorrupt(format!(
                        "read destination chunk accounting: {error}"
                    ))
                })?;
            if u64::try_from(count).ok() != Some(measured.staging_chunks)
                || decode_offset(&bytes).ok() != Some(measured.staging_chunk_bytes)
            {
                return Err(crate::application::Error::StoreCorrupt(
                    "destination bootstrap chunk counters do not match stored rows".into(),
                ));
            }
        }
        _ => {
            return Err(crate::application::Error::StoreCorrupt(
                "destination bootstrap chunk accounting schema is partial".into(),
            ));
        }
    }
    let record_columns: i64 = conn
        .query_row(
            "SELECT count(*) FROM pragma_table_info('replication_destination_accounting')
             WHERE name IN('staging_record_count','staging_record_bytes')",
            [],
            |row| row.get(0),
        )
        .map_err(|error| {
            crate::application::Error::StoreCorrupt(format!(
                "inspect destination record accounting schema: {error}"
            ))
        })?;
    match record_columns {
        0 => {
            conn.execute_batch(
                "BEGIN IMMEDIATE;
                 ALTER TABLE replication_destination_accounting
                   ADD COLUMN staging_record_count INTEGER NOT NULL DEFAULT 0 CHECK(staging_record_count>=0);
                 ALTER TABLE replication_destination_accounting
                   ADD COLUMN staging_record_bytes BLOB NOT NULL DEFAULT X'0000000000000000' CHECK(length(staging_record_bytes)=8);",
            )
            .map_err(|error| {
                crate::application::Error::StoreWriteFailed(format!(
                    "begin destination record accounting migration: {error}"
                ))
            })?;
            let updated = conn.execute(
                "UPDATE replication_destination_accounting
                 SET staging_record_count=?1,staging_record_bytes=?2 WHERE singleton=1",
                params![
                    i64::try_from(measured.staging_records)
                        .map_err(|_| crate::application::Error::CapacityExceeded)?,
                    be(measured.staging_record_bytes).as_slice()
                ],
            );
            if let Err(error) = updated {
                let _ = conn.execute_batch("ROLLBACK");
                return Err(crate::application::Error::StoreWriteFailed(format!(
                    "initialize destination record accounting: {error}"
                )));
            }
            conn.execute_batch("COMMIT").map_err(|error| {
                crate::application::Error::StoreWriteFailed(format!(
                    "commit destination record accounting migration: {error}"
                ))
            })?;
        }
        2 => {
            let (count, bytes): (i64, Vec<u8>) = conn
                .query_row(
                    "SELECT staging_record_count,staging_record_bytes
                     FROM replication_destination_accounting WHERE singleton=1",
                    [],
                    |row| Ok((row.get(0)?, row.get(1)?)),
                )
                .map_err(|error| {
                    crate::application::Error::StoreCorrupt(format!(
                        "read destination record accounting: {error}"
                    ))
                })?;
            if u64::try_from(count).ok() != Some(measured.staging_records)
                || decode_offset(&bytes).ok() != Some(measured.staging_record_bytes)
            {
                return Err(crate::application::Error::StoreCorrupt(
                    "destination bootstrap record counters do not match stored rows".into(),
                ));
            }
        }
        _ => {
            return Err(crate::application::Error::StoreCorrupt(
                "destination bootstrap record accounting schema is partial".into(),
            ));
        }
    }
    Ok(())
}

pub(super) fn rebuild_destination_accounting(conn: &Connection) -> crate::application::Result<()> {
    let measured = measured_destination_accounting(conn)?;
    conn.execute(
        "UPDATE replication_destination_accounting
         SET stream_count=?1,history_records=?2,history_bytes=?3,receipt_count=?4,receipt_bytes=?5,
             staging_chunk_count=?6,staging_chunk_bytes=?7,
             staging_record_count=?8,staging_record_bytes=?9
         WHERE singleton=1",
        params![
            i64::try_from(measured.streams)
                .map_err(|_| crate::application::Error::CapacityExceeded)?,
            i64::try_from(measured.history_records)
                .map_err(|_| crate::application::Error::CapacityExceeded)?,
            be(measured.history_bytes).as_slice(),
            i64::try_from(measured.receipts)
                .map_err(|_| crate::application::Error::CapacityExceeded)?,
            be(measured.receipt_bytes).as_slice(),
            i64::try_from(measured.staging_chunks)
                .map_err(|_| crate::application::Error::CapacityExceeded)?,
            be(measured.staging_chunk_bytes).as_slice(),
            i64::try_from(measured.staging_records)
                .map_err(|_| crate::application::Error::CapacityExceeded)?,
            be(measured.staging_record_bytes).as_slice()
        ],
    )
    .map_err(|error| {
        crate::application::Error::StoreWriteFailed(format!(
            "rebuild destination accounting: {error}"
        ))
    })?;
    Ok(())
}

fn validate_replication_state(
    conn: &Connection,
    options: &SqliteOptions,
) -> crate::application::Result<()> {
    let replica_count: i64 = conn
        .query_row(
            "SELECT count(*) FROM replication_origin_replicas",
            [],
            |row| row.get(0),
        )
        .map_err(|error| {
            crate::application::Error::StoreCorrupt(format!("count replica attachments: {error}"))
        })?;
    let oversized_stream: bool = conn
        .query_row(
            "SELECT EXISTS(SELECT 1 FROM replication_origin_replicas GROUP BY origin_id,public_id,incarnation HAVING count(*)>?1)",
            [i64::try_from(options.replication.attachments.max_replicas_per_stream).unwrap_or(i64::MAX)],
            |row| row.get(0),
        )
        .map_err(|error| crate::application::Error::StoreCorrupt(format!("audit per-stream replica attachments: {error}")))?;
    if usize::try_from(replica_count)
        .ok()
        .is_none_or(|count| count > options.replication.attachments.max_replicas)
        || oversized_stream
    {
        return Err(crate::application::Error::CapacityExceeded);
    }
    type DestinationLimitsRow = (i64, i64, Vec<u8>, i64, Vec<u8>, i64, Vec<u8>, i64, Vec<u8>);
    let (destination_streams, destination_records, destination_bytes, destination_receipts, destination_receipt_bytes, destination_chunks, destination_chunk_bytes, destination_staging_records, destination_staging_record_bytes): DestinationLimitsRow = conn
        .query_row("SELECT stream_count,history_records,history_bytes,receipt_count,receipt_bytes,staging_chunk_count,staging_chunk_bytes,staging_record_count,staging_record_bytes FROM replication_destination_accounting WHERE singleton=1",[],|row|Ok((row.get(0)?,row.get(1)?,row.get(2)?,row.get(3)?,row.get(4)?,row.get(5)?,row.get(6)?,row.get(7)?,row.get(8)?)))
        .map_err(|error| crate::application::Error::StoreCorrupt(format!("audit destination limits: {error}")))?;
    if usize::try_from(destination_streams)
        .ok()
        .is_none_or(|count| count > options.replica_destination.storage.max_origin_streams)
        || usize::try_from(destination_records)
            .ok()
            .is_none_or(|count| count > options.replica_destination.storage.max_history_records)
        || decode_offset(&destination_bytes)
            .ok()
            .is_none_or(|bytes| bytes > options.replica_destination.storage.max_history_bytes)
        || usize::try_from(destination_receipts)
            .ok()
            .is_none_or(|count| count > options.replica_destination.receipts.max_batch_receipts)
        || decode_offset(&destination_receipt_bytes)
            .ok()
            .is_none_or(|bytes| {
                bytes > options.replica_destination.receipts.max_batch_receipt_bytes as u64
            })
        || usize::try_from(destination_chunks)
            .ok()
            .is_none_or(|count| count > options.replica_destination.staging.max_staging_chunks)
        || decode_offset(&destination_chunk_bytes)
            .ok()
            .is_none_or(|bytes| bytes > options.replica_destination.staging.max_staging_bytes)
        || usize::try_from(destination_staging_records)
            .ok()
            .is_none_or(|count| count > options.replica_destination.staging.max_staging_records)
        || decode_offset(&destination_staging_record_bytes)
            .ok()
            .is_none_or(|bytes| {
                bytes > options.replica_destination.staging.max_staging_record_bytes
            })
    {
        return Err(crate::application::Error::CapacityExceeded);
    }
    let detached_invalid: bool = conn.query_row(
        "SELECT EXISTS(SELECT 1 FROM replication_origin_replicas WHERE mode=1 AND (backlog_records<>0 OR backlog_bytes<>zeroblob(8) OR oldest_backlog_at IS NOT NULL OR pending_batch IS NOT NULL))",
        [], |row| row.get(0),
    ).map_err(|error| crate::application::Error::StoreCorrupt(format!("validate detached replica accounting: {error}")))?;
    if detached_invalid {
        return Err(crate::application::Error::StoreCorrupt(
            "detached replica retains active backlog accounting".into(),
        ));
    }
    let limit = options
        .replication
        .attachments
        .max_replicas
        .checked_add(1)
        .and_then(|value| i64::try_from(value).ok())
        .unwrap_or(i64::MAX);
    let mut statement = conn.prepare(
        "WITH all_records AS (
           SELECT stream_key,offset,event_id,schema_id,payload FROM event_records
           UNION ALL
           SELECT stream_key,offset,event_id,schema_id,payload FROM retention_generated_records)
         SELECT r.backlog_records,CASE WHEN typeof(r.backlog_bytes)='blob' AND length(r.backlog_bytes)=8 THEN r.backlog_bytes END,
                count(e.offset),coalesce(sum(octet_length(e.event_id)+octet_length(e.schema_id)+octet_length(e.payload)+384),0),count(t.committed_at)
         FROM replication_origin_replicas r
         JOIN event_streams s ON s.public_id=r.public_id AND s.incarnation=r.incarnation
         LEFT JOIN all_records e ON e.stream_key=s.stream_key AND e.offset>r.acknowledged
         LEFT JOIN replication_origin_record_times t ON t.stream_key=e.stream_key AND t.offset=e.offset
         WHERE r.mode IN(0,2) GROUP BY r.rowid LIMIT ?1",
    ).map_err(|error| crate::application::Error::StoreCorrupt(format!("prepare replica accounting audit: {error}")))?;
    let mut rows = statement.query([limit]).map_err(|error| {
        crate::application::Error::StoreCorrupt(format!("query replica accounting audit: {error}"))
    })?;
    let mut count = 0usize;
    while let Some(row) = rows.next().map_err(|error| {
        crate::application::Error::StoreCorrupt(format!("step replica accounting audit: {error}"))
    })? {
        count = count
            .checked_add(1)
            .ok_or(crate::application::Error::CapacityExceeded)?;
        let stored_records: i64 = row.get(0).map_err(|error| {
            crate::application::Error::StoreCorrupt(format!(
                "decode replica record counter: {error}"
            ))
        })?;
        let stored_bytes: Option<Vec<u8>> = row.get(1).map_err(|error| {
            crate::application::Error::StoreCorrupt(format!("decode replica byte counter: {error}"))
        })?;
        let actual_records: i64 = row.get(2).map_err(|error| {
            crate::application::Error::StoreCorrupt(format!(
                "decode actual replica records: {error}"
            ))
        })?;
        let actual_bytes: i64 = row.get(3).map_err(|error| {
            crate::application::Error::StoreCorrupt(format!("decode actual replica bytes: {error}"))
        })?;
        let timed_records: i64 = row.get(4).map_err(|error| {
            crate::application::Error::StoreCorrupt(format!("decode replica timestamps: {error}"))
        })?;
        let stored_bytes = stored_bytes
            .as_deref()
            .map(|value| decode_offset(value).ok());
        if stored_records != actual_records
            || stored_bytes.flatten() != u64::try_from(actual_bytes).ok()
            || timed_records != actual_records
        {
            return Err(crate::application::Error::StoreCorrupt(
                "replica backlog counters do not match retained history".into(),
            ));
        }
    }
    if count > options.replication.attachments.max_replicas {
        return Err(crate::application::Error::CapacityExceeded);
    }
    Ok(())
}

pub(super) fn preflight_replication_append(
    conn: &Connection,
    stream: &StreamKey,
    _stream_key: i64,
    event: &NewEvent,
    options: &SqliteOptions,
) -> crate::application::Result<(DurableTimestampMillis, u64)> {
    let now = options.replication_clock.now();
    let last: Option<Vec<u8>> = conn
        .query_row(
            "SELECT last_clock FROM replication_metadata WHERE singleton=1",
            [],
            |row| row.get(0),
        )
        .map_err(|error| {
            crate::application::Error::StoreCorrupt(format!("read replication clock: {error}"))
        })?;
    if let Some(last) = last {
        let last = decode_offset(&last).map_err(|error| {
            crate::application::Error::StoreCorrupt(format!("invalid replication clock: {error}"))
        })?;
        if now.0 < last {
            return Err(crate::application::Error::ReplicationClockRollback);
        }
    }
    let charge = event
        .accounted_bytes()
        .checked_add(RECORD_OVERHEAD)
        .and_then(|bytes| u64::try_from(bytes).ok())
        .ok_or(crate::application::Error::CapacityExceeded)?;
    let origin = origin_id(conn)
        .map_err(|error| crate::application::Error::StoreCorrupt(error.to_string()))?;
    let mut statement = conn
        .prepare("SELECT replica_id,backlog_bytes,max_backlog_bytes,max_backlog_age_ms,oldest_backlog_at FROM replication_origin_replicas WHERE origin_id=?1 AND public_id=?2 AND incarnation=?3 AND mode IN(0,2)")
        .map_err(|error| crate::application::Error::StoreCorrupt(format!("prepare replica append admission: {error}")))?;
    let mut rows = statement
        .query(params![
            origin.0.as_slice(),
            stream.id.as_str(),
            stream.incarnation.0.as_slice()
        ])
        .map_err(|error| {
            crate::application::Error::StoreCorrupt(format!(
                "query replica append admission: {error}"
            ))
        })?;
    while let Some(row) = rows.next().map_err(|error| {
        crate::application::Error::StoreCorrupt(format!("step replica append admission: {error}"))
    })? {
        let replica: String = row.get(0).map_err(|error| {
            crate::application::Error::StoreCorrupt(format!("decode replica identity: {error}"))
        })?;
        let current_bytes: Vec<u8> = row.get(1).map_err(|error| {
            crate::application::Error::StoreCorrupt(format!("decode replica backlog: {error}"))
        })?;
        let max_bytes: Vec<u8> = row.get(2).map_err(|error| {
            crate::application::Error::StoreCorrupt(format!("decode replica byte limit: {error}"))
        })?;
        let max_age: Vec<u8> = row.get(3).map_err(|error| {
            crate::application::Error::StoreCorrupt(format!("decode replica age limit: {error}"))
        })?;
        let oldest: Option<Vec<u8>> = row.get(4).map_err(|error| {
            crate::application::Error::StoreCorrupt(format!("decode replica backlog time: {error}"))
        })?;
        let current_bytes = decode_offset(&current_bytes)
            .map_err(|error| crate::application::Error::StoreCorrupt(error.to_string()))?;
        let limit = decode_offset(&max_bytes)
            .map_err(|error| crate::application::Error::StoreCorrupt(error.to_string()))?;
        if current_bytes
            .checked_add(charge)
            .is_none_or(|bytes| bytes > limit)
        {
            return Err(crate::application::Error::ReplicaBacklogExceeded {
                replica: ReplicaId::new(replica)
                    .map_err(|error| crate::application::Error::StoreCorrupt(error.to_string()))?,
                limit_bytes: limit,
            });
        }
        if let Some(oldest) = oldest {
            let oldest = decode_offset(&oldest)
                .map_err(|error| crate::application::Error::StoreCorrupt(error.to_string()))?;
            let max_age = decode_offset(&max_age)
                .map_err(|error| crate::application::Error::StoreCorrupt(error.to_string()))?;
            if now.0.saturating_sub(oldest) > max_age {
                return Err(crate::application::Error::ReplicaBacklogExpired {
                    replica: ReplicaId::new(replica).map_err(|error| {
                        crate::application::Error::StoreCorrupt(error.to_string())
                    })?,
                });
            }
        }
    }
    Ok((now, charge))
}

pub(super) fn apply_replication_append(
    conn: &Connection,
    stream: &StreamKey,
    stream_key: i64,
    offset: u64,
    charge: u64,
    now: DurableTimestampMillis,
) -> crate::application::Result<()> {
    let origin = origin_id(conn)
        .map_err(|error| crate::application::Error::StoreCorrupt(error.to_string()))?;
    conn.execute(
        "UPDATE replication_metadata SET last_clock=?1 WHERE singleton=1",
        [be(now.0).as_slice()],
    )
    .map_err(|error| {
        crate::application::Error::StoreWriteFailed(format!("store replication clock: {error}"))
    })?;
    let mut statement = conn
        .prepare("SELECT replica_id,backlog_records,backlog_bytes FROM replication_origin_replicas WHERE origin_id=?1 AND public_id=?2 AND incarnation=?3 AND mode IN(0,2)")
        .map_err(|error| crate::application::Error::StoreWriteFailed(format!("prepare replica backlog update: {error}")))?;
    let rows = statement
        .query_map(
            params![
                origin.0.as_slice(),
                stream.id.as_str(),
                stream.incarnation.0.as_slice()
            ],
            |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, i64>(1)?,
                    row.get::<_, Vec<u8>>(2)?,
                ))
            },
        )
        .map_err(|error| {
            crate::application::Error::StoreWriteFailed(format!(
                "query replica backlog update: {error}"
            ))
        })?;
    let updates = rows
        .collect::<rusqlite::Result<Vec<_>>>()
        .map_err(|error| {
            crate::application::Error::StoreCorrupt(format!(
                "decode replica backlog update: {error}"
            ))
        })?;
    drop(statement);
    if !updates.is_empty() {
        conn.execute(
            "INSERT INTO replication_origin_record_times(stream_key,offset,committed_at) VALUES(?1,?2,?3)",
            params![stream_key, be(offset).as_slice(), be(now.0).as_slice()],
        )
        .map_err(|error| {
            crate::application::Error::StoreWriteFailed(format!(
                "store replication record time: {error}"
            ))
        })?;
    }
    for (replica, records, bytes) in updates {
        let records = records
            .checked_add(1)
            .ok_or(crate::application::Error::CapacityExceeded)?;
        let bytes = decode_offset(&bytes)
            .map_err(|error| crate::application::Error::StoreCorrupt(error.to_string()))?
            .checked_add(charge)
            .ok_or(crate::application::Error::CapacityExceeded)?;
        conn.execute(
            "UPDATE replication_origin_replicas SET backlog_records=?1,backlog_bytes=?2,oldest_backlog_at=coalesce(oldest_backlog_at,?3) WHERE replica_id=?4 AND origin_id=?5 AND public_id=?6 AND incarnation=?7",
            params![records, be(bytes).as_slice(), be(now.0).as_slice(), replica, origin.0.as_slice(), stream.id.as_str(), stream.incarnation.0.as_slice()],
        )
        .map_err(|error| crate::application::Error::StoreWriteFailed(format!("update replica backlog: {error}")))?;
    }
    Ok(())
}

pub(super) fn minimum_replica_offset(
    conn: &Connection,
    stream: &StreamKey,
) -> crate::application::Result<Option<u64>> {
    let value: Option<Vec<u8>> = conn
        .query_row(
            "SELECT acknowledged FROM replication_origin_replicas
             WHERE public_id=?1 AND incarnation=?2 AND mode IN (0,2)
             ORDER BY acknowledged LIMIT 1",
            params![stream.id.as_str(), stream.incarnation.0.as_slice()],
            |row| row.get(0),
        )
        .optional()
        .map_err(|error| {
            crate::application::Error::StoreCorrupt(format!(
                "read replica retention protection: {error}"
            ))
        })?;
    value
        .as_deref()
        .map(decode_offset)
        .transpose()
        .map_err(|error| {
            crate::application::Error::StoreCorrupt(format!(
                "invalid replica retention protection: {error}"
            ))
        })
}

fn origin_id(conn: &Connection) -> ReplicationResult<OriginId> {
    let value: Option<Vec<u8>> = conn
        .query_row(
            "SELECT CASE WHEN typeof(origin_id)='blob' AND length(origin_id)=16 THEN origin_id END FROM replication_metadata WHERE singleton=1",
            [],
            |row| row.get(0),
        )
        .map_err(|error| corrupt("read origin identity", error))?;
    let value =
        value.ok_or_else(|| ReplicationError::CorruptStorage("invalid origin identity".into()))?;
    Ok(OriginId(value.try_into().map_err(|_| {
        ReplicationError::CorruptStorage("invalid origin identity".into())
    })?))
}

fn destination_epoch(conn: &Connection) -> ReplicationResult<DestinationEpoch> {
    let value: Option<Vec<u8>> = conn
        .query_row(
            "SELECT CASE WHEN typeof(destination_epoch)='blob' AND length(destination_epoch)=16 THEN destination_epoch END FROM replication_metadata WHERE singleton=1",
            [],
            |row| row.get(0),
        )
        .map_err(|error| corrupt("read destination epoch", error))?;
    let value = value
        .ok_or_else(|| ReplicationError::CorruptStorage("invalid destination epoch".into()))?;
    Ok(DestinationEpoch(value.try_into().map_err(|_| {
        ReplicationError::CorruptStorage("invalid destination epoch".into())
    })?))
}

fn record_charge(record: &Record) -> ReplicationResult<usize> {
    record
        .event
        .accounted_bytes()
        .checked_add(RECORD_OVERHEAD)
        .ok_or(ReplicationError::CapacityExceeded)
}

fn validate_batch(
    batch: &ReplicaBatch,
    max_records: usize,
    max_bytes: usize,
) -> ReplicationResult<usize> {
    if batch.records.is_empty() || batch.records.len() > max_records {
        return Err(ReplicationError::InvalidInput(
            "replica batch record count is outside its finite limit".into(),
        ));
    }
    let mut bytes = 0usize;
    for (index, record) in batch.records.iter().enumerate() {
        let offset = batch
            .after
            .offset
            .checked_add(index as u64 + 1)
            .ok_or_else(|| ReplicationError::InvalidInput("replica offset overflow".into()))?;
        if record.cursor.version != CURSOR_VERSION
            || record.cursor.stream != batch.after.stream.stream
            || record.cursor.offset != offset
        {
            return Err(ReplicationError::InvalidInput(
                "replica batch is not one contiguous stream prefix".into(),
            ));
        }
        bytes = bytes
            .checked_add(record_charge(record)?)
            .ok_or(ReplicationError::CapacityExceeded)?;
    }
    if bytes > max_bytes {
        return Err(ReplicationError::CapacityExceeded);
    }
    Ok(bytes)
}

fn origin_record(
    conn: &Connection,
    stream: &StreamKey,
    key: i64,
    offset: u64,
    max_bytes: usize,
) -> ReplicationResult<Arc<Record>> {
    conn.query_row("SELECT CASE WHEN typeof(event_id)='text' AND octet_length(event_id) BETWEEN 1 AND 256 THEN event_id END,CASE WHEN typeof(schema_id)='text' AND octet_length(schema_id) BETWEEN 1 AND 256 THEN schema_id END,CASE WHEN typeof(schema_version)='integer' THEN schema_version END,CASE WHEN typeof(payload)='blob' AND octet_length(payload)<=?3 THEN payload END FROM (SELECT event_id,schema_id,schema_version,payload FROM event_records WHERE stream_key=?1 AND offset=?2 UNION ALL SELECT event_id,schema_id,schema_version,payload FROM retention_generated_records WHERE stream_key=?1 AND offset=?2)",params![key,be(offset).as_slice(),i64::try_from(max_bytes).unwrap_or(i64::MAX)],|row|{
        let id:Option<String>=row.get(0)?;let schema:Option<String>=row.get(1)?;let version:Option<i64>=row.get(2)?;let payload:Option<Vec<u8>>=row.get(3)?;
        let id=EventId::new(id.ok_or(rusqlite::Error::InvalidQuery)?).map_err(|_|rusqlite::Error::InvalidQuery)?;
        let schema=SchemaId::new(schema.ok_or(rusqlite::Error::InvalidQuery)?).map_err(|_|rusqlite::Error::InvalidQuery)?;
        let version=u32::try_from(version.ok_or(rusqlite::Error::InvalidQuery)?).map_err(|_|rusqlite::Error::InvalidQuery)?;
        Ok(Arc::new(Record{cursor:Cursor::new(stream.clone(),offset),event:NewEvent{id,schema:SchemaRef{id:schema,version},payload:Payload::copy_from_slice(&payload.ok_or(rusqlite::Error::InvalidQuery)?)}}))
    }).map_err(|error|corrupt("read origin batch record",error))
}

fn status(
    conn: &Connection,
    replica: &ReplicaId,
    stream: &OriginStream,
) -> ReplicationResult<ReplicaStatus> {
    if origin_id(conn)? != stream.origin {
        return Err(ReplicationError::InvalidInput(
            "origin identity does not name this store".into(),
        ));
    }
    let row:Option<ReplicaStatusRow>=conn.query_row("SELECT destination_epoch,acknowledged,max_backlog_bytes,backlog_records,backlog_bytes,oldest_backlog_at,mode,pending_batch FROM replication_origin_replicas WHERE replica_id=?1 AND origin_id=?2 AND public_id=?3 AND incarnation=?4",params![replica.as_str(),stream.origin.0.as_slice(),stream.stream.id.as_str(),stream.stream.incarnation.0.as_slice()],|r|Ok((r.get(0)?,r.get(1)?,r.get(2)?,r.get(3)?,r.get(4)?,r.get(5)?,r.get(6)?,r.get(7)?))).optional().map_err(|e|corrupt("read replica status",e))?;
    let Some((epoch, ack, max_bytes, records, bytes, oldest, mode, pending)) = row else {
        return Err(ReplicationError::NotFound {
            replica: replica.clone(),
        });
    };
    let (_key, _, _tail) = stream_row(conn, &stream.stream)
        .map_err(|e| ReplicationError::CorruptStorage(e.to_string()))?;
    let acknowledged = decode(&ack, "replica acknowledgement")?;
    let records = if mode != 1 { records } else { 0 };
    let bytes = if mode != 1 {
        decode(&bytes, "replica backlog bytes")?
    } else {
        0
    };
    let max = decode(&max_bytes, "replica byte limit")?;
    if bytes > max {
        return Err(ReplicationError::BacklogExceeded { limit_bytes: max });
    }
    Ok(ReplicaStatus {
        replica: replica.clone(),
        stream: stream.clone(),
        destination_epoch: DestinationEpoch(
            epoch
                .try_into()
                .map_err(|_| ReplicationError::CorruptStorage("invalid replica epoch".into()))?,
        ),
        acknowledged: ReplicaPosition {
            stream: stream.clone(),
            offset: acknowledged,
        },
        backlog_records: usize::try_from(records)
            .map_err(|_| ReplicationError::CorruptStorage("invalid backlog count".into()))?,
        backlog_bytes: bytes,
        oldest_backlog_at: if mode != 1 { oldest } else { None }
            .map(|v| decode(&v, "backlog timestamp").map(DurableTimestampMillis))
            .transpose()?,
        mode: match mode {
            0 => ReplicaMode::Required,
            1 => ReplicaMode::DetachedNeedsBootstrap,
            2 => ReplicaMode::Bootstrapping,
            _ => {
                return Err(ReplicationError::CorruptStorage(
                    "invalid replica mode".into(),
                ))
            }
        },
        pending_batch: pending
            .map(|v| {
                v.try_into()
                    .map(BatchId)
                    .map_err(|_| ReplicationError::CorruptStorage("invalid pending batch".into()))
            })
            .transpose()?,
    })
}

fn attach(
    conn: &mut Connection,
    request: AttachReplica,
    options: &SqliteOptions,
) -> ReplicationResult<AttachReplicaReceipt> {
    if request.stream.origin != origin_id(conn)?
        || request.max_backlog_bytes == 0
        || request.max_backlog_age.is_zero()
    {
        return Err(ReplicationError::InvalidInput(
            "invalid replica attachment".into(),
        ));
    }
    let prior: Option<SavedAttachRow> = conn
        .query_row(
            "SELECT kind,replica_id,origin_id,public_id,incarnation,max_backlog_bytes,max_backlog_age_ms,start_mode,destination_epoch,status_ack,status_backlog_records,status_backlog_bytes,status_oldest,status_mode,status_pending FROM replication_origin_operations WHERE operation_id=?1",
            [request.operation_id.as_str()],
            |row| Ok((row.get(0)?,row.get(1)?,row.get(2)?,row.get(3)?,row.get(4)?,row.get(5)?,row.get(6)?,row.get(7)?,row.get(8)?,row.get(9)?,row.get(10)?,row.get(11)?,row.get(12)?,row.get(13)?,row.get(14)?)),
        )
        .optional()
        .map_err(|error| corrupt("read attach receipt", error))?;
    if let Some((
        kind,
        replica,
        origin,
        public_id,
        incarnation,
        max_bytes,
        max_age,
        start,
        epoch,
        status_ack,
        status_records,
        status_bytes,
        status_oldest,
        status_mode,
        status_pending,
    )) = prior
    {
        let requested_age = u64::try_from(request.max_backlog_age.as_millis()).map_err(|_| {
            ReplicationError::InvalidInput("backlog age exceeds durable range".into())
        })?;
        let requested_start = match request.start {
            ReplicaStart::FromBeginning => 0,
            ReplicaStart::NeedsBootstrap => 1,
        };
        let exact = kind == 0
            && replica == request.replica.as_str()
            && origin == request.stream.origin.0
            && public_id == request.stream.stream.id.as_str()
            && incarnation == request.stream.stream.incarnation.0
            && decode(&max_bytes, "saved replica byte limit")? == request.max_backlog_bytes
            && decode(&max_age, "saved replica age limit")? == requested_age
            && start == requested_start;
        return if exact {
            let saved_epoch = DestinationEpoch(epoch.try_into().map_err(|_| {
                ReplicationError::CorruptStorage("invalid saved destination epoch".into())
            })?);
            let saved_pending = status_pending
                .map(|value| {
                    value.try_into().map(BatchId).map_err(|_| {
                        ReplicationError::CorruptStorage("invalid saved pending batch".into())
                    })
                })
                .transpose()?;
            Ok(AttachReplicaReceipt {
                status: ReplicaStatus {
                    replica: request.replica.clone(),
                    stream: request.stream.clone(),
                    destination_epoch: saved_epoch,
                    acknowledged: ReplicaPosition {
                        stream: request.stream.clone(),
                        offset: decode(&status_ack, "saved attach acknowledgement")?,
                    },
                    backlog_records: usize::try_from(status_records).map_err(|_| {
                        ReplicationError::CorruptStorage("invalid saved backlog count".into())
                    })?,
                    backlog_bytes: decode(&status_bytes, "saved attach backlog bytes")?,
                    oldest_backlog_at: status_oldest
                        .map(|value| {
                            decode(&value, "saved attach backlog timestamp")
                                .map(DurableTimestampMillis)
                        })
                        .transpose()?,
                    mode: match status_mode {
                        0 => ReplicaMode::Required,
                        1 => ReplicaMode::DetachedNeedsBootstrap,
                        2 => ReplicaMode::Bootstrapping,
                        _ => {
                            return Err(ReplicationError::CorruptStorage(
                                "invalid saved replica mode".into(),
                            ))
                        }
                    },
                    pending_batch: saved_pending,
                },
                request,
            })
        } else {
            Err(ReplicationError::InvalidInput(
                "replication operation identity was reused".into(),
            ))
        };
    }
    if status(conn, &request.replica, &request.stream).is_ok() {
        return Err(ReplicationError::InvalidInput(
            "replica is already attached".into(),
        ));
    }
    let (key, floor, tail) = stream_row(conn, &request.stream.stream)
        .map_err(|e| ReplicationError::InvalidInput(e.to_string()))?;
    let (mode, ack) = match request.start {
        ReplicaStart::FromBeginning if floor == 0 => (0, 0),
        ReplicaStart::FromBeginning => return Err(ReplicationError::NeedsBootstrap),
        ReplicaStart::NeedsBootstrap => (1, tail),
    };
    let count: i64 = conn
        .query_row(
            "SELECT count(*) FROM replication_origin_replicas",
            [],
            |r| r.get(0),
        )
        .map_err(|e| corrupt("count replicas", e))?;
    let stream_count: i64 = conn
        .query_row(
            "SELECT count(*) FROM replication_origin_replicas WHERE origin_id=?1 AND public_id=?2 AND incarnation=?3",
            params![request.stream.origin.0.as_slice(),request.stream.stream.id.as_str(),request.stream.stream.incarnation.0.as_slice()],
            |row| row.get(0),
        )
        .map_err(|error| corrupt("count stream replicas", error))?;
    if usize::try_from(count)
        .ok()
        .is_none_or(|count| count >= options.replication.attachments.max_replicas)
        || usize::try_from(stream_count)
            .ok()
            .is_none_or(|count| count >= options.replication.attachments.max_replicas_per_stream)
    {
        return Err(ReplicationError::CapacityExceeded);
    }
    let (records,bytes):(i64,i64)=conn.query_row("SELECT count(*),coalesce(sum(octet_length(event_id)+octet_length(schema_id)+octet_length(payload)+384),0) FROM (SELECT event_id,schema_id,payload FROM event_records WHERE stream_key=?1 AND offset>?2 UNION ALL SELECT event_id,schema_id,payload FROM retention_generated_records WHERE stream_key=?1 AND offset>?2)",params![key,be(ack).as_slice()],|r|Ok((r.get(0)?,r.get(1)?))).map_err(|e|corrupt("measure initial backlog",e))?;
    if bytes as u64 > request.max_backlog_bytes {
        return Err(ReplicationError::BacklogExceeded {
            limit_bytes: request.max_backlog_bytes,
        });
    }
    let now = options.replication_clock.now();
    let last: Option<Vec<u8>> = conn
        .query_row(
            "SELECT last_clock FROM replication_metadata WHERE singleton=1",
            [],
            |row| row.get(0),
        )
        .map_err(|error| corrupt("read replication clock", error))?;
    if let Some(last) = last {
        let last = decode(&last, "replication clock")?;
        if now.0 < last {
            return Err(ReplicationError::ClockRollback {
                last: DurableTimestampMillis(last),
                observed: now,
            });
        }
    }
    let tx = conn
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .map_err(|e| storage("begin replica attach", e))?;
    tx.execute(
        "UPDATE replication_metadata SET last_clock=?1 WHERE singleton=1",
        [be(now.0).as_slice()],
    )
    .map_err(|error| storage("store replication clock", error))?;
    tx.execute(
        "INSERT OR IGNORE INTO replication_origin_record_times(stream_key,offset,committed_at) SELECT stream_key,offset,?1 FROM (SELECT stream_key,offset FROM event_records WHERE stream_key=?2 AND offset>?3 UNION ALL SELECT stream_key,offset FROM retention_generated_records WHERE stream_key=?2 AND offset>?3)",
        params![be(now.0).as_slice(), key, be(ack).as_slice()],
    )
    .map_err(|error| storage("initialize replica backlog times", error))?;
    tx.execute(
        "INSERT INTO replication_origin_replicas(replica_id,origin_id,public_id,incarnation,destination_epoch,acknowledged,max_backlog_bytes,max_backlog_age_ms,backlog_records,backlog_bytes,oldest_backlog_at,mode,pending_batch) VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,NULL)",
        params![
            request.replica.as_str(),
            request.stream.origin.0.as_slice(),
            request.stream.stream.id.as_str(),
            request.stream.stream.incarnation.0.as_slice(),
            request.destination_epoch.0.as_slice(),
            be(ack).as_slice(),
            be(request.max_backlog_bytes).as_slice(),
            be(
                u64::try_from(request.max_backlog_age.as_millis()).map_err(|_| {
                    ReplicationError::InvalidInput("backlog age exceeds durable range".into())
                })?
            )
            .as_slice(),
            records,
            be(u64::try_from(bytes).map_err(|_| ReplicationError::CapacityExceeded)?).as_slice(),
            if records > 0 { Some(be(now.0)) } else { None },
            mode
        ],
    )
    .map_err(|e| storage("store replica attachment", e))?;
    let result_status = status(&tx, &request.replica, &request.stream)?;
    tx.execute(
        "INSERT INTO replication_origin_operations(operation_id,kind,replica_id,origin_id,public_id,incarnation,result_through,destination_epoch,max_backlog_bytes,max_backlog_age_ms,start_mode,status_ack,status_backlog_records,status_backlog_bytes,status_oldest,status_mode,status_pending) VALUES(?1,0,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14,?15,?16)",
        params![
            request.operation_id.as_str(),
            request.replica.as_str(),
            request.stream.origin.0.as_slice(),
            request.stream.stream.id.as_str(),
            request.stream.stream.incarnation.0.as_slice(),
            be(ack).as_slice(),
            request.destination_epoch.0.as_slice(),
            be(request.max_backlog_bytes).as_slice(),
            be(u64::try_from(request.max_backlog_age.as_millis()).unwrap()).as_slice(),
            match request.start { ReplicaStart::FromBeginning => 0, ReplicaStart::NeedsBootstrap => 1 },
            be(result_status.acknowledged.offset).as_slice(),
            i64::try_from(result_status.backlog_records).map_err(|_| ReplicationError::CapacityExceeded)?,
            be(result_status.backlog_bytes).as_slice(),
            result_status.oldest_backlog_at.map(|timestamp| be(timestamp.0)),
            match result_status.mode { ReplicaMode::Required => 0, ReplicaMode::DetachedNeedsBootstrap => 1, ReplicaMode::Bootstrapping => 2 },
            result_status.pending_batch.map(|batch| batch.0)
        ],
    )
    .map_err(|e| storage("store attach receipt", e))?;
    tx.commit()
        .map_err(|_| ReplicationError::AttachUnknown(Box::new(request.clone())))?;
    Ok(AttachReplicaReceipt {
        status: result_status,
        request,
    })
}

fn prepare(
    conn: &mut Connection,
    request: PrepareReplicaBatch,
    options: &SqliteOptions,
) -> ReplicationResult<PrepareReplicaBatchReceipt> {
    if request.limits.max_records == 0
        || request.limits.max_records > options.replication.batches.max_batch_records
        || request.limits.max_bytes == 0
        || request.limits.max_bytes > options.replication.batches.max_batch_bytes
    {
        return Err(ReplicationError::InvalidConfig(
            "replica batch limits exceed adapter bounds".into(),
        ));
    }
    type SavedPrepare = (
        i64,
        String,
        Vec<u8>,
        String,
        Vec<u8>,
        Vec<u8>,
        Vec<u8>,
        Vec<u8>,
        Vec<u8>,
        i64,
        Vec<u8>,
        Vec<u8>,
        i64,
        Vec<u8>,
        Option<Vec<u8>>,
        i64,
        Option<Vec<u8>>,
    );
    let saved: Option<SavedPrepare> = conn
        .query_row(
            "SELECT kind,replica_id,origin_id,public_id,incarnation,destination_epoch,batch_id,expected_after,result_through,limit_records,limit_bytes,status_ack,status_backlog_records,status_backlog_bytes,status_oldest,status_mode,status_pending FROM replication_origin_operations WHERE operation_id=?1",
            [request.operation_id.as_str()],
            |row| Ok((row.get(0)?,row.get(1)?,row.get(2)?,row.get(3)?,row.get(4)?,row.get(5)?,row.get(6)?,row.get(7)?,row.get(8)?,row.get(9)?,row.get(10)?,row.get(11)?,row.get(12)?,row.get(13)?,row.get(14)?,row.get(15)?,row.get(16)?)),
        )
        .optional()
        .map_err(|error| corrupt("read prepare receipt", error))?;
    if let Some((
        kind,
        replica,
        origin,
        public_id,
        incarnation,
        epoch,
        batch_id,
        expected,
        through,
        limit_records,
        limit_bytes,
        status_ack,
        status_records,
        status_bytes,
        status_oldest,
        status_mode,
        status_pending,
    )) = saved
    {
        let exact = kind == 1
            && replica == request.replica.as_str()
            && origin == request.stream.origin.0
            && public_id == request.stream.stream.id.as_str()
            && incarnation == request.stream.stream.incarnation.0
            && batch_id == request.batch_id.0
            && decode(&expected, "saved prepare position")? == request.expected_after.offset
            && usize::try_from(limit_records).ok() == Some(request.limits.max_records)
            && decode(&limit_bytes, "saved prepare byte limit")?
                == u64::try_from(request.limits.max_bytes)
                    .map_err(|_| ReplicationError::CapacityExceeded)?;
        if !exact {
            return Err(ReplicationError::InvalidInput(
                "replication operation identity was reused".into(),
            ));
        }
        let through = decode(&through, "saved prepare result")?;
        let (key, _, _) = stream_row(conn, &request.stream.stream)
            .map_err(|error| ReplicationError::CorruptStorage(error.to_string()))?;
        let mut records = Vec::new();
        for offset in request.expected_after.offset.saturating_add(1)..=through {
            records.push(origin_record(
                conn,
                &request.stream.stream,
                key,
                offset,
                options.max_record_bytes,
            )?);
        }
        let saved_epoch = DestinationEpoch(epoch.try_into().map_err(|_| {
            ReplicationError::CorruptStorage("invalid saved destination epoch".into())
        })?);
        let batch = (!records.is_empty()).then(|| ReplicaBatch {
            id: request.batch_id,
            destination_epoch: saved_epoch,
            after: request.expected_after.clone(),
            records,
        });
        let saved_pending = status_pending
            .map(|value| {
                value.try_into().map(BatchId).map_err(|_| {
                    ReplicationError::CorruptStorage("invalid saved pending batch".into())
                })
            })
            .transpose()?;
        let status = ReplicaStatus {
            replica: request.replica.clone(),
            stream: request.stream.clone(),
            destination_epoch: saved_epoch,
            acknowledged: ReplicaPosition {
                stream: request.stream.clone(),
                offset: decode(&status_ack, "saved status acknowledgement")?,
            },
            backlog_records: usize::try_from(status_records).map_err(|_| {
                ReplicationError::CorruptStorage("invalid saved backlog count".into())
            })?,
            backlog_bytes: decode(&status_bytes, "saved backlog bytes")?,
            oldest_backlog_at: status_oldest
                .map(|value| decode(&value, "saved backlog timestamp").map(DurableTimestampMillis))
                .transpose()?,
            mode: match status_mode {
                0 => ReplicaMode::Required,
                1 => ReplicaMode::DetachedNeedsBootstrap,
                2 => ReplicaMode::Bootstrapping,
                _ => {
                    return Err(ReplicationError::CorruptStorage(
                        "invalid saved replica mode".into(),
                    ))
                }
            },
            pending_batch: saved_pending,
        };
        return Ok(PrepareReplicaBatchReceipt {
            request,
            batch,
            status,
        });
    }
    let current = status(conn, &request.replica, &request.stream)?;
    if current.mode != ReplicaMode::Required {
        return Err(ReplicationError::NeedsBootstrap);
    }
    if current.acknowledged != request.expected_after {
        return Err(ReplicationError::StaleProgress {
            current: Box::new(current.acknowledged),
        });
    }
    if let Some(batch) = current.pending_batch {
        return if batch == request.batch_id {
            Err(ReplicationError::BatchConflict { batch })
        } else {
            Err(ReplicationError::CapacityExceeded)
        };
    }
    let (key, _, tail) = stream_row(conn, &request.stream.stream)
        .map_err(|e| ReplicationError::CorruptStorage(e.to_string()))?;
    let mut records = Vec::new();
    let mut bytes = 0usize;
    let mut offset = request.expected_after.offset;
    while offset < tail && records.len() < request.limits.max_records {
        let next = offset + 1;
        let record = origin_record(
            conn,
            &request.stream.stream,
            key,
            next,
            options.max_record_bytes,
        )?;
        let charge = record_charge(&record)?;
        if records.is_empty() && charge > request.limits.max_bytes {
            return Err(ReplicationError::CapacityExceeded);
        }
        if bytes + charge > request.limits.max_bytes {
            break;
        }
        bytes += charge;
        records.push(record);
        offset = next;
    }
    let batch = (!records.is_empty()).then(|| ReplicaBatch {
        id: request.batch_id,
        destination_epoch: current.destination_epoch,
        after: request.expected_after.clone(),
        records,
    });
    let batch_records = batch.as_ref().map_or(0, |batch| batch.records.len());
    let tx = conn
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .map_err(|e| storage("begin replica prepare", e))?;
    if batch.is_some() {
        tx.execute("UPDATE replication_origin_replicas SET pending_batch=?1 WHERE replica_id=?2 AND origin_id=?3 AND public_id=?4 AND incarnation=?5",params![request.batch_id.0.as_slice(),request.replica.as_str(),request.stream.origin.0.as_slice(),request.stream.stream.id.as_str(),request.stream.stream.incarnation.0.as_slice()]).map_err(|e|storage("store pending replica batch",e))?;
    }
    tx.execute(
        "INSERT INTO replication_origin_operations(operation_id,kind,replica_id,origin_id,public_id,incarnation,batch_id,expected_after,result_through,destination_epoch,limit_records,limit_bytes,status_ack,status_backlog_records,status_backlog_bytes,status_oldest,status_mode,status_pending,result_batch_records,result_batch_bytes) VALUES(?1,1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14,?15,?16,?17,?18,?19)",
        params![
            request.operation_id.as_str(),
            request.replica.as_str(),
            request.stream.origin.0.as_slice(),
            request.stream.stream.id.as_str(),
            request.stream.stream.incarnation.0.as_slice(),
            request.batch_id.0.as_slice(),
            be(request.expected_after.offset).as_slice(),
            be(offset).as_slice(),
            current.destination_epoch.0.as_slice(),
            i64::try_from(request.limits.max_records).map_err(|_| ReplicationError::CapacityExceeded)?,
            be(u64::try_from(request.limits.max_bytes).map_err(|_| ReplicationError::CapacityExceeded)?).as_slice(),
            be(current.acknowledged.offset).as_slice(),
            i64::try_from(current.backlog_records).map_err(|_| ReplicationError::CapacityExceeded)?,
            be(current.backlog_bytes).as_slice(),
            current.oldest_backlog_at.map(|timestamp| be(timestamp.0)),
            match current.mode { ReplicaMode::Required => 0, ReplicaMode::DetachedNeedsBootstrap => 1, ReplicaMode::Bootstrapping => 2 },
            batch.as_ref().map(|batch| batch.id.0),
            i64::try_from(batch_records).map_err(|_| ReplicationError::CapacityExceeded)?,
            be(u64::try_from(bytes).map_err(|_| ReplicationError::CapacityExceeded)?).as_slice()
        ],
    )
    .map_err(|e| storage("store prepare receipt", e))?;
    tx.commit()
        .map_err(|_| ReplicationError::PrepareUnknown(Box::new(request.clone())))?;
    Ok(PrepareReplicaBatchReceipt {
        status: status(conn, &request.replica, &request.stream)?,
        request,
        batch,
    })
}

fn acknowledge(
    conn: &mut Connection,
    request: AcknowledgeReplicaBatch,
) -> ReplicationResult<AcknowledgeReplicaBatchReceipt> {
    type SavedAcknowledge = (
        i64,
        String,
        Vec<u8>,
        String,
        Vec<u8>,
        Vec<u8>,
        Vec<u8>,
        Vec<u8>,
        Vec<u8>,
        Vec<u8>,
        i64,
        Vec<u8>,
        Option<Vec<u8>>,
        i64,
        Option<Vec<u8>>,
    );
    let saved: Option<SavedAcknowledge> = conn
        .query_row(
            "SELECT kind,replica_id,origin_id,public_id,incarnation,batch_id,expected_after,result_through,destination_epoch,status_ack,status_backlog_records,status_backlog_bytes,status_oldest,status_mode,status_pending FROM replication_origin_operations WHERE operation_id=?1",
            [request.operation_id.as_str()],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?, row.get(4)?, row.get(5)?, row.get(6)?, row.get(7)?, row.get(8)?, row.get(9)?, row.get(10)?, row.get(11)?, row.get(12)?, row.get(13)?, row.get(14)?)),
        )
        .optional()
        .map_err(|error| corrupt("read acknowledgement receipt", error))?;
    if let Some((
        kind,
        replica,
        origin,
        public_id,
        incarnation,
        batch,
        expected,
        through,
        epoch,
        status_ack,
        status_records,
        status_bytes,
        status_oldest,
        status_mode,
        status_pending,
    )) = saved
    {
        let exact = kind == 2
            && replica == request.replica.as_str()
            && origin == request.expected_after.stream.origin.0
            && public_id == request.expected_after.stream.stream.id.as_str()
            && incarnation == request.expected_after.stream.stream.incarnation.0
            && batch == request.receipt.batch.0
            && epoch == request.receipt.destination_epoch.0
            && request.receipt.committed_through.stream == request.expected_after.stream
            && decode(&expected, "acknowledgement expected position")?
                == request.expected_after.offset
            && decode(&through, "acknowledgement result position")?
                == request.receipt.committed_through.offset;
        if !exact {
            return Err(ReplicationError::InvalidInput(
                "operation identity was already used for a different acknowledgement".into(),
            ));
        }
        let saved_pending = status_pending
            .map(|value| {
                value.try_into().map(BatchId).map_err(|_| {
                    ReplicationError::CorruptStorage("invalid saved pending batch".into())
                })
            })
            .transpose()?;
        let saved_status = ReplicaStatus {
            replica: request.replica.clone(),
            stream: request.expected_after.stream.clone(),
            destination_epoch: request.receipt.destination_epoch,
            acknowledged: ReplicaPosition {
                stream: request.expected_after.stream.clone(),
                offset: decode(&status_ack, "saved acknowledgement status")?,
            },
            backlog_records: usize::try_from(status_records).map_err(|_| {
                ReplicationError::CorruptStorage("invalid saved backlog count".into())
            })?,
            backlog_bytes: decode(&status_bytes, "saved backlog bytes")?,
            oldest_backlog_at: status_oldest
                .map(|value| decode(&value, "saved oldest backlog").map(DurableTimestampMillis))
                .transpose()?,
            mode: match status_mode {
                0 => ReplicaMode::Required,
                1 => ReplicaMode::DetachedNeedsBootstrap,
                2 => ReplicaMode::Bootstrapping,
                _ => {
                    return Err(ReplicationError::CorruptStorage(
                        "invalid saved replica mode".into(),
                    ))
                }
            },
            pending_batch: saved_pending,
        };
        return Ok(AcknowledgeReplicaBatchReceipt {
            status: saved_status,
            request,
        });
    }
    let current = status(conn, &request.replica, &request.expected_after.stream)?;
    if current.acknowledged != request.expected_after {
        return Err(ReplicationError::StaleProgress {
            current: Box::new(current.acknowledged),
        });
    }
    if current.destination_epoch != request.receipt.destination_epoch {
        return Err(ReplicationError::DestinationReplaced {
            current: current.destination_epoch,
        });
    }
    let prepared: Option<(Vec<u8>, i64, Vec<u8>)> = conn
        .query_row(
            "SELECT result_through,result_batch_records,result_batch_bytes FROM replication_origin_operations WHERE kind=1 AND replica_id=?1 AND origin_id=?2 AND public_id=?3 AND incarnation=?4 AND batch_id=?5 ORDER BY rowid DESC LIMIT 1",
            params![request.replica.as_str(),request.expected_after.stream.origin.0.as_slice(),request.expected_after.stream.stream.id.as_str(),request.expected_after.stream.stream.incarnation.0.as_slice(),request.receipt.batch.0.as_slice()],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .optional()
        .map_err(|error| corrupt("read prepared replica boundary", error))?;
    let prepared_through = prepared
        .as_ref()
        .map(|(value, _, _)| decode(value, "prepared replica boundary"))
        .transpose()?;
    if current.pending_batch != Some(request.receipt.batch)
        || request.receipt.committed_through.stream != request.expected_after.stream
        || prepared_through != Some(request.receipt.committed_through.offset)
    {
        return Err(ReplicationError::InvalidReceipt(
            "receipt does not match pending batch".into(),
        ));
    }
    let (_, _, origin_tail) = stream_row(conn, &request.expected_after.stream.stream)
        .map_err(|error| ReplicationError::CorruptStorage(error.to_string()))?;
    let (_, acknowledged_records, acknowledged_bytes) = prepared.ok_or_else(|| {
        ReplicationError::CorruptStorage("pending replica batch has no durable receipt".into())
    })?;
    let acknowledged_bytes = decode(&acknowledged_bytes, "prepared replica bytes")?;
    let acknowledged_records = usize::try_from(acknowledged_records)
        .map_err(|_| ReplicationError::CorruptStorage("invalid prepared replica count".into()))?;
    let tx = conn
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .map_err(|e| storage("begin replica acknowledgement", e))?;
    let next_oldest: Option<Vec<u8>> = if request.receipt.committed_through.offset < origin_tail {
        let (stream_key, _, _) = stream_row(&tx, &request.expected_after.stream.stream)
            .map_err(|error| ReplicationError::CorruptStorage(error.to_string()))?;
        Some(tx.query_row(
            "SELECT committed_at FROM replication_origin_record_times WHERE stream_key=?1 AND offset=?2",
            params![stream_key, be(request.receipt.committed_through.offset + 1).as_slice()],
            |row| row.get(0),
        ).optional().map_err(|error| corrupt("read next replica backlog time", error))?
            .ok_or_else(|| ReplicationError::CorruptStorage("replica backlog timestamp is missing".into()))?)
    } else {
        None
    };
    let changed = tx.execute("UPDATE replication_origin_replicas SET acknowledged=?1,pending_batch=NULL,backlog_records=backlog_records-?2,backlog_bytes=?3,oldest_backlog_at=?4 WHERE replica_id=?5 AND origin_id=?6 AND public_id=?7 AND incarnation=?8 AND backlog_records>=?2",params![be(request.receipt.committed_through.offset).as_slice(),i64::try_from(acknowledged_records).map_err(|_| ReplicationError::CapacityExceeded)?,be(current.backlog_bytes.checked_sub(acknowledged_bytes).ok_or_else(|| ReplicationError::CorruptStorage("replica backlog byte underflow".into()))?).as_slice(),next_oldest,request.replica.as_str(),request.expected_after.stream.origin.0.as_slice(),request.expected_after.stream.stream.id.as_str(),request.expected_after.stream.stream.incarnation.0.as_slice()]).map_err(|e|storage("store replica acknowledgement",e))?;
    if changed != 1 {
        return Err(ReplicationError::CorruptStorage(
            "replica backlog row underflow".into(),
        ));
    }
    let result_status = status(&tx, &request.replica, &request.expected_after.stream)?;
    tx.execute(
        "INSERT INTO replication_origin_operations(operation_id,kind,replica_id,origin_id,public_id,incarnation,batch_id,expected_after,result_through,destination_epoch,status_ack,status_backlog_records,status_backlog_bytes,status_oldest,status_mode,status_pending) VALUES(?1,2,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14,?15)",
        params![
            request.operation_id.as_str(),
            request.replica.as_str(),
            request.expected_after.stream.origin.0.as_slice(),
            request.expected_after.stream.stream.id.as_str(),
            request
                .expected_after
                .stream
                .stream
                .incarnation
                .0
                .as_slice(),
            request.receipt.batch.0.as_slice(),
            be(request.expected_after.offset).as_slice(),
            be(request.receipt.committed_through.offset).as_slice(),
            request.receipt.destination_epoch.0.as_slice(),
            be(result_status.acknowledged.offset).as_slice(),
            i64::try_from(result_status.backlog_records).map_err(|_| ReplicationError::CapacityExceeded)?,
            be(result_status.backlog_bytes).as_slice(),
            result_status.oldest_backlog_at.map(|timestamp| be(timestamp.0)),
            match result_status.mode { ReplicaMode::Required => 0, ReplicaMode::DetachedNeedsBootstrap => 1, ReplicaMode::Bootstrapping => 2 },
            result_status.pending_batch.map(|batch| batch.0)
        ],
    )
    .map_err(|e| storage("store acknowledgement receipt", e))?;
    tx.commit()
        .map_err(|_| ReplicationError::AcknowledgeUnknown(Box::new(request.clone())))?;
    Ok(AcknowledgeReplicaBatchReceipt {
        status: result_status,
        request,
    })
}

fn detach(
    conn: &mut Connection,
    request: DetachReplica,
) -> ReplicationResult<DetachReplicaReceipt> {
    type SavedDetach = (
        i64,
        String,
        Vec<u8>,
        String,
        Vec<u8>,
        Vec<u8>,
        Vec<u8>,
        i64,
        Vec<u8>,
        Option<Vec<u8>>,
        i64,
        Option<Vec<u8>>,
    );
    let saved: Option<SavedDetach> = conn
        .query_row(
            "SELECT kind,replica_id,origin_id,public_id,incarnation,destination_epoch,status_ack,status_backlog_records,status_backlog_bytes,status_oldest,status_mode,status_pending FROM replication_origin_operations WHERE operation_id=?1",
            [request.operation_id.as_str()],
            |row| Ok((row.get(0)?,row.get(1)?,row.get(2)?,row.get(3)?,row.get(4)?,row.get(5)?,row.get(6)?,row.get(7)?,row.get(8)?,row.get(9)?,row.get(10)?,row.get(11)?)),
        )
        .optional()
        .map_err(|error| corrupt("read detach receipt", error))?;
    if let Some((
        kind,
        replica,
        origin,
        public_id,
        incarnation,
        epoch,
        ack,
        records,
        bytes,
        oldest,
        mode,
        pending,
    )) = saved
    {
        let exact = kind == 3
            && replica == request.replica.as_str()
            && origin == request.stream.origin.0
            && public_id == request.stream.stream.id.as_str()
            && incarnation == request.stream.stream.incarnation.0;
        if !exact {
            return Err(ReplicationError::InvalidInput(
                "replication operation identity was reused".into(),
            ));
        }
        let pending = pending
            .map(|value| {
                value.try_into().map(BatchId).map_err(|_| {
                    ReplicationError::CorruptStorage("invalid saved pending batch".into())
                })
            })
            .transpose()?;
        return Ok(DetachReplicaReceipt {
            status: ReplicaStatus {
                replica: request.replica.clone(),
                stream: request.stream.clone(),
                destination_epoch: DestinationEpoch(epoch.try_into().map_err(|_| {
                    ReplicationError::CorruptStorage("invalid saved destination epoch".into())
                })?),
                acknowledged: ReplicaPosition {
                    stream: request.stream.clone(),
                    offset: decode(&ack, "saved detach acknowledgement")?,
                },
                backlog_records: usize::try_from(records).map_err(|_| {
                    ReplicationError::CorruptStorage("invalid saved backlog count".into())
                })?,
                backlog_bytes: decode(&bytes, "saved detach backlog bytes")?,
                oldest_backlog_at: oldest
                    .map(|value| {
                        decode(&value, "saved detach backlog timestamp").map(DurableTimestampMillis)
                    })
                    .transpose()?,
                mode: match mode {
                    0 => ReplicaMode::Required,
                    1 => ReplicaMode::DetachedNeedsBootstrap,
                    2 => ReplicaMode::Bootstrapping,
                    _ => {
                        return Err(ReplicationError::CorruptStorage(
                            "invalid saved replica mode".into(),
                        ))
                    }
                },
                pending_batch: pending,
            },
            request,
        });
    }
    let current = status(conn, &request.replica, &request.stream)?;
    let tx = conn
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .map_err(|error| storage("begin replica detach", error))?;
    let changed = tx.execute(
        "UPDATE replication_origin_replicas SET backlog_records=0,backlog_bytes=zeroblob(8),oldest_backlog_at=NULL,mode=1,pending_batch=NULL WHERE replica_id=?1 AND origin_id=?2 AND public_id=?3 AND incarnation=?4",
        params![request.replica.as_str(),request.stream.origin.0.as_slice(),request.stream.stream.id.as_str(),request.stream.stream.incarnation.0.as_slice()],
    ).map_err(|error| storage("detach replica", error))?;
    if changed != 1 {
        return Err(ReplicationError::NotFound {
            replica: request.replica.clone(),
        });
    }
    let result_status = status(&tx, &request.replica, &request.stream)?;
    tx.execute(
        "INSERT INTO replication_origin_operations(operation_id,kind,replica_id,origin_id,public_id,incarnation,destination_epoch,status_ack,status_backlog_records,status_backlog_bytes,status_oldest,status_mode,status_pending) VALUES(?1,3,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12)",
        params![request.operation_id.as_str(),request.replica.as_str(),request.stream.origin.0.as_slice(),request.stream.stream.id.as_str(),request.stream.stream.incarnation.0.as_slice(),current.destination_epoch.0.as_slice(),be(result_status.acknowledged.offset).as_slice(),i64::try_from(result_status.backlog_records).map_err(|_| ReplicationError::CapacityExceeded)?,be(result_status.backlog_bytes).as_slice(),result_status.oldest_backlog_at.map(|timestamp|be(timestamp.0)),match result_status.mode { ReplicaMode::Required=>0,ReplicaMode::DetachedNeedsBootstrap=>1,ReplicaMode::Bootstrapping=>2 },result_status.pending_batch.map(|batch|batch.0)],
    ).map_err(|error| storage("store detach receipt", error))?;
    tx.commit()
        .map_err(|_| ReplicationError::DetachUnknown(Box::new(request.clone())))?;
    Ok(DetachReplicaReceipt {
        request,
        status: result_status,
    })
}

fn destination_record(
    row: &rusqlite::Row<'_>,
    stream: &OriginStream,
) -> rusqlite::Result<Arc<Record>> {
    let offset: Option<Vec<u8>> = row.get(0)?;
    let id: Option<String> = row.get(1)?;
    let schema: Option<String> = row.get(2)?;
    let version: Option<i64> = row.get(3)?;
    let payload: Option<Vec<u8>> = row.get(4)?;
    Ok(Arc::new(Record {
        cursor: Cursor::new(
            stream.stream.clone(),
            decode_offset(&offset.ok_or(rusqlite::Error::InvalidQuery)?)
                .map_err(|_| rusqlite::Error::InvalidQuery)?,
        ),
        event: NewEvent {
            id: EventId::new(id.ok_or(rusqlite::Error::InvalidQuery)?)
                .map_err(|_| rusqlite::Error::InvalidQuery)?,
            schema: SchemaRef {
                id: SchemaId::new(schema.ok_or(rusqlite::Error::InvalidQuery)?)
                    .map_err(|_| rusqlite::Error::InvalidQuery)?,
                version: u32::try_from(version.ok_or(rusqlite::Error::InvalidQuery)?)
                    .map_err(|_| rusqlite::Error::InvalidQuery)?,
            },
            payload: Payload::copy_from_slice(&payload.ok_or(rusqlite::Error::InvalidQuery)?),
        },
    }))
}

fn commit_destination(
    conn: &mut Connection,
    batch: ReplicaBatch,
    options: &SqliteOptions,
    failure_injection: &mut Option<super::sqlite::SqliteFailureInjection>,
) -> ReplicationResult<ReplicaReceipt> {
    if batch.destination_epoch != destination_epoch(conn)? {
        return Err(ReplicationError::DestinationReplaced {
            current: destination_epoch(conn)?,
        });
    }
    let batch_bytes = validate_batch(
        &batch,
        options.replica_destination.storage.max_batch_records,
        options.replica_destination.storage.max_batch_bytes,
    )?;
    if let Some((epoch,origin,id,inc,after,through))=conn.query_row("SELECT destination_epoch,origin_id,public_id,incarnation,after_offset,through_offset FROM replication_destination_receipts WHERE batch_id=?1",[batch.id.0.as_slice()],|r|Ok((r.get::<_,Vec<u8>>(0)?,r.get::<_,Vec<u8>>(1)?,r.get::<_,String>(2)?,r.get::<_,Vec<u8>>(3)?,r.get::<_,Vec<u8>>(4)?,r.get::<_,Vec<u8>>(5)?))).optional().map_err(|e|corrupt("read replica receipt",e))?{let exact=epoch==batch.destination_epoch.0&&origin==batch.after.stream.origin.0&&id==batch.after.stream.stream.id.as_str()&&inc==batch.after.stream.stream.incarnation.0&&decode(&after,"receipt after")?==batch.after.offset&&decode(&through,"receipt through")?==batch.records.last().unwrap().cursor.offset;if !exact{return Err(ReplicationError::BatchConflict{batch:batch.id})}for record in &batch.records{let saved:Arc<Record>=conn.query_row("SELECT offset,event_id,schema_id,schema_version,payload FROM replication_destination_records WHERE origin_id=?1 AND public_id=?2 AND incarnation=?3 AND offset=?4",params![batch.after.stream.origin.0.as_slice(),batch.after.stream.stream.id.as_str(),batch.after.stream.stream.incarnation.0.as_slice(),be(record.cursor.offset).as_slice()],|row|destination_record(row,&batch.after.stream)).map_err(|e|corrupt("validate replica retry",e))?;if saved.event!=record.event{return Err(ReplicationError::BatchConflict{batch:batch.id})}}return Ok(ReplicaReceipt{batch:batch.id,destination_epoch:batch.destination_epoch,committed_through:ReplicaPosition{stream:batch.after.stream,offset:decode(&through,"receipt through")?}})}
    let bounds:Option<(Vec<u8>,Vec<u8>)>=conn.query_row("SELECT floor,tail FROM replication_destination_streams WHERE origin_id=?1 AND public_id=?2 AND incarnation=?3",params![batch.after.stream.origin.0.as_slice(),batch.after.stream.stream.id.as_str(),batch.after.stream.stream.incarnation.0.as_slice()],|r|Ok((r.get(0)?,r.get(1)?))).optional().map_err(|e|corrupt("read destination bounds",e))?;
    let new_stream = usize::from(bounds.is_none());
    let (floor, tail) = match bounds {
        Some((floor, tail)) => (
            decode(&floor, "destination floor")?,
            decode(&tail, "destination tail")?,
        ),
        None => (0, 0),
    };
    if batch.after.offset < floor {
        return Err(ReplicationError::ReceiptExpired);
    }
    if tail != batch.after.offset {
        return Err(ReplicationError::StaleProgress {
            current: Box::new(ReplicaPosition {
                stream: batch.after.stream.clone(),
                offset: tail,
            }),
        });
    }
    let (stream_count, history_records, history_bytes, receipt_count, receipt_bytes): (
        i64,
        i64,
        Vec<u8>,
        i64,
        Vec<u8>,
    ) = conn
        .query_row(
            "SELECT stream_count,history_records,history_bytes,receipt_count,receipt_bytes FROM replication_destination_accounting WHERE singleton=1",
            [],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?, row.get(4)?)),
        )
        .map_err(|error| corrupt("read destination accounting", error))?;
    let stream_count = usize::try_from(stream_count)
        .map_err(|_| ReplicationError::CorruptStorage("invalid destination stream count".into()))?;
    let history_records = usize::try_from(history_records)
        .map_err(|_| ReplicationError::CorruptStorage("invalid destination record count".into()))?;
    let history_bytes = decode(&history_bytes, "destination history bytes")?;
    let receipt_count = usize::try_from(receipt_count).map_err(|_| {
        ReplicationError::CorruptStorage("invalid destination receipt count".into())
    })?;
    let receipt_bytes = decode(&receipt_bytes, "destination receipt bytes")?;
    let receipt_charge = batch_receipt_charge_from_parts(
        batch.after.stream.stream.id.as_str(),
        batch.after.offset,
        batch.records.last().expect("validated batch").cursor.offset,
    )
    .map_err(|_| ReplicationError::CapacityExceeded)?;
    let next_history_bytes = history_bytes
        .checked_add(u64::try_from(batch_bytes).map_err(|_| ReplicationError::CapacityExceeded)?)
        .ok_or(ReplicationError::CapacityExceeded)?;
    let next_receipt_bytes = receipt_bytes
        .checked_add(receipt_charge)
        .ok_or(ReplicationError::CapacityExceeded)?;
    if stream_count
        .checked_add(new_stream)
        .is_none_or(|count| count > options.replica_destination.storage.max_origin_streams)
        || history_records
            .checked_add(batch.records.len())
            .is_none_or(|count| count > options.replica_destination.storage.max_history_records)
        || next_history_bytes > options.replica_destination.storage.max_history_bytes
        || receipt_count
            .checked_add(1)
            .is_none_or(|count| count > options.replica_destination.receipts.max_batch_receipts)
        || next_receipt_bytes
            > u64::try_from(options.replica_destination.receipts.max_batch_receipt_bytes)
                .unwrap_or(u64::MAX)
    {
        return Err(ReplicationError::CapacityExceeded);
    }
    let tx = conn
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .map_err(|e| storage("begin destination batch", e))?;
    if matches!(
        failure_injection,
        Some(super::sqlite::SqliteFailureInjection::BeforeReplicaCommit)
    ) {
        *failure_injection = None;
        return Err(ReplicationError::StorageFailure(
            "injected failure before replica commit".into(),
        ));
    }
    tx.execute("INSERT OR IGNORE INTO replication_destination_streams VALUES(?1,?2,?3,zeroblob(8),zeroblob(8))",params![batch.after.stream.origin.0.as_slice(),batch.after.stream.stream.id.as_str(),batch.after.stream.stream.incarnation.0.as_slice()]).map_err(|e|storage("create destination stream",e))?;
    for record in &batch.records {
        tx.execute(
            "INSERT INTO replication_destination_records VALUES(?1,?2,?3,?4,?5,?6,?7,?8)",
            params![
                batch.after.stream.origin.0.as_slice(),
                batch.after.stream.stream.id.as_str(),
                batch.after.stream.stream.incarnation.0.as_slice(),
                be(record.cursor.offset).as_slice(),
                record.event.id.as_str(),
                record.event.schema.id.as_str(),
                i64::from(record.event.schema.version),
                record.event.payload.as_bytes()
            ],
        )
        .map_err(|e| storage("insert destination record", e))?;
    }
    let through = batch.records.last().unwrap().cursor.offset;
    tx.execute("UPDATE replication_destination_streams SET tail=?1 WHERE origin_id=?2 AND public_id=?3 AND incarnation=?4",params![be(through).as_slice(),batch.after.stream.origin.0.as_slice(),batch.after.stream.stream.id.as_str(),batch.after.stream.stream.incarnation.0.as_slice()]).map_err(|e|storage("advance destination tail",e))?;
    tx.execute(
        "INSERT INTO replication_destination_receipts VALUES(?1,?2,?3,?4,?5,?6,?7)",
        params![
            batch.id.0.as_slice(),
            batch.destination_epoch.0.as_slice(),
            batch.after.stream.origin.0.as_slice(),
            batch.after.stream.stream.id.as_str(),
            batch.after.stream.stream.incarnation.0.as_slice(),
            be(batch.after.offset).as_slice(),
            be(through).as_slice()
        ],
    )
    .map_err(|e| storage("store destination receipt", e))?;
    tx.execute(
        "UPDATE replication_destination_accounting SET stream_count=stream_count+?1,history_records=history_records+?2,history_bytes=?3,receipt_count=receipt_count+1,receipt_bytes=?4 WHERE singleton=1",
        params![
            i64::try_from(new_stream).map_err(|_| ReplicationError::CapacityExceeded)?,
            i64::try_from(batch.records.len()).map_err(|_| ReplicationError::CapacityExceeded)?,
            be(next_history_bytes).as_slice(),
            be(next_receipt_bytes).as_slice()
        ],
    )
    .map_err(|error| storage("update destination accounting", error))?;
    tx.commit()
        .map_err(|_| ReplicationError::CommitUnknown(Box::new(batch.clone())))?;
    if matches!(
        failure_injection,
        Some(super::sqlite::SqliteFailureInjection::AfterReplicaCommitAcknowledgementLost)
    ) {
        *failure_injection = None;
        return Err(ReplicationError::CommitUnknown(Box::new(batch)));
    }
    Ok(ReplicaReceipt {
        batch: batch.id,
        destination_epoch: batch.destination_epoch,
        committed_through: ReplicaPosition {
            stream: batch.after.stream,
            offset: through,
        },
    })
}

fn read_destination(
    conn: &Connection,
    after: ReplicaPosition,
    limits: ReplicaBatchLimits,
    options: &SqliteOptions,
) -> ReplicationResult<ReplicaPage> {
    if limits.max_records == 0
        || limits.max_records > options.replica_destination.storage.max_batch_records
        || limits.max_bytes == 0
        || limits.max_bytes > options.replica_destination.storage.max_batch_bytes
    {
        return Err(ReplicationError::InvalidConfig(
            "replica page limits exceed adapter bounds".into(),
        ));
    }
    let tail:Vec<u8>=conn.query_row("SELECT tail FROM replication_destination_streams WHERE origin_id=?1 AND public_id=?2 AND incarnation=?3",params![after.stream.origin.0.as_slice(),after.stream.stream.id.as_str(),after.stream.stream.incarnation.0.as_slice()],|r|r.get(0)).map_err(|e|corrupt("read replica stream",e))?;
    let tail = decode(&tail, "replica tail")?;
    if after.offset > tail {
        return Err(ReplicationError::StaleProgress {
            current: Box::new(ReplicaPosition {
                stream: after.stream.clone(),
                offset: tail,
            }),
        });
    }
    let mut stmt=conn.prepare("SELECT offset,event_id,schema_id,schema_version,payload FROM replication_destination_records WHERE origin_id=?1 AND public_id=?2 AND incarnation=?3 AND offset>?4 ORDER BY offset LIMIT ?5").map_err(|e|corrupt("prepare replica page",e))?;
    let mut rows = stmt
        .query(params![
            after.stream.origin.0.as_slice(),
            after.stream.stream.id.as_str(),
            after.stream.stream.incarnation.0.as_slice(),
            be(after.offset).as_slice(),
            i64::try_from(limits.max_records).unwrap_or(i64::MAX)
        ])
        .map_err(|e| corrupt("query replica page", e))?;
    let mut records = Vec::new();
    let mut bytes = 0usize;
    while let Some(row) = rows.next().map_err(|e| corrupt("step replica page", e))? {
        let record = destination_record(row, &after.stream)
            .map_err(|e| corrupt("decode replica page", e))?;
        let charge = record_charge(&record)?;
        if records.is_empty() && charge > limits.max_bytes {
            return Err(ReplicationError::CapacityExceeded);
        }
        if bytes + charge > limits.max_bytes {
            break;
        }
        bytes += charge;
        records.push(record)
    }
    let next = records
        .last()
        .map(|r| r.cursor.offset)
        .unwrap_or(after.offset);
    Ok(ReplicaPage {
        after: after.clone(),
        records,
        next: ReplicaPosition {
            stream: after.stream,
            offset: next,
        },
        complete: next == tail,
    })
}

fn floor_destination(
    conn: &mut Connection,
    request: AdvanceReplicaReceiptFloor,
    options: &SqliteOptions,
) -> ReplicationResult<AdvanceReplicaReceiptFloorReceipt> {
    if request.destination_epoch != destination_epoch(conn)? {
        return Err(ReplicationError::DestinationReplaced {
            current: destination_epoch(conn)?,
        });
    }
    if let Some((origin, public_id, incarnation, epoch, expected, new_floor)) = conn.query_row(
        "SELECT origin_id,public_id,incarnation,destination_epoch,expected_floor,new_floor FROM replication_destination_floor_receipts WHERE operation_id=?1",
        [request.operation_id.as_str()],
        |row| Ok((row.get::<_,Vec<u8>>(0)?,row.get::<_,String>(1)?,row.get::<_,Vec<u8>>(2)?,row.get::<_,Vec<u8>>(3)?,row.get::<_,Vec<u8>>(4)?,row.get::<_,Vec<u8>>(5)?)),
    ).optional().map_err(|e|corrupt("read floor receipt",e))? {
        let exact = origin == request.stream.origin.0
            && public_id == request.stream.stream.id.as_str()
            && incarnation == request.stream.stream.incarnation.0
            && epoch == request.destination_epoch.0
            && decode(&expected, "saved receipt floor")? == request.expected_floor.offset
            && decode(&new_floor, "saved new receipt floor")? == request.new_floor.offset;
        return if exact {
            Ok(AdvanceReplicaReceiptFloorReceipt { request })
        } else {
            Err(ReplicationError::InvalidInput(
                "operation identity was already used for a different receipt floor".into(),
            ))
        };
    }
    let floor:Vec<u8>=conn.query_row("SELECT floor FROM replication_destination_streams WHERE origin_id=?1 AND public_id=?2 AND incarnation=?3",params![request.stream.origin.0.as_slice(),request.stream.stream.id.as_str(),request.stream.stream.incarnation.0.as_slice()],|r|r.get(0)).map_err(|e|corrupt("read destination floor",e))?;
    let current = decode(&floor, "destination floor")?;
    if current != request.expected_floor.offset {
        return Err(ReplicationError::StaleProgress {
            current: Box::new(ReplicaPosition {
                stream: request.stream.clone(),
                offset: current,
            }),
        });
    }
    let tail: Vec<u8> = conn
        .query_row(
            "SELECT tail FROM replication_destination_streams WHERE origin_id=?1 AND public_id=?2 AND incarnation=?3",
            params![request.stream.origin.0.as_slice(),request.stream.stream.id.as_str(),request.stream.stream.incarnation.0.as_slice()],
            |row| row.get(0),
        )
        .map_err(|error| corrupt("read destination tail", error))?;
    if request.new_floor.offset < current
        || request.new_floor.offset > decode(&tail, "destination tail")?
    {
        return Err(ReplicationError::InvalidInput(
            "replica receipt floor is outside committed history".into(),
        ));
    }
    let mut expired_count = 0u64;
    let mut expired_bytes = 0u64;
    {
        let mut statement = conn.prepare("SELECT public_id,after_offset,through_offset FROM replication_destination_receipts WHERE origin_id=?1 AND public_id=?2 AND incarnation=?3 AND through_offset<=?4").map_err(|error|corrupt("prepare expired destination receipts",error))?;
        let mut rows = statement
            .query(params![
                request.stream.origin.0.as_slice(),
                request.stream.stream.id.as_str(),
                request.stream.stream.incarnation.0.as_slice(),
                be(request.new_floor.offset).as_slice()
            ])
            .map_err(|error| corrupt("query expired destination receipts", error))?;
        while let Some(row) = rows
            .next()
            .map_err(|error| corrupt("step expired destination receipts", error))?
        {
            let public_id: String = row
                .get(0)
                .map_err(|error| corrupt("decode expired destination receipt name", error))?;
            let after: Vec<u8> = row
                .get(1)
                .map_err(|error| corrupt("decode expired destination receipt start", error))?;
            let through: Vec<u8> = row
                .get(2)
                .map_err(|error| corrupt("decode expired destination receipt end", error))?;
            expired_count = expired_count
                .checked_add(1)
                .ok_or(ReplicationError::CapacityExceeded)?;
            expired_bytes = expired_bytes
                .checked_add(
                    batch_receipt_charge_from_parts(
                        &public_id,
                        decode(&after, "expired receipt start")?,
                        decode(&through, "expired receipt end")?,
                    )
                    .map_err(|_| ReplicationError::CapacityExceeded)?,
                )
                .ok_or(ReplicationError::CapacityExceeded)?;
        }
    }
    let (receipt_count, receipt_bytes):(i64,Vec<u8>)=conn.query_row("SELECT receipt_count,receipt_bytes FROM replication_destination_accounting WHERE singleton=1",[],|row|Ok((row.get(0)?,row.get(1)?))).map_err(|error|corrupt("read destination receipt accounting",error))?;
    let receipt_count = u64::try_from(receipt_count).map_err(|_| {
        ReplicationError::CorruptStorage("invalid destination receipt count".into())
    })?;
    let receipt_bytes = decode(&receipt_bytes, "destination receipt bytes")?;
    let new_charge = floor_receipt_charge(
        request.operation_id.as_str(),
        request.stream.stream.id.as_str(),
    )
    .map_err(|_| ReplicationError::CapacityExceeded)?;
    let next_count = receipt_count
        .checked_sub(expired_count)
        .and_then(|count| count.checked_add(1))
        .ok_or_else(|| {
            ReplicationError::CorruptStorage("destination receipt counter underflow".into())
        })?;
    let next_bytes = receipt_bytes
        .checked_sub(expired_bytes)
        .and_then(|bytes| bytes.checked_add(new_charge))
        .ok_or_else(|| {
            ReplicationError::CorruptStorage("destination receipt byte counter underflow".into())
        })?;
    if next_count > options.replica_destination.receipts.max_batch_receipts as u64
        || next_bytes > options.replica_destination.receipts.max_batch_receipt_bytes as u64
    {
        return Err(ReplicationError::CapacityExceeded);
    }
    let tx = conn
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .map_err(|e| storage("begin receipt floor", e))?;
    tx.execute("UPDATE replication_destination_streams SET floor=?1 WHERE origin_id=?2 AND public_id=?3 AND incarnation=?4",params![be(request.new_floor.offset).as_slice(),request.stream.origin.0.as_slice(),request.stream.stream.id.as_str(),request.stream.stream.incarnation.0.as_slice()]).map_err(|e|storage("advance destination floor",e))?;
    tx.execute("DELETE FROM replication_destination_receipts WHERE origin_id=?1 AND public_id=?2 AND incarnation=?3 AND through_offset<=?4",params![request.stream.origin.0.as_slice(),request.stream.stream.id.as_str(),request.stream.stream.incarnation.0.as_slice(),be(request.new_floor.offset).as_slice()]).map_err(|e|storage("expire replica receipts",e))?;
    tx.execute(
        "INSERT INTO replication_destination_floor_receipts VALUES(?1,?2,?3,?4,?5,?6,?7)",
        params![
            request.operation_id.as_str(),
            request.stream.origin.0.as_slice(),
            request.stream.stream.id.as_str(),
            request.stream.stream.incarnation.0.as_slice(),
            request.destination_epoch.0.as_slice(),
            be(request.expected_floor.offset).as_slice(),
            be(request.new_floor.offset).as_slice()
        ],
    )
    .map_err(|e| storage("store floor receipt", e))?;
    tx.execute(
        "UPDATE replication_destination_accounting SET receipt_count=?1,receipt_bytes=?2 WHERE singleton=1",
        params![i64::try_from(next_count).map_err(|_|ReplicationError::CapacityExceeded)?,be(next_bytes).as_slice()],
    ).map_err(|error|storage("update destination receipt accounting",error))?;
    tx.commit()
        .map_err(|_| ReplicationError::ReceiptFloorUnknown(Box::new(request.clone())))?;
    Ok(AdvanceReplicaReceiptFloorReceipt { request })
}

fn snapshot_error(error: SnapshotError) -> ReplicationError {
    match error {
        SnapshotError::CapacityExceeded => ReplicationError::CapacityExceeded,
        SnapshotError::MissingHistory { .. } | SnapshotError::NotFound { .. } => {
            ReplicationError::InvalidInput("bootstrap snapshot is unavailable".into())
        }
        other => ReplicationError::StorageFailure(other.to_string()),
    }
}

struct StoredDestinationBootstrap {
    request: ReplicaBootstrap,
    state: i64,
    accepted_bytes: u64,
    accepted_records: usize,
    accepted_record_bytes: u64,
    publish_operation: Option<String>,
    abort_operation: Option<String>,
}

fn load_destination_bootstrap(
    conn: &Connection,
    id: BootstrapId,
) -> ReplicationResult<Option<StoredDestinationBootstrap>> {
    type Row = (
        String,
        String,
        Vec<u8>,
        Vec<u8>,
        String,
        Vec<u8>,
        Vec<u8>,
        Vec<u8>,
        String,
        i64,
        Vec<u8>,
        Vec<u8>,
        Vec<u8>,
        i64,
        Vec<u8>,
        i64,
        Vec<u8>,
        Option<String>,
        Option<String>,
    );
    let row:Option<Row>=conn.query_row("SELECT operation_id,replica_id,destination_epoch,origin_id,public_id,incarnation,snapshot_id,covered,schema_id,schema_version,content_bytes,digest,through_offset,state,accepted_bytes,accepted_records,accepted_record_bytes,publish_operation_id,abort_operation_id FROM replication_destination_bootstraps WHERE bootstrap_id=?1",[id.0.as_slice()],|row|Ok((row.get(0)?,row.get(1)?,row.get(2)?,row.get(3)?,row.get(4)?,row.get(5)?,row.get(6)?,row.get(7)?,row.get(8)?,row.get(9)?,row.get(10)?,row.get(11)?,row.get(12)?,row.get(13)?,row.get(14)?,row.get(15)?,row.get(16)?,row.get(17)?,row.get(18)?))).optional().map_err(|error|corrupt("read destination bootstrap",error))?;
    let Some((
        operation,
        replica,
        epoch,
        origin,
        public_id,
        incarnation,
        snapshot,
        covered,
        schema_id,
        schema_version,
        content_bytes,
        digest,
        through,
        state,
        accepted_bytes,
        accepted_records,
        accepted_record_bytes,
        publish_operation,
        abort_operation,
    )) = row
    else {
        return Ok(None);
    };
    let stream =
        OriginStream {
            origin: OriginId(origin.try_into().map_err(|_| {
                ReplicationError::CorruptStorage("invalid bootstrap origin".into())
            })?),
            stream: StreamKey {
                id: StreamId::new(public_id).map_err(|_| {
                    ReplicationError::CorruptStorage("invalid bootstrap stream name".into())
                })?,
                incarnation: IncarnationId(incarnation.try_into().map_err(|_| {
                    ReplicationError::CorruptStorage("invalid bootstrap incarnation".into())
                })?),
            },
        };
    let covered = decode(&covered, "bootstrap covered")?;
    Ok(Some(StoredDestinationBootstrap {
        request: ReplicaBootstrap {
            operation_id: ReplicationOperationId::new(operation).map_err(|_| {
                ReplicationError::CorruptStorage("invalid bootstrap operation".into())
            })?,
            id,
            replica: ReplicaId::new(replica).map_err(|_| {
                ReplicationError::CorruptStorage("invalid bootstrap replica".into())
            })?,
            destination_epoch: DestinationEpoch(
                epoch.try_into().map_err(|_| {
                    ReplicationError::CorruptStorage("invalid bootstrap epoch".into())
                })?,
            ),
            stream: stream.clone(),
            snapshot: SnapshotDescriptor {
                id: SnapshotId(snapshot.try_into().map_err(|_| {
                    ReplicationError::CorruptStorage("invalid bootstrap snapshot".into())
                })?),
                covered: Cursor::new(stream.stream.clone(), covered),
                schema: SchemaRef {
                    id: SchemaId::new(schema_id).map_err(|_| {
                        ReplicationError::CorruptStorage("invalid bootstrap schema".into())
                    })?,
                    version: u32::try_from(schema_version).map_err(|_| {
                        ReplicationError::CorruptStorage("invalid bootstrap schema version".into())
                    })?,
                },
                content_bytes: decode(&content_bytes, "bootstrap content bytes")?,
                digest: SnapshotDigest(digest.try_into().map_err(|_| {
                    ReplicationError::CorruptStorage("invalid bootstrap digest".into())
                })?),
            },
            through: ReplicaPosition {
                stream,
                offset: decode(&through, "bootstrap through")?,
            },
        },
        state,
        accepted_bytes: decode(&accepted_bytes, "bootstrap accepted bytes")?,
        accepted_records: usize::try_from(accepted_records).map_err(|_| {
            ReplicationError::CorruptStorage("invalid bootstrap accepted records".into())
        })?,
        accepted_record_bytes: decode(&accepted_record_bytes, "bootstrap accepted record bytes")?,
        publish_operation,
        abort_operation,
    }))
}

fn begin_destination_bootstrap(
    conn: &mut Connection,
    request: ReplicaBootstrap,
    options: &SqliteOptions,
) -> ReplicationResult<BeginReplicaBootstrapReceipt> {
    if request.destination_epoch != destination_epoch(conn)? {
        return Err(ReplicationError::DestinationReplaced {
            current: destination_epoch(conn)?,
        });
    }
    if request.snapshot.covered.stream != request.stream.stream
        || request.through.stream != request.stream
        || request.snapshot.covered.offset > request.through.offset
    {
        return Err(ReplicationError::InvalidInput(
            "bootstrap boundaries must name one ordered stream".into(),
        ));
    }
    if let Some(saved) = load_destination_bootstrap(conn, request.id)? {
        return if saved.request == request {
            Ok(BeginReplicaBootstrapReceipt {
                request,
                state: match saved.state {
                    0 => ReplicaBootstrapState::Staging,
                    1 => ReplicaBootstrapState::Verified,
                    2 => ReplicaBootstrapState::Published,
                    3 => ReplicaBootstrapState::Aborted,
                    _ => {
                        return Err(ReplicationError::CorruptStorage(
                            "invalid bootstrap state".into(),
                        ))
                    }
                },
                accepted_bytes: saved.accepted_bytes,
            })
        } else {
            Err(ReplicationError::InvalidInput(
                "bootstrap identity was reused".into(),
            ))
        };
    }
    let operation_conflict: bool = conn
        .query_row(
            "SELECT EXISTS(SELECT 1 FROM replication_destination_bootstraps WHERE operation_id=?1)",
            [request.operation_id.as_str()],
            |row| row.get(0),
        )
        .map_err(|error| corrupt("inspect bootstrap operation", error))?;
    if operation_conflict {
        return Err(ReplicationError::InvalidInput(
            "bootstrap operation identity was reused".into(),
        ));
    }
    let (count,bytes):(i64,i64)=conn.query_row("SELECT count(*),coalesce(sum(CASE WHEN typeof(accepted_bytes)='blob' AND length(accepted_bytes)=8 THEN 0 ELSE NULL END),0) FROM replication_destination_bootstraps WHERE state IN(0,1)",[],|row|Ok((row.get(0)?,row.get(1)?))).map_err(|error|corrupt("measure staging bootstraps",error))?;
    let _ = bytes;
    if usize::try_from(count)
        .ok()
        .is_none_or(|count| count >= options.replica_destination.staging.max_staging_bootstraps)
    {
        return Err(ReplicationError::CapacityExceeded);
    }
    let tx = conn
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .map_err(|error| storage("begin destination bootstrap", error))?;
    tx.execute("INSERT INTO replication_destination_bootstraps VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14,0,zeroblob(8),0,zeroblob(8),NULL,NULL)",params![request.id.0.as_slice(),request.operation_id.as_str(),request.replica.as_str(),request.destination_epoch.0.as_slice(),request.stream.origin.0.as_slice(),request.stream.stream.id.as_str(),request.stream.stream.incarnation.0.as_slice(),request.snapshot.id.0.as_slice(),be(request.snapshot.covered.offset).as_slice(),request.snapshot.schema.id.as_str(),i64::from(request.snapshot.schema.version),be(request.snapshot.content_bytes).as_slice(),request.snapshot.digest.0.as_slice(),be(request.through.offset).as_slice()]).map_err(|error|storage("store destination bootstrap",error))?;
    tx.commit()
        .map_err(|_| ReplicationError::BootstrapUnknown(Box::new(request.clone())))?;
    Ok(BeginReplicaBootstrapReceipt {
        request,
        state: ReplicaBootstrapState::Staging,
        accepted_bytes: 0,
    })
}

fn destination_chunk_accounting(conn: &Connection) -> ReplicationResult<(u64, u64)> {
    let (count, bytes): (i64, Option<Vec<u8>>) = conn
        .query_row(
            "SELECT staging_chunk_count,
                    CASE WHEN typeof(staging_chunk_bytes)='blob'
                               AND length(staging_chunk_bytes)=8
                         THEN staging_chunk_bytes END
             FROM replication_destination_accounting WHERE singleton=1",
            [],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .map_err(|error| corrupt("read destination bootstrap chunk accounting", error))?;
    let count = u64::try_from(count).map_err(|_| {
        ReplicationError::CorruptStorage("invalid destination bootstrap chunk count".into())
    })?;
    let bytes = bytes.ok_or_else(|| {
        ReplicationError::CorruptStorage("invalid destination bootstrap chunk bytes".into())
    })?;
    Ok((count, decode(&bytes, "destination bootstrap chunk bytes")?))
}

fn destination_record_accounting(conn: &Connection) -> ReplicationResult<(u64, u64)> {
    let (count, bytes): (i64, Option<Vec<u8>>) = conn
        .query_row(
            "SELECT staging_record_count,
                    CASE WHEN typeof(staging_record_bytes)='blob'
                               AND length(staging_record_bytes)=8
                         THEN staging_record_bytes END
             FROM replication_destination_accounting WHERE singleton=1",
            [],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .map_err(|error| corrupt("read destination bootstrap record accounting", error))?;
    let count = u64::try_from(count).map_err(|_| {
        ReplicationError::CorruptStorage("invalid destination bootstrap record count".into())
    })?;
    let bytes = bytes.ok_or_else(|| {
        ReplicationError::CorruptStorage("invalid destination bootstrap record bytes".into())
    })?;
    Ok((count, decode(&bytes, "destination bootstrap record bytes")?))
}

fn put_bootstrap_chunk(
    conn: &mut Connection,
    request: ReplicaBootstrapChunk,
    options: &SqliteOptions,
    state: &mut SqliteReplicationState,
) -> ReplicationResult<ReplicaBootstrapChunkReceipt> {
    let stored = load_destination_bootstrap(conn, request.id)?
        .ok_or_else(|| ReplicationError::InvalidInput("bootstrap is unknown".into()))?;
    if stored.state != 0 {
        return Err(ReplicationError::InvalidInput(
            "bootstrap is not accepting chunks".into(),
        ));
    }
    let length = request.chunk.bytes.len();
    if length == 0 || length > options.replica_destination.staging.max_chunk_bytes {
        return Err(ReplicationError::CapacityExceeded);
    }
    let end = request
        .chunk
        .offset
        .checked_add(length as u64)
        .ok_or(ReplicationError::CapacityExceeded)?;
    if request.chunk.offset < stored.accepted_bytes {
        let saved:Vec<u8>=conn.query_row("SELECT bytes FROM replication_destination_bootstrap_chunks WHERE bootstrap_id=?1 AND offset=?2",params![request.id.0.as_slice(),be(request.chunk.offset).as_slice()],|row|row.get(0)).map_err(|error|corrupt("read bootstrap chunk retry",error))?;
        return if saved == request.chunk.bytes.as_bytes() {
            Ok(ReplicaBootstrapChunkReceipt {
                id: request.id,
                offset: request.chunk.offset,
                end,
            })
        } else {
            Err(ReplicationError::InvalidInput(
                "bootstrap chunk retry differs".into(),
            ))
        };
    }
    if request.chunk.offset != stored.accepted_bytes || end > stored.request.snapshot.content_bytes
    {
        return Err(ReplicationError::InvalidInput(
            "bootstrap chunk is not the next contiguous range".into(),
        ));
    }
    let (chunks, total) = destination_chunk_accounting(conn)?;
    let next_chunks = chunks
        .checked_add(1)
        .ok_or(ReplicationError::CapacityExceeded)?;
    let next_total = total
        .checked_add(length as u64)
        .ok_or(ReplicationError::CapacityExceeded)?;
    if usize::try_from(next_chunks)
        .ok()
        .is_none_or(|value| value > options.replica_destination.staging.max_staging_chunks)
        || next_total > options.replica_destination.staging.max_staging_bytes
    {
        return Err(ReplicationError::CapacityExceeded);
    }
    let tx = conn
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .map_err(|error| storage("begin bootstrap chunk", error))?;
    tx.execute(
        "INSERT INTO replication_destination_bootstrap_chunks VALUES(?1,?2,?3)",
        params![
            request.id.0.as_slice(),
            be(request.chunk.offset).as_slice(),
            request.chunk.bytes.as_bytes()
        ],
    )
    .map_err(|error| storage("store bootstrap chunk", error))?;
    tx.execute(
        "UPDATE replication_destination_bootstraps SET accepted_bytes=?1 WHERE bootstrap_id=?2",
        params![be(end).as_slice(), request.id.0.as_slice()],
    )
    .map_err(|error| storage("advance bootstrap bytes", error))?;
    tx.execute(
        "UPDATE replication_destination_accounting
         SET staging_chunk_count=?1,staging_chunk_bytes=?2 WHERE singleton=1",
        params![
            i64::try_from(next_chunks).map_err(|_| ReplicationError::CapacityExceeded)?,
            be(next_total).as_slice()
        ],
    )
    .map_err(|error| storage("account bootstrap chunk", error))?;
    tx.commit()
        .map_err(|error| storage("commit bootstrap chunk", error))?;
    state.verifications.remove(&request.id);
    Ok(ReplicaBootstrapChunkReceipt {
        id: request.id,
        offset: request.chunk.offset,
        end,
    })
}

fn put_bootstrap_batch(
    conn: &mut Connection,
    request: ReplicaBootstrapBatch,
    options: &SqliteOptions,
    state: &mut SqliteReplicationState,
    failure_injection: &mut Option<super::sqlite::SqliteFailureInjection>,
) -> ReplicationResult<ReplicaBootstrapBatchReceipt> {
    let stored = load_destination_bootstrap(conn, request.id)?
        .ok_or_else(|| ReplicationError::InvalidInput("bootstrap is unknown".into()))?;
    if stored.state != 0 {
        return Err(ReplicationError::InvalidInput(
            "bootstrap is not accepting records".into(),
        ));
    }
    if request.batch.destination_epoch != stored.request.destination_epoch
        || request.batch.after.stream != stored.request.stream
    {
        return Err(ReplicationError::InvalidInput(
            "bootstrap batch identifies another destination or stream".into(),
        ));
    }
    if let Some((after,through))=conn.query_row("SELECT after_offset,through_offset FROM replication_destination_bootstrap_batch_receipts WHERE bootstrap_id=?1 AND batch_id=?2",params![request.id.0.as_slice(),request.batch.id.0.as_slice()],|row|Ok((row.get::<_,Vec<u8>>(0)?,row.get::<_,Vec<u8>>(1)?))).optional().map_err(|error|corrupt("read bootstrap batch retry",error))?{let through=decode(&through,"saved bootstrap batch through")?;if decode(&after,"saved bootstrap batch start")?!=request.batch.after.offset||request.batch.records.last().map(|record|record.cursor.offset)!=Some(through){return Err(ReplicationError::BatchConflict{batch:request.batch.id});}return Ok(ReplicaBootstrapBatchReceipt{id:request.id,committed_through:ReplicaPosition{stream:stored.request.stream,offset:through}});}
    let bytes = validate_batch(
        &request.batch,
        options.replica_destination.storage.max_batch_records,
        options.replica_destination.storage.max_batch_bytes,
    )?;
    let added_records = u64::try_from(request.batch.records.len())
        .map_err(|_| ReplicationError::CapacityExceeded)?;
    let added_bytes = u64::try_from(bytes).map_err(|_| ReplicationError::CapacityExceeded)?;
    let expected = stored
        .request
        .snapshot
        .covered
        .offset
        .checked_add(stored.accepted_records as u64)
        .ok_or(ReplicationError::CapacityExceeded)?;
    if request.batch.after.offset != expected
        || request
            .batch
            .records
            .last()
            .is_none_or(|record| record.cursor.offset > stored.request.through.offset)
    {
        return Err(ReplicationError::StaleProgress {
            current: Box::new(ReplicaPosition {
                stream: stored.request.stream,
                offset: expected,
            }),
        });
    }
    let (staging_records, staging_bytes) = destination_record_accounting(conn)?;
    let next_staging_records = staging_records
        .checked_add(added_records)
        .ok_or(ReplicationError::CapacityExceeded)?;
    let next_staging_bytes = staging_bytes
        .checked_add(added_bytes)
        .ok_or(ReplicationError::CapacityExceeded)?;
    let next_accepted_record_bytes = stored
        .accepted_record_bytes
        .checked_add(added_bytes)
        .ok_or(ReplicationError::CapacityExceeded)?;
    if usize::try_from(next_staging_records)
        .ok()
        .is_none_or(|value| value > options.replica_destination.staging.max_staging_records)
        || next_staging_bytes > options.replica_destination.staging.max_staging_record_bytes
    {
        return Err(ReplicationError::CapacityExceeded);
    }
    let through = request
        .batch
        .records
        .last()
        .expect("validated batch")
        .cursor
        .offset;
    let tx = conn
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .map_err(|error| storage("begin bootstrap batch", error))?;
    for record in &request.batch.records {
        tx.execute(
            "INSERT INTO replication_destination_bootstrap_records VALUES(?1,?2,?3,?4,?5,?6)",
            params![
                request.id.0.as_slice(),
                be(record.cursor.offset).as_slice(),
                record.event.id.as_str(),
                record.event.schema.id.as_str(),
                i64::from(record.event.schema.version),
                record.event.payload.as_bytes()
            ],
        )
        .map_err(|error| storage("store bootstrap record", error))?;
    }
    tx.execute(
        "INSERT INTO replication_destination_bootstrap_batch_receipts VALUES(?1,?2,?3,?4)",
        params![
            request.id.0.as_slice(),
            request.batch.id.0.as_slice(),
            be(request.batch.after.offset).as_slice(),
            be(through).as_slice()
        ],
    )
    .map_err(|error| storage("store bootstrap batch receipt", error))?;
    tx.execute("UPDATE replication_destination_bootstraps SET accepted_records=accepted_records+?1,accepted_record_bytes=?2 WHERE bootstrap_id=?3",params![i64::try_from(added_records).map_err(|_|ReplicationError::CapacityExceeded)?,be(next_accepted_record_bytes).as_slice(),request.id.0.as_slice()]).map_err(|error|storage("advance bootstrap records",error))?;
    tx.execute(
        "UPDATE replication_destination_accounting
         SET staging_record_count=?1,staging_record_bytes=?2 WHERE singleton=1",
        params![
            i64::try_from(next_staging_records).map_err(|_| ReplicationError::CapacityExceeded)?,
            be(next_staging_bytes).as_slice()
        ],
    )
    .map_err(|error| storage("account bootstrap records", error))?;
    if matches!(
        failure_injection,
        Some(super::sqlite::SqliteFailureInjection::BeforeReplicaCommit)
    ) {
        *failure_injection = None;
        tx.rollback()
            .map_err(|error| storage("roll back injected bootstrap batch", error))?;
        return Err(ReplicationError::StorageFailure(
            "injected failure before bootstrap batch commit".into(),
        ));
    }
    tx.commit()
        .map_err(|_| ReplicationError::BootstrapBatchUnknown(Box::new(request.clone())))?;
    if matches!(
        failure_injection,
        Some(super::sqlite::SqliteFailureInjection::AfterReplicaCommitAcknowledgementLost)
    ) {
        *failure_injection = None;
        return Err(ReplicationError::BootstrapBatchUnknown(Box::new(request)));
    }
    state.verifications.remove(&request.id);
    Ok(ReplicaBootstrapBatchReceipt {
        id: request.id,
        committed_through: ReplicaPosition {
            stream: stored.request.stream,
            offset: through,
        },
    })
}

fn verify_bootstrap(
    conn: &mut Connection,
    request: VerifyReplicaBootstrap,
    options: &SqliteOptions,
    state: &mut SqliteReplicationState,
) -> ReplicationResult<ReplicaBootstrapVerificationProgress> {
    if request.limits.max_chunks == 0
        || request.limits.max_records == 0
        || request.limits.max_bytes == 0
        || request.limits.max_chunks > options.replica_destination.staging.max_staging_chunks
        || request.limits.max_records > options.replica_destination.staging.max_staging_records
    {
        return Err(ReplicationError::InvalidConfig(
            "bootstrap verification limits are invalid".into(),
        ));
    }
    let stored = load_destination_bootstrap(conn, request.id)?
        .ok_or_else(|| ReplicationError::InvalidInput("bootstrap is unknown".into()))?;
    if stored.state == 1 || stored.state == 2 {
        return Ok(ReplicaBootstrapVerificationProgress {
            id: request.id,
            verified_snapshot_bytes: stored.request.snapshot.content_bytes,
            verified_suffix_records: stored.accepted_records,
            verified_suffix_bytes: stored.accepted_record_bytes,
            complete: true,
        });
    }
    if stored.state != 0 {
        return Err(ReplicationError::InvalidInput(
            "aborted bootstrap cannot be verified".into(),
        ));
    }
    let expected_records = stored
        .request
        .through
        .offset
        .checked_sub(stored.request.snapshot.covered.offset)
        .ok_or_else(|| ReplicationError::CorruptStorage("bootstrap range is reversed".into()))?;
    if stored.accepted_bytes != stored.request.snapshot.content_bytes
        || stored.accepted_records as u64 != expected_records
    {
        return Err(ReplicationError::InvalidInput(
            "bootstrap upload is incomplete".into(),
        ));
    }
    let verification =
        state
            .verifications
            .entry(request.id)
            .or_insert_with(|| BootstrapVerification {
                next_snapshot: 0,
                next_record: stored.request.snapshot.covered.offset,
                hasher: Sha256::new(),
            });
    let mut used = 0usize;
    let mut chunks = 0usize;
    let mut records = 0usize;
    while verification.next_snapshot < stored.request.snapshot.content_bytes
        && chunks < request.limits.max_chunks
    {
        let bytes: Vec<u8> = conn
            .query_row(
                "SELECT CASE WHEN typeof(bytes)='blob' AND octet_length(bytes)<=?3 THEN bytes END FROM replication_destination_bootstrap_chunks WHERE bootstrap_id=?1 AND offset=?2",
                params![request.id.0.as_slice(),be(verification.next_snapshot).as_slice(),i64::try_from(options.replica_destination.staging.max_chunk_bytes).unwrap_or(i64::MAX)],
                |row| row.get::<_, Option<Vec<u8>>>(0),
            )
            .map_err(|error| corrupt("read bootstrap verification chunk", error))?
            .ok_or_else(|| ReplicationError::CorruptStorage("bootstrap chunk is malformed".into()))?;
        if bytes.is_empty()
            || used
                .checked_add(bytes.len())
                .is_none_or(|value| value > request.limits.max_bytes)
        {
            if used == 0 {
                return Err(ReplicationError::CapacityExceeded);
            }
            break;
        }
        verification.hasher.update(&bytes);
        verification.next_snapshot = verification
            .next_snapshot
            .checked_add(bytes.len() as u64)
            .ok_or(ReplicationError::CapacityExceeded)?;
        used += bytes.len();
        chunks += 1;
    }
    while verification.next_snapshot == stored.request.snapshot.content_bytes
        && verification.next_record < stored.request.through.offset
        && records < request.limits.max_records
    {
        let offset = verification
            .next_record
            .checked_add(1)
            .ok_or(ReplicationError::CapacityExceeded)?;
        let record: Arc<Record> = conn
            .query_row(
                "SELECT offset,event_id,schema_id,schema_version,payload FROM replication_destination_bootstrap_records WHERE bootstrap_id=?1 AND offset=?2",
                params![request.id.0.as_slice(),be(offset).as_slice()],
                |row| destination_record(row,&stored.request.stream),
            )
            .map_err(|error| corrupt("verify bootstrap record", error))?;
        let charge = record_charge(&record)?;
        if used
            .checked_add(charge)
            .is_none_or(|value| value > request.limits.max_bytes)
        {
            if used == 0 {
                return Err(ReplicationError::CapacityExceeded);
            }
            break;
        }
        used += charge;
        records += 1;
        verification.next_record = offset;
    }
    let complete = verification.next_snapshot == stored.request.snapshot.content_bytes
        && verification.next_record == stored.request.through.offset;
    let verified_snapshot_bytes = verification.next_snapshot;
    let verified_suffix_records = usize::try_from(
        verification
            .next_record
            .saturating_sub(stored.request.snapshot.covered.offset),
    )
    .unwrap_or(usize::MAX);
    if complete {
        let digest: [u8; 32] = verification.hasher.clone().finalize().into();
        if digest != stored.request.snapshot.digest.0 {
            state.verifications.remove(&request.id);
            return Err(ReplicationError::InvalidReceipt(
                "bootstrap snapshot digest does not match".into(),
            ));
        }
        conn.execute(
            "UPDATE replication_destination_bootstraps SET state=1 WHERE bootstrap_id=?1 AND state=0",
            [request.id.0.as_slice()],
        )
        .map_err(|error| storage("complete bootstrap verification", error))?;
        state.verifications.remove(&request.id);
    }
    Ok(ReplicaBootstrapVerificationProgress {
        id: request.id,
        verified_snapshot_bytes,
        verified_suffix_records,
        verified_suffix_bytes: if complete {
            stored.accepted_record_bytes
        } else {
            0
        },
        complete,
    })
}

fn published_bootstrap(
    conn: &Connection,
    stream: &OriginStream,
) -> ReplicationResult<Option<PublishedReplicaBootstrap>> {
    let id: Option<Vec<u8>> = conn
        .query_row(
            "SELECT bootstrap_id FROM replication_destination_published_bootstraps WHERE origin_id=?1 AND public_id=?2 AND incarnation=?3",
            params![stream.origin.0.as_slice(),stream.stream.id.as_str(),stream.stream.incarnation.0.as_slice()],
            |row| row.get(0),
        )
        .optional()
        .map_err(|error| corrupt("read published bootstrap", error))?;
    let Some(id) = id else { return Ok(None) };
    let id = BootstrapId(id.try_into().map_err(|_| {
        ReplicationError::CorruptStorage("invalid published bootstrap identity".into())
    })?);
    let stored = load_destination_bootstrap(conn, id)?
        .ok_or_else(|| ReplicationError::CorruptStorage("published bootstrap is missing".into()))?;
    if stored.state != 2 || stored.request.stream != *stream {
        return Err(ReplicationError::CorruptStorage(
            "published bootstrap pointer is inconsistent".into(),
        ));
    }
    Ok(Some(PublishedReplicaBootstrap {
        committed_through: stored.request.through.clone(),
        request: stored.request,
    }))
}

fn publish_bootstrap(
    conn: &mut Connection,
    request: PublishReplicaBootstrap,
    options: &SqliteOptions,
    failure_injection: &mut Option<super::sqlite::SqliteFailureInjection>,
) -> ReplicationResult<ReplicaBootstrapReceipt> {
    if request.destination_epoch != destination_epoch(conn)? {
        return Err(ReplicationError::DestinationReplaced {
            current: destination_epoch(conn)?,
        });
    }
    let stored = load_destination_bootstrap(conn, request.id)?
        .ok_or_else(|| ReplicationError::InvalidInput("bootstrap is unknown".into()))?;
    if stored.state == 2 {
        return if stored.publish_operation.as_deref() == Some(request.operation_id.as_str()) {
            Ok(ReplicaBootstrapReceipt {
                committed_through: stored.request.through.clone(),
                request: stored.request,
            })
        } else {
            Err(ReplicationError::InvalidInput(
                "bootstrap publication operation differs".into(),
            ))
        };
    }
    if stored.state != 1 {
        return Err(ReplicationError::InvalidInput(
            "bootstrap is not verified".into(),
        ));
    }
    let operation_conflict: bool = conn
        .query_row(
            "SELECT EXISTS(SELECT 1 FROM replication_destination_bootstraps WHERE publish_operation_id=?1 AND bootstrap_id<>?2)",
            params![request.operation_id.as_str(),request.id.0.as_slice()],
            |row| row.get(0),
        )
        .map_err(|error| corrupt("inspect bootstrap publication operation", error))?;
    if operation_conflict {
        return Err(ReplicationError::InvalidInput(
            "bootstrap publication operation identity was reused".into(),
        ));
    }
    let prior = published_bootstrap(conn, &stored.request.stream)?;
    let (prior_rows, prior_bytes): (i64, i64) = conn
        .query_row(
            "SELECT count(*),coalesce(sum(octet_length(event_id)+octet_length(schema_id)+octet_length(payload)+384),0) FROM replication_destination_records WHERE origin_id=?1 AND public_id=?2 AND incarnation=?3",
            params![stored.request.stream.origin.0.as_slice(),stored.request.stream.stream.id.as_str(),stored.request.stream.stream.incarnation.0.as_slice()],
            |row| Ok((row.get(0)?,row.get(1)?)),
        )
        .map_err(|error| corrupt("measure replaced destination history", error))?;
    let destination_stream_exists: bool = conn.query_row("SELECT EXISTS(SELECT 1 FROM replication_destination_streams WHERE origin_id=?1 AND public_id=?2 AND incarnation=?3)",params![stored.request.stream.origin.0.as_slice(),stored.request.stream.stream.id.as_str(),stored.request.stream.stream.incarnation.0.as_slice()],|row|row.get(0)).map_err(|error|corrupt("inspect destination bootstrap stream",error))?;
    let (account_streams, account_rows, account_bytes): (i64, i64, Vec<u8>) = conn
        .query_row(
            "SELECT stream_count,history_records,history_bytes FROM replication_destination_accounting WHERE singleton=1",
            [],
            |row| Ok((row.get(0)?,row.get(1)?,row.get(2)?)),
        )
        .map_err(|error| corrupt("read destination publication accounting", error))?;
    let next_rows = usize::try_from(account_rows)
        .ok()
        .and_then(|rows| rows.checked_sub(usize::try_from(prior_rows).ok()?))
        .and_then(|rows| rows.checked_add(stored.accepted_records))
        .ok_or_else(|| {
            ReplicationError::CorruptStorage("destination history counter underflow".into())
        })?;
    let next_streams = usize::try_from(account_streams)
        .ok()
        .and_then(|count| count.checked_add(usize::from(!destination_stream_exists)))
        .ok_or_else(|| {
            ReplicationError::CorruptStorage("invalid destination stream counter".into())
        })?;
    let next_bytes = decode(&account_bytes, "destination history bytes")?
        .checked_sub(u64::try_from(prior_bytes).map_err(|_| {
            ReplicationError::CorruptStorage("invalid replaced history bytes".into())
        })?)
        .and_then(|bytes| bytes.checked_add(stored.accepted_record_bytes))
        .ok_or_else(|| {
            ReplicationError::CorruptStorage("destination history byte counter underflow".into())
        })?;
    let published_count: i64 = conn
        .query_row(
            "SELECT count(*) FROM replication_destination_published_bootstraps",
            [],
            |row| row.get(0),
        )
        .map_err(|error| corrupt("measure published bootstraps", error))?;
    let published_count = usize::try_from(published_count).map_err(|_| {
        ReplicationError::CorruptStorage("invalid published bootstrap count".into())
    })?;
    let prior_content = prior
        .as_ref()
        .map_or(0, |value| value.request.snapshot.content_bytes);
    let published_bytes = if published_count == 0 {
        0
    } else {
        let mut total = 0u64;
        let mut statement=conn.prepare("SELECT content_bytes FROM replication_destination_published_bootstraps p JOIN replication_destination_bootstraps b USING(bootstrap_id)").map_err(|error|corrupt("prepare published byte audit",error))?;
        let mut rows = statement
            .query([])
            .map_err(|error| corrupt("query published byte audit", error))?;
        while let Some(row) = rows
            .next()
            .map_err(|error| corrupt("step published byte audit", error))?
        {
            let value: Vec<u8> = row
                .get(0)
                .map_err(|error| corrupt("decode published bytes", error))?;
            total = total
                .checked_add(decode(&value, "published content bytes")?)
                .ok_or(ReplicationError::CapacityExceeded)?;
        }
        total
    };
    let next_published_count = published_count + usize::from(prior.is_none());
    let next_published_bytes = published_bytes
        .checked_sub(prior_content)
        .and_then(|bytes| bytes.checked_add(stored.request.snapshot.content_bytes))
        .ok_or(ReplicationError::CapacityExceeded)?;
    if next_streams > options.replica_destination.storage.max_origin_streams
        || next_rows > options.replica_destination.storage.max_history_records
        || next_bytes > options.replica_destination.storage.max_history_bytes
        || next_published_count > options.replica_destination.storage.max_published_bootstraps
        || next_published_bytes
            > options
                .replica_destination
                .storage
                .max_published_snapshot_bytes
    {
        return Err(ReplicationError::CapacityExceeded);
    }
    let (staging_records, staging_record_bytes) = destination_record_accounting(conn)?;
    let published_staging_records =
        u64::try_from(stored.accepted_records).map_err(|_| ReplicationError::CapacityExceeded)?;
    let next_staging_records = staging_records
        .checked_sub(published_staging_records)
        .ok_or_else(|| {
            ReplicationError::CorruptStorage(
                "destination bootstrap record counter underflow at publication".into(),
            )
        })?;
    let next_staging_record_bytes = staging_record_bytes
        .checked_sub(stored.accepted_record_bytes)
        .ok_or_else(|| {
            ReplicationError::CorruptStorage(
                "destination bootstrap record byte counter underflow at publication".into(),
            )
        })?;
    let tx = conn
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .map_err(|error| storage("begin bootstrap publication", error))?;
    tx.execute("DELETE FROM replication_destination_records WHERE origin_id=?1 AND public_id=?2 AND incarnation=?3",params![stored.request.stream.origin.0.as_slice(),stored.request.stream.stream.id.as_str(),stored.request.stream.stream.incarnation.0.as_slice()]).map_err(|error|storage("replace destination history",error))?;
    tx.execute("INSERT INTO replication_destination_records SELECT ?1,?2,?3,offset,event_id,schema_id,schema_version,payload FROM replication_destination_bootstrap_records WHERE bootstrap_id=?4 ORDER BY offset",params![stored.request.stream.origin.0.as_slice(),stored.request.stream.stream.id.as_str(),stored.request.stream.stream.incarnation.0.as_slice(),request.id.0.as_slice()]).map_err(|error|storage("publish bootstrap suffix",error))?;
    let deleted_records = tx
        .execute(
            "DELETE FROM replication_destination_bootstrap_records WHERE bootstrap_id=?1",
            [request.id.0.as_slice()],
        )
        .map_err(|error| storage("release published bootstrap records", error))?;
    if deleted_records != stored.accepted_records {
        return Err(ReplicationError::CorruptStorage(
            "published bootstrap staging row count does not match its receipt".into(),
        ));
    }
    tx.execute(
        "DELETE FROM replication_destination_bootstrap_batch_receipts WHERE bootstrap_id=?1",
        [request.id.0.as_slice()],
    )
    .map_err(|error| storage("release published bootstrap batch receipts", error))?;
    tx.execute("INSERT INTO replication_destination_streams VALUES(?1,?2,?3,?4,?5) ON CONFLICT(origin_id,public_id,incarnation) DO UPDATE SET floor=excluded.floor,tail=excluded.tail",params![stored.request.stream.origin.0.as_slice(),stored.request.stream.stream.id.as_str(),stored.request.stream.stream.incarnation.0.as_slice(),be(stored.request.snapshot.covered.offset).as_slice(),be(stored.request.through.offset).as_slice()]).map_err(|error|storage("publish bootstrap bounds",error))?;
    tx.execute("INSERT INTO replication_destination_published_bootstraps VALUES(?1,?2,?3,?4) ON CONFLICT(origin_id,public_id,incarnation) DO UPDATE SET bootstrap_id=excluded.bootstrap_id",params![stored.request.stream.origin.0.as_slice(),stored.request.stream.stream.id.as_str(),stored.request.stream.stream.incarnation.0.as_slice(),request.id.0.as_slice()]).map_err(|error|storage("publish bootstrap pointer",error))?;
    tx.execute("UPDATE replication_destination_bootstraps SET state=2,publish_operation_id=?1 WHERE bootstrap_id=?2",params![request.operation_id.as_str(),request.id.0.as_slice()]).map_err(|error|storage("mark bootstrap published",error))?;
    tx.execute("UPDATE replication_destination_accounting SET stream_count=?1,history_records=?2,history_bytes=?3,staging_record_count=?4,staging_record_bytes=?5 WHERE singleton=1",params![i64::try_from(next_streams).map_err(|_|ReplicationError::CapacityExceeded)?,i64::try_from(next_rows).map_err(|_|ReplicationError::CapacityExceeded)?,be(next_bytes).as_slice(),i64::try_from(next_staging_records).map_err(|_|ReplicationError::CapacityExceeded)?,be(next_staging_record_bytes).as_slice()]).map_err(|error|storage("update published history accounting",error))?;
    if matches!(
        failure_injection,
        Some(super::sqlite::SqliteFailureInjection::BeforeReplicaCommit)
    ) {
        *failure_injection = None;
        tx.rollback()
            .map_err(|error| storage("roll back injected bootstrap publication", error))?;
        return Err(ReplicationError::StorageFailure(
            "injected failure before bootstrap publication commit".into(),
        ));
    }
    tx.commit()
        .map_err(|_| ReplicationError::BootstrapPublishUnknown(Box::new(request.clone())))?;
    if matches!(
        failure_injection,
        Some(super::sqlite::SqliteFailureInjection::AfterReplicaCommitAcknowledgementLost)
    ) {
        *failure_injection = None;
        return Err(ReplicationError::BootstrapPublishUnknown(Box::new(request)));
    }
    Ok(ReplicaBootstrapReceipt {
        committed_through: stored.request.through.clone(),
        request: stored.request,
    })
}

fn acquire_bootstrap_read(
    conn: &Connection,
    stream: OriginStream,
    lifetime: Duration,
    options: &SqliteOptions,
    state: &mut SqliteReplicationState,
) -> ReplicationResult<ReplicaBootstrapReadPlan> {
    if lifetime.is_zero() || lifetime > options.replica_destination.reads.max_lifetime {
        return Err(ReplicationError::InvalidInput(
            "replica read lifetime exceeds configured limit".into(),
        ));
    }
    let now = options.replication_clock.now();
    if let Some((&(expires_at, expired), _)) = state.reader_expiries.first_key_value() {
        if expires_at.0 <= now.0 {
            release_bootstrap_reader(state, expired)?;
        }
    }
    if state.readers.len() >= options.replica_destination.reads.max_leases {
        return Err(ReplicationError::CapacityExceeded);
    }
    let published = published_bootstrap(conn, &stream)?.ok_or_else(|| {
        ReplicationError::InvalidInput("published bootstrap is unavailable".into())
    })?;
    let millis = u64::try_from(lifetime.as_millis())
        .map_err(|_| ReplicationError::InvalidInput("replica read lifetime overflows".into()))?;
    let expires_at =
        DurableTimestampMillis(now.0.checked_add(millis).ok_or_else(|| {
            ReplicationError::InvalidInput("replica read lifetime overflows".into())
        })?);
    let lease = loop {
        let candidate = ReplicaReadLeaseId(*uuid::Uuid::new_v4().as_bytes());
        if !state.readers.contains_key(&candidate) {
            break candidate;
        }
    };
    state.readers.insert(
        lease,
        ReplicaReadLease {
            bootstrap: published.request.id,
            published: published.clone(),
            expires_at,
        },
    );
    state.reader_expiries.insert((expires_at, lease), ());
    *state.reader_counts.entry(published.request.id).or_default() += 1;
    Ok(ReplicaBootstrapReadPlan {
        lease,
        published,
        expires_at,
    })
}

fn read_bootstrap_bytes(
    conn: &Connection,
    lease: ReplicaReadLeaseId,
    offset: u64,
    max_bytes: usize,
    options: &SqliteOptions,
    state: &mut SqliteReplicationState,
) -> ReplicationResult<ReplicaBootstrapBytePage> {
    if max_bytes == 0 || max_bytes > options.replica_destination.reads.max_bytes_per_page {
        return Err(ReplicationError::InvalidInput(
            "replica read page exceeds configured limit".into(),
        ));
    }
    let now = options.replication_clock.now();
    if state
        .readers
        .get(&lease)
        .is_some_and(|reader| reader.expires_at.0 <= now.0)
    {
        release_bootstrap_reader(state, lease)?;
    }
    let Some(reader) = state.readers.get(&lease) else {
        return Err(ReplicationError::ReadLeaseExpired { lease });
    };
    let total = reader.published.request.snapshot.content_bytes;
    if offset > total {
        return Err(ReplicationError::InvalidInput(
            "replica read offset exceeds content".into(),
        ));
    }
    let mut output = Vec::with_capacity(
        max_bytes.min(usize::try_from(total.saturating_sub(offset)).unwrap_or(usize::MAX)),
    );
    let mut next = offset;
    while output.len() < max_bytes && next < total {
        let (start,bytes):(Vec<u8>,Vec<u8>)=conn.query_row("SELECT offset,CASE WHEN typeof(bytes)='blob' AND octet_length(bytes)<=?3 THEN bytes END FROM replication_destination_bootstrap_chunks WHERE bootstrap_id=?1 AND offset<=?2 ORDER BY offset DESC LIMIT 1",params![reader.bootstrap.0.as_slice(),be(next).as_slice(),i64::try_from(options.replica_destination.staging.max_chunk_bytes).unwrap_or(i64::MAX)],|row|Ok((row.get(0)?,row.get::<_,Option<Vec<u8>>>(1)?.ok_or(rusqlite::Error::InvalidQuery)?))).map_err(|error|corrupt("read published bootstrap bytes",error))?;
        let start = decode(&start, "published chunk start")?;
        let inside = usize::try_from(next.checked_sub(start).ok_or_else(|| {
            ReplicationError::CorruptStorage("published chunk starts after read offset".into())
        })?)
        .map_err(|_| ReplicationError::CapacityExceeded)?;
        if inside >= bytes.len() {
            return Err(ReplicationError::CorruptStorage(
                "published bootstrap has a byte gap".into(),
            ));
        }
        let take = (bytes.len() - inside).min(max_bytes - output.len());
        output.extend_from_slice(&bytes[inside..inside + take]);
        next += take as u64;
    }
    Ok(ReplicaBootstrapBytePage {
        id: reader.bootstrap,
        offset,
        bytes: Payload::copy_from_slice(&output),
        next_offset: next,
        complete: next == total,
    })
}

fn release_bootstrap_reader(
    state: &mut SqliteReplicationState,
    lease: ReplicaReadLeaseId,
) -> ReplicationResult<bool> {
    let Some(reader) = state.readers.remove(&lease) else {
        return Ok(false);
    };
    state.reader_expiries.remove(&(reader.expires_at, lease));
    let count = state
        .reader_counts
        .get_mut(&reader.bootstrap)
        .ok_or_else(|| {
            ReplicationError::CorruptStorage("bootstrap reader count is missing".into())
        })?;
    *count = count.checked_sub(1).ok_or_else(|| {
        ReplicationError::CorruptStorage("bootstrap reader count underflow".into())
    })?;
    if *count == 0 {
        state.reader_counts.remove(&reader.bootstrap);
    }
    Ok(true)
}

fn abort_bootstrap(
    conn: &mut Connection,
    request: AbortReplicaBootstrap,
) -> ReplicationResult<AbortReplicaBootstrapReceipt> {
    let current_epoch = destination_epoch(conn)?;
    if request.destination_epoch != current_epoch {
        return Err(ReplicationError::DestinationReplaced {
            current: current_epoch,
        });
    }
    let stored = load_destination_bootstrap(conn, request.id)?
        .ok_or_else(|| ReplicationError::InvalidInput("bootstrap is unknown".into()))?;
    if stored.state == 3 {
        return if stored.abort_operation.as_deref() == Some(request.operation_id.as_str()) {
            Ok(AbortReplicaBootstrapReceipt {
                request,
                state: ReplicaBootstrapState::Aborted,
            })
        } else {
            Err(ReplicationError::BootstrapAbortUnknown(Box::new(request)))
        };
    }
    if stored.state == 2 {
        return Err(ReplicationError::InvalidInput(
            "published bootstrap cannot be aborted".into(),
        ));
    }
    let conflict: bool = conn
        .query_row(
            "SELECT EXISTS(SELECT 1 FROM replication_destination_bootstraps WHERE abort_operation_id=?1 AND bootstrap_id<>?2)",
            params![request.operation_id.as_str(), request.id.0.as_slice()],
            |row| row.get(0),
        )
        .map_err(|error| corrupt("inspect bootstrap abort operation", error))?;
    if conflict {
        return Err(ReplicationError::BootstrapAbortUnknown(Box::new(request)));
    }
    let tx = conn
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .map_err(|error| storage("begin bootstrap abort", error))?;
    tx.execute(
        "UPDATE replication_destination_bootstraps SET state=3,abort_operation_id=?1 WHERE bootstrap_id=?2 AND state IN(0,1)",
        params![request.operation_id.as_str(), request.id.0.as_slice()],
    )
    .map_err(|error| storage("abort bootstrap", error))?;
    tx.commit()
        .map_err(|_| ReplicationError::BootstrapAbortUnknown(Box::new(request.clone())))?;
    Ok(AbortReplicaBootstrapReceipt {
        request,
        state: ReplicaBootstrapState::Aborted,
    })
}

fn cleanup_destination(
    conn: &mut Connection,
    limits: ReplicaCleanupLimits,
    options: &SqliteOptions,
    state: &mut SqliteReplicationState,
) -> ReplicationResult<ReplicaCleanupProgress> {
    if limits.max_receipt_rows == 0 || limits.max_staging_rows == 0 || limits.max_bytes == 0 {
        return Err(ReplicationError::InvalidInput(
            "replica cleanup limits must be nonzero".into(),
        ));
    }
    let now = options.replication_clock.now();
    let lease_charge = std::mem::size_of::<ReplicaReadLease>()
        .checked_add(std::mem::size_of::<ReplicaReadLeaseId>())
        .ok_or(ReplicationError::CapacityExceeded)?;
    let mut removed_staging_rows = 0usize;
    let mut removed_receipt_rows = 0usize;
    let mut removed_bytes = 0usize;
    while removed_staging_rows < limits.max_staging_rows {
        let Some((&(expires_at, id), _)) = state.reader_expiries.first_key_value() else {
            break;
        };
        if expires_at.0 > now.0 {
            break;
        }
        if removed_bytes
            .checked_add(lease_charge)
            .is_none_or(|bytes| bytes > limits.max_bytes)
        {
            break;
        }
        release_bootstrap_reader(state, id)?;
        removed_staging_rows += 1;
        removed_bytes += lease_charge;
    }

    while removed_receipt_rows < limits.max_receipt_rows {
        let row: Option<(Vec<u8>, String, i64, i64)> = conn
            .query_row(
                "SELECT r.batch_id,r.public_id,octet_length(r.public_id),octet_length(r.batch_id)
                 FROM replication_destination_receipts r
                 JOIN replication_destination_streams s USING(origin_id,public_id,incarnation)
                 WHERE r.through_offset<=s.floor ORDER BY r.origin_id,r.public_id,r.incarnation,r.through_offset LIMIT 1",
                [],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
            )
            .optional()
            .map_err(|error| corrupt("find expired destination receipt", error))?;
        let Some((batch, public_id, public_len, batch_len)) = row else {
            break;
        };
        let charge = usize::try_from(public_len)
            .ok()
            .and_then(|n| n.checked_add(usize::try_from(batch_len).ok()?))
            .and_then(|n| n.checked_add(RECEIPT_OVERHEAD))
            .ok_or(ReplicationError::CapacityExceeded)?;
        if removed_bytes
            .checked_add(charge)
            .is_none_or(|bytes| bytes > limits.max_bytes)
        {
            break;
        }
        let _ = public_id;
        conn.execute(
            "DELETE FROM replication_destination_receipts WHERE batch_id=?1",
            [batch],
        )
        .map_err(|error| storage("remove expired destination receipt", error))?;
        removed_receipt_rows += 1;
        removed_bytes += charge;
    }

    while removed_staging_rows < limits.max_staging_rows {
        let chunk: Option<(Vec<u8>, Vec<u8>, i64)> = conn
            .query_row(
                "SELECT c.bootstrap_id,c.offset,octet_length(c.bytes)
             FROM replication_destination_bootstrap_chunks c
             JOIN replication_destination_bootstraps b USING(bootstrap_id)
             LEFT JOIN replication_destination_published_bootstraps p USING(bootstrap_id)
             WHERE b.state=3 OR (b.state=2 AND p.bootstrap_id IS NULL)
             ORDER BY c.bootstrap_id,c.offset LIMIT 1",
                [],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .optional()
            .map_err(|error| corrupt("find bootstrap cleanup chunk", error))?;
        let record: Option<(Vec<u8>, Vec<u8>, i64)> = conn.query_row(
            "SELECT r.bootstrap_id,r.offset,octet_length(r.event_id)+octet_length(r.schema_id)+octet_length(r.payload)+384
             FROM replication_destination_bootstrap_records r
             JOIN replication_destination_bootstraps b USING(bootstrap_id)
             WHERE b.state IN(2,3) ORDER BY r.bootstrap_id,r.offset LIMIT 1",
            [],
            |row| Ok((row.get(0)?,row.get(1)?,row.get(2)?)),
        ).optional().map_err(|error|corrupt("find bootstrap cleanup record",error))?;
        let selected = match (chunk, record) {
            (Some(chunk), Some(record))
                if (chunk.0.as_slice(), chunk.1.as_slice())
                    <= (record.0.as_slice(), record.1.as_slice()) =>
            {
                Some((0, chunk))
            }
            (Some(chunk), None) => Some((0, chunk)),
            (_, Some(record)) => Some((1, record)),
            (None, None) => None,
        };
        let Some((kind, (bootstrap, offset, charge))) = selected else {
            break;
        };
        let id: [u8; 16] = bootstrap.as_slice().try_into().map_err(|_| {
            ReplicationError::CorruptStorage("invalid cleanup bootstrap identity".into())
        })?;
        if kind == 0 && state.reader_counts.contains_key(&BootstrapId(id)) {
            break;
        }
        let charge = usize::try_from(charge).map_err(|_| {
            ReplicationError::CorruptStorage("invalid bootstrap cleanup charge".into())
        })?;
        if removed_bytes
            .checked_add(charge)
            .is_none_or(|bytes| bytes > limits.max_bytes)
        {
            break;
        }
        if kind == 0 {
            let (chunks, chunk_bytes) = destination_chunk_accounting(conn)?;
            let next_chunks = chunks.checked_sub(1).ok_or_else(|| {
                ReplicationError::CorruptStorage(
                    "bootstrap chunk counter underflow during cleanup".into(),
                )
            })?;
            let next_chunk_bytes = chunk_bytes.checked_sub(charge as u64).ok_or_else(|| {
                ReplicationError::CorruptStorage(
                    "bootstrap chunk byte counter underflow during cleanup".into(),
                )
            })?;
            let tx = conn
                .transaction_with_behavior(TransactionBehavior::Immediate)
                .map_err(|error| storage("begin bootstrap chunk cleanup", error))?;
            let deleted = tx
                .execute(
                    "DELETE FROM replication_destination_bootstrap_chunks
                     WHERE bootstrap_id=?1 AND offset=?2",
                    params![bootstrap, offset],
                )
                .map_err(|error| storage("remove bootstrap chunk", error))?;
            if deleted != 1 {
                return Err(ReplicationError::CorruptStorage(
                    "selected bootstrap chunk disappeared during cleanup".into(),
                ));
            }
            tx.execute(
                "UPDATE replication_destination_accounting
                 SET staging_chunk_count=?1,staging_chunk_bytes=?2 WHERE singleton=1",
                params![
                    i64::try_from(next_chunks).map_err(|_| ReplicationError::CapacityExceeded)?,
                    be(next_chunk_bytes).as_slice()
                ],
            )
            .map_err(|error| storage("release bootstrap chunk accounting", error))?;
            tx.commit()
                .map_err(|error| storage("commit bootstrap chunk cleanup", error))?;
        } else {
            let (records, record_bytes) = destination_record_accounting(conn)?;
            let next_records = records.checked_sub(1).ok_or_else(|| {
                ReplicationError::CorruptStorage(
                    "bootstrap record counter underflow during cleanup".into(),
                )
            })?;
            let next_record_bytes = record_bytes.checked_sub(charge as u64).ok_or_else(|| {
                ReplicationError::CorruptStorage(
                    "bootstrap record byte counter underflow during cleanup".into(),
                )
            })?;
            let tx = conn
                .transaction_with_behavior(TransactionBehavior::Immediate)
                .map_err(|error| storage("begin bootstrap record cleanup", error))?;
            let deleted = tx
                .execute(
                    "DELETE FROM replication_destination_bootstrap_records
                     WHERE bootstrap_id=?1 AND offset=?2",
                    params![bootstrap, offset],
                )
                .map_err(|error| storage("remove bootstrap staging row", error))?;
            if deleted != 1 {
                return Err(ReplicationError::CorruptStorage(
                    "selected bootstrap record disappeared during cleanup".into(),
                ));
            }
            tx.execute(
                "UPDATE replication_destination_accounting
                 SET staging_record_count=?1,staging_record_bytes=?2 WHERE singleton=1",
                params![
                    i64::try_from(next_records).map_err(|_| ReplicationError::CapacityExceeded)?,
                    be(next_record_bytes).as_slice()
                ],
            )
            .map_err(|error| storage("release bootstrap record accounting", error))?;
            tx.commit()
                .map_err(|error| storage("commit bootstrap record cleanup", error))?;
        }
        removed_staging_rows += 1;
        removed_bytes += charge;
    }
    let remaining_receipts: bool = conn.query_row("SELECT EXISTS(SELECT 1 FROM replication_destination_receipts r JOIN replication_destination_streams s USING(origin_id,public_id,incarnation) WHERE r.through_offset<=s.floor)",[],|row|row.get(0)).map_err(|error|corrupt("inspect remaining destination receipts",error))?;
    let remaining_staging: bool = conn.query_row("SELECT EXISTS(SELECT 1 FROM replication_destination_bootstrap_chunks c JOIN replication_destination_bootstraps b USING(bootstrap_id) LEFT JOIN replication_destination_published_bootstraps p USING(bootstrap_id) WHERE b.state=3 OR (b.state=2 AND p.bootstrap_id IS NULL) UNION ALL SELECT 1 FROM replication_destination_bootstrap_records r JOIN replication_destination_bootstraps b USING(bootstrap_id) WHERE b.state IN(2,3))",[],|row|row.get(0)).map_err(|error|corrupt("inspect remaining bootstrap cleanup",error))?;
    let remaining_expired_readers = state
        .reader_expiries
        .first_key_value()
        .is_some_and(|(&(expires_at, _), _)| expires_at.0 <= now.0);
    Ok(ReplicaCleanupProgress {
        removed_receipt_rows,
        removed_staging_rows,
        removed_bytes,
        remaining: remaining_receipts || remaining_staging || remaining_expired_readers,
    })
}

fn begin_origin_bootstrap(
    conn: &mut Connection,
    request: BeginOriginBootstrap,
    options: &SqliteOptions,
    state: &mut SqliteReplicationState,
    snapshot_state: &mut super::sqlite_snapshot::SqliteSnapshotState,
) -> ReplicationResult<BeginOriginBootstrapReceipt> {
    type Saved = (
        i64,
        Vec<u8>,
        String,
        String,
        Vec<u8>,
        String,
        Vec<u8>,
        Vec<u8>,
        Vec<u8>,
        String,
        i64,
        Vec<u8>,
        Vec<u8>,
        Vec<u8>,
        Vec<u8>,
        i64,
        Vec<u8>,
        Option<Vec<u8>>,
        i64,
        Vec<u8>,
        i64,
    );
    let saved: Option<Saved> = conn.query_row(
        "SELECT kind,bootstrap_id,destination_operation_id,replica_id,origin_id,public_id,incarnation,destination_epoch,snapshot_id,schema_id,schema_version,content_bytes,digest,covered,captured_tail,status_backlog_records,status_backlog_bytes,status_oldest,status_mode,protection_expires,completed FROM replication_origin_bootstrap_receipts WHERE operation_id=?1",
        [request.operation_id.as_str()],
        |row| Ok((row.get(0)?,row.get(1)?,row.get(2)?,row.get(3)?,row.get(4)?,row.get(5)?,row.get(6)?,row.get(7)?,row.get(8)?,row.get(9)?,row.get(10)?,row.get(11)?,row.get(12)?,row.get(13)?,row.get(14)?,row.get(15)?,row.get(16)?,row.get(17)?,row.get(18)?,row.get(19)?,row.get(20)?)),
    ).optional().map_err(|error|corrupt("read origin bootstrap receipt",error))?;
    if let Some((
        kind,
        bootstrap,
        destination_operation,
        replica,
        origin,
        public_id,
        incarnation,
        epoch,
        snapshot,
        schema_id,
        schema_version,
        content_bytes,
        digest,
        covered,
        captured,
        status_records,
        status_bytes,
        status_oldest,
        status_mode,
        expires,
        completed,
    )) = saved
    {
        let exact = kind == 0
            && bootstrap == request.bootstrap_id.0
            && destination_operation == request.destination_operation_id.as_str()
            && replica == request.replica.as_str()
            && origin == request.stream.origin.0
            && public_id == request.stream.stream.id.as_str()
            && incarnation == request.stream.stream.incarnation.0
            && epoch == request.destination_epoch.0
            && snapshot == request.snapshot.id.0
            && schema_id == request.snapshot.schema.id.as_str()
            && u32::try_from(schema_version).ok() == Some(request.snapshot.schema.version)
            && decode(&content_bytes, "saved bootstrap content bytes")?
                == request.snapshot.content_bytes
            && digest == request.snapshot.digest.0
            && decode(&covered, "saved bootstrap covered")? == request.snapshot.covered.offset
            && decode(&captured, "saved bootstrap tail")? == request.captured_tail.offset;
        if !exact {
            return Err(ReplicationError::InvalidInput(
                "replication operation identity was reused".into(),
            ));
        }
        let mut lease = state.origin_leases.get(&request.bootstrap_id).copied();
        let now = options.snapshot_clock.now();
        if completed == 0 && lease.is_none() {
            let persisted: Option<(Vec<u8>, Vec<u8>)> = conn
                .query_row(
                    "SELECT recovery_lease,protection_expires FROM replication_origin_bootstraps WHERE bootstrap_id=?1 AND replica_id=?2 AND origin_id=?3 AND public_id=?4 AND incarnation=?5",
                    params![request.bootstrap_id.0.as_slice(),request.replica.as_str(),request.stream.origin.0.as_slice(),request.stream.stream.id.as_str(),request.stream.stream.incarnation.0.as_slice()],
                    |row| Ok((row.get(0)?,row.get(1)?)),
                )
                .optional()
                .map_err(|error|corrupt("read persisted bootstrap protection",error))?;
            if let Some((id, persisted_expires)) = persisted {
                let id = RecoveryLeaseId(id.try_into().map_err(|_| {
                    ReplicationError::CorruptStorage(
                        "invalid persisted bootstrap protection".into(),
                    )
                })?);
                let persisted_expires =
                    decode(&persisted_expires, "persisted bootstrap protection")?;
                if persisted_expires == decode(&expires, "saved bootstrap protection")? {
                    match snapshot_state.restore_replication_lease(
                        conn,
                        id,
                        &request.snapshot,
                        request.captured_tail.offset,
                        MonotonicTick(persisted_expires),
                        options,
                    ) {
                        Ok(()) => {
                            state.origin_leases.insert(request.bootstrap_id, id);
                            lease = Some(id);
                        }
                        Err(
                            SnapshotError::ExpiredProtection { .. }
                            | SnapshotError::NotFound { .. }
                            | SnapshotError::MissingHistory { .. },
                        ) => {}
                        Err(error) => return Err(snapshot_error(error)),
                    }
                }
            }
        }
        if completed == 0
            && lease.is_none_or(|lease| !snapshot_state.replication_lease_is_live(lease, now))
        {
            if let Some(lease) = lease {
                snapshot_state.release_for_replication(lease);
            }
            state.origin_leases.remove(&request.bootstrap_id);
            let tx = conn
                .transaction_with_behavior(TransactionBehavior::Immediate)
                .map_err(|error| storage("begin expired origin bootstrap cleanup", error))?;
            let removed = tx.execute(
                "DELETE FROM replication_origin_bootstraps WHERE bootstrap_id=?1 AND replica_id=?2 AND origin_id=?3 AND public_id=?4 AND incarnation=?5",
                params![request.bootstrap_id.0.as_slice(),request.replica.as_str(),request.stream.origin.0.as_slice(),request.stream.stream.id.as_str(),request.stream.stream.incarnation.0.as_slice()],
            )
            .map_err(|error| storage("remove expired origin bootstrap", error))?;
            if removed == 1 {
                tx.execute("UPDATE replication_origin_replicas SET mode=1,backlog_records=0,backlog_bytes=zeroblob(8),oldest_backlog_at=NULL,pending_batch=NULL WHERE replica_id=?1 AND origin_id=?2 AND public_id=?3 AND incarnation=?4 AND mode=2",params![request.replica.as_str(),request.stream.origin.0.as_slice(),request.stream.stream.id.as_str(),request.stream.stream.incarnation.0.as_slice()]).map_err(|error|storage("expire origin bootstrap",error))?;
            }
            tx.commit()
                .map_err(|error| storage("commit expired origin bootstrap", error))?;
            return Err(ReplicationError::BootstrapProtectionExpired {
                id: request.bootstrap_id,
            });
        }
        return Ok(BeginOriginBootstrapReceipt {
            status: ReplicaStatus {
                replica: request.replica.clone(),
                stream: request.stream.clone(),
                destination_epoch: request.destination_epoch,
                acknowledged: ReplicaPosition {
                    stream: request.stream.clone(),
                    offset: request.snapshot.covered.offset,
                },
                backlog_records: usize::try_from(status_records).map_err(|_| {
                    ReplicationError::CorruptStorage("invalid saved bootstrap row count".into())
                })?,
                backlog_bytes: decode(&status_bytes, "saved bootstrap bytes")?,
                oldest_backlog_at: status_oldest
                    .map(|value| {
                        decode(&value, "saved bootstrap oldest").map(DurableTimestampMillis)
                    })
                    .transpose()?,
                mode: match status_mode {
                    0 => ReplicaMode::Required,
                    2 => ReplicaMode::Bootstrapping,
                    _ => {
                        return Err(ReplicationError::CorruptStorage(
                            "invalid saved bootstrap mode".into(),
                        ))
                    }
                },
                pending_batch: None,
            },
            protection_expires_at: MonotonicTick(decode(&expires, "saved bootstrap protection")?),
            request,
        });
    }
    if request.captured_tail.stream != request.stream
        || request.snapshot.covered.stream != request.stream.stream
        || request.snapshot.covered.offset > request.captured_tail.offset
    {
        return Err(ReplicationError::InvalidInput(
            "origin bootstrap boundaries must name one ordered stream".into(),
        ));
    }
    let current = status(conn, &request.replica, &request.stream)?;
    if current.mode != ReplicaMode::DetachedNeedsBootstrap {
        return Err(ReplicationError::InvalidInput(
            "replica is not waiting for bootstrap".into(),
        ));
    }
    if current.destination_epoch != request.destination_epoch {
        return Err(ReplicationError::DestinationReplaced {
            current: current.destination_epoch,
        });
    }
    let (plan, expires) = snapshot_state
        .acquire_for_replication(conn, request.snapshot.id, options)
        .map_err(snapshot_error)?;
    if plan.snapshot != request.snapshot {
        snapshot_state.release_for_replication(plan.lease);
        return Err(ReplicationError::InvalidInput(
            "bootstrap descriptor does not match the published snapshot".into(),
        ));
    }
    if request.captured_tail.offset > plan.through.offset {
        snapshot_state.release_for_replication(plan.lease);
        return Err(ReplicationError::StaleProgress {
            current: Box::new(ReplicaPosition {
                stream: request.stream.clone(),
                offset: plan.through.offset,
            }),
        });
    }
    let (key, _, _) = stream_row(conn, &request.stream.stream)
        .map_err(|error| ReplicationError::CorruptStorage(error.to_string()))?;
    let (records,bytes):(i64,i64)=conn.query_row("SELECT count(*),coalesce(sum(octet_length(event_id)+octet_length(schema_id)+octet_length(payload)+384),0) FROM (SELECT event_id,schema_id,payload FROM event_records WHERE stream_key=?1 AND offset>?2 AND offset<=?3 UNION ALL SELECT event_id,schema_id,payload FROM retention_generated_records WHERE stream_key=?1 AND offset>?2 AND offset<=?3)",params![key,be(request.snapshot.covered.offset).as_slice(),be(request.captured_tail.offset).as_slice()],|row|Ok((row.get(0)?,row.get(1)?))).map_err(|error|corrupt("measure bootstrap suffix",error))?;
    let bytes = u64::try_from(bytes)
        .map_err(|_| ReplicationError::CorruptStorage("invalid bootstrap suffix bytes".into()))?;
    let backlog_limit=conn.query_row("SELECT max_backlog_bytes FROM replication_origin_replicas WHERE replica_id=?1 AND origin_id=?2 AND public_id=?3 AND incarnation=?4",params![request.replica.as_str(),request.stream.origin.0.as_slice(),request.stream.stream.id.as_str(),request.stream.stream.incarnation.0.as_slice()],|row|row.get::<_,Vec<u8>>(0)).map_err(|error|corrupt("read bootstrap backlog limit",error)).and_then(|value|decode(&value,"bootstrap backlog limit"))?;
    if bytes > backlog_limit {
        snapshot_state.release_for_replication(plan.lease);
        return Err(ReplicationError::BacklogExceeded {
            limit_bytes: backlog_limit,
        });
    }
    let now = options.replication_clock.now();
    let tx = conn
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .map_err(|error| storage("begin origin bootstrap", error))?;
    tx.execute("INSERT OR IGNORE INTO replication_origin_record_times(stream_key,offset,committed_at) SELECT stream_key,offset,?1 FROM (SELECT stream_key,offset FROM event_records WHERE stream_key=?2 AND offset>?3 AND offset<=?4 UNION ALL SELECT stream_key,offset FROM retention_generated_records WHERE stream_key=?2 AND offset>?3 AND offset<=?4)",params![be(now.0).as_slice(),key,be(request.snapshot.covered.offset).as_slice(),be(request.captured_tail.offset).as_slice()]).map_err(|error|storage("protect origin bootstrap timestamps",error))?;
    let oldest:Option<Vec<u8>>=tx.query_row("SELECT committed_at FROM replication_origin_record_times WHERE stream_key=?1 AND offset>?2 ORDER BY offset LIMIT 1",params![key,be(request.snapshot.covered.offset).as_slice()],|row|row.get(0)).optional().map_err(|error|corrupt("read bootstrap oldest timestamp",error))?;
    tx.execute("INSERT INTO replication_origin_bootstraps VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14,?15,?16,?17,?18)",params![request.bootstrap_id.0.as_slice(),request.replica.as_str(),request.stream.origin.0.as_slice(),request.stream.stream.id.as_str(),request.stream.stream.incarnation.0.as_slice(),request.destination_operation_id.as_str(),request.destination_epoch.0.as_slice(),request.snapshot.id.0.as_slice(),be(request.snapshot.covered.offset).as_slice(),request.snapshot.schema.id.as_str(),i64::from(request.snapshot.schema.version),be(request.snapshot.content_bytes).as_slice(),request.snapshot.digest.0.as_slice(),be(request.captured_tail.offset).as_slice(),records,be(bytes).as_slice(),plan.lease.0.as_slice(),be(expires.0).as_slice()]).map_err(|error|storage("store origin bootstrap",error))?;
    tx.execute("UPDATE replication_origin_replicas SET acknowledged=?1,backlog_records=?2,backlog_bytes=?3,oldest_backlog_at=?4,mode=2,pending_batch=NULL WHERE replica_id=?5 AND origin_id=?6 AND public_id=?7 AND incarnation=?8",params![be(request.snapshot.covered.offset).as_slice(),records,be(bytes).as_slice(),oldest,request.replica.as_str(),request.stream.origin.0.as_slice(),request.stream.stream.id.as_str(),request.stream.stream.incarnation.0.as_slice()]).map_err(|error|storage("activate origin bootstrap",error))?;
    tx.execute("INSERT INTO replication_origin_bootstrap_receipts VALUES(?1,0,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14,?15,?16,?17,?18,?19,2,?20,0)",params![request.operation_id.as_str(),request.bootstrap_id.0.as_slice(),request.destination_operation_id.as_str(),request.replica.as_str(),request.stream.origin.0.as_slice(),request.stream.stream.id.as_str(),request.stream.stream.incarnation.0.as_slice(),request.destination_epoch.0.as_slice(),request.snapshot.id.0.as_slice(),be(request.snapshot.covered.offset).as_slice(),request.snapshot.schema.id.as_str(),i64::from(request.snapshot.schema.version),be(request.snapshot.content_bytes).as_slice(),request.snapshot.digest.0.as_slice(),be(request.captured_tail.offset).as_slice(),be(request.snapshot.covered.offset).as_slice(),records,be(bytes).as_slice(),oldest,be(expires.0).as_slice()]).map_err(|error|storage("store origin bootstrap receipt",error))?;
    tx.commit()
        .map_err(|_| ReplicationError::BootstrapDriveUnknown(Box::new(request.clone())))?;
    state.origin_leases.insert(request.bootstrap_id, plan.lease);
    Ok(BeginOriginBootstrapReceipt {
        status: status(conn, &request.replica, &request.stream)?,
        protection_expires_at: expires,
        request,
    })
}

fn acknowledge_origin_bootstrap(
    conn: &mut Connection,
    request: AcknowledgeOriginBootstrap,
    options: &SqliteOptions,
    state: &mut SqliteReplicationState,
    snapshot_state: &mut super::sqlite_snapshot::SqliteSnapshotState,
) -> ReplicationResult<AcknowledgeOriginBootstrapReceipt> {
    type SavedAck = (
        i64,
        Vec<u8>,
        String,
        String,
        Vec<u8>,
        String,
        Vec<u8>,
        Vec<u8>,
        Vec<u8>,
        Vec<u8>,
        String,
        i64,
        Vec<u8>,
        Vec<u8>,
        Vec<u8>,
        Vec<u8>,
        i64,
        Vec<u8>,
        Option<Vec<u8>>,
        i64,
    );
    let prior: Option<SavedAck> = conn
        .query_row(
            "SELECT kind,bootstrap_id,destination_operation_id,replica_id,origin_id,public_id,incarnation,destination_epoch,snapshot_id,covered,schema_id,schema_version,content_bytes,digest,captured_tail,status_ack,status_backlog_records,status_backlog_bytes,status_oldest,status_mode FROM replication_origin_bootstrap_receipts WHERE operation_id=?1",
            [request.operation_id.as_str()],
            |row| Ok((row.get(0)?,row.get(1)?,row.get(2)?,row.get(3)?,row.get(4)?,row.get(5)?,row.get(6)?,row.get(7)?,row.get(8)?,row.get(9)?,row.get(10)?,row.get(11)?,row.get(12)?,row.get(13)?,row.get(14)?,row.get(15)?,row.get(16)?,row.get(17)?,row.get(18)?,row.get(19)?)),
        )
        .optional()
        .map_err(|error| corrupt("read origin bootstrap acknowledgement receipt", error))?;
    if let Some((
        kind,
        bootstrap,
        destination_operation,
        replica,
        origin,
        public_id,
        incarnation,
        epoch,
        snapshot,
        covered,
        schema_id,
        schema_version,
        content_bytes,
        digest,
        captured,
        status_ack,
        status_records,
        status_bytes,
        status_oldest,
        status_mode,
    )) = prior
    {
        let exact = kind == 1
            && bootstrap == request.receipt.request.id.0
            && destination_operation == request.receipt.request.operation_id.as_str()
            && replica == request.replica.as_str()
            && origin == request.stream.origin.0
            && public_id == request.stream.stream.id.as_str()
            && incarnation == request.stream.stream.incarnation.0
            && epoch == request.receipt.request.destination_epoch.0
            && snapshot == request.receipt.request.snapshot.id.0
            && decode(&covered, "saved acknowledgement covered")?
                == request.receipt.request.snapshot.covered.offset
            && schema_id == request.receipt.request.snapshot.schema.id.as_str()
            && u32::try_from(schema_version).ok()
                == Some(request.receipt.request.snapshot.schema.version)
            && decode(&content_bytes, "saved acknowledgement content bytes")?
                == request.receipt.request.snapshot.content_bytes
            && digest == request.receipt.request.snapshot.digest.0
            && decode(&captured, "saved acknowledgement tail")?
                == request.receipt.request.through.offset
            && request.receipt.committed_through == request.receipt.request.through
            && request.receipt.request.stream == request.stream;
        if !exact {
            return Err(ReplicationError::InvalidInput(
                "replication operation identity was reused".into(),
            ));
        }
        return Ok(AcknowledgeOriginBootstrapReceipt {
            request: request.clone(),
            status: ReplicaStatus {
                replica: request.replica.clone(),
                stream: request.stream.clone(),
                destination_epoch: request.receipt.request.destination_epoch,
                acknowledged: ReplicaPosition {
                    stream: request.stream.clone(),
                    offset: decode(&status_ack, "saved acknowledgement status")?,
                },
                backlog_records: usize::try_from(status_records).map_err(|_| {
                    ReplicationError::CorruptStorage("invalid acknowledgement backlog count".into())
                })?,
                backlog_bytes: decode(&status_bytes, "saved acknowledgement backlog bytes")?,
                oldest_backlog_at: status_oldest
                    .map(|value| {
                        decode(&value, "saved acknowledgement oldest").map(DurableTimestampMillis)
                    })
                    .transpose()?,
                mode: match status_mode {
                    0 => ReplicaMode::Required,
                    1 => ReplicaMode::DetachedNeedsBootstrap,
                    2 => ReplicaMode::Bootstrapping,
                    _ => {
                        return Err(ReplicationError::CorruptStorage(
                            "invalid saved acknowledgement mode".into(),
                        ))
                    }
                },
                pending_batch: None,
            },
        });
    }

    type Active = (
        String,
        String,
        Vec<u8>,
        Vec<u8>,
        Vec<u8>,
        String,
        i64,
        Vec<u8>,
        Vec<u8>,
        Vec<u8>,
        i64,
        Vec<u8>,
        Vec<u8>,
        Vec<u8>,
    );
    let active: Option<Active> = conn.query_row(
        "SELECT destination_operation_id,replica_id,destination_epoch,snapshot_id,covered,schema_id,schema_version,content_bytes,digest,captured_tail,suffix_records,suffix_bytes,recovery_lease,protection_expires FROM replication_origin_bootstraps WHERE bootstrap_id=?1 AND origin_id=?2 AND public_id=?3 AND incarnation=?4",
        params![request.receipt.request.id.0.as_slice(),request.stream.origin.0.as_slice(),request.stream.stream.id.as_str(),request.stream.stream.incarnation.0.as_slice()],
        |row| Ok((row.get(0)?,row.get(1)?,row.get(2)?,row.get(3)?,row.get(4)?,row.get(5)?,row.get(6)?,row.get(7)?,row.get(8)?,row.get(9)?,row.get(10)?,row.get(11)?,row.get(12)?,row.get(13)?)),
    ).optional().map_err(|error|corrupt("read active origin bootstrap",error))?;
    let Some((
        destination_operation,
        replica,
        epoch,
        snapshot,
        covered,
        schema_id,
        schema_version,
        content_bytes,
        digest,
        captured,
        suffix_records,
        suffix_bytes,
        recovery_lease,
        protection_expires,
    )) = active
    else {
        return Err(ReplicationError::InvalidInput(
            "replica has no matching active bootstrap".into(),
        ));
    };
    let exact = request.receipt.request.stream == request.stream
        && request.receipt.request.operation_id.as_str() == destination_operation
        && request.receipt.request.replica == request.replica
        && request.replica.as_str() == replica
        && epoch == request.receipt.request.destination_epoch.0
        && snapshot == request.receipt.request.snapshot.id.0
        && request.receipt.request.snapshot.covered.stream == request.stream.stream
        && request.receipt.request.snapshot.covered.offset
            == decode(&covered, "active bootstrap covered")?
        && request.receipt.request.snapshot.schema.id.as_str() == schema_id
        && u32::try_from(schema_version).ok()
            == Some(request.receipt.request.snapshot.schema.version)
        && request.receipt.request.snapshot.content_bytes
            == decode(&content_bytes, "active bootstrap content bytes")?
        && digest == request.receipt.request.snapshot.digest.0
        && request.receipt.request.through.stream == request.stream
        && request.receipt.request.through.offset == decode(&captured, "active bootstrap tail")?
        && request.receipt.committed_through == request.receipt.request.through;
    if !exact {
        return Err(ReplicationError::InvalidReceipt(
            "bootstrap receipt does not match the exact active attempt".into(),
        ));
    }
    let recovery_lease = RecoveryLeaseId(recovery_lease.try_into().map_err(|_| {
        ReplicationError::CorruptStorage("invalid origin bootstrap recovery lease".into())
    })?);
    let expires = decode(&protection_expires, "origin bootstrap protection")?;
    let now = options.snapshot_clock.now();
    let mut in_memory = state
        .origin_leases
        .get(&request.receipt.request.id)
        .copied();
    if in_memory.is_none() && now.0 < expires {
        match snapshot_state.restore_replication_lease(
            conn,
            recovery_lease,
            &request.receipt.request.snapshot,
            request.receipt.request.through.offset,
            MonotonicTick(expires),
            options,
        ) {
            Ok(()) => {
                state
                    .origin_leases
                    .insert(request.receipt.request.id, recovery_lease);
                in_memory = Some(recovery_lease);
            }
            Err(
                SnapshotError::ExpiredProtection { .. }
                | SnapshotError::NotFound { .. }
                | SnapshotError::MissingHistory { .. },
            ) => {}
            Err(error) => return Err(snapshot_error(error)),
        }
    }
    if now.0 >= expires
        || in_memory != Some(recovery_lease)
        || !snapshot_state.replication_lease_is_live(recovery_lease, now)
    {
        if in_memory == Some(recovery_lease) {
            state.origin_leases.remove(&request.receipt.request.id);
            snapshot_state.release_for_replication(recovery_lease);
        }
        let tx = conn
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|error| storage("begin expired bootstrap acknowledgement", error))?;
        let removed = tx.execute(
            "DELETE FROM replication_origin_bootstraps WHERE bootstrap_id=?1 AND replica_id=?2 AND origin_id=?3 AND public_id=?4 AND incarnation=?5",
            params![request.receipt.request.id.0.as_slice(),request.replica.as_str(),request.stream.origin.0.as_slice(),request.stream.stream.id.as_str(),request.stream.stream.incarnation.0.as_slice()],
        ).map_err(|error|storage("remove expired acknowledged bootstrap",error))?;
        if removed == 1 {
            tx.execute("UPDATE replication_origin_replicas SET mode=1,backlog_records=0,backlog_bytes=zeroblob(8),oldest_backlog_at=NULL,pending_batch=NULL WHERE replica_id=?1 AND origin_id=?2 AND public_id=?3 AND incarnation=?4 AND mode=2",params![request.replica.as_str(),request.stream.origin.0.as_slice(),request.stream.stream.id.as_str(),request.stream.stream.incarnation.0.as_slice()]).map_err(|error|storage("detach expired acknowledged bootstrap",error))?;
        }
        tx.commit()
            .map_err(|error| storage("commit expired bootstrap acknowledgement", error))?;
        return Err(ReplicationError::BootstrapProtectionExpired {
            id: request.receipt.request.id,
        });
    }
    let suffix_records = usize::try_from(suffix_records)
        .map_err(|_| ReplicationError::CorruptStorage("invalid bootstrap suffix count".into()))?;
    let suffix_bytes = decode(&suffix_bytes, "bootstrap suffix bytes")?;
    let current = status(conn, &request.replica, &request.stream)?;
    if current.mode != ReplicaMode::Bootstrapping
        || current.acknowledged.offset != request.receipt.request.snapshot.covered.offset
    {
        return Err(ReplicationError::CorruptStorage(
            "active bootstrap and replica status disagree".into(),
        ));
    }
    let next_records = current
        .backlog_records
        .checked_sub(suffix_records)
        .ok_or_else(|| {
            ReplicationError::CorruptStorage("bootstrap backlog row underflow".into())
        })?;
    let next_bytes = current
        .backlog_bytes
        .checked_sub(suffix_bytes)
        .ok_or_else(|| {
            ReplicationError::CorruptStorage("bootstrap backlog byte underflow".into())
        })?;
    let (key, _, _) = stream_row(conn, &request.stream.stream)
        .map_err(|error| ReplicationError::CorruptStorage(error.to_string()))?;
    let next_oldest: Option<Vec<u8>> = conn.query_row(
        "SELECT committed_at FROM replication_origin_record_times WHERE stream_key=?1 AND offset>?2 ORDER BY offset LIMIT 1",
        params![key,be(request.receipt.committed_through.offset).as_slice()],
        |row| row.get(0),
    ).optional().map_err(|error|corrupt("read post-bootstrap oldest timestamp",error))?;
    let begin_operation: String = conn.query_row(
        "SELECT operation_id FROM replication_origin_bootstrap_receipts WHERE kind=0 AND bootstrap_id=?1",
        [request.receipt.request.id.0.as_slice()],
        |row| row.get(0),
    ).map_err(|error|corrupt("read origin bootstrap begin operation",error))?;
    let tx = conn
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .map_err(|error| storage("begin origin bootstrap acknowledgement", error))?;
    tx.execute("UPDATE replication_origin_replicas SET acknowledged=?1,backlog_records=?2,backlog_bytes=?3,oldest_backlog_at=?4,mode=0,pending_batch=NULL WHERE replica_id=?5 AND origin_id=?6 AND public_id=?7 AND incarnation=?8 AND mode=2",params![be(request.receipt.committed_through.offset).as_slice(),i64::try_from(next_records).map_err(|_|ReplicationError::CapacityExceeded)?,be(next_bytes).as_slice(),next_oldest,request.replica.as_str(),request.stream.origin.0.as_slice(),request.stream.stream.id.as_str(),request.stream.stream.incarnation.0.as_slice()]).map_err(|error|storage("acknowledge origin bootstrap status",error))?;
    tx.execute(
        "DELETE FROM replication_origin_bootstraps WHERE bootstrap_id=?1",
        [request.receipt.request.id.0.as_slice()],
    )
    .map_err(|error| storage("remove acknowledged origin bootstrap", error))?;
    tx.execute("UPDATE replication_origin_bootstrap_receipts SET completed=1 WHERE operation_id=?1 AND kind=0",[begin_operation.as_str()]).map_err(|error|storage("complete origin bootstrap begin receipt",error))?;
    tx.execute("INSERT INTO replication_origin_bootstrap_receipts(operation_id,kind,bootstrap_id,destination_operation_id,replica_id,origin_id,public_id,incarnation,destination_epoch,snapshot_id,covered,schema_id,schema_version,content_bytes,digest,captured_tail,status_ack,status_backlog_records,status_backlog_bytes,status_oldest,status_mode,protection_expires,completed) VALUES(?1,1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14,?15,?16,?17,?18,?19,0,NULL,1)",params![request.operation_id.as_str(),request.receipt.request.id.0.as_slice(),request.receipt.request.operation_id.as_str(),request.replica.as_str(),request.stream.origin.0.as_slice(),request.stream.stream.id.as_str(),request.stream.stream.incarnation.0.as_slice(),request.receipt.request.destination_epoch.0.as_slice(),request.receipt.request.snapshot.id.0.as_slice(),be(request.receipt.request.snapshot.covered.offset).as_slice(),request.receipt.request.snapshot.schema.id.as_str(),i64::from(request.receipt.request.snapshot.schema.version),be(request.receipt.request.snapshot.content_bytes).as_slice(),request.receipt.request.snapshot.digest.0.as_slice(),be(request.receipt.request.through.offset).as_slice(),be(request.receipt.committed_through.offset).as_slice(),i64::try_from(next_records).map_err(|_|ReplicationError::CapacityExceeded)?,be(next_bytes).as_slice(),next_oldest]).map_err(|error|storage("store origin bootstrap acknowledgement",error))?;
    let begin_request = BeginOriginBootstrap {
        operation_id: ReplicationOperationId::new(begin_operation).map_err(|_| {
            ReplicationError::CorruptStorage("invalid begin operation identity".into())
        })?,
        bootstrap_id: request.receipt.request.id,
        destination_operation_id: request.receipt.request.operation_id.clone(),
        replica: request.replica.clone(),
        stream: request.stream.clone(),
        destination_epoch: request.receipt.request.destination_epoch,
        snapshot: request.receipt.request.snapshot.clone(),
        captured_tail: request.receipt.request.through.clone(),
    };
    tx.commit()
        .map_err(|_| ReplicationError::BootstrapDriveUnknown(Box::new(begin_request)))?;
    state.origin_leases.remove(&request.receipt.request.id);
    snapshot_state.release_for_replication(recovery_lease);
    Ok(AcknowledgeOriginBootstrapReceipt {
        request: request.clone(),
        status: status(conn, &request.replica, &request.stream)?,
    })
}

fn cleanup_origin(
    conn: &mut Connection,
    limits: ReplicaCleanupLimits,
) -> ReplicationResult<ReplicaCleanupProgress> {
    if limits.max_receipt_rows == 0 || limits.max_staging_rows == 0 || limits.max_bytes == 0 {
        return Err(ReplicationError::InvalidInput(
            "replication cleanup limits must be nonzero".into(),
        ));
    }
    let mut removed = 0usize;
    let mut removed_bytes = 0usize;
    while removed < limits.max_staging_rows {
        type DetachedReplicaRow = (String, Vec<u8>, String, Vec<u8>, i64);
        let row: Option<DetachedReplicaRow> = conn
            .query_row(
                "SELECT replica_id,origin_id,public_id,incarnation,
                        octet_length(replica_id)+octet_length(public_id)+192
                 FROM replication_origin_replicas r
                 WHERE mode=1 AND pending_batch IS NULL
                   AND NOT EXISTS(SELECT 1 FROM replication_origin_bootstraps b
                                  WHERE b.replica_id=r.replica_id AND b.origin_id=r.origin_id
                                    AND b.public_id=r.public_id AND b.incarnation=r.incarnation)
                 ORDER BY replica_id,origin_id,public_id,incarnation LIMIT 1",
                [],
                |row| {
                    Ok((
                        row.get(0)?,
                        row.get(1)?,
                        row.get(2)?,
                        row.get(3)?,
                        row.get(4)?,
                    ))
                },
            )
            .optional()
            .map_err(|error| corrupt("find detached replica cleanup", error))?;
        let Some((replica, origin, public_id, incarnation, charge)) = row else {
            break;
        };
        let charge = usize::try_from(charge).map_err(|_| {
            ReplicationError::CorruptStorage("invalid detached replica cleanup charge".into())
        })?;
        if removed_bytes
            .checked_add(charge)
            .is_none_or(|bytes| bytes > limits.max_bytes)
        {
            break;
        }
        conn.execute(
            "DELETE FROM replication_origin_replicas WHERE replica_id=?1 AND origin_id=?2 AND public_id=?3 AND incarnation=?4 AND mode=1 AND pending_batch IS NULL",
            params![replica,origin,public_id,incarnation],
        )
        .map_err(|error| storage("remove detached replica", error))?;
        removed += 1;
        removed_bytes += charge;
    }
    let remaining: bool = conn
        .query_row(
            "SELECT EXISTS(SELECT 1 FROM replication_origin_replicas r WHERE mode=1 AND pending_batch IS NULL AND NOT EXISTS(SELECT 1 FROM replication_origin_bootstraps b WHERE b.replica_id=r.replica_id AND b.origin_id=r.origin_id AND b.public_id=r.public_id AND b.incarnation=r.incarnation))",
            [],
            |row| row.get(0),
        )
        .map_err(|error| corrupt("inspect detached replica cleanup", error))?;
    Ok(ReplicaCleanupProgress {
        removed_receipt_rows: 0,
        removed_staging_rows: removed,
        removed_bytes,
        remaining,
    })
}

pub(super) fn handle_replication_command(
    conn: &mut Connection,
    command: ReplicationCommand,
    options: &SqliteOptions,
    state: &mut SqliteReplicationState,
    snapshot_state: &mut super::sqlite_snapshot::SqliteSnapshotState,
    failure_injection: &mut Option<super::sqlite::SqliteFailureInjection>,
) {
    match command {
        ReplicationCommand::Origin(tx) => {
            let _ = tx.send(origin_id(conn));
        }
        ReplicationCommand::Attach(v, tx) => {
            let _ = tx.send(attach(conn, v, options));
        }
        ReplicationCommand::Status(r, s, tx) => {
            let _ = tx.send(status(conn, &r, &s));
        }
        ReplicationCommand::Prepare(v, tx) => {
            let _ = tx.send(prepare(conn, v, options));
        }
        ReplicationCommand::Acknowledge(v, tx) => {
            let _ = tx.send(acknowledge(conn, v));
        }
        ReplicationCommand::Detach(v, tx) => {
            let _ = tx.send(detach(conn, v));
        }
        ReplicationCommand::DestinationEpoch(tx) => {
            let _ = tx.send(destination_epoch(conn));
        }
        ReplicationCommand::Commit(v, tx) => {
            let _ = tx.send(commit_destination(conn, v, options, failure_injection));
        }
        ReplicationCommand::Read(v, l, tx) => {
            let _ = tx.send(read_destination(conn, v, l, options));
        }
        ReplicationCommand::Floor(v, tx) => {
            let _ = tx.send(floor_destination(conn, v, options));
        }
        ReplicationCommand::BeginOriginBootstrap(request, tx) => {
            let _ = tx.send(begin_origin_bootstrap(
                conn,
                request,
                options,
                state,
                snapshot_state,
            ));
        }
        ReplicationCommand::AcknowledgeOriginBootstrap(request, tx) => {
            let _ = tx.send(acknowledge_origin_bootstrap(
                conn,
                request,
                options,
                state,
                snapshot_state,
            ));
        }
        ReplicationCommand::CleanupOrigin(limits, tx) => {
            let _ = tx.send(cleanup_origin(conn, limits));
        }
        ReplicationCommand::BeginDestinationBootstrap(request, tx) => {
            let _ = tx.send(begin_destination_bootstrap(conn, request, options));
        }
        ReplicationCommand::PutBootstrapChunk(request, tx) => {
            let _ = tx.send(put_bootstrap_chunk(conn, request, options, state));
        }
        ReplicationCommand::PutBootstrapBatch(request, tx) => {
            let _ = tx.send(put_bootstrap_batch(
                conn,
                request,
                options,
                state,
                failure_injection,
            ));
        }
        ReplicationCommand::VerifyBootstrap(request, tx) => {
            let _ = tx.send(verify_bootstrap(conn, request, options, state));
        }
        ReplicationCommand::PublishBootstrap(request, tx) => {
            let _ = tx.send(publish_bootstrap(conn, request, options, failure_injection));
        }
        ReplicationCommand::PublishedBootstrap(stream, tx) => {
            let _ = tx.send(published_bootstrap(conn, &stream));
        }
        ReplicationCommand::AcquireBootstrapRead(stream, lifetime, tx) => {
            let _ = tx.send(acquire_bootstrap_read(
                conn, stream, lifetime, options, state,
            ));
        }
        ReplicationCommand::ReadBootstrapBytes(lease, offset, max_bytes, tx) => {
            let _ = tx.send(read_bootstrap_bytes(
                conn, lease, offset, max_bytes, options, state,
            ));
        }
        ReplicationCommand::ReleaseBootstrapRead(lease, tx) => {
            let result = release_bootstrap_reader(state, lease).map(|released| {
                if released {
                    ReplicaReadRelease::Released
                } else {
                    ReplicaReadRelease::AlreadyReleased
                }
            });
            let _ = tx.send(result);
        }
        ReplicationCommand::AbortBootstrap(request, tx) => {
            let _ = tx.send(abort_bootstrap(conn, request));
        }
        ReplicationCommand::CleanupDestination(limits, tx) => {
            let _ = tx.send(cleanup_destination(conn, limits, options, state));
        }
    }
}

pub(super) fn fail_replication_command(command: ReplicationCommand) {
    let error = || ReplicationError::StorageFailure("SQLite worker cannot be safely reused".into());
    match command {
        ReplicationCommand::Origin(tx) => {
            let _ = tx.send(Err(error()));
        }
        ReplicationCommand::Attach(_, tx) => {
            let _ = tx.send(Err(error()));
        }
        ReplicationCommand::Status(_, _, tx) => {
            let _ = tx.send(Err(error()));
        }
        ReplicationCommand::Prepare(_, tx) => {
            let _ = tx.send(Err(error()));
        }
        ReplicationCommand::Acknowledge(_, tx) => {
            let _ = tx.send(Err(error()));
        }
        ReplicationCommand::Detach(_, tx) => {
            let _ = tx.send(Err(error()));
        }
        ReplicationCommand::DestinationEpoch(tx) => {
            let _ = tx.send(Err(error()));
        }
        ReplicationCommand::Commit(_, tx) => {
            let _ = tx.send(Err(error()));
        }
        ReplicationCommand::Read(_, _, tx) => {
            let _ = tx.send(Err(error()));
        }
        ReplicationCommand::Floor(_, tx) => {
            let _ = tx.send(Err(error()));
        }
        ReplicationCommand::BeginOriginBootstrap(_, tx) => {
            let _ = tx.send(Err(error()));
        }
        ReplicationCommand::AcknowledgeOriginBootstrap(_, tx) => {
            let _ = tx.send(Err(error()));
        }
        ReplicationCommand::CleanupOrigin(_, tx)
        | ReplicationCommand::CleanupDestination(_, tx) => {
            let _ = tx.send(Err(error()));
        }
        ReplicationCommand::BeginDestinationBootstrap(_, tx) => {
            let _ = tx.send(Err(error()));
        }
        ReplicationCommand::PutBootstrapChunk(_, tx) => {
            let _ = tx.send(Err(error()));
        }
        ReplicationCommand::PutBootstrapBatch(_, tx) => {
            let _ = tx.send(Err(error()));
        }
        ReplicationCommand::VerifyBootstrap(_, tx) => {
            let _ = tx.send(Err(error()));
        }
        ReplicationCommand::PublishBootstrap(_, tx) => {
            let _ = tx.send(Err(error()));
        }
        ReplicationCommand::PublishedBootstrap(_, tx) => {
            let _ = tx.send(Err(error()));
        }
        ReplicationCommand::AcquireBootstrapRead(_, _, tx) => {
            let _ = tx.send(Err(error()));
        }
        ReplicationCommand::ReadBootstrapBytes(_, _, _, tx) => {
            let _ = tx.send(Err(error()));
        }
        ReplicationCommand::ReleaseBootstrapRead(_, tx) => {
            let _ = tx.send(Err(error()));
        }
        ReplicationCommand::AbortBootstrap(_, tx) => {
            let _ = tx.send(Err(error()));
        }
    }
}

async fn receive<T>(rx: oneshot::Receiver<ReplicationResult<T>>) -> ReplicationResult<T> {
    rx.await.unwrap_or(Err(ReplicationError::Closed))
}
fn replication_command(command: ReplicationCommand) -> Command {
    Command::Replication(Box::new(command))
}

fn submitted(error: crate::application::Error) -> ReplicationError {
    match error {
        crate::application::Error::Overloaded => ReplicationError::Overloaded,
        crate::application::Error::Closed => ReplicationError::Closed,
        other => ReplicationError::StorageFailure(other.to_string()),
    }
}

#[async_trait]
impl ReplicationOriginStore for SqliteStore {
    async fn origin_identity(&self) -> ReplicationResult<OriginId> {
        let (tx, rx) = oneshot::channel();
        self.submit(replication_command(ReplicationCommand::Origin(tx)))
            .map_err(submitted)?;
        receive(rx).await
    }
    async fn attach_replica(&self, v: AttachReplica) -> ReplicationResult<AttachReplicaReceipt> {
        let (tx, rx) = oneshot::channel();
        self.submit(replication_command(ReplicationCommand::Attach(v, tx)))
            .map_err(submitted)?;
        receive(rx).await
    }
    async fn replica_status(
        &self,
        r: &ReplicaId,
        s: &OriginStream,
    ) -> ReplicationResult<ReplicaStatus> {
        let (tx, rx) = oneshot::channel();
        self.submit(replication_command(ReplicationCommand::Status(
            r.clone(),
            s.clone(),
            tx,
        )))
        .map_err(submitted)?;
        receive(rx).await
    }
    async fn prepare_replica_batch(
        &self,
        v: PrepareReplicaBatch,
    ) -> ReplicationResult<PrepareReplicaBatchReceipt> {
        let (tx, rx) = oneshot::channel();
        self.submit(replication_command(ReplicationCommand::Prepare(v, tx)))
            .map_err(submitted)?;
        receive(rx).await
    }
    async fn acknowledge_replica_batch(
        &self,
        v: AcknowledgeReplicaBatch,
    ) -> ReplicationResult<AcknowledgeReplicaBatchReceipt> {
        let (tx, rx) = oneshot::channel();
        self.submit(replication_command(ReplicationCommand::Acknowledge(v, tx)))
            .map_err(submitted)?;
        receive(rx).await
    }
    async fn detach_replica(&self, v: DetachReplica) -> ReplicationResult<DetachReplicaReceipt> {
        let (tx, rx) = oneshot::channel();
        self.submit(replication_command(ReplicationCommand::Detach(v, tx)))
            .map_err(submitted)?;
        receive(rx).await
    }
}

#[async_trait]
impl ReplicaBatchDestinationStore for SqliteStore {
    async fn destination_epoch(&self) -> ReplicationResult<DestinationEpoch> {
        let (tx, rx) = oneshot::channel();
        self.submit(replication_command(ReplicationCommand::DestinationEpoch(
            tx,
        )))
        .map_err(submitted)?;
        receive(rx).await
    }
    async fn commit_replica_batch(&self, v: ReplicaBatch) -> ReplicationResult<ReplicaReceipt> {
        let (tx, rx) = oneshot::channel();
        self.submit(replication_command(ReplicationCommand::Commit(v, tx)))
            .map_err(submitted)?;
        receive(rx).await
    }
    async fn read_replica_after(
        &self,
        v: &ReplicaPosition,
        l: ReplicaBatchLimits,
    ) -> ReplicationResult<ReplicaPage> {
        let (tx, rx) = oneshot::channel();
        self.submit(replication_command(ReplicationCommand::Read(
            v.clone(),
            l,
            tx,
        )))
        .map_err(submitted)?;
        receive(rx).await
    }
    async fn advance_replica_receipt_floor(
        &self,
        v: AdvanceReplicaReceiptFloor,
    ) -> ReplicationResult<AdvanceReplicaReceiptFloorReceipt> {
        let (tx, rx) = oneshot::channel();
        self.submit(replication_command(ReplicationCommand::Floor(v, tx)))
            .map_err(submitted)?;
        receive(rx).await
    }
    async fn close_replica_destination(&self) -> ReplicationResult<()> {
        EventStore::close(self).await.map_err(submitted)
    }
}

#[async_trait]
impl ReplicationStore for SqliteStore {
    async fn begin_origin_bootstrap(
        &self,
        request: BeginOriginBootstrap,
    ) -> ReplicationResult<BeginOriginBootstrapReceipt> {
        let (tx, rx) = oneshot::channel();
        self.submit(replication_command(
            ReplicationCommand::BeginOriginBootstrap(request, tx),
        ))
        .map_err(submitted)?;
        receive(rx).await
    }
    async fn acknowledge_origin_bootstrap(
        &self,
        request: AcknowledgeOriginBootstrap,
    ) -> ReplicationResult<AcknowledgeOriginBootstrapReceipt> {
        let (tx, rx) = oneshot::channel();
        self.submit(replication_command(
            ReplicationCommand::AcknowledgeOriginBootstrap(request, tx),
        ))
        .map_err(submitted)?;
        receive(rx).await
    }
    async fn cleanup_replication(
        &self,
        limits: ReplicaCleanupLimits,
    ) -> ReplicationResult<ReplicaCleanupProgress> {
        let (tx, rx) = oneshot::channel();
        self.submit(replication_command(ReplicationCommand::CleanupOrigin(
            limits, tx,
        )))
        .map_err(submitted)?;
        receive(rx).await
    }
}

#[async_trait]
impl ReplicaDestinationStore for SqliteStore {
    async fn begin_replica_bootstrap(
        &self,
        request: ReplicaBootstrap,
    ) -> ReplicationResult<BeginReplicaBootstrapReceipt> {
        let (tx, rx) = oneshot::channel();
        self.submit(replication_command(
            ReplicationCommand::BeginDestinationBootstrap(request, tx),
        ))
        .map_err(submitted)?;
        receive(rx).await
    }
    async fn put_replica_bootstrap_chunk(
        &self,
        request: ReplicaBootstrapChunk,
    ) -> ReplicationResult<ReplicaBootstrapChunkReceipt> {
        let (tx, rx) = oneshot::channel();
        self.submit(replication_command(ReplicationCommand::PutBootstrapChunk(
            request, tx,
        )))
        .map_err(submitted)?;
        receive(rx).await
    }
    async fn put_replica_bootstrap_batch(
        &self,
        request: ReplicaBootstrapBatch,
    ) -> ReplicationResult<ReplicaBootstrapBatchReceipt> {
        let (tx, rx) = oneshot::channel();
        self.submit(replication_command(ReplicationCommand::PutBootstrapBatch(
            request, tx,
        )))
        .map_err(submitted)?;
        receive(rx).await
    }
    async fn verify_replica_bootstrap_step(
        &self,
        request: VerifyReplicaBootstrap,
    ) -> ReplicationResult<ReplicaBootstrapVerificationProgress> {
        let (tx, rx) = oneshot::channel();
        self.submit(replication_command(ReplicationCommand::VerifyBootstrap(
            request, tx,
        )))
        .map_err(submitted)?;
        receive(rx).await
    }
    async fn publish_replica_bootstrap(
        &self,
        request: PublishReplicaBootstrap,
    ) -> ReplicationResult<ReplicaBootstrapReceipt> {
        let (tx, rx) = oneshot::channel();
        self.submit(replication_command(ReplicationCommand::PublishBootstrap(
            request, tx,
        )))
        .map_err(submitted)?;
        receive(rx).await
    }
    async fn published_replica_bootstrap(
        &self,
        stream: &OriginStream,
    ) -> ReplicationResult<Option<PublishedReplicaBootstrap>> {
        let (tx, rx) = oneshot::channel();
        self.submit(replication_command(ReplicationCommand::PublishedBootstrap(
            stream.clone(),
            tx,
        )))
        .map_err(submitted)?;
        receive(rx).await
    }
    async fn acquire_replica_bootstrap_read(
        &self,
        stream: &OriginStream,
        lifetime: Duration,
    ) -> ReplicationResult<ReplicaBootstrapReadPlan> {
        let (tx, rx) = oneshot::channel();
        self.submit(replication_command(
            ReplicationCommand::AcquireBootstrapRead(stream.clone(), lifetime, tx),
        ))
        .map_err(submitted)?;
        receive(rx).await
    }
    async fn read_replica_bootstrap_bytes(
        &self,
        lease: ReplicaReadLeaseId,
        offset: u64,
        max_bytes: usize,
    ) -> ReplicationResult<ReplicaBootstrapBytePage> {
        let (tx, rx) = oneshot::channel();
        self.submit(replication_command(ReplicationCommand::ReadBootstrapBytes(
            lease, offset, max_bytes, tx,
        )))
        .map_err(submitted)?;
        receive(rx).await
    }
    async fn release_replica_bootstrap_read(
        &self,
        lease: ReplicaReadLeaseId,
    ) -> ReplicationResult<ReplicaReadRelease> {
        let (tx, rx) = oneshot::channel();
        self.submit(replication_command(
            ReplicationCommand::ReleaseBootstrapRead(lease, tx),
        ))
        .map_err(submitted)?;
        receive(rx).await
    }
    async fn abort_replica_bootstrap(
        &self,
        request: AbortReplicaBootstrap,
    ) -> ReplicationResult<AbortReplicaBootstrapReceipt> {
        let (tx, rx) = oneshot::channel();
        self.submit(replication_command(ReplicationCommand::AbortBootstrap(
            request, tx,
        )))
        .map_err(submitted)?;
        receive(rx).await
    }
    async fn cleanup_replica_destination(
        &self,
        limits: ReplicaCleanupLimits,
    ) -> ReplicationResult<ReplicaCleanupProgress> {
        let (tx, rx) = oneshot::channel();
        self.submit(replication_command(ReplicationCommand::CleanupDestination(
            limits, tx,
        )))
        .map_err(submitted)?;
        receive(rx).await
    }
}
