use super::sqlite::{
    be, decode_offset, stream_row, Command, SqliteFailureInjection, SqliteOptions, SqliteStore,
};
use crate::{application::*, domain::*};
use async_trait::async_trait;
use rusqlite::{params, Connection, OptionalExtension, Transaction, TransactionBehavior};
use std::sync::Arc;
use tokio::sync::oneshot;

const ENABLE: i64 = 0;
const ADVANCE: i64 = 1;
const EXPIRE: i64 = 2;
const FLOOR: i64 = 3;
const RECEIPT_OVERHEAD: usize = 256;
const STORED_RECORD_OVERHEAD: usize = 256;

pub(super) fn retry_policy_enabled(
    conn: &Connection,
    stream_key: i64,
) -> crate::application::Result<bool> {
    conn.query_row(
        "SELECT EXISTS(SELECT 1 FROM retention_streams WHERE stream_key=?1)",
        [stream_key],
        |row| row.get(0),
    )
    .map_err(|error| crate::application::Error::StoreCorrupt(format!("read retry policy: {error}")))
}

pub(super) fn remove_retry_identity_for_retired(
    tx: &Transaction<'_>,
    stream_key: i64,
    offset: u64,
) -> crate::application::Result<()> {
    let charge: Option<i64> = tx
        .query_row(
            "SELECT charge FROM retention_retry_identities WHERE stream_key=?1 AND offset=?2",
            params![stream_key, be(offset).as_slice()],
            |row| row.get(0),
        )
        .optional()
        .map_err(|error| {
            crate::application::Error::StoreCorrupt(format!("read retired retry identity: {error}"))
        })?;
    let Some(charge) = charge else { return Ok(()) };
    let current: Vec<u8> = tx
        .query_row(
            "SELECT retry_bytes FROM retention_metadata WHERE singleton=1",
            [],
            |row| row.get(0),
        )
        .map_err(|error| {
            crate::application::Error::StoreCorrupt(format!("read retry accounting: {error}"))
        })?;
    let next = decode_offset(&current)?
        .checked_sub(u64::try_from(charge).map_err(|_| {
            crate::application::Error::StoreCorrupt("retry charge is invalid".into())
        })?)
        .ok_or_else(|| {
            crate::application::Error::StoreCorrupt("retry byte counter underflow".into())
        })?;
    tx.execute(
        "DELETE FROM retention_retry_identities WHERE stream_key=?1 AND offset=?2",
        params![stream_key, be(offset).as_slice()],
    )
    .map_err(|error| {
        crate::application::Error::StoreWriteFailed(format!(
            "delete retired retry identity: {error}"
        ))
    })?;
    tx.execute("UPDATE retention_metadata SET retry_rows=retry_rows-1,retry_bytes=?1 WHERE singleton=1 AND retry_rows>=1",[be(next).as_slice()]).map_err(|error|crate::application::Error::StoreWriteFailed(format!("release retired retry accounting: {error}")))?;
    Ok(())
}

pub(super) fn remove_retired_retention_state(
    tx: &Transaction<'_>,
    stream_key: i64,
) -> crate::application::Result<()> {
    let pending = tx
        .execute(
            "DELETE FROM retention_cleanup WHERE stream_key=?1",
            [stream_key],
        )
        .map_err(|error| {
            crate::application::Error::StoreWriteFailed(format!(
                "remove retired retention cleanup: {error}"
            ))
        })?;
    if pending != 0 {
        tx.execute("UPDATE retention_metadata SET pending_count=pending_count-1 WHERE singleton=1 AND pending_count>=1",[]).map_err(|error|crate::application::Error::StoreWriteFailed(format!("release retired cleanup accounting: {error}")))?;
    }
    tx.execute(
        "DELETE FROM retention_streams WHERE stream_key=?1",
        [stream_key],
    )
    .map_err(|error| {
        crate::application::Error::StoreWriteFailed(format!("remove retired retry policy: {error}"))
    })?;
    Ok(())
}

pub(super) enum RetentionCommand {
    Status(StreamKey, oneshot::Sender<RetentionResult<RetentionStatus>>),
    Enable(
        EnableRetryPolicy,
        oneshot::Sender<RetentionResult<EnableRetryPolicyReceipt>>,
    ),
    Advance(
        AdvanceRetryGeneration,
        oneshot::Sender<RetentionResult<AdvanceRetryGenerationReceipt>>,
    ),
    Expire(
        ExpireRetryGenerations,
        oneshot::Sender<RetentionResult<ExpireRetryGenerationsReceipt>>,
    ),
    Append(
        StreamKey,
        GeneratedEvent,
        oneshot::Sender<RetentionResult<AppendReceipt>>,
    ),
    Lookup(
        StreamKey,
        RetryGeneration,
        EventId,
        oneshot::Sender<RetentionResult<Option<Arc<Record>>>>,
    ),
    Floor(
        AdvanceRetentionFloor,
        oneshot::Sender<RetentionResult<AdvanceRetentionFloorReceipt>>,
    ),
    Cleanup(
        RetentionCleanupLimits,
        oneshot::Sender<RetentionResult<RetentionCleanupProgress>>,
    ),
}

pub(super) fn initialize_retention_schema(conn: &Connection) -> crate::application::Result<()> {
    let existing: u32 = conn
        .query_row(
            "SELECT count(*) FROM sqlite_schema WHERE type='table' AND name IN
             ('retention_metadata','retention_streams','retention_retry_identities',
              'retention_generated_records','retention_receipts','retention_cleanup')",
            [],
            |row| row.get(0),
        )
        .map_err(|error| {
            crate::application::Error::StoreCorrupt(format!("inspect retention schema: {error}"))
        })?;
    if existing != 0 && existing != 6 {
        return Err(crate::application::Error::StoreCorrupt(
            "retention schema is only partially present".into(),
        ));
    }
    conn.execute_batch(
        "BEGIN IMMEDIATE;
         CREATE TABLE IF NOT EXISTS retention_metadata(
           singleton INTEGER PRIMARY KEY CHECK(singleton=1),
           receipt_count INTEGER NOT NULL CHECK(receipt_count>=0),
           receipt_bytes INTEGER NOT NULL CHECK(receipt_bytes>=0),
           retry_rows INTEGER NOT NULL CHECK(retry_rows>=0),
           retry_bytes BLOB NOT NULL CHECK(typeof(retry_bytes)='blob' AND length(retry_bytes)=8),
           pending_count INTEGER NOT NULL CHECK(pending_count>=0));
         CREATE TABLE IF NOT EXISTS retention_streams(
           stream_key INTEGER PRIMARY KEY,
           oldest_generation BLOB NOT NULL CHECK(typeof(oldest_generation)='blob' AND length(oldest_generation)=8),
           current_generation BLOB NOT NULL CHECK(typeof(current_generation)='blob' AND length(current_generation)=8),
           FOREIGN KEY(stream_key) REFERENCES event_streams(stream_key));
         CREATE TABLE IF NOT EXISTS retention_retry_identities(
           stream_key INTEGER NOT NULL,
           generation BLOB NOT NULL CHECK(typeof(generation)='blob' AND length(generation)=8),
           event_id TEXT NOT NULL,
           offset BLOB NOT NULL CHECK(typeof(offset)='blob' AND length(offset)=8),
           charge INTEGER NOT NULL CHECK(charge>=0),
           PRIMARY KEY(stream_key,generation,event_id),
           FOREIGN KEY(stream_key) REFERENCES event_streams(stream_key));
         CREATE TABLE IF NOT EXISTS retention_generated_records(
           stream_key INTEGER NOT NULL,
           generation BLOB NOT NULL CHECK(typeof(generation)='blob' AND length(generation)=8),
           offset BLOB NOT NULL CHECK(typeof(offset)='blob' AND length(offset)=8),
           event_id TEXT NOT NULL,
           schema_id TEXT NOT NULL,
           schema_version INTEGER NOT NULL,
           payload BLOB NOT NULL,
           PRIMARY KEY(stream_key,generation,event_id),
           UNIQUE(stream_key,offset),
           FOREIGN KEY(stream_key) REFERENCES event_streams(stream_key));
         CREATE TABLE IF NOT EXISTS retention_receipts(
           operation_id TEXT PRIMARY KEY,
           kind INTEGER NOT NULL CHECK(kind BETWEEN 0 AND 3),
           public_id TEXT NOT NULL,
           incarnation BLOB NOT NULL CHECK(typeof(incarnation)='blob' AND length(incarnation)=16),
           argument_one BLOB,
           argument_two BLOB,
           result_floor BLOB NOT NULL CHECK(typeof(result_floor)='blob' AND length(result_floor)=8),
           result_tail BLOB NOT NULL CHECK(typeof(result_tail)='blob' AND length(result_tail)=8),
           result_oldest BLOB NOT NULL CHECK(typeof(result_oldest)='blob' AND length(result_oldest)=8),
           result_current BLOB NOT NULL CHECK(typeof(result_current)='blob' AND length(result_current)=8),
           charge INTEGER NOT NULL CHECK(charge>=0));
         CREATE TABLE IF NOT EXISTS retention_cleanup(
           stream_key INTEGER PRIMARY KEY,
           FOREIGN KEY(stream_key) REFERENCES event_streams(stream_key));
         INSERT OR IGNORE INTO retention_metadata VALUES(1,0,0,0,zeroblob(8),0);
         COMMIT;",
    )
    .map_err(|error| {
        if error.sqlite_error_code() == Some(rusqlite::ErrorCode::DiskFull) {
            crate::application::Error::CapacityExceeded
        } else {
            crate::application::Error::StoreWriteFailed(format!(
                "initialize retention schema: {error}"
            ))
        }
    })?;
    validate_retention_counters(conn)
}

pub(super) fn validate_retention_counters(conn: &Connection) -> crate::application::Result<()> {
    let stored: (i64, i64, i64, Vec<u8>, i64) = conn
        .query_row(
            "SELECT receipt_count,receipt_bytes,retry_rows,retry_bytes,pending_count
             FROM retention_metadata WHERE singleton=1",
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
        .map_err(|error| {
            crate::application::Error::StoreCorrupt(format!("read retention counters: {error}"))
        })?;
    let retry_bytes = decode_offset(&stored.3)?;
    let actual_receipts: (i64, i64) = conn
        .query_row(
            "SELECT count(*),coalesce(sum(charge),0) FROM retention_receipts",
            [],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .map_err(|error| {
            crate::application::Error::StoreCorrupt(format!("audit retention receipts: {error}"))
        })?;
    let actual_retry: (i64, i64) = conn
        .query_row(
            "SELECT count(*),coalesce(sum(charge),0) FROM retention_retry_identities",
            [],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .map_err(|error| {
            crate::application::Error::StoreCorrupt(format!("audit retry records: {error}"))
        })?;
    let actual_pending: i64 = conn
        .query_row("SELECT count(*) FROM retention_cleanup", [], |row| {
            row.get(0)
        })
        .map_err(|error| {
            crate::application::Error::StoreCorrupt(format!("audit retention cleanup: {error}"))
        })?;
    let invalid_policies: i64 = conn
        .query_row(
            "SELECT count(*) FROM retention_streams
             WHERE oldest_generation>current_generation OR current_generation=zeroblob(8)",
            [],
            |row| row.get(0),
        )
        .map_err(|error| {
            crate::application::Error::StoreCorrupt(format!("audit retry policies: {error}"))
        })?;
    let invalid_identities: i64 = conn
        .query_row(
            "SELECT count(*) FROM retention_retry_identities i
             LEFT JOIN event_records l ON i.generation=zeroblob(8) AND l.stream_key=i.stream_key AND l.offset=i.offset AND l.event_id=i.event_id
             LEFT JOIN retention_generated_records g ON i.generation!=zeroblob(8) AND g.stream_key=i.stream_key AND g.offset=i.offset AND g.event_id=i.event_id AND g.generation=i.generation
             WHERE (i.generation=zeroblob(8) AND l.rowid IS NULL)
                OR (i.generation!=zeroblob(8) AND g.rowid IS NULL)
                OR i.charge!=coalesce(octet_length(l.event_id)+octet_length(l.schema_id)+octet_length(l.payload)+384,
                                      octet_length(g.event_id)+octet_length(g.schema_id)+octet_length(g.payload)+384)",
            [],
            |row| row.get(0),
        )
        .map_err(|error| crate::application::Error::StoreCorrupt(format!("audit retry identities: {error}")))?;
    let invalid_receipts: i64 = conn
        .query_row(
            "SELECT count(*) FROM retention_receipts
             WHERE octet_length(operation_id) NOT BETWEEN 1 AND 256
                OR octet_length(public_id) NOT BETWEEN 1 AND 256
                OR result_floor>result_tail OR result_oldest>result_current
                OR charge!=octet_length(operation_id)+octet_length(public_id)+256
                OR (kind=0 AND (argument_one IS NOT NULL OR argument_two IS NOT NULL))
                OR (kind=1 AND (typeof(argument_one)!='blob' OR length(argument_one)!=8 OR argument_two IS NOT NULL))
                OR (kind IN (2,3) AND (typeof(argument_one)!='blob' OR length(argument_one)!=8 OR typeof(argument_two)!='blob' OR length(argument_two)!=8))",
            [],
            |row| row.get(0),
        )
        .map_err(|error| crate::application::Error::StoreCorrupt(format!("audit retention receipts: {error}")))?;
    if stored.0 < 0
        || stored.1 < 0
        || stored.2 < 0
        || stored.4 < 0
        || invalid_policies != 0
        || invalid_identities != 0
        || invalid_receipts != 0
        || (stored.0, stored.1) != actual_receipts
        || (stored.2, retry_bytes) != (actual_retry.0, actual_retry.1 as u64)
        || stored.4 != actual_pending
    {
        return Err(crate::application::Error::StoreCorrupt(
            "retention counters do not match stored rows".into(),
        ));
    }
    Ok(())
}

fn map_store(error: crate::application::Error) -> RetentionError {
    match error {
        crate::application::Error::CapacityExceeded => RetentionError::CapacityExceeded,
        crate::application::Error::Closed => RetentionError::Closed,
        crate::application::Error::StreamNotFound
        | crate::application::Error::StreamUnavailable { .. } => {
            RetentionError::StaleIncarnation { current: None }
        }
        crate::application::Error::StaleIncarnation { current } => {
            let current = match *current {
                StreamAvailability::Active(key) | StreamAvailability::Unavailable(key) => key,
            };
            RetentionError::StaleIncarnation {
                current: Some(Box::new(current)),
            }
        }
        crate::application::Error::StoreCorrupt(message) => RetentionError::CorruptStorage(message),
        other => RetentionError::StorageFailure(other.to_string()),
    }
}

fn sqlite_failure(action: &str, error: rusqlite::Error) -> RetentionError {
    if error.sqlite_error_code() == Some(rusqlite::ErrorCode::DiskFull) {
        RetentionError::CapacityExceeded
    } else {
        RetentionError::StorageFailure(format!("{action}: {error}"))
    }
}

fn status(conn: &Connection, stream: &StreamKey) -> RetentionResult<RetentionStatus> {
    let (key, floor, tail) = stream_row(conn, stream).map_err(map_store)?;
    let generations = conn
        .query_row(
            "SELECT oldest_generation,current_generation FROM retention_streams WHERE stream_key=?1",
            [key],
            |row| Ok((row.get::<_, Vec<u8>>(0)?, row.get::<_, Vec<u8>>(1)?)),
        )
        .optional()
        .map_err(|error| sqlite_failure("read retry policy", error))?;
    let retry_policy = match generations {
        None => RetryPolicyState::Lifetime,
        Some((oldest, current)) => RetryPolicyState::Generational {
            oldest_accepted: RetryGeneration::new(decode_offset(&oldest).map_err(map_store)?),
            current: RetryGeneration::new(decode_offset(&current).map_err(map_store)?),
        },
    };
    Ok(RetentionStatus {
        bounds: Bounds {
            floor: Cursor::new(stream.clone(), floor),
            tail: Cursor::new(stream.clone(), tail),
        },
        retry_policy,
    })
}

fn receipt_charge(operation: &RetentionOperationId, stream: &StreamKey) -> RetentionResult<usize> {
    operation
        .as_str()
        .len()
        .checked_add(stream.id.as_str().len())
        .and_then(|value| value.checked_add(RECEIPT_OVERHEAD))
        .ok_or(RetentionError::CapacityExceeded)
}

fn record_charge(event: &NewEvent) -> RetentionResult<usize> {
    event
        .accounted_bytes()
        .checked_add(STORED_RECORD_OVERHEAD)
        .ok_or(RetentionError::CapacityExceeded)
}

fn reserve_receipt(
    tx: &Transaction<'_>,
    operation: &RetentionOperationId,
    stream: &StreamKey,
    options: &SqliteOptions,
) -> RetentionResult<usize> {
    if tx
        .query_row(
            "SELECT 1 FROM retention_receipts WHERE operation_id=?1",
            [operation.as_str()],
            |_| Ok(()),
        )
        .optional()
        .map_err(|error| sqlite_failure("look up retention operation", error))?
        .is_some()
    {
        return Err(RetentionError::OperationConflict {
            operation_id: operation.clone(),
        });
    }
    let charge = receipt_charge(operation, stream)?;
    let (count, bytes): (i64, i64) = tx
        .query_row(
            "SELECT receipt_count,receipt_bytes FROM retention_metadata WHERE singleton=1",
            [],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .map_err(|error| sqlite_failure("read retention receipt counters", error))?;
    if usize::try_from(count)
        .ok()
        .is_none_or(|value| value >= options.retention.operations.max_receipts)
        || usize::try_from(bytes)
            .ok()
            .and_then(|value| value.checked_add(charge))
            .is_none_or(|value| value > options.retention.operations.max_receipt_bytes)
    {
        return Err(RetentionError::CapacityExceeded);
    }
    Ok(charge)
}

fn enqueue_cleanup(tx: &Transaction<'_>, key: i64, options: &SqliteOptions) -> RetentionResult<()> {
    let inserted = tx
        .execute(
            "INSERT OR IGNORE INTO retention_cleanup(stream_key) VALUES(?1)",
            [key],
        )
        .map_err(|error| sqlite_failure("queue retention cleanup", error))?;
    if inserted == 0 {
        return Ok(());
    }
    let count: i64 = tx
        .query_row(
            "SELECT pending_count FROM retention_metadata WHERE singleton=1",
            [],
            |row| row.get(0),
        )
        .map_err(|error| sqlite_failure("read cleanup counter", error))?;
    if usize::try_from(count)
        .ok()
        .is_none_or(|value| value >= options.retention.operations.max_pending_cleanup_ranges)
    {
        return Err(RetentionError::CapacityExceeded);
    }
    tx.execute(
        "UPDATE retention_metadata SET pending_count=pending_count+1 WHERE singleton=1",
        [],
    )
    .map_err(|error| sqlite_failure("update cleanup counter", error))?;
    Ok(())
}

fn encode_policy(policy: &RetryPolicyState) -> (u64, u64) {
    match policy {
        RetryPolicyState::Lifetime => (0, 0),
        RetryPolicyState::Generational {
            oldest_accepted,
            current,
        } => (oldest_accepted.get(), current.get()),
    }
}

enum StoredReceipt {
    Enable(EnableRetryPolicyReceipt),
    Advance(AdvanceRetryGenerationReceipt),
    Expire(ExpireRetryGenerationsReceipt),
    Floor(AdvanceRetentionFloorReceipt),
}

fn read_receipt(
    conn: &Connection,
    operation: &RetentionOperationId,
) -> RetentionResult<Option<StoredReceipt>> {
    type ReceiptRow = (
        i64,
        String,
        Vec<u8>,
        Option<Vec<u8>>,
        Option<Vec<u8>>,
        Vec<u8>,
        Vec<u8>,
        Vec<u8>,
        Vec<u8>,
    );
    let row: Option<ReceiptRow> = conn.query_row(
        "SELECT kind,
                CASE WHEN typeof(public_id)='text' AND octet_length(public_id) BETWEEN 1 AND 256 THEN public_id END,
                CASE WHEN typeof(incarnation)='blob' AND length(incarnation)=16 THEN incarnation END,
                CASE WHEN argument_one IS NULL OR (typeof(argument_one)='blob' AND length(argument_one)=8) THEN argument_one END,
                CASE WHEN argument_two IS NULL OR (typeof(argument_two)='blob' AND length(argument_two)=8) THEN argument_two END,
                CASE WHEN typeof(result_floor)='blob' AND length(result_floor)=8 THEN result_floor END,
                CASE WHEN typeof(result_tail)='blob' AND length(result_tail)=8 THEN result_tail END,
                CASE WHEN typeof(result_oldest)='blob' AND length(result_oldest)=8 THEN result_oldest END,
                CASE WHEN typeof(result_current)='blob' AND length(result_current)=8 THEN result_current END
         FROM retention_receipts WHERE operation_id=?1",
        [operation.as_str()],
        |row| Ok((row.get(0)?,row.get(1)?,row.get(2)?,row.get(3)?,row.get(4)?,row.get(5)?,row.get(6)?,row.get(7)?,row.get(8)?)),
    ).optional().map_err(|error| sqlite_failure("read retention receipt",error))?;
    let Some((kind, public_id, incarnation, one, two, floor, tail, oldest, current)) = row else {
        return Ok(None);
    };
    let stream = StreamKey {
        id: StreamId::new(public_id).map_err(|_| {
            RetentionError::CorruptStorage("receipt stream identifier is invalid".into())
        })?,
        incarnation: IncarnationId(incarnation.try_into().map_err(|_| {
            RetentionError::CorruptStorage("receipt incarnation is invalid".into())
        })?),
    };
    let decode = |value: &[u8]| decode_offset(value).map_err(map_store);
    let status = RetentionStatus {
        bounds: Bounds {
            floor: Cursor::new(stream.clone(), decode(&floor)?),
            tail: Cursor::new(stream.clone(), decode(&tail)?),
        },
        retry_policy: RetryPolicyState::Generational {
            oldest_accepted: RetryGeneration::new(decode(&oldest)?),
            current: RetryGeneration::new(decode(&current)?),
        },
    };
    let one = one.as_deref().map(decode).transpose()?;
    let two = two.as_deref().map(decode).transpose()?;
    Ok(Some(match kind {
        ENABLE if one.is_none() && two.is_none() => {
            StoredReceipt::Enable(EnableRetryPolicyReceipt {
                request: EnableRetryPolicy {
                    operation_id: operation.clone(),
                    stream,
                },
                status,
            })
        }
        ADVANCE if one.is_some() && two.is_none() => {
            StoredReceipt::Advance(AdvanceRetryGenerationReceipt {
                request: AdvanceRetryGeneration {
                    operation_id: operation.clone(),
                    stream,
                    expected_current: RetryGeneration::new(one.unwrap()),
                },
                status,
            })
        }
        EXPIRE if one.is_some() && two.is_some() => {
            StoredReceipt::Expire(ExpireRetryGenerationsReceipt {
                request: ExpireRetryGenerations {
                    operation_id: operation.clone(),
                    stream,
                    expected_oldest: RetryGeneration::new(one.unwrap()),
                    retain_from: RetryGeneration::new(two.unwrap()),
                },
                status,
            })
        }
        FLOOR if one.is_some() && two.is_some() => {
            StoredReceipt::Floor(AdvanceRetentionFloorReceipt {
                request: AdvanceRetentionFloor {
                    operation_id: operation.clone(),
                    stream: stream.clone(),
                    expected_floor: Cursor::new(stream.clone(), one.unwrap()),
                    new_floor: Cursor::new(stream, two.unwrap()),
                },
                status,
            })
        }
        _ => {
            return Err(RetentionError::CorruptStorage(
                "retention receipt shape is invalid".into(),
            ))
        }
    }))
}

pub(super) fn policy(
    tx: &Transaction<'_>,
    key: i64,
) -> RetentionResult<(RetryGeneration, RetryGeneration)> {
    let row = tx.query_row("SELECT oldest_generation,current_generation FROM retention_streams WHERE stream_key=?1",[key],|row|Ok((row.get::<_,Vec<u8>>(0)?,row.get::<_,Vec<u8>>(1)?))).optional().map_err(|error|sqlite_failure("read retry policy",error))?.ok_or(RetentionError::RetryPolicyRequired)?;
    let oldest = RetryGeneration::new(decode_offset(&row.0).map_err(map_store)?);
    let current = RetryGeneration::new(decode_offset(&row.1).map_err(map_store)?);
    if oldest > current {
        return Err(RetentionError::CorruptStorage(
            "oldest retry generation exceeds current".into(),
        ));
    }
    Ok((oldest, current))
}

fn advance(
    conn: &mut Connection,
    request: AdvanceRetryGeneration,
    options: &SqliteOptions,
    failure: &mut Option<SqliteFailureInjection>,
) -> RetentionResult<AdvanceRetryGenerationReceipt> {
    if let Some(prior) = read_receipt(conn, &request.operation_id)? {
        return match prior {
            StoredReceipt::Advance(receipt) if receipt.request == request => Ok(receipt),
            _ => Err(RetentionError::OperationConflict {
                operation_id: request.operation_id,
            }),
        };
    }
    let tx = conn
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .map_err(|e| sqlite_failure("begin generation advance", e))?;
    let charge = reserve_receipt(&tx, &request.operation_id, &request.stream, options)?;
    let (key, floor, tail) = stream_row(&tx, &request.stream).map_err(map_store)?;
    let (oldest, current) = policy(&tx, key)?;
    if current != request.expected_current {
        return Err(RetentionError::StaleGeneration { current });
    }
    let next = current
        .checked_next()
        .ok_or_else(|| RetentionError::InvalidInput("retry generation cannot overflow".into()))?;
    tx.execute(
        "UPDATE retention_streams SET current_generation=?1 WHERE stream_key=?2",
        params![be(next.get()).as_slice(), key],
    )
    .map_err(|e| sqlite_failure("advance retry generation", e))?;
    let status = RetentionStatus {
        bounds: Bounds {
            floor: Cursor::new(request.stream.clone(), floor),
            tail: Cursor::new(request.stream.clone(), tail),
        },
        retry_policy: RetryPolicyState::Generational {
            oldest_accepted: oldest,
            current: next,
        },
    };
    insert_receipt(
        &tx,
        &request.operation_id,
        ADVANCE,
        &request.stream,
        (Some(current.get()), None),
        &status,
        charge,
    )?;
    let unknown = request.clone();
    commit_mutation(
        tx,
        AdvanceRetryGenerationReceipt { request, status },
        "commit generation advance",
        failure,
        || RetentionError::AdvanceGenerationUnknown(Box::new(unknown.clone())),
    )
}

fn expire(
    conn: &mut Connection,
    request: ExpireRetryGenerations,
    options: &SqliteOptions,
    failure: &mut Option<SqliteFailureInjection>,
) -> RetentionResult<ExpireRetryGenerationsReceipt> {
    if let Some(prior) = read_receipt(conn, &request.operation_id)? {
        return match prior {
            StoredReceipt::Expire(receipt) if receipt.request == request => Ok(receipt),
            _ => Err(RetentionError::OperationConflict {
                operation_id: request.operation_id,
            }),
        };
    }
    let tx = conn
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .map_err(|e| sqlite_failure("begin generation expiry", e))?;
    let charge = reserve_receipt(&tx, &request.operation_id, &request.stream, options)?;
    let (key, floor, tail) = stream_row(&tx, &request.stream).map_err(map_store)?;
    let (oldest, current) = policy(&tx, key)?;
    if oldest != request.expected_oldest {
        return Err(RetentionError::StaleGeneration { current: oldest });
    }
    if request.retain_from < oldest || request.retain_from > current {
        return Err(RetentionError::InvalidInput(
            "retry expiry must move within the accepted generation range".into(),
        ));
    }
    #[cfg(feature = "source-journal")]
    if let Some(oldest_required) = super::sqlite_source_journal::minimum_generation_pin(&tx, key)
        .map_err(|error| {
            RetentionError::CorruptStorage(format!("read source-journal generation pin: {error}"))
        })?
    {
        if request.retain_from > oldest_required {
            return Err(RetentionError::JournalProtectionActive { oldest_required });
        }
    }
    if request.retain_from > oldest {
        enqueue_cleanup(&tx, key, options)?;
    }
    tx.execute(
        "UPDATE retention_streams SET oldest_generation=?1 WHERE stream_key=?2",
        params![be(request.retain_from.get()).as_slice(), key],
    )
    .map_err(|e| sqlite_failure("expire retry generations", e))?;
    let status = RetentionStatus {
        bounds: Bounds {
            floor: Cursor::new(request.stream.clone(), floor),
            tail: Cursor::new(request.stream.clone(), tail),
        },
        retry_policy: RetryPolicyState::Generational {
            oldest_accepted: request.retain_from,
            current,
        },
    };
    insert_receipt(
        &tx,
        &request.operation_id,
        EXPIRE,
        &request.stream,
        (Some(oldest.get()), Some(request.retain_from.get())),
        &status,
        charge,
    )?;
    let unknown = request.clone();
    commit_mutation(
        tx,
        ExpireRetryGenerationsReceipt { request, status },
        "commit generation expiry",
        failure,
        || RetentionError::ExpireGenerationsUnknown(Box::new(unknown.clone())),
    )
}

fn decode_retry_record(
    row: &rusqlite::Row<'_>,
    stream: &StreamKey,
    max_record_bytes: usize,
) -> rusqlite::Result<Arc<Record>> {
    let offset: Vec<u8> = row.get(0)?;
    let event_id: String = row.get(1)?;
    let schema_id: String = row.get(2)?;
    let schema_version: i64 = row.get(3)?;
    let payload: Vec<u8> = row.get(4)?;
    if event_id.is_empty()
        || event_id.len() > 256
        || schema_id.is_empty()
        || schema_id.len() > 256
        || event_id
            .len()
            .saturating_add(schema_id.len())
            .saturating_add(payload.len())
            .saturating_add(128)
            > max_record_bytes
        || schema_version < 0
        || schema_version > u32::MAX as i64
    {
        return Err(rusqlite::Error::InvalidQuery);
    }
    let offset = decode_offset(&offset).map_err(|_| rusqlite::Error::InvalidQuery)?;
    Ok(Arc::new(Record {
        cursor: Cursor::new(stream.clone(), offset),
        event: NewEvent {
            id: EventId::new(event_id).map_err(|_| rusqlite::Error::InvalidQuery)?,
            schema: SchemaRef {
                id: SchemaId::new(schema_id).map_err(|_| rusqlite::Error::InvalidQuery)?,
                version: schema_version as u32,
            },
            payload: Payload::copy_from_slice(&payload),
        },
    }))
}

pub(super) fn lookup_retry(
    conn: &Connection,
    stream: &StreamKey,
    key: i64,
    generation: RetryGeneration,
    event_id: &EventId,
    max_record_bytes: usize,
) -> RetentionResult<Option<Arc<Record>>> {
    let table = if generation == RetryGeneration::LEGACY {
        "event_records"
    } else {
        "retention_generated_records"
    };
    let sql=format!("SELECT CASE WHEN typeof(r.offset)='blob' AND length(r.offset)=8 THEN r.offset END,CASE WHEN typeof(r.event_id)='text' AND octet_length(r.event_id) BETWEEN 1 AND 256 THEN r.event_id END,CASE WHEN typeof(r.schema_id)='text' AND octet_length(r.schema_id) BETWEEN 1 AND 256 THEN r.schema_id END,CASE WHEN typeof(r.schema_version)='integer' THEN r.schema_version END,CASE WHEN typeof(r.payload)='blob' AND octet_length(r.payload)<=?4 THEN r.payload END FROM {table} r JOIN retention_retry_identities i ON i.stream_key=r.stream_key AND i.offset=r.offset WHERE i.stream_key=?1 AND i.generation=?2 AND i.event_id=?3");
    conn.query_row(
        &sql,
        params![
            key,
            be(generation.get()).as_slice(),
            event_id.as_str(),
            i64::try_from(max_record_bytes).unwrap_or(i64::MAX)
        ],
        |row| decode_retry_record(row, stream, max_record_bytes),
    )
    .optional()
    .map_err(|e| RetentionError::CorruptStorage(format!("read retry record: {e}")))
}

fn append_generated(
    conn: &mut Connection,
    stream: StreamKey,
    event: GeneratedEvent,
    options: &SqliteOptions,
    failure: &mut Option<SqliteFailureInjection>,
) -> RetentionResult<AppendReceipt> {
    let unknown = GeneratedEventIdentity {
        stream: stream.clone(),
        generation: event.generation,
        event_id: event.event.id.clone(),
    };
    let tx = conn
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .map_err(|e| sqlite_failure("begin generated append", e))?;
    let receipt = append_generated_in_transaction(&tx, stream, event, options)?;
    if receipt.kind == AppendKind::Deduplicated {
        tx.rollback()
            .map_err(|e| sqlite_failure("finish generated retry", e))?;
        return Ok(receipt);
    }
    commit_mutation(tx, receipt, "commit generated append", failure, || {
        RetentionError::GeneratedAppendUnknown(Box::new(unknown.clone()))
    })
}

pub(super) fn append_generated_in_transaction(
    tx: &Transaction<'_>,
    stream: StreamKey,
    event: GeneratedEvent,
    options: &SqliteOptions,
) -> RetentionResult<AppendReceipt> {
    let charge = record_charge(&event.event)?;
    let (key, _floor, tail) = stream_row(tx, &stream).map_err(map_store)?;
    let (oldest, current) = policy(tx, key)?;
    if event.generation < oldest {
        return Err(RetentionError::RetryGenerationExpired {
            oldest_accepted: oldest,
        });
    }
    if event.generation > current {
        return Err(RetentionError::RetryGenerationAhead { current });
    }
    if let Some(record) = lookup_retry(
        tx,
        &stream,
        key,
        event.generation,
        &event.event.id,
        options.max_record_bytes,
    )? {
        if record.event.schema != event.event.schema || record.event.payload != event.event.payload
        {
            return Err(RetentionError::IdempotencyConflict {
                identity: Box::new(GeneratedEventIdentity {
                    stream,
                    generation: event.generation,
                    event_id: event.event.id,
                }),
            });
        }
        return Ok(AppendReceipt {
            record,
            kind: AppendKind::Deduplicated,
        });
    }
    if event.generation == RetryGeneration::LEGACY {
        return Err(RetentionError::LegacyRetryNotFound {
            event_id: event.event.id,
        });
    }
    let (rows, bytes_blob): (i64, Vec<u8>) = tx
        .query_row(
            "SELECT retry_rows,retry_bytes FROM retention_metadata WHERE singleton=1",
            [],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .map_err(|e| sqlite_failure("read retry counters", e))?;
    let bytes = decode_offset(&bytes_blob).map_err(map_store)?;
    if usize::try_from(rows)
        .ok()
        .is_none_or(|v| v >= options.retention.receipts.max_rows)
        || bytes
            .checked_add(charge as u64)
            .is_none_or(|v| v > options.retention.receipts.max_bytes)
    {
        return Err(RetentionError::CapacityExceeded);
    }
    let offset = tail
        .checked_add(1)
        .ok_or_else(|| RetentionError::InvalidInput("stream offset overflow".into()))?;
    #[cfg(feature = "replication")]
    let (replication_now, replication_charge) =
        super::sqlite_replication::preflight_replication_append(
            tx,
            &stream,
            key,
            &event.event,
            options,
        )
        .map_err(map_store)?;
    tx.execute("INSERT INTO retention_generated_records(stream_key,generation,offset,event_id,schema_id,schema_version,payload) VALUES(?1,?2,?3,?4,?5,?6,?7)",params![key,be(event.generation.get()).as_slice(),be(offset).as_slice(),event.event.id.as_str(),event.event.schema.id.as_str(),i64::from(event.event.schema.version),event.event.payload.as_bytes()]).map_err(|e|sqlite_failure("insert generated event",e))?;
    tx.execute(
        "INSERT INTO retention_retry_identities VALUES(?1,?2,?3,?4,?5)",
        params![
            key,
            be(event.generation.get()).as_slice(),
            event.event.id.as_str(),
            be(offset).as_slice(),
            i64::try_from(charge).map_err(|_| RetentionError::CapacityExceeded)?
        ],
    )
    .map_err(|e| sqlite_failure("insert generated retry identity", e))?;
    tx.execute(
        "UPDATE event_streams SET tail=?1 WHERE stream_key=?2",
        params![be(offset).as_slice(), key],
    )
    .map_err(|e| sqlite_failure("advance generated tail", e))?;
    tx.execute(
        "UPDATE retention_metadata SET retry_rows=retry_rows+1,retry_bytes=?1 WHERE singleton=1",
        [be(bytes + charge as u64).as_slice()],
    )
    .map_err(|e| sqlite_failure("update retry counters", e))?;
    #[cfg(feature = "replication")]
    super::sqlite_replication::apply_replication_append(
        tx,
        &stream,
        key,
        offset,
        replication_charge,
        replication_now,
    )
    .map_err(map_store)?;
    let record = Arc::new(Record {
        cursor: Cursor::new(stream, offset),
        event: event.event,
    });
    Ok(AppendReceipt {
        record,
        kind: AppendKind::Inserted,
    })
}

fn lookup_generated(
    conn: &Connection,
    stream: &StreamKey,
    generation: RetryGeneration,
    event_id: &EventId,
    options: &SqliteOptions,
) -> RetentionResult<Option<Arc<Record>>> {
    let (key, _, _) = stream_row(conn, stream).map_err(map_store)?;
    let tx = conn
        .unchecked_transaction()
        .map_err(|e| sqlite_failure("begin generated lookup", e))?;
    let (oldest, current) = policy(&tx, key)?;
    if generation < oldest {
        return Err(RetentionError::RetryGenerationExpired {
            oldest_accepted: oldest,
        });
    }
    if generation > current {
        return Err(RetentionError::RetryGenerationAhead { current });
    }
    let result = lookup_retry(
        &tx,
        stream,
        key,
        generation,
        event_id,
        options.max_record_bytes,
    )?;
    tx.rollback()
        .map_err(|e| sqlite_failure("finish generated lookup", e))?;
    Ok(result)
}

fn advance_floor(
    conn: &mut Connection,
    request: AdvanceRetentionFloor,
    options: &SqliteOptions,
    snapshot_state: &mut super::sqlite_snapshot::SqliteSnapshotState,
    failure: &mut Option<SqliteFailureInjection>,
) -> RetentionResult<AdvanceRetentionFloorReceipt> {
    if request.expected_floor.version != CURSOR_VERSION
        || request.new_floor.version != CURSOR_VERSION
        || request.expected_floor.stream != request.stream
        || request.new_floor.stream != request.stream
    {
        return Err(RetentionError::InvalidInput(
            "retention cursors must identify the requested stream".into(),
        ));
    }
    if let Some(prior) = read_receipt(conn, &request.operation_id)? {
        return match prior {
            StoredReceipt::Floor(receipt) if receipt.request == request => Ok(receipt),
            _ => Err(RetentionError::OperationConflict {
                operation_id: request.operation_id,
            }),
        };
    }
    let tx = conn
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .map_err(|e| sqlite_failure("begin retention floor advance", e))?;
    let charge = reserve_receipt(&tx, &request.operation_id, &request.stream, options)?;
    let (key, floor, tail) = stream_row(&tx, &request.stream).map_err(map_store)?;
    let (oldest, current) = policy(&tx, key)?;
    if request.expected_floor.offset != floor {
        return Err(RetentionError::StaleFloor {
            current: Box::new(Cursor::new(request.stream.clone(), floor)),
        });
    }
    if request.new_floor.offset < floor || request.new_floor.offset > tail {
        return Err(RetentionError::InvalidInput(
            "retention floor must be monotonic and no greater than tail".into(),
        ));
    }
    #[cfg(feature = "replication")]
    if let Some(protected) = super::sqlite_replication::minimum_replica_offset(&tx, &request.stream)
        .map_err(map_store)?
    {
        if request.new_floor.offset > protected {
            return Err(RetentionError::ReplicaProtectionActive {
                maximum_floor: Box::new(Cursor::new(request.stream.clone(), protected)),
            });
        }
    }
    let maximum =
        snapshot_state.maximum_cleanup_offset(&request.stream, options.snapshot_clock.now());
    if request.new_floor.offset > maximum {
        return Err(RetentionError::RecoveryProtectionActive {
            maximum_floor: Box::new(Cursor::new(request.stream.clone(), maximum)),
        });
    }
    if request.new_floor.offset > floor {
        enqueue_cleanup(&tx, key, options)?;
    }
    tx.execute(
        "UPDATE event_streams SET floor=?1 WHERE stream_key=?2",
        params![be(request.new_floor.offset).as_slice(), key],
    )
    .map_err(|e| sqlite_failure("advance retention floor", e))?;
    let status = RetentionStatus {
        bounds: Bounds {
            floor: request.new_floor.clone(),
            tail: Cursor::new(request.stream.clone(), tail),
        },
        retry_policy: RetryPolicyState::Generational {
            oldest_accepted: oldest,
            current,
        },
    };
    insert_receipt(
        &tx,
        &request.operation_id,
        FLOOR,
        &request.stream,
        (Some(floor), Some(request.new_floor.offset)),
        &status,
        charge,
    )?;
    let unknown = request.clone();
    commit_mutation(
        tx,
        AdvanceRetentionFloorReceipt { request, status },
        "commit retention floor advance",
        failure,
        || RetentionError::AdvanceFloorUnknown(Box::new(unknown.clone())),
    )
}

fn cleanup(
    conn: &mut Connection,
    limits: RetentionCleanupLimits,
    options: &SqliteOptions,
) -> RetentionResult<RetentionCleanupProgress> {
    if limits.max_event_rows == 0 || limits.max_retry_rows == 0 || limits.max_bytes == 0 {
        return Err(RetentionError::InvalidInput(
            "cleanup limits must be nonzero".into(),
        ));
    }
    let maximum = options
        .max_record_bytes
        .saturating_add(STORED_RECORD_OVERHEAD);
    if maximum > limits.max_bytes {
        return Err(RetentionError::InvalidInput(
            "cleanup byte limit must fit one maximum-size stored record".into(),
        ));
    }
    let tx = conn
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .map_err(|e| sqlite_failure("begin retention cleanup", e))?;
    let Some(key) = tx
        .query_row(
            "SELECT stream_key FROM retention_cleanup ORDER BY stream_key LIMIT 1",
            [],
            |r| r.get::<_, i64>(0),
        )
        .optional()
        .map_err(|e| sqlite_failure("select retention cleanup", e))?
    else {
        return commit(
            tx,
            RetentionCleanupProgress {
                removed_event_rows: 0,
                removed_retry_rows: 0,
                removed_bytes: 0,
                remaining: false,
            },
            "finish empty retention cleanup",
        );
    };
    let policy = tx
        .query_row(
            "SELECT oldest_generation FROM retention_streams WHERE stream_key=?1",
            [key],
            |r| r.get::<_, Vec<u8>>(0),
        )
        .optional()
        .map_err(|e| sqlite_failure("read cleanup policy", e))?;
    let oldest = policy
        .as_deref()
        .map(decode_offset)
        .transpose()
        .map_err(map_store)?
        .unwrap_or(0);
    let floor = tx
        .query_row(
            "SELECT floor FROM event_streams WHERE stream_key=?1",
            [key],
            |r| r.get::<_, Vec<u8>>(0),
        )
        .optional()
        .map_err(|e| sqlite_failure("read cleanup floor", e))?
        .as_deref()
        .map(decode_offset)
        .transpose()
        .map_err(map_store)?
        .unwrap_or(0);
    let mut removed_retry_rows = 0usize;
    let mut removed_event_rows = 0usize;
    let mut removed_bytes = 0usize;
    let mut released_retry_bytes = 0u64;
    let retry_candidates = {
        let mut stmt=tx.prepare("SELECT generation,event_id,charge FROM retention_retry_identities WHERE stream_key=?1 AND generation<?2 ORDER BY generation,event_id LIMIT ?3").map_err(|e|sqlite_failure("prepare retry cleanup",e))?;
        let rows = stmt
            .query_map(
                params![
                    key,
                    be(oldest).as_slice(),
                    i64::try_from(limits.max_retry_rows).unwrap_or(i64::MAX)
                ],
                |r| {
                    Ok((
                        r.get::<_, Vec<u8>>(0)?,
                        r.get::<_, String>(1)?,
                        r.get::<_, i64>(2)?,
                    ))
                },
            )
            .map_err(|e| sqlite_failure("read retry cleanup", e))?
            .collect::<std::result::Result<Vec<_>, _>>()
            .map_err(|e| sqlite_failure("decode retry cleanup", e))?;
        rows
    };
    for (generation, event_id, charge) in retry_candidates {
        let charge = usize::try_from(charge)
            .map_err(|_| RetentionError::CorruptStorage("retry record charge is invalid".into()))?;
        if removed_bytes
            .checked_add(charge)
            .is_none_or(|v| v > limits.max_bytes)
        {
            break;
        }
        tx.execute("DELETE FROM retention_retry_identities WHERE stream_key=?1 AND generation=?2 AND event_id=?3",params![key,generation,event_id]).map_err(|e|sqlite_failure("delete expired retry identity",e))?;
        removed_retry_rows += 1;
        removed_bytes += charge;
        released_retry_bytes += charge as u64;
    }
    let event_candidates = {
        let mut stmt=tx.prepare("SELECT source,offset,charge FROM (SELECT 0 source,e.offset,octet_length(e.event_id)+octet_length(e.schema_id)+octet_length(e.payload)+384 charge FROM event_records e WHERE e.stream_key=?1 AND e.offset<=?2 AND NOT EXISTS(SELECT 1 FROM retention_retry_identities i WHERE i.stream_key=e.stream_key AND i.offset=e.offset) UNION ALL SELECT 1 source,e.offset,octet_length(e.event_id)+octet_length(e.schema_id)+octet_length(e.payload)+384 charge FROM retention_generated_records e WHERE e.stream_key=?1 AND e.offset<=?2 AND NOT EXISTS(SELECT 1 FROM retention_retry_identities i WHERE i.stream_key=e.stream_key AND i.offset=e.offset)) ORDER BY offset LIMIT ?3").map_err(|e|sqlite_failure("prepare event cleanup",e))?;
        let rows = stmt
            .query_map(
                params![
                    key,
                    be(floor).as_slice(),
                    i64::try_from(limits.max_event_rows).unwrap_or(i64::MAX)
                ],
                |r| {
                    Ok((
                        r.get::<_, i64>(0)?,
                        r.get::<_, Vec<u8>>(1)?,
                        r.get::<_, i64>(2)?,
                    ))
                },
            )
            .map_err(|e| sqlite_failure("read event cleanup", e))?
            .collect::<std::result::Result<Vec<_>, _>>()
            .map_err(|e| sqlite_failure("decode event cleanup", e))?;
        rows
    };
    for (source, offset, charge) in event_candidates {
        let charge = usize::try_from(charge).map_err(|_| {
            RetentionError::CorruptStorage("event cleanup charge is invalid".into())
        })?;
        if removed_bytes
            .checked_add(charge)
            .is_none_or(|v| v > limits.max_bytes)
        {
            break;
        }
        let table = if source == 0 {
            "event_records"
        } else {
            "retention_generated_records"
        };
        tx.execute(
            &format!("DELETE FROM {table} WHERE stream_key=?1 AND offset=?2"),
            params![key, offset],
        )
        .map_err(|e| sqlite_failure("delete retained event", e))?;
        removed_event_rows += 1;
        removed_bytes += charge;
    }
    if removed_retry_rows > 0 {
        let current: Vec<u8> = tx
            .query_row(
                "SELECT retry_bytes FROM retention_metadata WHERE singleton=1",
                [],
                |r| r.get(0),
            )
            .map_err(|e| sqlite_failure("read retry bytes", e))?;
        let current = decode_offset(&current).map_err(map_store)?;
        let next = current
            .checked_sub(released_retry_bytes)
            .ok_or_else(|| RetentionError::CorruptStorage("retry byte counter underflow".into()))?;
        tx.execute("UPDATE retention_metadata SET retry_rows=retry_rows-?1,retry_bytes=?2 WHERE singleton=1",params![i64::try_from(removed_retry_rows).unwrap(),be(next).as_slice()]).map_err(|e|sqlite_failure("release retry counters",e))?;
    }
    let more_retry:bool=tx.query_row("SELECT EXISTS(SELECT 1 FROM retention_retry_identities WHERE stream_key=?1 AND generation<?2)",params![key,be(oldest).as_slice()],|r|r.get(0)).map_err(|e|sqlite_failure("check retry cleanup",e))?;
    let more_events:bool=tx.query_row("SELECT EXISTS(SELECT 1 FROM (SELECT offset FROM event_records WHERE stream_key=?1 AND offset<=?2 UNION ALL SELECT offset FROM retention_generated_records WHERE stream_key=?1 AND offset<=?2) e WHERE NOT EXISTS(SELECT 1 FROM retention_retry_identities i WHERE i.stream_key=?1 AND i.offset=e.offset))",params![key,be(floor).as_slice()],|r|r.get(0)).map_err(|e|sqlite_failure("check event cleanup",e))?;
    if !more_retry && !more_events {
        tx.execute("DELETE FROM retention_cleanup WHERE stream_key=?1", [key])
            .map_err(|e| sqlite_failure("finish retention cleanup", e))?;
        tx.execute(
            "UPDATE retention_metadata SET pending_count=pending_count-1 WHERE singleton=1",
            [],
        )
        .map_err(|e| sqlite_failure("release cleanup counter", e))?;
    }
    let remaining: bool = tx
        .query_row("SELECT EXISTS(SELECT 1 FROM retention_cleanup)", [], |r| {
            r.get(0)
        })
        .map_err(|e| sqlite_failure("check pending retention cleanup", e))?;
    commit(
        tx,
        RetentionCleanupProgress {
            removed_event_rows,
            removed_retry_rows,
            removed_bytes,
            remaining,
        },
        "commit retention cleanup",
    )
}

fn insert_receipt(
    tx: &Transaction<'_>,
    operation: &RetentionOperationId,
    kind: i64,
    stream: &StreamKey,
    arguments: (Option<u64>, Option<u64>),
    result: &RetentionStatus,
    charge: usize,
) -> RetentionResult<()> {
    let (oldest, current) = encode_policy(&result.retry_policy);
    tx.execute(
        "INSERT INTO retention_receipts(operation_id,kind,public_id,incarnation,argument_one,argument_two,result_floor,result_tail,result_oldest,result_current,charge)
         VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11)",
        params![
            operation.as_str(), kind, stream.id.as_str(), stream.incarnation.0.as_slice(),
            arguments.0.map(be), arguments.1.map(be),
            be(result.bounds.floor.offset).as_slice(), be(result.bounds.tail.offset).as_slice(),
            be(oldest).as_slice(), be(current).as_slice(), i64::try_from(charge).map_err(|_| RetentionError::CapacityExceeded)?
        ],
    )
    .map_err(|error| sqlite_failure("write retention receipt", error))?;
    tx.execute(
        "UPDATE retention_metadata SET receipt_count=receipt_count+1,receipt_bytes=receipt_bytes+?1 WHERE singleton=1",
        [i64::try_from(charge).map_err(|_| RetentionError::CapacityExceeded)?],
    )
    .map_err(|error| sqlite_failure("update retention receipt counters", error))?;
    Ok(())
}

fn commit<T>(tx: Transaction<'_>, value: T, action: &str) -> RetentionResult<T> {
    tx.commit().map_err(|error| sqlite_failure(action, error))?;
    Ok(value)
}

fn consume_retention_failure(
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

fn commit_mutation<T>(
    tx: Transaction<'_>,
    value: T,
    _action: &str,
    failure: &mut Option<SqliteFailureInjection>,
    unknown: impl Fn() -> RetentionError,
) -> RetentionResult<T> {
    if consume_retention_failure(failure, SqliteFailureInjection::BeforeRetentionCommit) {
        return Err(RetentionError::StorageFailure(
            "injected failure before retention commit".into(),
        ));
    }
    tx.commit().map_err(|error| {
        if error.sqlite_error_code() == Some(rusqlite::ErrorCode::DiskFull) {
            RetentionError::CapacityExceeded
        } else {
            unknown()
        }
    })?;
    if consume_retention_failure(
        failure,
        SqliteFailureInjection::AfterRetentionCommitAcknowledgementLost,
    ) {
        return Err(unknown());
    }
    Ok(value)
}

fn enable(
    conn: &mut Connection,
    request: EnableRetryPolicy,
    options: &SqliteOptions,
    failure: &mut Option<SqliteFailureInjection>,
) -> RetentionResult<EnableRetryPolicyReceipt> {
    if let Some(prior) = read_receipt(conn, &request.operation_id)? {
        return match prior {
            StoredReceipt::Enable(receipt) if receipt.request == request => Ok(receipt),
            _ => Err(RetentionError::OperationConflict {
                operation_id: request.operation_id,
            }),
        };
    }
    let tx = conn
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .map_err(|error| sqlite_failure("begin retry-policy enable", error))?;
    let charge = reserve_receipt(&tx, &request.operation_id, &request.stream, options)?;
    let (key, floor, tail) = stream_row(&tx, &request.stream).map_err(map_store)?;
    if tx
        .query_row(
            "SELECT 1 FROM retention_streams WHERE stream_key=?1",
            [key],
            |_| Ok(()),
        )
        .optional()
        .map_err(|error| sqlite_failure("read retry policy", error))?
        .is_some()
    {
        return Err(RetentionError::InvalidInput(
            "retry policy is already generational".into(),
        ));
    }
    let (rows, bytes): (i64, i64) = tx
        .query_row(
            "SELECT count(*),coalesce(sum(octet_length(event_id)+octet_length(schema_id)+octet_length(payload)+384),0)
             FROM event_records WHERE stream_key=?1",
            [key],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .map_err(|error| sqlite_failure("measure legacy retry records", error))?;
    let (stored_rows, stored_bytes_blob): (i64, Vec<u8>) = tx
        .query_row(
            "SELECT retry_rows,retry_bytes FROM retention_metadata WHERE singleton=1",
            [],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .map_err(|error| sqlite_failure("read retry counters", error))?;
    let stored_bytes = decode_offset(&stored_bytes_blob).map_err(map_store)?;
    if usize::try_from(stored_rows)
        .ok()
        .and_then(|v| v.checked_add(rows as usize))
        .is_none_or(|v| v > options.retention.receipts.max_rows)
        || stored_bytes
            .checked_add(bytes as u64)
            .is_none_or(|v| v > options.retention.receipts.max_bytes)
    {
        return Err(RetentionError::CapacityExceeded);
    }
    tx.execute(
        "INSERT INTO retention_streams VALUES(?1,?2,?3)",
        params![key, be(0).as_slice(), be(1).as_slice()],
    )
    .map_err(|error| sqlite_failure("enable retry policy", error))?;
    tx.execute(
        "INSERT INTO retention_retry_identities(stream_key,generation,event_id,offset,charge)
         SELECT stream_key,zeroblob(8),event_id,offset,
                octet_length(event_id)+octet_length(schema_id)+octet_length(payload)+384
         FROM event_records WHERE stream_key=?1",
        [key],
    )
    .map_err(|error| sqlite_failure("copy legacy retry records", error))?;
    tx.execute(
        "UPDATE retention_metadata SET retry_rows=retry_rows+?1,retry_bytes=?2 WHERE singleton=1",
        params![rows, be(stored_bytes + bytes as u64).as_slice()],
    )
    .map_err(|error| sqlite_failure("update retry counters", error))?;
    let status = RetentionStatus {
        bounds: Bounds {
            floor: Cursor::new(request.stream.clone(), floor),
            tail: Cursor::new(request.stream.clone(), tail),
        },
        retry_policy: RetryPolicyState::Generational {
            oldest_accepted: RetryGeneration::LEGACY,
            current: RetryGeneration::FIRST,
        },
    };
    insert_receipt(
        &tx,
        &request.operation_id,
        ENABLE,
        &request.stream,
        (None, None),
        &status,
        charge,
    )?;
    let unknown = request.clone();
    commit_mutation(
        tx,
        EnableRetryPolicyReceipt { request, status },
        "commit retry-policy enable",
        failure,
        || RetentionError::EnableUnknown(Box::new(unknown.clone())),
    )
}

pub(super) fn handle_retention_command(
    conn: &mut Connection,
    command: RetentionCommand,
    options: &SqliteOptions,
    snapshot_state: &mut super::sqlite_snapshot::SqliteSnapshotState,
    failure: &mut Option<SqliteFailureInjection>,
) {
    match command {
        RetentionCommand::Status(stream, tx) => {
            let _ = tx.send(status(conn, &stream));
        }
        RetentionCommand::Enable(request, tx) => {
            let _ = tx.send(enable(conn, request, options, failure));
        }
        RetentionCommand::Advance(request, tx) => {
            let _ = tx.send(advance(conn, request, options, failure));
        }
        RetentionCommand::Expire(request, tx) => {
            let _ = tx.send(expire(conn, request, options, failure));
        }
        RetentionCommand::Append(stream, event, tx) => {
            let _ = tx.send(append_generated(conn, stream, event, options, failure));
        }
        RetentionCommand::Lookup(stream, generation, event_id, tx) => {
            let _ = tx.send(lookup_generated(
                conn, &stream, generation, &event_id, options,
            ));
        }
        RetentionCommand::Floor(request, tx) => {
            let _ = tx.send(advance_floor(
                conn,
                request,
                options,
                snapshot_state,
                failure,
            ));
        }
        RetentionCommand::Cleanup(limits, tx) => {
            let _ = tx.send(cleanup(conn, limits, options));
        }
    }
}

pub(super) fn fail_retention_command(command: RetentionCommand) {
    let error = || RetentionError::StorageFailure("SQLite worker faulted".into());
    match command {
        RetentionCommand::Status(_, tx) => {
            let _ = tx.send(Err(error()));
        }
        RetentionCommand::Enable(request, tx) => {
            let _ = tx.send(Err(RetentionError::EnableUnknown(Box::new(request))));
        }
        RetentionCommand::Advance(request, tx) => {
            let _ = tx.send(Err(RetentionError::AdvanceGenerationUnknown(Box::new(
                request,
            ))));
        }
        RetentionCommand::Expire(request, tx) => {
            let _ = tx.send(Err(RetentionError::ExpireGenerationsUnknown(Box::new(
                request,
            ))));
        }
        RetentionCommand::Append(stream, event, tx) => {
            let _ = tx.send(Err(RetentionError::GeneratedAppendUnknown(Box::new(
                GeneratedEventIdentity {
                    stream,
                    generation: event.generation,
                    event_id: event.event.id,
                },
            ))));
        }
        RetentionCommand::Lookup(_, _, _, tx) => {
            let _ = tx.send(Err(error()));
        }
        RetentionCommand::Floor(request, tx) => {
            let _ = tx.send(Err(RetentionError::AdvanceFloorUnknown(Box::new(request))));
        }
        RetentionCommand::Cleanup(_, tx) => {
            let _ = tx.send(Err(error()));
        }
    }
}

fn send_error(error: crate::application::Error) -> RetentionError {
    match error {
        crate::application::Error::Overloaded => RetentionError::Overloaded,
        crate::application::Error::Closed => RetentionError::Closed,
        other => RetentionError::StorageFailure(other.to_string()),
    }
}

#[async_trait]
impl RetentionStore for SqliteStore {
    async fn retention_status(&self, stream: &StreamKey) -> RetentionResult<RetentionStatus> {
        let (tx, rx) = oneshot::channel();
        self.submit(Command::Retention(RetentionCommand::Status(
            stream.clone(),
            tx,
        )))
        .map_err(send_error)?;
        rx.await.unwrap_or(Err(RetentionError::StorageFailure(
            "SQLite worker stopped".into(),
        )))
    }
    async fn enable_retry_policy(
        &self,
        request: EnableRetryPolicy,
    ) -> RetentionResult<EnableRetryPolicyReceipt> {
        let unknown = request.clone();
        let (tx, rx) = oneshot::channel();
        self.submit(Command::Retention(RetentionCommand::Enable(request, tx)))
            .map_err(send_error)?;
        rx.await
            .unwrap_or(Err(RetentionError::EnableUnknown(Box::new(unknown))))
    }
    async fn advance_retry_generation(
        &self,
        request: AdvanceRetryGeneration,
    ) -> RetentionResult<AdvanceRetryGenerationReceipt> {
        let unknown = request.clone();
        let (tx, rx) = oneshot::channel();
        self.submit(Command::Retention(RetentionCommand::Advance(request, tx)))
            .map_err(send_error)?;
        rx.await
            .unwrap_or(Err(RetentionError::AdvanceGenerationUnknown(Box::new(
                unknown,
            ))))
    }
    async fn expire_retry_generations(
        &self,
        request: ExpireRetryGenerations,
    ) -> RetentionResult<ExpireRetryGenerationsReceipt> {
        let unknown = request.clone();
        let (tx, rx) = oneshot::channel();
        self.submit(Command::Retention(RetentionCommand::Expire(request, tx)))
            .map_err(send_error)?;
        rx.await
            .unwrap_or(Err(RetentionError::ExpireGenerationsUnknown(Box::new(
                unknown,
            ))))
    }
    async fn append_generated(
        &self,
        stream: &StreamKey,
        event: GeneratedEvent,
    ) -> RetentionResult<AppendReceipt> {
        if event.event.accounted_bytes() > self.limits.record {
            return Err(RetentionError::InvalidInput(
                "event exceeds record limit".into(),
            ));
        }
        let identity = GeneratedEventIdentity {
            stream: stream.clone(),
            generation: event.generation,
            event_id: event.event.id.clone(),
        };
        let (tx, rx) = oneshot::channel();
        self.submit(Command::Retention(RetentionCommand::Append(
            stream.clone(),
            event,
            tx,
        )))
        .map_err(send_error)?;
        rx.await
            .unwrap_or(Err(RetentionError::GeneratedAppendUnknown(Box::new(
                identity,
            ))))
    }
    async fn lookup_generated(
        &self,
        stream: &StreamKey,
        generation: RetryGeneration,
        event_id: &EventId,
    ) -> RetentionResult<Option<Arc<Record>>> {
        let (tx, rx) = oneshot::channel();
        self.submit(Command::Retention(RetentionCommand::Lookup(
            stream.clone(),
            generation,
            event_id.clone(),
            tx,
        )))
        .map_err(send_error)?;
        rx.await.unwrap_or(Err(RetentionError::StorageFailure(
            "SQLite worker stopped".into(),
        )))
    }
    async fn advance_retention_floor(
        &self,
        request: AdvanceRetentionFloor,
    ) -> RetentionResult<AdvanceRetentionFloorReceipt> {
        let unknown = request.clone();
        let (tx, rx) = oneshot::channel();
        self.submit(Command::Retention(RetentionCommand::Floor(request, tx)))
            .map_err(send_error)?;
        rx.await
            .unwrap_or(Err(RetentionError::AdvanceFloorUnknown(Box::new(unknown))))
    }
    async fn cleanup_retention(
        &self,
        limits: RetentionCleanupLimits,
    ) -> RetentionResult<RetentionCleanupProgress> {
        if limits.max_event_rows == 0 || limits.max_retry_rows == 0 || limits.max_bytes == 0 {
            return Err(RetentionError::InvalidInput(
                "cleanup limits must be nonzero".into(),
            ));
        }
        let (tx, rx) = oneshot::channel();
        self.submit(Command::Retention(RetentionCommand::Cleanup(limits, tx)))
            .map_err(send_error)?;
        rx.await.unwrap_or(Err(RetentionError::StorageFailure(
            "SQLite worker stopped".into(),
        )))
    }
}
