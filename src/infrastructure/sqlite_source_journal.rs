use super::sqlite::{be, decode_offset, stream_row, Command, SqliteOptions, SqliteStore};
use crate::{application::*, domain::*};
use async_trait::async_trait;
use rusqlite::{params, Connection, OptionalExtension, TransactionBehavior};
use sha2::{Digest, Sha256};
use tokio::sync::oneshot;

const SEGMENT_OVERHEAD: usize = 128;
const MARKER_OVERHEAD: usize = 192;
const RECEIPT_OVERHEAD: usize = 160;
type JournalCounters = (i64, i64, Vec<u8>, i64, Vec<u8>, i64, Vec<u8>, i64, i64);
type SourceAppendState = (Vec<u8>, Option<Vec<u8>>, i64, Option<Vec<u8>>);

pub(super) enum JournalCommand {
    Begin(
        BeginSource,
        oneshot::Sender<JournalResult<BeginSourceReceipt>>,
    ),
    Capture(RawSegment, oneshot::Sender<JournalResult<CaptureReceipt>>),
    Floor(
        AdvanceCaptureReceiptFloor,
        oneshot::Sender<JournalResult<AdvanceCaptureReceiptFloorReceipt>>,
    ),
    Append(
        StreamKey,
        JournaledOutput,
        oneshot::Sender<JournalResult<AppendReceipt>>,
    ),
    Checkpoint(
        ParserCheckpoint,
        oneshot::Sender<JournalResult<CheckpointReceipt>>,
    ),
    Status(SourceKey, oneshot::Sender<JournalResult<SourceProgress>>),
    Read(
        SourceKey,
        u64,
        RawPageLimits,
        oneshot::Sender<JournalResult<RawPage>>,
    ),
    Latest(
        SourceKey,
        oneshot::Sender<JournalResult<Option<ParserCheckpoint>>>,
    ),
    Cleanup(
        JournalCleanupLimits,
        oneshot::Sender<JournalResult<JournalCleanupProgress>>,
    ),
    Seal(
        SealSource,
        oneshot::Sender<JournalResult<SealSourceReceipt>>,
    ),
    Finish(
        FinishSource,
        oneshot::Sender<JournalResult<FinishSourceReceipt>>,
    ),
    Finalization(
        SourceKey,
        oneshot::Sender<JournalResult<SourceFinalizationStatus>>,
    ),
}

pub(super) fn initialize_journal_schema(conn: &Connection) -> crate::application::Result<()> {
    let existing: i64 = conn
        .query_row(
            "SELECT count(*) FROM sqlite_schema WHERE type='table' AND name IN
             ('journal_metadata','journal_sources','journal_segments','journal_capture_receipts',
              'journal_markers','journal_operations')",
            [],
            |row| row.get(0),
        )
        .map_err(|e| {
            crate::application::Error::StoreCorrupt(format!("inspect source journal schema: {e}"))
        })?;
    if existing != 0 && existing != 6 {
        return Err(crate::application::Error::StoreCorrupt(
            "source journal schema is only partially present".into(),
        ));
    }
    if existing == 6 {
        let metadata_rows: i64 = conn
            .query_row(
                "SELECT count(*) FROM journal_metadata WHERE singleton=1",
                [],
                |row| row.get(0),
            )
            .map_err(|e| {
                crate::application::Error::StoreCorrupt(format!(
                    "inspect source journal metadata: {e}"
                ))
            })?;
        if metadata_rows != 1 {
            return Err(crate::application::Error::StoreCorrupt(
                "source journal metadata singleton is missing".into(),
            ));
        }
    }
    conn.execute_batch(
        "BEGIN IMMEDIATE;
         CREATE TABLE IF NOT EXISTS journal_metadata(
           singleton INTEGER PRIMARY KEY CHECK(singleton=1),source_count INTEGER NOT NULL CHECK(source_count>=0),
           segment_count INTEGER NOT NULL CHECK(segment_count>=0),captured_bytes BLOB NOT NULL CHECK(length(captured_bytes)=8),
           marker_count INTEGER NOT NULL CHECK(marker_count>=0),marker_bytes BLOB NOT NULL CHECK(length(marker_bytes)=8),
           checkpoint_count INTEGER NOT NULL CHECK(checkpoint_count>=0),checkpoint_bytes BLOB NOT NULL CHECK(length(checkpoint_bytes)=8),
           receipt_count INTEGER NOT NULL CHECK(receipt_count>=0),receipt_bytes INTEGER NOT NULL CHECK(receipt_bytes>=0));
         CREATE TABLE IF NOT EXISTS journal_sources(
           source_id TEXT NOT NULL,source_incarnation BLOB NOT NULL CHECK(length(source_incarnation)=16),
           parser_id TEXT NOT NULL,parser_version INTEGER NOT NULL,output_public_id TEXT NOT NULL,
           output_incarnation BLOB NOT NULL CHECK(length(output_incarnation)=16),captured_end BLOB NOT NULL CHECK(length(captured_end)=8),
           receipt_floor BLOB NOT NULL CHECK(length(receipt_floor)=8),next_item_index BLOB NOT NULL CHECK(length(next_item_index)=8),
           last_source_byte BLOB,checkpoint_offset BLOB,checkpoint_state BLOB,checkpoint_next_index BLOB,
           checkpoint_committed BLOB,checkpoint_charge INTEGER NOT NULL DEFAULT 0 CHECK(checkpoint_charge>=0),
           sealed_end BLOB CHECK(sealed_end IS NULL OR (typeof(sealed_end)='blob' AND length(sealed_end)=8)),
           parser_finished INTEGER NOT NULL DEFAULT 0 CHECK(parser_finished IN(0,1)),
           PRIMARY KEY(source_id,source_incarnation));
         CREATE TABLE IF NOT EXISTS journal_segments(
           source_id TEXT NOT NULL,source_incarnation BLOB NOT NULL,start BLOB NOT NULL CHECK(length(start)=8),
           end BLOB NOT NULL CHECK(length(end)=8),bytes BLOB NOT NULL,
           PRIMARY KEY(source_id,source_incarnation,start));
         CREATE TABLE IF NOT EXISTS journal_capture_receipts(
           source_id TEXT NOT NULL,source_incarnation BLOB NOT NULL,start BLOB NOT NULL CHECK(length(start)=8),
           end BLOB NOT NULL CHECK(length(end)=8),digest BLOB NOT NULL CHECK(length(digest)=32),charge INTEGER NOT NULL CHECK(charge>=0),
           PRIMARY KEY(source_id,source_incarnation,start));
         CREATE TABLE IF NOT EXISTS journal_markers(
           source_id TEXT NOT NULL,source_incarnation BLOB NOT NULL,item_index BLOB NOT NULL CHECK(length(item_index)=8),
           source_byte BLOB NOT NULL CHECK(length(source_byte)=8),generation BLOB NOT NULL CHECK(length(generation)=8),
           event_id TEXT NOT NULL,committed_offset BLOB NOT NULL CHECK(length(committed_offset)=8),charge INTEGER NOT NULL CHECK(charge>=0),
           PRIMARY KEY(source_id,source_incarnation,item_index));
         CREATE TABLE IF NOT EXISTS journal_operations(
           operation_id TEXT PRIMARY KEY,kind INTEGER NOT NULL CHECK(kind IN(0,1)),source_id TEXT NOT NULL,
           source_incarnation BLOB NOT NULL CHECK(length(source_incarnation)=16),argument_one BLOB,argument_two BLOB,
           parser_id TEXT,parser_version INTEGER,output_public_id TEXT,output_incarnation BLOB,
           result_captured_end BLOB NOT NULL CHECK(length(result_captured_end)=8),result_receipt_floor BLOB NOT NULL CHECK(length(result_receipt_floor)=8),
           result_checkpoint_offset BLOB NOT NULL CHECK(length(result_checkpoint_offset)=8),result_next_item_index BLOB NOT NULL CHECK(length(result_next_item_index)=8),
           result_committed BLOB,charge INTEGER NOT NULL CHECK(charge>=0));
         INSERT OR IGNORE INTO journal_metadata VALUES(1,0,0,zeroblob(8),0,zeroblob(8),0,zeroblob(8),0,0);
         COMMIT;",
    )
    .map_err(|e| {
        if e.sqlite_error_code() == Some(rusqlite::ErrorCode::DiskFull) {
            crate::application::Error::CapacityExceeded
        } else {
            crate::application::Error::StoreWriteFailed(format!("initialize source journal schema: {e}"))
        }
    })?;
    let finalization_columns: i64 = conn
        .query_row(
            "SELECT count(*) FROM pragma_table_info('journal_sources')
             WHERE name IN('sealed_end','parser_finished')",
            [],
            |row| row.get(0),
        )
        .map_err(|error| {
            crate::application::Error::StoreCorrupt(format!(
                "inspect source finalization schema: {error}"
            ))
        })?;
    match finalization_columns {
        0 => conn
            .execute_batch(
                "BEGIN IMMEDIATE;
                 ALTER TABLE journal_sources ADD COLUMN sealed_end BLOB
                   CHECK(sealed_end IS NULL OR (typeof(sealed_end)='blob' AND length(sealed_end)=8));
                 ALTER TABLE journal_sources ADD COLUMN parser_finished INTEGER NOT NULL DEFAULT 0
                   CHECK(parser_finished IN(0,1));
                 COMMIT;",
            )
            .map_err(|error| {
                crate::application::Error::StoreWriteFailed(format!(
                    "extend source finalization schema: {error}"
                ))
            })?,
        2 => {}
        _ => {
            return Err(crate::application::Error::StoreCorrupt(
                "source finalization schema is partial".into(),
            ));
        }
    }
    validate_journal_counters(conn)
}

pub(super) fn validate_journal_counters(conn: &Connection) -> crate::application::Result<()> {
    let stored: JournalCounters = conn
        .query_row(
            "SELECT source_count,segment_count,captured_bytes,marker_count,marker_bytes,
                    checkpoint_count,checkpoint_bytes,receipt_count,receipt_bytes
             FROM journal_metadata WHERE singleton=1",
            [],
            |r| {
                Ok((
                    r.get(0)?,
                    r.get(1)?,
                    r.get(2)?,
                    r.get(3)?,
                    r.get(4)?,
                    r.get(5)?,
                    r.get(6)?,
                    r.get(7)?,
                    r.get(8)?,
                ))
            },
        )
        .map_err(|e| {
            crate::application::Error::StoreCorrupt(format!("read source journal counters: {e}"))
        })?;
    if [stored.0, stored.1, stored.3, stored.5, stored.7, stored.8]
        .iter()
        .any(|v| *v < 0)
    {
        return Err(crate::application::Error::StoreCorrupt(
            "source journal counter is negative".into(),
        ));
    }
    let captured = decode_offset(&stored.2)?;
    let marker_bytes = decode_offset(&stored.4)?;
    let checkpoint_bytes = decode_offset(&stored.6)?;
    let sources: i64 = conn
        .query_row("SELECT count(*) FROM journal_sources", [], |r| r.get(0))
        .map_err(|e| {
            crate::application::Error::StoreCorrupt(format!("audit journal sources: {e}"))
        })?;
    let segments: (i64, i64) = conn
        .query_row(
            "SELECT count(*),coalesce(sum(octet_length(bytes)),0) FROM journal_segments",
            [],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .map_err(|e| {
            crate::application::Error::StoreCorrupt(format!("audit journal segments: {e}"))
        })?;
    let markers: (i64, i64) = conn
        .query_row(
            "SELECT count(*),coalesce(sum(charge),0) FROM journal_markers",
            [],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .map_err(|e| {
            crate::application::Error::StoreCorrupt(format!("audit journal markers: {e}"))
        })?;
    let checkpoints: (i64, i64) = conn.query_row("SELECT count(*),coalesce(sum(checkpoint_charge),0) FROM journal_sources WHERE checkpoint_offset IS NOT NULL", [], |r| Ok((r.get(0)?,r.get(1)?))).map_err(|e| crate::application::Error::StoreCorrupt(format!("audit journal checkpoints: {e}")))?;
    let receipts: (i64, i64) = conn.query_row("SELECT (SELECT count(*) FROM journal_capture_receipts)+(SELECT count(*) FROM journal_operations),(SELECT coalesce(sum(charge),0) FROM journal_capture_receipts)+(SELECT coalesce(sum(charge),0) FROM journal_operations)", [], |r| Ok((r.get(0)?,r.get(1)?))).map_err(|e| crate::application::Error::StoreCorrupt(format!("audit journal receipts: {e}")))?;
    let invalid_charge: bool = conn.query_row(
        "SELECT EXISTS(
           SELECT 1 FROM journal_capture_receipts
            WHERE charge<>2*octet_length(source_id)+160
           UNION ALL
           SELECT 1 FROM journal_markers m JOIN journal_sources s USING(source_id,source_incarnation)
            WHERE m.charge<>octet_length(m.source_id)+octet_length(m.event_id)+octet_length(s.output_public_id)+192
           UNION ALL
           SELECT 1 FROM journal_operations
            WHERE charge<>CASE kind
              WHEN 0 THEN octet_length(operation_id)+4*octet_length(source_id)+octet_length(parser_id)+2*octet_length(output_public_id)+160
              WHEN 1 THEN octet_length(operation_id)+3*octet_length(source_id)+160 END
           UNION ALL
           SELECT 1 FROM journal_sources WHERE
             (checkpoint_offset IS NULL AND checkpoint_charge<>0) OR
             (checkpoint_offset IS NOT NULL AND checkpoint_charge<>
               octet_length(checkpoint_state)+256+octet_length(source_id)+octet_length(parser_id)+octet_length(output_public_id))
         )",
        [],
        |row| row.get(0),
    ).map_err(|e| crate::application::Error::StoreCorrupt(format!("audit journal row charges: {e}")))?;
    let finalization_columns: i64 = conn
        .query_row(
            "SELECT count(*) FROM pragma_table_info('journal_sources')
             WHERE name IN('sealed_end','parser_finished')",
            [],
            |row| row.get(0),
        )
        .map_err(|error| {
            crate::application::Error::StoreCorrupt(format!(
                "inspect source finalization columns: {error}"
            ))
        })?;
    if !matches!(finalization_columns, 0 | 2) {
        return Err(crate::application::Error::StoreCorrupt(
            "source finalization schema is partial".into(),
        ));
    }
    let invalid_finalization: bool = if finalization_columns == 2 {
        conn.query_row(
            "SELECT EXISTS(
               SELECT 1 FROM journal_sources
                WHERE typeof(parser_finished)<>'integer' OR parser_finished NOT IN(0,1)
                   OR (sealed_end IS NOT NULL AND
                       (typeof(sealed_end)<>'blob' OR length(sealed_end)<>8 OR sealed_end<>captured_end))
                   OR (parser_finished=1 AND
                       (sealed_end IS NULL OR checkpoint_offset IS NULL OR checkpoint_offset<>sealed_end))
             )",
            [],
            |row| row.get(0),
        )
        .map_err(|error| {
            crate::application::Error::StoreCorrupt(format!(
                "audit source finalization state: {error}"
            ))
        })?
    } else {
        false
    };
    if stored.0 != sources
        || (stored.1, captured) != (segments.0, segments.1 as u64)
        || (stored.3, marker_bytes) != (markers.0, markers.1 as u64)
        || (stored.5, checkpoint_bytes) != (checkpoints.0, checkpoints.1 as u64)
        || (stored.7, stored.8) != receipts
        || invalid_charge
        || invalid_finalization
    {
        return Err(crate::application::Error::StoreCorrupt(
            "source journal counters disagree with stored rows".into(),
        ));
    }
    Ok(())
}

fn decode_journal(value: &[u8], label: &str) -> JournalResult<u64> {
    decode_offset(value)
        .map_err(|error| JournalError::CorruptStorage(format!("invalid {label}: {error}")))
}

fn failure(action: &str, error: rusqlite::Error) -> JournalError {
    if error.sqlite_error_code() == Some(rusqlite::ErrorCode::DiskFull) {
        JournalError::CapacityExceeded
    } else {
        JournalError::StorageFailure(format!("{action}: {error}"))
    }
}

fn source_key(id: String, incarnation: Vec<u8>) -> JournalResult<SourceKey> {
    Ok(SourceKey {
        id: SourceId::new(id)
            .map_err(|_| JournalError::CorruptStorage("invalid source ID".into()))?,
        incarnation: SourceIncarnation(
            incarnation
                .try_into()
                .map_err(|_| JournalError::CorruptStorage("invalid source incarnation".into()))?,
        ),
    })
}

fn progress(conn: &Connection, source: &SourceKey) -> JournalResult<SourceProgress> {
    type Row = (
        String,
        i64,
        String,
        Vec<u8>,
        Vec<u8>,
        Vec<u8>,
        Vec<u8>,
        Option<Vec<u8>>,
    );
    let row: Option<Row> = conn.query_row(
        "SELECT parser_id,parser_version,output_public_id,output_incarnation,captured_end,receipt_floor,
                coalesce(checkpoint_offset,zeroblob(8)),checkpoint_committed
         FROM journal_sources WHERE source_id=?1 AND source_incarnation=?2",
        params![source.id.as_str(),source.incarnation.0.as_slice()],
        |r| Ok((r.get(0)?,r.get(1)?,r.get(2)?,r.get(3)?,r.get(4)?,r.get(5)?,r.get(6)?,r.get(7)?)),
    ).optional().map_err(|e|JournalError::CorruptStorage(format!("read source progress: {e}")))?;
    let Some((
        parser_id,
        parser_version,
        output_id,
        output_inc,
        captured,
        floor,
        checkpoint,
        committed,
    )) = row
    else {
        let current=conn.query_row("SELECT source_incarnation FROM journal_sources WHERE source_id=?1 ORDER BY rowid DESC LIMIT 1",[source.id.as_str()],|r|r.get::<_,Vec<u8>>(0)).optional().map_err(|e|JournalError::CorruptStorage(format!("read current source: {e}")))?.map(|v|source_key(source.id.as_str().to_owned(),v)).transpose()?.map(Box::new);
        return Err(JournalError::StaleSource { current });
    };
    let next: Vec<u8>=conn.query_row("SELECT coalesce(checkpoint_next_index,zeroblob(8)) FROM journal_sources WHERE source_id=?1 AND source_incarnation=?2",params![source.id.as_str(),source.incarnation.0.as_slice()],|r|r.get(0)).map_err(|e|JournalError::CorruptStorage(format!("read checkpoint item index: {e}")))?;
    let output = StreamKey {
        id: StreamId::new(output_id)
            .map_err(|_| JournalError::CorruptStorage("invalid output stream ID".into()))?,
        incarnation: IncarnationId(
            output_inc
                .try_into()
                .map_err(|_| JournalError::CorruptStorage("invalid output incarnation".into()))?,
        ),
    };
    let committed = committed
        .map(|v| decode_offset(&v).map(|offset| Cursor::new(output.clone(), offset)))
        .transpose()
        .map_err(|e| JournalError::CorruptStorage(e.to_string()))?;
    Ok(SourceProgress {
        binding: SourceBinding {
            source: source.clone(),
            parser: ParserRef {
                id: ParserId::new(parser_id)
                    .map_err(|_| JournalError::CorruptStorage("invalid parser ID".into()))?,
                version: u32::try_from(parser_version)
                    .map_err(|_| JournalError::CorruptStorage("invalid parser version".into()))?,
            },
            output_stream: output,
        },
        captured_end: decode_offset(&captured)
            .map_err(|e| JournalError::CorruptStorage(e.to_string()))?,
        capture_receipt_floor: decode_offset(&floor)
            .map_err(|e| JournalError::CorruptStorage(e.to_string()))?,
        checkpoint_offset: decode_offset(&checkpoint)
            .map_err(|e| JournalError::CorruptStorage(e.to_string()))?,
        next_item_index: decode_offset(&next)
            .map_err(|e| JournalError::CorruptStorage(e.to_string()))?,
        committed_output: committed,
    })
}

fn operation_progress(
    binding: SourceBinding,
    captured: &[u8],
    floor: &[u8],
    checkpoint: &[u8],
    next: &[u8],
    committed: Option<&[u8]>,
) -> JournalResult<SourceProgress> {
    let decode = |value: &[u8]| {
        decode_offset(value).map_err(|error| JournalError::CorruptStorage(error.to_string()))
    };
    let committed_output = committed
        .map(|value| decode(value).map(|offset| Cursor::new(binding.output_stream.clone(), offset)))
        .transpose()?;
    Ok(SourceProgress {
        binding,
        captured_end: decode(captured)?,
        capture_receipt_floor: decode(floor)?,
        checkpoint_offset: decode(checkpoint)?,
        next_item_index: decode(next)?,
        committed_output,
    })
}

fn begin(
    conn: &mut Connection,
    request: BeginSource,
    options: &SqliteOptions,
) -> JournalResult<BeginSourceReceipt> {
    if let Some((kind, source_id, source_incarnation, parser_id, parser_version, output_id, output_incarnation, captured, floor, checkpoint, next, committed)) = conn
        .query_row(
            "SELECT kind,
                    CASE WHEN typeof(source_id)='text' AND octet_length(source_id) BETWEEN 1 AND 256 THEN source_id END,
                    CASE WHEN typeof(source_incarnation)='blob' AND length(source_incarnation)=16 THEN source_incarnation END,
                    CASE WHEN typeof(parser_id)='text' AND octet_length(parser_id) BETWEEN 1 AND 256 THEN parser_id END,
                    CASE WHEN typeof(parser_version)='integer' THEN parser_version END,
                    CASE WHEN typeof(output_public_id)='text' AND octet_length(output_public_id) BETWEEN 1 AND 256 THEN output_public_id END,
                    CASE WHEN typeof(output_incarnation)='blob' AND length(output_incarnation)=16 THEN output_incarnation END,
                    CASE WHEN typeof(result_captured_end)='blob' AND length(result_captured_end)=8 THEN result_captured_end END,
                    CASE WHEN typeof(result_receipt_floor)='blob' AND length(result_receipt_floor)=8 THEN result_receipt_floor END,
                    CASE WHEN typeof(result_checkpoint_offset)='blob' AND length(result_checkpoint_offset)=8 THEN result_checkpoint_offset END,
                    CASE WHEN typeof(result_next_item_index)='blob' AND length(result_next_item_index)=8 THEN result_next_item_index END,
                    CASE WHEN result_committed IS NULL OR (typeof(result_committed)='blob' AND length(result_committed)=8) THEN result_committed END
             FROM journal_operations WHERE operation_id=?1",
            [request.operation_id.as_str()],
            |r| Ok((r.get::<_, i64>(0)?,r.get::<_, Option<String>>(1)?,r.get::<_, Option<Vec<u8>>>(2)?,r.get::<_, Option<String>>(3)?,r.get::<_, Option<i64>>(4)?,r.get::<_, Option<String>>(5)?,r.get::<_, Option<Vec<u8>>>(6)?,r.get::<_, Option<Vec<u8>>>(7)?,r.get::<_, Option<Vec<u8>>>(8)?,r.get::<_, Option<Vec<u8>>>(9)?,r.get::<_, Option<Vec<u8>>>(10)?,r.get::<_, Option<Vec<u8>>>(11)?)),
        )
        .optional()
        .map_err(|e| failure("read begin receipt", e))?
    {
        let exact = kind == 0
            && source_id.as_deref() == Some(request.binding.source.id.as_str())
            && source_incarnation.as_deref() == Some(request.binding.source.incarnation.0.as_slice())
            && parser_id.as_deref() == Some(request.binding.parser.id.as_str())
            && parser_version == Some(i64::from(request.binding.parser.version))
            && output_id.as_deref() == Some(request.binding.output_stream.id.as_str())
            && output_incarnation.as_deref() == Some(request.binding.output_stream.incarnation.0.as_slice());
        if !exact {
            return Err(JournalError::OperationConflict {
                operation_id: request.operation_id,
            });
        }
        let required = |value: Option<Vec<u8>>, label: &str| {
            value.ok_or_else(|| JournalError::CorruptStorage(format!("invalid {label} in begin receipt")))
        };
        return Ok(BeginSourceReceipt {
            progress: operation_progress(
                request.binding.clone(),
                &required(captured, "captured end")?,
                &required(floor, "receipt floor")?,
                &required(checkpoint, "checkpoint offset")?,
                &required(next, "next item index")?,
                committed.as_deref(),
            )?,
            request,
        });
    }
    if conn.query_row("SELECT EXISTS(SELECT 1 FROM journal_sources WHERE source_id=?1 AND source_incarnation=?2)",params![request.binding.source.id.as_str(),request.binding.source.incarnation.0.as_slice()],|r|r.get::<_,bool>(0)).map_err(|e|failure("inspect source binding",e))? {return Err(JournalError::SourceBindingConflict{source:request.binding.source});}
    stream_row(conn, &request.binding.output_stream)
        .map_err(|error| JournalError::InvalidInput(format!("invalid output stream: {error}")))?;
    let charge = request
        .operation_id
        .as_str()
        .len()
        .checked_add(request.binding.source.id.as_str().len() * 4)
        .and_then(|v| v.checked_add(request.binding.parser.id.as_str().len()))
        .and_then(|v| {
            v.checked_add(request.binding.output_stream.id.as_str().len() * 2 + RECEIPT_OVERHEAD)
        })
        .ok_or(JournalError::CapacityExceeded)?;
    let (sources,receipts,bytes):(i64,i64,i64)=conn.query_row("SELECT source_count,receipt_count,receipt_bytes FROM journal_metadata WHERE singleton=1",[],|r|Ok((r.get(0)?,r.get(1)?,r.get(2)?))).map_err(|e|failure("read journal counters",e))?;
    let cfg = &options.source_journal;
    if sources as usize >= cfg.storage.max_sources
        || receipts as usize >= cfg.receipts.max_source_receipts
        || (bytes as usize)
            .checked_add(charge)
            .is_none_or(|v| v > cfg.receipts.max_receipt_bytes)
    {
        return Err(JournalError::CapacityExceeded);
    }
    let tx = conn
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .map_err(|e| failure("begin source", e))?;
    tx.execute("INSERT INTO journal_sources(source_id,source_incarnation,parser_id,parser_version,output_public_id,output_incarnation,captured_end,receipt_floor,next_item_index) VALUES(?1,?2,?3,?4,?5,?6,zeroblob(8),zeroblob(8),zeroblob(8))",params![request.binding.source.id.as_str(),request.binding.source.incarnation.0.as_slice(),request.binding.parser.id.as_str(),i64::from(request.binding.parser.version),request.binding.output_stream.id.as_str(),request.binding.output_stream.incarnation.0.as_slice()]).map_err(|e|failure("store source binding",e))?;
    tx.execute("INSERT INTO journal_operations(operation_id,kind,source_id,source_incarnation,parser_id,parser_version,output_public_id,output_incarnation,result_captured_end,result_receipt_floor,result_checkpoint_offset,result_next_item_index,charge) VALUES(?1,0,?2,?3,?4,?5,?6,?7,zeroblob(8),zeroblob(8),zeroblob(8),zeroblob(8),?8)",params![request.operation_id.as_str(),request.binding.source.id.as_str(),request.binding.source.incarnation.0.as_slice(),request.binding.parser.id.as_str(),i64::from(request.binding.parser.version),request.binding.output_stream.id.as_str(),request.binding.output_stream.incarnation.0.as_slice(),i64::try_from(charge).map_err(|_|JournalError::CapacityExceeded)?]).map_err(|e|failure("store begin receipt",e))?;
    tx.execute("UPDATE journal_metadata SET source_count=source_count+1,receipt_count=receipt_count+1,receipt_bytes=receipt_bytes+?1 WHERE singleton=1",[charge as i64]).map_err(|e|failure("update journal counters",e))?;
    tx.commit()
        .map_err(|e| failure("commit source binding", e))?;
    Ok(BeginSourceReceipt {
        progress: progress(conn, &request.binding.source)?,
        request,
    })
}

fn capture(
    conn: &mut Connection,
    segment: RawSegment,
    options: &SqliteOptions,
    failure_injection: &mut Option<super::sqlite::SqliteFailureInjection>,
) -> JournalResult<CaptureReceipt> {
    if segment.bytes.is_empty()
        || segment.bytes.len() > options.source_journal.storage.max_segment_bytes
    {
        return Err(JournalError::InvalidInput(
            "captured segment size is outside configured bounds".into(),
        ));
    }
    let end = segment
        .start
        .offset
        .checked_add(segment.bytes.len() as u64)
        .ok_or_else(|| JournalError::InvalidInput("captured position overflow".into()))?;
    let digest = CaptureDigest(Sha256::digest(segment.bytes.as_bytes()).into());
    let status = progress(conn, &segment.start.source)?;
    if segment.start.offset < status.capture_receipt_floor {
        return Err(JournalError::CaptureReceiptExpired {
            floor: status.capture_receipt_floor,
        });
    }
    if let Some((saved_end,saved_digest))=conn.query_row("SELECT end,digest FROM journal_capture_receipts WHERE source_id=?1 AND source_incarnation=?2 AND start=?3",params![segment.start.source.id.as_str(),segment.start.source.incarnation.0.as_slice(),be(segment.start.offset).as_slice()],|r|Ok((r.get::<_,Vec<u8>>(0)?,r.get::<_,Vec<u8>>(1)?))).optional().map_err(|e|failure("read capture receipt",e))? {return if decode_offset(&saved_end).ok()==Some(end)&&saved_digest==digest.0 {Ok(CaptureReceipt{start:segment.start,end,digest})}else{Err(JournalError::CaptureConflict{start:segment.start})};}
    let sealed: Option<Vec<u8>> = conn
        .query_row(
            "SELECT sealed_end FROM journal_sources
             WHERE source_id=?1 AND source_incarnation=?2",
            params![
                segment.start.source.id.as_str(),
                segment.start.source.incarnation.0.as_slice()
            ],
            |row| row.get(0),
        )
        .map_err(|error| failure("read source seal before capture", error))?;
    if let Some(sealed) = sealed {
        return Err(JournalError::SourceSealed {
            end: decode_journal(&sealed, "sealed source end")?,
        });
    }
    if segment.start.offset != status.captured_end {
        return Err(JournalError::CaptureConflict {
            start: segment.start,
        });
    }
    let charge = segment.start.source.id.as_str().len() * 2 + RECEIPT_OVERHEAD;
    let (segments,captured,receipts,receipt_bytes):(i64,Vec<u8>,i64,i64)=conn.query_row("SELECT segment_count,captured_bytes,receipt_count,receipt_bytes FROM journal_metadata WHERE singleton=1",[],|r|Ok((r.get(0)?,r.get(1)?,r.get(2)?,r.get(3)?))).map_err(|e|failure("read capture counters",e))?;
    let captured =
        decode_offset(&captured).map_err(|e| JournalError::CorruptStorage(e.to_string()))?;
    let cfg = &options.source_journal;
    if segments as usize >= cfg.storage.max_segments
        || captured
            .checked_add(segment.bytes.len() as u64)
            .is_none_or(|v| v > cfg.storage.max_captured_bytes)
        || receipts as usize >= cfg.receipts.max_source_receipts
        || (receipt_bytes as usize)
            .checked_add(charge)
            .is_none_or(|v| v > cfg.receipts.max_receipt_bytes)
    {
        return Err(JournalError::CapacityExceeded);
    }
    let tx = conn
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .map_err(|e| failure("begin capture", e))?;
    tx.execute(
        "INSERT INTO journal_segments VALUES(?1,?2,?3,?4,?5)",
        params![
            segment.start.source.id.as_str(),
            segment.start.source.incarnation.0.as_slice(),
            be(segment.start.offset).as_slice(),
            be(end).as_slice(),
            segment.bytes.as_bytes()
        ],
    )
    .map_err(|e| failure("store captured bytes", e))?;
    tx.execute(
        "INSERT INTO journal_capture_receipts VALUES(?1,?2,?3,?4,?5,?6)",
        params![
            segment.start.source.id.as_str(),
            segment.start.source.incarnation.0.as_slice(),
            be(segment.start.offset).as_slice(),
            be(end).as_slice(),
            digest.0.as_slice(),
            charge as i64
        ],
    )
    .map_err(|e| failure("store capture receipt", e))?;
    let advanced = tx
        .execute(
            "UPDATE journal_sources SET captured_end=?1
         WHERE source_id=?2 AND source_incarnation=?3
           AND captured_end=?4 AND sealed_end IS NULL",
            params![
                be(end).as_slice(),
                segment.start.source.id.as_str(),
                segment.start.source.incarnation.0.as_slice(),
                be(segment.start.offset).as_slice()
            ],
        )
        .map_err(|e| failure("advance captured end", e))?;
    if advanced != 1 {
        tx.rollback()
            .map_err(|error| failure("roll back stale journal capture", error))?;
        let finalization = finalization(conn, &segment.start.source)?;
        return if let Some(end) = finalization.sealed_end {
            Err(JournalError::SourceSealed { end })
        } else {
            Err(JournalError::CaptureConflict {
                start: segment.start,
            })
        };
    }
    tx.execute("UPDATE journal_metadata SET segment_count=segment_count+1,captured_bytes=?1,receipt_count=receipt_count+1,receipt_bytes=receipt_bytes+?2 WHERE singleton=1",params![be(captured+segment.bytes.len() as u64).as_slice(),charge as i64]).map_err(|e|failure("update capture counters",e))?;
    if matches!(
        failure_injection,
        Some(super::sqlite::SqliteFailureInjection::BeforeJournalCaptureCommit)
    ) {
        *failure_injection = None;
        tx.rollback()
            .map_err(|error| failure("roll back injected journal capture failure", error))?;
        return Err(JournalError::StorageFailure(
            "injected failure before journal capture commit".into(),
        ));
    }
    tx.commit()
        .map_err(|_| JournalError::CaptureUnknown(Box::new(segment.clone())))?;
    if matches!(
        failure_injection,
        Some(super::sqlite::SqliteFailureInjection::AfterJournalCaptureCommitAcknowledgementLost)
    ) {
        *failure_injection = None;
        return Err(JournalError::CaptureUnknown(Box::new(segment)));
    }
    Ok(CaptureReceipt {
        start: segment.start,
        end,
        digest,
    })
}

// Remaining operations are kept in this module so every journal mutation uses the SQLite worker's connection.
// Their focused contract is implemented below without opening a second connection.

pub(super) fn minimum_generation_pin(
    conn: &Connection,
    stream_key: i64,
) -> JournalResult<Option<RetryGeneration>> {
    let value:Option<Vec<u8>>=conn.query_row("SELECT min(m.generation) FROM journal_markers m JOIN journal_sources s USING(source_id,source_incarnation) JOIN event_streams e ON e.public_id=s.output_public_id AND e.incarnation=s.output_incarnation WHERE e.stream_key=?1",[stream_key],|r|r.get(0)).map_err(|e|JournalError::CorruptStorage(format!("read journal generation pin: {e}")))?;
    value
        .map(|v| {
            decode_offset(&v)
                .map(RetryGeneration::new)
                .map_err(|e| JournalError::CorruptStorage(e.to_string()))
        })
        .transpose()
}

fn advance_floor(
    conn: &mut Connection,
    request: AdvanceCaptureReceiptFloor,
    options: &SqliteOptions,
) -> JournalResult<AdvanceCaptureReceiptFloorReceipt> {
    if let Some((kind, one, two, captured, floor, checkpoint, next, committed)) = conn
        .query_row(
            "SELECT kind,argument_one,argument_two,
                    CASE WHEN typeof(result_captured_end)='blob' AND length(result_captured_end)=8 THEN result_captured_end END,
                    CASE WHEN typeof(result_receipt_floor)='blob' AND length(result_receipt_floor)=8 THEN result_receipt_floor END,
                    CASE WHEN typeof(result_checkpoint_offset)='blob' AND length(result_checkpoint_offset)=8 THEN result_checkpoint_offset END,
                    CASE WHEN typeof(result_next_item_index)='blob' AND length(result_next_item_index)=8 THEN result_next_item_index END,
                    CASE WHEN result_committed IS NULL OR (typeof(result_committed)='blob' AND length(result_committed)=8) THEN result_committed END
             FROM journal_operations WHERE operation_id=?1",
            [request.operation_id.as_str()],
            |r| {
                Ok((
                    r.get::<_, i64>(0)?,
                    r.get::<_, Option<Vec<u8>>>(1)?,
                    r.get::<_, Option<Vec<u8>>>(2)?,
                    r.get::<_, Option<Vec<u8>>>(3)?,
                    r.get::<_, Option<Vec<u8>>>(4)?,
                    r.get::<_, Option<Vec<u8>>>(5)?,
                    r.get::<_, Option<Vec<u8>>>(6)?,
                    r.get::<_, Option<Vec<u8>>>(7)?,
                ))
            },
        )
        .optional()
        .map_err(|e| failure("read receipt-floor receipt", e))?
    {
        let exact = kind == 1
            && one.as_deref().and_then(|v| decode_offset(v).ok()) == Some(request.expected_floor)
            && two.as_deref().and_then(|v| decode_offset(v).ok()) == Some(request.new_floor);
        if !exact {
            return Err(JournalError::OperationConflict {
                operation_id: request.operation_id,
            });
        }
        let binding = progress(conn, &request.source)?.binding;
        let required = |value: Option<Vec<u8>>, label: &str| {
            value.ok_or_else(|| {
                JournalError::CorruptStorage(format!("invalid {label} in receipt-floor receipt"))
            })
        };
        return Ok(AdvanceCaptureReceiptFloorReceipt {
            progress: operation_progress(
                binding,
                &required(captured, "captured end")?,
                &required(floor, "receipt floor")?,
                &required(checkpoint, "checkpoint offset")?,
                &required(next, "next item index")?,
                committed.as_deref(),
            )?,
            request,
        });
    }
    let current = progress(conn, &request.source)?;
    if current.capture_receipt_floor != request.expected_floor {
        return Err(JournalError::StaleCaptureReceiptFloor {
            current: current.capture_receipt_floor,
        });
    }
    if request.new_floor < request.expected_floor || request.new_floor > current.captured_end {
        return Err(JournalError::InvalidInput(
            "capture receipt floor is outside captured history".into(),
        ));
    }
    let charge = request.operation_id.as_str().len()
        + request.source.id.as_str().len() * 3
        + RECEIPT_OVERHEAD;
    let (count, bytes): (i64, i64) = conn
        .query_row(
            "SELECT receipt_count,receipt_bytes FROM journal_metadata WHERE singleton=1",
            [],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .map_err(|e| failure("read receipt counters", e))?;
    if count as usize >= options.source_journal.receipts.max_source_receipts
        || (bytes as usize)
            .checked_add(charge)
            .is_none_or(|v| v > options.source_journal.receipts.max_receipt_bytes)
    {
        return Err(JournalError::CapacityExceeded);
    }
    let tx = conn
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .map_err(|e| failure("begin receipt-floor advance", e))?;
    tx.execute(
        "UPDATE journal_sources SET receipt_floor=?1 WHERE source_id=?2 AND source_incarnation=?3",
        params![
            be(request.new_floor).as_slice(),
            request.source.id.as_str(),
            request.source.incarnation.0.as_slice()
        ],
    )
    .map_err(|e| failure("advance receipt floor", e))?;
    tx.execute("INSERT INTO journal_operations(operation_id,kind,source_id,source_incarnation,argument_one,argument_two,result_captured_end,result_receipt_floor,result_checkpoint_offset,result_next_item_index,result_committed,charge) VALUES(?1,1,?2,?3,?4,?5,?6,?5,?7,?8,?9,?10)", params![request.operation_id.as_str(), request.source.id.as_str(), request.source.incarnation.0.as_slice(), be(request.expected_floor).as_slice(), be(request.new_floor).as_slice(), be(current.captured_end).as_slice(), be(current.checkpoint_offset).as_slice(), be(current.next_item_index).as_slice(), current.committed_output.as_ref().map(|cursor| be(cursor.offset)), charge as i64]).map_err(|e| failure("store receipt-floor receipt", e))?;
    tx.execute("UPDATE journal_metadata SET receipt_count=receipt_count+1,receipt_bytes=receipt_bytes+?1 WHERE singleton=1", [charge as i64]).map_err(|e| failure("update receipt counters", e))?;
    tx.commit()
        .map_err(|_| JournalError::AdvanceCaptureReceiptFloorUnknown(Box::new(request.clone())))?;
    Ok(AdvanceCaptureReceiptFloorReceipt {
        progress: progress(conn, &request.source)?,
        request,
    })
}

fn append(
    conn: &mut Connection,
    output_stream: StreamKey,
    output: JournaledOutput,
    options: &SqliteOptions,
    failure_injection: &mut Option<super::sqlite::SqliteFailureInjection>,
) -> JournalResult<AppendReceipt> {
    let status = progress(conn, &output.source)?;
    if status.binding.output_stream != output_stream
        || output.position.source_byte > status.captured_end
    {
        return Err(JournalError::InvalidInput(
            "journal output is outside its immutable binding".into(),
        ));
    }
    let tx = conn
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .map_err(|e| failure("begin captured output", e))?;
    if let Some((source_byte, generation, event_id, committed)) = tx.query_row("SELECT source_byte,generation,event_id,committed_offset FROM journal_markers WHERE source_id=?1 AND source_incarnation=?2 AND item_index=?3", params![output.source.id.as_str(), output.source.incarnation.0.as_slice(), be(output.position.item_index).as_slice()], |r| Ok((r.get::<_, Vec<u8>>(0)?, r.get::<_, Vec<u8>>(1)?, r.get::<_, String>(2)?, r.get::<_, Vec<u8>>(3)?))).optional().map_err(|e| failure("read output marker", e))? {
        let generation = RetryGeneration::new(
            decode_offset(&generation)
                .map_err(|e| JournalError::CorruptStorage(e.to_string()))?,
        );
        if decode_offset(&source_byte).ok() != Some(output.position.source_byte)
            || event_id != output.event.id.as_str()
        { return Err(JournalError::OutputConflict { source: output.source, item_index: output.position.item_index }); }
        let (key, _, _) = stream_row(&tx, &output_stream)
            .map_err(|error| JournalError::CorruptStorage(format!("read marker output stream: {error}")))?;
        let (oldest, current) = super::sqlite_retention::policy(&tx, key)
            .map_err(JournalError::from)?;
        if generation < oldest || generation > current {
            return Err(JournalError::CorruptStorage(
                "journal marker generation is outside the output retry policy".into(),
            ));
        }
        let record = super::sqlite_retention::lookup_retry(
            &tx,
            &output_stream,
            key,
            generation,
            &output.event.id,
            options.max_record_bytes,
        )
        .map_err(JournalError::from)?
        .ok_or_else(|| {
            JournalError::CorruptStorage(
                "journal marker refers to a missing generated output".into(),
            )
        })?;
        if record.event.schema != output.event.schema || record.event.payload != output.event.payload {
            return Err(JournalError::OutputConflict {
                source: output.source,
                item_index: output.position.item_index,
            });
        }
        let receipt = AppendReceipt {
            record,
            kind: AppendKind::Deduplicated,
        };
        if receipt.record.cursor.offset != decode_offset(&committed).map_err(|e| JournalError::CorruptStorage(e.to_string()))? { return Err(JournalError::CorruptStorage("journal marker cursor differs from generated retry".into())); }
        tx.rollback().map_err(|e| failure("finish output retry", e))?;
        return Ok(receipt);
    }
    let (live_next, last, finished, sealed): SourceAppendState = tx.query_row("SELECT next_item_index,last_source_byte,parser_finished,sealed_end FROM journal_sources WHERE source_id=?1 AND source_incarnation=?2", params![output.source.id.as_str(), output.source.incarnation.0.as_slice()], |r| Ok((r.get(0)?,r.get(1)?,r.get(2)?,r.get(3)?))).map_err(|e| failure("read prior marker position", e))?;
    if finished == 1 {
        return Err(JournalError::SourceSealed {
            end: sealed
                .as_deref()
                .ok_or_else(|| JournalError::CorruptStorage("finished source has no seal".into()))
                .and_then(|value| decode_journal(value, "finished source end"))?,
        });
    }
    let live_next =
        decode_offset(&live_next).map_err(|e| JournalError::CorruptStorage(e.to_string()))?;
    if output.position.item_index != live_next
        || last
            .as_deref()
            .and_then(|v| decode_offset(v).ok())
            .is_some_and(|v| output.position.source_byte < v)
    {
        return Err(JournalError::OutputConflict {
            source: output.source,
            item_index: output.position.item_index,
        });
    }
    let charge = MARKER_OVERHEAD
        + output.event.id.as_str().len()
        + output.source.id.as_str().len()
        + output_stream.id.as_str().len();
    let (count, bytes): (i64, Vec<u8>) = tx
        .query_row(
            "SELECT marker_count,marker_bytes FROM journal_metadata WHERE singleton=1",
            [],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .map_err(|e| failure("read marker counters", e))?;
    let bytes = decode_offset(&bytes).map_err(|e| JournalError::CorruptStorage(e.to_string()))?;
    if count as usize >= options.source_journal.storage.max_output_markers
        || bytes
            .checked_add(charge as u64)
            .is_none_or(|v| v > options.source_journal.storage.max_marker_bytes)
    {
        return Err(JournalError::CapacityExceeded);
    }
    let (_, generation) = super::sqlite_retention::policy(
        &tx,
        stream_row(&tx, &output_stream)
            .map_err(|error| JournalError::InvalidInput(format!("invalid output stream: {error}")))?
            .0,
    )
    .map_err(JournalError::from)?;
    let receipt = super::sqlite_retention::append_generated_in_transaction(
        &tx,
        output_stream.clone(),
        GeneratedEvent {
            generation,
            event: output.event.clone(),
        },
        options,
    )
    .map_err(JournalError::from)?;
    let next = output
        .position
        .item_index
        .checked_add(1)
        .ok_or(JournalError::CapacityExceeded)?;
    tx.execute(
        "INSERT INTO journal_markers VALUES(?1,?2,?3,?4,?5,?6,?7,?8)",
        params![
            output.source.id.as_str(),
            output.source.incarnation.0.as_slice(),
            be(output.position.item_index).as_slice(),
            be(output.position.source_byte).as_slice(),
            be(generation.get()).as_slice(),
            output.event.id.as_str(),
            be(receipt.record.cursor.offset).as_slice(),
            charge as i64
        ],
    )
    .map_err(|e| failure("store output marker", e))?;
    tx.execute("UPDATE journal_sources SET next_item_index=?1,last_source_byte=?2 WHERE source_id=?3 AND source_incarnation=?4", params![be(next).as_slice(), be(output.position.source_byte).as_slice(), output.source.id.as_str(), output.source.incarnation.0.as_slice()]).map_err(|e| failure("advance output marker", e))?;
    tx.execute(
        "UPDATE journal_metadata SET marker_count=marker_count+1,marker_bytes=?1 WHERE singleton=1",
        [be(bytes + charge as u64).as_slice()],
    )
    .map_err(|e| failure("update marker counters", e))?;
    if matches!(
        failure_injection,
        Some(super::sqlite::SqliteFailureInjection::BeforeJournalOutputCommit)
    ) {
        *failure_injection = None;
        tx.rollback()
            .map_err(|error| failure("roll back injected journal output failure", error))?;
        return Err(JournalError::StorageFailure(
            "injected failure before journal output commit".into(),
        ));
    }
    tx.commit().map_err(|_| JournalError::OutputUnknown {
        output_stream: Box::new(output_stream.clone()),
        output: Box::new(output.clone()),
    })?;
    if let Some(
        super::sqlite::SqliteFailureInjection::PauseAfterJournalOutputCommitAcknowledgementLost(
            delay,
        ),
    ) = *failure_injection
    {
        *failure_injection = None;
        std::thread::park_timeout(delay);
        return Err(JournalError::OutputUnknown {
            output_stream: Box::new(output_stream),
            output: Box::new(output),
        });
    }
    if matches!(
        failure_injection,
        Some(super::sqlite::SqliteFailureInjection::AfterJournalOutputCommitAcknowledgementLost)
    ) {
        *failure_injection = None;
        return Err(JournalError::OutputUnknown {
            output_stream: Box::new(output_stream),
            output: Box::new(output),
        });
    }
    Ok(receipt)
}

fn latest(conn: &Connection, source: &SourceKey) -> JournalResult<Option<ParserCheckpoint>> {
    let status = progress(conn, source)?;
    let row: Option<(Vec<u8>, Vec<u8>, Vec<u8>)> = conn
        .query_row(
            "SELECT checkpoint_offset,checkpoint_state,checkpoint_next_index FROM journal_sources
         WHERE source_id=?1 AND source_incarnation=?2 AND checkpoint_offset IS NOT NULL",
            params![source.id.as_str(), source.incarnation.0.as_slice()],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
        )
        .optional()
        .map_err(|e| failure("read checkpoint", e))?;
    row.map(|(offset, state, next)| {
        Ok(ParserCheckpoint {
            source: SourcePosition {
                source: source.clone(),
                offset: decode_offset(&offset)
                    .map_err(|e| JournalError::CorruptStorage(e.to_string()))?,
            },
            parser: status.binding.parser,
            state: Payload::copy_from_slice(&state),
            next_item_index: decode_offset(&next)
                .map_err(|e| JournalError::CorruptStorage(e.to_string()))?,
            output_stream: status.binding.output_stream,
            committed_output: status.committed_output,
        })
    })
    .transpose()
}

fn checkpoint(
    conn: &mut Connection,
    value: ParserCheckpoint,
    options: &SqliteOptions,
    failure_injection: &mut Option<super::sqlite::SqliteFailureInjection>,
) -> JournalResult<CheckpointReceipt> {
    if value.state.len() > options.source_journal.storage.max_checkpoint_state_bytes {
        return Err(JournalError::CapacityExceeded);
    }
    let current = progress(conn, &value.source.source)?;
    if value.parser != current.binding.parser
        || value.output_stream != current.binding.output_stream
        || value.source.offset > current.captured_end
        || value.source.offset < current.checkpoint_offset
        || value.next_item_index < current.next_item_index
    {
        return Err(JournalError::CheckpointConflict {
            source: value.source.source,
        });
    }
    if latest(conn, &value.source.source)?.as_ref() == Some(&value) {
        return Ok(CheckpointReceipt { checkpoint: value });
    }
    let (finished, sealed): (i64, Option<Vec<u8>>) = conn
        .query_row(
            "SELECT parser_finished,sealed_end FROM journal_sources
             WHERE source_id=?1 AND source_incarnation=?2",
            params![
                value.source.source.id.as_str(),
                value.source.source.incarnation.0.as_slice()
            ],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .map_err(|error| failure("read source finalization before checkpoint", error))?;
    if finished == 1 {
        let _ = sealed;
        return Err(JournalError::CheckpointConflict {
            source: value.source.source,
        });
    }
    let expected = if value.next_item_index == current.next_item_index {
        current.committed_output.clone()
    } else {
        let (count, last): (i64, Option<Vec<u8>>) = conn.query_row(
            "SELECT count(*),(SELECT committed_offset FROM journal_markers WHERE source_id=?1 AND source_incarnation=?2 AND item_index<?4 ORDER BY item_index DESC LIMIT 1)
             FROM journal_markers WHERE source_id=?1 AND source_incarnation=?2 AND item_index>=?3 AND item_index<?4",
            params![value.source.source.id.as_str(), value.source.source.incarnation.0.as_slice(), be(current.next_item_index).as_slice(), be(value.next_item_index).as_slice()],
            |r| Ok((r.get(0)?, r.get(1)?)),
        ).map_err(|e| failure("validate checkpoint markers", e))?;
        if count as u64 != value.next_item_index - current.next_item_index {
            return Err(JournalError::CheckpointConflict {
                source: value.source.source,
            });
        }
        last.map(|v| {
            decode_offset(&v).map(|offset| Cursor::new(value.output_stream.clone(), offset))
        })
        .transpose()
        .map_err(|e| JournalError::CorruptStorage(e.to_string()))?
    };
    if value.committed_output != expected {
        return Err(JournalError::CheckpointConflict {
            source: value.source.source,
        });
    }
    let charge = value.state.len()
        + 256
        + value.source.source.id.as_str().len()
        + value.parser.id.as_str().len()
        + value.output_stream.id.as_str().len();
    let (count, bytes, old_charge): (i64, Vec<u8>, i64) = conn.query_row(
        "SELECT m.checkpoint_count,m.checkpoint_bytes,s.checkpoint_charge FROM journal_metadata m,journal_sources s WHERE s.source_id=?1 AND s.source_incarnation=?2",
        params![value.source.source.id.as_str(), value.source.source.incarnation.0.as_slice()], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
    ).map_err(|e| failure("read checkpoint counters", e))?;
    let bytes = decode_offset(&bytes).map_err(|e| JournalError::CorruptStorage(e.to_string()))?;
    let next_bytes = bytes
        .checked_sub(old_charge as u64)
        .and_then(|v| v.checked_add(charge as u64))
        .ok_or(JournalError::CapacityExceeded)?;
    if (old_charge == 0 && count as usize >= options.source_journal.storage.max_checkpoints)
        || next_bytes > options.source_journal.storage.max_staging_bytes
    {
        return Err(JournalError::CapacityExceeded);
    }
    let tx = conn
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .map_err(|e| failure("begin checkpoint", e))?;
    let changed = tx.execute("UPDATE journal_sources SET checkpoint_offset=?1,checkpoint_state=?2,checkpoint_next_index=?3,checkpoint_committed=?4,checkpoint_charge=?5 WHERE source_id=?6 AND source_incarnation=?7 AND parser_finished=0", params![be(value.source.offset).as_slice(), value.state.as_bytes(), be(value.next_item_index).as_slice(), value.committed_output.as_ref().map(|v| be(v.offset)), charge as i64, value.source.source.id.as_str(), value.source.source.incarnation.0.as_slice()]).map_err(|e| failure("store checkpoint", e))?;
    if changed != 1 {
        tx.rollback()
            .map_err(|error| failure("roll back checkpoint after source finish", error))?;
        return Err(JournalError::CheckpointConflict {
            source: value.source.source,
        });
    }
    tx.execute("UPDATE journal_metadata SET checkpoint_count=checkpoint_count+?1,checkpoint_bytes=?2 WHERE singleton=1", params![if old_charge == 0 { 1 } else { 0 }, be(next_bytes).as_slice()]).map_err(|e| failure("update checkpoint counters", e))?;
    if matches!(
        failure_injection,
        Some(super::sqlite::SqliteFailureInjection::BeforeJournalCheckpointCommit)
    ) {
        *failure_injection = None;
        tx.rollback()
            .map_err(|error| failure("roll back injected journal checkpoint failure", error))?;
        return Err(JournalError::StorageFailure(
            "injected failure before journal checkpoint commit".into(),
        ));
    }
    tx.commit()
        .map_err(|_| JournalError::CheckpointUnknown(Box::new(value.clone())))?;
    if matches!(
        failure_injection,
        Some(
            super::sqlite::SqliteFailureInjection::AfterJournalCheckpointCommitAcknowledgementLost
        )
    ) {
        *failure_injection = None;
        return Err(JournalError::CheckpointUnknown(Box::new(value)));
    }
    Ok(CheckpointReceipt { checkpoint: value })
}

fn read(
    conn: &Connection,
    source: &SourceKey,
    offset: u64,
    limits: RawPageLimits,
    options: &SqliteOptions,
) -> JournalResult<RawPage> {
    if limits.max_segments == 0
        || limits.max_bytes == 0
        || limits.max_segments > options.source_journal.cleanup.max_segment_rows
        || limits.max_bytes > options.source_journal.cleanup.max_bytes
    {
        return Err(JournalError::InvalidInput(
            "raw page limits are outside configured bounds".into(),
        ));
    }
    let status = progress(conn, source)?;
    if offset > status.captured_end {
        return Err(JournalError::InvalidInput(
            "raw page starts after captured end".into(),
        ));
    }
    if offset == status.captured_end {
        return Ok(RawPage {
            start: SourcePosition {
                source: source.clone(),
                offset,
            },
            bytes: Payload::copy_from_slice(&[]),
            next_offset: offset,
            complete: true,
        });
    }
    let mut position = offset;
    let mut output = Vec::with_capacity(limits.max_bytes);
    let mut rows = 0;
    while rows < limits.max_segments && output.len() < limits.max_bytes {
        type SegmentRow = (
            Option<Vec<u8>>,
            Option<Vec<u8>>,
            Option<i64>,
            Option<Vec<u8>>,
        );
        let row: Option<SegmentRow> = conn
            .query_row(
                "SELECT CASE WHEN typeof(start)='blob' AND length(start)=8 THEN start END,
                    CASE WHEN typeof(end)='blob' AND length(end)=8 THEN end END,
                    CASE WHEN typeof(bytes)='blob' THEN octet_length(bytes) END,
                    CASE WHEN typeof(bytes)='blob' AND octet_length(bytes)<=?4 THEN bytes END
             FROM journal_segments WHERE source_id=?1 AND source_incarnation=?2 AND start<=?3
             ORDER BY start DESC LIMIT 1",
                params![
                    source.id.as_str(),
                    source.incarnation.0.as_slice(),
                    be(position).as_slice(),
                    i64::try_from(options.source_journal.storage.max_segment_bytes)
                        .unwrap_or(i64::MAX)
                ],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?)),
            )
            .optional()
            .map_err(|e| failure("read captured segment", e))?;
        let row = row.ok_or(JournalError::MissingCapturedHistory {
            available_from: status.captured_end,
        })?;
        let (Some(start), Some(end), Some(length), Some(bytes)) = row else {
            return Err(JournalError::CorruptStorage(
                "captured segment columns are malformed or exceed configured bounds".into(),
            ));
        };
        let start =
            decode_offset(&start).map_err(|e| JournalError::CorruptStorage(e.to_string()))?;
        let end = decode_offset(&end).map_err(|e| JournalError::CorruptStorage(e.to_string()))?;
        let length = usize::try_from(length)
            .map_err(|_| JournalError::CorruptStorage("invalid captured segment length".into()))?;
        if length != bytes.len() || start.checked_add(length as u64) != Some(end) {
            return Err(JournalError::CorruptStorage(
                "captured segment bounds are inconsistent".into(),
            ));
        }
        if position < start || position >= end {
            return Err(JournalError::MissingCapturedHistory {
                available_from: status.captured_end,
            });
        }
        let inside = (position - start) as usize;
        let take = (bytes.len() - inside).min(limits.max_bytes - output.len());
        output.extend_from_slice(&bytes[inside..inside + take]);
        position += take as u64;
        rows += 1;
        if inside + take < bytes.len() || position == status.captured_end {
            break;
        }
    }
    Ok(RawPage {
        start: SourcePosition {
            source: source.clone(),
            offset,
        },
        bytes: Payload::copy_from_slice(&output),
        next_offset: position,
        complete: position == status.captured_end,
    })
}

fn cleanup(
    conn: &mut Connection,
    limits: JournalCleanupLimits,
    options: &SqliteOptions,
) -> JournalResult<JournalCleanupProgress> {
    if limits.max_segment_rows == 0
        || limits.max_marker_rows == 0
        || limits.max_receipt_rows == 0
        || limits.max_bytes == 0
    {
        return Err(JournalError::InvalidInput(
            "journal cleanup limits must be nonzero".into(),
        ));
    }
    let configured = &options.source_journal.cleanup;
    if limits.max_segment_rows > configured.max_segment_rows
        || limits.max_marker_rows > configured.max_marker_rows
        || limits.max_receipt_rows > configured.max_receipt_rows
        || limits.max_bytes > configured.max_bytes
    {
        return Err(JournalError::CapacityExceeded);
    }
    let tx = conn
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .map_err(|e| failure("begin journal cleanup", e))?;
    let mut segment_rows = 0usize;
    let mut marker_rows = 0usize;
    let mut receipt_rows = 0usize;
    let mut removed_bytes = 0usize;
    let mut segment_payload_bytes = 0u64;
    let mut marker_bytes = 0u64;
    let mut receipt_bytes = 0usize;
    while segment_rows < limits.max_segment_rows {
        let row: Option<(String, Vec<u8>, Vec<u8>, i64)> = tx.query_row("SELECT g.source_id,g.source_incarnation,g.start,octet_length(g.bytes) FROM journal_segments g JOIN journal_sources s USING(source_id,source_incarnation) WHERE s.checkpoint_offset IS NOT NULL AND g.end<=s.checkpoint_offset ORDER BY g.source_id,g.source_incarnation,g.start LIMIT 1", [], |r| Ok((r.get(0)?,r.get(1)?,r.get(2)?,r.get(3)?))).optional().map_err(|e| failure("select segment cleanup", e))?;
        let Some((id, incarnation, start, length)) = row else {
            break;
        };
        let charge = usize::try_from(length)
            .map_err(|_| JournalError::CorruptStorage("invalid captured segment length".into()))?
            + SEGMENT_OVERHEAD
            + id.len();
        if removed_bytes
            .checked_add(charge)
            .is_none_or(|v| v > limits.max_bytes)
        {
            break;
        }
        tx.execute("DELETE FROM journal_segments WHERE source_id=?1 AND source_incarnation=?2 AND start=?3", params![id,incarnation,start]).map_err(|e| failure("delete captured segment", e))?;
        segment_rows += 1;
        removed_bytes += charge;
        segment_payload_bytes += length as u64;
    }
    while marker_rows < limits.max_marker_rows {
        let row: Option<(String, Vec<u8>, Vec<u8>, i64)> = tx.query_row("SELECT m.source_id,m.source_incarnation,m.item_index,m.charge FROM journal_markers m JOIN journal_sources s USING(source_id,source_incarnation) WHERE s.checkpoint_next_index IS NOT NULL AND m.item_index<s.checkpoint_next_index ORDER BY m.source_id,m.source_incarnation,m.item_index LIMIT 1", [], |r| Ok((r.get(0)?,r.get(1)?,r.get(2)?,r.get(3)?))).optional().map_err(|e| failure("select marker cleanup", e))?;
        let Some((id, incarnation, index, charge)) = row else {
            break;
        };
        let charge = usize::try_from(charge)
            .map_err(|_| JournalError::CorruptStorage("invalid marker charge".into()))?;
        if removed_bytes
            .checked_add(charge)
            .is_none_or(|v| v > limits.max_bytes)
        {
            break;
        }
        tx.execute("DELETE FROM journal_markers WHERE source_id=?1 AND source_incarnation=?2 AND item_index=?3", params![id,incarnation,index]).map_err(|e| failure("delete output marker", e))?;
        marker_rows += 1;
        removed_bytes += charge;
        marker_bytes += charge as u64;
    }
    while receipt_rows < limits.max_receipt_rows {
        let row: Option<(String, Vec<u8>, Vec<u8>, i64)> = tx.query_row("SELECT r.source_id,r.source_incarnation,r.start,r.charge FROM journal_capture_receipts r JOIN journal_sources s USING(source_id,source_incarnation) WHERE r.end<=s.receipt_floor ORDER BY r.source_id,r.source_incarnation,r.start LIMIT 1", [], |r| Ok((r.get(0)?,r.get(1)?,r.get(2)?,r.get(3)?))).optional().map_err(|e| failure("select receipt cleanup", e))?;
        let Some((id, incarnation, start, charge)) = row else {
            break;
        };
        let charge = usize::try_from(charge)
            .map_err(|_| JournalError::CorruptStorage("invalid receipt charge".into()))?;
        if removed_bytes
            .checked_add(charge)
            .is_none_or(|v| v > limits.max_bytes)
        {
            break;
        }
        tx.execute("DELETE FROM journal_capture_receipts WHERE source_id=?1 AND source_incarnation=?2 AND start=?3", params![id,incarnation,start]).map_err(|e| failure("delete capture receipt", e))?;
        receipt_rows += 1;
        removed_bytes += charge;
        receipt_bytes += charge;
    }
    let (captured, markers, receipts): (Vec<u8>, Vec<u8>, i64) = tx.query_row("SELECT captured_bytes,marker_bytes,receipt_bytes FROM journal_metadata WHERE singleton=1", [], |r| Ok((r.get(0)?,r.get(1)?,r.get(2)?))).map_err(|e| failure("read cleanup counters", e))?;
    let captured = decode_offset(&captured)
        .map_err(|e| JournalError::CorruptStorage(e.to_string()))?
        .checked_sub(segment_payload_bytes)
        .ok_or_else(|| JournalError::CorruptStorage("captured byte counter underflow".into()))?;
    let markers = decode_offset(&markers)
        .map_err(|e| JournalError::CorruptStorage(e.to_string()))?
        .checked_sub(marker_bytes)
        .ok_or_else(|| JournalError::CorruptStorage("marker byte counter underflow".into()))?;
    if receipts < receipt_bytes as i64 {
        return Err(JournalError::CorruptStorage(
            "receipt byte counter underflow".into(),
        ));
    }
    tx.execute("UPDATE journal_metadata SET segment_count=segment_count-?1,captured_bytes=?2,marker_count=marker_count-?3,marker_bytes=?4,receipt_count=receipt_count-?5,receipt_bytes=receipt_bytes-?6 WHERE singleton=1", params![segment_rows as i64,be(captured).as_slice(),marker_rows as i64,be(markers).as_slice(),receipt_rows as i64,receipt_bytes as i64]).map_err(|e| failure("release journal counters", e))?;
    let remaining: bool = tx.query_row("SELECT EXISTS(SELECT 1 FROM journal_segments g JOIN journal_sources s USING(source_id,source_incarnation) WHERE s.checkpoint_offset IS NOT NULL AND g.end<=s.checkpoint_offset UNION ALL SELECT 1 FROM journal_markers m JOIN journal_sources s USING(source_id,source_incarnation) WHERE s.checkpoint_next_index IS NOT NULL AND m.item_index<s.checkpoint_next_index UNION ALL SELECT 1 FROM journal_capture_receipts r JOIN journal_sources s USING(source_id,source_incarnation) WHERE r.end<=s.receipt_floor)", [], |r| r.get(0)).map_err(|e| failure("check journal cleanup", e))?;
    tx.commit()
        .map_err(|e| failure("commit journal cleanup", e))?;
    Ok(JournalCleanupProgress {
        removed_segment_rows: segment_rows,
        removed_marker_rows: marker_rows,
        removed_receipt_rows: receipt_rows,
        removed_bytes,
        remaining,
    })
}

fn finalization(conn: &Connection, source: &SourceKey) -> JournalResult<SourceFinalizationStatus> {
    let row: Option<(Option<Vec<u8>>, i64)> = conn
        .query_row(
            "SELECT sealed_end,parser_finished FROM journal_sources
             WHERE source_id=?1 AND source_incarnation=?2",
            params![source.id.as_str(), source.incarnation.0.as_slice()],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .optional()
        .map_err(|error| failure("read source finalization", error))?;
    let Some((sealed, finished)) = row else {
        return Err(JournalError::NotFound {
            source: source.clone(),
        });
    };
    if !matches!(finished, 0 | 1) {
        return Err(JournalError::CorruptStorage(
            "invalid parser finished flag".into(),
        ));
    }
    let sealed_end = sealed
        .as_deref()
        .map(|value| decode_journal(value, "sealed source end"))
        .transpose()?;
    if finished == 1 && sealed_end.is_none() {
        return Err(JournalError::CorruptStorage(
            "finished source has no seal".into(),
        ));
    }
    Ok(SourceFinalizationStatus {
        sealed_end,
        parser_finished: finished == 1,
    })
}

fn seal(
    conn: &mut Connection,
    request: SealSource,
    failure_injection: &mut Option<super::sqlite::SqliteFailureInjection>,
) -> JournalResult<SealSourceReceipt> {
    let current = progress(conn, &request.end.source)?;
    let state = finalization(conn, &request.end.source)?;
    if let Some(end) = state.sealed_end {
        return if end == request.end.offset {
            Ok(SealSourceReceipt { request })
        } else {
            Err(JournalError::InvalidInput(
                "source was sealed at another boundary".into(),
            ))
        };
    }
    if request.end.offset != current.captured_end {
        return Err(JournalError::InvalidInput(
            "source seal must equal the captured end".into(),
        ));
    }
    let tx = conn
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .map_err(|error| failure("begin source seal", error))?;
    let changed = tx
        .execute(
            "UPDATE journal_sources SET sealed_end=?1
             WHERE source_id=?2 AND source_incarnation=?3
               AND sealed_end IS NULL AND captured_end=?1",
            params![
                be(request.end.offset).as_slice(),
                request.end.source.id.as_str(),
                request.end.source.incarnation.0.as_slice()
            ],
        )
        .map_err(|error| failure("store source seal", error))?;
    if changed != 1 {
        tx.rollback()
            .map_err(|error| failure("roll back stale source seal", error))?;
        return Err(JournalError::InvalidInput(
            "source changed before the seal committed".into(),
        ));
    }
    if matches!(
        failure_injection,
        Some(super::sqlite::SqliteFailureInjection::BeforeJournalSealCommit)
    ) {
        *failure_injection = None;
        tx.rollback()
            .map_err(|error| failure("roll back injected source seal", error))?;
        return Err(JournalError::StorageFailure(
            "injected failure before source seal commit".into(),
        ));
    }
    tx.commit()
        .map_err(|_| JournalError::SealUnknown(Box::new(request.clone())))?;
    if matches!(
        failure_injection,
        Some(super::sqlite::SqliteFailureInjection::AfterJournalSealCommitAcknowledgementLost)
    ) {
        *failure_injection = None;
        return Err(JournalError::SealUnknown(Box::new(request)));
    }
    Ok(SealSourceReceipt { request })
}

fn finish(
    conn: &mut Connection,
    request: FinishSource,
    failure_injection: &mut Option<super::sqlite::SqliteFailureInjection>,
) -> JournalResult<FinishSourceReceipt> {
    let state = finalization(conn, &request.checkpoint.source.source)?;
    let Some(sealed_end) = state.sealed_end else {
        return Err(JournalError::CheckpointConflict {
            source: request.checkpoint.source.source,
        });
    };
    let stored = latest(conn, &request.checkpoint.source.source)?;
    if stored.as_ref() != Some(&request.checkpoint)
        || request.checkpoint.source.offset != sealed_end
    {
        return Err(JournalError::CheckpointConflict {
            source: request.checkpoint.source.source,
        });
    }
    if state.parser_finished {
        return Ok(FinishSourceReceipt { request });
    }
    let tx = conn
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .map_err(|error| failure("begin parser finish", error))?;
    let changed = tx
        .execute(
            "UPDATE journal_sources SET parser_finished=1
             WHERE source_id=?1 AND source_incarnation=?2 AND parser_finished=0
               AND sealed_end=?3 AND checkpoint_offset=?3",
            params![
                request.checkpoint.source.source.id.as_str(),
                request.checkpoint.source.source.incarnation.0.as_slice(),
                be(sealed_end).as_slice()
            ],
        )
        .map_err(|error| failure("store parser finish", error))?;
    if changed != 1 {
        tx.rollback()
            .map_err(|error| failure("roll back stale parser finish", error))?;
        return Err(JournalError::CheckpointConflict {
            source: request.checkpoint.source.source,
        });
    }
    if matches!(
        failure_injection,
        Some(super::sqlite::SqliteFailureInjection::BeforeJournalFinishCommit)
    ) {
        *failure_injection = None;
        tx.rollback()
            .map_err(|error| failure("roll back injected parser finish", error))?;
        return Err(JournalError::StorageFailure(
            "injected failure before parser finish commit".into(),
        ));
    }
    tx.commit()
        .map_err(|_| JournalError::FinishUnknown(Box::new(request.clone())))?;
    if matches!(
        failure_injection,
        Some(super::sqlite::SqliteFailureInjection::AfterJournalFinishCommitAcknowledgementLost)
    ) {
        *failure_injection = None;
        return Err(JournalError::FinishUnknown(Box::new(request)));
    }
    Ok(FinishSourceReceipt { request })
}

pub(super) fn handle_journal_command(
    conn: &mut Connection,
    command: JournalCommand,
    options: &SqliteOptions,
    failure_injection: &mut Option<super::sqlite::SqliteFailureInjection>,
) {
    match command {
        JournalCommand::Begin(v, tx) => {
            let _ = tx.send(begin(conn, v, options));
        }
        JournalCommand::Capture(v, tx) => {
            let _ = tx.send(capture(conn, v, options, failure_injection));
        }
        JournalCommand::Floor(v, tx) => {
            let _ = tx.send(advance_floor(conn, v, options));
        }
        JournalCommand::Append(stream, v, tx) => {
            let _ = tx.send(append(conn, stream, v, options, failure_injection));
        }
        JournalCommand::Checkpoint(v, tx) => {
            let _ = tx.send(checkpoint(conn, v, options, failure_injection));
        }
        JournalCommand::Status(v, tx) => {
            let _ = tx.send(progress(conn, &v));
        }
        JournalCommand::Read(v, offset, limits, tx) => {
            let _ = tx.send(read(conn, &v, offset, limits, options));
        }
        JournalCommand::Latest(v, tx) => {
            let _ = tx.send(latest(conn, &v));
        }
        JournalCommand::Cleanup(limits, tx) => {
            let _ = tx.send(cleanup(conn, limits, options));
        }
        JournalCommand::Seal(request, tx) => {
            let _ = tx.send(seal(conn, request, failure_injection));
        }
        JournalCommand::Finish(request, tx) => {
            let _ = tx.send(finish(conn, request, failure_injection));
        }
        JournalCommand::Finalization(source, tx) => {
            let _ = tx.send(finalization(conn, &source));
        }
    }
}

pub(super) fn fail_journal_command(command: JournalCommand) {
    let e = || JournalError::StorageFailure("SQLite worker faulted".into());
    match command {
        JournalCommand::Begin(v, tx) => {
            let _ = tx.send(Err(JournalError::BeginUnknown(Box::new(v))));
        }
        JournalCommand::Capture(v, tx) => {
            let _ = tx.send(Err(JournalError::CaptureUnknown(Box::new(v))));
        }
        JournalCommand::Floor(v, tx) => {
            let _ = tx.send(Err(JournalError::AdvanceCaptureReceiptFloorUnknown(
                Box::new(v),
            )));
        }
        JournalCommand::Append(stream, v, tx) => {
            let _ = tx.send(Err(JournalError::OutputUnknown {
                output_stream: Box::new(stream),
                output: Box::new(v),
            }));
        }
        JournalCommand::Checkpoint(v, tx) => {
            let _ = tx.send(Err(JournalError::CheckpointUnknown(Box::new(v))));
        }
        JournalCommand::Status(_, tx) => {
            let _ = tx.send(Err(e()));
        }
        JournalCommand::Read(_, _, _, tx) => {
            let _ = tx.send(Err(e()));
        }
        JournalCommand::Latest(_, tx) => {
            let _ = tx.send(Err(e()));
        }
        JournalCommand::Cleanup(_, tx) => {
            let _ = tx.send(Err(e()));
        }
        JournalCommand::Seal(request, tx) => {
            let _ = tx.send(Err(JournalError::SealUnknown(Box::new(request))));
        }
        JournalCommand::Finish(request, tx) => {
            let _ = tx.send(Err(JournalError::FinishUnknown(Box::new(request))));
        }
        JournalCommand::Finalization(_, tx) => {
            let _ = tx.send(Err(e()));
        }
    }
}

async fn receive<T>(
    rx: oneshot::Receiver<JournalResult<T>>,
    unknown: impl FnOnce() -> JournalError,
) -> JournalResult<T> {
    rx.await.unwrap_or_else(|_| Err(unknown()))
}

#[async_trait]
impl SourceJournalStore for SqliteStore {
    async fn begin_source(&self, v: BeginSource) -> JournalResult<BeginSourceReceipt> {
        let unknown = v.clone();
        let (tx, rx) = oneshot::channel();
        self.submit(Command::Journal(JournalCommand::Begin(v, tx)))
            .map_err(|e| JournalError::StorageFailure(e.to_string()))?;
        receive(rx, || JournalError::BeginUnknown(Box::new(unknown))).await
    }
    async fn capture_segment(&self, v: RawSegment) -> JournalResult<CaptureReceipt> {
        let unknown = v.clone();
        let (tx, rx) = oneshot::channel();
        self.submit(Command::Journal(JournalCommand::Capture(v, tx)))
            .map_err(|e| JournalError::StorageFailure(e.to_string()))?;
        receive(rx, || JournalError::CaptureUnknown(Box::new(unknown))).await
    }
    async fn advance_capture_receipt_floor(
        &self,
        v: AdvanceCaptureReceiptFloor,
    ) -> JournalResult<AdvanceCaptureReceiptFloorReceipt> {
        let unknown = v.clone();
        let (tx, rx) = oneshot::channel();
        self.submit(Command::Journal(JournalCommand::Floor(v, tx)))
            .map_err(|e| JournalError::StorageFailure(e.to_string()))?;
        receive(rx, || {
            JournalError::AdvanceCaptureReceiptFloorUnknown(Box::new(unknown))
        })
        .await
    }
    async fn append_captured(
        &self,
        stream: &StreamKey,
        v: JournaledOutput,
    ) -> JournalResult<AppendReceipt> {
        let s = stream.clone();
        let unknown = v.clone();
        let (tx, rx) = oneshot::channel();
        self.submit(Command::Journal(JournalCommand::Append(s.clone(), v, tx)))
            .map_err(|e| JournalError::StorageFailure(e.to_string()))?;
        receive(rx, || JournalError::OutputUnknown {
            output_stream: Box::new(s),
            output: Box::new(unknown),
        })
        .await
    }
    async fn publish_parser_checkpoint(
        &self,
        v: ParserCheckpoint,
    ) -> JournalResult<CheckpointReceipt> {
        let unknown = v.clone();
        let (tx, rx) = oneshot::channel();
        self.submit(Command::Journal(JournalCommand::Checkpoint(v, tx)))
            .map_err(|e| JournalError::StorageFailure(e.to_string()))?;
        receive(rx, || JournalError::CheckpointUnknown(Box::new(unknown))).await
    }
    async fn source_status(&self, v: &SourceKey) -> JournalResult<SourceProgress> {
        let (tx, rx) = oneshot::channel();
        self.submit(Command::Journal(JournalCommand::Status(v.clone(), tx)))
            .map_err(|e| JournalError::StorageFailure(e.to_string()))?;
        receive(rx, || {
            JournalError::StorageFailure("SQLite worker stopped".into())
        })
        .await
    }
    async fn read_captured(
        &self,
        v: &SourceKey,
        offset: u64,
        l: RawPageLimits,
    ) -> JournalResult<RawPage> {
        let (tx, rx) = oneshot::channel();
        self.submit(Command::Journal(JournalCommand::Read(
            v.clone(),
            offset,
            l,
            tx,
        )))
        .map_err(|e| JournalError::StorageFailure(e.to_string()))?;
        receive(rx, || {
            JournalError::StorageFailure("SQLite worker stopped".into())
        })
        .await
    }
    async fn latest_checkpoint(&self, v: &SourceKey) -> JournalResult<Option<ParserCheckpoint>> {
        let (tx, rx) = oneshot::channel();
        self.submit(Command::Journal(JournalCommand::Latest(v.clone(), tx)))
            .map_err(|e| JournalError::StorageFailure(e.to_string()))?;
        receive(rx, || {
            JournalError::StorageFailure("SQLite worker stopped".into())
        })
        .await
    }
    async fn cleanup_captured(
        &self,
        l: JournalCleanupLimits,
    ) -> JournalResult<JournalCleanupProgress> {
        let (tx, rx) = oneshot::channel();
        self.submit(Command::Journal(JournalCommand::Cleanup(l, tx)))
            .map_err(|e| JournalError::StorageFailure(e.to_string()))?;
        receive(rx, || {
            JournalError::StorageFailure("SQLite worker stopped".into())
        })
        .await
    }
}

#[async_trait]
impl SourceFinalizationStore for SqliteStore {
    async fn seal_source(&self, request: SealSource) -> JournalResult<SealSourceReceipt> {
        let unknown = request.clone();
        let (tx, rx) = oneshot::channel();
        self.submit(Command::Journal(JournalCommand::Seal(request, tx)))
            .map_err(|error| JournalError::StorageFailure(error.to_string()))?;
        receive(rx, || JournalError::SealUnknown(Box::new(unknown))).await
    }

    async fn finish_source(&self, request: FinishSource) -> JournalResult<FinishSourceReceipt> {
        let unknown = request.clone();
        let (tx, rx) = oneshot::channel();
        self.submit(Command::Journal(JournalCommand::Finish(request, tx)))
            .map_err(|error| JournalError::StorageFailure(error.to_string()))?;
        receive(rx, || JournalError::FinishUnknown(Box::new(unknown))).await
    }

    async fn source_finalization(
        &self,
        source: &SourceKey,
    ) -> JournalResult<SourceFinalizationStatus> {
        let (tx, rx) = oneshot::channel();
        self.submit(Command::Journal(JournalCommand::Finalization(
            source.clone(),
            tx,
        )))
        .map_err(|error| JournalError::StorageFailure(error.to_string()))?;
        receive(rx, || {
            JournalError::StorageFailure("SQLite worker stopped".into())
        })
        .await
    }
}
