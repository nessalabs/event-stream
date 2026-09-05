use super::{
    Cursor, DecodedPosition, EventId, NewEvent, Payload, RetryGeneration, StreamKey,
    ValidationError, MAX_IDENTIFIER_BYTES,
};

macro_rules! journal_identifier {
    ($name:ident) => {
        #[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
        pub struct $name(Box<str>);

        impl $name {
            pub fn new(value: impl AsRef<str>) -> Result<Self, ValidationError> {
                let value = value.as_ref();
                if value.is_empty() || value.len() > MAX_IDENTIFIER_BYTES {
                    return Err(ValidationError::InvalidIdentifier);
                }
                Ok(Self(value.into()))
            }

            pub fn as_str(&self) -> &str {
                &self.0
            }
        }
    };
}

journal_identifier!(SourceId);
journal_identifier!(ParserId);
journal_identifier!(JournalOperationId);

#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub struct SourceIncarnation(pub [u8; 16]);

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct SourceKey {
    pub id: SourceId,
    pub incarnation: SourceIncarnation,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SourcePosition {
    pub source: SourceKey,
    pub offset: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RawSegment {
    pub start: SourcePosition,
    pub bytes: Payload,
}

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct ParserRef {
    pub id: ParserId,
    pub version: u32,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SourceBinding {
    pub source: SourceKey,
    pub parser: ParserRef,
    pub output_stream: StreamKey,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BeginSource {
    pub operation_id: JournalOperationId,
    pub binding: SourceBinding,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AdvanceCaptureReceiptFloor {
    pub operation_id: JournalOperationId,
    pub source: SourceKey,
    pub expected_floor: u64,
    pub new_floor: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ParserCheckpoint {
    pub source: SourcePosition,
    pub parser: ParserRef,
    pub state: Payload,
    pub next_item_index: u64,
    pub output_stream: StreamKey,
    pub committed_output: Option<Cursor>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct JournaledOutput {
    pub source: SourceKey,
    /// The decoder's frame start and item index. `source_byte` is not a consumed-byte boundary.
    pub position: DecodedPosition,
    pub event: NewEvent,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub struct CaptureDigest(pub [u8; 32]);

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OutputMarker {
    pub source: SourceKey,
    pub position: DecodedPosition,
    pub retry_generation: RetryGeneration,
    pub event_id: EventId,
    pub committed: Cursor,
}

/// Permanently end capture for one source lifetime at its exact committed tail.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SealSource {
    pub end: SourcePosition,
}

/// Confirm that the stored parser checkpoint represents completed EOF handling.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FinishSource {
    pub checkpoint: ParserCheckpoint,
}
