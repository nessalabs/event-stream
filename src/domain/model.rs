use std::sync::Arc;

#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub struct DecodedPosition {
    pub source_byte: u64,
    pub item_index: u64,
}

pub const CURSOR_VERSION: u8 = 1;
pub const MAX_IDENTIFIER_BYTES: usize = 256;

macro_rules! identifier {
    ($name:ident) => {
        #[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
        pub struct $name(Box<str>);
        impl $name {
            pub fn new(value: impl AsRef<str>) -> std::result::Result<Self, ValidationError> {
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
identifier!(StreamId);
identifier!(EventId);
identifier!(SchemaId);

#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct IncarnationId(pub [u8; 16]);
#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct StreamKey {
    pub id: StreamId,
    pub incarnation: IncarnationId,
}
#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct Cursor {
    pub version: u8,
    pub stream: StreamKey,
    pub offset: u64,
}
impl Cursor {
    pub fn new(stream: StreamKey, offset: u64) -> Self {
        Self {
            version: CURSOR_VERSION,
            stream,
            offset,
        }
    }
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SchemaRef {
    pub id: SchemaId,
    pub version: u32,
}
/// Exact-size immutable allocation: a short payload cannot retain a larger sliced buffer.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Payload(Arc<[u8]>);
impl Payload {
    pub fn copy_from_slice(bytes: &[u8]) -> Self {
        Self(Arc::from(bytes))
    }
    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }
    pub fn len(&self) -> usize {
        self.0.len()
    }
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NewEvent {
    pub id: EventId,
    pub schema: SchemaRef,
    pub payload: Payload,
}
impl NewEvent {
    /// Logical envelope accounting, separate from allocator/adapter overhead measurements.
    pub fn accounted_bytes(&self) -> usize {
        self.payload
            .len()
            .saturating_add(self.id.as_str().len())
            .saturating_add(self.schema.id.as_str().len())
            .saturating_add(128)
    }
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Record {
    pub cursor: Cursor,
    pub event: NewEvent,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AppendKind {
    Inserted,
    Deduplicated,
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AppendReceipt {
    pub record: Arc<Record>,
    pub kind: AppendKind,
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Bounds {
    pub floor: Cursor,
    pub tail: Cursor,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PageLimits {
    pub max_records: usize,
    pub max_bytes: usize,
}
#[derive(Clone, Debug)]
pub struct Page {
    pub records: Vec<Arc<Record>>,
    pub next_after: Cursor,
    pub through: Cursor,
    pub complete: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ValidationError {
    InvalidIdentifier,
}
impl std::fmt::Display for ValidationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self:?}")
    }
}
impl std::error::Error for ValidationError {}
