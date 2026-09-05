//! Storage and operating-system adapters.
pub mod memory;
pub use memory::*;
#[cfg(feature = "replication")]
mod memory_replication;
#[cfg(feature = "retention")]
mod memory_retention;
#[cfg(feature = "source-journal")]
mod memory_source_journal;
#[cfg(feature = "sqlite")]
mod sqlite;
#[cfg(feature = "sqlite")]
pub use sqlite::*;
#[cfg(all(feature = "sqlite", feature = "replication"))]
mod sqlite_replication;
#[cfg(feature = "sqlite")]
mod sqlite_restore;
#[cfg(all(feature = "sqlite", feature = "retention"))]
mod sqlite_retention;
#[cfg(all(feature = "sqlite", feature = "snapshots"))]
mod sqlite_snapshot;
#[cfg(all(feature = "sqlite", feature = "source-journal"))]
mod sqlite_source_journal;
#[cfg(feature = "sqlite")]
pub use sqlite_restore::*;
#[cfg(all(feature = "sqlite", feature = "test-support"))]
mod sqlite_test_vfs;
#[cfg(all(feature = "sqlite", feature = "test-support"))]
pub use sqlite_test_vfs::*;

pub fn new_incarnation() -> crate::domain::IncarnationId {
    crate::domain::IncarnationId(*uuid::Uuid::new_v4().as_bytes())
}
