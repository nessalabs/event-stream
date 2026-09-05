//! Bounded ordered event history with injectable storage and optional decoding.
pub mod application;
pub mod domain;
pub mod infrastructure;
#[cfg(feature = "codec")]
pub mod ingestion;
pub use application::*;
pub use domain::*;
