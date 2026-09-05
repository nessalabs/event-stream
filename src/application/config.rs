//! Bounded runtime policy grouped by the resource or operation it controls.

#[cfg(feature = "source-journal")]
use super::JournalAdmissionConfig;
use super::PersistenceProfile;
#[cfg(feature = "retention")]
use super::RetentionAdmissionConfig;
#[cfg(feature = "replication")]
use super::RuntimeReplicationConfig;
#[cfg(feature = "snapshots")]
use super::SnapshotAdmissionConfig;
use crate::domain::{CleanupLimits, PageLimits};
use std::time::Duration;

#[derive(Clone, Debug, Default)]
pub struct RuntimeConfig {
    pub events: EventConfig,
    pub appends: AppendConfig,
    pub reads: ReadConfig,
    pub subscriptions: SubscriptionConfig,
    pub scheduling: SchedulingConfig,
    pub maintenance: MaintenanceConfig,
    #[cfg(feature = "snapshots")]
    pub snapshots: SnapshotAdmissionConfig,
    #[cfg(feature = "retention")]
    pub retention: RetentionAdmissionConfig,
    #[cfg(feature = "source-journal")]
    pub journal: JournalAdmissionConfig,
    #[cfg(feature = "replication")]
    pub replication: RuntimeReplicationConfig,
    pub diagnostics: DiagnosticsConfig,
}

#[derive(Clone, Debug)]
pub struct EventConfig {
    pub max_bytes: usize,
    pub minimum_persistence: PersistenceProfile,
}

#[derive(Clone, Debug)]
pub struct AppendConfig {
    pub max_queued: usize,
    pub max_queued_bytes: usize,
    pub max_queued_per_stream: usize,
    pub max_queued_bytes_per_stream: usize,
    pub max_waiters: usize,
    pub max_waiter_bytes: usize,
    pub admission_timeout: Duration,
}

#[derive(Clone, Debug)]
pub struct ReadConfig {
    pub max_concurrent: usize,
    pub max_waiters: usize,
    pub admission_timeout: Duration,
    pub page: PageLimits,
    pub max_buffered_page_bytes: usize,
}

#[derive(Clone, Debug)]
pub struct SubscriptionConfig {
    pub max_total: usize,
    pub max_per_stream: usize,
    pub sweep_interval: Duration,
    pub checks_per_sweep: usize,
}

#[derive(Clone, Debug)]
pub struct SchedulingConfig {
    pub max_coordinators: usize,
    pub storage_workers: usize,
    pub max_appends_per_stream_turn: usize,
}

#[derive(Clone, Debug)]
pub struct DiagnosticsConfig {
    pub capacity: usize,
}

#[derive(Clone, Debug)]
pub struct MaintenanceConfig {
    pub max_operations: usize,
    pub max_operation_bytes: usize,
    pub max_cleanup_operations: usize,
    pub cleanup: CleanupLimits,
}

impl Default for EventConfig {
    fn default() -> Self {
        Self {
            max_bytes: 1024 * 1024,
            minimum_persistence: PersistenceProfile::Ephemeral,
        }
    }
}

impl Default for AppendConfig {
    fn default() -> Self {
        Self {
            max_queued: 1024,
            max_queued_bytes: 16 * 1024 * 1024,
            max_queued_per_stream: 128,
            max_queued_bytes_per_stream: 4 * 1024 * 1024,
            max_waiters: 1024,
            max_waiter_bytes: 16 * 1024 * 1024,
            admission_timeout: Duration::from_secs(5),
        }
    }
}

impl Default for ReadConfig {
    fn default() -> Self {
        Self {
            max_concurrent: 1,
            max_waiters: 1024,
            admission_timeout: Duration::from_secs(5),
            page: PageLimits {
                max_records: 256,
                max_bytes: 1024 * 1024,
            },
            max_buffered_page_bytes: 16 * 1024 * 1024,
        }
    }
}

impl Default for SubscriptionConfig {
    fn default() -> Self {
        Self {
            max_total: 1024,
            max_per_stream: 128,
            sweep_interval: Duration::from_millis(250),
            checks_per_sweep: 64,
        }
    }
}

impl Default for SchedulingConfig {
    fn default() -> Self {
        Self {
            max_coordinators: 1024,
            storage_workers: 1,
            max_appends_per_stream_turn: 1,
        }
    }
}

impl Default for DiagnosticsConfig {
    fn default() -> Self {
        Self { capacity: 256 }
    }
}

impl Default for MaintenanceConfig {
    fn default() -> Self {
        Self {
            max_operations: 8,
            max_operation_bytes: 64 * 1024,
            max_cleanup_operations: 1,
            cleanup: CleanupLimits {
                max_records: 256,
                max_bytes: 2 * 1024 * 1024,
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn grouped_defaults_preserve_the_finite_runtime_policy() {
        let config = RuntimeConfig::default();
        assert_eq!(config.events.max_bytes, 1024 * 1024);
        assert_eq!(
            config.events.minimum_persistence,
            PersistenceProfile::Ephemeral
        );
        assert_eq!(config.appends.max_queued, 1024);
        assert_eq!(config.appends.max_queued_bytes, 16 * 1024 * 1024);
        assert_eq!(config.appends.max_queued_per_stream, 128);
        assert_eq!(config.appends.max_queued_bytes_per_stream, 4 * 1024 * 1024);
        assert_eq!(config.appends.max_waiters, 1024);
        assert_eq!(config.appends.max_waiter_bytes, 16 * 1024 * 1024);
        assert_eq!(config.appends.admission_timeout, Duration::from_secs(5));
        assert_eq!(config.reads.max_concurrent, 1);
        assert_eq!(config.reads.max_waiters, 1024);
        assert_eq!(config.reads.admission_timeout, Duration::from_secs(5));
        assert_eq!(config.reads.page.max_records, 256);
        assert_eq!(config.reads.page.max_bytes, 1024 * 1024);
        assert_eq!(config.reads.max_buffered_page_bytes, 16 * 1024 * 1024);
        assert_eq!(config.subscriptions.max_total, 1024);
        assert_eq!(config.subscriptions.max_per_stream, 128);
        assert_eq!(
            config.subscriptions.sweep_interval,
            Duration::from_millis(250)
        );
        assert_eq!(config.subscriptions.checks_per_sweep, 64);
        assert_eq!(config.scheduling.max_coordinators, 1024);
        assert_eq!(config.scheduling.storage_workers, 1);
        assert_eq!(config.scheduling.max_appends_per_stream_turn, 1);
        assert_eq!(config.maintenance.max_operations, 8);
        assert_eq!(config.maintenance.max_operation_bytes, 64 * 1024);
        assert_eq!(config.maintenance.max_cleanup_operations, 1);
        assert_eq!(config.maintenance.cleanup.max_records, 256);
        assert_eq!(config.maintenance.cleanup.max_bytes, 2 * 1024 * 1024);
        #[cfg(feature = "snapshots")]
        config.snapshots.validate().unwrap();
        #[cfg(feature = "retention")]
        config.retention.validate().unwrap();
        #[cfg(feature = "replication")]
        {
            assert_eq!(config.replication.max_concurrent, 4);
            assert_eq!(config.replication.max_in_flight_bytes, 8 * 1024 * 1024);
        }
        assert_eq!(config.diagnostics.capacity, 256);
    }
}
