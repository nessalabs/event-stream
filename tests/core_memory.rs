mod common;
use event_stream::infrastructure::{MemoryStore, MemoryStoreOptions};
use event_stream::*;

#[cfg(feature = "snapshots")]
#[derive(Debug)]
struct ManualSnapshotClock(std::sync::atomic::AtomicU64);

#[cfg(feature = "snapshots")]
impl MonotonicClock for ManualSnapshotClock {
    fn now(&self) -> MonotonicTick {
        MonotonicTick(self.0.load(std::sync::atomic::Ordering::Acquire))
    }
}

#[tokio::test]
async fn memory_store_passes_shared_contract() {
    common::event_store_contract!(MemoryStore::open(MemoryStoreOptions::default()));
}

#[tokio::test]
async fn memory_store_passes_lifecycle_contract() {
    common::lifecycle_store_contract!(MemoryStore::open(MemoryStoreOptions::default()));
}

#[cfg(feature = "snapshots")]
#[tokio::test]
async fn memory_store_passes_snapshot_contract() {
    common::snapshot_store_contract!(MemoryStore::open(MemoryStoreOptions::default()));
}

#[cfg(feature = "snapshots")]
#[tokio::test]
async fn tiny_snapshot_chunks_hit_metadata_quota_before_payload_quota() {
    use sha2::{Digest, Sha256};

    let mut options = MemoryStoreOptions::default();
    options.snapshots.storage.max_chunk_bytes = 1;
    options.snapshots.storage.max_chunks = 2;
    options.snapshots.storage.max_chunk_metadata_bytes = 2 * SNAPSHOT_CHUNK_ENVELOPE_BYTES;
    let store = MemoryStore::open(options).await.unwrap();
    let stream = store
        .create_if_absent(&StreamId::new("tiny-chunks").unwrap())
        .await
        .unwrap();
    let id = SnapshotId::from_bytes([4; 16]);
    store
        .begin_snapshot(SnapshotDescriptor {
            id,
            covered: Cursor::new(stream, 0),
            schema: SchemaRef {
                id: SchemaId::new("tiny.state").unwrap(),
                version: 1,
            },
            content_bytes: 3,
            digest: SnapshotDigest::from_bytes(Sha256::digest(b"abc").into()),
        })
        .await
        .unwrap();
    for (offset, byte) in b"ab".iter().copied().enumerate() {
        store
            .put_snapshot_chunk(
                id,
                SnapshotChunk {
                    offset: offset as u64,
                    bytes: Payload::copy_from_slice(&[byte]),
                },
            )
            .await
            .unwrap();
    }
    assert_eq!(
        store
            .put_snapshot_chunk(
                id,
                SnapshotChunk {
                    offset: 2,
                    bytes: Payload::copy_from_slice(b"c"),
                },
            )
            .await
            .unwrap_err(),
        SnapshotError::CapacityExceeded
    );
    assert_eq!(store.snapshot_status(id).await.unwrap().accepted_bytes, 2);
}

#[cfg(feature = "snapshots")]
#[tokio::test]
async fn recovery_lease_protects_only_the_required_retired_suffix_until_expiry() {
    use sha2::{Digest, Sha256};
    use std::sync::{atomic::Ordering, Arc};

    let clock = Arc::new(ManualSnapshotClock(std::sync::atomic::AtomicU64::new(100)));
    let options = MemoryStoreOptions {
        snapshot_clock: clock.clone(),
        ..MemoryStoreOptions::default()
    };
    let store = MemoryStore::open(options).await.unwrap();
    let stream = store
        .create_if_absent(&StreamId::new("protected-recovery").unwrap())
        .await
        .unwrap();
    for offset in 1..=3 {
        store
            .append_atomic(
                &stream,
                common::event(&format!("protected-{offset}"), &[offset]),
            )
            .await
            .unwrap();
    }
    let id = SnapshotId::from_bytes([8; 16]);
    store
        .begin_snapshot(SnapshotDescriptor {
            id,
            covered: Cursor::new(stream.clone(), 1),
            schema: SchemaRef {
                id: SchemaId::new("protected.state").unwrap(),
                version: 1,
            },
            content_bytes: 0,
            digest: SnapshotDigest::from_bytes(Sha256::digest([]).into()),
        })
        .await
        .unwrap();
    store
        .verify_snapshot_step(
            id,
            VerificationLimits {
                max_chunks: 1,
                max_bytes: 1,
            },
        )
        .await
        .unwrap();
    store.publish_snapshot(id).await.unwrap();
    let plan = store
        .acquire_recovery(id, std::time::Duration::from_nanos(10))
        .await
        .unwrap();
    store
        .change_lifecycle(LifecycleRequest {
            operation_id: LifecycleOperationId::new("protected-reset").unwrap(),
            expected: stream.clone(),
            action: LifecycleAction::Reset,
        })
        .await
        .unwrap();

    let first = store
        .cleanup_retired(CleanupLimits {
            max_records: 8,
            max_bytes: 2 * 1024 * 1024,
        })
        .await
        .unwrap();
    assert_eq!(first.removed_records, 1);
    assert!(first.remaining);
    let suffix = store
        .read_recovery_page(
            plan.lease,
            1,
            PageLimits {
                max_records: 8,
                max_bytes: 4096,
            },
        )
        .await
        .unwrap();
    assert_eq!(
        suffix
            .records
            .iter()
            .map(|record| record.cursor.offset)
            .collect::<Vec<_>>(),
        vec![2, 3]
    );

    clock.0.store(110, Ordering::Release);
    assert_eq!(
        store
            .read_recovery_page(
                plan.lease,
                1,
                PageLimits {
                    max_records: 8,
                    max_bytes: 4096,
                },
            )
            .await
            .unwrap_err(),
        SnapshotError::ExpiredProtection { lease: plan.lease }
    );
    let final_cleanup = store
        .cleanup_retired(CleanupLimits {
            max_records: 8,
            max_bytes: 2 * 1024 * 1024,
        })
        .await
        .unwrap();
    assert_eq!(final_cleanup.removed_records, 2);
    assert!(!final_cleanup.remaining);
}

#[cfg(feature = "snapshots")]
#[tokio::test]
async fn abort_retains_identity_while_cleanup_releases_chunks_in_bounded_turns() {
    use sha2::{Digest, Sha256};

    let mut options = MemoryStoreOptions::default();
    options.snapshots.storage.max_chunk_bytes = 1;
    options.snapshots.cleanup = SnapshotCleanupLimits {
        max_rows: 1,
        max_bytes: 512,
    };
    let store = MemoryStore::open(options).await.unwrap();
    let stream = store
        .create_if_absent(&StreamId::new("bounded-abort").unwrap())
        .await
        .unwrap();
    let id = SnapshotId::from_bytes([10; 16]);
    let descriptor = SnapshotDescriptor {
        id,
        covered: Cursor::new(stream, 0),
        schema: SchemaRef {
            id: SchemaId::new("bounded.state").unwrap(),
            version: 1,
        },
        content_bytes: 3,
        digest: SnapshotDigest::from_bytes(Sha256::digest(b"abc").into()),
    };
    store.begin_snapshot(descriptor.clone()).await.unwrap();
    let active = SnapshotDescriptor {
        id: SnapshotId::from_bytes([9; 16]),
        covered: descriptor.covered.clone(),
        schema: descriptor.schema.clone(),
        content_bytes: 0,
        digest: SnapshotDigest::from_bytes(Sha256::digest([]).into()),
    };
    store.begin_snapshot(active.clone()).await.unwrap();
    for (offset, byte) in b"abc".iter().copied().enumerate() {
        store
            .put_snapshot_chunk(
                id,
                SnapshotChunk {
                    offset: offset as u64,
                    bytes: Payload::copy_from_slice(&[byte]),
                },
            )
            .await
            .unwrap();
    }
    assert!(!store.abort_snapshot(id).await.unwrap().already_aborted);
    assert_eq!(
        store.begin_snapshot(descriptor).await.unwrap().state,
        SnapshotUploadState::Aborted
    );
    let limits = SnapshotCleanupLimits {
        max_rows: 1,
        max_bytes: 512,
    };
    let uploads = store
        .list_snapshot_uploads(
            None,
            PageLimits {
                max_records: 8,
                max_bytes: 4096,
            },
        )
        .await
        .unwrap();
    assert_eq!(
        uploads
            .entries
            .iter()
            .map(|entry| (entry.descriptor.id, entry.state))
            .collect::<Vec<_>>(),
        vec![
            (active.id, SnapshotUploadState::Uploading),
            (id, SnapshotUploadState::Aborted)
        ]
    );
    for _ in 0..3 {
        let progress = store.cleanup_snapshot_staging(limits).await.unwrap();
        assert_eq!(progress.removed_chunks, 1);
        assert_eq!(progress.removed_snapshots, 0);
        assert!(progress.remaining);
        assert_eq!(
            store.snapshot_status(id).await.unwrap().state,
            SnapshotUploadState::Aborted
        );
    }
    let final_turn = store.cleanup_snapshot_staging(limits).await.unwrap();
    assert_eq!(final_turn.removed_chunks, 0);
    assert_eq!(final_turn.removed_snapshots, 1);
    assert!(!final_turn.remaining);
    assert_eq!(
        store.snapshot_status(id).await.unwrap().state,
        SnapshotUploadState::Aborted
    );
    let remaining_uploads = store
        .list_snapshot_uploads(
            None,
            PageLimits {
                max_records: 8,
                max_bytes: 4096,
            },
        )
        .await
        .unwrap();
    assert_eq!(remaining_uploads.entries.len(), 1);
    assert_eq!(remaining_uploads.entries[0].descriptor, active);
}

#[cfg(feature = "snapshots")]
#[tokio::test]
async fn descriptor_metadata_quota_is_aggregate_and_retry_safe() {
    use sha2::{Digest, Sha256};

    let mut options = MemoryStoreOptions::default();
    options.snapshots.storage.max_descriptor_metadata_bytes = 430;
    let store = MemoryStore::open(options).await.unwrap();
    let stream = store
        .create_if_absent(&StreamId::new("descriptor-quota").unwrap())
        .await
        .unwrap();
    let descriptor = SnapshotDescriptor {
        id: SnapshotId::from_bytes([11; 16]),
        covered: Cursor::new(stream, 0),
        schema: SchemaRef {
            id: SchemaId::new("descriptor.state").unwrap(),
            version: 1,
        },
        content_bytes: 0,
        digest: SnapshotDigest::from_bytes(Sha256::digest([]).into()),
    };
    let first = store.begin_snapshot(descriptor.clone()).await.unwrap();
    assert_eq!(
        store.begin_snapshot(descriptor.clone()).await.unwrap(),
        first
    );
    let mut second = descriptor;
    second.id = SnapshotId::from_bytes([12; 16]);
    assert_eq!(
        store.begin_snapshot(second).await.unwrap_err(),
        SnapshotError::CapacityExceeded
    );
}

#[cfg(feature = "snapshots")]
#[tokio::test]
async fn close_rejects_every_recovery_lease_operation() {
    use sha2::{Digest, Sha256};

    let store = MemoryStore::open(MemoryStoreOptions::default())
        .await
        .unwrap();
    let stream = store
        .create_if_absent(&StreamId::new("closed-recovery").unwrap())
        .await
        .unwrap();
    let id = SnapshotId::from_bytes([13; 16]);
    store
        .begin_snapshot(SnapshotDescriptor {
            id,
            covered: Cursor::new(stream, 0),
            schema: SchemaRef {
                id: SchemaId::new("closed.state").unwrap(),
                version: 1,
            },
            content_bytes: 0,
            digest: SnapshotDigest::from_bytes(Sha256::digest([]).into()),
        })
        .await
        .unwrap();
    store
        .verify_snapshot_step(
            id,
            VerificationLimits {
                max_chunks: 1,
                max_bytes: 1,
            },
        )
        .await
        .unwrap();
    store.publish_snapshot(id).await.unwrap();
    let plan = store
        .acquire_recovery(id, std::time::Duration::from_secs(1))
        .await
        .unwrap();
    store.close().await.unwrap();
    assert_eq!(
        store
            .read_snapshot_chunk(plan.lease, 0, 1)
            .await
            .unwrap_err(),
        SnapshotError::Closed
    );
    assert_eq!(
        store
            .read_recovery_page(
                plan.lease,
                0,
                PageLimits {
                    max_records: 1,
                    max_bytes: 1,
                },
            )
            .await
            .unwrap_err(),
        SnapshotError::Closed
    );
    assert_eq!(
        store.release_recovery(plan.lease).await.unwrap_err(),
        SnapshotError::Closed
    );
    assert_eq!(
        store
            .acquire_recovery(id, std::time::Duration::from_secs(1))
            .await
            .unwrap_err(),
        SnapshotError::Closed
    );
}

#[cfg(feature = "snapshots")]
#[tokio::test]
async fn generated_snapshot_schedule_preserves_terminal_states_and_order() {
    use sha2::{Digest, Sha256};

    let store = MemoryStore::open(MemoryStoreOptions::default())
        .await
        .unwrap();
    let stream = store
        .create_if_absent(&StreamId::new("generated-snapshots").unwrap())
        .await
        .unwrap();
    let mut expected_published = Vec::new();
    for index in 0..32u8 {
        let content = vec![index; usize::from(index % 7) + 1];
        let id = SnapshotId::from_bytes([index.saturating_add(32); 16]);
        let descriptor = SnapshotDescriptor {
            id,
            covered: Cursor::new(stream.clone(), 0),
            schema: SchemaRef {
                id: SchemaId::new("generated.state").unwrap(),
                version: 1,
            },
            content_bytes: content.len() as u64,
            digest: SnapshotDigest::from_bytes(Sha256::digest(&content).into()),
        };
        store.begin_snapshot(descriptor.clone()).await.unwrap();
        let width = usize::from(index % 3) + 1;
        let mut offset = 0usize;
        for bytes in content.chunks(width) {
            let chunk = SnapshotChunk {
                offset: offset as u64,
                bytes: Payload::copy_from_slice(bytes),
            };
            store.put_snapshot_chunk(id, chunk.clone()).await.unwrap();
            if offset == 0 && index % 2 == 0 {
                store.put_snapshot_chunk(id, chunk).await.unwrap();
            }
            offset += bytes.len();
        }
        if index % 4 == 0 {
            store.abort_snapshot(id).await.unwrap();
            while store
                .cleanup_snapshot_staging(SnapshotCleanupLimits {
                    max_rows: 2,
                    max_bytes: 2 * 1024 * 1024,
                })
                .await
                .unwrap()
                .remaining
            {}
            assert_eq!(
                store.snapshot_status(id).await.unwrap().state,
                SnapshotUploadState::Aborted
            );
        } else {
            loop {
                let progress = store
                    .verify_snapshot_step(
                        id,
                        VerificationLimits {
                            max_chunks: usize::from(index % 3) + 1,
                            max_bytes: usize::from(index % 5) + 1,
                        },
                    )
                    .await
                    .unwrap();
                if progress.state == SnapshotUploadState::Verified {
                    break;
                }
            }
            store.publish_snapshot(id).await.unwrap();
            expected_published.push(id);
        }
    }

    let mut observed = Vec::new();
    let mut after = None;
    loop {
        let page = store
            .list_snapshots(
                &stream,
                after,
                PageLimits {
                    max_records: 5,
                    max_bytes: 4096,
                },
            )
            .await
            .unwrap();
        observed.extend(page.entries.iter().map(|descriptor| descriptor.id));
        if page.complete {
            break;
        }
        after = page.next_after;
    }
    assert_eq!(observed, expected_published);
}

#[cfg(feature = "snapshots")]
#[tokio::test]
async fn generated_recovery_schedule_preserves_content_suffix_and_expiry() {
    use sha2::{Digest, Sha256};
    use std::sync::{atomic::Ordering, Arc};

    let clock = Arc::new(ManualSnapshotClock(std::sync::atomic::AtomicU64::new(100)));
    let store = MemoryStore::open(MemoryStoreOptions {
        snapshot_clock: clock.clone(),
        ..MemoryStoreOptions::default()
    })
    .await
    .unwrap();
    let stream = store
        .create_if_absent(&StreamId::new("generated-recovery").unwrap())
        .await
        .unwrap();
    for offset in 1..=12 {
        store
            .append_atomic(
                &stream,
                common::event(&format!("recovery-{offset}"), &[offset]),
            )
            .await
            .unwrap();
    }

    for (turn, covered) in [0u64, 3, 6, 9].into_iter().enumerate() {
        let content = format!("state-through-{covered}").into_bytes();
        let id = SnapshotId::from_bytes([80 + turn as u8; 16]);
        store
            .begin_snapshot(SnapshotDescriptor {
                id,
                covered: Cursor::new(stream.clone(), covered),
                schema: SchemaRef {
                    id: SchemaId::new("generated.recovery.state").unwrap(),
                    version: 1,
                },
                content_bytes: content.len() as u64,
                digest: SnapshotDigest::from_bytes(Sha256::digest(&content).into()),
            })
            .await
            .unwrap();
        for (index, bytes) in content.chunks(3).enumerate() {
            store
                .put_snapshot_chunk(
                    id,
                    SnapshotChunk {
                        offset: (index * 3) as u64,
                        bytes: Payload::copy_from_slice(bytes),
                    },
                )
                .await
                .unwrap();
        }
        loop {
            let progress = store
                .verify_snapshot_step(
                    id,
                    VerificationLimits {
                        max_chunks: 2,
                        max_bytes: 4,
                    },
                )
                .await
                .unwrap();
            if progress.state == SnapshotUploadState::Verified {
                break;
            }
        }
        store.publish_snapshot(id).await.unwrap();
        let plan = store
            .acquire_recovery(id, std::time::Duration::from_nanos(10))
            .await
            .unwrap();
        assert_eq!(plan.snapshot.covered.offset, covered);
        assert_eq!(plan.through.offset, 12);

        if turn % 2 == 1 {
            clock.0.fetch_add(10, Ordering::AcqRel);
            assert_eq!(
                store
                    .read_snapshot_chunk(plan.lease, 0, 5)
                    .await
                    .unwrap_err(),
                SnapshotError::ExpiredProtection { lease: plan.lease }
            );
        } else {
            let mut restored = Vec::new();
            let mut offset = 0u64;
            loop {
                let page = store
                    .read_snapshot_chunk(plan.lease, offset, 5)
                    .await
                    .unwrap();
                restored.extend_from_slice(page.bytes.as_bytes());
                offset = page.next_offset;
                if page.complete {
                    break;
                }
            }
            assert_eq!(restored, content);
            let suffix = store
                .read_recovery_page(
                    plan.lease,
                    covered,
                    PageLimits {
                        max_records: 32,
                        max_bytes: 4096,
                    },
                )
                .await
                .unwrap();
            assert_eq!(
                suffix
                    .records
                    .iter()
                    .map(|record| record.cursor.offset)
                    .collect::<Vec<_>>(),
                ((covered + 1)..=12).collect::<Vec<_>>()
            );
            assert_eq!(
                store.release_recovery(plan.lease).await.unwrap(),
                RecoveryRelease::Released
            );
        }
        clock.0.fetch_add(20, Ordering::AcqRel);
    }
}

#[tokio::test]
async fn quota_rejects_new_history_but_allows_retry() {
    let store = MemoryStore::open(MemoryStoreOptions {
        max_record_bytes: 1024,
        max_history_records: 1,
        max_history_bytes: 2048,
        ..MemoryStoreOptions::default()
    })
    .await
    .unwrap();
    let key = store
        .create_if_absent(&StreamId::new("quota").unwrap())
        .await
        .unwrap();
    let original = common::event("one", b"one");
    store.append_atomic(&key, original.clone()).await.unwrap();
    assert!(matches!(
        store
            .append_atomic(&key, common::event("two", b"two"))
            .await,
        Err(Error::CapacityExceeded)
    ));
    assert_eq!(
        store.append_atomic(&key, original).await.unwrap().kind,
        AppendKind::Deduplicated
    );
}

#[tokio::test]
async fn stream_metadata_has_finite_count_and_byte_quotas() {
    let store = MemoryStore::open(MemoryStoreOptions {
        max_streams: 1,
        max_stream_metadata_bytes: 300,
        ..MemoryStoreOptions::default()
    })
    .await
    .unwrap();
    let first_id = StreamId::new("one").unwrap();
    let first = store.create_if_absent(&first_id).await.unwrap();
    assert_eq!(store.create_if_absent(&first_id).await.unwrap(), first);
    assert!(matches!(
        store.create_if_absent(&StreamId::new("two").unwrap()).await,
        Err(Error::CapacityExceeded)
    ));

    let byte_limited = MemoryStore::open(MemoryStoreOptions {
        max_streams: 10,
        max_stream_metadata_bytes: 260,
        ..MemoryStoreOptions::default()
    })
    .await
    .unwrap();
    byte_limited
        .create_if_absent(&StreamId::new("a").unwrap())
        .await
        .unwrap();
    assert!(matches!(
        byte_limited
            .create_if_absent(&StreamId::new("bbbb").unwrap())
            .await,
        Err(Error::CapacityExceeded)
    ));
}

#[tokio::test]
async fn lifecycle_receipt_and_retired_metadata_quotas_are_finite_and_retry_safe() {
    let receipt_limited = MemoryStore::open(MemoryStoreOptions {
        max_lifecycle_receipts: 1,
        max_lifecycle_receipt_bytes: 1024,
        ..MemoryStoreOptions::default()
    })
    .await
    .unwrap();
    let first = receipt_limited
        .create_if_absent(&StreamId::new("receipt-quota").unwrap())
        .await
        .unwrap();
    let request = LifecycleRequest {
        operation_id: LifecycleOperationId::new("receipt-one").unwrap(),
        expected: first,
        action: LifecycleAction::Reset,
    };
    let receipt = receipt_limited
        .change_lifecycle(request.clone())
        .await
        .unwrap();
    assert_eq!(
        receipt_limited.change_lifecycle(request).await.unwrap(),
        receipt
    );
    assert!(matches!(
        receipt_limited
            .change_lifecycle(LifecycleRequest {
                operation_id: LifecycleOperationId::new("receipt-two").unwrap(),
                expected: receipt.replacement.unwrap(),
                action: LifecycleAction::Delete,
            })
            .await,
        Err(Error::CapacityExceeded)
    ));

    let retired_limited = MemoryStore::open(MemoryStoreOptions {
        max_retired_lifetimes: 1,
        max_retired_metadata_bytes: 1024,
        ..MemoryStoreOptions::default()
    })
    .await
    .unwrap();
    let a = retired_limited
        .create_if_absent(&StreamId::new("retired-quota").unwrap())
        .await
        .unwrap();
    let b = retired_limited
        .change_lifecycle(LifecycleRequest {
            operation_id: LifecycleOperationId::new("retire-a").unwrap(),
            expected: a,
            action: LifecycleAction::Reset,
        })
        .await
        .unwrap()
        .replacement
        .unwrap();
    let retire_b = LifecycleRequest {
        operation_id: LifecycleOperationId::new("retire-b").unwrap(),
        expected: b,
        action: LifecycleAction::Reset,
    };
    assert!(matches!(
        retired_limited.change_lifecycle(retire_b.clone()).await,
        Err(Error::CapacityExceeded)
    ));
    let cleanup = retired_limited
        .cleanup_retired(CleanupLimits {
            max_records: 1,
            max_bytes: 2 * 1024 * 1024,
        })
        .await
        .unwrap();
    assert_eq!(cleanup.removed_records, 0);
    assert!(!cleanup.remaining);
    assert!(retired_limited
        .change_lifecycle(retire_b)
        .await
        .unwrap()
        .replacement
        .is_some());
}

#[tokio::test]
async fn generated_lifecycle_sequence_matches_the_name_state_model() {
    let store = MemoryStore::open(MemoryStoreOptions::default())
        .await
        .unwrap();
    let id = StreamId::new("lifecycle-model").unwrap();
    let original = store.create_if_absent(&id).await.unwrap();
    let mut model = StreamAvailability::Active(original.clone());
    let mut committed: Vec<(LifecycleRequest, LifecycleReceipt)> = Vec::new();
    let mut seed = 0x4d59_5df4_d0f3_3173u64;
    for step in 0..128 {
        seed = seed.wrapping_mul(6_364_136_223_846_793_005).wrapping_add(1);
        if step % 5 == 4 && !committed.is_empty() {
            let index = (seed as usize) % committed.len();
            let (request, expected) = &committed[index];
            assert_eq!(
                store.change_lifecycle(request.clone()).await.unwrap(),
                *expected
            );
            continue;
        }
        let current = match &model {
            StreamAvailability::Active(key) | StreamAvailability::Unavailable(key) => key,
        };
        let expected = if step % 7 == 6 {
            original.clone()
        } else {
            current.clone()
        };
        let action = if seed & 1 == 0 {
            LifecycleAction::Reset
        } else {
            LifecycleAction::Delete
        };
        let request = LifecycleRequest {
            operation_id: LifecycleOperationId::new(format!("model-{step}")).unwrap(),
            expected,
            action,
        };
        if request.expected != *current {
            assert!(matches!(
                store.change_lifecycle(request).await,
                Err(Error::StaleIncarnation { .. })
            ));
            continue;
        }
        let receipt = store.change_lifecycle(request.clone()).await.unwrap();
        model = match &receipt.replacement {
            Some(replacement) => StreamAvailability::Active(replacement.clone()),
            None => StreamAvailability::Unavailable(request.expected.clone()),
        };
        committed.push((request, receipt));
        match &model {
            StreamAvailability::Active(key) => {
                assert_eq!(store.create_if_absent(&id).await.unwrap(), *key)
            }
            StreamAvailability::Unavailable(last) => assert!(matches!(
                store.create_if_absent(&id).await,
                Err(Error::StreamUnavailable { last: actual }) if *actual == *last
            )),
        }
    }
}
