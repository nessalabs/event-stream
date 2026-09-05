#![cfg(feature = "replication")]

use event_stream::infrastructure::{MemoryStore, MemoryStoreOptions};
use event_stream::*;
use sha2::{Digest, Sha256};
use std::sync::Arc;

#[tokio::test]
async fn bootstrap_verification_makes_bounded_progress_across_multiple_calls() {
    let store = MemoryStore::open(MemoryStoreOptions::default())
        .await
        .unwrap();
    let epoch = ReplicaBatchDestinationStore::destination_epoch(&store)
        .await
        .unwrap();
    let stream = OriginStream {
        origin: OriginId([1; 16]),
        stream: StreamKey {
            id: StreamId::new("bootstrap").unwrap(),
            incarnation: IncarnationId([2; 16]),
        },
    };
    let content = b"abcdefghijkl";
    let bootstrap = ReplicaBootstrap {
        operation_id: ReplicationOperationId::new("begin").unwrap(),
        id: BootstrapId([3; 16]),
        replica: ReplicaId::new("remote").unwrap(),
        destination_epoch: epoch,
        stream: stream.clone(),
        snapshot: SnapshotDescriptor {
            id: SnapshotId([4; 16]),
            covered: Cursor::new(stream.stream.clone(), 2),
            schema: SchemaRef {
                id: SchemaId::new("state").unwrap(),
                version: 1,
            },
            content_bytes: content.len() as u64,
            digest: SnapshotDigest(Sha256::digest(content).into()),
        },
        through: ReplicaPosition {
            stream: stream.clone(),
            offset: 5,
        },
    };
    store
        .begin_replica_bootstrap(bootstrap.clone())
        .await
        .unwrap();
    for (index, bytes) in content.chunks(4).enumerate() {
        store
            .put_replica_bootstrap_chunk(ReplicaBootstrapChunk {
                id: bootstrap.id,
                chunk: SnapshotChunk {
                    offset: (index * 4) as u64,
                    bytes: Payload::copy_from_slice(bytes),
                },
            })
            .await
            .unwrap();
    }
    let records = (3..=5)
        .map(|offset| {
            Arc::new(Record {
                cursor: Cursor::new(stream.stream.clone(), offset),
                event: NewEvent {
                    id: EventId::new(format!("event-{offset}")).unwrap(),
                    schema: SchemaRef {
                        id: SchemaId::new("bytes").unwrap(),
                        version: 1,
                    },
                    payload: Payload::copy_from_slice(&[offset as u8]),
                },
            })
        })
        .collect();
    store
        .put_replica_bootstrap_batch(ReplicaBootstrapBatch {
            id: bootstrap.id,
            batch: ReplicaBatch {
                id: BatchId([5; 16]),
                destination_epoch: epoch,
                after: ReplicaPosition {
                    stream: stream.clone(),
                    offset: 2,
                },
                records,
            },
        })
        .await
        .unwrap();
    let publish = PublishReplicaBootstrap {
        operation_id: ReplicationOperationId::new("publish").unwrap(),
        id: bootstrap.id,
        destination_epoch: epoch,
    };
    assert!(store
        .publish_replica_bootstrap(publish.clone())
        .await
        .is_err());
    assert!(store
        .published_replica_bootstrap(&stream)
        .await
        .unwrap()
        .is_none());
    let mut prior_bytes = 0;
    let mut prior_records = 0;
    let mut complete = false;
    for _ in 0..8 {
        let progress = store
            .verify_replica_bootstrap_step(VerifyReplicaBootstrap {
                id: bootstrap.id,
                limits: ReplicaBootstrapVerificationLimits {
                    max_chunks: 1,
                    max_records: 1,
                    max_bytes: 512,
                },
            })
            .await
            .unwrap();
        assert!(progress.verified_snapshot_bytes - prior_bytes <= 4);
        assert!(progress.verified_suffix_records - prior_records <= 1);
        assert!(
            progress.verified_snapshot_bytes > prior_bytes
                || progress.verified_suffix_records > prior_records,
            "verification stalled before completion: {progress:?}"
        );
        prior_bytes = progress.verified_snapshot_bytes;
        prior_records = progress.verified_suffix_records;
        assert!(store
            .published_replica_bootstrap(&stream)
            .await
            .unwrap()
            .is_none());
        if progress.complete {
            complete = true;
            break;
        }
    }
    assert!(
        complete,
        "bounded calls must eventually verify the entire fixed fixture"
    );
    let receipt = store
        .publish_replica_bootstrap(publish.clone())
        .await
        .unwrap();
    assert_eq!(receipt.committed_through.offset, 5);
    assert_eq!(
        store.publish_replica_bootstrap(publish).await.unwrap(),
        receipt
    );
    let read_plan = store
        .acquire_replica_bootstrap_read(&stream, std::time::Duration::from_secs(30))
        .await
        .unwrap();
    let mut bytes = Vec::new();
    let mut offset = 0;
    while offset < content.len() as u64 {
        let page = store
            .read_replica_bootstrap_bytes(read_plan.lease, offset, 3)
            .await
            .unwrap();
        assert!(page.bytes.len() <= 3);
        assert!(page.next_offset > offset);
        bytes.extend_from_slice(page.bytes.as_bytes());
        offset = page.next_offset;
    }
    assert_eq!(bytes, content);
    store
        .release_replica_bootstrap_read(read_plan.lease)
        .await
        .unwrap();
    let page = store
        .read_replica_after(
            &ReplicaPosition { stream, offset: 2 },
            ReplicaBatchLimits {
                max_records: 3,
                max_bytes: 4096,
            },
        )
        .await
        .unwrap();
    assert_eq!(
        page.records
            .iter()
            .map(|r| r.cursor.offset)
            .collect::<Vec<_>>(),
        vec![3, 4, 5]
    );
    assert!(page.complete);
}

#[tokio::test]
async fn snapshot_only_bootstrap_has_its_covered_cursor_as_the_replica_tail() {
    let store = MemoryStore::open(MemoryStoreOptions::default())
        .await
        .unwrap();
    let epoch = ReplicaBatchDestinationStore::destination_epoch(&store)
        .await
        .unwrap();
    let stream = OriginStream {
        origin: OriginId([11; 16]),
        stream: StreamKey {
            id: StreamId::new("snapshot-only").unwrap(),
            incarnation: IncarnationId([12; 16]),
        },
    };
    let bootstrap = ReplicaBootstrap {
        operation_id: ReplicationOperationId::new("begin-only").unwrap(),
        id: BootstrapId([13; 16]),
        replica: ReplicaId::new("remote").unwrap(),
        destination_epoch: epoch,
        stream: stream.clone(),
        snapshot: SnapshotDescriptor {
            id: SnapshotId([14; 16]),
            covered: Cursor::new(stream.stream.clone(), 20),
            schema: SchemaRef {
                id: SchemaId::new("state").unwrap(),
                version: 1,
            },
            content_bytes: 1,
            digest: SnapshotDigest(Sha256::digest(b"s").into()),
        },
        through: ReplicaPosition {
            stream: stream.clone(),
            offset: 20,
        },
    };
    store
        .begin_replica_bootstrap(bootstrap.clone())
        .await
        .unwrap();
    store
        .put_replica_bootstrap_chunk(ReplicaBootstrapChunk {
            id: bootstrap.id,
            chunk: SnapshotChunk {
                offset: 0,
                bytes: Payload::copy_from_slice(b"s"),
            },
        })
        .await
        .unwrap();
    assert!(
        store
            .verify_replica_bootstrap_step(VerifyReplicaBootstrap {
                id: bootstrap.id,
                limits: ReplicaBootstrapVerificationLimits {
                    max_chunks: 1,
                    max_records: 1,
                    max_bytes: 512
                },
            })
            .await
            .unwrap()
            .complete
    );
    store
        .publish_replica_bootstrap(PublishReplicaBootstrap {
            operation_id: ReplicationOperationId::new("publish-only").unwrap(),
            id: bootstrap.id,
            destination_epoch: epoch,
        })
        .await
        .unwrap();
    let after = ReplicaPosition {
        stream: stream.clone(),
        offset: 20,
    };
    let page = store
        .read_replica_after(
            &after,
            ReplicaBatchLimits {
                max_records: 1,
                max_bytes: 4096,
            },
        )
        .await
        .unwrap();
    assert!(page.records.is_empty());
    assert!(page.complete);
    assert_eq!(page.next, after);
    // The next live replication batch continues after the snapshot's covered cursor.
    let receipt = store
        .commit_replica_batch(ReplicaBatch {
            id: BatchId([15; 16]),
            destination_epoch: epoch,
            after,
            records: vec![Arc::new(Record {
                cursor: Cursor::new(stream.stream.clone(), 21),
                event: NewEvent {
                    id: EventId::new("next").unwrap(),
                    schema: SchemaRef {
                        id: SchemaId::new("bytes").unwrap(),
                        version: 1,
                    },
                    payload: Payload::copy_from_slice(b"next"),
                },
            })],
        })
        .await
        .unwrap();
    assert_eq!(receipt.committed_through.offset, 21);
}
