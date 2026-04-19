//! Concurrent readers observing monotone snapshots while one writer appends.
//!
//! Proves the snapshot-isolated contract: readers never see row counts that decrease,
//! and the final snapshot after the writer finishes has exactly the expected row count.
//!
//! This is the single test that validates the async-design claim ("lock-free readers,
//! single-writer swap"). Run under `RUSTFLAGS=-Zsanitizer=thread` for stronger evidence
//! in CI — not required for the default test suite.

use std::sync::Arc;

use arrow_array::RecordBatch;
use mqd_core::{ComponentKind, ComponentName, EntityPath, Event, Payload, Timeline};
use mqd_engine::ingest::batcher::KindBatcher;
use mqd_engine::store::{Store, DEFAULT_BUCKET_NS};

fn make_batch(base_ns: i64, rows: usize) -> RecordBatch {
    let mut b = KindBatcher::new(ComponentKind::Transform3D);
    for i in 0..rows {
        let ev = Event {
            entity_path: EntityPath::new(format!("/agent/{}", i % 8)).unwrap(),
            timeline: Timeline::sim(),
            t_ns: base_ns + i as i64,
            component: ComponentName::new("pose"),
            payload: Payload::Transform3D {
                tx: i as f64,
                ty: 0.0,
                tz: 0.0,
                qx: 0.0,
                qy: 0.0,
                qz: 0.0,
                qw: 1.0,
            },
        };
        b.append(&ev).unwrap();
    }
    b.drain().unwrap().unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn readers_see_monotone_row_counts_while_writer_appends() {
    const WRITER_BATCHES: usize = 2_000;
    const ROWS_PER_BATCH: usize = 16;
    const READER_COUNT: usize = 16;

    let store = Arc::new(Store::new(DEFAULT_BUCKET_NS));

    let writer_store = store.clone();
    let writer = tokio::spawn(async move {
        for b in 0..WRITER_BATCHES {
            let base = (b * ROWS_PER_BATCH) as i64;
            let batch = make_batch(base, ROWS_PER_BATCH);
            writer_store.append_batch(ComponentKind::Transform3D, batch);
            // Yield often to let readers interleave.
            if b % 32 == 0 {
                tokio::task::yield_now().await;
            }
        }
    });

    let mut readers = Vec::with_capacity(READER_COUNT);
    for _ in 0..READER_COUNT {
        let rs = store.clone();
        readers.push(tokio::spawn(async move {
            let mut last = 0u64;
            let mut observations: u64 = 0;
            // Spin-read until the writer is done.
            loop {
                let snap = rs.snapshot();
                let rows = snap.total_rows();
                assert!(
                    rows >= last,
                    "reader saw row count decrease: {last} -> {rows}"
                );
                last = rows;
                observations += 1;
                if rows as usize >= WRITER_BATCHES * ROWS_PER_BATCH {
                    break;
                }
                tokio::task::yield_now().await;
            }
            observations
        }));
    }

    writer.await.unwrap();
    let mut min_obs = u64::MAX;
    for r in readers {
        let obs = r.await.unwrap();
        min_obs = min_obs.min(obs);
    }
    // Every reader completed its spin-loop without panicking; we require that every reader
    // got at least one observation. We don't require a minimum beyond that, because on a
    // fast machine a reader might observe only the final state.
    assert!(min_obs >= 1);

    let final_rows = store.snapshot().total_rows();
    assert_eq!(final_rows, (WRITER_BATCHES * ROWS_PER_BATCH) as u64);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn snapshot_arc_held_during_append_is_stable() {
    let store = Arc::new(Store::new(DEFAULT_BUCKET_NS));
    store.append_batch(ComponentKind::Transform3D, make_batch(0, 8));
    let held = store.snapshot();
    assert_eq!(held.total_rows(), 8);

    // Append more from a different task while we hold the old snapshot.
    let writer_store = store.clone();
    let handle = tokio::spawn(async move {
        for b in 0..100 {
            writer_store.append_batch(
                ComponentKind::Transform3D,
                make_batch(1_000_000 * (b + 1), 8),
            );
        }
    });
    handle.await.unwrap();

    // Held snapshot still says 8 rows.
    assert_eq!(held.total_rows(), 8);
    // Current snapshot reflects the new writes.
    assert_eq!(store.snapshot().total_rows(), 8 + 100 * 8);
}
