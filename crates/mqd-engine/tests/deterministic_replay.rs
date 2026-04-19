//! Deterministic replay test: pins SHA256 fingerprints for (a) the generator's JSONL
//! bytestream at a fixed seed and (b) the `proximity_pairs` result over that bytestream.
//!
//! Any accidental change to the generator, batcher, store layout, latest-at tie-break, or
//! proximity operator that affects observable output will flip one of these hashes and
//! fail this test. That's the point — this is the wall between "refactor" and "semantic
//! drift".
//!
//! The fingerprint inputs are chosen to be small (fast test) but non-trivial (exercises
//! more than one partition, more than one entity).

use arrow_array::{Float64Array, Int64Array, RecordBatch, StringArray};
use mqd_core::{schema_for, ComponentKind};
use mqd_engine::generator::{generate_run, GenConfig};
use mqd_engine::ops::{proximity_pairs, ProximityQuery};
use mqd_engine::store::{Store, DEFAULT_BUCKET_NS};
use sha2::{Digest, Sha256};

/// Hash every event's serde_json line + '\n', in emit order. Same bytes `mqd generate`
/// writes to disk.
fn fingerprint_jsonl(events: &[mqd_core::Event]) -> String {
    let mut hasher = Sha256::new();
    for e in events {
        let line = serde_json::to_string(e).expect("serialize event");
        hasher.update(line.as_bytes());
        hasher.update(b"\n");
    }
    hex::encode(hasher.finalize())
}

/// Hash the proximity result's columnar contents in row order. We hash the raw column
/// values rather than Arrow IPC bytes so the fingerprint is robust to Arrow encoding
/// differences across versions (metadata, buffer padding) while still pinning every
/// user-visible number and string.
fn fingerprint_proximity(batch: &RecordBatch) -> String {
    let mut hasher = Sha256::new();
    let at_col = batch
        .column_by_name("at_ns")
        .expect("at_ns")
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("at_ns Int64");
    let a_col = batch
        .column_by_name("entity_a")
        .expect("entity_a")
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("entity_a Utf8");
    let b_col = batch
        .column_by_name("entity_b")
        .expect("entity_b")
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("entity_b Utf8");
    let d_col = batch
        .column_by_name("distance")
        .expect("distance")
        .as_any()
        .downcast_ref::<Float64Array>()
        .expect("distance Float64");

    hasher.update((batch.num_rows() as u64).to_le_bytes());
    for i in 0..batch.num_rows() {
        hasher.update(at_col.value(i).to_le_bytes());
        let a = a_col.value(i);
        hasher.update((a.len() as u64).to_le_bytes());
        hasher.update(a.as_bytes());
        let b = b_col.value(i);
        hasher.update((b.len() as u64).to_le_bytes());
        hasher.update(b.as_bytes());
        // f64 bit pattern is the right unit of determinism here: two f64 values compare
        // equal iff their bit patterns match (excluding NaN, which proximity cannot emit).
        hasher.update(d_col.value(i).to_bits().to_le_bytes());
    }
    hex::encode(hasher.finalize())
}

/// Stuff the generated events into a store one kind at a time, monotonic in t_ns. This
/// mirrors the path the async ingest pipeline drives.
fn build_store(events: &[mqd_core::Event]) -> Store {
    let store = Store::new(DEFAULT_BUCKET_NS);
    let kinds = [
        ComponentKind::Transform3D,
        ComponentKind::Scalar,
        ComponentKind::ImageRef,
        ComponentKind::Annotation,
        ComponentKind::Points3D,
    ];
    for kind in kinds {
        let filtered: Vec<&mqd_core::Event> = events.iter().filter(|e| e.kind() == kind).collect();
        if filtered.is_empty() {
            continue;
        }
        let schema = schema_for(kind);
        let mut batcher = mqd_engine::ingest::batcher::KindBatcher::new(kind);
        for ev in filtered {
            batcher.append(ev).expect("append event");
        }
        if let Some(batch) = batcher.drain().expect("drain") {
            // Sanity: schema check keeps us honest against any field drift.
            assert_eq!(batch.schema(), schema);
            store.append_batch(kind, batch);
        }
    }
    store
}

const PINNED_JSONL_FINGERPRINT: &str =
    "7b2a6c5189d12ed46bfe350f35ced74983df7504c4108f463d7d2ba4f21e0dd6";
const PINNED_PROXIMITY_FINGERPRINT: &str =
    "3a2ec9a75d7ca0dc0e47b6b62006bc91858878ec251b3323362a9485ac0245d5";

#[test]
fn jsonl_fingerprint_is_pinned_at_seed_42_agents_4_ticks_100() {
    let cfg = GenConfig {
        seed: 42,
        agents: 4,
        ticks: 100,
        ..Default::default()
    };
    let events = generate_run(&cfg);
    let got = fingerprint_jsonl(&events);
    assert_eq!(
        got, PINNED_JSONL_FINGERPRINT,
        "\nGenerator output fingerprint drift detected.\n\
         If this is an intentional generator change, update PINNED_JSONL_FINGERPRINT to: {got}\n\
         Otherwise, something semantic drifted — investigate before pinning."
    );
}

#[test]
fn proximity_fingerprint_is_pinned_at_seed_42_radius_2_5_at_mid() {
    let cfg = GenConfig {
        seed: 42,
        agents: 4,
        ticks: 100,
        ..Default::default()
    };
    let events = generate_run(&cfg);
    let store = build_store(&events);
    let snap = store.snapshot();

    // at_ns = middle of the run — with tick_ns=5e6 and 100 ticks, the run covers
    // [0, 5e8) ns. Pick 2.5e8 so we land inside a populated partition, not at the edge.
    let at_ns = 250_000_000;
    let result =
        proximity_pairs(&snap, ProximityQuery { at_ns, radius: 2.5 }).expect("proximity_pairs");
    let got = fingerprint_proximity(&result.batch);
    assert_eq!(
        got, PINNED_PROXIMITY_FINGERPRINT,
        "\nProximity result fingerprint drift detected (at_ns={at_ns}, radius=2.5).\n\
         If this is an intentional semantic change, update PINNED_PROXIMITY_FINGERPRINT to: {got}\n\
         Otherwise, latest-at / bucketing / proximity semantics have drifted."
    );

    // Extra sanity: the result is non-empty (otherwise the fingerprint would trivially
    // match an empty batch) and sorted a < b.
    assert!(
        result.batch.num_rows() > 0,
        "expected some proximity pairs at midpoint"
    );
    let a = result
        .batch
        .column_by_name("entity_a")
        .unwrap()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let b = result
        .batch
        .column_by_name("entity_b")
        .unwrap()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    for i in 0..result.batch.num_rows() {
        assert!(a.value(i) < b.value(i));
    }
}
