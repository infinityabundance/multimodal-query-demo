//! Proximity pairs at a point in time.
//!
//! **Semantics** — at `at_ns`, resolve each agent's latest `Transform3D` pose (per
//! `latest_at` rules), then emit every unordered pair `(entity_a, entity_b)` whose pose
//! separation is `<= radius`. Pairs are deduplicated by the `entity_a < entity_b`
//! invariant and sorted lexicographically for determinism.
//!
//! **Complexity** — O(N^2) in the number of distinct entities at `at_ns`, by construction.
//! No spatial index is used. This is documented as a non-claim and surfaced in the
//! query plan's `notes` field so `--explain` makes it visible.

use std::sync::Arc;
use std::time::Instant;

use anyhow::Result;
use arrow_array::{Float64Array, Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use mqd_core::ComponentKind;

use crate::query::{latest_at, LatestAtQuery, QueryPlan};
use crate::store::StoreSnapshot;

#[derive(Clone, Debug)]
pub struct ProximityQuery {
    pub at_ns: i64,
    pub radius: f64,
}

#[derive(Debug)]
pub struct ProximityResult {
    pub batch: RecordBatch,
    pub plan: QueryPlan,
    /// Number of entities whose latest-at pose was considered (drives the O(N^2) cost).
    pub considered_entities: u64,
}

pub fn proximity_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("at_ns", DataType::Int64, false),
        Field::new("entity_a", DataType::Utf8, false),
        Field::new("entity_b", DataType::Utf8, false),
        Field::new("distance", DataType::Float64, false),
    ]))
}

pub fn proximity_pairs(snapshot: &StoreSnapshot, query: ProximityQuery) -> Result<ProximityResult> {
    if !(query.radius.is_finite() && query.radius >= 0.0) {
        anyhow::bail!(
            "proximity: radius must be non-negative finite, got {}",
            query.radius
        );
    }
    let start = Instant::now();

    // Step 1: per-entity latest-at pose. Reuses the plan; pruning stats propagate.
    let latest = latest_at(
        snapshot,
        LatestAtQuery {
            kind: ComponentKind::Transform3D,
            at_ns: query.at_ns,
            entity: None,
        },
    )?;
    let mut plan = QueryPlan::new("proximity_pairs");
    plan.candidate_partitions = latest.plan.candidate_partitions;
    plan.pruned_by_kind = latest.plan.pruned_by_kind;
    plan.pruned_by_time = latest.plan.pruned_by_time;
    plan.scanned_partitions = latest.plan.scanned_partitions;
    plan.scanned_rows = latest.plan.scanned_rows;

    let n = latest.batch.num_rows();
    if n == 0 {
        plan.returned_rows = 0;
        plan.latency_us = start.elapsed().as_micros();
        return Ok(ProximityResult {
            batch: RecordBatch::new_empty(proximity_schema()),
            plan,
            considered_entities: 0,
        });
    }

    let entity_col = latest
        .batch
        .column_by_name("entity_path")
        .expect("entity_path column")
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("entity_path must be Utf8");
    let tx_col = column_f64(&latest.batch, "tx")?;
    let ty_col = column_f64(&latest.batch, "ty")?;
    let tz_col = column_f64(&latest.batch, "tz")?;

    // Snapshot positions into a Vec for O(1) random access. Sorted by entity_path for
    // deterministic output order — latest_at already returns one row per entity in sorted
    // order (BTreeMap iteration), but we re-sort defensively.
    let mut positions: Vec<(String, [f64; 3])> = (0..n)
        .map(|i| {
            (
                entity_col.value(i).to_string(),
                [tx_col.value(i), ty_col.value(i), tz_col.value(i)],
            )
        })
        .collect();
    positions.sort_by(|a, b| a.0.cmp(&b.0));

    let r2 = query.radius * query.radius;
    let mut at_ns_out = Vec::new();
    let mut a_out: Vec<String> = Vec::new();
    let mut b_out: Vec<String> = Vec::new();
    let mut d_out: Vec<f64> = Vec::new();

    // O(N^2) pair enumeration with i<j to dedupe unordered pairs.
    for i in 0..positions.len() {
        let (ref ea, pa) = positions[i];
        for position in positions.iter().skip(i + 1) {
            let (ref eb, pb) = *position;
            let dx = pa[0] - pb[0];
            let dy = pa[1] - pb[1];
            let dz = pa[2] - pb[2];
            let d2 = dx * dx + dy * dy + dz * dz;
            if d2 <= r2 {
                at_ns_out.push(query.at_ns);
                a_out.push(ea.clone());
                b_out.push(eb.clone());
                d_out.push(d2.sqrt());
            }
        }
    }

    let schema = proximity_schema();
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(at_ns_out)),
            Arc::new(StringArray::from(a_out)),
            Arc::new(StringArray::from(b_out)),
            Arc::new(Float64Array::from(d_out)),
        ],
    )?;

    plan.returned_rows = batch.num_rows() as u64;
    plan.latency_us = start.elapsed().as_micros();

    Ok(ProximityResult {
        batch,
        plan,
        considered_entities: n as u64,
    })
}

fn column_f64<'a>(batch: &'a RecordBatch, name: &str) -> Result<&'a Float64Array> {
    batch
        .column_by_name(name)
        .ok_or_else(|| anyhow::anyhow!("missing column {name}"))?
        .as_any()
        .downcast_ref::<Float64Array>()
        .ok_or_else(|| anyhow::anyhow!("column {name} is not Float64"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ingest::batcher::KindBatcher;
    use crate::store::{Store, DEFAULT_BUCKET_NS};
    use mqd_core::{ComponentName, EntityPath, Event, Payload, Timeline};

    fn pose(entity: &str, t_ns: i64, tx: f64, ty: f64, tz: f64) -> Event {
        Event {
            entity_path: EntityPath::new(entity).unwrap(),
            timeline: Timeline::sim(),
            t_ns,
            component: ComponentName::new("pose"),
            payload: Payload::Transform3D {
                tx,
                ty,
                tz,
                qx: 0.0,
                qy: 0.0,
                qz: 0.0,
                qw: 1.0,
            },
        }
    }

    fn ingest(store: &Store, events: &[Event]) {
        let mut b = KindBatcher::new(ComponentKind::Transform3D);
        for ev in events {
            b.append(ev).unwrap();
        }
        if let Some(batch) = b.drain().unwrap() {
            store.append_batch(ComponentKind::Transform3D, batch);
        }
    }

    fn pair_rows(batch: &RecordBatch) -> Vec<(String, String, f64)> {
        let a = batch
            .column_by_name("entity_a")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let b = batch
            .column_by_name("entity_b")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let d = batch
            .column_by_name("distance")
            .unwrap()
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        (0..batch.num_rows())
            .map(|i| (a.value(i).to_string(), b.value(i).to_string(), d.value(i)))
            .collect()
    }

    #[test]
    fn proximity_pair_at_origin_and_offset() {
        let store = Store::new(DEFAULT_BUCKET_NS);
        ingest(
            &store,
            &[
                pose("/a/0", 100, 0.0, 0.0, 0.0),
                pose("/a/1", 100, 1.0, 0.0, 0.0),
                pose("/a/2", 100, 10.0, 0.0, 0.0),
            ],
        );
        let snap = store.snapshot();
        let result = proximity_pairs(
            &snap,
            ProximityQuery {
                at_ns: 100,
                radius: 2.0,
            },
        )
        .unwrap();
        let rows = pair_rows(&result.batch);
        assert_eq!(rows.len(), 1);
        let (a, b, d) = &rows[0];
        assert_eq!(a, "/a/0");
        assert_eq!(b, "/a/1");
        assert!((d - 1.0).abs() < 1e-12);
        assert_eq!(result.considered_entities, 3);
    }

    #[test]
    fn proximity_no_pairs_when_all_far() {
        let store = Store::new(DEFAULT_BUCKET_NS);
        ingest(
            &store,
            &[
                pose("/a/0", 100, 0.0, 0.0, 0.0),
                pose("/a/1", 100, 10.0, 0.0, 0.0),
            ],
        );
        let snap = store.snapshot();
        let result = proximity_pairs(
            &snap,
            ProximityQuery {
                at_ns: 100,
                radius: 1.0,
            },
        )
        .unwrap();
        assert_eq!(result.batch.num_rows(), 0);
    }

    #[test]
    fn proximity_uses_latest_at_not_current() {
        // At t=100, /a/0 is at origin, /a/1 at distance 5 (out of 1.0 radius).
        // At t=200, both are at origin. Query at t=150 should resolve /a/0 to its t=100 pose
        // and /a/1 to its t=100 pose. Expect 0 pairs.
        let store = Store::new(DEFAULT_BUCKET_NS);
        ingest(
            &store,
            &[
                pose("/a/0", 100, 0.0, 0.0, 0.0),
                pose("/a/1", 100, 5.0, 0.0, 0.0),
                pose("/a/0", 200, 0.0, 0.0, 0.0),
                pose("/a/1", 200, 0.0, 0.0, 0.0),
            ],
        );
        let snap = store.snapshot();
        let result = proximity_pairs(
            &snap,
            ProximityQuery {
                at_ns: 150,
                radius: 1.0,
            },
        )
        .unwrap();
        assert_eq!(result.batch.num_rows(), 0);
    }

    #[test]
    fn proximity_is_undirected_and_sorted() {
        // Three colocated agents at the origin: expect pairs (a,b), (a,c), (b,c).
        let store = Store::new(DEFAULT_BUCKET_NS);
        ingest(
            &store,
            &[
                pose("/a", 100, 0.0, 0.0, 0.0),
                pose("/b", 100, 0.0, 0.0, 0.0),
                pose("/c", 100, 0.0, 0.0, 0.0),
            ],
        );
        let snap = store.snapshot();
        let result = proximity_pairs(
            &snap,
            ProximityQuery {
                at_ns: 100,
                radius: 0.5,
            },
        )
        .unwrap();
        let rows = pair_rows(&result.batch);
        assert_eq!(rows.len(), 3);
        for (a, b, _) in &rows {
            assert!(a < b, "entity_a must be lex-less than entity_b: {a} {b}");
        }
        let pair_set: Vec<(String, String)> = rows
            .iter()
            .map(|(a, b, _)| (a.clone(), b.clone()))
            .collect();
        assert_eq!(
            pair_set,
            vec![
                ("/a".to_string(), "/b".to_string()),
                ("/a".to_string(), "/c".to_string()),
                ("/b".to_string(), "/c".to_string()),
            ]
        );
    }

    #[test]
    fn proximity_rejects_negative_radius() {
        let store = Store::new(DEFAULT_BUCKET_NS);
        ingest(&store, &[pose("/a", 100, 0.0, 0.0, 0.0)]);
        let snap = store.snapshot();
        let err = proximity_pairs(
            &snap,
            ProximityQuery {
                at_ns: 100,
                radius: -1.0,
            },
        );
        assert!(err.is_err());
    }
}
