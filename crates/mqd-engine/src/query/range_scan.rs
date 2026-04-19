//! Range scan over a half-open time window.
//!
//! **Semantics** — returns every row satisfying `start_ns <= t_ns < end_ns` (half-open, like
//! Rust's `Range<i64>`). Rows for a single kind are returned as a concatenated `RecordBatch`
//! preserving insertion order; callers that need per-partition access should read the
//! snapshot directly.
//!
//! Pruning:
//! - A partition is pruned by time when `t_max_ns < start_ns` OR `t_min_ns >= end_ns`.
//! - A partition is pruned by kind when `key.kind != query.kind`.
//! - Within a surviving partition, rows are filtered by the inclusive-exclusive predicate.

use std::time::Instant;

use anyhow::Result;
use arrow::compute::{concat_batches, filter_record_batch};
use arrow_array::{BooleanArray, Int64Array, RecordBatch, StringArray};
use mqd_core::{schema_for, ComponentKind, EntityPath};

use crate::query::QueryPlan;
use crate::store::StoreSnapshot;

#[derive(Clone, Debug)]
pub struct RangeScanQuery {
    pub kind: ComponentKind,
    pub start_ns: i64,
    pub end_ns: i64,
    /// Optional entity filter.
    pub entity: Option<EntityPath>,
}

#[derive(Debug)]
pub struct RangeScanResult {
    pub batch: RecordBatch,
    pub plan: QueryPlan,
}

pub fn range_scan(snapshot: &StoreSnapshot, query: RangeScanQuery) -> Result<RangeScanResult> {
    let start = Instant::now();
    let mut plan = QueryPlan::new("range_scan");

    if query.start_ns > query.end_ns {
        anyhow::bail!(
            "range_scan: start_ns ({}) must be <= end_ns ({})",
            query.start_ns,
            query.end_ns
        );
    }

    let candidate_total = snapshot.partitions.len() as u64;
    plan.candidate_partitions = candidate_total;

    let same_kind: Vec<_> = snapshot
        .partitions
        .iter()
        .filter(|(k, _)| k.kind == query.kind)
        .collect();
    plan.pruned_by_kind = candidate_total - same_kind.len() as u64;

    // Half-open: a partition is relevant iff [t_min, t_max] intersects [start, end).
    // Intersection fails when t_max < start OR t_min >= end.
    let time_survivors: Vec<_> = same_kind
        .into_iter()
        .filter(|(_, p)| p.t_max_ns >= query.start_ns && p.t_min_ns < query.end_ns)
        .collect();
    plan.pruned_by_time = (candidate_total - plan.pruned_by_kind) - time_survivors.len() as u64;
    plan.scanned_partitions = time_survivors.len() as u64;

    let entity_filter = query.entity.as_ref().map(|e| e.as_str());

    let mut scanned_rows: u64 = 0;
    let mut filtered: Vec<RecordBatch> = Vec::new();
    for (_, partition) in &time_survivors {
        for batch in &partition.batches {
            scanned_rows += batch.num_rows() as u64;
            let mask = build_mask(batch, query.start_ns, query.end_ns, entity_filter);
            if mask.true_count() == 0 {
                continue;
            }
            let filt = filter_record_batch(batch, &mask)?;
            if filt.num_rows() > 0 {
                filtered.push(filt);
            }
        }
    }
    plan.scanned_rows = scanned_rows;

    let schema = schema_for(query.kind);
    let result = if filtered.is_empty() {
        RecordBatch::new_empty(schema)
    } else {
        concat_batches(&schema, &filtered)?
    };
    plan.returned_rows = result.num_rows() as u64;
    plan.latency_us = start.elapsed().as_micros();

    Ok(RangeScanResult {
        batch: result,
        plan,
    })
}

fn build_mask(
    batch: &RecordBatch,
    start_ns: i64,
    end_ns: i64,
    entity_filter: Option<&str>,
) -> BooleanArray {
    let t_col = batch
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("t_ns column");
    let entity_col = batch
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("entity_path column");
    let n = batch.num_rows();
    let mut bits = Vec::with_capacity(n);
    for i in 0..n {
        let t = t_col.value(i);
        let in_time = t >= start_ns && t < end_ns;
        let in_entity = entity_filter
            .map(|e| entity_col.value(i) == e)
            .unwrap_or(true);
        bits.push(in_time && in_entity);
    }
    BooleanArray::from(bits)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ingest::batcher::KindBatcher;
    use crate::store::{Store, DEFAULT_BUCKET_NS};
    use mqd_core::{ComponentName, Event, Payload, Timeline};

    fn tf_event(entity: &str, t_ns: i64) -> Event {
        Event {
            entity_path: EntityPath::new(entity).unwrap(),
            timeline: Timeline::sim(),
            t_ns,
            component: ComponentName::new("pose"),
            payload: Payload::Transform3D {
                tx: 0.0,
                ty: 0.0,
                tz: 0.0,
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

    #[test]
    fn range_scan_is_half_open() {
        let store = Store::new(DEFAULT_BUCKET_NS);
        ingest(
            &store,
            &[
                tf_event("/a/0", 100),
                tf_event("/a/1", 200),
                tf_event("/a/2", 300),
                tf_event("/a/3", 400),
            ],
        );
        let snap = store.snapshot();
        let result = range_scan(
            &snap,
            RangeScanQuery {
                kind: ComponentKind::Transform3D,
                start_ns: 200,
                end_ns: 400,
                entity: None,
            },
        )
        .unwrap();
        // [200, 400) includes 200 and 300, excludes 400.
        assert_eq!(result.batch.num_rows(), 2);
        let t_col = result
            .batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let vals: Vec<i64> = (0..t_col.len()).map(|i| t_col.value(i)).collect();
        assert_eq!(vals, vec![200, 300]);
    }

    #[test]
    fn range_scan_empty_when_start_equals_end() {
        let store = Store::new(DEFAULT_BUCKET_NS);
        ingest(&store, &[tf_event("/a/0", 100), tf_event("/a/1", 200)]);
        let snap = store.snapshot();
        let result = range_scan(
            &snap,
            RangeScanQuery {
                kind: ComponentKind::Transform3D,
                start_ns: 150,
                end_ns: 150,
                entity: None,
            },
        )
        .unwrap();
        assert_eq!(result.batch.num_rows(), 0);
    }

    #[test]
    fn range_scan_rejects_inverted_bounds() {
        let store = Store::new(DEFAULT_BUCKET_NS);
        ingest(&store, &[tf_event("/a/0", 100)]);
        let snap = store.snapshot();
        let err = range_scan(
            &snap,
            RangeScanQuery {
                kind: ComponentKind::Transform3D,
                start_ns: 200,
                end_ns: 100,
                entity: None,
            },
        );
        assert!(err.is_err());
    }

    #[test]
    fn range_scan_prunes_partitions_outside_window() {
        let store = Store::new(DEFAULT_BUCKET_NS);
        let b = DEFAULT_BUCKET_NS;
        ingest(&store, &[tf_event("/a/0", 0)]);
        ingest(&store, &[tf_event("/a/0", 2 * b)]);
        ingest(&store, &[tf_event("/a/0", 4 * b)]);
        let snap = store.snapshot();
        let result = range_scan(
            &snap,
            RangeScanQuery {
                kind: ComponentKind::Transform3D,
                start_ns: 2 * b,
                end_ns: 3 * b,
                entity: None,
            },
        )
        .unwrap();
        assert_eq!(result.plan.candidate_partitions, 3);
        assert_eq!(result.plan.scanned_partitions, 1);
        assert_eq!(result.plan.pruned_by_time, 2);
        assert_eq!(result.batch.num_rows(), 1);
    }

    #[test]
    fn range_scan_filters_by_entity() {
        let store = Store::new(DEFAULT_BUCKET_NS);
        ingest(
            &store,
            &[
                tf_event("/a/0", 100),
                tf_event("/a/1", 200),
                tf_event("/a/0", 300),
            ],
        );
        let snap = store.snapshot();
        let result = range_scan(
            &snap,
            RangeScanQuery {
                kind: ComponentKind::Transform3D,
                start_ns: 0,
                end_ns: 1000,
                entity: Some(EntityPath::new("/a/0").unwrap()),
            },
        )
        .unwrap();
        assert_eq!(result.batch.num_rows(), 2);
    }
}
