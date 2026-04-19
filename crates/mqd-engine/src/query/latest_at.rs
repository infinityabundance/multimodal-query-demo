//! Point-in-time (latest-at) resolver.
//!
//! **Semantics** — for each entity, return the single row whose `t_ns` is the largest value
//! satisfying `t_ns <= at_ns`. If multiple rows share that maximum `t_ns` (a tie), the
//! later-inserted row wins. Insertion order is defined by `(batch_index, row_index)` within
//! a partition; two rows with identical `t_ns` must land in the same partition because
//! bucketing is a pure function of `t_ns`, so this total order is well-defined.
//!
//! The tie-break rule is a **contract**, not an implementation detail — see the
//! `latest_at_ties_are_deterministic` test.

use std::collections::BTreeMap;
use std::time::Instant;

use anyhow::Result;
use arrow::compute::concat_batches;
use arrow_array::{Int64Array, RecordBatch, StringArray};
use mqd_core::{schema_for, ComponentKind, EntityPath};

use crate::query::QueryPlan;
use crate::store::{PartitionKey, StoreSnapshot};

#[derive(Clone, Debug)]
pub struct LatestAtQuery {
    pub kind: ComponentKind,
    pub at_ns: i64,
    /// Optional entity filter; `None` returns one row per distinct entity.
    pub entity: Option<EntityPath>,
}

#[derive(Debug)]
pub struct LatestAtResult {
    pub batch: RecordBatch,
    pub plan: QueryPlan,
}

pub fn latest_at(snapshot: &StoreSnapshot, query: LatestAtQuery) -> Result<LatestAtResult> {
    let start = Instant::now();
    let mut plan = QueryPlan::new("latest_at");

    let candidate_total = snapshot.partitions.len() as u64;
    plan.candidate_partitions = candidate_total;

    let same_kind: Vec<_> = snapshot
        .partitions
        .iter()
        .filter(|(k, _)| k.kind == query.kind)
        .collect();
    plan.pruned_by_kind = candidate_total - same_kind.len() as u64;

    let time_survivors: Vec<_> = same_kind
        .into_iter()
        .filter(|(_, p)| p.t_min_ns <= query.at_ns)
        .collect();
    plan.pruned_by_time = (candidate_total - plan.pruned_by_kind) - time_survivors.len() as u64;
    plan.scanned_partitions = time_survivors.len() as u64;

    let entity_filter = query.entity.as_ref().map(|e| e.as_str());

    // Per-entity running best: `(t_ns, (batch_idx, row_idx))`.
    // Tie-breaker: larger `(batch_idx, row_idx)` wins, i.e., the later-inserted row.
    let mut best: BTreeMap<String, (i64, (usize, usize))> = BTreeMap::new();
    let mut scanned_rows: u64 = 0;
    // Track which partition each entity's winning row lives in, so we can slice it later.
    let mut winner_partition: BTreeMap<String, PartitionKey> = BTreeMap::new();

    for (pk, partition) in &time_survivors {
        for (batch_idx, batch) in partition.batches.iter().enumerate() {
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
            scanned_rows += n as u64;
            for row in 0..n {
                let t = t_col.value(row);
                if t > query.at_ns {
                    continue;
                }
                let entity = entity_col.value(row);
                if let Some(filter) = entity_filter {
                    if entity != filter {
                        continue;
                    }
                }
                let candidate_key = (batch_idx, row);
                let entry = best.get(entity).copied();
                let replace = match entry {
                    None => true,
                    Some((cur_t, cur_key)) => match t.cmp(&cur_t) {
                        std::cmp::Ordering::Greater => true,
                        std::cmp::Ordering::Equal => {
                            // Ties only occur within one partition because bucketing is a
                            // pure function of t_ns.
                            let cur_pk = winner_partition.get(entity).unwrap();
                            debug_assert_eq!(
                                cur_pk, *pk,
                                "tied t_ns rows must live in the same partition; \
                                 cur={cur_pk:?} new={pk:?}"
                            );
                            candidate_key > cur_key
                        }
                        std::cmp::Ordering::Less => false,
                    },
                };
                if replace {
                    best.insert(entity.to_string(), (t, candidate_key));
                    winner_partition.insert(entity.to_string(), (*pk).clone());
                }
            }
        }
    }

    plan.scanned_rows = scanned_rows;

    let schema = schema_for(query.kind);
    let mut slices: Vec<RecordBatch> = Vec::with_capacity(best.len());
    for (entity, (_, (batch_idx, row_idx))) in &best {
        let pk = winner_partition
            .get(entity)
            .expect("winner must have partition key");
        let partition = snapshot
            .partitions
            .get(pk)
            .expect("partition must exist for winner");
        let batch = &partition.batches[*batch_idx];
        slices.push(batch.slice(*row_idx, 1));
    }
    let result = if slices.is_empty() {
        RecordBatch::new_empty(schema)
    } else {
        concat_batches(&schema, &slices)?
    };

    plan.returned_rows = result.num_rows() as u64;
    plan.latency_us = start.elapsed().as_micros();

    Ok(LatestAtResult {
        batch: result,
        plan,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ingest::batcher::KindBatcher;
    use crate::store::{Store, DEFAULT_BUCKET_NS};
    use arrow_array::Float64Array;
    use mqd_core::{ComponentName, Event, Payload, Timeline};

    fn tf_event(entity: &str, t_ns: i64, tx: f64) -> Event {
        Event {
            entity_path: EntityPath::new(entity).unwrap(),
            timeline: Timeline::sim(),
            t_ns,
            component: ComponentName::new("pose"),
            payload: Payload::Transform3D {
                tx,
                ty: 0.0,
                tz: 0.0,
                qx: 0.0,
                qy: 0.0,
                qz: 0.0,
                qw: 1.0,
            },
        }
    }

    fn ingest_events(store: &Store, events: &[Event]) {
        let mut b = KindBatcher::new(ComponentKind::Transform3D);
        for ev in events {
            b.append(ev).unwrap();
        }
        if let Some(batch) = b.drain().unwrap() {
            store.append_batch(ComponentKind::Transform3D, batch);
        }
    }

    fn tx_of(batch: &RecordBatch, row: usize) -> f64 {
        batch
            .column_by_name("tx")
            .unwrap()
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap()
            .value(row)
    }

    fn entity_of(batch: &RecordBatch, row: usize) -> String {
        batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .value(row)
            .to_string()
    }

    #[test]
    fn latest_at_picks_largest_t_leq_at() {
        let store = Store::new(DEFAULT_BUCKET_NS);
        ingest_events(
            &store,
            &[
                tf_event("/agent/1", 100, 1.0),
                tf_event("/agent/1", 200, 2.0),
                tf_event("/agent/1", 300, 3.0),
            ],
        );
        let snap = store.snapshot();
        let result = latest_at(
            &snap,
            LatestAtQuery {
                kind: ComponentKind::Transform3D,
                at_ns: 250,
                entity: Some(EntityPath::new("/agent/1").unwrap()),
            },
        )
        .unwrap();
        assert_eq!(result.batch.num_rows(), 1);
        assert_eq!(tx_of(&result.batch, 0), 2.0);
        assert_eq!(result.plan.returned_rows, 1);
    }

    #[test]
    fn latest_at_returns_empty_when_all_rows_after_at() {
        let store = Store::new(DEFAULT_BUCKET_NS);
        ingest_events(&store, &[tf_event("/agent/1", 500, 5.0)]);
        let snap = store.snapshot();
        let result = latest_at(
            &snap,
            LatestAtQuery {
                kind: ComponentKind::Transform3D,
                at_ns: 100,
                entity: None,
            },
        )
        .unwrap();
        assert_eq!(result.batch.num_rows(), 0);
        assert_eq!(result.plan.returned_rows, 0);
    }

    #[test]
    fn latest_at_returns_one_row_per_entity_when_no_filter() {
        let store = Store::new(DEFAULT_BUCKET_NS);
        ingest_events(
            &store,
            &[
                tf_event("/agent/1", 100, 1.0),
                tf_event("/agent/2", 150, 2.5),
                tf_event("/agent/3", 180, 9.0),
                tf_event("/agent/1", 200, 2.0),
            ],
        );
        let snap = store.snapshot();
        let result = latest_at(
            &snap,
            LatestAtQuery {
                kind: ComponentKind::Transform3D,
                at_ns: 300,
                entity: None,
            },
        )
        .unwrap();
        assert_eq!(result.batch.num_rows(), 3);
        let mut got: Vec<(String, f64)> = (0..result.batch.num_rows())
            .map(|i| (entity_of(&result.batch, i), tx_of(&result.batch, i)))
            .collect();
        got.sort_by(|a, b| a.0.cmp(&b.0));
        assert_eq!(got[0], ("/agent/1".to_string(), 2.0));
        assert_eq!(got[1], ("/agent/2".to_string(), 2.5));
        assert_eq!(got[2], ("/agent/3".to_string(), 9.0));
    }

    #[test]
    fn latest_at_ties_are_deterministic() {
        // Contract: when two rows share the max t_ns <= at_ns, the later-inserted row wins.
        let store = Store::new(DEFAULT_BUCKET_NS);
        ingest_events(
            &store,
            &[
                tf_event("/agent/1", 100, 1.0),
                tf_event("/agent/1", 100, 2.0), // later-inserted duplicate at identical t_ns
            ],
        );
        let snap = store.snapshot();
        let result = latest_at(
            &snap,
            LatestAtQuery {
                kind: ComponentKind::Transform3D,
                at_ns: 100,
                entity: Some(EntityPath::new("/agent/1").unwrap()),
            },
        )
        .unwrap();
        assert_eq!(result.batch.num_rows(), 1);
        assert_eq!(
            tx_of(&result.batch, 0),
            2.0,
            "later-inserted row must win on tie"
        );
    }

    #[test]
    fn latest_at_tie_across_separate_batches_picks_later_batch() {
        let store = Store::new(DEFAULT_BUCKET_NS);
        ingest_events(&store, &[tf_event("/agent/1", 100, 1.0)]);
        ingest_events(&store, &[tf_event("/agent/1", 100, 7.0)]);
        let snap = store.snapshot();
        let result = latest_at(
            &snap,
            LatestAtQuery {
                kind: ComponentKind::Transform3D,
                at_ns: 100,
                entity: Some(EntityPath::new("/agent/1").unwrap()),
            },
        )
        .unwrap();
        assert_eq!(tx_of(&result.batch, 0), 7.0);
    }

    #[test]
    fn latest_at_time_pruning_tracked_in_plan() {
        let store = Store::new(DEFAULT_BUCKET_NS);
        // bucket 0s
        ingest_events(&store, &[tf_event("/agent/1", 100, 1.0)]);
        // bucket 2s
        ingest_events(&store, &[tf_event("/agent/1", 2 * DEFAULT_BUCKET_NS, 5.0)]);
        // bucket 4s
        ingest_events(&store, &[tf_event("/agent/1", 4 * DEFAULT_BUCKET_NS, 9.0)]);
        let snap = store.snapshot();
        let result = latest_at(
            &snap,
            LatestAtQuery {
                kind: ComponentKind::Transform3D,
                at_ns: DEFAULT_BUCKET_NS + 500,
                entity: None,
            },
        )
        .unwrap();
        // Only bucket 0 survives.
        assert_eq!(result.plan.candidate_partitions, 3);
        assert_eq!(result.plan.scanned_partitions, 1);
        assert_eq!(result.plan.pruned_by_time, 2);
        assert_eq!(tx_of(&result.batch, 0), 1.0);
    }

    #[test]
    fn latest_at_kind_pruning_tracked_in_plan() {
        let store = Store::new(DEFAULT_BUCKET_NS);
        ingest_events(&store, &[tf_event("/agent/1", 100, 1.0)]);
        // Add an Annotation partition.
        let mut ann = KindBatcher::new(ComponentKind::Annotation);
        ann.append(&Event {
            entity_path: EntityPath::new("/agent/1").unwrap(),
            timeline: Timeline::sim(),
            t_ns: 100,
            component: ComponentName::new("tag"),
            payload: Payload::Annotation {
                label: "hello".into(),
            },
        })
        .unwrap();
        if let Some(b) = ann.drain().unwrap() {
            store.append_batch(ComponentKind::Annotation, b);
        }
        let snap = store.snapshot();
        let result = latest_at(
            &snap,
            LatestAtQuery {
                kind: ComponentKind::Transform3D,
                at_ns: 1000,
                entity: None,
            },
        )
        .unwrap();
        assert_eq!(result.plan.candidate_partitions, 2);
        assert_eq!(result.plan.pruned_by_kind, 1);
    }
}
