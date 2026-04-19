//! Speed-over-ground for a single entity.
//!
//! **Semantics** — given an entity and an optional window `[start_ns, end_ns)`, select every
//! `Transform3D` row for that entity within the window (half-open, same as `range_scan`),
//! sort by `t_ns`, and emit one row per consecutive pair containing:
//!
//! - `t_start_ns`, `t_end_ns`: the pair's endpoints,
//! - `dt_ns`: `t_end_ns - t_start_ns` (strictly positive; pairs with `dt_ns == 0` are skipped
//!   because speed is undefined at zero elapsed time),
//! - `distance_m`: Euclidean distance between the two poses in the Transform3D coordinate
//!   frame,
//! - `speed_mps`: `distance_m / (dt_ns * 1e-9)`.
//!
//! **Unit claim** — the unit label "m" and "m/s" is only honest if the Transform3D `tx/ty/tz`
//! components are in meters. The prototype's synthetic generator emits meters, and this is
//! called out in the paper's Data Model section. No unit conversion is performed here.
//!
//! **Ties at identical `t_ns`** — two samples sharing `t_ns` produce a pair with `dt_ns == 0`
//! which is skipped. Upstream tie-break follows `latest_at`'s contract (later-inserted wins)
//! only when a single sample is needed; here we take all samples and simply skip zero-dt
//! pairs rather than pick a winner.

use std::sync::Arc;
use std::time::Instant;

use anyhow::Result;
use arrow_array::{Float64Array, Int64Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use mqd_core::{ComponentKind, EntityPath};

use crate::query::{range_scan, QueryPlan, RangeScanQuery};
use crate::store::StoreSnapshot;

#[derive(Clone, Debug)]
pub struct SpeedQuery {
    pub entity: EntityPath,
    /// Inclusive lower bound. `None` means "from the earliest known sample".
    pub start_ns: Option<i64>,
    /// Exclusive upper bound. `None` means "to the latest known sample + 1".
    pub end_ns: Option<i64>,
}

#[derive(Debug)]
pub struct SpeedResult {
    pub batch: RecordBatch,
    pub plan: QueryPlan,
    /// Number of Transform3D samples considered after the window filter.
    pub considered_samples: u64,
}

pub fn speed_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("entity_path", DataType::Utf8, false),
        Field::new("t_start_ns", DataType::Int64, false),
        Field::new("t_end_ns", DataType::Int64, false),
        Field::new("dt_ns", DataType::Int64, false),
        Field::new("distance_m", DataType::Float64, false),
        Field::new("speed_mps", DataType::Float64, false),
    ]))
}

pub fn speed_over_ground(snapshot: &StoreSnapshot, query: SpeedQuery) -> Result<SpeedResult> {
    let start = Instant::now();

    let start_ns = query.start_ns.unwrap_or(i64::MIN);
    let end_ns = query.end_ns.unwrap_or(i64::MAX);
    if start_ns > end_ns {
        anyhow::bail!("speed_over_ground: start_ns ({start_ns}) must be <= end_ns ({end_ns})");
    }

    let range = range_scan(
        snapshot,
        RangeScanQuery {
            kind: ComponentKind::Transform3D,
            start_ns,
            end_ns,
            entity: Some(query.entity.clone()),
        },
    )?;
    let mut plan = QueryPlan::new("speed_over_ground");
    plan.candidate_partitions = range.plan.candidate_partitions;
    plan.pruned_by_kind = range.plan.pruned_by_kind;
    plan.pruned_by_time = range.plan.pruned_by_time;
    plan.scanned_partitions = range.plan.scanned_partitions;
    plan.scanned_rows = range.plan.scanned_rows;

    let n = range.batch.num_rows();
    if n < 2 {
        plan.returned_rows = 0;
        plan.latency_us = start.elapsed().as_micros();
        return Ok(SpeedResult {
            batch: RecordBatch::new_empty(speed_schema()),
            plan,
            considered_samples: n as u64,
        });
    }

    let t_col = range
        .batch
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("t_ns column");
    let tx_col = column_f64(&range.batch, "tx")?;
    let ty_col = column_f64(&range.batch, "ty")?;
    let tz_col = column_f64(&range.batch, "tz")?;

    // Sort indices by t_ns (stable) — range_scan preserves insertion order but does not
    // guarantee sorted t_ns across partition boundaries. A stable sort keeps tied t_ns in
    // insertion order, which is the same total order latest_at defines.
    let mut idx: Vec<usize> = (0..n).collect();
    idx.sort_by(|&a, &b| t_col.value(a).cmp(&t_col.value(b)));

    let entity_str = query.entity.as_str();
    let mut entity_out: Vec<&str> = Vec::new();
    let mut t_start_out: Vec<i64> = Vec::new();
    let mut t_end_out: Vec<i64> = Vec::new();
    let mut dt_out: Vec<i64> = Vec::new();
    let mut dist_out: Vec<f64> = Vec::new();
    let mut speed_out: Vec<f64> = Vec::new();

    for pair in idx.windows(2) {
        let i = pair[0];
        let j = pair[1];
        let t0 = t_col.value(i);
        let t1 = t_col.value(j);
        let dt = t1 - t0;
        if dt <= 0 {
            continue;
        }
        let dx = tx_col.value(j) - tx_col.value(i);
        let dy = ty_col.value(j) - ty_col.value(i);
        let dz = tz_col.value(j) - tz_col.value(i);
        let dist = (dx * dx + dy * dy + dz * dz).sqrt();
        let dt_s = (dt as f64) * 1e-9;
        let speed = dist / dt_s;

        entity_out.push(entity_str);
        t_start_out.push(t0);
        t_end_out.push(t1);
        dt_out.push(dt);
        dist_out.push(dist);
        speed_out.push(speed);
    }

    let schema = speed_schema();
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(arrow_array::StringArray::from(entity_out)),
            Arc::new(Int64Array::from(t_start_out)),
            Arc::new(Int64Array::from(t_end_out)),
            Arc::new(Int64Array::from(dt_out)),
            Arc::new(Float64Array::from(dist_out)),
            Arc::new(Float64Array::from(speed_out)),
        ],
    )?;

    plan.returned_rows = batch.num_rows() as u64;
    plan.latency_us = start.elapsed().as_micros();

    Ok(SpeedResult {
        batch,
        plan,
        considered_samples: n as u64,
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
    use mqd_core::{ComponentName, Event, Payload, Timeline};

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

    fn speed_col(batch: &RecordBatch) -> Vec<f64> {
        let c = batch
            .column_by_name("speed_mps")
            .unwrap()
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        (0..batch.num_rows()).map(|i| c.value(i)).collect()
    }

    #[test]
    fn speed_constant_one_meter_per_second() {
        // Agent moves 1 m east per second for 10 ticks at 1 Hz (t_ns increments by 1e9).
        let store = Store::new(DEFAULT_BUCKET_NS);
        let events: Vec<Event> = (0..=10)
            .map(|i| {
                pose(
                    "/agent/1",
                    i as i64 * 1_000_000_000,
                    i as f64, // tx = i meters
                    0.0,
                    0.0,
                )
            })
            .collect();
        ingest(&store, &events);
        let snap = store.snapshot();
        let result = speed_over_ground(
            &snap,
            SpeedQuery {
                entity: EntityPath::new("/agent/1").unwrap(),
                start_ns: None,
                end_ns: None,
            },
        )
        .unwrap();
        assert_eq!(result.batch.num_rows(), 10, "expect 10 consecutive pairs");
        for v in speed_col(&result.batch) {
            assert!((v - 1.0).abs() < 1e-9, "expected 1.0 m/s, got {v}");
        }
        assert_eq!(result.considered_samples, 11);
    }

    #[test]
    fn speed_fewer_than_two_samples_returns_empty() {
        let store = Store::new(DEFAULT_BUCKET_NS);
        ingest(&store, &[pose("/agent/1", 100, 0.0, 0.0, 0.0)]);
        let snap = store.snapshot();
        let result = speed_over_ground(
            &snap,
            SpeedQuery {
                entity: EntityPath::new("/agent/1").unwrap(),
                start_ns: None,
                end_ns: None,
            },
        )
        .unwrap();
        assert_eq!(result.batch.num_rows(), 0);
    }

    #[test]
    fn speed_respects_half_open_window() {
        let store = Store::new(DEFAULT_BUCKET_NS);
        ingest(
            &store,
            &[
                pose("/agent/1", 0, 0.0, 0.0, 0.0),
                pose("/agent/1", 1_000_000_000, 1.0, 0.0, 0.0),
                pose("/agent/1", 2_000_000_000, 2.0, 0.0, 0.0),
                pose("/agent/1", 3_000_000_000, 3.0, 0.0, 0.0),
            ],
        );
        let snap = store.snapshot();
        // Window [1s, 3s) selects samples at 1s and 2s → one pair.
        let result = speed_over_ground(
            &snap,
            SpeedQuery {
                entity: EntityPath::new("/agent/1").unwrap(),
                start_ns: Some(1_000_000_000),
                end_ns: Some(3_000_000_000),
            },
        )
        .unwrap();
        assert_eq!(result.batch.num_rows(), 1);
    }

    #[test]
    fn speed_skips_zero_dt_duplicates() {
        let store = Store::new(DEFAULT_BUCKET_NS);
        ingest(
            &store,
            &[
                pose("/agent/1", 100, 0.0, 0.0, 0.0),
                pose("/agent/1", 100, 5.0, 0.0, 0.0), // duplicate t_ns
                pose("/agent/1", 1_000_000_100, 1.0, 0.0, 0.0),
            ],
        );
        let snap = store.snapshot();
        let result = speed_over_ground(
            &snap,
            SpeedQuery {
                entity: EntityPath::new("/agent/1").unwrap(),
                start_ns: None,
                end_ns: None,
            },
        )
        .unwrap();
        // Three samples, but the 100↔100 pair is skipped (dt=0). Expect one real pair.
        assert_eq!(result.batch.num_rows(), 1);
    }

    #[test]
    fn speed_filters_to_requested_entity() {
        let store = Store::new(DEFAULT_BUCKET_NS);
        ingest(
            &store,
            &[
                pose("/agent/1", 0, 0.0, 0.0, 0.0),
                pose("/agent/2", 500_000_000, 100.0, 0.0, 0.0),
                pose("/agent/1", 1_000_000_000, 1.0, 0.0, 0.0),
            ],
        );
        let snap = store.snapshot();
        let result = speed_over_ground(
            &snap,
            SpeedQuery {
                entity: EntityPath::new("/agent/1").unwrap(),
                start_ns: None,
                end_ns: None,
            },
        )
        .unwrap();
        assert_eq!(result.batch.num_rows(), 1);
        assert!((speed_col(&result.batch)[0] - 1.0).abs() < 1e-9);
    }
}
