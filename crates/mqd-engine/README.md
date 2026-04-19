# mqd-engine

Async ingest pipeline, snapshot-isolated store, point-in-time and range queries with query-plan telemetry, and two domain operators for the [multimodal-query-demo](https://github.com/infinityabundance/multimodal-query-demo) workspace.

One crate for everything that touches Arrow `RecordBatch` data in motion. Depends only on [`mqd-core`](https://crates.io/crates/mqd-core) for types and schemas.

## What's here

- **`ingest::run`** — producer/flusher topology over `tokio::sync::mpsc`, with `tokio_util::sync::CancellationToken`-driven shutdown that guarantees exactly one final `flush_all` on clean exit or cancellation.
- **`store::Store`** — partitioned by `(ComponentKind, bucket_start_ns)`, single-writer / lock-free reader via `arc_swap::ArcSwap<StoreSnapshot>`. Input batches spanning a bucket boundary are split so every partition's `t_ns` range fits within one bucket — making range-scan time pruning exact.
- **`query::latest_at`** — for each matching entity, the single row with the largest `t_ns ≤ at_ns`. Ties at equal `t_ns` are broken by lexicographic `(partition_ix, batch_ix, row_ix)` order; later-inserted wins. No sequence column needed.
- **`query::range_scan`** — every row with `start_ns ≤ t_ns < end_ns` (half-open, matching Rust's `Range`). Inverted bounds error.
- **`QueryPlan`** — every query returns `candidate_partitions`, `pruned_by_kind`, `pruned_by_time`, `scanned_partitions`, `scanned_rows`, `returned_rows`, `latency_us` so callers can see the engine's work, not just the wall clock.
- **`ops::proximity_pairs`** — at time `t`, every unordered pair of entities within `radius`. O(N²) in distinct entities, by design.
- **`ops::speed_over_ground`** — for one entity over an optional half-open window, consecutive-pair `distance_m / Δt` with `Δt > 0`.

## Example

```rust
use std::sync::Arc;
use mqd_core::{ComponentKind, EntityPath};
use mqd_engine::{
    store::{Store, DEFAULT_BUCKET_NS},
    query::{latest_at, LatestAtQuery},
};

let store = Arc::new(Store::new(DEFAULT_BUCKET_NS));
// ... append batches via store.append_batch(kind, batch) ...

let snap = store.snapshot();
let result = latest_at(&snap, LatestAtQuery {
    kind: ComponentKind::Transform3D,
    at_ns: 5_000_000_000,
    entity: Some(EntityPath::new("/agent/0").unwrap()),
})?;

println!(
    "rows={} scanned_partitions={} latency_us={}",
    result.batch.num_rows(),
    result.plan.scanned_partitions,
    result.plan.latency_us,
);
# Ok::<_, Box<dyn std::error::Error>>(())
```

## License

Apache-2.0.
