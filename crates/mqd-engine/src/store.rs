use std::collections::BTreeMap;
use std::sync::Arc;

use arc_swap::ArcSwap;
use arrow_array::{Int64Array, RecordBatch};
use mqd_core::ComponentKind;
use parking_lot::Mutex;

/// Partition key groups `RecordBatch`es by (component kind, coarse time bucket).
///
/// Time bucketing is heuristic and tuned for the prototype's pruning story — not a claim
/// about optimal layout.
#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct PartitionKey {
    pub kind: ComponentKind,
    pub bucket_start_ns: i64,
}

pub const DEFAULT_BUCKET_NS: i64 = 1_000_000_000; // 1 second

pub fn bucket_of(t_ns: i64, bucket_ns: i64) -> i64 {
    if bucket_ns <= 0 {
        return 0;
    }
    let q = t_ns.div_euclid(bucket_ns);
    q * bucket_ns
}

#[derive(Clone, Debug)]
pub struct Partition {
    pub key: PartitionKey,
    pub batches: Vec<RecordBatch>,
    pub rows: u64,
    pub t_min_ns: i64,
    pub t_max_ns: i64,
}

/// A shared immutable view of the store at a single moment. Readers hold an `Arc<StoreSnapshot>`
/// with no lock; concurrent writes produce new snapshots and swap atomically.
#[derive(Clone, Debug, Default)]
pub struct StoreSnapshot {
    pub partitions: BTreeMap<PartitionKey, Partition>,
    pub bucket_ns: i64,
}

impl StoreSnapshot {
    pub fn empty(bucket_ns: i64) -> Self {
        Self {
            partitions: BTreeMap::new(),
            bucket_ns,
        }
    }

    pub fn num_partitions(&self) -> usize {
        self.partitions.len()
    }

    pub fn total_rows(&self) -> u64 {
        self.partitions.values().map(|p| p.rows).sum()
    }

    pub fn batches_by_kind(&self) -> BTreeMap<ComponentKind, Vec<RecordBatch>> {
        let mut out: BTreeMap<ComponentKind, Vec<RecordBatch>> = BTreeMap::new();
        for (key, p) in &self.partitions {
            out.entry(key.kind)
                .or_default()
                .extend(p.batches.iter().cloned());
        }
        out
    }

    /// Partitions for a given kind, in key order.
    pub fn partitions_for(&self, kind: ComponentKind) -> Vec<&Partition> {
        self.partitions
            .iter()
            .filter(|(k, _)| k.kind == kind)
            .map(|(_, p)| p)
            .collect()
    }
}

/// Single-writer, multi-reader store. The write path serializes all appends behind a mutex
/// while readers access `Arc<StoreSnapshot>` lock-free via `ArcSwap`.
pub struct Store {
    current: ArcSwap<StoreSnapshot>,
    writer_lock: Mutex<()>,
    bucket_ns: i64,
}

impl Store {
    pub fn new(bucket_ns: i64) -> Self {
        Self {
            current: ArcSwap::new(Arc::new(StoreSnapshot::empty(bucket_ns))),
            writer_lock: Mutex::new(()),
            bucket_ns,
        }
    }

    pub fn bucket_ns(&self) -> i64 {
        self.bucket_ns
    }

    pub fn snapshot(&self) -> Arc<StoreSnapshot> {
        self.current.load_full()
    }

    /// Append a batch. Batches that span multiple buckets are split at bucket boundaries; each
    /// slice is assigned to its own partition. Assumes `t_ns` is monotonic-non-decreasing within
    /// the batch — enforced in debug builds by an assertion. This holds by construction for the
    /// single-producer, single-flusher ingest path, and for direct test inputs built in order.
    ///
    /// Copy-on-write: a new snapshot is built under the writer lock and atomically swapped.
    pub fn append_batch(&self, kind: ComponentKind, batch: RecordBatch) {
        if batch.num_rows() == 0 {
            return;
        }

        let t_col = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("first column must be t_ns:Int64");

        #[cfg(debug_assertions)]
        debug_assert_monotonic(t_col);

        let slices = split_by_bucket(t_col, self.bucket_ns);

        let _guard = self.writer_lock.lock();
        let current = self.current.load_full();
        let mut new = (*current).clone();

        for (bucket_start_ns, offset, length, t_min, t_max) in slices {
            let key = PartitionKey {
                kind,
                bucket_start_ns,
            };
            let sliced = batch.slice(offset, length);
            let partition = new
                .partitions
                .entry(key.clone())
                .or_insert_with(|| Partition {
                    key: key.clone(),
                    batches: Vec::new(),
                    rows: 0,
                    t_min_ns: t_min,
                    t_max_ns: t_max,
                });
            partition.rows += length as u64;
            partition.t_min_ns = partition.t_min_ns.min(t_min);
            partition.t_max_ns = partition.t_max_ns.max(t_max);
            partition.batches.push(sliced);
        }

        self.current.store(Arc::new(new));
    }
}

/// Walk the t_ns column once; emit `(bucket_start_ns, offset, length, t_min, t_max)` tuples.
/// Because the column is monotonic-non-decreasing, rows with equal buckets are contiguous, so
/// one linear scan suffices.
fn split_by_bucket(t_col: &Int64Array, bucket_ns: i64) -> Vec<(i64, usize, usize, i64, i64)> {
    let n = t_col.len();
    let mut out = Vec::new();
    if n == 0 {
        return out;
    }
    let mut start = 0usize;
    let mut cur_bucket = bucket_of(t_col.value(0), bucket_ns);
    let mut t_min = t_col.value(0);
    let mut t_max = t_col.value(0);
    for i in 1..n {
        let v = t_col.value(i);
        let b = bucket_of(v, bucket_ns);
        if b != cur_bucket {
            out.push((cur_bucket, start, i - start, t_min, t_max));
            start = i;
            cur_bucket = b;
            t_min = v;
            t_max = v;
        } else {
            if v < t_min {
                t_min = v;
            }
            if v > t_max {
                t_max = v;
            }
        }
    }
    out.push((cur_bucket, start, n - start, t_min, t_max));
    out
}

#[cfg(debug_assertions)]
fn debug_assert_monotonic(t_col: &Int64Array) {
    let mut prev = i64::MIN;
    for i in 0..t_col.len() {
        let v = t_col.value(i);
        debug_assert!(
            v >= prev,
            "Store::append_batch invariant violated: t_ns not monotonic non-decreasing \
             (index={i}, prev={prev}, cur={v}). Sort before appending."
        );
        prev = v;
    }
}

// ─────────────────────────────────────────────────────────────────────────────────────────────
// Kani proof harnesses for `bucket_of`.
//
// Scope: this file machine-checks one linear property of `bucket_of` — the degenerate-input
// guard. The three multiplicative invariants (containment, alignment, monotonicity) are
// corollaries of Rust's `i64::div_euclid` contract and are covered by conventional unit tests
// (`batch_spanning_two_buckets_is_split`, `batch_spanning_three_buckets_is_split`, and
// `append_creates_partitions_by_bucket`). They are NOT machine-checked here because CBMC's
// CaDiCaL SAT backend does not discharge the nonlinear `div_euclid + multiply` query at any
// `i64` bound large enough to be worth claiming. See `PROOFS.md` §"What is NOT verified" for
// the honest statement and references to the standard library's own `div_euclid` documentation
// for the contract we rely on.
// ─────────────────────────────────────────────────────────────────────────────────────────────
#[cfg(kani)]
mod kani_proofs {
    use super::bucket_of;

    #[kani::proof]
    fn bucket_of_degenerate_bucket_ns_returns_zero() {
        let t: i64 = kani::any();
        let b: i64 = kani::any();
        kani::assume(b <= 0);
        assert!(bucket_of(t, b) == 0);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ingest::batcher::KindBatcher;
    use mqd_core::{ComponentName, EntityPath, Event, Payload, Timeline};

    fn make_batch(kind: ComponentKind, base_ns: i64, rows: usize) -> RecordBatch {
        let mut b = KindBatcher::new(kind);
        for i in 0..rows {
            let ev = Event {
                entity_path: EntityPath::new(format!("/agent/{i}")).unwrap(),
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

    fn make_batch_with_times(kind: ComponentKind, times_ns: &[i64]) -> RecordBatch {
        let mut b = KindBatcher::new(kind);
        for (i, t) in times_ns.iter().enumerate() {
            let ev = Event {
                entity_path: EntityPath::new(format!("/agent/{i}")).unwrap(),
                timeline: Timeline::sim(),
                t_ns: *t,
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
            };
            b.append(&ev).unwrap();
        }
        b.drain().unwrap().unwrap()
    }

    #[test]
    fn append_creates_partitions_by_bucket() {
        let store = Store::new(DEFAULT_BUCKET_NS);
        store.append_batch(
            ComponentKind::Transform3D,
            make_batch(ComponentKind::Transform3D, 0, 3),
        );
        store.append_batch(
            ComponentKind::Transform3D,
            make_batch(ComponentKind::Transform3D, DEFAULT_BUCKET_NS, 3),
        );
        let snap = store.snapshot();
        assert_eq!(snap.num_partitions(), 2);
        assert_eq!(snap.total_rows(), 6);
    }

    #[test]
    fn snapshot_is_stable_across_appends() {
        let store = Store::new(DEFAULT_BUCKET_NS);
        store.append_batch(
            ComponentKind::Transform3D,
            make_batch(ComponentKind::Transform3D, 0, 3),
        );
        let before = store.snapshot();
        store.append_batch(
            ComponentKind::Transform3D,
            make_batch(ComponentKind::Transform3D, 100, 3),
        );
        // Old snapshot is stable.
        assert_eq!(before.total_rows(), 3);
        // New snapshot has 6.
        assert_eq!(store.snapshot().total_rows(), 6);
    }

    #[test]
    fn batch_spanning_two_buckets_is_split() {
        let store = Store::new(DEFAULT_BUCKET_NS);
        // Three rows in bucket 0, two rows in bucket 1.
        let times = &[
            0,
            DEFAULT_BUCKET_NS / 2,
            DEFAULT_BUCKET_NS - 1,
            DEFAULT_BUCKET_NS,
            DEFAULT_BUCKET_NS + 10,
        ];
        let batch = make_batch_with_times(ComponentKind::Transform3D, times);
        store.append_batch(ComponentKind::Transform3D, batch);
        let snap = store.snapshot();
        assert_eq!(
            snap.num_partitions(),
            2,
            "expected a split into two buckets"
        );
        let p0 = snap
            .partitions
            .get(&PartitionKey {
                kind: ComponentKind::Transform3D,
                bucket_start_ns: 0,
            })
            .unwrap();
        let p1 = snap
            .partitions
            .get(&PartitionKey {
                kind: ComponentKind::Transform3D,
                bucket_start_ns: DEFAULT_BUCKET_NS,
            })
            .unwrap();
        assert_eq!(p0.rows, 3);
        assert_eq!(p1.rows, 2);
        assert_eq!(p0.t_min_ns, 0);
        assert_eq!(p0.t_max_ns, DEFAULT_BUCKET_NS - 1);
        assert_eq!(p1.t_min_ns, DEFAULT_BUCKET_NS);
        assert_eq!(p1.t_max_ns, DEFAULT_BUCKET_NS + 10);
    }

    #[test]
    fn batch_spanning_three_buckets_is_split() {
        let store = Store::new(DEFAULT_BUCKET_NS);
        let b = DEFAULT_BUCKET_NS;
        let times = &[0, b / 2, b, b + b / 2, 2 * b, 2 * b + 10];
        let batch = make_batch_with_times(ComponentKind::Transform3D, times);
        store.append_batch(ComponentKind::Transform3D, batch);
        let snap = store.snapshot();
        assert_eq!(snap.num_partitions(), 3);
        assert_eq!(snap.total_rows(), 6);
    }
}
