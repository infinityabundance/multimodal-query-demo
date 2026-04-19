# multimodal-query-demo

[![Open in Colab](https://colab.research.google.com/assets/colab-badge.svg)](https://colab.research.google.com/github/infinityabundance/multimodal-query-demo/blob/main/notebooks/reproduce.ipynb)

**Status**: bounded prototype, `v0.1.0`.

An Arrow-native, tokio-based Rust workspace for querying **sparse multi-rate multimodal temporal event streams** — the shape of data produced by robotics stacks, sensor fusion pipelines, and scientific instrumentation, where a 200 Hz IMU, a 30 Hz camera, and a 1 Hz annotator share one event-time timeline without sharing a clock.

The artefact is deliberately small and deliberately finished. It demonstrates a concrete implementation of point-in-time query semantics, async snapshot-isolated reads, and two domain operators over Apache Arrow `RecordBatch`es. It is not a database. The numbered non-claims charter in [`crates/mqd-core/src/non_claims.rs`](crates/mqd-core/src/non_claims.rs) is pinned both in code and in the paper, enforced by [`tests/non_claim_lock.rs`](crates/mqd-core/tests/non_claim_lock.rs) — see [`SCOPE.md`](SCOPE.md) for the explicit out-of-scope list.

## What this code actually does

Events arrive through a `tokio::sync::mpsc` channel from one or more producer tasks. A single flusher task drains the channel into per-kind Arrow `RecordBatch` builders, then hands each batch to a store partitioned by `(ComponentKind, bucket_start_ns)`. The store swaps its `ArcSwap<StoreSnapshot>` pointer atomically on each append; readers call `store.snapshot()` to get an `Arc<StoreSnapshot>` in O(1) and operate on an immutable view for the lifetime of their query. A `CancellationToken` wired through the ingest loop guarantees exactly one final `flush_all` on clean exit or cancellation.

Five typed component kinds are modelled: `Transform3D` (pose + unit quaternion), `Points3D` (variable-length point cloud), `Scalar` (numeric with optional unit), `ImageRef` (URI + dimensions — no pixels), and `Annotation` (string label). Every row carries the non-null prefix `t_ns: Int64, entity_path: Utf8`.

## The math and the invariants

Let `b` be the bucket width in nanoseconds (`DEFAULT_BUCKET_NS = 10^9`, one second). For any bucket `B_k = [kb, (k+1)b)`, every partition entry placed under `(kind, kb)` has every row's `t_ns` in `B_k`. This is enforced by splitting any input `RecordBatch` whose `t_ns` range spans a boundary into the minimum number of sub-batches needed. The consequence: time-range pruning on `range_scan` is exact and arithmetic — a bucket is either fully outside `[s, e)`, fully inside, or the edge case of partial overlap (which is still dominated by one bucket's width).

**`latest_at(at_ns, kind, entity?)`.** For each matching entity, return the single row

```
argmax { t_ns(r) : t_ns(r) ≤ at_ns, kind(r) = kind, entity_path(r) = entity? }
```

Ties at equal `t_ns` are broken by lexicographic order on `(partition_ix, batch_ix, row_ix)` — the **later-inserted** row wins. This works because equal `t_ns` values necessarily land in the same bucket, so the three-tuple is a total order within any tie set. No dedicated sequence column is needed. The contract is pinned by `latest_at_ties_are_deterministic`.

**`range_scan(start_ns, end_ns, kind, entity?)`.** Return every row satisfying

```
start_ns ≤ t_ns(r) < end_ns
```

Half-open, matching Rust's `Range<i64>`. Inverted bounds return a typed error.

**`proximity_pairs(at_ns, radius)`.** Resolve every entity's latest `Transform3D` via an internal `latest_at` call, then emit every unordered pair `{a, b}` such that

```
‖p_a − p_b‖₂ ≤ radius
```

where `p_e` is the `(tx, ty, tz)` translation vector. This is `O(N²)` in the number of distinct entities present at `at_ns`; no spatial index is used. The complexity is intentional and called out in the paper's §Limitations.

**`speed_over_ground(entity, start_ns?, end_ns?)`.** Pull the entity's `Transform3D` rows (internally a `range_scan`), stable-sort by `t_ns`, and for each consecutive pair `(i, i+1)` with `Δt = t_{i+1} − t_i > 0` emit

```
distance_m  = ‖p_{i+1} − p_i‖₂
speed_mps   = distance_m / (Δt / 10^9)
```

Pairs with `Δt ≤ 0` are skipped — they are duplicate-timestamp noise, not a semantic event. The consequence: the output has at most `n − 1` rows for `n` input samples, minus duplicates.

**Query telemetry.** Both `latest_at` and `range_scan` return a `QueryPlan` alongside the result: `candidate_partitions`, `pruned_by_kind`, `pruned_by_time`, `scanned_partitions`, `scanned_rows`, `returned_rows`, `latency_us`. Under `--explain` these are serialised in the reporter output so a caller can see the engine's work — not just the wall clock.

## Quickstart

```bash
cargo run -p mqd-cli --release -- generate --seed 42 --agents 8 --ticks 10000 --out data/run.jsonl
cargo run -p mqd-cli --release -- ingest --input data/run.jsonl --batch-rows 4096
cargo run -p mqd-cli --release -- query latest-at --input data/run.jsonl --at 25000000000 --explain
cargo run -p mqd-cli --release -- op proximity --input data/run.jsonl --at 25000000000 --radius 2.5 --json | jq .
cargo run -p mqd-cli --release -- non-claims
```

Prefix `--json` before any subcommand to switch the reporter to NDJSON:

```bash
cargo run -p mqd-cli --release -- --json ingest --input data/run.jsonl | jq .
```

One-click reproducibility from a clean VM: [`notebooks/reproduce.ipynb`](notebooks/reproduce.ipynb).

## Verification

```bash
cargo test --workspace
cargo clippy --workspace --all-targets -- -D warnings
cargo fmt --check
```

39+ tests: the engine's unit tests; `tests/concurrent_snapshots.rs` (reader/writer stress with a background flusher); `tests/deterministic_replay.rs` (SHA-256 pinned generator + proximity output); `tests/non_claim_lock.rs` (parses the paper, asserts each `\item` matches `NON_CLAIMS` verbatim).

## Paper

```bash
./paper/build.sh   # produces paper/multimodal-query-demo.pdf
```

Figures regenerate from a single `out/bench.json` produced by `mqd bench`, then rendered by `mqd figs`. The paper's §Reproducibility lists the exact commands.

## The four crates

### [`mqd-core`](crates/mqd-core/) — types, schemas, traits · [![crates.io](https://img.shields.io/crates/v/mqd-core.svg)](https://crates.io/crates/mqd-core)

**What.** `EntityPath` (validated slash-delimited hierarchical path), `ComponentKind` enum, Arrow `Schema` factory per kind, `Event` (the ingest unit), `StatusReporter` trait with `TerminalReporter` / `JsonReporter` / `NullReporter`, and the numbered `NON_CLAIMS` constant.

**Why.** Pure types and traits in a leaf crate. No tokio, no I/O, no dependency on the engine. This lets `mqd-engine` be fully async without contaminating the testability of the type layer, and lets downstream consumers depend on the vocabulary without pulling in the runtime. The `StatusReporter` trait is threaded through engine calls as `&dyn StatusReporter` (for functions) or `Arc<dyn StatusReporter>` (for long-lived tasks) so phase/counter/summary output is decoupled from whether the caller wants a terminal formatter or NDJSON.

**How to use.**

```rust
use mqd_core::{EntityPath, ComponentKind, schema::schema_for};

let path = EntityPath::new("/agent/0").unwrap();
let schema = schema_for(ComponentKind::Transform3D);
assert_eq!(schema.field(0).name(), "t_ns");
```

### [`mqd-engine`](crates/mqd-engine/) — ingest, store, query, ops · [![crates.io](https://img.shields.io/crates/v/mqd-engine.svg)](https://crates.io/crates/mqd-engine)

**What.** The async ingest pipeline (`ingest::run` with producer/flusher topology and `CancellationToken`-driven shutdown), the snapshot-isolated `Store`, the `latest_at` / `range_scan` query implementations with `QueryPlan` telemetry, and the two domain operators (`proximity_pairs`, `speed_over_ground`).

**Why.** One crate for everything that touches Arrow `RecordBatch` data in motion. Ingest and store cannot be meaningfully separated without a durability boundary, which this prototype explicitly does not provide. Query and operators live next to the store because they need efficient access to the partition index; splitting them would require exposing internal indices as a public API for no gain.

**How to use.**

```rust
use std::sync::Arc;
use mqd_core::{ComponentKind, EntityPath};
use mqd_engine::{
    store::{Store, DEFAULT_BUCKET_NS},
    query::{latest_at, LatestAtQuery},
};

let store = Arc::new(Store::new(DEFAULT_BUCKET_NS));
// ... append batches ...
let snap = store.snapshot();
let result = latest_at(&snap, LatestAtQuery {
    kind: ComponentKind::Transform3D,
    at_ns: 5_000_000_000,
    entity: Some(EntityPath::new("/agent/0").unwrap()),
})?;
println!("rows={} scanned_partitions={}", result.batch.num_rows(), result.plan.scanned_partitions);
```

### [`mqd-cli`](crates/mqd-cli/) — the `mqd` binary · [![crates.io](https://img.shields.io/crates/v/mqd-cli.svg)](https://crates.io/crates/mqd-cli)

**What.** clap-based subcommands: `generate` (seeded synthetic event stream), `ingest` (JSONL → store, with `--batch-rows`), `query latest-at` / `query range` (with `--explain`), `op proximity` / `op speed`, `bench` (single-file JSON benchmark report), `figs` (PNGs from that report), and `non-claims` (prints the numbered charter). Reporter switch: `--json` before any subcommand emits NDJSON.

**Why.** One binary is the friendly surface for the whole workspace — a reviewer can exercise every engine path without writing Rust. The `--explain` flag on queries serialises the `QueryPlan` so the reader sees kind/time pruning and partition scan counts. `--json` makes every command pipeable into `jq`. Kept thin: every subcommand is a small `commands/<name>.rs` wrapper around an engine call.

**How to use.**

```bash
cargo run -p mqd-cli --release -- query range \
  --input data/run.jsonl --start 0 --end 10000000000 \
  --entity /agent/0 --kind transform3d --explain
```

### [`mqd-bench`](crates/mqd-bench/) — criterion benches + shared fixtures · [![crates.io](https://img.shields.io/crates/v/mqd-bench.svg)](https://crates.io/crates/mqd-bench)

**What.** Three criterion benches (`ingest_throughput`, `query_latency`, `ops_scale`) plus a shared `Fixture { seed, agents, ticks }` helper consumed by both the criterion harnesses and the `mqd bench` CLI subcommand.

**Why.** Keeps the seeded-workload construction in one place so criterion benches and the single-file `mqd bench` JSON report measure the same thing. The CLI bench is what the paper reads; criterion is for when you want cargo-bench statistics and flamegraphs.

**How to use.**

```bash
cargo bench -p mqd-bench
cargo run -p mqd-cli --release -- bench --out out/bench.json --samples 51
cargo run -p mqd-cli --release -- figs --from out/bench.json --out paper/figures/
```

## Query semantics — quick reference

- `latest_at(at_ns, kind, entity?)` — for each matching entity, the single row with the **largest `t_ns ≤ at_ns`**. Ties broken by lexicographic `(partition_ix, batch_ix, row_ix)` — later-inserted wins. Enforced by `latest_at_ties_are_deterministic`.
- `range_scan(start_ns, end_ns, kind, entity?)` — every row with `start_ns ≤ t_ns < end_ns` (half-open). Inverted bounds error.
- Both queries surface a `QueryPlan` with `candidate_partitions`, `pruned_by_{kind,time}`, `scanned_{partitions,rows}`, `returned_rows`, `latency_us` — serialised under `--explain`.
- Snapshot isolation: a reader's `Arc<StoreSnapshot>` is frozen for the duration of the query regardless of concurrent writes. Verified by `tests/concurrent_snapshots.rs`.

## Author

Riaan de Beer · [predictiverendezvous@proton.me](mailto:predictiverendezvous@proton.me)

Released under [Apache-2.0](LICENSE).
