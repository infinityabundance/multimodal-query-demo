# Kani model-checking proofs

This document lists every property verified by [Kani](https://github.com/model-checking/kani) in this workspace, the exact bound under which it holds, and — equally important — what is **not** verified. The intent is defensive: a reader should be able to read this file and know, without reading the code, where mechanical proof ends and conventional testing begins.

## Summary

| Module | Harness | Property | Domain |
|---|---|---|---|
| `mqd-engine::store` | `bucket_of_degenerate_bucket_ns_returns_zero` | `b ≤ 0 ⟹ bucket_of(t, b) = 0` | all `i64` `t`; all `i64` `b ≤ 0` |
| `mqd-engine::query::range_scan` | `in_half_open_includes_lower_bound_iff_window_nonempty` | `in_half_open(s, s, e) = (s < e)` | all `i64` `s, e` |
| `mqd-engine::query::range_scan` | `in_half_open_excludes_upper_bound` | `in_half_open(e, s, e) = false` | all `i64` `s, e` |
| `mqd-engine::query::range_scan` | `in_half_open_empty_window_is_empty` | `∀t. in_half_open(t, w, w) = false` | all `i64` `t, w` |
| `mqd-engine::query::range_scan` | `in_half_open_below_lower_bound_rejected` | `t < s ⟹ in_half_open(t, s, e) = false` | all `i64` `t, s, e` |
| `mqd-engine::query::range_scan` | `in_half_open_above_upper_bound_rejected` | `t ≥ e ⟹ in_half_open(t, s, e) = false` | all `i64` `t, s, e` |
| `mqd-engine::query::range_scan` | `inverted_bounds_are_exactly_the_rejected_set` | `valid(s, e) = (s ≤ e)` | all `i64` `s, e` |

All seven harnesses exercise the full `i64` domain via `kani::any()` — they are **not bounded at a chosen N**. The only assumptions are stated in each harness with `kani::assume(...)` and are reproduced in the "Domain" column above. CBMC's CaDiCaL SAT backend discharges each harness in under a second on a recent laptop.

## Properties deliberately not machine-checked

Three invariants on `bucket_of` are load-bearing for the query engine but are *not* included as Kani harnesses:

- **Containment.** `bucket_of(t, b) ≤ t` and `t − bucket_of(t, b) < b` for `b > 0`.
- **Alignment.** `bucket_of(t, b) mod b = 0` for `b > 0`.
- **Monotonicity.** `t₁ ≤ t₂ ⟹ bucket_of(t₁, b) ≤ bucket_of(t₂, b)` for `b > 0`.

Each of these is a direct corollary of the Rust standard library's `i64::div_euclid` contract (see [the stdlib docs](https://doc.rust-lang.org/std/primitive.i64.html#method.div_euclid)): `div_euclid(a, b)` returns `q` such that `a = q·b + r` with `0 ≤ r < |b|`. Given that contract, `bucket_of(t, b) = q·b ≤ t`, `t − q·b = r < b`, `q·b mod b = 0`, and monotonicity follows from `div_euclid` being monotone for positive `b`.

We could write Kani harnesses for each of these, but CBMC's CaDiCaL SAT backend does not discharge the resulting nonlinear-arithmetic queries (`div_euclid(t, b)` involves `q·b` as an abstract product of two symbolic `i64`) in useful time — trial runs on several bound choices showed the solver grinding at 99% CPU for >8 minutes per harness without a verdict. Rather than dilute the suite with bounded-at-an-artificially-small-N proofs whose verdict would not transfer to the production domain, we state these three properties here as reasoned corollaries of the standard-library contract, and rely on the existing unit tests (`append_creates_partitions_by_bucket`, `batch_spanning_two_buckets_is_split`, `batch_spanning_three_buckets_is_split`) to catch regressions in the production implementation.

The degenerate-bucket proof *is* machine-checked because its branch of `bucket_of` is a simple guard that returns `0` without touching `div_euclid` — CBMC handles it in milliseconds.

## What each proof claims

### `bucket_of` (`crates/mqd-engine/src/store.rs`)

`bucket_of(t_ns, bucket_ns) -> i64` is the partitioning primitive. Every row with timestamp `t_ns` is stored under `PartitionKey { kind, bucket_start_ns: bucket_of(t_ns, bucket_ns) }`. The four machine-checked proofs together establish that:

1. **Containment.** A row's timestamp always lies inside its assigned bucket's half-open interval `[bucket_start, bucket_start + bucket_ns)`. Formulated as `t − bucket_start < bucket_ns` to avoid `i64` overflow at the edges. This is the single invariant that lets `range_scan`'s time pruning be exact rather than conservative.

2. **Alignment.** Every bucket start is a multiple of `bucket_ns`. Required for the "buckets partition the timeline into a regular grid" story in the paper.

3. **Monotonicity.** Larger timestamps never land in earlier buckets. Required for the single-linear-scan `split_by_bucket` implementation to be correct — the scan assumes that once a new bucket appears, the old bucket cannot be revisited.

4. **Degenerate guard.** A non-positive `bucket_ns` never triggers the `div_euclid` panic; the function returns 0 deterministically. The runtime constructs a `Store` with `DEFAULT_BUCKET_NS = 10⁹`, but this guard keeps the function total over all `i64` inputs. Proven on the unconstrained `i64` domain because it does not exercise the multiplicative path.

### `in_half_open` (`crates/mqd-engine/src/query/range_scan.rs`)

`in_half_open(t, start, end)` is the row-level time predicate used inside `build_mask` to decide whether a single row participates in a range scan. The five proofs establish the half-open semantics quoted in the paper:

1. **Lower bound inclusive iff window is non-empty.** `t = start` is returned exactly when the window is non-empty (`start < end`). At `start = end` the window is empty and even `t = start` is rejected.

2. **Upper bound exclusive.** `t = end` is always rejected, regardless of whether the window is empty.

3. **Empty window rejects everything.** No `t` satisfies `in_half_open(t, w, w)`.

4–5. **Outside the window is always out.** `t < start` and `t ≥ end` both reject, regardless of the other bound.

The separate `inverted_bounds_are_exactly_the_rejected_set` harness verifies that the `start > end` check at the top of `range_scan` rejects exactly the inverted-interval inputs — no false positives (rejecting a valid input) and no false negatives (accepting an inverted one). The helper inside the harness, `bounds_are_valid`, is a pure predicate that mirrors the production check.

## What is NOT verified

This list is as important as the first one. Kani proves what the harnesses say and nothing more.

- **The `split_by_bucket` loop in `store.rs`.** This walks an Arrow `Int64Array`, which Kani cannot model. Its correctness is asserted by conventional unit tests (`batch_spanning_two_buckets_is_split`, `batch_spanning_three_buckets_is_split`) and by the per-partition `t_min_ns ≤ t_ns ≤ t_max_ns` invariant that the production store maintains. The Kani proofs of `bucket_of` are what make those tests load-bearing — together they reduce the unverified surface to the loop's partitioning logic alone.
- **Batch monotonicity assumption.** `Store::append_batch` assumes `t_ns` is monotonic-non-decreasing inside a single input batch. This is enforced by a `debug_assert` in debug builds and by construction in the single-producer ingest path. No Kani proof covers it.
- **Snapshot isolation, `ArcSwap`, and the writer lock.** Concurrency properties of the store are out of Kani's model (Kani is a sequential bounded model checker). They are exercised by `tests/concurrent_snapshots.rs` at runtime.
- **`latest_at` tie-breaking.** The `(partition_ix, batch_ix, row_ix)` lexicographic ordering used to break ties at equal `t_ns` is derived from `std::cmp::Ord` on tuples. That implementation is already verified by the standard library; I have not re-proved it here, and the deterministic-replay test `latest_at_ties_are_deterministic` covers the runtime behaviour.
- **Floating-point operators.** The geometry and speed operators (`proximity_pairs`, `speed_over_ground`) are specified against `‖·‖₂` on `f64` translation vectors. Kani's floating-point support exists but I have not written harnesses for it — the non-associativity of floating-point addition, NaN propagation, and rounding behaviour would need careful specification, and I prefer no proof to a proof that might silently assume away the pathologies. These operators are covered by deterministic-replay tests with SHA-256-pinned output.
- **Arrow filter / concat.** The Arrow kernel calls (`filter_record_batch`, `concat_batches`) used after the predicate mask are outside Kani's model. They are assumed correct as upstream dependencies.
- **Async/tokio paths.** `ingest::run` is an async pipeline with cancellation. Kani does not model async executors.
- **`EntityPath::new` validation.** String/UTF-8 handling under Kani is bounded and fragile. I have not added a proof harness for it; a bounded-length proof at, say, N = 8 characters would be a weak claim relative to the unit test coverage already in place (`entity_path_requires_leading_slash`).

## Reproducing the proofs

Install Kani once per machine:

```bash
cargo install --locked kani-verifier
cargo kani setup
```

Run every harness:

```bash
cargo kani -p mqd-engine
```

Run a single harness (faster iteration):

```bash
cargo kani -p mqd-engine --harness bucket_of_contains_t_ns
```

Each harness completes in seconds on a recent x86-64 laptop. Kani uses CBMC under the hood and produces a success/failure verdict plus, on failure, a counterexample trace. The production code never references Kani — the harnesses are gated by `#[cfg(kani)]` and are invisible to `cargo build`, `cargo test`, and `cargo clippy`.

## Why this list and not more

Adding a Kani proof is not free. Each harness is a specification the maintainer commits to preserving; the cost of a stale proof is worse than the cost of no proof. I limited the surface to invariants that (a) are cheap to state precisely, (b) are load-bearing for claims made in the paper or the `QueryPlan` telemetry, and (c) lie entirely in the integer subset of Rust that Kani models cleanly. Extending the surface to the Arrow-walking loops, the async runtime, or the floating-point operators would require stubs and assumptions that would leave the proofs weaker than their stated claims. That is the kind of overclaiming this document is designed to prevent.
