# SCOPE

This file is committed before the first line of implementation so that scope cannot silently expand. It is a contract with the reader of this repository.

## In scope

- In-memory, single-process Rust prototype.
- Arrow-native chunked storage of sparse multi-rate multimodal temporal events.
- Async ingest: multiple concurrent producer tasks into a single flusher via `tokio::sync::mpsc`.
- Snapshot-isolated reads over an atomically swapped `Arc<StoreSnapshot>`.
- Typed query surface exposed through a CLI:
  - Point-in-time (latest-at-style) resolution, per entity and per component.
  - Time-range scan with time-bucket and kind pruning.
  - Two domain operators: `proximity_pairs`, `speed_over_ground`.
- A `--explain` flag that reports pruning decisions and per-phase row counts.
- A `--json` reporter that emits NDJSON to stdout.
- Seeded deterministic synthetic data generator with SHA256 fingerprint replay.
- Benchmarks for ingest throughput, point-in-time query latency, range-scan, and proximity scaling.
- A LaTeX design note (`paper/`) that compiles to PDF via `./paper/build.sh`.
- A `Non-Claims` list enforced by a compile-time lock test tying code, CLI output, and paper together.

## Out of scope

The following are explicitly not implemented and will not be added within the initial tag. Any temptation to add them mid-build is deferred to future work.

- SQL parsing or integration with any third-party query engine.
- Byte-level decoding of image or mesh component payloads. Image payloads are represented as typed metadata references only.
- Network protocols, client libraries, or wire-format compatibility with any specific external system.
- Disk persistence. An optional `--dump out.arrow` stretch goal using Arrow IPC may ship if time allows; it is the first thing cut on slip.
- Multi-timeline joins beyond per-entity point-in-time resolution on a single timeline.
- Distributed or multi-node execution.
- Claims of compatibility, parity, or equivalence with any specific production system.
- GPU kernels, SIMD hand-tuning, or jemalloc swapping.
- A custom memory allocator.
- Writable HTTP surface or embedded web UI.

## Non-negotiable rules

1. Benchmark numbers are prototype signal only. They are not throughput, latency, or scale claims against any other system.
2. The `Non-Claims` list in `crates/mqd-core/src/non_claims.rs` must match the paper's enumerate block verbatim. The lock test fails otherwise.
3. No `unsafe` blocks without an accompanying safety comment and a justification in the paper's Implementation Notes.
4. No `HashMap` iteration on any deterministic output path. Use `BTreeMap` or pre-sort.
5. Every figure in the paper must regenerate from `mqd bench` output via `mqd figs`.
