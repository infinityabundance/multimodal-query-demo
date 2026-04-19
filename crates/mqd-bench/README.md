# mqd-bench

Criterion benches and the shared `Fixture` helper for the [multimodal-query-demo](https://github.com/infinityabundance/multimodal-query-demo) workspace.

Keeps the seeded-workload construction in one place so the three criterion harnesses and the `mqd bench` single-file JSON report measure the same thing. Depends on [`mqd-core`](https://crates.io/crates/mqd-core) and [`mqd-engine`](https://crates.io/crates/mqd-engine).

## What's here

- **`Fixture { seed, agents, ticks }`** — the canonical workload descriptor. Constructs a deterministic event stream via the same generator used by the `mqd generate` CLI command, so bench numbers are reproducible from a seed.
- **`ingest_throughput`** — criterion bench: events/second through the async ingest pipeline.
- **`query_latency`** — criterion bench: `latest_at` + `range_scan` at three entity-count scales.
- **`ops_scale`** — criterion bench: `proximity_pairs` across entity counts and `speed_over_ground` across tick counts.

The `mqd bench` CLI subcommand consumes the same `Fixture` and writes a JSON report (p50/p95 per suite × scale) that `mqd figs` turns into PNGs for the paper.

## Example

```bash
cargo bench -p mqd-bench                                          # criterion HTML reports
cargo run -p mqd-cli --release -- bench --out out/bench.json      # single-file JSON
cargo run -p mqd-cli --release -- figs --from out/bench.json --out paper/figures/
```

## License

Apache-2.0.
