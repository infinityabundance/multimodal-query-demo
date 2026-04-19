# mqd-cli

[![Open in Colab](https://colab.research.google.com/assets/colab-badge.svg)](https://colab.research.google.com/github/infinityabundance/multimodal-query-demo/blob/main/notebooks/reproduce.ipynb)
[![DSFB Gray Audit: 51.7% limited assurance evidence](https://img.shields.io/badge/DSFB%20Gray%20Audit-51.7%25-orange)](https://github.com/infinityabundance/multimodal-query-demo/blob/main/audit/mqd-cli/mqd_cli_scan.txt)

The `mqd` binary — a clap-based command-line surface for every engine path in the [multimodal-query-demo](https://github.com/infinityabundance/multimodal-query-demo) workspace.

One binary, every engine path. A reviewer can exercise ingest, queries, operators, benches, and figure generation without writing Rust. Depends on [`mqd-core`](https://crates.io/crates/mqd-core), [`mqd-engine`](https://crates.io/crates/mqd-engine), and [`mqd-bench`](https://crates.io/crates/mqd-bench).

## Subcommands

- `generate --seed --agents --ticks --out` — seeded synthetic event stream to JSONL.
- `ingest --input --batch-rows` — JSONL → store via the async pipeline.
- `query latest-at --input --at [--entity] [--kind] [--explain]` — point-in-time query.
- `query range --input --start --end [--entity] [--kind] [--explain]` — half-open range scan.
- `op proximity --input --at --radius` — unordered pairs within `radius` at time `at_ns`.
- `op speed --input --entity [--start] [--end]` — consecutive-pair `distance_m / Δt`.
- `bench --out --samples` — single-file JSON benchmark report (same workload as `cargo bench`).
- `figs --from --out` — PNGs from a bench report.
- `non-claims` — prints the numbered non-claims charter.

A `--json` prefix before any subcommand switches the reporter to NDJSON.

## Example

```bash
cargo run -p mqd-cli --release -- generate --seed 42 --agents 8 --ticks 10000 --out data/run.jsonl
cargo run -p mqd-cli --release -- query range \
  --input data/run.jsonl --start 0 --end 10000000000 \
  --entity /agent/0 --kind transform3d --explain
cargo run -p mqd-cli --release -- --json op proximity \
  --input data/run.jsonl --at 25000000000 --radius 2.5 | jq .
```

## License

Apache-2.0.
