# DSFB Gray audit reports

Per-crate static source-visible audit reports produced by [`dsfb-gray`](https://crates.io/crates/dsfb-gray). Scoring rubric version `dsfb-assurance-score-v1`.

| Crate | Badge | Report | Source SHA-256 |
|---|---|---|---|
| `mqd-core` | [![DSFB Gray Audit: 68.0% mixed assurance posture](https://img.shields.io/badge/DSFB%20Gray%20Audit-68.0%25-yellowgreen)](mqd-core/mqd_core_scan.txt) | [`mqd-core/mqd_core_scan.txt`](mqd-core/mqd_core_scan.txt) | `3399288e6176d3deeb4a526acc0862b94d277cc663b30548b0e595314acb9321` |
| `mqd-engine` | [![DSFB Gray Audit: 58.8% mixed assurance posture](https://img.shields.io/badge/DSFB%20Gray%20Audit-58.8%25-yellowgreen)](mqd-engine/mqd_engine_scan.txt) | [`mqd-engine/mqd_engine_scan.txt`](mqd-engine/mqd_engine_scan.txt) | `949c2f9d35d7ce1b4062f890a9d596b66bf893063033f6c7e214e3eac67dcd4c` |
| `mqd-bench` | [![DSFB Gray Audit: 61.3% mixed assurance posture](https://img.shields.io/badge/DSFB%20Gray%20Audit-61.3%25-yellowgreen)](mqd-bench/mqd_bench_scan.txt) | [`mqd-bench/mqd_bench_scan.txt`](mqd-bench/mqd_bench_scan.txt) | `b680f646406be9178bdcfc7ac5e4ceb6da6fb8cd08431c8f99bcc2d918de3a21` |
| `mqd-cli` | [![DSFB Gray Audit: 51.7% limited assurance evidence](https://img.shields.io/badge/DSFB%20Gray%20Audit-51.7%25-orange)](mqd-cli/mqd_cli_scan.txt) | [`mqd-cli/mqd_cli_scan.txt`](mqd-cli/mqd_cli_scan.txt) | `543243796f4d392a027a262a8003f9ac68c5f9fbaaa07767d54712ee4e33c7d7` |

Workspace arithmetic mean: **59.95%**. Reports generated 2026-04-19 UTC against the `v0.1.1` source tree.

## What each report contains

Each per-crate directory holds four artefacts:

- `*_scan.txt` — human-readable report. Overall score, per-section subscores (Safety Surface, Verification Evidence, Build/Tooling Complexity, Lifecycle/Governance, NASA/JPL Power of Ten, Advanced Structural Checks), per-finding evidence, remediation guide, verification suggestions.
- `*_scan.sarif.json` — SARIF 2.1.0 findings for CI ingestion (GitHub code scanning, etc.).
- `*_scan.intoto.json` — in-toto v0.1 statement binding the source SHA-256 to the scoring predicate.
- `*_scan.dsse.json` — DSSE envelope wrapping the in-toto statement.

## What this is not

These are **not certification badges**. DSFB Gray is a source-visible structural audit against a locked rubric — it surfaces reviewable patterns (bounded loops, assertion density, lifecycle artefacts, dependency pinning, etc.) but does not certify compliance with IEC, ISO, RTCA, MIL, or NIST standards, and does not run the code.

Treat the score as a review-readiness target: `mqd-core` at 68.0% shows strong structural discipline for a leaf types crate; `mqd-cli` at 51.7% reflects that CLI glue code does less of what the rubric rewards (lower assertion density, more heap allocation on string-formatted output paths, fewer per-crate governance artefacts than a library crate).

## Reproducing these reports

```bash
# from the dsfb workspace
cargo run --bin dsfb-scan-crate -- /path/to/multimodal-query-demo/crates/mqd-core
```

Repeat for each crate. Output goes to `output-dsfb-gray/dsfb-gray-<UTC-timestamp>/`.
