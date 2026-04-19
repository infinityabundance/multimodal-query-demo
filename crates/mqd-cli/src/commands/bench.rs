//! `mqd bench` — run a small, fixed reproducible benchmark grid and emit a single JSON
//! report. Parallel to the criterion benches under `crates/mqd-bench/benches/` but this
//! path is designed to feed the paper's figures: one file, stable schema, deterministic
//! input set.

use std::fmt::Display;
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use mqd_bench::Fixture;
use mqd_core::{ComponentKind, EntityPath, StatusReporter};
use mqd_engine::ops::{proximity_pairs, speed_over_ground, ProximityQuery, SpeedQuery};
use mqd_engine::query::{latest_at, range_scan, LatestAtQuery, RangeScanQuery};
use serde::{Deserialize, Serialize};

use crate::cli::BenchArgs;

#[derive(Debug, Serialize, Deserialize)]
pub struct BenchReport {
    pub version: String,
    pub machine: Machine,
    pub runs: Vec<BenchRun>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Machine {
    pub os: String,
    pub arch: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct BenchRun {
    /// Suite the run belongs to (e.g., `latest_at`, `range_scan`, `proximity`, `speed`,
    /// `ingest`).
    pub suite: String,
    /// Human-friendly scale label ("8x500", "N=100", etc.).
    pub scale: String,
    /// The primary scale knob as a single number — the x-axis value in plots.
    pub x: f64,
    /// Number of samples collected.
    pub samples: usize,
    /// p50 latency in microseconds.
    pub p50_us: f64,
    /// p95 latency in microseconds.
    pub p95_us: f64,
    /// p99 latency in microseconds.
    pub p99_us: f64,
    /// Optional throughput in rows-or-events per second, where meaningful.
    pub throughput_per_sec: Option<f64>,
    /// Rows the measured operation returned, for back-of-envelope sanity.
    pub returned_rows: u64,
}

pub async fn run(args: BenchArgs, reporter: Arc<dyn StatusReporter>) -> Result<()> {
    reporter.phase("bench");
    reporter.info("samples_per_scenario", &args.samples as &dyn Display);

    let mut runs: Vec<BenchRun> = Vec::new();

    // ingest_throughput — batcher + store append for a whole generated run.
    for &(agents, ticks) in &[(8u32, 500u32), (16u32, 500u32), (32u32, 500u32)] {
        let f = Fixture::new(agents, ticks);
        let events = f.events();
        let n_events = events.len();
        let timings = measure_us(args.samples, || {
            bench_ingest(&events);
        });
        let summary = summarize(&timings);
        let throughput = n_events as f64 / (summary.p50_us / 1e6);
        runs.push(BenchRun {
            suite: "ingest".into(),
            scale: format!("{agents}x{ticks}"),
            x: n_events as f64,
            samples: args.samples,
            p50_us: summary.p50_us,
            p95_us: summary.p95_us,
            p99_us: summary.p99_us,
            throughput_per_sec: Some(throughput),
            returned_rows: n_events as u64,
        });
        reporter.info(
            &format!("ingest.{agents}x{ticks}.p50_us"),
            &format!("{:.1}", summary.p50_us) as &dyn Display,
        );
    }

    // latest_at latency at scales up to 64 agents × 1000 ticks.
    for &(agents, ticks) in &[(8u32, 500u32), (32u32, 500u32), (64u32, 1000u32)] {
        let store = Fixture::new(agents, ticks).build_store();
        let snap = store.snapshot();
        let at_ns = (ticks as i64 / 2) * 5_000_000;
        let mut returned = 0u64;
        let timings = measure_us(args.samples, || {
            let r = latest_at(
                &snap,
                LatestAtQuery {
                    kind: ComponentKind::Transform3D,
                    at_ns,
                    entity: None,
                },
            )
            .unwrap();
            returned = r.batch.num_rows() as u64;
        });
        let summary = summarize(&timings);
        runs.push(BenchRun {
            suite: "latest_at".into(),
            scale: format!("{agents}x{ticks}"),
            x: agents as f64,
            samples: args.samples,
            p50_us: summary.p50_us,
            p95_us: summary.p95_us,
            p99_us: summary.p99_us,
            throughput_per_sec: None,
            returned_rows: returned,
        });
        reporter.info(
            &format!("latest_at.{agents}x{ticks}.p50_us"),
            &format!("{:.1}", summary.p50_us) as &dyn Display,
        );
    }

    // range_scan latency — 25% window in the middle, for one entity.
    for &(agents, ticks) in &[(8u32, 500u32), (32u32, 500u32), (64u32, 1000u32)] {
        let store = Fixture::new(agents, ticks).build_store();
        let snap = store.snapshot();
        let entity = EntityPath::new("/agent/0").unwrap();
        let run_span_ns = (ticks as i64) * 5_000_000;
        let start_ns = run_span_ns * 3 / 8;
        let end_ns = run_span_ns * 5 / 8;
        let mut returned = 0u64;
        let timings = measure_us(args.samples, || {
            let r = range_scan(
                &snap,
                RangeScanQuery {
                    kind: ComponentKind::Transform3D,
                    start_ns,
                    end_ns,
                    entity: Some(entity.clone()),
                },
            )
            .unwrap();
            returned = r.batch.num_rows() as u64;
        });
        let summary = summarize(&timings);
        runs.push(BenchRun {
            suite: "range_scan".into(),
            scale: format!("{agents}x{ticks}"),
            x: agents as f64,
            samples: args.samples,
            p50_us: summary.p50_us,
            p95_us: summary.p95_us,
            p99_us: summary.p99_us,
            throughput_per_sec: None,
            returned_rows: returned,
        });
        reporter.info(
            &format!("range_scan.{agents}x{ticks}.p50_us"),
            &format!("{:.1}", summary.p50_us) as &dyn Display,
        );
    }

    // proximity scaling — agents ∈ {50, 100, 200, 400}, big radius so every pair is
    // checked and we measure the quadratic enumeration.
    for &agents in &[50u32, 100u32, 200u32, 400u32] {
        let store = Fixture::new(agents, 100).build_store();
        let snap = store.snapshot();
        let at_ns = 250_000_000i64;
        let mut returned = 0u64;
        let timings = measure_us(args.samples, || {
            let r = proximity_pairs(&snap, ProximityQuery { at_ns, radius: 1e9 }).unwrap();
            returned = r.batch.num_rows() as u64;
        });
        let summary = summarize(&timings);
        runs.push(BenchRun {
            suite: "proximity".into(),
            scale: format!("N={agents}"),
            x: agents as f64,
            samples: args.samples,
            p50_us: summary.p50_us,
            p95_us: summary.p95_us,
            p99_us: summary.p99_us,
            throughput_per_sec: None,
            returned_rows: returned,
        });
        reporter.info(
            &format!("proximity.N={agents}.p50_us"),
            &format!("{:.1}", summary.p50_us) as &dyn Display,
        );
    }

    // speed scaling — ticks ∈ {200, 500, 1000, 2000}, one entity.
    for &ticks in &[200u32, 500u32, 1000u32, 2000u32] {
        let store = Fixture::new(4, ticks).build_store();
        let snap = store.snapshot();
        let entity = EntityPath::new("/agent/0").unwrap();
        let mut returned = 0u64;
        let timings = measure_us(args.samples, || {
            let r = speed_over_ground(
                &snap,
                SpeedQuery {
                    entity: entity.clone(),
                    start_ns: None,
                    end_ns: None,
                },
            )
            .unwrap();
            returned = r.batch.num_rows() as u64;
        });
        let summary = summarize(&timings);
        runs.push(BenchRun {
            suite: "speed".into(),
            scale: format!("ticks={ticks}"),
            x: ticks as f64,
            samples: args.samples,
            p50_us: summary.p50_us,
            p95_us: summary.p95_us,
            p99_us: summary.p99_us,
            throughput_per_sec: None,
            returned_rows: returned,
        });
        reporter.info(
            &format!("speed.ticks={ticks}.p50_us"),
            &format!("{:.1}", summary.p50_us) as &dyn Display,
        );
    }

    let report = BenchReport {
        version: env!("CARGO_PKG_VERSION").to_string(),
        machine: Machine {
            os: std::env::consts::OS.to_string(),
            arch: std::env::consts::ARCH.to_string(),
        },
        runs,
    };

    write_report(&args.out, &report).context("writing bench report")?;
    reporter.info("out", &args.out.display() as &dyn Display);
    reporter.info("runs", &report.runs.len() as &dyn Display);
    reporter.done("bench");
    Ok(())
}

fn measure_us<F: FnMut()>(samples: usize, mut f: F) -> Vec<f64> {
    // One warmup pass to dodge cold-cache bias, then `samples` measured runs.
    f();
    let mut out = Vec::with_capacity(samples);
    for _ in 0..samples {
        let t0 = Instant::now();
        f();
        let elapsed: Duration = t0.elapsed();
        out.push(elapsed.as_secs_f64() * 1e6);
    }
    out
}

struct Summary {
    p50_us: f64,
    p95_us: f64,
    p99_us: f64,
}

fn summarize(samples: &[f64]) -> Summary {
    assert!(!samples.is_empty(), "cannot summarize zero samples");
    let mut sorted: Vec<f64> = samples.to_vec();
    sorted.sort_by(|a, b| a.partial_cmp(b).unwrap());
    Summary {
        p50_us: percentile(&sorted, 0.50),
        p95_us: percentile(&sorted, 0.95),
        p99_us: percentile(&sorted, 0.99),
    }
}

fn percentile(sorted: &[f64], q: f64) -> f64 {
    // Nearest-rank. Fine for the small sample counts we take here.
    let rank = (q * (sorted.len() as f64 - 1.0)).round() as usize;
    sorted[rank.min(sorted.len() - 1)]
}

fn bench_ingest(events: &[mqd_core::Event]) {
    use mqd_engine::ingest::batcher::KindBatcher;
    use mqd_engine::store::{Store, DEFAULT_BUCKET_NS};
    let store = Store::new(DEFAULT_BUCKET_NS);
    let kinds = [
        ComponentKind::Transform3D,
        ComponentKind::Scalar,
        ComponentKind::ImageRef,
        ComponentKind::Annotation,
        ComponentKind::Points3D,
    ];
    for kind in kinds {
        let mut batcher = KindBatcher::new(kind);
        for ev in events.iter().filter(|e| e.kind() == kind) {
            batcher.append(ev).unwrap();
        }
        if let Some(batch) = batcher.drain().unwrap() {
            store.append_batch(kind, batch);
        }
    }
    std::hint::black_box(store.snapshot().total_rows());
}

fn write_report(path: &Path, report: &BenchReport) -> Result<()> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let s = serde_json::to_string_pretty(report)?;
    std::fs::write(path, s)?;
    Ok(())
}
