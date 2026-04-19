//! Query latencies: `latest_at` and `range_scan` over a populated store. The store is
//! built once outside the measurement loop so we're timing the query path, not ingest.

use std::hint::black_box;

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use mqd_bench::Fixture;
use mqd_core::{ComponentKind, EntityPath};
use mqd_engine::query::{latest_at, range_scan, LatestAtQuery, RangeScanQuery};

fn latest_at_latency(c: &mut Criterion) {
    let mut group = c.benchmark_group("latest_at_latency");
    for &(agents, ticks) in &[(8u32, 500u32), (32u32, 500u32), (64u32, 1000u32)] {
        let store = Fixture::new(agents, ticks).build_store();
        let snap = store.snapshot();
        // Pick the midpoint so we exercise time pruning on both sides.
        let at_ns = (ticks as i64 / 2) * 5_000_000;
        group.bench_with_input(
            BenchmarkId::from_parameter(format!("{}x{}", agents, ticks)),
            &(snap, at_ns),
            |b, (snap, at_ns)| {
                b.iter(|| {
                    let r = latest_at(
                        snap,
                        LatestAtQuery {
                            kind: ComponentKind::Transform3D,
                            at_ns: *at_ns,
                            entity: None,
                        },
                    )
                    .unwrap();
                    black_box(r.batch.num_rows())
                });
            },
        );
    }
    group.finish();
}

fn range_scan_latency(c: &mut Criterion) {
    let mut group = c.benchmark_group("range_scan_latency");
    for &(agents, ticks) in &[(8u32, 500u32), (32u32, 500u32), (64u32, 1000u32)] {
        let store = Fixture::new(agents, ticks).build_store();
        let snap = store.snapshot();
        let entity = EntityPath::new("/agent/0").unwrap();
        // 25% window in the middle.
        let run_span_ns = (ticks as i64) * 5_000_000;
        let start_ns = run_span_ns * 3 / 8;
        let end_ns = run_span_ns * 5 / 8;
        group.bench_with_input(
            BenchmarkId::from_parameter(format!("{}x{}", agents, ticks)),
            &(snap, entity, start_ns, end_ns),
            |b, (snap, entity, start_ns, end_ns)| {
                b.iter(|| {
                    let r = range_scan(
                        snap,
                        RangeScanQuery {
                            kind: ComponentKind::Transform3D,
                            start_ns: *start_ns,
                            end_ns: *end_ns,
                            entity: Some(entity.clone()),
                        },
                    )
                    .unwrap();
                    black_box(r.batch.num_rows())
                });
            },
        );
    }
    group.finish();
}

criterion_group!(benches, latest_at_latency, range_scan_latency);
criterion_main!(benches);
