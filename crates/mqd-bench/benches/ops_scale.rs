//! Operator scaling. `proximity_pairs` is O(N^2) in the number of distinct entities at
//! the query instant — this bench exists to *confirm* that quadratic shape, not refute
//! it. The paper's "How to Read" section points at this bench as the honest evidence.
//!
//! `speed_over_ground` is linear in samples for one entity; we include it as a cheap
//! linear counterpoint.

use std::hint::black_box;

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use mqd_bench::Fixture;
use mqd_core::EntityPath;
use mqd_engine::ops::{proximity_pairs, speed_over_ground, ProximityQuery, SpeedQuery};

fn proximity_scale(c: &mut Criterion) {
    let mut group = c.benchmark_group("proximity_scale");
    // Scale the agent count; hold ticks fixed so only N (entities) moves.
    for &agents in &[50u32, 100u32, 200u32, 400u32] {
        let store = Fixture::new(agents, 100).build_store();
        let snap = store.snapshot();
        let at_ns = 250_000_000i64; // middle of the 100-tick run
        group.bench_with_input(
            BenchmarkId::from_parameter(agents),
            &(snap, at_ns),
            |b, (snap, at_ns)| {
                b.iter(|| {
                    let r = proximity_pairs(
                        snap,
                        ProximityQuery {
                            at_ns: *at_ns,
                            // Large radius -> every pair is checked, measures the N^2
                            // enumeration rather than the filter path.
                            radius: 1e9,
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

fn speed_scale(c: &mut Criterion) {
    let mut group = c.benchmark_group("speed_scale");
    // Scale the per-entity sample count (ticks) and measure for one entity.
    for &ticks in &[200u32, 500u32, 1000u32, 2000u32] {
        let store = Fixture::new(4, ticks).build_store();
        let snap = store.snapshot();
        let entity = EntityPath::new("/agent/0").unwrap();
        group.bench_with_input(
            BenchmarkId::from_parameter(ticks),
            &(snap, entity),
            |b, (snap, entity)| {
                b.iter(|| {
                    let r = speed_over_ground(
                        snap,
                        SpeedQuery {
                            entity: entity.clone(),
                            start_ns: None,
                            end_ns: None,
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

criterion_group!(benches, proximity_scale, speed_scale);
criterion_main!(benches);
