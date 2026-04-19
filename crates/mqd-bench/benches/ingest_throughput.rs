//! Ingest throughput: time to batch-and-commit an entire synthetic run, excluding JSONL
//! parsing (we feed pre-materialized `Event`s). This isolates the Arrow batcher + store
//! snapshot-swap path from I/O so the numbers stay interpretable.

use std::hint::black_box;

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use mqd_bench::Fixture;
use mqd_core::ComponentKind;
use mqd_engine::ingest::batcher::KindBatcher;
use mqd_engine::store::{Store, DEFAULT_BUCKET_NS};

fn ingest_throughput(c: &mut Criterion) {
    let mut group = c.benchmark_group("ingest_throughput");
    // Small scales so the bench completes in a few seconds locally. These are signal,
    // not benchmarks — single-machine, single-process, synthetic only.
    for &(agents, ticks) in &[(8u32, 500u32), (16u32, 500u32), (32u32, 500u32)] {
        let fixture = Fixture::new(agents, ticks);
        let events = fixture.events();
        group.throughput(Throughput::Elements(events.len() as u64));
        group.bench_with_input(
            BenchmarkId::from_parameter(format!("{}x{}", agents, ticks)),
            &events,
            |b, events| {
                b.iter(|| {
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
                    black_box(store.snapshot().total_rows())
                });
            },
        );
    }
    group.finish();
}

criterion_group!(benches, ingest_throughput);
criterion_main!(benches);
