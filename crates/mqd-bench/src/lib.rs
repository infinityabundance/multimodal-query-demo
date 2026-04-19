//! Shared benchmark fixtures.
//!
//! `mqd-bench` keeps its helpers in a library so both criterion benches (under `benches/`)
//! and the `mqd bench` CLI subcommand can share a single fixture builder. This matters for
//! reproducibility: the paper's figures and the criterion numbers consume the same
//! deterministic scenarios.

use std::sync::Arc;

use mqd_core::{schema_for, ComponentKind, Event};
use mqd_engine::generator::{generate_run, GenConfig};
use mqd_engine::ingest::batcher::KindBatcher;
use mqd_engine::store::{Store, DEFAULT_BUCKET_NS};

/// Fixed-shape input for a bench run. All knobs are explicit; no hidden defaults.
#[derive(Clone, Debug)]
pub struct Fixture {
    pub seed: u64,
    pub agents: u32,
    pub ticks: u32,
}

impl Fixture {
    pub fn new(agents: u32, ticks: u32) -> Self {
        Self {
            seed: 42,
            agents,
            ticks,
        }
    }

    pub fn events(&self) -> Vec<Event> {
        generate_run(&GenConfig {
            seed: self.seed,
            agents: self.agents,
            ticks: self.ticks,
            ..Default::default()
        })
    }

    /// Build a `Store` directly by batching events, skipping the async channel. Use this
    /// in benches where you want query/operator latency without the ingest path's noise.
    pub fn build_store(&self) -> Arc<Store> {
        let events = self.events();
        let store = Arc::new(Store::new(DEFAULT_BUCKET_NS));
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
                batcher.append(ev).expect("append event");
            }
            if let Some(batch) = batcher.drain().expect("drain") {
                assert_eq!(batch.schema(), schema_for(kind));
                store.append_batch(kind, batch);
            }
        }
        store
    }

    pub fn events_count(&self) -> usize {
        self.events().len()
    }
}
