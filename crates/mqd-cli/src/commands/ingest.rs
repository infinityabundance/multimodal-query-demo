use std::fmt::Display;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use mqd_core::StatusReporter;
use mqd_engine::store::DEFAULT_BUCKET_NS;
use mqd_engine::{ingest::jsonl_events, ingest_stream, IngestConfig, Store};
use tokio_util::sync::CancellationToken;

use crate::cli::IngestArgs;

pub async fn run(args: IngestArgs, reporter: Arc<dyn StatusReporter>) -> Result<()> {
    let stream = jsonl_events(&args.input).await?;
    let store = Arc::new(Store::new(DEFAULT_BUCKET_NS));
    let config = IngestConfig {
        channel_capacity: (args.batch_rows * 4).max(1024),
        max_batch_rows: args.batch_rows,
        max_batch_delay: Duration::from_millis(args.max_delay_ms),
    };
    let _producers = args.producers; // single-source for day-1; multi-producer fan-in lands later.

    let cancel = CancellationToken::new();
    let ctrlc = cancel.clone();
    let ctrlc_reporter = reporter.clone();
    tokio::spawn(async move {
        if tokio::signal::ctrl_c().await.is_ok() {
            ctrlc_reporter.warn("ctrl-c received; draining and exiting");
            ctrlc.cancel();
        }
    });

    let _summary = ingest_stream(
        config,
        store.clone(),
        reporter.clone(),
        cancel,
        Box::pin(stream),
    )
    .await?;
    let snap = store.snapshot();
    reporter.counter("store.partitions", snap.num_partitions() as u64);
    reporter.counter("store.total_rows", snap.total_rows());
    reporter.info("store.bucket_ns", &snap.bucket_ns as &dyn Display);
    Ok(())
}
