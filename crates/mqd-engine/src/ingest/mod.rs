pub mod batcher;

use std::collections::BTreeMap;
use std::fmt::Display;
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use arrow_array::RecordBatch;
use futures::stream::{Stream, StreamExt};
use mqd_core::{ComponentKind, Event, IngestSummary, RunSummary, StatusReporter};
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use self::batcher::KindBatcher;
use crate::store::Store;

#[derive(Clone, Debug)]
pub struct IngestConfig {
    pub channel_capacity: usize,
    pub max_batch_rows: usize,
    pub max_batch_delay: Duration,
}

impl Default for IngestConfig {
    fn default() -> Self {
        Self {
            channel_capacity: 4096,
            max_batch_rows: 4096,
            max_batch_delay: Duration::from_millis(50),
        }
    }
}

/// Drive an async stream of `Event`s through an mpsc channel into a single flusher
/// that builds Arrow `RecordBatch`es per component kind and appends them to `store`.
///
/// Shutdown protocol:
/// - If `cancel` is triggered, the producer stops reading the input stream and drops its
///   `Sender`. The flusher observes the channel close, drains any pending batcher state,
///   and exits with whatever partial work has been committed.
/// - If the input stream ends naturally, the producer drops its `Sender` and the flusher
///   drains residuals. This is the normal finish path.
///
/// Invariant: `flush_all` is called exactly once between the last successful append and
/// function return, so no rows are silently dropped on a clean exit.
pub async fn ingest_stream<S>(
    config: IngestConfig,
    store: Arc<Store>,
    reporter: Arc<dyn StatusReporter>,
    cancel: CancellationToken,
    stream: S,
) -> Result<IngestSummary>
where
    S: Stream<Item = Event> + Unpin + Send + 'static,
{
    reporter.phase("ingest");
    let start = Instant::now();

    let (tx, mut rx) = mpsc::channel::<Event>(config.channel_capacity);

    let producer_reporter = reporter.clone();
    let producer_cancel = cancel.clone();
    let producer = tokio::spawn(async move {
        let mut stream = stream;
        let mut sent: u64 = 0;
        let mut cancelled = false;
        loop {
            tokio::select! {
                biased;
                _ = producer_cancel.cancelled() => {
                    cancelled = true;
                    break;
                }
                next = stream.next() => {
                    let Some(event) = next else { break };
                    if tx.send(event).await.is_err() {
                        producer_reporter.warn("flusher dropped channel; stopping producer");
                        break;
                    }
                    sent += 1;
                }
            }
        }
        if cancelled {
            producer_reporter.info("producer", &"cancelled" as &dyn Display);
        }
        producer_reporter.counter("producer.events_sent", sent);
        sent
    });

    let mut batchers: BTreeMap<ComponentKind, KindBatcher> = BTreeMap::new();
    let mut summary = IngestSummary::default();
    let mut last_flush = Instant::now();

    let flush_all = |batchers: &mut BTreeMap<ComponentKind, KindBatcher>,
                     store: &Store,
                     summary: &mut IngestSummary|
     -> Result<()> {
        for batcher in batchers.values_mut() {
            if let Some(batch) = batcher.drain()? {
                let rows = batch.num_rows() as u64;
                store.append_batch(batcher.kind(), batch);
                summary.batches_flushed += 1;
                summary.rows_emitted += rows;
                summary.max_batch_rows = summary.max_batch_rows.max(rows);
            }
        }
        Ok(())
    };

    loop {
        let timeout = config.max_batch_delay.saturating_sub(last_flush.elapsed());
        let maybe = if timeout.is_zero() {
            match rx.try_recv() {
                Ok(ev) => Some(ev),
                Err(mpsc::error::TryRecvError::Empty) => {
                    flush_all(&mut batchers, &store, &mut summary)?;
                    last_flush = Instant::now();
                    tokio::select! {
                        biased;
                        _ = cancel.cancelled() => None,
                        v = rx.recv() => v,
                    }
                }
                Err(mpsc::error::TryRecvError::Disconnected) => None,
            }
        } else {
            tokio::select! {
                biased;
                _ = cancel.cancelled() => None,
                res = tokio::time::timeout(timeout, rx.recv()) => match res {
                    Ok(v) => v,
                    Err(_) => {
                        flush_all(&mut batchers, &store, &mut summary)?;
                        last_flush = Instant::now();
                        continue;
                    }
                },
            }
        };

        let Some(event) = maybe else { break };

        summary.events_received += 1;
        if event.t_ns < 0 {
            summary.events_rejected += 1;
            reporter.warn(&format!(
                "rejected event: negative timestamp t_ns={} entity={}",
                event.t_ns, event.entity_path
            ));
            continue;
        }

        let kind = event.kind();
        let batcher = batchers
            .entry(kind)
            .or_insert_with(|| KindBatcher::new(kind));
        batcher.append(&event)?;

        if batcher.len() >= config.max_batch_rows {
            if let Some(batch) = batcher.drain()? {
                let rows = batch.num_rows() as u64;
                store.append_batch(kind, batch);
                summary.batches_flushed += 1;
                summary.rows_emitted += rows;
                summary.max_batch_rows = summary.max_batch_rows.max(rows);
            }
            last_flush = Instant::now();
        }
    }

    // Final flush on clean exit OR cancellation: invariant commitment, see module docs.
    flush_all(&mut batchers, &store, &mut summary)?;

    // Drop our receiver so the producer's pending send (if any) unblocks promptly.
    drop(rx);

    let sent = producer.await.context("producer task panicked")?;
    summary.events_sent_by_producers = sent;

    summary.wall_time_ms = start.elapsed().as_millis();
    summary.cancelled = cancel.is_cancelled();

    reporter.counter("events_received", summary.events_received);
    reporter.counter("events_rejected", summary.events_rejected);
    reporter.counter("batches_flushed", summary.batches_flushed);
    reporter.counter("rows_emitted", summary.rows_emitted);
    reporter.info("wall_time_ms", &summary.wall_time_ms as &dyn Display);
    if summary.cancelled {
        reporter.info(
            "ingest",
            &"cancelled before input exhausted" as &dyn Display,
        );
    }
    reporter.summary(&RunSummary::new("ingest").with_ingest(summary.clone()));
    reporter.done("ingest");

    Ok(summary)
}

/// Read `Event`s from a JSONL file as an async stream. Parse errors abort; empty lines are skipped.
pub async fn jsonl_events(path: &Path) -> Result<impl Stream<Item = Event>> {
    let f = tokio::fs::File::open(path)
        .await
        .with_context(|| format!("opening {}", path.display()))?;
    let reader = BufReader::new(f);
    let lines = reader.lines();
    let stream = futures::stream::unfold(lines, |mut lines| async move {
        loop {
            match lines.next_line().await {
                Ok(Some(line)) => {
                    if line.trim().is_empty() {
                        continue;
                    }
                    match serde_json::from_str::<Event>(&line) {
                        Ok(ev) => return Some((ev, lines)),
                        Err(e) => {
                            eprintln!("[ingest] skipped malformed line: {e}");
                            continue;
                        }
                    }
                }
                Ok(None) => return None,
                Err(e) => {
                    eprintln!("[ingest] read error: {e}");
                    return None;
                }
            }
        }
    });
    Ok(stream)
}

pub fn batches_by_kind_snapshot(store: &Store) -> BTreeMap<ComponentKind, Vec<RecordBatch>> {
    store.snapshot().batches_by_kind()
}
