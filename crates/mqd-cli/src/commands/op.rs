use std::fmt::Display;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use arrow_array::RecordBatch;
use mqd_core::{EntityPath, QuerySummary, RunSummary, StatusReporter};
use mqd_engine::ops::{proximity_pairs, speed_over_ground, ProximityQuery, SpeedQuery};
use mqd_engine::query::QueryPlan;
use mqd_engine::store::DEFAULT_BUCKET_NS;
use mqd_engine::{ingest::jsonl_events, ingest_stream, IngestConfig, Store};
use tokio_util::sync::CancellationToken;

use crate::cli::{OpCommand, ProximityArgs, SpeedArgs};

pub async fn run(cmd: OpCommand, reporter: Arc<dyn StatusReporter>, json_mode: bool) -> Result<()> {
    match cmd {
        OpCommand::Proximity(args) => run_proximity(args, reporter, json_mode).await,
        OpCommand::Speed(args) => run_speed(args, reporter, json_mode).await,
    }
}

async fn run_proximity(
    args: ProximityArgs,
    reporter: Arc<dyn StatusReporter>,
    json_mode: bool,
) -> Result<()> {
    let store = ingest_to_store(&args.input, args.batch_rows, reporter.clone()).await?;

    reporter.phase("op proximity");
    let snap = store.snapshot();
    let result = proximity_pairs(
        &snap,
        ProximityQuery {
            at_ns: args.at,
            radius: args.radius,
        },
    )?;
    reporter.info(
        "considered_entities",
        &result.considered_entities as &dyn Display,
    );
    // Quadratic-by-design note: surfaced in the plan so --explain makes it legible.
    reporter.info(
        "complexity",
        &"O(N^2) in considered_entities" as &dyn Display,
    );
    report_plan(&*reporter, args.explain, &result.plan);
    emit_result(
        &*reporter,
        "proximity",
        &result.batch,
        &result.plan,
        json_mode,
    )?;
    Ok(())
}

async fn run_speed(
    args: SpeedArgs,
    reporter: Arc<dyn StatusReporter>,
    json_mode: bool,
) -> Result<()> {
    let store = ingest_to_store(&args.input, args.batch_rows, reporter.clone()).await?;

    reporter.phase("op speed");
    let entity = EntityPath::new(args.entity.as_str()).context("parsing --entity")?;
    let snap = store.snapshot();
    let result = speed_over_ground(
        &snap,
        SpeedQuery {
            entity,
            start_ns: args.start,
            end_ns: args.end,
        },
    )?;
    reporter.info(
        "considered_samples",
        &result.considered_samples as &dyn Display,
    );
    report_plan(&*reporter, args.explain, &result.plan);
    emit_result(&*reporter, "speed", &result.batch, &result.plan, json_mode)?;
    Ok(())
}

async fn ingest_to_store(
    input: &std::path::Path,
    batch_rows: usize,
    reporter: Arc<dyn StatusReporter>,
) -> Result<Arc<Store>> {
    let stream = jsonl_events(input).await?;
    let store = Arc::new(Store::new(DEFAULT_BUCKET_NS));
    let config = IngestConfig {
        channel_capacity: (batch_rows * 4).max(1024),
        max_batch_rows: batch_rows,
        max_batch_delay: Duration::from_millis(50),
    };
    let cancel = CancellationToken::new();
    let _summary = ingest_stream(
        config,
        store.clone(),
        reporter.clone(),
        cancel,
        Box::pin(stream),
    )
    .await?;
    Ok(store)
}

fn report_plan(reporter: &dyn StatusReporter, explain: bool, plan: &QueryPlan) {
    if !explain {
        reporter.info(
            "plan.scanned_partitions",
            &plan.scanned_partitions as &dyn Display,
        );
        reporter.info("plan.returned_rows", &plan.returned_rows as &dyn Display);
        return;
    }
    reporter.info("plan.query_type", &plan.query_type as &dyn Display);
    reporter.info(
        "plan.candidate_partitions",
        &plan.candidate_partitions as &dyn Display,
    );
    reporter.info("plan.pruned_by_kind", &plan.pruned_by_kind as &dyn Display);
    reporter.info("plan.pruned_by_time", &plan.pruned_by_time as &dyn Display);
    reporter.info(
        "plan.scanned_partitions",
        &plan.scanned_partitions as &dyn Display,
    );
    reporter.info("plan.scanned_rows", &plan.scanned_rows as &dyn Display);
    reporter.info("plan.returned_rows", &plan.returned_rows as &dyn Display);
    reporter.info("plan.latency_us", &plan.latency_us as &dyn Display);
}

fn emit_result(
    reporter: &dyn StatusReporter,
    command: &str,
    batch: &RecordBatch,
    plan: &QueryPlan,
    json_mode: bool,
) -> Result<()> {
    if batch.num_rows() == 0 {
        reporter.info("result", &"empty" as &dyn Display);
    } else if json_mode {
        emit_rows_ndjson(batch)?;
    } else {
        let pretty = arrow::util::pretty::pretty_format_batches(&[batch.clone()])?;
        println!("\n{pretty}");
    }

    let summary = QuerySummary {
        query_type: plan.query_type.clone(),
        candidate_partitions: plan.candidate_partitions,
        pruned_by_time: plan.pruned_by_time,
        pruned_by_kind: plan.pruned_by_kind,
        scanned_partitions: plan.scanned_partitions,
        scanned_rows: plan.scanned_rows,
        returned_rows: plan.returned_rows,
        latency_ms: plan.latency_us as f64 / 1000.0,
    };
    reporter.summary(&RunSummary::new(command).with_query(summary));
    reporter.done(command);
    Ok(())
}

fn emit_rows_ndjson(batch: &RecordBatch) -> Result<()> {
    let stdout = std::io::stdout();
    let mut locked = stdout.lock();
    let mut writer = arrow::json::writer::LineDelimitedWriter::new(&mut locked);
    writer.write(batch)?;
    writer.finish()?;
    Ok(())
}
