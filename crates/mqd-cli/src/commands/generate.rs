use std::sync::Arc;
use std::time::Instant;

use anyhow::Result;
use mqd_core::{RunSummary, StatusReporter};
use mqd_engine::{generate_run, write_jsonl, GenConfig};

use crate::cli::GenerateArgs;

pub async fn run(args: GenerateArgs, reporter: Arc<dyn StatusReporter>) -> Result<()> {
    reporter.phase("generate");
    let config = GenConfig {
        seed: args.seed,
        agents: args.agents,
        ticks: args.ticks,
        tick_ns: args.tick_ns,
        ..Default::default()
    };
    reporter.info("seed", &config.seed);
    reporter.info("agents", &config.agents);
    reporter.info("ticks", &config.ticks);
    reporter.info("tick_ns", &config.tick_ns);
    reporter.info("out", &args.out.display());

    let start = Instant::now();
    let events = tokio::task::spawn_blocking(move || generate_run(&config)).await?;
    reporter.counter("events_generated", events.len() as u64);

    let path = args.out.clone();
    let n = events.len();
    tokio::task::spawn_blocking(move || write_jsonl(&events, &path)).await??;
    reporter.info("wall_time_ms", &(start.elapsed().as_millis() as u64));

    let summary = RunSummary::new("generate")
        .note("events", n)
        .note("out", args.out.display());
    reporter.summary(&summary);
    reporter.done("generate");
    Ok(())
}
