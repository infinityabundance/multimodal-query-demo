mod cli;
mod commands;
mod reporter;

use anyhow::Result;
use clap::Parser;

use cli::{Cli, Command};

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    let reporter = reporter::make_reporter(cli.json);

    match cli.command {
        Command::Generate(args) => commands::generate::run(args, reporter).await,
        Command::Ingest(args) => commands::ingest::run(args, reporter).await,
        Command::Query(sub) => commands::query::run(sub, reporter, cli.json).await,
        Command::Op(sub) => commands::op::run(sub, reporter, cli.json).await,
        Command::Bench(args) => commands::bench::run(args, reporter).await,
        Command::Figs(args) => commands::figs::run(args, reporter).await,
        Command::NonClaims => commands::non_claims::run().await,
    }
}
