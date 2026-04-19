use std::path::PathBuf;

use clap::{Parser, Subcommand, ValueEnum};

#[derive(Debug, Parser)]
#[command(
    name = "mqd",
    version,
    about = "Sparse multimodal temporal query prototype"
)]
pub struct Cli {
    /// Emit NDJSON status events to stdout instead of the terminal renderer.
    #[arg(long, global = true)]
    pub json: bool,

    #[command(subcommand)]
    pub command: Command,
}

#[derive(Debug, Subcommand)]
pub enum Command {
    /// Generate a synthetic JSONL event stream.
    Generate(GenerateArgs),
    /// Ingest events from a JSONL file, build Arrow batches, print a summary.
    Ingest(IngestArgs),
    /// Run a point-in-time or range query over an in-memory ingest.
    #[command(subcommand)]
    Query(QueryCommand),
    /// Run a domain operator (proximity pairs / speed-over-ground) over an in-memory ingest.
    #[command(subcommand)]
    Op(OpCommand),
    /// Run a small reproducible benchmark grid and emit a single JSON report.
    Bench(BenchArgs),
    /// Render PNG figures from a bench JSON report (used by the paper).
    Figs(FigsArgs),
    /// Print the numbered non-claims charter.
    NonClaims,
}

#[derive(Debug, clap::Args)]
pub struct BenchArgs {
    /// Destination path for the JSON report.
    #[arg(long, value_name = "FILE", default_value = "out/bench.json")]
    pub out: PathBuf,
    /// Samples per scenario. 51 keeps nearest-rank p95 and p99 distinct (p95=sorted[48],
    /// p99=sorted[50]) without being slow enough to discourage running it.
    #[arg(long, default_value_t = 51)]
    pub samples: usize,
}

#[derive(Debug, clap::Args)]
pub struct FigsArgs {
    /// Bench JSON report to read.
    #[arg(long, value_name = "FILE")]
    pub from: PathBuf,
    /// Destination directory for the rendered PNGs.
    #[arg(long, value_name = "DIR")]
    pub out: PathBuf,
}

#[derive(Debug, clap::Args)]
pub struct GenerateArgs {
    #[arg(long, default_value_t = 42)]
    pub seed: u64,
    #[arg(long, default_value_t = 8)]
    pub agents: u32,
    #[arg(long, default_value_t = 1_000)]
    pub ticks: u32,
    #[arg(long, default_value_t = 5_000_000)]
    pub tick_ns: i64,
    #[arg(long, value_name = "FILE")]
    pub out: PathBuf,
}

#[derive(Debug, clap::Args)]
pub struct IngestArgs {
    #[arg(long, value_name = "FILE")]
    pub input: PathBuf,
    #[arg(long, default_value_t = 1)]
    pub producers: usize,
    #[arg(long, default_value_t = 4096)]
    pub batch_rows: usize,
    #[arg(long, default_value_t = 50)]
    pub max_delay_ms: u64,
}

#[derive(Copy, Clone, Debug, ValueEnum)]
pub enum KindArg {
    Transform3d,
    Points3d,
    Scalar,
    ImageRef,
    Annotation,
}

impl KindArg {
    pub fn to_kind(self) -> mqd_core::ComponentKind {
        use mqd_core::ComponentKind as K;
        match self {
            Self::Transform3d => K::Transform3D,
            Self::Points3d => K::Points3D,
            Self::Scalar => K::Scalar,
            Self::ImageRef => K::ImageRef,
            Self::Annotation => K::Annotation,
        }
    }
}

#[derive(Debug, Subcommand)]
pub enum QueryCommand {
    /// Per-entity latest row at or before `--at` (with deterministic tie-break on t_ns equality).
    LatestAt(LatestAtArgs),
    /// Half-open [--start, --end) range scan.
    Range(RangeArgs),
}

#[derive(Debug, clap::Args)]
pub struct LatestAtArgs {
    #[arg(long, value_name = "FILE")]
    pub input: PathBuf,
    #[arg(long, value_enum, default_value_t = KindArg::Transform3d)]
    pub kind: KindArg,
    /// Point-in-time cutoff, in nanoseconds.
    #[arg(long)]
    pub at: i64,
    /// Optional entity path filter. Defaults to one row per distinct entity.
    #[arg(long)]
    pub entity: Option<String>,
    /// Print a structured query plan in addition to the result.
    #[arg(long)]
    pub explain: bool,
    #[arg(long, default_value_t = 4096)]
    pub batch_rows: usize,
}

#[derive(Debug, Subcommand)]
pub enum OpCommand {
    /// Pair-wise proximity at a single point in time (O(N^2), no spatial index).
    Proximity(ProximityArgs),
    /// Per-entity speed-over-ground over an optional half-open window.
    Speed(SpeedArgs),
}

#[derive(Debug, clap::Args)]
pub struct ProximityArgs {
    #[arg(long, value_name = "FILE")]
    pub input: PathBuf,
    /// Point-in-time cutoff in nanoseconds (resolved per latest_at rules).
    #[arg(long)]
    pub at: i64,
    /// Maximum pairwise Euclidean distance for a pair to appear in the result.
    #[arg(long)]
    pub radius: f64,
    #[arg(long)]
    pub explain: bool,
    #[arg(long, default_value_t = 4096)]
    pub batch_rows: usize,
}

#[derive(Debug, clap::Args)]
pub struct SpeedArgs {
    #[arg(long, value_name = "FILE")]
    pub input: PathBuf,
    /// Entity path to compute speed-over-ground for (required; speed is per-entity).
    #[arg(long)]
    pub entity: String,
    /// Optional inclusive lower bound in nanoseconds.
    #[arg(long)]
    pub start: Option<i64>,
    /// Optional exclusive upper bound in nanoseconds.
    #[arg(long)]
    pub end: Option<i64>,
    #[arg(long)]
    pub explain: bool,
    #[arg(long, default_value_t = 4096)]
    pub batch_rows: usize,
}

#[derive(Debug, clap::Args)]
pub struct RangeArgs {
    #[arg(long, value_name = "FILE")]
    pub input: PathBuf,
    #[arg(long, value_enum, default_value_t = KindArg::Transform3d)]
    pub kind: KindArg,
    /// Inclusive lower bound in nanoseconds.
    #[arg(long)]
    pub start: i64,
    /// Exclusive upper bound in nanoseconds.
    #[arg(long)]
    pub end: i64,
    #[arg(long)]
    pub entity: Option<String>,
    #[arg(long)]
    pub explain: bool,
    #[arg(long, default_value_t = 4096)]
    pub batch_rows: usize,
}
