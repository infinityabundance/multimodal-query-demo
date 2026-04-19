pub mod latest_at;
pub mod range_scan;

use serde::{Deserialize, Serialize};

pub use latest_at::{latest_at, LatestAtQuery, LatestAtResult};
pub use range_scan::{range_scan, RangeScanQuery, RangeScanResult};

/// Instrumentation emitted alongside every query. Surfaced as data under `--explain`;
/// not a log line. Stable field names let downstream tooling consume it as JSON.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct QueryPlan {
    pub query_type: String,
    pub candidate_partitions: u64,
    pub pruned_by_kind: u64,
    pub pruned_by_time: u64,
    pub scanned_partitions: u64,
    pub scanned_rows: u64,
    pub returned_rows: u64,
    pub latency_us: u128,
}

impl QueryPlan {
    pub fn new(query_type: impl Into<String>) -> Self {
        Self {
            query_type: query_type.into(),
            ..Default::default()
        }
    }
}
