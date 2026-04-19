use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct IngestSummary {
    pub events_received: u64,
    pub events_rejected: u64,
    pub events_sent_by_producers: u64,
    pub batches_flushed: u64,
    pub rows_emitted: u64,
    pub max_batch_rows: u64,
    pub wall_time_ms: u128,
    pub cancelled: bool,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct QuerySummary {
    pub query_type: String,
    pub candidate_partitions: u64,
    pub pruned_by_time: u64,
    pub pruned_by_kind: u64,
    pub scanned_partitions: u64,
    pub scanned_rows: u64,
    pub returned_rows: u64,
    pub latency_ms: f64,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct RunSummary {
    pub command: String,
    pub ingest: Option<IngestSummary>,
    pub query: Option<QuerySummary>,
    pub extra: Vec<(String, String)>,
}

impl RunSummary {
    pub fn new(command: impl Into<String>) -> Self {
        Self {
            command: command.into(),
            ..Default::default()
        }
    }

    pub fn with_ingest(mut self, ingest: IngestSummary) -> Self {
        self.ingest = Some(ingest);
        self
    }

    pub fn with_query(mut self, query: QuerySummary) -> Self {
        self.query = Some(query);
        self
    }

    pub fn note(mut self, key: impl Into<String>, value: impl ToString) -> Self {
        self.extra.push((key.into(), value.to_string()));
        self
    }
}
