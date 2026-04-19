use std::fmt::Display;
use std::io::{self, Write};
use std::sync::{Arc, Mutex};

use mqd_core::{RunSummary, StatusReporter};
use serde::Serialize;

pub fn make_reporter(json: bool) -> Arc<dyn StatusReporter> {
    if json {
        Arc::new(JsonReporter::new())
    } else {
        Arc::new(TerminalReporter::new())
    }
}

pub struct TerminalReporter {
    out: Mutex<io::Stdout>,
}

impl TerminalReporter {
    pub fn new() -> Self {
        Self {
            out: Mutex::new(io::stdout()),
        }
    }
    fn write_line(&self, line: &str) {
        if let Ok(mut w) = self.out.lock() {
            let _ = writeln!(w, "{line}");
            let _ = w.flush();
        }
    }
}

impl Default for TerminalReporter {
    fn default() -> Self {
        Self::new()
    }
}

impl StatusReporter for TerminalReporter {
    fn phase(&self, name: &str) {
        self.write_line(&format!("[mqd] {name}"));
    }
    fn step(&self, msg: &str) {
        self.write_line(&format!("[step] {msg}"));
    }
    fn counter(&self, key: &str, value: u64) {
        self.write_line(&format!("[metric] {key}={value}"));
    }
    fn info(&self, key: &str, value: &dyn Display) {
        self.write_line(&format!("[info] {key}={value}"));
    }
    fn warn(&self, msg: &str) {
        self.write_line(&format!("[warn] {msg}"));
    }
    fn summary(&self, summary: &RunSummary) {
        self.write_line(&format!("[summary] command={}", summary.command));
        if let Some(ingest) = &summary.ingest {
            self.write_line(&format!(
                "  ingest: received={} rejected={} batches={} rows={} max_batch={} wall_ms={}",
                ingest.events_received,
                ingest.events_rejected,
                ingest.batches_flushed,
                ingest.rows_emitted,
                ingest.max_batch_rows,
                ingest.wall_time_ms,
            ));
        }
        if let Some(q) = &summary.query {
            self.write_line(&format!(
                "  query: type={} candidate={} scanned={} returned={} latency_ms={:.3}",
                q.query_type,
                q.candidate_partitions,
                q.scanned_partitions,
                q.returned_rows,
                q.latency_ms,
            ));
        }
        for (k, v) in &summary.extra {
            self.write_line(&format!("  {k}={v}"));
        }
    }
    fn done(&self, name: &str) {
        self.write_line(&format!("[done] {name}"));
    }
}

pub struct JsonReporter {
    out: Mutex<io::Stdout>,
}

impl JsonReporter {
    pub fn new() -> Self {
        Self {
            out: Mutex::new(io::stdout()),
        }
    }
    fn emit<T: Serialize>(&self, event: &T) {
        if let Ok(mut w) = self.out.lock() {
            let _ = serde_json::to_writer(&mut *w, event);
            let _ = w.write_all(b"\n");
            let _ = w.flush();
        }
    }
}

impl Default for JsonReporter {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Serialize)]
#[serde(tag = "event")]
enum Record<'a> {
    #[serde(rename = "phase")]
    Phase { name: &'a str },
    #[serde(rename = "step")]
    Step { msg: &'a str },
    #[serde(rename = "counter")]
    Counter { key: &'a str, value: u64 },
    #[serde(rename = "info")]
    Info { key: &'a str, value: String },
    #[serde(rename = "warn")]
    Warn { msg: &'a str },
    #[serde(rename = "done")]
    Done { name: &'a str },
}

impl StatusReporter for JsonReporter {
    fn phase(&self, name: &str) {
        self.emit(&Record::Phase { name });
    }
    fn step(&self, msg: &str) {
        self.emit(&Record::Step { msg });
    }
    fn counter(&self, key: &str, value: u64) {
        self.emit(&Record::Counter { key, value });
    }
    fn info(&self, key: &str, value: &dyn Display) {
        self.emit(&Record::Info {
            key,
            value: value.to_string(),
        });
    }
    fn warn(&self, msg: &str) {
        self.emit(&Record::Warn { msg });
    }
    fn summary(&self, summary: &RunSummary) {
        self.emit(summary);
    }
    fn done(&self, name: &str) {
        self.emit(&Record::Done { name });
    }
}
