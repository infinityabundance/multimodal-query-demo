use std::fmt::Display;

use crate::summary::RunSummary;

pub trait StatusReporter: Send + Sync {
    fn phase(&self, name: &str);
    fn step(&self, msg: &str);
    fn counter(&self, key: &str, value: u64);
    fn info(&self, key: &str, value: &dyn Display);
    fn warn(&self, msg: &str);
    fn summary(&self, summary: &RunSummary);
    fn done(&self, name: &str);
}

pub struct NullReporter;

impl StatusReporter for NullReporter {
    fn phase(&self, _: &str) {}
    fn step(&self, _: &str) {}
    fn counter(&self, _: &str, _: u64) {}
    fn info(&self, _: &str, _: &dyn Display) {}
    fn warn(&self, _: &str) {}
    fn summary(&self, _: &RunSummary) {}
    fn done(&self, _: &str) {}
}
