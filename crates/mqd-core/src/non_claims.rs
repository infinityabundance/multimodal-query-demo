pub const NON_CLAIMS: [&str; 6] = [
    "multimodal-query-demo is not a database. It is not durable, not transactional, and does not survive process restart.",
    "multimodal-query-demo does not claim compatibility or parity with any specific production system. It is an independent design exercise against public literature on sparse multimodal temporal logs.",
    "Point-in-time query semantics are validated only against the synthetic fixtures in the test suite. Correctness against arbitrary real-world workloads is not claimed.",
    "multimodal-query-demo does not implement SQL, a third-party query engine, a network protocol, or any query interface beyond the typed operators exposed on its CLI.",
    "Benchmark numbers are single-machine, synthetic-workload latencies on the author's hardware. They are not throughput claims, not SLA claims, and not comparisons against any other system.",
    "multimodal-query-demo does not claim cloud-native, distributed, or multi-node behavior. It is one process, one memory space, orchestrated by a single tokio runtime.",
];

pub fn print_numbered() -> String {
    let mut out = String::new();
    for (i, item) in NON_CLAIMS.iter().enumerate() {
        out.push_str(&format!("{}. {}\n", i + 1, item));
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn each_non_claim_ends_in_period_and_is_nonempty() {
        for s in NON_CLAIMS {
            assert!(!s.is_empty(), "empty non-claim");
            assert!(s.ends_with('.'), "non-claim missing trailing period: {s:?}");
        }
    }

    #[test]
    fn numbered_output_is_stable() {
        let s = print_numbered();
        assert!(s.starts_with("1. multimodal-query-demo is not a database."));
        assert_eq!(s.lines().count(), NON_CLAIMS.len());
    }
}
