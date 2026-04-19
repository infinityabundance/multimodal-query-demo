//! Lock test: the paper's Non-Claims enumerate block must be a verbatim match of the
//! `NON_CLAIMS` constant in source. Any drift — in either direction — fails CI, so the
//! paper and the code cannot silently diverge.

use std::path::PathBuf;

use mqd_core::non_claims::NON_CLAIMS;

fn paper_path() -> PathBuf {
    // This test lives at crates/mqd-core/tests/non_claim_lock.rs; the paper is two
    // directories up, then paper/multimodal-query-demo.tex.
    let manifest = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    manifest
        .parent()
        .and_then(|p| p.parent())
        .expect("workspace root")
        .join("paper")
        .join("multimodal-query-demo.tex")
}

fn extract_nonclaims_items(tex: &str) -> Vec<String> {
    // Grab the first enumerate block following the \section{Non-claims} marker. The
    // paper is small enough that a line-scan beats pulling in a LaTeX parser.
    let marker = tex
        .find("\\section{Non-claims}")
        .expect("paper must contain a \\section{Non-claims}");
    let tail = &tex[marker..];
    let begin = tail
        .find("\\begin{enumerate}")
        .expect("Non-claims section must contain an enumerate block");
    let end = tail[begin..]
        .find("\\end{enumerate}")
        .expect("enumerate block must terminate");
    let body = &tail[begin..begin + end];

    let mut items = Vec::new();
    for raw in body.split("\\item").skip(1) {
        // Stop each \item at the next \item (already split) or at a blank line which is
        // never present inside our short single-sentence items.
        let cleaned = raw.trim().trim_end_matches('\\').trim().to_string();
        if cleaned.is_empty() {
            continue;
        }
        items.push(cleaned);
    }
    items
}

#[test]
fn paper_nonclaims_matches_source_constant_verbatim() {
    let path = paper_path();
    let tex = std::fs::read_to_string(&path)
        .unwrap_or_else(|e| panic!("failed to read {}: {e}", path.display()));
    let items = extract_nonclaims_items(&tex);
    assert_eq!(
        items.len(),
        NON_CLAIMS.len(),
        "paper has {} enumerate items, source has {}. They must match.",
        items.len(),
        NON_CLAIMS.len()
    );
    for (i, (paper_line, src_line)) in items.iter().zip(NON_CLAIMS.iter()).enumerate() {
        assert_eq!(
            paper_line,
            src_line,
            "non-claim #{}: paper text diverges from NON_CLAIMS[{}]",
            i + 1,
            i
        );
    }
}

#[test]
fn paper_file_exists_and_is_nonempty() {
    let path = paper_path();
    let meta = std::fs::metadata(&path)
        .unwrap_or_else(|e| panic!("paper missing at {}: {e}", path.display()));
    assert!(meta.len() > 0, "paper file is empty: {}", path.display());
}
