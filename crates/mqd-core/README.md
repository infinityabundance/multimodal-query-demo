# mqd-core

[![Open in Colab](https://colab.research.google.com/assets/colab-badge.svg)](https://colab.research.google.com/github/infinityabundance/multimodal-query-demo/blob/main/notebooks/reproduce.ipynb)
[![DSFB Gray Audit: 68.0% mixed assurance posture](https://img.shields.io/badge/DSFB%20Gray%20Audit-68.0%25-yellowgreen)](https://github.com/infinityabundance/multimodal-query-demo/blob/main/audit/mqd-core/mqd_core_scan.txt)

Shared types, Arrow schemas, `StatusReporter` trait, and the numbered non-claims constant for the [multimodal-query-demo](https://github.com/infinityabundance/multimodal-query-demo) workspace.

Pure types and traits. No tokio, no I/O, no dependency on the engine. This keeps the engine crate fully async without contaminating the testability of the type layer, and lets downstream consumers depend on the vocabulary without pulling in the runtime.

## What's here

- **`EntityPath`** — validated slash-delimited hierarchical path (`"/agent/0"`).
- **`ComponentKind`** — the five typed event kinds: `Transform3D`, `Points3D`, `Scalar`, `ImageRef`, `Annotation`.
- **`Event`** — the ingest unit carrying `(t_ns, entity_path, kind-specific payload)`.
- **`schema::schema_for(kind)`** — canonical `arrow::datatypes::Schema` per kind; every row has the non-null prefix `t_ns: Int64, entity_path: Utf8`.
- **`StatusReporter` trait** with `TerminalReporter`, `JsonReporter`, `NullReporter` — threaded through engine calls so phase/counter/summary output is decoupled from the caller's preferred format.
- **`NON_CLAIMS: [&str; 6]`** — the numbered non-claims charter. Locked to the paper by a workspace-level integration test.

## Example

```rust
use mqd_core::{EntityPath, ComponentKind, schema::schema_for};

let path = EntityPath::new("/agent/0").unwrap();
let schema = schema_for(ComponentKind::Transform3D);
assert_eq!(schema.field(0).name(), "t_ns");
assert_eq!(schema.field(1).name(), "entity_path");
```

## License

Apache-2.0.
