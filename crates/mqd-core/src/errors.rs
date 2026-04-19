use thiserror::Error;

#[derive(Debug, Error)]
pub enum CoreError {
    #[error("invalid entity path {path:?}: {reason}")]
    InvalidEntityPath { path: String, reason: &'static str },

    #[error("negative timestamp {t_ns} ns on {entity:?}/{component:?}")]
    NegativeTimestamp {
        t_ns: i64,
        entity: String,
        component: String,
    },

    #[error("component kind {name:?} is not recognised")]
    UnknownComponentKind { name: String },

    #[error("arrow error: {0}")]
    Arrow(#[from] arrow::error::ArrowError),

    #[error("schema mismatch: expected {expected}, got {got}")]
    SchemaMismatch { expected: String, got: String },
}

pub type Result<T> = std::result::Result<T, CoreError>;
