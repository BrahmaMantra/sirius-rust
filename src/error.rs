use thiserror::Error;

/// Sirius-Rust 统一错误类型
#[derive(Debug, Error)]
pub enum SiriusError {
    #[error("Type error: {0}")]
    TypeError(String),

    #[error("Aggregate error: {0}")]
    AggregateError(String),

    #[error("GPU error: {0}")]
    GpuError(String),

    #[error("Substrait error: {0}")]
    SubstraitError(String),

    #[error("DuckDB error: {0}")]
    DuckDBError(String),

    #[error("Overflow: {0}")]
    Overflow(String),

    #[error("Not implemented: {0}")]
    NotImplemented(String),
}

pub type Result<T> = std::result::Result<T, SiriusError>;
