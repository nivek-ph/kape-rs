use kape::KapeError;
use thiserror::Error;

/// A `Redis` adapter failure.
#[derive(Debug, Error)]
pub enum RedisBackendError {
    /// Key or value encoding failed.
    #[error("Redis codec failed: {0}")]
    Codec(#[source] Box<dyn std::error::Error + Send + Sync>),
    /// `Redis` client or server operation failed.
    #[error("Redis operation failed: {0}")]
    Redis(#[from] redis::RedisError),
    /// `Redis` returned an undocumented PTTL sentinel.
    #[error("Redis returned invalid PTTL {0}")]
    InvalidPttl(i64),
    /// A pipelined command returned an unexpected response shape.
    #[error("Redis returned an invalid batch response; expected {0}")]
    InvalidBatchResponse(&'static str),
}

impl RedisBackendError {
    /// Erases a codec-specific error at the adapter boundary.
    pub fn codec<E>(error: E) -> Self
    where
        E: std::error::Error + Send + Sync + 'static,
    {
        Self::Codec(Box::new(error))
    }
}

impl From<RedisBackendError> for KapeError {
    fn from(error: RedisBackendError) -> Self {
        Self::backend(error)
    }
}
