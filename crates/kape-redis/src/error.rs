use std::error::Error as StdError;

use kape::KapeError;
use thiserror::Error;

/// A `Redis` adapter failure.
#[derive(Debug, Error)]
pub enum RedisBackendError {
    /// Key or value encoding failed.
    #[error("Redis codec failed: {0}")]
    Codec(#[source] Box<dyn StdError + Send + Sync>),
    /// `Redis` client or server operation failed.
    #[error("Redis operation failed: {0}")]
    Redis(#[from] redis::RedisError),
    /// A duration cannot be represented as `Redis` milliseconds.
    #[error("TTL exceeds Redis millisecond range")]
    TTLOverflow,
    /// `Redis` returned an undocumented PTTL sentinel.
    #[error("Redis returned invalid PTTL {0}")]
    InvalidPttl(i64),
    /// A namespace length cannot be represented by the key frame.
    #[error("Redis namespace is too long")]
    NamespaceTooLong,
    /// A pipelined command returned an unexpected response shape.
    #[error("Redis returned an invalid batch response; expected {0}")]
    InvalidBatchResponse(&'static str),
    /// The iteration cursor was not produced by this adapter.
    #[error("invalid Redis iteration cursor")]
    InvalidCursor,
    /// A scanned key did not match the configured namespace frame.
    #[error("invalid Redis Kape key frame")]
    InvalidKeyFrame,
}

impl RedisBackendError {
    /// Erases a codec-specific error at the adapter boundary.
    pub fn codec<E>(error: E) -> Self
    where
        E: StdError + Send + Sync + 'static,
    {
        Self::Codec(Box::new(error))
    }
}

impl From<RedisBackendError> for KapeError {
    fn from(error: RedisBackendError) -> Self {
        Self::backend(error)
    }
}
