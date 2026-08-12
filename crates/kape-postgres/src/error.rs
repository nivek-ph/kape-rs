use std::error::Error as StdError;

use kape::KapeError;
use thiserror::Error;

/// A `PostgreSQL` adapter failure.
#[derive(Debug, Error)]
pub enum PostgresBackendError<E> {
    /// Key or value encoding failed.
    #[error("PostgreSQL codec failed: {0}")]
    Codec(#[source] E),
    /// `PostgreSQL` operation failed.
    #[error("PostgreSQL operation failed: {0}")]
    Sqlx(#[from] sqlx::Error),
    /// A duration cannot be represented as signed milliseconds.
    #[error("TTL exceeds PostgreSQL millisecond range")]
    TTLOverflow,
    /// `PostgreSQL` returned an invalid positive remaining TTL.
    #[error("PostgreSQL returned invalid remaining TTL {0}")]
    InvalidRemainingTTL(i64),
    /// A table name was empty, unsafe, or contained more than schema and table.
    #[error("invalid table name '{0}'")]
    InvalidTableName(String),
    /// The configured table does not exist or is not visible in this session.
    #[error("PostgreSQL table '{0}' does not exist")]
    TableNotFound(String),
    /// A namespace length cannot be represented by the key frame.
    #[error("PostgreSQL namespace is too long")]
    NamespaceTooLong,
    /// The iteration cursor does not belong to this namespace.
    #[error("invalid PostgreSQL iteration cursor")]
    InvalidCursor,
    /// The requested page size cannot be represented by `PostgreSQL`.
    #[error("PostgreSQL iteration limit is too large")]
    IterationLimitOverflow,
}

impl<E> From<PostgresBackendError<E>> for KapeError
where
    E: StdError + Send + Sync + 'static,
{
    fn from(error: PostgresBackendError<E>) -> Self {
        Self::backend(error)
    }
}
