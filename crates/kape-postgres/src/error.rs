use kape::KapeError;

/// A `PostgreSQL` adapter failure.
#[derive(Debug, thiserror::Error)]
pub enum PostgresBackendError {
    /// Key or value encoding failed.
    #[error("PostgreSQL codec failed: {0}")]
    Codec(#[source] Box<dyn std::error::Error + Send + Sync>),
    /// `PostgreSQL` operation failed.
    #[error("PostgreSQL operation failed: {0}")]
    Sqlx(#[from] sqlx::Error),
    /// A finite TTL cannot be represented as an absolute Unix millisecond timestamp.
    #[error("TTL exceeds PostgreSQL millisecond range")]
    TtlOverflow,
    /// A table name was empty, unsafe, or contained more than schema and table.
    #[error("invalid table name '{0}'")]
    InvalidTableName(String),
}

impl PostgresBackendError {
    /// Erases a codec-specific error at the adapter boundary.
    pub fn codec<E>(error: E) -> Self
    where
        E: std::error::Error + Send + Sync + 'static,
    {
        Self::Codec(Box::new(error))
    }
}

impl From<PostgresBackendError> for KapeError {
    fn from(error: PostgresBackendError) -> Self {
        Self::backend(error)
    }
}
