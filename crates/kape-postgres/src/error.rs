use std::error::Error as StdError;
use std::fmt;

/// A `PostgreSQL` adapter failure.
#[derive(Debug)]
pub enum PostgresBackendError<E> {
    /// Key or value encoding failed.
    Codec(E),
    /// `PostgreSQL` operation failed.
    Sqlx(sqlx::Error),
    /// A duration cannot be represented as signed milliseconds.
    TTLOverflow,
    /// `PostgreSQL` returned an invalid positive remaining TTL.
    InvalidRemainingTTL(i64),
    /// A table name was empty, unsafe, or contained more than schema and table.
    InvalidTableName(String),
    /// The configured table does not exist or is not visible in this session.
    TableNotFound(String),
    /// A namespace length cannot be represented by the key frame.
    NamespaceTooLong,
    /// The iteration cursor does not belong to this namespace.
    InvalidCursor,
    /// The requested page size cannot be represented by `PostgreSQL`.
    IterationLimitOverflow,
}

impl<E> fmt::Display for PostgresBackendError<E>
where
    E: fmt::Display,
{
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Codec(error) => write!(formatter, "PostgreSQL codec failed: {error}"),
            Self::Sqlx(error) => write!(formatter, "PostgreSQL operation failed: {error}"),
            Self::TTLOverflow => formatter.write_str("TTL exceeds PostgreSQL millisecond range"),
            Self::InvalidRemainingTTL(value) => {
                write!(
                    formatter,
                    "PostgreSQL returned invalid remaining TTL {value}"
                )
            }
            Self::InvalidTableName(name) => write!(formatter, "invalid table name '{name}'"),
            Self::TableNotFound(name) => {
                write!(formatter, "PostgreSQL table '{name}' does not exist")
            }
            Self::NamespaceTooLong => formatter.write_str("PostgreSQL namespace is too long"),
            Self::InvalidCursor => formatter.write_str("invalid PostgreSQL iteration cursor"),
            Self::IterationLimitOverflow => {
                formatter.write_str("PostgreSQL iteration limit is too large")
            }
        }
    }
}

impl<E> StdError for PostgresBackendError<E>
where
    E: StdError + 'static,
{
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        match self {
            Self::Codec(error) => Some(error),
            Self::Sqlx(error) => Some(error),
            Self::TTLOverflow
            | Self::InvalidRemainingTTL(_)
            | Self::InvalidTableName(_)
            | Self::TableNotFound(_)
            | Self::NamespaceTooLong
            | Self::InvalidCursor
            | Self::IterationLimitOverflow => None,
        }
    }
}

impl<E> From<sqlx::Error> for PostgresBackendError<E> {
    fn from(error: sqlx::Error) -> Self {
        Self::Sqlx(error)
    }
}
