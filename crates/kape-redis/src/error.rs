use std::error::Error as StdError;
use std::fmt;

/// A `Redis` adapter failure.
#[derive(Debug)]
pub enum RedisBackendError<E> {
    /// Key or value encoding failed.
    Codec(E),
    /// `Redis` client or server operation failed.
    Redis(redis::RedisError),
    /// A duration cannot be represented as `Redis` milliseconds.
    TTLOverflow,
    /// `Redis` returned an undocumented PTTL sentinel.
    InvalidPttl(i64),
    /// A namespace length cannot be represented by the key frame.
    NamespaceTooLong,
    /// A pipelined command returned an unexpected response shape.
    InvalidBatchResponse(&'static str),
    /// The iteration cursor was not produced by this adapter.
    InvalidCursor,
    /// A scanned key did not match the configured namespace frame.
    InvalidKeyFrame,
}

impl<E> fmt::Display for RedisBackendError<E>
where
    E: fmt::Display,
{
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Codec(error) => write!(formatter, "Redis codec failed: {error}"),
            Self::Redis(error) => write!(formatter, "Redis operation failed: {error}"),
            Self::TTLOverflow => formatter.write_str("TTL exceeds Redis millisecond range"),
            Self::InvalidPttl(value) => write!(formatter, "Redis returned invalid PTTL {value}"),
            Self::NamespaceTooLong => formatter.write_str("Redis namespace is too long"),
            Self::InvalidBatchResponse(expected) => {
                write!(
                    formatter,
                    "Redis returned an invalid batch response; expected {expected}"
                )
            }
            Self::InvalidCursor => formatter.write_str("invalid Redis iteration cursor"),
            Self::InvalidKeyFrame => formatter.write_str("invalid Redis Kape key frame"),
        }
    }
}

impl<E> StdError for RedisBackendError<E>
where
    E: StdError + 'static,
{
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        match self {
            Self::Codec(error) => Some(error),
            Self::Redis(error) => Some(error),
            Self::TTLOverflow
            | Self::InvalidPttl(_)
            | Self::NamespaceTooLong
            | Self::InvalidBatchResponse(_)
            | Self::InvalidCursor
            | Self::InvalidKeyFrame => None,
        }
    }
}

impl<E> From<redis::RedisError> for RedisBackendError<E> {
    fn from(error: redis::RedisError) -> Self {
        Self::Redis(error)
    }
}
