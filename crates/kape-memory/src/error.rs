use kape::KapeError;
use thiserror::Error;

/// An in-memory adapter failure.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum MemoryError {
    /// The requested TTL cannot be represented by `Instant`.
    #[error("TTL exceeds the in-memory clock range")]
    TTLOverflow,
    /// The iteration cursor was not produced by this adapter.
    #[error("invalid memory iteration cursor")]
    InvalidCursor,
}

impl From<MemoryError> for KapeError {
    fn from(error: MemoryError) -> Self {
        Self::backend(error)
    }
}
