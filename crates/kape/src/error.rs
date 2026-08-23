use std::sync::Arc;
use thiserror::Error;

/// A cache-chain operation that can produce a named backend failure.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum Operation {
    /// Reading one or more values.
    Get,
    /// Writing one or more values.
    Set,
    /// Refilling an earlier backend from a later hit.
    Backfill,
    /// Removing one or more values.
    Remove,
    /// Clearing every value owned by a backend instance.
    Clear,
}

/// One named backend failure within an operation.
#[derive(Debug, Error)]
#[error("backend '{backend}' failed during {operation:?}: {source}")]
pub struct BackendFailure {
    /// Operation produced by the core.
    pub operation: Operation,
    /// Unique backend instance name.
    pub backend: Arc<str>,
    /// Original backend or core-generated contract error.
    pub source: Box<dyn std::error::Error + Send + Sync>,
}

/// An orchestration, validation, or loader error.
#[derive(Debug, thiserror::Error)]
pub enum KapeError {
    /// A backend implementation failed before orchestration context was added.
    #[error(transparent)]
    BackendSource {
        /// Original backend error.
        source: Box<dyn std::error::Error + Send + Sync>,
    },
    /// A named backend failed during cache-chain orchestration.
    #[error(transparent)]
    Backend(BackendFailure),
    /// A loader failed while computing a missing value.
    #[error("loader failed: {source}")]
    Loader {
        /// Original loader error.
        source: Box<dyn std::error::Error + Send + Sync>,
    },
    /// A write TTL was below the supported `-1` sentinel.
    #[error("invalid TTL {0}: expected -1, 0, or a positive millisecond value")]
    InvalidTtl(i64),
    /// A cache needs at least one backend instance.
    #[error("cache requires at least one backend")]
    NoBackends,
    /// Backend names must not be empty or whitespace-only.
    #[error("backend name must not be empty")]
    EmptyBackendName,
    /// Every backend instance must have a unique name.
    #[error("duplicate backend name '{0}'")]
    DuplicateBackendName(String),
}

impl KapeError {
    /// Wraps a backend-specific error without discarding its concrete source.
    pub fn backend<E>(error: E) -> Self
    where
        E: std::error::Error + Send + Sync + 'static,
    {
        Self::BackendSource {
            source: Box::new(error),
        }
    }

    pub(crate) fn loader<E>(error: E) -> Self
    where
        E: std::error::Error + Send + Sync + 'static,
    {
        Self::Loader {
            source: Box::new(error),
        }
    }

    pub(crate) fn into_source(self) -> Box<dyn std::error::Error + Send + Sync> {
        match self {
            Self::BackendSource { source } => source,
            error => Box::new(error),
        }
    }
}
