use std::{error::Error as StdError, sync::Arc};

use thiserror::Error;

/// A shareable backend or loader error source.
pub type ErrorSource = Arc<dyn StdError + Send + Sync + 'static>;

/// The operation that encountered a failure.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Operation {
    /// Reading a key.
    Get,
    /// Writing an explicit value.
    Set,
    /// Refilling an earlier backend.
    Backfill,
    /// Removing a key.
    Remove,
    /// Computing a value through a loader.
    Load,
    /// Clearing all entries owned by a backend.
    Clear,
    /// Iterating entries owned by a backend.
    Iterate,
    /// Releasing backend resources.
    Disconnect,
}

/// One named backend failure within an operation.
#[derive(Clone, Debug, Error)]
#[error("backend '{backend}' failed during {operation:?}: {source}")]
pub struct BackendFailure {
    /// Operation attempted on the backend.
    pub operation: Operation,
    /// Unique backend instance name.
    pub backend: Arc<str>,
    /// Original backend error.
    pub source: ErrorSource,
}

/// An orchestration or loader error.
#[derive(Clone, Debug, Error)]
pub enum KapeError {
    /// A backend implementation failed before orchestration context was added.
    #[error(transparent)]
    BackendSource {
        /// Original backend error.
        source: ErrorSource,
    },
    /// A single backend failure that must be propagated.
    #[error(transparent)]
    Backend(BackendFailure),
    /// One or more backends failed during a best-effort fan-out operation.
    #[error("{} backend(s) failed during {operation:?}", failures.len())]
    PartialFailure {
        /// Operation attempted across the backend chain.
        operation: Operation,
        /// Failures in the order in which they occurred.
        failures: Vec<BackendFailure>,
    },
    /// No backend participates in the requested operation.
    #[error("no backend is enabled for {0:?}")]
    NoBackendEnabled(Operation),
    /// A loader failed while computing a missing value.
    #[error("loader failed: {source}")]
    Loader {
        /// Original loader error.
        source: ErrorSource,
    },
    /// No backend with this configured instance name exists.
    #[error("backend '{0}' was not found")]
    BackendNotFound(Arc<str>),
    /// Iteration pages require a non-zero item limit.
    #[error("iteration limit must be greater than zero")]
    InvalidIterationLimit,
    /// Backend names must not be empty or whitespace-only.
    #[error("backend name must not be empty")]
    EmptyBackendName,
    /// Every backend instance must have a unique name.
    #[error("duplicate backend name '{0}'")]
    DuplicateBackendName(String),
    /// A cache needs at least one backend.
    #[error("cache requires at least one backend")]
    NoBackends,
}

impl KapeError {
    /// Wraps a backend-specific error without discarding its concrete source.
    pub fn backend<E>(error: E) -> Self
    where
        E: StdError + Send + Sync + 'static,
    {
        Self::BackendSource {
            source: Arc::new(error),
        }
    }

    pub(crate) fn loader<E>(error: E) -> Self
    where
        E: StdError + Send + Sync + 'static,
    {
        Self::Loader {
            source: Arc::new(error),
        }
    }

    pub(crate) fn into_source(self) -> ErrorSource {
        match self {
            Self::BackendSource { source } => source,
            error => Arc::new(error),
        }
    }
}
