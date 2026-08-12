use std::{error::Error as StdError, fmt, sync::Arc};

/// A shareable backend or loader error source.
pub type SharedError = Arc<dyn StdError + Send + Sync + 'static>;

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
#[derive(Clone, Debug)]
pub struct BackendFailure {
    /// Operation attempted on the backend.
    pub operation: Operation,
    /// Unique backend instance name.
    pub backend: Arc<str>,
    /// Original backend error.
    pub source: SharedError,
}

impl fmt::Display for BackendFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "backend '{}' failed during {:?}: {}",
            self.backend, self.operation, self.source
        )
    }
}

impl StdError for BackendFailure {
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        Some(self.source.as_ref())
    }
}

/// An orchestration or loader error.
#[derive(Clone, Debug)]
pub enum Error {
    /// A single backend failure that must be propagated.
    Backend(BackendFailure),
    /// One or more backends failed during a best-effort fan-out operation.
    PartialFailure {
        /// Operation attempted across the backend chain.
        operation: Operation,
        /// Failures in the order in which they occurred.
        failures: Vec<BackendFailure>,
    },
    /// No backend participates in the requested operation.
    NoBackendEnabled(Operation),
    /// A loader failed while computing a missing value.
    Loader {
        /// Original loader error.
        source: SharedError,
    },
    /// The load leader was cancelled before publishing a result.
    LoadCancelled,
    /// No backend with this configured instance name exists.
    BackendNotFound(Arc<str>),
    /// Iteration pages require a non-zero item limit.
    InvalidIterationLimit,
}

impl Error {
    pub(crate) fn loader<E>(error: E) -> Self
    where
        E: StdError + Send + Sync + 'static,
    {
        Self::Loader {
            source: Arc::new(error),
        }
    }
}

impl fmt::Display for Error {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Backend(failure) => failure.fmt(formatter),
            Self::PartialFailure {
                operation,
                failures,
            } => write!(
                formatter,
                "{} backend(s) failed during {operation:?}",
                failures.len()
            ),
            Self::NoBackendEnabled(operation) => {
                write!(formatter, "no backend is enabled for {operation:?}")
            }
            Self::Loader { source } => write!(formatter, "loader failed: {source}"),
            Self::LoadCancelled => formatter.write_str("load leader was cancelled"),
            Self::BackendNotFound(backend) => {
                write!(formatter, "backend '{backend}' was not found")
            }
            Self::InvalidIterationLimit => {
                formatter.write_str("iteration limit must be greater than zero")
            }
        }
    }
}

impl StdError for Error {
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        match self {
            Self::Backend(failure) => Some(failure),
            Self::PartialFailure { failures, .. } => failures.first().map(|failure| failure as _),
            Self::Loader { source } => Some(source.as_ref()),
            Self::NoBackendEnabled(_)
            | Self::LoadCancelled
            | Self::BackendNotFound(_)
            | Self::InvalidIterationLimit => None,
        }
    }
}

/// Error source used when a backend does not implement an optional operation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct UnsupportedCapability {
    /// Unsupported operation.
    pub operation: Operation,
}

impl fmt::Display for UnsupportedCapability {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "backend does not support {:?}", self.operation)
    }
}

impl StdError for UnsupportedCapability {}

/// An invalid cache builder configuration.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum BuildError {
    /// Backend names must not be empty or whitespace-only.
    EmptyBackendName,
    /// Every backend instance must have a unique name.
    DuplicateBackendName(String),
    /// A cache needs at least one backend.
    NoBackends,
}

impl fmt::Display for BuildError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::EmptyBackendName => formatter.write_str("backend name must not be empty"),
            Self::DuplicateBackendName(name) => {
                write!(formatter, "duplicate backend name '{name}'")
            }
            Self::NoBackends => formatter.write_str("cache requires at least one backend"),
        }
    }
}

impl StdError for BuildError {}
