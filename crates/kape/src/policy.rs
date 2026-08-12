use std::time::Duration;

use crate::{RemainingTTL, ResolvedTTL, TTL};

/// How an error from a backend read is handled.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum ReadFailurePolicy {
    /// Stop and return the named backend failure.
    #[default]
    Propagate,
    /// Report the failure through observability and continue reading.
    SkipBackend,
    /// Serve an earlier stale candidate, or propagate if none exists.
    ServeStale,
}

/// How a failed refill of an earlier backend is handled.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum BackfillFailurePolicy {
    /// Return the fresh hit and attach the refill failure to its metadata.
    #[default]
    ReportAndContinue,
    /// Fail the lookup even though a later backend returned a fresh value.
    Propagate,
}

/// How a loader error is handled by `get_or_load`.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum LoaderFailurePolicy {
    /// Return the loader error.
    #[default]
    Propagate,
    /// Return the earliest stale candidate, or propagate if none exists.
    ServeStale,
}

/// How a cache write failure after a successful load is handled.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum LoadWriteFailurePolicy {
    /// Return the cache write failure.
    #[default]
    Propagate,
    /// Return the loaded value and report the write failure through tracing.
    ReturnValue,
}

/// TTL constraints for one backend instance.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct BackendTTLPolicy {
    /// Used by explicit writes whose requested TTL is [`TTL::Default`].
    pub default_ttl: Option<Duration>,
    /// Hard upper bound applied to every write to this backend.
    pub max_ttl: Option<Duration>,
    /// Additional upper bound applied only to backfills.
    pub backfill_ttl_cap: Option<Duration>,
}

impl BackendTTLPolicy {
    /// Creates a policy without defaults or caps.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            default_ttl: None,
            max_ttl: None,
            backfill_ttl_cap: None,
        }
    }

    /// Sets the default TTL for explicit writes.
    #[must_use]
    pub const fn default_ttl(mut self, ttl: Duration) -> Self {
        self.default_ttl = Some(ttl);
        self
    }

    /// Sets the hard maximum TTL.
    #[must_use]
    pub const fn max_ttl(mut self, ttl: Duration) -> Self {
        self.max_ttl = Some(ttl);
        self
    }

    /// Sets the backfill-only TTL cap.
    #[must_use]
    pub const fn backfill_ttl_cap(mut self, ttl: Duration) -> Self {
        self.backfill_ttl_cap = Some(ttl);
        self
    }

    pub(crate) fn resolve_write(self, requested: TTL) -> ResolvedTTL {
        let resolved = match requested {
            TTL::Default => self
                .default_ttl
                .map_or(ResolvedTTL::Never, ResolvedTTL::After),
            TTL::Never => ResolvedTTL::Never,
            TTL::After(duration) => ResolvedTTL::After(duration),
        };
        cap_ttl(resolved, self.max_ttl)
    }

    pub(crate) fn resolve_backfill(self, remaining: RemainingTTL) -> Option<ResolvedTTL> {
        let resolved = match remaining {
            RemainingTTL::Never => ResolvedTTL::Never,
            RemainingTTL::Known(duration) if !duration.is_zero() => ResolvedTTL::After(duration),
            RemainingTTL::Known(_) | RemainingTTL::Unknown => return None,
        };
        let resolved = cap_ttl(resolved, self.backfill_ttl_cap);
        let resolved = cap_ttl(resolved, self.max_ttl);
        match resolved {
            ResolvedTTL::After(duration) if duration.is_zero() => None,
            other => Some(other),
        }
    }
}

fn cap_ttl(ttl: ResolvedTTL, cap: Option<Duration>) -> ResolvedTTL {
    let Some(cap) = cap else {
        return ttl;
    };
    match ttl {
        ResolvedTTL::Never => ResolvedTTL::After(cap),
        ResolvedTTL::After(duration) => ResolvedTTL::After(duration.min(cap)),
    }
}

/// Per-instance participation and policy settings for a backend.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct BackendOptions {
    /// Query this backend during reads.
    pub read: bool,
    /// Write explicit `set` operations to this backend.
    pub write: bool,
    /// Refill this backend after a later backend hits.
    pub backfill: bool,
    /// Policy applied to read failures.
    pub read_failure: ReadFailurePolicy,
    /// Policy applied to backfill failures.
    pub backfill_failure: BackfillFailurePolicy,
    /// TTL resolution policy.
    pub ttl: BackendTTLPolicy,
}

impl Default for BackendOptions {
    fn default() -> Self {
        Self::new()
    }
}

impl BackendOptions {
    /// Creates the default read/write/backfill configuration.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            read: true,
            write: true,
            backfill: true,
            read_failure: ReadFailurePolicy::Propagate,
            backfill_failure: BackfillFailurePolicy::ReportAndContinue,
            ttl: BackendTTLPolicy::new(),
        }
    }

    /// Enables or disables reads.
    #[must_use]
    pub const fn read(mut self, enabled: bool) -> Self {
        self.read = enabled;
        self
    }

    /// Enables or disables explicit writes and removals.
    #[must_use]
    pub const fn write(mut self, enabled: bool) -> Self {
        self.write = enabled;
        self
    }

    /// Enables or disables refill writes.
    #[must_use]
    pub const fn backfill(mut self, enabled: bool) -> Self {
        self.backfill = enabled;
        self
    }

    /// Sets the read failure policy.
    #[must_use]
    pub const fn read_failure(mut self, policy: ReadFailurePolicy) -> Self {
        self.read_failure = policy;
        self
    }

    /// Sets the backfill failure policy.
    #[must_use]
    pub const fn backfill_failure(mut self, policy: BackfillFailurePolicy) -> Self {
        self.backfill_failure = policy;
        self
    }

    /// Sets the backend TTL policy.
    #[must_use]
    pub const fn ttl(mut self, policy: BackendTTLPolicy) -> Self {
        self.ttl = policy;
        self
    }
}

/// Per-call policies for `get_or_load`.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct LoadOptions {
    /// TTL requested for the loaded value.
    pub ttl: TTL,
    /// Policy applied if the loader fails.
    pub loader_failure: LoaderFailurePolicy,
    /// Policy applied if caching a successfully loaded value fails.
    pub write_failure: LoadWriteFailurePolicy,
}

impl LoadOptions {
    /// Creates options with default TTL and propagated failures.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            ttl: TTL::Default,
            loader_failure: LoaderFailurePolicy::Propagate,
            write_failure: LoadWriteFailurePolicy::Propagate,
        }
    }

    /// Sets the write TTL.
    #[must_use]
    pub const fn ttl(mut self, ttl: TTL) -> Self {
        self.ttl = ttl;
        self
    }

    /// Sets the loader failure policy.
    #[must_use]
    pub const fn loader_failure(mut self, policy: LoaderFailurePolicy) -> Self {
        self.loader_failure = policy;
        self
    }

    /// Sets the cache write failure policy.
    #[must_use]
    pub const fn write_failure(mut self, policy: LoadWriteFailurePolicy) -> Self {
        self.write_failure = policy;
        self
    }
}
