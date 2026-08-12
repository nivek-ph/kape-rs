use std::{sync::Arc, time::Duration};

/// A caller's requested lifetime for an explicit cache write.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum TTL {
    /// Resolve the destination backend's default TTL.
    #[default]
    Default,
    /// Request an entry without expiration.
    Never,
    /// Request expiration after the supplied duration.
    After(Duration),
}

/// Context supplied when selecting an explicit write TTL per backend.
///
/// The backend index is its absolute position in the configured chain, even
/// when some earlier backends do not participate in explicit writes.
#[derive(Clone, Copy, Debug)]
pub struct TTLContext<'a, K, V> {
    /// Unique backend instance name.
    pub backend: &'a str,
    /// Zero-based position in the user-defined backend chain.
    pub backend_index: usize,
    /// Key being written.
    pub key: &'a K,
    /// Value being written.
    pub value: &'a V,
}

/// One typed item supplied to a batch write.
#[derive(Clone, Debug)]
pub struct SetItem<K, V> {
    /// Key to write.
    pub key: K,
    /// Shared value to write.
    pub value: Arc<V>,
    /// Fallback TTL for this item before per-backend dynamic selection.
    pub ttl: TTL,
}

impl<K, V> SetItem<K, V> {
    /// Creates a batch write item.
    #[must_use]
    pub fn new(key: K, value: impl Into<Arc<V>>, ttl: TTL) -> Self {
        Self {
            key,
            value: value.into(),
            ttl,
        }
    }
}

/// One batch write after core TTL policy resolution.
///
/// Backend implementations receive these borrowed items from
/// [`CacheBackend::set_many`](crate::CacheBackend::set_many).
#[derive(Clone, Copy, Debug)]
pub struct BackendSetItem<'a, K, V> {
    /// Key to write.
    pub key: &'a K,
    /// Shared value to write.
    pub value: &'a Arc<V>,
    /// TTL already resolved for this destination backend.
    pub ttl: ResolvedTTL,
}

/// A TTL after core policy resolution, ready for a backend adapter.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ResolvedTTL {
    /// Store without expiration.
    Never,
    /// Expire after the supplied duration.
    After(Duration),
}

/// Expiry information observed while reading an entry.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RemainingTTL {
    /// The entry does not expire.
    Never,
    /// The backend can report the entry's remaining lifetime.
    Known(Duration),
    /// The backend cannot report the entry's remaining lifetime.
    Unknown,
}

/// A typed cache value together with its observed remaining lifetime.
#[derive(Clone, Debug)]
pub struct CacheEntry<V> {
    /// The shared value. Local backends can retain this directly.
    pub value: Arc<V>,
    /// Remaining lifetime reported by the backend.
    pub remaining_ttl: RemainingTTL,
}

impl<V> CacheEntry<V> {
    /// Creates an entry from a shared value and remaining lifetime.
    #[must_use]
    pub const fn new(value: Arc<V>, remaining_ttl: RemainingTTL) -> Self {
        Self {
            value,
            remaining_ttl,
        }
    }
}

/// The structural result of querying one backend.
#[derive(Clone, Debug)]
pub enum Lookup<V> {
    /// No value exists for the key.
    Miss,
    /// A fresh value exists for the key.
    Hit(CacheEntry<V>),
    /// An expired value is retained and may be used by a stale policy.
    Stale(CacheEntry<V>),
}

/// Freshness of an entry returned by backend iteration.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum IterationFreshness {
    /// The entry is currently usable as a normal cache hit.
    Fresh,
    /// The entry has expired but is still retained by the backend.
    Stale,
}

/// One typed entry returned by backend iteration.
#[derive(Clone, Debug)]
pub struct IterationEntry<K, V> {
    /// Decoded cache key.
    pub key: K,
    /// Shared cached value.
    pub value: Arc<V>,
    /// Remaining lifetime reported by the backend.
    pub remaining_ttl: RemainingTTL,
    /// Whether the retained entry is fresh or stale.
    pub freshness: IterationFreshness,
}

/// One weakly-consistent page returned by backend iteration.
///
/// Its entry count can differ from the requested target page size.
#[derive(Clone, Debug)]
pub struct IterationPage<K, V> {
    /// Entries in backend-defined order.
    pub entries: Vec<IterationEntry<K, V>>,
    /// Opaque cursor for the next call, or `None` when iteration is complete.
    pub next_cursor: Option<Vec<u8>>,
}
