use std::sync::Arc;

/// A typed cache value together with its exact observed remaining lifetime.
#[derive(Clone, Debug)]
pub struct CacheEntry<V> {
    /// The shared cached value.
    pub value: Arc<V>,
    /// Exact remaining lifetime in milliseconds; `-1` means no expiry.
    pub remaining_ttl: i64,
}

impl<V> CacheEntry<V> {
    /// Creates a cache entry from a shared value and exact remaining TTL.
    #[must_use]
    pub const fn new(value: Arc<V>, remaining_ttl: i64) -> Self {
        Self {
            value,
            remaining_ttl,
        }
    }
}

/// The structural result of querying one backend instance.
#[derive(Clone, Debug)]
pub enum Lookup<V> {
    /// No usable value exists for the key.
    Miss,
    /// A fresh value exists for the key.
    Hit(CacheEntry<V>),
}
