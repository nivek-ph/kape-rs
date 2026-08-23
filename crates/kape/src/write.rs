use std::sync::Arc;

use crate::KapeError;

/// Validates the TTL value.
pub(crate) fn validate_ttl(ttl: i64) -> Result<(), KapeError> {
    if ttl < -1 {
        Err(KapeError::InvalidTtl(ttl))
    } else {
        Ok(())
    }
}

/// One ordered batch-write input.
#[derive(Clone, Debug)]
pub struct SetItem<K, V> {
    /// Key to write.
    pub key: K,
    /// Shared value to write.
    pub value: Arc<V>,
    /// Write TTL in milliseconds.
    pub ttl: i64,
}

impl<K, V> SetItem<K, V> {
    /// Creates a batch-write item.
    #[must_use]
    pub fn new(key: K, value: impl Into<Arc<V>>, ttl: i64) -> Self {
        Self {
            key,
            value: value.into(),
            ttl,
        }
    }
}
