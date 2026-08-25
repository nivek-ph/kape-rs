use std::{collections::HashMap, hash::Hash, sync::Arc};

use crate::{KapeError, KapeResult};

/// Validates the TTL value.
pub(crate) fn validate_ttl(ttl: i64) -> KapeResult<()> {
    if ttl < -1 {
        Err(KapeError::InvalidTtl(ttl))
    } else {
        Ok(())
    }
}

/// Validates TTLs and rejects duplicate keys before mutation.
///
/// # Errors
///
/// Returns [`KapeError::InvalidTtl`] for an invalid TTL or
/// [`KapeError::DuplicateBatchKey`] for the first repeated key.
#[doc(hidden)]
pub fn validate_set_items<K, V>(items: &[SetItem<K, V>]) -> KapeResult<()>
where
    K: Eq + Hash,
{
    let mut positions = HashMap::with_capacity(items.len());
    for (index, item) in items.iter().enumerate() {
        validate_ttl(item.ttl)?;
        if let Some(first_index) = positions.insert(&item.key, index) {
            return Err(KapeError::DuplicateBatchKey {
                first_index,
                duplicate_index: index,
            });
        }
    }
    Ok(())
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

impl<K, V> From<(K, V, i64)> for SetItem<K, V> {
    fn from((key, value, ttl): (K, V, i64)) -> Self {
        Self::new(key, value, ttl)
    }
}

impl<'a, K, V> From<&'a SetItem<K, V>> for SetItem<&'a K, V> {
    fn from(item: &'a SetItem<K, V>) -> Self {
        Self {
            key: &item.key,
            value: Arc::clone(&item.value),
            ttl: item.ttl,
        }
    }
}
