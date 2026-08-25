use std::{collections::HashMap, hash::Hash, sync::Arc};

use crate::KapeError;

/// Validates the TTL value.
pub(crate) fn validate_ttl(ttl: i64) -> Result<(), KapeError> {
    if ttl < -1 {
        Err(KapeError::InvalidTtl(ttl))
    } else {
        Ok(())
    }
}

/// Validates write TTLs and rejects duplicate batch keys before mutation.
///
/// # Errors
///
/// Returns [`KapeError::InvalidTtl`] for an invalid TTL or
/// [`KapeError::DuplicateBatchKey`] for the first repeated key.
#[doc(hidden)]
pub fn validate_set_items<K, V>(items: &[SetItem<K, V>]) -> Result<(), KapeError>
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
