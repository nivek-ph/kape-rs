use std::sync::Arc;

use crate::{KapeError, Lookup, SetItem, write::validate_ttl};

/// A typed cache backend implementation.
///
/// Implementations must preserve structural misses, exact remaining TTL, input
/// order, and duplicate positions. Apply `#[async_trait::async_trait]` to each
/// implementation block.
#[async_trait::async_trait]
pub trait CacheBackend<K, V>: Send + Sync
where
    K: Sync,
    V: Send + Sync,
{
    /// Queries one key.
    async fn get(&self, key: &K) -> Result<Lookup<V>, KapeError>;

    /// Stores or immediately invalidates one key using a millisecond TTL.
    async fn set(&self, key: &K, value: Arc<V>, ttl: i64) -> Result<(), KapeError>;

    /// Removes one key.
    async fn remove(&self, key: &K) -> Result<(), KapeError>;

    /// Clears entries owned by this backend instance.
    async fn clear(&self) -> Result<(), KapeError>;

    /// Queries multiple keys while preserving input order and duplicates.
    async fn get_many(&self, keys: &[&K]) -> Result<Vec<Lookup<V>>, KapeError> {
        let mut results = Vec::with_capacity(keys.len());
        for key in keys {
            results.push(self.get(key).await?);
        }
        Ok(results)
    }

    /// Writes multiple items in input order.
    async fn set_many(&self, items: &[SetItem<&K, V>]) -> Result<(), KapeError> {
        for item in items {
            validate_ttl(item.ttl)?;
        }
        for item in items {
            self.set(item.key, Arc::clone(&item.value), item.ttl)
                .await?;
        }
        Ok(())
    }

    /// Removes multiple keys in input order.
    async fn remove_many(&self, keys: &[&K]) -> Result<(), KapeError> {
        for key in keys {
            self.remove(key).await?;
        }
        Ok(())
    }
}
