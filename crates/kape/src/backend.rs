use std::{hash::Hash, sync::Arc};

use crate::{CacheEntry, KapeResult, SetItem, validate_set_items};

/// A typed cache backend implementation.
///
/// Implementations must represent misses as `None`, preserve exact remaining
/// TTL and read positions, and reject duplicate batch-write keys before
/// mutation. Apply `#[async_trait::async_trait]` to each implementation block.
#[async_trait::async_trait]
pub trait CacheBackend<K, V>: Send + Sync
where
    K: Sync,
    V: Send + Sync,
{
    /// Queries one key.
    async fn get(&self, key: &K) -> KapeResult<Option<CacheEntry<V>>>;

    /// Stores or immediately invalidates one key using a millisecond TTL.
    async fn set(&self, key: &K, value: Arc<V>, ttl: i64) -> KapeResult<()>;

    /// Removes one key.
    async fn remove(&self, key: &K) -> KapeResult<()>;

    /// Clears entries owned by this backend instance.
    async fn clear(&self) -> KapeResult<()>;

    /// Queries multiple keys while preserving input order and duplicates.
    async fn get_many(&self, keys: &[&K]) -> KapeResult<Vec<Option<CacheEntry<V>>>> {
        let mut results = Vec::with_capacity(keys.len());
        for key in keys {
            results.push(self.get(key).await?);
        }
        Ok(results)
    }

    /// Writes multiple uniquely keyed items in input order.
    async fn set_many(&self, items: &[SetItem<&K, V>]) -> KapeResult<()>
    where
        K: Eq + Hash,
    {
        validate_set_items(items)?;
        for item in items {
            self.set(item.key, Arc::clone(&item.value), item.ttl)
                .await?;
        }
        Ok(())
    }

    /// Removes multiple keys in input order.
    async fn remove_many(&self, keys: &[&K]) -> KapeResult<()> {
        for key in keys {
            self.remove(key).await?;
        }
        Ok(())
    }
}
