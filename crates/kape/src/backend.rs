use crate::{BackendSetItem, IterationPage, KapeError, Lookup, ResolvedTTL};

use std::sync::Arc;

/// A typed cache backend implementation.
///
/// The public trait retains `K` and `V`, while all implementations use
/// [`KapeError`] so heterogeneous backends share one dynamic interface.
/// Implementations must apply `#[async_trait::async_trait]` to their `impl`
/// block.
#[async_trait::async_trait]
pub trait CacheBackend<K, V>: Send + Sync
where
    K: Sync,
    V: Send + Sync,
{
    /// Queries a key.
    async fn get(&self, key: &K) -> Result<Lookup<V>, KapeError>;

    /// Stores a shared value with an already-resolved lifetime.
    async fn set(&self, key: &K, value: Arc<V>, ttl: ResolvedTTL) -> Result<(), KapeError>;

    /// Removes a key.
    async fn remove(&self, key: &K) -> Result<(), KapeError>;

    /// Queries multiple keys, preserving input order and duplicates.
    ///
    /// The default implementation calls [`Self::get`] sequentially. Backends
    /// with a native batch operation should override it. Implementations must
    /// return exactly one result for every input key.
    async fn get_many(&self, keys: &[&K]) -> Result<Vec<Lookup<V>>, KapeError> {
        let mut results = Vec::with_capacity(keys.len());
        for key in keys {
            results.push(self.get(key).await?);
        }
        Ok(results)
    }

    /// Stores multiple already-resolved items in input order.
    ///
    /// The default implementation calls [`Self::set`] sequentially. Backends
    /// with a native batch operation should override it.
    async fn set_many(&self, items: &[BackendSetItem<'_, K, V>]) -> Result<(), KapeError> {
        for item in items {
            self.set(item.key, Arc::clone(item.value), item.ttl).await?;
        }
        Ok(())
    }

    /// Checks fresh existence for multiple keys without triggering backfill.
    ///
    /// Stale entries are reported as absent. The default implementation uses
    /// [`Self::get_many`].
    async fn has_many(&self, keys: &[&K]) -> Result<Vec<bool>, KapeError> {
        Ok(self
            .get_many(keys)
            .await?
            .into_iter()
            .map(|lookup| matches!(lookup, Lookup::Hit(_)))
            .collect())
    }

    /// Removes multiple keys in input order.
    ///
    /// The default implementation calls [`Self::remove`] sequentially.
    async fn remove_many(&self, keys: &[&K]) -> Result<(), KapeError> {
        for key in keys {
            self.remove(key).await?;
        }
        Ok(())
    }

    /// Clears every entry owned by this backend instance.
    ///
    /// A remote adapter must restrict this operation to its configured `Kape`
    /// namespace and must not clear unrelated shared storage.
    async fn clear(&self) -> Result<(), KapeError>;

    /// Returns one weakly-consistent page in backend-defined order.
    ///
    /// `cursor` is opaque and is only valid for subsequent calls against the
    /// same backend instance. `limit` is a target page size rather than a hard
    /// bound because some storage cursors return backend-chosen batch sizes.
    async fn iterate(
        &self,
        cursor: Option<&[u8]>,
        limit: usize,
    ) -> Result<IterationPage<K, V>, KapeError>;

    /// Releases backend-owned resources when an explicit shutdown exists.
    ///
    /// The default is an idempotent no-op for backends whose resources are
    /// managed by handle lifetime.
    async fn disconnect(&self) -> Result<(), KapeError> {
        Ok(())
    }
}
