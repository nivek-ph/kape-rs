use std::{future::Future, pin::Pin, sync::Arc};

use crate::{BackendCapability, BackendSetItem, IterationPage, Lookup, ResolvedTTL, SharedError};
use async_trait::async_trait;

/// A typed cache backend implementation.
///
/// The public trait retains `K` and `V`. `Kape` only erases the concrete
/// backend type and its error after the backend is added to a cache builder.
/// Implementations must apply `#[async_trait::async_trait]` to their `impl`
/// block.
#[async_trait]
pub trait CacheBackend<K, V>: Send + Sync
where
    K: Sync,
    V: Send + Sync,
{
    /// The backend-specific error type.
    type Error: std::error::Error + Send + Sync + 'static;

    /// Queries a key.
    async fn get(&self, key: &K) -> Result<Lookup<V>, Self::Error>;

    /// Stores a shared value with an already-resolved lifetime.
    async fn set(&self, key: &K, value: Arc<V>, ttl: ResolvedTTL) -> Result<(), Self::Error>;

    /// Removes a key.
    async fn remove(&self, key: &K) -> Result<(), Self::Error>;

    /// Queries multiple keys, preserving input order and duplicates.
    ///
    /// The default implementation calls [`Self::get`] sequentially. Backends
    /// with a native batch operation should override it. Implementations must
    /// return exactly one result for every input key.
    async fn get_many(&self, keys: &[&K]) -> Result<Vec<Lookup<V>>, Self::Error> {
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
    async fn set_many(&self, items: &[BackendSetItem<'_, K, V>]) -> Result<(), Self::Error> {
        for item in items {
            self.set(item.key, Arc::clone(item.value), item.ttl).await?;
        }
        Ok(())
    }

    /// Checks fresh existence for multiple keys without triggering backfill.
    ///
    /// Stale entries are reported as absent. The default implementation uses
    /// [`Self::get_many`].
    async fn has_many(&self, keys: &[&K]) -> Result<Vec<bool>, Self::Error> {
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
    async fn remove_many(&self, keys: &[&K]) -> Result<(), Self::Error> {
        for key in keys {
            self.remove(key).await?;
        }
        Ok(())
    }

    /// Clears every entry owned by this backend instance.
    ///
    /// A remote adapter must restrict this operation to its configured `Kape`
    /// namespace and must not clear unrelated shared storage.
    async fn clear(&self) -> Result<BackendCapability<()>, Self::Error> {
        Ok(BackendCapability::Unsupported)
    }

    /// Returns one weakly-consistent page in backend-defined order.
    ///
    /// `cursor` is opaque and is only valid for subsequent calls against the
    /// same backend instance. `limit` is a target page size rather than a hard
    /// bound because some storage cursors return backend-chosen batch sizes.
    async fn iterate(
        &self,
        _cursor: Option<&[u8]>,
        _limit: usize,
    ) -> Result<BackendCapability<IterationPage<K, V>>, Self::Error> {
        Ok(BackendCapability::Unsupported)
    }

    /// Releases backend-owned resources when an explicit shutdown exists.
    ///
    /// The default is an idempotent no-op for backends whose resources are
    /// managed by handle lifetime.
    async fn disconnect(&self) -> Result<(), Self::Error> {
        Ok(())
    }
}

pub(crate) type BackendFuture<'a, T> =
    Pin<Box<dyn Future<Output = Result<T, SharedError>> + Send + 'a>>;

pub(crate) trait ErasedBackend<K, V>: Send + Sync {
    fn get<'a>(&'a self, key: &'a K) -> BackendFuture<'a, Lookup<V>>;
    fn set<'a>(&'a self, key: &'a K, value: Arc<V>, ttl: ResolvedTTL) -> BackendFuture<'a, ()>
    where
        V: 'a;
    fn remove<'a>(&'a self, key: &'a K) -> BackendFuture<'a, ()>;
    fn get_many<'a>(&'a self, keys: &'a [&'a K]) -> BackendFuture<'a, Vec<Lookup<V>>>;
    fn set_many<'a>(&'a self, items: &'a [BackendSetItem<'a, K, V>]) -> BackendFuture<'a, ()>;
    fn has_many<'a>(&'a self, keys: &'a [&'a K]) -> BackendFuture<'a, Vec<bool>>;
    fn remove_many<'a>(&'a self, keys: &'a [&'a K]) -> BackendFuture<'a, ()>;
    fn clear(&self) -> BackendFuture<'_, BackendCapability<()>>;
    fn iterate<'a>(
        &'a self,
        cursor: Option<&'a [u8]>,
        limit: usize,
    ) -> BackendFuture<'a, BackendCapability<IterationPage<K, V>>>;
    fn disconnect(&self) -> BackendFuture<'_, ()>;
}

pub(crate) struct BackendAdapter<B>(pub(crate) B);

impl<K, V, B> ErasedBackend<K, V> for BackendAdapter<B>
where
    K: Sync,
    V: Send + Sync,
    B: CacheBackend<K, V>,
{
    fn get<'a>(&'a self, key: &'a K) -> BackendFuture<'a, Lookup<V>> {
        Box::pin(async move { self.0.get(key).await.map_err(|error| Arc::new(error) as _) })
    }

    fn set<'a>(&'a self, key: &'a K, value: Arc<V>, ttl: ResolvedTTL) -> BackendFuture<'a, ()>
    where
        V: 'a,
    {
        Box::pin(async move {
            self.0
                .set(key, value, ttl)
                .await
                .map_err(|error| Arc::new(error) as _)
        })
    }

    fn remove<'a>(&'a self, key: &'a K) -> BackendFuture<'a, ()> {
        Box::pin(async move {
            self.0
                .remove(key)
                .await
                .map_err(|error| Arc::new(error) as _)
        })
    }

    fn get_many<'a>(&'a self, keys: &'a [&'a K]) -> BackendFuture<'a, Vec<Lookup<V>>> {
        Box::pin(async move {
            self.0
                .get_many(keys)
                .await
                .map_err(|error| Arc::new(error) as _)
        })
    }

    fn set_many<'a>(&'a self, items: &'a [BackendSetItem<'a, K, V>]) -> BackendFuture<'a, ()> {
        Box::pin(async move {
            self.0
                .set_many(items)
                .await
                .map_err(|error| Arc::new(error) as _)
        })
    }

    fn has_many<'a>(&'a self, keys: &'a [&'a K]) -> BackendFuture<'a, Vec<bool>> {
        Box::pin(async move {
            self.0
                .has_many(keys)
                .await
                .map_err(|error| Arc::new(error) as _)
        })
    }

    fn remove_many<'a>(&'a self, keys: &'a [&'a K]) -> BackendFuture<'a, ()> {
        Box::pin(async move {
            self.0
                .remove_many(keys)
                .await
                .map_err(|error| Arc::new(error) as _)
        })
    }

    fn clear(&self) -> BackendFuture<'_, BackendCapability<()>> {
        Box::pin(async move { self.0.clear().await.map_err(|error| Arc::new(error) as _) })
    }

    fn iterate<'a>(
        &'a self,
        cursor: Option<&'a [u8]>,
        limit: usize,
    ) -> BackendFuture<'a, BackendCapability<IterationPage<K, V>>> {
        Box::pin(async move {
            self.0
                .iterate(cursor, limit)
                .await
                .map_err(|error| Arc::new(error) as _)
        })
    }

    fn disconnect(&self) -> BackendFuture<'_, ()> {
        Box::pin(async move {
            self.0
                .disconnect()
                .await
                .map_err(|error| Arc::new(error) as _)
        })
    }
}
