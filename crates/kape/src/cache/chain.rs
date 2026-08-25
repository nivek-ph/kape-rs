use std::{future::Future, hash::Hash, sync::Arc};

use crate::{
    BackendFailure, CacheBackend, KapeError, KapeResult, Operation, SetItem, validate_set_items,
    set::validate_ttl,
};

pub(super) struct ChainLink<K, V> {
    pub(super) name: Arc<str>,
    pub(super) backend: Arc<dyn CacheBackend<K, V>>,
}

pub(super) struct CacheInner<K, V> {
    pub(super) links: Vec<ChainLink<K, V>>,
    pub(super) backend_names: Box<[Arc<str>]>,
}

/// An ordered chain of named cache backend instances.
pub struct Cache<K, V> {
    pub(super) inner: Arc<CacheInner<K, V>>,
}

impl<K, V> Clone for Cache<K, V> {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}

impl<K, V> Cache<K, V>
where
    K: Send + Sync,
    V: Send + Sync,
{
    /// Returns backend instance names in configured order.
    #[must_use]
    pub fn backend_names(&self) -> &[Arc<str>] {
        &self.inner.backend_names
    }

    /// Writes every backend instance in reverse configured order.
    ///
    /// # Errors
    ///
    /// Returns invalid TTL before mutation or the first named write failure.
    pub async fn set(&self, key: &K, value: Arc<V>, ttl: i64) -> KapeResult<()> {
        validate_ttl(ttl)?;
        for link in self.inner.links.iter().rev() {
            link.backend
                .set(key, Arc::clone(&value), ttl)
                .await
                .map_err(|source| backend_error(Operation::Set, link, source))?;
        }
        Ok(())
    }

    /// Removes a key from every backend instance in reverse configured order.
    ///
    /// # Errors
    ///
    /// Returns the first named removal failure.
    pub async fn remove(&self, key: &K) -> KapeResult<()> {
        for link in self.inner.links.iter().rev() {
            link.backend
                .remove(key)
                .await
                .map_err(|source| backend_error(Operation::Remove, link, source))?;
        }
        Ok(())
    }

    /// Clears every backend instance in reverse configured order.
    ///
    /// # Errors
    ///
    /// Returns the first named clear failure.
    pub async fn clear(&self) -> KapeResult<()> {
        for link in self.inner.links.iter().rev() {
            link.backend
                .clear()
                .await
                .map_err(|source| backend_error(Operation::Clear, link, source))?;
        }
        Ok(())
    }

    /// Gets a value or computes and writes it with an explicit TTL.
    ///
    /// # Errors
    ///
    /// Returns invalid TTL, read, loader, or named write failures.
    pub async fn get_or_load<F, Fut, E>(&self, key: &K, loader: F, ttl: i64) -> KapeResult<Arc<V>>
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = Result<V, E>>,
        E: std::error::Error + Send + Sync + 'static,
    {
        validate_ttl(ttl)?;
        if let Some(value) = self.get(key).await? {
            return Ok(value);
        }

        let value = Arc::new(loader().await.map_err(KapeError::loader)?);
        self.set(key, Arc::clone(&value), ttl).await?;
        Ok(value)
    }

    /// Gets a value or computes it, derives its TTL from the loaded value, and writes it.
    ///
    /// The TTL selector runs only after a cache miss and a successful loader call. The
    /// selected TTL is validated before any backend is mutated.
    ///
    /// # Errors
    ///
    /// Returns invalid TTL, read, loader, or named write failures.
    pub async fn wrap<F, Fut, E, T>(&self, key: &K, loader: F, ttl: T) -> KapeResult<Arc<V>>
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = Result<V, E>>,
        E: std::error::Error + Send + Sync + 'static,
        T: FnOnce(&V) -> i64,
    {
        if let Some(value) = self.get(key).await? {
            return Ok(value);
        }

        let value = Arc::new(loader().await.map_err(KapeError::loader)?);
        let resolved_ttl = ttl(value.as_ref());
        validate_ttl(resolved_ttl)?;
        self.set(key, Arc::clone(&value), resolved_ttl).await?;
        Ok(value)
    }

    /// Writes uniquely keyed items to every backend in reverse configured order.
    ///
    /// # Errors
    ///
    /// Returns invalid TTL or duplicate-key input before mutation, or the first
    /// named batch failure.
    pub async fn set_many(&self, items: &[SetItem<K, V>]) -> KapeResult<()>
    where
        K: Eq + Hash,
    {
        validate_set_items(items)?;
        if items.is_empty() {
            return Ok(());
        }

        for link in self.inner.links.iter().rev() {
            let borrowed = items
                .iter()
                .map(|item| SetItem {
                    key: &item.key,
                    value: Arc::clone(&item.value),
                    ttl: item.ttl,
                })
                .collect::<Vec<_>>();
            link.backend
                .set_many(&borrowed)
                .await
                .map_err(|source| backend_error(Operation::Set, link, source))?;
        }
        Ok(())
    }

    /// Removes multiple keys from every backend in reverse configured order.
    ///
    /// # Errors
    ///
    /// Returns the first named batch removal failure.
    pub async fn remove_many(&self, keys: &[K]) -> KapeResult<()> {
        if keys.is_empty() {
            return Ok(());
        }
        let backend_keys = keys.iter().collect::<Vec<_>>();
        for link in self.inner.links.iter().rev() {
            link.backend
                .remove_many(&backend_keys)
                .await
                .map_err(|source| backend_error(Operation::Remove, link, source))?;
        }
        Ok(())
    }
}

/// Creates a named backend failure.
pub(super) fn backend_error<K, V>(
    operation: Operation,
    link: &ChainLink<K, V>,
    source: KapeError,
) -> KapeError {
    KapeError::Backend(BackendFailure {
        operation,
        backend: Arc::clone(&link.name),
        source: source.into_source(),
    })
}
