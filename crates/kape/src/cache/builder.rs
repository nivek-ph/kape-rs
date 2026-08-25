use std::{collections::HashSet, sync::Arc};

use crate::{CacheBackend, KapeError, KapeResult};

use super::chain::{Cache, CacheInner, ChainLink};

/// Builds a [`Cache`] while retaining typed keys and values.
pub struct CacheBuilder<K, V> {
    links: Vec<ChainLink<K, V>>,
}

impl<K, V> Cache<K, V>
where
    K: Send + Sync,
    V: Send + Sync,
{
    /// Starts an empty cache builder.
    #[must_use]
    pub fn builder() -> CacheBuilder<K, V> {
        CacheBuilder::new()
    }
}

impl<K, V> CacheBuilder<K, V>
where
    K: Send + Sync,
    V: Send + Sync,
{
    /// Creates an empty builder.
    #[must_use]
    pub const fn new() -> Self {
        Self { links: Vec::new() }
    }

    /// Appends one named backend instance.
    #[must_use]
    pub fn backend<B>(mut self, name: impl Into<Arc<str>>, backend: B) -> Self
    where
        B: CacheBackend<K, V> + 'static,
    {
        self.links.push(ChainLink {
            name: name.into(),
            backend: Arc::new(backend),
        });
        self
    }

    /// Validates backend names and builds the cache chain.
    ///
    /// # Errors
    ///
    /// Returns an error for an empty chain, blank name, or duplicate name.
    pub fn build(self) -> KapeResult<Cache<K, V>> {
        if self.links.is_empty() {
            return Err(KapeError::NoBackends);
        }

        let mut seen = HashSet::with_capacity(self.links.len());
        for link in &self.links {
            if link.name.trim().is_empty() {
                return Err(KapeError::EmptyBackendName);
            }
            if !seen.insert(&link.name) {
                return Err(KapeError::DuplicateBackendName(link.name.to_string()));
            }
        }

        let backend_names = self
            .links
            .iter()
            .map(|link| Arc::clone(&link.name))
            .collect();
        Ok(Cache {
            inner: Arc::new(CacheInner {
                links: self.links,
                backend_names,
            }),
        })
    }
}

impl<K, V> Default for CacheBuilder<K, V>
where
    K: Send + Sync,
    V: Send + Sync,
{
    fn default() -> Self {
        Self::new()
    }
}
