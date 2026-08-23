use std::{collections::HashSet, fmt, future::Future, sync::Arc};

use crate::{
    BackendFailure, CacheBackend, CacheEntry, KapeError as Error, Lookup, Operation, SetItem,
    write::validate_ttl,
};

struct CacheLayer<K, V> {
    name: Arc<str>,
    backend: Arc<dyn CacheBackend<K, V>>,
}

struct CacheInner<K, V> {
    layers: Vec<CacheLayer<K, V>>,
    backend_names: Box<[Arc<str>]>,
}

/// The structural result of querying the full cache chain.
#[derive(Clone, Debug)]
pub enum CacheLookup<V> {
    /// Every configured backend instance missed.
    Miss,
    /// A backend instance returned a fresh value.
    Hit {
        /// Shared cached value.
        value: Arc<V>,
        /// Name of the backend instance that returned the value.
        backend: Arc<str>,
        /// Exact remaining lifetime in milliseconds; `-1` means no expiry.
        remaining_ttl: i64,
    },
}

impl<V> CacheLookup<V> {
    /// Returns the contained value when this lookup is a Hit.
    #[must_use]
    pub const fn value(&self) -> Option<&Arc<V>> {
        match self {
            Self::Miss => None,
            Self::Hit { value, .. } => Some(value),
        }
    }

    fn into_value(self) -> Option<Arc<V>> {
        match self {
            Self::Miss => None,
            Self::Hit { value, .. } => Some(value),
        }
    }
}

/// An ordered chain of named cache backend instances.
pub struct Cache<K, V> {
    inner: Arc<CacheInner<K, V>>,
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
    /// Starts an empty cache builder.
    #[must_use]
    pub fn builder() -> CacheBuilder<K, V> {
        CacheBuilder::new()
    }

    /// Returns backend instance names in configured order.
    #[must_use]
    pub fn backend_names(&self) -> &[Arc<str>] {
        &self.inner.backend_names
    }

    /// Reads backend instances in configured order and returns full metadata.
    ///
    /// # Errors
    ///
    /// Returns the first named read, contract, or backfill failure.
    pub async fn lookup(&self, key: &K) -> Result<CacheLookup<V>, Error> {
        for (index, layer) in self.inner.layers.iter().enumerate() {
            let lookup = layer
                .backend
                .get(key)
                .await
                .map_err(|source| named_failure(Operation::Get, layer, source))?;
            match lookup {
                Lookup::Miss => {}
                Lookup::Hit(entry) => {
                    let entry = validate_hit(layer, entry)?;
                    self.backfill(key, &entry, index).await?;
                    return Ok(CacheLookup::Hit {
                        value: entry.value,
                        backend: Arc::clone(&layer.name),
                        remaining_ttl: entry.remaining_ttl,
                    });
                }
            }
        }
        Ok(CacheLookup::Miss)
    }

    /// Reads a cached value, discarding lookup metadata.
    ///
    /// # Errors
    ///
    /// Returns the same failures as [`Self::lookup`].
    pub async fn get(&self, key: &K) -> Result<Option<Arc<V>>, Error> {
        Ok(self.lookup(key).await?.into_value())
    }

    /// Writes every backend instance in reverse configured order.
    ///
    /// # Errors
    ///
    /// Returns invalid TTL before mutation or the first named write failure.
    pub async fn set(&self, key: &K, value: Arc<V>, ttl: i64) -> Result<(), Error> {
        validate_ttl(ttl)?;
        for layer in self.inner.layers.iter().rev() {
            layer
                .backend
                .set(key, Arc::clone(&value), ttl)
                .await
                .map_err(|source| named_failure(Operation::Set, layer, source))?;
        }
        Ok(())
    }

    /// Removes a key from every backend instance in reverse configured order.
    ///
    /// # Errors
    ///
    /// Returns the first named removal failure.
    pub async fn remove(&self, key: &K) -> Result<(), Error> {
        for layer in self.inner.layers.iter().rev() {
            layer
                .backend
                .remove(key)
                .await
                .map_err(|source| named_failure(Operation::Remove, layer, source))?;
        }
        Ok(())
    }

    /// Clears every backend instance in reverse configured order.
    ///
    /// # Errors
    ///
    /// Returns the first named clear failure.
    pub async fn clear(&self) -> Result<(), Error> {
        for layer in self.inner.layers.iter().rev() {
            layer
                .backend
                .clear()
                .await
                .map_err(|source| named_failure(Operation::Clear, layer, source))?;
        }
        Ok(())
    }

    /// Gets a value or computes and writes it with an explicit TTL.
    ///
    /// # Errors
    ///
    /// Returns invalid TTL, read, loader, or named write failures.
    pub async fn get_or_load<F, Fut, E>(
        &self,
        key: &K,
        loader: F,
        ttl: i64,
    ) -> Result<Arc<V>, Error>
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = Result<V, E>>,
        E: std::error::Error + Send + Sync + 'static,
    {
        validate_ttl(ttl)?;
        if let Some(value) = self.get(key).await? {
            return Ok(value);
        }

        let value = Arc::new(loader().await.map_err(Error::loader)?);
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
    pub async fn wrap<F, Fut, E, T>(&self, key: &K, loader: F, ttl: T) -> Result<Arc<V>, Error>
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = Result<V, E>>,
        E: std::error::Error + Send + Sync + 'static,
        T: FnOnce(&V) -> i64,
    {
        if let Some(value) = self.get(key).await? {
            return Ok(value);
        }

        let value = Arc::new(loader().await.map_err(Error::loader)?);
        let resolved_ttl = ttl(value.as_ref());
        validate_ttl(resolved_ttl)?;
        self.set(key, Arc::clone(&value), resolved_ttl).await?;
        Ok(value)
    }

    /// Reads multiple keys while preserving input order and duplicates.
    ///
    /// # Errors
    ///
    /// Returns the first named read, contract, or backfill failure.
    pub async fn lookup_many(&self, keys: &[K]) -> Result<Vec<CacheLookup<V>>, Error> {
        if keys.is_empty() {
            return Ok(Vec::new());
        }

        let mut states = (0..keys.len())
            .map(|_| BatchLookupState::default())
            .collect::<Vec<_>>();

        for (layer_index, layer) in self.inner.layers.iter().enumerate() {
            let unresolved = states
                .iter()
                .enumerate()
                .filter_map(|(index, state)| state.lookup.is_none().then_some(index))
                .collect::<Vec<_>>();
            if unresolved.is_empty() {
                break;
            }

            let backend_keys = unresolved
                .iter()
                .map(|index| &keys[*index])
                .collect::<Vec<_>>();
            let results = layer
                .backend
                .get_many(&backend_keys)
                .await
                .map_err(|source| named_failure(Operation::Get, layer, source))?;
            validate_batch_len(layer, unresolved.len(), results.len())?;

            for (item_index, lookup) in unresolved.into_iter().zip(results) {
                if let Lookup::Hit(entry) = lookup {
                    let entry = validate_hit(layer, entry)?;
                    states[item_index].hit_index = Some(layer_index);
                    states[item_index].lookup = Some(CacheLookup::Hit {
                        value: entry.value,
                        backend: Arc::clone(&layer.name),
                        remaining_ttl: entry.remaining_ttl,
                    });
                }
            }
        }

        for state in &mut states {
            if state.lookup.is_none() {
                state.lookup = Some(CacheLookup::Miss);
            }
        }

        for item_index in 0..states.len() {
            let Some(hit_index) = states[item_index].hit_index else {
                continue;
            };
            let Some(lookup) = states[item_index].lookup.as_ref() else {
                continue;
            };
            let (value, remaining_ttl) = match lookup {
                CacheLookup::Miss => continue,
                CacheLookup::Hit {
                    value,
                    remaining_ttl,
                    ..
                } => (Arc::clone(value), *remaining_ttl),
            };
            self.backfill(
                &keys[item_index],
                &CacheEntry::new(value, remaining_ttl),
                hit_index,
            )
            .await?;
        }

        Ok(states
            .into_iter()
            .map(|state| state.lookup.unwrap_or(CacheLookup::Miss))
            .collect())
    }

    /// Reads multiple values while preserving input order and duplicates.
    ///
    /// # Errors
    ///
    /// Returns the same failures as [`Self::lookup_many`].
    pub async fn get_many(&self, keys: &[K]) -> Result<Vec<Option<Arc<V>>>, Error> {
        Ok(self
            .lookup_many(keys)
            .await?
            .into_iter()
            .map(CacheLookup::into_value)
            .collect())
    }

    /// Writes multiple items to every backend in reverse configured order.
    ///
    /// # Errors
    ///
    /// Returns invalid TTL before mutation or the first named batch failure.
    pub async fn set_many(&self, items: &[SetItem<K, V>]) -> Result<(), Error> {
        for item in items {
            validate_ttl(item.ttl)?;
        }
        if items.is_empty() {
            return Ok(());
        }

        for layer in self.inner.layers.iter().rev() {
            let borrowed = items
                .iter()
                .map(|item| SetItem {
                    key: &item.key,
                    value: Arc::clone(&item.value),
                    ttl: item.ttl,
                })
                .collect::<Vec<_>>();
            layer
                .backend
                .set_many(&borrowed)
                .await
                .map_err(|source| named_failure(Operation::Set, layer, source))?;
        }
        Ok(())
    }

    /// Removes multiple keys from every backend in reverse configured order.
    ///
    /// # Errors
    ///
    /// Returns the first named batch removal failure.
    pub async fn remove_many(&self, keys: &[K]) -> Result<(), Error> {
        if keys.is_empty() {
            return Ok(());
        }
        let backend_keys = keys.iter().collect::<Vec<_>>();
        for layer in self.inner.layers.iter().rev() {
            layer
                .backend
                .remove_many(&backend_keys)
                .await
                .map_err(|source| named_failure(Operation::Remove, layer, source))?;
        }
        Ok(())
    }

    async fn backfill(
        &self,
        key: &K,
        entry: &CacheEntry<V>,
        hit_index: usize,
    ) -> Result<(), Error> {
        for layer in self.inner.layers[..hit_index].iter().rev() {
            layer
                .backend
                .set(key, Arc::clone(&entry.value), entry.remaining_ttl)
                .await
                .map_err(|source| named_failure(Operation::Backfill, layer, source))?;
        }
        Ok(())
    }
}

/// Builds a [`Cache`] while retaining typed keys and values.
pub struct CacheBuilder<K, V> {
    layers: Vec<CacheLayer<K, V>>,
}

impl<K, V> CacheBuilder<K, V>
where
    K: Send + Sync,
    V: Send + Sync,
{
    /// Creates an empty builder.
    #[must_use]
    pub const fn new() -> Self {
        Self { layers: Vec::new() }
    }

    /// Appends one named backend instance.
    #[must_use]
    pub fn backend<B>(mut self, name: impl Into<Arc<str>>, backend: B) -> Self
    where
        B: CacheBackend<K, V> + 'static,
    {
        self.layers.push(CacheLayer {
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
    pub fn build(self) -> Result<Cache<K, V>, Error> {
        if self.layers.is_empty() {
            return Err(Error::NoBackends);
        }

        let mut seen = HashSet::with_capacity(self.layers.len());
        for layer in &self.layers {
            if layer.name.trim().is_empty() {
                return Err(Error::EmptyBackendName);
            }
            if !seen.insert(&layer.name) {
                return Err(Error::DuplicateBackendName(layer.name.to_string()));
            }
        }

        let backend_names = self
            .layers
            .iter()
            .map(|layer| Arc::clone(&layer.name))
            .collect();
        Ok(Cache {
            inner: Arc::new(CacheInner {
                layers: self.layers,
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

struct BatchLookupState<V> {
    lookup: Option<CacheLookup<V>>,
    hit_index: Option<usize>,
}

impl<V> Default for BatchLookupState<V> {
    fn default() -> Self {
        Self {
            lookup: None,
            hit_index: None,
        }
    }
}

fn validate_hit<K, V>(
    layer: &CacheLayer<K, V>,
    entry: CacheEntry<V>,
) -> Result<CacheEntry<V>, Error> {
    if entry.remaining_ttl == -1 || entry.remaining_ttl > 0 {
        Ok(entry)
    } else {
        Err(named_failure(
            Operation::Get,
            layer,
            Error::backend(InvalidRemainingTtlError(entry.remaining_ttl)),
        ))
    }
}

fn named_failure<K, V>(operation: Operation, layer: &CacheLayer<K, V>, source: Error) -> Error {
    Error::Backend(BackendFailure {
        operation,
        backend: Arc::clone(&layer.name),
        source: source.into_source(),
    })
}

fn validate_batch_len<K, V>(
    layer: &CacheLayer<K, V>,
    expected: usize,
    actual: usize,
) -> Result<(), Error> {
    if expected == actual {
        Ok(())
    } else {
        Err(named_failure(
            Operation::Get,
            layer,
            Error::backend(BatchResultLengthError { expected, actual }),
        ))
    }
}

#[derive(Debug)]
struct InvalidRemainingTtlError(i64);

impl fmt::Display for InvalidRemainingTtlError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "backend returned Hit with invalid remaining TTL {}",
            self.0
        )
    }
}

impl std::error::Error for InvalidRemainingTtlError {}

#[derive(Debug)]
struct BatchResultLengthError {
    expected: usize,
    actual: usize,
}

impl fmt::Display for BatchResultLengthError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "backend batch returned {} result(s), expected {}",
            self.actual, self.expected
        )
    }
}

impl std::error::Error for BatchResultLengthError {}
