use std::{
    collections::HashSet,
    fmt,
    future::Future,
    sync::Arc,
    time::{Duration, Instant},
};

use crate::{
    BackendFailure, CacheBackend, KapeError as Error, Operation, SetItem, write::validate_ttl,
};

struct CacheLayer<K, V> {
    name: Arc<str>,
    backend: Arc<dyn CacheBackend<K, V>>,
}

struct CacheInner<K, V> {
    layers: Vec<CacheLayer<K, V>>,
    backend_names: Box<[Arc<str>]>,
}

/// A typed cache value together with its exact observed remaining lifetime.
#[derive(Clone, Debug)]
pub struct CacheEntry<V> {
    /// The shared cached value.
    pub value: Arc<V>,

    /// Exact remaining lifetime in milliseconds; `-1` means no expiry.
    pub remaining_ttl: i64,
}

impl<V> CacheEntry<V> {
    /// Creates a cache entry from a shared value and exact remaining TTL.
    #[must_use]
    pub const fn new(value: Arc<V>, remaining_ttl: i64) -> Self {
        Self {
            value,
            remaining_ttl,
        }
    }
}

/// An entry found in the cache chain together with its source backend.
#[derive(Clone, Debug)]
pub struct CacheHit<V> {
    /// Name of the backend instance that returned the entry.
    pub backend: Arc<str>,

    /// The entry returned by the backend.
    pub entry: CacheEntry<V>,
}

impl<V> CacheHit<V> {
    fn into_value(self) -> Arc<V> {
        self.entry.value
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
    pub async fn lookup(&self, key: &K) -> Result<Option<CacheHit<V>>, Error> {
        for (index, layer) in self.inner.layers.iter().enumerate() {
            let read_started_at = Instant::now();
            let entry = layer
                .backend
                .get(key)
                .await
                .map_err(|source| backend_error(Operation::Get, layer, source))?;
            if let Some(entry) = entry {
                let entry = validate_hit(layer, entry)?;
                self.backfill(key, &entry, index, read_started_at).await?;
                return Ok(Some(CacheHit {
                    entry,
                    backend: Arc::clone(&layer.name),
                }));
            }
        }
        Ok(None)
    }

    /// Reads a cached value, discarding lookup metadata.
    ///
    /// # Errors
    ///
    /// Returns the same failures as [`Self::lookup`].
    pub async fn get(&self, key: &K) -> Result<Option<Arc<V>>, Error> {
        Ok(self.lookup(key).await?.map(CacheHit::into_value))
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
                .map_err(|source| backend_error(Operation::Set, layer, source))?;
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
                .map_err(|source| backend_error(Operation::Remove, layer, source))?;
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
                .map_err(|source| backend_error(Operation::Clear, layer, source))?;
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
    pub async fn lookup_many(&self, keys: &[K]) -> Result<Vec<Option<CacheHit<V>>>, Error> {
        if keys.is_empty() {
            return Ok(Vec::new());
        }

        let mut hits = (0..keys.len()).map(|_| None).collect::<Vec<_>>();

        for (layer_index, layer) in self.inner.layers.iter().enumerate() {
            let unresolved = hits
                .iter()
                .enumerate()
                .filter_map(|(index, hit)| hit.is_none().then_some(index))
                .collect::<Vec<_>>();
            if unresolved.is_empty() {
                break;
            }

            let backend_keys = unresolved
                .iter()
                .map(|index| &keys[*index])
                .collect::<Vec<_>>();
            let read_started_at = Instant::now();
            let results = layer
                .backend
                .get_many(&backend_keys)
                .await
                .map_err(|source| backend_error(Operation::Get, layer, source))?;
            validate_batch_result_len(layer, unresolved.len(), results.len())?;

            for (item_index, entry) in unresolved.into_iter().zip(results) {
                if let Some(entry) = entry {
                    let entry = validate_hit(layer, entry)?;
                    hits[item_index] = Some(LocatedHit {
                        layer_index,
                        read_started_at,
                        hit: CacheHit {
                            backend: Arc::clone(&layer.name),
                            entry,
                        },
                    });
                }
            }
        }

        for (item_index, located) in hits.iter().enumerate() {
            let Some(located) = located else {
                continue;
            };
            self.backfill(
                &keys[item_index],
                &located.hit.entry,
                located.layer_index,
                located.read_started_at,
            )
            .await?;
        }

        Ok(hits
            .into_iter()
            .map(|located| located.map(|located| located.hit))
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
            .map(|hit| hit.map(CacheHit::into_value))
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
                .map_err(|source| backend_error(Operation::Set, layer, source))?;
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
                .map_err(|source| backend_error(Operation::Remove, layer, source))?;
        }
        Ok(())
    }

    async fn backfill(
        &self,
        key: &K,
        entry: &CacheEntry<V>,
        hit_index: usize,
        read_started_at: Instant,
    ) -> Result<(), Error> {
        for layer in self.inner.layers[..hit_index].iter().rev() {
            let Some(ttl) = remaining_backfill_ttl(entry.remaining_ttl, read_started_at.elapsed())
            else {
                break;
            };
            layer
                .backend
                .set(key, Arc::clone(&entry.value), ttl)
                .await
                .map_err(|source| backend_error(Operation::Backfill, layer, source))?;
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

struct LocatedHit<V> {
    layer_index: usize,
    read_started_at: Instant,
    hit: CacheHit<V>,
}

/// Computes the relative TTL immediately before a destination write is invoked.
///
/// A backend may apply that TTL later during `set`; its internal write latency
/// remains storage-specific and is not observable by the core in advance.
fn remaining_backfill_ttl(remaining_ttl: i64, elapsed: Duration) -> Option<i64> {
    if remaining_ttl == -1 {
        return Some(-1);
    }

    let elapsed_ms = elapsed.as_nanos().div_ceil(1_000_000);
    let remaining_ttl = u128::try_from(remaining_ttl).ok()?;
    let adjusted_ttl = remaining_ttl.checked_sub(elapsed_ms)?;
    i64::try_from(adjusted_ttl).ok().filter(|ttl| *ttl > 0)
}

fn validate_hit<K, V>(
    layer: &CacheLayer<K, V>,
    entry: CacheEntry<V>,
) -> Result<CacheEntry<V>, Error> {
    match entry.remaining_ttl {
        -1 | 1.. => Ok(entry),
        remaining_ttl => Err(backend_error(
            Operation::Get,
            layer,
            Error::backend(InvalidRemainingTtlError(remaining_ttl)),
        )),
    }
}

/// Creates a named backend failure.
fn backend_error<K, V>(operation: Operation, layer: &CacheLayer<K, V>, source: Error) -> Error {
    Error::Backend(BackendFailure {
        operation,
        backend: Arc::clone(&layer.name),
        source: source.into_source(),
    })
}

/// Ensures each requested key has one result so batch positions remain aligned.
fn validate_batch_result_len<K, V>(
    layer: &CacheLayer<K, V>,
    expected: usize,
    actual: usize,
) -> Result<(), Error> {
    if expected == actual {
        Ok(())
    } else {
        Err(backend_error(
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
            "backend returned a cache entry with invalid remaining TTL {}",
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

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::remaining_backfill_ttl;

    #[test]
    fn finite_backfill_ttl_deducts_elapsed_time_conservatively() {
        assert_eq!(
            remaining_backfill_ttl(100, Duration::from_nanos(1)),
            Some(99)
        );
        assert_eq!(
            remaining_backfill_ttl(100, Duration::from_millis(20)),
            Some(80)
        );
    }

    #[test]
    fn exhausted_backfill_ttl_skips_the_write() {
        assert_eq!(
            remaining_backfill_ttl(100, Duration::from_millis(100)),
            None
        );
        assert_eq!(
            remaining_backfill_ttl(100, Duration::from_millis(101)),
            None
        );
    }

    #[test]
    fn non_expiring_backfill_ttl_is_unchanged() {
        assert_eq!(remaining_backfill_ttl(-1, Duration::from_mins(1)), Some(-1));
    }
}
