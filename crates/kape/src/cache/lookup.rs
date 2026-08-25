use std::{
    sync::Arc,
    time::{Duration, Instant},
};

use crate::{KapeError, KapeResult, Operation};

use super::chain::{Cache, ChainLink, backend_error};

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

/// A validated hit tied to its source position and read start time.
struct LocatedHit<V> {
    link_index: usize,
    read_at: Instant,
    hit: CacheHit<V>,
}

impl<V> LocatedHit<V> {
    fn new<K>(
        link: &ChainLink<K, V>,
        link_index: usize,
        read_at: Instant,
        entry: CacheEntry<V>,
    ) -> KapeResult<Self> {
        let entry = validate_hit(link, entry)?;
        Ok(Self {
            link_index,
            read_at,
            hit: CacheHit {
                backend: Arc::clone(&link.name),
                entry,
            },
        })
    }

    /// Backfills earlier links before returning the public hit.
    async fn backfill<K>(self, key: &K, links: &[ChainLink<K, V>]) -> KapeResult<CacheHit<V>>
    where
        K: Send + Sync,
        V: Send + Sync,
    {
        for link in links[..self.link_index].iter().rev() {
            let Some(ttl) =
                remaining_backfill_ttl(self.hit.entry.remaining_ttl, self.read_at.elapsed())
            else {
                break;
            };
            link.backend
                .set(key, Arc::clone(&self.hit.entry.value), ttl)
                .await
                .map_err(|source| backend_error(Operation::Backfill, link, source))?;
        }
        Ok(self.hit)
    }
}

impl<K, V> Cache<K, V>
where
    K: Send + Sync,
    V: Send + Sync,
{
    /// Reads backend instances in configured order and returns full metadata.
    ///
    /// # Errors
    ///
    /// Returns the first named read, contract, or backfill failure.
    pub async fn lookup(&self, key: &K) -> KapeResult<Option<CacheHit<V>>> {
        for (link_index, link) in self.inner.links.iter().enumerate() {
            let read_at = Instant::now();
            let entry = link
                .backend
                .get(key)
                .await
                .map_err(|source| backend_error(Operation::Get, link, source))?;
            if let Some(entry) = entry {
                let located = LocatedHit::new(link, link_index, read_at, entry)?;
                let hit = located.backfill(key, &self.inner.links).await?;
                return Ok(Some(hit));
            }
        }
        Ok(None)
    }

    /// Reads a cached value, discarding lookup metadata.
    ///
    /// # Errors
    ///
    /// Returns the same failures as [`Self::lookup`].
    pub async fn get(&self, key: &K) -> KapeResult<Option<Arc<V>>> {
        Ok(self.lookup(key).await?.map(CacheHit::into_value))
    }

    /// Reads multiple keys while preserving input order and duplicates.
    ///
    /// # Errors
    ///
    /// Returns the first named read, contract, or backfill failure.
    pub async fn lookup_many(&self, keys: &[K]) -> KapeResult<Vec<Option<CacheHit<V>>>> {
        if keys.is_empty() {
            return Ok(Vec::new());
        }

        let mut hits = (0..keys.len()).map(|_| None).collect::<Vec<_>>();

        for (link_index, link) in self.inner.links.iter().enumerate() {
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
            let read_at = Instant::now();
            let results = link
                .backend
                .get_many(&backend_keys)
                .await
                .map_err(|source| backend_error(Operation::Get, link, source))?;
            validate_batch_result_len(link, unresolved.len(), results.len())?;

            for (item_index, entry) in unresolved.into_iter().zip(results) {
                if let Some(entry) = entry {
                    hits[item_index] = Some(LocatedHit::new(link, link_index, read_at, entry)?);
                }
            }
        }

        let mut results = Vec::with_capacity(keys.len());
        for (key, located) in keys.iter().zip(hits) {
            let hit = match located {
                Some(located) => Some(located.backfill(key, &self.inner.links).await?),
                None => None,
            };
            results.push(hit);
        }
        Ok(results)
    }

    /// Reads multiple values while preserving input order and duplicates.
    ///
    /// # Errors
    ///
    /// Returns the same failures as [`Self::lookup_many`].
    pub async fn get_many(&self, keys: &[K]) -> KapeResult<Vec<Option<Arc<V>>>> {
        Ok(self
            .lookup_many(keys)
            .await?
            .into_iter()
            .map(|hit| hit.map(CacheHit::into_value))
            .collect())
    }
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

fn validate_hit<K, V>(link: &ChainLink<K, V>, entry: CacheEntry<V>) -> KapeResult<CacheEntry<V>> {
    match entry.remaining_ttl {
        -1 | 1.. => Ok(entry),
        remaining_ttl => Err(backend_error(
            Operation::Get,
            link,
            KapeError::backend(InvalidRemainingTtlError(remaining_ttl)),
        )),
    }
}

/// Ensures each requested key has one result so batch positions remain aligned.
fn validate_batch_result_len<K, V>(
    link: &ChainLink<K, V>,
    expected: usize,
    actual: usize,
) -> KapeResult<()> {
    if expected == actual {
        Ok(())
    } else {
        Err(backend_error(
            Operation::Get,
            link,
            KapeError::backend(BatchResultLengthError { expected, actual }),
        ))
    }
}

#[derive(Debug, thiserror::Error)]
#[error("backend returned a cache entry with invalid remaining TTL {0}")]
struct InvalidRemainingTtlError(i64);

#[derive(Debug, thiserror::Error)]
#[error("backend batch returned {actual} result(s), expected {expected}")]
struct BatchResultLengthError {
    expected: usize,
    actual: usize,
}

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
