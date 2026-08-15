use crate::MemoryError;
use async_trait::async_trait;
use kape::{CacheBackend, IterationPage, KapeError, Lookup, ResolvedTTL};
use moka::future::Cache;
use std::{hash::Hash, sync::Arc, time::Instant};

/// Internal value representation used by the current storage engine.
struct MemoryEntry<V> {
    value: Arc<V>,
    expires_at: Option<Instant>,
}

impl<V> Clone for MemoryEntry<V> {
    fn clone(&self) -> Self {
        Self {
            value: Arc::clone(&self.value),
            expires_at: self.expires_at,
        }
    }
}

/// Kape's local in-memory backend.
///
/// The storage engine is intentionally not exposed so it can evolve without
/// changing the public backend API.
pub struct MemoryBackend<K, V> {
    cache: Cache<K, MemoryEntry<V>>,
    retain_stale: bool,
}

impl<K, V> Clone for MemoryBackend<K, V> {
    fn clone(&self) -> Self {
        Self {
            cache: self.cache.clone(),
            retain_stale: self.retain_stale,
        }
    }
}

impl<K, V> MemoryBackend<K, V>
where
    K: Clone + Eq + Hash + Send + Sync + 'static,
    V: Send + Sync + 'static,
{
    /// Creates an in-memory backend with a maximum entry count.
    #[must_use]
    pub fn new(max_capacity: u64) -> Self {
        Self {
            cache: Cache::new(max_capacity),
            retain_stale: true,
        }
    }

    /// Chooses whether expired entries are returned as stale candidates.
    #[must_use]
    pub const fn retain_stale(mut self, retain: bool) -> Self {
        self.retain_stale = retain;
        self
    }
}

#[async_trait]
impl<K, V> CacheBackend<K, V> for MemoryBackend<K, V>
where
    K: Clone + Eq + Hash + Send + Sync + 'static,
    V: Send + Sync + 'static,
{
    async fn get(&self, key: &K) -> Result<Lookup<V>, KapeError> {
        let Some(entry) = self.cache.get(key).await else {
            return Ok(Lookup::Miss);
        };

        match crate::lookup::lookup(
            entry.value,
            entry.expires_at,
            Instant::now(),
            self.retain_stale,
        ) {
            Some(lookup) => Ok(lookup),
            None => {
                self.cache.invalidate(key).await;
                Ok(Lookup::Miss)
            }
        }
    }

    async fn set(&self, key: &K, value: Arc<V>, ttl: ResolvedTTL) -> Result<(), KapeError> {
        let expires_at = match ttl {
            ResolvedTTL::Never => None,
            ResolvedTTL::After(duration) => Some(
                Instant::now()
                    .checked_add(duration)
                    .ok_or(MemoryError::TTLOverflow)?,
            ),
        };
        self.cache
            .insert(key.clone(), MemoryEntry { value, expires_at })
            .await;
        Ok(())
    }

    async fn remove(&self, key: &K) -> Result<(), KapeError> {
        self.cache.invalidate(key).await;
        Ok(())
    }

    async fn clear(&self) -> Result<(), KapeError> {
        self.cache.invalidate_all();
        self.cache.run_pending_tasks().await;
        Ok(())
    }

    async fn iterate(
        &self,
        cursor: Option<&[u8]>,
        limit: usize,
    ) -> Result<IterationPage<K, V>, KapeError> {
        let offset = decode_cursor(cursor)?;
        let now = Instant::now();
        let mut entries = self
            .cache
            .iter()
            .skip(offset)
            .take(limit.saturating_add(1))
            .map(|(key, entry)| {
                crate::lookup::iteration_entry(
                    key.as_ref().clone(),
                    entry.value,
                    entry.expires_at,
                    now,
                )
            })
            .collect::<Vec<_>>();
        let has_more = entries.len() > limit;
        entries.truncate(limit);
        let next_offset = offset
            .checked_add(entries.len())
            .ok_or(MemoryError::InvalidCursor)?;
        let next_offset = u64::try_from(next_offset).map_err(|_| MemoryError::InvalidCursor)?;
        let next_cursor = has_more.then(|| next_offset.to_be_bytes().to_vec());
        Ok(IterationPage {
            entries,
            next_cursor,
        })
    }
}

fn decode_cursor(cursor: Option<&[u8]>) -> Result<usize, MemoryError> {
    let Some(cursor) = cursor else {
        return Ok(0);
    };
    let bytes: [u8; 8] = cursor.try_into().map_err(|_| MemoryError::InvalidCursor)?;
    usize::try_from(u64::from_be_bytes(bytes)).map_err(|_| MemoryError::InvalidCursor)
}
