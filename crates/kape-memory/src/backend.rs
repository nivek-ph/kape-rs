use std::{hash::Hash, sync::Arc, time::Instant};

use async_trait::async_trait;
use kape::{CacheBackend, CacheEntry, KapeError, KapeResult};
use moka::future::Cache;

use crate::MemoryError;

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

/// A process-local cache backend with an entry-count capacity.
///
/// Clones share the same storage. Independently constructed instances are
/// isolated, including for [`CacheBackend::clear`].
pub struct MemoryBackend<K, V> {
    cache: Cache<K, MemoryEntry<V>>,
}

impl<K, V> Clone for MemoryBackend<K, V> {
    fn clone(&self) -> Self {
        Self {
            cache: self.cache.clone(),
        }
    }
}

impl<K, V> MemoryBackend<K, V>
where
    K: Clone + Eq + Hash + Send + Sync + 'static,
    V: Send + Sync + 'static,
{
    /// Creates a backend capped at `max_capacity` entries.
    #[must_use]
    pub fn new(max_capacity: u64) -> Self {
        Self {
            cache: Cache::new(max_capacity),
        }
    }
}

#[async_trait]
impl<K, V> CacheBackend<K, V> for MemoryBackend<K, V>
where
    K: Clone + Eq + Hash + Send + Sync + 'static,
    V: Send + Sync + 'static,
{
    async fn get(&self, key: &K) -> KapeResult<Option<CacheEntry<V>>> {
        let Some(entry) = self.cache.get(key).await else {
            return Ok(None);
        };

        let Some(expires_at) = entry.expires_at else {
            return Ok(Some(CacheEntry::new(entry.value, -1)));
        };
        let now = Instant::now();
        if expires_at <= now {
            self.cache.invalidate(key).await;
            return Ok(None);
        }

        let remaining_ttl = expires_at.duration_since(now).as_millis();
        if remaining_ttl == 0 {
            self.cache.invalidate(key).await;
            return Ok(None);
        }
        let remaining_ttl = i64::try_from(remaining_ttl).map_err(|_| MemoryError::TtlOverflow)?;
        Ok(Some(CacheEntry::new(entry.value, remaining_ttl)))
    }

    async fn set(&self, key: &K, value: Arc<V>, ttl: i64) -> KapeResult<()> {
        if ttl < -1 {
            return Err(KapeError::InvalidTtl(ttl));
        }
        if ttl == 0 {
            return self.remove(key).await;
        }

        let expires_at = if ttl == -1 {
            None
        } else {
            let ttl = std::time::Duration::from_millis(
                u64::try_from(ttl).map_err(|_| KapeError::InvalidTtl(ttl))?,
            );
            Some(
                Instant::now()
                    .checked_add(ttl)
                    .ok_or(MemoryError::TtlOverflow)?,
            )
        };
        self.cache
            .insert(key.clone(), MemoryEntry { value, expires_at })
            .await;
        Ok(())
    }

    async fn remove(&self, key: &K) -> KapeResult<()> {
        self.cache.invalidate(key).await;
        Ok(())
    }

    async fn clear(&self) -> KapeResult<()> {
        self.cache.invalidate_all();
        self.cache.run_pending_tasks().await;
        Ok(())
    }
}
