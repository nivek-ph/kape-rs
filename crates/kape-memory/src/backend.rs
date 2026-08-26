use std::{
    hash::Hash,
    sync::Arc,
    time::{Duration, Instant},
};

use async_trait::async_trait;
use kape::{CacheBackend, CacheEntry, KapeError, KapeResult};
use moka::{Expiry, future::Cache};

use crate::MemoryError;

struct MemoryEntry<V> {
    value: Arc<V>,
    expires_at: Option<Instant>,
}

impl<V> MemoryEntry<V> {
    fn duration_until_expiry(&self, now: Instant) -> Option<Duration> {
        self.expires_at
            .map(|expires_at| expires_at.saturating_duration_since(now))
    }
}

impl<V> Clone for MemoryEntry<V> {
    fn clone(&self) -> Self {
        Self {
            value: Arc::clone(&self.value),
            expires_at: self.expires_at,
        }
    }
}

/// An expiry policy that expires entries after a duration.
struct EntryExpiry;

impl<K, V> Expiry<K, MemoryEntry<V>> for EntryExpiry {
    fn expire_after_create(
        &self,
        _key: &K,
        value: &MemoryEntry<V>,
        created_at: Instant,
    ) -> Option<Duration> {
        value.duration_until_expiry(created_at)
    }

    fn expire_after_update(
        &self,
        _key: &K,
        value: &MemoryEntry<V>,
        updated_at: Instant,
        _duration_until_expiry: Option<Duration>,
    ) -> Option<Duration> {
        // Moka otherwise preserves the previous deadline when an existing key is replaced.
        value.duration_until_expiry(updated_at)
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
            cache: Cache::builder()
                .max_capacity(max_capacity)
                .expire_after(EntryExpiry)
                .build(),
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

#[cfg(test)]
mod tests {
    use std::{sync::Arc, time::Duration};

    use kape::CacheBackend;
    use tokio::time::sleep;

    use super::MemoryBackend;

    #[tokio::test]
    async fn moka_maintenance_removes_expired_entries_without_reading_each_key() {
        let backend = MemoryBackend::<String, String>::new(100);
        for index in 0..8 {
            let key = format!("key-{index}");
            backend
                .set(&key, Arc::new(key.clone()), 500)
                .await
                .expect("expiring write failed");
        }
        backend.cache.run_pending_tasks().await;
        assert_eq!(backend.cache.entry_count(), 8);

        sleep(Duration::from_millis(1_200)).await;
        backend.cache.run_pending_tasks().await;

        assert_eq!(backend.cache.entry_count(), 0);
    }
}
