use std::{hash::Hash, sync::Arc, time::Instant};

use async_trait::async_trait;
use kape::{CacheBackend, CacheEntry, KapeResult};
use moka::future::Cache;

use super::entry::{EntryExpiry, MemoryEntry};

/// An in-memory cache backend with an entry-count capacity.
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
        if let Some(entry) = self.cache.get(key).await {
            if let Some(entry) = entry.into_cache_entry_at(Instant::now())? {
                return Ok(Some(entry));
            }
            self.cache.invalidate(key).await;
        }
        Ok(None)
    }

    async fn set(&self, key: &K, value: Arc<V>, ttl: i64) -> KapeResult<()> {
        let Some(entry) = MemoryEntry::from_write(value, ttl, Instant::now())? else {
            return self.remove(key).await;
        };
        self.cache.insert(key.clone(), entry).await;
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
