#![doc = include_str!("../README.md")]

use std::{
    hash::Hash,
    sync::Arc,
    time::{Duration, Instant},
};

use async_trait::async_trait;
use kape::{
    CacheBackend, CacheEntry, IterationEntry, IterationFreshness, IterationPage, KapeError, Lookup,
    RemainingTTL, ResolvedTTL,
};
use moka::future::Cache;
use thiserror::Error;

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

/// An in-memory adapter failure.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum MemoryError {
    /// The requested TTL cannot be represented by `Instant`.
    #[error("TTL exceeds the in-memory clock range")]
    TTLOverflow,
    /// The iteration cursor was not produced by this adapter.
    #[error("invalid memory iteration cursor")]
    InvalidCursor,
}

impl From<MemoryError> for KapeError {
    fn from(error: MemoryError) -> Self {
        Self::backend(error)
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

        let Some(expires_at) = entry.expires_at else {
            return Ok(Lookup::Hit(CacheEntry::new(
                entry.value,
                RemainingTTL::Never,
            )));
        };
        let now = Instant::now();
        if expires_at > now {
            return Ok(Lookup::Hit(CacheEntry::new(
                entry.value,
                RemainingTTL::Known(expires_at.duration_since(now)),
            )));
        }

        if self.retain_stale {
            Ok(Lookup::Stale(CacheEntry::new(
                entry.value,
                RemainingTTL::Known(Duration::ZERO),
            )))
        } else {
            self.cache.invalidate(key).await;
            Ok(Lookup::Miss)
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
                let (remaining_ttl, freshness) = match entry.expires_at {
                    None => (RemainingTTL::Never, IterationFreshness::Fresh),
                    Some(expires_at) if expires_at > now => (
                        RemainingTTL::Known(expires_at.duration_since(now)),
                        IterationFreshness::Fresh,
                    ),
                    Some(_) => (
                        RemainingTTL::Known(Duration::ZERO),
                        IterationFreshness::Stale,
                    ),
                };
                IterationEntry {
                    key: key.as_ref().clone(),
                    value: entry.value,
                    remaining_ttl,
                    freshness,
                }
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
