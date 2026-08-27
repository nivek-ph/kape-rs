use std::{
    sync::Arc,
    time::{Duration, Instant},
};

use kape::{CacheEntry, KapeError, KapeResult};
use moka::Expiry;

use crate::MemoryError;

pub(super) struct MemoryEntry<V> {
    value: Arc<V>,
    expires_at: Option<Instant>,
}

impl<V> MemoryEntry<V> {
    /// Builds an entry from a Write TTL, or returns `None` for immediate invalidation.
    pub(super) fn from_write(value: Arc<V>, ttl: i64, now: Instant) -> KapeResult<Option<Self>> {
        if ttl < -1 {
            return Err(KapeError::InvalidTtl(ttl));
        }
        if ttl == 0 {
            return Ok(None);
        }

        let expires_at = if ttl == -1 {
            None
        } else {
            let duration =
                Duration::from_millis(u64::try_from(ttl).map_err(|_| KapeError::InvalidTtl(ttl))?);
            Some(now.checked_add(duration).ok_or(MemoryError::TtlOverflow)?)
        };
        Ok(Some(Self { value, expires_at }))
    }

    /// Projects this stored entry into Kape's exact Remaining TTL model at `now`.
    pub(super) fn into_cache_entry_at(
        self,
        now: Instant,
    ) -> Result<Option<CacheEntry<V>>, MemoryError> {
        let Some(expires_at) = self.expires_at else {
            return Ok(Some(CacheEntry::new(self.value, -1)));
        };
        if expires_at <= now {
            return Ok(None);
        }

        let Some(remaining_ttl) = remaining_ttl(expires_at.duration_since(now))? else {
            return Ok(None);
        };
        Ok(Some(CacheEntry::new(self.value, remaining_ttl)))
    }

    fn duration_until_expiry(&self, now: Instant) -> Option<Duration> {
        self.expires_at
            .map(|expires_at| expires_at.saturating_duration_since(now))
    }
}

fn remaining_ttl(duration: Duration) -> Result<Option<i64>, MemoryError> {
    let millis = duration.as_millis();
    if millis == 0 {
        return Ok(None);
    }
    i64::try_from(millis)
        .map(Some)
        .map_err(|_| MemoryError::TtlOverflow)
}

impl<V> Clone for MemoryEntry<V> {
    fn clone(&self) -> Self {
        Self {
            value: Arc::clone(&self.value),
            expires_at: self.expires_at,
        }
    }
}

/// Adapts an entry's absolute expiry to Moka's relative expiry interface.
pub(super) struct EntryExpiry;

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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn write_ttl_classifies_invalid_immediate_and_immortal_entries() {
        let now = Instant::now();
        let value = Arc::new("value".to_owned());

        assert!(matches!(
            MemoryEntry::from_write(Arc::clone(&value), -2, now),
            Err(KapeError::InvalidTtl(-2))
        ));
        assert!(
            MemoryEntry::from_write(Arc::clone(&value), 0, now)
                .unwrap()
                .is_none()
        );

        let entry = MemoryEntry::from_write(value, -1, now).unwrap().unwrap();
        assert!(matches!(
            entry.into_cache_entry_at(now).unwrap(),
            Some(entry) if entry.remaining_ttl == -1
        ));
    }

    #[test]
    fn finite_entry_projects_remaining_ttl_and_expiration_without_sleeping() {
        let written_at = Instant::now();
        let entry = MemoryEntry::from_write(Arc::new("value".to_owned()), 25, written_at)
            .unwrap()
            .unwrap();

        assert!(matches!(
            entry.clone().into_cache_entry_at(written_at + Duration::from_millis(5)).unwrap(),
            Some(entry) if entry.remaining_ttl == 20
        ));
        assert!(
            entry
                .into_cache_entry_at(written_at + Duration::from_millis(25))
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn submillisecond_remainder_projects_as_a_miss() {
        let now = Instant::now();
        let entry = MemoryEntry {
            value: Arc::new("value".to_owned()),
            expires_at: Some(now + Duration::from_micros(999)),
        };

        assert!(entry.into_cache_entry_at(now).unwrap().is_none());
    }

    #[test]
    fn unrepresentable_remaining_ttl_is_rejected() {
        let max_i64 = u64::try_from(i64::MAX).expect("i64::MAX must fit u64");
        let duration = Duration::from_millis(max_i64 + 1);

        assert_eq!(remaining_ttl(duration), Err(MemoryError::TtlOverflow));
    }

    #[test]
    fn moka_expiry_uses_the_same_absolute_deadline_for_create_and_update() {
        let written_at = Instant::now();
        let entry = MemoryEntry::from_write(Arc::new("value".to_owned()), 25, written_at)
            .unwrap()
            .unwrap();
        let policy = EntryExpiry;

        assert_eq!(
            policy.expire_after_create(&"key", &entry, written_at),
            Some(Duration::from_millis(25))
        );
        assert_eq!(
            policy.expire_after_update(
                &"key",
                &entry,
                written_at + Duration::from_millis(5),
                Some(Duration::from_secs(1)),
            ),
            Some(Duration::from_millis(20))
        );
    }
}
