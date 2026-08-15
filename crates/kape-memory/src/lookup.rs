use std::{
    sync::Arc,
    time::{Duration, Instant},
};

use kape::{CacheEntry, IterationEntry, IterationFreshness, Lookup, RemainingTTL};

pub(crate) fn lookup<V>(
    value: Arc<V>,
    expires_at: Option<Instant>,
    now: Instant,
    retain_stale: bool,
) -> Option<Lookup<V>> {
    let (remaining_ttl, fresh) = remaining_ttl(expires_at, now);
    let entry = CacheEntry::new(value, remaining_ttl);
    if fresh {
        Some(Lookup::Hit(entry))
    } else if retain_stale {
        Some(Lookup::Stale(entry))
    } else {
        None
    }
}

pub(crate) fn iteration_entry<K, V>(
    key: K,
    value: Arc<V>,
    expires_at: Option<Instant>,
    now: Instant,
) -> IterationEntry<K, V> {
    let (remaining_ttl, fresh) = remaining_ttl(expires_at, now);
    IterationEntry {
        key,
        value,
        remaining_ttl,
        freshness: if fresh {
            IterationFreshness::Fresh
        } else {
            IterationFreshness::Stale
        },
    }
}

fn remaining_ttl(expires_at: Option<Instant>, now: Instant) -> (RemainingTTL, bool) {
    match expires_at {
        None => (RemainingTTL::Never, true),
        Some(expires_at) if expires_at > now => {
            (RemainingTTL::Known(expires_at.duration_since(now)), true)
        }
        Some(_) => (RemainingTTL::Known(Duration::ZERO), false),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn value() -> Arc<String> {
        Arc::new(String::from("value"))
    }

    #[test]
    fn projects_fresh_stale_and_dropped_entries() {
        let now = Instant::now();
        let future = now + Duration::from_millis(1);

        assert!(matches!(
            lookup(value(), None, now, true).unwrap(),
            Lookup::Hit(entry) if entry.remaining_ttl == RemainingTTL::Never
        ));
        assert!(matches!(
            lookup(value(), Some(future), now, true).unwrap(),
            Lookup::Hit(entry) if entry.remaining_ttl == RemainingTTL::Known(Duration::from_millis(1))
        ));
        assert!(matches!(
            lookup(value(), Some(now), now, true).unwrap(),
            Lookup::Stale(entry) if entry.remaining_ttl == RemainingTTL::Known(Duration::ZERO)
        ));
        assert!(lookup(value(), Some(now), now, false).is_none());
    }

    #[test]
    fn iteration_includes_stale_entries() {
        let now = Instant::now();
        let entry = iteration_entry("key", value(), Some(now), now);
        assert_eq!(entry.freshness, IterationFreshness::Stale);
        assert_eq!(entry.remaining_ttl, RemainingTTL::Known(Duration::ZERO));
    }
}
