#![doc = include_str!("../README.md")]

use std::{
    fmt::Debug,
    hash::{BuildHasher, RandomState},
    sync::Arc,
    time::Duration,
};

use kape::{CacheBackend, KapeError, Lookup, SetItem};

/// Returns a 16-character alphanumeric string from OS-seeded hasher state.
///
/// Use this for example and test keys that should not collide across runs.
#[must_use]
pub fn get_random_string() -> String {
    const ALPHANUMERIC: &[u8; 62] =
        b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
    let state = RandomState::new();
    (0..16)
        .map(|i| {
            let index = (state.hash_one(i) % 62) as u8;
            char::from(ALPHANUMERIC[usize::from(index)])
        })
        .collect()
}

/// Checks scalar miss, immortal and zero-TTL writes, invalid TTL rejection,
/// removal, and zero-value-safe hit semantics.
///
/// # Panics
///
/// Panics when the backend violates the public adapter contract.
pub async fn assert_backend_contract<B, K, V>(backend: &B, key: &K, value: V)
where
    B: CacheBackend<K, V>,
    K: Sync,
    V: Debug + PartialEq + Send + Sync + 'static,
{
    backend
        .remove(key)
        .await
        .expect("contract setup remove failed");
    assert!(matches!(
        backend.get(key).await.expect("contract miss read failed"),
        Lookup::Miss
    ));

    let expected = Arc::new(value);
    backend
        .set(key, Arc::clone(&expected), -1)
        .await
        .expect("contract immortal write failed");
    match backend
        .get(key)
        .await
        .expect("contract immortal read failed")
    {
        Lookup::Hit(entry) => {
            assert_eq!(entry.value.as_ref(), expected.as_ref());
            assert_eq!(entry.remaining_ttl, -1);
        }
        other @ Lookup::Miss => panic!("expected immortal hit, got {other:?}"),
    }

    assert!(matches!(
        backend.set(key, Arc::clone(&expected), -2).await,
        Err(KapeError::InvalidTtl(-2))
    ));
    assert!(matches!(
        backend.get(key).await.expect("invalid TTL changed value"),
        Lookup::Hit(entry) if entry.value == expected && entry.remaining_ttl == -1
    ));

    backend
        .set(key, Arc::clone(&expected), 0)
        .await
        .expect("contract zero-TTL write failed");
    assert!(matches!(
        backend
            .get(key)
            .await
            .expect("contract zero-TTL read failed"),
        Lookup::Miss
    ));

    backend
        .set(key, expected, -1)
        .await
        .expect("contract rewrite failed");
    backend.remove(key).await.expect("contract remove failed");
    assert!(matches!(
        backend
            .get(key)
            .await
            .expect("contract post-remove read failed"),
        Lookup::Miss
    ));
}

/// Checks positive millisecond TTL reporting and expiration.
///
/// # Panics
///
/// Panics when `ttl_ms` is not positive or the backend violates expiration.
pub async fn assert_expiring_contract<B, K, V>(backend: &B, key: &K, value: V, ttl_ms: i64)
where
    B: CacheBackend<K, V>,
    K: Sync,
    V: Debug + PartialEq + Send + Sync + 'static,
{
    assert!(ttl_ms > 0, "contract TTL must be positive");
    backend
        .set(key, Arc::new(value), ttl_ms)
        .await
        .expect("contract expiring write failed");

    match backend
        .get(key)
        .await
        .expect("contract expiring read failed")
    {
        Lookup::Hit(entry) => {
            assert!(entry.remaining_ttl > 0, "remaining TTL must be positive");
            assert!(
                entry.remaining_ttl <= ttl_ms,
                "remaining TTL exceeds requested TTL"
            );
        }
        other @ Lookup::Miss => panic!("expected expiring hit, got {other:?}"),
    }

    let wait_ms = u64::try_from(ttl_ms).expect("positive TTL must fit u64") + 25;
    std::thread::sleep(Duration::from_millis(wait_ms));
    assert!(matches!(
        backend
            .get(key)
            .await
            .expect("contract expired read failed"),
        Lookup::Miss
    ));
}

/// Checks ordered batch operations, duplicate keys, misses, pre-validation,
/// and empty-batch behavior.
///
/// # Panics
///
/// Panics when a batch operation violates the public adapter contract.
pub async fn assert_batch_contract<B, K, V>(
    backend: &B,
    first_key: &K,
    second_key: &K,
    missing_key: &K,
    first_value: V,
    second_value: V,
) where
    B: CacheBackend<K, V>,
    K: Sync,
    V: Debug + PartialEq + Send + Sync + 'static,
{
    backend
        .remove_many(&[first_key, second_key, missing_key])
        .await
        .expect("batch contract setup remove failed");

    let first_value = Arc::new(first_value);
    let second_value = Arc::new(second_value);
    backend
        .set_many(&[
            SetItem::new(first_key, Arc::clone(&first_value), -1),
            SetItem::new(second_key, Arc::clone(&second_value), 10_000),
        ])
        .await
        .expect("batch contract write failed");

    let read_keys = [first_key, missing_key, first_key, second_key];
    let results = backend
        .get_many(&read_keys)
        .await
        .expect("batch contract read failed");
    assert_eq!(results.len(), read_keys.len());
    assert!(matches!(&results[0], Lookup::Hit(entry) if entry.value == first_value));
    assert!(matches!(&results[1], Lookup::Miss));
    assert!(matches!(&results[2], Lookup::Hit(entry) if entry.value == first_value));
    assert!(matches!(&results[3], Lookup::Hit(entry) if entry.value == second_value));

    assert!(matches!(
        backend
            .set_many(&[
                SetItem::new(first_key, Arc::clone(&first_value), -1),
                SetItem::new(second_key, Arc::clone(&second_value), -2),
            ])
            .await,
        Err(KapeError::InvalidTtl(-2))
    ));

    backend
        .set_many(&[SetItem::new(first_key, Arc::clone(&first_value), 0)])
        .await
        .expect("batch zero-TTL invalidation failed");
    assert!(matches!(
        backend
            .get(first_key)
            .await
            .expect("batch zero-TTL read failed"),
        Lookup::Miss
    ));
    assert!(matches!(
        backend
            .get(second_key)
            .await
            .expect("batch zero-TTL changed another key"),
        Lookup::Hit(_)
    ));

    backend
        .remove_many(&[first_key, second_key])
        .await
        .expect("batch contract remove failed");
    assert!(
        backend
            .get_many(&[first_key, second_key])
            .await
            .expect("batch contract post-remove read failed")
            .iter()
            .all(|lookup| matches!(lookup, Lookup::Miss))
    );

    assert!(
        backend
            .get_many(&[])
            .await
            .expect("empty batch read failed")
            .is_empty()
    );
    backend
        .set_many(&[])
        .await
        .expect("empty batch write failed");
    backend
        .remove_many(&[])
        .await
        .expect("empty batch remove failed");
}

/// Checks that clear removes all entries visible to this backend.
///
/// # Panics
///
/// Panics when clear fails or leaves an entry behind.
pub async fn assert_clear_contract<B, K, V>(
    backend: &B,
    first_key: &K,
    second_key: &K,
    first_value: V,
    second_value: V,
) where
    B: CacheBackend<K, V>,
    K: Sync,
    V: Debug + PartialEq + Send + Sync + 'static,
{
    backend
        .set_many(&[
            SetItem::new(first_key, Arc::new(first_value), -1),
            SetItem::new(second_key, Arc::new(second_value), 10_000),
        ])
        .await
        .expect("clear contract write failed");

    backend.clear().await.expect("contract clear failed");
    assert!(
        backend
            .get_many(&[first_key, second_key])
            .await
            .expect("contract post-clear read failed")
            .iter()
            .all(|lookup| matches!(lookup, Lookup::Miss))
    );
}
