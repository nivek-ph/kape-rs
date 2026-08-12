#![doc = include_str!("../README.md")]

use std::{fmt::Debug, sync::Arc, time::Duration};

use kape::{
    BackendCapability, BackendSetItem, CacheBackend, IterationFreshness, Lookup, RemainingTTL,
    ResolvedTTL,
};

/// Checks miss, immortal round-trip, removal, and zero-value-safe hit semantics.
///
/// The supplied key must be isolated from other tests. Failures panic with the
/// backend error so adapter integration tests retain their native diagnostics.
///
/// # Panics
///
/// Panics when the backend violates any required contract behavior or returns
/// an operation error.
pub async fn assert_backend_contract<B, K, V>(backend: &B, key: &K, value: V)
where
    B: CacheBackend<K, V>,
    B::Error: Debug,
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
        .set(key, Arc::clone(&expected), ResolvedTTL::Never)
        .await
        .expect("contract immortal write failed");
    match backend
        .get(key)
        .await
        .expect("contract immortal read failed")
    {
        Lookup::Hit(entry) => {
            assert_eq!(entry.value.as_ref(), expected.as_ref());
            assert_eq!(entry.remaining_ttl, RemainingTTL::Never);
        }
        other => panic!("expected immortal hit, got {other:?}"),
    }

    backend.remove(key).await.expect("contract remove failed");
    assert!(matches!(
        backend
            .get(key)
            .await
            .expect("contract post-remove read failed"),
        Lookup::Miss
    ));
}

/// Checks that a backend reports a positive remaining TTL no greater than the
/// requested duration.
///
/// # Panics
///
/// Panics when `ttl` is zero, the backend returns an operation error, or its
/// hit and remaining-TTL metadata violate the contract.
pub async fn assert_expiring_contract<B, K, V>(backend: &B, key: &K, value: V, ttl: Duration)
where
    B: CacheBackend<K, V>,
    B::Error: Debug,
    K: Sync,
    V: Debug + PartialEq + Send + Sync + 'static,
{
    assert!(!ttl.is_zero(), "contract TTL must be positive");
    backend
        .set(key, Arc::new(value), ResolvedTTL::After(ttl))
        .await
        .expect("contract expiring write failed");

    match backend
        .get(key)
        .await
        .expect("contract expiring read failed")
    {
        Lookup::Hit(entry) => match entry.remaining_ttl {
            RemainingTTL::Known(remaining) => {
                assert!(!remaining.is_zero(), "remaining TTL must be positive");
                assert!(remaining <= ttl, "remaining TTL exceeds requested TTL");
            }
            other => panic!("expected known remaining TTL, got {other:?}"),
        },
        other => panic!("expected expiring hit, got {other:?}"),
    }
}

/// Checks ordered batch set/get/has/remove behavior, including duplicate keys
/// and misses.
///
/// All supplied keys must be isolated from other tests and mutually distinct.
///
/// # Panics
///
/// Panics when a batch result changes input length/order, treats a miss as a
/// hit, loses a duplicate, fails to remove an item, or returns an operation
/// error.
pub async fn assert_batch_contract<B, K, V>(
    backend: &B,
    first_key: &K,
    second_key: &K,
    missing_key: &K,
    first_value: V,
    second_value: V,
) where
    B: CacheBackend<K, V>,
    B::Error: Debug,
    K: Sync,
    V: Debug + PartialEq + Send + Sync + 'static,
{
    let setup_keys = [first_key, second_key, missing_key];
    backend
        .remove_many(&setup_keys)
        .await
        .expect("batch contract setup remove failed");

    let first_value = Arc::new(first_value);
    let second_value = Arc::new(second_value);
    let items = [
        BackendSetItem {
            key: first_key,
            value: &first_value,
            ttl: ResolvedTTL::Never,
        },
        BackendSetItem {
            key: second_key,
            value: &second_value,
            ttl: ResolvedTTL::After(Duration::from_secs(10)),
        },
    ];
    backend
        .set_many(&items)
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

    assert_eq!(
        backend
            .has_many(&read_keys)
            .await
            .expect("batch contract has failed"),
        [true, false, true, true]
    );

    backend
        .remove_many(&[first_key, second_key])
        .await
        .expect("batch contract remove failed");
    assert_eq!(
        backend
            .has_many(&[first_key, second_key])
            .await
            .expect("batch contract post-remove has failed"),
        [false, false]
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
    assert!(
        backend
            .has_many(&[])
            .await
            .expect("empty batch has failed")
            .is_empty()
    );
    backend
        .remove_many(&[])
        .await
        .expect("empty batch remove failed");
}

/// Checks iteration, namespace-scoped clear, and idempotent disconnect.
///
/// The supplied keys must be isolated from other tests. This contract calls
/// `disconnect` last because a backend may release shared resources.
///
/// # Panics
///
/// Panics when a capability is unsupported, iteration loses either entry,
/// clear leaves an entry behind, disconnect fails, or cursor progress does not
/// terminate within a defensive bound.
pub async fn assert_management_contract<B, K, V>(
    backend: &B,
    first_key: &K,
    second_key: &K,
    first_value: V,
    second_value: V,
) where
    B: CacheBackend<K, V>,
    B::Error: Debug,
    K: Clone + Debug + Eq + Sync,
    V: Debug + PartialEq + Send + Sync + 'static,
{
    let keys = [first_key, second_key];
    backend
        .remove_many(&keys)
        .await
        .expect("management contract setup remove failed");
    let first_value = Arc::new(first_value);
    let second_value = Arc::new(second_value);
    backend
        .set_many(&[
            BackendSetItem {
                key: first_key,
                value: &first_value,
                ttl: ResolvedTTL::Never,
            },
            BackendSetItem {
                key: second_key,
                value: &second_value,
                ttl: ResolvedTTL::After(Duration::from_secs(10)),
            },
        ])
        .await
        .expect("management contract write failed");

    let mut cursor = None;
    let mut seen = Vec::new();
    for _ in 0..1_000 {
        let capability = backend
            .iterate(cursor.as_deref(), 1)
            .await
            .expect("management contract iteration failed");
        let BackendCapability::Supported(page) = capability else {
            panic!("management contract iteration is unsupported");
        };
        for entry in page.entries {
            assert_eq!(entry.freshness, IterationFreshness::Fresh);
            seen.push(entry.key);
        }
        cursor = page.next_cursor;
        if cursor.is_none() {
            break;
        }
    }
    assert!(cursor.is_none(), "management iteration did not terminate");
    assert!(seen.contains(first_key), "iteration lost first key");
    assert!(seen.contains(second_key), "iteration lost second key");

    assert!(matches!(
        backend.clear().await.expect("management clear failed"),
        BackendCapability::Supported(())
    ));
    assert_eq!(
        backend
            .has_many(&keys)
            .await
            .expect("management post-clear has failed"),
        [false, false]
    );
    backend
        .disconnect()
        .await
        .expect("management disconnect failed");
    backend
        .disconnect()
        .await
        .expect("management repeated disconnect failed");
}
