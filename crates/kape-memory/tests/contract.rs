use std::{sync::Arc, time::Duration};

use futures_lite::future::block_on;
use kape::{Cache, CacheBackend, Lookup, ResolvedTTL, TTL};
use kape_memory::MemoryBackend;
use kape_testkit::{
    assert_backend_contract, assert_batch_contract, assert_expiring_contract,
    assert_management_contract,
};

#[test]
fn satisfies_backend_contract() {
    block_on(async {
        let backend = MemoryBackend::<String, String>::new(100);
        assert_backend_contract(&backend, &"contract".to_owned(), String::new()).await;
        assert_expiring_contract(
            &backend,
            &"ttl".to_owned(),
            "value".to_owned(),
            Duration::from_secs(10),
        )
        .await;
        assert_batch_contract(
            &backend,
            &"batch-first".to_owned(),
            &"batch-second".to_owned(),
            &"batch-missing".to_owned(),
            "first".to_owned(),
            "second".to_owned(),
        )
        .await;
        assert_management_contract(
            &backend,
            &"management-first".to_owned(),
            &"management-second".to_owned(),
            "first".to_owned(),
            "second".to_owned(),
        )
        .await;
    });
}

#[test]
fn can_retain_or_drop_stale_entries() {
    block_on(async {
        let retained = MemoryBackend::<String, String>::new(10);
        retained
            .set(
                &"key".to_owned(),
                "value".to_owned().into(),
                ResolvedTTL::After(Duration::ZERO),
            )
            .await
            .unwrap();
        assert!(matches!(
            retained.get(&"key".to_owned()).await.unwrap(),
            Lookup::Stale(_)
        ));

        let dropped = MemoryBackend::<String, String>::new(10).retain_stale(false);
        dropped
            .set(
                &"key".to_owned(),
                "value".to_owned().into(),
                ResolvedTTL::After(Duration::ZERO),
            )
            .await
            .unwrap();
        assert!(matches!(
            dropped.get(&"key".to_owned()).await.unwrap(),
            Lookup::Miss
        ));
    });
}

#[test]
fn cache_applies_dynamic_ttl_to_each_memory_backend() {
    block_on(async {
        let hot = MemoryBackend::<String, String>::new(10);
        let shared = MemoryBackend::<String, String>::new(10);
        let cache = Cache::builder()
            .backend("hot", hot.clone())
            .backend("shared", shared.clone())
            .build()
            .expect("cache should build");
        let key = "dynamic".to_owned();

        cache
            .set_with_ttl(
                &key,
                Arc::new("value".to_owned()),
                TTL::Never,
                |context| match context.backend {
                    "hot" => Some(TTL::After(Duration::from_secs(2))),
                    "shared" => Some(TTL::After(Duration::from_secs(20))),
                    _ => None,
                },
            )
            .await
            .expect("dynamic write should succeed");

        let hot_remaining = remaining_ttl(hot.get(&key).await.expect("hot read should succeed"));
        let shared_remaining =
            remaining_ttl(shared.get(&key).await.expect("shared read should succeed"));

        assert!(hot_remaining <= Duration::from_secs(2));
        assert!(shared_remaining <= Duration::from_secs(20));
        assert!(shared_remaining.saturating_sub(hot_remaining) >= Duration::from_secs(17));
    });
}

fn remaining_ttl(lookup: Lookup<String>) -> Duration {
    match lookup {
        Lookup::Hit(entry) => match entry.remaining_ttl {
            kape::RemainingTTL::Known(remaining) => remaining,
            other => panic!("expected known remaining TTL, got {other:?}"),
        },
        other => panic!("expected hit, got {other:?}"),
    }
}
