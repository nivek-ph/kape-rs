use std::sync::Arc;

use futures_lite::future::block_on;
use kape::{CacheBackend, Lookup};
use kape_memory::MemoryBackend;
use kape_testkit::{
    assert_backend_contract, assert_batch_contract, assert_clear_contract, assert_expiring_contract,
};

#[test]
fn satisfies_backend_contract() {
    block_on(async {
        let backend = MemoryBackend::<String, String>::new(100);
        assert_backend_contract(&backend, &"contract".to_owned(), String::new()).await;
        assert_expiring_contract(&backend, &"ttl".to_owned(), "value".to_owned(), 50).await;
        assert_batch_contract(
            &backend,
            &"batch-first".to_owned(),
            &"batch-second".to_owned(),
            &"batch-missing".to_owned(),
            "first".to_owned(),
            "second".to_owned(),
        )
        .await;
        assert_clear_contract(
            &backend,
            &"clear-first".to_owned(),
            &"clear-second".to_owned(),
            "first".to_owned(),
            "second".to_owned(),
        )
        .await;
    });
}

#[test]
fn clear_is_scoped_to_the_memory_backend_instance() {
    block_on(async {
        let first = MemoryBackend::<String, String>::new(10);
        let second = MemoryBackend::<String, String>::new(10);
        let key = "same-key".to_owned();

        first
            .set(&key, Arc::new("first".to_owned()), -1)
            .await
            .unwrap();
        second
            .set(&key, Arc::new("second".to_owned()), -1)
            .await
            .unwrap();
        first.clear().await.unwrap();

        assert!(matches!(first.get(&key).await.unwrap(), Lookup::Miss));
        assert!(matches!(
            second.get(&key).await.unwrap(),
            Lookup::Hit(entry) if entry.value.as_str() == "second"
        ));
    });
}

#[test]
fn capacity_is_an_entry_count_upper_bound() {
    block_on(async {
        let backend = MemoryBackend::<String, String>::new(1);
        let keys = (0..8)
            .map(|index| format!("key-{index}"))
            .collect::<Vec<_>>();
        for key in &keys {
            backend
                .set(key, Arc::new(key.clone()), -1)
                .await
                .expect("capacity test write failed");
        }

        for _ in 0..100 {
            let references = keys.iter().collect::<Vec<_>>();
            let hits = backend
                .get_many(&references)
                .await
                .expect("capacity test read failed")
                .into_iter()
                .filter(|lookup| matches!(lookup, Lookup::Hit(_)))
                .count();
            if hits <= 1 {
                return;
            }
            futures_lite::future::yield_now().await;
        }
        panic!("memory backend retained more entries than its capacity");
    });
}
