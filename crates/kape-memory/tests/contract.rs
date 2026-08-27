use std::sync::Arc;
use std::time::Duration;

use kape::CacheBackend;
use kape_memory::MemoryBackend;
use kape_testkit::assert_adapter_contract;
use tokio::time::sleep;

#[tokio::test]
async fn satisfies_backend_contract() {
    let backend = MemoryBackend::<String, String>::new(100);
    assert_adapter_contract(&backend, 50).await;
}

#[tokio::test]
async fn clear_is_scoped_to_the_memory_backend_instance() {
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

    assert!(first.get(&key).await.unwrap().is_none());
    assert!(matches!(
        second.get(&key).await.unwrap(),
        Some(entry) if entry.value.as_str() == "second"
    ));
}

#[tokio::test]
async fn capacity_is_an_entry_count_upper_bound() {
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
            .flatten()
            .count();
        if hits <= 1 {
            return;
        }
        tokio::task::yield_now().await;
    }
    panic!("memory backend retained more entries than its capacity");
}

#[tokio::test]
async fn replacing_a_value_replaces_its_expiry() {
    let backend = MemoryBackend::<String, String>::new(10);
    let key = "key".to_owned();

    backend
        .set(&key, Arc::new("immortal".to_owned()), -1)
        .await
        .unwrap();
    backend
        .set(&key, Arc::new("finite".to_owned()), 10)
        .await
        .unwrap();
    sleep(Duration::from_millis(35)).await;
    assert!(backend.get(&key).await.unwrap().is_none());

    backend
        .set(&key, Arc::new("finite".to_owned()), 10)
        .await
        .unwrap();
    backend
        .set(&key, Arc::new("immortal".to_owned()), -1)
        .await
        .unwrap();
    sleep(Duration::from_millis(35)).await;
    assert!(matches!(
        backend.get(&key).await.unwrap(),
        Some(entry) if entry.value.as_str() == "immortal" && entry.remaining_ttl == -1
    ));
}
