use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use futures_lite::future::block_on;
use kape::{Cache, CacheBackend, CacheEntry, KapeError};

type Entries = Arc<Mutex<HashMap<String, (Arc<String>, i64)>>>;

#[derive(Clone, Default)]
struct TestBackend {
    entries: Entries,
}

#[async_trait::async_trait]
impl CacheBackend<String, String> for TestBackend {
    async fn get(&self, key: &String) -> Result<Option<CacheEntry<String>>, KapeError> {
        Ok(self
            .entries
            .lock()
            .expect("test backend mutex poisoned")
            .get(key)
            .map(|(value, remaining_ttl)| CacheEntry::new(Arc::clone(value), *remaining_ttl)))
    }

    async fn set(&self, key: &String, value: Arc<String>, ttl: i64) -> Result<(), KapeError> {
        let mut entries = self.entries.lock().expect("test backend mutex poisoned");
        if ttl == 0 {
            entries.remove(key);
        } else {
            entries.insert(key.clone(), (value, ttl));
        }
        Ok(())
    }

    async fn remove(&self, key: &String) -> Result<(), KapeError> {
        self.entries
            .lock()
            .expect("test backend mutex poisoned")
            .remove(key);
        Ok(())
    }

    async fn clear(&self) -> Result<(), KapeError> {
        self.entries
            .lock()
            .expect("test backend mutex poisoned")
            .clear();
        Ok(())
    }
}

#[test]
fn single_backend_scalar_contract() {
    block_on(async {
        let backend = TestBackend::default();
        let cache = Cache::builder()
            .backend("memory", backend.clone())
            .build()
            .expect("cache should build");
        let key = "key".to_owned();

        assert_eq!(cache.backend_names(), &[Arc::<str>::from("memory")]);
        assert!(
            cache
                .get(&key)
                .await
                .expect("miss should succeed")
                .is_none()
        );

        cache
            .set(&key, Arc::new("value".to_owned()), -1)
            .await
            .expect("immortal write should succeed");
        assert_eq!(
            cache
                .get(&key)
                .await
                .expect("hit should succeed")
                .expect("value should exist")
                .as_str(),
            "value"
        );

        cache
            .set(&key, Arc::new("discarded".to_owned()), 0)
            .await
            .expect("zero TTL should invalidate");
        assert!(
            cache
                .get(&key)
                .await
                .expect("miss should succeed")
                .is_none()
        );

        let error = cache
            .set(&key, Arc::new("invalid".to_owned()), -2)
            .await
            .expect_err("invalid TTL must fail");
        assert!(matches!(error, KapeError::InvalidTtl(-2)));
        assert!(matches!(
            backend
                .get(&key)
                .await
                .expect("backend read should succeed"),
            None
        ));

        cache.remove(&key).await.expect("remove should succeed");
        cache.clear().await.expect("clear should succeed");
    });
}

#[test]
fn builder_validates_backend_names() {
    let no_backends = Cache::<String, String>::builder()
        .build()
        .err()
        .expect("empty chain must fail");
    assert!(matches!(no_backends, KapeError::NoBackends));

    let blank = Cache::builder()
        .backend("  ", TestBackend::default())
        .build()
        .err()
        .expect("blank name must fail");
    assert!(matches!(blank, KapeError::EmptyBackendName));

    let duplicate = Cache::builder()
        .backend("same", TestBackend::default())
        .backend("same", TestBackend::default())
        .build()
        .err()
        .expect("duplicate name must fail");
    assert!(matches!(
        duplicate,
        KapeError::DuplicateBackendName(name) if name == "same"
    ));
}
