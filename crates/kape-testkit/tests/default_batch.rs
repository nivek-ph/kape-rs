use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use futures_lite::future::block_on;
use kape::{CacheBackend, CacheEntry, KapeError, KapeResult, SetItem};

#[derive(Default)]
struct ScalarBackend {
    entries: Mutex<HashMap<String, Arc<String>>>,
}

#[async_trait::async_trait]
impl CacheBackend<String, String> for ScalarBackend {
    async fn get(&self, key: &String) -> KapeResult<Option<CacheEntry<String>>> {
        Ok(self
            .entries
            .lock()
            .expect("entries mutex poisoned")
            .get(key)
            .cloned()
            .map(|value| CacheEntry::new(value, -1)))
    }

    async fn set(&self, key: &String, value: Arc<String>, ttl: i64) -> KapeResult<()> {
        if key == "fail" {
            return Err(KapeError::backend(std::io::Error::other("set failed")));
        }
        let mut entries = self.entries.lock().expect("entries mutex poisoned");
        if ttl == 0 {
            entries.remove(key);
        } else {
            entries.insert(key.clone(), value);
        }
        Ok(())
    }

    async fn remove(&self, key: &String) -> KapeResult<()> {
        self.entries
            .lock()
            .expect("entries mutex poisoned")
            .remove(key);
        Ok(())
    }

    async fn clear(&self) -> KapeResult<()> {
        self.entries.lock().expect("entries mutex poisoned").clear();
        Ok(())
    }
}

#[test]
fn default_batch_is_sequential_and_retains_earlier_effects() {
    block_on(async {
        let backend = ScalarBackend::default();
        let error = backend
            .set_many(&[
                SetItem::new(&"first".to_owned(), "stored".to_owned(), -1),
                SetItem::new(&"fail".to_owned(), "rejected".to_owned(), -1),
                SetItem::new(&"later".to_owned(), "unreached".to_owned(), -1),
            ])
            .await
            .expect_err("second scalar write should fail");
        assert!(matches!(error, KapeError::BackendSource { .. }));

        assert!(backend.get(&"first".to_owned()).await.unwrap().is_some());
        assert!(backend.get(&"later".to_owned()).await.unwrap().is_none());
    });
}
