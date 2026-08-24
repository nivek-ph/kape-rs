use std::{
    collections::HashMap,
    error::Error,
    sync::{Arc, Mutex, MutexGuard},
    time::Instant,
};

use async_trait::async_trait;
use kape::{Cache, CacheBackend, CacheEntry, KapeError, Lookup};
use kape_testkit::get_random_string;

#[derive(Clone, Default)]
struct CustomBackend {
    entries: Arc<Mutex<HashMap<String, Entry>>>,
}

#[derive(Clone)]
struct Entry {
    value: Arc<String>,
    expires_at: Option<Instant>,
}

impl CustomBackend {
    fn entries(&self) -> Result<MutexGuard<'_, HashMap<String, Entry>>, KapeError> {
        self.entries
            .lock()
            .map_err(|_| KapeError::backend(std::io::Error::other("custom cache lock poisoned")))
    }
}

#[async_trait]
impl CacheBackend<String, String> for CustomBackend {
    async fn get(&self, key: &String) -> Result<Lookup<String>, KapeError> {
        let mut entries = self.entries()?;
        let Some(entry) = entries.get(key).cloned() else {
            return Ok(Lookup::Miss);
        };
        let now = Instant::now();
        let remaining_ttl = match entry.expires_at {
            None => -1,
            Some(expires_at) if expires_at > now => {
                let millis = expires_at.duration_since(now).as_millis();
                let Ok(millis) = i64::try_from(millis) else {
                    return Err(KapeError::backend(std::io::Error::other("TTL overflow")));
                };
                if millis == 0 {
                    entries.remove(key);
                    return Ok(Lookup::Miss);
                }
                millis
            }
            Some(_) => {
                entries.remove(key);
                return Ok(Lookup::Miss);
            }
        };
        Ok(Lookup::Hit(CacheEntry::new(entry.value, remaining_ttl)))
    }

    async fn set(&self, key: &String, value: Arc<String>, ttl: i64) -> Result<(), KapeError> {
        if ttl < -1 {
            return Err(KapeError::InvalidTtl(ttl));
        }
        if ttl == 0 {
            return self.remove(key).await;
        }
        let expires_at = if ttl == -1 {
            None
        } else {
            let ttl = ttl.cast_unsigned();
            Some(
                Instant::now()
                    .checked_add(std::time::Duration::from_millis(ttl))
                    .ok_or_else(|| KapeError::backend(std::io::Error::other("TTL overflow")))?,
            )
        };
        self.entries()?
            .insert(key.clone(), Entry { value, expires_at });
        Ok(())
    }

    async fn remove(&self, key: &String) -> Result<(), KapeError> {
        self.entries()?.remove(key);
        Ok(())
    }

    async fn clear(&self) -> Result<(), KapeError> {
        self.entries()?.clear();
        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let cache = Cache::builder()
        .backend("custom", CustomBackend::default())
        .build()?;
    let key = get_random_string();
    let value = get_random_string();
    cache.set(&key, Arc::new(value), 60_000).await?;
    println!("value: {:?}", cache.get(&key).await?);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::CustomBackend;
    use kape_testkit::{
        assert_backend_contract, assert_batch_contract, assert_clear_contract,
        assert_expiring_contract,
    };

    #[tokio::test]
    async fn satisfies_backend_contract() {
        let backend = CustomBackend::default();
        assert_backend_contract(&backend, &"contract".to_owned(), String::new()).await;
        assert_expiring_contract(&backend, &"ttl".to_owned(), "value".to_owned(), 100).await;
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
    }
}
