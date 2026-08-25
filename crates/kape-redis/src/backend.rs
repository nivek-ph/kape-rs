use std::{hash::Hash, marker::PhantomData, sync::Arc};

use async_trait::async_trait;
use kape::{CacheBackend, CacheEntry, KapeError, SetItem, validate_set_items};
use redis::aio::ConnectionManager;

use crate::{RedisBackendError, RedisCodec, StringCodec};

/// A Redis-backed cache adapter.
pub struct RedisBackend<K = String, V = String, C = StringCodec> {
    namespace: String,
    connection: ConnectionManager,
    codec: C,
    marker: PhantomData<fn(K, V)>,
}

impl<K, V, C> Clone for RedisBackend<K, V, C>
where
    C: Clone,
{
    fn clone(&self) -> Self {
        Self {
            namespace: self.namespace.clone(),
            connection: self.connection.clone(),
            codec: self.codec.clone(),
            marker: PhantomData,
        }
    }
}

impl<K, V> RedisBackend<K, V> {
    /// Connects to Redis with the default string codec.
    ///
    /// # Errors
    ///
    /// Returns a backend error when the URL or initial connection fails.
    pub async fn connect(url: &str) -> Result<Self, KapeError> {
        let client = redis::Client::open(url).map_err(RedisBackendError::Redis)?;
        let connection = ConnectionManager::new(client)
            .await
            .map_err(RedisBackendError::Redis)?;
        Ok(Self::from_connection(connection, StringCodec))
    }
}

impl<K, V, C> RedisBackend<K, V, C> {
    /// Creates an adapter from an application-owned connection manager.
    #[must_use]
    pub const fn from_connection(connection: ConnectionManager, codec: C) -> Self {
        Self {
            namespace: String::new(),
            connection,
            codec,
            marker: PhantomData,
        }
    }

    /// Replaces the key/value codec.
    #[must_use]
    pub fn with_codec<D>(self, codec: D) -> RedisBackend<K, V, D>
    where
        D: RedisCodec<K, V>,
    {
        RedisBackend {
            namespace: self.namespace,
            connection: self.connection,
            codec,
            marker: PhantomData,
        }
    }

    /// Sets the caller-owned namespace.
    ///
    /// The namespace is used directly in the Redis key prefix. Callers are
    /// responsible for choosing a unique, storage-safe value.
    #[must_use]
    pub fn namespace(mut self, namespace: impl Into<String>) -> Self {
        self.namespace = namespace.into();
        self
    }
}

impl<K, V, C> RedisBackend<K, V, C>
where
    C: RedisCodec<K, V>,
{
    fn prefix(&self) -> Vec<u8> {
        namespace_prefix(&self.namespace)
    }

    fn encode_key(&self, key: &K) -> Result<Vec<u8>, RedisBackendError> {
        let encoded = self.codec.encode_key(key)?;
        let prefix = self.prefix();
        let mut framed = Vec::with_capacity(prefix.len() + encoded.len());
        framed.extend_from_slice(&prefix);
        framed.extend_from_slice(&encoded);
        Ok(framed)
    }
}

#[async_trait]
impl<K, V, C> CacheBackend<K, V> for RedisBackend<K, V, C>
where
    K: Send + Sync,
    V: Send + Sync,
    C: RedisCodec<K, V>,
{
    async fn get(&self, key: &K) -> Result<Option<CacheEntry<V>>, KapeError> {
        let key = self.encode_key(key)?;
        let mut connection = self.connection.clone();
        let mut pipeline = redis::pipe();
        pipeline.atomic().cmd("GET").arg(&key).cmd("PTTL").arg(&key);
        let (bytes, pttl): (Option<Vec<u8>>, i64) = pipeline
            .query_async(&mut connection)
            .await
            .map_err(RedisBackendError::Redis)?;
        crate::lookup::decode_lookup(&self.codec, bytes.as_deref(), pttl).map_err(Into::into)
    }

    async fn set(&self, key: &K, value: Arc<V>, ttl: i64) -> Result<(), KapeError> {
        if ttl < -1 {
            return Err(KapeError::InvalidTtl(ttl));
        }
        if ttl == 0 {
            return self.remove(key).await;
        }

        let key = self.encode_key(key)?;
        let bytes = self.codec.encode_value(value.as_ref())?;
        let mut connection = self.connection.clone();
        let mut command = redis::cmd("SET");
        command.arg(key).arg(bytes);
        if ttl > 0 {
            command.arg("PX").arg(ttl);
        }
        command
            .query_async::<()>(&mut connection)
            .await
            .map_err(RedisBackendError::Redis)?;
        Ok(())
    }

    async fn remove(&self, key: &K) -> Result<(), KapeError> {
        let key = self.encode_key(key)?;
        let mut connection = self.connection.clone();
        redis::cmd("DEL")
            .arg(key)
            .query_async::<u64>(&mut connection)
            .await
            .map_err(RedisBackendError::Redis)?;
        Ok(())
    }

    async fn get_many(&self, keys: &[&K]) -> Result<Vec<Option<CacheEntry<V>>>, KapeError> {
        if keys.is_empty() {
            return Ok(Vec::new());
        }
        let keys = keys
            .iter()
            .map(|key| self.encode_key(key))
            .collect::<Result<Vec<_>, _>>()?;
        let mut pipeline = redis::pipe();
        pipeline.atomic();
        for key in &keys {
            pipeline.cmd("GET").arg(key).cmd("PTTL").arg(key);
        }
        let mut connection = self.connection.clone();
        let responses = pipeline
            .query_async::<Vec<redis::Value>>(&mut connection)
            .await
            .map_err(RedisBackendError::Redis)?;
        if responses.len() != keys.len() * 2 {
            return Err(RedisBackendError::InvalidBatchResponse(
                "one GET and PTTL response per key",
            )
            .into());
        }
        responses
            .chunks_exact(2)
            .map(|pair| {
                crate::lookup::decode_pair(&self.codec, &pair[0], &pair[1]).map_err(Into::into)
            })
            .collect()
    }

    async fn set_many(&self, items: &[SetItem<&K, V>]) -> Result<(), KapeError>
    where
        K: Eq + Hash,
    {
        validate_set_items(items)?;
        if items.is_empty() {
            return Ok(());
        }
        let encoded = items
            .iter()
            .map(|item| {
                let key = self.encode_key(item.key)?;
                let value = if item.ttl == 0 {
                    None
                } else {
                    Some(self.codec.encode_value(item.value.as_ref())?)
                };
                Ok((key, value, item.ttl))
            })
            .collect::<Result<Vec<_>, RedisBackendError>>()?;
        let mut pipeline = redis::pipe();
        pipeline.atomic();
        for (key, value, ttl) in encoded {
            if let Some(value) = value {
                pipeline.cmd("SET").arg(key).arg(value);
                if ttl > 0 {
                    pipeline.arg("PX").arg(ttl);
                }
            } else {
                pipeline.cmd("DEL").arg(key);
            }
        }
        let mut connection = self.connection.clone();
        pipeline
            .query_async::<()>(&mut connection)
            .await
            .map_err(RedisBackendError::Redis)?;
        Ok(())
    }

    async fn remove_many(&self, keys: &[&K]) -> Result<(), KapeError> {
        if keys.is_empty() {
            return Ok(());
        }
        let keys = keys
            .iter()
            .map(|key| self.encode_key(key))
            .collect::<Result<Vec<_>, _>>()?;
        let mut connection = self.connection.clone();
        redis::cmd("DEL")
            .arg(keys)
            .query_async::<u64>(&mut connection)
            .await
            .map_err(RedisBackendError::Redis)?;
        Ok(())
    }

    async fn clear(&self) -> Result<(), KapeError> {
        let mut pattern = self.prefix();
        pattern.push(b'*');
        let mut connection = self.connection.clone();
        let mut cursor = 0_u64;
        loop {
            let (next, keys): (u64, Vec<Vec<u8>>) = redis::cmd("SCAN")
                .arg(cursor)
                .arg("MATCH")
                .arg(&pattern)
                .arg("COUNT")
                .arg(256_u64)
                .query_async(&mut connection)
                .await
                .map_err(RedisBackendError::Redis)?;
            if !keys.is_empty() {
                redis::cmd("DEL")
                    .arg(keys)
                    .query_async::<u64>(&mut connection)
                    .await
                    .map_err(RedisBackendError::Redis)?;
            }
            cursor = next;
            if cursor == 0 {
                break;
            }
        }
        Ok(())
    }
}

fn namespace_prefix(namespace: &str) -> Vec<u8> {
    let mut prefix = Vec::with_capacity(6 + namespace.len());
    prefix.extend_from_slice(b"kape:");
    prefix.extend_from_slice(namespace.as_bytes());
    prefix.push(b':');
    prefix
}

#[cfg(test)]
mod tests {
    use super::namespace_prefix;

    #[test]
    fn namespace_prefix_uses_the_caller_string_directly() {
        assert_eq!(namespace_prefix("orders"), b"kape:orders:");
        assert_eq!(namespace_prefix(""), b"kape::");
        assert_eq!(namespace_prefix("[*]?"), b"kape:[*]?:");
    }
}
