use crate::codec::StringCodec;
use crate::{RedisBackendError, RedisCodec};
use async_trait::async_trait;
use kape::{BackendSetItem, CacheBackend, IterationPage, KapeError, Lookup, ResolvedTTL};
use redis::aio::ConnectionManager;
use std::{marker::PhantomData, sync::Arc, time::Duration};

/// A `Kape` backend using `Redis`.
pub struct RedisBackend<K, V, C = StringCodec> {
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
    /// Connects to Redis and creates an adapter.
    ///
    /// # Errors
    ///
    /// Returns a `Redis` error when the URL is invalid or the initial
    /// connection cannot be established.
    pub async fn connect(url: &str) -> Result<Self, KapeError> {
        let client = redis::Client::open(url).map_err(RedisBackendError::Redis)?;
        let connection = ConnectionManager::new(client)
            .await
            .map_err(RedisBackendError::Redis)?;
        Ok(Self {
            namespace: String::new(),
            connection,
            codec: StringCodec::default(),
            marker: PhantomData,
        })
    }
}

impl<K, V, C> RedisBackend<K, V, C> {
    /// Replaces the current codec with an application-specific codec.
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
}

impl<K, V, C> RedisBackend<K, V, C>
where
    C: RedisCodec<K, V>,
{
    /// Creates an adapter from an existing connection manager.
    #[must_use]
    pub const fn from_connection(connection: ConnectionManager, codec: C) -> Self {
        Self {
            connection,
            codec,
            namespace: String::new(),
            marker: PhantomData,
        }
    }

    /// Frames every encoded key with the supplied namespace.
    ///
    /// The namespace is joined to each encoded key with `:`. Namespace and
    /// encoded key bytes must not contain `:`. The namespace must also avoid
    /// Redis glob metacharacters (`*`, `?`, `[`, `]`, or `\\`) because it is
    /// used directly in `SCAN MATCH` patterns.
    #[must_use]
    pub fn namespace(mut self, namespace: impl Into<String>) -> Self {
        self.namespace = namespace.into();
        self
    }

    /// Returns the shared `Redis` connection manager.
    #[must_use]
    pub const fn connection(&self) -> &ConnectionManager {
        &self.connection
    }

    fn encode_key(&self, key: &K) -> Result<Vec<u8>, RedisBackendError> {
        let encoded = self.codec.encode_key(key)?;
        Ok(build_key(&self.namespace.as_bytes(), &encoded))
    }
}

#[async_trait]
impl<K, V, C> CacheBackend<K, V> for RedisBackend<K, V, C>
where
    K: Send + Sync,
    V: Send + Sync,
    C: RedisCodec<K, V>,
{
    async fn get(&self, key: &K) -> Result<Lookup<V>, KapeError> {
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

    async fn set(&self, key: &K, value: Arc<V>, ttl: ResolvedTTL) -> Result<(), KapeError> {
        let key = self.encode_key(key)?;
        let bytes = self.codec.encode_value(value.as_ref())?;
        let mut connection = self.connection.clone();
        let mut command = redis::cmd("SET");
        command.arg(key).arg(bytes);
        if let ResolvedTTL::After(duration) = ttl {
            command.arg("PX").arg(duration_millis(duration)?);
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

    async fn get_many(&self, keys: &[&K]) -> Result<Vec<Lookup<V>>, KapeError> {
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

    async fn set_many(&self, items: &[BackendSetItem<'_, K, V>]) -> Result<(), KapeError> {
        if items.is_empty() {
            return Ok(());
        }
        let encoded = items
            .iter()
            .map(|item| {
                Ok((
                    self.encode_key(item.key)?,
                    self.codec.encode_value(item.value.as_ref())?,
                    item.ttl,
                ))
            })
            .collect::<Result<Vec<_>, RedisBackendError>>()?;
        let mut pipeline = redis::pipe();
        pipeline.atomic();
        for (key, value, ttl) in encoded {
            pipeline.cmd("SET").arg(key).arg(value);
            if let ResolvedTTL::After(duration) = ttl {
                pipeline.arg("PX").arg(duration_millis(duration)?);
            }
        }
        let mut connection = self.connection.clone();
        pipeline
            .query_async::<()>(&mut connection)
            .await
            .map_err(RedisBackendError::Redis)?;
        Ok(())
    }

    async fn has_many(&self, keys: &[&K]) -> Result<Vec<bool>, KapeError> {
        if keys.is_empty() {
            return Ok(Vec::new());
        }
        let keys = keys
            .iter()
            .map(|key| self.encode_key(key))
            .collect::<Result<Vec<_>, _>>()?;
        let mut pipeline = redis::pipe();
        for key in &keys {
            pipeline.cmd("EXISTS").arg(key);
        }
        let mut connection = self.connection.clone();
        let responses = pipeline
            .query_async::<Vec<redis::Value>>(&mut connection)
            .await
            .map_err(RedisBackendError::Redis)?;
        if responses.len() != keys.len() {
            return Err(
                RedisBackendError::InvalidBatchResponse("one EXISTS response per key").into(),
            );
        }
        responses
            .into_iter()
            .map(|response| match response {
                redis::Value::Int(value) => Ok(value != 0),
                _ => {
                    Err(RedisBackendError::InvalidBatchResponse("integer EXISTS responses").into())
                }
            })
            .collect()
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
        let prefix = build_key(&self.namespace.as_bytes(), &[]);
        let mut pattern = prefix;
        pattern.push(b'*');
        let mut connection = self.connection.clone();
        loop {
            let mut cursor = 0_u64;
            let mut deleted = 0_u64;
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
                    deleted += redis::cmd("DEL")
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
            if deleted == 0 {
                break;
            }
        }
        Ok(())
    }

    async fn iterate(
        &self,
        cursor: Option<&[u8]>,
        limit: usize,
    ) -> Result<IterationPage<K, V>, KapeError> {
        let cursor = decode_cursor(cursor)?;
        let prefix = build_key(&self.namespace.as_bytes(), &[]);
        let mut pattern = prefix.clone();
        pattern.push(b'*');
        let mut connection = self.connection.clone();
        let (next, framed_keys): (u64, Vec<Vec<u8>>) = redis::cmd("SCAN")
            .arg(cursor)
            .arg("MATCH")
            .arg(pattern)
            .arg("COUNT")
            .arg(limit)
            .query_async(&mut connection)
            .await
            .map_err(RedisBackendError::Redis)?;

        let mut pipeline = redis::pipe();
        pipeline.atomic();
        for key in &framed_keys {
            pipeline.cmd("GET").arg(key).cmd("PTTL").arg(key);
        }
        let responses = if framed_keys.is_empty() {
            Vec::new()
        } else {
            pipeline
                .query_async::<Vec<redis::Value>>(&mut connection)
                .await
                .map_err(RedisBackendError::Redis)?
        };
        if responses.len() != framed_keys.len() * 2 {
            return Err(RedisBackendError::InvalidBatchResponse(
                "one GET and PTTL response per scanned key",
            )
            .into());
        }

        let mut entries = Vec::with_capacity(framed_keys.len());
        for (key, pair) in framed_keys.iter().zip(responses.chunks_exact(2)) {
            let lookup = crate::lookup::decode_pair(&self.codec, &pair[0], &pair[1])?;
            let encoded_key = key
                .strip_prefix(prefix.as_slice())
                .ok_or(RedisBackendError::InvalidKeyFrame)?;
            if let Some(entry) = crate::lookup::iteration_entry(&self.codec, encoded_key, lookup)? {
                entries.push(entry);
            }
        }
        Ok(IterationPage {
            entries,
            next_cursor: (next != 0).then(|| next.to_be_bytes().to_vec()),
        })
    }
}

/// Converts a duration to milliseconds, rounding up to 1ms if the duration is
fn duration_millis(duration: Duration) -> Result<u64, RedisBackendError> {
    let millis = duration.as_millis();
    let millis = if millis == 0 { 1 } else { millis };
    u64::try_from(millis).map_err(|_| RedisBackendError::TTLOverflow)
}

fn decode_cursor(cursor: Option<&[u8]>) -> Result<u64, RedisBackendError> {
    let Some(cursor) = cursor else {
        return Ok(0);
    };
    let bytes: [u8; 8] = cursor
        .try_into()
        .map_err(|_| RedisBackendError::InvalidCursor)?;
    Ok(u64::from_be_bytes(bytes))
}

/// Builds a key by concatenating the namespace and key with a delimiter.
fn build_key(namespace: &[u8], key: &[u8]) -> Vec<u8> {
    let mut framed = Vec::with_capacity(namespace.len() + 1 + key.len());
    framed.extend_from_slice(namespace);
    framed.push(b':');
    framed.extend_from_slice(key);
    framed
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn rounds_sub_millisecond_ttl_up() {
        assert_eq!(duration_millis(Duration::from_nanos(1)).unwrap(), 1);
    }

    #[test]
    fn namespace_frame_uses_a_delimiter() {
        assert_eq!(build_key(b"a", b"b"), b"a:b".to_vec());
    }
}
