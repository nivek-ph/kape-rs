use std::{marker::PhantomData, sync::Arc, time::Duration};

use crate::{RedisBackendError, RedisCodec};
use async_trait::async_trait;
use kape::{
    BackendCapability, BackendSetItem, CacheBackend, CacheEntry, IterationEntry,
    IterationFreshness, IterationPage, Lookup, RemainingTTL, ResolvedTTL,
};
use redis::aio::ConnectionManager;

/// A `Kape` backend using `Redis`.
pub struct RedisBackend<K, V, C> {
    connection: ConnectionManager,
    codec: C,
    namespace: Vec<u8>,
    marker: PhantomData<fn(K, V)>,
}

impl<K, V, C> Clone for RedisBackend<K, V, C>
where
    C: Clone,
{
    fn clone(&self) -> Self {
        Self {
            connection: self.connection.clone(),
            codec: self.codec.clone(),
            namespace: self.namespace.clone(),
            marker: PhantomData,
        }
    }
}

impl<K, V, C> RedisBackend<K, V, C>
where
    C: RedisCodec<K, V>,
{
    /// Connects to Redis and creates an adapter.
    ///
    /// # Errors
    ///
    /// Returns a `Redis` error when the URL is invalid or the initial
    /// connection cannot be established.
    pub async fn connect(url: &str, codec: C) -> Result<Self, RedisBackendError<C::Error>> {
        let client = redis::Client::open(url)?;
        let connection = ConnectionManager::new(client).await?;
        Ok(Self::from_connection(connection, codec))
    }

    /// Creates an adapter from an existing connection manager.
    #[must_use]
    pub const fn from_connection(connection: ConnectionManager, codec: C) -> Self {
        Self {
            connection,
            codec,
            namespace: Vec::new(),
            marker: PhantomData,
        }
    }

    /// Frames every encoded key with the supplied namespace.
    #[must_use]
    pub fn namespace(mut self, namespace: impl Into<Vec<u8>>) -> Self {
        self.namespace = namespace.into();
        self
    }

    /// Returns the shared `Redis` connection manager.
    #[must_use]
    pub const fn connection(&self) -> &ConnectionManager {
        &self.connection
    }

    fn encode_key(&self, key: &K) -> Result<Vec<u8>, RedisBackendError<C::Error>> {
        let encoded = self
            .codec
            .encode_key(key)
            .map_err(RedisBackendError::Codec)?;
        frame_key(&self.namespace, &encoded).ok_or(RedisBackendError::NamespaceTooLong)
    }
}

#[async_trait]
impl<K, V, C> CacheBackend<K, V> for RedisBackend<K, V, C>
where
    K: Send + Sync,
    V: Send + Sync,
    C: RedisCodec<K, V>,
{
    type Error = RedisBackendError<C::Error>;

    async fn get(&self, key: &K) -> Result<Lookup<V>, Self::Error> {
        let key = self.encode_key(key)?;
        let mut connection = self.connection.clone();
        let mut pipeline = redis::pipe();
        pipeline.atomic().cmd("GET").arg(&key).cmd("PTTL").arg(&key);
        let (bytes, pttl): (Option<Vec<u8>>, i64) = pipeline.query_async(&mut connection).await?;

        let Some(bytes) = bytes else {
            return Ok(Lookup::Miss);
        };
        if pttl == -2 {
            return Ok(Lookup::Miss);
        }
        let value = Arc::new(
            self.codec
                .decode_value(&bytes)
                .map_err(RedisBackendError::Codec)?,
        );
        let remaining_ttl = match pttl {
            -1 => RemainingTTL::Never,
            value if value >= 0 => {
                let millis =
                    u64::try_from(value).map_err(|_| RedisBackendError::InvalidPttl(value))?;
                RemainingTTL::Known(Duration::from_millis(millis))
            }
            value => return Err(RedisBackendError::InvalidPttl(value)),
        };
        Ok(Lookup::Hit(CacheEntry::new(value, remaining_ttl)))
    }

    async fn set(&self, key: &K, value: Arc<V>, ttl: ResolvedTTL) -> Result<(), Self::Error> {
        let key = self.encode_key(key)?;
        let bytes = self
            .codec
            .encode_value(value.as_ref())
            .map_err(RedisBackendError::Codec)?;
        let mut connection = self.connection.clone();
        let mut command = redis::cmd("SET");
        command.arg(key).arg(bytes);
        if let ResolvedTTL::After(duration) = ttl {
            command.arg("PX").arg(duration_millis(duration)?);
        }
        command.query_async::<()>(&mut connection).await?;
        Ok(())
    }

    async fn remove(&self, key: &K) -> Result<(), Self::Error> {
        let key = self.encode_key(key)?;
        let mut connection = self.connection.clone();
        redis::cmd("DEL")
            .arg(key)
            .query_async::<u64>(&mut connection)
            .await?;
        Ok(())
    }

    async fn get_many(&self, keys: &[&K]) -> Result<Vec<Lookup<V>>, Self::Error> {
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
            .await?;
        if responses.len() != keys.len() * 2 {
            return Err(RedisBackendError::InvalidBatchResponse(
                "one GET and PTTL response per key",
            ));
        }

        responses
            .chunks_exact(2)
            .map(|pair| self.decode_lookup_pair(&pair[0], &pair[1]))
            .collect()
    }

    async fn set_many(&self, items: &[BackendSetItem<'_, K, V>]) -> Result<(), Self::Error> {
        if items.is_empty() {
            return Ok(());
        }
        let encoded = items
            .iter()
            .map(|item| {
                Ok((
                    self.encode_key(item.key)?,
                    self.codec
                        .encode_value(item.value.as_ref())
                        .map_err(RedisBackendError::Codec)?,
                    item.ttl,
                ))
            })
            .collect::<Result<Vec<_>, RedisBackendError<C::Error>>>()?;
        let mut pipeline = redis::pipe();
        pipeline.atomic();
        for (key, value, ttl) in encoded {
            pipeline.cmd("SET").arg(key).arg(value);
            if let ResolvedTTL::After(duration) = ttl {
                pipeline.arg("PX").arg(duration_millis(duration)?);
            }
        }
        let mut connection = self.connection.clone();
        pipeline.query_async::<()>(&mut connection).await?;
        Ok(())
    }

    async fn has_many(&self, keys: &[&K]) -> Result<Vec<bool>, Self::Error> {
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
            .await?;
        if responses.len() != keys.len() {
            return Err(RedisBackendError::InvalidBatchResponse(
                "one EXISTS response per key",
            ));
        }
        responses
            .into_iter()
            .map(|response| match response {
                redis::Value::Int(value) => Ok(value != 0),
                _ => Err(RedisBackendError::InvalidBatchResponse(
                    "integer EXISTS responses",
                )),
            })
            .collect()
    }

    async fn remove_many(&self, keys: &[&K]) -> Result<(), Self::Error> {
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
            .await?;
        Ok(())
    }

    async fn clear(&self) -> Result<BackendCapability<()>, Self::Error> {
        let pattern = namespace_pattern(&self.namespace)?;
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
                    .await?;
                if !keys.is_empty() {
                    deleted += redis::cmd("DEL")
                        .arg(keys)
                        .query_async::<u64>(&mut connection)
                        .await?;
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
        Ok(BackendCapability::Supported(()))
    }

    async fn iterate(
        &self,
        cursor: Option<&[u8]>,
        limit: usize,
    ) -> Result<BackendCapability<IterationPage<K, V>>, Self::Error> {
        let cursor = decode_cursor(cursor)?;
        let prefix = frame_key(&self.namespace, &[]).ok_or(RedisBackendError::NamespaceTooLong)?;
        let pattern = namespace_pattern(&self.namespace)?;
        let mut connection = self.connection.clone();
        let (next, framed_keys): (u64, Vec<Vec<u8>>) = redis::cmd("SCAN")
            .arg(cursor)
            .arg("MATCH")
            .arg(pattern)
            .arg("COUNT")
            .arg(limit)
            .query_async(&mut connection)
            .await?;

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
                .await?
        };
        if responses.len() != framed_keys.len() * 2 {
            return Err(RedisBackendError::InvalidBatchResponse(
                "one GET and PTTL response per scanned key",
            ));
        }

        let mut entries = Vec::with_capacity(framed_keys.len());
        for (key, pair) in framed_keys.iter().zip(responses.chunks_exact(2)) {
            let lookup = self.decode_lookup_pair(&pair[0], &pair[1])?;
            let (entry, freshness) = match lookup {
                Lookup::Miss => continue,
                Lookup::Hit(entry) => (entry, IterationFreshness::Fresh),
                Lookup::Stale(entry) => (entry, IterationFreshness::Stale),
            };
            let encoded_key = key
                .strip_prefix(prefix.as_slice())
                .ok_or(RedisBackendError::InvalidKeyFrame)?;
            let key = self
                .codec
                .decode_key(encoded_key)
                .map_err(RedisBackendError::Codec)?;
            entries.push(IterationEntry {
                key,
                value: entry.value,
                remaining_ttl: entry.remaining_ttl,
                freshness,
            });
        }
        Ok(BackendCapability::Supported(IterationPage {
            entries,
            next_cursor: (next != 0).then(|| next.to_be_bytes().to_vec()),
        }))
    }
}

impl<K, V, C> RedisBackend<K, V, C>
where
    C: RedisCodec<K, V>,
{
    fn decode_lookup_pair(
        &self,
        value: &redis::Value,
        pttl: &redis::Value,
    ) -> Result<Lookup<V>, RedisBackendError<C::Error>> {
        let bytes = match value {
            redis::Value::Nil => return Ok(Lookup::Miss),
            redis::Value::BulkString(bytes) => bytes,
            _ => {
                return Err(RedisBackendError::InvalidBatchResponse(
                    "bulk-string or nil GET responses",
                ));
            }
        };
        let redis::Value::Int(pttl) = pttl else {
            return Err(RedisBackendError::InvalidBatchResponse(
                "integer PTTL responses",
            ));
        };
        if *pttl == -2 {
            return Ok(Lookup::Miss);
        }
        let remaining_ttl = match *pttl {
            -1 => RemainingTTL::Never,
            value if value >= 0 => {
                let millis =
                    u64::try_from(value).map_err(|_| RedisBackendError::InvalidPttl(value))?;
                RemainingTTL::Known(Duration::from_millis(millis))
            }
            value => return Err(RedisBackendError::InvalidPttl(value)),
        };
        let value = Arc::new(
            self.codec
                .decode_value(bytes)
                .map_err(RedisBackendError::Codec)?,
        );
        Ok(Lookup::Hit(CacheEntry::new(value, remaining_ttl)))
    }
}

fn duration_millis<E>(duration: Duration) -> Result<u64, RedisBackendError<E>> {
    let millis = duration.as_millis();
    let millis = if millis == 0 { 1 } else { millis };
    u64::try_from(millis).map_err(|_| RedisBackendError::TTLOverflow)
}

fn decode_cursor<E>(cursor: Option<&[u8]>) -> Result<u64, RedisBackendError<E>> {
    let Some(cursor) = cursor else {
        return Ok(0);
    };
    let bytes: [u8; 8] = cursor
        .try_into()
        .map_err(|_| RedisBackendError::InvalidCursor)?;
    Ok(u64::from_be_bytes(bytes))
}

fn namespace_pattern<E>(namespace: &[u8]) -> Result<Vec<u8>, RedisBackendError<E>> {
    let prefix = frame_key(namespace, &[]).ok_or(RedisBackendError::NamespaceTooLong)?;
    let mut pattern = Vec::with_capacity(prefix.len() + 1);
    for byte in prefix {
        if matches!(byte, b'*' | b'?' | b'[' | b']' | b'\\') {
            pattern.push(b'\\');
        }
        pattern.push(byte);
    }
    pattern.push(b'*');
    Ok(pattern)
}

fn frame_key(namespace: &[u8], key: &[u8]) -> Option<Vec<u8>> {
    let namespace_len = u64::try_from(namespace.len()).ok()?;
    let mut framed = Vec::with_capacity(8 + namespace.len() + key.len());
    framed.extend_from_slice(&namespace_len.to_be_bytes());
    framed.extend_from_slice(namespace);
    framed.extend_from_slice(key);
    Some(framed)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::StringCodecError;

    #[test]
    fn rounds_sub_millisecond_ttl_up() {
        assert_eq!(
            duration_millis::<StringCodecError>(Duration::from_nanos(1)).unwrap(),
            1
        );
    }

    #[test]
    fn namespace_frame_has_no_delimiter_collision() {
        assert_ne!(
            frame_key(b"a", b"b:c").unwrap(),
            frame_key(b"a:b", b"c").unwrap()
        );
    }
}
