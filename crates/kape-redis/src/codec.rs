use crate::RedisBackendError;

/// Encodes typed keys and values at the `Redis` adapter boundary.
pub trait RedisCodec<K, V>: Send + Sync + 'static {
    /// Encodes a typed key as `Redis` key bytes.
    ///
    /// # Errors
    ///
    /// Returns a backend error whose source is the codec error when the key
    /// cannot be represented.
    fn encode_key(&self, key: &K) -> Result<Vec<u8>, RedisBackendError>;

    /// Decodes typed key bytes during iteration.
    ///
    /// # Errors
    ///
    /// Returns a backend error whose source is the codec error when stored key
    /// bytes are invalid.
    fn decode_key(&self, bytes: &[u8]) -> Result<K, RedisBackendError>;

    /// Encodes a typed value.
    ///
    /// # Errors
    ///
    /// Returns a backend error whose source is the codec error when the value
    /// cannot be represented.
    fn encode_value(&self, value: &V) -> Result<Vec<u8>, RedisBackendError>;

    /// Decodes a typed value.
    ///
    /// # Errors
    ///
    /// Returns a backend error whose source is the codec error when the stored
    /// bytes are invalid.
    fn decode_value(&self, bytes: &[u8]) -> Result<V, RedisBackendError>;
}

/// UTF-8 codec for `String` keys and values.
#[derive(Clone, Copy, Debug, Default)]
pub struct StringCodec;

impl RedisCodec<String, String> for StringCodec {
    fn encode_key(&self, key: &String) -> Result<Vec<u8>, RedisBackendError> {
        Ok(key.as_bytes().to_vec())
    }

    fn decode_key(&self, bytes: &[u8]) -> Result<String, RedisBackendError> {
        String::from_utf8(bytes.to_vec()).map_err(RedisBackendError::codec)
    }

    fn encode_value(&self, value: &String) -> Result<Vec<u8>, RedisBackendError> {
        Ok(value.as_bytes().to_vec())
    }

    fn decode_value(&self, bytes: &[u8]) -> Result<String, RedisBackendError> {
        String::from_utf8(bytes.to_vec()).map_err(RedisBackendError::codec)
    }
}
