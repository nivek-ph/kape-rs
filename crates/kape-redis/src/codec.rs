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

    fn encode_value(&self, value: &String) -> Result<Vec<u8>, RedisBackendError> {
        Ok(value.as_bytes().to_vec())
    }

    fn decode_value(&self, bytes: &[u8]) -> Result<String, RedisBackendError> {
        String::from_utf8(bytes.to_vec()).map_err(RedisBackendError::codec)
    }
}

/// Identity codec for byte-vector keys and values.
#[derive(Clone, Copy, Debug, Default)]
pub struct BytesCodec;

impl RedisCodec<Vec<u8>, Vec<u8>> for BytesCodec {
    fn encode_key(&self, key: &Vec<u8>) -> Result<Vec<u8>, RedisBackendError> {
        Ok(key.clone())
    }

    fn encode_value(&self, value: &Vec<u8>) -> Result<Vec<u8>, RedisBackendError> {
        Ok(value.clone())
    }

    fn decode_value(&self, bytes: &[u8]) -> Result<Vec<u8>, RedisBackendError> {
        Ok(bytes.to_vec())
    }
}

#[cfg(test)]
mod tests {
    use super::{BytesCodec, RedisCodec};

    #[test]
    fn bytes_codec_round_trips_bytes() {
        let codec = BytesCodec;
        let bytes = vec![0, 1, 2, 255];
        assert_eq!(codec.encode_key(&bytes).unwrap(), bytes);
        assert_eq!(codec.encode_value(&bytes).unwrap(), bytes);
        assert_eq!(codec.decode_value(&bytes).unwrap(), bytes);
    }
}
