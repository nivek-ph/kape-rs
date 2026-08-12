use std::error::Error as StdError;
use std::fmt;

/// Encodes typed keys and values at the `Redis` adapter boundary.
pub trait RedisCodec<K, V>: Send + Sync + 'static {
    /// Codec-specific error type.
    type Error: StdError + Send + Sync + 'static;

    /// Encodes a typed key as `Redis` key bytes.
    ///
    /// # Errors
    ///
    /// Returns the codec error when the key cannot be represented.
    fn encode_key(&self, key: &K) -> Result<Vec<u8>, Self::Error>;

    /// Decodes typed key bytes during iteration.
    ///
    /// # Errors
    ///
    /// Returns the codec error when stored key bytes are invalid.
    fn decode_key(&self, bytes: &[u8]) -> Result<K, Self::Error>;

    /// Encodes a typed value.
    ///
    /// # Errors
    ///
    /// Returns the codec error when the value cannot be represented.
    fn encode_value(&self, value: &V) -> Result<Vec<u8>, Self::Error>;

    /// Decodes a typed value.
    ///
    /// # Errors
    ///
    /// Returns the codec error when the stored bytes are invalid.
    fn decode_value(&self, bytes: &[u8]) -> Result<V, Self::Error>;
}

/// UTF-8 codec for `String` keys and values.
#[derive(Clone, Copy, Debug, Default)]
pub struct StringCodec;

/// A UTF-8 value failed to decode.
#[derive(Debug)]
pub struct StringCodecError(std::string::FromUtf8Error);

impl fmt::Display for StringCodecError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("Redis value is not valid UTF-8")
    }
}

impl StdError for StringCodecError {
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        Some(&self.0)
    }
}

impl RedisCodec<String, String> for StringCodec {
    type Error = StringCodecError;

    fn encode_key(&self, key: &String) -> Result<Vec<u8>, Self::Error> {
        Ok(key.as_bytes().to_vec())
    }

    fn decode_key(&self, bytes: &[u8]) -> Result<String, Self::Error> {
        String::from_utf8(bytes.to_vec()).map_err(StringCodecError)
    }

    fn encode_value(&self, value: &String) -> Result<Vec<u8>, Self::Error> {
        Ok(value.as_bytes().to_vec())
    }

    fn decode_value(&self, bytes: &[u8]) -> Result<String, Self::Error> {
        String::from_utf8(bytes.to_vec()).map_err(StringCodecError)
    }
}
