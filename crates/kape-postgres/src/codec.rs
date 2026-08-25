use sqlx::{Decode, Encode, Postgres, Type, postgres::PgHasArrayType};

use crate::PostgresBackendError;

pub(crate) mod sealed {
    pub trait Sealed {}

    impl Sealed for String {}
    impl Sealed for Vec<u8> {}
}

/// A PostgreSQL column representation supported by [`PostgresCodec`].
///
/// `String` maps to `TEXT`; `Vec<u8>` maps to `BYTEA`. This trait is sealed so
/// the adapter can keep SQL binding and decoding behavior complete.
#[doc(hidden)]
pub trait PostgresValue:
    sealed::Sealed
    + Clone
    + Send
    + Sync
    + Type<Postgres>
    + PgHasArrayType
    + for<'q> Encode<'q, Postgres>
    + for<'r> Decode<'r, Postgres>
{
    /// Returns the SQL array type used to resolve bulk `UNNEST` parameters.
    #[doc(hidden)]
    fn array_type_name() -> &'static str;
}

impl PostgresValue for String {
    fn array_type_name() -> &'static str {
        "TEXT[]"
    }
}

impl PostgresValue for Vec<u8> {
    fn array_type_name() -> &'static str {
        "BYTEA[]"
    }
}

/// A PostgreSQL key representation that can carry the namespace prefix.
#[doc(hidden)]
pub trait PostgresKey: PostgresValue {
    #[doc(hidden)]
    fn namespace_prefix(namespace: &str) -> Self;

    #[doc(hidden)]
    fn join(prefix: Self, key: Self) -> Self;
}

impl PostgresKey for String {
    #[inline]
    fn namespace_prefix(namespace: &str) -> Self {
        format!("kape:{namespace}:")
    }

    #[inline]
    fn join(mut prefix: Self, key: Self) -> Self {
        prefix.push_str(&key);
        prefix
    }
}

impl PostgresKey for Vec<u8> {
    fn namespace_prefix(namespace: &str) -> Self {
        let mut prefix = Vec::with_capacity(6 + namespace.len());
        prefix.extend_from_slice(b"kape:");
        prefix.extend_from_slice(namespace.as_bytes());
        prefix.push(b':');
        prefix
    }

    fn join(mut prefix: Self, key: Self) -> Self {
        prefix.extend_from_slice(&key);
        prefix
    }
}

/// Encodes typed keys and values for application-owned `PostgreSQL` columns.
///
/// The associated representations determine the required column types. Use
/// `String` for `TEXT` and `Vec<u8>` for `BYTEA`.
pub trait PostgresCodec<K, V>: Send + Sync + 'static {
    /// `PostgreSQL` representation of the key column.
    type EncodedKey: PostgresKey;
    /// `PostgreSQL` representation of the value column.
    type EncodedValue: PostgresValue;

    /// Encodes a typed key.
    ///
    /// # Errors
    ///
    /// Returns a backend error whose source is the codec error when the key
    /// cannot be represented.
    fn encode_key(&self, key: &K) -> Result<Self::EncodedKey, PostgresBackendError>;

    /// Encodes a typed value.
    ///
    /// # Errors
    ///
    /// Returns a backend error whose source is the codec error when the value
    /// cannot be represented.
    fn encode_value(&self, value: &V) -> Result<Self::EncodedValue, PostgresBackendError>;

    /// Decodes a typed value.
    ///
    /// # Errors
    ///
    /// Returns a backend error whose source is the codec error when the stored
    /// value is invalid.
    fn decode_value(&self, value: Self::EncodedValue) -> Result<V, PostgresBackendError>;
}

/// Identity codec for `String` keys and values stored in `TEXT` columns.
#[derive(Clone, Copy, Debug, Default)]
pub struct StringCodec;

impl PostgresCodec<String, String> for StringCodec {
    type EncodedKey = String;
    type EncodedValue = String;

    #[inline]
    fn encode_key(&self, key: &String) -> Result<Self::EncodedKey, PostgresBackendError> {
        Ok(key.clone())
    }

    #[inline]
    fn encode_value(&self, value: &String) -> Result<Self::EncodedValue, PostgresBackendError> {
        Ok(value.clone())
    }

    #[inline]
    fn decode_value(&self, value: Self::EncodedValue) -> Result<String, PostgresBackendError> {
        Ok(value)
    }
}

/// Identity codec for byte-vector keys and values stored in `BYTEA` columns.
#[derive(Clone, Copy, Debug, Default)]
pub struct BytesCodec;

impl PostgresCodec<Vec<u8>, Vec<u8>> for BytesCodec {
    type EncodedKey = Vec<u8>;
    type EncodedValue = Vec<u8>;

    #[inline]
    fn encode_key(&self, key: &Vec<u8>) -> Result<Self::EncodedKey, PostgresBackendError> {
        Ok(key.clone())
    }

    #[inline]
    fn encode_value(&self, value: &Vec<u8>) -> Result<Self::EncodedValue, PostgresBackendError> {
        Ok(value.clone())
    }

    #[inline]
    fn decode_value(&self, value: Self::EncodedValue) -> Result<Vec<u8>, PostgresBackendError> {
        Ok(value)
    }
}

#[cfg(test)]
mod tests {
    use super::{BytesCodec, PostgresCodec, PostgresKey, StringCodec};

    #[test]
    fn string_codec_keeps_text_readable() {
        let codec = StringCodec;
        let value = "hello".to_owned();
        assert_eq!(codec.encode_key(&value).unwrap(), value);
        assert_eq!(codec.encode_value(&value).unwrap(), value);
        assert_eq!(codec.decode_value(value.clone()).unwrap(), value);
        assert_eq!(String::namespace_prefix("orders"), "kape:orders:");
    }

    #[test]
    fn bytes_codec_round_trips_bytes() {
        let codec = BytesCodec;
        let bytes = vec![0, 1, 2, 255];
        assert_eq!(codec.encode_key(&bytes).unwrap(), bytes);
        assert_eq!(codec.encode_value(&bytes).unwrap(), bytes);
        assert_eq!(codec.decode_value(bytes.clone()).unwrap(), bytes);
        assert_eq!(Vec::<u8>::namespace_prefix("raw"), b"kape:raw:");
    }
}
