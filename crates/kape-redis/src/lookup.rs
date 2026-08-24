use std::sync::Arc;

use kape::CacheEntry;

use crate::{RedisBackendError, RedisCodec};

pub(crate) fn decode_lookup<K, V, C>(
    codec: &C,
    bytes: Option<&[u8]>,
    pttl: i64,
) -> Result<Option<CacheEntry<V>>, RedisBackendError>
where
    C: RedisCodec<K, V>,
{
    let Some(bytes) = bytes else {
        return Ok(None);
    };
    let remaining_ttl = match pttl {
        -2 | 0 => return Ok(None),
        -1 => -1,
        value if value > 0 => value,
        value => return Err(RedisBackendError::InvalidPttl(value)),
    };
    let value = Arc::new(codec.decode_value(bytes)?);
    Ok(Some(CacheEntry::new(value, remaining_ttl)))
}

pub(crate) fn decode_pair<K, V, C>(
    codec: &C,
    value: &redis::Value,
    pttl: &redis::Value,
) -> Result<Option<CacheEntry<V>>, RedisBackendError>
where
    C: RedisCodec<K, V>,
{
    let bytes = match value {
        redis::Value::Nil => return Ok(None),
        redis::Value::BulkString(bytes) => Some(bytes.as_slice()),
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
    decode_lookup(codec, bytes, *pttl)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::StringCodec;

    #[test]
    fn projects_only_valid_redis_hits() {
        let codec = StringCodec;
        let bytes = b"value".as_slice();

        assert!(matches!(
            decode_lookup::<String, String, _>(&codec, Some(bytes), -2).unwrap(),
            None
        ));
        assert!(matches!(
            decode_lookup::<String, String, _>(&codec, Some(bytes), -1).unwrap(),
            Some(entry) if entry.remaining_ttl == -1
        ));
        assert!(matches!(
            decode_lookup::<String, String, _>(&codec, Some(bytes), 1).unwrap(),
            Some(entry) if entry.remaining_ttl == 1
        ));
        assert!(matches!(
            decode_lookup::<String, String, _>(&codec, Some(bytes), 0).unwrap(),
            None
        ));
        assert!(matches!(
            decode_lookup::<String, String, _>(&codec, Some(bytes), -3),
            Err(RedisBackendError::InvalidPttl(-3))
        ));
    }

    #[test]
    fn nil_value_is_a_miss_without_interpreting_pttl() {
        let codec = StringCodec;
        assert!(matches!(
            decode_pair::<String, String, _>(
                &codec,
                &redis::Value::Nil,
                &redis::Value::BulkString(b"invalid".to_vec()),
            )
            .unwrap(),
            None
        ));
    }
}
