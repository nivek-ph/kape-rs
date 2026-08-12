use std::{sync::Arc, time::Duration};

use kape::{CacheEntry, IterationEntry, IterationFreshness, Lookup, RemainingTTL};

use crate::{RedisBackendError, RedisCodec};

pub(crate) fn decode_lookup<K, V, C>(
    codec: &C,
    bytes: Option<&[u8]>,
    pttl: i64,
) -> Result<Lookup<V>, RedisBackendError<C::Error>>
where
    C: RedisCodec<K, V>,
{
    let Some(bytes) = bytes else {
        return Ok(Lookup::Miss);
    };
    let Some(remaining_ttl) = remaining_ttl(pttl)? else {
        return Ok(Lookup::Miss);
    };
    let value = Arc::new(
        codec
            .decode_value(bytes)
            .map_err(RedisBackendError::Codec)?,
    );
    Ok(Lookup::Hit(CacheEntry::new(value, remaining_ttl)))
}

pub(crate) fn decode_pair<K, V, C>(
    codec: &C,
    value: &redis::Value,
    pttl: &redis::Value,
) -> Result<Lookup<V>, RedisBackendError<C::Error>>
where
    C: RedisCodec<K, V>,
{
    let bytes = match value {
        redis::Value::Nil => return Ok(Lookup::Miss),
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

pub(crate) fn iteration_entry<K, V, C>(
    codec: &C,
    encoded_key: &[u8],
    lookup: Lookup<V>,
) -> Result<Option<IterationEntry<K, V>>, RedisBackendError<C::Error>>
where
    C: RedisCodec<K, V>,
{
    let (entry, freshness) = match lookup {
        Lookup::Miss => return Ok(None),
        Lookup::Hit(entry) => (entry, IterationFreshness::Fresh),
        Lookup::Stale(entry) => (entry, IterationFreshness::Stale),
    };
    let key = codec
        .decode_key(encoded_key)
        .map_err(RedisBackendError::Codec)?;
    Ok(Some(IterationEntry {
        key,
        value: entry.value,
        remaining_ttl: entry.remaining_ttl,
        freshness,
    }))
}

fn remaining_ttl<E>(pttl: i64) -> Result<Option<RemainingTTL>, RedisBackendError<E>> {
    match pttl {
        -2 => Ok(None),
        -1 => Ok(Some(RemainingTTL::Never)),
        value if value >= 0 => {
            let millis = u64::try_from(value).map_err(|_| RedisBackendError::InvalidPttl(value))?;
            Ok(Some(RemainingTTL::Known(Duration::from_millis(millis))))
        }
        value => Err(RedisBackendError::InvalidPttl(value)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::StringCodec;

    #[test]
    fn projects_redis_ttl_sentinels() {
        let codec = StringCodec;
        let bytes = b"value".as_slice();

        assert!(matches!(
            decode_lookup::<String, String, _>(&codec, Some(bytes), -2).unwrap(),
            Lookup::Miss
        ));
        assert!(matches!(
            decode_lookup::<String, String, _>(&codec, Some(bytes), -1).unwrap(),
            Lookup::Hit(entry) if entry.remaining_ttl == RemainingTTL::Never
        ));
        assert!(matches!(
            decode_lookup::<String, String, _>(&codec, Some(bytes), 0).unwrap(),
            Lookup::Hit(entry) if entry.remaining_ttl == RemainingTTL::Known(Duration::ZERO)
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
            Lookup::Miss
        ));
    }
}
