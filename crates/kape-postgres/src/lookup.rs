use std::{sync::Arc, time::Duration};

use kape::{CacheEntry, IterationEntry, IterationFreshness, Lookup, RemainingTTL};

use crate::{PostgresBackendError, PostgresCodec};

pub(crate) fn lookup<K, V, C>(
    codec: &C,
    bytes: Option<&[u8]>,
    remaining_ms: Option<i64>,
) -> Result<Lookup<V>, PostgresBackendError>
where
    C: PostgresCodec<K, V>,
{
    let Some(bytes) = bytes else {
        return Ok(Lookup::Miss);
    };
    let value = Arc::new(codec.decode_value(bytes)?);
    let (remaining_ttl, fresh) = remaining_ttl(remaining_ms)?;
    let entry = CacheEntry::new(value, remaining_ttl);
    if fresh {
        Ok(Lookup::Hit(entry))
    } else {
        Ok(Lookup::Stale(entry))
    }
}

pub(crate) fn iteration_entry<K, V, C>(
    codec: &C,
    encoded_key: &[u8],
    bytes: &[u8],
    remaining_ms: Option<i64>,
) -> Result<IterationEntry<K, V>, PostgresBackendError>
where
    C: PostgresCodec<K, V>,
{
    let key = codec.decode_key(encoded_key)?;
    let lookup = lookup(codec, Some(bytes), remaining_ms)?;
    let (entry, freshness) = match lookup {
        Lookup::Hit(entry) => (entry, IterationFreshness::Fresh),
        Lookup::Stale(entry) => (entry, IterationFreshness::Stale),
        Lookup::Miss => unreachable!("iteration rows always contain a value"),
    };
    Ok(IterationEntry {
        key,
        value: entry.value,
        remaining_ttl: entry.remaining_ttl,
        freshness,
    })
}

fn remaining_ttl(remaining_ms: Option<i64>) -> Result<(RemainingTTL, bool), PostgresBackendError> {
    match remaining_ms {
        None => Ok((RemainingTTL::Never, true)),
        Some(remaining) if remaining > 0 => {
            let millis = u64::try_from(remaining)
                .map_err(|_| PostgresBackendError::InvalidRemainingTTL(remaining))?;
            Ok((RemainingTTL::Known(Duration::from_millis(millis)), true))
        }
        Some(_) => Ok((RemainingTTL::Known(Duration::ZERO), false)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::StringCodec;

    #[test]
    fn projects_fresh_stale_and_immortal_rows() {
        let codec = StringCodec;
        let bytes = b"value".as_slice();

        assert!(matches!(
            lookup::<String, String, _>(&codec, Some(bytes), None).unwrap(),
            Lookup::Hit(entry) if entry.remaining_ttl == RemainingTTL::Never
        ));
        assert!(matches!(
            lookup::<String, String, _>(&codec, Some(bytes), Some(1)).unwrap(),
            Lookup::Hit(entry) if entry.remaining_ttl == RemainingTTL::Known(Duration::from_millis(1))
        ));
        assert!(matches!(
            lookup::<String, String, _>(&codec, Some(bytes), Some(0)).unwrap(),
            Lookup::Stale(entry) if entry.remaining_ttl == RemainingTTL::Known(Duration::ZERO)
        ));
    }
}
