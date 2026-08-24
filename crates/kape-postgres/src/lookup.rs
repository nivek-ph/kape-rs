use std::sync::Arc;

use kape::CacheEntry;

use crate::{PostgresBackendError, PostgresCodec};

pub(crate) fn decode_lookup<K, V, C>(
    codec: &C,
    value: Option<C::EncodedValue>,
    remaining_ms: Option<i64>,
) -> Result<Option<CacheEntry<V>>, PostgresBackendError>
where
    C: PostgresCodec<K, V>,
{
    let Some(value) = value else {
        return Ok(None);
    };
    let remaining_ttl = match remaining_ms {
        None => -1,
        Some(remaining) if remaining > 0 => remaining,
        Some(_) => return Ok(None),
    };
    let value = Arc::new(codec.decode_value(value)?);
    Ok(Some(CacheEntry::new(value, remaining_ttl)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::StringCodec;

    #[test]
    fn projects_only_fresh_or_immortal_rows_as_hits() {
        let codec = StringCodec;
        let text = "value".to_owned();
        assert!(matches!(
            decode_lookup::<String, String, _>(&codec, Some(text.clone()), None).unwrap(),
            Some(entry) if entry.remaining_ttl == -1
        ));
        assert!(matches!(
            decode_lookup::<String, String, _>(&codec, Some(text.clone()), Some(1)).unwrap(),
            Some(entry) if entry.remaining_ttl == 1
        ));
        assert!(
            decode_lookup::<String, String, _>(&codec, Some(text.clone()), Some(0))
                .unwrap()
                .is_none()
        );
        assert!(
            decode_lookup::<String, String, _>(&codec, Some(text), Some(-1))
                .unwrap()
                .is_none()
        );
    }
}
