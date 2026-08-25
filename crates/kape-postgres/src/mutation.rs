use kape::{KapeError, KapeResult, SetItem};

use crate::{PostgresBackendError, PostgresCodec, PostgresKey};

#[derive(Debug, Eq, PartialEq)]
pub(crate) struct PlannedUpsert<K, V> {
    pub(crate) key: K,
    pub(crate) value: V,
    pub(crate) expires_at_ms: Option<i64>,
}

#[derive(Debug, Eq, PartialEq)]
pub(crate) enum PlannedSet<K, V> {
    Upsert(PlannedUpsert<K, V>),
    Delete(K),
}

#[derive(Debug, Eq, PartialEq)]
pub(crate) struct MutationPlan<K, V> {
    pub(crate) upserts: Vec<PlannedUpsert<K, V>>,
    pub(crate) deletes: Vec<K>,
}

pub(crate) struct MutationPlanner<'a, C> {
    codec: &'a C,
    namespace: &'a str,
}

impl<'a, C> MutationPlanner<'a, C> {
    pub(crate) fn new(codec: &'a C, namespace: &'a str) -> Self {
        Self { codec, namespace }
    }
}

impl<C> MutationPlanner<'_, C> {
    pub(crate) fn plan_set<K, V>(
        &self,
        key: &K,
        value: &V,
        ttl: i64,
        now_ms: Option<i64>,
    ) -> KapeResult<PlannedSet<C::EncodedKey, C::EncodedValue>>
    where
        C: PostgresCodec<K, V>,
    {
        validate_ttl(ttl)?;

        let key = self.codec.encode_key(key)?;
        let key = C::EncodedKey::join(C::EncodedKey::namespace_prefix(self.namespace), key);
        if ttl == 0 {
            return Ok(PlannedSet::Delete(key));
        }

        let value = self.codec.encode_value(value)?;
        let expires_at_ms = match ttl {
            -1 => None,
            ttl => Some(
                now_ms
                    .and_then(|now_ms| now_ms.checked_add(ttl))
                    .ok_or(PostgresBackendError::TtlOverflow)?,
            ),
        };
        Ok(PlannedSet::Upsert(PlannedUpsert {
            key,
            value,
            expires_at_ms,
        }))
    }

    pub(crate) fn plan_many<K, V>(
        &self,
        items: &[SetItem<&K, V>],
        now_ms: Option<i64>,
    ) -> KapeResult<MutationPlan<C::EncodedKey, C::EncodedValue>>
    where
        C: PostgresCodec<K, V>,
    {
        let mut plan = MutationPlan {
            upserts: Vec::with_capacity(items.len()),
            deletes: Vec::new(),
        };
        for item in items {
            match self.plan_set(item.key, item.value.as_ref(), item.ttl, now_ms)? {
                PlannedSet::Upsert(upsert) => plan.upserts.push(upsert),
                PlannedSet::Delete(key) => plan.deletes.push(key),
            }
        }
        Ok(plan)
    }
}

/// Validates a TTL value.
pub(crate) fn validate_ttl(ttl: i64) -> KapeResult<()> {
    if ttl < -1 {
        Err(KapeError::InvalidTtl(ttl))
    } else {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::{
        fmt,
        sync::{
            Arc, Mutex,
            atomic::{AtomicUsize, Ordering},
        },
    };

    use super::*;

    #[derive(Debug)]
    struct CodecFailure;

    impl fmt::Display for CodecFailure {
        fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
            formatter.write_str("intentional planner codec failure")
        }
    }

    impl std::error::Error for CodecFailure {}

    #[derive(Default)]
    struct TestCodec {
        key_calls: AtomicUsize,
        value_calls: AtomicUsize,
        encoded_values: Mutex<Vec<String>>,
        fail_on_value: Option<String>,
    }

    impl TestCodec {
        fn failing_on(value: &str) -> Self {
            Self {
                fail_on_value: Some(value.to_owned()),
                ..Self::default()
            }
        }
    }

    impl PostgresCodec<String, String> for TestCodec {
        type EncodedKey = String;
        type EncodedValue = String;

        fn encode_key(&self, key: &String) -> Result<String, PostgresBackendError> {
            self.key_calls.fetch_add(1, Ordering::Relaxed);
            Ok(key.clone())
        }

        fn encode_value(&self, value: &String) -> Result<String, PostgresBackendError> {
            self.value_calls.fetch_add(1, Ordering::Relaxed);
            self.encoded_values.lock().unwrap().push(value.clone());
            if self.fail_on_value.as_deref() == Some(value) {
                return Err(PostgresBackendError::codec(CodecFailure));
            }
            Ok(value.clone())
        }

        fn decode_value(&self, value: String) -> Result<String, PostgresBackendError> {
            Ok(value)
        }
    }

    #[test]
    fn invalid_ttl_fails_before_encoding() {
        let codec = TestCodec::default();
        let planner = MutationPlanner::new(&codec, "");

        let result = planner.plan_set(&"key".to_owned(), &"value".to_owned(), -2, None);

        assert!(matches!(result, Err(KapeError::InvalidTtl(-2))));
        assert_eq!(codec.key_calls.load(Ordering::Relaxed), 0);
        assert_eq!(codec.value_calls.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn zero_ttl_plans_delete_without_encoding_value() {
        let codec = TestCodec::default();
        let planner = MutationPlanner::new(&codec, "orders");

        let planned = planner
            .plan_set(&"key".to_owned(), &"value".to_owned(), 0, None)
            .unwrap();

        assert_eq!(planned, PlannedSet::Delete("kape:orders:key".to_owned()));
        assert_eq!(codec.value_calls.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn immortal_ttl_plans_upsert_without_expiry() {
        let codec = TestCodec::default();
        let planner = MutationPlanner::new(&codec, "");

        let planned = planner
            .plan_set(&"key".to_owned(), &"value".to_owned(), -1, None)
            .unwrap();

        assert_eq!(
            planned,
            PlannedSet::Upsert(PlannedUpsert {
                key: "kape::key".to_owned(),
                value: "value".to_owned(),
                expires_at_ms: None,
            })
        );
    }

    #[test]
    fn positive_ttl_plans_checked_absolute_expiry() {
        let codec = TestCodec::default();
        let planner = MutationPlanner::new(&codec, "");

        let planned = planner
            .plan_set(&"key".to_owned(), &"value".to_owned(), 25, Some(1_000))
            .unwrap();

        assert!(matches!(
            planned,
            PlannedSet::Upsert(PlannedUpsert {
                expires_at_ms: Some(1_025),
                ..
            })
        ));
    }

    #[test]
    fn positive_ttl_overflow_returns_backend_error() {
        let codec = TestCodec::default();
        let planner = MutationPlanner::new(&codec, "");

        let error = planner
            .plan_set(&"key".to_owned(), &"value".to_owned(), 1, Some(i64::MAX))
            .unwrap_err();

        assert!(matches!(
            error,
            KapeError::BackendSource { ref source }
                if matches!(source.downcast_ref(), Some(PostgresBackendError::TtlOverflow))
        ));
    }

    #[test]
    fn mixed_batch_separates_intents_and_preserves_subsequence_order() {
        let codec = TestCodec::default();
        let planner = MutationPlanner::new(&codec, "batch");
        let keys = [
            "upsert-1".to_owned(),
            "delete-1".to_owned(),
            "upsert-2".to_owned(),
            "delete-2".to_owned(),
        ];
        let values = [
            "value-1".to_owned(),
            "ignored-1".to_owned(),
            "value-2".to_owned(),
            "ignored-2".to_owned(),
        ];
        let items = [
            SetItem::new(&keys[0], Arc::new(values[0].clone()), -1),
            SetItem::new(&keys[1], Arc::new(values[1].clone()), 0),
            SetItem::new(&keys[2], Arc::new(values[2].clone()), 10),
            SetItem::new(&keys[3], Arc::new(values[3].clone()), 0),
        ];

        let plan = planner.plan_many(&items, Some(100)).unwrap();

        assert_eq!(
            plan.upserts,
            vec![
                PlannedUpsert {
                    key: "kape:batch:upsert-1".to_owned(),
                    value: "value-1".to_owned(),
                    expires_at_ms: None,
                },
                PlannedUpsert {
                    key: "kape:batch:upsert-2".to_owned(),
                    value: "value-2".to_owned(),
                    expires_at_ms: Some(110),
                },
            ]
        );
        assert_eq!(
            plan.deletes,
            vec![
                "kape:batch:delete-1".to_owned(),
                "kape:batch:delete-2".to_owned(),
            ]
        );
        assert_eq!(
            *codec.encoded_values.lock().unwrap(),
            vec!["value-1".to_owned(), "value-2".to_owned()]
        );
    }

    #[test]
    fn planned_upsert_keeps_sql_fields_together() {
        let codec = TestCodec::default();
        let planner = MutationPlanner::new(&codec, "typed");

        let planned = planner
            .plan_set(&"key".to_owned(), &"value".to_owned(), 5, Some(10))
            .unwrap();
        let PlannedSet::Upsert(upsert) = planned else {
            panic!("positive TTL should plan an upsert");
        };

        assert_eq!(upsert.key, "kape:typed:key");
        assert_eq!(upsert.value, "value");
        assert_eq!(upsert.expires_at_ms, Some(15));
    }

    #[test]
    fn positive_batch_items_share_supplied_now() {
        let codec = TestCodec::default();
        let planner = MutationPlanner::new(&codec, "");
        let keys = ["first".to_owned(), "second".to_owned()];
        let values = ["one".to_owned(), "two".to_owned()];
        let items = [
            SetItem::new(&keys[0], Arc::new(values[0].clone()), 10),
            SetItem::new(&keys[1], Arc::new(values[1].clone()), 20),
        ];

        let plan = planner.plan_many(&items, Some(1_000)).unwrap();

        assert_eq!(plan.upserts[0].expires_at_ms, Some(1_010));
        assert_eq!(plan.upserts[1].expires_at_ms, Some(1_020));
    }

    #[test]
    fn codec_failure_returns_no_partial_plan() {
        let codec = TestCodec::failing_on("fail");
        let planner = MutationPlanner::new(&codec, "");
        let keys = ["first".to_owned(), "second".to_owned(), "third".to_owned()];
        let values = ["one".to_owned(), "fail".to_owned(), "three".to_owned()];
        let items = [
            SetItem::new(&keys[0], Arc::new(values[0].clone()), -1),
            SetItem::new(&keys[1], Arc::new(values[1].clone()), -1),
            SetItem::new(&keys[2], Arc::new(values[2].clone()), -1),
        ];

        let result = planner.plan_many(&items, None);

        assert!(matches!(
            result,
            Err(KapeError::BackendSource { ref source })
                if matches!(source.downcast_ref(), Some(PostgresBackendError::Codec(_)))
        ));
        assert_eq!(codec.value_calls.load(Ordering::Relaxed), 2);
        assert_eq!(
            *codec.encoded_values.lock().unwrap(),
            vec!["one".to_owned(), "fail".to_owned()]
        );
    }
}
