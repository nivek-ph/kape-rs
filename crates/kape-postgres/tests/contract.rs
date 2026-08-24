use std::{fmt, sync::Arc};

use kape::{Cache, CacheBackend, KapeError, Operation};
use kape_postgres::{PostgresBackend, PostgresBackendError, PostgresCodec};
use kape_testkit::{
    assert_backend_contract, assert_batch_contract, assert_clear_contract, assert_expiring_contract,
};

#[derive(Debug)]
struct CodecError;

impl fmt::Display for CodecError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("intentional codec failure")
    }
}

impl std::error::Error for CodecError {}

#[derive(Clone, Copy)]
struct FailingCodec;

impl PostgresCodec<String, String> for FailingCodec {
    type EncodedKey = String;
    type EncodedValue = String;

    fn encode_key(&self, key: &String) -> Result<String, PostgresBackendError> {
        Ok(key.clone())
    }

    fn encode_value(&self, _value: &String) -> Result<String, PostgresBackendError> {
        Err(PostgresBackendError::codec(CodecError))
    }

    fn decode_value(&self, _value: String) -> Result<String, PostgresBackendError> {
        Err(PostgresBackendError::codec(CodecError))
    }
}

#[tokio::test]
async fn rejects_unsafe_table_identifiers_without_connecting() {
    let pool = sqlx::postgres::PgPoolOptions::new()
        .connect_lazy("postgres://postgres:postgres@127.0.0.1/kape")
        .expect("test URL should parse");
    let result =
        PostgresBackend::<String, String>::new(pool).with_table("public.entries;DROP TABLE users");
    let Err(KapeError::BackendSource { source }) = result else {
        panic!("unsafe table identifier should fail");
    };
    assert!(matches!(
        source.downcast_ref::<PostgresBackendError>(),
        Some(PostgresBackendError::InvalidTableName(_))
    ));
}

#[tokio::test]
#[ignore = "requires KAPE_POSTGRES_URL"]
async fn satisfies_backend_contract() {
    let url = std::env::var("KAPE_POSTGRES_URL").expect("KAPE_POSTGRES_URL is required");
    let pool = sqlx::PgPool::connect(&url)
        .await
        .expect("PostgreSQL connection failed");
    sqlx::query(
        "CREATE TABLE IF NOT EXISTS kape_text_contract_entries (\
         key TEXT PRIMARY KEY, value TEXT NOT NULL, expires_at_ms BIGINT NULL\
         )",
    )
    .execute(&pool)
    .await
    .expect("application-owned test table setup failed");

    let namespace = format!("kape-contract-[*]:{}", std::process::id());
    let backend = PostgresBackend::new(pool.clone())
        .with_table("kape_text_contract_entries")
        .unwrap()
        .namespace(namespace);
    assert_backend_contract(&backend, &"round-trip".to_owned(), String::new()).await;
    assert_expiring_contract(&backend, &"ttl".to_owned(), "value".to_owned(), 1_000).await;
    assert_batch_contract(
        &backend,
        &"batch-first".to_owned(),
        &"batch-second".to_owned(),
        &"batch-missing".to_owned(),
        "first".to_owned(),
        "second".to_owned(),
    )
    .await;

    let codec_cache = Cache::builder()
        .backend(
            "postgres-codec",
            PostgresBackend::new(pool.clone())
                .with_table("kape_text_contract_entries")
                .unwrap()
                .with_codec(FailingCodec)
                .namespace(format!("kape-codec:{}", std::process::id())),
        )
        .build()
        .expect("codec cache should build");
    let error = codec_cache
        .set(&"codec".to_owned(), Arc::new("value".to_owned()), -1)
        .await
        .expect_err("codec write should fail");
    let KapeError::Backend(failure) = error else {
        panic!("codec failure should be named");
    };
    assert_eq!(failure.backend.as_ref(), "postgres-codec");
    assert_eq!(failure.operation, Operation::Set);
    assert!(matches!(
        failure.source.downcast_ref::<PostgresBackendError>(),
        Some(PostgresBackendError::Codec(_))
    ));

    let protected = PostgresBackend::new(pool.clone())
        .with_table("kape_text_contract_entries")
        .unwrap()
        .namespace(format!("kape-protected-?*:{}", std::process::id()));
    protected
        .set(&"protected".to_owned(), Arc::new("value".to_owned()), -1)
        .await
        .unwrap();
    backend.clear().await.unwrap();
    let protected_hit = protected.get(&"protected".to_owned()).await.unwrap();
    assert!(protected_hit.is_some());

    backend
        .set(&"expired".to_owned(), Arc::new("value".to_owned()), 1)
        .await
        .unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(25)).await;
    assert!(backend.purge_expired().await.unwrap() >= 1);
    let protected_hit = protected.get(&"protected".to_owned()).await.unwrap();
    assert!(protected_hit.is_some());

    let overflow = backend
        .set(
            &"overflow".to_owned(),
            Arc::new("value".to_owned()),
            i64::MAX,
        )
        .await;
    let Err(KapeError::BackendSource { source }) = overflow else {
        panic!("unrepresentable TTL should be rejected");
    };
    assert!(matches!(
        source.downcast_ref::<PostgresBackendError>(),
        Some(PostgresBackendError::TtlOverflow)
    ));

    assert_clear_contract(
        &backend,
        &"clear-first".to_owned(),
        &"clear-second".to_owned(),
        "first".to_owned(),
        "second".to_owned(),
    )
    .await;
    protected.clear().await.unwrap();
    pool.close().await;
}
