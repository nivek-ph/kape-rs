use std::{sync::Arc, time::Duration};

use kape::{BackendCapability, CacheBackend, Lookup, ResolvedTTL};
use kape_postgres::{PostgresBackend, StringCodec};
use kape_testkit::{
    assert_backend_contract, assert_batch_contract, assert_expiring_contract,
    assert_management_contract,
};

#[tokio::test]
#[ignore = "requires KAPE_POSTGRES_URL"]
async fn satisfies_backend_contract() {
    let url = std::env::var("KAPE_POSTGRES_URL").expect("KAPE_POSTGRES_URL is required");
    let pool = sqlx::PgPool::connect(&url)
        .await
        .expect("PostgreSQL connection failed");
    sqlx::query(
        "CREATE TABLE IF NOT EXISTS kape_entries (\
         key BYTEA PRIMARY KEY, \
         value BYTEA NOT NULL, \
         expires_at_ms BIGINT NULL\
         )",
    )
    .execute(&pool)
    .await
    .expect("test table setup failed");
    let namespace = format!("kape-contract-{}", std::process::id());
    let backend = PostgresBackend::new(pool, StringCodec).namespace(namespace);
    backend
        .check_table()
        .await
        .expect("test table should exist");

    let missing_table = format!("kape_missing_{}", std::process::id());
    let missing = PostgresBackend::<String, String, _>::new(backend.pool().clone(), StringCodec)
        .with_table(&missing_table)
        .expect("generated table name should be valid");
    assert!(matches!(
        missing.check_table().await,
        Err(kape_postgres::PostgresBackendError::TableNotFound(_))
    ));

    assert_backend_contract(&backend, &"round-trip".to_owned(), String::new()).await;
    assert_expiring_contract(
        &backend,
        &"ttl".to_owned(),
        "value".to_owned(),
        Duration::from_secs(10),
    )
    .await;
    assert_batch_contract(
        &backend,
        &"batch-first".to_owned(),
        &"batch-second".to_owned(),
        &"batch-missing".to_owned(),
        "first".to_owned(),
        "second".to_owned(),
    )
    .await;
    backend
        .remove(&"ttl".to_owned())
        .await
        .expect("contract cleanup failed");

    let protected = PostgresBackend::new(backend.pool().clone(), StringCodec)
        .namespace(format!("kape-protected-{}", std::process::id()));
    protected
        .set(
            &"protected".to_owned(),
            Arc::new("value".to_owned()),
            ResolvedTTL::Never,
        )
        .await
        .expect("protected namespace write failed");
    assert!(matches!(
        backend.clear().await.expect("namespace clear failed"),
        BackendCapability::Supported(())
    ));
    assert!(matches!(
        protected
            .get(&"protected".to_owned())
            .await
            .expect("protected namespace read failed"),
        Lookup::Hit(_)
    ));
    protected
        .remove(&"protected".to_owned())
        .await
        .expect("protected namespace cleanup failed");

    assert_management_contract(
        &backend,
        &"management-first".to_owned(),
        &"management-second".to_owned(),
        "first".to_owned(),
        "second".to_owned(),
    )
    .await;
}
