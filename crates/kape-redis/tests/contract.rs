use std::{sync::Arc, time::Duration};

use kape::{CacheBackend, Lookup, ResolvedTTL};
use kape_redis::{RedisBackend, StringCodec};
use kape_testkit::{
    assert_backend_contract, assert_batch_contract, assert_expiring_contract,
    assert_management_contract,
};

#[tokio::test]
#[ignore = "requires KAPE_REDIS_URL or Redis at redis://127.0.0.1/"]
async fn satisfies_backend_contract() {
    let url = std::env::var("KAPE_REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1/".to_owned());
    let namespace = format!("kape-contract-{}", std::process::id());
    let backend = RedisBackend::connect(&url, StringCodec)
        .await
        .expect("Redis connection failed")
        .namespace(namespace);

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

    let protected = RedisBackend::from_connection(backend.connection().clone(), StringCodec)
        .namespace(format!("kape-protected-{}", std::process::id()));
    protected
        .set(
            &"protected".to_owned(),
            Arc::new("value".to_owned()),
            ResolvedTTL::Never,
        )
        .await
        .expect("protected namespace write failed");
    backend.clear().await.expect("namespace clear failed");
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
