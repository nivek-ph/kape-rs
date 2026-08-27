use std::{fmt, sync::Arc};

use kape::{Cache, CacheBackend, KapeError, Operation};
use kape_redis::{RedisBackend, RedisBackendError, RedisCodec, StringCodec};
use kape_testkit::assert_adapter_contract;
use redis::aio::ConnectionManager;

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

impl RedisCodec<String, String> for FailingCodec {
    fn encode_key(&self, key: &String) -> Result<Vec<u8>, RedisBackendError> {
        Ok(key.as_bytes().to_vec())
    }

    fn encode_value(&self, _value: &String) -> Result<Vec<u8>, RedisBackendError> {
        Err(RedisBackendError::codec(CodecError))
    }

    fn decode_value(&self, _bytes: &[u8]) -> Result<String, RedisBackendError> {
        Err(RedisBackendError::codec(CodecError))
    }
}

#[tokio::test]
#[ignore = "requires KAPE_REDIS_URL or Redis at redis://127.0.0.1/"]
async fn satisfies_backend_contract() {
    let url = std::env::var("KAPE_REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1/".to_owned());
    let client = redis::Client::open(url.as_str()).expect("Redis URL should be valid");
    let manager = ConnectionManager::new(client)
        .await
        .expect("Redis connection failed");
    let namespace = format!("kape-contract-{}", std::process::id());
    let backend = RedisBackend::from_connection(manager.clone(), StringCodec).namespace(namespace);

    let url_backend = RedisBackend::connect(&url)
        .await
        .expect("URL constructor failed")
        .namespace(format!("kape-url-constructor:{}", std::process::id()));
    url_backend
        .set(&"url".to_owned(), Arc::new("value".to_owned()), -1)
        .await
        .expect("URL-constructed backend write failed");
    url_backend
        .clear()
        .await
        .expect("URL backend cleanup failed");

    assert_adapter_contract(&backend, 100).await;

    let codec_cache = Cache::builder()
        .backend(
            "redis-codec",
            RedisBackend::from_connection(manager.clone(), FailingCodec)
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
    assert_eq!(failure.backend.as_ref(), "redis-codec");
    assert_eq!(failure.operation, Operation::Set);
    assert!(matches!(
        failure.source.downcast_ref::<RedisBackendError>(),
        Some(RedisBackendError::Codec(_))
    ));

    let overflow = backend
        .set(
            &"overflow".to_owned(),
            Arc::new("value".to_owned()),
            i64::MAX,
        )
        .await;
    assert!(matches!(overflow, Err(KapeError::BackendSource { .. })));

    let protected = RedisBackend::from_connection(manager, StringCodec)
        .namespace(format!("kape-protected-{}", std::process::id()));
    protected
        .set(&"protected".to_owned(), Arc::new("value".to_owned()), -1)
        .await
        .expect("protected namespace write failed");
    backend.clear().await.expect("namespace clear failed");
    let protected_hit = protected.get(&"protected".to_owned()).await.unwrap();
    assert!(protected_hit.is_some());
    protected.clear().await.expect("protected cleanup failed");
}
