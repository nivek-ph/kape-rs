# kape-testkit

Reusable contract checks for Kape backend adapters.

The helpers exercise the public `CacheBackend` seam: scalar and batch
miss/hit behavior, `i64` millisecond TTL boundaries, expiry, duplicate
positions, removal, and clear. Built-in adapters use the same suite while
keeping service-backed Redis and `PostgreSQL` runs explicit. `get_random_string`
supplies unique keys for examples and tests.

```toml
[dev-dependencies]
kape-testkit = "0.1.0-alpha.1"
tokio = { version = "1", features = ["macros", "rt-multi-thread", "time"] }
```

```rust,no_run
use kape::CacheBackend;
use kape_testkit::{assert_backend_contract, assert_expiring_contract};

async fn check_backend<B>(backend: &B)
where
    B: CacheBackend<String, String>,
{
    assert_backend_contract(backend, &"contract".to_owned(), String::new()).await;
    assert_expiring_contract(backend, &"ttl".to_owned(), "value".to_owned(), 100).await;
}
```

`assert_expiring_contract` uses `tokio::time::sleep`, so expiration checks must
run inside a Tokio runtime, such as a `#[tokio::test]`. The remaining helpers do
not perform timed waits.
