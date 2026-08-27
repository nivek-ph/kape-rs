# kape-testkit

Reusable contract checks for Kape backend adapters.

`assert_adapter_contract` exercises the complete public `CacheBackend` seam
for string adapters: scalar and batch
miss/hit behavior, `i64` millisecond TTL boundaries, expiry, duplicate read
positions, duplicate batch-write rejection, removal, and clear. Built-in
adapters use the same suite while keeping service-backed Redis and `PostgreSQL`
runs explicit. Granular generic helpers remain available for other key and
value types. `get_random_string` supplies unique keys for examples and tests.

```toml
[dev-dependencies]
kape-testkit = "0.1.0"
tokio = { version = "1", features = ["macros", "rt-multi-thread", "time"] }
```

```rust,no_run
use kape::CacheBackend;
use kape_testkit::assert_adapter_contract;

async fn check_backend<B>(backend: &B)
where
    B: CacheBackend<String, String>,
{
    assert_adapter_contract(backend, 100).await;
}
```

`assert_adapter_contract` and `assert_expiring_contract` use
`tokio::time::sleep`, so they must run inside a Tokio runtime, such as a
`#[tokio::test]`. The remaining granular helpers do not perform timed waits.
