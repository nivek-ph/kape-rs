# kape-redis

Redis adapter for Kape using Redis' asynchronous connection manager.

```toml
[dependencies]
kape = "0.1.0"
kape-redis = "0.1.0"
tokio = { version = "1", features = ["macros", "rt-multi-thread"] }
```

```rust,no_run
use kape_redis::RedisBackend;

async fn example() -> Result<(), kape::KapeError> {
    let backend = RedisBackend::<String, String>::connect("redis://127.0.0.1/")
        .await?
        .namespace("my-service");
    let _ = backend;
    Ok(())
}
```

`RedisBackend` defaults to `String` keys, `String` values, and `StringCodec`.
Use `BytesCodec` with `Vec<u8>` keys and values when raw bytes are required, or
provide a custom `RedisCodec<K, V>` with `with_codec`. Applications may retain
their own `ConnectionManager` clone and use `from_connection`.

The namespace is used directly in readable keys such as
`kape:my-service:user:42`. Callers own namespace uniqueness and storage safety;
a simple identifier containing letters, digits, `-`, or `_` is recommended.
The adapter does not encode, escape, or validate the namespace. In particular,
Redis glob characters can broaden `clear`, which appends `*` and uses `SCAN`
followed by `DEL`. It never uses `FLUSHDB` or `FLUSHALL`. On a large database,
scanning can be expensive and should be scheduled with that trade-off in mind.
One bounded scan removes entries observed during that traversal; clear is not
linearizable with concurrent writes to the same namespace.

Reads obtain `GET` and exact `PTTL` in an atomic pipeline. Batch reads preserve
positions and duplicate keys; batch writes reject duplicate keys before using a
transactional pipeline. TTL zero is implemented as deletion, avoiding an
invalid zero-millisecond `SET`.

TLS is opt-in through the `native-tls` and `rustls` features. Connection
lifecycle remains owned by application handles.
